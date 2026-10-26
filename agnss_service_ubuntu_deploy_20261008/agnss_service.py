import os
os.environ["MAVLINK20"] = "1"
os.environ["MAVLINK_20"] = "1"
#当前系统主要实现的是 GPS、Galileo、QZSS，以及实验性 BDS；GLONASS 部分因为实时 SFRBX 解码复杂，稳定版默认不参与封装和注入
import time
import struct
import threading
import datetime as dt
import json
import sys
import csv
import uuid
import zlib
import socket
import errno
import urllib.error
import urllib.request
from pathlib import Path
from dataclasses import dataclass, asdict, field
from typing import Dict, Optional, List, Tuple, Callable, Set

import serial
from pymavlink import mavutil

APP_DIR = Path(__file__).resolve().parent
LOG_DIR = APP_DIR / "logs"
ALERT_DIR = APP_DIR / "alerts"
ALERT_OUTBOX_DIR = ALERT_DIR / "outbox"

# 尽量把底层崩溃/无声退出写入文件，便于排查。
try:
    import faulthandler
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    _fault_log = open(LOG_DIR / "agnss_fault_log.txt", "a", encoding="utf-8")
    faulthandler.enable(file=_fault_log, all_threads=True)
except Exception:
    faulthandler = None

SCRIPT_VERSION = "2026-08-10_v6_neo3pro_can_fastboot_v14"
CAN_REQUIRED_PX4_PARAMS = {
    "UAVCAN_ENABLE": {"description": "PX4 UAVCAN/DroneCAN driver enabled", "minimum": 1.0},
    "UAVCAN_SUB_GPS": {"description": "PX4 subscribes to DroneCAN GNSS", "equals": 1.0},
    "UAVCAN_PUB_RTCM": {"description": "PX4 publishes RTCMStream (DTID 1062)", "equals": 1.0},
}
MAV_SYS_STATUS_PREARM_CHECK = getattr(
    mavutil.mavlink,
    "MAV_SYS_STATUS_PREARM_CHECK",
    1 << 28,
)


def create_resilient_tcp_mavlink_connection(
    endpoint: str,
    bind_address: str = "",
    source_system: int = 255,
    source_component: int = 190,
):
    """Create a TCP client that lets the service own reconnect/backoff policy.

    pymavlink's default TCP reader prints an EOF and returns an empty read. A
    closed peer can therefore make a receive thread spin and print EOF forever.
    Raising here lets the service mark the link offline and reconnect once per
    maintenance interval instead.
    """
    endpoint = str(endpoint).strip()
    bind_address = str(bind_address).strip()
    if not endpoint.lower().startswith("tcp:"):
        raise ValueError("bind_address is only supported for tcp: endpoints")

    class ResilientMavTcp(mavutil.mavtcp):
        def handle_eof(self):
            raise ConnectionError("peer closed TCP socket (EOF)")

        def handle_disconnect(self):
            raise ConnectionError("peer reset TCP socket")

        def write(self, buf):
            if self.port is None:
                raise ConnectionError("TCP socket is not connected")
            try:
                return self.port.send(buf)
            except socket.error as exc:
                if exc.errno in (errno.ECONNRESET, errno.EPIPE, errno.ENOTCONN):
                    raise ConnectionError("peer closed TCP socket during send") from exc
                raise

    if not bind_address:
        return ResilientMavTcp(
            endpoint[4:],
            autoreconnect=False,
            source_system=source_system,
            source_component=source_component,
        )

    class BoundMavTcp(ResilientMavTcp):
        def __init__(self, device, local_address, **kwargs):
            self.local_address = local_address
            super().__init__(device, **kwargs)

        def do_connect(self):
            attempts = max(1, int(self.retries))
            last_error = None
            self.port = None
            for attempt in range(attempts):
                port = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    port.bind((self.local_address, 0))
                    port.connect(self.destination_addr)
                    self.port = port
                    break
                except Exception as exc:
                    last_error = exc
                    port.close()
                    if attempt + 1 < attempts:
                        time.sleep(1)

            if self.port is None:
                raise last_error or ConnectionError(
                    f"failed to connect {self.destination_addr} from {self.local_address}"
                )

            self.port.setblocking(False)
            self.port.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                os.set_inheritable(self.port.fileno(), False)
            except (AttributeError, OSError):
                pass

    return BoundMavTcp(
        endpoint[4:],
        bind_address,
        autoreconnect=False,
        source_system=source_system,
        source_component=source_component,
    )


def create_bound_tcp_mavlink_connection(
    endpoint: str,
    bind_address: str,
    source_system: int = 255,
    source_component: int = 190,
):
    """Backward-compatible wrapper for callers that explicitly bind locally."""
    return create_resilient_tcp_mavlink_connection(
        endpoint,
        bind_address=bind_address,
        source_system=source_system,
        source_component=source_component,
    )

# GLONASS 实时 SFRBX 解码需要较复杂的 GLONASS Hamming/字符串解析。
# 这里优先使用 pyrtklib 的 decode_glostr 作为实验解码后端；
# 若当前电脑未安装 pyrtklib，脚本仍可运行，但只统计 GLO，不封装 GLO-MGA。
try:
    import pyrtklib  # pip install pyrtklib
except Exception:
    pyrtklib = None


class TeeStdout:
    """
    将控制台输出同时写入日志文件。
    工控机长期运行时，建议配合 Windows 任务计划程序或 NSSM 守护本脚本。
    """

    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            try:
                stream.write(data)
                stream.flush()
            except Exception:
                pass

    def flush(self):
        for stream in self.streams:
            try:
                stream.flush()
            except Exception:
                pass


class DailyRotatingTextFile:
    """按日期和大小轮转 print 日志，并清理超过保留期的历史文件。"""

    def __init__(self, log_dir: Path, max_bytes: int, retention_days: int):
        self.log_dir = Path(log_dir)
        self.max_bytes = max(1024 * 1024, int(max_bytes))
        self.retention_days = max(1, int(retention_days))
        self.lock = threading.Lock()
        self.current_date = None
        self.current_index = 0
        self.stream = None
        # 不能只在进程启动时清理：服务可能数月不重启。按写入触发、
        # 每小时最多扫描一次，避免为每一行日志启动额外线程或定时器。
        self.cleanup_interval_s = 3600.0
        self.last_cleanup_at = 0.0
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._cleanup_old_logs()
        self.last_cleanup_at = time.time()

    def _cleanup_old_logs(self):
        cutoff = time.time() - self.retention_days * 86400
        for path in self.log_dir.glob("agnss_service_*.log"):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink()
            except OSError:
                pass

    def _cleanup_old_logs_if_due(self, now: Optional[float] = None):
        """在服务持续运行期间周期性清理过期的托管日志。"""
        now = time.time() if now is None else float(now)
        if now - self.last_cleanup_at < self.cleanup_interval_s:
            return
        self._cleanup_old_logs()
        self.last_cleanup_at = now

    def _path_for(self, date_text: str, index: int) -> Path:
        suffix = "" if index == 0 else f"_{index:02d}"
        return self.log_dir / f"agnss_service_{date_text}{suffix}.log"

    def _ensure_stream(self):
        date_text = dt.datetime.now().strftime("%Y%m%d")
        if self.current_date != date_text:
            if self.stream is not None:
                self.stream.close()
            self.current_date = date_text
            self.current_index = 0
            self.stream = None

        if self.stream is None:
            while self._path_for(date_text, self.current_index).exists():
                path = self._path_for(date_text, self.current_index)
                if path.stat().st_size < self.max_bytes:
                    break
                self.current_index += 1
            self.stream = open(
                self._path_for(date_text, self.current_index),
                "a",
                encoding="utf-8",
                buffering=1,
            )

        if self.stream.tell() >= self.max_bytes:
            self.stream.close()
            self.current_index += 1
            self.stream = open(
                self._path_for(date_text, self.current_index),
                "a",
                encoding="utf-8",
                buffering=1,
            )

    def write(self, data):
        with self.lock:
            self._cleanup_old_logs_if_due()
            self._ensure_stream()
            self.stream.write(data)

    def flush(self):
        with self.lock:
            if self.stream is not None:
                self.stream.flush()


def setup_console_file_logging(log_dir=LOG_DIR, max_mb=20, retention_days=30):
    """把 print 输出复制到轮转日志，适合 24 小时无人值守运行。"""
    try:
        log_path = Path(log_dir)
        log_f = DailyRotatingTextFile(
            log_path,
            max_bytes=int(max_mb * 1024 * 1024),
            retention_days=retention_days,
        )
        sys.stdout = TeeStdout(sys.__stdout__, log_f)
        sys.stderr = TeeStdout(sys.__stderr__, log_f)
        print(f"[Log] 控制台日志同步写入并轮转: {log_path}")
    except Exception as exc:
        print(f"[Log] 日志文件初始化失败，仅输出到控制台: {exc}")


def load_early_logging_options() -> Tuple[int, int]:
    """在创建主对象前读取日志参数，确保首次输出也进入正确轮转策略。"""
    try:
        data = json.loads((APP_DIR / "agnss_config.json").read_text(encoding="utf-8"))
        return (
            int(data.get("LOG_MAX_MB", 20)),
            int(data.get("LOG_RETENTION_DAYS", 30)),
        )
    except Exception:
        return 20, 30


_instance_lock_handle = None


def acquire_single_instance_lock() -> bool:
    """防止任务计划程序或人工操作重复启动多个服务实例。"""
    global _instance_lock_handle
    lock_path = APP_DIR / "agnss_service.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "a+b")
    try:
        handle.seek(0)
        handle.write(b"0")
        handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        _instance_lock_handle = handle
        return True
    except (OSError, IOError):
        handle.close()
        return False

# ============================================================
# 1. UBX 基础工具
# ============================================================

def ubx_checksum(body: bytes):
    ck_a = 0
    ck_b = 0

    for b in body:
        ck_a = (ck_a + b) & 0xFF
        ck_b = (ck_b + ck_a) & 0xFF

    return ck_a, ck_b


def build_ubx(cls_id: int, msg_id: int, payload: bytes = b"") -> bytes:
    body = struct.pack("<BBH", cls_id, msg_id, len(payload)) + payload
    ck_a, ck_b = ubx_checksum(body)
    return b"\xB5\x62" + body + bytes([ck_a, ck_b])


def verify_ubx(full_msg: bytes) -> bool:
    if len(full_msg) < 8 or full_msg[:2] != b"\xB5\x62":
        return False

    ck_a, ck_b = ubx_checksum(full_msg[2:-2])
    return full_msg[-2:] == bytes([ck_a, ck_b])


def cfg_valset_u1(items: Dict[int, int], ram=True, bbr=True, flash=False) -> bytes:
    """
    UBX-CFG-VALSET，写入 U1 类型配置项。

    常用配置：
    CFG-MSGOUT-UBX_NAV_PVT_UART1   = 0x20910007
    CFG-MSGOUT-UBX_RXM_SFRBX_UART1 = 0x20910232
    """
    layers = 0

    if ram:
        layers |= 0x01

    if bbr:
        layers |= 0x02

    if flash:
        layers |= 0x04

    payload = bytearray()
    payload += struct.pack("<BBBB", 0x00, layers, 0x00, 0x00)

    for key, value in items.items():
        payload += struct.pack("<I", key)
        payload += struct.pack("<B", value & 0xFF)

    return build_ubx(0x06, 0x8A, bytes(payload))


def poll_nav_pvt() -> bytes:
    return build_ubx(0x01, 0x07)


def poll_mon_ver() -> bytes:
    return build_ubx(0x0A, 0x04)


# ============================================================
# 1.1 目标 GPS ACK / 实际使用卫星监听工具
# ============================================================

GNSS_NAME = {
    0: "GPS",
    1: "SBAS",
    2: "GAL",
    3: "BDS",
    5: "QZSS",
    6: "GLO",
    7: "NavIC",
}

MGA_INFO_CODE = {
    0: "OK：接收机接受了辅助数据",
    1: "时间未知：需要先注入 MGA-INI-TIME_UTC",
    2: "消息版本不支持",
    3: "消息长度与版本不匹配",
    4: "数据不能存入数据库",
    5: "接收机尚未准备好使用该数据",
    6: "未知消息类型",
}


def gnss_key(gnss_id: int, sv_id: int) -> str:
    name = GNSS_NAME.get(gnss_id, f"GNSS{gnss_id}")
    return f"{name}-{sv_id:02d}"


def mga_msg_name(msg_id: int, payload_start: bytes) -> str:
    if msg_id == 0x40:
        if payload_start and payload_start[0] == 0x10:
            return "MGA-INI-TIME_UTC"
        if payload_start and payload_start[0] == 0x01:
            return "MGA-INI-POS_LLH"
        return "MGA-INI"
    if msg_id == 0x00:
        gps_type = payload_start[0] if payload_start else None
        gps_type_name = {
            0x01: "EPH",
            0x02: "ALM",
            0x04: "HEALTH",
            0x05: "UTC",
            0x06: "IONO",
        }.get(gps_type, f"type={gps_type}")
        if gps_type in (0x01, 0x02):
            return f"MGA-GPS-{gps_type_name} sv={payload_start[2] if len(payload_start) >= 3 else '?'}"
        return f"MGA-GPS-{gps_type_name}"
    if msg_id == 0x02:
        return f"MGA-GAL sv={payload_start[2] if len(payload_start) >= 3 else '?'}"
    if msg_id == 0x03:
        return f"MGA-BDS sv={payload_start[2] if len(payload_start) >= 3 else '?'}"
    if msg_id == 0x05:
        return f"MGA-QZSS sv={payload_start[2] if len(payload_start) >= 3 else '?'}"
    if msg_id == 0x06:
        return f"MGA-GLO sv={payload_start[2] if len(payload_start) >= 3 else '?'}"
    if msg_id == 0x80:
        return "MGA-DBD"
    return f"MGA msgId=0x{msg_id:02X}"


def mga_ack_identity(ubx_msg: bytes) -> Optional[Tuple[int, bytes]]:
    """Return the fields echoed by UBX-MGA-ACK for a sent MGA message."""
    if (
        len(ubx_msg) < 8
        or ubx_msg[:2] != b"\xB5\x62"
        or ubx_msg[2] != 0x13
        or not verify_ubx(ubx_msg)
    ):
        return None
    return ubx_msg[3], bytes(ubx_msg[6:10]).ljust(4, b"\x00")


def mga_constellation(ubx_msg: bytes) -> str:
    if len(ubx_msg) < 4 or ubx_msg[:3] != b"\xB5\x62\x13":
        return "OTHER"
    return {
        0x00: "GPS",
        0x02: "GAL",
        0x03: "BDS",
        0x05: "QZSS",
        0x06: "GLO",
        0x40: "INI",
        0x80: "DBD",
    }.get(ubx_msg[3], "MGA_OTHER")


@dataclass(frozen=True)
class MGAckEvent:
    sequence: int
    received_at: float
    ack_type: int
    version: int
    info_code: int
    acked_msg_id: int
    payload_start: bytes
    name: str
    accepted: bool


def build_cfg_ack_aiding(enable=True) -> bytes:
    """
    CFG-NAVSPG-ACKAIDING = true/false。
    只有打开该项，目标 GPS 才会输出 UBX-MGA-ACK。

    UART 路径需要 PX4 gps.cpp 旁路允许 UBX-CFG-VALSET；CAN 路径还要求
    PX4 发布 RTCMStream，且目标 CAN GPS 已验证可透传该原始 UBX 消息。
    """
    return cfg_valset_u1({0x10110025: 1 if enable else 0}, ram=True, bbr=True, flash=False)


def build_cfg_navsat_uart1(enable=True) -> bytes:
    """
    打开目标 GPS 的 UBX-NAV-SAT UART1 输出，用于统计实际参与解算的卫星。
    CFG-MSGOUT-UBX_NAV_SAT_UART1 = 0x20910016。
    """
    return cfg_valset_u1({0x20910016: 1 if enable else 0}, ram=True, bbr=True, flash=False)


class TargetGpsAckSatMonitor:
    """
    旁路监听目标无人机 GPS 的 TX 输出。

    接线方式：
        目标 GPS TX  -> USB-TTL RX
        目标 GPS GND -> USB-TTL GND
        不要接 USB-TTL TX 到 GPS RX，避免和飞控 TX 冲突。

    可解析：
        UBX-MGA-ACK  class=0x13 id=0x60
        UBX-NAV-SAT  class=0x01 id=0x35
    """

    def __init__(self, port: str, baud: int, get_injected_keys: Callable[[], Set[str]]):
        self.port = port
        self.baud = baud
        self.get_injected_keys = get_injected_keys
        self.running = False
        self.buf = b""
        self.thread = None
        self.latest_used_keys: Set[str] = set()
        self.latest_seen_keys: Set[str] = set()
        self.ack_total = 0
        self.ack_ok = 0
        self.ack_not_used = 0
        self.ack_by_name: Dict[str, Dict[str, int]] = {}
        self.connected = False
        self.last_rx_time: Optional[float] = None
        self.nav_sat_last_time: Optional[float] = None
        self._ack_sequence = 0
        self._ack_events: List[MGAckEvent] = []
        self._ack_condition = threading.Condition()

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()
        print(f"[ACK] 目标 GPS TX 监听已启动: {self.port}, baud={self.baud}")

    def stop(self):
        self.running = False
        with self._ack_condition:
            self._ack_condition.notify_all()

    def _parse_stream(self):
        messages = []
        while len(self.buf) >= 8:
            if self.buf[:2] != b"\xB5\x62":
                self.buf = self.buf[1:]
                continue

            payload_len = self.buf[4] + (self.buf[5] << 8)
            full_len = 6 + payload_len + 2
            if len(self.buf) < full_len:
                break

            msg = self.buf[:full_len]
            if verify_ubx(msg):
                messages.append(msg)
                self.buf = self.buf[full_len:]
            else:
                self.buf = self.buf[1:]

        return messages

    def _worker(self):
        while self.running:
            try:
                with serial.Serial(self.port, self.baud, timeout=0.1) as ser:
                    self.connected = True
                    print("[ACK] 已打开目标 GPS TX 监听串口，等待 MGA-ACK / NAV-SAT ...")
                    while self.running:
                        data = ser.read(ser.in_waiting or 1)
                        if data:
                            self.last_rx_time = time.time()
                            self.buf += data

                        for msg in self._parse_stream():
                            cls_id = msg[2]
                            msg_id = msg[3]
                            payload = msg[6:-2]
                            if cls_id == 0x13 and msg_id == 0x60:
                                self._handle_mga_ack(payload)
                            elif cls_id == 0x01 and msg_id == 0x35:
                                self._handle_nav_sat(payload)

                        if len(self.buf) > 8192:
                            self.buf = b""
                        time.sleep(0.01)

            except Exception as e:
                self.connected = False
                print(f"\n[ACK] 目标 GPS TX 监听异常: {e}。3秒后重试。")
                time.sleep(3)

        self.connected = False

    def snapshot_ack_sequence(self) -> int:
        with self._ack_condition:
            return self._ack_sequence

    def wait_for_ack(
        self,
        ubx_msg: bytes,
        after_sequence: int,
        timeout_s: float,
    ) -> Optional[MGAckEvent]:
        identity = mga_ack_identity(ubx_msg)
        if identity is None:
            return None
        msg_id, payload_start = identity
        deadline = time.monotonic() + max(0.0, float(timeout_s))
        with self._ack_condition:
            while True:
                for event in self._ack_events:
                    if (
                        event.sequence > after_sequence
                        and event.acked_msg_id == msg_id
                        and event.payload_start == payload_start
                    ):
                        return event
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._ack_condition.wait(remaining)

    def _handle_mga_ack(self, payload: bytes):
        if len(payload) != 8:
            print(f"[MGA-ACK] payload长度异常: {len(payload)}")
            return

        ack_type = payload[0]
        version = payload[1]
        info_code = payload[2]
        acked_msg_id = payload[3]
        payload_start = payload[4:8]

        name = mga_msg_name(acked_msg_id, payload_start)
        accepted = (ack_type == 1 and info_code == 0)

        with self._ack_condition:
            self.ack_total += 1
            if accepted:
                self.ack_ok += 1
            else:
                self.ack_not_used += 1

            rec = self.ack_by_name.setdefault(name, {"OK": 0, "NO": 0})
            if accepted:
                rec["OK"] += 1
            else:
                rec["NO"] += 1

            self._ack_sequence += 1
            self._ack_events.append(
                MGAckEvent(
                    sequence=self._ack_sequence,
                    received_at=time.time(),
                    ack_type=ack_type,
                    version=version,
                    info_code=info_code,
                    acked_msg_id=acked_msg_id,
                    payload_start=bytes(payload_start),
                    name=name,
                    accepted=accepted,
                )
            )
            if len(self._ack_events) > 256:
                del self._ack_events[:-256]
            self._ack_condition.notify_all()

        state = "ACCEPTED" if accepted else "NOT_USED"
        info = MGA_INFO_CODE.get(info_code, f"未知infoCode={info_code}")
        print(
            f"\n[MGA-ACK] {state} | {name} | "
            f"type={ack_type}, ver={version}, infoCode={info_code}({info}) | "
            f"payloadStart={payload_start.hex(' ').upper()}"
        )

    def _handle_nav_sat(self, payload: bytes):
        if len(payload) < 8:
            return
        num_svs = payload[5]
        used = set()
        seen = set()
        off = 8
        for _ in range(num_svs):
            if off + 12 > len(payload):
                break
            gnss_id = payload[off]
            sv_id = payload[off + 1]
            cno = payload[off + 2]
            flags = struct.unpack_from("<I", payload, off + 8)[0]
            key = gnss_key(gnss_id, sv_id)
            seen.add(key)
            sv_used = bool(flags & 0x08)   # bit 3 = svUsed
            if sv_used:
                used.add(key)
            off += 12

        changed = used != self.latest_used_keys or seen != self.latest_seen_keys
        self.latest_used_keys = used
        self.latest_seen_keys = seen
        self.nav_sat_last_time = time.time()
        if changed:
            injected = self.get_injected_keys()
            common = sorted(used & injected)
            missing = sorted(used - injected)
            injected_not_used = sorted(injected - used)
            print(
                f"\n[NAV-SAT] 目标GPS当前用于解算: {sorted(used)}\n"
                f"          注入且被使用: {common}\n"
                f"          被使用但未在本脚本注入: {missing}\n"
                f"          已注入但当前未使用: {injected_not_used}"
            )

    @staticmethod
    def _count_constellation(keys: Set[str]) -> Dict[str, int]:
        counts = {}
        for key in keys:
            sys_name = key.split("-", 1)[0]
            counts[sys_name] = counts.get(sys_name, 0) + 1
        return counts

    def print_target_used_snapshot(self, title="目标GPS NAV-SAT快照"):
        """打印目标GPS最近一次 NAV-SAT 中实际 used 的卫星统计。"""
        print(f"\n========== {title} ==========")
        if not self.latest_used_keys:
            print("[NAV-SAT] 尚未解析到目标 GPS 的 UBX-NAV-SAT。")
            print("[NAV-SAT] 请确认：目标GPS TX -> USB-TTL RX，且目标GPS已开启 UBX-NAV-SAT UART输出。")
            print("==========================================\n")
            return

        injected = self.get_injected_keys()
        used = set(self.latest_used_keys)
        seen = set(self.latest_seen_keys)
        common = used & injected
        missing = used - injected
        injected_not_used = injected - used
        bds_used = {k for k in used if k.startswith("BDS-")}
        bds_common = {k for k in common if k.startswith("BDS-")}

        print(f"[NAV-SAT] 目标GPS可见卫星数: {len(seen)}")
        print(f"[NAV-SAT] 目标GPS实际used卫星数: {len(used)}")
        print(f"[NAV-SAT] used星座统计: {self._count_constellation(used)}")
        print(f"[NAV-SAT] BDS used数量: {len(bds_used)} | {sorted(bds_used)}")
        print(f"[NAV-SAT] 本脚本已注入星历总数: {len(injected)}")
        print(f"[NAV-SAT] 注入且被used数量: {len(common)} | {sorted(common)}")
        print(f"[NAV-SAT] 注入且被used的BDS数量: {len(bds_common)} | {sorted(bds_common)}")
        print(f"[NAV-SAT] used但未由本脚本注入: {sorted(missing)}")
        print(f"[NAV-SAT] 已注入但当前未used: {sorted(injected_not_used)}")
        print("==========================================\n")

    def summary(self):
        print("\n========== MGA-ACK / NAV-SAT 监听摘要 ==========")
        print(f"[ACK] 总ACK数={self.ack_total}, accepted={self.ack_ok}, not_used={self.ack_not_used}")
        for name, rec in sorted(self.ack_by_name.items()):
            print(f"[ACK] {name}: OK={rec['OK']}, NOT_USED={rec['NO']}")
        self.print_target_used_snapshot("目标GPS最近一次实际used卫星统计")
        print("================================================\n")


# ============================================================
# 2. GPS / QZSS L1C/A LNAV 位操作
# ============================================================

def twos(value: int, bits: int) -> int:
    sign = 1 << (bits - 1)

    if value & sign:
        value -= 1 << bits

    return value


def get_word_bits(data24: int, start: int, length: int) -> int:
    """
    从 GPS/QZSS LNAV 24 bit 数据字中取字段。

    start 为 1-based：
    start=1 表示最高位；
    start=24 表示最低位。
    """
    if start < 1 or start + length - 1 > 24:
        raise ValueError(f"非法位域: start={start}, length={length}")

    shift = 24 - (start + length - 1)
    mask = (1 << length) - 1

    return (data24 >> shift) & mask


# ============================================================
# 3. GPS / QZSS 通用 LNAV 星历结构
# ============================================================

@dataclass
class LNAVEphemeris:
    system: str
    sv_id: int

    fit_interval: int
    ura_index: int
    sv_health: int
    tgd: int
    iodc: int
    toc: int
    af2: int
    af1: int
    af0: int

    crs: int
    delta_n: int
    m0: int
    cuc: int
    cus: int
    e: int
    sqrt_a: int
    toe: int

    cic: int
    omega0: int
    cis: int
    crc: int
    i0: int
    omega: int
    omega_dot: int
    idot: int

    iode: int
    created_at: float


class LNAVCollector:
    """
    GPS / QZSS L1C/A LNAV 星历收集器。

    GPS:
        gnss_id = 0
        sig_id  = 0
        sv_id   = 1..32

    QZSS:
        gnss_id = 5
        sig_id  = 0
        sv_id   = 1..10
        说明：实测 u-center 中可能出现 QZSS svId=7，
        因此这里不再限制到 1..5，避免误过滤。
    """

    def __init__(self, system_name: str, gnss_id: int, sig_id: int, sv_min: int, sv_max: int):
        self.system_name = system_name
        self.gnss_id = gnss_id
        self.sig_id = sig_id
        self.sv_min = sv_min
        self.sv_max = sv_max

        self.subframes: Dict[int, Dict[int, List[int]]] = {}
        self.last_ephemeris: Dict[int, LNAVEphemeris] = {}

        self.total_sfrbx = 0
        self.total_eph = 0

    def handle_sfrbx_payload(self, payload: bytes) -> Optional[LNAVEphemeris]:
        if len(payload) < 8:
            return None

        gnss_id = payload[0]
        sv_id = payload[1]
        sig_id = payload[2]
        num_words = payload[4]

        if gnss_id != self.gnss_id:
            return None

        if sig_id != self.sig_id:
            return None

        if not (self.sv_min <= sv_id <= self.sv_max):
            return None

        if num_words != 10:
            return None

        expected_len = 8 + 4 * num_words

        if len(payload) < expected_len:
            return None

        words = list(struct.unpack_from("<" + "I" * num_words, payload, 8))

        # u-blox GPS/QZSS LNAV dwrd 低 6 位为 parity，bit6..bit29 为 24bit 数据
        data24 = [(w >> 6) & 0xFFFFFF for w in words]

        preamble = get_word_bits(data24[0], 1, 8)

        if preamble != 0x8B:
            return None

        # HOW 中 subframe ID 位于第 2 个数据字的 bit20..22
        subframe_id = get_word_bits(data24[1], 20, 3)

        if subframe_id not in (1, 2, 3):
            return None

        self.total_sfrbx += 1
        self.subframes.setdefault(sv_id, {})[subframe_id] = data24

        if all(k in self.subframes[sv_id] for k in (1, 2, 3)):
            eph = self._try_build_ephemeris(sv_id)

            if eph:
                old = self.last_ephemeris.get(sv_id)

                # 相同 IODE 不重复刷新 created_at。否则旧星历会每 60 秒被伪装成新星历，
                # 永远无法按两小时有效期淘汰。
                if old is None or old.iode != eph.iode:
                    self.last_ephemeris[sv_id] = eph
                    self.total_eph += 1
                    return eph

        return None

    def _try_build_ephemeris(self, sv_id: int) -> Optional[LNAVEphemeris]:
        sf1 = self.subframes[sv_id][1]
        sf2 = self.subframes[sv_id][2]
        sf3 = self.subframes[sv_id][3]

        # -----------------------------
        # Subframe 1：钟差与健康状态
        # -----------------------------
        ura_index = get_word_bits(sf1[2], 13, 4)
        sv_health = get_word_bits(sf1[2], 17, 6)

        iodc_msb = get_word_bits(sf1[2], 23, 2)
        iodc_lsb = get_word_bits(sf1[7], 1, 8)
        iodc = (iodc_msb << 8) | iodc_lsb

        tgd = twos(get_word_bits(sf1[6], 17, 8), 8)
        toc = get_word_bits(sf1[7], 9, 16)

        af2 = twos(get_word_bits(sf1[8], 1, 8), 8)
        af1 = twos(get_word_bits(sf1[8], 9, 16), 16)
        af0 = twos(get_word_bits(sf1[9], 1, 22), 22)

        # -----------------------------
        # Subframe 2
        # -----------------------------
        iode2 = get_word_bits(sf2[2], 1, 8)

        crs = twos(get_word_bits(sf2[2], 9, 16), 16)
        delta_n = twos(get_word_bits(sf2[3], 1, 16), 16)

        m0_raw = (get_word_bits(sf2[3], 17, 8) << 24) | get_word_bits(sf2[4], 1, 24)
        m0 = twos(m0_raw, 32)

        cuc = twos(get_word_bits(sf2[5], 1, 16), 16)

        e = (get_word_bits(sf2[5], 17, 8) << 24) | get_word_bits(sf2[6], 1, 24)

        cus = twos(get_word_bits(sf2[7], 1, 16), 16)

        sqrt_a = (get_word_bits(sf2[7], 17, 8) << 24) | get_word_bits(sf2[8], 1, 24)

        toe = get_word_bits(sf2[9], 1, 16)
        fit_interval = get_word_bits(sf2[9], 17, 1)

        # -----------------------------
        # Subframe 3
        # -----------------------------
        cic = twos(get_word_bits(sf3[2], 1, 16), 16)

        omega0_raw = (get_word_bits(sf3[2], 17, 8) << 24) | get_word_bits(sf3[3], 1, 24)
        omega0 = twos(omega0_raw, 32)

        cis = twos(get_word_bits(sf3[4], 1, 16), 16)

        i0_raw = (get_word_bits(sf3[4], 17, 8) << 24) | get_word_bits(sf3[5], 1, 24)
        i0 = twos(i0_raw, 32)

        crc = twos(get_word_bits(sf3[6], 1, 16), 16)

        omega_raw = (get_word_bits(sf3[6], 17, 8) << 24) | get_word_bits(sf3[7], 1, 24)
        omega = twos(omega_raw, 32)

        omega_dot = twos(get_word_bits(sf3[8], 1, 24), 24)

        iode3 = get_word_bits(sf3[9], 1, 8)
        idot = twos(get_word_bits(sf3[9], 9, 14), 14)

        if iode2 != iode3:
            return None

        if (iodc & 0xFF) != iode2:
            return None

        return LNAVEphemeris(
            system=self.system_name,
            sv_id=sv_id,

            fit_interval=fit_interval,
            ura_index=ura_index,
            sv_health=sv_health,
            tgd=tgd,
            iodc=iodc,
            toc=toc,
            af2=af2,
            af1=af1,
            af0=af0,

            crs=crs,
            delta_n=delta_n,
            m0=m0,
            cuc=cuc,
            cus=cus,
            e=e,
            sqrt_a=sqrt_a,
            toe=toe,

            cic=cic,
            omega0=omega0,
            cis=cis,
            crc=crc,
            i0=i0,
            omega=omega,
            omega_dot=omega_dot,
            idot=idot,

            iode=iode2,
            created_at=time.time()
        )


# ============================================================
# 4. MGA-GPS-EPH / MGA-QZSS-EPH 封装
# ============================================================

def build_mga_lnav_eph(e: LNAVEphemeris, mga_msg_id: int) -> bytes:
    """
    GPS:
        Class = 0x13
        ID    = 0x00
        Payload length = 68

    QZSS:
        Class = 0x13
        ID    = 0x05
        Payload length = 68
    """
    fmt = "<BBBBBBBbHHBbhihhihhIIHhihhiiihBB"

    payload = struct.pack(
        fmt,
        0x01,                    # type: ephemeris
        0x00,                    # version
        e.sv_id,
        0x00,                    # reserved0

        e.fit_interval & 0xFF,
        e.ura_index & 0xFF,
        e.sv_health & 0xFF,
        e.tgd,

        e.iodc,
        e.toc,
        0x00,                    # reserved1
        e.af2,
        e.af1,
        e.af0,

        e.crs,
        e.delta_n,
        e.m0,
        e.cuc,
        e.cus,
        e.e,
        e.sqrt_a,
        e.toe,
        e.cic,

        e.omega0,
        e.cis,
        e.crc,
        e.i0,
        e.omega,
        e.omega_dot,
        e.idot,
        0x00,
        0x00
    )

    if len(payload) != 68:
        raise RuntimeError(f"{e.system} EPH payload 长度错误: {len(payload)}")

    return build_ubx(0x13, mga_msg_id, payload)


def build_mga_gps_eph(e: LNAVEphemeris) -> bytes:
    return build_mga_lnav_eph(e, 0x00)


def build_mga_qzss_eph(e: LNAVEphemeris) -> bytes:
    return build_mga_lnav_eph(e, 0x05)


# ============================================================
# 4.1 Galileo I/NAV + BeiDou B1I D1/D2 辅助星历解析/封装（实验版）
# ============================================================

CRC24Q_POLY = 0x1864CFB


def crc24q(data: bytes) -> int:
    """CRC-24Q，Galileo I/NAV page CRC 校验使用。"""
    crc = 0
    for b in data:
        crc ^= (b << 16)
        for _ in range(8):
            crc <<= 1
            if crc & 0x1000000:
                crc ^= CRC24Q_POLY
            crc &= 0xFFFFFF
    return crc & 0xFFFFFF


def getbitu_bytes(buff: bytes, pos: int, length: int) -> int:
    v = 0
    for i in range(length):
        k = pos + i
        v = (v << 1) | ((buff[k // 8] >> (7 - (k % 8))) & 1)
    return v


def getbits_bytes(buff: bytes, pos: int, length: int) -> int:
    return twos(getbitu_bytes(buff, pos, length), length)


def setbitu_bytes(buff: bytearray, pos: int, length: int, value: int):
    for i in range(length):
        k = pos + i
        bit = (value >> (length - 1 - i)) & 1
        mask = 1 << (7 - (k % 8))
        if bit:
            buff[k // 8] |= mask
        else:
            buff[k // 8] &= (~mask) & 0xFF


@dataclass
class GalileoEphemeris:
    sv_id: int
    iodNav: int
    deltaN: int
    m0: int
    e: int
    sqrtA: int
    omega0: int
    i0: int
    omega: int
    omegaDot: int
    iDot: int
    cuc: int
    cus: int
    crc: int
    crs: int
    cic: int
    cis: int
    toe: int
    af0: int
    af1: int
    af2: int
    sisaIndexE1E5b: int
    toc: int
    bgdE1E5b: int
    healthE1B: int
    dataValidityE1B: int
    healthE5b: int
    dataValidityE5b: int
    created_at: float


class GalileoINavCollector:
    """
    Galileo I/NAV 星历解析器（实验版）。

    支持 u-blox RXM-SFRBX 中形如 GAL/sig?/w8 的 Galileo I/NAV page。
    解析逻辑参考 u-blox SFRBX 的 Galileo page 重组方式：每个 SFRBX page
    包含 even + odd 两个 page part，组合出 I/NAV word type 0..5 后封装
    UBX-MGA-GAL-EPH。

    注意：F/NAV 尚未启用注入。若你的日志主要是 F/NAV 信号，本类会统计到 GAL
    但不会生成可注入星历。
    """

    def __init__(self):
        self.words: Dict[int, Dict[int, bytes]] = {}
        self.last_ephemeris: Dict[int, GalileoEphemeris] = {}
        self.total_sfrbx = 0
        self.total_eph = 0
        self.crc_failed = 0
        self.unsupported = 0

    def handle_sfrbx_payload(self, payload: bytes) -> Optional[GalileoEphemeris]:
        if len(payload) < 8:
            return None

        gnss_id = payload[0]
        sv_id = payload[1]
        sig_id = payload[2]
        num_words = payload[4]

        if gnss_id != 2:
            return None
        if not (1 <= sv_id <= 36):
            return None
        if num_words < 8:
            self.unsupported += 1
            return None

        # 只取前 8 个 32-bit word。部分模块可能报告 w9，但前 8 个才是 I/NAV page 数据主体。
        raw = payload[8:8 + 8 * 4]
        if len(raw) < 32:
            return None

        # u-blox Galileo SFRBX：每个 32-bit word 需要字节反序后按 bit 解析。
        buff = bytearray()
        for i in range(8):
            w = raw[i * 4:(i + 1) * 4]
            buff.extend(w[::-1])
        buff = bytes(buff)

        part1 = getbitu_bytes(buff, 0, 1)
        page1 = getbitu_bytes(buff, 1, 1)
        part2 = getbitu_bytes(buff, 128, 1)
        page2 = getbitu_bytes(buff, 129, 1)

        # alert page 或 even/odd 顺序不对时不使用
        if page1 == 1 or page2 == 1:
            return None
        if part1 != 0 or part2 != 1:
            self.unsupported += 1
            return None

        # CRC 校验：4 pad + even 114 bits + odd 82 bits
        crc_buff = bytearray(26)
        for i in range(15):
            setbitu_bytes(crc_buff, 4 + i * 8, 8, getbitu_bytes(buff, i * 8, 8))
        for i in range(11):
            setbitu_bytes(crc_buff, 118 + i * 8, 8, getbitu_bytes(buff, 128 + i * 8, 8))
        calc = crc24q(bytes(crc_buff[:25]))
        recv = getbitu_bytes(buff, 128 + 82, 24)
        if calc != recv:
            self.crc_failed += 1
            return None

        word_type = getbitu_bytes(buff, 2, 6)
        if word_type > 5:
            return None

        self.total_sfrbx += 1

        # 保存 word type 的 128 bits：even part 从 bit2 起取 112 bits，odd part 从 bit2 起取 16 bits。
        word_data = bytearray(16)
        for i in range(14):
            word_data[i] = getbitu_bytes(buff, 2 + i * 8, 8)
        for i in range(2):
            word_data[14 + i] = getbitu_bytes(buff, 128 + 2 + i * 8, 8)

        self.words.setdefault(sv_id, {})[word_type] = bytes(word_data)

        if not all(t in self.words[sv_id] for t in (0, 1, 2, 3, 4, 5)):
            return None

        eph = self._try_build_ephemeris(sv_id)
        if not eph:
            return None

        old = self.last_ephemeris.get(sv_id)
        if old is None or old.iodNav != eph.iodNav:
            self.last_ephemeris[sv_id] = eph
            self.total_eph += 1
            return eph
        return None

    def _try_build_ephemeris(self, sv_id: int) -> Optional[GalileoEphemeris]:
        frame = b"".join(self.words[sv_id][t] for t in range(6))

        # word type 0
        i = 0
        type0 = getbitu_bytes(frame, i, 6); i += 6
        time_f = getbitu_bytes(frame, i, 2); i += 2 + 88
        # week/tow 这里不用于 MGA-GAL-EPH 封装，但用于基本一致性检查
        _week = getbitu_bytes(frame, i, 12); i += 12
        _tow = getbitu_bytes(frame, i, 20)

        # word type 1
        i = 128
        type1 = getbitu_bytes(frame, i, 6); i += 6
        iod0 = getbitu_bytes(frame, i, 10); i += 10
        toe = getbitu_bytes(frame, i, 14); i += 14
        m0 = getbits_bytes(frame, i, 32); i += 32
        ecc = getbitu_bytes(frame, i, 32); i += 32
        sqrtA = getbitu_bytes(frame, i, 32)

        # word type 2
        i = 128 * 2
        type2 = getbitu_bytes(frame, i, 6); i += 6
        iod1 = getbitu_bytes(frame, i, 10); i += 10
        omega0 = getbits_bytes(frame, i, 32); i += 32
        i0 = getbits_bytes(frame, i, 32); i += 32
        omega = getbits_bytes(frame, i, 32); i += 32
        iDot = getbits_bytes(frame, i, 14)

        # word type 3
        i = 128 * 3
        type3 = getbitu_bytes(frame, i, 6); i += 6
        iod2 = getbitu_bytes(frame, i, 10); i += 10
        omegaDot = getbits_bytes(frame, i, 24); i += 24
        deltaN = getbits_bytes(frame, i, 16); i += 16
        cuc = getbits_bytes(frame, i, 16); i += 16
        cus = getbits_bytes(frame, i, 16); i += 16
        crc = getbits_bytes(frame, i, 16); i += 16
        crs = getbits_bytes(frame, i, 16); i += 16
        sisa = getbitu_bytes(frame, i, 8)

        # word type 4
        i = 128 * 4
        type4 = getbitu_bytes(frame, i, 6); i += 6
        iod3 = getbitu_bytes(frame, i, 10); i += 10
        svid = getbitu_bytes(frame, i, 6); i += 6
        cic = getbits_bytes(frame, i, 16); i += 16
        cis = getbits_bytes(frame, i, 16); i += 16
        toc = getbitu_bytes(frame, i, 14); i += 14
        af0 = getbits_bytes(frame, i, 31); i += 31
        af1 = getbits_bytes(frame, i, 21); i += 21
        af2 = getbits_bytes(frame, i, 6)

        # word type 5
        i = 128 * 5
        type5 = getbitu_bytes(frame, i, 6); i += 6 + 41
        _bgdE1E5a = getbits_bytes(frame, i, 10); i += 10
        bgdE1E5b = getbits_bytes(frame, i, 10); i += 10
        healthE5b = getbitu_bytes(frame, i, 2); i += 2
        healthE1B = getbitu_bytes(frame, i, 2); i += 2
        dataValidityE5b = getbitu_bytes(frame, i, 1); i += 1
        dataValidityE1B = getbitu_bytes(frame, i, 1)

        if (type0, type1, type2, type3, type4, type5) != (0, 1, 2, 3, 4, 5):
            return None
        if time_f != 2:
            return None
        if not (iod0 == iod1 == iod2 == iod3):
            return None
        if svid != sv_id:
            return None

        return GalileoEphemeris(
            sv_id=sv_id,
            iodNav=iod0,
            deltaN=deltaN,
            m0=m0,
            e=ecc,
            sqrtA=sqrtA,
            omega0=omega0,
            i0=i0,
            omega=omega,
            omegaDot=omegaDot,
            iDot=iDot,
            cuc=cuc,
            cus=cus,
            crc=crc,
            crs=crs,
            cic=cic,
            cis=cis,
            toe=toe,
            af0=af0,
            af1=af1,
            af2=af2,
            sisaIndexE1E5b=sisa,
            toc=toc,
            bgdE1E5b=bgdE1E5b,
            healthE1B=healthE1B,
            dataValidityE1B=dataValidityE1B,
            healthE5b=healthE5b,
            dataValidityE5b=dataValidityE5b,
            created_at=time.time()
        )


def build_mga_gal_eph(e: GalileoEphemeris) -> bytes:
    """UBX-MGA-GAL-EPH，Class=0x13，ID=0x02，payload=76 bytes。"""
    payload = struct.pack(
        "<BBBBHhiiIiiiihhhhhhhHiibBHh2sBBBB4s",
        0x01, 0x00, e.sv_id, 0x00,
        e.iodNav,
        e.deltaN,
        e.m0,
        e.e,
        e.sqrtA,
        e.omega0,
        e.i0,
        e.omega,
        e.omegaDot,
        e.iDot,
        e.cuc,
        e.cus,
        e.crc,
        e.crs,
        e.cic,
        e.cis,
        e.toe,
        e.af0,
        e.af1,
        e.af2,
        e.sisaIndexE1E5b,
        e.toc,
        e.bgdE1E5b,
        b"\x00\x00",
        e.healthE1B,
        e.dataValidityE1B,
        e.healthE5b,
        e.dataValidityE5b,
        b"\x00\x00\x00\x00"
    )
    if len(payload) != 76:
        raise RuntimeError(f"MGA-GAL-EPH payload 长度错误: {len(payload)}")
    return build_ubx(0x13, 0x02, payload)


def words30_to_bits(words: List[int]) -> List[int]:
    bits = []
    for w in words:
        v = w & 0x3FFFFFFF
        for i in range(29, -1, -1):
            bits.append((v >> i) & 1)
    return bits


def getbitu_list(bits: List[int], pos: int, length: int) -> int:
    v = 0
    for i in range(length):
        v = (v << 1) | bits[pos + i]
    return v


def getbits_list(bits: List[int], pos: int, length: int) -> int:
    return twos(getbitu_list(bits, pos, length), length)


def combine_unsigned(parts: List[Tuple[int, int]]) -> int:
    v = 0
    for value, width in parts:
        v = (v << width) | (value & ((1 << width) - 1))
    return v


def combine_signed(parts: List[Tuple[int, int]], total_bits: int) -> int:
    return twos(combine_unsigned(parts), total_bits)


@dataclass
class BDSEphemeris:
    sv_id: int
    SatH1: int
    IODC: int
    a2: int
    a1: int
    a0: int
    toc: int
    TGD1: int
    URAI: int
    IODE: int
    toe: int
    sqrtA: int
    e: int
    omega: int
    Deltan: int
    IDOT: int
    M0: int
    Omega0: int
    OmegaDot: int
    i0: int
    Cuc: int
    Cus: int
    Crc: int
    Crs: int
    Cic: int
    Cis: int
    source: str
    created_at: float


class BDSB1ICollector:
    """
    BeiDou B1I D1/D2 星历解析器（实验版）。

    只处理 BDS B1I D1/D2。你当前日志中常见的 BDS/sig6、BDS/sig8 属于
    B1C/B2a 一类新信号，本类会统计为 unsupported，不会错误封装。
    """

    def __init__(self):
        self.d1_subframes: Dict[int, Dict[int, List[int]]] = {}
        self.d2_pages: Dict[int, Dict[int, List[int]]] = {}
        self.last_ephemeris: Dict[int, BDSEphemeris] = {}
        self.total_b1i_sfrbx = 0
        self.total_eph = 0
        self.unsupported_bds = 0

    def handle_sfrbx_payload(self, payload: bytes) -> Optional[BDSEphemeris]:
        if len(payload) < 8:
            return None
        gnss_id = payload[0]
        sv_id = payload[1]
        sig_id = payload[2]
        num_words = payload[4]
        if gnss_id != 3:
            return None
        if not (1 <= sv_id <= 37):
            return None
        if sig_id not in (0, 1):
            self.unsupported_bds += 1
            return None
        if num_words != 10:
            self.unsupported_bds += 1
            return None
        expected_len = 8 + 4 * num_words
        if len(payload) < expected_len:
            return None
        words = list(struct.unpack_from("<" + "I" * num_words, payload, 8))
        bits = words30_to_bits(words)
        self.total_b1i_sfrbx += 1
        if sig_id == 0:
            return self._handle_d1(sv_id, bits)
        if sig_id == 1:
            return self._handle_d2(sv_id, bits)
        return None

    def _handle_d1(self, sv_id: int, bits: List[int]) -> Optional[BDSEphemeris]:
        frn = getbitu_list(bits, 15, 3)
        if frn not in (1, 2, 3):
            return None
        self.d1_subframes.setdefault(sv_id, {})[frn] = bits
        if not all(k in self.d1_subframes[sv_id] for k in (1, 2, 3)):
            return None
        eph = self._decode_d1(sv_id)
        if eph:
            old = self.last_ephemeris.get(sv_id)
            if old is None or old.IODE != eph.IODE:
                self.last_ephemeris[sv_id] = eph
                self.total_eph += 1
                return eph
        return None

    def _decode_d1(self, sv_id: int) -> Optional[BDSEphemeris]:
        sf1 = self.d1_subframes[sv_id][1]
        sf2 = self.d1_subframes[sv_id][2]
        sf3 = self.d1_subframes[sv_id][3]

        frn1 = getbitu_list(sf1, 15, 3)
        sow1 = combine_unsigned([(getbitu_list(sf1, 18, 8), 8), (getbitu_list(sf1, 30, 12), 12)])
        SatH1 = getbitu_list(sf1, 42, 1)
        IODC = getbitu_list(sf1, 43, 5)
        URAI = getbitu_list(sf1, 48, 4)
        toc = combine_unsigned([(getbitu_list(sf1, 73, 9), 9), (getbitu_list(sf1, 90, 8), 8)])
        TGD1 = getbits_list(sf1, 98, 10)
        a2 = getbits_list(sf1, 214, 11)
        a0 = combine_signed([(getbitu_list(sf1, 225, 7), 7), (getbitu_list(sf1, 240, 17), 17)], 24)
        a1 = combine_signed([(getbitu_list(sf1, 257, 5), 5), (getbitu_list(sf1, 270, 17), 17)], 22)
        IODE = getbitu_list(sf1, 287, 5)

        frn2 = getbitu_list(sf2, 15, 3)
        sow2 = combine_unsigned([(getbitu_list(sf2, 18, 8), 8), (getbitu_list(sf2, 30, 12), 12)])
        Deltan = combine_signed([(getbitu_list(sf2, 42, 10), 10), (getbitu_list(sf2, 60, 6), 6)], 16)
        Cuc = combine_signed([(getbitu_list(sf2, 66, 16), 16), (getbitu_list(sf2, 90, 2), 2)], 18)
        M0 = combine_signed([(getbitu_list(sf2, 92, 20), 20), (getbitu_list(sf2, 120, 12), 12)], 32)
        ecc = combine_unsigned([(getbitu_list(sf2, 132, 10), 10), (getbitu_list(sf2, 150, 22), 22)])
        Cus = getbits_list(sf2, 180, 18)
        Crc = combine_signed([(getbitu_list(sf2, 198, 4), 4), (getbitu_list(sf2, 210, 14), 14)], 18)
        Crs = combine_signed([(getbitu_list(sf2, 224, 8), 8), (getbitu_list(sf2, 240, 10), 10)], 18)
        sqrtA = combine_unsigned([(getbitu_list(sf2, 250, 12), 12), (getbitu_list(sf2, 270, 20), 20)])
        toe1 = getbitu_list(sf2, 290, 2)

        frn3 = getbitu_list(sf3, 15, 3)
        sow3 = combine_unsigned([(getbitu_list(sf3, 18, 8), 8), (getbitu_list(sf3, 30, 12), 12)])
        toe2 = combine_unsigned([(getbitu_list(sf3, 42, 10), 10), (getbitu_list(sf3, 60, 5), 5)])
        toe = (toe1 << 15) | toe2
        i0 = combine_signed([(getbitu_list(sf3, 65, 17), 17), (getbitu_list(sf3, 90, 15), 15)], 32)
        Cic = combine_signed([(getbitu_list(sf3, 105, 7), 7), (getbitu_list(sf3, 120, 11), 11)], 18)
        OmegaDot = combine_signed([(getbitu_list(sf3, 131, 11), 11), (getbitu_list(sf3, 150, 13), 13)], 24)
        Cis = combine_signed([(getbitu_list(sf3, 163, 9), 9), (getbitu_list(sf3, 180, 9), 9)], 18)
        IDOT = combine_signed([(getbitu_list(sf3, 189, 13), 13), (getbitu_list(sf3, 210, 1), 1)], 14)
        Omega0 = combine_signed([(getbitu_list(sf3, 211, 21), 21), (getbitu_list(sf3, 240, 11), 11)], 32)
        omega = combine_signed([(getbitu_list(sf3, 251, 11), 11), (getbitu_list(sf3, 270, 21), 21)], 32)

        if (frn1, frn2, frn3) != (1, 2, 3):
            return None
        if not (sow2 == sow1 + 6 and sow3 == sow2 + 6):
            return None
        if toc != toe:
            return None

        return BDSEphemeris(sv_id, SatH1, IODC, a2, a1, a0, toc, TGD1, URAI, IODE,
                            toe, sqrtA, ecc, omega, Deltan, IDOT, M0, Omega0,
                            OmegaDot, i0, Cuc, Cus, Crc, Crs, Cic, Cis, "BDS-D1", time.time())

    def _handle_d2(self, sv_id: int, bits: List[int]) -> Optional[BDSEphemeris]:
        pgn = getbitu_list(bits, 42, 4)
        if pgn not in (1, 3, 4, 5, 6, 7, 8, 9, 10):
            return None
        self.d2_pages.setdefault(sv_id, {})[pgn] = bits
        required = (1, 3, 4, 5, 6, 7, 8, 9, 10)
        if not all(k in self.d2_pages[sv_id] for k in required):
            return None
        eph = self._decode_d2(sv_id)
        if eph:
            old = self.last_ephemeris.get(sv_id)
            if old is None or old.IODE != eph.IODE:
                self.last_ephemeris[sv_id] = eph
                self.total_eph += 1
                return eph
        return None

    def _decode_d2(self, sv_id: int) -> Optional[BDSEphemeris]:
        p = self.d2_pages[sv_id]
        pg1, pg3, pg4, pg5, pg6, pg7, pg8, pg9, pg10 = [p[k] for k in (1, 3, 4, 5, 6, 7, 8, 9, 10)]

        SatH1 = getbitu_list(pg1, 46, 1)
        IODC = getbitu_list(pg1, 47, 5)
        URAI = getbitu_list(pg1, 60, 4)
        toc = combine_unsigned([(getbitu_list(pg1, 77, 5), 5), (getbitu_list(pg1, 90, 12), 12)])
        TGD1 = getbits_list(pg1, 102, 10)

        a0 = combine_signed([(getbitu_list(pg3, 100, 12), 12), (getbitu_list(pg3, 120, 12), 12)], 24)
        f1p3 = getbitu_list(pg3, 132, 4)
        f1p4 = combine_unsigned([(getbitu_list(pg4, 46, 6), 6), (getbitu_list(pg4, 60, 12), 12)])
        a1 = combine_signed([(f1p3, 4), (f1p4, 18)], 22)
        a2 = combine_signed([(getbitu_list(pg4, 72, 10), 10), (getbitu_list(pg4, 90, 1), 1)], 11)
        IODE = getbitu_list(pg4, 91, 5)
        Deltan = getbits_list(pg4, 96, 16)
        Cuc = combine_signed([(getbitu_list(pg4, 120, 14), 14), (getbitu_list(pg5, 46, 4), 4)], 18)
        M0 = combine_signed([(getbitu_list(pg5, 50, 2), 2), (getbitu_list(pg5, 60, 22), 22), (getbitu_list(pg5, 90, 8), 8)], 32)
        Cus = combine_signed([(getbitu_list(pg5, 98, 14), 14), (getbitu_list(pg5, 120, 4), 4)], 18)
        ecc = combine_unsigned([(getbitu_list(pg5, 124, 10), 10), (getbitu_list(pg6, 46, 6), 6), (getbitu_list(pg6, 60, 16), 16)])
        sqrtA = combine_unsigned([(getbitu_list(pg6, 76, 6), 6), (getbitu_list(pg6, 90, 22), 22), (getbitu_list(pg6, 120, 4), 4)])
        Cic = combine_signed([(getbitu_list(pg6, 124, 10), 10), (getbitu_list(pg7, 46, 6), 6), (getbitu_list(pg7, 60, 2), 2)], 18)
        Cis = getbits_list(pg7, 62, 18)
        toe = combine_unsigned([(getbitu_list(pg7, 80, 2), 2), (getbitu_list(pg7, 90, 15), 15)])
        i0 = combine_signed([(getbitu_list(pg7, 105, 7), 7), (getbitu_list(pg7, 120, 14), 14), (getbitu_list(pg8, 46, 6), 6), (getbitu_list(pg8, 60, 5), 5)], 32)
        Crc = combine_signed([(getbitu_list(pg8, 65, 17), 17), (getbitu_list(pg8, 90, 1), 1)], 18)
        Crs = getbits_list(pg8, 91, 18)
        OmegaDot = combine_signed([(getbitu_list(pg8, 109, 3), 3), (getbitu_list(pg8, 120, 16), 16), (getbitu_list(pg9, 46, 5), 5)], 24)
        Omega0 = combine_signed([(getbitu_list(pg9, 51, 1), 1), (getbitu_list(pg9, 60, 22), 22), (getbitu_list(pg9, 90, 9), 9)], 32)
        omega = combine_signed([(getbitu_list(pg9, 99, 13), 13), (getbitu_list(pg9, 120, 14), 14), (getbitu_list(pg10, 46, 5), 5)], 32)
        IDOT = combine_signed([(getbitu_list(pg10, 51, 1), 1), (getbitu_list(pg10, 60, 13), 13)], 14)

        if toc != toe:
            return None
        return BDSEphemeris(sv_id, SatH1, IODC, a2, a1, a0, toc, TGD1, URAI, IODE,
                            toe, sqrtA, ecc, omega, Deltan, IDOT, M0, Omega0,
                            OmegaDot, i0, Cuc, Cus, Crc, Crs, Cic, Cis, "BDS-D2", time.time())


def build_mga_bds_eph(e: BDSEphemeris) -> bytes:
    """UBX-MGA-BDS-EPH，Class=0x13，ID=0x03，payload=88 bytes。"""
    payload = struct.pack(
        "<BBBBBBhiiIhBBIIIihhiiiiiiiiii4s",
        0x01, 0x00, e.sv_id, 0x00,
        e.SatH1 & 0xFF,
        e.IODC & 0xFF,
        e.a2,
        e.a1,
        e.a0,
        e.toc,
        e.TGD1,
        e.URAI & 0xFF,
        e.IODE & 0xFF,
        e.toe,
        e.sqrtA,
        e.e,
        e.omega,
        e.Deltan,
        e.IDOT,
        e.M0,
        e.Omega0,
        e.OmegaDot,
        e.i0,
        e.Cuc,
        e.Cus,
        e.Crc,
        e.Crs,
        e.Cic,
        e.Cis,
        b"\x00\x00\x00\x00"
    )
    if len(payload) != 88:
        raise RuntimeError(f"MGA-BDS-EPH payload 长度错误: {len(payload)}")
    return build_ubx(0x13, 0x03, payload)


# ============================================================
# 4.2 GLONASS L1OF 辅助星历解析/封装（实验版）
# ============================================================

@dataclass
class GLONASSEphemeris:
    sv_id: int
    freq_id: int          # u-blox SFRBX freqId，一般为频点号 + 7
    FT: int
    B: int
    M: int
    H: int
    x: int
    y: int
    z: int
    dx: int
    dy: int
    dz: int
    ddx: int
    ddy: int
    ddz: int
    tb: int
    gamma: int
    E: int
    deltaTau: int
    tau: int
    iode: int
    created_at: float


def _clip_int(value: int, bits: int) -> int:
    lo = -(1 << (bits - 1))
    hi = (1 << (bits - 1)) - 1
    return max(lo, min(hi, int(value)))


def _clip_uint(value: int, bits: int) -> int:
    hi = (1 << bits) - 1
    return max(0, min(hi, int(value)))


def _scale_to_int(value: float, scale: float, bits: int) -> int:
    return _clip_int(round(float(value) / scale), bits)


def _scale_to_uint(value: float, scale: float, bits: int) -> int:
    return _clip_uint(round(float(value) / scale), bits)


def build_mga_glo_eph(e: GLONASSEphemeris) -> bytes:
    """
    UBX-MGA-GLO-EPH, Class=0x13, ID=0x06, payload=48 bytes.

    说明：
    - 该封装字段布局采用 u-blox MGA-GLO-EPH 的 ECEF 状态向量格式。
    - x/y/z 单位为 km，dx/dy/dz 单位为 km/s，ddx/ddy/ddz 单位为 km/s^2，
      在 GLONASSEphemeris 中已经按 u-blox 缩放成整数。
    - GLONASS 实时 SFRBX 解码依赖 pyrtklib 的 decode_glostr，因此该功能为实验版。
    """
    payload = struct.pack(
        "<BBBBBBBBiii iii bbb B h B b i I".replace(" ", ""),
        0x01,                    # type: ephemeris
        0x00,                    # version
        e.sv_id & 0xFF,
        0x00,                    # reserved0
        e.FT & 0xFF,
        e.B & 0xFF,
        e.M & 0xFF,
        _clip_int(e.H, 8),
        _clip_int(e.x, 32),
        _clip_int(e.y, 32),
        _clip_int(e.z, 32),
        _clip_int(e.dx, 32),
        _clip_int(e.dy, 32),
        _clip_int(e.dz, 32),
        _clip_int(e.ddx, 8),
        _clip_int(e.ddy, 8),
        _clip_int(e.ddz, 8),
        e.tb & 0xFF,
        _clip_int(e.gamma, 16),
        e.E & 0xFF,
        _clip_int(e.deltaTau, 8),
        _clip_int(e.tau, 32),
        0x00000000,
    )
    if len(payload) != 48:
        raise RuntimeError(f"MGA-GLO-EPH payload 长度错误: {len(payload)}")
    return build_ubx(0x13, 0x06, payload)


class GLONASSCollector:
    """
    GLONASS L1OF SFRBX 收集器（实验版）。

    你的日志中出现 GLO/sig0/w4，说明基准站确实输出了 GLONASS L1OF 导航字符串。
    但 GLONASS 从原始字符串解出可注入 EPH 比 GPS/QZSS 更复杂，因此本类使用
    pyrtklib.decode_glostr 做字符串解码；若没有安装 pyrtklib，只统计 GLO，不生成 MGA-GLO。

    安装方式：
        pip install pyrtklib
    """

    def __init__(self):
        self.enabled = pyrtklib is not None
        self.strings: Dict[int, Dict[int, bytes]] = {}
        self.freq_id: Dict[int, int] = {}
        self.last_ephemeris: Dict[int, GLONASSEphemeris] = {}
        self.total_sfrbx = 0
        self.total_eph = 0
        self.decode_failed = 0
        self.unsupported = 0
        if not self.enabled:
            print("[GLO] 未检测到 pyrtklib：GLONASS 将只统计，不封装 MGA-GLO-EPH。可执行 pip install pyrtklib 启用实验版 GLO 注入。")

    def _sfrbx_to_glostr_10bytes(self, payload: bytes) -> Optional[Tuple[int, bytes]]:
        if len(payload) < 8:
            return None
        gnss_id = payload[0]
        sv_id = payload[1]
        sig_id = payload[2]
        freq_id = payload[3]
        num_words = payload[4]
        if gnss_id != 6:
            return None
        if sig_id != 0 or num_words != 4:
            self.unsupported += 1
            return None
        if len(payload) < 8 + 4 * num_words:
            return None
        words_le = payload[8:8 + 16]
        # RTKLIB 对 u-blox GLONASS SFRBX 的处理方式是按 32-bit word 字节反序，
        # 然后取前 10 字节作为 85-bit GLONASS 字符串输入 decode_glostr。
        buff = bytearray()
        for i in range(4):
            buff.extend(words_le[i * 4:(i + 1) * 4][::-1])
        return freq_id, bytes(buff[:10])

    def handle_sfrbx_payload(self, payload: bytes) -> Optional[GLONASSEphemeris]:
        if len(payload) < 8 or payload[0] != 6:
            return None
        self.total_sfrbx += 1
        if not self.enabled:
            return None
        sv_id = payload[1]
        converted = self._sfrbx_to_glostr_10bytes(payload)
        if converted is None:
            return None
        freq_id, glostr = converted
        # GLONASS strings 1..4 才组成即时星历。string number 在 bit1..bit4。
        m = getbitu_bytes(glostr, 1, 4)
        if m not in (1, 2, 3, 4):
            self.unsupported += 1
            return None
        self.strings.setdefault(sv_id, {})[m] = glostr
        self.freq_id[sv_id] = freq_id
        if not all(k in self.strings[sv_id] for k in (1, 2, 3, 4)):
            return None

        try:
            ArrU = getattr(pyrtklib, "Arr1Dunsigned char")
            ArrD = getattr(pyrtklib, "Arr1Ddouble")
            raw = ArrU(40)
            # decode_glostr 期望 4 个字符串连续排列，每个 10 字节。
            frame = b"".join(self.strings[sv_id][k] for k in (1, 2, 3, 4))
            for i, b in enumerate(frame):
                raw[i] = b
            utc = ArrD(1)
            utc[0] = 0.0
            geph = pyrtklib.geph_t()
            ok = pyrtklib.decode_glostr(raw, geph, utc)
        except Exception as exc:
            self.decode_failed += 1
            if self.decode_failed <= 3:
                print(f"[GLO] decode_glostr异常: {exc}")
            return None

        if not ok:
            self.decode_failed += 1
            return None

        # RTKLIB 里 geph.frq 通常为 -7..+6；u-blox MGA 字段 M 为 0..13，优先用 SFRBX freqId。
        freq_m = int(self.freq_id.get(sv_id, int(getattr(geph, 'frq', 0)) + 7)) & 0xFF
        pos = [float(geph.pos[i]) for i in range(3)]  # m
        vel = [float(geph.vel[i]) for i in range(3)]  # m/s
        acc = [float(geph.acc[i]) for i in range(3)]  # m/s^2

        # UBX-MGA-GLO-EPH 使用 km / km/s / km/s^2 再按字段比例缩放。
        x = _scale_to_int(pos[0] / 1000.0, 2**-11, 32)
        y = _scale_to_int(pos[1] / 1000.0, 2**-11, 32)
        z = _scale_to_int(pos[2] / 1000.0, 2**-11, 32)
        dx = _scale_to_int(vel[0] / 1000.0, 2**-20, 32)
        dy = _scale_to_int(vel[1] / 1000.0, 2**-20, 32)
        dz = _scale_to_int(vel[2] / 1000.0, 2**-20, 32)
        ddx = _scale_to_int(acc[0] / 1000.0, 2**-30, 8)
        ddy = _scale_to_int(acc[1] / 1000.0, 2**-30, 8)
        ddz = _scale_to_int(acc[2] / 1000.0, 2**-30, 8)
        gamma = _scale_to_int(float(geph.gamn), 2**-40, 16)
        delta_tau = _scale_to_int(float(getattr(geph, 'dtaun', 0.0)), 2**-30, 8)
        # u-blox MGA-GLO-EPH 的 tau 字段按 pyubx2/实测习惯使用 -tauN。
        tau = _scale_to_int(-float(geph.taun), 2**-30, 32)

        # tb 为 15 分钟单位；RTKLIB iode 对 GLONASS 通常为 tb/900。
        tb = int(getattr(geph, 'iode', 0)) & 0xFF
        eph = GLONASSEphemeris(
            sv_id=sv_id,
            freq_id=freq_m,
            FT=int(getattr(geph, 'sva', 0)) & 0xFF,
            B=int(getattr(geph, 'age', 0)) & 0xFF,
            M=freq_m,
            H=int(getattr(geph, 'svh', 0)) & 0x7F,
            x=x, y=y, z=z,
            dx=dx, dy=dy, dz=dz,
            ddx=ddx, ddy=ddy, ddz=ddz,
            tb=tb,
            gamma=gamma,
            E=0,
            deltaTau=delta_tau,
            tau=tau,
            iode=tb,
            created_at=time.time(),
        )
        old = self.last_ephemeris.get(sv_id)
        if old is None or old.iode != eph.iode:
            self.last_ephemeris[sv_id] = eph
            self.total_eph += 1
            return eph
        return None


# ============================================================
# 5. 多星座统一收集器：GPS + QZSS + GAL + BDS + GLO
# ============================================================

@dataclass
class AssistRecord:
    key: str
    label: str
    iode: int
    ubx: bytes
    created_at: float
    kind: str = "eph"
    constellation: str = ""
    sv_id: Optional[int] = None
    source: str = "local_sfrbx"
    toe_s: Optional[float] = None
    toc_s: Optional[float] = None
    semantic_toe_epoch: Optional[float] = None
    validity_basis: str = "reception_age"


GNSS_WEEK_S = 7 * 24 * 60 * 60
GPS_EPOCH_UNIX_S = 315964800.0


def nearest_week_toe_epoch(reference_epoch: float, toe_s: float) -> float:
    """Resolve seconds-of-week to the week nearest the reception timestamp."""
    week_index = int((reference_epoch - GPS_EPOCH_UNIX_S) // GNSS_WEEK_S)
    week_start = GPS_EPOCH_UNIX_S + week_index * GNSS_WEEK_S
    candidates = [
        week_start + toe_s - GNSS_WEEK_S,
        week_start + toe_s,
        week_start + toe_s + GNSS_WEEK_S,
    ]
    return min(candidates, key=lambda value: abs(value - reference_epoch))


def build_ephemeris_record(
    key: str,
    label: str,
    iode: int,
    ubx: bytes,
    created_at: float,
    toe_raw: int,
    toe_scale_s: float,
    toc_raw: Optional[int] = None,
    source: str = "local_sfrbx",
) -> AssistRecord:
    constellation = key.split("-", 1)[0]
    try:
        sv_id = int(key.split("-", 1)[1])
    except (IndexError, ValueError):
        sv_id = None
    toe_s = float(toe_raw) * float(toe_scale_s)
    toc_s = None if toc_raw is None else float(toc_raw) * float(toe_scale_s)
    return AssistRecord(
        key=key,
        label=label,
        iode=iode,
        ubx=ubx,
        created_at=created_at,
        kind="eph",
        constellation=constellation,
        sv_id=sv_id,
        source=source,
        toe_s=toe_s,
        toc_s=toc_s,
        semantic_toe_epoch=nearest_week_toe_epoch(created_at, toe_s),
        validity_basis="toe_window+reception_age",
    )


def build_mga_gps_alm(
    sv_id: int,
    sv_health: int,
    e: int,
    alm_wna: int,
    toa: int,
    delta_i: int,
    omega_dot: int,
    sqrt_a: int,
    omega0: int,
    omega: int,
    m0: int,
    af0: int,
    af1: int,
) -> bytes:
    """UBX-MGA-GPS-ALM, Class=0x13, ID=0x00, payload=36 bytes."""
    payload = struct.pack(
        "<BBBBHBBhhIiiihhI",
        0x02,
        0x00,
        sv_id & 0xFF,
        sv_health & 0xFF,
        e & 0xFFFF,
        alm_wna & 0xFF,
        toa & 0xFF,
        delta_i,
        omega_dot,
        sqrt_a & 0xFFFFFFFF,
        omega0,
        omega,
        m0,
        af0,
        af1,
        0,
    )
    if len(payload) != 36:
        raise RuntimeError(f"MGA-GPS-ALM payload 长度错误: {len(payload)}")
    return build_ubx(0x13, 0x00, payload)


def build_mga_gps_health(health_codes: List[int]) -> bytes:
    """UBX-MGA-GPS-HEALTH, Class=0x13, ID=0x00, payload=40 bytes."""
    if len(health_codes) != 32:
        raise ValueError("GPS HEALTH 必须包含32颗卫星的健康码")
    payload = struct.pack(
        "<BBH" + "B" * 32 + "I",
        0x04,
        0x00,
        0,
        *[value & 0x3F for value in health_codes],
        0,
    )
    if len(payload) != 40:
        raise RuntimeError(f"MGA-GPS-HEALTH payload 长度错误: {len(payload)}")
    return build_ubx(0x13, 0x00, payload)


def build_mga_gps_iono(params: List[int]) -> bytes:
    """UBX-MGA-GPS-IONO, Class=0x13, ID=0x00, payload=16 bytes."""
    if len(params) != 8:
        raise ValueError("GPS IONO 必须包含8个Klobuchar参数")
    payload = struct.pack("<BBH8bI", 0x06, 0x00, 0, *params, 0)
    if len(payload) != 16:
        raise RuntimeError(f"MGA-GPS-IONO payload 长度错误: {len(payload)}")
    return build_ubx(0x13, 0x00, payload)


def build_mga_gps_utc(
    utc_a0: int,
    utc_a1: int,
    utc_dt_ls: int,
    utc_tot: int,
    utc_wnt: int,
    utc_wn_lsf: int,
    utc_dn: int,
    utc_dt_lsf: int,
) -> bytes:
    """UBX-MGA-GPS-UTC, Class=0x13, ID=0x00, payload=20 bytes."""
    payload = struct.pack(
        "<BBHiibBBBBbH",
        0x05,
        0x00,
        0,
        utc_a0,
        utc_a1,
        utc_dt_ls,
        utc_tot & 0xFF,
        utc_wnt & 0xFF,
        utc_wn_lsf & 0xFF,
        utc_dn & 0xFF,
        utc_dt_lsf,
        0,
    )
    if len(payload) != 20:
        raise RuntimeError(f"MGA-GPS-UTC payload 长度错误: {len(payload)}")
    return build_ubx(0x13, 0x00, payload)


class GPSLNAVAuxCollector:
    """从 GPS L1 C/A LNAV 子帧4/5生成 Almanac、Iono、UTC 和健康状态。"""

    def __init__(self):
        self.almanac_pages: Dict[int, dict] = {}
        self.alm_wna: Optional[int] = None
        self.alm_toa: Optional[int] = None
        self.health_codes: Dict[int, int] = {}
        self.last_records: Dict[str, AssistRecord] = {}
        self.total_alm = 0
        self.total_iono = 0
        self.total_utc = 0
        self.total_health = 0
        self.page18_count = 0
        self.page25_sf4_count = 0
        self.page25_sf5_count = 0

    @staticmethod
    def _frame_bytes(data24: List[int]) -> bytes:
        return b"".join((word & 0xFFFFFF).to_bytes(3, "big") for word in data24)

    def _new_record(self, key: str, label: str, kind: str, ubx: bytes) -> Optional[AssistRecord]:
        fingerprint = zlib.crc32(ubx) & 0xFFFFFFFF
        old = self.last_records.get(key)
        if old is not None and old.iode == fingerprint:
            return None
        record = AssistRecord(
            key=key,
            label=label,
            iode=fingerprint,
            ubx=ubx,
            created_at=time.time(),
            kind=kind,
            constellation="GPS",
            sv_id=(
                int(key.rsplit("-", 1)[1])
                if kind == "alm" and key.rsplit("-", 1)[1].isdigit()
                else None
            ),
            source="local_sfrbx:gps_lnav_aux",
        )
        self.last_records[key] = record
        return record

    def _build_almanac_record(self, sv_id: int) -> Optional[AssistRecord]:
        page = self.almanac_pages.get(sv_id)
        if page is None or self.alm_wna is None:
            return None
        if self.alm_toa is not None and page["toa"] != self.alm_toa:
            return None
        ubx = build_mga_gps_alm(
            sv_id=sv_id,
            sv_health=page["sv_health"],
            e=page["e"],
            alm_wna=self.alm_wna,
            toa=page["toa"],
            delta_i=page["delta_i"],
            omega_dot=page["omega_dot"],
            sqrt_a=page["sqrt_a"],
            omega0=page["omega0"],
            omega=page["omega"],
            m0=page["m0"],
            af0=page["af0"],
            af1=page["af1"],
        )
        return self._new_record(
            key=f"GPS-ALM-{sv_id:02d}",
            label=f"GPS ALM PRN={sv_id:02d}",
            kind="alm",
            ubx=ubx,
        )

    def _build_health_record(self) -> Optional[AssistRecord]:
        if not all(sv_id in self.health_codes for sv_id in range(1, 33)):
            return None
        health = [self.health_codes[sv_id] for sv_id in range(1, 33)]
        return self._new_record(
            key="GPS-HEALTH",
            label="GPS HEALTH 32SV",
            kind="health",
            ubx=build_mga_gps_health(health),
        )

    def handle_sfrbx_payload(self, payload: bytes) -> List[AssistRecord]:
        records: List[AssistRecord] = []
        if len(payload) < 48:
            return records
        gnss_id, sig_id, num_words = payload[0], payload[2], payload[4]
        if gnss_id != 0 or sig_id != 0 or num_words != 10:
            return records

        words = list(struct.unpack_from("<10I", payload, 8))
        data24 = [(word >> 6) & 0xFFFFFF for word in words]
        if get_word_bits(data24[0], 1, 8) != 0x8B:
            return records
        subframe_id = get_word_bits(data24[1], 20, 3)
        if subframe_id not in (4, 5):
            return records
        data_id = get_word_bits(data24[2], 1, 2)
        page_id = get_word_bits(data24[2], 3, 6)
        if data_id != 1:
            return records

        is_almanac = (
            (subframe_id == 4 and 25 <= page_id <= 32)
            or (subframe_id == 5 and 1 <= page_id <= 24)
        )
        if is_almanac:
            af0_msb = get_word_bits(data24[9], 1, 8)
            af0_lsb = get_word_bits(data24[9], 20, 3)
            self.almanac_pages[page_id] = {
                "e": get_word_bits(data24[2], 9, 16),
                "toa": get_word_bits(data24[3], 1, 8),
                "delta_i": twos(get_word_bits(data24[3], 9, 16), 16),
                "omega_dot": twos(get_word_bits(data24[4], 1, 16), 16),
                "sv_health": get_word_bits(data24[4], 17, 8),
                "sqrt_a": get_word_bits(data24[5], 1, 24),
                "omega0": twos(get_word_bits(data24[6], 1, 24), 24),
                "omega": twos(get_word_bits(data24[7], 1, 24), 24),
                "m0": twos(get_word_bits(data24[8], 1, 24), 24),
                "af0": twos((af0_msb << 3) | af0_lsb, 11),
                "af1": twos(get_word_bits(data24[9], 9, 11), 11),
            }
            record = self._build_almanac_record(page_id)
            if record is not None:
                self.total_alm += 1
                records.append(record)

        frame = self._frame_bytes(data24)
        if subframe_id == 4 and page_id == 56:
            self.page18_count += 1
            iono = [getbits_bytes(frame, 56 + 8 * index, 8) for index in range(8)]
            iono_record = self._new_record(
                key="GPS-IONO",
                label="GPS IONO Klobuchar",
                kind="iono",
                ubx=build_mga_gps_iono(iono),
            )
            if iono_record is not None:
                self.total_iono += 1
                records.append(iono_record)

            utc_record = self._new_record(
                key="GPS-UTC",
                label="GPS UTC parameters",
                kind="utc",
                ubx=build_mga_gps_utc(
                    utc_a0=getbits_bytes(frame, 144, 32),
                    utc_a1=getbits_bytes(frame, 120, 24),
                    utc_dt_ls=getbits_bytes(frame, 192, 8),
                    utc_tot=getbitu_bytes(frame, 176, 8),
                    utc_wnt=getbitu_bytes(frame, 184, 8),
                    utc_wn_lsf=getbitu_bytes(frame, 200, 8),
                    utc_dn=getbitu_bytes(frame, 208, 8),
                    utc_dt_lsf=getbits_bytes(frame, 216, 8),
                ),
            )
            if utc_record is not None:
                self.total_utc += 1
                records.append(utc_record)

        elif subframe_id == 5 and page_id == 51:
            self.page25_sf5_count += 1
            self.alm_toa = get_word_bits(data24[2], 9, 8)
            self.alm_wna = get_word_bits(data24[2], 17, 8)
            bit_pos = 72
            for sv_id in range(1, 25):
                self.health_codes[sv_id] = getbitu_bytes(frame, bit_pos, 6)
                bit_pos += 6
            for sv_id in sorted(self.almanac_pages):
                record = self._build_almanac_record(sv_id)
                if record is not None:
                    self.total_alm += 1
                    records.append(record)
            health_record = self._build_health_record()
            if health_record is not None:
                self.total_health += 1
                records.append(health_record)

        elif subframe_id == 4 and page_id == 63:
            self.page25_sf4_count += 1
            bit_pos = 186
            for sv_id in range(25, 33):
                self.health_codes[sv_id] = getbitu_bytes(frame, bit_pos, 6)
                bit_pos += 6
            health_record = self._build_health_record()
            if health_record is not None:
                self.total_health += 1
                records.append(health_record)

        return records


@dataclass
class DroneLink:
    """一条独立飞机数据链。一个 link 对应一个固定机位/IP。"""

    name: str
    vehicle_id: int
    endpoint: str
    bind_address: str = ""
    system_id: Optional[int] = None
    enabled: bool = True
    baud: int = 0
    target_gps_model: str = "UNSPECIFIED"
    can_raw_ubx_validated: Optional[bool] = None
    master: object = None
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    rx_thread: Optional[threading.Thread] = None
    rx_running: bool = False
    rx_error: Optional[Exception] = None
    connected: bool = False
    last_open_time: Optional[float] = None
    last_message_time: Optional[float] = None
    last_heartbeat_time: Optional[float] = None
    last_error: Optional[str] = None
    heartbeat_sent_count: int = 0
    inject_packet_count: int = 0
    inject_byte_count: int = 0
    inject_error_count: int = 0

    @property
    def key(self) -> str:
        return str(self.name or f"vehicle{self.vehicle_id}")


@dataclass
class VehicleState:
    """同一 MAVLink 总线上一架飞行器的独立在线与定位状态。"""

    system_id: int
    component_id: int
    mav_type: int
    autopilot: int
    session_id: int
    online: bool
    online_since: float
    last_heartbeat: float
    last_message: float
    heartbeat_seen: bool = False
    first_heartbeat_time: Optional[float] = None
    estimated_boot_time: Optional[float] = None
    last_time_boot_ms: Optional[int] = None
    first_can_ready_time: Optional[float] = None
    first_inject_time: Optional[float] = None
    fix_type: int = 0
    satellites: int = 0
    gps_raw_count: int = 0
    eph_cm: Optional[int] = None
    epv_cm: Optional[int] = None
    gps_lat_raw: Optional[int] = None
    gps_lon_raw: Optional[int] = None
    gps_alt_mm: Optional[int] = None
    gps_vel_cms: Optional[int] = None
    gps_cog_cdeg: Optional[int] = None
    gps_h_acc_mm: Optional[int] = None
    gps_v_acc_mm: Optional[int] = None
    gps_yaw_cdeg: Optional[int] = None
    first_gps_time: Optional[float] = None
    first_raw_fix_time: Optional[float] = None
    raw_fix_sats: Optional[int] = None
    first_fix_time: Optional[float] = None
    quality_fix_sats: Optional[int] = None
    armed: bool = False
    first_armed_time: Optional[float] = None
    first_armed_source: Optional[str] = None
    last_armed_time: Optional[float] = None
    last_disarmed_time: Optional[float] = None
    actual_arm_csv_written: bool = False
    first_armable_time: Optional[float] = None
    last_fix_update: Optional[float] = None
    last_gps_quality_update: Optional[float] = None
    last_preflight_block_time: Optional[float] = None
    last_preflight_block_text: Optional[str] = None
    active_preflight_blocks: Dict[str, float] = field(default_factory=dict)
    armable_csv_written: bool = False
    last_statustext_time: Optional[float] = None
    last_statustext_text: Optional[str] = None
    estimator_status_count: int = 0
    estimator_flags: Optional[int] = None
    estimator_pos_horiz_ratio: Optional[float] = None
    estimator_pos_vert_ratio: Optional[float] = None
    estimator_pos_horiz_accuracy_m: Optional[float] = None
    estimator_pos_vert_accuracy_m: Optional[float] = None
    last_estimator_status_time: Optional[float] = None
    local_position_count: int = 0
    last_local_position_time: Optional[float] = None
    global_position_count: int = 0
    first_global_position_time: Optional[float] = None
    last_global_position_time: Optional[float] = None
    sys_status_count: int = 0
    last_sys_status_time: Optional[float] = None
    sys_status_present_flags: Optional[int] = None
    sys_status_enabled_flags: Optional[int] = None
    sys_status_health_flags: Optional[int] = None
    sys_status_prearm_present: Optional[bool] = None
    sys_status_prearm_enabled: Optional[bool] = None
    sys_status_prearm_healthy: Optional[bool] = None
    first_prearm_healthy_time: Optional[float] = None
    burst_rounds_remaining: int = 0
    next_burst_time: float = 0.0
    last_inject_time: float = 0.0
    almanac_sent_session_id: Optional[int] = None
    csv_written: bool = False
    warning_alert_sent: bool = False
    critical_alert_sent: bool = False
    recovery_alert_sent: bool = False
    alert_level: str = "NONE"
    first_alert_time: Optional[float] = None
    last_alert_time: Optional[float] = None
    last_alert_event_id: Optional[str] = None
    alert_count: int = 0
    link_name: Optional[str] = None
    configured_vehicle_id: Optional[int] = None
    configured_system_id: Optional[int] = None

    @property
    def key(self) -> str:
        base = f"{self.system_id}:{self.component_id}"
        return f"{self.link_name}/{base}" if self.link_name else base

    def reset_for_new_session(
        self,
        now: float,
        session_id: int,
        burst_rounds: int,
        settle_s: float,
    ):
        self.session_id = int(session_id)
        self.online = True
        self.online_since = now
        self.last_heartbeat = now
        self.last_message = now
        self.heartbeat_seen = False
        self.first_heartbeat_time = None
        self.estimated_boot_time = None
        self.last_time_boot_ms = None
        self.first_can_ready_time = None
        self.first_inject_time = None
        self.fix_type = 0
        self.satellites = 0
        self.gps_raw_count = 0
        self.eph_cm = None
        self.epv_cm = None
        self.gps_lat_raw = None
        self.gps_lon_raw = None
        self.gps_alt_mm = None
        self.gps_vel_cms = None
        self.gps_cog_cdeg = None
        self.gps_h_acc_mm = None
        self.gps_v_acc_mm = None
        self.gps_yaw_cdeg = None
        self.first_gps_time = None
        self.first_raw_fix_time = None
        self.raw_fix_sats = None
        self.first_fix_time = None
        self.quality_fix_sats = None
        self.armed = False
        self.first_armed_time = None
        self.first_armed_source = None
        self.last_armed_time = None
        self.last_disarmed_time = None
        self.actual_arm_csv_written = False
        self.first_armable_time = None
        self.last_fix_update = None
        self.last_gps_quality_update = None
        self.last_preflight_block_time = None
        self.last_preflight_block_text = None
        self.active_preflight_blocks = {}
        self.armable_csv_written = False
        self.last_statustext_time = None
        self.last_statustext_text = None
        self.estimator_status_count = 0
        self.estimator_flags = None
        self.estimator_pos_horiz_ratio = None
        self.estimator_pos_vert_ratio = None
        self.estimator_pos_horiz_accuracy_m = None
        self.estimator_pos_vert_accuracy_m = None
        self.last_estimator_status_time = None
        self.local_position_count = 0
        self.last_local_position_time = None
        self.global_position_count = 0
        self.first_global_position_time = None
        self.last_global_position_time = None
        self.sys_status_count = 0
        self.last_sys_status_time = None
        self.sys_status_present_flags = None
        self.sys_status_enabled_flags = None
        self.sys_status_health_flags = None
        self.sys_status_prearm_present = None
        self.sys_status_prearm_enabled = None
        self.sys_status_prearm_healthy = None
        self.first_prearm_healthy_time = None
        self.burst_rounds_remaining = max(0, int(burst_rounds))
        self.next_burst_time = now + max(0.0, float(settle_s))
        self.last_inject_time = 0.0
        self.almanac_sent_session_id = None
        self.csv_written = False
        self.warning_alert_sent = False
        self.critical_alert_sent = False
        self.recovery_alert_sent = False
        self.alert_level = "NONE"
        self.first_alert_time = None
        self.last_alert_time = None
        self.last_alert_event_id = None
        self.alert_count = 0


class MultiGNSSCollector:
    def __init__(
        self,
        enable_glo_mga_experimental: bool = False,
        enable_gps_lnav_aux: bool = True,
    ):
        self.gps = LNAVCollector(
            system_name="GPS",
            gnss_id=0,
            sig_id=0,
            sv_min=1,
            sv_max=32
        )

        self.qzss = LNAVCollector(
            system_name="QZSS",
            gnss_id=5,
            sig_id=0,
            sv_min=1,
            sv_max=10
        )

        self.gal = GalileoINavCollector()
        self.bds = BDSB1ICollector()
        self.enable_gps_lnav_aux = enable_gps_lnav_aux
        self.gps_aux = GPSLNAVAuxCollector()
        # GLONASS 默认只统计，不参与解析/封装。
        # 原因：当前实验中 GLO/sig0/w4 很多，但实验解析器可能导致进程无声退出。
        # 后续若单独验证稳定，可把 enable_glo_mga_experimental 改为 True。
        self.enable_glo_mga_experimental = enable_glo_mga_experimental
        self.glo = GLONASSCollector()

        self.raw_sfrbx = 0
        self.sfrbx_stat: Dict[Tuple[int, int, int], int] = {}

        self.bds_seen = 0
        self.gal_seen = 0
        self.glo_seen = 0
        self.qzss_l1s_seen = 0
        self.qzss_unsupported = 0

    def handle_sfrbx_payload(self, payload: bytes) -> List[AssistRecord]:
        records: List[AssistRecord] = []

        if len(payload) < 8:
            return records

        self.raw_sfrbx += 1

        gnss_id = payload[0]
        sv_id = payload[1]
        sig_id = payload[2]
        num_words = payload[4]

        stat_key = (gnss_id, sig_id, num_words)
        self.sfrbx_stat[stat_key] = self.sfrbx_stat.get(stat_key, 0) + 1

        if gnss_id == 3:
            self.bds_seen += 1

        if gnss_id == 2:
            self.gal_seen += 1

        if gnss_id == 6:
            self.glo_seen += 1

        if gnss_id == 5 and sig_id == 1:
            self.qzss_l1s_seen += 1

        if gnss_id == 5 and sig_id != 0:
            self.qzss_unsupported += 1

        if self.enable_gps_lnav_aux:
            records.extend(self.gps_aux.handle_sfrbx_payload(payload))

        # -------------------------
        # GPS L1C/A
        # -------------------------
        gps_eph = self.gps.handle_sfrbx_payload(payload)

        if gps_eph:
            records.append(
                build_ephemeris_record(
                    key=f"GPS-{gps_eph.sv_id:02d}",
                    label=f"GPS PRN={gps_eph.sv_id:02d}",
                    iode=gps_eph.iode,
                    ubx=build_mga_gps_eph(gps_eph),
                    created_at=gps_eph.created_at,
                    toe_raw=gps_eph.toe,
                    toc_raw=gps_eph.toc,
                    toe_scale_s=16.0,
                )
            )

        # -------------------------
        # QZSS L1C/A
        # -------------------------
        qzss_eph = self.qzss.handle_sfrbx_payload(payload)

        if qzss_eph:
            records.append(
                build_ephemeris_record(
                    key=f"QZSS-{qzss_eph.sv_id:02d}",
                    label=f"QZSS svId={qzss_eph.sv_id:02d}",
                    iode=qzss_eph.iode,
                    ubx=build_mga_qzss_eph(qzss_eph),
                    created_at=qzss_eph.created_at,
                    toe_raw=qzss_eph.toe,
                    toc_raw=qzss_eph.toc,
                    toe_scale_s=16.0,
                )
            )

        # -------------------------
        # Galileo I/NAV（实验版）
        # -------------------------
        gal_eph = self.gal.handle_sfrbx_payload(payload)

        if gal_eph:
            records.append(
                build_ephemeris_record(
                    key=f"GAL-{gal_eph.sv_id:02d}",
                    label=f"GAL svId={gal_eph.sv_id:02d}",
                    iode=gal_eph.iodNav,
                    ubx=build_mga_gal_eph(gal_eph),
                    created_at=gal_eph.created_at,
                    toe_raw=gal_eph.toe,
                    toc_raw=gal_eph.toc,
                    toe_scale_s=60.0,
                )
            )

        # -------------------------
        # BeiDou B1I D1/D2（实验版；B1C/B2a暂不注入）
        # -------------------------
        bds_eph = self.bds.handle_sfrbx_payload(payload)

        if bds_eph:
            records.append(
                build_ephemeris_record(
                    key=f"BDS-{bds_eph.sv_id:02d}",
                    label=f"BDS svId={bds_eph.sv_id:02d} {bds_eph.source}",
                    iode=bds_eph.IODE,
                    ubx=build_mga_bds_eph(bds_eph),
                    created_at=bds_eph.created_at,
                    toe_raw=bds_eph.toe,
                    toc_raw=bds_eph.toc,
                    toe_scale_s=8.0,
                    source=f"local_sfrbx:{bds_eph.source}",
                )
            )

        # -------------------------
        # GLONASS L1OF
        # -------------------------
        # 当前稳定版默认“只统计 GLO_SEEN / GLO/sig0/w4”，不尝试解析和封装 MGA-GLO-EPH。
        # 原因：你的日志中 GLO 很多，但 GLO 实验解析尚不稳定，曾出现无 traceback 直接退回 PowerShell。
        # 这样不会影响 GPS/GAL/BDS/QZSS 主流程，也不会让 GLONASS 解析失败拖垮脚本。
        if self.enable_glo_mga_experimental:
            try:
                glo_eph = self.glo.handle_sfrbx_payload(payload)
            except BaseException as e:
                # BaseException 用于兜住少数第三方库异常；真正的段错误仍会由 faulthandler 记录。
                self.glo.decode_failed += 1
                print(f"\n[GLO] 实验解析异常，已跳过本条 GLO SFRBX: {repr(e)}")
                glo_eph = None

            if glo_eph:
                records.append(
                    AssistRecord(
                        key=f"GLO-{glo_eph.sv_id:02d}",
                        label=f"GLO svId={glo_eph.sv_id:02d} freqId={glo_eph.freq_id}",
                        iode=glo_eph.iode,
                        ubx=build_mga_glo_eph(glo_eph),
                        created_at=glo_eph.created_at,
                        constellation="GLO",
                        sv_id=glo_eph.sv_id,
                        source="local_sfrbx:experimental_glo",
                    )
                )

        return records

    def sfrbx_stat_string(self):
        if not self.sfrbx_stat:
            return "无"

        name_map = {
            0: "GPS",
            1: "SBAS",
            2: "GAL",
            3: "BDS",
            5: "QZSS",
            6: "GLO",
            7: "NavIC",
        }

        parts = []

        for (gnss_id, sig_id, num_words), count in sorted(self.sfrbx_stat.items()):
            name = name_map.get(gnss_id, f"GNSS{gnss_id}")
            parts.append(f"{name}/sig{sig_id}/w{num_words}:{count}")

        return " | ".join(parts)


# ============================================================
# 6. MGA-INI 时间/位置辅助
# ============================================================

def build_mga_ini_pos_llh(pvt: dict, pos_acc_cm: int) -> bytes:
    """
    UBX-MGA-INI-POS_LLH
    Class = 0x13
    ID    = 0x40
    Payload length = 20
    """
    pos_acc = max(
        int(pos_acc_cm),
        int(pvt.get("h_acc_cm", pos_acc_cm)),
        int(pvt.get("v_acc_cm", pos_acc_cm)),
    )
    payload = struct.pack(
        "<BBHiiiI",
        0x01,
        0x00,
        0x0000,
        pvt["lat_raw"],
        pvt["lon_raw"],
        pvt["height_cm"],
        pos_acc
    )

    return build_ubx(0x13, 0x40, payload)


def build_mga_ini_time_utc(pvt: dict, pvt_monotonic: float, t_acc_s: int, t_acc_ns: int) -> bytes:
    """
    UBX-MGA-INI-TIME_UTC
    Class = 0x13
    ID    = 0x40
    Payload length = 24
    """
    base_dt = dt.datetime(
        pvt["year"], pvt["month"], pvt["day"],
        pvt["hour"], pvt["minute"], pvt["second"],
        tzinfo=dt.timezone.utc
    )

    elapsed = max(0.0, time.monotonic() - pvt_monotonic)
    inj_dt = base_dt + dt.timedelta(seconds=elapsed)

    payload = struct.pack(
        "<BBBbHBBBBBBIH2sI",
        0x10,
        0x00,
        0x00,
        -128,
        inj_dt.year,
        inj_dt.month,
        inj_dt.day,
        inj_dt.hour,
        inj_dt.minute,
        inj_dt.second,
        0x00,
        inj_dt.microsecond * 1000,
        t_acc_s,
        b"\x00\x00",
        t_acc_ns
    )

    return build_ubx(0x13, 0x40, payload)


# ============================================================
# 7. 主程序
# ============================================================

class LocalAGNSSBridge:
    def __init__(self):
        # ============================================================
        # 工程接口配置
        # ============================================================
        # BASE_PORT：工控机连接常上电基准站 GPS，长期采集 NAV-PVT / RXM-SFRBX。
        self.BASE_PORT = "COM3"
        self.BAUD_BASE = 115200

        # 数据链连接方式：
        #   serial  ：工控机串口/USB 数传，例如 COM4；
        #   udpout  ：工控机通过网口主动发往数据链/飞控，例如 udpout:192.168.1.50:14550；
        #   udpin   ：工控机监听 UDP，例如 udpin:0.0.0.0:14550；
        #   tcp     ：TCP 客户端，例如 tcp:192.168.1.50:5760。
        # 依据你的电气图，机巢内数据链长期由机库供电，因此默认仍可长期打开数据链，
        # 但真正有效注入必须等检测到 PX4 heartbeat，也就是飞控/GPS 已经上电。


        #self.LINK_MODE = "serial"

        #如果改为数据链有线传输，就用下面的这个模式
        self.LINK_MODE = "tcp"
        
        self.DRONE_SERIAL_PORT = "COM4"
        self.DRONE_SERIAL_BAUD = 115200
        #工控机主动把 MAVLink 数据发到 192.168.1.21 的 1024 端口，一般是 数据链网口设备的 IP，或者是 飞控 MAVLink 网络入口的 IP，不是工控机自己的 IP。
        self.DRONE_UDPOUT = "udpout:192.168.1.21:1024"
        self.DRONE_UDPIN = "udpin:0.0.0.0:1024"
        self.DRONE_TCP = "tcpin:0.0.0.0:1024"
        # 四独立数据链模式：每个元素对应一个固定机位/飞机的数据链。
        # endpoint 直接使用 pymavlink 连接串，例如：
        #   tcp:192.168.1.21:1024      工控机主动连数据链 TCP 服务端
        #   udpout:192.168.1.21:1024   工控机主动向数据链 UDP 端口发送
        #   udpin:0.0.0.0:10241        工控机监听本地 UDP 端口
        # system_id 可选；填了以后该链路只接受对应 PX4 SYSID_THISMAV 的心跳。
        self.DRONE_LINKS = []
        self.MULTI_LINK_SEND_MODE = "online_only"

        # 兼容旧变量名：脚本内部日志和旧函数仍可引用。
        self.DRONE_PORT = self.DRONE_SERIAL_PORT
        self.BAUD_DRONE = self.DRONE_SERIAL_BAUD

        # 目标 GPS 接入形态：
        #   uart_via_px4：PX4 将 GPS_RTCM_DATA 中的 UBX-MGA 旁路写入 GPS UART；
        #   can_via_px4 ：仅当 PX4 会发布 RTCMStream，且目标 CAN GPS 已验证原始 UBX 透传时可用。
        self.TARGET_GPS_LINK_TYPE = "uart_via_px4"
        self.TARGET_GPS_MODEL = "UNSPECIFIED"

        # CAN 模式的软件侧安全门槛。收到飞控 heartbeat 后，脚本通过 MAVLink
        # 读取三个 PX4 UAVCAN 参数；参数未读回或不合格时，不发送 UBX-MGA。
        # 这只能确认 PX4 会发布 DroneCAN RTCMStream，不能替代目标
        # CAN GPS 是否接收/透传 UBX-MGA 的物理验证。
        # CAN_TARGET_VALIDATION_CONFIRMED 仅作为旧单链路配置的兼容回退；
        # 多链路优先使用 DRONE_LINKS[].can_raw_ubx_validated。
        self.CAN_PX4_PARAM_CHECK_ENABLED = True
        self.CAN_REQUIRE_PX4_PARAMS_BEFORE_INJECT = True
        self.CAN_TARGET_VALIDATION_CONFIRMED = False
        self.CAN_PARAM_REQUEST_INTERVAL_S = 2.0
        self.CAN_PARAM_CACHE_TTL_S = 3600.0
        self.CAN_PROBE_TIMEOUT_S = 180.0
        self.CAN_PROBE_OBSERVE_S = 5.0

        # 状态文件：机巢/地面站可轮询该 JSON，判断基准站、数据链、PX4 和 GPS 状态。
        self.STATUS_JSON = str(APP_DIR / "agnss_status.json")
        self.CONFIG_JSON = str(APP_DIR / "agnss_config.json")
        self.DATALINK_LOSS_TIMEOUT_S = 10.0

        # ------------------------------------------------------------------
        # 基准站 GNSS 信号配置
        # ------------------------------------------------------------------
        # 目的：尽量让 NEO3 基准站输出 BeiDou B1I D1/D2 导航电文，
        # 从而出现 BDS/sig0/w10 或 BDS/sig1/w10，供 BDS-D1/D2 解析器
        # 封装 UBX-MGA-BDS-EPH。
        #
        # 注意：如果硬件/固件/地区配置不支持 B1I 输出，仍可能只看到
        # BDS/sig6 或 BDS/sig8；此时脚本仍会统计，但不会错误封装注入。
        self.TRY_ENABLE_BASE_BDS_B1I = True
        # 同时尝试启用 QZSS 总开关和 QZSS L1C/A，
        # 目标是让 RXM-SFRBX 中出现 QZSS/sig0/w10，供 QZSS-MGA-EPH 解析。
        self.TRY_ENABLE_BASE_QZSS_L1CA = True
        self.BASE_SIGNAL_CFG_SAVE_TO_BBR = True
        self.BASE_SIGNAL_CFG_SAVE_TO_FLASH = False

        # 辅助精度不要填得过于乐观
        # 位置/时间辅助精度：
        # 若基准站与无人机距离很近（例如20 m内），可将POS_ACC_CM设为2000；
        # 若两者距离不确定，建议改回5000或10000。
        self.POS_ACC_CM = 5000  # 50米：作为稳定基准；若基准站与无人机距离<20m，可改为2000
        self.TIME_ACC_S = 1     # 基准站PVT稳定时可由5s收紧到1s
        self.TIME_ACC_NS = 0

        # 星历新鲜度过滤：过旧星历不再注入，避免目标GPS拒绝或不使用。
        self.MAX_EPH_AGE_S = 7200  # 2小时
        self.EPH_SEMANTIC_HALF_WINDOW_S = 7200.0
        self.EPH_CLEANUP_INTERVAL_S = 60.0

        # 稳定注入白名单。GLO 保持实验关闭；GAL/BDS 只接受当前已实现的数据类型。
        self.ENABLED_ASSIST_CONSTELLATIONS = ["GPS", "QZSS", "GAL", "BDS"]
        self.CONSTELLATION_PRIORITY = {
            "GPS": 40.0,
            "QZSS": 25.0,
            "GAL": 20.0,
            "BDS": 15.0,
            "GLO": 5.0,
        }

        # 基准站健康门限。PVT 超时后不再声明 1 秒精度的 TIME/POS 辅助；
        # SFRBX 超时后仍保留尚未过期的星历，但状态进入降级。
        self.BASE_PVT_MAX_AGE_S = 5.0
        self.BASE_SFRBX_MAX_AGE_S = 30.0

        # GPS LNAV 长期辅助数据库：子帧4/5可提供 Almanac、Iono、UTC 和32星健康状态。
        # Almanac/UTC/Iono由导航电文低速轮播，基准站长期上电后才能逐步收齐。
        self.ENABLE_ALM_IONO_UTC_EXPERIMENTAL = True
        self.MAX_ALM_AGE_S = 604800.0
        self.MAX_AUX_AGE_S = 604800.0
        self.INJECT_AUX_IN_FAST_ROUNDS = True
        self.INJECT_ALMANAC_ON_FIRST_FIX = True
        self.FORCE_SESSION_ALMANAC = True
        self.ALMANAC_REINJECT_INTERVAL_S = 300.0
        self.MAX_ALMANAC_PER_ROUND = 32
        self.SUPPRESS_BACKGROUND_DURING_ACTIVE_ASSIST = True

        # 星历等待策略：
        # DESIRED_EPH_BEFORE_POWER_ON：理想门槛，尽量等到 12 条有效可注入星历。
        # MIN_EPH_BEFORE_POWER_ON：最低门槛，长时间达不到 12 条时，至少达到 6 条才允许进入注入。
        # MAX_WAIT_AFTER_MIN_EPH：达到最低门槛后继续等待理想门槛的最长时间。
        # 若一直达不到最低 6 条，则只提示“基准站星历不足”，不进入注入阶段。
        self.MIN_EPH_BEFORE_POWER_ON = 6
        self.DESIRED_EPH_BEFORE_POWER_ON = 12
        self.MAX_WAIT_AFTER_MIN_EPH = 120
        self.BASE_EPH_INSUFFICIENT_WARN_S = 180

        # BaseUsedInjectable 仍用于判断这些星历是否被基准站 used，并用于注入排序和日志，
        # 但不再作为硬门槛。因为当前 used 卫星中可能存在 GLONASS，而稳定版暂不封装 GLONASS。
        self.MIN_USED_EPH_BEFORE_POWER_ON = 0
        self.DESIRED_USED_EPH_BEFORE_POWER_ON = 12

        # 目标GPS ACK/NAV-SAT监听：需要第三个 USB-TTL 只接目标GPS TX和GND。
        self.ACK_MONITOR_ENABLED = False
        self.ACK_PORT = "COM10"
        self.ACK_BAUD = 115200
        self.ACK_FLOW_CONTROL_MODE = "observe"
        self.ACK_TARGET_LINK_NAME = ""
        self.ACK_WAIT_TIMEOUT_S = 0.4
        self.ACK_MAX_RETRIES = 1
        self.ACK_TRANSIENT_BACKOFF_S = 0.10
        self.ACK_TIMEOUT_CIRCUIT_BREAKER = 3
        self.ACK_SUSPEND_S = 30.0
        self.ADAPTIVE_INJECT_DELAY_MIN_S = 0.01
        self.ADAPTIVE_INJECT_DELAY_MAX_S = 0.10
        self.ack_monitor = None

        # 目标GPS实际使用卫星门限：用于确认3D Fix后目标GPS确实used了多少颗星。
        # 注意：这个门限只能在目标GPS上电并输出UBX-NAV-SAT后判断，不能作为上电前条件。
        self.TARGET_USED_SATS_THRESHOLD = 12
        self.target_used_threshold_reported = False
        self.target_cfg_sent = False

        # 工程状态机：用于日志和 status.json。
        self.engineering_state = "INIT"
        self.last_effective_inject_round_time = None
        self.status_write_lock = threading.Lock()

        # 脚本启动时间：用于基准站等待阶段的长时间回退，避免因为某个星座暂时无法解析而一直卡住。
        self.script_start_time = time.time()

        # UART 路径需确认 PX4 gps.cpp 允许 UBX-CFG-VALSET 旁路；CAN 路径需确认
        # 目标节点会透传该配置且存在可用回传通道。未完成验证前保持 False。
        self.ENABLE_TARGET_CFG_VIA_PX4 = False

        # 未定位前，每2秒重复注入
        self.INJECT_INTERVAL_BEFORE_FIX = 2.0

        # 默认关闭盲发：不提前打开数传端口，不在PX4 heartbeat前发送数据。
        # 这样可避免同一脚本提前占用串口后，在mavlink_connect()中再次打开导致Permission denied。
        self.BLIND_INJECT_BEFORE_HEARTBEAT = False
        self.BLIND_INJECT_INTERVAL = 1.0
        self.BLIND_INJECT_DURATION = 15.0
        self.BLIND_INJECT_VERBOSE = True
        self.blind_inject_stop = False
        self.first_blind_inject_time = None

        # PX4 heartbeat建立后，立即执行可靠的快速burst有效注入。
        self.EFFECTIVE_BURST_AFTER_HEARTBEAT = True
        self.BURST_ROUNDS = 3
        self.BURST_INTERVAL = 0.3
        self.first_effective_inject_time = None

        # 首次定位优化：heartbeat后burst和3D Fix前重复注入最多发送 Top N 条星历。
        # 注意：DESIRED_EPH_BEFORE_POWER_ON=12 只是“开始注入前的理想等待门槛”，
        # 不再限制每轮实际注入数量。若基准站后续缓存到 20 条有效星历，则每轮最多注入 20 条。
        self.FIRST_FIX_TOP_N = 20
        self.MAX_INJECT_EPH_PER_ROUND = 20

        # 连续数据链测试模式：
        # 基准站准备完成后，脚本不等待手动上电计时，直接打开数传串口；
        # 持续发送 GCS heartbeat 和 A-GNSS 数据；
        # 第一次检测到 PX4 heartbeat 时，把该时刻作为 T0，记录“检测到飞控上电 → 3D Fix”。
        self.CONTINUOUS_DATALINK_TEST = True

        # 机巢工程模式：飞控/GPS 未上电时，数据链虽然有电，但 A-GNSS 不能真正进入 GPS。
        # 因此默认 heartbeat 前只保持链路和心跳，不发送大量 A-GNSS，避免占满数据链缓存。
        # 如需实验“飞控刚上电瞬间链路缓存里已有辅助数据”，可改为 True。
        self.PRE_HEARTBEAT_INJECT_ENABLED = False
        self.PRE_HEARTBEAT_INJECT_INTERVAL = 8.0
        self.PRE_HEARTBEAT_HEARTBEAT_INTERVAL = 0.5
        self.PRE_HEARTBEAT_STATUS_INTERVAL = 3.0
        self.PRE_HEARTBEAT_INJECT_VERBOSE = False

        # PX4 can emit attitude before its 1 Hz heartbeat. A fresh time_boot_ms
        # is enough to create a provisional session and begin the guarded CAN
        # parameter handshake. The first real heartbeat always replenishes the
        # burst, so a buffered early message cannot consume the only injection.
        self.EARLY_BOOT_ASSIST_ENABLED = True
        self.EARLY_BOOT_MAX_TIME_BOOT_S = 15.0

        # 检测到 PX4 heartbeat 后，先请求 GPS_RAW_INT，再等待 GPS 驱动初始化，随后 burst 注入。
        # 机巢上电场景下，0.5~1.5s 通常比 0.15s 更稳。
        self.GPS_RAW_INT_REQUEST_HZ = 5
        self.POST_HEARTBEAT_REQUEST_SETTLE_S = 0.0

        # 每个 MAVLink GPS_RTCM_DATA 包之间的发送间隔。数传不稳定时可改回 0.04。
        self.INJECT_PACKET_DELAY = 0.02

        # 24x7 广播策略：
        # - 无论当前是否已有飞机定位，低频广播始终运行；
        # - 任意新飞机上线时执行快速 burst；
        # - 只要存在未定位在线飞机，就按较高频率重复 Top-N。
        self.BACKGROUND_BROADCAST_ENABLED = True
        self.BACKGROUND_BROADCAST_INTERVAL_S = 30.0
        self.NEW_EPH_BROADCAST_ENABLED = True
        self.NEW_EPH_BROADCAST_MIN_INTERVAL_S = 2.0
        self.ACTIVE_UNFIXED_INJECT_INTERVAL_S = 2.0
        self.VEHICLE_OFFLINE_TIMEOUT_S = 8.0
        self.VEHICLE_RETENTION_S = 86400.0
        self.ACCEPTED_AUTOPILOT_COMPONENT_IDS = [1]
        self.LOG_MAX_MB = 20
        self.LOG_RETENTION_DAYS = 30

        # "3D Fix" is only a middle milestone. PX4 mission arming also needs
        # GPS quality and EKF/heading preflight checks to settle.
        self.CONTINUE_INJECT_AFTER_3D_FIX_S = 30.0
        self.ARMABLE_PREFLIGHT_SILENCE_S = 1.0
        self.ARMABLE_MIN_FIX_TYPE = 3
        self.MIN_QUALITY_FIX_SATS = 10
        self.PREFLIGHT_BLOCK_PATTERNS = [
            "Preflight Fail:",
            "GPS Vertical Pos Drift too high",
            "GPS Horizontal Pos Drift too high",
            "GPS Horizontal Pos Error too high",
            "GPS PDOP too high",
            "not enough GPS Satellites",
            "heading estimate invalid",
            "Strong magnetic interference",
            "No valid position estimate",
            "No valid local position",
            "No valid global position",
            "valid position estimate",
            "valid local position",
            "valid global position",
            "local position invalid",
            "global position invalid",
            "position estimate invalid",
            "Resolve system health failures",
            "Kill switch engaged",
            "Flight termination active",
        ]

        # 定位超时告警与指挥中心反馈。事件先写本地 outbox，再异步 HTTP POST，
        # 网络或指挥中心短时故障不会造成告警丢失。
        self.FIX_WARNING_TIMEOUT_S = 60.0
        self.FIX_CRITICAL_TIMEOUT_S = 120.0
        self.FIX_CRITICAL_REPEAT_INTERVAL_S = 60.0
        self.COMMAND_CENTER_WEBHOOK_URL = ""
        self.COMMAND_CENTER_DEVICE_ID = "AGNSS-DOCK-01"
        self.COMMAND_CENTER_HTTP_TIMEOUT_S = 5.0
        self.COMMAND_CENTER_RETRY_INTERVAL_S = 10.0
        self.COMMAND_CENTER_TOKEN_ENV = "AGNSS_COMMAND_CENTER_TOKEN"

        # 3D Fix 后停止重复注入；若 CONTINUE_INJECT_AFTER_3D_FIX_S > 0，
        # 实际停止点会延后到该窗口结束或预检观察通过。
        self.STOP_INJECT_AFTER_3D_FIX = True

        # Local AssistNow-like database: persist generated UBX-MGA records so a
        # long-running base station can keep an immediately injectable database.
        self.ASSIST_DB_ENABLED = True
        self.ASSIST_DB_PATH = str(APP_DIR / "agnss_assist_db.json")
        self.ASSIST_DB_SAVE_INTERVAL_S = 5.0

        # 未定位前重复发送星历，防止链路丢包
        self.REPEAT_EPH_UNTIL_FIX = True

        # 默认关闭 InjectRaw 原始数据刷屏；需要调试时改成 True
        self.VERBOSE_INJECT_RAW = False
        self.BASE_SAT_LOG_ENABLED = False
        self.BASE_SAT_LOG_INTERVAL_S = 60.0
        self.last_base_sat_log_time = 0.0

        # 当前稳定版：GLONASS 只统计，不解析/封装 MGA-GLO-EPH。
        # 若后续单独验证 GLONASS 解码稳定，再改为 True。
        self.ENABLE_GLO_MGA_EXPERIMENTAL = False

        self.is_running = True
        self.base_buffer = b""

        # collector 必须在读取 agnss_config.json 之后再创建，避免配置文件中
        # ENABLE_GLO_MGA_EXPERIMENTAL 等解析器相关配置已经加载，
        # 但内部 collector 仍保持旧默认值。
        self.collector = None

        self.last_pvt = None
        self.last_pvt_monotonic = None
        self.formatted_pvt = "等待基准站定位..."

        self.master = None
        self.drone_links: List[DroneLink] = []
        self.link_by_name: Dict[str, DroneLink] = {}
        self.system_to_link: Dict[int, str] = {}
        self.configured_system_to_link: Dict[int, str] = {}

        # MAVLink 收发解耦：
        # 发送线程/主循环只负责 heartbeat 和 A-GNSS 注入；
        # 接收线程持续读取 HEARTBEAT / GPS_RAW_INT，避免注入期间漏读或延迟处理 GPS_RAW_INT。
        self.mav_send_lock = threading.Lock()
        self.mav_rx_thread = None
        self.mav_rx_running = False
        self.px4_detected = False
        self.heartbeat_burst_done = False
        self.mav_rx_error = None
        self.mav_last_msg_time = None
        self.gps_raw_count = 0
        self.mav_message_counts: Dict[str, int] = {}
        self.mav_source_stats: Dict[str, Dict[str, object]] = {}
        self.heartbeat_reject_counts: Dict[str, int] = {}
        self.can_px4_params: Dict[str, Dict[str, Dict[str, object]]] = {}
        self.can_verified_param_cache: Dict[str, Dict[str, object]] = {}
        self.can_last_param_request_time: Dict[str, float] = {}
        self.can_param_request_count = 0
        self.can_param_response_count = 0
        self.can_inject_block_count = 0
        self.can_last_block_reason = None
        self.can_last_block_print_time = 0.0
        self.can_probe_mode = False
        self.can_probe_started_at = None
        self.can_probe_sent_at = None
        self.can_probe_payload = None
        self.can_probe_result = None

        # 多机状态以 (system_id, component_id) 为键。GPS_RAW_INT 按 system_id
        # 归属到对应飞控组件，避免不同飞机互相覆盖定位状态。
        self.vehicles: Dict[Tuple[int, int], VehicleState] = {}
        self.vehicle_session_counter = 0

        self.drone_sats = 0
        self.drone_fix_type = 0

        self.assist_to_send: Dict[str, AssistRecord] = {}
        self.aux_assist_to_send: Dict[str, AssistRecord] = {}
        self.assist_sent_iode: Dict[str, int] = {}
        self.assist_update_pending = False
        self.last_assist_update_time = None
        self.last_eph_cleanup_time = 0.0
        self.assist_db_dirty = False
        self.assist_db_loaded_count = 0
        self.assist_db_loaded_fresh_count = 0
        self.assist_db_stale_count = 0
        self.assist_db_last_load_time = None
        self.assist_db_last_save_time = None
        self.assist_db_last_save_count = 0
        self.assist_db_load_error = None
        self.assist_db_save_error = None
        self.last_background_broadcast_time = 0.0
        self.last_new_eph_broadcast_time = 0.0
        self.last_almanac_broadcast_time = 0.0
        self.alert_dispatch_thread = None
        self.alert_dispatch_running = False
        self.alert_wakeup = threading.Event()
        self.alert_file_lock = threading.Lock()
        self.alert_generated_count = 0
        self.alert_delivered_count = 0
        self.alert_delivery_error_count = 0
        self.last_alert_delivery_error = None

        # 基准站NAV-SAT信息，用于按“最可能被目标GPS使用”的卫星排序注入。
        # key: GPS-xx / GAL-xx / QZSS-xx / BDS-xx
        self.base_sat_info: Dict[str, Dict[str, float]] = {}

        # TTFF 多阶段计时
        self.gps_power_on_time = None       # T0：用户按回车并给无人机/GPS上电
        self.px4_connected_time = None      # T1：PX4 heartbeat连接完成
        self.first_gps_msg_time = None      # T2：首次收到GPS_RAW_INT
        self.first_inject_time = None       # T3：首次A-GNSS注入开始
        self.first_3d_fix_time = None       # T4：首次3D Fix
        self.first_gps_msg_fix_type = None
        self.first_gps_msg_sats = None
        self.first_blind_inject_time = None
        self.first_effective_inject_time = None
        self.blind_inject_stop = False
        self.ttff_done = False
        self.ttff_start_reason_text = "用户按回车并给无人机/GPS上电"

        self.last_reported_sats = -1
        self.last_reported_fix = -1

        # 工程统计/健康监控变量。
        self.base_last_rx_time = None
        self.base_ubx_ok_count = 0
        self.base_ubx_crc_fail_count = 0
        self.base_sfrbx_last_time = None
        self.base_pvt_packet_last_time = None
        self.base_pvt_last_time = None

        self.inject_packet_count = 0
        self.inject_byte_count = 0
        self.inject_error_count = 0
        self.inject_round_count = 0
        self.ack_wait_count = 0
        self.ack_accept_count = 0
        self.ack_reject_count = 0
        self.ack_timeout_count = 0
        self.ack_retry_count = 0
        self.ack_consecutive_timeouts = 0
        self.ack_flow_suspended_until = 0.0
        self.current_inject_delay_s = self.INJECT_PACKET_DELAY
        self.inject_by_constellation: Dict[str, int] = {}
        self.experiment_csv_written = False

        self.gps_no_msg_warned = False
        self.gps_zero_sats_warned = False
        self.px4_checklist_printed = False
        self.tcpin_hint_printed = False

        # 读取工程配置文件；没有配置文件时使用上面的默认值。
        self.load_runtime_config()
        self.sync_legacy_link_fields()
        self.configure_drone_links()
        self.validate_runtime_config()
        self.current_inject_delay_s = min(
            self.ADAPTIVE_INJECT_DELAY_MAX_S,
            max(self.ADAPTIVE_INJECT_DELAY_MIN_S, self.INJECT_PACKET_DELAY),
        )
        self.warn_target_gps_link_mode()
        if self.COMMAND_CENTER_WEBHOOK_URL:
            print(
                f"[Config] 指挥中心告警上报已启用: "
                f"{self.COMMAND_CENTER_WEBHOOK_URL}"
            )
        else:
            print(
                "[Warn] COMMAND_CENTER_WEBHOOK_URL 尚未配置；定位超时告警会写入"
                "本机 alerts/outbox 和 agnss_alert_state.json，但暂时无法远程上报。"
            )

        self.lock = threading.Lock()

        # 读取配置后再创建多星座解析器。稳定工程版默认不解析/封装 GLONASS，
        # 只统计 GLO_SEEN，避免实验解析影响主流程。
        self.collector = MultiGNSSCollector(
            enable_glo_mga_experimental=self.ENABLE_GLO_MGA_EXPERIMENTAL,
            enable_gps_lnav_aux=self.ENABLE_ALM_IONO_UTC_EXPERIMENTAL,
        )
        self.load_assist_db()

    @staticmethod
    def fix_type_name(fix_type: int) -> str:
        return {
            0: "No GPS / No Fix",
            1: "No Fix",
            2: "2D Fix",
            3: "3D Fix",
            4: "DGPS",
            5: "RTK Float",
            6: "RTK Fixed",
        }.get(fix_type, f"Unknown({fix_type})")

    def load_runtime_config(self):
        """
        可选工程配置文件 agnss_config.json。
        只覆盖已存在的简单字段，避免现场改代码。
        示例：
        {
          "LINK_MODE": "udpout",
          "DRONE_UDPOUT": "udpout:192.168.1.50:14550",
          "BASE_PORT": "COM3",
          "TARGET_GPS_LINK_TYPE": "uart_via_px4",
          "ENABLE_TARGET_CFG_VIA_PX4": true,
          "ACK_MONITOR_ENABLED": false
        }
        """
        cfg_path = Path(getattr(self, "CONFIG_JSON", "agnss_config.json"))
        if not cfg_path.exists():
            self.write_default_config_template(cfg_path)
            return

        try:
            data = json.loads(cfg_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                print(f"[Config] {cfg_path} 不是 JSON 对象，忽略。")
                return

            applied = []
            for key, value in data.items():
                if not hasattr(self, key):
                    print(f"[Config] 未知配置项 {key}，忽略。")
                    continue
                if key == "DRONE_LINKS":
                    if isinstance(value, list):
                        setattr(self, key, value)
                        applied.append(key)
                    else:
                        print("[Config] DRONE_LINKS 必须是数组，忽略。")
                    continue
                if key == "PREFLIGHT_BLOCK_PATTERNS":
                    if isinstance(value, list):
                        setattr(self, key, [str(item) for item in value])
                        applied.append(key)
                    else:
                        print("[Config] PREFLIGHT_BLOCK_PATTERNS 必须是数组，忽略。")
                    continue
                old_value = getattr(self, key)
                if isinstance(old_value, bool):
                    if isinstance(value, str):
                        value = value.strip().lower() in ("1", "true", "yes", "on")
                    else:
                        value = bool(value)
                elif isinstance(old_value, int) and not isinstance(old_value, bool):
                    value = int(value)
                elif isinstance(old_value, float):
                    value = float(value)
                elif isinstance(old_value, str):
                    value = str(value)
                setattr(self, key, value)
                applied.append(key)
            if applied:
                print(f"[Config] 已加载 {cfg_path}: {', '.join(applied)}")
        except Exception as exc:
            print(f"[Config] 读取 {cfg_path} 失败，使用默认配置: {exc}")

    def write_default_config_template(self, cfg_path: Path):
        """首次运行生成配置模板，不影响本次默认运行。"""
        try:
            template = {
                "BASE_PORT": self.BASE_PORT,
                "BAUD_BASE": self.BAUD_BASE,

                "LINK_MODE": self.LINK_MODE,
                "DRONE_SERIAL_PORT": self.DRONE_SERIAL_PORT,
                "DRONE_SERIAL_BAUD": self.DRONE_SERIAL_BAUD,
                "DRONE_UDPOUT": self.DRONE_UDPOUT,
                "DRONE_UDPIN": self.DRONE_UDPIN,
                "DRONE_TCP": self.DRONE_TCP,
                "DRONE_LINKS": [
                    {
                        "name": "plane1",
                        "vehicle_id": 1,
                        "endpoint": "tcp:192.168.1.21:1024",
                        "system_id": 1,
                        "enabled": True,
                        "target_gps_model": "UNSPECIFIED",
                        "can_raw_ubx_validated": False,
                    },
                    {
                        "name": "plane2",
                        "vehicle_id": 2,
                        "endpoint": "tcp:192.168.1.22:1024",
                        "system_id": 2,
                        "enabled": True,
                        "target_gps_model": "UNSPECIFIED",
                        "can_raw_ubx_validated": False,
                    },
                    {
                        "name": "plane3",
                        "vehicle_id": 3,
                        "endpoint": "tcp:192.168.1.23:1024",
                        "system_id": 3,
                        "enabled": True,
                        "target_gps_model": "UNSPECIFIED",
                        "can_raw_ubx_validated": False,
                    },
                    {
                        "name": "plane4",
                        "vehicle_id": 4,
                        "endpoint": "tcp:192.168.1.24:1024",
                        "system_id": 4,
                        "enabled": True,
                        "target_gps_model": "UNSPECIFIED",
                        "can_raw_ubx_validated": False,
                    },
                ],
                "MULTI_LINK_SEND_MODE": self.MULTI_LINK_SEND_MODE,

                "TARGET_GPS_LINK_TYPE": self.TARGET_GPS_LINK_TYPE,
                "TARGET_GPS_MODEL": self.TARGET_GPS_MODEL,
                "CAN_PX4_PARAM_CHECK_ENABLED": self.CAN_PX4_PARAM_CHECK_ENABLED,
                "CAN_REQUIRE_PX4_PARAMS_BEFORE_INJECT": self.CAN_REQUIRE_PX4_PARAMS_BEFORE_INJECT,
                "CAN_TARGET_VALIDATION_CONFIRMED": self.CAN_TARGET_VALIDATION_CONFIRMED,
                "CAN_PARAM_REQUEST_INTERVAL_S": self.CAN_PARAM_REQUEST_INTERVAL_S,
                "CAN_PARAM_CACHE_TTL_S": self.CAN_PARAM_CACHE_TTL_S,
                "CAN_PROBE_TIMEOUT_S": self.CAN_PROBE_TIMEOUT_S,
                "CAN_PROBE_OBSERVE_S": self.CAN_PROBE_OBSERVE_S,
                "ENABLE_GLO_MGA_EXPERIMENTAL": self.ENABLE_GLO_MGA_EXPERIMENTAL,
                "ENABLE_TARGET_CFG_VIA_PX4": self.ENABLE_TARGET_CFG_VIA_PX4,

                "ACK_MONITOR_ENABLED": self.ACK_MONITOR_ENABLED,
                "ACK_PORT": self.ACK_PORT,
                "ACK_BAUD": self.ACK_BAUD,
                "ACK_FLOW_CONTROL_MODE": self.ACK_FLOW_CONTROL_MODE,
                "ACK_TARGET_LINK_NAME": self.ACK_TARGET_LINK_NAME,
                "ACK_WAIT_TIMEOUT_S": self.ACK_WAIT_TIMEOUT_S,
                "ACK_MAX_RETRIES": self.ACK_MAX_RETRIES,
                "ACK_TRANSIENT_BACKOFF_S": self.ACK_TRANSIENT_BACKOFF_S,
                "ACK_TIMEOUT_CIRCUIT_BREAKER": self.ACK_TIMEOUT_CIRCUIT_BREAKER,
                "ACK_SUSPEND_S": self.ACK_SUSPEND_S,
                "ADAPTIVE_INJECT_DELAY_MIN_S": self.ADAPTIVE_INJECT_DELAY_MIN_S,
                "ADAPTIVE_INJECT_DELAY_MAX_S": self.ADAPTIVE_INJECT_DELAY_MAX_S,

                "PRE_HEARTBEAT_INJECT_ENABLED": self.PRE_HEARTBEAT_INJECT_ENABLED,
                "POST_HEARTBEAT_REQUEST_SETTLE_S": self.POST_HEARTBEAT_REQUEST_SETTLE_S,
                "EARLY_BOOT_ASSIST_ENABLED": self.EARLY_BOOT_ASSIST_ENABLED,
                "EARLY_BOOT_MAX_TIME_BOOT_S": self.EARLY_BOOT_MAX_TIME_BOOT_S,

                "FIRST_FIX_TOP_N": self.FIRST_FIX_TOP_N,
                "MAX_INJECT_EPH_PER_ROUND": self.MAX_INJECT_EPH_PER_ROUND,
                "INJECT_PACKET_DELAY": self.INJECT_PACKET_DELAY,
                "EPH_SEMANTIC_HALF_WINDOW_S": self.EPH_SEMANTIC_HALF_WINDOW_S,
                "ENABLED_ASSIST_CONSTELLATIONS": self.ENABLED_ASSIST_CONSTELLATIONS,
                "CONSTELLATION_PRIORITY": self.CONSTELLATION_PRIORITY,
                "BACKGROUND_BROADCAST_ENABLED": self.BACKGROUND_BROADCAST_ENABLED,
                "BACKGROUND_BROADCAST_INTERVAL_S": self.BACKGROUND_BROADCAST_INTERVAL_S,
                "NEW_EPH_BROADCAST_ENABLED": self.NEW_EPH_BROADCAST_ENABLED,
                "NEW_EPH_BROADCAST_MIN_INTERVAL_S": self.NEW_EPH_BROADCAST_MIN_INTERVAL_S,
                "ACTIVE_UNFIXED_INJECT_INTERVAL_S": self.ACTIVE_UNFIXED_INJECT_INTERVAL_S,
                "CONTINUE_INJECT_AFTER_3D_FIX_S": self.CONTINUE_INJECT_AFTER_3D_FIX_S,
                "ARMABLE_PREFLIGHT_SILENCE_S": self.ARMABLE_PREFLIGHT_SILENCE_S,
                "ARMABLE_MIN_FIX_TYPE": self.ARMABLE_MIN_FIX_TYPE,
                "MIN_QUALITY_FIX_SATS": self.MIN_QUALITY_FIX_SATS,
                "ASSIST_DB_ENABLED": self.ASSIST_DB_ENABLED,
                "ASSIST_DB_PATH": self.ASSIST_DB_PATH,
                "ASSIST_DB_SAVE_INTERVAL_S": self.ASSIST_DB_SAVE_INTERVAL_S,
                "ENABLE_ALM_IONO_UTC_EXPERIMENTAL": self.ENABLE_ALM_IONO_UTC_EXPERIMENTAL,
                "MAX_ALM_AGE_S": self.MAX_ALM_AGE_S,
                "MAX_AUX_AGE_S": self.MAX_AUX_AGE_S,
                "INJECT_AUX_IN_FAST_ROUNDS": self.INJECT_AUX_IN_FAST_ROUNDS,
                "INJECT_ALMANAC_ON_FIRST_FIX": self.INJECT_ALMANAC_ON_FIRST_FIX,
                "FORCE_SESSION_ALMANAC": self.FORCE_SESSION_ALMANAC,
                "ALMANAC_REINJECT_INTERVAL_S": self.ALMANAC_REINJECT_INTERVAL_S,
                "MAX_ALMANAC_PER_ROUND": self.MAX_ALMANAC_PER_ROUND,
                "SUPPRESS_BACKGROUND_DURING_ACTIVE_ASSIST": self.SUPPRESS_BACKGROUND_DURING_ACTIVE_ASSIST,
                "PREFLIGHT_BLOCK_PATTERNS": self.PREFLIGHT_BLOCK_PATTERNS,
                "VEHICLE_OFFLINE_TIMEOUT_S": self.VEHICLE_OFFLINE_TIMEOUT_S,
                "VEHICLE_RETENTION_S": self.VEHICLE_RETENTION_S,
                "ACCEPTED_AUTOPILOT_COMPONENT_IDS": self.ACCEPTED_AUTOPILOT_COMPONENT_IDS,
                "FIX_WARNING_TIMEOUT_S": self.FIX_WARNING_TIMEOUT_S,
                "FIX_CRITICAL_TIMEOUT_S": self.FIX_CRITICAL_TIMEOUT_S,
                "FIX_CRITICAL_REPEAT_INTERVAL_S": self.FIX_CRITICAL_REPEAT_INTERVAL_S,
                "COMMAND_CENTER_WEBHOOK_URL": self.COMMAND_CENTER_WEBHOOK_URL,
                "COMMAND_CENTER_DEVICE_ID": self.COMMAND_CENTER_DEVICE_ID,
                "COMMAND_CENTER_HTTP_TIMEOUT_S": self.COMMAND_CENTER_HTTP_TIMEOUT_S,
                "COMMAND_CENTER_RETRY_INTERVAL_S": self.COMMAND_CENTER_RETRY_INTERVAL_S,
                "COMMAND_CENTER_TOKEN_ENV": self.COMMAND_CENTER_TOKEN_ENV,

                "MIN_EPH_BEFORE_POWER_ON": self.MIN_EPH_BEFORE_POWER_ON,
                "DESIRED_EPH_BEFORE_POWER_ON": self.DESIRED_EPH_BEFORE_POWER_ON,
                "MAX_WAIT_AFTER_MIN_EPH": self.MAX_WAIT_AFTER_MIN_EPH,

                "DATALINK_LOSS_TIMEOUT_S": self.DATALINK_LOSS_TIMEOUT_S,
                "MAX_EPH_AGE_S": self.MAX_EPH_AGE_S,
                "BASE_PVT_MAX_AGE_S": self.BASE_PVT_MAX_AGE_S,
                "BASE_SFRBX_MAX_AGE_S": self.BASE_SFRBX_MAX_AGE_S,
                "POS_ACC_CM": self.POS_ACC_CM,
                "TIME_ACC_S": self.TIME_ACC_S,
                "TIME_ACC_NS": self.TIME_ACC_NS,
                "BASE_SAT_LOG_ENABLED": self.BASE_SAT_LOG_ENABLED,
                "BASE_SAT_LOG_INTERVAL_S": self.BASE_SAT_LOG_INTERVAL_S,
            }
            cfg_path.write_text(json.dumps(template, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[Config] 未发现 {cfg_path}，已生成默认模板；本次使用代码默认值运行。")
        except Exception as exc:
            print(f"[Config] 生成默认配置模板失败: {exc}")

    def sync_legacy_link_fields(self):
        """同步旧变量名，减少对原脚本其它位置的侵入式修改。"""
        self.LINK_MODE = str(self.LINK_MODE).lower().strip()
        if self.LINK_MODE == "serial":
            self.DRONE_PORT = self.DRONE_SERIAL_PORT
            self.BAUD_DRONE = int(self.DRONE_SERIAL_BAUD)
        elif self.LINK_MODE == "udpout":
            self.DRONE_PORT = self.DRONE_UDPOUT
            self.BAUD_DRONE = 0
        elif self.LINK_MODE == "udpin":
            self.DRONE_PORT = self.DRONE_UDPIN
            self.BAUD_DRONE = 0
        elif self.LINK_MODE == "tcp":
            self.DRONE_PORT = self.DRONE_TCP
            self.BAUD_DRONE = 0
        else:
            raise ValueError(f"LINK_MODE 只能是 serial/udpout/udpin/tcp，当前为: {self.LINK_MODE}")

    def multi_link_enabled(self) -> bool:
        return bool(getattr(self, "DRONE_LINKS", None))

    @staticmethod
    def optional_config_bool(value) -> Optional[bool]:
        """解析链路内的可选布尔值；None 表示使用旧的全局回退配置。"""
        if value is None:
            return None
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off"}:
                return False
            raise ValueError(f"无法解析布尔值: {value}")
        return bool(value)

    @staticmethod
    def normalize_gps_model(value) -> str:
        model = str(value or "UNSPECIFIED").strip().upper()
        return model.replace("-", "_").replace(" ", "_") or "UNSPECIFIED"

    def configure_drone_links(self):
        """把 DRONE_LINKS 配置转换为运行时链路对象。为空时保留旧单链路模式。"""
        self.drone_links = []
        self.link_by_name = {}
        self.configured_system_to_link = {}

        if not self.multi_link_enabled():
            return

        if not isinstance(self.DRONE_LINKS, list):
            raise ValueError("DRONE_LINKS 必须是数组")

        seen_names = set()
        for index, item in enumerate(self.DRONE_LINKS, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"DRONE_LINKS[{index}] 必须是 JSON 对象")

            enabled = bool(item.get("enabled", True))
            name = str(item.get("name") or f"plane{index}").strip()
            endpoint = str(item.get("endpoint") or "").strip()
            bind_address = str(item.get("bind_address") or "").strip()
            if not name:
                raise ValueError(f"DRONE_LINKS[{index}] name 不能为空")
            if name in seen_names:
                raise ValueError(f"DRONE_LINKS 存在重复 name: {name}")
            seen_names.add(name)
            if not endpoint:
                raise ValueError(f"DRONE_LINKS[{index}] endpoint 不能为空")
            if bind_address:
                if not endpoint.lower().startswith(("tcp:", "udpout:")):
                    raise ValueError(f"{name}.bind_address 仅支持 tcp:/udpout: endpoint")
                try:
                    socket.inet_pton(socket.AF_INET, bind_address)
                except OSError as exc:
                    raise ValueError(f"{name}.bind_address 必须是有效 IPv4 地址") from exc

            vehicle_id = int(item.get("vehicle_id", index))
            system_id_value = item.get("system_id")
            system_id = None
            if system_id_value not in (None, ""):
                system_id = int(system_id_value)
                if system_id <= 0 or system_id == 255:
                    raise ValueError(f"{name}.system_id 必须是 1..254")

            baud = int(item.get("baud", 0) or 0)
            target_gps_model = self.normalize_gps_model(
                item.get("target_gps_model", self.TARGET_GPS_MODEL)
            )
            can_raw_ubx_validated = self.optional_config_bool(
                item.get("can_raw_ubx_validated")
            )
            link = DroneLink(
                name=name,
                vehicle_id=vehicle_id,
                endpoint=endpoint,
                bind_address=bind_address,
                system_id=system_id,
                enabled=enabled,
                baud=baud,
                target_gps_model=target_gps_model,
                can_raw_ubx_validated=can_raw_ubx_validated,
            )
            self.drone_links.append(link)
            self.link_by_name[link.name] = link
            if system_id is not None:
                if system_id in self.configured_system_to_link:
                    raise ValueError(f"DRONE_LINKS 存在重复 system_id: {system_id}")
                self.configured_system_to_link[system_id] = link.name

        enabled_count = sum(1 for link in self.drone_links if link.enabled)
        if enabled_count <= 0:
            raise ValueError("DRONE_LINKS 至少要启用一条链路")
        self.DRONE_PORT = ",".join(
            f"{link.name}={link.endpoint}" for link in self.drone_links if link.enabled
        )

    def validate_runtime_config(self):
        """在打开硬件前拒绝明显危险或自相矛盾的配置。"""
        self.TARGET_GPS_MODEL = self.normalize_gps_model(self.TARGET_GPS_MODEL)
        if self.target_gps_is_can():
            profiles = self.configured_can_target_profiles()
            invalid = [
                profile["link_name"]
                for profile in profiles
                if profile["enabled"]
                and profile["can_raw_ubx_validated"]
                and profile["target_gps_model"] in {"UNSPECIFIED", "UNKNOWN"}
            ]
            if invalid:
                raise ValueError(
                    "CAN 原始 UBX 已标记验证，但未配置目标 GPS 型号: "
                    + ", ".join(invalid)
                )

        positive_fields = (
            "MAX_EPH_AGE_S",
            "MAX_ALM_AGE_S",
            "MAX_AUX_AGE_S",
            "BASE_PVT_MAX_AGE_S",
            "BASE_SFRBX_MAX_AGE_S",
            "BASE_SAT_LOG_INTERVAL_S",
            "BACKGROUND_BROADCAST_INTERVAL_S",
            "NEW_EPH_BROADCAST_MIN_INTERVAL_S",
            "ACTIVE_UNFIXED_INJECT_INTERVAL_S",
            "ARMABLE_PREFLIGHT_SILENCE_S",
            "MIN_QUALITY_FIX_SATS",
            "VEHICLE_OFFLINE_TIMEOUT_S",
            "INJECT_PACKET_DELAY",
            "FIX_WARNING_TIMEOUT_S",
            "FIX_CRITICAL_TIMEOUT_S",
            "FIX_CRITICAL_REPEAT_INTERVAL_S",
            "COMMAND_CENTER_HTTP_TIMEOUT_S",
            "COMMAND_CENTER_RETRY_INTERVAL_S",
            "ASSIST_DB_SAVE_INTERVAL_S",
            "ALMANAC_REINJECT_INTERVAL_S",
            "EPH_SEMANTIC_HALF_WINDOW_S",
            "ACK_WAIT_TIMEOUT_S",
            "ACK_TRANSIENT_BACKOFF_S",
            "ACK_SUSPEND_S",
            "ADAPTIVE_INJECT_DELAY_MIN_S",
            "ADAPTIVE_INJECT_DELAY_MAX_S",
            "CAN_PARAM_REQUEST_INTERVAL_S",
            "CAN_PARAM_CACHE_TTL_S",
            "CAN_PROBE_TIMEOUT_S",
            "CAN_PROBE_OBSERVE_S",
            "EARLY_BOOT_MAX_TIME_BOOT_S",
        )
        for field_name in positive_fields:
            if float(getattr(self, field_name)) <= 0:
                raise ValueError(f"{field_name} 必须大于 0")

        if self.MIN_EPH_BEFORE_POWER_ON <= 0:
            raise ValueError("MIN_EPH_BEFORE_POWER_ON 必须大于 0")
        if self.DESIRED_EPH_BEFORE_POWER_ON < self.MIN_EPH_BEFORE_POWER_ON:
            raise ValueError("DESIRED_EPH_BEFORE_POWER_ON 不能小于最低星历门槛")
        if self.FIRST_FIX_TOP_N <= 0 or self.MAX_INJECT_EPH_PER_ROUND <= 0:
            raise ValueError("每轮注入星历数量必须大于 0")
        if self.MAX_ALMANAC_PER_ROUND <= 0 or self.MAX_ALMANAC_PER_ROUND > 32:
            raise ValueError("MAX_ALMANAC_PER_ROUND 必须是 1..32")
        if self.CONTINUE_INJECT_AFTER_3D_FIX_S < 0:
            raise ValueError("CONTINUE_INJECT_AFTER_3D_FIX_S 不能小于 0")
        if self.ARMABLE_MIN_FIX_TYPE < 3:
            raise ValueError("ARMABLE_MIN_FIX_TYPE 不能小于 3")
        if self.MIN_QUALITY_FIX_SATS < 4:
            raise ValueError("MIN_QUALITY_FIX_SATS 不能小于 4")
        if self.BURST_ROUNDS < 0:
            raise ValueError("BURST_ROUNDS 不能小于 0")
        if self.FIX_CRITICAL_TIMEOUT_S <= self.FIX_WARNING_TIMEOUT_S:
            raise ValueError("FIX_CRITICAL_TIMEOUT_S 必须大于 FIX_WARNING_TIMEOUT_S")
        self.ACK_FLOW_CONTROL_MODE = self.ACK_FLOW_CONTROL_MODE.strip().lower()
        if self.ACK_FLOW_CONTROL_MODE not in {"off", "observe", "stop_and_wait"}:
            raise ValueError("ACK_FLOW_CONTROL_MODE 只能是 off/observe/stop_and_wait")
        if self.ACK_MAX_RETRIES < 0:
            raise ValueError("ACK_MAX_RETRIES 不能小于 0")
        if self.ACK_TIMEOUT_CIRCUIT_BREAKER <= 0:
            raise ValueError("ACK_TIMEOUT_CIRCUIT_BREAKER 必须大于 0")
        if self.ADAPTIVE_INJECT_DELAY_MIN_S > self.ADAPTIVE_INJECT_DELAY_MAX_S:
            raise ValueError("ADAPTIVE_INJECT_DELAY_MIN_S 不能大于最大值")
        if self.CAN_PARAM_CACHE_TTL_S < 0:
            raise ValueError("CAN_PARAM_CACHE_TTL_S 不能小于 0")

        if not isinstance(self.ENABLED_ASSIST_CONSTELLATIONS, list):
            raise ValueError("ENABLED_ASSIST_CONSTELLATIONS 必须是数组")
        allowed_constellations = {"GPS", "QZSS", "GAL", "BDS", "GLO"}
        self.ENABLED_ASSIST_CONSTELLATIONS = [
            str(value).strip().upper()
            for value in self.ENABLED_ASSIST_CONSTELLATIONS
            if str(value).strip()
        ]
        unknown_constellations = (
            set(self.ENABLED_ASSIST_CONSTELLATIONS) - allowed_constellations
        )
        if unknown_constellations:
            raise ValueError(
                "ENABLED_ASSIST_CONSTELLATIONS 含未知星座: "
                + ",".join(sorted(unknown_constellations))
            )
        if not isinstance(self.CONSTELLATION_PRIORITY, dict):
            raise ValueError("CONSTELLATION_PRIORITY 必须是 JSON 对象")
        self.CONSTELLATION_PRIORITY = {
            str(key).strip().upper(): float(value)
            for key, value in self.CONSTELLATION_PRIORITY.items()
        }

        try:
            self.ACCEPTED_AUTOPILOT_COMPONENT_IDS = [
                int(value) for value in self.ACCEPTED_AUTOPILOT_COMPONENT_IDS
            ]
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "ACCEPTED_AUTOPILOT_COMPONENT_IDS 必须是整数数组"
            ) from exc

    def warn_target_gps_link_mode(self):
        if self.TARGET_GPS_LINK_TYPE.lower().startswith("can"):
            print(
                "[CAN] 当前 TARGET_GPS_LINK_TYPE=can_via_px4。"
                "脚本会先读取 UAVCAN_ENABLE/UAVCAN_SUB_GPS/UAVCAN_PUB_RTCM；"
                "参数未确认前禁止注入。"
            )
            profile_text = ", ".join(
                f"{item['link_name']}={item['target_gps_model']}"
                f"/raw_ubx={'yes' if item['can_raw_ubx_validated'] else 'no'}"
                for item in self.configured_can_target_profiles()
                if item["enabled"]
            )
            print(f"[CAN] 目标 GPS 配置: {profile_text or '无启用链路'}")
            print(
                "[CAN] PX4 参数合格只证明具备发布 DroneCAN RTCMStream(DTID 1062)的条件；"
                "每条飞机链路还必须单独标记目标 CAN GPS 的原始 UBX 透传验证结果。"
            )
        else:
            print(
                "[Config] 目标GPS链路模式: uart_via_px4。"
                "请确保 PX4 gps.cpp 已允许 UBX-MGA/必要时 UBX-CFG-VALSET 旁路写入 GPS UART。"
            )

    def target_gps_is_can(self) -> bool:
        return self.TARGET_GPS_LINK_TYPE.strip().lower().startswith("can")

    def can_target_profile(self, link: Optional[DroneLink] = None) -> dict:
        if link is None:
            return {
                "link_name": "single",
                "enabled": True,
                "configured_system_id": None,
                "target_gps_model": self.normalize_gps_model(self.TARGET_GPS_MODEL),
                "can_raw_ubx_validated": bool(self.CAN_TARGET_VALIDATION_CONFIRMED),
                "validation_source": "legacy_global",
            }

        explicit = link.can_raw_ubx_validated is not None
        return {
            "link_name": link.name,
            "enabled": bool(link.enabled),
            "configured_system_id": link.system_id,
            "target_gps_model": self.normalize_gps_model(link.target_gps_model),
            "can_raw_ubx_validated": bool(
                link.can_raw_ubx_validated
                if explicit
                else self.CAN_TARGET_VALIDATION_CONFIRMED
            ),
            "validation_source": "per_link" if explicit else "legacy_global",
        }

    def configured_can_target_profiles(self) -> List[dict]:
        if self.multi_link_enabled():
            return [self.can_target_profile(link) for link in self.drone_links]
        return [self.can_target_profile(None)]

    def can_target_is_validated(self, link: Optional[DroneLink] = None) -> bool:
        return bool(self.can_target_profile(link)["can_raw_ubx_validated"])

    @staticmethod
    def can_param_scope(system_id: int, link: Optional[DroneLink] = None) -> str:
        prefix = link.name if link is not None else "single"
        return f"{prefix}/{int(system_id)}"

    @staticmethod
    def decode_mavlink_param_id(value) -> str:
        if isinstance(value, bytes):
            value = value.decode("ascii", errors="ignore")
        return str(value).split("\x00", 1)[0].strip()

    @staticmethod
    def decode_mavlink_param_value(raw_value, param_type: int):
        """兼容 PX4 对整数 PARAM_VALUE 使用的 byte-wise 位模式编码。"""
        value = float(raw_value)
        integer_layouts = {
            1: (1, False),  # MAV_PARAM_TYPE_UINT8
            2: (1, True),   # MAV_PARAM_TYPE_INT8
            3: (2, False),  # MAV_PARAM_TYPE_UINT16
            4: (2, True),   # MAV_PARAM_TYPE_INT16
            5: (4, False),  # MAV_PARAM_TYPE_UINT32
            6: (4, True),   # MAV_PARAM_TYPE_INT32
        }
        layout = integer_layouts.get(int(param_type))
        if layout is None:
            return value

        # C-cast 编码会直接得到 1.0/2.0；PX4 byte-wise 编码的小整数则表现为
        # 1.40e-45/2.80e-45。零值在两种编码下结果相同。
        if value != 0.0 and value == value and abs(value) >= 1e-30:
            return value
        packed = struct.pack("<f", value)
        width, signed = layout
        return int.from_bytes(packed[:width], byteorder="little", signed=signed)

    @staticmethod
    def can_param_value_matches(name: str, value: float) -> bool:
        requirement = CAN_REQUIRED_PX4_PARAMS.get(name)
        if requirement is None:
            return False
        if "equals" in requirement:
            return abs(float(value) - float(requirement["equals"])) < 0.001
        return float(value) >= float(requirement.get("minimum", 0.0))

    def process_can_px4_param_value(
        self,
        msg,
        link: Optional[DroneLink] = None,
    ) -> bool:
        """保存 CAN 注入所需的 PX4 参数读回值。"""
        vehicle = self.find_vehicle_by_system(msg.get_srcSystem(), link=link)
        if vehicle is None or int(msg.get_srcComponent()) != vehicle.component_id:
            return False
        name = self.decode_mavlink_param_id(getattr(msg, "param_id", ""))
        if name not in CAN_REQUIRED_PX4_PARAMS:
            return False
        param_type = int(getattr(msg, "param_type", 0) or 0)
        value = self.decode_mavlink_param_value(
            getattr(msg, "param_value", 0.0),
            param_type,
        )
        scope = self.can_param_scope(msg.get_srcSystem(), link)
        observation = {
            "name": name,
            "value": value,
            "ok": self.can_param_value_matches(name, value),
            "system_id": int(msg.get_srcSystem()),
            "component_id": int(msg.get_srcComponent()),
            "link_name": link.name if link is not None else None,
            "param_type": param_type,
            "received_at": time.time(),
        }
        with self.lock:
            self.can_px4_params.setdefault(scope, {})[name] = observation
            self.can_param_response_count += 1
        scope_ready = self.can_scope_ready(scope)
        if scope_ready:
            with self.lock:
                self.can_verified_param_cache[scope] = {
                    "verified_at": time.time(),
                    "params": dict(self.can_px4_params.get(scope, {})),
                }
        if scope_ready and vehicle.first_can_ready_time is None:
            vehicle.first_can_ready_time = time.time()
            reference = vehicle.estimated_boot_time or vehicle.online_since
            print(
                f"\n[CAN:{scope}] 启动所需 CAN 参数已全部确认，"
                f"启动参考到 CAN 就绪={vehicle.first_can_ready_time - reference:.3f}s"
            )
        state = "OK" if observation["ok"] else "NOT_READY"
        print(f"\n[CAN:{scope}] PX4 参数 {name}={value:g} [{state}]")
        return True

    def can_scope_ready(self, scope: str) -> bool:
        with self.lock:
            observed = dict(self.can_px4_params.get(scope, {}))
        return all(
            name in observed and bool(observed[name].get("ok"))
            for name in CAN_REQUIRED_PX4_PARAMS
        )

    def reset_vehicle_can_param_scope(
        self,
        vehicle: VehicleState,
        link: Optional[DroneLink] = None,
    ):
        """使用近期已验证值抢先注入，并在新会话中立即后台复核。"""
        scope = self.can_param_scope(vehicle.system_id, link)
        now = time.time()
        with self.lock:
            cached = self.can_verified_param_cache.get(scope)
            cache_age = None if cached is None else now - float(cached.get("verified_at", 0.0))
            if (
                cached is not None
                and cache_age is not None
                and cache_age <= self.CAN_PARAM_CACHE_TTL_S
            ):
                cached_params = {
                    name: dict(value, cached=True, cache_age_s=round(cache_age, 3))
                    for name, value in dict(cached.get("params", {})).items()
                }
                self.can_px4_params[scope] = cached_params
            else:
                self.can_px4_params.pop(scope, None)
        self.can_last_param_request_time.pop(scope, None)
        if self.can_scope_ready(scope):
            vehicle.first_can_ready_time = now
            reference = vehicle.estimated_boot_time or vehicle.online_since
            print(
                f"\n[CAN:{scope}] 使用近期已验证 CAN 参数缓存抢先注入，"
                f"启动参考到 CAN 就绪={now - reference:.3f}s；已同时发起后台复核"
            )

    def request_vehicle_can_params(
        self,
        vehicle: VehicleState,
        link: Optional[DroneLink] = None,
        force: bool = False,
    ) -> bool:
        if not self.target_gps_is_can() or not self.CAN_PX4_PARAM_CHECK_ENABLED:
            return False
        master = link.master if link is not None else self.master
        send_lock = link.send_lock if link is not None else self.mav_send_lock
        if master is None:
            return False
        scope = self.can_param_scope(vehicle.system_id, link)
        now = time.time()
        last_request = self.can_last_param_request_time.get(scope, 0.0)
        if not force and now - last_request < self.CAN_PARAM_REQUEST_INTERVAL_S:
            return False
        if self.can_scope_ready(scope) and not force:
            return False
        try:
            with send_lock:
                for name in CAN_REQUIRED_PX4_PARAMS:
                    master.mav.param_request_read_send(
                        vehicle.system_id,
                        vehicle.component_id,
                        name.encode("ascii"),
                        -1,
                    )
            self.can_last_param_request_time[scope] = now
            self.can_param_request_count += len(CAN_REQUIRED_PX4_PARAMS)
            return True
        except Exception as exc:
            print(f"\n[CAN:{scope}] 请求 PX4 CAN 参数失败: {exc}")
            return False

    def request_pending_can_params(self):
        if not self.target_gps_is_can() or not self.CAN_PX4_PARAM_CHECK_ENABLED:
            return
        for vehicle in self.online_vehicles():
            link = self.link_by_name.get(vehicle.link_name) if vehicle.link_name else None
            self.request_vehicle_can_params(vehicle, link=link)

    def can_injection_allowed(self, link: Optional[DroneLink] = None) -> bool:
        if not self.target_gps_is_can():
            return True
        if not self.can_probe_mode:
            if link is not None:
                target_profiles = [self.can_target_profile(link)]
            elif self.multi_link_enabled():
                target_profiles = [
                    self.can_target_profile(item)
                    for item in self.drone_links
                    if item.enabled
                ]
            else:
                target_profiles = [self.can_target_profile(None)]
            unvalidated = [
                f"{item['link_name']}={item['target_gps_model']}"
                for item in target_profiles
                if not item["can_raw_ubx_validated"]
            ]
            if unvalidated:
                self.note_can_inject_block(
                    "目标 CAN GPS 原始 UBX 透传未验证: "
                    + ", ".join(unvalidated)
                )
                return False
        if not self.CAN_REQUIRE_PX4_PARAMS_BEFORE_INJECT:
            return True
        if not self.CAN_PX4_PARAM_CHECK_ENABLED:
            self.note_can_inject_block("参数检查已关闭，但注入安全门槛仍启用")
            return False
        candidates = [
            vehicle for vehicle in self.online_vehicles()
            if link is None or vehicle.link_name == link.name
        ]
        if not candidates:
            self.note_can_inject_block("尚无在线 PX4")
            return False
        not_ready = []
        for vehicle in candidates:
            scope_link = self.link_by_name.get(vehicle.link_name) if vehicle.link_name else None
            scope = self.can_param_scope(vehicle.system_id, scope_link)
            if not self.can_scope_ready(scope):
                not_ready.append(scope)
        if not_ready:
            self.note_can_inject_block(
                "等待 PX4 CAN 参数合格: " + ", ".join(not_ready)
            )
            return False
        return True

    def note_can_inject_block(self, reason: str):
        self.can_inject_block_count += 1
        self.can_last_block_reason = reason
        now = time.time()
        if now - self.can_last_block_print_time >= 5.0:
            print(f"\n[CAN] 已阻止 UBX-MGA 注入: {reason}")
            self.can_last_block_print_time = now

    def can_status_snapshot(self) -> Dict[str, object]:
        now = time.time()
        target_profiles = self.configured_can_target_profiles()
        enabled_profiles = [item for item in target_profiles if item["enabled"]]
        all_enabled_targets_validated = bool(enabled_profiles) and all(
            item["can_raw_ubx_validated"] for item in enabled_profiles
        )
        with self.lock:
            raw_scopes = {
                scope: {name: dict(item) for name, item in values.items()}
                for scope, values in self.can_px4_params.items()
            }
        scopes = {}
        for scope, values in raw_scopes.items():
            params = {}
            for name in CAN_REQUIRED_PX4_PARAMS:
                item = values.get(name)
                params[name] = None if item is None else {
                    "value": item["value"],
                    "ok": item["ok"],
                    "age_s": round(now - float(item["received_at"]), 2),
                }
            scopes[scope] = {
                "ready": all(
                    item is not None and bool(item.get("ok"))
                    for item in values.values()
                ) and all(name in values for name in CAN_REQUIRED_PX4_PARAMS),
                "params": params,
            }

        online = self.online_vehicles()
        online_scopes = []
        online_profiles = []
        seen_profile_links = set()
        for vehicle in online:
            scope_link = self.link_by_name.get(vehicle.link_name) if vehicle.link_name else None
            online_scopes.append(self.can_param_scope(vehicle.system_id, scope_link))
            profile = self.can_target_profile(scope_link)
            if profile["link_name"] not in seen_profile_links:
                seen_profile_links.add(profile["link_name"])
                online_profiles.append(profile)
        all_px4_ready = bool(online_scopes) and all(
            self.can_scope_ready(scope) for scope in online_scopes
        )
        all_online_targets_validated = bool(online_profiles) and all(
            item["can_raw_ubx_validated"] for item in online_profiles
        )
        if not self.target_gps_is_can():
            support_state = "NOT_APPLICABLE_UART_MODE"
        elif not online_scopes:
            support_state = "WAITING_FOR_PX4"
        elif not all_online_targets_validated:
            support_state = "BLOCKED_TARGET_RAW_UBX_UNVALIDATED"
        elif not all_px4_ready:
            support_state = "BLOCKED_WAITING_PX4_CAN_PARAMS"
        else:
            support_state = "CAN_ASSIST_ENABLED_OPERATOR_VALIDATED"
        return {
            "mode": self.target_gps_is_can(),
            "support_state": support_state,
            "target_profiles": target_profiles,
            "all_enabled_targets_raw_ubx_validated": all_enabled_targets_validated,
            "all_online_targets_raw_ubx_validated": all_online_targets_validated,
            "required_px4_params": CAN_REQUIRED_PX4_PARAMS,
            "online_scopes": online_scopes,
            "px4_rtcmstream_publisher_ready": all_px4_ready,
            "scopes": scopes,
            "param_request_count": self.can_param_request_count,
            "param_response_count": self.can_param_response_count,
            "inject_block_count": self.can_inject_block_count,
            "last_block_reason": self.can_last_block_reason,
            "mavlink_gps_rtcm_data_sent_count": self.inject_packet_count,
            "dronecan_rtcmstream_1062_observed": None,
            "target_can_raw_ubx_validated_by_operator": all_online_targets_validated,
            "target_acceptance_verified_by_runtime": self.ack_accept_count > 0,
            "target_gps_ack_observed": self.ack_accept_count > 0,
            "probe": {
                "active": self.can_probe_mode,
                "started_at": self.can_probe_started_at,
                "sent_at": self.can_probe_sent_at,
                "payload": self.can_probe_payload,
                "result": self.can_probe_result,
            },
        }

    def start_can_probe(self):
        if not self.target_gps_is_can():
            raise ValueError("--can-probe 要求 TARGET_GPS_LINK_TYPE=can_via_px4")
        self.can_probe_mode = True
        self.can_probe_started_at = time.time()
        self.can_probe_result = "RUNNING_WAITING_FOR_SINGLE_MGA_PACKET"
        print(
            "[CAN-Probe] 单包探测模式启动：参数合格且辅助数据就绪后只发送 1 包 UBX-MGA，"
            "随后退出；该结果不等同于目标 CAN GPS 已接收。"
        )

    def record_can_probe_send(self, ubx_msg: bytes, link_name: Optional[str]):
        if not self.can_probe_mode or self.can_probe_sent_at is not None:
            return
        self.can_probe_sent_at = time.time()
        self.can_probe_payload = {
            "link_name": link_name,
            "length": len(ubx_msg),
            "ubx_class": ubx_msg[2] if len(ubx_msg) >= 4 else None,
            "ubx_id": ubx_msg[3] if len(ubx_msg) >= 4 else None,
            "head_hex": ubx_msg[:12].hex(" ").upper(),
        }
        self.can_probe_result = "MAVLINK_PACKET_SENT_WAITING_OBSERVE_WINDOW"
        print(
            f"\n[CAN-Probe] 已向 PX4 发送唯一探测包: "
            f"link={link_name or 'single'}, len={len(ubx_msg)}, "
            f"class/id={ubx_msg[2]:02X}/{ubx_msg[3]:02X}"
        )

    def can_probe_allows_packet(self, ubx_msg: bytes) -> bool:
        if not self.can_probe_mode:
            return True
        if self.can_probe_sent_at is not None:
            return False
        if len(ubx_msg) < 8 or ubx_msg[2] != 0x13 or not verify_ubx(ubx_msg):
            self.note_can_inject_block("探测模式只允许校验正确的 UBX-MGA")
            return False
        return True

    def update_can_probe_lifecycle(self):
        if not self.can_probe_mode or self.can_probe_started_at is None:
            return
        now = time.time()
        if self.can_probe_sent_at is not None:
            if now - self.can_probe_sent_at >= self.CAN_PROBE_OBSERVE_S:
                self.can_probe_result = "ONE_MGA_PACKET_SENT_TO_PX4_TARGET_UNVERIFIED"
                self.write_status_json()
                print(
                    "\n[CAN-Probe] 探测结束：已送达 PX4 的 MAVLink 接口；"
                    "请用 CAN 抓包确认 DTID 1062，并检查目标 GPS 的接收证据。"
                )
                self.is_running = False
            return
        if now - self.can_probe_started_at >= self.CAN_PROBE_TIMEOUT_S:
            self.can_probe_result = "TIMEOUT_NO_MGA_PACKET_SENT"
            self.write_status_json()
            print("\n[CAN-Probe] 超时：未发送探测包，请检查基准站、PX4 在线状态和 CAN 参数。")
            self.is_running = False

    def create_mavlink_connection(self):
        """按 LINK_MODE 创建 MAVLink 连接，兼容原 COM4 数传和机巢网口。"""
        self.sync_legacy_link_fields()
        if self.LINK_MODE == "serial":
            print(f"[DataLink] 使用串口链路: {self.DRONE_SERIAL_PORT}, baud={self.DRONE_SERIAL_BAUD}")
            return mavutil.mavlink_connection(
                self.DRONE_SERIAL_PORT,
                baud=int(self.DRONE_SERIAL_BAUD),
                source_system=255,
                source_component=190
            )

        conn_str = {
            "udpout": self.DRONE_UDPOUT,
            "udpin": self.DRONE_UDPIN,
            "tcp": self.DRONE_TCP,
        }[self.LINK_MODE]
        print(f"[DataLink] 使用网络链路: {conn_str}")
        return mavutil.mavlink_connection(
            conn_str,
            source_system=255,
            source_component=190
        )

    def create_mavlink_connection_for_endpoint(
        self,
        endpoint: str,
        baud: int = 0,
        bind_address: str = "",
    ):
        """按 pymavlink endpoint 创建一条独立数据链连接。"""
        endpoint = str(endpoint).strip()
        if not endpoint:
            raise ValueError("MAVLink endpoint 不能为空")
        if endpoint.upper().startswith("COM") or endpoint.startswith("/dev/"):
            print(f"[DataLink] 使用独立串口链路: {endpoint}, baud={baud or self.DRONE_SERIAL_BAUD}")
            return mavutil.mavlink_connection(
                endpoint,
                baud=int(baud or self.DRONE_SERIAL_BAUD),
                source_system=255,
                source_component=190,
            )
        bind_address = str(bind_address or "").strip()
        if endpoint.lower().startswith("tcp:"):
            if bind_address:
                print(f"[DataLink] 使用独立网络链路: {endpoint}, bind={bind_address}")
                return create_bound_tcp_mavlink_connection(
                    endpoint,
                    bind_address,
                    source_system=255,
                    source_component=190,
                )
            else:
                print(f"[DataLink] 使用独立网络链路: {endpoint}")
            return create_resilient_tcp_mavlink_connection(
                endpoint,
                bind_address=bind_address,
                source_system=255,
                source_component=190,
            )
        if endpoint.lower().startswith("udpout:") and bind_address:
            print(f"[DataLink] 使用 UDP 输出链路: {endpoint}, bind={bind_address}")
            host_port = endpoint.split(":", 1)[1]
            if ":" not in host_port:
                raise ValueError(f"UDP endpoint 无效: {endpoint}")
            remote_host, remote_port = host_port.rsplit(":", 1)
            connection = mavutil.mavlink_connection(
                f"udpout:{remote_host}:{int(remote_port)}",
                source_system=255,
                source_component=190,
                input=False,
            )
            try:
                connection.port.bind((bind_address, int(remote_port)))
            except Exception:
                connection.close()
                raise
            return connection
        print(f"[DataLink] 使用独立网络链路: {endpoint}")
        return mavutil.mavlink_connection(
            endpoint,
            source_system=255,
            source_component=190,
        )

    def open_drone_link(self, link: DroneLink):
        """打开一条固定飞机数据链。失败时保留错误，主循环稍后重试。"""
        self.close_drone_link(link, join_thread=False)
        try:
            link.master = self.create_mavlink_connection_for_endpoint(
                link.endpoint,
                baud=link.baud,
                bind_address=link.bind_address,
            )
            link.connected = True
            link.last_open_time = time.time()
            link.last_error = None
            link.rx_error = None
            print(
                f"[DataLink:{link.name}] 已打开，vehicle_id={link.vehicle_id}, "
                f"system_id={link.system_id}, endpoint={link.endpoint}"
            )
            self.start_drone_link_rx_thread(link)
        except Exception as exc:
            link.master = None
            link.connected = False
            link.last_error = str(exc)
            print(f"\n[DataLink:{link.name}] 打开失败: {exc}")

    def close_drone_link(self, link: DroneLink, join_thread: bool = True):
        """关闭一条独立数据链。"""
        link.rx_running = False
        if join_thread and link.rx_thread is not None and link.rx_thread.is_alive():
            try:
                link.rx_thread.join(timeout=1.0)
            except Exception:
                pass
        link.rx_thread = None
        if link.master is not None:
            try:
                link.master.close()
            except Exception:
                pass
        link.master = None
        link.connected = False

    def set_engineering_state(self, state: str):
        if state != getattr(self, "engineering_state", None):
            self.engineering_state = state
            print(f"\n[State] -> {state}")

    def pending_alert_count(self) -> int:
        try:
            return sum(1 for _ in ALERT_OUTBOX_DIR.glob("*.json"))
        except OSError:
            return 0

    def _append_alert_journal(self, event: dict):
        ALERT_DIR.mkdir(parents=True, exist_ok=True)
        date_text = dt.datetime.now().strftime("%Y%m%d")
        journal = ALERT_DIR / f"agnss_alerts_{date_text}.jsonl"
        with self.alert_file_lock:
            with journal.open("a", encoding="utf-8") as file_obj:
                file_obj.write(json.dumps(event, ensure_ascii=False) + "\n")

    def _write_outbox_event(self, event: dict):
        ALERT_OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
        path = ALERT_OUTBOX_DIR / f"{event['event_id']}.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(event, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(path)

    def write_alert_state_json(self):
        """供本机监控程序直接读取的轻量告警状态。"""
        now = time.time()
        with self.lock:
            active = [
                {
                    "vehicle": vehicle.key,
                    "system_id": vehicle.system_id,
                    "component_id": vehicle.component_id,
                    "session_id": vehicle.session_id,
                    "level": vehicle.alert_level,
                    "unfixed_duration_s": round(now - vehicle.online_since, 1),
                    "fix_type": vehicle.fix_type,
                    "satellites": vehicle.satellites,
                    "last_event_id": vehicle.last_alert_event_id,
                    "last_alert_time": vehicle.last_alert_time,
                }
                for vehicle in self.vehicles.values()
                if (
                    vehicle.online
                    and not self.vehicle_quality_fix_ready(vehicle)
                    and vehicle.alert_level != "NONE"
                )
            ]
        state = {
            "time": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
            "device_id": self.COMMAND_CENTER_DEVICE_ID,
            "active_alert_count": len(active),
            "active_alerts": active,
            "pending_delivery_count": self.pending_alert_count(),
            "generated_count": self.alert_generated_count,
            "delivered_count": self.alert_delivered_count,
            "delivery_error_count": self.alert_delivery_error_count,
            "last_delivery_error": self.last_alert_delivery_error,
            "command_center_configured": bool(self.COMMAND_CENTER_WEBHOOK_URL),
        }
        path = APP_DIR / "agnss_alert_state.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def emit_vehicle_alert(
        self,
        vehicle: VehicleState,
        event_type: str,
        severity: str,
        message: str,
        decision_required: bool,
    ) -> str:
        """先落盘再通知发送线程，确保告警不会因网络故障丢失。"""
        now = time.time()
        event_id = str(uuid.uuid4())
        event = {
            "schema_version": 1,
            "event_id": event_id,
            "event_type": event_type,
            "severity": severity,
            "event_time": dt.datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "device_id": self.COMMAND_CENTER_DEVICE_ID,
            "decision_required": decision_required,
            "recommended_actions": (
                ["CONTINUE_WAITING", "SWITCH_TO_NEXT_AIRCRAFT", "ABORT_MISSION"]
                if decision_required else []
            ),
            "message": message,
            "vehicle": {
                "system_id": vehicle.system_id,
                "component_id": vehicle.component_id,
                "session_id": vehicle.session_id,
                "online_since": dt.datetime.fromtimestamp(
                    vehicle.online_since
                ).astimezone().isoformat(timespec="seconds"),
                "unfixed_duration_s": round(now - vehicle.online_since, 3),
                "fix_type": vehicle.fix_type,
                "fix_name": self.fix_type_name(vehicle.fix_type),
                "satellites": vehicle.satellites,
                "gps_raw_count": vehicle.gps_raw_count,
            },
            "agnss": {
                "fresh_eph_count": self.fresh_assist_count(),
                "base_pvt_ok": self.base_pvt_is_fresh(now),
                "base_sfrbx_ok": self.base_sfrbx_is_fresh(now),
                "inject_round_count": self.inject_round_count,
                "inject_error_count": self.inject_error_count,
                "last_inject_age_s": (
                    None if vehicle.last_inject_time <= 0
                    else round(now - vehicle.last_inject_time, 3)
                ),
            },
            "delivery": {
                "attempts": 0,
                "last_attempt_time": None,
                "last_error": None,
            },
        }
        self._write_outbox_event(event)
        self._append_alert_journal(event)
        self.alert_generated_count += 1
        print(
            f"\n[ALERT][{severity}] {event_type} | vehicle={vehicle.key} | "
            f"event_id={event_id} | {message}"
        )
        self.alert_wakeup.set()
        self.write_alert_state_json()
        return event_id

    def evaluate_fix_timeout_alerts(self):
        now = time.time()
        pending = []
        with self.lock:
            for vehicle in self.vehicles.values():
                if not vehicle.online or self.vehicle_quality_fix_ready(vehicle):
                    continue
                elapsed = now - vehicle.online_since
                if elapsed >= self.FIX_CRITICAL_TIMEOUT_S:
                    repeat_due = (
                        vehicle.critical_alert_sent
                        and vehicle.last_alert_time is not None
                        and now - vehicle.last_alert_time
                        >= self.FIX_CRITICAL_REPEAT_INTERVAL_S
                    )
                    if not vehicle.critical_alert_sent or repeat_due:
                        pending.append((
                            vehicle,
                            "GNSS_FIX_TIMEOUT_CRITICAL",
                            "CRITICAL",
                            (
                                f"飞机 {vehicle.key} 上线后 {elapsed:.1f} 秒仍未达到质量定位，"
                                "需要指挥中心决定继续等待、切换下一架飞机或终止任务。"
                            ),
                        ))
                        vehicle.critical_alert_sent = True
                        vehicle.warning_alert_sent = True
                        vehicle.alert_level = "CRITICAL"
                        vehicle.last_alert_time = now
                        if vehicle.first_alert_time is None:
                            vehicle.first_alert_time = now
                elif elapsed >= self.FIX_WARNING_TIMEOUT_S and not vehicle.warning_alert_sent:
                    pending.append((
                        vehicle,
                        "GNSS_FIX_TIMEOUT_WARNING",
                        "WARNING",
                        (
                            f"飞机 {vehicle.key} 上线后 {elapsed:.1f} 秒仍未达到质量定位，"
                            "请关注定位状态并准备任务处置。"
                        ),
                    ))
                    vehicle.warning_alert_sent = True
                    vehicle.alert_level = "WARNING"
                    vehicle.last_alert_time = now
                    if vehicle.first_alert_time is None:
                        vehicle.first_alert_time = now

        for vehicle, event_type, severity, message in pending:
            event_id = self.emit_vehicle_alert(
                vehicle,
                event_type=event_type,
                severity=severity,
                message=message,
                decision_required=True,
            )
            with self.lock:
                vehicle.last_alert_event_id = event_id
                vehicle.alert_count += 1
        if pending:
            self.write_alert_state_json()

    def _deliver_alert_file(self, path: Path) -> bool:
        if not self.COMMAND_CENTER_WEBHOOK_URL:
            return False
        event = json.loads(path.read_text(encoding="utf-8"))
        body = json.dumps(event, ensure_ascii=False).encode("utf-8")
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-AGNSS-Event-ID": event["event_id"],
        }
        token = os.environ.get(self.COMMAND_CENTER_TOKEN_ENV, "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(
            self.COMMAND_CENTER_WEBHOOK_URL,
            data=body,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self.COMMAND_CENTER_HTTP_TIMEOUT_S,
            ) as response:
                status = int(getattr(response, "status", 200))
                if not 200 <= status < 300:
                    raise RuntimeError(f"HTTP {status}")
            path.unlink(missing_ok=True)
            self.alert_delivered_count += 1
            self.last_alert_delivery_error = None
            print(
                f"\n[AlertDelivery] 指挥中心已接收 event_id={event['event_id']}"
            )
            return True
        except Exception as exc:
            event["delivery"]["attempts"] = int(
                event["delivery"].get("attempts", 0)
            ) + 1
            event["delivery"]["last_attempt_time"] = (
                dt.datetime.now().astimezone().isoformat(timespec="seconds")
            )
            event["delivery"]["last_error"] = str(exc)
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps(event, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(path)
            self.alert_delivery_error_count += 1
            self.last_alert_delivery_error = str(exc)
            print(
                f"\n[AlertDelivery] 上报失败，保留待重试 "
                f"event_id={event['event_id']}: {exc}"
            )
            return False

    def alert_dispatch_worker(self):
        print("[AlertDelivery] 告警上报线程已启动。")
        while self.is_running and self.alert_dispatch_running:
            if self.COMMAND_CENTER_WEBHOOK_URL:
                try:
                    ALERT_OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
                    for path in sorted(ALERT_OUTBOX_DIR.glob("*.json")):
                        if not self.is_running or not self.alert_dispatch_running:
                            break
                        self._deliver_alert_file(path)
                except Exception as exc:
                    self.alert_delivery_error_count += 1
                    self.last_alert_delivery_error = str(exc)
                    print(f"\n[AlertDelivery] 扫描待发送告警失败: {exc}")
                self.write_alert_state_json()
            self.alert_wakeup.wait(self.COMMAND_CENTER_RETRY_INTERVAL_S)
            self.alert_wakeup.clear()
        print("[AlertDelivery] 告警上报线程退出。")

    def start_alert_dispatch_thread(self):
        if self.alert_dispatch_thread is not None and self.alert_dispatch_thread.is_alive():
            return
        ALERT_OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
        self.write_alert_state_json()
        self.alert_dispatch_running = True
        self.alert_dispatch_thread = threading.Thread(
            target=self.alert_dispatch_worker,
            name="alert_dispatch_worker",
            daemon=True,
        )
        self.alert_dispatch_thread.start()

    def stop_alert_dispatch_thread(self):
        self.alert_dispatch_running = False
        self.alert_wakeup.set()
        thread = self.alert_dispatch_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        self.alert_dispatch_thread = None

    def vehicle_status_snapshot(self) -> List[dict]:
        now = time.time()
        with self.lock:
            states = list(self.vehicles.values())
        result = []
        for vehicle in sorted(states, key=lambda item: (item.link_name or "", item.system_id, item.component_id)):
            active_blocks = self.active_preflight_blocks_for_vehicle(vehicle, now)
            stable_armable_now = self.vehicle_armable_observed(vehicle, now)
            boot_reference = vehicle.estimated_boot_time or vehicle.online_since
            result.append({
                "key": vehicle.key,
                "link_name": vehicle.link_name,
                "configured_vehicle_id": vehicle.configured_vehicle_id,
                "configured_system_id": vehicle.configured_system_id,
                "system_id": vehicle.system_id,
                "component_id": vehicle.component_id,
                "session_id": vehicle.session_id,
                "online": vehicle.online,
                "heartbeat_seen": vehicle.heartbeat_seen,
                "heartbeat_age_s": round(now - vehicle.last_heartbeat, 2),
                "online_duration_s": round(now - vehicle.online_since, 2) if vehicle.online else None,
                "estimated_boot_time": (
                    None if vehicle.estimated_boot_time is None
                    else dt.datetime.fromtimestamp(vehicle.estimated_boot_time).isoformat(timespec="milliseconds")
                ),
                "time_boot_to_heartbeat_s": (
                    None if vehicle.first_heartbeat_time is None
                    else round(vehicle.first_heartbeat_time - boot_reference, 3)
                ),
                "time_boot_to_can_ready_s": (
                    None if vehicle.first_can_ready_time is None
                    else round(vehicle.first_can_ready_time - boot_reference, 3)
                ),
                "time_boot_to_first_inject_s": (
                    None if vehicle.first_inject_time is None
                    else round(vehicle.first_inject_time - boot_reference, 3)
                ),
                "time_boot_to_first_global_position_s": (
                    None if vehicle.first_global_position_time is None
                    else round(vehicle.first_global_position_time - boot_reference, 3)
                ),
                "fix_type": vehicle.fix_type,
                "fix_name": self.fix_type_name(vehicle.fix_type),
                "satellites": vehicle.satellites,
                "gps_raw_count": vehicle.gps_raw_count,
                "eph_cm": vehicle.eph_cm,
                "epv_cm": vehicle.epv_cm,
                "gps_lat": (
                    None if vehicle.gps_lat_raw is None
                    else round(vehicle.gps_lat_raw / 1e7, 7)
                ),
                "gps_lon": (
                    None if vehicle.gps_lon_raw is None
                    else round(vehicle.gps_lon_raw / 1e7, 7)
                ),
                "gps_alt_m": (
                    None if vehicle.gps_alt_mm is None
                    else round(vehicle.gps_alt_mm / 1000.0, 3)
                ),
                "gps_vel_m_s": (
                    None if vehicle.gps_vel_cms is None
                    else round(vehicle.gps_vel_cms / 100.0, 2)
                ),
                "gps_cog_deg": (
                    None if vehicle.gps_cog_cdeg is None or vehicle.gps_cog_cdeg >= 36000
                    else round(vehicle.gps_cog_cdeg / 100.0, 2)
                ),
                "gps_h_acc_m": (
                    None if vehicle.gps_h_acc_mm is None
                    else round(vehicle.gps_h_acc_mm / 1000.0, 3)
                ),
                "gps_v_acc_m": (
                    None if vehicle.gps_v_acc_mm is None
                    else round(vehicle.gps_v_acc_mm / 1000.0, 3)
                ),
                "gps_yaw_deg": (
                    None if vehicle.gps_yaw_cdeg is None or vehicle.gps_yaw_cdeg == 0
                    else round(vehicle.gps_yaw_cdeg / 100.0, 2)
                ),
                "raw_3d_fix_observed": bool(vehicle.first_raw_fix_time is not None),
                "raw_ttff_s": (
                    round(vehicle.first_raw_fix_time - vehicle.online_since, 3)
                    if vehicle.first_raw_fix_time is not None else None
                ),
                "time_boot_to_raw_3d_s": (
                    round(vehicle.first_raw_fix_time - boot_reference, 3)
                    if vehicle.first_raw_fix_time is not None else None
                ),
                "raw_fix_sats": vehicle.raw_fix_sats,
                "quality_fix_observed": bool(vehicle.first_fix_time is not None),
                "quality_fix_min_sats": self.MIN_QUALITY_FIX_SATS,
                "quality_fix_sats": vehicle.quality_fix_sats,
                "time_boot_to_quality_fix_s": (
                    round(vehicle.first_fix_time - boot_reference, 3)
                    if vehicle.first_fix_time is not None else None
                ),
                "estimator_status_count": vehicle.estimator_status_count,
                "estimator_flags": vehicle.estimator_flags,
                "estimator_pos_horiz_abs": (
                    None if vehicle.estimator_flags is None
                    else bool(vehicle.estimator_flags & 16)
                ),
                "estimator_pos_vert_abs": (
                    None if vehicle.estimator_flags is None
                    else bool(vehicle.estimator_flags & 32)
                ),
                "estimator_gps_glitch": (
                    None if vehicle.estimator_flags is None
                    else bool(vehicle.estimator_flags & 1024)
                ),
                "estimator_pos_horiz_ratio": vehicle.estimator_pos_horiz_ratio,
                "estimator_pos_vert_ratio": vehicle.estimator_pos_vert_ratio,
                "estimator_pos_horiz_accuracy_m": vehicle.estimator_pos_horiz_accuracy_m,
                "estimator_pos_vert_accuracy_m": vehicle.estimator_pos_vert_accuracy_m,
                "last_estimator_status_age_s": (
                    None if vehicle.last_estimator_status_time is None
                    else round(now - vehicle.last_estimator_status_time, 2)
                ),
                "local_position_count": vehicle.local_position_count,
                "last_local_position_age_s": (
                    None if vehicle.last_local_position_time is None
                    else round(now - vehicle.last_local_position_time, 2)
                ),
                "global_position_count": vehicle.global_position_count,
                "last_global_position_age_s": (
                    None if vehicle.last_global_position_time is None
                    else round(now - vehicle.last_global_position_time, 2)
                ),
                "sys_status_count": vehicle.sys_status_count,
                "last_sys_status_age_s": (
                    None if vehicle.last_sys_status_time is None
                    else round(now - vehicle.last_sys_status_time, 2)
                ),
                "sys_status_prearm_present": vehicle.sys_status_prearm_present,
                "sys_status_prearm_enabled": vehicle.sys_status_prearm_enabled,
                "sys_status_prearm_healthy": vehicle.sys_status_prearm_healthy,
                "first_prearm_healthy_s": (
                    None if vehicle.first_prearm_healthy_time is None
                    else round(vehicle.first_prearm_healthy_time - vehicle.online_since, 3)
                ),
                "ttff_s": (
                    round(vehicle.first_fix_time - vehicle.online_since, 3)
                    if vehicle.first_fix_time is not None else None
                ),
                "armed": bool(vehicle.armed),
                "actual_arm_observed": bool(vehicle.first_armed_time is not None),
                "time_to_actual_arm_s": (
                    round(vehicle.first_armed_time - vehicle.online_since, 3)
                    if vehicle.first_armed_time is not None else None
                ),
                "time_boot_to_actual_arm_s": (
                    round(vehicle.first_armed_time - boot_reference, 3)
                    if vehicle.first_armed_time is not None else None
                ),
                "first_armed_source": vehicle.first_armed_source,
                "last_armed_age_s": (
                    None if vehicle.last_armed_time is None
                    else round(now - vehicle.last_armed_time, 2)
                ),
                "last_disarmed_age_s": (
                    None if vehicle.last_disarmed_time is None
                    else round(now - vehicle.last_disarmed_time, 2)
                ),
                "stable_armable_observed": bool(vehicle.first_armable_time is not None),
                "stable_armable_now": bool(stable_armable_now),
                "time_to_stable_armable_s": (
                    round(vehicle.first_armable_time - vehicle.online_since, 3)
                    if vehicle.first_armable_time is not None else None
                ),
                "time_boot_to_stable_armable_s": (
                    round(vehicle.first_armable_time - boot_reference, 3)
                    if vehicle.first_armable_time is not None else None
                ),
                # Backward-compatible aliases for existing status consumers.
                "armable_observed": bool(vehicle.first_armable_time is not None),
                "time_to_armable_s": (
                    round(vehicle.first_armable_time - vehicle.online_since, 3)
                    if vehicle.first_armable_time is not None else None
                ),
                "almanac_sent_for_session": (
                    vehicle.almanac_sent_session_id == vehicle.session_id
                ),
                "last_preflight_block_age_s": (
                    None if vehicle.last_preflight_block_time is None
                    else round(now - vehicle.last_preflight_block_time, 2)
                ),
                "last_preflight_block_text": vehicle.last_preflight_block_text,
                "active_preflight_blocks": active_blocks,
                "last_statustext_age_s": (
                    None if vehicle.last_statustext_time is None
                    else round(now - vehicle.last_statustext_time, 2)
                ),
                "last_statustext_text": vehicle.last_statustext_text,
                "burst_rounds_remaining": vehicle.burst_rounds_remaining,
                "alert_level": vehicle.alert_level,
                "alert_count": vehicle.alert_count,
                "first_alert_time": vehicle.first_alert_time,
                "last_alert_time": vehicle.last_alert_time,
                "last_alert_event_id": vehicle.last_alert_event_id,
                "last_inject_age_s": (
                    None if vehicle.last_inject_time <= 0
                    else round(now - vehicle.last_inject_time, 2)
                ),
            })
        return result

    def datalink_status_snapshot(self) -> List[dict]:
        now = time.time()
        result = []
        for link in self.drone_links:
            result.append({
                "name": link.name,
                "vehicle_id": link.vehicle_id,
                "configured_system_id": link.system_id,
                "endpoint": link.endpoint,
                "bind_address": link.bind_address or None,
                "enabled": link.enabled,
                "target_gps_model": link.target_gps_model,
                "can_raw_ubx_validated": self.can_target_is_validated(link),
                "can_validation_source": (
                    "per_link"
                    if link.can_raw_ubx_validated is not None
                    else "legacy_global"
                ),
                "connected": bool(link.connected and link.master is not None),
                "last_open_age_s": (
                    None if link.last_open_time is None
                    else round(now - link.last_open_time, 2)
                ),
                "last_message_age_s": (
                    None if link.last_message_time is None
                    else round(now - link.last_message_time, 2)
                ),
                "last_heartbeat_age_s": (
                    None if link.last_heartbeat_time is None
                    else round(now - link.last_heartbeat_time, 2)
                ),
                "heartbeat_sent_count": link.heartbeat_sent_count,
                "inject_packet_count": link.inject_packet_count,
                "inject_byte_count": link.inject_byte_count,
                "inject_error_count": link.inject_error_count,
                "last_error": link.last_error,
                "rx_error": None if link.rx_error is None else str(link.rx_error),
            })
        return result

    def write_status_json(self):
        """写出轻量状态文件，供机巢/地面站轮询。使用临时文件原子替换，避免读取半截 JSON。"""
        try:
            with self.status_write_lock:
                self._write_status_json_unlocked()
        except Exception as exc:
            print(f"\n[StatusJSON] 写入失败: {exc}")

    def _write_status_json_unlocked(self):
        try:
            now = time.time()
            with self.lock:
                eph_count = len(self.assist_to_send)
                fresh_records = {
                    key: record for key, record in self.assist_to_send.items()
                    if self.record_is_fresh(record, now)
                    and self.constellation_enabled(record)
                }
                assist_keys = sorted(fresh_records.keys())[:50]
                fresh_aux_records = {
                    key: record for key, record in self.aux_assist_to_send.items()
                    if self.record_is_fresh(record, now)
                    and self.constellation_enabled(record)
                }
                aux_keys = sorted(fresh_aux_records.keys())[:50]

            constellation_coverage = {}
            for constellation in ("GPS", "QZSS", "GAL", "BDS", "GLO"):
                constellation_coverage[constellation] = {
                    "enabled": constellation in self.ENABLED_ASSIST_CONSTELLATIONS
                    and (
                        constellation != "GLO"
                        or self.ENABLE_GLO_MGA_EXPERIMENTAL
                    ),
                    "fresh_eph": sum(
                        1 for record in fresh_records.values()
                        if self.record_constellation(record) == constellation
                    ),
                    "fresh_aux": sum(
                        1 for record in fresh_aux_records.values()
                        if self.record_constellation(record) == constellation
                    ),
                    "injected_packets": self.inject_by_constellation.get(
                        constellation, 0
                    ),
                }

            vehicles = self.vehicle_status_snapshot()
            armable_vehicles = [
                item for item in vehicles
                if item.get("online") and item.get("stable_armable_now")
            ]
            actually_armed_vehicles = [
                item for item in vehicles
                if item.get("online") and item.get("actual_arm_observed")
            ]
            active_preflight_blocks = sorted({
                block for item in vehicles
                for block in item.get("active_preflight_blocks", [])
            })
            status = {
                "version": SCRIPT_VERSION,
                "time": dt.datetime.now().isoformat(timespec="seconds"),
                "state": self.engineering_state,
                "uptime_s": round(now - self.script_start_time, 1),
                "link_mode": self.LINK_MODE,
                "datalink_endpoint": self.DRONE_PORT,
                "multi_link_enabled": self.multi_link_enabled(),
                "datalinks": self.datalink_status_snapshot(),
                "target_gps_link_type": self.TARGET_GPS_LINK_TYPE,
                "default_target_gps_model": self.TARGET_GPS_MODEL,
                "can_assist": self.can_status_snapshot(),

                "base_pvt_ok": self.base_pvt_is_fresh(now),
                "base_sfrbx_ok": self.base_sfrbx_is_fresh(now),
                "base_pvt": self.formatted_pvt,
                "base_last_rx_age_s": None if self.base_last_rx_time is None else round(now - self.base_last_rx_time, 2),
                "base_pvt_packet_last_age_s": None if self.base_pvt_packet_last_time is None else round(now - self.base_pvt_packet_last_time, 2),
                "base_pvt_last_age_s": None if self.base_pvt_last_time is None else round(now - self.base_pvt_last_time, 2),
                "base_sfrbx_last_age_s": None if self.base_sfrbx_last_time is None else round(now - self.base_sfrbx_last_time, 2),
                "base_ubx_ok_count": self.base_ubx_ok_count,
                "base_ubx_crc_fail_count": self.base_ubx_crc_fail_count,

                "assist_eph_count": eph_count,
                "assist_fresh_eph_count": len(fresh_records),
                "assist_keys_preview": assist_keys,
                "assist_aux_count": len(self.aux_assist_to_send),
                "assist_fresh_aux_count": len(fresh_aux_records),
                "assist_aux_keys_preview": aux_keys,
                "assist_aux_by_kind": {
                    kind: sum(
                        1 for record in fresh_aux_records.values()
                        if record.kind == kind
                    )
                    for kind in ("alm", "iono", "utc", "health")
                },
                "enabled_assist_constellations": list(
                    self.ENABLED_ASSIST_CONSTELLATIONS
                ),
                "constellation_coverage": constellation_coverage,
                "semantic_validity_eph_count": sum(
                    1 for record in fresh_records.values()
                    if record.semantic_toe_epoch is not None
                ),
                "gps_almanac_fresh_count": sum(
                    1 for record in fresh_aux_records.values()
                    if record.kind == "alm"
                ),
                "gps_almanac_complete_32": sum(
                    1 for record in fresh_aux_records.values()
                    if record.kind == "alm"
                ) >= 32,
                "assist_db": self.assist_db_status_snapshot(now),
                "collector_stats": None if self.collector is None else {
                    "raw_sfrbx": self.collector.raw_sfrbx,
                    "gps_eph": self.collector.gps.total_eph,
                    "qzss_eph": self.collector.qzss.total_eph,
                    "gal_eph": self.collector.gal.total_eph,
                    "bds_eph": self.collector.bds.total_eph,
                    "glo_eph": self.collector.glo.total_eph,
                    "glo_seen": self.collector.glo_seen,
                    "glo_decode_fail": self.collector.glo.decode_failed,
                    "gps_alm": self.collector.gps_aux.total_alm,
                    "gps_iono": self.collector.gps_aux.total_iono,
                    "gps_utc": self.collector.gps_aux.total_utc,
                    "gps_health": self.collector.gps_aux.total_health,
                    "gps_page18": self.collector.gps_aux.page18_count,
                    "gps_page25_sf4": self.collector.gps_aux.page25_sf4_count,
                    "gps_page25_sf5": self.collector.gps_aux.page25_sf5_count,
                },

                "px4_detected": bool(self.px4_detected),
                "mav_last_msg_age_s": None if self.mav_last_msg_time is None else round(now - self.mav_last_msg_time, 2),
                "mav_message_counts": dict(self.mav_message_counts),
                "mav_sources": dict(self.mav_source_stats),
                "heartbeat_reject_counts": dict(self.heartbeat_reject_counts),
                "vehicle_count": len(vehicles),
                "online_vehicle_count": sum(1 for item in vehicles if item["online"]),
                "unfixed_online_vehicle_count": sum(
                    1 for item in vehicles
                    if item["online"] and not item["quality_fix_observed"]
                ),
                "armable_observed_vehicle_count": len(armable_vehicles),
                "actual_arm_observed_vehicle_count": len(actually_armed_vehicles),
                "stable_armable_observed_vehicle_count": len(armable_vehicles),
                "quality_fix_min_sats": self.MIN_QUALITY_FIX_SATS,
                "active_preflight_blocks": active_preflight_blocks,
                "vehicles": vehicles,
                "active_fix_timeout_alert_count": sum(
                    1 for item in vehicles
                    if item["online"] and item["alert_level"] != "NONE"
                ),
                "pending_command_center_alert_count": self.pending_alert_count(),
                "command_center_configured": bool(self.COMMAND_CENTER_WEBHOOK_URL),
                "alert_generated_count": self.alert_generated_count,
                "alert_delivered_count": self.alert_delivered_count,
                "alert_delivery_error_count": self.alert_delivery_error_count,
                "last_alert_delivery_error": self.last_alert_delivery_error,
                "gps_fix_type": self.drone_fix_type,
                "gps_fix_name": self.fix_type_name(self.drone_fix_type),
                "gps_sats": self.drone_sats,
                "gps_raw_count": self.gps_raw_count,

                "inject_packet_count": self.inject_packet_count,
                "inject_byte_count": self.inject_byte_count,
                "inject_error_count": self.inject_error_count,
                "inject_round_count": self.inject_round_count,

                "first_inject_time": self.first_inject_time,
                "first_3d_fix_time": self.first_3d_fix_time,
                "ttff_done": bool(self.ttff_done),
                "ack_monitor_enabled": bool(self.ACK_MONITOR_ENABLED),
                "ack_flow_control_mode": self.ACK_FLOW_CONTROL_MODE,
                "ack_target_link_name": self.ACK_TARGET_LINK_NAME or None,
                "ack_monitor_connected": False if self.ack_monitor is None else self.ack_monitor.connected,
                "ack_ok": None if self.ack_monitor is None else self.ack_monitor.ack_ok,
                "ack_total": None if self.ack_monitor is None else self.ack_monitor.ack_total,
                "ack_wait_count": self.ack_wait_count,
                "ack_accept_count": self.ack_accept_count,
                "ack_reject_count": self.ack_reject_count,
                "ack_timeout_count": self.ack_timeout_count,
                "ack_retry_count": self.ack_retry_count,
                "ack_flow_suspended": now < self.ack_flow_suspended_until,
                "adaptive_inject_delay_s": round(self.current_inject_delay_s, 4),
                "target_nav_sat_seen": (
                    0 if self.ack_monitor is None
                    else len(self.ack_monitor.latest_seen_keys)
                ),
                "target_nav_sat_used": (
                    0 if self.ack_monitor is None
                    else len(self.ack_monitor.latest_used_keys)
                ),
            }
            target = Path(self.STATUS_JSON)
            tmp = target.with_name(
                f"{target.name}.tmp.{os.getpid()}.{threading.get_ident()}"
            )
            tmp.write_text(json.dumps(status, ensure_ascii=True, indent=2), encoding="utf-8")
            for attempt in range(3):
                try:
                    tmp.replace(target)
                    break
                except PermissionError:
                    if attempt >= 2:
                        raise
                    time.sleep(0.05)
        except Exception as exc:
            print(f"\n[StatusJSON] 写入失败: {exc}")

    def start_ttff_timer(self, reason="用户确认无人机 GPS 上电"):
        """
        手动设置 TTFF 计时起点。
        建议在用户按回车并给无人机/GPS上电时调用。
        """
        self.gps_power_on_time = time.time()
        self.ttff_start_reason_text = reason
        self.px4_connected_time = None
        self.first_gps_msg_time = None
        self.first_inject_time = None
        self.first_3d_fix_time = None
        self.first_gps_msg_fix_type = None
        self.first_gps_msg_sats = None
        self.ttff_done = False
        self.experiment_csv_written = False

        self.last_reported_sats = -1
        self.last_reported_fix = -1
        self.drone_sats = 0
        self.drone_fix_type = 0

        print(
            f"\n[TTFF] 开始计时：{reason} | "
            f"T0 = 当前电脑时间"
        )

    def elapsed_from_power_on(self, t: Optional[float]) -> Optional[float]:
        if self.gps_power_on_time is None or t is None:
            return None
        return t - self.gps_power_on_time

    def ttff_elapsed_text(self) -> str:
        if self.gps_power_on_time is None:
            return "TTFF: 未开始"

        end_time = self.first_3d_fix_time if self.first_3d_fix_time else time.time()
        elapsed = end_time - self.gps_power_on_time

        if self.first_3d_fix_time:
            return f"T0到3D Fix: {elapsed:.1f}s"
        else:
            return f"T0计时中: {elapsed:.1f}s"

    def print_timing_summary(self):
        """
        打印多阶段计时结果。
        """
        print("\n========== TTFF 计时摘要 ==========")

        if self.gps_power_on_time is None:
            print("[Time] T0 未设置。")
            if self.ack_monitor is not None:
                self.ack_monitor.summary()
            print("==================================\n")
            return

        print(f"[Time] T0: {self.ttff_start_reason_text}")

        t_px4 = self.elapsed_from_power_on(self.px4_connected_time)
        if t_px4 is not None:
            print(f"[Time] T1: T0 → PX4 heartbeat连接 = {t_px4:.1f} 秒")
        else:
            print("[Time] T1: 尚未连接 PX4")

        t_gps = self.elapsed_from_power_on(self.first_gps_msg_time)
        if t_gps is not None:
            print(
                f"[Time] T2: T0 → 首次收到 GPS_RAW_INT = {t_gps:.1f} 秒 "
                f"(首次状态: fix={self.first_gps_msg_fix_type}, sats={self.first_gps_msg_sats})"
            )
        else:
            print("[Time] T2: 尚未收到 GPS_RAW_INT")

        t_inj = self.elapsed_from_power_on(self.first_inject_time)
        if t_inj is not None:
            print(f"[Time] T3: T0 → 首次 A-GNSS 注入开始 = {t_inj:.1f} 秒")
        else:
            print("[Time] T3: 尚未执行 A-GNSS 注入")

        t_eff = self.elapsed_from_power_on(self.first_effective_inject_time)
        if t_eff is not None:
            print(f"[Time] T0 → heartbeat后首次有效注入 = {t_eff:.1f} 秒")
        else:
            print("[Time] heartbeat后首次有效注入：尚未执行")

        t_fix = self.elapsed_from_power_on(self.first_3d_fix_time)
        if t_fix is not None:
            print(f"[Time] T4: T0 → 首次 3D Fix = {t_fix:.1f} 秒")
        else:
            print("[Time] T4: 尚未 3D Fix")

        if self.first_inject_time is not None and self.first_3d_fix_time is not None:
            dt_inj_fix = self.first_3d_fix_time - self.first_inject_time
            print(f"[Time] 首次A-GNSS注入 → 3D Fix = {dt_inj_fix:.1f} 秒")

        if self.first_effective_inject_time is not None and self.first_3d_fix_time is not None:
            dt_eff_fix = self.first_3d_fix_time - self.first_effective_inject_time
            print(f"[Time] heartbeat后首次有效注入 → 3D Fix = {dt_eff_fix:.1f} 秒")

        if self.first_gps_msg_time is not None and self.first_3d_fix_time is not None:
            dt_gps_fix = self.first_3d_fix_time - self.first_gps_msg_time
            print(f"[Time] 首次GPS状态 → 3D Fix = {dt_gps_fix:.1f} 秒")

            if self.first_gps_msg_fix_type is not None and self.first_gps_msg_fix_type >= 3:
                print(
                    "[Warn] 第一次收到 GPS_RAW_INT 时已经是 3D Fix，"
                    "说明真实 GPS Fix 时间可能早于 T2，"
                    "脚本记录的 T4 是上限值。"
                )

        if self.ack_monitor is not None:
            self.ack_monitor.summary()

        print("==================================\n")

    def request_gps_raw_int_rate(self, hz=5):
        """
        尝试请求 PX4 以更高频率发送 GPS_RAW_INT。
        这可以减少“GPS已经fix但PC端还没收到”的滞后。
        """
        if not self.master:
            return

        try:
            msg_id = getattr(mavutil.mavlink, "MAVLINK_MSG_ID_GPS_RAW_INT", 24)
            cmd_id = getattr(mavutil.mavlink, "MAV_CMD_SET_MESSAGE_INTERVAL", 511)
            interval_us = int(1_000_000 / hz)

            with self.mav_send_lock:
                self.master.mav.command_long_send(
                    self.master.target_system,
                    self.master.target_component,
                    cmd_id,
                    0,
                    msg_id,
                    interval_us,
                    0,
                    0,
                    0,
                    0,
                    0
                )

            print(f"[MAVLink] 已请求 GPS_RAW_INT 输出频率约 {hz} Hz")

        except Exception as e:
            print(f"[MAVLink] 请求 GPS_RAW_INT 频率失败: {e}")


    def request_basic_streams(self):
        """兼容旧式 MAVLink stream 请求，降低 GPS_RAW_INT 不输出的概率。"""
        if not self.master:
            return
        try:
            with self.mav_send_lock:
                self.master.mav.request_data_stream_send(
                    self.master.target_system,
                    self.master.target_component,
                    mavutil.mavlink.MAV_DATA_STREAM_ALL,
                    5,
                    1
                )
            print("[MAVLink] 已请求 MAV_DATA_STREAM_ALL 5Hz")
        except Exception as e:
            print(f"[MAVLink] 请求 MAV_DATA_STREAM_ALL 失败: {e}")

    def print_px4_gps_checklist_once(self):
        """PX4 上线后打印一次工程检查清单，便于现场排查 GPS_RAW_INT=0。"""
        if self.px4_checklist_printed:
            return
        self.px4_checklist_printed = True
        if self.target_gps_is_can():
            print(
                "\n[CheckList] PX4已上线，请确认："
                "\n  1) 飞控 TELEM 口 MAVLink 参数正确，数据链串口波特率一致；"
                "\n  2) NEO 3 Pro 已连接 DroneCAN、上电且天线无遮挡；"
                "\n  3) UAVCAN_ENABLE/UAVCAN_SUB_GPS/UAVCAN_PUB_RTCM 均已设置并重启飞控；"
                "\n  4) 服务状态中目标型号和 raw_ubx 验证结果与当前实物一致；"
                "\n  5) 若10秒后 GPS_RAW_INT 仍为0，请检查 uavcan status、节点ID和CAN终端电阻。"
            )
            return
        print(
            "\n[CheckList] PX4已上线，请确认："
            "\n  1) 飞控 TELEM 口 MAVLink 参数正确，数据链串口波特率一致；"
            "\n  2) GPS_1_CONFIG / SER_GPS1_BAUD 指向 NEO3 所在端口；"
            "\n  3) NEO3 已上电，天线无遮挡；"
            "\n  4) PX4 gps.cpp 已允许 UBX-MGA 通过 gps_inject_data 旁路写入 GPS UART；"
            "\n  5) 若10秒后 GPS_RAW_INT 仍为0，请优先查 PX4 GPS 驱动和 MAVLink stream。"
        )

    def print_tcppin_hint_once(self):
        """tcpin 模式下长时间无 heartbeat 时，给出 TCP 连接层排查提示。"""
        if self.tcpin_hint_printed:
            return
        if self.LINK_MODE != "tcp" or not str(self.DRONE_TCP).startswith("tcpin:"):
            return
        self.tcpin_hint_printed = True
        port = str(self.DRONE_TCP).split(":")[-1]
        print(
            f"\n[DataLinkCheck] 当前为 tcpin 监听模式，若长时间无 PX4 heartbeat，请在另一个 CMD 执行："
            f"\n  netstat -ano | findstr :{port}"
            f"\n[DataLinkCheck] 期望看到 LISTENING 以及 ESTABLISHED。若只有 LISTENING，说明数据链尚未连入工程电脑。"
        )

    def get_injected_assist_keys(self) -> Set[str]:
        now = time.time()
        with self.lock:
            return {
                key for key, record in self.assist_to_send.items()
                if self.record_is_fresh(record, now)
                and self.constellation_enabled(record)
            }

    def start_ack_monitor_if_enabled(self):
        if not self.ACK_MONITOR_ENABLED:
            print("[ACK] 目标GPS ACK/NAV-SAT监听未启用。如需启用：ACK_MONITOR_ENABLED=True，并连接目标GPS TX到USB-TTL RX。")
            return
        self.ack_monitor = TargetGpsAckSatMonitor(
            self.ACK_PORT,
            self.ACK_BAUD,
            self.get_injected_assist_keys
        )
        self.ack_monitor.start()

    def enable_target_ack_and_navsat_via_px4(
        self,
        link: Optional[DroneLink] = None,
    ):
        """
        尝试经PX4注入链路打开目标GPS的：
        1. CFG-NAVSPG-ACKAIDING：输出 UBX-MGA-ACK；
        2. CFG-MSGOUT-UBX_NAV_SAT_UART1：输出 UBX-NAV-SAT。

        UART 路径要求 PX4 gps.cpp 允许 UBX-CFG-VALSET；CAN 路径要求目标节点
        已验证可透传配置消息。当前 NEO 3 Pro CAN 配置默认关闭此功能。
        """
        if not self.ENABLE_TARGET_CFG_VIA_PX4:
            return
        if self.target_cfg_sent:
            return

        if self.ACK_TARGET_LINK_NAME:
            if link is None and self.multi_link_enabled():
                return
            if link is not None and link.name != self.ACK_TARGET_LINK_NAME:
                return

        def send_config(message: bytes) -> bool:
            if link is not None:
                return self.inject_ubx_to_link(link, message)
            return self.inject_ubx_to_px4(message)

        print("\n[CFG] 尝试通过PX4给目标GPS打开 ACKAIDING 和 NAV-SAT 输出...")
        try:
            if not send_config(build_cfg_ack_aiding(True)):
                raise RuntimeError("CFG-NAVSPG-ACKAIDING 发送失败")
            print("[CFG] 已发送 CFG-NAVSPG-ACKAIDING=true")
            time.sleep(0.2)
            if not send_config(build_cfg_navsat_uart1(True)):
                raise RuntimeError("CFG-MSGOUT-UBX_NAV_SAT_UART1 发送失败")
            print("[CFG] 已发送 CFG-MSGOUT-UBX_NAV_SAT_UART1=1")
            time.sleep(0.2)
            self.target_cfg_sent = True
        except Exception as e:
            print(f"[CFG] 目标GPS配置发送失败: {e}")

    def decode_base_nav_sat(self, payload: bytes):
        """
        解析基准站 UBX-NAV-SAT，用于获取每颗卫星的 C/N0、仰角和是否被基准站用于解算。
        后续注入星历时优先发送基准站当前 used 且 C/N0 高的卫星。
        """
        if len(payload) < 8:
            return

        num_svs = payload[5]
        off = 8
        now = time.time()
        updated = 0

        with self.lock:
            for _ in range(num_svs):
                if off + 12 > len(payload):
                    break

                gnss_id = payload[off]
                sv_id = payload[off + 1]
                cno = payload[off + 2]
                elev = struct.unpack_from("<b", payload, off + 3)[0]
                flags = struct.unpack_from("<I", payload, off + 8)[0]
                used = bool(flags & 0x08)  # bit 3 = svUsed
                key = gnss_key(gnss_id, sv_id)

                self.base_sat_info[key] = {
                    "gnss_id": gnss_id,
                    "sv_id": sv_id,
                    "cno": cno,
                    "elev": elev,
                    "used": 1.0 if used else 0.0,
                    "updated_at": now,
                }
                updated += 1
                off += 12

        if (
            updated > 0
            and self.BASE_SAT_LOG_ENABLED
            and now - self.last_base_sat_log_time >= self.BASE_SAT_LOG_INTERVAL_S
        ):
            used_count = sum(1 for v in self.base_sat_info.values() if v.get("used"))
            print(f"\n[BaseSat] 已更新基准站 NAV-SAT: {updated}颗，本地缓存{len(self.base_sat_info)}颗，used={used_count}颗")
            self.last_base_sat_log_time = now

    def base_pvt_is_fresh(self, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        return (
            self.last_pvt is not None
            and self.base_pvt_last_time is not None
            and now - self.base_pvt_last_time <= self.BASE_PVT_MAX_AGE_S
        )

    def base_sfrbx_is_fresh(self, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        return (
            self.base_sfrbx_last_time is not None
            and now - self.base_sfrbx_last_time <= self.BASE_SFRBX_MAX_AGE_S
        )

    def record_is_fresh(self, record: AssistRecord, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        age = now - record.created_at
        if record.kind == "alm":
            max_age_s = self.MAX_ALM_AGE_S
        elif record.kind == "eph":
            max_age_s = self.MAX_EPH_AGE_S
        else:
            max_age_s = self.MAX_AUX_AGE_S
        if not 0.0 <= age <= max_age_s:
            return False
        if record.kind == "eph" and record.semantic_toe_epoch is not None:
            if abs(now - record.semantic_toe_epoch) > self.EPH_SEMANTIC_HALF_WINDOW_S:
                return False
        return True

    @staticmethod
    def record_constellation(record: AssistRecord) -> str:
        return (record.constellation or record.key.split("-", 1)[0]).upper()

    def constellation_enabled(self, record: AssistRecord) -> bool:
        constellation = self.record_constellation(record)
        if constellation == "GLO" and not self.ENABLE_GLO_MGA_EXPERIMENTAL:
            return False
        return constellation in set(self.ENABLED_ASSIST_CONSTELLATIONS)

    def fresh_assist_count(self) -> int:
        now = time.time()
        with self.lock:
            return sum(
                1 for record in self.assist_to_send.values()
                if self.record_is_fresh(record, now)
                and self.constellation_enabled(record)
            )

    def prune_stale_assist(self, force: bool = False, save_db: bool = True) -> int:
        """删除过期 EPH 和长期辅助记录，避免数据库长期虚高。"""
        now = time.time()
        if not force and now - self.last_eph_cleanup_time < self.EPH_CLEANUP_INTERVAL_S:
            return 0

        with self.lock:
            stale_keys = [
                key for key, record in self.assist_to_send.items()
                if not self.record_is_fresh(record, now)
            ]
            for key in stale_keys:
                self.assist_to_send.pop(key, None)
                self.assist_sent_iode.pop(key, None)

            stale_aux_keys = [
                key for key, record in self.aux_assist_to_send.items()
                if not self.record_is_fresh(record, now)
            ]
            for key in stale_aux_keys:
                self.aux_assist_to_send.pop(key, None)
                self.assist_sent_iode.pop(key, None)

            stale_sat_keys = [
                key for key, info in self.base_sat_info.items()
                if now - info.get("updated_at", 0.0) > 600.0
            ]
            for key in stale_sat_keys:
                self.base_sat_info.pop(key, None)

        self.last_eph_cleanup_time = now
        if stale_keys or stale_aux_keys:
            self.assist_db_dirty = True
            print(
                f"\n[Cache] 已删除过期辅助记录: "
                f"EPH={len(stale_keys)}, AUX={len(stale_aux_keys)}"
            )
            if save_db:
                self.save_assist_db(force=False)
        return len(stale_keys) + len(stale_aux_keys)

    def assist_priority_score(self, key: str, record: AssistRecord) -> float:
        """
        注入排序分数：
        1. 基准站当前用于解算的卫星优先；
        2. C/N0 高的卫星优先；
        3. 仰角较高的卫星优先；
        4. 星历越新越优先。
        """
        now = time.time()
        age = now - record.created_at
        constellation = self.record_constellation(record)
        score = -age / 60.0 + self.CONSTELLATION_PRIORITY.get(constellation, 0.0)

        monitor = self.ack_monitor
        if (
            monitor is not None
            and monitor.nav_sat_last_time is not None
            and now - monitor.nav_sat_last_time <= 120.0
        ):
            if key in monitor.latest_used_keys:
                score += 20000.0
            elif key in monitor.latest_seen_keys:
                score += 5000.0

        info = self.base_sat_info.get(key)
        if info and now - info.get("updated_at", 0) <= 120:
            if info.get("used", 0):
                score += 10000
            score += float(info.get("cno", 0)) * 100
            score += float(info.get("elev", 0))
        else:
            score += self.CONSTELLATION_PRIORITY.get(constellation, 0.0) * 10.0

        return score

    def get_sorted_fresh_assist_items(self) -> List[Tuple[str, AssistRecord]]:
        """
        获取“新鲜且按优先级排序”的辅助星历。
        过旧星历不注入；新鲜星历按基准站可见性/CN0/used状态排序。
        """
        now = time.time()
        with self.lock:
            items = list(self.assist_to_send.items())

        fresh = [
            item for item in items
            if self.record_is_fresh(item[1], now)
            and self.constellation_enabled(item[1])
        ]
        stale_count = len(items) - len(fresh)
        if stale_count > 0:
            print(f"\n[Inject] 已过滤过旧星历 {stale_count} 条，MAX_EPH_AGE_S={self.MAX_EPH_AGE_S}s")

        fresh.sort(key=lambda item: self.assist_priority_score(item[0], item[1]), reverse=True)
        return fresh

    def get_fresh_aux_assist_items(
        self,
        include_almanac: bool = False,
    ) -> List[Tuple[str, AssistRecord]]:
        """返回 GPS Iono/UTC/Health；需要时在末尾追加 Almanac。"""
        now = time.time()
        with self.lock:
            items = list(self.aux_assist_to_send.items())
            base_items = dict(self.base_sat_info)
        items = [
            item for item in items
            if self.record_is_fresh(item[1], now)
            and self.constellation_enabled(item[1])
        ]
        kind_order = {"utc": 0, "iono": 1, "health": 2, "alm": 3}

        def score(item):
            key, record = item
            if record.kind == "alm":
                sv_key = key.replace("GPS-ALM-", "GPS-")
                info = base_items.get(sv_key, {})
                fresh_info = now - info.get("updated_at", 0.0) <= 120.0
                used = 1 if fresh_info and info.get("used", 0) else 0
                cno = float(info.get("cno", 0)) if fresh_info else 0.0
                return (kind_order["alm"], -used, -cno, key)
            return (kind_order.get(record.kind, 9), 0, 0.0, key)

        items.sort(key=score)
        global_items = [item for item in items if item[1].kind != "alm"]
        if not include_almanac:
            return global_items
        almanac_items = [item for item in items if item[1].kind == "alm"]
        return global_items + almanac_items[:self.MAX_ALMANAC_PER_ROUND]

    def fresh_almanac_count(self) -> int:
        now = time.time()
        with self.lock:
            return sum(
                1 for record in self.aux_assist_to_send.values()
                if record.kind == "alm"
                and self.record_is_fresh(record, now)
                and self.constellation_enabled(record)
            )

    def almanac_due_for_round(
        self,
        fast_round: bool,
        force_almanac: bool = False,
    ) -> bool:
        if not self.ENABLE_ALM_IONO_UTC_EXPERIMENTAL:
            return False
        if self.fresh_almanac_count() <= 0:
            return False
        if force_almanac:
            return True
        # 飞机未上线时不发送 Almanac，也不占用新 session 的首发机会。
        if not self.online_vehicles():
            return False
        if fast_round and not self.INJECT_ALMANAC_ON_FIRST_FIX:
            return False
        if time.time() - self.last_almanac_broadcast_time < self.ALMANAC_REINJECT_INTERVAL_S:
            return False
        return True

    def get_aux_items_for_round(
        self,
        fast_round: bool,
        force_almanac: bool = False,
    ) -> Tuple[List[Tuple[str, AssistRecord]], List[Tuple[str, AssistRecord]]]:
        include_global = not fast_round or self.INJECT_AUX_IN_FAST_ROUNDS
        include_almanac = self.almanac_due_for_round(
            fast_round,
            force_almanac=force_almanac,
        )
        if not include_global and not include_almanac:
            return [], []
        items = self.get_fresh_aux_assist_items(include_almanac=include_almanac)
        global_items = [item for item in items if item[1].kind != "alm"]
        almanac_items = [item for item in items if item[1].kind == "alm"]
        if not include_global:
            global_items = []
        return global_items, almanac_items

    def count_base_used_injectable(self) -> Tuple[int, List[str]]:
        """
        统计“基准站当前用于解算 used=True，且本脚本已经生成可注入星历”的卫星。
        这个数量比单纯 EPH 总数更能代表目标GPS上电后可立即利用的辅助价值。
        """
        now = time.time()
        with self.lock:
            assist_keys = {
                key for key, record in self.assist_to_send.items()
                if self.record_is_fresh(record, now)
            }
            base_items = dict(self.base_sat_info)

        used_injectable = []
        for key in assist_keys:
            info = base_items.get(key)
            if not info:
                continue
            if now - info.get("updated_at", 0) > 120:
                continue
            if info.get("used", 0) > 0:
                used_injectable.append(key)

        return len(used_injectable), sorted(used_injectable)

    def assist_db_path(self) -> Path:
        path = Path(str(self.ASSIST_DB_PATH))
        if not path.is_absolute():
            path = APP_DIR / path
        return path

    def assist_record_to_cache_item(self, record: AssistRecord) -> dict:
        return {
            "key": record.key,
            "label": record.label,
            "iode": int(record.iode),
            "created_at": float(record.created_at),
            "kind": record.kind,
            "constellation": record.constellation,
            "sv_id": record.sv_id,
            "source": record.source,
            "toe_s": record.toe_s,
            "toc_s": record.toc_s,
            "semantic_toe_epoch": record.semantic_toe_epoch,
            "validity_basis": record.validity_basis,
            "ubx_hex": record.ubx.hex(),
        }

    def assist_record_from_cache_item(self, item: dict) -> Optional[AssistRecord]:
        try:
            key = str(item["key"])
            label = str(item.get("label") or key)
            iode = int(item["iode"])
            created_at = float(item["created_at"])
            kind = str(item.get("kind") or "eph").lower()
            ubx = bytes.fromhex(str(item["ubx_hex"]))
            constellation = str(
                item.get("constellation") or key.split("-", 1)[0]
            ).upper()
            sv_id = None if item.get("sv_id") is None else int(item["sv_id"])
            toe_s = None if item.get("toe_s") is None else float(item["toe_s"])
            toc_s = None if item.get("toc_s") is None else float(item["toc_s"])
            semantic_toe_epoch = (
                None
                if item.get("semantic_toe_epoch") is None
                else float(item["semantic_toe_epoch"])
            )
        except (KeyError, TypeError, ValueError):
            return None
        if kind not in {"eph", "alm", "iono", "utc", "health"}:
            return None
        if not key or not ubx or len(ubx) > 180 or not verify_ubx(ubx):
            return None
        return AssistRecord(
            key=key,
            label=label,
            iode=iode,
            ubx=ubx,
            created_at=created_at,
            kind=kind,
            constellation=constellation,
            sv_id=sv_id,
            source=str(item.get("source") or "legacy_cache"),
            toe_s=toe_s,
            toc_s=toc_s,
            semantic_toe_epoch=semantic_toe_epoch,
            validity_basis=str(item.get("validity_basis") or "reception_age"),
        )

    def load_assist_db(self):
        if not self.ASSIST_DB_ENABLED:
            return
        path = self.assist_db_path()
        if not path.exists():
            return
        loaded = 0
        stale = 0
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            records = data.get("records", []) if isinstance(data, dict) else []
            now = time.time()
            restored_eph: Dict[str, AssistRecord] = {}
            restored_aux: Dict[str, AssistRecord] = {}
            for item in records:
                if not isinstance(item, dict):
                    continue
                record = self.assist_record_from_cache_item(item)
                if record is None:
                    continue
                loaded += 1
                if self.record_is_fresh(record, now):
                    target = restored_eph if record.kind == "eph" else restored_aux
                    current = target.get(record.key)
                    if current is None or record.created_at >= current.created_at:
                        target[record.key] = record
                else:
                    stale += 1
            if restored_eph or restored_aux:
                with self.lock:
                    for key, record in restored_eph.items():
                        current = self.assist_to_send.get(key)
                        if current is None or record.created_at >= current.created_at:
                            self.assist_to_send[key] = record
                    for key, record in restored_aux.items():
                        current = self.aux_assist_to_send.get(key)
                        if current is None or record.created_at >= current.created_at:
                            self.aux_assist_to_send[key] = record
                    self.assist_update_pending = True
                    self.last_assist_update_time = now
            self.assist_db_loaded_count = loaded
            self.assist_db_loaded_fresh_count = len(restored_eph) + len(restored_aux)
            self.assist_db_stale_count = stale
            self.assist_db_last_load_time = now
            self.assist_db_load_error = None
            if restored_eph or restored_aux:
                print(
                    f"[AssistDB] 已恢复本地辅助数据库: "
                    f"EPH={len(restored_eph)}, AUX={len(restored_aux)}, "
                    f"loaded={loaded}, stale={stale}, path={path}"
                )
            elif loaded or stale:
                print(
                    f"[AssistDB] 本地辅助数据库无可用新鲜记录: "
                    f"loaded={loaded}, stale={stale}, path={path}"
                )
        except Exception as exc:
            self.assist_db_load_error = str(exc)
            print(f"[AssistDB] 读取本地辅助数据库失败: {path} | {exc}")

    def save_assist_db(self, force: bool = False):
        if not self.ASSIST_DB_ENABLED:
            return
        now = time.time()
        if (
            not force
            and not self.assist_db_dirty
            and self.assist_db_last_save_time is not None
        ):
            return
        if (
            not force
            and self.assist_db_last_save_time is not None
            and now - self.assist_db_last_save_time < self.ASSIST_DB_SAVE_INTERVAL_S
        ):
            return
        self.prune_stale_assist(force=False, save_db=False)
        with self.lock:
            records = [
                self.assist_record_to_cache_item(record)
                for record in (
                    list(self.assist_to_send.values())
                    + list(self.aux_assist_to_send.values())
                )
                if self.record_is_fresh(record, now)
            ]
        records.sort(key=lambda item: item["key"])
        path = self.assist_db_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "schema": 3,
                "saved_at": dt.datetime.now().isoformat(timespec="seconds"),
                "saved_at_epoch": now,
                "max_eph_age_s": self.MAX_EPH_AGE_S,
                "max_alm_age_s": self.MAX_ALM_AGE_S,
                "max_aux_age_s": self.MAX_AUX_AGE_S,
                "record_count": len(records),
                "records": records,
            }
            tmp = path.with_name(
                f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
            )
            tmp.write_text(
                json.dumps(data, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )
            tmp.replace(path)
            self.assist_db_last_save_time = now
            self.assist_db_last_save_count = len(records)
            self.assist_db_save_error = None
            self.assist_db_dirty = False
        except Exception as exc:
            self.assist_db_save_error = str(exc)
            print(f"\n[AssistDB] 保存本地辅助数据库失败: {path} | {exc}")

    def assist_db_status_snapshot(self, now: Optional[float] = None) -> dict:
        now = time.time() if now is None else now
        with self.lock:
            fresh_eph_records = [
                record for record in self.assist_to_send.values()
                if self.record_is_fresh(record, now)
                and self.constellation_enabled(record)
            ]
            fresh_aux_records = [
                record for record in self.aux_assist_to_send.values()
                if self.record_is_fresh(record, now)
            ]
        fresh_records = fresh_eph_records + fresh_aux_records
        ages = [now - record.created_at for record in fresh_records]
        kind_counts = {
            kind: sum(1 for record in fresh_aux_records if record.kind == kind)
            for kind in ("alm", "iono", "utc", "health")
        }
        return {
            "enabled": bool(self.ASSIST_DB_ENABLED),
            "path": str(self.assist_db_path()),
            "fresh_record_count": len(fresh_records),
            "fresh_eph_count": len(fresh_eph_records),
            "fresh_aux_count": len(fresh_aux_records),
            "fresh_aux_by_kind": kind_counts,
            "almanac_complete_32": kind_counts["alm"] >= 32,
            "ready_min": len(fresh_eph_records) >= self.MIN_EPH_BEFORE_POWER_ON,
            "ready_desired": len(fresh_eph_records) >= self.DESIRED_EPH_BEFORE_POWER_ON,
            "oldest_fresh_age_s": None if not ages else round(max(ages), 1),
            "newest_fresh_age_s": None if not ages else round(min(ages), 1),
            "semantic_validity_eph_count": sum(
                1 for record in fresh_eph_records
                if record.semantic_toe_epoch is not None
            ),
            "loaded_count": self.assist_db_loaded_count,
            "loaded_fresh_count": self.assist_db_loaded_fresh_count,
            "stale_count": self.assist_db_stale_count,
            "last_load_age_s": (
                None if self.assist_db_last_load_time is None
                else round(now - self.assist_db_last_load_time, 1)
            ),
            "last_save_age_s": (
                None if self.assist_db_last_save_time is None
                else round(now - self.assist_db_last_save_time, 1)
            ),
            "last_save_count": self.assist_db_last_save_count,
            "dirty": bool(self.assist_db_dirty),
            "load_error": self.assist_db_load_error,
            "save_error": self.assist_db_save_error,
        }

    def get_first_fix_assist_items(self) -> List[Tuple[str, AssistRecord]]:
        """
        首次3D Fix前的Top-N星历选择。
        只选最可能马上被目标GPS使用的星历，减少第一轮有效注入总耗时。
        优先级：基准站used=True > C/N0高 > 仰角高 > GPS/QZSS/GAL基础权重 > 星历新鲜。
        """
        now = time.time()
        with self.lock:
            items = list(self.assist_to_send.items())
            base_items = dict(self.base_sat_info)

        fresh = [
            item for item in items
            if self.record_is_fresh(item[1], now)
            and self.constellation_enabled(item[1])
        ]

        def score(item):
            key, record = item
            info = base_items.get(key, {})
            info_is_fresh = (now - info.get("updated_at", 0) <= 120)
            used = 1.0 if info_is_fresh and info.get("used", 0) > 0 else 0.0
            cno = float(info.get("cno", 0)) if info_is_fresh else 0.0
            elev = float(info.get("elev", 0)) if info_is_fresh else 0.0
            age = now - record.created_at

            constellation = self.record_constellation(record)
            sys_bonus = self.CONSTELLATION_PRIORITY.get(constellation, 0.0)

            target_used = 0.0
            target_seen = 0.0
            monitor = self.ack_monitor
            if (
                monitor is not None
                and monitor.nav_sat_last_time is not None
                and now - monitor.nav_sat_last_time <= 120.0
            ):
                target_used = 1.0 if key in monitor.latest_used_keys else 0.0
                target_seen = 1.0 if key in monitor.latest_seen_keys else 0.0

            return (
                target_used * 20000.0
                + target_seen * 5000.0
                + used * 1000.0
                + cno * 5.0
                + max(elev, 0.0) * 2.0
                + sys_bonus
                - age / 60.0
            )

        fresh.sort(key=score, reverse=True)
        return fresh[:self.FIRST_FIX_TOP_N]

    def open_mavlink_port_early(self):
        """
        提前打开数传串口，不等无人机上电后再打开。
        这样用户按回车后可以立即盲发A-GNSS数据。
        """
        if self.master is not None:
            return

        print(f"\n[MAVLink] 提前打开数据链端口: mode={self.LINK_MODE}, endpoint={self.DRONE_PORT}")
        if self.master is None:
            self.master = self.create_mavlink_connection()
        print("[MAVLink] 数据链端口已打开，等待无人机/PX4上电。")

    def blind_inject_worker(self):
        """
        用户按回车给无人机上电后立即启动：不等待PX4 heartbeat，先周期性盲发。
        前几轮可能丢失，但PX4/数传一旦可用，下一轮可尽快被接收。
        """
        if not self.BLIND_INJECT_BEFORE_HEARTBEAT:
            return

        start = time.time()
        count = 0
        print(
            f"\n[BlindInject] 启动：不等待PX4 heartbeat，"
            f"每 {self.BLIND_INJECT_INTERVAL:.1f}s 盲发一次，"
            f"最长持续 {self.BLIND_INJECT_DURATION:.0f}s。"
        )

        while self.is_running and not self.blind_inject_stop and time.time() - start < self.BLIND_INJECT_DURATION:
            if not self.any_vehicle_needs_assist():
                break
            if self.first_blind_inject_time is None:
                self.first_blind_inject_time = time.time()

            self.send_gcs_heartbeat()
            self.inject_sequence()
            count += 1
            if self.BLIND_INJECT_VERBOSE:
                print(f"\r[BlindInject] 已盲发 {count} 轮", end="")
            time.sleep(self.BLIND_INJECT_INTERVAL)

        print("\n[BlindInject] 盲发阶段结束。")

    def effective_burst_after_heartbeat(self):
        """
        PX4 heartbeat 出现后，执行多轮快速“有效注入”。
        这几轮比上电前盲发更可靠，因为此时PX4 MAVLink链路已经建立。
        """
        if not self.EFFECTIVE_BURST_AFTER_HEARTBEAT:
            return
        if not self.any_vehicle_needs_assist():
            return

        # heartbeat 已出现，停止盲发线程，避免并发重复注入过多。
        self.blind_inject_stop = True

        print("\n[Burst] PX4 heartbeat 已建立，开始快速有效注入。")
        for i in range(self.BURST_ROUNDS):
            if not self.any_vehicle_needs_assist():
                print("[Burst] 定位质量已达标，提前结束 burst。")
                break

            if self.first_effective_inject_time is None:
                self.first_effective_inject_time = time.time()
                if self.gps_power_on_time is not None:
                    print(
                        f"[Time] T0 → heartbeat后首次有效注入 = "
                        f"{self.first_effective_inject_time - self.gps_power_on_time:.1f} 秒"
                    )

            print(f"[Burst] 第 {i + 1}/{self.BURST_ROUNDS} 轮有效注入")
            self.inject_sequence_first_fix_fast()

            t0 = time.time()
            while time.time() - t0 < self.BURST_INTERVAL:
                self.send_gcs_heartbeat()
                self.update_drone_status(timeout=0.1)
                if not self.any_vehicle_needs_assist():
                    break

    def configure_base_gps(self, ser: serial.Serial):
        """
        配置基准站 GPS 输出：
        1. UBX-NAV-PVT
        2. UBX-RXM-SFRBX
        3. 尝试启用 BeiDou B1I，使基准站输出 BDS/sig0/w10 或 BDS/sig1/w10。

        说明：
        - BDS/sig0 或 sig1 才能走当前 BDS-D1/D2 -> UBX-MGA-BDS-EPH 路线；
        - BDS/sig6、sig8 属于 B1C/B2a 等新信号，当前只统计，不强行封装；
        - 如果配置键在你的固件上不支持，接收机会忽略或 NAK，不会影响 GPS/GAL/QZSS 流程。
        """
        print("[Base] 配置基准站输出 NAV-PVT 与 RXM-SFRBX ...")

        # 1. 打开基准站 PVT 和 SFRBX 输出
        msg_out = cfg_valset_u1({
            0x20910007: 1,   # CFG-MSGOUT-UBX_NAV_PVT_UART1
            0x20910232: 1,   # CFG-MSGOUT-UBX_RXM_SFRBX_UART1
            0x20910016: 1,   # CFG-MSGOUT-UBX_NAV_SAT_UART1，用于基准站卫星C/N0排序
        }, ram=True, bbr=True, flash=False)

        ser.write(msg_out)
        time.sleep(0.2)

        # 2. 尝试启用 BeiDou B1I D1/D2 信号输出
        #    目标是让 RXM-SFRBX 中出现 BDS/sig0/w10 或 BDS/sig1/w10。
        if self.TRY_ENABLE_BASE_BDS_B1I:
            print("[Base] 尝试启用 BeiDou B1I(D1/D2) 信号，用于生成 BDS-MGA-EPH ...")
            print("[Base] 期望后续日志出现：BDS/sig0/w10 或 BDS/sig1/w10。")

            # 常见 u-blox CFG-SIGNAL 配置键：
            # CFG-SIGNAL-BDS_ENA    = 0x10310022
            # CFG-SIGNAL-BDS_B1_ENA = 0x1031000D   # BeiDou B1I
            # 这里只强制启用 BDS 和 B1I，不主动关闭 B1C/B2a，避免影响基准站正常跟踪。
            msg_bds_b1i = cfg_valset_u1({
                0x10310022: 1,   # CFG-SIGNAL-BDS_ENA
                0x1031000D: 1,   # CFG-SIGNAL-BDS_B1_ENA / B1I
            },
                ram=True,
                bbr=self.BASE_SIGNAL_CFG_SAVE_TO_BBR,
                flash=self.BASE_SIGNAL_CFG_SAVE_TO_FLASH
            )

            ser.write(msg_bds_b1i)
            time.sleep(0.3)

            print(
                "[Base] 已发送 BDS B1I 使能配置。若仍只看到 BDS/sig6/sig8，"
                "说明当前模块/固件/配置未输出 B1I D1/D2。"
            )

        # 3. 尝试启用 QZSS L1C/A。
        #    从你的 u-center 截图看，基准站已经能收到 QZSS 7 L1C/A，
        #    因此脚本需要打开 QZSS 总开关，并允许 svId=7 通过解析器。
        if self.TRY_ENABLE_BASE_QZSS_L1CA:
            print("[Base] 尝试启用 QZSS L1C/A，用于生成 QZSS-MGA-EPH ...")
            print("[Base] 期望后续日志出现：QZSS/sig0/w10，且 QZSS_L1CA_SFRBX > 0。")

            msg_qzss_l1ca = cfg_valset_u1({
                0x10310024: 1,   # CFG-SIGNAL-QZSS_ENA
                0x10310012: 1,   # CFG-SIGNAL-QZSS_L1CA_ENA
            },
                ram=True,
                bbr=self.BASE_SIGNAL_CFG_SAVE_TO_BBR,
                flash=self.BASE_SIGNAL_CFG_SAVE_TO_FLASH
            )

            ser.write(msg_qzss_l1ca)
            time.sleep(0.5)

            print("[Base] 已发送 QZSS L1C/A 使能配置。")

        # 4. 查询版本/触发接收机回包，便于确认串口可用
        ser.write(poll_mon_ver())
        time.sleep(0.2)

    def decode_nav_pvt(self, full_msg: bytes) -> bool:
        payload = full_msg[6:-2]

        if len(payload) < 92:
            return False

        year = struct.unpack_from("<H", payload, 4)[0]
        month = payload[6]
        day = payload[7]
        hour = payload[8]
        minute = payload[9]
        second = payload[10]
        valid = payload[11]
        fix_type = payload[20]
        num_sv = payload[23]

        lon_raw = struct.unpack_from("<i", payload, 24)[0]
        lat_raw = struct.unpack_from("<i", payload, 28)[0]
        height_mm = struct.unpack_from("<i", payload, 32)[0]
        h_acc_mm = struct.unpack_from("<I", payload, 40)[0]
        v_acc_mm = struct.unpack_from("<I", payload, 44)[0]

        valid_date = bool(valid & 0x01)
        valid_time = bool(valid & 0x02)

        if not (valid_date and valid_time) or fix_type < 2:
            self.formatted_pvt = f"等待有效 PVT... fix={fix_type}, sv={num_sv}"
            return False

        try:
            dt.datetime(year, month, day, hour, minute, min(second, 59))
        except ValueError:
            self.formatted_pvt = "等待有效 PVT... 日期时间字段非法"
            return False

        lat = lat_raw / 1e7
        lon = lon_raw / 1e7

        self.last_pvt = {
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "minute": minute,
            "second": second,
            "lat_raw": lat_raw,
            "lon_raw": lon_raw,
            "height_cm": int(round(height_mm / 10.0)),
            "h_acc_cm": max(1, int(round(h_acc_mm / 10.0))),
            "v_acc_cm": max(1, int(round(v_acc_mm / 10.0))),
            "fix_type": fix_type,
            "num_sv": num_sv,
        }

        self.last_pvt_monotonic = time.monotonic()
        self.base_pvt_last_time = time.time()

        self.formatted_pvt = (
            f"{year}-{month:02d}-{day:02d} "
            f"{hour:02d}:{minute:02d}:{second:02d} | "
            f"{lat:.7f},{lon:.7f} | fix={fix_type}, sv={num_sv}, "
            f"hAcc={h_acc_mm / 1000.0:.1f}m, vAcc={v_acc_mm / 1000.0:.1f}m"
        )
        return True

    def parse_base_stream(self, data: bytes) -> bytes:
        if data:
            self.base_last_rx_time = time.time()
        while len(data) >= 8:
            if data[:2] != b"\xB5\x62":
                data = data[1:]
                continue

            payload_len = data[4] + (data[5] << 8)
            full_len = 6 + payload_len + 2

            if len(data) < full_len:
                break

            full_msg = data[:full_len]

            if not verify_ubx(full_msg):
                self.base_ubx_crc_fail_count += 1
                data = data[1:]
                continue

            self.base_ubx_ok_count += 1

            cls_id = full_msg[2]
            msg_id = full_msg[3]
            payload = full_msg[6:-2]

            if cls_id == 0x01 and msg_id == 0x07:
                self.base_pvt_packet_last_time = time.time()
                self.decode_nav_pvt(full_msg)

            elif cls_id == 0x01 and msg_id == 0x35:
                self.decode_base_nav_sat(payload)

            elif cls_id == 0x02 and msg_id == 0x13:
                self.base_sfrbx_last_time = time.time()
                records = self.collector.handle_sfrbx_payload(payload)

                for record in records:
                    with self.lock:
                        target = (
                            self.assist_to_send
                            if record.kind == "eph"
                            else self.aux_assist_to_send
                        )
                        target[record.key] = record
                        self.assist_update_pending = True
                        self.last_assist_update_time = time.time()
                        self.assist_db_dirty = True

                    if record.kind == "eph":
                        print(
                            f"\n[Base] 生成辅助星历: {record.label}, "
                            f"IODE={record.iode}, 当前可注入星历数={len(self.assist_to_send)}"
                        )
                    else:
                        print(
                            f"\n[BaseAux] 生成 {record.kind.upper()}: {record.label}, "
                            f"当前长期辅助记录数={len(self.aux_assist_to_send)}"
                        )
                    self.save_assist_db(force=False)

            data = data[full_len:]

        return data

    def base_worker(self):
        """
        基准站线程：
        只负责读取常上电 GPS 的 NAV-PVT 和 RXM-SFRBX，
        并缓存多星座星历及 GPS LNAV 长期辅助数据。
        """
        print(f"[Base] 启动，端口={self.BASE_PORT}, baud={self.BAUD_BASE}")

        while self.is_running:
            try:
                with serial.Serial(self.BASE_PORT, self.BAUD_BASE, timeout=0.1) as ser:
                    ser.reset_input_buffer()
                    self.configure_base_gps(ser)

                    last_poll = 0.0

                    while self.is_running:
                        now = time.time()

                        if now - last_poll > 1.0:
                            ser.write(poll_nav_pvt())
                            last_poll = now

                        n = ser.in_waiting

                        if n:
                            self.base_buffer += ser.read(n)
                            self.base_buffer = self.parse_base_stream(self.base_buffer)

                        if len(self.base_buffer) > 65536:
                            self.base_buffer = b""

                        time.sleep(0.02)

            except Exception as e:
                print(f"\n[Base] 异常: {e}")
                time.sleep(3)

    def close_mavlink_connection(self):
        """
        关闭已有 MAVLink 串口句柄，避免重复打开同一 COM 口导致 Permission denied。
        同时停止 MAVLink 接收线程，避免线程继续读已关闭的串口。
        """
        if self.multi_link_enabled():
            for link in self.drone_links:
                self.close_drone_link(link)

        self.stop_mavlink_rx_thread()

        if self.master is not None:
            try:
                self.master.close()
            except Exception:
                pass
            self.master = None

    def mavlink_connect(self):
        """
        严格在基准站星历准备好后才调用。
        不做盲发，不提前打开串口；收到 PX4 heartbeat 后才开始后续注入。
        """
        print(f"\n[MAVLink] 等待 PX4 heartbeat，mode={self.LINK_MODE}, endpoint={self.DRONE_PORT}")

        # 每次重新连接前先释放旧句柄，防止脚本内部重复打开同一端口。
        self.close_mavlink_connection()

        self.master = self.create_mavlink_connection()

        while self.is_running:
            self.send_gcs_heartbeat()

            msg = self.master.recv_match(type="HEARTBEAT", blocking=True, timeout=1.0)

            if not msg:
                continue

            src_sys = msg.get_srcSystem()
            src_comp = msg.get_srcComponent()
            mav_type = getattr(msg, "type", None)

            if src_sys in (0, 255):
                continue

            if mav_type == mavutil.mavlink.MAV_TYPE_GCS:
                continue

            self.master.target_system = src_sys
            self.master.target_component = src_comp

            self.px4_connected_time = time.time()

            print(
                f"[MAVLink] 已连接 PX4: "
                f"target_system={src_sys}, target_component={src_comp}, mav_type={mav_type}"
            )

            if self.gps_power_on_time is not None:
                print(
                    f"[Time] T0 → PX4 heartbeat连接 = "
                    f"{self.px4_connected_time - self.gps_power_on_time:.1f} 秒"
                )

            self.request_gps_raw_int_rate(hz=5)
            self.request_basic_streams()

            return

    def send_gcs_heartbeat(self):
        if not self.master:
            return

        try:
            with self.mav_send_lock:
                self.master.mav.heartbeat_send(
                    mavutil.mavlink.MAV_TYPE_GCS,
                    mavutil.mavlink.MAV_AUTOPILOT_INVALID,
                    0,
                    0,
                    0
                )
        except Exception as e:
            print(f"\n[MAVLink] 发送 GCS heartbeat 失败: {e}")

    def send_gcs_heartbeat_to_link(self, link: DroneLink):
        if not link.enabled or link.master is None:
            return
        try:
            with link.send_lock:
                link.master.mav.heartbeat_send(
                    mavutil.mavlink.MAV_TYPE_GCS,
                    mavutil.mavlink.MAV_AUTOPILOT_INVALID,
                    0,
                    0,
                    0,
                )
            link.heartbeat_sent_count += 1
        except Exception as exc:
            link.last_error = str(exc)
            link.connected = False
            print(f"\n[MAVLink:{link.name}] 发送 GCS heartbeat 失败: {exc}")

    def send_gcs_heartbeat_all_links(self):
        if not self.multi_link_enabled():
            self.send_gcs_heartbeat()
            return
        for link in self.drone_links:
            self.send_gcs_heartbeat_to_link(link)

    def _inject_ubx_to_px4_once(self, ubx_msg: bytes) -> bool:
        """
        通过 MAVLink GPS_RTCM_DATA 把 UBX-MGA 放入 PX4 的 gps_inject_data 话题。

        UART 模式由已适配的 PX4 gps.cpp 写入 GPS 串口；CAN 模式由 PX4 在
        UAVCAN_PUB_RTCM=1 时发布 RTCMStream，并由已验证的目标节点透传原始 UBX。
        """
        if not self.master:
            return False

        if len(ubx_msg) > 180:
            raise ValueError("UBX 消息超过 GPS_RTCM_DATA 单包 180 字节限制")
        if not self.can_injection_allowed():
            return False
        if not self.can_probe_allows_packet(ubx_msg):
            return False

        payload = list(ubx_msg) + [0] * (180 - len(ubx_msg))

        if self.VERBOSE_INJECT_RAW:
            print(
                f"[InjectRaw] len={len(ubx_msg):3d}, "
                f"head={ubx_msg[:6].hex(' ').upper()}"
            )

        try:
            with self.mav_send_lock:
                self.master.mav.gps_rtcm_data_send(
                    0,
                    len(ubx_msg),
                    payload
                )
            self.inject_packet_count += 1
            self.inject_byte_count += len(ubx_msg)
            self.record_can_probe_send(ubx_msg, link_name=None)
            return True
        except Exception:
            self.inject_error_count += 1
            raise

    def _inject_ubx_to_link_once(self, link: DroneLink, ubx_msg: bytes) -> bool:
        """通过指定飞机数据链发送一包 UBX-MGA。"""
        if not link.enabled or link.master is None:
            return False

        if len(ubx_msg) > 180:
            raise ValueError("UBX 消息超过 GPS_RTCM_DATA 单包 180 字节限制")
        if not self.can_injection_allowed(link=link):
            return False
        if not self.can_probe_allows_packet(ubx_msg):
            return False

        payload = list(ubx_msg) + [0] * (180 - len(ubx_msg))

        if self.VERBOSE_INJECT_RAW:
            print(
                f"[InjectRaw:{link.name}] len={len(ubx_msg):3d}, "
                f"head={ubx_msg[:6].hex(' ').upper()}"
            )

        try:
            with link.send_lock:
                link.master.mav.gps_rtcm_data_send(
                    0,
                    len(ubx_msg),
                    payload,
                )
            link.inject_packet_count += 1
            link.inject_byte_count += len(ubx_msg)
            self.inject_packet_count += 1
            self.inject_byte_count += len(ubx_msg)
            self.record_can_probe_send(ubx_msg, link_name=link.name)
            return True
        except Exception as exc:
            link.inject_error_count += 1
            link.last_error = str(exc)
            link.connected = False
            self.inject_error_count += 1
            print(f"\n[Inject:{link.name}] 发送失败: {exc}")
            return False

    def ack_flow_applies(self, ubx_msg: bytes, link_name: Optional[str]) -> bool:
        if self.ACK_FLOW_CONTROL_MODE != "stop_and_wait":
            return False
        if not self.ACK_MONITOR_ENABLED or self.ack_monitor is None:
            return False
        if not self.ack_monitor.connected:
            return False
        if mga_ack_identity(ubx_msg) is None:
            return False
        if self.ACK_TARGET_LINK_NAME:
            if link_name != self.ACK_TARGET_LINK_NAME:
                return False
        now = time.time()
        if now < self.ack_flow_suspended_until:
            return False
        if self.ack_flow_suspended_until > 0:
            self.ack_flow_suspended_until = 0.0
            self.ack_consecutive_timeouts = 0
        return True

    def _record_successful_inject(self, ubx_msg: bytes):
        constellation = mga_constellation(ubx_msg)
        self.inject_by_constellation[constellation] = (
            self.inject_by_constellation.get(constellation, 0) + 1
        )

    def _inject_with_policy(
        self,
        ubx_msg: bytes,
        send_once: Callable[[], bool],
        link_name: Optional[str],
    ) -> bool:
        monitor = self.ack_monitor
        use_ack = self.ack_flow_applies(ubx_msg, link_name)
        attempts = 1 + (self.ACK_MAX_RETRIES if use_ack else 0)

        for attempt in range(attempts):
            sequence = monitor.snapshot_ack_sequence() if use_ack else 0
            sent = bool(send_once())
            if not sent:
                return False
            self._record_successful_inject(ubx_msg)

            if not use_ack:
                time.sleep(self.current_inject_delay_s)
                self.update_drone_status(timeout=0.0)
                return True

            self.ack_wait_count += 1
            event = monitor.wait_for_ack(
                ubx_msg,
                after_sequence=sequence,
                timeout_s=self.ACK_WAIT_TIMEOUT_S,
            )
            if event is None:
                self.ack_timeout_count += 1
                self.ack_consecutive_timeouts += 1
                self.current_inject_delay_s = min(
                    self.ADAPTIVE_INJECT_DELAY_MAX_S,
                    max(
                        self.current_inject_delay_s * 1.5,
                        self.ACK_TRANSIENT_BACKOFF_S,
                    ),
                )
                if self.ack_consecutive_timeouts >= self.ACK_TIMEOUT_CIRCUIT_BREAKER:
                    self.ack_flow_suspended_until = time.time() + self.ACK_SUSPEND_S
                    print(
                        f"\n[ACK] 连续 {self.ack_consecutive_timeouts} 次等待超时，"
                        f"闭环等待暂停 {self.ACK_SUSPEND_S:.0f}s，期间继续按观察模式发送。"
                    )
                    use_ack = False
                if attempt + 1 < attempts and use_ack:
                    self.ack_retry_count += 1
                    time.sleep(self.current_inject_delay_s)
                    continue
                return True

            self.ack_consecutive_timeouts = 0
            if event.accepted:
                self.ack_accept_count += 1
                self.current_inject_delay_s = max(
                    self.ADAPTIVE_INJECT_DELAY_MIN_S,
                    self.current_inject_delay_s * 0.85,
                )
                self.update_drone_status(timeout=0.0)
                return True

            self.ack_reject_count += 1
            self.current_inject_delay_s = min(
                self.ADAPTIVE_INJECT_DELAY_MAX_S,
                max(self.current_inject_delay_s * 1.5, self.ACK_TRANSIENT_BACKOFF_S),
            )
            transient = event.info_code in {1, 5}
            if transient and attempt + 1 < attempts:
                self.ack_retry_count += 1
                time.sleep(self.current_inject_delay_s)
                continue
            print(
                f"\n[ACK] 接收机拒绝 {event.name}: "
                f"infoCode={event.info_code}，本轮不再重试。"
            )
            return False

        return True

    def inject_ubx_to_px4(self, ubx_msg: bytes) -> bool:
        return self._inject_with_policy(
            ubx_msg,
            lambda: self._inject_ubx_to_px4_once(ubx_msg),
            link_name=None,
        )

    def inject_ubx_to_link(self, link: DroneLink, ubx_msg: bytes) -> bool:
        return self._inject_with_policy(
            ubx_msg,
            lambda: self._inject_ubx_to_link_once(link, ubx_msg),
            link_name=link.name,
        )

    def update_drone_status(self, timeout=0.0):
        """
        兼容旧代码的状态更新入口。

        新版脚本已经把 MAVLink 接收放到独立线程中，GPS_RAW_INT 会由
        mavlink_rx_worker 持续处理。因此这里不再调用 recv_match()，避免
        发送注入数据时和接收线程抢读同一个串口。

        timeout > 0 时只做短暂等待，给接收线程时间处理刚到达的消息。
        """
        if timeout and timeout > 0:
            end_time = time.time() + timeout
            while self.is_running and time.time() < end_time:
                if not self.any_vehicle_needs_assist():
                    break
                time.sleep(0.02)

    def assist_ready_for_injection(self) -> bool:
        return self.fresh_assist_count() >= self.MIN_EPH_BEFORE_POWER_ON

    def link_for_vehicle(self, vehicle: VehicleState) -> Optional[DroneLink]:
        if not self.multi_link_enabled():
            return None
        if vehicle.link_name and vehicle.link_name in self.link_by_name:
            return self.link_by_name[vehicle.link_name]
        link_name = self.system_to_link.get(vehicle.system_id)
        if link_name:
            return self.link_by_name.get(link_name)
        link_name = self.configured_system_to_link.get(vehicle.system_id)
        if link_name:
            return self.link_by_name.get(link_name)
        return None

    def select_links_for_injection(
        self,
        target_vehicles: Optional[List[VehicleState]] = None,
    ) -> List[DroneLink]:
        """选择本轮要发送辅助数据的数据链。"""
        if not self.multi_link_enabled():
            return []

        selected: Dict[str, DroneLink] = {}
        vehicles = target_vehicles
        if vehicles is None:
            vehicles = self.online_unfixed_vehicles()

        if not vehicles and self.MULTI_LINK_SEND_MODE == "all_enabled":
            for link in self.drone_links:
                if link.enabled and link.master is not None:
                    selected[link.name] = link
            return list(selected.values())

        for vehicle in vehicles:
            link = self.link_for_vehicle(vehicle)
            if link is not None and link.enabled and link.master is not None:
                selected[link.name] = link

        return list(selected.values())

    def all_enabled_open_links(self) -> List[DroneLink]:
        if not self.multi_link_enabled():
            return []
        return [
            link for link in self.drone_links
            if link.enabled and link.master is not None
        ]

    def inject_broadcast_round_to_links(
        self,
        reason: str,
        links: List[DroneLink],
        top_n: Optional[int] = None,
        force_almanac: bool = False,
    ) -> bool:
        """向一组独立数据链分别发送同一轮辅助数据。"""
        links = [link for link in links if link.enabled and link.master is not None]
        if not links:
            return False

        self.prune_stale_assist()
        if not self.assist_ready_for_injection():
            return False

        assist_items = (
            self.get_first_fix_assist_items()
            if top_n is not None
            else self.get_sorted_fresh_assist_items()
        )
        limit = self.MAX_INJECT_EPH_PER_ROUND
        if top_n is not None:
            limit = min(limit, max(1, int(top_n)))
        assist_items = assist_items[:limit]
        if not assist_items:
            return False
        aux_items, almanac_items = self.get_aux_items_for_round(
            fast_round=top_n is not None,
            force_almanac=force_almanac,
        )

        include_time_pos = self.base_pvt_is_fresh()
        packets_before = self.inject_packet_count
        started = time.time()
        messages = []

        if include_time_pos:
            pvt = self.last_pvt
            pvt_monotonic = self.last_pvt_monotonic
            messages.append(
                build_mga_ini_time_utc(
                    pvt,
                    pvt_monotonic,
                    self.TIME_ACC_S,
                    self.TIME_ACC_NS,
                )
            )
            messages.append(build_mga_ini_pos_llh(pvt, self.POS_ACC_CM))
        else:
            print(
                f"\n[Inject] {reason}: 基准站 PVT 已过期，"
                "本轮只发送仍在有效期内的星历，不发送 TIME/POS。"
            )

        for key, record in assist_items:
            messages.append(record.ubx)
        for key, record in aux_items:
            messages.append(record.ubx)
        for key, record in almanac_items:
            messages.append(record.ubx)

        sent_links = []
        for link in links:
            link_ok = True
            for ubx in messages:
                if not self.inject_ubx_to_link(link, ubx):
                    link_ok = False
                    break
            if link_ok:
                sent_links.append(link.name)

        if not sent_links:
            return False

        for key, record in assist_items:
            self.assist_sent_iode[key] = record.iode
        for key, record in aux_items + almanac_items:
            self.assist_sent_iode[key] = record.iode
        if almanac_items:
            self.last_almanac_broadcast_time = time.time()

        self.inject_round_count += 1
        self.last_effective_inject_round_time = time.time()
        print(
            f"\n[Inject] {reason}: 已发送到 {','.join(sent_links)}，"
            f"EPH={len(assist_items)}, AUX={len(aux_items)}, ALM={len(almanac_items)}, "
            f"TIME_POS={'yes' if include_time_pos else 'no'}, "
            f"packets={self.inject_packet_count - packets_before}, "
            f"duration={time.time() - started:.2f}s"
        )
        return True

    def inject_broadcast_round(
        self,
        reason: str,
        top_n: Optional[int] = None,
        force_almanac: bool = False,
    ) -> bool:
        """
        在当前数据链上广播一轮辅助数据。

        GPS_RTCM_DATA 本身不带目标 system_id，因此共享总线上的所有 PX4 都会收到。
        这正适合机巢多机并发上电；每架飞机的状态只用于决定发送频率，不用于寻址。
        """
        if not self.master:
            return False

        self.prune_stale_assist()
        if not self.assist_ready_for_injection():
            return False

        assist_items = (
            self.get_first_fix_assist_items()
            if top_n is not None
            else self.get_sorted_fresh_assist_items()
        )
        limit = self.MAX_INJECT_EPH_PER_ROUND
        if top_n is not None:
            limit = min(limit, max(1, int(top_n)))
        assist_items = assist_items[:limit]
        if not assist_items:
            return False
        aux_items, almanac_items = self.get_aux_items_for_round(
            fast_round=top_n is not None,
            force_almanac=force_almanac,
        )

        include_time_pos = self.base_pvt_is_fresh()
        packets_before = self.inject_packet_count
        started = time.time()

        if include_time_pos:
            pvt = self.last_pvt
            pvt_monotonic = self.last_pvt_monotonic
            if not self.inject_ubx_to_px4(
                build_mga_ini_time_utc(
                    pvt,
                    pvt_monotonic,
                    self.TIME_ACC_S,
                    self.TIME_ACC_NS,
                )
            ):
                return False
            if not self.inject_ubx_to_px4(
                build_mga_ini_pos_llh(pvt, self.POS_ACC_CM)
            ):
                return False
        else:
            print(
                f"\n[Inject] {reason}: 基准站 PVT 已过期，"
                "本轮只广播仍在有效期内的星历，不发送 TIME/POS。"
            )

        for key, record in assist_items:
            if self.inject_ubx_to_px4(record.ubx):
                self.assist_sent_iode[key] = record.iode
        for key, record in aux_items:
            if self.inject_ubx_to_px4(record.ubx):
                self.assist_sent_iode[key] = record.iode
        for key, record in almanac_items:
            if self.inject_ubx_to_px4(record.ubx):
                self.assist_sent_iode[key] = record.iode
        if almanac_items:
            self.last_almanac_broadcast_time = time.time()

        self.inject_round_count += 1
        self.last_effective_inject_round_time = time.time()
        print(
            f"\n[Inject] {reason}: 广播完成，EPH={len(assist_items)}, "
            f"AUX={len(aux_items)}, ALM={len(almanac_items)}, "
            f"TIME_POS={'yes' if include_time_pos else 'no'}, "
            f"packets={self.inject_packet_count - packets_before}, "
            f"duration={time.time() - started:.2f}s"
        )
        return True

    def run_injection_scheduler_tick(self):
        """执行一次非阻塞调度判断；每个 tick 最多发一轮，避免链路突发拥塞。"""
        now = time.time()
        self.refresh_vehicle_online_states()
        self.prune_stale_assist()
        self.sync_legacy_vehicle_summary()

        with self.lock:
            burst_due = [
                vehicle for vehicle in self.vehicles.values()
                if (
                    vehicle.online
                    and self.vehicle_should_receive_assist(vehicle, now)
                    and vehicle.burst_rounds_remaining > 0
                    and now >= vehicle.next_burst_time
                )
            ]

        if burst_due:
            labels = ",".join(vehicle.key for vehicle in burst_due)
            force_almanac = (
                self.FORCE_SESSION_ALMANAC
                and self.fresh_almanac_count() > 0
                and any(
                    vehicle.almanac_sent_session_id != vehicle.session_id
                    for vehicle in burst_due
                )
            )
            if self.multi_link_enabled():
                injected = self.inject_broadcast_round_to_links(
                    reason=f"新飞机 burst [{labels}]",
                    links=self.select_links_for_injection(burst_due),
                    top_n=self.FIRST_FIX_TOP_N,
                    force_almanac=force_almanac,
                )
            else:
                injected = self.inject_broadcast_round(
                    reason=f"新飞机 burst [{labels}]",
                    top_n=self.FIRST_FIX_TOP_N,
                    force_almanac=force_almanac,
                )
            if injected:
                sent_at = time.time()
                with self.lock:
                    for vehicle in burst_due:
                        if vehicle.first_inject_time is None:
                            vehicle.first_inject_time = sent_at
                        vehicle.burst_rounds_remaining = max(
                            0, vehicle.burst_rounds_remaining - 1
                        )
                        vehicle.next_burst_time = sent_at + self.BURST_INTERVAL
                        vehicle.last_inject_time = sent_at
                        if force_almanac:
                            vehicle.almanac_sent_session_id = vehicle.session_id
                return

        with self.lock:
            burst_pending = any(
                vehicle.online
                and self.vehicle_should_receive_assist(vehicle, now)
                and vehicle.burst_rounds_remaining > 0
                for vehicle in self.vehicles.values()
            )
        if burst_pending:
            # 等待 GPS 驱动初始化或下一轮 burst，不用后台广播抢占这段窗口。
            if self.SUPPRESS_BACKGROUND_DURING_ACTIVE_ASSIST:
                return

        unfixed = self.online_unfixed_vehicles()
        if unfixed:
            oldest_inject = min(vehicle.last_inject_time for vehicle in unfixed)
            if now - oldest_inject >= self.ACTIVE_UNFIXED_INJECT_INTERVAL_S:
                if self.multi_link_enabled():
                    injected = self.inject_broadcast_round_to_links(
                        reason=f"未定位飞机持续辅助 x{len(unfixed)}",
                        links=self.select_links_for_injection(unfixed),
                        top_n=self.FIRST_FIX_TOP_N,
                    )
                else:
                    injected = self.inject_broadcast_round(
                        reason=f"未定位飞机持续辅助 x{len(unfixed)}",
                        top_n=self.FIRST_FIX_TOP_N,
                    )
                if injected:
                    sent_at = time.time()
                    with self.lock:
                        for vehicle in unfixed:
                            vehicle.last_inject_time = sent_at
                    return
            # 活跃定位阶段只允许 Top-N 快速辅助，不穿插后台或新 EPH 广播。
            if self.SUPPRESS_BACKGROUND_DURING_ACTIVE_ASSIST:
                return

        postfix = self.vehicles_needing_postfix_assist(now)
        if postfix:
            oldest_inject = min(vehicle.last_inject_time for vehicle in postfix)
            if now - oldest_inject >= self.ACTIVE_UNFIXED_INJECT_INTERVAL_S:
                if self.multi_link_enabled():
                    injected = self.inject_broadcast_round_to_links(
                        reason=f"3D Fix后预检观察继续辅助 x{len(postfix)}",
                        links=self.select_links_for_injection(postfix),
                        top_n=self.FIRST_FIX_TOP_N,
                    )
                else:
                    injected = self.inject_broadcast_round(
                        reason=f"3D Fix后预检观察继续辅助 x{len(postfix)}",
                        top_n=self.FIRST_FIX_TOP_N,
                    )
                if injected:
                    sent_at = time.time()
                    with self.lock:
                        for vehicle in postfix:
                            vehicle.last_inject_time = sent_at
                    return
            # 3D Fix 后预检观察阶段同样抑制低优先级广播，降低 GPS UART 瞬时负载。
            if self.SUPPRESS_BACKGROUND_DURING_ACTIVE_ASSIST:
                return

        with self.lock:
            new_eph_pending = self.assist_update_pending
        if (
            self.NEW_EPH_BROADCAST_ENABLED
            and new_eph_pending
            and now - self.last_new_eph_broadcast_time
            >= self.NEW_EPH_BROADCAST_MIN_INTERVAL_S
        ):
            if self.multi_link_enabled():
                links = (
                    self.all_enabled_open_links()
                    if self.MULTI_LINK_SEND_MODE == "all_enabled"
                    else self.select_links_for_injection(self.online_unfixed_vehicles())
                )
                injected = self.inject_broadcast_round_to_links(
                    reason="新星历更新定向广播",
                    links=links,
                )
            else:
                injected = self.inject_broadcast_round(reason="新星历更新广播")
            if injected:
                with self.lock:
                    self.assist_update_pending = False
                self.last_new_eph_broadcast_time = time.time()
                return

        if (
            self.BACKGROUND_BROADCAST_ENABLED
            and now - self.last_background_broadcast_time
            >= self.BACKGROUND_BROADCAST_INTERVAL_S
        ):
            if self.multi_link_enabled():
                links = (
                    self.all_enabled_open_links()
                    if self.MULTI_LINK_SEND_MODE == "all_enabled"
                    else self.select_links_for_injection(self.online_unfixed_vehicles())
                )
                injected = self.inject_broadcast_round_to_links(
                    reason="24x7 后台保活定向广播",
                    links=links,
                )
            else:
                injected = self.inject_broadcast_round(reason="24x7 后台保活广播")
            if injected:
                self.last_background_broadcast_time = time.time()

    def inject_sequence_first_fix_fast(self):
        """
        首次定位优化版注入：
        1. TIME + POS 必发；
        2. 只发送Top-N条最可能用于首次定位的星历；
        3. 3D Fix前每2秒可重复该轻量注入；
        4. 目标达到3D Fix后立即停止。
        """
        self.update_drone_status(timeout=0.0)

        if self.STOP_INJECT_AFTER_3D_FIX and not self.any_vehicle_needs_assist():
            return

        if not self.last_pvt:
            print("[InjectFast] 还没有基准站有效 PVT，暂不注入。")
            return

        if self.first_inject_time is None:
            self.first_inject_time = time.time()
            if self.gps_power_on_time is not None:
                print(
                    f"\n[Time] T0 → 首次 A-GNSS 注入开始 = "
                    f"{self.first_inject_time - self.gps_power_on_time:.1f} 秒"
                )

        fresh_total_count = len(self.get_sorted_fresh_assist_items())
        assist_items = self.get_first_fix_assist_items()
        aux_items, almanac_items = self.get_aux_items_for_round(fast_round=True)

        print("\n[InjectFast] 开始首次定位优化注入")
        print(f"[InjectFast] 基准站 PVT: {self.formatted_pvt}")
        print(f"[InjectFast] 当前新鲜有效星历总数={fresh_total_count}, 本轮最多注入Top-N={self.FIRST_FIX_TOP_N}, 当前选择星历数: {len(assist_items)}")
        if assist_items:
            print("[InjectFast] Top星历顺序: " + ", ".join(record.label for _, record in assist_items))

        try:
            msg_time = build_mga_ini_time_utc(
                self.last_pvt,
                self.last_pvt_monotonic,
                self.TIME_ACC_S,
                self.TIME_ACC_NS
            )
            if not self.inject_ubx_to_px4(msg_time):
                raise RuntimeError("目标 GPS 未接受 TIME_UTC")
            print("[InjectFast] 已发送 UBX-MGA-INI-TIME_UTC")

            if not self.any_vehicle_needs_assist():
                print("[InjectFast] 目标 GPS 已达到观察停止条件，停止后续辅助注入。")
                return

            msg_pos = build_mga_ini_pos_llh(
                self.last_pvt,
                self.POS_ACC_CM
            )
            if not self.inject_ubx_to_px4(msg_pos):
                raise RuntimeError("目标 GPS 未接受 POS_LLH")
            print("[InjectFast] 已发送 UBX-MGA-INI-POS_LLH")

            if not self.any_vehicle_needs_assist():
                print("[InjectFast] 目标 GPS 已达到观察停止条件，停止后续辅助注入。")
                return

        except Exception as e:
            print(f"[InjectFast] 时间/位置辅助注入失败: {e}")
            return

        round_start = time.time()
        packets_before = self.inject_packet_count
        bytes_before = self.inject_byte_count
        errors_before = self.inject_error_count

        sent = 0
        for key, record in assist_items:
            self.update_drone_status(timeout=0.0)
            if self.STOP_INJECT_AFTER_3D_FIX and not self.any_vehicle_needs_assist():
                print("[InjectFast] 目标 GPS 已达到观察停止条件，停止后续星历注入。")
                break

            try:
                if self.inject_ubx_to_px4(record.ubx):
                    self.assist_sent_iode[key] = record.iode
                    sent += 1
                    print(f"[InjectFast] 已发送 {record.label}, IODE={record.iode}")
            except Exception as e:
                print(f"[InjectFast] 发送 {record.label} 失败: {e}")

        aux_sent = 0
        for key, record in aux_items:
            try:
                if self.inject_ubx_to_px4(record.ubx):
                    self.assist_sent_iode[key] = record.iode
                    aux_sent += 1
                    print(f"[InjectFast] 已发送 {record.label}")
            except Exception as e:
                print(f"[InjectFast] 发送 {record.label} 失败: {e}")

        alm_sent = 0
        for key, record in almanac_items:
            try:
                if self.inject_ubx_to_px4(record.ubx):
                    self.assist_sent_iode[key] = record.iode
                    alm_sent += 1
            except Exception as e:
                print(f"[InjectFast] 发送 {record.label} 失败: {e}")
        if alm_sent:
            self.last_almanac_broadcast_time = time.time()

        self.inject_round_count += 1
        duration = time.time() - round_start
        print(
            f"[InjectFast] 本次发送: AUX={aux_sent}, "
            f"Top-EPH={sent}, ALM={alm_sent}"
        )
        print(
            f"[InjectStats] round={self.inject_round_count}, "
            f"packets={self.inject_packet_count - packets_before}, "
            f"bytes={self.inject_byte_count - bytes_before}, "
            f"duration={duration:.2f}s, "
            f"errors={self.inject_error_count - errors_before}"
        )
        self.last_effective_inject_round_time = time.time()
        self.write_status_json()
        self.update_drone_status(timeout=0.2)

    def inject_sequence(self):
        """
        执行一次完整注入：
        1. MGA-INI-TIME_UTC
        2. MGA-INI-POS_LLH
        3. MGA-GPS-EPH
        4. MGA-QZSS-EPH
        """
        self.update_drone_status(timeout=0.0)

        if self.STOP_INJECT_AFTER_3D_FIX and not self.any_vehicle_needs_assist():
            return

        if not self.last_pvt:
            print("[Inject] 还没有基准站有效 PVT，暂不注入。")
            return

        if self.first_inject_time is None:
            self.first_inject_time = time.time()

            if self.gps_power_on_time is not None:
                print(
                    f"\n[Time] T0 → 首次 A-GNSS 注入开始 = "
                    f"{self.first_inject_time - self.gps_power_on_time:.1f} 秒"
                )

        assist_items = self.get_sorted_fresh_assist_items()
        aux_items, almanac_items = self.get_aux_items_for_round(fast_round=False)

        print("\n[Inject] 开始 A-GNSS 快速注入")
        print(f"[Inject] 基准站 PVT: {self.formatted_pvt}")
        print(f"[Inject] 当前可注入新鲜辅助星历数: {len(assist_items)}")
        if assist_items:
            order_preview = ", ".join([rec.label for _, rec in assist_items[:10]])
            print(f"[Inject] 星历注入优先顺序: {order_preview}")

        try:
            msg_time = build_mga_ini_time_utc(
                self.last_pvt,
                self.last_pvt_monotonic,
                self.TIME_ACC_S,
                self.TIME_ACC_NS
            )

            if not self.inject_ubx_to_px4(msg_time):
                raise RuntimeError("目标 GPS 未接受 TIME_UTC")
            print("[Inject] 已发送 UBX-MGA-INI-TIME_UTC")

            if not self.any_vehicle_needs_assist():
                print("[Inject] 目标 GPS 已达到观察停止条件，停止后续辅助注入。")
                return

            msg_pos = build_mga_ini_pos_llh(
                self.last_pvt,
                self.POS_ACC_CM
            )

            if not self.inject_ubx_to_px4(msg_pos):
                raise RuntimeError("目标 GPS 未接受 POS_LLH")
            print("[Inject] 已发送 UBX-MGA-INI-POS_LLH")

            if not self.any_vehicle_needs_assist():
                print("[Inject] 目标 GPS 已达到观察停止条件，停止后续辅助注入。")
                return

        except Exception as e:
            print(f"[Inject] 时间/位置辅助注入失败: {e}")
            return

        round_start = time.time()
        packets_before = self.inject_packet_count
        bytes_before = self.inject_byte_count
        errors_before = self.inject_error_count

        sent = 0

        for key, record in assist_items:
            if self.STOP_INJECT_AFTER_3D_FIX and not self.any_vehicle_needs_assist():
                break

            if not self.REPEAT_EPH_UNTIL_FIX or not self.any_vehicle_needs_assist():
                if self.assist_sent_iode.get(key) == record.iode:
                    continue

            try:
                if self.inject_ubx_to_px4(record.ubx):
                    self.assist_sent_iode[key] = record.iode
                    sent += 1
                    print(f"[Inject] 已发送 {record.label}, IODE={record.iode}")

            except Exception as e:
                print(f"[Inject] 发送 {record.label} 失败: {e}")

        aux_sent = 0
        for key, record in aux_items:
            try:
                if self.inject_ubx_to_px4(record.ubx):
                    self.assist_sent_iode[key] = record.iode
                    aux_sent += 1
                    print(f"[Inject] 已发送 {record.label}")
            except Exception as e:
                print(f"[Inject] 发送 {record.label} 失败: {e}")

        alm_sent = 0
        for key, record in almanac_items:
            try:
                if self.inject_ubx_to_px4(record.ubx):
                    self.assist_sent_iode[key] = record.iode
                    alm_sent += 1
            except Exception as e:
                print(f"[Inject] 发送 {record.label} 失败: {e}")
        if alm_sent:
            self.last_almanac_broadcast_time = time.time()

        self.inject_round_count += 1
        duration = time.time() - round_start
        print(f"[Inject] 本次发送: AUX={aux_sent}, EPH={sent}, ALM={alm_sent}")
        print(
            f"[InjectStats] round={self.inject_round_count}, "
            f"packets={self.inject_packet_count - packets_before}, "
            f"bytes={self.inject_byte_count - bytes_before}, "
            f"duration={duration:.2f}s, "
            f"errors={self.inject_error_count - errors_before}"
        )
        self.last_effective_inject_round_time = time.time()
        self.write_status_json()

        time.sleep(0.3)
        self.update_drone_status(timeout=0.5)

        print(
            f"[Status] PX4 GPS: fix_type={self.drone_fix_type}, "
            f"sats={self.drone_sats} | {self.ttff_elapsed_text()}"
        )

    def preheartbeat_inject_once(self):
        """
        PX4 heartbeat 出现前的连续数据链发送：
        只负责把 TIME + POS + Top-N EPH 低频从数传口发出去，
        不记录 first_inject_time，避免污染“检测到heartbeat后”的有效注入计时。
        默认间隔为 8 秒，主要用于链路刚恢复时让最近一轮数据有机会进入 PX4，
        不是持续高频刷屏。
        """
        if not self.master:
            return

        if not self.last_pvt:
            return

        # 如果飞控已经出现并且已经 3D Fix，就不用再发
        if self.STOP_INJECT_AFTER_3D_FIX and not self.any_vehicle_needs_assist():
            return

        assist_items = self.get_first_fix_assist_items()
        aux_items = self.get_fresh_aux_assist_items(include_almanac=False)

        try:
            msg_time = build_mga_ini_time_utc(
                self.last_pvt,
                self.last_pvt_monotonic,
                self.TIME_ACC_S,
                self.TIME_ACC_NS
            )
            self.inject_ubx_to_px4(msg_time)

            msg_pos = build_mga_ini_pos_llh(
                self.last_pvt,
                self.POS_ACC_CM
            )
            self.inject_ubx_to_px4(msg_pos)

            sent = 0
            for key, record in assist_items:
                if self.inject_ubx_to_px4(record.ubx):
                    sent += 1

            for key, record in aux_items:
                self.inject_ubx_to_px4(record.ubx)

            if self.PRE_HEARTBEAT_INJECT_VERBOSE:
                print(
                    f"[PreHB] 已通过数据链预发送 TIME+POS+"
                    f"AUX {len(aux_items)}条+Top星历 {sent} 条"
                )

        except Exception as e:
            print(f"\n[PreHB] 预发送辅助数据失败: {e}")

    def _handle_first_px4_heartbeat_for_continuous_test(self, msg):
        """
        连续数据链测试模式下，第一次收到真实 PX4 heartbeat 时调用。
        该时刻作为 T0：检测到飞控上电/上线。
        """
        src_sys = msg.get_srcSystem()
        src_comp = msg.get_srcComponent()
        mav_type = getattr(msg, "type", None)

        if src_sys in (0, 255):
            return False
        if mav_type == mavutil.mavlink.MAV_TYPE_GCS:
            return False

        self.master.target_system = src_sys
        self.master.target_component = src_comp

        # 以检测到 PX4 heartbeat 的时刻作为本实验 T0
        self.start_ttff_timer("检测到 PX4 heartbeat / 飞控上电")
        self.px4_connected_time = self.gps_power_on_time
        self.first_inject_time = None
        self.first_effective_inject_time = None

        print(
            f"\n[MAVLink] ✅ 检测到 PX4 heartbeat："
            f"target_system={src_sys}, target_component={src_comp}, mav_type={mav_type}"
        )
        print("[TTFF] 本轮计时起点改为：检测到 PX4 heartbeat 的时刻。")
        print("[Time] 检测到PX4 heartbeat → PX4连接 = 0.0 秒")
        self.set_engineering_state("PX4_ONLINE")

        # 检测到 heartbeat 后，先请求 GPS_RAW_INT 输出，再稍等片刻，
        # 然后主循环才会开始 Top-N burst 注入。
        self.request_gps_raw_int_rate(hz=self.GPS_RAW_INT_REQUEST_HZ)
        self.request_basic_streams()
        self.print_px4_gps_checklist_once()
        if self.POST_HEARTBEAT_REQUEST_SETTLE_S > 0:
            time.sleep(self.POST_HEARTBEAT_REQUEST_SETTLE_S)
        return True

    def _accepted_autopilot_component(self, component_id: int) -> bool:
        configured = self.ACCEPTED_AUTOPILOT_COMPONENT_IDS
        if configured in (None, [], ()):
            return True
        try:
            accepted = {int(value) for value in configured}
        except TypeError:
            accepted = {int(configured)}
        return int(component_id) in accepted

    def request_vehicle_streams(
        self,
        vehicle: VehicleState,
        link: Optional[DroneLink] = None,
    ):
        """按飞行器 system/component 定向请求 GPS_RAW_INT，不改写其它飞行器状态。"""
        master = link.master if link is not None else self.master
        send_lock = link.send_lock if link is not None else self.mav_send_lock
        if not master:
            return
        try:
            msg_id = getattr(mavutil.mavlink, "MAVLINK_MSG_ID_GPS_RAW_INT", 24)
            cmd_id = getattr(mavutil.mavlink, "MAV_CMD_SET_MESSAGE_INTERVAL", 511)
            interval_us = int(1_000_000 / max(1, self.GPS_RAW_INT_REQUEST_HZ))
            with send_lock:
                master.mav.command_long_send(
                    vehicle.system_id,
                    vehicle.component_id,
                    cmd_id,
                    0,
                    msg_id,
                    interval_us,
                    0,
                    0,
                    0,
                    0,
                    0,
                )
                master.mav.request_data_stream_send(
                    vehicle.system_id,
                    vehicle.component_id,
                    mavutil.mavlink.MAV_DATA_STREAM_ALL,
                    5,
                    1,
                )
            print(f"[MAVLink] 已向飞机 {vehicle.key} 请求 GPS_RAW_INT / 基础数据流")
        except Exception as exc:
            print(f"\n[MAVLink] 请求飞机 {vehicle.key} 数据流失败: {exc}")

    def update_vehicle_armed_state(
        self,
        vehicle: VehicleState,
        is_armed: bool,
        now: Optional[float] = None,
        source: str = "HEARTBEAT",
    ):
        """记录真实 armed 状态；与“连续稳定10秒可解锁”指标分开。"""
        now = time.time() if now is None else now
        first_armed = False
        state_changed = False
        with self.lock:
            was_armed = bool(vehicle.armed)
            vehicle.armed = bool(is_armed)
            state_changed = was_armed != vehicle.armed
            if vehicle.armed:
                if state_changed:
                    vehicle.last_armed_time = now
                if vehicle.first_armed_time is None:
                    vehicle.first_armed_time = now
                    vehicle.first_armed_source = source
                    vehicle.burst_rounds_remaining = 0
                    first_armed = True
            elif state_changed:
                vehicle.last_disarmed_time = now

        if first_armed:
            print(
                f"\n[ActualArm] 飞机 {vehicle.key} 首次真实解锁 | "
                f"source={source} | heartbeat 到真实解锁="
                f"{now - vehicle.online_since:.1f}s"
            )
            self.append_actual_arm_result_csv(vehicle)
            self.write_status_json()
        elif state_changed:
            state_name = "ARMED" if is_armed else "DISARMED"
            print(f"\n[ArmState:{vehicle.key}] {state_name} | source={source}")

    def handle_early_boot_message(
        self,
        msg,
        link: Optional[DroneLink] = None,
    ) -> Optional[VehicleState]:
        """Track the PX4 boot identity and start the guarded CAN handshake early."""
        if not self.EARLY_BOOT_ASSIST_ENABLED:
            return None

        time_boot_ms = getattr(msg, "time_boot_ms", None)
        if time_boot_ms is None:
            return None
        try:
            boot_ms = int(time_boot_ms)
            boot_age_s = float(boot_ms) / 1000.0
        except (TypeError, ValueError):
            return None
        if boot_age_s < 0:
            return None
        fresh_boot = boot_age_s <= self.EARLY_BOOT_MAX_TIME_BOOT_S

        src_sys = int(msg.get_srcSystem())
        src_comp = int(msg.get_srcComponent())
        if src_sys in (0, 255) or not self._accepted_autopilot_component(src_comp):
            return None
        if link is not None and link.system_id is not None and src_sys != int(link.system_id):
            return None

        now = time.time()
        candidate_boot_time = now - boot_age_s
        key = (link.name, src_sys, src_comp) if link is not None else (src_sys, src_comp)
        is_new_session = False
        resumed_session = False
        reboot_detected = False
        with self.lock:
            vehicle = self.vehicles.get(key)
            if vehicle is None:
                if not fresh_boot:
                    return None
                self.vehicle_session_counter += 1
                vehicle = VehicleState(
                    system_id=src_sys,
                    component_id=src_comp,
                    mav_type=-1,
                    autopilot=mavutil.mavlink.MAV_AUTOPILOT_PX4,
                    session_id=self.vehicle_session_counter,
                    online=True,
                    online_since=now,
                    last_heartbeat=now,
                    last_message=now,
                )
                vehicle.link_name = link.name if link is not None else None
                vehicle.configured_vehicle_id = link.vehicle_id if link is not None else None
                vehicle.configured_system_id = link.system_id if link is not None else None
                vehicle.burst_rounds_remaining = self.BURST_ROUNDS
                vehicle.next_burst_time = now + self.POST_HEARTBEAT_REQUEST_SETTLE_S
                self.vehicles[key] = vehicle
                is_new_session = True
            else:
                same_boot = (
                    vehicle.estimated_boot_time is not None
                    and abs(candidate_boot_time - vehicle.estimated_boot_time) <= 5.0
                )
                newer_boot = (
                    vehicle.estimated_boot_time is not None
                    and candidate_boot_time > vehicle.estimated_boot_time + 5.0
                )
                stale_older_boot = (
                    vehicle.estimated_boot_time is not None
                    and candidate_boot_time < vehicle.estimated_boot_time - 5.0
                )
                if stale_older_boot:
                    return vehicle
                reboot_detected = newer_boot or (
                    vehicle.estimated_boot_time is None
                    and vehicle.last_time_boot_ms is not None
                    and boot_ms + 2000 < vehicle.last_time_boot_ms
                )

            if vehicle is not None and reboot_detected:
                self.vehicle_session_counter += 1
                vehicle.reset_for_new_session(
                    now,
                    session_id=self.vehicle_session_counter,
                    burst_rounds=self.BURST_ROUNDS,
                    settle_s=self.POST_HEARTBEAT_REQUEST_SETTLE_S,
                )
                vehicle.link_name = link.name if link is not None else vehicle.link_name
                vehicle.configured_vehicle_id = link.vehicle_id if link is not None else vehicle.configured_vehicle_id
                vehicle.configured_system_id = link.system_id if link is not None else vehicle.configured_system_id
                is_new_session = True
            elif vehicle is not None and not is_new_session and not vehicle.online:
                if same_boot:
                    vehicle.online = True
                    vehicle.last_heartbeat = now
                    vehicle.last_message = now
                    resumed_session = True
                elif not fresh_boot:
                    return vehicle
                else:
                    self.vehicle_session_counter += 1
                    vehicle.reset_for_new_session(
                        now,
                        session_id=self.vehicle_session_counter,
                        burst_rounds=self.BURST_ROUNDS,
                        settle_s=self.POST_HEARTBEAT_REQUEST_SETTLE_S,
                    )
                    vehicle.link_name = link.name if link is not None else vehicle.link_name
                    vehicle.configured_vehicle_id = link.vehicle_id if link is not None else vehicle.configured_vehicle_id
                    vehicle.configured_system_id = link.system_id if link is not None else vehicle.configured_system_id
                    is_new_session = True
            elif vehicle.heartbeat_seen:
                vehicle.last_time_boot_ms = boot_ms
                if vehicle.estimated_boot_time is None:
                    vehicle.estimated_boot_time = candidate_boot_time
                return vehicle

            vehicle.last_message = now
            vehicle.last_time_boot_ms = boot_ms
            if vehicle.estimated_boot_time is None or is_new_session:
                vehicle.estimated_boot_time = candidate_boot_time
            self.px4_detected = True
            if link is not None:
                self.system_to_link[src_sys] = link.name

        if not is_new_session and not resumed_session:
            return vehicle

        target_master = link.master if link is not None else self.master
        if target_master is not None:
            target_master.target_system = src_sys
            target_master.target_component = src_comp

        if resumed_session:
            print(
                f"\n[VehicleResume] 数据链重连仍属于同一次 PX4 启动: {vehicle.key}, "
                f"session={vehicle.session_id}, time_boot_ms={boot_ms}"
            )
        else:
            reason = "PX4 重启" if reboot_detected else "PX4 启动早期消息"
            print(
                f"\n[VehicleEarly] {reason}建立预备会话: {vehicle.key}, "
                f"session={vehicle.session_id}, time_boot_ms={boot_ms}"
            )
        self.set_engineering_state("VEHICLE_EARLY_BOOT")
        self.request_vehicle_streams(vehicle, link=link)
        self.reset_vehicle_can_param_scope(vehicle, link=link)
        self.request_vehicle_can_params(vehicle, link=link, force=True)
        return vehicle

    def handle_vehicle_heartbeat(
        self,
        msg,
        link: Optional[DroneLink] = None,
    ) -> Optional[VehicleState]:
        src_sys = int(msg.get_srcSystem())
        src_comp = int(msg.get_srcComponent())
        mav_type = int(getattr(msg, "type", -1))
        autopilot = int(getattr(msg, "autopilot", -1))
        base_mode = int(getattr(msg, "base_mode", 0) or 0)
        heartbeat_armed = bool(
            base_mode & mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED
        )

        if src_sys in (0, 255):
            reason = "invalid_or_gcs_system_id"
            self.heartbeat_reject_counts[reason] = (
                self.heartbeat_reject_counts.get(reason, 0) + 1
            )
            return None
        if mav_type == mavutil.mavlink.MAV_TYPE_GCS:
            reason = "mav_type_gcs"
            self.heartbeat_reject_counts[reason] = (
                self.heartbeat_reject_counts.get(reason, 0) + 1
            )
            return None
        if not self._accepted_autopilot_component(src_comp):
            reason = f"component_not_allowed:{src_comp}"
            self.heartbeat_reject_counts[reason] = (
                self.heartbeat_reject_counts.get(reason, 0) + 1
            )
            return None

        if link is not None and link.system_id is not None and src_sys != int(link.system_id):
            reason = f"system_id_mismatch:{link.name}:{src_sys}"
            self.heartbeat_reject_counts[reason] = (
                self.heartbeat_reject_counts.get(reason, 0) + 1
            )
            return None

        now = time.time()
        key = (link.name, src_sys, src_comp) if link is not None else (src_sys, src_comp)
        is_new_session = False
        first_real_heartbeat = False
        resumed_pending_boot_identity = False
        with self.lock:
            vehicle = self.vehicles.get(key)
            if vehicle is None:
                self.vehicle_session_counter += 1
                vehicle = VehicleState(
                    system_id=src_sys,
                    component_id=src_comp,
                    mav_type=mav_type,
                    autopilot=autopilot,
                    session_id=self.vehicle_session_counter,
                    online=True,
                    online_since=now,
                    last_heartbeat=now,
                    last_message=now,
                )
                vehicle.link_name = link.name if link is not None else None
                vehicle.configured_vehicle_id = link.vehicle_id if link is not None else None
                vehicle.configured_system_id = link.system_id if link is not None else None
                vehicle.burst_rounds_remaining = self.BURST_ROUNDS
                vehicle.next_burst_time = now + self.POST_HEARTBEAT_REQUEST_SETTLE_S
                vehicle.heartbeat_seen = True
                vehicle.first_heartbeat_time = now
                self.vehicles[key] = vehicle
                is_new_session = True
            elif not vehicle.online or now - vehicle.last_heartbeat > self.VEHICLE_OFFLINE_TIMEOUT_S:
                if vehicle.estimated_boot_time is not None:
                    # HEARTBEAT has no time_boot_ms. Preserve the session until the
                    # next ATTITUDE sample proves whether PX4 rebooted or only the
                    # transparent data link reconnected.
                    vehicle.online = True
                    vehicle.last_heartbeat = now
                    vehicle.last_message = now
                    vehicle.mav_type = mav_type
                    vehicle.autopilot = autopilot
                    vehicle.heartbeat_seen = True
                    resumed_pending_boot_identity = True
                else:
                    self.vehicle_session_counter += 1
                    vehicle.reset_for_new_session(
                        now,
                        session_id=self.vehicle_session_counter,
                        burst_rounds=self.BURST_ROUNDS,
                        settle_s=self.POST_HEARTBEAT_REQUEST_SETTLE_S,
                    )
                    vehicle.mav_type = mav_type
                    vehicle.autopilot = autopilot
                    vehicle.link_name = link.name if link is not None else vehicle.link_name
                    vehicle.configured_vehicle_id = link.vehicle_id if link is not None else vehicle.configured_vehicle_id
                    vehicle.configured_system_id = link.system_id if link is not None else vehicle.configured_system_id
                    vehicle.heartbeat_seen = True
                    vehicle.first_heartbeat_time = now
                    is_new_session = True
            else:
                first_real_heartbeat = not vehicle.heartbeat_seen
                vehicle.online = True
                vehicle.last_heartbeat = now
                vehicle.last_message = now
                vehicle.mav_type = mav_type
                vehicle.autopilot = autopilot
                vehicle.heartbeat_seen = True
                if vehicle.first_heartbeat_time is None:
                    vehicle.first_heartbeat_time = now
                if first_real_heartbeat:
                    vehicle.burst_rounds_remaining = max(
                        vehicle.burst_rounds_remaining,
                        self.BURST_ROUNDS,
                    )
                    vehicle.next_burst_time = now + self.POST_HEARTBEAT_REQUEST_SETTLE_S

            self.px4_detected = True
            if link is not None:
                self.system_to_link[src_sys] = link.name
                link.last_heartbeat_time = now
                link.last_message_time = now

        # 保留 pymavlink 的默认目标，仅用于旧兼容函数；多机命令使用显式目标。
        target_master = link.master if link is not None else self.master
        if target_master is not None:
            target_master.target_system = src_sys
            target_master.target_component = src_comp

        if is_new_session:
            print(
                f"\n[Vehicle] 飞机上线: {vehicle.key}, session={vehicle.session_id}, "
                f"type={mav_type}, autopilot={autopilot}"
            )
            self.set_engineering_state("VEHICLE_ONLINE")
            self.request_vehicle_streams(vehicle, link=link)
            self.reset_vehicle_can_param_scope(vehicle, link=link)
            self.request_vehicle_can_params(vehicle, link=link, force=True)
            self.enable_target_ack_and_navsat_via_px4(link=link)
        elif first_real_heartbeat:
            reference = vehicle.estimated_boot_time or vehicle.online_since
            print(
                f"\n[Vehicle] 预备会话收到首次正式 HEARTBEAT: {vehicle.key} | "
                f"启动参考到 heartbeat={now - reference:.3f}s"
            )
            self.request_vehicle_streams(vehicle, link=link)
            self.request_vehicle_can_params(vehicle, link=link, force=False)
        elif resumed_pending_boot_identity:
            print(
                f"\n[VehicleResume] HEARTBEAT 先到，暂保留会话 {vehicle.key} "
                f"session={vehicle.session_id}，等待 time_boot_ms 判定是重连还是重启"
            )
            self.request_vehicle_streams(vehicle, link=link)

        self.update_vehicle_armed_state(
            vehicle,
            is_armed=heartbeat_armed,
            now=now,
            source="HEARTBEAT",
        )
        return vehicle

    def find_vehicle_by_system(
        self,
        system_id: int,
        link: Optional[DroneLink] = None,
    ) -> Optional[VehicleState]:
        with self.lock:
            candidates = [
                vehicle for vehicle in self.vehicles.values()
                if vehicle.system_id == int(system_id)
                and (link is None or vehicle.link_name == link.name)
            ]
        if not candidates:
            return None
        candidates.sort(
            key=lambda vehicle: (
                vehicle.component_id != 1,
                not vehicle.online,
                -vehicle.last_heartbeat,
            )
        )
        return candidates[0]

    def refresh_vehicle_online_states(self):
        now = time.time()
        offline = []
        with self.lock:
            for vehicle in self.vehicles.values():
                if vehicle.online and now - vehicle.last_heartbeat > self.VEHICLE_OFFLINE_TIMEOUT_S:
                    vehicle.online = False
                    vehicle.burst_rounds_remaining = 0
                    offline.append(vehicle.key)

            expired = [
                key for key, vehicle in self.vehicles.items()
                if not vehicle.online and now - vehicle.last_heartbeat > self.VEHICLE_RETENTION_S
            ]
            for key in expired:
                self.vehicles.pop(key, None)

            self.px4_detected = any(vehicle.online for vehicle in self.vehicles.values())

        for key in offline:
            print(f"\n[Vehicle] 飞机离线: {key}")

    def online_unfixed_vehicles(self) -> List[VehicleState]:
        with self.lock:
            return [
                vehicle for vehicle in self.vehicles.values()
                if vehicle.online and not self.vehicle_quality_fix_ready(vehicle)
            ]

    def vehicle_quality_fix_ready(self, vehicle: VehicleState) -> bool:
        return (
            vehicle.fix_type >= self.ARMABLE_MIN_FIX_TYPE
            and vehicle.satellites >= self.MIN_QUALITY_FIX_SATS
        )

    def active_preflight_blocks_for_vehicle(
        self, vehicle: VehicleState, now: Optional[float] = None
    ) -> List[str]:
        now = time.time() if now is None else now
        stale_after = max(1.0, float(self.ARMABLE_PREFLIGHT_SILENCE_S))
        return sorted(
            text for text, seen_at in vehicle.active_preflight_blocks.items()
            if now - seen_at <= stale_after
        )

    def vehicle_armable_observed(self, vehicle: VehicleState, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        if not vehicle.online or not self.vehicle_quality_fix_ready(vehicle):
            return False
        active_blocks = self.active_preflight_blocks_for_vehicle(vehicle, now)
        if active_blocks:
            return False

        recent_position_s = max(2.0, min(10.0, self.ARMABLE_PREFLIGHT_SILENCE_S))
        if vehicle.last_estimator_status_time is None:
            return False
        if now - vehicle.last_estimator_status_time > recent_position_s:
            return False
        if vehicle.estimator_flags is None:
            return False
        if not (vehicle.estimator_flags & 16):
            return False
        if not (vehicle.estimator_flags & 32):
            return False
        if vehicle.last_local_position_time is None:
            return False
        if now - vehicle.last_local_position_time > recent_position_s:
            return False
        if vehicle.last_global_position_time is None:
            return False
        if now - vehicle.last_global_position_time > recent_position_s:
            return False

        if vehicle.sys_status_prearm_present and vehicle.sys_status_prearm_enabled:
            return bool(vehicle.sys_status_prearm_healthy)

        if vehicle.last_preflight_block_time is None:
            return False
        if now - vehicle.last_preflight_block_time < self.ARMABLE_PREFLIGHT_SILENCE_S:
            return False
        return True

    def vehicles_needing_postfix_assist(self, now: Optional[float] = None) -> List[VehicleState]:
        now = time.time() if now is None else now
        window_s = float(self.CONTINUE_INJECT_AFTER_3D_FIX_S)
        if window_s <= 0:
            return []
        with self.lock:
            vehicles = list(self.vehicles.values())
        result = []
        for vehicle in vehicles:
            if not vehicle.online:
                continue
            if not self.vehicle_quality_fix_ready(vehicle):
                continue
            if vehicle.first_fix_time is None:
                continue
            if vehicle.first_armed_time is not None:
                continue
            if self.vehicle_armable_observed(vehicle, now):
                continue
            if now - vehicle.first_fix_time > window_s:
                continue
            result.append(vehicle)
        return result

    def vehicle_should_receive_assist(self, vehicle: VehicleState, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        if not vehicle.online:
            return False
        if vehicle.first_armed_time is not None:
            return False
        if not self.vehicle_quality_fix_ready(vehicle):
            return True
        if self.vehicle_armable_observed(vehicle, now):
            return False
        if vehicle.first_fix_time is None:
            return False
        if self.CONTINUE_INJECT_AFTER_3D_FIX_S <= 0:
            return False
        return now - vehicle.first_fix_time <= self.CONTINUE_INJECT_AFTER_3D_FIX_S

    def any_vehicle_needs_assist(self, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        with self.lock:
            vehicles = list(self.vehicles.values())
        if vehicles:
            return any(self.vehicle_should_receive_assist(vehicle, now) for vehicle in vehicles)
        return (
            self.drone_fix_type < self.ARMABLE_MIN_FIX_TYPE
            or self.drone_sats < self.MIN_QUALITY_FIX_SATS
        )

    def mark_vehicle_armable_if_ready(self, vehicle: VehicleState, now: Optional[float] = None):
        now = time.time() if now is None else now
        if vehicle.first_armable_time is not None:
            return
        if not self.vehicle_armable_observed(vehicle, now):
            return
        with self.lock:
            if vehicle.first_armable_time is not None:
                return
            vehicle.first_armable_time = now
        print(
            f"\n[StableArmable] 飞机 {vehicle.key} 预检阻塞已静默 "
            f"{self.ARMABLE_PREFLIGHT_SILENCE_S:.1f}s | "
            f"上线参考到连续稳定可解锁观察={now - vehicle.online_since:.1f}s | "
            f"PX4启动到观察={now - (vehicle.estimated_boot_time or vehicle.online_since):.1f}s"
        )
        self.append_armable_result_csv(vehicle)
        self.write_status_json()

    def refresh_armable_observations(self):
        for vehicle in self.online_vehicles():
            self.mark_vehicle_armable_if_ready(vehicle)

    def online_vehicles(self) -> List[VehicleState]:
        with self.lock:
            return [vehicle for vehicle in self.vehicles.values() if vehicle.online]

    def sync_legacy_vehicle_summary(self):
        """供旧状态字段和控制台显示使用，不参与多机注入决策。"""
        with self.lock:
            online = [vehicle for vehicle in self.vehicles.values() if vehicle.online]
            self.gps_raw_count = sum(vehicle.gps_raw_count for vehicle in online)
            if online:
                self.drone_fix_type = min(vehicle.fix_type for vehicle in online)
                self.drone_sats = max(vehicle.satellites for vehicle in online)
            else:
                self.drone_fix_type = 0
                self.drone_sats = 0


    # ============================================================
    # MAVLink 独立接收线程：持续处理 HEARTBEAT / GPS_RAW_INT
    # ============================================================

    def start_mavlink_rx_thread(self):
        """
        启动 MAVLink 接收线程。
        主循环只负责发送 heartbeat 和 A-GNSS；接收线程负责第一时间处理
        PX4 heartbeat 和 GPS_RAW_INT，避免注入期间漏读或延迟处理。
        """
        if self.mav_rx_thread is not None and self.mav_rx_thread.is_alive():
            return

        self.mav_rx_running = True
        self.mav_rx_error = None
        self.mav_rx_thread = threading.Thread(
            target=self.mavlink_rx_worker,
            name="mavlink_rx_worker",
            daemon=True
        )
        self.mav_rx_thread.start()
        print("[MAVLinkRX] 接收线程已启动：持续监听 HEARTBEAT / GPS_RAW_INT。")

    def stop_mavlink_rx_thread(self):
        """停止 MAVLink 接收线程。"""
        self.mav_rx_running = False
        th = self.mav_rx_thread
        if th is not None and th.is_alive():
            try:
                th.join(timeout=1.0)
            except Exception:
                pass
        self.mav_rx_thread = None

    def mavlink_rx_worker(self):
        """
        MAVLink 接收线程。

        只在这里调用 recv_match()，主发送循环和注入函数不再读串口。
        这样可以保证 GPS_RAW_INT 一到达就被记录，不会因为正在发送 MGA
        数据而延迟处理。
        """
        print("[MAVLinkRX] 等待 PX4 HEARTBEAT / GPS_RAW_INT ...")

        while self.is_running and self.mav_rx_running:
            try:
                if self.master is None:
                    time.sleep(0.05)
                    continue

                msg = self.master.recv_match(blocking=True, timeout=0.2)
                if msg is None:
                    continue

                self.mav_last_msg_time = time.time()
                self.handle_mavlink_message(msg)

            except Exception as e:
                self.mav_rx_error = e
                if self.is_running and self.mav_rx_running:
                    print(f"\n[MAVLinkRX] 接收线程异常: {e}")
                break

        print("[MAVLinkRX] 接收线程退出。")

    def start_drone_link_rx_thread(self, link: DroneLink):
        """启动某条独立数据链的接收线程。"""
        if link.rx_thread is not None and link.rx_thread.is_alive():
            return
        link.rx_running = True
        link.rx_error = None
        link.rx_thread = threading.Thread(
            target=self.drone_link_rx_worker,
            args=(link,),
            name=f"mavlink_rx_{link.name}",
            daemon=True,
        )
        link.rx_thread.start()
        print(f"[MAVLinkRX:{link.name}] 接收线程已启动。")

    def drone_link_rx_worker(self, link: DroneLink):
        """单条独立数据链的 MAVLink 接收线程。"""
        print(f"[MAVLinkRX:{link.name}] 等待 HEARTBEAT / GPS_RAW_INT ...")

        while self.is_running and link.rx_running:
            try:
                if link.master is None:
                    time.sleep(0.05)
                    continue

                msg = link.master.recv_match(blocking=True, timeout=0.2)
                if msg is None:
                    continue

                now = time.time()
                link.last_message_time = now
                self.mav_last_msg_time = now
                self.handle_mavlink_message(msg, link=link)

            except Exception as exc:
                link.rx_error = exc
                link.last_error = str(exc)
                link.connected = False
                if self.is_running and link.rx_running:
                    print(f"\n[MAVLinkRX:{link.name}] 接收线程异常: {exc}")
                break

        link.rx_running = False
        print(f"[MAVLinkRX:{link.name}] 接收线程退出。")

    def handle_mavlink_message(self, msg, link: Optional[DroneLink] = None):
        """统一处理 MAVLink 消息。"""
        mtype = msg.get_type()
        src_sys = int(msg.get_srcSystem())
        src_comp = int(msg.get_srcComponent())
        now = time.time()
        count_key = f"{link.name}:{mtype}" if link is not None else mtype
        self.mav_message_counts[count_key] = self.mav_message_counts.get(count_key, 0) + 1
        source_key = f"{link.name}/{src_sys}:{src_comp}" if link is not None else f"{src_sys}:{src_comp}"
        source = self.mav_source_stats.setdefault(
            source_key,
            {
                "link_name": link.name if link is not None else None,
                "system_id": src_sys,
                "component_id": src_comp,
                "message_count": 0,
                "last_message_type": None,
                "last_seen": None,
            },
        )
        source["message_count"] = int(source["message_count"]) + 1
        source["last_message_type"] = mtype
        source["last_seen"] = dt.datetime.now().astimezone().isoformat(
            timespec="seconds"
        )

        if mtype in {"ATTITUDE", "ATTITUDE_QUATERNION"}:
            self.handle_early_boot_message(msg, link=link)

        if mtype == "HEARTBEAT":
            self.handle_vehicle_heartbeat(msg, link=link)
            return

        if mtype == "PARAM_VALUE":
            self.process_can_px4_param_value(msg, link=link)
            return

        if mtype == "GPS_RAW_INT":
            vehicle = self.find_vehicle_by_system(msg.get_srcSystem(), link=link)
            if vehicle is not None:
                self.process_gps_raw_int_msg(msg, vehicle, link=link)
            return

        if mtype == "STATUSTEXT":
            self.process_statustext_msg(msg, link=link)
            return

        vehicle = self.find_vehicle_by_system(msg.get_srcSystem(), link=link)
        if vehicle is not None:
            if mtype == "SYS_STATUS":
                self.process_sys_status_msg(msg, vehicle)
                return
            if mtype == "ESTIMATOR_STATUS":
                self.process_estimator_status_msg(msg, vehicle)
                return
            if mtype == "LOCAL_POSITION_NED":
                self.process_local_position_msg(vehicle)
                return
            if mtype == "GLOBAL_POSITION_INT":
                self.process_global_position_msg(vehicle)
                return
            with self.lock:
                vehicle.last_message = time.time()

    def append_experiment_csv(self):
        """3D Fix 后自动写入一行实验结果，方便后续统计 TCP/UDP/无线链路 TTFF。"""
        if self.experiment_csv_written:
            return
        self.experiment_csv_written = True
        path = Path("agnss_experiment_results.csv")
        is_new = not path.exists()
        try:
            with self.lock:
                eph_total = len(self.assist_to_send)

            row = {
                "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
                "script_version": SCRIPT_VERSION,
                "link_mode": self.LINK_MODE,
                "datalink_endpoint": self.DRONE_PORT,
                "eph_total": eph_total,
                "gps_eph": self.collector.gps.total_eph,
                "qzss_eph": self.collector.qzss.total_eph,
                "gal_eph": self.collector.gal.total_eph,
                "bds_eph": self.collector.bds.total_eph,
                "glo_eph": self.collector.glo.total_eph,
                "glo_seen": self.collector.glo_seen,
                "glo_decode_fail": self.collector.glo.decode_failed,
                "gps_alm": self.collector.gps_aux.total_alm,
                "gps_iono": self.collector.gps_aux.total_iono,
                "gps_utc": self.collector.gps_aux.total_utc,
                "gps_health": self.collector.gps_aux.total_health,
                "t_px4_heartbeat_s": None if self.px4_connected_time is None or self.gps_power_on_time is None else round(self.px4_connected_time - self.gps_power_on_time, 3),
                "t_first_gps_raw_s": None if self.first_gps_msg_time is None or self.gps_power_on_time is None else round(self.first_gps_msg_time - self.gps_power_on_time, 3),
                "t_first_inject_s": None if self.first_inject_time is None or self.gps_power_on_time is None else round(self.first_inject_time - self.gps_power_on_time, 3),
                "t_first_effective_inject_s": None if self.first_effective_inject_time is None or self.gps_power_on_time is None else round(self.first_effective_inject_time - self.gps_power_on_time, 3),
                "t_3d_fix_s": None if self.first_3d_fix_time is None or self.gps_power_on_time is None else round(self.first_3d_fix_time - self.gps_power_on_time, 3),
                "sats_at_fix": self.drone_sats,
                "gps_raw_count": self.gps_raw_count,
                "inject_packets": self.inject_packet_count,
                "inject_bytes": self.inject_byte_count,
                "inject_errors": self.inject_error_count,
                "pos_acc_cm": self.POS_ACC_CM,
                "time_acc_s": self.TIME_ACC_S,
                "first_fix_top_n": self.FIRST_FIX_TOP_N,
            }
            with path.open("a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                if is_new:
                    writer.writeheader()
                writer.writerow(row)
            print(f"[CSV] 已写入实验结果: {path}")
        except Exception as exc:
            print(f"\n[CSV] 写入实验结果失败: {exc}")

    def append_vehicle_result_csv(self, vehicle: VehicleState):
        if vehicle.csv_written:
            return
        vehicle.csv_written = True
        path = APP_DIR / "agnss_vehicle_results.csv"
        raw_ttff_s = (
            None if vehicle.first_raw_fix_time is None
            else round(vehicle.first_raw_fix_time - vehicle.online_since, 3)
        )
        quality_ttff_s = (
            None if vehicle.first_fix_time is None
            else round(vehicle.first_fix_time - vehicle.online_since, 3)
        )
        boot_reference = vehicle.estimated_boot_time or vehicle.online_since
        row = {
            "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
            "script_version": SCRIPT_VERSION,
            "system_id": vehicle.system_id,
            "component_id": vehicle.component_id,
            "session_id": vehicle.session_id,
            "online_since": dt.datetime.fromtimestamp(vehicle.online_since).isoformat(timespec="seconds"),
            "estimated_boot_time": (
                None if vehicle.estimated_boot_time is None
                else dt.datetime.fromtimestamp(vehicle.estimated_boot_time).isoformat(timespec="milliseconds")
            ),
            "time_boot_to_heartbeat_s": (
                None if vehicle.first_heartbeat_time is None
                else round(vehicle.first_heartbeat_time - boot_reference, 3)
            ),
            "time_boot_to_can_ready_s": (
                None if vehicle.first_can_ready_time is None
                else round(vehicle.first_can_ready_time - boot_reference, 3)
            ),
            "time_boot_to_first_inject_s": (
                None if vehicle.first_inject_time is None
                else round(vehicle.first_inject_time - boot_reference, 3)
            ),
            "time_boot_to_first_global_position_s": (
                None if vehicle.first_global_position_time is None
                else round(vehicle.first_global_position_time - boot_reference, 3)
            ),
            "ttff_s": quality_ttff_s,
            "sats_at_fix": vehicle.quality_fix_sats or vehicle.satellites,
            "raw_ttff_s": raw_ttff_s,
            "time_boot_to_raw_3d_s": (
                None if vehicle.first_raw_fix_time is None
                else round(vehicle.first_raw_fix_time - boot_reference, 3)
            ),
            "raw_sats_at_fix": vehicle.raw_fix_sats,
            "quality_ttff_s": quality_ttff_s,
            "time_boot_to_quality_fix_s": (
                None if vehicle.first_fix_time is None
                else round(vehicle.first_fix_time - boot_reference, 3)
            ),
            "quality_sats_at_fix": vehicle.quality_fix_sats,
            "min_quality_fix_sats": self.MIN_QUALITY_FIX_SATS,
            "gps_raw_count": vehicle.gps_raw_count,
            "link_mode": self.LINK_MODE,
            "datalink_endpoint": self.DRONE_PORT,
            "fresh_eph_count": self.fresh_assist_count(),
            "inject_packets_total": self.inject_packet_count,
            "inject_errors_total": self.inject_error_count,
        }
        try:
            fieldnames = list(row.keys())
            path, is_new = self.csv_path_for_schema(path, fieldnames)
            with path.open("a", encoding="utf-8", newline="") as file_obj:
                writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
                if is_new:
                    writer.writeheader()
                writer.writerow(row)
            print(f"[CSV] 已记录飞机 {vehicle.key} session={vehicle.session_id}: {path}")
        except Exception as exc:
            vehicle.csv_written = False
            print(f"\n[CSV] 写入飞机 {vehicle.key} 结果失败: {exc}")

    def csv_path_for_schema(self, path: Path, fieldnames: List[str]) -> Tuple[Path, bool]:
        if not path.exists():
            return path, True
        try:
            with path.open("r", encoding="utf-8", newline="") as file_obj:
                reader = csv.reader(file_obj)
                existing = next(reader, [])
        except Exception:
            existing = []
        if existing == fieldnames:
            return path, False
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        return path.with_name(f"{path.stem}_{stamp}{path.suffix}"), True

    def append_actual_arm_result_csv(self, vehicle: VehicleState):
        if vehicle.actual_arm_csv_written or vehicle.first_armed_time is None:
            return
        vehicle.actual_arm_csv_written = True
        path = APP_DIR / "agnss_actual_arm_results.csv"
        boot_reference = vehicle.estimated_boot_time or vehicle.online_since
        row = {
            "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
            "script_version": SCRIPT_VERSION,
            "system_id": vehicle.system_id,
            "component_id": vehicle.component_id,
            "session_id": vehicle.session_id,
            "online_since": dt.datetime.fromtimestamp(vehicle.online_since).isoformat(timespec="seconds"),
            "first_armed_time": dt.datetime.fromtimestamp(vehicle.first_armed_time).isoformat(timespec="seconds"),
            "first_armed_source": vehicle.first_armed_source,
            "time_to_actual_arm_s": round(vehicle.first_armed_time - vehicle.online_since, 3),
            "time_boot_to_actual_arm_s": round(vehicle.first_armed_time - boot_reference, 3),
            "time_boot_to_first_global_position_s": (
                None if vehicle.first_global_position_time is None
                else round(vehicle.first_global_position_time - boot_reference, 3)
            ),
            "raw_ttff_s": (
                None if vehicle.first_raw_fix_time is None
                else round(vehicle.first_raw_fix_time - vehicle.online_since, 3)
            ),
            "quality_ttff_s": (
                None if vehicle.first_fix_time is None
                else round(vehicle.first_fix_time - vehicle.online_since, 3)
            ),
            "quality_sats_at_fix": vehicle.quality_fix_sats,
            "fix_type": vehicle.fix_type,
            "satellites": vehicle.satellites,
            "eph_cm": vehicle.eph_cm,
            "epv_cm": vehicle.epv_cm,
            "last_preflight_block_text": vehicle.last_preflight_block_text,
            "stable_armable_observed_at_arm": bool(vehicle.first_armable_time is not None),
            "time_to_stable_armable_s_at_arm": (
                None if vehicle.first_armable_time is None
                else round(vehicle.first_armable_time - vehicle.online_since, 3)
            ),
            "fresh_eph_count": self.fresh_assist_count(),
            "fresh_almanac_count": self.fresh_almanac_count(),
            "almanac_sent_for_session": (
                vehicle.almanac_sent_session_id == vehicle.session_id
            ),
            "inject_packets_total": self.inject_packet_count,
            "inject_errors_total": self.inject_error_count,
        }
        try:
            fieldnames = list(row.keys())
            path, is_new = self.csv_path_for_schema(path, fieldnames)
            with path.open("a", encoding="utf-8", newline="") as file_obj:
                writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
                if is_new:
                    writer.writeheader()
                writer.writerow(row)
            print(f"[CSV] 已记录飞机 {vehicle.key} 真实解锁结果: {path}")
        except Exception as exc:
            vehicle.actual_arm_csv_written = False
            print(f"\n[CSV] 写入飞机 {vehicle.key} 真实解锁结果失败: {exc}")

    def append_armable_result_csv(self, vehicle: VehicleState):
        if vehicle.armable_csv_written:
            return
        vehicle.armable_csv_written = True
        path = APP_DIR / "agnss_armable_results.csv"
        raw_ttff_s = (
            None if vehicle.first_raw_fix_time is None
            else round(vehicle.first_raw_fix_time - vehicle.online_since, 3)
        )
        quality_ttff_s = (
            None if vehicle.first_fix_time is None
            else round(vehicle.first_fix_time - vehicle.online_since, 3)
        )
        boot_reference = vehicle.estimated_boot_time or vehicle.online_since
        row = {
            "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
            "script_version": SCRIPT_VERSION,
            "system_id": vehicle.system_id,
            "component_id": vehicle.component_id,
            "session_id": vehicle.session_id,
            "online_since": dt.datetime.fromtimestamp(vehicle.online_since).isoformat(timespec="seconds"),
            "estimated_boot_time": (
                None if vehicle.estimated_boot_time is None
                else dt.datetime.fromtimestamp(vehicle.estimated_boot_time).isoformat(timespec="milliseconds")
            ),
            "ttff_s": quality_ttff_s,
            "raw_ttff_s": raw_ttff_s,
            "raw_sats_at_fix": vehicle.raw_fix_sats,
            "quality_ttff_s": quality_ttff_s,
            "time_boot_to_quality_fix_s": (
                None if vehicle.first_fix_time is None
                else round(vehicle.first_fix_time - boot_reference, 3)
            ),
            "quality_sats_at_fix": vehicle.quality_fix_sats,
            "min_quality_fix_sats": self.MIN_QUALITY_FIX_SATS,
            "time_to_armable_s": (
                None if vehicle.first_armable_time is None
                else round(vehicle.first_armable_time - vehicle.online_since, 3)
            ),
            "time_to_stable_armable_s": (
                None if vehicle.first_armable_time is None
                else round(vehicle.first_armable_time - vehicle.online_since, 3)
            ),
            "time_boot_to_stable_armable_s": (
                None if vehicle.first_armable_time is None
                else round(vehicle.first_armable_time - boot_reference, 3)
            ),
            "actual_arm_observed": bool(vehicle.first_armed_time is not None),
            "time_to_actual_arm_s": (
                None if vehicle.first_armed_time is None
                else round(vehicle.first_armed_time - vehicle.online_since, 3)
            ),
            "first_armed_source": vehicle.first_armed_source,
            "fix_type": vehicle.fix_type,
            "satellites": vehicle.satellites,
            "eph_cm": vehicle.eph_cm,
            "epv_cm": vehicle.epv_cm,
            "gps_raw_count": vehicle.gps_raw_count,
            "last_preflight_block_text": vehicle.last_preflight_block_text,
            "sys_status_count": vehicle.sys_status_count,
            "sys_status_prearm_present": vehicle.sys_status_prearm_present,
            "sys_status_prearm_enabled": vehicle.sys_status_prearm_enabled,
            "sys_status_prearm_healthy": vehicle.sys_status_prearm_healthy,
            "first_prearm_healthy_s": (
                None if vehicle.first_prearm_healthy_time is None
                else round(vehicle.first_prearm_healthy_time - vehicle.online_since, 3)
            ),
            "link_mode": self.LINK_MODE,
            "datalink_endpoint": self.DRONE_PORT,
            "fresh_eph_count": self.fresh_assist_count(),
            "inject_packets_total": self.inject_packet_count,
            "inject_errors_total": self.inject_error_count,
        }
        try:
            fieldnames = list(row.keys())
            path, is_new = self.csv_path_for_schema(path, fieldnames)
            with path.open("a", encoding="utf-8", newline="") as file_obj:
                writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
                if is_new:
                    writer.writeheader()
                writer.writerow(row)
            print(f"[CSV] 已记录飞机 {vehicle.key} 可解锁观察结果: {path}")
        except Exception as exc:
            vehicle.armable_csv_written = False
            print(f"\n[CSV] 写入飞机 {vehicle.key} 可解锁结果失败: {exc}")

    def extract_statustext(self, msg) -> str:
        text = getattr(msg, "text", "")
        if isinstance(text, bytes):
            text = text.decode("utf-8", errors="ignore")
        return str(text).strip().strip("\x00")

    def match_preflight_block(self, text: str) -> Optional[str]:
        lower_text = text.lower()
        for pattern in self.PREFLIGHT_BLOCK_PATTERNS:
            if pattern.lower() in lower_text:
                return pattern
        return None

    def statustext_is_relevant_for_log(self, text: str) -> bool:
        lower_text = text.lower()
        keywords = (
            "preflight",
            "position",
            "gps",
            "ekf",
            "estimate",
            "local",
            "global",
            "heading",
            "arm",
            "mode",
            "takeoff",
            "mission",
            "magnetic",
            "compass",
        )
        return any(keyword in lower_text for keyword in keywords)

    def process_statustext_msg(self, msg, link: Optional[DroneLink] = None):
        text = self.extract_statustext(msg)
        if not text:
            return

        now = time.time()
        vehicle = self.find_vehicle_by_system(msg.get_srcSystem(), link=link)
        if vehicle is None:
            online = self.online_vehicles()
            if len(online) == 1:
                vehicle = online[0]
        if vehicle is None:
            return

        lower_text = text.lower()
        if lower_text.startswith("disarmed by") or lower_text == "disarmed":
            self.update_vehicle_armed_state(
                vehicle,
                is_armed=False,
                now=now,
                source="STATUSTEXT",
            )
        elif lower_text.startswith("armed by") or lower_text == "armed":
            self.update_vehicle_armed_state(
                vehicle,
                is_armed=True,
                now=now,
                source="STATUSTEXT",
            )

        block = self.match_preflight_block(text)
        with self.lock:
            vehicle.last_message = now
            vehicle.last_statustext_time = now
            vehicle.last_statustext_text = text
            if block is not None:
                vehicle.last_preflight_block_time = now
                vehicle.last_preflight_block_text = text
                vehicle.active_preflight_blocks[block] = now

        if block is not None:
            print(f"\n[Preflight:{vehicle.key}] {text}")
            self.write_status_json()
        elif self.statustext_is_relevant_for_log(text):
            print(f"\n[STATUSTEXT:{vehicle.key}] {text}")

    def process_sys_status_msg(self, msg, vehicle: VehicleState):
        now = time.time()
        present = int(getattr(msg, "onboard_control_sensors_present", 0) or 0)
        enabled = int(getattr(msg, "onboard_control_sensors_enabled", 0) or 0)
        health = int(getattr(msg, "onboard_control_sensors_health", 0) or 0)
        prearm_present = bool(present & MAV_SYS_STATUS_PREARM_CHECK)
        prearm_enabled = bool(enabled & MAV_SYS_STATUS_PREARM_CHECK)
        prearm_healthy = bool(health & MAV_SYS_STATUS_PREARM_CHECK)
        prearm_ready = prearm_present and prearm_enabled and prearm_healthy
        with self.lock:
            vehicle.last_message = now
            vehicle.sys_status_count += 1
            vehicle.last_sys_status_time = now
            vehicle.sys_status_present_flags = present
            vehicle.sys_status_enabled_flags = enabled
            vehicle.sys_status_health_flags = health
            vehicle.sys_status_prearm_present = prearm_present
            vehicle.sys_status_prearm_enabled = prearm_enabled
            vehicle.sys_status_prearm_healthy = prearm_healthy
            first_prearm_healthy = (
                prearm_ready
                and vehicle.first_prearm_healthy_time is None
            )
            if first_prearm_healthy:
                vehicle.first_prearm_healthy_time = now

        if first_prearm_healthy:
            print(
                f"\n[Prearm:{vehicle.key}] SYS_STATUS prearm healthy | "
                f"heartbeat 到 prearm healthy={now - vehicle.online_since:.1f}s"
            )
        self.mark_vehicle_armable_if_ready(vehicle, now)

    def process_estimator_status_msg(self, msg, vehicle: VehicleState):
        now = time.time()
        flags = getattr(msg, "flags", None)
        pos_horiz_ratio = getattr(msg, "pos_horiz_ratio", None)
        pos_vert_ratio = getattr(msg, "pos_vert_ratio", None)
        pos_horiz_accuracy = getattr(msg, "pos_horiz_accuracy", None)
        pos_vert_accuracy = getattr(msg, "pos_vert_accuracy", None)
        with self.lock:
            vehicle.last_message = now
            vehicle.estimator_status_count += 1
            vehicle.estimator_flags = None if flags is None else int(flags)
            vehicle.estimator_pos_horiz_ratio = (
                None if pos_horiz_ratio is None else round(float(pos_horiz_ratio), 3)
            )
            vehicle.estimator_pos_vert_ratio = (
                None if pos_vert_ratio is None else round(float(pos_vert_ratio), 3)
            )
            vehicle.estimator_pos_horiz_accuracy_m = (
                None if pos_horiz_accuracy is None else round(float(pos_horiz_accuracy), 3)
            )
            vehicle.estimator_pos_vert_accuracy_m = (
                None if pos_vert_accuracy is None else round(float(pos_vert_accuracy), 3)
            )
            vehicle.last_estimator_status_time = now
        self.mark_vehicle_armable_if_ready(vehicle, now)

    def process_local_position_msg(self, vehicle: VehicleState):
        now = time.time()
        with self.lock:
            vehicle.last_message = now
            vehicle.local_position_count += 1
            vehicle.last_local_position_time = now
        self.mark_vehicle_armable_if_ready(vehicle, now)

    def process_global_position_msg(self, vehicle: VehicleState):
        now = time.time()
        with self.lock:
            vehicle.last_message = now
            vehicle.global_position_count += 1
            if vehicle.first_global_position_time is None:
                vehicle.first_global_position_time = now
            vehicle.last_global_position_time = now
        self.mark_vehicle_armable_if_ready(vehicle, now)

    def process_gps_raw_int_msg(
        self,
        msg,
        vehicle: Optional[VehicleState] = None,
        link: Optional[DroneLink] = None,
    ):
        """处理一架飞机的 GPS_RAW_INT，不覆盖其它飞机状态。"""
        now = time.time()

        try:
            sats = int(msg.satellites_visible)
            fix_type = int(msg.fix_type)
        except Exception:
            return

        if sats == 255:
            sats = 0

        if vehicle is None:
            vehicle = self.find_vehicle_by_system(msg.get_srcSystem(), link=link)
        if vehicle is None:
            return

        eph = getattr(msg, "eph", None)
        epv = getattr(msg, "epv", None)
        lat = getattr(msg, "lat", None)
        lon = getattr(msg, "lon", None)
        alt = getattr(msg, "alt", None)
        vel = getattr(msg, "vel", None)
        cog = getattr(msg, "cog", None)
        h_acc = getattr(msg, "h_acc", None)
        v_acc = getattr(msg, "v_acc", None)
        yaw = getattr(msg, "yaw", None)

        with self.lock:
            vehicle.last_message = now
            vehicle.gps_raw_count += 1
            vehicle.satellites = sats
            vehicle.fix_type = fix_type
            vehicle.eph_cm = None if eph is None or int(eph) == 65535 else int(eph)
            vehicle.epv_cm = None if epv is None or int(epv) == 65535 else int(epv)
            vehicle.gps_lat_raw = None if lat is None else int(lat)
            vehicle.gps_lon_raw = None if lon is None else int(lon)
            vehicle.gps_alt_mm = None if alt is None else int(alt)
            vehicle.gps_vel_cms = None if vel is None or int(vel) == 65535 else int(vel)
            vehicle.gps_cog_cdeg = None if cog is None or int(cog) == 65535 else int(cog)
            vehicle.gps_h_acc_mm = None if h_acc is None or int(h_acc) == 0xFFFFFFFF else int(h_acc)
            vehicle.gps_v_acc_mm = None if v_acc is None or int(v_acc) == 0xFFFFFFFF else int(v_acc)
            vehicle.gps_yaw_cdeg = None if yaw is None or int(yaw) == 0 else int(yaw)
            vehicle.last_fix_update = now
            vehicle.last_gps_quality_update = now
            is_first_gps = vehicle.first_gps_time is None
            if is_first_gps:
                vehicle.first_gps_time = now
            first_raw_fix = fix_type >= 3 and vehicle.first_raw_fix_time is None
            quality_fix = (
                fix_type >= self.ARMABLE_MIN_FIX_TYPE
                and sats >= self.MIN_QUALITY_FIX_SATS
            )
            first_quality_fix = quality_fix and vehicle.first_fix_time is None
            if first_raw_fix:
                vehicle.first_raw_fix_time = now
                vehicle.raw_fix_sats = sats
            if first_quality_fix:
                vehicle.first_fix_time = now
                vehicle.quality_fix_sats = sats
                vehicle.burst_rounds_remaining = 0
            needs_recovery_alert = (
                first_quality_fix
                and vehicle.alert_level in ("WARNING", "CRITICAL")
                and not vehicle.recovery_alert_sent
            )

        if is_first_gps:
            print(
                f"\n[GPS] 飞机 {vehicle.key} 首次 GPS_RAW_INT | "
                f"fix={self.fix_type_name(fix_type)}, sats={sats}, "
                f"上线后 {now - vehicle.online_since:.1f}s"
            )

        if first_raw_fix:
            boot_reference = vehicle.estimated_boot_time or vehicle.online_since
            print(
                f"\n[TTFF] 飞机 {vehicle.key} 首次 3D Fix(raw) | "
                f"sats={sats}/{self.MIN_QUALITY_FIX_SATS} | "
                f"上线参考到 raw 3D={now - vehicle.online_since:.1f}s | "
                f"PX4启动到 raw 3D={now - boot_reference:.1f}s"
            )

        if first_quality_fix:
            boot_reference = vehicle.estimated_boot_time or vehicle.online_since
            print(
                f"\n[QualityFix] 飞机 {vehicle.key} 定位质量达标 | "
                f"fix={self.fix_type_name(fix_type)}, sats={sats}/{self.MIN_QUALITY_FIX_SATS} | "
                f"上线参考到质量定位={now - vehicle.online_since:.1f}s | "
                f"PX4启动到质量定位={now - boot_reference:.1f}s"
            )
            self.append_vehicle_result_csv(vehicle)
            if needs_recovery_alert:
                event_id = self.emit_vehicle_alert(
                    vehicle,
                    event_type="GNSS_FIX_RECOVERED",
                    severity="INFO",
                    message=(
                        f"飞机 {vehicle.key} 已达到质量定位，"
                        f"总定位耗时 {now - vehicle.online_since:.1f} 秒。"
                    ),
                    decision_required=False,
                )
                with self.lock:
                    vehicle.recovery_alert_sent = True
                    vehicle.alert_level = "RECOVERED"
                    vehicle.last_alert_event_id = event_id
                    vehicle.last_alert_time = now
                    vehicle.alert_count += 1
                self.write_alert_state_json()

        self.sync_legacy_vehicle_summary()
        self.mark_vehicle_armable_if_ready(vehicle, now)
        self.write_status_json()

    def main_loop(self):
        """
        连续数据链 + 独立 MAVLink 接收线程测试流程：
        1. 先启动基准站 GPS 线程，缓存 PVT 和多星座星历；
        2. 基准站准备完成后，打开数传串口；
        3. 启动 MAVLink 接收线程，持续监听 HEARTBEAT / GPS_RAW_INT；
        4. 主循环持续发送 GCS heartbeat 和 A-GNSS 数据；
        5. 第一次检测到 PX4 heartbeat 时，将该时刻作为 T0；
        6. heartbeat 后立即执行 3 轮 Top-N burst；
        7. 3D Fix 前每 2 秒继续重复 Top-N 注入；
        8. 接收线程第一时间记录首次 GPS_RAW_INT 和首次 3D Fix。
        """
        threading.Thread(target=self.base_worker, daemon=True).start()
        self.start_ack_monitor_if_enabled()

        print("\n[System] 脚本已启动：连续数据链 A-GNSS 测试模式 + MAVLink独立接收线程。")
        print("[System] 先采集基准站 GPS/QZSS/Galileo/BDS 星历，暂不要求飞控上电。")
        print("[System] 基准站准备完成后，将打开数传端口，并持续发送 heartbeat；A-GNSS 在 heartbeat 前仅低频预发送。")
        print("[System] MAVLink 接收线程会持续监听 HEARTBEAT / GPS_RAW_INT，避免注入期间漏读。")

        # -----------------------------
        # 1. 等待基准站准备完成
        # -----------------------------
        last_tip_time = 0.0
        min_used_reached_time = None
        min_total_eph_reached_time = None
        low_eph_reported = False

        print(
            f"[Config] 星历等待门限：理想有效EPH={self.DESIRED_EPH_BEFORE_POWER_ON}，"
            f"最低有效EPH={self.MIN_EPH_BEFORE_POWER_ON}。"
        )
        print(
            f"[Config] 逻辑：优先等待12条有效可注入星历；如果长时间不足12条，"
            f"达到最低6条后再等待 {self.MAX_WAIT_AFTER_MIN_EPH:.0f}s 后进入注入；"
            f"如果一直不足6条，则只提示基准站星历不足，不进入注入。"
        )
        print(f"[Config] GLONASS MGA封装启用状态：{self.ENABLE_GLO_MGA_EXPERIMENTAL}。当前稳定版默认只统计GLO，不解析封装。")
        print(
            f"[Config] GPS长期辅助：enabled={self.ENABLE_ALM_IONO_UTC_EXPERIMENTAL}, "
            f"ALM有效期={self.MAX_ALM_AGE_S / 86400.0:.1f}天, "
            f"UTC/IONO/HEALTH有效期={self.MAX_AUX_AGE_S / 86400.0:.1f}天, "
            f"ALM重发间隔={self.ALMANAC_REINJECT_INTERVAL_S:.0f}s。"
        )
        print(f"[Config] 每轮最大注入星历数：{self.FIRST_FIX_TOP_N}。若当前有效EPH小于该值，则注入全部有效EPH；若大于该值，则按优先级注入Top-{self.FIRST_FIX_TOP_N}。")

        while self.is_running:
            with self.lock:
                eph_count = len(self.assist_to_send)

            has_valid_pvt = self.last_pvt is not None
            used_eph_count, used_eph_keys = self.count_base_used_injectable()
            now = time.time()
            elapsed_since_start = now - self.script_start_time

            # 记录最低总 EPH 门槛首次达到时间。
            reached_min_total = has_valid_pvt and eph_count >= self.MIN_EPH_BEFORE_POWER_ON
            if reached_min_total and min_total_eph_reached_time is None:
                min_total_eph_reached_time = now
                print(
                    f"\n[System] 已达到最低有效星历门限："
                    f"EPH={eph_count}/{self.MIN_EPH_BEFORE_POWER_ON}，"
                    f"BaseUsedInjectable={used_eph_count}/{self.DESIRED_USED_EPH_BEFORE_POWER_ON}。"
                    f"继续等待理想门限 EPH={self.DESIRED_EPH_BEFORE_POWER_ON}，"
                    f"最多再等 {self.MAX_WAIT_AFTER_MIN_EPH:.0f} 秒。"
                )

            # 理想条件：有效可注入星历达到 12 条。
            # 这里看的是 assist_to_send 总数，而不是硬要求 BaseUsedInjectable=12。
            # 原因：当前稳定版暂不封装 GLONASS，基准站 used 卫星中若 GLO 占比较高，
            # BaseUsedInjectable 可能偏低，但 GPS/GAL/BDS/QZSS 的有效 EPH 仍可用于注入。
            ready_by_desired = (
                has_valid_pvt
                and eph_count >= self.DESIRED_EPH_BEFORE_POWER_ON
            )

            # 最低条件：长时间达不到 12 条时，至少要有 6 条有效星历，
            # 并且达到 6 条后再等待一段时间，才允许进入注入。
            ready_by_min_total_timeout = (
                has_valid_pvt
                and min_total_eph_reached_time is not None
                and now - min_total_eph_reached_time >= self.MAX_WAIT_AFTER_MIN_EPH
                and eph_count >= self.MIN_EPH_BEFORE_POWER_ON
            )

            if ready_by_desired or ready_by_min_total_timeout:
                if ready_by_desired:
                    reason = "达到12条理想有效可注入星历门限"
                else:
                    reason = "长时间不足12条，达到最低6条有效星历后进入注入"

                print(
                    f"\n\n[System] ✅ 基准站准备完成：{reason}，"
                    f"已缓存 {eph_count} 条辅助星历，"
                    f"其中基准站used且可注入 {used_eph_count} 条：{used_eph_keys[:12]}，"
                    f"且已有有效 PVT。"
                )

                if ready_by_min_total_timeout and not ready_by_desired:
                    print(
                        "[System] ⚠️ 当前未达到12条理想星历，属于最低门槛测试："
                        f"GPS_EPH={self.collector.gps.total_eph}, "
                        f"QZSS_EPH={self.collector.qzss.total_eph}, "
                        f"GAL_EPH={self.collector.gal.total_eph}, "
                        f"BDS_EPH={self.collector.bds.total_eph}, "
                        f"GLO_EPH={self.collector.glo.total_eph}, "
                        f"GLO_SEEN={self.collector.glo_seen}, "
                        f"GLO_DECODE_FAIL={self.collector.glo.decode_failed}。"
                    )
                break

            # 如果一直达不到最低 6 条，只提示，不进入注入。
            if (has_valid_pvt and eph_count < self.MIN_EPH_BEFORE_POWER_ON
                    and elapsed_since_start >= self.BASE_EPH_INSUFFICIENT_WARN_S
                    and now - last_tip_time > 15):
                print(
                    f"\n[WaitBase] ⚠️ 基准站星历不足：当前有效EPH={eph_count}/{self.MIN_EPH_BEFORE_POWER_ON}，"
                    f"还不能进入注入阶段。请继续等待或检查基准站 BDS/GPS/QZSS/GAL SFRBX 输出。"
                )

            # 等待状态输出
            wait_extra_used = 0.0 if min_used_reached_time is None else now - min_used_reached_time
            wait_extra_total = 0.0 if min_total_eph_reached_time is None else now - min_total_eph_reached_time
            print(
                f"\r[WaitBase] 等待基准站准备完成 | "
                f"EPH={eph_count}/{self.DESIRED_EPH_BEFORE_POWER_ON} "
                f"(最低{self.MIN_EPH_BEFORE_POWER_ON}, 总EPH已等{wait_extra_total:.0f}s) | "
                f"BaseUsedInjectable={used_eph_count}/{self.DESIRED_USED_EPH_BEFORE_POWER_ON} "
                f"(用于排序/统计) | "
                f"PVT={'OK' if has_valid_pvt else 'NO'} | "
                f"{self.formatted_pvt[:55]}",
                end=""
            )

            if elapsed_since_start > 120 and eph_count < self.MIN_EPH_BEFORE_POWER_ON and not low_eph_reported:
                low_eph_reported = True
                print(
                    f"\n[WaitBase] ⚠️ 已等待超过120秒，但有效EPH仍不足最低门槛："
                    f"EPH={eph_count}/{self.MIN_EPH_BEFORE_POWER_ON}。"
                    "如果 GLO_SEEN 很高但 GLO_EPH=0，说明当前稳定版只统计GLONASS，不把它作为注入门槛；"
                    "脚本不会在EPH不足6条时进入注入。"
                )

            if now - last_tip_time > 15:
                print(
                    f"\n[WaitBase] RawSFRBX={self.collector.raw_sfrbx}, "
                    f"GPS_EPH={self.collector.gps.total_eph}, "
                    f"QZSS_EPH={self.collector.qzss.total_eph}, "
                    f"GAL_EPH={self.collector.gal.total_eph}, "
                    f"BDS_EPH={self.collector.bds.total_eph}, "
                    f"GLO_EPH={self.collector.glo.total_eph}, "
                    f"QZSS_L1CA_SFRBX={self.collector.qzss.total_sfrbx}, "
                    f"QZSS_L1S_SEEN={self.collector.qzss_l1s_seen}, "
                    f"BDS_UNSUPPORTED={self.collector.bds.unsupported_bds}, "
                    f"BDS_SEEN={self.collector.bds_seen}, "
                    f"GAL_SEEN={self.collector.gal_seen}, "
                    f"GLO_SEEN={self.collector.glo_seen}, "
                    f"GLO_DECODE_FAIL={self.collector.glo.decode_failed} | "
                    f"{self.collector.sfrbx_stat_string()[:130]} | "
                    f"UsedInjectable={used_eph_keys[:12]}"
                )
                last_tip_time = now

            time.sleep(1.0)

        # -----------------------------
        # 2. 打开数传串口，启动接收线程，主循环只负责发送
        # -----------------------------
        print(f"\n[DataLink] 打开数传端口：{self.DRONE_PORT}, baud={self.BAUD_DRONE}")
        print("[DataLink] 之后脚本会持续发送 GCS heartbeat 和 A-GNSS 数据。")
        print("[DataLink] 现在可以给飞控/无人机GPS上电；第一次检测到 PX4 heartbeat 时开始计时。")

        while self.is_running:
            try:
                self.close_mavlink_connection()

                # 每轮重新打开串口前重置本轮 MAVLink 状态
                self.px4_detected = False
                self.heartbeat_burst_done = False
                self.mav_rx_error = None
                self.gps_raw_count = 0
                self.drone_sats = 0
                self.drone_fix_type = 0
                self.last_reported_sats = -1
                self.last_reported_fix = -1

                self.master = self.create_mavlink_connection()

                self.start_mavlink_rx_thread()

                last_pre_hb = 0.0
                last_hb = 0.0
                last_print = 0.0
                last_tcp_hint_time = time.time()
                last_inject = 0.0

                while self.is_running:
                    now = time.time()

                    if self.mav_rx_error is not None:
                        raise RuntimeError(f"MAVLink接收线程异常: {self.mav_rx_error}")

                    if (self.px4_detected and self.mav_last_msg_time is not None and
                            now - self.mav_last_msg_time > self.DATALINK_LOSS_TIMEOUT_S):
                        raise RuntimeError(
                            f"超过 {self.DATALINK_LOSS_TIMEOUT_S:.1f}s 未收到 MAVLink 消息，判定数据链/PX4离线，准备重连"
                        )

                    # PX4 已上线但没有 GPS_RAW_INT 时，给出明确工程告警。
                    if self.px4_detected and self.px4_connected_time is not None:
                        online_age = now - self.px4_connected_time
                        if self.gps_raw_count == 0 and online_age > 10 and not self.gps_no_msg_warned:
                            self.gps_no_msg_warned = True
                            print(
                                "\n[Warn] PX4 已上线超过10秒，但未收到 GPS_RAW_INT。"
                                "请检查 PX4 GPS 参数、GPS供电/接线、MAVLink stream 或运行 listener sensor_gps。"
                            )
                        if self.gps_raw_count > 0 and self.drone_sats == 0 and online_age > 10 and not self.gps_zero_sats_warned:
                            self.gps_zero_sats_warned = True
                            print(
                                "\n[Warn] 已收到 GPS_RAW_INT，但 satellites_visible=0。"
                                "请检查 NEO3 天线、供电、协议输出和 PX4 GPS 驱动状态。"
                            )

                    # tcpin 模式下长时间没有 heartbeat 时，提示检查 TCP ESTABLISHED。
                    if (not self.px4_detected and now - last_tcp_hint_time > 12.0):
                        self.print_tcppin_hint_once()
                        last_tcp_hint_time = now

                    # 主循环持续主动发送 GCS heartbeat，相当于持续“握手请求”。
                    if now - last_hb >= self.PRE_HEARTBEAT_HEARTBEAT_INTERVAL:
                        self.send_gcs_heartbeat()
                        last_hb = now

                    # PX4 heartbeat 出现前：默认只发送 GCS heartbeat。
                    # 依据你的电气图，机巢内数据链可长期上电，但飞控/GPS断电时预发送 A-GNSS 没有实际注入意义。
                    if not self.px4_detected:
                        if (self.PRE_HEARTBEAT_INJECT_ENABLED and
                                self.any_vehicle_needs_assist(now) and
                                now - last_pre_hb >= self.PRE_HEARTBEAT_INJECT_INTERVAL):
                            self.preheartbeat_inject_once()
                            last_pre_hb = now

                    # 接收线程检测到 PX4 heartbeat 后，主循环执行 burst 和后续重复注入。
                    if self.px4_detected and not self.heartbeat_burst_done:
                        if self.EFFECTIVE_BURST_AFTER_HEARTBEAT:
                            print("\n[System] 已检测到 PX4，执行 heartbeat 后快速 Top-N burst 有效注入。")
                            self.effective_burst_after_heartbeat()
                        else:
                            print("\n[System] 已检测到 PX4，立即执行首次 A-GNSS Top-N 注入。")
                            self.inject_sequence_first_fix_fast()

                        self.heartbeat_burst_done = True
                        last_inject = time.time()

                    self.refresh_armable_observations()

                    # heartbeat 后，直到 3D Fix 后观察窗口结束/预检阻塞消失前继续重复 Top-N 注入。
                    if self.px4_detected and self.any_vehicle_needs_assist(now):
                        if now - last_inject > self.INJECT_INTERVAL_BEFORE_FIX:
                            self.inject_sequence_first_fix_fast()
                            last_inject = time.time()

                    # 控制台状态输出
                    if now - last_print > 1.0:
                        with self.lock:
                            eph_count = len(self.assist_to_send)

                        if not self.px4_detected:
                            print(
                                f"\r[Standby] 数据链待机，持续发送 heartbeat；"
                                f"尚未检测到 PX4 heartbeat | EPH={eph_count} | "
                                f"基准站: {self.formatted_pvt[:35]}",
                                end=""
                            )
                        else:
                            inject_state = (
                                "Top-N注入中"
                                if self.any_vehicle_needs_assist(now)
                                else "已停止注入"
                            )
                            print(
                                f"\r[Live] PX4 GPS fix={self.drone_fix_type}, "
                                f"sats={self.drone_sats:2d} | "
                                f"{self.ttff_elapsed_text()} | "
                                f"{inject_state} | "
                                f"GPS_RAW_INT={self.gps_raw_count} | "
                                f"基准站: {self.formatted_pvt[:35]} | "
                                f"EPH={eph_count}",
                                end=""
                            )

                        # 3D Fix后继续观察：目标GPS实际used卫星达到阈值时给出一次明确提示。
                        if (self.ack_monitor is not None and
                                not self.target_used_threshold_reported and
                                len(self.ack_monitor.latest_used_keys) >= self.TARGET_USED_SATS_THRESHOLD):
                            self.target_used_threshold_reported = True
                            self.ack_monitor.print_target_used_snapshot(
                                f"目标GPS used卫星已达到 {self.TARGET_USED_SATS_THRESHOLD} 颗，可以开始下一步操作"
                            )

                        self.save_assist_db(force=False)
                        self.write_status_json()
                        last_print = now

                    time.sleep(0.02)

            except Exception as e:
                print(f"\n[DataLink] 异常: {e}")
                print("[DataLink] 3 秒后重新打开数传端口并继续待机。")
                self.stop_mavlink_rx_thread()
                try:
                    if self.master is not None:
                        self.master.close()
                except Exception:
                    pass
                self.master = None
                time.sleep(3)

    def mark_all_vehicles_offline(self, reason: str):
        changed = []
        with self.lock:
            for vehicle in self.vehicles.values():
                if vehicle.online:
                    vehicle.online = False
                    vehicle.burst_rounds_remaining = 0
                    changed.append(vehicle.key)
            self.px4_detected = False
        if changed:
            print(f"\n[Vehicle] 因{reason}标记离线: {', '.join(changed)}")

    def mark_link_vehicles_offline(self, link: DroneLink, reason: str):
        changed = []
        with self.lock:
            for vehicle in self.vehicles.values():
                if vehicle.link_name == link.name and vehicle.online:
                    vehicle.online = False
                    vehicle.burst_rounds_remaining = 0
                    changed.append(vehicle.key)
            self.px4_detected = any(vehicle.online for vehicle in self.vehicles.values())
        if changed:
            print(f"\n[Vehicle] 因{link.name} {reason}标记离线: {', '.join(changed)}")

    def maintain_drone_links(self):
        """多独立数据链模式下，逐条维护连接和接收线程。"""
        if not self.multi_link_enabled():
            return
        now = time.time()
        for link in self.drone_links:
            if not link.enabled:
                continue
            needs_open = link.master is None or not link.connected
            rx_failed = link.rx_error is not None or (
                link.rx_thread is not None and not link.rx_thread.is_alive() and link.rx_running
            )
            if rx_failed:
                needs_open = True
            if needs_open:
                if link.last_open_time is not None and now - link.last_open_time < 3.0:
                    continue
                self.mark_link_vehicles_offline(link, "链路重连")
                self.open_drone_link(link)

    def update_service_state(self):
        with self.lock:
            has_critical_fix_timeout = any(
                vehicle.online
                and not self.vehicle_quality_fix_ready(vehicle)
                and vehicle.alert_level == "CRITICAL"
                for vehicle in self.vehicles.values()
            )
        if has_critical_fix_timeout:
            self.set_engineering_state("FIX_TIMEOUT_CRITICAL")
            return
        if not self.base_pvt_is_fresh() or not self.base_sfrbx_is_fresh():
            self.set_engineering_state("BASE_DEGRADED")
            return
        online = self.online_vehicles()
        if not online:
            self.set_engineering_state("STANDBY")
        elif any(not self.vehicle_quality_fix_ready(vehicle) for vehicle in online):
            self.set_engineering_state("ASSISTING")
        else:
            self.set_engineering_state("ONLINE_FIXED_BACKGROUND_BROADCAST")

    def run_service_loop(self):
        """24x7 多飞机服务主循环。基准站采集与数据链从启动开始并行工作。"""
        base_thread = threading.Thread(
            target=self.base_worker,
            name="base_gnss_worker",
            daemon=True,
        )
        base_thread.start()
        self.start_ack_monitor_if_enabled()
        self.start_alert_dispatch_thread()

        print("\n[System] 24x7 多飞机 A-GNSS 服务已启动。")
        if self.multi_link_enabled():
            print(
                "[System] 四独立数据链模式：每条数据链对应一个固定机位/IP；"
                "辅助数据按在线飞机定向单发/多发。"
            )
            for link in self.drone_links:
                if link.enabled:
                    print(
                        f"[System] link={link.name}, vehicle_id={link.vehicle_id}, "
                        f"system_id={link.system_id}, endpoint={link.endpoint}"
                    )
        else:
            print(
                "[System] 支持同一 MAVLink 总线上的多个 system_id；"
                "辅助数据广播发送，各飞机定位状态独立管理。"
            )
        print(
            f"[System] 后台广播={self.BACKGROUND_BROADCAST_ENABLED}, "
            f"间隔={self.BACKGROUND_BROADCAST_INTERVAL_S}s；"
            f"未定位飞机重复间隔={self.ACTIVE_UNFIXED_INJECT_INTERVAL_S}s。"
        )

        if self.multi_link_enabled():
            last_heartbeat = 0.0
            last_status = 0.0
            last_console = 0.0
            while self.is_running:
                now = time.time()
                self.maintain_drone_links()

                if now - last_heartbeat >= self.PRE_HEARTBEAT_HEARTBEAT_INTERVAL:
                    self.send_gcs_heartbeat_all_links()
                    last_heartbeat = now

                self.request_pending_can_params()
                self.run_injection_scheduler_tick()
                self.refresh_armable_observations()
                self.evaluate_fix_timeout_alerts()
                self.update_service_state()
                self.update_can_probe_lifecycle()

                if now - last_status >= 1.0:
                    self.save_assist_db(force=False)
                    self.write_status_json()
                    last_status = now

                if now - last_console >= 5.0:
                    fresh_eph = self.fresh_assist_count()
                    fresh_alm = self.fresh_almanac_count()
                    online = self.online_vehicles()
                    unfixed = [
                        vehicle for vehicle in online
                        if not self.vehicle_quality_fix_ready(vehicle)
                    ]
                    actually_armed = [vehicle for vehicle in online if vehicle.first_armed_time is not None]
                    stable_armable = [
                        vehicle for vehicle in online
                        if self.vehicle_armable_observed(vehicle, now)
                    ]
                    active_blocks = sum(
                        len(self.active_preflight_blocks_for_vehicle(vehicle, now))
                        for vehicle in online
                    )
                    connected_links = sum(
                        1 for link in self.drone_links
                        if link.enabled and link.connected and link.master is not None
                    )
                    print(
                        f"\r[Service] state={self.engineering_state} | "
                        f"links={connected_links}/{sum(1 for link in self.drone_links if link.enabled)} | "
                        f"basePVT={'OK' if self.base_pvt_is_fresh() else 'STALE'} | "
                        f"baseSFRBX={'OK' if self.base_sfrbx_is_fresh() else 'STALE'} | "
                        f"EPH={fresh_eph} | ALM={fresh_alm}/32 | vehicles={len(online)} | "
                        f"unfixed={len(unfixed)} | actual_arm={len(actually_armed)} | "
                        f"stable_armable={len(stable_armable)} | "
                        f"preflight_blocks={active_blocks} | rounds={self.inject_round_count}",
                        end="",
                    )
                    last_console = now

                time.sleep(0.05)
            return

        while self.is_running:
            try:
                self.close_mavlink_connection()
                self.master = self.create_mavlink_connection()
                self.start_mavlink_rx_thread()
                print("[DataLink] 数据链已打开，进入全天候监听与广播。")

                last_heartbeat = 0.0
                last_status = 0.0
                last_console = 0.0

                while self.is_running:
                    now = time.time()
                    if self.mav_rx_error is not None:
                        raise RuntimeError(f"MAVLink 接收线程异常: {self.mav_rx_error}")

                    if now - last_heartbeat >= self.PRE_HEARTBEAT_HEARTBEAT_INTERVAL:
                        self.send_gcs_heartbeat()
                        last_heartbeat = now

                    self.request_pending_can_params()
                    self.run_injection_scheduler_tick()
                    self.refresh_armable_observations()
                    self.evaluate_fix_timeout_alerts()
                    self.update_service_state()
                    self.update_can_probe_lifecycle()

                    if now - last_status >= 1.0:
                        self.save_assist_db(force=False)
                        self.write_status_json()
                        last_status = now

                    if now - last_console >= 5.0:
                        fresh_eph = self.fresh_assist_count()
                        fresh_alm = self.fresh_almanac_count()
                        online = self.online_vehicles()
                        unfixed = [
                            vehicle for vehicle in online
                            if not self.vehicle_quality_fix_ready(vehicle)
                        ]
                        actually_armed = [vehicle for vehicle in online if vehicle.first_armed_time is not None]
                        stable_armable = [
                            vehicle for vehicle in online
                            if self.vehicle_armable_observed(vehicle, now)
                        ]
                        active_blocks = sum(
                            len(self.active_preflight_blocks_for_vehicle(vehicle, now))
                            for vehicle in online
                        )
                        print(
                            f"\r[Service] state={self.engineering_state} | "
                            f"basePVT={'OK' if self.base_pvt_is_fresh() else 'STALE'} | "
                            f"baseSFRBX={'OK' if self.base_sfrbx_is_fresh() else 'STALE'} | "
                            f"EPH={fresh_eph} | ALM={fresh_alm}/32 | vehicles={len(online)} | "
                            f"unfixed={len(unfixed)} | actual_arm={len(actually_armed)} | "
                            f"stable_armable={len(stable_armable)} | "
                            f"preflight_blocks={active_blocks} | rounds={self.inject_round_count}",
                            end="",
                        )
                        last_console = now

                    time.sleep(0.05)

            except Exception as exc:
                print(f"\n[DataLink] 服务链路异常: {exc}")
                self.mark_all_vehicles_offline("数据链重连")
                self.stop_mavlink_rx_thread()
                try:
                    if self.master is not None:
                        self.master.close()
                except Exception:
                    pass
                self.master = None
                self.write_status_json()
                if self.is_running:
                    print("[DataLink] 3 秒后重新建立数据链。")
                    time.sleep(3)



if __name__ == "__main__":
    import traceback

    log_max_mb, log_retention_days = load_early_logging_options()
    setup_console_file_logging(
        max_mb=log_max_mb,
        retention_days=log_retention_days,
    )

    if not acquire_single_instance_lock():
        print("[System] 已有一个 A-GNSS 服务实例正在运行，本次启动退出。")
        raise SystemExit(2)

    app = None
    exit_code = 0

    try:
        app = LocalAGNSSBridge()
        print(f"[Version] {SCRIPT_VERSION}")
        print("=== PX4 + GNSS 24x7 多飞机 A-GNSS 服务 ===")
        if "--check" in sys.argv:
            print("[Check] 依赖导入、配置解析和核心对象初始化均正常。")
            print(
                f"[Check] BASE={app.BASE_PORT}@{app.BAUD_BASE}, "
                f"LINK={app.LINK_MODE}:{app.DRONE_PORT}, "
                f"后台广播间隔={app.BACKGROUND_BROADCAST_INTERVAL_S}s"
            )
            print(
                f"[Check] TARGET_GPS_LINK_TYPE={app.TARGET_GPS_LINK_TYPE}, "
                f"CAN参数门槛={app.CAN_REQUIRE_PX4_PARAMS_BEFORE_INJECT}, "
                "目标配置="
                + ",".join(
                    f"{item['link_name']}:{item['target_gps_model']}"
                    f"/raw_ubx={item['can_raw_ubx_validated']}"
                    for item in app.configured_can_target_profiles()
                    if item["enabled"]
                )
                + ", "
                + f"所需参数={','.join(CAN_REQUIRED_PX4_PARAMS)}"
            )
        else:
            if "--can-probe" in sys.argv:
                app.start_can_probe()
            app.run_service_loop()
            if app.can_probe_mode:
                print(f"\n[CAN-Probe] result={app.can_probe_result}")
                if app.can_probe_sent_at is None:
                    exit_code = 3
            else:
                print("\n[System] 服务主循环正常返回。")

    except KeyboardInterrupt:
        print("\n[System] 用户退出。")
        if app is not None:
            app.is_running = False

    except Exception as e:
        exit_code = 1
        if app is not None:
            app.is_running = False
        print("\n[System] ❌ 主线程异常退出：")
        print(repr(e))
        traceback.print_exc()
        try:
            with open(LOG_DIR / "agnss_crash_log.txt", "w", encoding="utf-8") as f:
                f.write("主线程异常退出：\n")
                f.write(repr(e))
                f.write("\n\n")
                f.write(traceback.format_exc())
            print(f"[System] 异常详情已保存到 {LOG_DIR / 'agnss_crash_log.txt'}")
        except Exception:
            pass

    finally:
        if app is not None:
            app.is_running = False
            try:
                app.save_assist_db(force=True)
            except Exception:
                pass
            try:
                app.stop_mavlink_rx_thread()
            except Exception:
                pass
            try:
                app.stop_alert_dispatch_thread()
            except Exception:
                pass
            try:
                app.close_mavlink_connection()
            except Exception:
                pass
            try:
                if app.ack_monitor is not None:
                    app.ack_monitor.stop()
            except Exception:
                pass
        print("[System] 程序结束。")
    raise SystemExit(exit_code)
