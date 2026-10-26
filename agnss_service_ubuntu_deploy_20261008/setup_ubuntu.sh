#!/usr/bin/env bash
set -Eeuo pipefail

SERVICE_NAME="agnss.service"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
VENV_DIR="${SCRIPT_DIR}/.venv"
PYTHON_BIN="${VENV_DIR}/bin/python"
CONFIG_FILE="${SCRIPT_DIR}/agnss_config.json"
SERVICE_SCRIPT="${SCRIPT_DIR}/agnss_service.py"
REQUIREMENTS_FILE="${SCRIPT_DIR}/requirements.txt"

if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    RUN_USER="${SUDO_USER}"
else
    RUN_USER="$(id -un)"
fi
RUN_GROUP="$(id -gn "${RUN_USER}" 2>/dev/null || id -g "${RUN_USER}")"

if [[ "${EUID}" -eq 0 ]]; then
    printf '[ERROR] Run this script as your normal Ubuntu user, without sudo. The script requests sudo only when needed.\n' >&2
    exit 1
fi

info() {
    printf '[INFO] %s\n' "$*"
}

warn() {
    printf '[WARN] %s\n' "$*" >&2
}

fail() {
    printf '[ERROR] %s\n' "$*" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Usage: ./setup_ubuntu.sh ACTION

Actions:
  prepare          Install packages, create the virtual environment, and grant serial access
  devices          List serial devices and stable /dev/serial/by-id paths
  check            Validate the JSON, serial path, dependencies, and application startup
  probe-base       Read the base GNSS for 10 seconds and count valid UBX/PVT/SAT/SFRBX frames
  probe-base-bauds Probe common base-GNSS baud rates without writing receiver settings
  install-service  Install the systemd unit without starting or enabling it
  status           Show systemd status and recent logs

Recommended order:
  ./setup_ubuntu.sh prepare
  ./setup_ubuntu.sh devices
  nano agnss_config.json
  ./setup_ubuntu.sh check
  ./setup_ubuntu.sh probe-base
  ./setup_ubuntu.sh install-service
EOF
}

require_project_files() {
    local file
    for file in "${SERVICE_SCRIPT}" "${CONFIG_FILE}" "${REQUIREMENTS_FILE}"; do
        [[ -f "${file}" ]] || fail "Required file is missing: ${file}"
    done
}

require_sudo() {
    command -v sudo >/dev/null 2>&1 || fail "sudo is required for this action."
    sudo -v
}

prepare() {
    require_project_files
    require_sudo

    info "Installing Ubuntu packages."
    sudo apt-get update
    sudo apt-get install -y python3 python3-venv python3-pip udev jq netcat-openbsd

    if ! id -nG "${RUN_USER}" | tr ' ' '\n' | grep -qx dialout; then
        info "Adding ${RUN_USER} to the dialout group."
        sudo usermod -aG dialout "${RUN_USER}"
        warn "Log out and log in again before using the serial port from a terminal."
    else
        info "${RUN_USER} already belongs to the dialout group."
    fi

    if [[ ! -x "${PYTHON_BIN}" ]]; then
        info "Creating Python virtual environment at ${VENV_DIR}."
        python3 -m venv "${VENV_DIR}"
    fi

    "${PYTHON_BIN}" -m pip install --upgrade pip
    "${PYTHON_BIN}" -m pip install -r "${REQUIREMENTS_FILE}"
    "${PYTHON_BIN}" -m pip check

    info "Environment preparation completed."
    devices
    printf '\nNext: set BASE_PORT in %s, then run:\n  ./setup_ubuntu.sh check\n' "${CONFIG_FILE}"
}

devices() {
    info "Stable serial paths:"
    if [[ -d /dev/serial/by-id ]]; then
        find -L /dev/serial/by-id -maxdepth 1 -type c -printf '%p -> %l\n' 2>/dev/null || true
        ls -l /dev/serial/by-id/ 2>/dev/null || true
    else
        warn "/dev/serial/by-id does not exist. Connect the base GNSS and run this action again."
    fi

    info "ttyUSB/ttyACM devices:"
    compgen -G '/dev/ttyUSB*' || true
    compgen -G '/dev/ttyACM*' || true

    if [[ -x "${PYTHON_BIN}" ]]; then
        info "pyserial device report:"
        "${PYTHON_BIN}" -m serial.tools.list_ports -v || true
    fi
}

read_config() {
    local config_output
    [[ -x "${PYTHON_BIN}" ]] || fail "Virtual environment is missing. Run: ./setup_ubuntu.sh prepare"

    if ! config_output="$("${PYTHON_BIN}" - "${CONFIG_FILE}" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    config = json.load(handle)

base_port = str(config.get("BASE_PORT", "")).strip()
base_baud = int(config.get("BAUD_BASE", 0))
print(base_port)
print(base_baud)

for link in config.get("DRONE_LINKS", []):
    if link.get("enabled"):
        print("LINK\t{}\t{}\t{}".format(
            link.get("name", "unnamed"),
            link.get("endpoint", ""),
            link.get("system_id", ""),
        ))
PY
    )"; then
        fail "agnss_config.json is not valid JSON."
    fi
    mapfile -t CONFIG_VALUES <<<"${config_output}"

    BASE_PORT="${CONFIG_VALUES[0]:-}"
    BASE_BAUD="${CONFIG_VALUES[1]:-0}"
    ENABLED_LINKS=("${CONFIG_VALUES[@]:2}")
}

check_network_link() {
    local record="$1"
    local marker name endpoint system_id host port
    IFS=$'\t' read -r marker name endpoint system_id <<<"${record}"

    info "Enabled link: name=${name}, endpoint=${endpoint}, SYSID=${system_id}"
    if [[ "${endpoint}" =~ ^tcp:([^:]+):([0-9]+)$ ]]; then
        host="${BASH_REMATCH[1]}"
        port="${BASH_REMATCH[2]}"
        if nc -z -w 3 "${host}" "${port}"; then
            info "TCP endpoint is reachable: ${host}:${port}"
        else
            warn "TCP endpoint is not reachable now: ${host}:${port}. This may be normal if the aircraft link is powered off."
        fi
    fi
}

check_setup() {
    require_project_files
    read_config

    [[ -n "${BASE_PORT}" ]] || fail "BASE_PORT is empty in agnss_config.json."
    [[ "${BASE_BAUD}" -gt 0 ]] || fail "BAUD_BASE must be a positive integer."
    if [[ "${BASE_PORT^^}" == COM* ]]; then
        fail "BASE_PORT is still a Windows port (${BASE_PORT}). Set it to a Linux path such as /dev/serial/by-id/..."
    fi
    [[ "${BASE_PORT}" == /dev/* ]] || fail "BASE_PORT must be an absolute /dev/... path on Ubuntu."
    [[ -e "${BASE_PORT}" ]] || fail "Configured base serial device does not exist: ${BASE_PORT}"

    info "Configured base GNSS: ${BASE_PORT}@${BASE_BAUD}"
    if [[ -r "${BASE_PORT}" && -w "${BASE_PORT}" ]]; then
        info "${RUN_USER} has read/write access to the base serial device."
    else
        fail "${RUN_USER} cannot read/write ${BASE_PORT}. Re-login after joining dialout, then retry."
    fi

    "${PYTHON_BIN}" -m pip check
    "${PYTHON_BIN}" -m json.tool "${CONFIG_FILE}" >/dev/null
    "${PYTHON_BIN}" -u "${SERVICE_SCRIPT}" --check

    local record
    for record in "${ENABLED_LINKS[@]}"; do
        [[ -n "${record}" ]] && check_network_link "${record}"
    done

    info "Configuration checks completed. No injection service was started."
}

probe_base() {
    require_project_files
    read_config

    [[ -n "${BASE_PORT}" ]] || fail "BASE_PORT is empty in agnss_config.json."
    [[ "${BASE_PORT^^}" != COM* ]] || fail "BASE_PORT is still a Windows port (${BASE_PORT})."
    [[ "${BASE_PORT}" == /dev/* ]] || fail "BASE_PORT must be an absolute /dev/... path on Ubuntu."
    [[ -e "${BASE_PORT}" ]] || fail "Configured base serial device does not exist: ${BASE_PORT}"

    info "Read-only base GNSS probe: ${BASE_PORT}@${BASE_BAUD} for 10 seconds."
    "${PYTHON_BIN}" - "${BASE_PORT}" "${BASE_BAUD}" <<'PY'
import sys
import time
import serial

port = sys.argv[1]
baud = int(sys.argv[2])
deadline = time.monotonic() + 10.0
buffer = bytearray()
counts = {"valid_ubx": 0, "NAV-PVT": 0, "NAV-SAT": 0, "RXM-SFRBX": 0}
last_error = None

def ubx_checksum(frame):
    ck_a = 0
    ck_b = 0
    for value in frame[2:-2]:
        ck_a = (ck_a + value) & 0xff
        ck_b = (ck_b + ck_a) & 0xff
    return frame[-2] == ck_a and frame[-1] == ck_b

try:
    with serial.Serial(port, baudrate=baud, timeout=0.2) as handle:
        while time.monotonic() < deadline:
            chunk = handle.read(4096)
            if chunk:
                buffer.extend(chunk)
            while True:
                sync = buffer.find(b"\xb5\x62")
                if sync < 0:
                    buffer = buffer[-1:]
                    break
                if sync:
                    del buffer[:sync]
                if len(buffer) < 6:
                    break
                length = buffer[4] | (buffer[5] << 8)
                total = 8 + length
                if total > 4096:
                    del buffer[:2]
                    continue
                if len(buffer) < total:
                    break
                frame = bytes(buffer[:total])
                del buffer[:total]
                if not ubx_checksum(frame):
                    continue
                counts["valid_ubx"] += 1
                identity = (frame[2], frame[3])
                if identity == (1, 7):
                    counts["NAV-PVT"] += 1
                elif identity == (1, 53):
                    counts["NAV-SAT"] += 1
                elif identity == (2, 19):
                    counts["RXM-SFRBX"] += 1
except Exception as exc:
    last_error = str(exc)

print("base_probe=" + " ".join(f"{key}={value}" for key, value in counts.items()))
if last_error:
    print("base_probe_error=" + last_error)
    raise SystemExit(1)
if counts["valid_ubx"] == 0:
    print("base_probe_result=FAIL_NO_VALID_UBX")
    raise SystemExit(2)
if counts["NAV-PVT"] == 0 or counts["RXM-SFRBX"] == 0:
    print("base_probe_result=DEGRADED_MISSING_PVT_OR_SFRBX")
    raise SystemExit(3)
print("base_probe_result=OK_REALTIME_ASSIST_INPUT")
PY
}

probe_base_bauds() {
    require_project_files
    read_config
    [[ -n "${BASE_PORT}" ]] || fail "BASE_PORT is empty in agnss_config.json."
    [[ "${BASE_PORT}" == /dev/* ]] || fail "BASE_PORT must be an absolute /dev/... path on Ubuntu."
    [[ -e "${BASE_PORT}" ]] || fail "Configured base serial device does not exist: ${BASE_PORT}"
    info "Read-only baud probe on ${BASE_PORT}; no receiver configuration is written."
    "${PYTHON_BIN}" - "${BASE_PORT}" <<'PY'
import sys
import time
import serial

port = sys.argv[1]
rates = (4800, 9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600)

def valid_ubx(frame):
    a = b = 0
    for value in frame[2:-2]:
        a = (a + value) & 0xff
        b = (b + a) & 0xff
    return frame[-2] == a and frame[-1] == b

for rate in rates:
    raw_count = sync_count = valid_count = 0
    ids = {}
    error = None
    buffer = bytearray()
    try:
        with serial.Serial(port, baudrate=rate, timeout=0.2, dsrdtr=False, rtscts=False, xonxoff=False) as handle:
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                chunk = handle.read(4096)
                raw_count += len(chunk)
                if chunk:
                    buffer.extend(chunk)
                while True:
                    start = buffer.find(b"\xb5\x62")
                    if start < 0:
                        buffer = buffer[-1:]
                        break
                    sync_count += 1
                    if start:
                        del buffer[:start]
                    if len(buffer) < 6:
                        break
                    length = buffer[4] | (buffer[5] << 8)
                    total = 8 + length
                    if total > 4096:
                        del buffer[:2]
                        continue
                    if len(buffer) < total:
                        break
                    frame = bytes(buffer[:total])
                    del buffer[:total]
                    if valid_ubx(frame):
                        valid_count += 1
                        key = f"{frame[2]:02X}/{frame[3]:02X}"
                        ids[key] = ids.get(key, 0) + 1
    except Exception as exc:
        error = str(exc)
    suffix = f" ids={ids}" if ids else ""
    if error:
        print(f"baud={rate} error={error}")
    else:
        print(f"baud={rate} raw={raw_count} ubx_sync={sync_count} valid_ubx={valid_count}{suffix}")
PY
}

install_service() {
    require_sudo
    check_setup

    [[ "${SCRIPT_DIR}" != *[[:space:]]* ]] || fail "The project path contains whitespace, which is not supported by this installer."
    [[ "${SCRIPT_DIR}" != "/" && "${SCRIPT_DIR}" != "/opt" && "${SCRIPT_DIR}" != "/home" ]] \
        || fail "Refusing to install from an unsafe broad directory: ${SCRIPT_DIR}"

    if [[ ! -w "${SCRIPT_DIR}" ]]; then
        fail "${RUN_USER} cannot write to ${SCRIPT_DIR}. Fix directory ownership before installing the service."
    fi

    local unit_file
    unit_file="$(mktemp)"
    trap 'rm -f -- "${unit_file:-}"' EXIT

    cat >"${unit_file}" <<EOF
[Unit]
Description=Local GNSS Assistance Bridge
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
Group=${RUN_GROUP}
SupplementaryGroups=dialout
WorkingDirectory=${SCRIPT_DIR}
Environment=PYTHONUNBUFFERED=1
ExecStart=${PYTHON_BIN} -u ${SERVICE_SCRIPT}
Restart=on-failure
RestartSec=3
KillSignal=SIGINT
TimeoutStopSec=20
UMask=0027
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

    sudo install -o root -g root -m 0644 "${unit_file}" "/etc/systemd/system/${SERVICE_NAME}"
    sudo systemctl daemon-reload

    info "${SERVICE_NAME} was installed, but it was not started or enabled for boot."
    printf 'After foreground validation and confirming the aircraft is disarmed, enable and start it with:\n  sudo systemctl enable --now %s\n' "${SERVICE_NAME}"
}

show_status() {
    require_sudo
    sudo systemctl status --no-pager "${SERVICE_NAME}" || true
    sudo journalctl -u "${SERVICE_NAME}" -n 100 --no-pager || true
}

action="${1:-}"
case "${action}" in
    prepare)
        prepare
        ;;
    devices)
        devices
        ;;
    check)
        check_setup
        ;;
  probe-base)
        probe_base
        ;;
    probe-base-bauds)
        probe_base_bauds
        ;;
    install-service)
        install_service
        ;;
    status)
        show_status
        ;;
    -h|--help|help|"")
        usage
        ;;
    *)
        usage >&2
        fail "Unknown action: ${action}"
        ;;
esac
