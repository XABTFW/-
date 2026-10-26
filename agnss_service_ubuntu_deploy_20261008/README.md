# 24x7 多飞机 A-GNSS 服务

该目录是在原工程脚本基础上改造的长期运行版本。桌面原脚本和现有
`AGNSS_Demo_V1.0` 未被覆盖。

## 主要变化

- 基准站采集和数据链监听从启动起并行运行。
- 按 MAVLink `(system_id, component_id)` 保存每架飞机的独立状态。
- 新飞机上线后执行多轮 Top-N 快速注入。
- 存在未定位飞机时，每 2 秒重复辅助。
- 所有飞机定位后仍每 30 秒广播最新辅助数据。
- 新 IODE 星历生成后尽快广播。
- 相同 IODE 不再刷新年龄，超过两小时的星历会从缓存真正删除。
- 基准站 PVT 超时后停止发送不可信的 TIME/POS，仅保留仍有效的星历。
- 配置、状态、CSV 和日志全部固定在脚本目录。
- 日志按日期和大小轮转，默认保留 30 天；清理会在启动时执行，并在服务持续运行期间有日志写入时最多每小时扫描一次。
- 使用文件锁防止多个服务实例同时占用串口和端口。
- 飞机长时间未定位时生成 WARNING/CRITICAL 告警并可靠上报指挥中心。
- 指挥中心不可达时告警保留在 outbox，恢复后自动重试。
- 超时飞机最终定位成功时发送 `GNSS_FIX_RECOVERED` 恢复事件。
- CAN 模式先读回 PX4 UAVCAN 参数，参数不合格时禁止发送 UBX-MGA。
- `--can-probe` 最多发送一包，供首次 CAN GPS 抓包验证。

## V6 基线 + CUAV NEO 3 Pro CAN 模式

当前脚本以 `2026-07-21_ack_validity_scheduler_v6` 为采集、有效期、ACK 和
注入调度基线，在此基础上增加 NEO 3 Pro 的 CAN 传输验证门槛。压缩包中的
历史状态、辅助数据库和日志没有覆盖当前运行目录。

当前启用的 `plane2` 已绑定经物理验证的 NEO 3 Pro：

```json
"TARGET_GPS_LINK_TYPE": "can_via_px4",
"TARGET_GPS_MODEL": "NEO3_PRO",
"DRONE_LINKS": [
  {
    "name": "plane2",
    "target_gps_model": "NEO3_PRO",
    "can_raw_ubx_validated": true
  }
]
```

脚本会读取 `UAVCAN_ENABLE`、`UAVCAN_SUB_GPS`、`UAVCAN_PUB_RTCM`。
未全部读回合格值时，`GPS_RTCM_DATA` 发送会被阻止。正常服务还会
按当前飞机链路检查 `can_raw_ubx_validated`；未验证的链路只能使用
`--can-probe`，不会被旧的全局开关误放行。

`CAN_TARGET_VALIDATION_CONFIRMED` 仅保留给旧单链路配置兼容。新配置应始终在
`DRONE_LINKS` 中显式填写 `target_gps_model` 和 `can_raw_ubx_validated`。将某架
飞机换回 4NANO 时，必须把该链路的验证值改回 `false`。

## 定位超时与指挥中心

默认告警门限：

| 配置 | 默认值 | 作用 |
|---|---:|---|
| `FIX_WARNING_TIMEOUT_S` | 60 | 飞机上线后未 3D Fix 的预警门限 |
| `FIX_CRITICAL_TIMEOUT_S` | 120 | 要求指挥中心决策的关键告警门限 |
| `FIX_CRITICAL_REPEAT_INTERVAL_S` | 60 | 关键告警重复提醒间隔 |
| `COMMAND_CENTER_WEBHOOK_URL` | 空 | 指挥中心 HTTP POST 接口 |
| `COMMAND_CENTER_RETRY_INTERVAL_S` | 10 | 待发送告警重试间隔 |

必须把 `COMMAND_CENTER_WEBHOOK_URL` 改成指挥中心提供的实际地址，例如：

```json
"COMMAND_CENTER_WEBHOOK_URL": "http://192.168.1.100:8080/api/agnss/alerts"
```

如果接口需要 Bearer Token，不要把密钥直接写入 JSON。设置环境变量：

```powershell
setx AGNSS_COMMAND_CENTER_TOKEN "实际Token"
```

Webhook 请求使用 `application/json`。关键字段如下：

```json
{
  "event_id": "UUID",
  "event_type": "GNSS_FIX_TIMEOUT_CRITICAL",
  "severity": "CRITICAL",
  "decision_required": true,
  "recommended_actions": [
    "CONTINUE_WAITING",
    "SWITCH_TO_NEXT_AIRCRAFT",
    "ABORT_MISSION"
  ],
  "vehicle": {
    "system_id": 2,
    "component_id": 1,
    "session_id": 7,
    "unfixed_duration_s": 120.4,
    "fix_type": 1,
    "satellites": 3
  }
}
```

指挥中心返回任意 HTTP `2xx` 表示成功接收。其他状态码、连接失败或超时均
视为失败，事件文件不会删除并会继续重试。

## 多机拓扑要求

当前配置使用每架飞机独立的 TCP/UDP/串口数据链。脚本可以按链路选择目标
PX4，但 `GPS_RTCM_DATA` 进入该 PX4 后仍会在该飞机的 CAN 总线上广播。
因此每架飞机的 CAN 总线只能安装已确认可接收该字节流的目标 GPS，
或明确证明其它 CAN 节点会安全忽略该数据。

每架飞机必须配置唯一的 PX4 `SYSID_THISMAV`。如果两架飞机使用相同
system ID，服务无法区分它们的心跳和 `GPS_RAW_INT`。

如果每个机位使用独立 TCP、UDP 或串口，建议为每条物理链路复制一个
服务目录并运行一个独立实例，分别设置端口和状态文件，获得更好的故障隔离。

## 首次部署

Ubuntu 部署时，先运行 `setup_ubuntu.sh devices`，把 `BASE_PORT` 设置为稳定的
`/dev/serial/by-id/...` 路径，不能保留 Windows 的 `COM3`。完成 `check` 后运行
`./setup_ubuntu.sh probe-base`；该命令只读打开基准站 10 秒并报告合法 UBX、
`NAV-PVT`、`NAV-SAT`、`RXM-SFRBX` 数量。只有 `base_probe_result=OK_REALTIME_ASSIST_INPUT`
才说明实时辅助数据输入链路已具备，只有串口有字节或 `NAV-PVT` 而没有
`RXM-SFRBX` 时仍不能进入实时星历解析。

在 Ubuntu 目标机上执行：

```bash
cd /path/to/agnss_service
./setup_ubuntu.sh prepare
./setup_ubuntu.sh devices
./setup_ubuntu.sh check
./setup_ubuntu.sh probe-base
./setup_ubuntu.sh install-service
sudo systemctl enable --now agnss.service
```

检查并修改 `agnss_config.json`，然后检查服务：

```bash
sudo systemctl status agnss.service --no-pager
sudo journalctl -u agnss.service -f
```

## 运行状态

- `agnss_status.json`：整体健康和所有飞机状态。
- `agnss_vehicle_results.csv`：每架飞机每次上电会话的 TTFF。
- `agnss_alert_state.json`：当前活动告警和指挥中心投递状态。
- `alerts/agnss_alerts_YYYYMMDD.jsonl`：不可变告警流水。
- `alerts/outbox/*.json`：尚未被指挥中心确认接收的事件。
- `logs/agnss_service_YYYYMMDD.log`：轮转运行日志，默认保留 30 天；服务运行期间有日志写入时最多每小时清理一次过期文件。
- `logs/agnss_crash_log.txt`：主线程致命异常。

重点关注：

- `base_pvt_ok`
- `base_sfrbx_ok`
- `assist_fresh_eph_count`
- `online_vehicle_count`
- `unfixed_online_vehicle_count`
- `vehicles`
- `inject_error_count`
- `active_fix_timeout_alert_count`
- `pending_command_center_alert_count`
- `last_alert_delivery_error`
- `can_assist.support_state`
- `can_assist.target_profiles`
- `can_assist.all_online_targets_raw_ubx_validated`
- `can_assist.px4_rtcmstream_publisher_ready`
- `can_assist.scopes`
- `can_assist.probe`

### v6 ACK、有效期与调度参数

- `ACK_MONITOR_ENABLED`：接好目标 GPS TX -> USB-TTL RX 后才设为 `true`。
- `ACK_FLOW_CONTROL_MODE`：`observe` 只统计；`stop_and_wait` 逐包等待 ACK 并有限重试。
- `ACK_TARGET_LINK_NAME`：多链路模式下指定 ACK 监听对应的飞机，当前为 `plane2`。
- `ACK_WAIT_TIMEOUT_S` / `ACK_MAX_RETRIES`：ACK 等待超时和临时拒绝重试次数。
- `ACK_TIMEOUT_CIRCUIT_BREAKER` / `ACK_SUSPEND_S`：监听异常时暂停闭环等待，防止整轮阻塞。
- `ADAPTIVE_INJECT_DELAY_MIN_S` / `MAX_S`：根据 ACK 结果调整包间隔的边界。
- `EPH_SEMANTIC_HALF_WINDOW_S`：除接收时间外，以 Toe 为中心的星历语义有效窗口。
- `ENABLED_ASSIST_CONSTELLATIONS`：稳定注入白名单；默认不包含实验性 GLO。
- `CONSTELLATION_PRIORITY`：没有目标端/基准站可见性证据时的排序权重。

`agnss_status.json` 新增 ACK 等待/接受/拒绝/超时/重试计数、当前自适应包间隔、
目标端 NAV-SAT seen/used 数量、各星座新鲜星历和已注入包数。现场打开 ACK 前，
先确认 `COM10` 能持续收到目标 GPS 的 UBX 输出；未接监听线时保持
`ACK_MONITOR_ENABLED=false`。NEO 3 Pro 的外部接口只有 CAN，当前无内部 UART
回传通道，因此继续保持 `ACK_MONITOR_ENABLED=false` 和
`ENABLE_TARGET_CFG_VIA_PX4=false`。

## NEO 3 Pro 快速冷启动验证边界

当前版本具备 NEO 3 Pro 冷启动辅助的完整发送路径，但“发送成功”不等同于
“接收机已采用辅助数据”。正式计时前必须同时满足：

- `base_pvt_ok=true`、`base_sfrbx_ok=true`；
- `assist_fresh_eph_count >= 10`，建议达到 `12`；
- `can_assist.support_state=CAN_ASSIST_ENABLED_OPERATOR_VALIDATED`；
- PX4 已读回 `UAVCAN_ENABLE=2`、`UAVCAN_SUB_GPS=1`、`UAVCAN_PUB_RTCM=1`；
- 基准站位置可代表飞机当前位置，或另行提供可信的飞机近似位置；
- NEO 3 Pro 完成真正冷复位，不能把保留电池备份状态的重新上电当作冷启动。

验收时分别记录首次 Raw 3D Fix、达到最少卫星数的 Quality Fix、稳定可解锁和
实际解锁时间。至少完成一组同条件“关闭注入/开启注入”A/B 测试后，才能评价
快速冷启动效果；仅有 `GPS_RTCM_DATA` 发送计数或 DTID 1062 抓包不能作为结论。

## 无硬件测试

```powershell
python .\test_service_logic.py
```

该测试覆盖多机状态隔离、重新上电会话、过期/Toe 语义有效期、PVT 有效包
新鲜度、ACK 关联与重试、目标 NAV-SAT 排序、CAN 参数门槛、单包探测，
以及包含坏校验帧的基准站字节流回放。
