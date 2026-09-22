# 传感器遥测汇聚 API

基于 **FastAPI + SQLite** 的传感器遥测汇聚服务，支持事件时间水位线、允许迟到窗口、幂等去重与迟到数据修正，并提供**设备组同步窗口汇总**（固定成员组，sum/avg，全成员封存才出数，迟到修正原位升版）与**组窗口修订审计**（首次 ready 与每次升版在同事务追加不可变快照，提供修订列表与两版本 diff）。

## 核心语义

- **水位线（watermark）**：每个设备维护一条持久化的事件时间水位线，等于该设备已见最大 `event_time`，单调递增、落盘保存，重启不丢失。
- **时间窗**：按 `window_size_seconds`（默认 60s）对齐 epoch 的滚动窗口，按设备独立维护。
- **窗口封存**：当 `watermark >= window_end + allowed_lateness_seconds`（默认 30s）时，窗口状态从 `open` 变为 `sealed`。
- **幂等去重**：同一 `(device_id, event_id)` 重复提交只生效一次，返回 `duplicate`，不影响聚合结果。
- **迟到修正**：窗口封存后到达的事件标记为 `late`，仍计入窗口聚合，同时生成一条修正记录（correction），窗口的 `correction_count` 递增。
- 批次内事件按提交顺序处理，与流式处理语义一致。

### 设备组同步窗口汇总

- **固定成员组**：创建组时显式给出成员设备、`aggregation`（`sum` 或 `avg`）以及窗口配置；所有成员设备必须已存在，且 `window_size_seconds` / `allowed_lateness_seconds` 与组配置完全一致（否则分别返回 404 / 409），保证各成员窗口在同一时间轴对齐、封存口径一致。成员在组创建后不可变更。
- **同步封存才出数**：对每个 `window_start`，只有**全部成员**的设备窗口都已封存（sealed）时才生成组汇总（`status=ready`）；任一成员窗口仍 open 或尚无数据时，组窗口保持 `pending`、`value=null`，**绝不用部分成员提前计算**。
- **聚合口径**：`sum` 为各成员窗口 `sum` 之和；`avg` 为各成员窗口 `avg`（窗口内事件均值）的算术平均，每个固定成员权重相同。
- **修正原位升版**：成员窗口因迟到事件被修正后，已 ready 的组汇总在原记录上重算，`revision` 加一。系统以各成员窗口 `correction_count` 之和作为修正签名：同一 `event_id` 重试会被幂等去重、不产生新修正，签名不变，因此 **revision 不会重复升版**。ready 之前发生的修正包含在首次结果中，`revision` 从 1 起。
- 组也可以在成员窗口已封存之后创建，创建时即补算历史窗口。

### 组窗口修订审计（不可变快照）

- **同事务留档**：组窗口**首次 ready**、以及迟到修正令 **revision 升版**时，在与组汇总写入的**同一事务**内追加一份不可变快照，记录当时的 group `value`，以及每个固定成员窗口当时的 `count`、`sum`、`avg`、`correction_count`。
- **不重复留档**：快照以 `(group_id, window_start, revision)` 唯一约束兜底，重复上报（幂等去重、签名未推进）绝不会产生第二份同版本快照。
- **不可变、不可覆盖**：审计表只允许 `INSERT`，数据库触发器会直接中止任何 `UPDATE`/`DELETE`；后续修正只追加新 revision 的快照，**历史快照永不被覆盖**。
- **初始化只补基线**：旧版本数据库升级启动时，只对**已 ready** 但尚无快照的组窗口补一份**当前 revision** 的 baseline（标记 `kind=baseline`），**绝不伪造** ready 之后可能存在过的早期版本；新库及已有快照的窗口不受影响。
- **审计接口**：提供修订列表与两版本 diff：
  - 修订列表按 revision 升序返回每个快照的 `value`、`kind`（`revision`/`baseline`）与成员数；
  - diff 按**成员顺序**返回每个**发生变化**成员的前后贡献（sum 聚合贡献为窗口 `sum`，avg 聚合贡献为窗口 `avg`）与 `count/sum/avg/correction_count` 的数值差，未变化成员不列出；
  - 版本参数非法（缺失、`<1`、`from >= to`）返回 `422`；组、窗口或任一修订不存在返回 `404`。

## 启动

### Docker（推荐，一键启动）

确保已安装 Docker Engine 与 Docker Compose 插件，然后在项目根目录执行**单条命令**：

```bash
docker compose up --build
```

服务在前台启动（加 `-d` 可后台运行），监听并映射宿主机 **8000** 端口：

- 健康检查：http://localhost:8000/health
- Swagger 交互式文档：http://localhost:8000/docs
- ReDoc 文档：http://localhost:8000/redoc

停止服务用 `Ctrl+C`（后台模式则 `docker compose down`）。数据保存在 Docker 卷 `telemetry-data` 中，`docker compose down` 不会删除；如需清空可执行 `docker compose down -v`。

或纯 Docker：

```bash
docker build -t telemetry-api .
docker run -p 8000:8000 -v telemetry-data:/data telemetry-api
```

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

启动后同样通过 **http://localhost:8000** 访问（文档见 `/docs`）。

### 必要配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TELEMETRY_DB_PATH` | `telemetry.db`（本地）/ `/data/telemetry.db`（容器） | SQLite 数据库文件路径；容器内 `/data` 已挂载卷持久化 |

设备默认窗口参数（也可经 `PUT /devices/{id}/config` 逐设备覆盖）：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `window_size_seconds` | `60` | 滚动窗口大小（秒） |
| `allowed_lateness_seconds` | `30` | 允许迟到时间（秒），超过 `window_end + 该值` 窗口封存 |

### 运行测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```

## API 示例

### 1. 配置设备（可选，不配置则首次上报时按默认值自动注册）

```bash
curl -X PUT http://localhost:8000/devices/sensor-001/config \
  -H 'Content-Type: application/json' \
  -d '{"window_size_seconds": 60, "allowed_lateness_seconds": 30}'
```

> 设备已有数据后不允许修改 `window_size_seconds`（返回 409），`allowed_lateness_seconds` 可随时调整。

### 2. 提交事件批次

```bash
curl -X POST http://localhost:8000/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "device_id": "sensor-001",
    "events": [
      {"event_id": "evt-1", "event_time": "2026-09-22T10:00:10Z", "value": 21.5},
      {"event_id": "evt-2", "event_time": "2026-09-22T10:00:40Z", "value": 22.0},
      {"event_id": "evt-3", "event_time": "2026-09-22T10:02:00Z", "value": 23.1}
    ]
  }'
```

响应（水位线推进到 10:02:00，窗口 `[10:00, 10:01)` 因 `10:01:00 + 30s <= 10:02:00` 被封存）：

```json
{
  "device_id": "sensor-001",
  "watermark": "2026-09-22T10:02:00Z",
  "accepted": 3, "duplicates": 0, "late": 0,
  "results": [
    {"event_id": "evt-1", "status": "accepted", "window_start": "2026-09-22T10:00:00Z"},
    {"event_id": "evt-2", "status": "accepted", "window_start": "2026-09-22T10:00:00Z"},
    {"event_id": "evt-3", "status": "accepted", "window_start": "2026-09-22T10:02:00Z"}
  ]
}
```

### 3. 重复提交 + 迟到数据

```bash
curl -X POST http://localhost:8000/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "device_id": "sensor-001",
    "events": [
      {"event_id": "evt-1", "event_time": "2026-09-22T10:00:10Z", "value": 21.5},
      {"event_id": "evt-4", "event_time": "2026-09-22T10:00:25Z", "value": 20.9}
    ]
  }'
```

`evt-1` 为重复事件被去重；`evt-4` 落入已封存窗口，标记为 `late` 并生成修正记录：

```json
{
  "accepted": 0, "duplicates": 1, "late": 1,
  "results": [
    {"event_id": "evt-1", "status": "duplicate", "window_start": null},
    {"event_id": "evt-4", "status": "late", "window_start": "2026-09-22T10:00:00Z"}
  ]
}
```

### 4. 查询窗口聚合

```bash
curl http://localhost:8000/devices/sensor-001/windows
# 可按状态过滤：curl 'http://localhost:8000/devices/sensor-001/windows?status=sealed'
```

```json
[
  {
    "device_id": "sensor-001",
    "window_start": "2026-09-22T10:00:00Z",
    "window_end": "2026-09-22T10:01:00Z",
    "status": "sealed",
    "count": 3, "sum": 64.4, "avg": 21.4667, "min": 20.9, "max": 22.0,
    "correction_count": 1,
    "late_event_count": 1,
    "sealed_at": "2026-09-22T10:02:31Z"
  },
  {
    "device_id": "sensor-001",
    "window_start": "2026-09-22T10:02:00Z",
    "window_end": "2026-09-22T10:03:00Z",
    "status": "open",
    "count": 1, "sum": 23.1, "avg": 23.1, "min": 23.1, "max": 23.1,
    "correction_count": 0,
    "late_event_count": 0,
    "sealed_at": null
  }
]
```

### 5. 查询修正记录

```bash
curl http://localhost:8000/devices/sensor-001/corrections
```

```json
[
  {
    "id": 1,
    "device_id": "sensor-001",
    "event_id": "evt-4",
    "window_start": "2026-09-22T10:00:00Z",
    "event_time": "2026-09-22T10:00:25Z",
    "value": 20.9,
    "created_at": "2026-09-22T10:02:45Z"
  }
]
```

### 6. 查询设备水位线

```bash
curl http://localhost:8000/devices/sensor-001
```

```json
{
  "device_id": "sensor-001",
  "window_size_seconds": 60,
  "allowed_lateness_seconds": 30,
  "watermark": "2026-09-22T10:02:00Z",
  "event_count": 4,
  "open_windows": 1,
  "sealed_windows": 1,
  "created_at": "2026-09-22T10:00:01Z"
}
```

### 7. 创建设备组（固定成员，sum/avg）

成员设备必须**先存在**且窗口配置与组一致（可先用第 1 步的 config 接口或上报事件完成注册）：

```bash
curl -X POST http://localhost:8000/groups \
  -H 'Content-Type: application/json' \
  -d '{
    "group_id": "floor-1",
    "member_ids": ["sensor-001", "sensor-002"],
    "aggregation": "avg",
    "window_size_seconds": 60,
    "allowed_lateness_seconds": 30
  }'
```

- 成员不存在 → `404`；窗口配置不一致或组 ID 已存在 → `409`；成员列表重复 → `400`。
- 组创建后即对成员已有窗口做一次补算。

### 8. 按窗口查询组汇总

```bash
# 全部窗口（pending + ready）
curl http://localhost:8000/groups/floor-1/windows

# 可按状态 / window_start 过滤
curl 'http://localhost:8000/groups/floor-1/windows?status=pending'
curl 'http://localhost:8000/groups/floor-1/windows?window_start=2026-09-22T10:00:00Z'

# 查询单个窗口
curl http://localhost:8000/groups/floor-1/windows/2026-09-22T10:00:00Z
```

仅当 `sensor-001`、`sensor-002` 在 `10:00` 的窗口都封存后才 ready；此前为 pending 且 `value` 为 `null`：

```json
[
  {
    "group_id": "floor-1",
    "window_start": "2026-09-22T10:00:00Z",
    "window_end": "2026-09-22T10:01:00Z",
    "status": "pending",
    "value": null,
    "member_count": 2,
    "sealed_member_count": 1,
    "revision": 0,
    "ready_at": null,
    "created_at": "2026-09-22T10:02:31Z",
    "updated_at": "2026-09-22T10:02:31Z"
  }
]
```

全部封存后：`status` 变为 `ready`，`revision` 从 1 起。此后任一成员窗口收到迟到事件，该记录原位重算、`revision` 递增；同一 `event_id` 重试不升版。

### 9. 查询 pending 成员明细

```bash
curl http://localhost:8000/groups/floor-1/windows/2026-09-22T10:00:00Z/pending
```

`reason=open` 表示成员窗口尚未封存；`reason=missing` 表示该成员在该窗口还没有任何事件（窗口不存在）。ready 窗口返回 `[]`，尚无任何成员数据的窗口返回 `404`：

```json
[
  {
    "device_id": "sensor-002",
    "window_start": null,
    "window_end": "2026-09-22T10:01:00Z",
    "window_status": null,
    "reason": "missing"
  }
]
```

### 10. 查询组窗口修订列表（不可变快照）

组窗口每次首次 ready、以及迟到修正升版时，都会在同一事务留一份不可变快照：

```bash
curl http://localhost:8000/groups/floor-1/windows/2026-09-22T10:00:00Z/revisions
```

```json
[
  {"revision": 1, "value": 62.3, "kind": "revision", "created_at": "2026-09-22T10:02:31Z", "member_count": 2},
  {"revision": 2, "value": 67.8, "kind": "revision", "created_at": "2026-09-22T10:03:10Z", "member_count": 2}
]
```

- 尚未 ready（pending）的窗口返回 `[]`；组或窗口不存在返回 `404`。
- 旧版本数据库升级后首次启动，只会为已 ready 窗口补一份**当前 revision** 的 `baseline`，不伪造早期版本。
- 快照只追加、不可修改：任何对审计表的 `UPDATE`/`DELETE` 都会被数据库触发器中止；同一 revision 重复上报也受唯一约束保护，不会重复留档。

### 11. 对比两个 revision（diff）

```bash
curl 'http://localhost:8000/groups/floor-1/windows/2026-09-22T10:00:00Z/revisions/diff?from_revision=1&to_revision=2'
```

响应按成员顺序只列出发生变化的成员，给出前后贡献与各项数值差（`sum` 聚合下贡献为成员窗口 `sum`，`avg` 聚合下为成员窗口 `avg`）：

```json
{
  "group_id": "floor-1",
  "window_start": "2026-09-22T10:00:00Z",
  "aggregation": "sum",
  "from_revision": 1,
  "to_revision": 2,
  "from_value": 62.3,
  "to_value": 67.8,
  "value_delta": 5.5,
  "members": [
    {
      "device_id": "sensor-001",
      "position": 0,
      "before": {"device_id": "sensor-001", "position": 0, "count": 3, "sum": 41.4, "avg": 13.8, "correction_count": 0, "contribution": 41.4},
      "after":  {"device_id": "sensor-001", "position": 0, "count": 4, "sum": 46.9, "avg": 11.725, "correction_count": 1, "contribution": 46.9},
      "count_delta": 1,
      "sum_delta": 5.5,
      "avg_delta": -2.075,
      "correction_count_delta": 1,
      "contribution_delta": 5.5
    }
  ]
}
```

- `from_revision` / `to_revision` 均必填且 `>= 1`，且 `from_revision < to_revision`，否则返回 `422`。
- 组、窗口不存在，或指定的任一 revision 无快照（例如 baseline 之前被伪造的旧版本号）返回 `404`。

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `POST` | `/ingest` | 按设备提交事件批次（幂等） |
| `PUT` | `/devices/{device_id}/config` | 创建/更新设备窗口与允许迟到配置 |
| `GET` | `/devices/{device_id}` | 设备水位线与统计 |
| `GET` | `/devices/{device_id}/windows` | 各时间窗聚合值、状态、修正次数（支持 `?status=open\|sealed`） |
| `GET` | `/devices/{device_id}/corrections` | 迟到数据修正记录 |
| `POST` | `/groups` | 创建设备组（固定成员、sum/avg、窗口配置一致校验） |
| `GET` | `/groups/{group_id}` | 组信息与成员列表 |
| `GET` | `/groups/{group_id}/windows` | 组窗口汇总列表（支持 `?status=pending\|ready&window_start=...`） |
| `GET` | `/groups/{group_id}/windows/{window_start}` | 查询单个组窗口 |
| `GET` | `/groups/{group_id}/windows/{window_start}/pending` | pending 成员明细（`missing`/`open`；ready 返回空列表） |
| `GET` | `/groups/{group_id}/windows/{window_start}/revisions` | 组窗口修订审计列表（不可变快照，含初始化 baseline） |
| `GET` | `/groups/{group_id}/windows/{window_start}/revisions/diff?from_revision=&to_revision=` | 两版本 diff：按成员顺序返回变化成员的前后贡献与数值差（非法版本 422，不存在 404） |

## 项目结构

```
app/
  main.py        # FastAPI 应用、遥测聚合、设备组同步窗口汇总与修订审计快照逻辑
  db.py          # SQLite 连接、建表（含 append-only 审计表/触发器）与事务会话
tests/
  test_api.py       # 端到端测试（水位线/封存/去重/迟到修正/持久化）
  test_groups.py    # 设备组测试（pending/ready、修正升版、重试幂等、成员明细）
  test_revisions.py # 修订审计测试（快照留档、diff、422/404、不可变性、旧库 baseline 补档）
Dockerfile
docker-compose.yml
requirements.txt
requirements-dev.txt
```
