"""传感器遥测汇聚 API。

核心语义：
- 每个设备维护一条持久化的事件时间水位线 watermark = 已见最大 event_time。
- 时间窗为按 window_size_seconds 对齐 epoch 的滚动窗口。
- 当 watermark >= window_end + allowed_lateness_seconds 时，窗口封存（sealed）。
- 重复 (device_id, event_id) 幂等去重，不影响聚合结果。
- 窗口封存后到达的事件标记为 late，仍计入聚合，同时生成一条修正记录，
  窗口的 correction_count 随之递增。

设备组同步窗口汇总：
- 设备组为固定成员组，创建时所有成员设备必须存在且窗口配置一致
  （window_size_seconds / allowed_lateness_seconds），聚合方式为 sum 或 avg。
- 同一 window_start 下仅当全部成员的设备窗口均已封存（sealed）时，
  才生成组汇总（ready）；任一成员窗口未封存（或尚无窗口）时保持 pending，
  value 为 NULL，绝不用部分成员提前计算。
- 成员窗口因迟到事件发生修正后，已生成的组汇总原位重算且 revision 加一；
- 修正以成员窗口 correction_count 之和为签名：同一 event_id 重试是幂等去重，
  不会产生新修正，签名不变，revision 不重复升版。

组窗口修订审计（append-only 快照）：
- 组窗口首次 ready、以及迟到修正令 revision 升版时，在**同一事务**追加一份不可变快照，
  记录当时的 group value 与每个固定成员窗口的 count、sum、avg、correction_count。
- (group_id, window_start, revision) 唯一约束保证重复上报不会重复留档；
  审计表带禁止 UPDATE/DELETE 的触发器，后续修正只能追加新版本、旧快照永不被覆盖。
- 服务初始化时只对旧库中**已 ready** 的组窗口补一份**当前 revision** 的 baseline，
  绝不伪造更早的历史版本。
- 提供修订列表与两版本 diff 接口；diff 按成员顺序给出前后贡献与数值差，仅列发生变化的成员，
  非法版本返回 422、不存在的组/窗口/修订返回 404。
"""

from __future__ import annotations

import math
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import db

DEFAULT_WINDOW_SIZE_SECONDS = 60
DEFAULT_ALLOWED_LATENESS_SECONDS = 30
MAX_GROUP_MEMBERS = 500


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    backfill_baseline_snapshots()
    yield


app = FastAPI(title="Sensor Telemetry Aggregation API", version="1.2.0", lifespan=lifespan)


# ---------- Pydantic 模型 ----------

class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    event_time: datetime  # ISO 8601；不带时区时按 UTC 处理
    value: float


class IngestRequest(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    events: list[EventIn] = Field(min_length=1, max_length=10000)


class EventResult(BaseModel):
    event_id: str
    status: Literal["accepted", "duplicate", "late"]
    window_start: Optional[datetime] = None


class IngestResponse(BaseModel):
    device_id: str
    watermark: Optional[datetime]
    accepted: int
    duplicates: int
    late: int
    results: list[EventResult]


class DeviceConfig(BaseModel):
    window_size_seconds: int = Field(default=DEFAULT_WINDOW_SIZE_SECONDS, gt=0, le=86400)
    allowed_lateness_seconds: int = Field(default=DEFAULT_ALLOWED_LATENESS_SECONDS, ge=0, le=86400)


class DeviceOut(BaseModel):
    device_id: str
    window_size_seconds: int
    allowed_lateness_seconds: int
    watermark: Optional[datetime]
    event_count: int
    open_windows: int
    sealed_windows: int
    created_at: datetime


class WindowOut(BaseModel):
    device_id: str
    window_start: datetime
    window_end: datetime
    status: Literal["open", "sealed"]
    count: int
    sum: float
    avg: Optional[float]
    min: Optional[float]
    max: Optional[float]
    correction_count: int
    late_event_count: int
    sealed_at: Optional[datetime]


class CorrectionOut(BaseModel):
    id: int
    device_id: str
    event_id: str
    window_start: datetime
    event_time: datetime
    value: float
    created_at: datetime


class GroupCreate(BaseModel):
    group_id: str = Field(min_length=1, max_length=128)
    member_ids: list[str] = Field(min_length=1, max_length=MAX_GROUP_MEMBERS)
    aggregation: Literal["sum", "avg"]
    window_size_seconds: int = Field(gt=0, le=86400)
    allowed_lateness_seconds: int = Field(ge=0, le=86400)


class GroupOut(BaseModel):
    group_id: str
    member_ids: list[str]
    aggregation: Literal["sum", "avg"]
    window_size_seconds: int
    allowed_lateness_seconds: int
    created_at: datetime


class GroupWindowOut(BaseModel):
    group_id: str
    window_start: datetime
    window_end: datetime
    status: Literal["pending", "ready"]
    value: Optional[float] = None
    member_count: int
    sealed_member_count: int
    revision: int
    ready_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class PendingMemberOut(BaseModel):
    device_id: str
    window_start: Optional[datetime] = None
    window_end: datetime
    window_status: Optional[Literal["open", "sealed"]] = None  # None = 成员尚无该窗口
    reason: Literal["missing", "open"]


class GroupWindowRevisionOut(BaseModel):
    revision: int
    value: float
    kind: Literal["revision", "baseline"]
    created_at: datetime
    member_count: int


class RevisionMemberStateOut(BaseModel):
    device_id: str
    position: int
    count: int
    sum: float
    avg: Optional[float] = None
    correction_count: int
    contribution: float  # 该成员当时对 group value 的贡献：sum 聚合为 sum，avg 聚合为窗口 avg


class RevisionMemberDeltaOut(BaseModel):
    device_id: str
    position: int
    before: RevisionMemberStateOut
    after: RevisionMemberStateOut
    count_delta: int
    sum_delta: float
    avg_delta: Optional[float] = None
    correction_count_delta: int
    contribution_delta: float


class RevisionDiffOut(BaseModel):
    group_id: str
    window_start: datetime
    aggregation: Literal["sum", "avg"]
    from_revision: int
    to_revision: int
    from_value: float
    to_value: float
    value_delta: float
    members: list[RevisionMemberDeltaOut]  # 仅含发生变化的成员，按成员顺序排列


# ---------- 工具函数 ----------

def to_epoch(dt: datetime) -> float:
    """datetime -> epoch 秒；naive 时间按 UTC 处理。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def to_iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def window_floor(t: float, size: int) -> float:
    return math.floor(t / size) * size


def get_device(conn: sqlite3.Connection, device_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,)).fetchone()


def get_or_create_device(conn: sqlite3.Connection, device_id: str, now: str) -> sqlite3.Row:
    row = get_device(conn, device_id)
    if row is None:
        conn.execute(
            "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds, watermark, created_at)"
            " VALUES (?, ?, ?, NULL, ?)",
            (device_id, DEFAULT_WINDOW_SIZE_SECONDS, DEFAULT_ALLOWED_LATENESS_SECONDS, now),
        )
        row = get_device(conn, device_id)
    return row


def require_device(conn: sqlite3.Connection, device_id: str) -> sqlite3.Row:
    row = get_device(conn, device_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"device not found: {device_id}")
    return row


def device_out(conn: sqlite3.Connection, device_id: str) -> DeviceOut:
    dev = require_device(conn, device_id)
    stats = conn.execute(
        "SELECT"
        " (SELECT COUNT(*) FROM events WHERE device_id = ?) AS event_count,"
        " (SELECT COUNT(*) FROM windows WHERE device_id = ? AND status = 'open') AS open_windows,"
        " (SELECT COUNT(*) FROM windows WHERE device_id = ? AND status = 'sealed') AS sealed_windows",
        (device_id, device_id, device_id),
    ).fetchone()
    return DeviceOut(
        device_id=dev["device_id"],
        window_size_seconds=dev["window_size_seconds"],
        allowed_lateness_seconds=dev["allowed_lateness_seconds"],
        watermark=to_iso(dev["watermark"]),
        event_count=stats["event_count"],
        open_windows=stats["open_windows"],
        sealed_windows=stats["sealed_windows"],
        created_at=dev["created_at"],
    )


# ---------- 设备组工具 ----------

def placeholders(n: int) -> str:
    return ",".join("?" * n)


def require_group(conn: sqlite3.Connection, group_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM device_groups WHERE group_id = ?", (group_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"group not found: {group_id}")
    return row


def group_member_ids(conn: sqlite3.Connection, group_id: str) -> list[str]:
    rows = conn.execute(
        "SELECT device_id FROM device_group_members WHERE group_id = ? ORDER BY position",
        (group_id,),
    ).fetchall()
    return [r["device_id"] for r in rows]


def reconcile_group(conn: sqlite3.Connection, group_id: str, now: str) -> None:
    """对一个组的各 window_start 做对账。

    - 全部成员窗口 sealed → ready，计算 sum/avg；已 ready 且成员发生新修正时原位重算、revision+1。
    - 否则维持/落为 pending，value 始终为 NULL，绝不使用部分成员数据。
    触发时机：成员设备 ingest 完成封存后，以及组创建后。
    """
    group = conn.execute("SELECT * FROM device_groups WHERE group_id = ?", (group_id,)).fetchone()
    if group is None:
        return
    size = group["window_size_seconds"]
    aggregation = group["aggregation"]

    members = group_member_ids(conn, group_id)
    member_n = len(members)

    # 组窗口口径：任一成员出现过的、属于本组对齐网格的 window_start 都是候选。
    # 组创建时已保证成员窗口配置一致，这里再按窗口大小过滤以排除理论上的异网格数据。
    starts = [
        r["window_start"]
        for r in conn.execute(
            f"SELECT DISTINCT w.window_start AS window_start FROM windows w"
            f" JOIN device_group_members m ON m.device_id = w.device_id AND m.group_id = ?"
            f" WHERE (w.window_start - CAST(w.window_start AS INTEGER)) = 0"
            f"   AND CAST(w.window_start AS INTEGER) % ? = 0"
            f" ORDER BY w.window_start",
            (group_id, size),
        ).fetchall()
    ]

    for w_start in starts:
        w_end = w_start + size
        win_rows = {
            r["device_id"]: r
            for r in conn.execute(
                f"SELECT w.* FROM windows w"
                f" JOIN device_group_members m ON m.device_id = w.device_id AND m.group_id = ?"
                f" WHERE w.window_start = ? ORDER BY m.position",
                (group_id, w_start),
            ).fetchall()
        }
        sealed = [win_rows[d] for d in members if d in win_rows and win_rows[d]["status"] == "sealed"]
        sealed_n = len(sealed)
        all_sealed = sealed_n == member_n
        correction_sig = sum(r["correction_count"] for r in sealed)

        existing = conn.execute(
            "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
            (group_id, w_start),
        ).fetchone()

        if all_sealed:
            if aggregation == "sum":
                value = sum(r["sum"] for r in sealed)
            else:
                # avg：各成员窗口平均值的算术平均（每个固定成员权重相同）
                value = sum(r["sum"] / r["count"] for r in sealed if r["count"] > 0) / member_n

            if existing is None:
                conn.execute(
                    "INSERT INTO group_windows (group_id, window_start, window_end, status, value,"
                    " member_count, sealed_member_count, revision, correction_sig, ready_at,"
                    " created_at, updated_at)"
                    " VALUES (?, ?, ?, 'ready', ?, ?, ?, 1, ?, ?, ?, ?)",
                    (group_id, w_start, w_end, value, member_n, member_n, correction_sig, now, now, now),
                )
                # 首次 ready：同一事务追加 revision=1 的不可变快照
                append_revision_snapshot(conn, group_id, w_start, 1, value, members, win_rows, now)
            elif existing["status"] == "ready":
                # 已生成的组汇总原位重算：仅当成员窗口出现新的迟到修正（签名推进）时升版。
                # 同一 event_id 重试在 ingest 侧幂等去重，correction_count 不变，故不重复升版。
                if correction_sig > existing["correction_sig"]:
                    new_revision = existing["revision"] + 1
                    conn.execute(
                        "UPDATE group_windows SET status = 'ready', value = ?, sealed_member_count = ?,"
                        " revision = revision + 1, correction_sig = ?, updated_at = ?"
                        " WHERE group_id = ? AND window_start = ?",
                        (value, member_n, correction_sig, now, group_id, w_start),
                    )
                    # revision 升版：同一事务追加新快照；旧快照不动、绝不覆盖
                    append_revision_snapshot(
                        conn, group_id, w_start, new_revision, value, members, win_rows, now
                    )
            else:
                # pending → ready（迟到修正发生在 ready 之前，已包含在首次结果里，revision 从 1 起）
                conn.execute(
                    "UPDATE group_windows SET status = 'ready', value = ?, sealed_member_count = ?,"
                    " revision = 1, correction_sig = ?, ready_at = ?, updated_at = ?"
                    " WHERE group_id = ? AND window_start = ?",
                    (value, member_n, correction_sig, now, now, group_id, w_start),
                )
                # pending → ready 的首次 ready：同一事务追加 revision=1 快照
                append_revision_snapshot(conn, group_id, w_start, 1, value, members, win_rows, now)
        else:
            if existing is None:
                conn.execute(
                    "INSERT INTO group_windows (group_id, window_start, window_end, status, value,"
                    " member_count, sealed_member_count, revision, correction_sig, ready_at,"
                    " created_at, updated_at)"
                    " VALUES (?, ?, ?, 'pending', NULL, ?, ?, 0, 0, NULL, ?, ?)",
                    (group_id, w_start, w_end, member_n, sealed_n, now, now),
                )
            elif existing["status"] == "pending" and existing["sealed_member_count"] != sealed_n:
                conn.execute(
                    "UPDATE group_windows SET sealed_member_count = ?, updated_at = ?"
                    " WHERE group_id = ? AND window_start = ?",
                    (sealed_n, now, group_id, w_start),
                )


def reconcile_groups_for_device(conn: sqlite3.Connection, device_id: str, now: str) -> None:
    """成员设备 ingest 后，对其所属的全部组做对账。"""
    group_ids = [
        r["group_id"]
        for r in conn.execute(
            "SELECT group_id FROM device_group_members WHERE device_id = ?", (device_id,)
        ).fetchall()
    ]
    for group_id in group_ids:
        reconcile_group(conn, group_id, now)


def append_revision_snapshot(
    conn: sqlite3.Connection,
    group_id: str,
    window_start: float,
    revision: int,
    value: float,
    members: list[str],
    win_rows: dict[str, sqlite3.Row],
    now: str,
    kind: Literal["revision", "baseline"] = "revision",
) -> None:
    """在当前事务内为组窗口的一个 revision 追加不可变快照。

    快照头记录当时的 group value；成员明细按固定成员顺序记录每个成员窗口当时的
    count/sum/avg/correction_count。(group_id, window_start, revision) 唯一约束兜底，
    重复上报（签名未推进）不会再次进入这里，即使进入也会被约束挡下而不重复留档。
    """
    cur = conn.execute(
        "INSERT INTO group_window_snapshots"
        " (group_id, window_start, revision, value, kind, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (group_id, window_start, revision, value, kind, now),
    )
    snapshot_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO group_window_snapshot_members"
        " (snapshot_id, group_id, device_id, position, count, sum, avg, correction_count)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                snapshot_id,
                group_id,
                device_id,
                position,
                win_rows[device_id]["count"],
                win_rows[device_id]["sum"],
                (
                    win_rows[device_id]["sum"] / win_rows[device_id]["count"]
                    if win_rows[device_id]["count"]
                    else None
                ),
                win_rows[device_id]["correction_count"],
            )
            for position, device_id in enumerate(members)
        ],
    )


def backfill_baseline_snapshots() -> None:
    """旧库升级时为已 ready 但没有任何快照的组窗口补一份**当前 revision** 的 baseline。

    只补当前版本这一份，绝不伪造 ready 之后可能存在过的早期版本；幂等：
    已有任意快照的 ready 窗口跳过，新库（启动即为空）无任何可补数据。
    """
    now = utcnow_iso()
    with db.session() as conn:
        ready_rows = conn.execute(
            "SELECT * FROM group_windows WHERE status = 'ready'"
        ).fetchall()
        for gw in ready_rows:
            exists = conn.execute(
                "SELECT 1 FROM group_window_snapshots"
                " WHERE group_id = ? AND window_start = ? LIMIT 1",
                (gw["group_id"], gw["window_start"]),
            ).fetchone()
            if exists:
                continue

            members = group_member_ids(conn, gw["group_id"])
            win_rows = {
                r["device_id"]: r
                for r in conn.execute(
                    "SELECT w.* FROM windows w"
                    " JOIN device_group_members m ON m.device_id = w.device_id AND m.group_id = ?"
                    " WHERE w.window_start = ?",
                    (gw["group_id"], gw["window_start"]),
                ).fetchall()
            }
            # 数据不自洽（有成员缺窗口）时不补，避免留下不完整的基线
            if len(win_rows) != len(members):
                continue
            append_revision_snapshot(
                conn,
                gw["group_id"],
                gw["window_start"],
                gw["revision"],
                gw["value"],
                members,
                win_rows,
                now,
                kind="baseline",
            )


def group_window_out(r: sqlite3.Row) -> GroupWindowOut:
    return GroupWindowOut(
        group_id=r["group_id"],
        window_start=to_iso(r["window_start"]),
        window_end=to_iso(r["window_end"]),
        status=r["status"],
        value=r["value"],
        member_count=r["member_count"],
        sealed_member_count=r["sealed_member_count"],
        revision=r["revision"],
        ready_at=r["ready_at"],
        created_at=r["created_at"],
        updated_at=r["updated_at"],
    )


# ---------- 接口 ----------

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/ingest", response_model=IngestResponse)
def ingest(req: IngestRequest) -> IngestResponse:
    """按设备提交事件批次。批次内事件按提交顺序处理（与流式语义一致）。"""
    now = utcnow_iso()
    results: list[EventResult] = []

    with db.session() as conn:
        dev = get_or_create_device(conn, req.device_id, now)
        watermark = dev["watermark"]
        size = dev["window_size_seconds"]
        lateness = dev["allowed_lateness_seconds"]

        for ev in req.events:
            t = to_epoch(ev.event_time)

            # 幂等去重：同一 (device_id, event_id) 只生效一次
            dup = conn.execute(
                "SELECT 1 FROM events WHERE device_id = ? AND event_id = ?",
                (req.device_id, ev.event_id),
            ).fetchone()
            if dup:
                results.append(EventResult(event_id=ev.event_id, status="duplicate"))
                continue

            w_start = window_floor(t, size)
            w_end = w_start + size
            # 所属窗口已封存（水位线已越过 window_end + 允许迟到窗口）=> 迟到数据
            is_late = watermark is not None and watermark >= w_end + lateness
            status = "late" if is_late else "accepted"

            conn.execute(
                "INSERT INTO events (device_id, event_id, event_time, value, status, ingested_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (req.device_id, ev.event_id, t, ev.value, status, now),
            )
            conn.execute(
                """
                INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,
                                     status, correction_count, late_event_count)
                VALUES (?, ?, ?, 1, ?, ?, ?, 'open', ?, ?)
                ON CONFLICT (device_id, window_start) DO UPDATE SET
                    count             = count + 1,
                    sum               = sum + excluded.sum,
                    min               = MIN(windows.min, excluded.min),
                    max               = MAX(windows.max, excluded.max),
                    correction_count  = correction_count + excluded.correction_count,
                    late_event_count  = late_event_count + excluded.late_event_count
                """,
                (
                    req.device_id, w_start, w_end, ev.value, ev.value, ev.value,
                    1 if is_late else 0,
                    1 if is_late else 0,
                ),
            )
            if is_late:
                # 迟到数据落入已封存窗口：生成修正记录
                conn.execute(
                    "INSERT INTO corrections (device_id, event_id, window_start, event_time, value, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (req.device_id, ev.event_id, w_start, t, ev.value, now),
                )

            if watermark is None or t > watermark:
                watermark = t
            results.append(EventResult(event_id=ev.event_id, status=status, window_start=to_iso(w_start)))

        # 持久化水位线，并封存所有越过允许迟到窗口的窗口
        conn.execute(
            "UPDATE devices SET watermark = ? WHERE device_id = ?",
            (watermark, req.device_id),
        )
        conn.execute(
            "UPDATE windows SET status = 'sealed', sealed_at = ?"
            " WHERE device_id = ? AND status = 'open' AND window_end + ? <= ?",
            (now, req.device_id, lateness, watermark),
        )

        # 窗口封存 / 迟到修正后，同步重算该设备所在组的同步窗口汇总
        reconcile_groups_for_device(conn, req.device_id, now)

    return IngestResponse(
        device_id=req.device_id,
        watermark=to_iso(watermark),
        accepted=sum(1 for r in results if r.status == "accepted"),
        duplicates=sum(1 for r in results if r.status == "duplicate"),
        late=sum(1 for r in results if r.status == "late"),
        results=results,
    )


@app.put("/devices/{device_id}/config", response_model=DeviceOut)
def put_device_config(device_id: str, cfg: DeviceConfig) -> DeviceOut:
    """创建或更新设备配置。已有数据后不允许修改窗口大小（避免历史窗口口径不一致）。"""
    with db.session() as conn:
        dev = get_device(conn, device_id)
        if dev is None:
            conn.execute(
                "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds, watermark, created_at)"
                " VALUES (?, ?, ?, NULL, ?)",
                (device_id, cfg.window_size_seconds, cfg.allowed_lateness_seconds, utcnow_iso()),
            )
        else:
            if cfg.window_size_seconds != dev["window_size_seconds"]:
                n = conn.execute(
                    "SELECT COUNT(*) AS c FROM windows WHERE device_id = ?", (device_id,)
                ).fetchone()["c"]
                if n > 0:
                    raise HTTPException(
                        status_code=409,
                        detail="window_size_seconds cannot be changed after data has been ingested",
                    )
            conn.execute(
                "UPDATE devices SET window_size_seconds = ?, allowed_lateness_seconds = ? WHERE device_id = ?",
                (cfg.window_size_seconds, cfg.allowed_lateness_seconds, device_id),
            )
        return device_out(conn, device_id)


@app.get("/devices/{device_id}", response_model=DeviceOut)
def get_device_info(device_id: str) -> DeviceOut:
    with db.session() as conn:
        return device_out(conn, device_id)


@app.get("/devices/{device_id}/windows", response_model=list[WindowOut])
def list_windows(
    device_id: str,
    status: Optional[Literal["open", "sealed"]] = None,
) -> list[WindowOut]:
    """返回设备各时间窗的聚合值、状态与修正次数。"""
    with db.session() as conn:
        require_device(conn, device_id)
        sql = "SELECT * FROM windows WHERE device_id = ?"
        params: list = [device_id]
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY window_start"
        rows = conn.execute(sql, params).fetchall()
        return [
            WindowOut(
                device_id=r["device_id"],
                window_start=to_iso(r["window_start"]),
                window_end=to_iso(r["window_end"]),
                status=r["status"],
                count=r["count"],
                sum=r["sum"],
                avg=r["sum"] / r["count"] if r["count"] else None,
                min=r["min"],
                max=r["max"],
                correction_count=r["correction_count"],
                late_event_count=r["late_event_count"],
                sealed_at=r["sealed_at"],
            )
            for r in rows
        ]


@app.get("/devices/{device_id}/corrections", response_model=list[CorrectionOut])
def list_corrections(device_id: str) -> list[CorrectionOut]:
    """返回设备全部修正记录（窗口封存后到达的迟到数据）。"""
    with db.session() as conn:
        require_device(conn, device_id)
        rows = conn.execute(
            "SELECT * FROM corrections WHERE device_id = ? ORDER BY id", (device_id,)
        ).fetchall()
        return [
            CorrectionOut(
                id=r["id"],
                device_id=r["device_id"],
                event_id=r["event_id"],
                window_start=to_iso(r["window_start"]),
                event_time=to_iso(r["event_time"]),
                value=r["value"],
                created_at=r["created_at"],
            )
            for r in rows
        ]


# ---------- 设备组接口 ----------

@app.post("/groups", response_model=GroupOut, status_code=201)
def create_group(req: GroupCreate) -> GroupOut:
    """创建固定成员设备组。

    成员设备必须均已存在，且窗口配置（window_size_seconds、allowed_lateness_seconds）
    与请求一致，保证同一 window_start 的成员窗口对齐、封存口径一致。
    """
    now = utcnow_iso()
    member_ids = req.member_ids
    if len(set(member_ids)) != len(member_ids):
        raise HTTPException(status_code=400, detail="member_ids must not contain duplicates")

    with db.session() as conn:
        if conn.execute(
            "SELECT 1 FROM device_groups WHERE group_id = ?", (req.group_id,)
        ).fetchone():
            raise HTTPException(status_code=409, detail=f"group already exists: {req.group_id}")

        missing: list[str] = []
        mismatched: list[str] = []
        for device_id in member_ids:
            dev = get_device(conn, device_id)
            if dev is None:
                missing.append(device_id)
            elif (
                dev["window_size_seconds"] != req.window_size_seconds
                or dev["allowed_lateness_seconds"] != req.allowed_lateness_seconds
            ):
                mismatched.append(device_id)
        if missing:
            raise HTTPException(
                status_code=404, detail=f"devices not found: {', '.join(missing)}"
            )
        if mismatched:
            raise HTTPException(
                status_code=409,
                detail=(
                    "devices with window config different from group config: "
                    + ", ".join(mismatched)
                ),
            )

        conn.execute(
            "INSERT INTO device_groups (group_id, window_size_seconds, allowed_lateness_seconds,"
            " aggregation, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                req.group_id,
                req.window_size_seconds,
                req.allowed_lateness_seconds,
                req.aggregation,
                now,
            ),
        )
        conn.executemany(
            "INSERT INTO device_group_members (group_id, device_id, position) VALUES (?, ?, ?)",
            [(req.group_id, device_id, i) for i, device_id in enumerate(member_ids)],
        )

        # 若成员已有封存窗口，组创建后立即对账补算
        reconcile_group(conn, req.group_id, now)

        return GroupOut(
            group_id=req.group_id,
            member_ids=member_ids,
            aggregation=req.aggregation,
            window_size_seconds=req.window_size_seconds,
            allowed_lateness_seconds=req.allowed_lateness_seconds,
            created_at=now,
        )


@app.get("/groups/{group_id}", response_model=GroupOut)
def get_group(group_id: str) -> GroupOut:
    with db.session() as conn:
        group = require_group(conn, group_id)
        return GroupOut(
            group_id=group["group_id"],
            member_ids=group_member_ids(conn, group_id),
            aggregation=group["aggregation"],
            window_size_seconds=group["window_size_seconds"],
            allowed_lateness_seconds=group["allowed_lateness_seconds"],
            created_at=group["created_at"],
        )


@app.get("/groups/{group_id}/windows", response_model=list[GroupWindowOut])
def list_group_windows(
    group_id: str,
    status: Optional[Literal["pending", "ready"]] = None,
    window_start: Optional[datetime] = None,
) -> list[GroupWindowOut]:
    """按窗口查询组汇总，可按 status / window_start 过滤。pending 窗口 value 为 null。"""
    with db.session() as conn:
        require_group(conn, group_id)
        sql = "SELECT * FROM group_windows WHERE group_id = ?"
        params: list = [group_id]
        if status:
            sql += " AND status = ?"
            params.append(status)
        if window_start is not None:
            sql += " AND window_start = ?"
            params.append(to_epoch(window_start))
        sql += " ORDER BY window_start"
        rows = conn.execute(sql, params).fetchall()
        return [group_window_out(r) for r in rows]


@app.get("/groups/{group_id}/windows/{window_start}", response_model=GroupWindowOut)
def get_group_window(group_id: str, window_start: datetime) -> GroupWindowOut:
    """查询组在单个 window_start 上的汇总（pending 或 ready）。"""
    with db.session() as conn:
        require_group(conn, group_id)
        row = conn.execute(
            "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
            (group_id, to_epoch(window_start)),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="group window not found (no member has data yet)")
        return group_window_out(row)


@app.get(
    "/groups/{group_id}/windows/{window_start}/pending",
    response_model=list[PendingMemberOut],
)
def list_pending_members(group_id: str, window_start: datetime) -> list[PendingMemberOut]:
    """返回该组窗口尚未封存的成员明细。

    - 组窗口已 ready：返回空列表（无缺失成员）。
    - 组窗口尚不存在（成员均无数据）：404。
    - 成员在该 window_start 没有窗口：reason=missing；窗口仍 open：reason=open。
    """
    w_start = to_epoch(window_start)
    with db.session() as conn:
        group = require_group(conn, group_id)
        gw = conn.execute(
            "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
            (group_id, w_start),
        ).fetchone()
        if gw is None:
            raise HTTPException(status_code=404, detail="group window not found (no member has data yet)")
        if gw["status"] == "ready":
            return []

        w_end = w_start + group["window_size_seconds"]
        members = group_member_ids(conn, group_id)
        placeholders_sql = placeholders(len(members))
        win_rows = {
            r["device_id"]: r
            for r in conn.execute(
                f"SELECT * FROM windows WHERE window_start = ? AND device_id IN ({placeholders_sql})",
                [w_start, *members],
            ).fetchall()
        }

        result: list[PendingMemberOut] = []
        for device_id in members:
            w = win_rows.get(device_id)
            if w is None:
                result.append(PendingMemberOut(
                    device_id=device_id,
                    window_start=None,
                    window_end=to_iso(w_end),
                    window_status=None,
                    reason="missing",
                ))
            elif w["status"] != "sealed":
                result.append(PendingMemberOut(
                    device_id=device_id,
                    window_start=to_iso(w["window_start"]),
                    window_end=to_iso(w["window_end"]),
                    window_status=w["status"],
                    reason="open",
                ))
        return result


# ---------- 组窗口修订审计接口 ----------

def _require_group_window(conn: sqlite3.Connection, group_id: str, w_start: float) -> sqlite3.Row:
    require_group(conn, group_id)
    gw = conn.execute(
        "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
        (group_id, w_start),
    ).fetchone()
    if gw is None:
        raise HTTPException(status_code=404, detail="group window not found (no member has data yet)")
    return gw


def _require_snapshot(
    conn: sqlite3.Connection, group_id: str, w_start: float, revision: int
) -> sqlite3.Row:
    snap = conn.execute(
        "SELECT * FROM group_window_snapshots WHERE group_id = ? AND window_start = ? AND revision = ?",
        (group_id, w_start, revision),
    ).fetchone()
    if snap is None:
        raise HTTPException(
            status_code=404, detail=f"revision not found: {revision}"
        )
    return snap


def _snapshot_member_states(
    conn: sqlite3.Connection, aggregation: str, snapshot_id: int
) -> dict[str, dict]:
    """读取某份快照的成员明细，并算出各成员当时对 group value 的贡献。"""
    rows = conn.execute(
        "SELECT * FROM group_window_snapshot_members WHERE snapshot_id = ? ORDER BY position",
        (snapshot_id,),
    ).fetchall()
    states: dict[str, dict] = {}
    for r in rows:
        contribution = r["sum"] if aggregation == "sum" else (r["avg"] if r["avg"] is not None else 0.0)
        states[r["device_id"]] = {
            "device_id": r["device_id"],
            "position": r["position"],
            "count": r["count"],
            "sum": r["sum"],
            "avg": r["avg"],
            "correction_count": r["correction_count"],
            "contribution": contribution,
        }
    return states


@app.get(
    "/groups/{group_id}/windows/{window_start}/revisions",
    response_model=list[GroupWindowRevisionOut],
)
def list_group_window_revisions(group_id: str, window_start: datetime) -> list[GroupWindowRevisionOut]:
    """返回组窗口的修订审计列表（不可变快照），按 revision 升序。

    组窗口存在但尚未 ready（无任何修订）时返回空列表；组或窗口不存在返回 404。
    """
    w_start = to_epoch(window_start)
    with db.session() as conn:
        _require_group_window(conn, group_id, w_start)
        rows = conn.execute(
            "SELECT s.*, ("
            "  SELECT COUNT(*) FROM group_window_snapshot_members m WHERE m.snapshot_id = s.id"
            " ) AS member_count"
            " FROM group_window_snapshots s"
            " WHERE s.group_id = ? AND s.window_start = ?"
            " ORDER BY s.revision",
            (group_id, w_start),
        ).fetchall()
        return [
            GroupWindowRevisionOut(
                revision=r["revision"],
                value=r["value"],
                kind=r["kind"],
                created_at=r["created_at"],
                member_count=r["member_count"],
            )
            for r in rows
        ]


@app.get(
    "/groups/{group_id}/windows/{window_start}/revisions/diff",
    response_model=RevisionDiffOut,
)
def diff_group_window_revisions(
    group_id: str,
    window_start: datetime,
    from_revision: int = Query(..., ge=1),
    to_revision: int = Query(..., ge=1),
) -> RevisionDiffOut:
    """对比组窗口两个 revision 的不可变快照。

    按成员顺序返回每个发生变化成员的前后贡献与各数值差（count/sum/avg/correction_count），
    仅列变化成员。from/to 必须为 >=1 且 from < to，否则 422；
    组、窗口或任一 revision 不存在返回 404。
    """
    if from_revision >= to_revision:
        raise HTTPException(
            status_code=422, detail="from_revision must be a positive revision smaller than to_revision"
        )

    w_start = to_epoch(window_start)
    with db.session() as conn:
        _require_group_window(conn, group_id, w_start)
        group = conn.execute(
            "SELECT aggregation FROM device_groups WHERE group_id = ?", (group_id,)
        ).fetchone()
        snap_from = _require_snapshot(conn, group_id, w_start, from_revision)
        snap_to = _require_snapshot(conn, group_id, w_start, to_revision)

        before_states = _snapshot_member_states(conn, group["aggregation"], snap_from["id"])
        after_states = _snapshot_member_states(conn, group["aggregation"], snap_to["id"])

        member_deltas: list[RevisionMemberDeltaOut] = []
        for device_id in sorted(set(before_states) | set(after_states),
                                key=lambda d: after_states.get(d, before_states[d])["position"]):
            b = before_states.get(device_id)
            a = after_states.get(device_id)
            if b is None or a is None or (
                b["count"], b["sum"], b["avg"], b["correction_count"]
            ) != (
                a["count"], a["sum"], a["avg"], a["correction_count"]
            ):
                # 固定成员组两侧成员集合必然相同；任一缺失视为变化（防御性处理）
                b = b or a
                a = a or b
                member_deltas.append(RevisionMemberDeltaOut(
                    device_id=device_id,
                    position=a["position"],
                    before=RevisionMemberStateOut(**b),
                    after=RevisionMemberStateOut(**a),
                    count_delta=a["count"] - b["count"],
                    sum_delta=a["sum"] - b["sum"],
                    avg_delta=(
                        a["avg"] - b["avg"]
                        if a["avg"] is not None and b["avg"] is not None
                        else None
                    ),
                    correction_count_delta=a["correction_count"] - b["correction_count"],
                    contribution_delta=a["contribution"] - b["contribution"],
                ))

        return RevisionDiffOut(
            group_id=group_id,
            window_start=to_iso(w_start),
            aggregation=group["aggregation"],
            from_revision=from_revision,
            to_revision=to_revision,
            from_value=snap_from["value"],
            to_value=snap_to["value"],
            value_delta=snap_to["value"] - snap_from["value"],
            members=member_deltas,
        )
