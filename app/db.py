"""SQLite 数据访问层。

数据库路径通过环境变量 TELEMETRY_DB_PATH 配置，默认 ./telemetry.db。
每次连接时读取环境变量，便于测试隔离与容器挂载持久化。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id                TEXT PRIMARY KEY,
    window_size_seconds      INTEGER NOT NULL DEFAULT 60,
    allowed_lateness_seconds INTEGER NOT NULL DEFAULT 30,
    watermark                REAL,          -- 已见最大 event_time（epoch 秒），持久化水位线
    created_at               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    device_id   TEXT NOT NULL,
    event_id    TEXT NOT NULL,
    event_time  REAL NOT NULL,              -- epoch 秒
    value       REAL NOT NULL,
    status      TEXT NOT NULL,              -- accepted | late
    ingested_at TEXT NOT NULL,
    PRIMARY KEY (device_id, event_id)       -- 幂等去重约束
);

CREATE TABLE IF NOT EXISTS windows (
    device_id         TEXT NOT NULL,
    window_start      REAL NOT NULL,
    window_end        REAL NOT NULL,
    count             INTEGER NOT NULL DEFAULT 0,
    sum               REAL NOT NULL DEFAULT 0,
    min               REAL,
    max               REAL,
    status            TEXT NOT NULL DEFAULT 'open',   -- open | sealed
    correction_count  INTEGER NOT NULL DEFAULT 0,
    late_event_count  INTEGER NOT NULL DEFAULT 0,
    sealed_at         TEXT,
    PRIMARY KEY (device_id, window_start)
);

CREATE TABLE IF NOT EXISTS corrections (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    window_start REAL NOT NULL,
    event_time   REAL NOT NULL,
    value        REAL NOT NULL,
    created_at   TEXT NOT NULL
);

-- 设备组：固定成员，成员设备的窗口配置（大小/允许迟到）必须一致
CREATE TABLE IF NOT EXISTS device_groups (
    group_id                 TEXT PRIMARY KEY,
    window_size_seconds      INTEGER NOT NULL,
    allowed_lateness_seconds INTEGER NOT NULL,
    aggregation              TEXT NOT NULL CHECK (aggregation IN ('sum', 'avg')),
    created_at               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device_group_members (
    group_id  TEXT NOT NULL,
    device_id TEXT NOT NULL,
    position  INTEGER NOT NULL,              -- 保留创建时的成员顺序
    PRIMARY KEY (group_id, device_id)
);

CREATE INDEX IF NOT EXISTS idx_group_members_device
    ON device_group_members (device_id);

-- 组同步窗口汇总：pending（有成员窗口未封存）→ ready（全部封存）
CREATE TABLE IF NOT EXISTS group_windows (
    group_id            TEXT NOT NULL,
    window_start        REAL NOT NULL,
    window_end          REAL NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',  -- pending | ready
    value               REAL,                              -- pending 时为 NULL，禁止部分成员提前计算
    member_count        INTEGER NOT NULL,                 -- 固定成员总数
    sealed_member_count INTEGER NOT NULL DEFAULT 0,
    revision            INTEGER NOT NULL DEFAULT 0,       -- ready 后从 1 起，每次成员修正原位 +1
    correction_sig      INTEGER NOT NULL DEFAULT 0,       -- Σ 成员窗口 correction_count，用于判定是否升版
    ready_at            TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (group_id, window_start)
);
"""


def db_path() -> str:
    return os.environ.get("TELEMETRY_DB_PATH", "telemetry.db")


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def session() -> Iterator[sqlite3.Connection]:
    """事务会话：正常退出提交，异常回滚，始终关闭连接。"""
    conn = connect()
    try:
        with conn:
            yield conn
    finally:
        conn.close()
