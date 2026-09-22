"""设备组窗口修订审计（不可变快照）测试。"""

import sqlite3

import pytest

from app import db

W0 = "2026-09-22T00:00:00Z"


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def seal_window(client, device_id, at="2026-09-22T00:02:00Z", value=99.0, event_id=None):
    """推进水位线到 00:02:00，封存窗口 [00:00, 00:01)（默认 lateness=30s）。"""
    event_id = event_id or f"seal-{device_id}"
    return ingest(client, device_id, [
        {"event_id": event_id, "event_time": at, "value": value},
    ])


def make_ready_sum_group(client, members=("d1", "d2"), initial=(10.0, 20.0)):
    """构造一个 00:00 窗口已 ready 的 sum 组（revision=1）。"""
    ingest(client, members[0], [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z",
                                 "value": initial[0]}])
    ingest(client, members[1], [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z",
                                 "value": initial[1]}])
    r = client.post("/groups", json={
        "group_id": "g1",
        "member_ids": list(members),
        "aggregation": "sum",
        "window_size_seconds": 60,
        "allowed_lateness_seconds": 30,
    })
    assert r.status_code == 201
    seal_window(client, members[0], event_id="s1")
    seal_window(client, members[1], event_id="s2")


# ---------- 快照写入 ----------

def test_first_ready_appends_revision_1_snapshot(client):
    make_ready_sum_group(client)

    revs = client.get(f"/groups/g1/windows/{W0}/revisions").json()
    assert [r["revision"] for r in revs] == [1]
    r1 = revs[0]
    assert r1["value"] == 30.0
    assert r1["aggregation"] == "sum"
    assert r1["member_count"] == 2
    assert r1["ready_at"] is not None
    assert r1["snapshot_created_at"] is not None

    detail = client.get(f"/groups/g1/windows/{W0}/revisions/1").json()
    members = detail["members"]
    # 按组成员顺序留档每个固定成员当时的 count/sum/avg/correction_count
    assert [m["device_id"] for m in members] == ["d1", "d2"]
    assert members[0] == {
        "position": 0, "device_id": "d1",
        "count": 1, "sum": 10.0, "avg": 10.0, "correction_count": 0,
    }
    assert members[1] == {
        "position": 1, "device_id": "d2",
        "count": 1, "sum": 20.0, "avg": 20.0, "correction_count": 0,
    }


def test_late_correction_appends_new_snapshot_but_keeps_old(client):
    make_ready_sum_group(client)

    ingest(client, "d1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ])
    revs = client.get(f"/groups/g1/windows/{W0}/revisions").json()
    assert [r["revision"] for r in revs] == [1, 2]
    assert revs[0]["value"] == 30.0  # 旧快照值不被覆盖
    assert revs[1]["value"] == 35.0

    # revision=1 的成员明细保持原样
    m1 = client.get(f"/groups/g1/windows/{W0}/revisions/1").json()["members"]
    assert m1[0]["sum"] == 10.0
    assert m1[0]["correction_count"] == 0
    # revision=2 记录修正后的状态
    m2 = client.get(f"/groups/g1/windows/{W0}/revisions/2").json()["members"]
    assert m2[0]["sum"] == 15.0
    assert m2[0]["count"] == 2
    assert m2[0]["avg"] == 7.5
    assert m2[0]["correction_count"] == 1
    # 未发生修正的成员数值不变
    assert m2[1] == {
        "position": 1, "device_id": "d2",
        "count": 1, "sum": 20.0, "avg": 20.0, "correction_count": 0,
    }

    # 再一个迟到事件 → revision 3
    ingest(client, "d1", [
        {"event_id": "late-2", "event_time": "2026-09-22T00:00:40Z", "value": 1.0},
    ])
    revs = client.get(f"/groups/g1/windows/{W0}/revisions").json()
    assert [r["revision"] for r in revs] == [1, 2, 3]
    assert [r["value"] for r in revs] == [30.0, 35.0, 36.0]


def test_duplicate_report_does_not_duplicate_snapshot(client):
    make_ready_sum_group(client)
    late = [{"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0}]
    ingest(client, "d1", late)

    # 同一 event_id 重试：幂等去重，不产生新修正，也不留新快照
    ingest(client, "d1", late)
    revs = client.get(f"/groups/g1/windows/{W0}/revisions").json()
    assert [r["revision"] for r in revs] == [1, 2]

    # 数据库层面每个 revision 只有一份快照
    conn = sqlite3.connect(db.db_path())
    n = conn.execute(
        "SELECT COUNT(*) FROM group_window_snapshots WHERE group_id = 'g1'"
    ).fetchone()[0]
    n_members = conn.execute(
        "SELECT COUNT(*) FROM group_window_snapshot_members"
    ).fetchone()[0]
    conn.close()
    assert n == 2
    assert n_members == 4  # 2 个快照 × 2 个成员


def test_pending_and_missing_window_revision_404(client):
    ingest(client, "d1", [{"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0}])
    ingest(client, "d2", [{"event_id": "e1", "event_time": "2026-09-22T00:00:20Z", "value": 2.0}])
    client.post("/groups", json={
        "group_id": "g1", "member_ids": ["d1", "d2"], "aggregation": "sum",
        "window_size_seconds": 60, "allowed_lateness_seconds": 30,
    })

    # pending 窗口还没有任何快照 → 404
    assert client.get(f"/groups/g1/windows/{W0}/revisions").status_code == 404
    assert client.get(f"/groups/g1/windows/{W0}/revisions/1").status_code == 404

    # 组 / 窗口不存在
    assert client.get(f"/groups/ghost/windows/{W0}/revisions").status_code == 404
    assert client.get("/groups/g1/windows/2030-01-01T00:00:00Z/revisions").status_code == 404


def test_revision_detail_404_for_unknown_revision(client):
    make_ready_sum_group(client)
    assert client.get(f"/groups/g1/windows/{W0}/revisions/2").status_code == 404
    assert client.get(f"/groups/g1/windows/{W0}/revisions/999").status_code == 404


def test_revision_detail_422_for_invalid_revision(client):
    make_ready_sum_group(client)
    # revision=0、负数均为非法版本
    assert client.get(f"/groups/g1/windows/{W0}/revisions/0").status_code == 422
    assert client.get(f"/groups/g1/windows/{W0}/revisions/-3").status_code == 422


# ---------- diff ----------

def test_diff_returns_only_changed_members_in_order(client):
    make_ready_sum_group(client)
    # d1 两次迟到修正，d2 不动
    ingest(client, "d1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ])
    ingest(client, "d1", [
        {"event_id": "late-2", "event_time": "2026-09-22T00:00:40Z", "value": 1.0},
    ])

    d = client.get(f"/groups/g1/windows/{W0}/revisions/1/diff/3").json()
    assert d["from_revision"] == 1
    assert d["to_revision"] == 3
    assert d["from_value"] == 30.0
    assert d["to_value"] == 36.0
    assert d["value_delta"] == 6.0
    # 仅 d1 变化，且按成员顺序（d1 position=0）
    assert len(d["members"]) == 1
    md = d["members"][0]
    assert md["device_id"] == "d1"
    assert md["position"] == 0
    assert md["before_contribution"] == 10.0
    assert md["after_contribution"] == 16.0
    assert md["contribution_delta"] == 6.0
    assert (md["before_count"], md["after_count"], md["count_delta"]) == (1, 3, 2)
    assert (md["before_sum"], md["after_sum"], md["sum_delta"]) == (10.0, 16.0, 6.0)
    assert (md["before_avg"], md["after_avg"]) == (10.0, pytest.approx(16.0 / 3))
    assert (md["before_correction_count"], md["after_correction_count"],
            md["correction_count_delta"]) == (0, 2, 2)


def test_diff_adjacent_revisions(client):
    make_ready_sum_group(client)
    ingest(client, "d1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ])
    d = client.get(f"/groups/g1/windows/{W0}/revisions/1/diff/2").json()
    assert d["value_delta"] == 5.0
    assert len(d["members"]) == 1
    assert d["members"][0]["contribution_delta"] == 5.0


def test_diff_no_material_change_between_same_state(client):
    # 两个成员各自迟到修正一次（分两次升版），1→3 之间两个成员都发生过变化，
    # 各自只出现一次；而 1→2 时 d2 不变，不出现在 members 里。
    make_ready_sum_group(client)
    ingest(client, "d1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ])
    ingest(client, "d2", [
        {"event_id": "late-2", "event_time": "2026-09-22T00:00:31Z", "value": 7.0},
    ])
    d13 = client.get(f"/groups/g1/windows/{W0}/revisions/1/diff/3").json()
    assert [m["device_id"] for m in d13["members"]] == ["d1", "d2"]  # 成员顺序
    assert d13["members"][1]["contribution_delta"] == 7.0
    assert d13["value_delta"] == 12.0


def test_diff_422_for_invalid_or_bad_ordered_versions(client):
    make_ready_sum_group(client)
    base = f"/groups/g1/windows/{W0}/revisions"
    # 非正版本号
    assert client.get(f"{base}/0/diff/2").status_code == 422
    assert client.get(f"{base}/1/diff/0").status_code == 422
    assert client.get(f"{base}/-1/diff/2").status_code == 422
    # from >= to
    assert client.get(f"{base}/2/diff/2").status_code == 422
    assert client.get(f"{base}/2/diff/1").status_code == 422


def test_diff_404_for_missing_version(client):
    make_ready_sum_group(client)
    base = f"/groups/g1/windows/{W0}/revisions"
    assert client.get(f"{base}/1/diff/5").status_code == 404
    assert client.get(f"{base}/5/diff/6").status_code == 404
    assert client.get("/groups/ghost/windows/2030-01-01T00:00:00Z/revisions/1/diff/2").status_code == 404


# ---------- avg 组贡献口径 ----------

def test_avg_snapshot_diff_uses_member_avg_contribution(client):
    # d1：avg 15；d2：avg 40
    ingest(client, "d1", [
        {"event_id": "a", "event_time": "2026-09-22T00:00:10Z", "value": 10.0},
        {"event_id": "b", "event_time": "2026-09-22T00:00:20Z", "value": 20.0},
    ])
    ingest(client, "d2", [
        {"event_id": "c", "event_time": "2026-09-22T00:00:20Z", "value": 40.0},
    ])
    client.post("/groups", json={
        "group_id": "g1", "member_ids": ["d1", "d2"], "aggregation": "avg",
        "window_size_seconds": 60, "allowed_lateness_seconds": 30,
    })
    seal_window(client, "d1", event_id="s1")
    seal_window(client, "d2", event_id="s2")

    r1 = client.get(f"/groups/g1/windows/{W0}/revisions/1").json()
    assert r1["value"] == pytest.approx((15.0 + 40.0) / 2)
    assert r1["members"][0]["avg"] == 15.0

    # d1 迟到一个 30 的事件：d1 avg = 60/3 = 20
    ingest(client, "d1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 30.0},
    ])
    d = client.get(f"/groups/g1/windows/{W0}/revisions/1/diff/2").json()
    assert d["aggregation"] == "avg"
    md = d["members"][0]
    assert md["before_contribution"] == 15.0
    assert md["after_contribution"] == 20.0
    assert md["contribution_delta"] == 5.0
    # 组 value_delta = 成员 avg 差 / 成员数
    assert d["value_delta"] == pytest.approx(5.0 / 2)
    assert d["to_value"] == pytest.approx((20.0 + 40.0) / 2)


# ---------- 快照不可变 ----------

def test_snapshots_are_immutable_at_database_level(client):
    make_ready_sum_group(client)
    conn = sqlite3.connect(db.db_path())
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "UPDATE group_window_snapshots SET value = 999 WHERE revision = 1"
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM group_window_snapshots WHERE revision = 1")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "UPDATE group_window_snapshot_members SET sum = 999 WHERE device_id = 'd1'"
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM group_window_snapshot_members")
    conn.close()

    # 后续修正产生新版本，旧快照数值依旧
    ingest(client, "d1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ])
    r1 = client.get(f"/groups/g1/windows/{W0}/revisions/1").json()
    assert r1["value"] == 30.0
    assert r1["members"][0]["sum"] == 10.0


# ---------- 旧库 baseline 补档 ----------

LEGACY_SCHEMA = """
CREATE TABLE devices (
    device_id TEXT PRIMARY KEY, window_size_seconds INTEGER NOT NULL DEFAULT 60,
    allowed_lateness_seconds INTEGER NOT NULL DEFAULT 30,
    watermark REAL, created_at TEXT NOT NULL
);
CREATE TABLE events (
    device_id TEXT NOT NULL, event_id TEXT NOT NULL, event_time REAL NOT NULL,
    value REAL NOT NULL, status TEXT NOT NULL, ingested_at TEXT NOT NULL,
    PRIMARY KEY (device_id, event_id)
);
CREATE TABLE windows (
    device_id TEXT NOT NULL, window_start REAL NOT NULL, window_end REAL NOT NULL,
    count INTEGER NOT NULL DEFAULT 0, sum REAL NOT NULL DEFAULT 0, min REAL, max REAL,
    status TEXT NOT NULL DEFAULT 'open', correction_count INTEGER NOT NULL DEFAULT 0,
    late_event_count INTEGER NOT NULL DEFAULT 0, sealed_at TEXT,
    PRIMARY KEY (device_id, window_start)
);
CREATE TABLE corrections (
    id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT NOT NULL, event_id TEXT NOT NULL,
    window_start REAL NOT NULL, event_time REAL NOT NULL, value REAL NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE device_groups (
    group_id TEXT PRIMARY KEY, window_size_seconds INTEGER NOT NULL,
    allowed_lateness_seconds INTEGER NOT NULL, aggregation TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE device_group_members (
    group_id TEXT NOT NULL, device_id TEXT NOT NULL, position INTEGER NOT NULL,
    PRIMARY KEY (group_id, device_id)
);
CREATE TABLE group_windows (
    group_id TEXT NOT NULL, window_start REAL NOT NULL, window_end REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', value REAL, member_count INTEGER NOT NULL,
    sealed_member_count INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 0,
    correction_sig INTEGER NOT NULL DEFAULT 0, ready_at TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (group_id, window_start)
);
"""


def test_startup_backfills_one_baseline_for_existing_ready_window(tmp_path, monkeypatch):
    db_file = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(LEGACY_SCHEMA)
    ws0 = 1790035200.0  # 2026-09-22T00:00:00Z
    # 构造一个已经经历过两次升版（revision=3）的旧库 ready 窗口：只补当前版本，不伪造历史
    conn.execute(
        "INSERT INTO devices VALUES ('d1', 60, 30, ?, '2026-09-22T00:00:00Z')",
        (ws0 + 120,),
    )
    conn.execute(
        "INSERT INTO devices VALUES ('d2', 60, 30, ?, '2026-09-22T00:00:00Z')",
        (ws0 + 120,),
    )
    conn.execute(
        "INSERT INTO device_groups VALUES ('g1', 60, 30, 'sum', '2026-09-22T00:00:00Z')"
    )
    conn.executemany(
        "INSERT INTO device_group_members VALUES (?, ?, ?)",
        [("g1", "d1", 0), ("g1", "d2", 1)],
    )
    conn.execute(
        "INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,"
        " status, correction_count, late_event_count, sealed_at)"
        " VALUES ('d1', ?, ?, 3, 16.0, 1.0, 10.0, 'sealed', 2, 2, '2026-09-22T00:02:00Z')",
        (ws0, ws0 + 60),
    )
    conn.execute(
        "INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,"
        " status, correction_count, late_event_count, sealed_at)"
        " VALUES ('d2', ?, ?, 1, 20.0, 20.0, 20.0, 'sealed', 0, 0, '2026-09-22T00:02:00Z')",
        (ws0, ws0 + 60),
    )
    conn.execute(
        "INSERT INTO group_windows VALUES ('g1', ?, ?, 'ready', 36.0, 2, 2, 3, 2,"
        " '2026-09-22T00:02:00Z', '2026-09-22T00:02:00Z', '2026-09-22T00:05:00Z')",
        (ws0, ws0 + 60),
    )
    # 一个 pending 窗口：不应补档
    conn.execute(
        "INSERT INTO group_windows VALUES ('g1', ?, ?, 'pending', NULL, 2, 0, 0, 0,"
        " NULL, '2026-09-22T00:02:00Z', '2026-09-22T00:02:00Z')",
        (ws0 + 60, ws0 + 120),
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("TELEMETRY_DB_PATH", str(db_file))
    # 重置连接缓存后触发建表 + baseline 补档
    db.init_db()
    from app.main import backfill_baseline_snapshots
    backfill_baseline_snapshots()

    from fastapi.testclient import TestClient
    from app.main import app
    with TestClient(app) as client:
        revs = client.get(f"/groups/g1/windows/{W0}/revisions").json()
        # 只补当前 revision=3 一份 baseline，不伪造 1/2
        assert [r["revision"] for r in revs] == [3]
        assert revs[0]["value"] == 36.0
        assert revs[0]["ready_at"] == "2026-09-22T00:02:00Z"

        detail = client.get(f"/groups/g1/windows/{W0}/revisions/3").json()
        assert [m["device_id"] for m in detail["members"]] == ["d1", "d2"]
        d1, d2 = detail["members"]
        assert (d1["count"], d1["sum"], d1["avg"], d1["correction_count"]) == (3, 16.0, 16.0 / 3, 2)
        assert (d2["count"], d2["sum"], d2["avg"], d2["correction_count"]) == (1, 20.0, 20.0, 0)

        # baseline 之后新修正正常升版为 revision=4
        ingest(client, "d1", [
            {"event_id": "late-x", "event_time": "2026-09-22T00:00:05Z", "value": 4.0},
        ])
        revs = client.get(f"/groups/g1/windows/{W0}/revisions").json()
        assert [r["revision"] for r in revs] == [3, 4]
        assert revs[1]["value"] == 40.0

        # pending 窗口仍无快照
        assert client.get("/groups/g1/windows/2026-09-22T00:01:00Z/revisions").status_code == 404
