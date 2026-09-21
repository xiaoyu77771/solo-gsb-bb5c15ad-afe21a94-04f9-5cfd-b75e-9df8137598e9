"""校准时间线服务的端到端测试。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client() -> TestClient:
    c = TestClient(app)
    c.post("/admin/reset")
    yield c
    c.post("/admin/reset")


def put(eid, parent, start, end, value, batch="B1", sensor="S1"):
    return {
        "event_id": eid,
        "batch_id": batch,
        "sensor_id": sensor,
        "parent_event_id": parent,
        "op": "put",
        "start": start,
        "end": end,
        "value": value,
    }


def retract(eid, parent, target, batch="B1", sensor="S1"):
    return {
        "event_id": eid,
        "batch_id": batch,
        "sensor_id": sensor,
        "parent_event_id": parent,
        "op": "retract",
        "target_event_id": target,
    }


def submit(client, *events, batch=None, sensor=None):
    batch = batch if batch is not None else events[0]["batch_id"]
    sensor = sensor if sensor is not None else events[0]["sensor_id"]
    return client.post(
        f"/v1/batches/{batch}/sensors/{sensor}/events",
        json={"events": list(events)},
    )


# ---------------------------------------------------------------- 基础时间线


def test_overlapping_fragments_later_version_wins(client):
    r = submit(
        client,
        put("e1", None, 0, 10, "A"),
        put("e2", "e1", 5, 15, "B"),
    )
    assert r.status_code == 200
    tl = r.json()["timeline"]
    assert [(s["start"], s["end"], s["value"], s["event_id"]) for s in tl] == [
        (0, 5, "A", "e1"),
        (5, 15, "B", "e2"),
    ]
    assert r.json()["gaps"] == []
    assert r.json()["connected_events"] == 2
    assert r.json()["detached_events"] == 0


def test_narrow_overriding_fragment_splits_timeline(client):
    submit(client, put("e1", None, 0, 100, "A"))
    r = submit(client, put("e2", "e1", 40, 60, "B"))
    assert [(s["start"], s["end"], s["value"]) for s in r.json()["timeline"]] == [
        (0, 40, "A"),
        (40, 60, "B"),
        (60, 100, "A"),
    ]


def test_deterministic_regardless_of_arrival_order(client):
    # 先传子版本（断链），再补根
    r1 = submit(client, put("e2", "e1", 5, 15, "B"))
    assert r1.status_code == 200
    body = r1.json()
    assert body["timeline"] == []
    assert body["detached_events"] == 1
    assert body["gaps"] == [
        {
            "type": "missing_parent",
            "required_event_id": "e1",
            "blocked_by_event_id": "e2",
        }
    ]

    r2 = submit(client, put("e1", None, 0, 10, "A"))
    tl = r2.json()["timeline"]
    assert [(s["start"], s["end"], s["value"], s["event_id"]) for s in tl] == [
        (0, 5, "A", "e1"),
        (5, 15, "B", "e2"),
    ]
    assert r2.json()["gaps"] == []


def test_gap_backfill_is_idempotent_and_chain_extends(client):
    # 根、孙一起上传，缺 e2
    submit(client, put("e1", None, 0, 10, "A"))
    r = submit(client, put("e3", "e2", 0, 20, "C"))
    assert r.json()["gaps"] == [
        {
            "type": "missing_parent",
            "required_event_id": "e2",
            "blocked_by_event_id": "e3",
        }
    ]
    # 此时只有 e1 生效
    assert [(s["start"], s["end"]) for s in r.json()["timeline"]] == [(0, 10)]

    submit(client, put("e2", "e1", 5, 15, "B"))
    r = client.get("/v1/batches/B1/sensors/S1/timeline")
    assert r.json()["gaps"] == []
    # e3 [0,20] 深度最大，在整个区间上胜出
    assert [(s["start"], s["end"], s["value"]) for s in r.json()["timeline"]] == [
        (0, 20, "C"),
    ]


# ---------------------------------------------------------------- 撤回


def test_retract_removes_fragment_and_exposes_older_version(client):
    submit(
        client,
        put("e1", None, 0, 10, "A"),
        put("e2", "e1", 0, 10, "B"),
    )
    r = submit(client, retract("e3", "e2", "e2"))
    # e2 撤回自己后整段恢复为 e1
    assert [(s["start"], s["end"], s["value"]) for s in r.json()["timeline"]] == [
        (0, 10, "A")
    ]


def test_retract_middle_fragment_rejoins_same_source_segments(client):
    submit(
        client,
        put("e1", None, 0, 100, "A"),
        put("e2", "e1", 40, 60, "B"),
    )
    r = submit(client, retract("e3", "e2", "e2"))
    # 中间的 B 被撤回，A 的两段相邻且同源，合并为一段
    assert [(s["start"], s["end"], s["value"], s["event_id"]) for s in r.json()["timeline"]] == [
        (0, 100, "A", "e1"),
    ]


def test_retract_before_target_arrives_reports_gap_then_resolves(client):
    # 撤回先到，被撤回的 put（既是父版本又是撤回目标）尚未上传
    r = submit(client, retract("r1", "p9", "p9"))
    assert r.json()["gaps"] == [
        {
            "type": "missing_parent",
            "required_event_id": "p9",
            "blocked_by_event_id": "r1",
        }
    ]
    # 目标 put 补传后撤回立即生效：时间线为空
    r2 = submit(client, put("p9", None, 0, 10, "A"))
    assert r2.json()["timeline"] == []
    assert r2.json()["gaps"] == []


def test_connected_retract_of_completely_unknown_target_reports_gap(client):
    submit(client, put("p1", None, 0, 10, "A"))
    r = submit(client, retract("r1", "p1", "q5"))
    assert r.json()["gaps"] == [
        {
            "type": "missing_target",
            "required_event_id": "q5",
            "blocked_by_event_id": "r1",
        }
    ]
    # 缺口期间撤回不生效，p1 仍在时间线上
    assert [(s["start"], s["end"], s["value"]) for s in r.json()["timeline"]] == [
        (0, 10, "A")
    ]
    # q5 挂接到链尾并被撤回
    r2 = submit(client, put("q5", "r1", 0, 10, "Q"))
    assert r2.json()["gaps"] == []
    assert all(s["event_id"] != "q5" for s in r2.json()["timeline"])


def test_retract_unknown_target_detached_target_via_missing_parent(client):
    # 目标 p3 已上传但断链；r1 挂在 p3 之后，因此一并断链。
    # 只报 missing_parent（最近缺失祖先 mx），不重复报 missing_target。
    submit(client, put("p1", None, 0, 10, "A"))
    submit(client, put("p2", "p1", 0, 10, "B"))
    submit(client, put("p3", "mx", 0, 10, "C"))
    r = submit(client, retract("r1", "p3", "p3"))
    assert r.json()["gaps"] == [
        {
            "type": "missing_parent",
            "required_event_id": "mx",
            "blocked_by_event_id": "p3",
        }
    ]
    # 补全 mx：链 p2 -> mx -> p3 -> r1 连通，p3 随即被撤回
    submit(client, put("mx", "p2", 5, 6, "M"))
    r = client.get("/v1/batches/B1/sensors/S1/timeline")
    assert all(s["value"] not in ("C",) for s in r.json()["timeline"])
    assert r.json()["gaps"] == []
    assert r.json()["connected_events"] == 5


# ---------------------------------------------------------------- 幂等与冲突


def test_duplicate_events_are_idempotent(client):
    payload = [put("e1", None, 0, 10, "A"), put("e2", "e1", 5, 15, "B")]
    submit(client, *payload)
    r = submit(client, *payload)
    body = r.json()
    assert body["accepted"] == 0
    assert body["duplicate"] == 2
    assert body["total_events"] == 2


def test_same_event_id_different_payload_conflicts(client):
    submit(client, put("e1", None, 0, 10, "A"))
    r = submit(client, put("e1", None, 0, 10, "OTHER"))
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "event_conflict"
    # 冲突不影响原状态
    r = client.get("/v1/batches/B1/sensors/S1/timeline")
    assert r.json()["timeline"][0]["value"] == "A"


def test_batch_submit_is_atomic_on_validation_error(client):
    submit(client, put("e1", None, 0, 10, "A"))
    # e2 合法、e3 非法（分叉：e2、e3 同为 e1 的后继）
    r = submit(
        client,
        put("e2", "e1", 0, 5, "B"),
        put("e3", "e1", 5, 10, "C"),
    )
    assert r.status_code == 422
    r = client.get("/v1/batches/B1/sensors/S1/timeline")
    assert r.json()["total_events"] == 1


def test_multiple_roots_rejected(client):
    r = submit(
        client,
        put("e1", None, 0, 10, "A"),
        put("e2", None, 0, 10, "B"),
    )
    assert r.status_code == 422


def test_cross_scope_parent_rejected(client):
    submit(client, put("x1", None, 0, 10, "X", batch="B1", sensor="SX"))
    r = submit(client, put("e1", "x1", 0, 10, "A", batch="B2", sensor="S1"))
    assert r.status_code == 422


def test_scope_mismatch_with_path_rejected(client):
    r = client.post(
        "/v1/batches/OTHER/sensors/S1/events",
        json={"events": [put("e1", None, 0, 10, "A", batch="B1", sensor="S1")]},
    )
    assert r.status_code == 422


def test_invalid_interval_rejected(client):
    r = submit(client, put("e1", None, 10, 10, "A"))
    assert r.status_code == 422
    r = submit(client, put("e1", None, 10, 5, "A"))
    assert r.status_code == 422


def test_retract_must_target_put(client):
    submit(client, put("e1", None, 0, 10, "A"))
    submit(client, retract("r1", "e1", "e1"))
    r = submit(client, retract("r2", "r1", "r1"))
    assert r.status_code == 422


def test_retract_with_interval_fields_rejected(client):
    e = put("e1", None, 0, 10, "A")
    bad = retract("r1", "e1", "e1")
    bad["start"] = 0
    r = submit(client, e, bad)
    assert r.status_code == 422


# ---------------------------------------------------------------- 查询与运维


def test_batch_status_contains_all_sensors(client):
    submit(client, put("a1", None, 0, 10, "A", sensor="S1"))
    submit(client, put("b1", None, 0, 5, "X", sensor="S2"))
    r = client.get("/v1/batches/B1")
    body = r.json()
    assert body["sensor_count"] == 2
    assert [s["sensor_id"] for s in body["sensors"]] == ["S1", "S2"]


def test_batch_isolation(client):
    submit(client, put("a1", None, 0, 10, "A"), batch="B1", sensor="S1")
    submit(client, put("b1", None, 0, 10, "Z"), batch="B2", sensor="S1")
    r = client.get("/v1/batches/B1/sensors/S1/timeline")
    assert r.json()["timeline"][0]["value"] == "A"
    assert client.get("/v1/batches/NOPE").status_code == 404


def test_reset_all_and_reset_one_batch(client):
    submit(client, put("a1", None, 0, 10, "A", batch="B1"))
    submit(client, put("b1", None, 0, 10, "Z", batch="B2"))
    r = client.post("/admin/reset?batch_id=B1")
    assert r.json()["dropped_events"] == 1
    assert client.get("/v1/batches/B1/sensors/S1/timeline").status_code == 404
    assert client.get("/v1/batches/B2/sensors/S1/timeline").status_code == 200

    r = client.post("/admin/reset")
    assert r.json()["dropped_events"] == 1
    assert r.json()["remaining_batches"] == 0


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}
