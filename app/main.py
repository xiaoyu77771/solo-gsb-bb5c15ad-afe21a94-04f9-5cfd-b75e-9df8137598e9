"""FastAPI 应用：校准片段修订上传、时间线/缺口查询、批次查询与状态重置。"""

from __future__ import annotations

import os

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .errors import APIError, api_error_handler
from .models import (
    BatchStatus,
    BatchSummary,
    IngestResult,
    ResetResult,
    RevisionEvent,
)
from .store import MemoryStore, _ScopeView

app = FastAPI(
    title="工厂校准时间线服务",
    version="1.0.0",
    description=(
        "接收同一传感器在校准批次中乱序、重叠且可能撤回的测量片段修订，"
        "按版本因果关系与半开时间区间切分规则生成确定性的有效标定时间线；"
        "缺失父版本时返回可补传的缺口清单；重复事件幂等。"
    ),
)
app.add_exception_handler(APIError, api_error_handler)

store = MemoryStore()


class EventEnvelope(BaseModel):
    events: list[RevisionEvent] = Field(..., min_length=1)


def _summary(sensor_id: str, view: _ScopeView) -> BatchSummary:
    return BatchSummary(
        sensor_id=sensor_id,
        total_events=view.total_events,
        connected_events=view.connected_events,
        detached_events=view.detached_events,
        gaps=view.gaps,
        timeline=view.timeline,
    )


@app.get("/healthz", tags=["ops"])
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/v1/batches/{batch_id}/sensors/{sensor_id}/events",
    response_model=IngestResult,
    tags=["events"],
)
async def submit_events(
    batch_id: str, sensor_id: str, envelope: EventEnvelope
) -> IngestResult:
    """原子上传一个或多个修订事件。

    - 乱序上传允许：父版本缺失时事件断链，缺口在 ``gaps`` 中返回，补传后自动生效。
    - 幂等：相同 ``event_id`` 与完整负载重复上传计为 ``duplicate``，结果不变。
    - 同 ``event_id`` 但负载不同、分叉链、跨 batch/sensor 引用等返回 409/422。
    """
    events = envelope.events
    for e in events:
        if e.batch_id != batch_id or e.sensor_id != sensor_id:
            raise APIError(
                422,
                "scope_mismatch",
                "事件的 batch_id/sensor_id 必须与 URL 路径一致",
            )
    try:
        accepted, duplicate, scope = await store.submit(events)
    except ValueError as exc:
        # 与已存状态冲突（幂等键被不同内容占用）用 409；其余结构问题用 422
        msg = str(exc)
        conflict = "已存在且内容不同" in msg
        raise APIError(
            409 if conflict else 422,
            "event_conflict" if conflict else "invalid_events",
            msg,
        )
    assert scope is not None

    view = await store.get_view(scope[0], scope[1])
    assert view is not None
    return IngestResult(
        accepted=accepted,
        duplicate=duplicate,
        total_events=view.total_events,
        timeline=view.timeline,
        gaps=view.gaps,
        connected_events=view.connected_events,
        detached_events=view.detached_events,
    )


@app.get(
    "/v1/batches/{batch_id}/sensors/{sensor_id}/timeline",
    response_model=BatchSummary,
    tags=["query"],
)
async def sensor_timeline(batch_id: str, sensor_id: str) -> BatchSummary:
    """查询单个传感器的有效标定时间线与缺口清单。"""
    view = await store.get_view(batch_id, sensor_id)
    if view is None:
        raise APIError(404, "not_found", "该 batch/sensor 尚不存在")
    return _summary(sensor_id, view)


@app.get("/v1/batches/{batch_id}", response_model=BatchStatus, tags=["query"])
async def batch_status(batch_id: str) -> BatchStatus:
    """查询整个批次（全部传感器）的校准结果。"""
    views = await store.batch_status(batch_id)
    if not views:
        raise APIError(404, "not_found", "该 batch 尚不存在")
    return BatchStatus(
        batch_id=batch_id,
        sensor_count=len(views),
        sensors=[_summary(sid, views[sid]) for sid in sorted(views)],
    )


@app.get("/v1/batches", tags=["query"])
async def list_batches() -> dict[str, list[str]]:
    return {"batches": await store.list_batches()}


@app.post("/admin/reset", response_model=ResetResult, tags=["ops"])
async def reset_state(batch_id: str | None = None) -> ResetResult:
    """重置内存状态：默认清空全部，带 ``?batch_id=...`` 仅清空指定批次。"""
    dropped, remaining = await store.reset(batch_id)
    return ResetResult(reset=True, dropped_events=dropped, remaining_batches=remaining)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8080")),
    )
