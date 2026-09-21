"""Pydantic 模型：入站修订事件、时间线片段、缺口与响应封装。"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class RevisionEvent(BaseModel):
    """一条测量片段修订（put）或撤回（retract）。

    版本因果关系由 ``parent_event_id`` 表达：事件按 parent 指针形成从根
    （parent 为 null）出发的链；同一 (batch, sensor) 只允许线性链，不允许分叉。
    """

    event_id: str = Field(..., min_length=1, description="全局唯一事件 ID（幂等键）")
    batch_id: str = Field(..., min_length=1)
    sensor_id: str = Field(..., min_length=1)
    parent_event_id: str | None = Field(
        None, description="因果父版本；根事件为 null"
    )
    op: Literal["put", "retract"]
    target_event_id: str | None = Field(
        None, description="retract 必填：要撤回的 put 事件 ID"
    )
    start: float | None = Field(None, description="put 必填：区间起点（含）")
    end: float | None = Field(None, description="put 必填：区间终点（不含）")
    value: Any = Field(None, description="put 必填：标定载荷（任意 JSON 值）")

    @model_validator(mode="after")
    def _check_shape(self) -> "RevisionEvent":
        if self.op == "put":
            missing = [
                name
                for name in ("start", "end", "value")
                if getattr(self, name) is None
            ]
            if missing:
                raise ValueError(f"put 事件必须提供字段: {', '.join(missing)}")
            if self.target_event_id is not None:
                raise ValueError("put 事件不得携带 target_event_id")
            if self.start >= self.end:
                raise ValueError("时间区间必须满足 start < end（半开区间）")
        else:  # retract
            if self.target_event_id is None:
                raise ValueError("retract 事件必须提供 target_event_id")
            illegal = [
                name for name in ("start", "end") if getattr(self, name) is not None
            ]
            if illegal:
                raise ValueError(
                    f"retract 事件不得携带时间字段: {', '.join(illegal)}"
                )
        return self

    model_config = {"extra": "forbid"}


class TimelineSegment(BaseModel):
    """有效标定时间线上的一段。"""

    start: float
    end: float
    value: Any
    event_id: str = Field(..., description="该段当前生效的 put 修订事件")
    depth: int = Field(..., description="因果深度（在线性链中的序号，越大越新）")


class Gap(BaseModel):
    """可补传的版本缺口。"""

    type: Literal["missing_parent", "missing_target"]
    required_event_id: str = Field(..., description="需要补传的事件 ID")
    blocked_by_event_id: str = Field(
        ..., description="因该缺口而无法挂接的已上传事件"
    )


class BatchSummary(BaseModel):
    sensor_id: str
    total_events: int
    connected_events: int
    detached_events: int
    gaps: list[Gap]
    timeline: list[TimelineSegment]


class IngestResult(BaseModel):
    """POST 上传结果（已去重的重复事件也算成功，保证幂等）。"""

    accepted: int = Field(..., description="本次新写入的事件数")
    duplicate: int = Field(..., description="已存在且负载一致的重复事件数")
    total_events: int
    timeline: list[TimelineSegment]
    gaps: list[Gap]
    connected_events: int
    detached_events: int


class BatchStatus(BaseModel):
    batch_id: str
    sensor_count: int
    sensors: list[BatchSummary]


class ResetResult(BaseModel):
    reset: bool
    dropped_events: int
    remaining_batches: int


class ErrorBody(BaseModel):
    error: dict[str, Any]
