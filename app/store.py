"""内存事件存储与确定性时间线引擎。

模型
----
每个 (batch_id, sensor_id) 的修订事件按 ``parent_event_id`` 构成一条从根
（parent 为 null）出发的因果链；链上只允许线性演进，不允许分叉。

- 父版本尚未上传的事件进入 *detached*（断链）状态，其最近缺失祖先汇总为
  ``missing_parent`` 缺口；待父版本补传后自动挂接，时间线随之延展。
- ``retract`` 撤回一个 put：撤回生效后该 put 不再覆盖任何时间点。
- 时间轴按半开区间 [start, end) 切分；重叠处由因果深度（链上序号）更大、
  即更新的修订胜出。结果只取决于事件内容与因果关系，与上传顺序无关。
"""

from __future__ import annotations

import asyncio

from .models import BatchSummary, Gap, RevisionEvent, TimelineSegment


class _ScopeView:
    __slots__ = (
        "order",
        "timeline",
        "gaps",
        "connected_events",
        "detached_events",
        "total_events",
    )

    def __init__(
        self,
        order: list[str],
        timeline: list[TimelineSegment],
        gaps: list[Gap],
        connected: list[str],
        detached: list[str],
    ):
        self.order = order
        self.timeline = timeline
        self.gaps = gaps
        self.connected_events = len(connected)
        self.detached_events = len(detached)
        self.total_events = len(connected) + len(detached)


class MemoryStore:
    def __init__(self) -> None:
        # (batch_id, sensor_id) -> {event_id: RevisionEvent}
        self._scopes: dict[tuple[str, str], dict[str, RevisionEvent]] = {}
        # event_id -> (batch_id, sensor_id)，保证 event_id 全局唯一并便于防跨域引用
        self._index: dict[str, tuple[str, str]] = {}
        self._lock = asyncio.Lock()

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    # ------------------------------------------------------------------ 写入

    async def submit(
        self, events: list[RevisionEvent]
    ) -> tuple[int, int, tuple[str, str] | None]:
        """原子写入一批事件。

        返回 ``(新写入数, 重复数, 作用域)``。任何一条事件不合法则整批拒绝，
        已存在且负载完全一致的事件按幂等重复处理。
        """
        if not events:
            raise ValueError("events 不能为空")

        scope = (events[0].batch_id, events[0].sensor_id)
        for e in events:
            if (e.batch_id, e.sensor_id) != scope:
                raise ValueError("一次提交中的事件必须属于同一个 batch 与 sensor")

        async with self._lock:
            stored = self._scopes.setdefault(scope, {})

            # 1) 请求体内不得有重复 event_id
            seen: set[str] = set()
            for e in events:
                if e.event_id in seen:
                    raise ValueError(f"请求体内 event_id 重复: {e.event_id}")
                seen.add(e.event_id)

            # 2) 与已存事件比对：负载一致 => 幂等重复；否则冲突
            fresh: list[RevisionEvent] = []
            duplicate = 0
            for e in events:
                old = stored.get(e.event_id)
                if old is not None:
                    if old != e:
                        raise ValueError(
                            f"event_id {e.event_id} 已存在且内容不同"
                        )
                    duplicate += 1
                else:
                    fresh.append(e)

            if not fresh:
                return 0, duplicate, scope

            # 合并后的候选集合仅用于结构校验，校验通过才提交
            candidate = dict(stored)
            candidate.update({e.event_id: e for e in fresh})

            self._validate(scope, candidate)

            for e in fresh:
                stored[e.event_id] = e
                self._index[e.event_id] = scope

            return len(fresh), duplicate, scope

    # ------------------------------------------------------------------ 读取

    async def get_view(self, batch_id: str, sensor_id: str) -> _ScopeView | None:
        async with self._lock:
            scope = (batch_id, sensor_id)
            stored = self._scopes.get(scope)
            if not stored:
                return None
            return self._build_view(scope, stored)

    async def batch_status(self, batch_id: str) -> dict[str, _ScopeView] | None:
        async with self._lock:
            result = {
                sensor_id: self._build_view((batch_id, sensor_id), stored)
                for (bid, sensor_id), stored in self._scopes.items()
                if bid == batch_id
            }
            return result if result else None

    async def list_batches(self) -> list[str]:
        async with self._lock:
            return sorted({bid for bid, _ in self._scopes})

    async def reset(self, batch_id: str | None = None) -> tuple[int, int]:
        """清空全部内存状态，或仅清空指定批次。返回 (删除事件数, 剩余批次数)。"""
        async with self._lock:
            if batch_id is None:
                dropped = sum(len(v) for v in self._scopes.values())
                self._scopes.clear()
                self._index.clear()
                return dropped, 0

            victims = [key for key in self._scopes if key[0] == batch_id]
            dropped = 0
            for key in victims:
                for eid in self._scopes[key]:
                    self._index.pop(eid, None)
                dropped += len(self._scopes[key])
                del self._scopes[key]
            return dropped, len({bid for bid, _ in self._scopes})

    # ------------------------------------------------------------------ 校验

    def _validate(
        self, scope: tuple[str, str], events: dict[str, RevisionEvent]
    ) -> None:
        """对合并后的完整作用域做结构校验。"""
        # 1) 单一根事件
        roots = [e.event_id for e in events.values() if e.parent_event_id is None]
        if len(roots) > 1:
            raise ValueError(
                f"同一 (batch, sensor) 只允许一个根事件，发现多个: "
                f"{', '.join(sorted(roots))}"
            )

        # 2) parent：必须存在、同域且不能指向自己
        for e in events.values():
            pid = e.parent_event_id
            if pid is None:
                continue
            if pid == e.event_id:
                raise ValueError(f"事件 {e.event_id} 不能以自身为父版本")
            parent = events.get(pid)
            if parent is None:
                other = self._index.get(pid)
                if other is not None:
                    raise ValueError(
                        f"事件 {e.event_id} 的父版本 {pid} 属于其他 batch/sensor"
                    )
                # 缺失父版本 => detached，合法，运行时报告缺口
                continue
            if (parent.batch_id, parent.sensor_id) != scope:
                raise ValueError(
                    f"事件 {e.event_id} 的父版本 {pid} 属于其他 batch/sensor"
                )

        # 3) 线性链：每个事件至多一个后继（不允许分叉）
        child_count: dict[str, int] = {}
        for e in events.values():
            if e.parent_event_id is not None:
                child_count[e.parent_event_id] = (
                    child_count.get(e.parent_event_id, 0) + 1
                )
        branched = sorted(pid for pid, n in child_count.items() if n > 1)
        if branched:
            raise ValueError(
                f"版本链不允许分叉，事件有多个后继: {', '.join(branched)}"
            )

        # 4) 环检测（parent 指针，缺失节点视为链终止）
        for e in events.values():
            visited = {e.event_id}
            cur = e.parent_event_id
            while cur is not None and cur in events:
                if cur in visited:
                    raise ValueError(f"版本链中检测到环（涉及事件 {cur}）")
                visited.add(cur)
                cur = events[cur].parent_event_id

        # 5) retract：目标必须存在且为 put（未知目标 => 运行时缺口）
        for e in events.values():
            if e.op != "retract":
                continue
            tid = e.target_event_id
            assert tid is not None
            if tid == e.event_id:
                raise ValueError(f"事件 {e.event_id} 不能撤回自身")
            target = events.get(tid)
            if target is None:
                other = self._index.get(tid)
                if other is not None and other != scope:
                    raise ValueError(
                        f"retract {e.event_id} 的目标 {tid} 属于其他 batch/sensor"
                    )
                continue
            if (target.batch_id, target.sensor_id) != scope:
                raise ValueError(
                    f"retract {e.event_id} 的目标 {tid} 属于其他 batch/sensor"
                )
            if target.op != "put":
                raise ValueError(
                    f"retract {e.event_id} 的目标 {tid} 不是 put 事件"
                )

    # ------------------------------------------------------------- 视图构建

    def _build_view(
        self, scope: tuple[str, str], stored: dict[str, RevisionEvent]
    ) -> _ScopeView:
        root = next(
            (e for e in stored.values() if e.parent_event_id is None), None
        )

        # 从根出发按因果链线性化；子节点 id 排序仅为防御性的确定性
        order: list[str] = []
        if root is not None:
            children: dict[str | None, list[str]] = {}
            for e in stored.values():
                children.setdefault(e.parent_event_id, []).append(e.event_id)
            for ids in children.values():
                ids.sort()
            stack: list[str | None] = [root.event_id]
            while stack:
                cur = stack.pop()
                if cur is None or cur not in stored or cur in order:
                    continue
                order.append(cur)
                for child in children.get(cur, []):
                    stack.append(child)

        connected = set(order)
        detached = sorted(eid for eid in stored if eid not in connected)

        gaps = self._collect_gaps(stored, order, connected, detached)
        timeline = self._build_timeline(stored, order)
        return _ScopeView(order, timeline, gaps, sorted(connected), detached)

    @staticmethod
    def _nearest_missing(
        e: RevisionEvent,
        stored: dict[str, RevisionEvent],
        connected: set[str],
    ) -> str | None:
        """沿 parent 向上找最近的、既未连通也不在本作用域的缺失事件 id。"""
        cur = e.parent_event_id
        while cur is not None:
            if cur in connected:
                return None
            if cur not in stored:
                return cur
            cur = stored[cur].parent_event_id
        return None  # 理论上 detached 且无环时不可达

    def _collect_gaps(
        self,
        stored: dict[str, RevisionEvent],
        order: list[str],
        connected: set[str],
        detached: list[str],
    ) -> list[Gap]:
        gap_map: dict[tuple[str, str], Gap] = {}

        # 断链事件 => missing_parent（每个最近缺失祖先只报一次）
        for eid in detached:
            missing = self._nearest_missing(stored[eid], stored, connected)
            if missing is not None:
                gap_map.setdefault(
                    (missing, "p"),
                    Gap(
                        type="missing_parent",
                        required_event_id=missing,
                        blocked_by_event_id=eid,
                    ),
                )

        # 已连通的 retract 若撤回目标完全未知 => missing_target；
        # 目标已上传但断链的情形由 missing_parent 覆盖（父版本补传后自动生效）
        for eid in order:
            e = stored[eid]
            if e.op == "retract":
                tid = e.target_event_id
                assert tid is not None
                if tid not in stored:
                    gap_map.setdefault(
                        (tid, "t"),
                        Gap(
                            type="missing_target",
                            required_event_id=tid,
                            blocked_by_event_id=eid,
                        ),
                    )

        return sorted(
            gap_map.values(),
            key=lambda g: (g.type, g.required_event_id, g.blocked_by_event_id),
        )

    @staticmethod
    def _build_timeline(
        stored: dict[str, RevisionEvent], order: list[str]
    ) -> list[TimelineSegment]:
        depth = {eid: i for i, eid in enumerate(order)}

        # 被已连通 retract 撤回的 put
        retracted: set[str] = set()
        for eid in order:
            e = stored[eid]
            if e.op == "retract":
                tid = e.target_event_id
                assert tid is not None
                if tid in stored and stored[tid].op == "put":
                    retracted.add(tid)

        active = [
            stored[eid]
            for eid in order
            if stored[eid].op == "put" and eid not in retracted
        ]
        if not active:
            return []

        # 扫描线：端点处先移除结束区间再加入开始区间（半开 [start, end)）；
        # 同深度仅一个 put（线性链），深度最大者胜出，天然确定性。
        points: list[tuple[float, int, str]] = []
        for e in active:
            points.append((e.start, 1, e.event_id))
            points.append((e.end, 0, e.event_id))
        points.sort(key=lambda p: (p[0], p[1]))

        segments: list[TimelineSegment] = []
        active_ids: set[str] = set()
        seg_start: float | None = None
        winner: str | None = None

        for t, kind, eid in points:
            if kind == 1:
                prev_winner = winner
                active_ids.add(eid)
                new_winner = max(active_ids, key=lambda x: depth[x])
                if seg_start is not None and prev_winner != new_winner:
                    if t > seg_start:
                        ev = stored[prev_winner] if prev_winner else None
                        segments.append(
                            TimelineSegment(
                                start=seg_start,
                                end=t,
                                value=ev.value,  # type: ignore[union-attr]
                                event_id=prev_winner,  # type: ignore[arg-type]
                                depth=depth[prev_winner],  # type: ignore[arg-type]
                            )
                        )
                        seg_start = t
                if seg_start is None:
                    seg_start = t
                winner = new_winner
            else:
                prev_winner = winner
                active_ids.discard(eid)
                new_winner = max(active_ids, key=lambda x: depth[x]) if active_ids else None
                if seg_start is not None and prev_winner != new_winner:
                    if t > seg_start:
                        ev = stored[prev_winner] if prev_winner else None
                        segments.append(
                            TimelineSegment(
                                start=seg_start,
                                end=t,
                                value=ev.value,  # type: ignore[union-attr]
                                event_id=prev_winner,  # type: ignore[arg-type]
                                depth=depth[prev_winner],  # type: ignore[arg-type]
                            )
                        )
                    seg_start = t if new_winner is not None else None
                winner = new_winner

        # 同一来源的相邻段合并（被中间版本挖开、随后中间版本撤回时会出现）
        merged: list[TimelineSegment] = []
        for seg in segments:
            if (
                merged
                and merged[-1].end == seg.start
                and merged[-1].event_id == seg.event_id
            ):
                merged[-1] = TimelineSegment(
                    start=merged[-1].start,
                    end=seg.end,
                    value=merged[-1].value,
                    event_id=seg.event_id,
                    depth=seg.depth,
                )
            else:
                merged.append(seg)
        return merged
