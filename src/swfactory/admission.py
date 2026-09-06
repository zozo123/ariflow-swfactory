"""Deterministic admission control and backpressure for factory work orders."""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass, field
from enum import IntEnum


class Priority(IntEnum):
    HOTFIX = 0
    MANUAL = 10
    NORMAL = 20
    BACKGROUND = 30


@dataclass(frozen=True)
class Limits:
    global_active: int = 64
    per_repo_active: int = 8
    per_actor_active: int = 8
    per_blueprint_active: int = 32
    queue_size: int = 4096


@dataclass(order=True)
class QueuedWork:
    sort_key: tuple[int, int] = field(init=False, repr=False)
    priority: Priority
    sequence: int
    work_id: str = field(compare=False)
    repo: str = field(compare=False)
    actor: str = field(compare=False)
    blueprint: str = field(compare=False)
    enqueued_at: float = field(default_factory=time.time, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "sort_key", (int(self.priority), self.sequence))


@dataclass(frozen=True)
class Decision:
    admitted: bool
    reason: str
    queue_position: int | None = None


class AdmissionController:
    def __init__(self, limits: Limits = Limits()):
        self.limits = limits
        self._queue: list[QueuedWork] = []
        self._sequence = 0
        self._active: dict[str, QueuedWork] = {}

    def submit(
        self, work_id: str, repo: str, actor: str, blueprint: str, priority: Priority
    ) -> Decision:
        if work_id in self._active or any(item.work_id == work_id for item in self._queue):
            return Decision(False, "duplicate")
        if self._capacity_for(repo, actor, blueprint):
            item = self._item(work_id, repo, actor, blueprint, priority)
            self._active[work_id] = item
            return Decision(True, "admitted")
        if len(self._queue) >= self.limits.queue_size:
            return Decision(False, "queue_full")
        item = self._item(work_id, repo, actor, blueprint, priority)
        heapq.heappush(self._queue, item)
        return Decision(False, "queued", self._position(work_id))

    def complete(self, work_id: str) -> list[str]:
        self._active.pop(work_id, None)
        return self.drain()

    def drain(self) -> list[str]:
        admitted: list[str] = []
        deferred: list[QueuedWork] = []
        while self._queue and len(self._active) < self.limits.global_active:
            item = heapq.heappop(self._queue)
            if self._capacity_for(item.repo, item.actor, item.blueprint):
                self._active[item.work_id] = item
                admitted.append(item.work_id)
            else:
                deferred.append(item)
        for item in deferred:
            heapq.heappush(self._queue, item)
        return admitted

    def snapshot(self) -> dict:
        now = time.time()
        ordered = sorted(self._queue)
        return {
            "active": [self._row(item, now) for item in self._active.values()],
            "queued": [
                dict(self._row(item, now), position=i + 1) for i, item in enumerate(ordered)
            ],
            "limits": self.limits.__dict__,
        }

    def _item(
        self, work_id: str, repo: str, actor: str, blueprint: str, priority: Priority
    ) -> QueuedWork:
        self._sequence += 1
        return QueuedWork(priority, self._sequence, work_id, repo, actor, blueprint)

    def _capacity_for(self, repo: str, actor: str, blueprint: str) -> bool:
        values = list(self._active.values())
        return (
            len(values) < self.limits.global_active
            and sum(v.repo == repo for v in values) < self.limits.per_repo_active
            and sum(v.actor == actor for v in values) < self.limits.per_actor_active
            and sum(v.blueprint == blueprint for v in values) < self.limits.per_blueprint_active
        )

    def _position(self, work_id: str) -> int:
        return next(i for i, item in enumerate(sorted(self._queue), 1) if item.work_id == work_id)

    @staticmethod
    def _row(item: QueuedWork, now: float) -> dict:
        return {
            "work_id": item.work_id,
            "repo": item.repo,
            "actor": item.actor,
            "blueprint": item.blueprint,
            "priority": item.priority.name.lower(),
            "wait_s": max(0.0, now - item.enqueued_at),
        }
