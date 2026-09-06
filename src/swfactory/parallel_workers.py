"""Seven-lane bounded execution for inner software-factory work.

This is not a scheduler.  An Airflow build task calls :class:`SevenWorkerPool` with one bounded
batch and waits for the deterministic fan-in result.  Each role owns one sequential lane; the
seven role lanes may execute concurrently.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class WorkerRole(StrEnum):
    AUTHORITY = "authority"
    AIRFLOW = "airflow"
    WORKGRAPH = "workgraph"
    RECOVERY = "recovery"
    SECURITY = "security"
    EVIDENCE = "evidence"
    OPERATOR = "operator"


ROLE_ORDER = tuple(WorkerRole)
MAX_WORKERS = len(ROLE_ORDER)


class WorkerCancelled(RuntimeError):
    """The bounded worker batch was cancelled before completion."""


@dataclass(frozen=True)
class WorkerTask:
    task_id: str
    role: WorkerRole
    payload: Mapping[str, Any]

    def validate(self) -> None:
        if not self.task_id.strip() or len(self.task_id) > 256:
            raise ValueError("worker task id must be nonempty and at most 256 characters")


@dataclass(frozen=True)
class WorkerReceipt:
    task_id: str
    role: WorkerRole
    state: str
    started_at: float
    finished_at: float
    result: Any = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WorkerBatch:
    receipts: tuple[WorkerReceipt, ...]
    cancelled: bool

    @property
    def ok(self) -> bool:
        return not self.cancelled and all(row.state == "success" for row in self.receipts)

    def by_role(self) -> dict[str, list[dict[str, Any]]]:
        grouped = {role.value: [] for role in ROLE_ORDER}
        for receipt in self.receipts:
            grouped[receipt.role.value].append(receipt.to_dict())
        return grouped


WorkerHandler = Callable[[WorkerTask, threading.Event], Any]


class SevenWorkerPool:
    """Execute one bounded fan-out/fan-in batch with exactly seven named role lanes."""

    def __init__(self, handlers: Mapping[WorkerRole, WorkerHandler]) -> None:
        missing = set(ROLE_ORDER) - set(handlers)
        extra = set(handlers) - set(ROLE_ORDER)
        if missing:
            raise ValueError(f"missing worker handlers: {sorted(role.value for role in missing)}")
        if extra:
            raise ValueError(f"unknown worker handlers: {sorted(str(role) for role in extra)}")
        self.handlers = dict(handlers)

    def execute(
        self,
        tasks: Iterable[WorkerTask],
        *,
        cancel: threading.Event | None = None,
        stop_on_failure: bool = False,
    ) -> WorkerBatch:
        cancellation = cancel or threading.Event()
        lanes: dict[WorkerRole, list[WorkerTask]] = {role: [] for role in ROLE_ORDER}
        seen: set[str] = set()
        for task in tasks:
            task.validate()
            if task.task_id in seen:
                raise ValueError(f"duplicate worker task id: {task.task_id}")
            seen.add(task.task_id)
            lanes[task.role].append(task)
        for lane in lanes.values():
            lane.sort(key=lambda item: item.task_id)

        futures: dict[Future[list[WorkerReceipt]], WorkerRole] = {}
        receipts: list[WorkerReceipt] = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="swf-worker") as pool:
            for role in ROLE_ORDER:
                if not lanes[role]:
                    continue
                futures[pool.submit(self._run_lane, role, lanes[role], cancellation)] = role
            for future in as_completed(futures):
                lane_receipts = future.result()
                receipts.extend(lane_receipts)
                if stop_on_failure and any(row.state == "failed" for row in lane_receipts):
                    cancellation.set()

        role_index = {role: index for index, role in enumerate(ROLE_ORDER)}
        receipts.sort(key=lambda row: (role_index[row.role], row.task_id))
        return WorkerBatch(tuple(receipts), cancellation.is_set())

    def _run_lane(
        self,
        role: WorkerRole,
        tasks: list[WorkerTask],
        cancellation: threading.Event,
    ) -> list[WorkerReceipt]:
        handler = self.handlers[role]
        receipts: list[WorkerReceipt] = []
        for task in tasks:
            if cancellation.is_set():
                now = time.time()
                receipts.append(
                    WorkerReceipt(
                        task.task_id,
                        role,
                        "cancelled",
                        now,
                        now,
                        error="batch_cancelled",
                    )
                )
                continue
            started = time.time()
            try:
                result = handler(task, cancellation)
            except WorkerCancelled as error:
                cancellation.set()
                receipts.append(
                    WorkerReceipt(
                        task.task_id,
                        role,
                        "cancelled",
                        started,
                        time.time(),
                        error=str(error) or "worker_cancelled",
                    )
                )
            except Exception as error:  # noqa: BLE001 - failures are data at the fan-in boundary.
                receipts.append(
                    WorkerReceipt(
                        task.task_id,
                        role,
                        "failed",
                        started,
                        time.time(),
                        error=f"{type(error).__name__}:{error}",
                    )
                )
            else:
                receipts.append(
                    WorkerReceipt(
                        task.task_id,
                        role,
                        "success",
                        started,
                        time.time(),
                        result=result,
                    )
                )
        return receipts


def role_manifest() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "max_workers": MAX_WORKERS,
        "scheduler": "airflow",
        "roles": [role.value for role in ROLE_ORDER],
        "semantics": "bounded_inner_fanout_fanin",
    }
