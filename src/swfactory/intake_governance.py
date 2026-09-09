from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum


class WorkOrderState(StrEnum):
    QUEUED = "queued"
    ADMITTED = "admitted"
    DISPATCHING = "dispatching"
    BOUND = "bound"
    TERMINAL = "terminal"
    CANCELLED = "cancelled"


_ALLOWED: dict[WorkOrderState, frozenset[WorkOrderState]] = {
    WorkOrderState.QUEUED: frozenset({WorkOrderState.ADMITTED, WorkOrderState.CANCELLED}),
    WorkOrderState.ADMITTED: frozenset({WorkOrderState.DISPATCHING, WorkOrderState.CANCELLED}),
    WorkOrderState.DISPATCHING: frozenset({WorkOrderState.BOUND, WorkOrderState.CANCELLED}),
    WorkOrderState.BOUND: frozenset({WorkOrderState.TERMINAL, WorkOrderState.CANCELLED}),
    WorkOrderState.TERMINAL: frozenset(),
    WorkOrderState.CANCELLED: frozenset(),
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


@dataclass(frozen=True)
class AcceptedSnapshot:
    issue_ref: str
    issue_revision: str
    issue_body: str
    blueprint: str
    blueprint_revision: str
    policy: Mapping[str, object]
    target: str
    base_revision: str

    @property
    def digest(self) -> str:
        return digest(
            {
                "issue_ref": self.issue_ref,
                "issue_revision": self.issue_revision,
                "issue_body": self.issue_body,
                "blueprint": self.blueprint,
                "blueprint_revision": self.blueprint_revision,
                "policy": dict(self.policy),
                "target": self.target,
                "base_revision": self.base_revision,
            }
        )


@dataclass(frozen=True)
class CellBinding:
    job_idx: int
    cell_id: str
    epoch: int
    repo: str
    snapshot_digest: str

    def validate(self) -> None:
        if not self.cell_id:
            raise ValueError("cell_id is required")
        if self.epoch <= 0:
            raise ValueError("cell epoch must be positive")
        if not self.repo:
            raise ValueError("repository is required")
        if len(self.snapshot_digest) != 64:
            raise ValueError("snapshot digest must be sha256")


@dataclass(frozen=True)
class ManagedWorkOrder:
    request_id: str
    actor: str
    source: str
    snapshot: AcceptedSnapshot
    bindings: tuple[CellBinding, ...]
    state: WorkOrderState = WorkOrderState.QUEUED

    @property
    def request_digest(self) -> str:
        return digest(
            {
                "request_id": self.request_id,
                "actor": self.actor,
                "source": self.source,
                "snapshot": self.snapshot.digest,
                "bindings": [
                    {
                        "job_idx": row.job_idx,
                        "cell_id": row.cell_id,
                        "epoch": row.epoch,
                        "repo": row.repo,
                        "snapshot_digest": row.snapshot_digest,
                    }
                    for row in self.bindings
                ],
            }
        )

    def transition(self, target: WorkOrderState) -> ManagedWorkOrder:
        if target not in _ALLOWED[self.state]:
            raise ValueError(f"invalid work-order transition {self.state} -> {target}")
        return ManagedWorkOrder(
            request_id=self.request_id,
            actor=self.actor,
            source=self.source,
            snapshot=self.snapshot,
            bindings=self.bindings,
            state=target,
        )

    def validate_bindings(self) -> None:
        if not self.bindings:
            raise ValueError("managed work requires at least one Cell binding")
        seen: set[tuple[str, int]] = set()
        for binding in self.bindings:
            binding.validate()
            if binding.snapshot_digest != self.snapshot.digest:
                raise ValueError("Cell binding snapshot differs from accepted snapshot")
            key = (binding.cell_id, binding.epoch)
            if key in seen:
                raise ValueError("duplicate Cell binding")
            seen.add(key)


@dataclass(frozen=True)
class HumanGate:
    gate: str
    required: bool
    cell_id: str
    epoch: int
    artifact_digest: str


@dataclass(frozen=True)
class GateResponse:
    gate: str
    actor: str
    decision: str
    cell_id: str
    epoch: int
    artifact_digest: str
    recorded_at: datetime


def authorize_gate(policy: HumanGate, response: GateResponse | None, *, allow_automatic: bool = False) -> GateResponse:
    if response is None:
        raise PermissionError("required gate has no response")
    if response.gate != policy.gate:
        raise PermissionError("gate response targets a different gate")
    if response.cell_id != policy.cell_id or response.epoch != policy.epoch:
        raise PermissionError("gate response targets stale Cell authority")
    if response.artifact_digest != policy.artifact_digest:
        raise PermissionError("gate response targets stale artifacts")
    if response.decision != "approve":
        raise PermissionError("gate was not approved")
    actor = response.actor.strip().lower()
    if policy.required and actor in {"", "auto", "system", "bot"} and not allow_automatic:
        raise PermissionError("required human gate cannot be satisfied automatically")
    return response


@dataclass(frozen=True)
class BacklogCandidate:
    issue: int
    revision: str
    state: str
    priority: int
    prerequisites: tuple[int, ...] = ()
    active: bool = False
    implementation_pr_open: bool = False


@dataclass(frozen=True)
class Selection:
    selected: tuple[BacklogCandidate, ...]
    skipped: Mapping[int, str]


def select_backlog(
    candidates: Iterable[BacklogCandidate],
    *,
    completed: set[int],
    limit: int,
) -> Selection:
    if limit <= 0:
        raise ValueError("selection limit must be positive")
    selected: list[BacklogCandidate] = []
    skipped: dict[int, str] = {}
    ordered = sorted(candidates, key=lambda item: (item.priority, item.issue))
    for item in ordered:
        if item.state.lower() != "open":
            skipped[item.issue] = "not-open"
        elif item.active:
            skipped[item.issue] = "active-cell"
        elif item.implementation_pr_open:
            skipped[item.issue] = "implementation-pr-open"
        elif missing := [dep for dep in item.prerequisites if dep not in completed]:
            skipped[item.issue] = "blocked:" + ",".join(str(dep) for dep in missing)
        elif len(selected) >= limit:
            skipped[item.issue] = "batch-limit"
        else:
            selected.append(item)
    return Selection(tuple(selected), skipped)


@dataclass(frozen=True)
class ScheduleLimits:
    origin: datetime
    period: timedelta
    max_active_runs: int
    max_cells: int
    run_timeout: timedelta

    def __post_init__(self) -> None:
        if self.origin.tzinfo is None:
            raise ValueError("schedule origin must be timezone-aware")
        if self.period <= timedelta(0):
            raise ValueError("schedule period must be positive")
        if self.max_active_runs <= 0 or self.max_cells <= 0:
            raise ValueError("schedule limits must be positive")
        if self.run_timeout <= timedelta(0):
            raise ValueError("run timeout must be positive")

    def next_tick(self, now: datetime) -> datetime:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        origin = self.origin.astimezone(UTC)
        current = now.astimezone(UTC)
        if current < origin:
            return self.origin
        elapsed = current - origin
        periods = elapsed // self.period + 1
        return (origin + periods * self.period).astimezone(self.origin.tzinfo)

    def admit_run(self, *, active_runs: int, active_cells: int, requested_cells: int) -> tuple[bool, str]:
        if active_runs >= self.max_active_runs:
            return False, "active-run-limit"
        if requested_cells <= 0:
            return False, "empty-work"
        if active_cells + requested_cells > self.max_cells:
            return False, "cell-capacity"
        return True, "admitted"


def cross_channel_key(line: str, snapshot_digest: str, actor_scope: str = "factory") -> str:
    return "work_" + digest({"line": line, "snapshot": snapshot_digest, "scope": actor_scope})[:32]


def require_complete_bindings(bindings: Sequence[Mapping[str, object]], expected_jobs: int) -> tuple[CellBinding, ...]:
    if len(bindings) != expected_jobs:
        raise ValueError("partial managed Cell bindings are forbidden")
    parsed = tuple(
        CellBinding(
            job_idx=int(row["job_idx"]),
            cell_id=str(row["cell_id"]),
            epoch=int(row["epoch"]),
            repo=str(row["repo"]),
            snapshot_digest=str(row["snapshot_digest"]),
        )
        for row in bindings
    )
    if {item.job_idx for item in parsed} != set(range(expected_jobs)):
        raise ValueError("managed bindings must cover every job exactly once")
    for item in parsed:
        item.validate()
    return parsed
