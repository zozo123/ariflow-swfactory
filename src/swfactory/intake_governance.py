from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from swfactory.models import Issue
from swfactory.scm import FACTORY_BRANCH_PREFIX

if TYPE_CHECKING:
    from swfactory.blueprint import Blueprint

log = logging.getLogger(__name__)


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


# ---------------------------------------------------------------- the scheduled line's backlog

# `[bug] [P1] Select only eligible ...`: the priority every issue of the plan carries in its title.
_PRIORITY = re.compile(r"\[P(\d)\]")
UNPRIORITISED = 9  # sorts after every declared priority, never ahead of one
# `Dependencies: #1219, #2058.` -- the prerequisites line of the plan's issues.
_PREREQUISITES = re.compile(r"^\s*(?:Dependencies|Depends on|Blocked by):\s*(.+)$", re.IGNORECASE | re.MULTILINE)
_REF = re.compile(r"#(\d+)")


def priority_of(title: str) -> int:
    m = _PRIORITY.search(title)
    return int(m.group(1)) if m else UNPRIORITISED


def prerequisites_of(body: str) -> tuple[int, ...]:
    m = _PREREQUISITES.search(body)
    return tuple(int(ref) for ref in _REF.findall(m.group(1))) if m else ()


def issue_revision(issue: Issue) -> str:
    """The digest ``accepted_inputs`` pins as ``issue_sha256``: the selection records the same
    revision admission later checks, so the two are comparable when the issue changed between."""
    from swfactory.accepted_inputs import issue_document

    return digest(issue_document(issue))


class BacklogSource(Protocol):
    """What the selection reads: ``GitHubScm`` in production, a fake in tests."""

    def list_open_issues(self, label: str, *, limit: int) -> Sequence[Issue]: ...
    def list_open_pr_heads(self, *, limit: int) -> Sequence[str]: ...
    def fetch_issue(self, ref: str) -> Issue: ...


def drain_line(
    bp: Blueprint,
    *,
    source: BacklogSource | None = None,
    root: Path | None = None,
    active: Collection[int] = frozenset(),
) -> Selection:
    """Select the next batch of ``bp.trigger.backlog`` and append the decision to the line's record.

    This is the one selection path: ``Blueprint.jobs`` reaches it for a scheduled run with no conf
    (the ``fan_out`` task) and ``swfactory run`` reaches it when no ``--issue`` is named. Reads are
    bounded and their order is fixed -- one listing, one PR scan, one fetch per prerequisite that
    was not listed -- so the same repository state selects the same issue. An SCM failure raises:
    an outage must look like an outage, never like an empty backlog.

    ``active`` is the set of issues an admission authority knows to have a live Cell. The direct
    scheduled path has no such authority and passes none; a backend admitting cron work supplies
    its Cell store here.

    Race policy: selection sees the issue open; if it is closed before the task admits it,
    ``runtime.ctx_for`` refuses the issue (non-retryable, before any sandbox exists). The record
    keeps the revision selected so that refusal can be read against what was chosen.
    """
    spec = bp.trigger.backlog
    if spec is None:
        raise ValueError(f"line {bp.name!r} declares no trigger.backlog")
    if source is None:
        from swfactory.scm import GitHubScm

        (repo,) = {t.repo for t in bp.targets}
        source = GitHubScm(repo, bp.targets[0].base_branch)
    listed = list(source.list_open_issues(spec.label, limit=spec.scan))
    heads = set(source.list_open_pr_heads(limit=spec.scan))
    candidates = [
        BacklogCandidate(
            issue=int(item.id),
            revision=issue_revision(item),
            state=item.state,
            priority=priority_of(item.title),
            prerequisites=prerequisites_of(item.body),
            active=int(item.id) in active,
            implementation_pr_open=any(head.startswith(f"{FACTORY_BRANCH_PREFIX}{item.id}-") for head in heads),
        )
        for item in listed
    ]
    # Every listed issue is open, so a prerequisite is completed only when it is closed; the ones
    # not listed are read one by one, in issue order, so the read sequence is reproducible.
    known = {int(item.id) for item in listed}
    completed = {
        dep
        for dep in sorted({dep for c in candidates for dep in c.prerequisites} - known)
        if source.fetch_issue(str(dep)).state == "closed"
    }
    selection = select_backlog(candidates, completed=completed, limit=spec.batch)
    _record(bp, selection, root)
    return selection


def _record(bp: Blueprint, selection: Selection, root: Path | None) -> Path:
    """Append one line to ``.factory/backlog/<line>.jsonl``: the line's selection history, next to
    the runs it produced (``runtime.job_run_dir`` uses the same root), readable with ``tail``."""
    spec = bp.trigger.backlog
    assert spec is not None
    row = {
        "at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "line": bp.name,
        "label": spec.label,
        "batch": spec.batch,
        "selected": [{"issue": c.issue, "priority": c.priority, "revision": c.revision} for c in selection.selected],
        "skipped": {str(issue): reason for issue, reason in sorted(selection.skipped.items())},
    }
    path = (Path(root) if root is not None else Path()) / ".factory" / "backlog" / f"{bp.name}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    log.info("backlog %s: selected %s, skipped %s", bp.name, [c.issue for c in selection.selected], row["skipped"])
    return path


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
