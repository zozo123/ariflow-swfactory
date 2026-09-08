from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Iterable, Mapping, Sequence


def _hash(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


@dataclass(frozen=True)
class SandboxIdentity:
    provider: str
    cell_id: str
    epoch: int
    attempt_id: str

    def validate(self) -> None:
        if not self.provider:
            raise ValueError("provider is required")
        if not self.cell_id:
            raise ValueError("cell_id is required")
        if self.epoch <= 0:
            raise ValueError("epoch must be positive")
        if not self.attempt_id:
            raise ValueError("attempt_id is required")

    @property
    def stable_name(self) -> str:
        self.validate()
        suffix = _hash(self.__dict__)[:20]
        prefix = "swf-" + "".join(ch for ch in self.cell_id.lower() if ch.isalnum())[:20]
        return f"{prefix}-{suffix}"[:63]

    @property
    def labels(self) -> Mapping[str, str]:
        return {
            "swfactory.owned": "true",
            "swfactory.cell": self.cell_id,
            "swfactory.epoch": str(self.epoch),
            "swfactory.attempt": self.attempt_id,
        }


class CleanupDecision(StrEnum):
    REMOVE = "remove"
    KEEP = "keep"
    OBSERVE = "observe"
    REFUSE = "refuse"


@dataclass(frozen=True)
class ResourceObservation:
    provider_id: str
    labels: Mapping[str, str]
    running: bool
    reachable: bool = True


def authorize_cleanup(
    identity: SandboxIdentity,
    observation: ResourceObservation,
    *,
    current_epoch: int | None,
    active: bool,
) -> CleanupDecision:
    identity.validate()
    if not observation.reachable:
        return CleanupDecision.OBSERVE
    if observation.labels.get("swfactory.owned") != "true":
        return CleanupDecision.REFUSE
    if observation.labels.get("swfactory.cell") != identity.cell_id:
        return CleanupDecision.REFUSE
    if observation.labels.get("swfactory.epoch") != str(identity.epoch):
        return CleanupDecision.REFUSE
    if observation.labels.get("swfactory.attempt") != identity.attempt_id:
        return CleanupDecision.REFUSE
    if current_epoch is None:
        return CleanupDecision.OBSERVE
    if current_epoch != identity.epoch:
        return CleanupDecision.REMOVE if not active else CleanupDecision.REFUSE
    if active:
        return CleanupDecision.KEEP
    return CleanupDecision.REMOVE


@dataclass(frozen=True)
class WorkNodeResult:
    node_id: str
    parallel_safe: bool
    declared_paths: frozenset[str]
    touched_paths: frozenset[str]
    base_head: str
    output_head: str
    cell_id: str
    epoch: int


@dataclass(frozen=True)
class Conflict:
    kind: str
    nodes: tuple[str, ...]
    paths: tuple[str, ...]


def classify_workgraph_conflicts(results: Sequence[WorkNodeResult], *, protected_paths: Iterable[str] = ()) -> tuple[Conflict, ...]:
    conflicts: list[Conflict] = []
    protected = frozenset(protected_paths)
    for result in results:
        unexpected = result.touched_paths - result.declared_paths
        if unexpected:
            conflicts.append(Conflict("undeclared-path", (result.node_id,), tuple(sorted(unexpected))))
        protected_touched = result.touched_paths & protected
        if protected_touched:
            conflicts.append(Conflict("protected-path", (result.node_id,), tuple(sorted(protected_touched))))
        if not result.parallel_safe and len(results) > 1:
            conflicts.append(Conflict("parallel-unsafe", (result.node_id,), ()))
    for idx, left in enumerate(results):
        for right in results[idx + 1 :]:
            overlap = left.touched_paths & right.touched_paths
            if overlap:
                conflicts.append(Conflict("sibling-overlap", (left.node_id, right.node_id), tuple(sorted(overlap))))
            if left.base_head != right.base_head:
                conflicts.append(Conflict("base-divergence", (left.node_id, right.node_id), ()))
            if left.cell_id != right.cell_id or left.epoch != right.epoch:
                conflicts.append(Conflict("authority-divergence", (left.node_id, right.node_id), ()))
    return tuple(conflicts)


def authorize_fan_in(results: Sequence[WorkNodeResult], *, protected_paths: Iterable[str] = ()) -> None:
    if not results:
        raise ValueError("fan-in requires at least one result")
    conflicts = classify_workgraph_conflicts(results, protected_paths=protected_paths)
    if conflicts:
        detail = "; ".join(
            f"{row.kind}:{','.join(row.nodes)}:{','.join(row.paths)}" for row in conflicts
        )
        raise RuntimeError("workgraph fan-in refused: " + detail)


class ReplayStrategy(StrEnum):
    REPAIR = "repair"
    REPLAN = "replan"
    RESTART = "restart"


@dataclass(frozen=True)
class ReplayCandidate:
    strategy: ReplayStrategy
    input_digest: str
    output_digest: str
    correct: bool
    elapsed_ms: int
    cost_usd: float
    evidence_digest: str

    def score(self) -> tuple[int, float, int, str]:
        # Correctness dominates. Among correct candidates, prefer lower cost then lower latency.
        return (0 if self.correct else 1, self.cost_usd, self.elapsed_ms, self.strategy.value)


@dataclass(frozen=True)
class TimeMachineResult:
    frozen_input_digest: str
    candidates: tuple[ReplayCandidate, ...]
    winner: ReplayCandidate


def choose_replay_winner(candidates: Sequence[ReplayCandidate]) -> TimeMachineResult:
    if {candidate.strategy for candidate in candidates} != set(ReplayStrategy):
        raise ValueError("time-machine replay requires repair, replan and restart candidates")
    inputs = {candidate.input_digest for candidate in candidates}
    if len(inputs) != 1:
        raise ValueError("competing futures must share one frozen input")
    for candidate in candidates:
        if candidate.elapsed_ms < 0 or candidate.cost_usd < 0:
            raise ValueError("candidate metrics must be non-negative")
        if len(candidate.evidence_digest) != 64:
            raise ValueError("candidate evidence digest must be sha256")
    winner = min(candidates, key=ReplayCandidate.score)
    return TimeMachineResult(next(iter(inputs)), tuple(candidates), winner)


@dataclass
class CleanupDebt:
    outstanding: dict[str, SandboxIdentity] = field(default_factory=dict)

    def record(self, provider_id: str, identity: SandboxIdentity) -> None:
        identity.validate()
        existing = self.outstanding.get(provider_id)
        if existing is not None and existing != identity:
            raise RuntimeError("provider identity reused across Factory Cells")
        self.outstanding[provider_id] = identity

    def settle(self, provider_id: str, identity: SandboxIdentity) -> None:
        if self.outstanding.get(provider_id) != identity:
            raise RuntimeError("cleanup receipt does not match outstanding debt")
        del self.outstanding[provider_id]
