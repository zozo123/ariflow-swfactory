"""Provider-neutral executable population manifests for search-only swarm plans.

A SwarmPlan says what kinds of trajectories should exist. This module turns that abstract
allocation into immutable per-trajectory work descriptions and later summarizes behavior receipts.

The manifest is deliberately not a scheduler and carries no authority material. Airflow remains
the lifecycle substrate; providers may consume these tasks, but they cannot publish, promote,
mint credentials, mutate Cells, or widen policy through this contract.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Any, Literal, Sequence

from swfactory.swarm_dynamics import (
    AgentRole,
    ComputeTier,
    ContextPolicy,
    SwarmPlan,
    effective_independent_search,
    mean_pairwise_correlation,
)

POPULATION_MANIFEST_SCHEMA_VERSION = 1
POPULATION_MANIFEST_AUTHORITY = "search-only"
ReceiptState = Literal["answered", "failed", "cancelled", "refused"]


class PopulationManifestError(ValueError):
    """The provider-neutral population contract is invalid or internally inconsistent."""


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _require_digest(value: str, *, field: str) -> None:
    raw = value.removeprefix("sha256:")
    if len(raw) != 64 or any(char not in "0123456789abcdef" for char in raw):
        raise PopulationManifestError(f"{field} must be a canonical sha256 digest")


@dataclass(frozen=True)
class PopulationTask:
    """One provider-neutral trajectory request materialized from a swarm lane."""

    task_id: str
    lane_index: int
    replica_index: int
    role: AgentRole
    compute_tier: ComputeTier
    context: ContextPolicy
    temperature: float
    independent_verification: bool
    diversity_axes: tuple[str, ...]
    focus_hotspots: tuple[str, ...]
    variant_digest: str

    def validate(self) -> None:
        if not self.task_id.startswith("pop_"):
            raise PopulationManifestError("population task id must use the pop_ namespace")
        if self.lane_index < 0 or self.replica_index < 0:
            raise PopulationManifestError("population task indexes must be non-negative")
        if not math.isfinite(self.temperature) or not 0.0 <= self.temperature <= 2.0:
            raise PopulationManifestError("population task temperature must be finite and in [0, 2]")
        if not self.diversity_axes:
            raise PopulationManifestError("population task must declare at least one diversity axis")
        if self.compute_tier == ComputeTier.EXACT_REPLAY and self.temperature != 0.0:
            raise PopulationManifestError("exact replay population tasks must have zero temperature")
        if self.independent_verification:
            if self.role not in {AgentRole.VERIFIER, AgentRole.RED_TEAM}:
                raise PopulationManifestError("independent verification must use verifier/red-team roles")
            if self.context not in {ContextPolicy.FRESH, ContextPolicy.FROZEN}:
                raise PopulationManifestError("independent verification cannot inherit another trajectory context")
        _require_digest(self.variant_digest, field="variant_digest")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "task_id": self.task_id,
            "lane_index": self.lane_index,
            "replica_index": self.replica_index,
            "role": self.role.value,
            "compute_tier": self.compute_tier.value,
            "context": self.context.value,
            "temperature": self.temperature,
            "independent_verification": self.independent_verification,
            "diversity_axes": list(self.diversity_axes),
            "focus_hotspots": list(self.focus_hotspots),
            "variant_digest": self.variant_digest,
        }


@dataclass(frozen=True)
class PopulationManifest:
    """Immutable search-only work description that any provider adapter can consume."""

    swarm_plan_digest: str
    search_provenance_digest: str
    phase: str
    mode: str
    tasks: tuple[PopulationTask, ...]
    authority: str = POPULATION_MANIFEST_AUTHORITY
    scheduler: str = "airflow"
    schema_version: int = POPULATION_MANIFEST_SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != POPULATION_MANIFEST_SCHEMA_VERSION:
            raise PopulationManifestError("unsupported population manifest schema")
        if self.authority != POPULATION_MANIFEST_AUTHORITY:
            raise PopulationManifestError("population manifests must remain search-only")
        if self.scheduler != "airflow":
            raise PopulationManifestError("population manifest cannot introduce a second scheduler")
        _require_digest(self.swarm_plan_digest, field="swarm_plan_digest")
        _require_digest(self.search_provenance_digest, field="search_provenance_digest")
        if not self.phase.strip() or not self.mode.strip():
            raise PopulationManifestError("population manifest phase and mode must be nonempty")
        ids: set[str] = set()
        variants: set[str] = set()
        for task in self.tasks:
            task.validate()
            if task.task_id in ids:
                raise PopulationManifestError(f"duplicate population task id {task.task_id}")
            if task.variant_digest in variants:
                raise PopulationManifestError(
                    f"duplicate population variant digest {task.variant_digest}; replicas must be distinct questions"
                )
            ids.add(task.task_id)
            variants.add(task.variant_digest)

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "scheduler": self.scheduler,
            "swarm_plan_digest": self.swarm_plan_digest,
            "search_provenance_digest": self.search_provenance_digest,
            "phase": self.phase,
            "mode": self.mode,
            "tasks": [task.canonical_dict() for task in self.tasks],
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())


@dataclass(frozen=True)
class BehaviorReceipt:
    """Gauge-dependent provider result reduced to replayable search telemetry."""

    task_id: str
    state: ReceiptState
    behavior_signature: tuple[str, ...] = ()
    candidate_digest: str | None = None
    evidence_digest: str | None = None
    provider: str | None = None
    model: str | None = None
    runtime: str | None = None
    cost_usd: float = 0.0
    duration_s: float = 0.0

    def validate(self) -> None:
        if not self.task_id.startswith("pop_"):
            raise PopulationManifestError("behavior receipt task id must use the pop_ namespace")
        if self.state == "answered" and not self.behavior_signature:
            raise PopulationManifestError("answered behavior receipts need a nonempty behavior signature")
        for field, digest in (
            ("candidate_digest", self.candidate_digest),
            ("evidence_digest", self.evidence_digest),
        ):
            if digest is not None:
                _require_digest(digest, field=field)
        if not math.isfinite(self.cost_usd) or self.cost_usd < 0.0:
            raise PopulationManifestError("behavior receipt cost must be finite and non-negative")
        if not math.isfinite(self.duration_s) or self.duration_s < 0.0:
            raise PopulationManifestError("behavior receipt duration must be finite and non-negative")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            **asdict(self),
            "behavior_signature": list(self.behavior_signature),
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())


@dataclass(frozen=True)
class PopulationTelemetry:
    """Measured population behavior used to decide whether more trajectories buy information."""

    manifest_digest: str
    total_tasks: int
    receipts: int
    answered: int
    independent_verifier_answers: int
    unique_candidates: int
    effective_independent_search: float
    mean_correlation: float
    candidate_disagreement: float
    total_cost_usd: float
    total_duration_s: float
    receipt_digests: tuple[str, ...]
    authority: str = POPULATION_MANIFEST_AUTHORITY
    schema_version: int = POPULATION_MANIFEST_SCHEMA_VERSION

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "manifest_digest": self.manifest_digest,
            "total_tasks": self.total_tasks,
            "receipts": self.receipts,
            "answered": self.answered,
            "independent_verifier_answers": self.independent_verifier_answers,
            "unique_candidates": self.unique_candidates,
            "effective_independent_search": self.effective_independent_search,
            "mean_correlation": self.mean_correlation,
            "candidate_disagreement": self.candidate_disagreement,
            "total_cost_usd": self.total_cost_usd,
            "total_duration_s": self.total_duration_s,
            "receipt_digests": list(self.receipt_digests),
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())


def build_population_manifest(
    plan: SwarmPlan,
    *,
    search_provenance_digest: str,
) -> PopulationManifest:
    """Deterministically materialize every swarm lane into provider-neutral search tasks."""

    plan_digest = plan.digest()
    _require_digest(search_provenance_digest, field="search_provenance_digest")
    tasks: list[PopulationTask] = []
    for lane_index, lane in enumerate(plan.lanes):
        lane.validate()
        for replica_index in range(lane.count):
            variant_digest = _digest(
                {
                    "swarm_plan_digest": plan_digest,
                    "lane_index": lane_index,
                    "replica_index": replica_index,
                    "role": lane.role.value,
                    "compute_tier": lane.compute_tier.value,
                    "context": lane.context.value,
                    "temperature": lane.temperature,
                    "diversity_axes": list(lane.diversity_axes),
                    "focus_hotspots": list(lane.focus_hotspots),
                }
            )
            task_id = "pop_" + variant_digest.removeprefix("sha256:")[:24]
            tasks.append(
                PopulationTask(
                    task_id=task_id,
                    lane_index=lane_index,
                    replica_index=replica_index,
                    role=lane.role,
                    compute_tier=lane.compute_tier,
                    context=lane.context,
                    temperature=lane.temperature,
                    independent_verification=lane.independent_verification,
                    diversity_axes=lane.diversity_axes,
                    focus_hotspots=lane.focus_hotspots,
                    variant_digest=variant_digest,
                )
            )
    manifest = PopulationManifest(
        swarm_plan_digest=plan_digest,
        search_provenance_digest=search_provenance_digest,
        phase=plan.phase,
        mode=plan.mode,
        tasks=tuple(tasks),
    )
    manifest.validate()
    return manifest


def summarize_population(
    manifest: PopulationManifest,
    receipts: Sequence[BehaviorReceipt],
    *,
    require_complete: bool = False,
) -> PopulationTelemetry:
    """Reduce provider receipts into correlation, effective-search and disagreement telemetry."""

    manifest.validate()
    tasks = {task.task_id: task for task in manifest.tasks}
    rows: dict[str, BehaviorReceipt] = {}
    for receipt in receipts:
        receipt.validate()
        if receipt.task_id not in tasks:
            raise PopulationManifestError(f"receipt names unknown population task {receipt.task_id}")
        if receipt.task_id in rows:
            raise PopulationManifestError(f"duplicate receipt for population task {receipt.task_id}")
        rows[receipt.task_id] = receipt
    if require_complete and set(rows) != set(tasks):
        missing = sorted(set(tasks) - set(rows))
        raise PopulationManifestError(f"population receipts incomplete; missing {', '.join(missing)}")

    answered = [receipt for receipt in rows.values() if receipt.state == "answered"]
    signatures = [receipt.behavior_signature for receipt in answered]
    candidate_digests = {
        receipt.candidate_digest
        for receipt in answered
        if receipt.candidate_digest is not None
    }
    disagreement = (
        0.0
        if len(answered) < 2
        else max(0.0, (len(candidate_digests) - 1) / max(1, len(answered) - 1))
    )
    verifier_answers = sum(
        1
        for receipt in answered
        if tasks[receipt.task_id].independent_verification
    )
    telemetry = PopulationTelemetry(
        manifest_digest=manifest.digest(),
        total_tasks=len(tasks),
        receipts=len(rows),
        answered=len(answered),
        independent_verifier_answers=verifier_answers,
        unique_candidates=len(candidate_digests),
        effective_independent_search=effective_independent_search(signatures),
        mean_correlation=mean_pairwise_correlation(signatures),
        candidate_disagreement=round(min(1.0, disagreement), 6),
        total_cost_usd=round(sum(receipt.cost_usd for receipt in rows.values()), 6),
        total_duration_s=round(sum(receipt.duration_s for receipt in rows.values()), 6),
        receipt_digests=tuple(sorted(receipt.digest() for receipt in rows.values())),
    )
    return telemetry
