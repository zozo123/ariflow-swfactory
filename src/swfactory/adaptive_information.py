"""Adaptive search-budget control from retained population information.

The factory already knows how to create and execute bounded heterogeneous populations. This module
answers a narrower question for the *next* round: did another unit of compute buy useful independent
information?

The controller is deliberately search-only. It may narrow the next SwarmBudget, cap previously
measured roles, or recommend a reversible search mode (measure, perturb, drain). It may never widen
past the operator-declared budget, schedule Airflow, mutate a Factory Cell, mint credentials, relax
verification, approve, publish, merge, or promote.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

from swfactory.cognitive_harness import marginal_information_value
from swfactory.phase_control import ControlMode
from swfactory.population_execution import PopulationExecutionReport
from swfactory.population_manifest import (
    BehaviorReceipt,
    PopulationManifest,
    PopulationManifestError,
)
from swfactory.swarm_dynamics import (
    AgentRole,
    ComputeTier,
    SwarmBudget,
    effective_independent_search,
    mean_pairwise_correlation,
)

INFORMATION_BUDGET_SCHEMA_VERSION = 1
INFORMATION_BUDGET_AUTHORITY = "search-only"


class BudgetAction(StrEnum):
    STOP = "stop"
    PROBE = "probe"
    SHRINK = "shrink"
    HOLD = "hold"
    VERIFY = "verify"


@dataclass(frozen=True)
class InformationBudgetPolicy:
    """Human-declared thresholds for adaptive search spending."""

    min_value_per_compute_unit: float = 0.05
    correlation_collapse: float = 0.85
    high_disagreement: float = 0.55
    settled_disagreement: float = 0.15
    min_independent_verifier_answers: int = 1
    min_retention_fraction: float = 0.25

    def validate(self) -> None:
        for name, value in {
            "min_value_per_compute_unit": self.min_value_per_compute_unit,
            "correlation_collapse": self.correlation_collapse,
            "high_disagreement": self.high_disagreement,
            "settled_disagreement": self.settled_disagreement,
            "min_retention_fraction": self.min_retention_fraction,
        }.items():
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        if self.min_value_per_compute_unit < 0.0:
            raise ValueError("min_value_per_compute_unit must be non-negative")
        for name, value in {
            "correlation_collapse": self.correlation_collapse,
            "high_disagreement": self.high_disagreement,
            "settled_disagreement": self.settled_disagreement,
            "min_retention_fraction": self.min_retention_fraction,
        }.items():
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        if self.settled_disagreement > self.high_disagreement:
            raise ValueError("settled_disagreement cannot exceed high_disagreement")
        if self.min_independent_verifier_answers < 0:
            raise ValueError("min_independent_verifier_answers must be non-negative")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)

    def digest(self) -> str:
        return _digest(self.canonical_dict())


@dataclass(frozen=True)
class LaneInformation:
    lane_index: int
    role: AgentRole
    compute_tier: ComputeTier
    task_count: int
    receipt_count: int
    answered: int
    unique_candidates: int
    evidence_answers: int
    effective_independent_search: float
    mean_correlation: float
    expected_information: float
    compute_units_per_task: float
    marginal_information_value: float
    recommended_count: int
    action: BudgetAction
    reason: str

    def validate(self) -> None:
        if self.lane_index < 0:
            raise ValueError("lane_index must be non-negative")
        counts = (
            self.task_count,
            self.receipt_count,
            self.answered,
            self.unique_candidates,
            self.evidence_answers,
            self.recommended_count,
        )
        if min(counts) < 0:
            raise ValueError("lane information counts must be non-negative")
        if self.receipt_count > self.task_count or self.answered > self.receipt_count:
            raise ValueError("lane information counts are inconsistent")
        if self.unique_candidates > self.answered or self.evidence_answers > self.answered:
            raise ValueError("lane evidence/candidate counts exceed answered tasks")
        if self.recommended_count > self.task_count:
            raise ValueError("adaptive budget may not widen a measured lane")
        for name, value in {
            "effective_independent_search": self.effective_independent_search,
            "mean_correlation": self.mean_correlation,
            "expected_information": self.expected_information,
            "compute_units_per_task": self.compute_units_per_task,
            "marginal_information_value": self.marginal_information_value,
        }.items():
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.mean_correlation > 1.0 or self.expected_information > 1.0:
            raise ValueError("lane correlation/information must be in [0, 1]")
        if not self.reason.strip():
            raise ValueError("lane information reason must be nonempty")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "lane_index": self.lane_index,
            "role": self.role.value,
            "compute_tier": self.compute_tier.value,
            "task_count": self.task_count,
            "receipt_count": self.receipt_count,
            "answered": self.answered,
            "unique_candidates": self.unique_candidates,
            "evidence_answers": self.evidence_answers,
            "effective_independent_search": self.effective_independent_search,
            "mean_correlation": self.mean_correlation,
            "expected_information": self.expected_information,
            "compute_units_per_task": self.compute_units_per_task,
            "marginal_information_value": self.marginal_information_value,
            "recommended_count": self.recommended_count,
            "action": self.action.value,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class InformationBudgetDecision:
    """Replayable search-only decision for the next population envelope."""

    source_manifest_digest: str
    source_execution_report_digest: str
    policy_digest: str
    base_budget: SwarmBudget
    next_budget: SwarmBudget
    lanes: tuple[LaneInformation, ...]
    role_caps: tuple[tuple[AgentRole, int], ...]
    mode_override: ControlMode | None
    stop_new_work: bool
    verifier_reserve: int
    reason: str
    authority: str = INFORMATION_BUDGET_AUTHORITY
    schema_version: int = INFORMATION_BUDGET_SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != INFORMATION_BUDGET_SCHEMA_VERSION:
            raise ValueError("unsupported information-budget schema")
        if self.authority != INFORMATION_BUDGET_AUTHORITY:
            raise ValueError("information-budget decisions must remain search-only")
        _require_sha256(self.source_manifest_digest, field="source_manifest_digest")
        _require_sha256(self.source_execution_report_digest, field="source_execution_report_digest")
        _require_sha256(self.policy_digest, field="policy_digest")
        self.base_budget.validate()
        self.next_budget.validate()
        if self.next_budget.max_agents > self.base_budget.max_agents:
            raise ValueError("adaptive budget may not widen max_agents")
        if self.next_budget.max_parallel > self.base_budget.max_parallel:
            raise ValueError("adaptive budget may not widen max_parallel")
        if self.next_budget.max_deep_agents > self.base_budget.max_deep_agents:
            raise ValueError("adaptive budget may not widen max_deep_agents")
        if self.next_budget.max_exact_replays > self.base_budget.max_exact_replays:
            raise ValueError("adaptive budget may not widen max_exact_replays")
        if self.next_budget.max_compute_units > self.base_budget.max_compute_units + 1e-9:
            raise ValueError("adaptive budget may not widen max_compute_units")
        seen_lanes: set[int] = set()
        for lane in self.lanes:
            lane.validate()
            if lane.lane_index in seen_lanes:
                raise ValueError("duplicate lane information")
            seen_lanes.add(lane.lane_index)
        seen_roles: set[AgentRole] = set()
        for role, cap in self.role_caps:
            if role in seen_roles:
                raise ValueError("duplicate role cap")
            if cap < 0:
                raise ValueError("role caps must be non-negative")
            seen_roles.add(role)
        if self.verifier_reserve < 0 or self.verifier_reserve > self.base_budget.max_agents:
            raise ValueError("verifier reserve is outside the declared budget")
        if self.stop_new_work and self.mode_override not in {None, ControlMode.DRAIN}:
            raise ValueError("stop_new_work may only pair with drain mode")
        if not self.reason.strip():
            raise ValueError("information-budget reason must be nonempty")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "source_manifest_digest": self.source_manifest_digest,
            "source_execution_report_digest": self.source_execution_report_digest,
            "policy_digest": self.policy_digest,
            "base_budget": _budget_dict(self.base_budget),
            "next_budget": _budget_dict(self.next_budget),
            "lanes": [lane.canonical_dict() for lane in self.lanes],
            "role_caps": [{"role": role.value, "cap": cap} for role, cap in self.role_caps],
            "mode_override": self.mode_override.value if self.mode_override is not None else None,
            "stop_new_work": self.stop_new_work,
            "verifier_reserve": self.verifier_reserve,
            "reason": self.reason,
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())

    def role_cap_map(self) -> dict[AgentRole, int]:
        self.validate()
        return dict(self.role_caps)


def evaluate_information_budget(
    report: PopulationExecutionReport,
    *,
    base_budget: SwarmBudget,
    manifest: PopulationManifest | None = None,
    policy: InformationBudgetPolicy | None = None,
) -> InformationBudgetDecision:
    """Turn retained population behavior into the next bounded search envelope.

    With a manifest, the decision is lane-aware. Without it, the reducer still produces a global
    budget from the execution telemetry; this supports offline/replay callers that retained the
    execution report but not the full prior plan.
    """

    report.validate()
    base_budget.validate()
    policy = policy or InformationBudgetPolicy()
    policy.validate()
    if manifest is not None:
        manifest.validate()
        if manifest.digest() != report.population_manifest_digest:
            raise PopulationManifestError("information budget manifest does not match execution report")

    if report.cancelled:
        return _cancelled_decision(report, base_budget=base_budget, policy=policy)

    lanes = (
        _lane_information(manifest, report, policy)
        if manifest is not None
        else (_global_information(report, policy),)
    )
    all_below = all(lane.marginal_information_value < policy.min_value_per_compute_unit for lane in lanes)
    telemetry = report.telemetry
    needs_verifier = (
        telemetry.candidate_disagreement >= policy.high_disagreement
        and telemetry.independent_verifier_answers < policy.min_independent_verifier_answers
    )
    collapsed = (
        telemetry.mean_correlation >= policy.correlation_collapse
        and telemetry.effective_independent_search
        <= max(1.0, float(max(1, telemetry.answered)) * 0.35)
    )
    settled = (
        telemetry.answered > 0
        and telemetry.candidate_disagreement <= policy.settled_disagreement
        and all_below
    )

    mode_override: ControlMode | None = None
    stop_new_work = False
    verifier_reserve = 0
    if settled:
        mode_override = ControlMode.DRAIN
        stop_new_work = True
    elif needs_verifier:
        mode_override = ControlMode.MEASURE
        verifier_reserve = 1 if base_budget.max_deep_agents > 0 else 0
    elif collapsed:
        mode_override = ControlMode.PERTURB

    role_caps = _role_caps(lanes)
    if verifier_reserve:
        role_caps[AgentRole.VERIFIER] = max(role_caps.get(AgentRole.VERIFIER, 0), verifier_reserve)

    if stop_new_work:
        next_budget = SwarmBudget(
            max_agents=1,
            max_parallel=1,
            max_deep_agents=0,
            max_exact_replays=0,
            max_compute_units=1.0,
        )
    else:
        retained = sum(lane.recommended_count for lane in lanes)
        if manifest is None:
            retained = _global_retained_count(report, policy, base_budget)
        retained = max(1, min(base_budget.max_agents, retained))
        deep = sum(
            lane.recommended_count
            for lane in lanes
            if lane.compute_tier == ComputeTier.DEEP
        )
        exact = sum(
            lane.recommended_count
            for lane in lanes
            if lane.compute_tier == ComputeTier.EXACT_REPLAY
        )
        deep = max(deep, verifier_reserve)
        deep = min(base_budget.max_deep_agents, deep)
        exact = min(base_budget.max_exact_replays, exact)
        compute = sum(
            lane.recommended_count * lane.compute_units_per_task
            for lane in lanes
        )
        if manifest is None:
            # A global reducer has no tier breakdown. Preserve only the fraction of the declared
            # compute envelope justified by measured effective independence.
            ratio = min(
                1.0,
                telemetry.effective_independent_search / max(1.0, float(telemetry.total_tasks)),
            )
            compute = max(1.0, base_budget.max_compute_units * max(0.10, ratio))
        if verifier_reserve:
            compute = max(compute, min(base_budget.max_compute_units, 8.0))
        next_budget = SwarmBudget(
            max_agents=retained,
            max_parallel=min(base_budget.max_parallel, retained),
            max_deep_agents=min(deep, retained),
            max_exact_replays=min(exact, retained),
            max_compute_units=min(base_budget.max_compute_units, max(1.0, compute)),
        )
    next_budget.validate()

    reason_bits = [
        f"effective={telemetry.effective_independent_search:.3f}/{telemetry.total_tasks}",
        f"correlation={telemetry.mean_correlation:.3f}",
        f"disagreement={telemetry.candidate_disagreement:.3f}",
        f"retained={next_budget.max_agents}/{base_budget.max_agents}",
    ]
    if mode_override is not None:
        reason_bits.append(f"mode={mode_override.value}")
    if needs_verifier:
        reason_bits.append("independent-verifier-reserve")
    decision = InformationBudgetDecision(
        source_manifest_digest=report.population_manifest_digest,
        source_execution_report_digest=report.digest(),
        policy_digest=policy.digest(),
        base_budget=base_budget,
        next_budget=next_budget,
        lanes=tuple(lanes),
        role_caps=tuple(sorted(role_caps.items(), key=lambda item: item[0].value)),
        mode_override=mode_override,
        stop_new_work=stop_new_work,
        verifier_reserve=verifier_reserve,
        reason="; ".join(reason_bits),
    )
    decision.validate()
    return decision


def information_budget_from_document(document: Mapping[str, Any]) -> InformationBudgetDecision:
    """Rehydrate a retained adaptive-budget decision and re-check its no-widening invariant."""

    raw_lanes = document.get("lanes")
    raw_caps = document.get("role_caps")
    if not isinstance(raw_lanes, list) or not isinstance(raw_caps, list):
        raise ValueError("information budget lanes/role_caps must be arrays")
    lanes = tuple(
        LaneInformation(
            lane_index=int(row["lane_index"]),
            role=AgentRole(str(row["role"])),
            compute_tier=ComputeTier(str(row["compute_tier"])),
            task_count=int(row["task_count"]),
            receipt_count=int(row["receipt_count"]),
            answered=int(row["answered"]),
            unique_candidates=int(row["unique_candidates"]),
            evidence_answers=int(row["evidence_answers"]),
            effective_independent_search=float(row["effective_independent_search"]),
            mean_correlation=float(row["mean_correlation"]),
            expected_information=float(row["expected_information"]),
            compute_units_per_task=float(row["compute_units_per_task"]),
            marginal_information_value=float(row["marginal_information_value"]),
            recommended_count=int(row["recommended_count"]),
            action=BudgetAction(str(row["action"])),
            reason=str(row["reason"]),
        )
        for row in raw_lanes
        if isinstance(row, Mapping)
    )
    if len(lanes) != len(raw_lanes):
        raise ValueError("information budget lane entries must be objects")
    caps = tuple(
        (AgentRole(str(row["role"])), int(row["cap"]))
        for row in raw_caps
        if isinstance(row, Mapping)
    )
    if len(caps) != len(raw_caps):
        raise ValueError("information budget role caps must be objects")
    mode = document.get("mode_override")
    decision = InformationBudgetDecision(
        source_manifest_digest=str(document["source_manifest_digest"]),
        source_execution_report_digest=str(document["source_execution_report_digest"]),
        policy_digest=str(document["policy_digest"]),
        base_budget=_budget_from_document(document["base_budget"]),
        next_budget=_budget_from_document(document["next_budget"]),
        lanes=lanes,
        role_caps=caps,
        mode_override=ControlMode(str(mode)) if mode is not None else None,
        stop_new_work=bool(document["stop_new_work"]),
        verifier_reserve=int(document["verifier_reserve"]),
        reason=str(document["reason"]),
        authority=str(document.get("authority", INFORMATION_BUDGET_AUTHORITY)),
        schema_version=int(document.get("schema_version", INFORMATION_BUDGET_SCHEMA_VERSION)),
    )
    decision.validate()
    return decision


def _lane_information(
    manifest: PopulationManifest,
    report: PopulationExecutionReport,
    policy: InformationBudgetPolicy,
) -> tuple[LaneInformation, ...]:
    receipts = {receipt.task_id: receipt for receipt in report.receipts}
    grouped: dict[int, list] = defaultdict(list)
    for task in manifest.tasks:
        grouped[task.lane_index].append(task)

    result: list[LaneInformation] = []
    for lane_index in sorted(grouped):
        tasks = tuple(grouped[lane_index])
        role = tasks[0].role
        tier = tasks[0].compute_tier
        if any(task.role != role or task.compute_tier != tier for task in tasks):
            raise PopulationManifestError(f"population lane {lane_index} has inconsistent role/compute tier")
        lane_receipts = tuple(receipts[task.task_id] for task in tasks if task.task_id in receipts)
        answered = tuple(receipt for receipt in lane_receipts if receipt.state == "answered")
        signatures = tuple(receipt.behavior_signature for receipt in answered)
        correlation = mean_pairwise_correlation(signatures)
        independent = effective_independent_search(signatures)
        unique_candidates = len(
            {receipt.candidate_digest for receipt in answered if receipt.candidate_digest is not None}
        )
        evidence_answers = sum(1 for receipt in answered if receipt.evidence_digest is not None)
        answer_rate = _ratio(len(answered), len(lane_receipts))
        novelty = _ratio(unique_candidates, len(answered))
        evidence_yield = _ratio(evidence_answers, len(answered))
        verification_pressure = (
            report.telemetry.candidate_disagreement
            if any(task.independent_verification for task in tasks)
            else max(0.0, 1.0 - report.telemetry.mean_correlation)
        )
        unresolved = max(0.0, 1.0 - evidence_yield)
        expected = _clamp(
            0.30 * novelty
            + 0.20 * evidence_yield
            + 0.20 * answer_rate
            + 0.20 * verification_pressure
            + 0.10 * unresolved
        )
        cost_units = _tier_units(tier)
        value = marginal_information_value(
            correlation=correlation,
            expected_information=expected,
            cost=cost_units,
        )
        count = _recommended_count(
            task_count=len(tasks),
            marginal_value=value,
            correlation=correlation,
            verifier=any(task.independent_verification for task in tasks),
            disagreement=report.telemetry.candidate_disagreement,
            policy=policy,
        )
        if count == 0:
            action = BudgetAction.STOP
        elif count < len(tasks):
            action = BudgetAction.SHRINK
        elif any(task.independent_verification for task in tasks):
            action = BudgetAction.VERIFY
        else:
            action = BudgetAction.HOLD
        result.append(
            LaneInformation(
                lane_index=lane_index,
                role=role,
                compute_tier=tier,
                task_count=len(tasks),
                receipt_count=len(lane_receipts),
                answered=len(answered),
                unique_candidates=unique_candidates,
                evidence_answers=evidence_answers,
                effective_independent_search=independent,
                mean_correlation=correlation,
                expected_information=round(expected, 6),
                compute_units_per_task=cost_units,
                marginal_information_value=value,
                recommended_count=count,
                action=action,
                reason=(
                    f"answer={answer_rate:.3f}; novelty={novelty:.3f}; evidence={evidence_yield:.3f}; "
                    f"verification-pressure={verification_pressure:.3f}; value={value:.6f}"
                ),
            )
        )
    return tuple(result)


def _global_information(
    report: PopulationExecutionReport,
    policy: InformationBudgetPolicy,
) -> LaneInformation:
    answered = tuple(receipt for receipt in report.receipts if receipt.state == "answered")
    evidence_answers = sum(1 for receipt in answered if receipt.evidence_digest is not None)
    expected = _clamp(
        0.35 * _ratio(report.telemetry.unique_candidates, report.telemetry.answered)
        + 0.25 * _ratio(evidence_answers, len(answered))
        + 0.20 * _ratio(report.telemetry.answered, report.telemetry.receipts)
        + 0.20 * report.telemetry.candidate_disagreement
    )
    value = marginal_information_value(
        correlation=report.telemetry.mean_correlation,
        expected_information=expected,
        cost=1.0,
    )
    count = _global_retained_count(report, policy, SwarmBudget(max_agents=max(1, report.telemetry.total_tasks)))
    action = BudgetAction.HOLD if count >= report.telemetry.total_tasks else BudgetAction.SHRINK
    if count == 0:
        action = BudgetAction.STOP
    return LaneInformation(
        lane_index=0,
        role=AgentRole.EXPLORER,
        compute_tier=ComputeTier.CHEAP,
        task_count=report.telemetry.total_tasks,
        receipt_count=report.telemetry.receipts,
        answered=report.telemetry.answered,
        unique_candidates=report.telemetry.unique_candidates,
        evidence_answers=evidence_answers,
        effective_independent_search=report.telemetry.effective_independent_search,
        mean_correlation=report.telemetry.mean_correlation,
        expected_information=round(expected, 6),
        compute_units_per_task=1.0,
        marginal_information_value=value,
        recommended_count=count,
        action=action,
        reason=f"global telemetry value={value:.6f}",
    )


def _recommended_count(
    *,
    task_count: int,
    marginal_value: float,
    correlation: float,
    verifier: bool,
    disagreement: float,
    policy: InformationBudgetPolicy,
) -> int:
    if task_count <= 0:
        return 0
    if marginal_value < policy.min_value_per_compute_unit:
        count = 0
    else:
        denominator = max(policy.min_value_per_compute_unit * 4.0, 1e-9)
        retention = max(policy.min_retention_fraction, min(1.0, marginal_value / denominator))
        count = max(1, min(task_count, math.ceil(task_count * retention)))
    if correlation >= policy.correlation_collapse and task_count > 1:
        count = min(count, 1)
    if verifier and disagreement >= policy.high_disagreement:
        count = max(count, 1)
    return count


def _global_retained_count(
    report: PopulationExecutionReport,
    policy: InformationBudgetPolicy,
    budget: SwarmBudget,
) -> int:
    telemetry = report.telemetry
    if telemetry.total_tasks <= 0:
        return 1
    independence_ratio = min(
        1.0,
        telemetry.effective_independent_search / max(1.0, float(telemetry.total_tasks)),
    )
    if (
        telemetry.candidate_disagreement <= policy.settled_disagreement
        and telemetry.mean_correlation >= policy.correlation_collapse
    ):
        return 1
    return max(
        1,
        min(
            budget.max_agents,
            telemetry.total_tasks,
            math.ceil(telemetry.total_tasks * max(policy.min_retention_fraction, independence_ratio)),
        ),
    )


def _cancelled_decision(
    report: PopulationExecutionReport,
    *,
    base_budget: SwarmBudget,
    policy: InformationBudgetPolicy,
) -> InformationBudgetDecision:
    next_budget = SwarmBudget(
        max_agents=1,
        max_parallel=1,
        max_deep_agents=0,
        max_exact_replays=0,
        max_compute_units=1.0,
    )
    decision = InformationBudgetDecision(
        source_manifest_digest=report.population_manifest_digest,
        source_execution_report_digest=report.digest(),
        policy_digest=policy.digest(),
        base_budget=base_budget,
        next_budget=next_budget,
        lanes=(),
        role_caps=(),
        mode_override=ControlMode.DRAIN,
        stop_new_work=True,
        verifier_reserve=0,
        reason="previous population was cancelled; drain before creating new search work",
    )
    decision.validate()
    return decision


def _role_caps(lanes: Sequence[LaneInformation]) -> dict[AgentRole, int]:
    caps: dict[AgentRole, int] = defaultdict(int)
    for lane in lanes:
        caps[lane.role] += lane.recommended_count
    return dict(caps)


def _tier_units(tier: ComputeTier) -> float:
    return {
        ComputeTier.CHEAP: 1.0,
        ComputeTier.STANDARD: 2.0,
        ComputeTier.DEEP: 8.0,
        ComputeTier.EXACT_REPLAY: 3.0,
    }[tier]


def _budget_dict(budget: SwarmBudget) -> dict[str, Any]:
    budget.validate()
    return {
        "max_agents": budget.max_agents,
        "max_parallel": budget.max_parallel,
        "max_deep_agents": budget.max_deep_agents,
        "max_exact_replays": budget.max_exact_replays,
        "max_compute_units": budget.max_compute_units,
    }


def _budget_from_document(document: Any) -> SwarmBudget:
    if not isinstance(document, Mapping):
        raise ValueError("information budget envelope must be an object")
    budget = SwarmBudget(
        max_agents=int(document["max_agents"]),
        max_parallel=int(document["max_parallel"]),
        max_deep_agents=int(document["max_deep_agents"]),
        max_exact_replays=int(document["max_exact_replays"]),
        max_compute_units=float(document["max_compute_units"]),
    )
    budget.validate()
    return budget


def _ratio(numerator: int, denominator: int) -> float:
    return 0.0 if denominator <= 0 else numerator / denominator


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _require_sha256(value: str, *, field: str) -> None:
    raw = value.removeprefix("sha256:")
    if len(raw) != 64 or any(char not in "0123456789abcdef" for char in raw):
        raise ValueError(f"{field} must be a canonical sha256 digest")


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()
