"""Cognitive harness contract for the Liquid / Dark Software Factory.

This module models cognition, not authority.

The harness may create stochastic worlds, combine heterogeneous agents, change search posture,
coarse-grain context, crystallize memory, and request that an exact candidate cross an authority
boundary. It cannot itself make that crossing.

The core split is intentional:

* System 1 creates useful entropy.
* System 2 measures and destroys entropy.
* The phase controller decides which kind of cognition is useful now.
* Gauge fixing collapses equivalent representations to one canonical view.
* Only gauge-invariant observables may participate in promotion evidence.
* Durable real-world authority remains outside this module in the fenced Rust/control kernel.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum

from swfactory.phase_control import ControlMode, PhaseAssessment

COGNITIVE_HARNESS_SCHEMA_VERSION = 1
COGNITIVE_HARNESS_AUTHORITY = "search-only"
REALITY_BOUNDARY = "rust-authority-kernel"

GAUGE_DEPENDENT_QUANTITIES = frozenset(
    {
        "agent_id",
        "model",
        "prompt",
        "reasoning_style",
        "token_count",
        "branch_name",
        "explanation",
        "trajectory_id",
        "runtime_name",
    }
)

GAUGE_INVARIANT_QUANTITIES = frozenset(
    {
        "candidate_digest",
        "source_digest",
        "recipe_digest",
        "policy_digest",
        "evidence_digest",
        "artifact_digest",
        "external_effect_digest",
        "cell_id",
        "epoch",
    }
)


class CognitiveLayer(StrEnum):
    REFLEX = "reflex"
    SYSTEM1 = "system1"
    SYSTEM2 = "system2"
    CONSOLIDATION = "consolidation"
    AUTHORITY_BOUNDARY = "authority-boundary"


class CognitiveTimescale(StrEnum):
    REFLEX = "milliseconds-seconds"
    FAST = "seconds-minutes"
    DELIBERATE = "minutes-hours"
    SLOW = "hours-days"


class QuantityKind(StrEnum):
    GAUGE_DEPENDENT = "gauge-dependent"
    GAUGE_INVARIANT = "gauge-invariant"
    UNKNOWN = "unknown"


class WorldAction(StrEnum):
    EXPAND = "expand-worlds"
    COORDINATE = "coordinate-worlds"
    FREEZE = "freeze-worlds"
    PRUNE = "prune-worlds"
    VERIFY = "verify-world"
    PERTURB = "perturb-world"
    DRAIN = "drain-worlds"


class MemoryPhase(StrEnum):
    OBSERVATION = "observation"
    TRACE = "trace"
    CORRELATED = "correlated"
    CANDIDATE_BELIEF = "candidate-belief"
    CRYSTAL = "memory-crystal"


class MemoryAction(StrEnum):
    RECORD = "record"
    RETAIN = "retain"
    CORRELATE = "correlate"
    CHALLENGE = "challenge"
    CRYSTALLIZE = "crystallize"
    COMPACT = "compact"


class CognitiveAttention(StrEnum):
    ROUTINE = "routine"
    EXCEPTION = "exception"
    AUTHORITY = "authority-boundary"


class MeasurementKind(StrEnum):
    TEST = "test"
    STATIC_ANALYSIS = "static-analysis"
    REPLAY = "replay"
    PERFORMANCE = "performance"
    SECURITY = "security"
    FORMAL = "formal"
    HUMAN = "human"


class ImprovementDisposition(StrEnum):
    SHADOW = "shadow"
    CANDIDATE = "candidate"
    ADOPTABLE = "adoptable"
    REJECTED = "rejected"


@dataclass(frozen=True)
class WorldCandidate:
    """One executable hypothesis/world.

    Representation metadata is deliberately separated from invariant observables. Two worlds may
    have different agents/prompts/runtimes yet be gauge-equivalent if they produce the same exact
    externally meaningful candidate/evidence state.
    """

    world_id: str
    trajectory_id: str
    model: str
    runtime: str
    role: str
    candidate_digest: str
    source_digest: str
    recipe_digest: str
    policy_digest: str
    evidence_digest: str | None
    artifact_digest: str | None = None
    external_effect_digest: str | None = None
    verified: bool = False
    score: float = 0.0
    cost: float = 0.0

    def validate(self) -> None:
        required = {
            "world_id": self.world_id,
            "trajectory_id": self.trajectory_id,
            "candidate_digest": self.candidate_digest,
            "source_digest": self.source_digest,
            "recipe_digest": self.recipe_digest,
            "policy_digest": self.policy_digest,
        }
        for name, value in required.items():
            if not value.strip():
                raise ValueError(f"{name} must be nonempty")
        if not math.isfinite(self.score):
            raise ValueError("score must be finite")
        if not math.isfinite(self.cost) or self.cost < 0.0:
            raise ValueError("cost must be finite and non-negative")

    def invariant_observables(self) -> dict[str, object]:
        self.validate()
        return {
            "candidate_digest": self.candidate_digest,
            "source_digest": self.source_digest,
            "recipe_digest": self.recipe_digest,
            "policy_digest": self.policy_digest,
            "evidence_digest": self.evidence_digest,
            "artifact_digest": self.artifact_digest,
            "external_effect_digest": self.external_effect_digest,
            "verified": self.verified,
        }

    def invariant_fingerprint(self) -> str:
        return stable_digest(self.invariant_observables())


@dataclass(frozen=True)
class GaugeEquivalenceClass:
    invariant_fingerprint: str
    canonical_world_id: str
    member_world_ids: tuple[str, ...]


@dataclass(frozen=True)
class EnsembleMember:
    """One cognitive sampler/evaluator and a retained behavior signature.

    The signature is intentionally behavior/evidence-oriented instead of a vendor/model label. Two
    differently named models that behave identically are highly correlated and add little search
    information.
    """

    member_id: str
    model: str
    role: str
    runtime: str
    behavior_signature: tuple[str, ...]


@dataclass(frozen=True)
class PairwiseCorrelation:
    left: str
    right: str
    value: float


@dataclass(frozen=True)
class StochasticField:
    """A bounded probability field over factory-declared options.

    Jev or any future oracle may supply the weights. It cannot add values, sample external
    authority, or promote a result.
    """

    source: str
    probabilities: Mapping[str, float]
    authority: str = "exploration-only"

    def normalized(self, allowed: Sequence[str]) -> dict[str, float]:
        allowed_set = set(allowed)
        if not allowed_set:
            raise ValueError("allowed values must be nonempty")
        if set(self.probabilities) - allowed_set:
            raise ValueError("stochastic field contains undeclared values")
        weights: dict[str, float] = {}
        for value in allowed:
            weight = float(self.probabilities.get(value, 0.0))
            if not math.isfinite(weight) or weight < 0.0:
                raise ValueError("field weights must be finite and non-negative")
            weights[value] = weight
        total = sum(weights.values())
        if total <= 0.0:
            uniform = 1.0 / len(weights)
            return {key: uniform for key in weights}
        return {key: weight / total for key, weight in weights.items()}


@dataclass(frozen=True)
class MemoryEvidence:
    recurrence: float
    independent_confirmations: int
    contradictions: int
    evidence_strength: float
    context_pressure: float

    def validate(self) -> None:
        for name, value in {
            "recurrence": self.recurrence,
            "evidence_strength": self.evidence_strength,
            "context_pressure": self.context_pressure,
        }.items():
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be finite and in [0, 1]")
        if self.independent_confirmations < 0 or self.contradictions < 0:
            raise ValueError("memory counts must be non-negative")


@dataclass(frozen=True)
class MeasurementReceipt:
    """One observation produced by executing/challenging a world."""

    world_id: str
    kind: MeasurementKind
    observable: str
    result_digest: str
    evidence_digest: str
    independent: bool
    passed: bool

    def validate(self) -> None:
        for name, value in {
            "world_id": self.world_id,
            "observable": self.observable,
            "result_digest": self.result_digest,
            "evidence_digest": self.evidence_digest,
        }.items():
            if not value.strip():
                raise ValueError(f"{name} must be nonempty")


@dataclass(frozen=True)
class DissipationSnapshot:
    """Waste exported to keep the cognitive process ordered."""

    discarded_worlds: int
    reclaimed_contexts: int
    cleaned_sandboxes: int
    cancelled_retries: int
    stale_memories_retired: int

    def total(self) -> int:
        values = (
            self.discarded_worlds,
            self.reclaimed_contexts,
            self.cleaned_sandboxes,
            self.cancelled_retries,
            self.stale_memories_retired,
        )
        if any(value < 0 for value in values):
            raise ValueError("dissipation counts must be non-negative")
        return sum(values)


@dataclass(frozen=True)
class SelfImprovementExperiment:
    """A proposed change to the factory treated exactly like another bounded world."""

    experiment_id: str
    baseline_digest: str
    candidate_world_ids: tuple[str, ...]
    objective_digest: str
    evidence_digest: str | None
    disposition: ImprovementDisposition = ImprovementDisposition.SHADOW
    authority: str = "proposal-only"

    def validate(self) -> None:
        if not self.experiment_id.strip() or not self.baseline_digest.strip() or not self.objective_digest.strip():
            raise ValueError("self-improvement identity/objective fields must be nonempty")
        if not self.candidate_world_ids:
            raise ValueError("self-improvement experiment needs at least one candidate world")


@dataclass(frozen=True)
class CognitivePlan:
    schema_version: int
    authority: str
    reality_boundary: str
    layer: CognitiveLayer
    timescale: CognitiveTimescale
    system1_enabled: bool
    system2_enabled: bool
    world_action: WorldAction
    memory_action: MemoryAction
    attention: CognitiveAttention
    may_request_authority: bool
    ensemble_diversity: float
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            key: str(value) if isinstance(value, StrEnum) else value
            for key, value in asdict(self).items()
        }


@dataclass(frozen=True)
class AuthorityRequest:
    """A request to cross the reality boundary, never a grant."""

    cell_id: str
    epoch: int
    candidate_digest: str
    source_digest: str
    recipe_digest: str
    policy_digest: str
    evidence_digest: str
    requested_effect: str
    authority: str = "request-only"
    requires: str = REALITY_BOUNDARY

    def validate(self) -> None:
        if self.epoch < 0:
            raise ValueError("epoch must be non-negative")
        for name, value in {
            "cell_id": self.cell_id,
            "candidate_digest": self.candidate_digest,
            "source_digest": self.source_digest,
            "recipe_digest": self.recipe_digest,
            "policy_digest": self.policy_digest,
            "evidence_digest": self.evidence_digest,
            "requested_effect": self.requested_effect,
        }.items():
            if not value.strip():
                raise ValueError(f"{name} must be nonempty")

    def digest(self) -> str:
        self.validate()
        return stable_digest(asdict(self))


@dataclass(frozen=True)
class CognitiveReceipt:
    schema_version: int
    authority: str
    phase_assessment_digest: str
    worlds_digest: str
    ensemble_digest: str
    plan: CognitivePlan
    authority_request_digest: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "phase_assessment_digest": self.phase_assessment_digest,
            "worlds_digest": self.worlds_digest,
            "ensemble_digest": self.ensemble_digest,
            "plan": self.plan.as_dict(),
            "authority_request_digest": self.authority_request_digest,
        }


def validate_measurement_set(
    world: WorldCandidate,
    measurements: Sequence[MeasurementReceipt],
) -> bool:
    """Require independent evidence to bind to the exact executable world."""

    relevant = [measurement for measurement in measurements if measurement.world_id == world.world_id]
    for measurement in relevant:
        measurement.validate()
    return bool(relevant) and all(measurement.passed for measurement in relevant) and any(
        measurement.independent for measurement in relevant
    )


def classify_self_improvement(
    experiment: SelfImprovementExperiment,
    *,
    evidence_complete: bool,
    independent_verification: bool,
) -> ImprovementDisposition:
    experiment.validate()
    if not evidence_complete:
        return ImprovementDisposition.SHADOW
    if not independent_verification:
        return ImprovementDisposition.CANDIDATE
    return ImprovementDisposition.ADOPTABLE


def stable_digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(payload).hexdigest()


def quantity_kind(name: str) -> QuantityKind:
    if name in GAUGE_DEPENDENT_QUANTITIES:
        return QuantityKind.GAUGE_DEPENDENT
    if name in GAUGE_INVARIANT_QUANTITIES:
        return QuantityKind.GAUGE_INVARIANT
    return QuantityKind.UNKNOWN


def gauge_fix(worlds: Iterable[WorldCandidate]) -> tuple[GaugeEquivalenceClass, ...]:
    """Collapse representation-equivalent worlds without choosing between inequivalent candidates."""

    groups: dict[str, list[WorldCandidate]] = {}
    for world in worlds:
        fingerprint = world.invariant_fingerprint()
        groups.setdefault(fingerprint, []).append(world)

    classes: list[GaugeEquivalenceClass] = []
    for fingerprint, members in sorted(groups.items()):
        ordered = sorted(members, key=lambda world: (world.world_id, world.trajectory_id))
        classes.append(
            GaugeEquivalenceClass(
                invariant_fingerprint=fingerprint,
                canonical_world_id=ordered[0].world_id,
                member_world_ids=tuple(world.world_id for world in ordered),
            )
        )
    return tuple(classes)


def pairwise_correlation(left: EnsembleMember, right: EnsembleMember) -> PairwiseCorrelation:
    a, b = set(left.behavior_signature), set(right.behavior_signature)
    if not a and not b:
        value = 1.0
    elif not a or not b:
        value = 0.0
    else:
        value = len(a & b) / len(a | b)
    return PairwiseCorrelation(left=left.member_id, right=right.member_id, value=round(value, 6))


def ensemble_diversity(members: Sequence[EnsembleMember]) -> float:
    if len(members) < 2:
        return 0.0
    correlations: list[float] = []
    for index, left in enumerate(members):
        for right in members[index + 1 :]:
            correlations.append(pairwise_correlation(left, right).value)
    return round(1.0 - (sum(correlations) / len(correlations)), 6)


def marginal_information_value(*, correlation: float, expected_information: float, cost: float) -> float:
    for name, value in {"correlation": correlation, "expected_information": expected_information}.items():
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            raise ValueError(f"{name} must be finite and in [0, 1]")
    if not math.isfinite(cost) or cost <= 0.0:
        raise ValueError("cost must be finite and positive")
    return ((1.0 - correlation) * expected_information) / cost


def memory_phase(evidence: MemoryEvidence) -> MemoryPhase:
    evidence.validate()
    if evidence.contradictions > 0:
        return MemoryPhase.CANDIDATE_BELIEF
    if (
        evidence.independent_confirmations >= 3
        and evidence.recurrence >= 0.75
        and evidence.evidence_strength >= 0.85
    ):
        return MemoryPhase.CRYSTAL
    if evidence.independent_confirmations >= 2 and evidence.evidence_strength >= 0.65:
        return MemoryPhase.CANDIDATE_BELIEF
    if evidence.recurrence >= 0.50:
        return MemoryPhase.CORRELATED
    if evidence.independent_confirmations >= 1:
        return MemoryPhase.TRACE
    return MemoryPhase.OBSERVATION


def _memory_action(evidence: MemoryEvidence | None, mode: ControlMode) -> MemoryAction:
    if mode in {ControlMode.DRAIN, ControlMode.VERIFY}:
        return MemoryAction.COMPACT
    if evidence is None:
        return MemoryAction.RECORD
    phase = memory_phase(evidence)
    return {
        MemoryPhase.OBSERVATION: MemoryAction.RECORD,
        MemoryPhase.TRACE: MemoryAction.RETAIN,
        MemoryPhase.CORRELATED: MemoryAction.CORRELATE,
        MemoryPhase.CANDIDATE_BELIEF: MemoryAction.CHALLENGE,
        MemoryPhase.CRYSTAL: MemoryAction.CRYSTALLIZE,
    }[phase]


def plan_cognition(
    phase: PhaseAssessment,
    *,
    ensemble_members: Sequence[EnsembleMember] = (),
    memory_evidence: MemoryEvidence | None = None,
    human_attention_pressure: float = 0.0,
) -> CognitivePlan:
    """Translate phase/mode into cognitive architecture, never into real-world authority."""

    if not math.isfinite(human_attention_pressure) or not 0.0 <= human_attention_pressure <= 1.0:
        raise ValueError("human_attention_pressure must be finite and in [0, 1]")

    diversity = ensemble_diversity(ensemble_members)
    mode = phase.recommendation.mode

    if mode == ControlMode.DIVERGE:
        layer, timescale, world_action = CognitiveLayer.SYSTEM1, CognitiveTimescale.FAST, WorldAction.EXPAND
        system1, system2 = True, False
    elif mode == ControlMode.COORDINATE:
        layer, timescale, world_action = CognitiveLayer.SYSTEM1, CognitiveTimescale.FAST, WorldAction.COORDINATE
        system1, system2 = True, True
    elif mode == ControlMode.MEASURE:
        layer, timescale, world_action = CognitiveLayer.SYSTEM2, CognitiveTimescale.DELIBERATE, WorldAction.FREEZE
        system1, system2 = False, True
    elif mode == ControlMode.ANNEAL:
        layer, timescale, world_action = CognitiveLayer.SYSTEM2, CognitiveTimescale.DELIBERATE, WorldAction.PRUNE
        system1, system2 = False, True
    elif mode == ControlMode.VERIFY:
        layer, timescale, world_action = (
            CognitiveLayer.AUTHORITY_BOUNDARY,
            CognitiveTimescale.DELIBERATE,
            WorldAction.VERIFY,
        )
        system1, system2 = False, True
    elif mode == ControlMode.PERTURB:
        layer, timescale, world_action = CognitiveLayer.SYSTEM1, CognitiveTimescale.FAST, WorldAction.PERTURB
        system1, system2 = True, True
    else:
        layer, timescale, world_action = CognitiveLayer.REFLEX, CognitiveTimescale.REFLEX, WorldAction.DRAIN
        system1, system2 = False, True

    may_request_authority = (
        mode == ControlMode.VERIFY
        and phase.phase == "crystal"
        and phase.observation.evidence_completeness >= 0.90
        and phase.observation.verifier_disagreement <= 0.15
    )

    attention = CognitiveAttention.ROUTINE
    if human_attention_pressure >= 0.80 or mode in {
        ControlMode.MEASURE,
        ControlMode.PERTURB,
        ControlMode.DRAIN,
    }:
        attention = CognitiveAttention.EXCEPTION
    if may_request_authority:
        attention = CognitiveAttention.AUTHORITY

    reason = (
        f"phase={phase.phase} mode={mode}; use {layer} cognition on the {timescale} timescale; "
        f"ensemble_diversity={diversity:.3f}; reality remains behind {REALITY_BOUNDARY}"
    )

    return CognitivePlan(
        schema_version=COGNITIVE_HARNESS_SCHEMA_VERSION,
        authority=COGNITIVE_HARNESS_AUTHORITY,
        reality_boundary=REALITY_BOUNDARY,
        layer=layer,
        timescale=timescale,
        system1_enabled=system1,
        system2_enabled=system2,
        world_action=world_action,
        memory_action=_memory_action(memory_evidence, mode),
        attention=attention,
        may_request_authority=may_request_authority,
        ensemble_diversity=diversity,
        reason=reason,
    )


def make_receipt(
    phase: PhaseAssessment,
    worlds: Sequence[WorldCandidate],
    ensemble_members: Sequence[EnsembleMember],
    *,
    memory_evidence: MemoryEvidence | None = None,
    human_attention_pressure: float = 0.0,
    authority_request: AuthorityRequest | None = None,
) -> CognitiveReceipt:
    plan = plan_cognition(
        phase,
        ensemble_members=ensemble_members,
        memory_evidence=memory_evidence,
        human_attention_pressure=human_attention_pressure,
    )
    if authority_request is not None:
        if not plan.may_request_authority:
            raise ValueError("authority may only be requested from a crystal verification posture")
        request_digest = authority_request.digest()
    else:
        request_digest = None

    return CognitiveReceipt(
        schema_version=COGNITIVE_HARNESS_SCHEMA_VERSION,
        authority=COGNITIVE_HARNESS_AUTHORITY,
        phase_assessment_digest=stable_digest(phase.as_dict()),
        worlds_digest=stable_digest([world.invariant_observables() for world in worlds]),
        ensemble_digest=stable_digest([asdict(member) for member in ensemble_members]),
        plan=plan,
        authority_request_digest=request_digest,
    )
