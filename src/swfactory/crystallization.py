"""Advisory policy for when *not* to freeze or formalize a candidate.

Phase classification describes an observed search regime.  This module answers a different question:
is it epistemically sensible to freeze this candidate and, if so, which verification style fits the
claim?  It is deliberately search/evidence advisory and grants no promotion authority.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from swfactory.formal_claims import EvidenceMethod

CRYSTALLIZATION_SCHEMA_VERSION = 1
CRYSTALLIZATION_AUTHORITY = "search-only"


class FreezePosture(StrEnum):
    KEEP_LIQUID = "keep-liquid"
    MEASURE = "measure"
    FREEZE = "freeze"


class FormalizationFit(StrEnum):
    DEFER = "defer"
    EMPIRICAL = "empirical"
    OPTIONAL = "optional"
    FORMAL = "formal"


class ClaimShape(StrEnum):
    TRANSITION_SYSTEM = "transition-system"
    PURE_FUNCTION = "pure-function"
    OPEN_WORLD = "open-world"
    PERFORMANCE = "performance"
    EXTERNAL_INTEGRATION = "external-integration"
    HEURISTIC = "heuristic"


@dataclass(frozen=True)
class CrystallizationContext:
    candidate_entropy: float
    specification_stability: float
    abstraction_fidelity: float
    evidence_coverage: float
    verifier_disagreement: float
    environment_volatility: float
    change_velocity: float
    consequence_of_error: float
    formalization_cost: float

    def validate(self) -> None:
        for name, value in vars(self).items():
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be finite and in [0, 1]")


@dataclass(frozen=True)
class CrystallizationPolicy:
    max_entropy_to_freeze: float = 0.35
    min_specification_stability: float = 0.65
    min_evidence_coverage: float = 0.70
    max_verifier_disagreement: float = 0.30
    max_change_velocity: float = 0.55
    min_abstraction_fidelity_for_formal: float = 0.72
    max_environment_volatility_for_formal: float = 0.55
    max_cost_for_formal: float = 0.80
    high_consequence: float = 0.72

    def validate(self) -> None:
        for name, value in vars(self).items():
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be finite and in [0, 1]")


@dataclass(frozen=True)
class CrystallizationAssessment:
    freeze_posture: FreezePosture
    formalization_fit: FormalizationFit
    reasons: tuple[str, ...]
    authority: str = CRYSTALLIZATION_AUTHORITY
    schema_version: int = CRYSTALLIZATION_SCHEMA_VERSION

    @property
    def should_freeze(self) -> bool:
        return self.freeze_posture == FreezePosture.FREEZE

    @property
    def should_formalize(self) -> bool:
        return self.formalization_fit == FormalizationFit.FORMAL


def assess_crystallization(
    context: CrystallizationContext,
    *,
    policy: CrystallizationPolicy | None = None,
) -> CrystallizationAssessment:
    """Recommend whether to keep searching, measure, or freeze.

    The order is intentional: unstable specifications and active search defeat crystallization
    before proof tractability is considered.  Formal methods are then selected only for a frozen
    candidate whose abstraction is credible enough to make the proof about the right thing.
    """

    context.validate()
    policy = policy or CrystallizationPolicy()
    policy.validate()
    reasons: list[str] = []

    if context.specification_stability < policy.min_specification_stability:
        reasons.append("specification is still moving")
    if context.candidate_entropy > policy.max_entropy_to_freeze:
        reasons.append("candidate distribution remains too broad")
    if context.change_velocity > policy.max_change_velocity:
        reasons.append("candidate semantics are still changing materially")

    if reasons:
        return CrystallizationAssessment(
            freeze_posture=FreezePosture.KEEP_LIQUID,
            formalization_fit=FormalizationFit.DEFER,
            reasons=tuple(reasons),
        )

    if context.evidence_coverage < policy.min_evidence_coverage:
        reasons.append("required evidence coverage is incomplete")
    if context.verifier_disagreement > policy.max_verifier_disagreement:
        reasons.append("independent verifiers still disagree")

    if reasons:
        return CrystallizationAssessment(
            freeze_posture=FreezePosture.MEASURE,
            formalization_fit=FormalizationFit.DEFER,
            reasons=tuple(reasons),
        )

    weak_abstraction = context.abstraction_fidelity < policy.min_abstraction_fidelity_for_formal
    open_environment = context.environment_volatility > policy.max_environment_volatility_for_formal

    if weak_abstraction:
        reasons.append("formal abstraction is not faithful enough to carry the desired claim")
    if open_environment:
        reasons.append("environment is too volatile for a strong closed-model claim")

    if weak_abstraction or open_environment:
        return CrystallizationAssessment(
            freeze_posture=FreezePosture.FREEZE,
            formalization_fit=FormalizationFit.EMPIRICAL,
            reasons=tuple(reasons),
        )

    if (
        context.consequence_of_error >= policy.high_consequence
        and context.formalization_cost <= policy.max_cost_for_formal
    ):
        return CrystallizationAssessment(
            freeze_posture=FreezePosture.FREEZE,
            formalization_fit=FormalizationFit.FORMAL,
            reasons=("high-consequence claim has a credible, tractable abstraction",),
        )

    if context.formalization_cost <= policy.max_cost_for_formal:
        return CrystallizationAssessment(
            freeze_posture=FreezePosture.FREEZE,
            formalization_fit=FormalizationFit.OPTIONAL,
            reasons=("formalization is plausible but not required by epistemic risk",),
        )

    return CrystallizationAssessment(
        freeze_posture=FreezePosture.FREEZE,
        formalization_fit=FormalizationFit.EMPIRICAL,
        reasons=("formalization cost exceeds the declared budget; retain empirical evidence",),
    )


def recommend_evidence_method(
    shape: ClaimShape,
    assessment: CrystallizationAssessment,
) -> EvidenceMethod:
    """Choose a claim-shaped evidence method without pretending one method dominates all others."""

    if assessment.formalization_fit == FormalizationFit.DEFER:
        return {
            ClaimShape.TRANSITION_SYSTEM: EvidenceMethod.BOUNDED_EXHAUSTIVE,
            ClaimShape.PURE_FUNCTION: EvidenceMethod.TEST,
            ClaimShape.OPEN_WORLD: EvidenceMethod.FUZZ,
            ClaimShape.PERFORMANCE: EvidenceMethod.BENCHMARK,
            ClaimShape.EXTERNAL_INTEGRATION: EvidenceMethod.OBSERVATION,
            ClaimShape.HEURISTIC: EvidenceMethod.FUZZ,
        }[shape]
    if shape == ClaimShape.TRANSITION_SYSTEM:
        if assessment.formalization_fit == FormalizationFit.FORMAL:
            return EvidenceMethod.MODEL_CHECK
        return EvidenceMethod.BOUNDED_EXHAUSTIVE
    if shape == ClaimShape.PURE_FUNCTION:
        if assessment.formalization_fit == FormalizationFit.FORMAL:
            return EvidenceMethod.THEOREM
        if assessment.formalization_fit == FormalizationFit.OPTIONAL:
            return EvidenceMethod.STATIC_ANALYSIS
        return EvidenceMethod.TEST
    if shape == ClaimShape.OPEN_WORLD:
        return EvidenceMethod.FUZZ
    if shape == ClaimShape.PERFORMANCE:
        return EvidenceMethod.BENCHMARK
    if shape == ClaimShape.EXTERNAL_INTEGRATION:
        return EvidenceMethod.OBSERVATION
    return EvidenceMethod.FUZZ
