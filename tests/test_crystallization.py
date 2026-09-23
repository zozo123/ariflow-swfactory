from __future__ import annotations

from swfactory.crystallization import (
    ClaimShape,
    CrystallizationContext,
    FormalizationFit,
    FreezePosture,
    assess_crystallization,
    recommend_evidence_method,
)
from swfactory.formal_claims import EvidenceMethod


def _context(**overrides: float) -> CrystallizationContext:
    values = {
        "candidate_entropy": 0.15,
        "specification_stability": 0.90,
        "abstraction_fidelity": 0.85,
        "evidence_coverage": 0.90,
        "verifier_disagreement": 0.10,
        "environment_volatility": 0.20,
        "change_velocity": 0.10,
        "consequence_of_error": 0.85,
        "formalization_cost": 0.40,
    }
    values.update(overrides)
    return CrystallizationContext(**values)


def test_unstable_specification_should_not_crystallize() -> None:
    assessment = assess_crystallization(_context(specification_stability=0.35))

    assert assessment.freeze_posture == FreezePosture.KEEP_LIQUID
    assert assessment.formalization_fit == FormalizationFit.DEFER
    assert assessment.should_freeze is False
    assert "specification is still moving" in assessment.reasons


def test_high_candidate_entropy_should_stay_liquid_even_if_formal_model_is_good() -> None:
    assessment = assess_crystallization(_context(candidate_entropy=0.75))

    assert assessment.freeze_posture == FreezePosture.KEEP_LIQUID
    assert assessment.formalization_fit == FormalizationFit.DEFER


def test_disagreement_means_measure_before_freezing() -> None:
    assessment = assess_crystallization(_context(verifier_disagreement=0.55))

    assert assessment.freeze_posture == FreezePosture.MEASURE
    assert assessment.formalization_fit == FormalizationFit.DEFER
    assert assessment.should_freeze is False


def test_weak_abstraction_prefers_empirical_evidence_over_decorative_proof() -> None:
    assessment = assess_crystallization(_context(abstraction_fidelity=0.40))

    assert assessment.freeze_posture == FreezePosture.FREEZE
    assert assessment.formalization_fit == FormalizationFit.EMPIRICAL
    assert assessment.should_formalize is False
    assert recommend_evidence_method(ClaimShape.OPEN_WORLD, assessment) == EvidenceMethod.FUZZ


def test_high_consequence_finite_state_claim_is_a_good_formalization_target() -> None:
    assessment = assess_crystallization(_context())

    assert assessment.freeze_posture == FreezePosture.FREEZE
    assert assessment.formalization_fit == FormalizationFit.FORMAL
    assert assessment.should_formalize is True
    assert (
        recommend_evidence_method(ClaimShape.TRANSITION_SYSTEM, assessment)
        == EvidenceMethod.MODEL_CHECK
    )
    assert recommend_evidence_method(ClaimShape.PURE_FUNCTION, assessment) == EvidenceMethod.THEOREM


def test_low_consequence_claim_can_keep_formalization_optional() -> None:
    assessment = assess_crystallization(_context(consequence_of_error=0.30))

    assert assessment.freeze_posture == FreezePosture.FREEZE
    assert assessment.formalization_fit == FormalizationFit.OPTIONAL
    assert assessment.should_formalize is False
    assert (
        recommend_evidence_method(ClaimShape.PURE_FUNCTION, assessment)
        == EvidenceMethod.STATIC_ANALYSIS
    )


def test_open_world_and_performance_claims_keep_property_specific_methods() -> None:
    assessment = assess_crystallization(_context())

    assert recommend_evidence_method(ClaimShape.OPEN_WORLD, assessment) == EvidenceMethod.FUZZ
    assert recommend_evidence_method(ClaimShape.PERFORMANCE, assessment) == EvidenceMethod.BENCHMARK
    assert (
        recommend_evidence_method(ClaimShape.EXTERNAL_INTEGRATION, assessment)
        == EvidenceMethod.OBSERVATION
    )
