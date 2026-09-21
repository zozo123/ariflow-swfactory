from __future__ import annotations

import pytest

from swfactory.cognitive_harness import (
    AuthorityRequest,
    CognitiveAttention,
    CognitiveLayer,
    EnsembleMember,
    ImprovementDisposition,
    MeasurementKind,
    MeasurementReceipt,
    MemoryEvidence,
    MemoryPhase,
    QuantityKind,
    SelfImprovementExperiment,
    StochasticField,
    WorldCandidate,
    classify_self_improvement,
    ensemble_diversity,
    gauge_fix,
    make_receipt,
    memory_phase,
    pairwise_correlation,
    plan_cognition,
    quantity_kind,
    validate_measurement_set,
)
from swfactory.phase_control import PhaseObservation, assess


def _world(world_id: str, *, model: str = "m1", candidate: str = "cand") -> WorldCandidate:
    return WorldCandidate(
        world_id=world_id,
        trajectory_id=f"t-{world_id}",
        model=model,
        runtime="sandbox",
        role="builder",
        candidate_digest=candidate,
        source_digest="src",
        recipe_digest="recipe",
        policy_digest="policy",
        evidence_digest="evidence",
        verified=True,
    )


def _crystal():
    return assess(
        PhaseObservation(
            candidate_entropy=0.10,
            coherence=0.95,
            mobility=0.20,
            queue_pressure=0.10,
            queue_acceleration=-0.10,
            resource_pressure=0.10,
            branching_ratio=0.20,
            evidence_completeness=0.96,
            context_pressure=0.40,
            debt_pressure=0.05,
            verifier_disagreement=0.05,
        )
    )


def _gas():
    return assess(
        PhaseObservation(
            candidate_entropy=0.90,
            coherence=0.20,
            mobility=0.90,
            queue_pressure=0.10,
            queue_acceleration=0.0,
            resource_pressure=0.10,
            branching_ratio=0.30,
            evidence_completeness=0.25,
            context_pressure=0.40,
            debt_pressure=0.10,
            verifier_disagreement=0.20,
        )
    )


def test_gauge_boundary_distinguishes_representation_from_observable_identity() -> None:
    assert quantity_kind("prompt") == QuantityKind.GAUGE_DEPENDENT
    assert quantity_kind("candidate_digest") == QuantityKind.GAUGE_INVARIANT
    assert quantity_kind("confidence") == QuantityKind.UNKNOWN


def test_gauge_fix_collapses_equivalent_worlds_without_merging_distinct_candidates() -> None:
    left = _world("b", model="model-a")
    right = _world("a", model="model-b")
    distinct = _world("c", model="model-a", candidate="other")

    classes = gauge_fix([left, right, distinct])

    assert len(classes) == 2
    equivalent = next(item for item in classes if len(item.member_world_ids) == 2)
    assert equivalent.canonical_world_id == "a"
    assert equivalent.member_world_ids == ("a", "b")


def test_stochastic_field_can_weight_declared_search_but_not_add_choices() -> None:
    field = StochasticField(source="jev", probabilities={"repair": 1.0, "scratch": 3.0})

    assert field.normalized(["repair", "scratch"]) == {"repair": 0.25, "scratch": 0.75}
    with pytest.raises(ValueError, match="undeclared"):
        field.normalized(["repair"])


def test_ensemble_values_independent_behavior_not_agent_count() -> None:
    a = EnsembleMember("a", "m1", "builder", "r1", ("same", "repair"))
    b = EnsembleMember("b", "m2", "critic", "r2", ("same", "security"))
    clone = EnsembleMember("clone", "m3", "builder", "r3", ("same", "repair"))

    assert pairwise_correlation(a, clone).value == 1.0
    assert pairwise_correlation(a, b).value < 1.0
    assert ensemble_diversity([a, b]) > ensemble_diversity([a, clone])


def test_measurement_requires_exact_world_success_and_independence() -> None:
    world = _world("w")
    independent = MeasurementReceipt(
        world_id="w",
        kind=MeasurementKind.REPLAY,
        observable="tests",
        result_digest="result",
        evidence_digest="evidence",
        independent=True,
        passed=True,
    )
    self_report = MeasurementReceipt(
        world_id="w",
        kind=MeasurementKind.TEST,
        observable="tests",
        result_digest="result-2",
        evidence_digest="evidence-2",
        independent=False,
        passed=True,
    )

    assert validate_measurement_set(world, [independent, self_report]) is True
    assert validate_measurement_set(world, [self_report]) is False


def test_memory_crystallizes_only_after_repeated_uncontradicted_evidence() -> None:
    crystal = MemoryEvidence(0.90, 3, 0, 0.95, 0.20)
    contradicted = MemoryEvidence(0.90, 4, 1, 0.99, 0.20)

    assert memory_phase(crystal) == MemoryPhase.CRYSTAL
    assert memory_phase(contradicted) == MemoryPhase.CANDIDATE_BELIEF


def test_crystal_may_request_authority_but_never_grants_it() -> None:
    phase = _crystal()
    plan = plan_cognition(phase)

    assert plan.layer == CognitiveLayer.AUTHORITY_BOUNDARY
    assert plan.attention == CognitiveAttention.AUTHORITY
    assert plan.may_request_authority is True
    assert plan.authority == "search-only"
    assert plan.reality_boundary == "rust-authority-kernel"

    request = AuthorityRequest(
        cell_id="cell_" + "a" * 24,
        epoch=2,
        candidate_digest="cand",
        source_digest="src",
        recipe_digest="recipe",
        policy_digest="policy",
        evidence_digest="evidence",
        requested_effect="publish",
    )
    receipt = make_receipt(phase, [_world("w")], [], authority_request=request)
    assert receipt.authority == "search-only"
    assert receipt.authority_request_digest


def test_non_crystal_cognition_cannot_smuggle_an_authority_request() -> None:
    request = AuthorityRequest(
        cell_id="cell_" + "a" * 24,
        epoch=1,
        candidate_digest="cand",
        source_digest="src",
        recipe_digest="recipe",
        policy_digest="policy",
        evidence_digest="evidence",
        requested_effect="publish",
    )

    with pytest.raises(ValueError, match="crystal verification posture"):
        make_receipt(_gas(), [_world("w")], [], authority_request=request)


def test_self_improvement_can_become_adoptable_but_cannot_adopt_itself() -> None:
    experiment = SelfImprovementExperiment(
        experiment_id="controller-v2",
        baseline_digest="baseline",
        candidate_world_ids=("w1",),
        objective_digest="objective",
        evidence_digest="evidence",
    )

    assert (
        classify_self_improvement(experiment, evidence_complete=False, independent_verification=False)
        == ImprovementDisposition.SHADOW
    )
    assert (
        classify_self_improvement(experiment, evidence_complete=True, independent_verification=False)
        == ImprovementDisposition.CANDIDATE
    )
    assert (
        classify_self_improvement(experiment, evidence_complete=True, independent_verification=True)
        == ImprovementDisposition.ADOPTABLE
    )
    assert experiment.authority == "proposal-only"
