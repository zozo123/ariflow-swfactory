from __future__ import annotations

from swfactory.phase_control import PhaseObservation, assess
from swfactory.swarm_dynamics import (
    AgentRole,
    CandidateCrystal,
    ComputeTier,
    DisagreementHotspot,
    SwarmBudget,
    SwarmObservation,
    allocate_population,
    effective_independent_search,
    prioritize_hotspots,
)


def _observation(**overrides: float) -> SwarmObservation:
    values = {
        "effective_independent_search": 3.0,
        "mean_correlation": 0.2,
        "novelty": 0.8,
        "verifier_disagreement": 0.3,
        "evidence_completeness": 0.5,
        "resource_pressure": 0.2,
        "context_pressure": 0.2,
        "branching_ratio": 0.5,
        "progress_rate": 0.6,
        "expected_information": 0.7,
    }
    values.update(overrides)
    return SwarmObservation(**values)


def _phase(**overrides: float):
    values = {
        "candidate_entropy": 0.9,
        "coherence": 0.2,
        "mobility": 0.9,
        "queue_pressure": 0.1,
        "queue_acceleration": 0.0,
        "resource_pressure": 0.1,
        "branching_ratio": 0.5,
        "evidence_completeness": 0.2,
        "context_pressure": 0.1,
        "debt_pressure": 0.1,
        "verifier_disagreement": 0.7,
    }
    values.update(overrides)
    return assess(PhaseObservation(**values))


def test_duplicate_agents_count_as_one_effective_search_lane() -> None:
    duplicate = effective_independent_search((("a", "b"), ("a", "b"), ("a", "b")))
    diverse = effective_independent_search((("a",), ("b",), ("c",)))

    assert duplicate == 1.0
    assert diverse == 3.0


def test_disagreement_hotspots_are_ranked_by_information_value() -> None:
    low = DisagreementHotspot("low", "sha256:low", 0.2, 0.4, 0.5)
    high = DisagreementHotspot("high", "sha256:high", 0.9, 0.9, 1.0)

    assert prioritize_hotspots((low, high), limit=1) == (high,)


def test_gas_spends_population_on_cheap_diverse_search() -> None:
    assessment = _phase()
    assert assessment.recommendation.mode.value == "diverge"

    plan = allocate_population(
        assessment,
        _observation(),
        budget=SwarmBudget(max_agents=12, max_parallel=8, max_deep_agents=2, max_exact_replays=1),
    )

    assert plan.authority == "search-only"
    assert any(lane.role == AgentRole.EXPLORER for lane in plan.lanes)
    assert all(
        lane.compute_tier in {ComputeTier.CHEAP, ComputeTier.STANDARD}
        for lane in plan.lanes
    )


def test_crystal_requires_exact_identity_but_grants_no_authority() -> None:
    crystal = CandidateCrystal(
        candidate_digest="sha256:candidate",
        source_digest="sha256:source",
        recipe_digest="sha256:recipe",
        policy_digest="sha256:policy",
        evidence_digest="sha256:evidence",
        coherence=0.98,
        evidence_completeness=0.99,
        verifier_disagreement=0.02,
        independent_verifiers=3,
    )

    assert crystal.verify_ready is True
    assert crystal.authority_request_ready is True
    assert crystal.invariant_digest().startswith("sha256:")


def test_jammed_phase_stops_new_exploration() -> None:
    assessment = _phase(
        queue_pressure=0.95,
        resource_pressure=0.95,
        debt_pressure=0.95,
        candidate_entropy=0.4,
        verifier_disagreement=0.2,
    )
    assert assessment.recommendation.mode.value == "drain"

    plan = allocate_population(
        assessment,
        _observation(resource_pressure=0.95),
        budget=SwarmBudget(max_agents=8, max_parallel=4, max_deep_agents=2, max_exact_replays=1),
    )

    assert plan.stop_new_work is True
    assert not any(
        lane.role in {AgentRole.EXPLORER, AgentRole.MUTATOR} and lane.count
        for lane in plan.lanes
    )


def test_effective_independent_search_is_a_count_not_a_probability() -> None:
    observation = _observation(effective_independent_search=7.5)

    observation.validate()

    assert observation.effective_independent_search == 7.5


def test_low_effective_independence_buys_diversity_not_duplicate_agents() -> None:
    assessment = _phase()
    budget = SwarmBudget(
        max_agents=12,
        max_parallel=8,
        max_deep_agents=2,
        max_exact_replays=1,
    )
    collapsed = allocate_population(
        assessment,
        _observation(
            effective_independent_search=1.0,
            mean_correlation=0.0,
            novelty=1.0,
            resource_pressure=0.0,
        ),
        budget=budget,
    )
    independent = allocate_population(
        assessment,
        _observation(
            effective_independent_search=12.0,
            mean_correlation=0.0,
            novelty=1.0,
            resource_pressure=0.0,
        ),
        budget=budget,
    )

    def search_width(plan) -> int:
        return sum(
            lane.count
            for lane in plan.lanes
            if lane.role in {AgentRole.EXPLORER, AgentRole.MUTATOR}
        )

    assert search_width(collapsed) > search_width(independent)


def test_resource_pressure_narrows_the_population_before_jam() -> None:
    assessment = _phase()
    budget = SwarmBudget(
        max_agents=12,
        max_parallel=8,
        max_deep_agents=2,
        max_exact_replays=1,
    )
    relaxed = allocate_population(
        assessment,
        _observation(resource_pressure=0.0),
        budget=budget,
    )
    pressured = allocate_population(
        assessment,
        _observation(resource_pressure=1.0),
        budget=budget,
    )

    relaxed_count = sum(lane.count for lane in relaxed.lanes)
    pressured_count = sum(lane.count for lane in pressured.lanes)

    assert pressured_count < relaxed_count
    assert pressured.estimated_compute_units < relaxed.estimated_compute_units
    assert "resource-cap=3/12" in pressured.reason
