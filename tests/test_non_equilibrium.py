from __future__ import annotations

import math

from swfactory.non_equilibrium import (
    ControlAction,
    Current,
    FactoryState,
    Phase,
    barrier_crossing_probability,
    canonical_pitches,
    classify_phase,
    classical_nucleation_barrier,
    crooks_log_ratio,
    default_couplings,
    entropy_production,
    evaluate_factory,
    jarzynski_delta_free_energy,
    max_entropy_weights,
    mix_pitches,
    shannon_entropy,
)


def state(**overrides):
    values = {
        "currents": (
            Current("work", 0.7, 0.5, 1.0, 0.2),
            Current("evidence", 0.6, 0.5, 1.0, 0.3),
            Current("cleanup", 0.4, 0.3, 1.0, 0.4),
        ),
        "configurational_entropy": 0.5,
        "failure_fraction": 0.1,
        "blocked_fraction": 0.1,
        "cleanup_debt": 1.0,
        "evidence_gap": 0.1,
        "security_refusal_fraction": 0.05,
        "cost_pressure": 0.2,
        "effective_temperature": 1.0,
    }
    values.update(overrides)
    return FactoryState(**values)


def test_shannon_entropy_is_maximal_for_uniform_distribution() -> None:
    uniform = shannon_entropy([1, 1, 1, 1], normalize=True)
    concentrated = shannon_entropy([1, 0, 0, 0], normalize=True)
    assert math.isclose(uniform, 1.0)
    assert concentrated == 0.0


def test_gibbs_weights_preserve_diversity_and_prefer_lower_cost() -> None:
    weights = max_entropy_weights({"a": 0.0, "b": 1.0, "c": 2.0}, beta=2.0)
    assert math.isclose(sum(weights.values()), 1.0)
    assert weights["a"] > weights["b"] > weights["c"] > 0.0


def test_current_acceleration_and_entropy_production_are_vector_quantities() -> None:
    currents = (
        Current("work", 3.0, 1.0, 2.0, 0.5),
        Current("cleanup", -1.0, -0.5, 1.0, -0.25),
    )
    assert currents[0].acceleration == 1.0
    assert currents[1].acceleration == -0.5
    assert entropy_production(currents) == 1.75


def test_phase_classifier_distinguishes_crystal_glass_and_jammed() -> None:
    crystal = classify_phase(
        state(
            configurational_entropy=0.1,
            failure_fraction=0.02,
            blocked_fraction=0.02,
            evidence_gap=0.01,
            security_refusal_fraction=0.01,
        )
    )
    assert crystal.phase is Phase.CRYSTAL

    glass = classify_phase(
        state(
            currents=(Current("work", 0.02, 0.02),),
            configurational_entropy=0.55,
            failure_fraction=0.45,
            blocked_fraction=0.35,
            evidence_gap=0.4,
            security_refusal_fraction=0.2,
        )
    )
    assert glass.phase is Phase.GLASS

    jammed = classify_phase(state(blocked_fraction=0.8, cleanup_debt=9.0))
    assert jammed.phase is Phase.JAMMED


def test_hysteresis_prevents_marginal_liquid_crystal_flip() -> None:
    candidate = state(
        configurational_entropy=0.30,
        failure_fraction=0.10,
        blocked_fraction=0.10,
        evidence_gap=0.10,
        security_refusal_fraction=0.10,
    )
    decision = classify_phase(candidate, previous=Phase.LIQUID)
    if decision.phase is Phase.CRYSTAL:
        assert abs(decision.order_parameter - 0.65) >= 0.08


def test_nucleation_barrier_and_kramers_probability_move_in_expected_direction() -> None:
    high_barrier = classical_nucleation_barrier(surface_penalty=2.0, driving_force=1.0)
    low_barrier = classical_nucleation_barrier(surface_penalty=1.0, driving_force=2.0)
    assert high_barrier > low_barrier
    assert barrier_crossing_probability(barrier=high_barrier, effective_temperature=1.0) < barrier_crossing_probability(
        barrier=low_barrier,
        effective_temperature=1.0,
    )


def test_jarzynski_constant_work_returns_that_work() -> None:
    estimate = jarzynski_delta_free_energy([3.0, 3.0, 3.0], beta=0.7)
    assert math.isclose(estimate, 3.0, rel_tol=1e-12)
    assert math.isclose(crooks_log_ratio(work=3.0, delta_free_energy=3.0, beta=0.7), 0.0)


def test_all_canonical_models_participate_in_the_mixture() -> None:
    value = state()
    phase = classify_phase(value)
    pitches = canonical_pitches(value)
    result = mix_pitches(
        pitches,
        phase=phase,
        couplings=default_couplings(),
        effective_temperature=value.effective_temperature,
    )
    assert {name for name, _ in result.model_weights} == {pitch.model for pitch in pitches}
    assert all(weight > 0.0 for _, weight in result.model_weights)
    assert math.isclose(sum(weight for _, weight in result.model_weights), 1.0)
    assert math.isclose(sum(probability for _, probability in result.action_probabilities), 1.0)


def test_recovery_pressure_changes_ensemble_toward_recovery_or_cleanup() -> None:
    healthy = evaluate_factory(state())
    degraded = evaluate_factory(
        state(
            failure_fraction=0.9,
            blocked_fraction=0.8,
            cleanup_debt=9.0,
            evidence_gap=0.6,
        )
    )
    healthy_probs = dict(healthy.action_probabilities)
    degraded_probs = dict(degraded.action_probabilities)
    assert degraded_probs[ControlAction.RECOVER] > healthy_probs[ControlAction.RECOVER]
    assert degraded_probs[ControlAction.CLEANUP] > healthy_probs[ControlAction.CLEANUP]


def test_evaluate_factory_retains_physics_diagnostics() -> None:
    value = state()
    result = evaluate_factory(value)
    assert result.phase.phase in set(Phase)
    assert 0.0 <= result.disagreement_entropy <= 1.0
    assert math.isclose(result.entropy_production, value.entropy_production)
