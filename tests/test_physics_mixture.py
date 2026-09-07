from __future__ import annotations

import math

from swfactory.physics_mixture import (
    ControlAction,
    Current,
    MatterPhase,
    PhysicsSample,
    evaluate_mixture,
    jarzynski_delta_free_energy,
)


def sample(**overrides: object) -> PhysicsSample:
    values: dict[str, object] = {
        "cell_id": "cell_test",
        "epoch": 1,
        "currents": (Current("work", flux=0.1, velocity=0.2, gradient=0.1),),
        "entropy": 0.2,
        "entropy_ceiling": 1.0,
        "order_parameter": 0.4,
        "occupancy": 1,
        "capacity": 8,
    }
    values.update(overrides)
    return PhysicsSample(**values)  # type: ignore[arg-type]


def test_non_airflow_authority_is_refused() -> None:
    decision = evaluate_mixture(sample(scheduler="other"))
    assert decision.action == ControlAction.REFUSE
    assert "non-Airflow" in decision.reason


def test_supercritical_retry_branching_is_shed() -> None:
    decision = evaluate_mixture(sample(retry_branching_ratio=1.3))
    assert decision.action == ControlAction.SHED
    assert decision.phase == MatterPhase.SUPERCRITICAL


def test_exclusive_occupancy_fences_over_capacity() -> None:
    decision = evaluate_mixture(sample(occupancy=3, capacity=2))
    assert decision.action == ControlAction.FENCE


def test_crystal_candidate_is_held_until_release_gate() -> None:
    candidate = sample(
        entropy=0.01,
        order_parameter=0.99,
        nucleation_barrier=0.01,
        occupancy=0,
        cooperative_gain=0.0,
        stoichiometric_stress=0.0,
    )
    held = evaluate_mixture(candidate)
    opened = evaluate_mixture(candidate, release_gate_open=True)
    assert held.action == ControlAction.HOLD
    assert held.phase == MatterPhase.CRYSTAL_CANDIDATE
    assert opened.action == ControlAction.CRYSTALLIZE_CANDIDATE


def test_many_body_cooperative_overload_holds() -> None:
    decision = evaluate_mixture(
        sample(
            cooperative_gain=1.0,
            condensate_fraction=1.0,
            stoichiometric_stress=0.5,
            degradation_rate=0.1,
        )
    )
    assert decision.action == ControlAction.HOLD


def test_jarzynski_estimator_is_stable_for_large_work_values() -> None:
    estimate = jarzynski_delta_free_energy((1000.0, 1001.0, 1002.0))
    assert estimate is not None
    assert math.isfinite(estimate)
    assert 999.0 < estimate < 1002.0
