from __future__ import annotations

import json
from pathlib import Path

import pytest

from swfactory.phase_control import (
    PHASE_CONTROL_AUTHORITY,
    ControlMode,
    PhaseObservation,
    assess,
    branching_ratio,
    normalized_entropy,
)


def _fixture() -> dict[str, object]:
    path = Path(__file__).parent / "fixtures" / "contract" / "phase_control.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_cross_language_phase_fixture_is_canonical() -> None:
    data = _fixture()
    assert data["schema_version"] == 1
    for case in data["cases"]:
        observation = PhaseObservation(**case["observation"])
        assessment = assess(observation, previous_phase=case.get("previous_phase"))
        assert assessment.raw_phase == case["expected"]["raw_phase"], case["name"]
        assert assessment.phase == case["expected"]["phase"], case["name"]
        assert assessment.recommendation.mode == ControlMode(case["expected"]["mode"]), case["name"]
        assert assessment.authority == PHASE_CONTROL_AUTHORITY


def test_recommendations_shape_search_but_never_claim_authority() -> None:
    crystal = PhaseObservation(
        candidate_entropy=0.1,
        coherence=0.95,
        mobility=0.2,
        queue_pressure=0.1,
        queue_acceleration=-0.1,
        resource_pressure=0.1,
        branching_ratio=0.2,
        evidence_completeness=0.96,
        context_pressure=0.4,
        debt_pressure=0.05,
        verifier_disagreement=0.05,
    )
    result = assess(crystal)
    assert result.phase == "crystal"
    assert result.recommendation.mode == ControlMode.VERIFY
    assert result.recommendation.allow_new_implementation_lanes is False
    assert result.recommendation.spawn == "stop"
    assert result.recommendation.attention == "authority-boundary"
    assert result.authority == "search-only"


def test_entropy_and_branching_helpers_are_bounded_and_interpretable() -> None:
    assert normalized_entropy([1.0]) == 0.0
    assert normalized_entropy([1.0, 1.0, 1.0]) == pytest.approx(1.0)
    assert normalized_entropy([9.0, 1.0]) < normalized_entropy([1.0, 1.0])
    assert branching_ratio(children_or_retries=0, terminal_parents=0) == 0.0
    assert branching_ratio(children_or_retries=6, terminal_parents=3) == 2.0
    assert branching_ratio(children_or_retries=10, terminal_parents=0) == 4.0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("candidate_entropy", 1.1),
        ("coherence", -0.1),
        ("queue_acceleration", 1.1),
        ("branching_ratio", 4.1),
    ],
)
def test_observation_refuses_out_of_contract_values(field: str, value: float) -> None:
    values = {
        "candidate_entropy": 0.5,
        "coherence": 0.5,
        "mobility": 0.5,
        "queue_pressure": 0.5,
        "queue_acceleration": 0.0,
        "resource_pressure": 0.5,
        "branching_ratio": 0.5,
        "evidence_completeness": 0.5,
        "context_pressure": 0.5,
        "debt_pressure": 0.5,
        "verifier_disagreement": 0.5,
    }
    values[field] = value
    with pytest.raises(ValueError):
        assess(PhaseObservation(**values))


def test_jam_and_glass_are_not_mistaken_for_success() -> None:
    cases = {case["name"]: case for case in _fixture()["cases"]}
    glass = assess(PhaseObservation(**cases["glass-local-minimum"]["observation"]))
    jammed = assess(PhaseObservation(**cases["jammed-drain"]["observation"]))
    assert glass.phase == "glass"
    assert glass.recommendation.context == "fresh"
    assert glass.recommendation.candidates == "reset"
    assert jammed.phase == "jammed"
    assert jammed.recommendation.queue == "drain"
    assert jammed.recommendation.allow_new_implementation_lanes is False
