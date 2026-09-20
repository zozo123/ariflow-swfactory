from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory.cli import app
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
        expected = case["expected"]
        assert assessment.raw_phase == expected["raw_phase"], case["name"]
        assert assessment.phase == expected["phase"], case["name"]
        assert assessment.recommendation.mode == ControlMode(expected["mode"]), case["name"]
        recommendation = assessment.recommendation.as_dict()
        for field in (
            "spawn",
            "trajectory",
            "context",
            "candidates",
            "queue",
            "verification",
            "attention",
            "allow_new_implementation_lanes",
        ):
            assert recommendation[field] == expected[field], f"{case['name']}:{field}"
        assert assessment.authority == PHASE_CONTROL_AUTHORITY
        assert assessment.as_dict()["observation"] == case["observation"]
        assert assessment.previous_phase == case.get("previous_phase")


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


def test_cli_exposes_read_only_phase_assessment(tmp_path: Path) -> None:
    case = {item["name"]: item for item in _fixture()["cases"]}["critical-freeze-and-measure"]
    path = tmp_path / "phase.json"
    path.write_text(json.dumps(case["observation"]), encoding="utf-8")

    result = CliRunner().invoke(app, ["phase-assess", str(path), "--json"])

    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["phase"] == "critical"
    assert document["recommendation"]["mode"] == "measure"
    assert document["authority"] == "search-only"


def test_high_context_pressure_recommends_compaction_without_changing_authority() -> None:
    observation = PhaseObservation(
        candidate_entropy=0.55,
        coherence=0.55,
        mobility=0.70,
        queue_pressure=0.20,
        queue_acceleration=0.0,
        resource_pressure=0.20,
        branching_ratio=0.40,
        evidence_completeness=0.60,
        context_pressure=0.95,
        debt_pressure=0.10,
        verifier_disagreement=0.15,
    )
    result = assess(observation)
    assert result.phase == "liquid"
    assert result.recommendation.mode == "coordinate"
    assert result.recommendation.context == "compact"
    assert result.authority == "search-only"
