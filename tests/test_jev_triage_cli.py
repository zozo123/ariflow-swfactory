from __future__ import annotations

import json

from typer.testing import CliRunner

from swfactory.cli import app


def test_triage_shadow_cli_is_read_only_and_keeps_acceptance_criteria_visible(tmp_path) -> None:
    proposal = tmp_path / "proposal.json"
    existing = tmp_path / "existing.json"
    advisories = tmp_path / "advisories.json"
    proposal.write_text(
        json.dumps(
            {
                "key": "proposal",
                "title": "Reject stale execution requests before a stage handler runs",
                "acceptance_criteria": ["stale request returns before handler invocation"],
            }
        ),
        encoding="utf-8",
    )
    existing.write_text(
        json.dumps(
            [
                {
                    "key": "#2228",
                    "title": "Port CellStore epoch fencing into Rust",
                    "acceptance_criteria": ["Rust rejects stale cell epochs"],
                },
                {
                    "key": "#2255",
                    "title": "Require a valid fence before invoking a stage handler",
                    "acceptance_criteria": ["stale fence keeps handler call count at zero"],
                },
            ]
        ),
        encoding="utf-8",
    )
    advisories.write_text(
        json.dumps(
            {
                "#2255": {
                    "existing_issue": 2255,
                    "relationship": "potential_duplicate",
                    "confidence": 0.82,
                    "probability": 0.80,
                    "needs_review": True,
                    "rationale": "compare done-conditions before filing",
                }
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        ["triage-shadow", str(proposal), str(existing), "--advisories", str(advisories), "--json"],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["proposal_preserved"] is True
    assert document["operator_decision_required"] is True
    assert document["authority"] == "none"
    assert {row["issue"] for row in document["related"]} == {"#2228", "#2255"}
    row = next(value for value in document["related"] if value["issue"] == "#2255")
    assert row["advisory"]["relationship"] == "potential_duplicate"
    assert row["acceptance_criteria"] == ["stale fence keeps handler call count at zero"]


def test_triage_shadow_cli_forces_review_when_acceptance_criteria_are_missing(tmp_path) -> None:
    proposal = tmp_path / "proposal.json"
    existing = tmp_path / "existing.json"
    advisories = tmp_path / "advisories.json"
    proposal.write_text(json.dumps({"key": "proposal", "title": "Fix the flaky upload"}), encoding="utf-8")
    existing.write_text(json.dumps([{"key": "#77", "title": "Make upload reliable"}]), encoding="utf-8")
    advisories.write_text(
        json.dumps(
            {
                "#77": {
                    "existing_issue": 77,
                    "relationship": "potential_duplicate",
                    "confidence": 0.93,
                    "probability": 0.94,
                }
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        ["triage-shadow", str(proposal), str(existing), "--advisories", str(advisories), "--json"],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["proposal"]["acceptance_criteria_missing"] is True
    assert document["related"][0]["advisory"]["needs_review"] is True
    assert document["proposal_preserved"] is True
