"""Exploration-width cooling for stacked campaigns, without self-promotion."""

from __future__ import annotations

import json

from typer.testing import CliRunner

from swfactory.cli import app
from swfactory.research_loop import annealed_strategy_schedule


def test_default_schedule_cools_from_broad_search_to_single_strategy() -> None:
    schedule = annealed_strategy_schedule(3, max_candidates=4)

    assert [tuple(item.value for item in round_) for round_ in schedule] == [
        ("repair", "rethink", "scratch"),
        ("repair", "rethink"),
        ("repair",),
        ("repair",),
    ]


def test_cli_renders_the_cooling_schedule_without_running_candidates() -> None:
    result = CliRunner().invoke(
        app,
        ["research-schedule", "--max-depth", "2", "--max-candidates", "3", "--json"],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["authority"] == "exploration-only"
    assert document["scheduler"] == "airflow"
    assert [len(round_["strategies"]) for round_ in document["rounds"]] == [3, 2, 1]
