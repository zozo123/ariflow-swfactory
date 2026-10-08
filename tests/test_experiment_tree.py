"""Experiment-tree semantics for bounded candidate campaigns."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory.cli import app
from swfactory.evolution import CampaignError, Strategy, plan_requests
from swfactory.experiment_tree import (
    ExperimentNode,
    ExperimentRound,
    ExperimentTreeError,
    NodeState,
    render,
    render_mermaid,
    stack_rounds,
)


def _round(round_id: str, input_head: str, head: str, *, depth: int = 0, parent: str | None = None) -> ExperimentRound:
    node = ExperimentNode(
        id=f"cand-{round_id}",
        parent_id=parent,
        depth=depth,
        strategy="repair",
        input_head=input_head,
        result="ok",
        state=NodeState.ANSWERED,
        recorded_head=head,
        selected=True,
    )
    return ExperimentRound(round_id, input_head, depth, parent, (node,), winner_id=node.id)


def test_stacked_bushes_descend_from_the_previous_winner_exact_head() -> None:
    first = _round("round-0", "base", "sha-round-0")
    second = _round("round-1", "sha-round-0", "sha-round-1", depth=1, parent=first.winner_id)

    tree = stack_rounds((first, second))
    text = render(tree)

    assert tree.root_head == "base"
    assert tree.rounds[1].parent_candidate == first.winner_id
    assert tree.rounds[1].input_head == "sha-round-0"
    assert "round 0" in text
    assert "round 1" in text
    assert "frozen" in text


def test_stacked_bushes_refuse_a_second_round_from_the_wrong_parent() -> None:
    first = _round("round-0", "base", "sha-round-0")
    wrong = _round("round-1", "sha-round-0", "sha-round-1", depth=1, parent="cand_wrong")

    with pytest.raises(ExperimentTreeError, match="parent must be previous winner"):
        stack_rounds((first, wrong))


def test_planner_refuses_descendant_without_a_selected_parent() -> None:
    with pytest.raises(CampaignError, match="requires the previous winner"):
        plan_requests(
            campaign_id="round-1",
            cell_id="cell",
            epoch=1,
            input_head="sha-parent",
            strategies=(Strategy.REPAIR,),
            depth=1,
        )


def test_cli_renders_stored_campaign_reports(tmp_path: Path) -> None:
    first = _round("round-0", "base", "sha-round-0")
    second = _round("round-1", "sha-round-0", "sha-round-1", depth=1, parent=first.winner_id)

    paths = []
    for index, round_ in enumerate((first, second)):
        path = tmp_path / f"round-{index}.json"
        path.write_text(json.dumps({"experiment_round": round_.to_dict()}), encoding="utf-8")
        paths.append(path)

    result = CliRunner().invoke(app, ["experiment-tree", *(str(path) for path in paths)])

    assert result.exit_code == 0, result.output
    assert "round 0  round-0" in result.output
    assert "round 1  round-1" in result.output
    assert "sha-round-1" in result.output

    mermaid = render_mermaid(stack_rounds((first, second)))
    assert mermaid.startswith("flowchart TD")
    assert "root --> n0_0" in mermaid
    assert "n0_0 --> n1_0" in mermaid
    assert "class n0_0,n1_0 selected" in mermaid

    mermaid_result = CliRunner().invoke(
        app,
        ["experiment-tree", *(str(path) for path in paths), "--mermaid"],
    )
    assert mermaid_result.exit_code == 0, mermaid_result.output
    assert mermaid_result.output.startswith("flowchart TD")
