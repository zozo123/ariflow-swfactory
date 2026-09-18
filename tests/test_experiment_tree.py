"""Experiment-tree semantics for bounded candidate campaigns."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory.evolution import (
    CandidateOutcome,
    CandidateRequest,
    Strategy,
    evaluation,
    plan_requests,
    run_campaign,
    select,
)
from swfactory.experiment_tree import ExperimentTreeError, NodeState, render, stack_rounds
from swfactory.cli import app
from swfactory.generations import Dimension


def _passing_outcome(request: CandidateRequest, head: str) -> CandidateOutcome:
    return CandidateOutcome(
        logical_id=request.logical_id,
        strategy=request.strategy,
        state="ok",
        input_head=request.input_head,
        output_head=head,
        evaluations=(
            evaluation(Dimension.CORRECTNESS, passed=True, evidence="tests"),
            evaluation(Dimension.EVIDENCE, passed=True, evidence="report"),
        ),
    )


def test_selection_requires_a_distinct_recorded_revision() -> None:
    request = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR,),
    )[0]
    missing = _passing_outcome(request, "")
    missing = CandidateOutcome(
        logical_id=missing.logical_id,
        strategy=missing.strategy,
        state=missing.state,
        input_head=missing.input_head,
        output_head=None,
        evaluations=missing.evaluations,
    )
    unchanged = _passing_outcome(request, "base")

    missing_selection = select((missing,), human_approved=True)
    unchanged_selection = select((unchanged,), human_approved=True)

    assert missing_selection.winner is None
    assert missing_selection.refusals == (f"{request.logical_id}: no_output_head",)
    assert unchanged_selection.winner is None
    assert unchanged_selection.refusals == (f"{request.logical_id}: unchanged_output_head",)


def test_campaign_report_freezes_answered_nodes_but_not_runner_failures() -> None:
    requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR, Strategy.SCRATCH),
    )

    def runner(request: CandidateRequest) -> CandidateOutcome:
        if request.strategy is Strategy.SCRATCH:
            raise MemoryError("sandbox died")
        return _passing_outcome(request, "sha-repair")

    report = run_campaign(runner, requests, human_approved=True)
    assert report.experiment_round is not None
    by_strategy = {node.strategy: node for node in report.experiment_round.nodes}

    assert by_strategy["repair"].state == NodeState.ANSWERED
    assert by_strategy["repair"].frozen is True
    assert by_strategy["repair"].recorded_head == "sha-repair"
    assert by_strategy["repair"].selected is True

    assert by_strategy["scratch"].state == NodeState.PROVISIONAL
    assert by_strategy["scratch"].frozen is False
    assert by_strategy["scratch"].recorded_head is None
    assert by_strategy["scratch"].selected is False


def test_stacked_bushes_descend_from_the_previous_winner_exact_head() -> None:
    first_requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR,),
    )
    first = run_campaign(
        lambda request: _passing_outcome(request, "sha-round-0"),
        first_requests,
        human_approved=True,
    )
    assert first.experiment_round is not None
    winner = first.selection.winner
    assert winner is not None

    second_requests = plan_requests(
        campaign_id="round-1",
        cell_id="cell",
        epoch=1,
        input_head="sha-round-0",
        strategies=(Strategy.RETHINK, Strategy.SCRATCH),
        parent_candidate=winner,
        depth=1,
    )

    def second_runner(request: CandidateRequest) -> CandidateOutcome:
        return _passing_outcome(request, f"sha-{request.strategy.value}")

    second = run_campaign(second_runner, second_requests, human_approved=True)
    assert second.experiment_round is not None

    tree = stack_rounds((first.experiment_round, second.experiment_round))
    text = render(tree)

    assert tree.root_head == "base"
    assert tree.rounds[1].parent_candidate == winner
    assert tree.rounds[1].input_head == "sha-round-0"
    assert "round 0" in text
    assert "round 1" in text
    assert "frozen" in text


def test_stacked_bushes_refuse_a_second_round_from_the_wrong_parent() -> None:
    first_requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR,),
    )
    first = run_campaign(
        lambda request: _passing_outcome(request, "sha-round-0"),
        first_requests,
        human_approved=True,
    )
    assert first.experiment_round is not None

    wrong_requests = plan_requests(
        campaign_id="round-1",
        cell_id="cell",
        epoch=1,
        input_head="sha-round-0",
        strategies=(Strategy.RETHINK,),
        parent_candidate="cand_wrong",
        depth=1,
    )
    wrong = run_campaign(
        lambda request: _passing_outcome(request, "sha-round-1"),
        wrong_requests,
        human_approved=True,
    )
    assert wrong.experiment_round is not None

    with pytest.raises(ExperimentTreeError, match="parent must be previous winner"):
        stack_rounds((first.experiment_round, wrong.experiment_round))


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
    first_requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR,),
    )
    first = run_campaign(
        lambda request: _passing_outcome(request, "sha-round-0"),
        first_requests,
        human_approved=True,
    )
    assert first.selection.winner is not None

    second_requests = plan_requests(
        campaign_id="round-1",
        cell_id="cell",
        epoch=1,
        input_head="sha-round-0",
        strategies=(Strategy.SCRATCH,),
        parent_candidate=first.selection.winner,
        depth=1,
    )
    second = run_campaign(
        lambda request: _passing_outcome(request, "sha-round-1"),
        second_requests,
        human_approved=True,
    )

    paths = []
    for index, report in enumerate((first, second)):
        path = tmp_path / f"round-{index}.json"
        path.write_text(json.dumps(report.to_dict()), encoding="utf-8")
        paths.append(path)

    result = CliRunner().invoke(app, ["experiment-tree", *(str(path) for path in paths)])

    assert result.exit_code == 0, result.output
    assert "round 0  round-0" in result.output
    assert "round 1  round-1" in result.output
    assert "sha-round-1" in result.output
