"""Bounded autoresearch-style exploration without self-promotion."""

from __future__ import annotations

import json

from typer.testing import CliRunner

from swfactory.cli import app
from swfactory.evolution import CandidateOutcome, Strategy, evaluation
from swfactory.generations import CampaignBudget, Dimension
from swfactory.research_loop import annealed_strategy_schedule, entropy_strategy_schedule, run_annealing_loop


def _runner(request):
    cost = {
        Strategy.REPAIR: 0.1,
        Strategy.RETHINK: 0.2,
        Strategy.SCRATCH: 0.3,
    }[request.strategy]
    return CandidateOutcome(
        logical_id=request.logical_id,
        strategy=request.strategy,
        state="ok",
        input_head=request.input_head,
        output_head=f"{request.input_head}-{request.strategy.value}-{request.depth}",
        evaluations=(
            evaluation(Dimension.CORRECTNESS, passed=True, evidence="tests"),
            evaluation(Dimension.EVIDENCE, passed=True, evidence="receipt"),
        ),
        cost_usd=cost,
        candidate_ref=f"refs/swfactory/candidates/{request.logical_id}",
        evidence_bundle_path=f"/retained/{request.logical_id}",
        evidence_digest="sha256:" + "a" * 64,
    )


def test_default_schedule_cools_from_broad_search_to_single_strategy() -> None:
    schedule = annealed_strategy_schedule(3, max_candidates=4)

    assert [tuple(item.value for item in round_) for round_ in schedule] == [
        ("repair", "rethink", "scratch"),
        ("repair", "rethink"),
        ("repair",),
        ("repair",),
    ]


def test_loop_descends_from_evidence_winner_without_granting_promotion() -> None:
    report = run_annealing_loop(
        _runner,
        loop_id="research",
        cell_id="cell",
        epoch=7,
        input_head="base",
        budget=CampaignBudget(max_depth=2, max_candidates=3, max_cost_usd=10, max_wall_s=60),
        parallel=False,
        human_approved=False,
    )

    assert len(report.rounds) == 3
    assert [len(round_.outcomes) for round_ in report.rounds] == [3, 2, 1]
    assert report.stop_reason == "max_depth"

    first, second, third = report.rounds
    first_winner = next(item for item in first.outcomes if item.logical_id == first.exploration_selection.winner)
    second_winner = next(item for item in second.outcomes if item.logical_id == second.exploration_selection.winner)

    assert first.selection.winner is None
    assert "human_gate" in first.selection.refusals[0]
    assert second.input_head == first_winner.output_head
    assert second.experiment_round.parent_candidate == first_winner.logical_id
    assert third.input_head == second_winner.output_head
    assert third.experiment_round.parent_candidate == second_winner.logical_id
    assert report.exploration_winner is not None
    assert report.promotion_winner is None
    report.tree.validate()


def test_human_gate_only_changes_promotion_selection_not_exploration_path() -> None:
    kwargs = dict(
        loop_id="same-loop",
        cell_id="cell",
        epoch=1,
        input_head="base",
        budget=CampaignBudget(max_depth=1, max_candidates=3, max_cost_usd=10, max_wall_s=60),
        parallel=False,
    )
    autonomous = run_annealing_loop(_runner, human_approved=False, **kwargs)
    approved = run_annealing_loop(_runner, human_approved=True, **kwargs)

    assert [round_.exploration_selection.winner for round_ in autonomous.rounds] == [
        round_.exploration_selection.winner for round_ in approved.rounds
    ]
    assert autonomous.promotion_winner is None
    assert approved.promotion_winner is not None
    assert approved.promotion_winner.logical_id == approved.exploration_winner.logical_id


def test_global_cost_budget_stops_before_a_second_round() -> None:
    def expensive(request):
        outcome = _runner(request)
        return outcome.__class__(
            logical_id=outcome.logical_id,
            strategy=outcome.strategy,
            state=outcome.state,
            input_head=outcome.input_head,
            output_head=outcome.output_head,
            evaluations=outcome.evaluations,
            cost_usd=1.0,
            candidate_ref=outcome.candidate_ref,
            evidence_bundle_path=outcome.evidence_bundle_path,
            evidence_digest=outcome.evidence_digest,
            inherited_recipe_digest=outcome.inherited_recipe_digest,
        )

    report = run_annealing_loop(
        expensive,
        loop_id="budgeted",
        cell_id="cell",
        epoch=1,
        input_head="base",
        budget=CampaignBudget(max_depth=3, max_candidates=3, max_cost_usd=3.0, max_wall_s=60),
        parallel=False,
    )

    assert len(report.rounds) == 1
    assert report.total_cost_usd == 3.0
    assert report.stop_reason == "budget_exhausted"


def test_loop_stops_when_no_candidate_has_required_evidence() -> None:
    def missing_evidence(request):
        return CandidateOutcome(
            logical_id=request.logical_id,
            strategy=request.strategy,
            state="ok",
            input_head=request.input_head,
            output_head=f"{request.input_head}-{request.strategy.value}",
            evaluations=(evaluation(Dimension.CORRECTNESS, passed=True, evidence="tests"),),
            candidate_ref=f"refs/swfactory/candidates/{request.logical_id}",
            evidence_bundle_path=f".factory/candidate-evidence/{request.logical_id}",
            evidence_digest=f"sha256:{'b' * 64}",
        )

    report = run_annealing_loop(
        missing_evidence,
        loop_id="blocked",
        cell_id="cell",
        epoch=1,
        input_head="base",
        budget=CampaignBudget(max_depth=3, max_candidates=3, max_cost_usd=10, max_wall_s=60),
        parallel=False,
    )

    assert len(report.rounds) == 1
    assert report.rounds[0].exploration_selection.winner is None
    assert report.stop_reason == "no_exploration_candidate"
    assert report.promotion_winner is None


def test_explicit_schedule_may_narrow_faster_but_not_exceed_budget() -> None:
    report = run_annealing_loop(
        _runner,
        loop_id="custom",
        cell_id="cell",
        epoch=1,
        input_head="base",
        budget=CampaignBudget(max_depth=2, max_candidates=3, max_cost_usd=10, max_wall_s=60),
        strategy_schedule=((Strategy.SCRATCH, Strategy.REPAIR), (Strategy.REPAIR,)),
        parallel=False,
    )

    assert [round_.strategies for round_ in report.rounds] == [
        ("scratch", "repair"),
        ("repair",),
    ]
    assert report.stop_reason == "schedule_exhausted"


def test_report_serializes_both_exploration_and_promotion_authority() -> None:
    report = run_annealing_loop(
        _runner,
        loop_id="serialized",
        cell_id="cell",
        epoch=1,
        input_head="base",
        budget=CampaignBudget(max_depth=0, max_candidates=3, max_cost_usd=10, max_wall_s=60),
        parallel=False,
    )

    document = report.to_dict()
    assert document["authority"] == "exploration-only"
    assert document["scheduler"] == "airflow"
    assert document["rounds"][0]["schema_version"] == 3
    assert document["rounds"][0]["exploration_selection"]["winner"]
    assert document["rounds"][0]["selection"]["winner"] is None


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


def test_entropy_schedule_randomizes_search_but_keeps_annealing_shape(monkeypatch) -> None:
    class FixedOrder:
        entropy_token = "fixed"
        values = ("scratch", "repair", "rethink")

    monkeypatch.setattr("swfactory.research_loop.permute_exploration", lambda values: FixedOrder())
    order, schedule = entropy_strategy_schedule(2, max_candidates=3)

    assert order.entropy_token == "fixed"
    assert tuple(strategy.value for strategy in schedule[0]) == ("scratch", "repair", "rethink")
    assert tuple(strategy.value for strategy in schedule[1]) == ("scratch", "repair")
    assert tuple(strategy.value for strategy in schedule[2]) == ("scratch",)
