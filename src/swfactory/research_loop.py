"""Bounded autoresearch-style annealing over candidate campaigns.

One campaign explores a decision in parallel. This controller stacks those sibling
bushes into a bounded research loop: inspect the evidence-best answer, descend from
its exact recorded revision, reduce strategy breadth, and repeat until the depth or
global budget is exhausted.

This is deliberately an in-stage controller, not a scheduler and not a promotion
authority. Airflow owns lifecycle scheduling; CampaignReport.selection keeps the
human gate. Only exploration_selection drives the next experiment round.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from swfactory.evolution import (
    DEFAULT_STRATEGIES,
    REQUIRED_DIMENSIONS,
    CampaignError,
    CampaignReport,
    CandidateOutcome,
    CandidateRunner,
    Strategy,
    plan_requests,
    run_campaign,
)
from swfactory.experiment_tree import ExperimentTree, stack_rounds
from swfactory.generations import CampaignBudget, Dimension
from swfactory.work_executor import Cancellation


@dataclass(frozen=True)
class AnnealingLoopReport:
    """Durable result of several evidence-linked candidate rounds."""

    loop_id: str
    cell_id: str
    epoch: int
    root_head: str
    rounds: tuple[CampaignReport, ...]
    stop_reason: str
    total_cost_usd: float
    total_wall_s: int

    @property
    def tree(self) -> ExperimentTree:
        experiment_rounds = tuple(
            report.experiment_round for report in self.rounds if report.experiment_round is not None
        )
        if len(experiment_rounds) != len(self.rounds) or not experiment_rounds:
            raise CampaignError("annealing loop cannot build a tree from incomplete campaign reports")
        return stack_rounds(experiment_rounds)

    @property
    def exploration_winner(self) -> CandidateOutcome | None:
        if not self.rounds:
            return None
        report = self.rounds[-1]
        winner = report.exploration_selection.winner
        if winner is None:
            return None
        return next((outcome for outcome in report.outcomes if outcome.logical_id == winner), None)

    @property
    def promotion_winner(self) -> CandidateOutcome | None:
        if not self.rounds:
            return None
        report = self.rounds[-1]
        winner = report.selection.winner
        if winner is None:
            return None
        return next((outcome for outcome in report.outcomes if outcome.logical_id == winner), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "authority": "exploration-only",
            "scheduler": "airflow",
            "loop_id": self.loop_id,
            "cell_id": self.cell_id,
            "epoch": self.epoch,
            "root_head": self.root_head,
            "stop_reason": self.stop_reason,
            "total_cost_usd": self.total_cost_usd,
            "total_wall_s": self.total_wall_s,
            "rounds": [report.to_dict() for report in self.rounds],
            "experiment_tree": self.tree.to_dict(),
        }


def annealed_strategy_schedule(
    max_depth: int,
    *,
    max_candidates: int,
    strategies: Sequence[Strategy] = DEFAULT_STRATEGIES,
) -> tuple[tuple[Strategy, ...], ...]:
    """Reduce exploration width monotonically while retaining at least one strategy."""
    if max_depth < 0:
        raise CampaignError("annealing max_depth must be nonnegative")
    if max_candidates < 1:
        raise CampaignError("annealing max_candidates must be positive")
    if not strategies:
        raise CampaignError("annealing needs at least one strategy")
    if len(set(strategies)) != len(strategies):
        raise CampaignError("annealing strategies must be distinct")

    base = tuple(strategies[:max_candidates])
    return tuple(base[: max(1, len(base) - depth)] for depth in range(max_depth + 1))


def run_annealing_loop(
    runner: CandidateRunner,
    *,
    loop_id: str,
    cell_id: str,
    epoch: int,
    input_head: str,
    budget: CampaignBudget | None = None,
    strategy_schedule: Sequence[Sequence[Strategy]] | None = None,
    max_parallel: int = 3,
    parallel: bool = True,
    human_approved: bool = False,
    required: Iterable[Dimension] = REQUIRED_DIMENSIONS,
    cancellation: Cancellation | None = None,
) -> AnnealingLoopReport:
    """Run bounded candidate rounds, descending only from evidence-selected answers.

    human_approved affects only each round's promotion-aware selection. The next
    research round follows exploration_selection, which requires the same
    correctness/evidence dimensions but never grants release authority.
    """
    budget = budget or CampaignBudget()
    cancellation = cancellation or Cancellation()
    if not loop_id.strip():
        raise CampaignError("annealing loop id must be nonempty")
    if not input_head.strip():
        raise CampaignError("annealing input head must be nonempty")
    if budget.max_depth < 0 or budget.max_candidates < 1:
        raise CampaignError("annealing budget has invalid depth or candidate count")
    if budget.max_cost_usd < 0 or budget.max_wall_s < 0:
        raise CampaignError("annealing budget has a negative cost or wall limit")

    if strategy_schedule is None:
        schedule = annealed_strategy_schedule(
            budget.max_depth,
            max_candidates=budget.max_candidates,
        )
    else:
        schedule = tuple(tuple(round_strategies) for round_strategies in strategy_schedule)
        if not schedule:
            raise CampaignError("annealing strategy schedule must contain at least one round")
        if len(schedule) > budget.max_depth + 1:
            raise CampaignError("annealing strategy schedule exceeds max_depth")
        for depth, strategies in enumerate(schedule):
            if not strategies:
                raise CampaignError(f"annealing round {depth} has no strategies")
            if len(strategies) > budget.max_candidates:
                raise CampaignError(
                    f"annealing round {depth} has {len(strategies)} candidates; "
                    f"budget admits {budget.max_candidates}"
                )
            if len(set(strategies)) != len(strategies):
                raise CampaignError(f"annealing round {depth} repeats a strategy")

    started = time.monotonic()
    rounds: list[CampaignReport] = []
    current_head = input_head
    parent_candidate: str | None = None
    total_cost = 0.0
    stop_reason = "schedule_exhausted"

    for depth, strategies in enumerate(schedule):
        if cancellation.cancelled:
            stop_reason = "cancelled"
            break

        elapsed = int(time.monotonic() - started)
        remaining_cost = round(budget.max_cost_usd - total_cost, 6)
        remaining_wall = budget.max_wall_s - elapsed
        if remaining_cost <= 0 or remaining_wall <= 0:
            stop_reason = "budget_exhausted"
            break

        round_budget = CampaignBudget(
            max_depth=budget.max_depth,
            max_candidates=budget.max_candidates,
            max_cost_usd=remaining_cost,
            max_wall_s=remaining_wall,
        )
        requests = plan_requests(
            campaign_id=f"{loop_id}:round-{depth}",
            cell_id=cell_id,
            epoch=epoch,
            input_head=current_head,
            strategies=strategies,
            budget=round_budget,
            parent_candidate=parent_candidate,
            depth=depth,
        )
        report = run_campaign(
            runner,
            requests,
            budget=round_budget,
            max_parallel=max_parallel,
            parallel=parallel,
            human_approved=human_approved,
            required=required,
            cancellation=cancellation,
        )
        rounds.append(report)
        total_cost = round(total_cost + sum(outcome.cost_usd for outcome in report.outcomes), 6)

        if report.cancelled or cancellation.cancelled:
            stop_reason = "cancelled"
            break
        if report.exploration_selection.reason == "campaign_exceeded_budget":
            stop_reason = "budget_exhausted"
            break

        winner_id = report.exploration_selection.winner
        if winner_id is None:
            stop_reason = "no_exploration_candidate"
            break
        winner = next(outcome for outcome in report.outcomes if outcome.logical_id == winner_id)
        if not winner.output_head:
            raise CampaignError(f"exploration winner {winner_id} has no output head")

        current_head = winner.output_head
        parent_candidate = winner.logical_id

        elapsed = int(time.monotonic() - started)
        if total_cost >= budget.max_cost_usd or elapsed >= budget.max_wall_s:
            stop_reason = "budget_exhausted"
            break
        if depth == budget.max_depth:
            stop_reason = "max_depth"
            break

    total_wall = int(time.monotonic() - started)
    if not rounds:
        raise CampaignError(f"annealing loop produced no rounds: {stop_reason}")

    result = AnnealingLoopReport(
        loop_id=loop_id,
        cell_id=cell_id,
        epoch=epoch,
        root_head=input_head,
        rounds=tuple(rounds),
        stop_reason=stop_reason,
        total_cost_usd=total_cost,
        total_wall_s=total_wall,
    )
    result.tree.validate()
    return result
