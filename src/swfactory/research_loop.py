"""Exploration-width cooling for stacked candidate campaigns.

One campaign explores a decision in parallel; a descendant round starts from the evidence-best
answer of the previous one. The schedule here narrows strategy breadth with depth, so later
rounds refine an answer rather than re-explore the decision.

This is deliberately not a scheduler and not a promotion authority. Airflow owns lifecycle
scheduling; CampaignReport.selection keeps the human gate.
"""

from __future__ import annotations

from collections.abc import Sequence

from swfactory.evolution import DEFAULT_STRATEGIES, CampaignError, Strategy


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
