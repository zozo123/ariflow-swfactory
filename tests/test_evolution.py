"""Candidate campaign planning: the exact, bounded questions a campaign may ask.

A campaign that is wider or deeper than its budget, or that asks one strategy twice, explores
less than it claims.  These tests pin that the planner refuses such a campaign before it spends
anything, and that the budget is split across the population it admits.
"""

from __future__ import annotations

import pytest

from swfactory.evolution import DEFAULT_STRATEGIES, CampaignError, CandidateRequest, Strategy, plan_requests
from swfactory.generations import CampaignBudget

BASE = "base_head"


def _requests(*strategies: Strategy, budget: CampaignBudget | None = None) -> tuple[CandidateRequest, ...]:
    return plan_requests(
        campaign_id="camp",
        cell_id="cell_7cb2f608ee524068af1a75ad",
        epoch=3,
        input_head=BASE,
        strategies=strategies or DEFAULT_STRATEGIES,
        budget=budget,
    )


def test_a_campaign_wider_than_its_budget_is_refused_before_it_spends_anything() -> None:
    with pytest.raises(CampaignError, match="admits at most 2 candidates"):
        _requests(budget=CampaignBudget(max_candidates=2))


def test_a_campaign_deeper_than_its_budget_is_refused() -> None:
    with pytest.raises(CampaignError, match="admits at most"):
        plan_requests(
            campaign_id="camp",
            cell_id="cell_7cb2f608ee524068af1a75ad",
            epoch=3,
            input_head=BASE,
            budget=CampaignBudget(max_depth=1),
            depth=2,
        )


def test_the_same_strategy_twice_is_not_a_population() -> None:
    with pytest.raises(CampaignError, match="same strategy twice"):
        _requests(Strategy.REPAIR, Strategy.REPAIR)


def test_the_budget_splits_across_the_population() -> None:
    requests = _requests(budget=CampaignBudget(max_cost_usd=9.0))

    assert [request.budget_usd for request in requests] == [3.0, 3.0, 3.0]
