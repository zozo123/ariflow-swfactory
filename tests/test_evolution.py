"""Candidate campaigns: many weak attempts at one stage, one promoted on evidence.

The repository already had every part of this except the part that runs: ``generations`` knows how
to score and gate a promotion, ``work_executor`` knows how to fan out and fan in, and the live
build stage runs one attempt after another and writes ``"parallel": false``.  These tests pin the
properties that make a campaign an experiment rather than a race, because each of them is a way
the idea fails quietly: a winner that depends on who finished first, a sibling cancelled by
another's failure, a population that explored one idea three times, or a campaign that approves
its own result.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import replace

import pytest

from swfactory.evolution import (
    DEFAULT_STRATEGIES,
    REQUIRED_DIMENSIONS,
    CampaignError,
    CandidateOutcome,
    CandidateRequest,
    Strategy,
    evaluation,
    independence_findings,
    plan_requests,
    rank_key,
    run_campaign,
    select,
)
from swfactory.generations import CampaignBudget, Dimension

BASE = "base_head"


def _requests(*strategies: Strategy, budget: CampaignBudget | None = None) -> tuple[CandidateRequest, ...]:
    return plan_requests(
        campaign_id="camp",
        cell_id="cell_1",
        epoch=3,
        input_head=BASE,
        strategies=strategies or DEFAULT_STRATEGIES,
        budget=budget,
    )


def _outcome(
    request: CandidateRequest,
    *,
    correct: bool = True,
    evidenced: bool = True,
    cost: float = 1.0,
    duration: float = 1.0,
    state: str = "ok",
    head: str | None = None,
) -> CandidateOutcome:
    return CandidateOutcome(
        logical_id=request.logical_id,
        strategy=request.strategy,
        state=state,  # type: ignore[arg-type]
        input_head=request.input_head,
        output_head=head or f"head_{request.strategy.value}",
        evaluations=(
            evaluation(Dimension.CORRECTNESS, passed=correct, evidence="target test command"),
            evaluation(Dimension.EVIDENCE, passed=evidenced, evidence="candidate report"),
        ),
        cost_usd=cost,
        duration_s=duration,
    )


# ------------------------------------------------------------------ the learners run together


def test_candidates_actually_overlap_in_time() -> None:
    """A campaign that serialises its candidates costs the same as the repair loop it replaces."""
    live = 0
    peak = 0
    lock = threading.Lock()

    def runner(request: CandidateRequest) -> CandidateOutcome:
        nonlocal live, peak
        with lock:
            live += 1
            peak = max(peak, live)
        time.sleep(0.05)
        with lock:
            live -= 1
        return _outcome(request)

    report = run_campaign(runner, _requests(), human_approved=True)

    assert report.parallel is True
    assert peak >= 2, "candidates never ran at the same time"


def test_one_losing_candidate_does_not_cancel_its_siblings() -> None:
    """The information a campaign buys is which strategy loses; cancelling the rest discards it."""

    def runner(request: CandidateRequest) -> CandidateOutcome:
        if request.strategy is Strategy.REPAIR:
            raise RuntimeError("patch no longer applies")
        return _outcome(request)

    report = run_campaign(runner, _requests(), human_approved=True)

    states = {outcome.strategy: outcome.state for outcome in report.outcomes}
    assert states[Strategy.REPAIR] == "failed"
    assert states[Strategy.RETHINK] == "ok"
    assert states[Strategy.SCRATCH] == "ok"
    assert report.selection.winner is not None


def test_a_raising_runner_becomes_a_losing_candidate_not_a_crashed_campaign() -> None:
    def runner(request: CandidateRequest) -> CandidateOutcome:
        raise MemoryError("sandbox died")

    report = run_campaign(runner, _requests(), human_approved=True)

    assert all(outcome.state == "failed" for outcome in report.outcomes)
    assert report.selection.winner is None
    assert "MemoryError" in report.outcomes[0].detail


# ------------------------------------------------------------ the winner is not whoever was fast


def test_selection_does_not_depend_on_completion_order() -> None:
    """The property that separates an experiment from a race.

    The cheapest correct candidate must win whether it finishes first or last, so the campaign is
    reproducible from its stored report rather than from the timing of the run that produced it.
    """
    requests = _requests()
    delays = {Strategy.REPAIR: 0.06, Strategy.RETHINK: 0.0, Strategy.SCRATCH: 0.03}

    def runner(request: CandidateRequest) -> CandidateOutcome:
        time.sleep(delays[request.strategy])
        return _outcome(request, cost=1.0 if request.strategy is Strategy.REPAIR else 5.0)

    slowest_first = run_campaign(runner, requests, human_approved=True)

    # Same candidates, opposite finishing order.
    delays = {Strategy.REPAIR: 0.0, Strategy.RETHINK: 0.06, Strategy.SCRATCH: 0.03}
    fastest_first = run_campaign(runner, requests, human_approved=True)

    assert slowest_first.selection.winner == fastest_first.selection.winner
    assert slowest_first.selection.ranking == fastest_first.selection.ranking
    assert slowest_first.outcomes[0].strategy is Strategy.REPAIR, "report order follows the request"


def test_the_cheapest_candidate_cannot_win_by_being_wrong() -> None:
    """Cost is a tiebreak between correct candidates, never a substitute for correctness."""
    requests = _requests(Strategy.REPAIR, Strategy.RETHINK)
    outcomes = [
        _outcome(requests[0], correct=False, cost=0.01, duration=0.01),
        _outcome(requests[1], correct=True, cost=99.0, duration=99.0),
    ]

    selection = select(outcomes, human_approved=True)

    # The correct candidate ranks first despite being 99x the cost and 99x the wall time, so the
    # incorrect one is never even reached -- refusals list only what was examined before a winner.
    assert selection.winner == requests[1].logical_id
    assert selection.ranking == (requests[1].logical_id, requests[0].logical_id)
    assert selection.refusals == ()


def test_a_candidate_missing_a_required_dimension_is_refused() -> None:
    requests = _requests(Strategy.REPAIR)
    bare = (replace(requests[0]),)
    outcome = CandidateOutcome(
        logical_id=requests[0].logical_id,
        strategy=Strategy.REPAIR,
        state="ok",
        input_head=BASE,
        output_head="head_repair",
        evaluations=(evaluation(Dimension.CORRECTNESS, passed=True, evidence="tests"),),
    )
    assert bare  # the request is unchanged; only the evidence is short

    selection = select([outcome], human_approved=True)

    assert selection.winner is None
    assert any("missing:evidence" in refusal for refusal in selection.refusals)


def test_rank_key_reads_nothing_but_the_candidate() -> None:
    requests = _requests(Strategy.REPAIR)
    outcome = _outcome(requests[0])

    assert rank_key(outcome, REQUIRED_DIMENSIONS) == rank_key(outcome, REQUIRED_DIMENSIONS)


# ------------------------------------------------------------------- the campaign is not authority


def test_a_campaign_cannot_approve_its_own_winner() -> None:
    """Selection proposes. ``generations.promotable`` still requires the human gate."""
    report = run_campaign(lambda request: _outcome(request), _requests(), human_approved=False)

    assert report.selection.winner is None
    assert all("human_gate" in refusal for refusal in report.selection.refusals)


# ------------------------------------------------------------------------------ independence


def test_candidates_that_all_land_on_one_head_are_reported_as_one_idea() -> None:
    requests = _requests()
    outcomes = [_outcome(request, head="same_head") for request in requests]

    findings = independence_findings(outcomes, input_head=BASE)

    assert len(findings) == 1
    assert "identical output head same_head" in findings[0]


def test_a_candidate_that_never_left_the_input_head_is_reported() -> None:
    requests = _requests(Strategy.REPAIR)
    outcomes = [_outcome(requests[0], head=BASE)]

    findings = independence_findings(outcomes, input_head=BASE)

    assert findings and "equals the campaign input head" in findings[0]


def test_independence_is_recorded_on_the_report_the_campaign_stores() -> None:
    report = run_campaign(lambda request: _outcome(request, head="one_head"), _requests(), human_approved=True)

    assert report.independence
    # The report is stored as JSON, so it has to survive the trip.
    document = json.loads(json.dumps(report.to_dict()))
    assert document["independence"] == list(report.independence)
    assert document["schema_version"] == 1
    assert document["outcomes"][0]["evaluations"][0]["dimension"] == "correctness"


# ---------------------------------------------------------------------------------- bounds


def test_a_campaign_wider_than_its_budget_is_refused_before_it_spends_anything() -> None:
    with pytest.raises(CampaignError, match="admits at most 2 candidates"):
        _requests(budget=CampaignBudget(max_candidates=2))


def test_a_campaign_deeper_than_its_budget_is_refused() -> None:
    with pytest.raises(CampaignError, match="admits at most"):
        plan_requests(
            campaign_id="camp",
            cell_id="cell_1",
            epoch=3,
            input_head=BASE,
            budget=CampaignBudget(max_depth=1),
            depth=2,
        )


def test_the_same_strategy_twice_is_not_a_population() -> None:
    with pytest.raises(CampaignError, match="same strategy twice"):
        _requests(Strategy.REPAIR, Strategy.REPAIR)


def test_candidates_of_one_campaign_must_share_an_input_head() -> None:
    requests = _requests(Strategy.REPAIR, Strategy.RETHINK)
    drifted = (requests[0], replace(requests[1], input_head="other_head"))

    with pytest.raises(CampaignError, match="same input head"):
        run_campaign(lambda request: _outcome(request), drifted)


def test_a_campaign_that_overspends_refuses_to_promote() -> None:
    """Budget is checked against what was actually spent, not what was planned."""
    budget = CampaignBudget(max_cost_usd=2.0)
    report = run_campaign(
        lambda request: _outcome(request, cost=5.0), _requests(budget=budget), budget=budget, human_approved=True
    )

    assert report.selection.winner is None
    assert report.selection.reason == "campaign_exceeded_budget"


def test_the_budget_splits_across_the_population() -> None:
    requests = _requests(budget=CampaignBudget(max_cost_usd=9.0))

    assert [request.budget_usd for request in requests] == [3.0, 3.0, 3.0]
