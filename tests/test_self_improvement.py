"""The loop that turns the factory's own evidence into work it can verify it finished.

The failure mode of a self-improving loop is not that it does nothing: it is that it proposes
things nobody can check and then reports success against its own prose. These tests pin the two
properties that prevent that -- every emitted order names a check this repository already runs, and
no single source may own the budget -- plus the arithmetic that decides what counts as debt.
"""

from __future__ import annotations

import pytest

from swfactory.self_improvement import (
    VERIFIABLE_CHECKS,
    Assessment,
    DoneWhen,
    ProposalError,
    Signal,
    Source,
    WorkOrder,
    capability_signals,
    delivery_signals,
    interleave,
    issue_commands,
    issue_plan,
    ledger_signals,
    propose,
    rank_key,
    report,
)


def _signal(source: Source, key: str, weight: float = 1.0) -> Signal:
    return Signal(source, key, weight, "detail")


# ------------------------------------------------------- a proposal must be falsifiable to exist


def test_a_done_condition_naming_no_real_check_is_refused() -> None:
    """The safety property. An order whose completion can only be taken on trust is not emitted."""
    with pytest.raises(ProposalError, match="not a check this repository runs"):
        DoneWhen("make it better", "the code is nicer").validate()


def test_every_emitted_order_cites_a_check_the_repository_runs() -> None:
    assessment = propose([_signal(Source.REACHABILITY, "mod", 10), _signal(Source.CAPABILITY, "cap")], budget=9)

    assert assessment.orders
    for order in assessment.orders:
        assert order.done_when.check in VERIFIABLE_CHECKS


def test_the_predicate_may_not_be_empty() -> None:
    with pytest.raises(ProposalError, match="no predicate"):
        DoneWhen("uv run swfactory metrics --root .", "   ").validate()


def test_a_budget_below_one_is_refused() -> None:
    with pytest.raises(ProposalError, match="at least one"):
        propose([_signal(Source.CAPABILITY, "cap")], budget=0)


# ------------------------------------------------------------------ no source owns the budget


def test_one_source_cannot_monopolise_the_proposal() -> None:
    """A straight weight ranking proposes twenty-four module chores and never mentions a stuck
    capability claim. A loop that only descends its steepest gradient stops when that flattens."""
    signals = [_signal(Source.REACHABILITY, f"mod{i}", 1000 - i) for i in range(10)]
    signals.append(_signal(Source.CAPABILITY, "claim", 1))

    assessment = propose(signals, budget=4)

    assert {order.source for order in assessment.orders} == {Source.REACHABILITY, Source.CAPABILITY}


def test_within_a_source_the_heaviest_debt_comes_first() -> None:
    signals = [_signal(Source.REACHABILITY, "small", 5), _signal(Source.REACHABILITY, "big", 500)]

    assessment = propose(signals, budget=2)

    assert [order.key for order in assessment.orders] == ["big", "small"]


def test_interleaving_a_single_source_is_just_that_source() -> None:
    orders = [
        WorkOrder(Source.CAPABILITY, k, k, "why", DoneWhen("uv run swfactory metrics --root .", "p"), 1.0)
        for k in ("a", "b")
    ]

    assert [order.key for order in interleave(orders, 5)] == ["a", "b"]


def test_ranking_reads_nothing_but_the_order() -> None:
    order = WorkOrder(Source.DELIVERY, "k", "t", "why", DoneWhen("uv run swfactory metrics --root .", "p"), 2.0)

    assert rank_key(order) == rank_key(order)


# ------------------------------------------------------------------------- reading the evidence


def test_a_metric_at_target_is_not_a_signal() -> None:
    """A loop that proposes work against a met target manufactures its own backlog."""
    healthy = {"runs": 4, "first_pass_rate": 1.0, "tests_pass_rate": 1.0, "mean_iterations": 1.0, "blockers": 0}

    assert delivery_signals(healthy) == []


def test_a_missed_target_is_weighted_by_the_gap() -> None:
    summary = {"runs": 4, "first_pass_rate": 0.5, "tests_pass_rate": 1.0, "mean_iterations": 1.0, "blockers": 0}

    (signal,) = delivery_signals(summary)

    assert signal.key == "first_pass_rate"
    assert signal.weight == pytest.approx(0.3)


def test_no_runs_means_no_delivery_opinion() -> None:
    assert delivery_signals({"runs": 0, "first_pass_rate": 0.0}) == []


def test_a_validated_claim_is_not_debt() -> None:
    document = {
        "claims": [
            {"id": "done", "state": "validated", "support": "supported"},
            {"id": "pending", "state": "experimental", "support": "experimental"},
        ]
    }

    assert [signal.key for signal in capability_signals(document)] == ["pending"]


def test_a_claim_further_from_validated_weighs_more() -> None:
    document = {
        "claims": [
            {"id": "near", "state": "experimental", "support": "experimental"},
            {"id": "far", "state": "declared", "support": "experimental"},
        ]
    }

    weights = {signal.key: signal.weight for signal in capability_signals(document)}

    assert weights["far"] > weights["near"]


def test_unreachable_modules_are_weighted_by_the_lines_that_run_nothing() -> None:
    signals = ledger_signals({"big": "reason", "small": "reason"}, {"big": 400, "small": 9})

    assert {signal.key: signal.weight for signal in signals} == {"big": 400.0, "small": 9.0}


# --------------------------------------------------------------------------------- the output


def test_the_issue_body_leads_with_the_done_condition() -> None:
    (order,) = propose([_signal(Source.CAPABILITY, "sandbox.islo")], budget=1).orders

    body = order.as_issue()

    assert "**Done when:**" in body
    assert order.done_when.check in body
    assert order.labels == ("liquid",)


def test_nothing_to_propose_says_so_rather_than_inventing_work() -> None:
    assert "every measured signal is at target" in report(Assessment().orders)


# ------------------------------------------------- the last mechanical gap: proposal -> backlog


def test_a_rendered_issue_carries_the_label_the_line_drains() -> None:
    """`blueprints/liquid.toml` enrols on `trigger.backlog.label`; without it the issue is inert."""
    (issue,) = issue_plan(propose([_signal(Source.CAPABILITY, "sandbox.islo")], budget=1).orders)

    assert "liquid" in issue["labels"]
    assert "**Done when:**" in issue["body"]


def test_issue_commands_survive_a_body_full_of_backticks_and_quotes() -> None:
    """Bodies are markdown with backticks and fenced blocks. An unquoted one is a broken command
    at best and an injected one at worst, so the rendering has to be shell-safe by construction."""
    orders = propose([_signal(Source.REACHABILITY, "mod", 10)], budget=1).orders

    (command,) = issue_commands(orders)

    assert command.startswith("gh issue create --title ")
    assert "```sh" in command
    # Every embedded single quote is escaped rather than closing the argument early.
    assert command.count("'") % 2 == 0


def test_rendering_issues_files_nothing() -> None:
    """The boundary this module exists to hold: it proposes, a person acts."""
    orders = propose([_signal(Source.CAPABILITY, "claim")], budget=1).orders

    plan = issue_plan(orders)

    assert isinstance(plan, list) and plan[0]["title"]
    assert all(isinstance(line, str) for line in issue_commands(orders))
