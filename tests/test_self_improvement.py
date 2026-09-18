"""The loop that turns the factory's own evidence into work it can verify it finished.

The failure mode of a self-improving loop is not that it does nothing: it is that it proposes
things nobody can check and then reports success against its own prose. These tests pin the two
properties that prevent that -- every emitted order names a check this repository already runs, and
no single source may own the budget -- plus the arithmetic that decides what counts as debt.
"""

from __future__ import annotations

import json
from pathlib import Path

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
    delta,
    history,
    interleave,
    issue_commands,
    issue_plan,
    ledger_signals,
    propose,
    rank_key,
    record,
    report,
    stalled,
    trajectory_report,
)

ROOT = Path(__file__).resolve().parents[1]


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


# ------------------------------------------------------------------------ the loop's memory


def _assessment(signal_keys: dict[str, float], order_keys: tuple[str, ...] = ()) -> dict:
    return {
        "signals": [{"source": "reachability", "key": k, "weight": w} for k, w in signal_keys.items()],
        "orders": [{"source": "reachability", "key": k} for k in order_keys],
    }


def test_retired_debt_is_the_only_outcome_that_counts_as_done() -> None:
    moved = delta(_assessment({"a": 10, "b": 5})["signals"], _assessment({"b": 5})["signals"])

    assert moved.retired == ("reachability:a",)
    assert moved.converging


def test_debt_that_grew_is_not_progress() -> None:
    moved = delta(_assessment({"a": 10})["signals"], _assessment({"a": 40, "b": 1})["signals"])

    assert moved.grew == ("reachability:a",)
    assert moved.appeared == ("reachability:b",)
    assert not moved.converging


def test_a_standstill_is_reported_as_not_converging() -> None:
    """A loop that calls no movement success is a loop that has stopped measuring."""
    moved = delta(_assessment({"a": 10})["signals"], _assessment({"a": 10})["signals"])

    assert not moved.converging


def test_a_stall_counts_proposals_not_measurements() -> None:
    """Measuring three times in an afternoon is one cycle, not three. Counting measurements flags
    the whole ledger the third time anyone runs the command, and a noisy alarm is an ignored one."""
    measured_often = [_assessment({"a": 1, "b": 2}) for _ in range(4)]

    assert stalled(measured_often) == ()


def test_something_proposed_every_cycle_and_never_done_is_flagged() -> None:
    proposed = [_assessment({"a": 1, "b": 2}, order_keys=("a",)) for _ in range(3)]

    assert stalled(proposed) == ("reachability:a",)


def test_a_short_history_cannot_stall() -> None:
    assert stalled([_assessment({"a": 1}, ("a",))], threshold=3) == ()


def test_a_corrupt_trajectory_entry_does_not_stop_todays_measurement(tmp_path) -> None:
    (tmp_path / "0001.json").write_text(json.dumps(_assessment({"a": 1})), encoding="utf-8")
    (tmp_path / "0002.json").write_text("{ this is not json", encoding="utf-8")

    assert len(history(tmp_path)) == 1


def test_the_first_measurement_says_there_is_no_trajectory_yet(tmp_path) -> None:
    assert "no trajectory yet" in trajectory_report([], Assessment())


def test_an_assessment_round_trips_through_the_trajectory(tmp_path) -> None:
    assessment = propose([_signal(Source.REACHABILITY, "mod", 10)], budget=1)

    record(assessment, tmp_path, at="20260101T000000")

    assert [entry["orders"][0]["key"] for entry in history(tmp_path)] == ["mod"]


# ------------------------------------------------- the loop acting on its own stall signal


def test_a_stalled_order_is_demoted_behind_work_that_can_still_move() -> None:
    """Detecting a stall and then proposing it at position one anyway is the loop ignoring its own
    signal: every cycle reports an identical top priority, which reads like focus and is a
    standstill."""
    signals = [_signal(Source.REACHABILITY, "huge", 900), _signal(Source.REACHABILITY, "small", 10)]

    assessment = propose(signals, budget=2, stalled_keys=["reachability:huge"])

    assert [order.key for order in assessment.orders] == ["small", "huge"]


def test_a_stalled_order_is_demoted_but_never_dropped() -> None:
    """Still real debt. It needs re-scoping by someone, not forgetting."""
    assessment = propose([_signal(Source.REACHABILITY, "huge", 900)], budget=3, stalled_keys=["reachability:huge"])

    assert [order.key for order in assessment.orders] == ["huge"]
    assert assessment.orders[0].stalled is True


def test_demotion_never_silences_a_whole_source() -> None:
    """Demotion is within a source, so a stalled reachability item cannot push capability work out."""
    signals = [_signal(Source.REACHABILITY, "stuck", 900), _signal(Source.CAPABILITY, "claim", 1)]

    assessment = propose(signals, budget=2, stalled_keys=["reachability:stuck"])

    assert {order.source for order in assessment.orders} == {Source.REACHABILITY, Source.CAPABILITY}


def test_the_report_says_why_an_order_slid_down() -> None:
    assessment = propose([_signal(Source.REACHABILITY, "huge", 900)], budget=1, stalled_keys=["reachability:huge"])

    assert "[stalled: re-scope]" in report(assessment.orders)


def test_the_assessment_records_what_was_stalled_when_it_was_taken() -> None:
    assessment = propose([_signal(Source.CAPABILITY, "c")], budget=1, stalled_keys=["capability:c"])

    assert assessment.to_dict()["stalled"] == ["capability:c"]


def test_with_no_trajectory_nothing_is_stalled_and_ranking_is_unchanged() -> None:
    signals = [_signal(Source.REACHABILITY, "huge", 900), _signal(Source.REACHABILITY, "small", 10)]

    assert [o.key for o in propose(signals, budget=2).orders] == ["huge", "small"]


def test_demoting_a_stalled_order_does_not_clear_its_own_stall() -> None:
    """The oscillation this fixes, measured on a real trajectory:

        cycle 1-3  evolution proposed
        cycle 4    evolution stalled, demoted, therefore absent from that cycle's orders
        cycle 5    a consecutive-run rule reads the gap as progress -> back at position one

    The correction erased the evidence for the correction. Counting occurrences over the whole
    trajectory makes the flag sticky: once earned it holds until the debt is actually retired.
    """
    trajectory = [_assessment({"a": 1}, order_keys=("a",)) for _ in range(3)]
    trajectory.append(_assessment({"a": 1}, order_keys=("b",)))  # the cycle where it was demoted

    assert stalled(trajectory) == ("reachability:a",)


def test_retired_debt_stops_being_stalled() -> None:
    """A stall is a property of debt the factory still carries; paid debt must not haunt it."""
    trajectory = [_assessment({"a": 1}, order_keys=("a",)) for _ in range(3)]

    assert stalled(trajectory, present=["reachability:a"]) == ("reachability:a",)
    assert stalled(trajectory, present=[]) == ()


# --------------------------------------------------- annealing re-admits what the loop avoids


def test_re_admission_undemotes_a_stalled_order_without_forgetting_it_stalled() -> None:
    """When the loop has retired nothing for several cycles, the item it keeps sidestepping is
    usually the one in its way. It comes back to be re-scoped -- still marked, not reset."""
    signals = [_signal(Source.REACHABILITY, "huge", 900), _signal(Source.REACHABILITY, "small", 10)]

    cold = propose(signals, budget=2, stalled_keys=["reachability:huge"])
    hot = propose(signals, budget=2, stalled_keys=["reachability:huge"], readmit_stalled=True)

    assert [o.key for o in cold.orders] == ["small", "huge"]
    assert [o.key for o in hot.orders] == ["huge", "small"]
    assert hot.orders[0].stalled is True and hot.orders[0].demoted is False


def test_the_report_distinguishes_demoted_from_re_admitted() -> None:
    signals = [_signal(Source.REACHABILITY, "huge", 900)]

    cold = report(propose(signals, budget=1, stalled_keys=["reachability:huge"]).orders)
    hot = report(propose(signals, budget=1, stalled_keys=["reachability:huge"], readmit_stalled=True).orders)

    assert "[stalled: re-scope]" in cold
    assert "[stalled: re-admitted to re-scope]" in hot


# ------------------------------------------- capability debt: resolution, and which end comes first


def _claim(cid: str, *, state="experimental", test="tests/test_self_improvement.py", environment="") -> dict:
    return {"id": cid, "state": state, "support": "experimental", "test": test, "environment": environment}


def test_the_nearest_capability_claim_is_proposed_before_the_furthest() -> None:
    """Dead code and capability claims disagree about what is worth doing next, and the disagreement
    is real: 360 unreachable lines cost more than 60, but the claim NEAREST validation is the
    cheapest to close. Proposing the furthest one first is how a backlog never moves."""
    signals = [
        Signal(Source.CAPABILITY, "far", 3.0, "d"),
        Signal(Source.CAPABILITY, "near", 1.0, "d"),
    ]

    assert [o.key for o in propose(signals, budget=2).orders] == ["near", "far"]


def test_dead_code_still_leads_with_the_biggest_block() -> None:
    """The opposite direction, in the same ranking, because the sources mean different things."""
    signals = [Signal(Source.REACHABILITY, "small", 60, "d"), Signal(Source.REACHABILITY, "big", 360, "d")]

    assert [o.key for o in propose(signals, budget=2).orders] == ["big", "small"]


def test_weight_still_means_remaining_distance_everywhere() -> None:
    """Inverting the weight for one source would have inverted delta() with it, where "shrank" must
    always mean progress. The ordering differs per source; the units do not."""
    moved = delta(
        [{"source": "capability", "key": "c", "weight": 3.0}],
        [{"source": "capability", "key": "c", "weight": 1.0}],
    )

    assert moved.shrank == ("capability:c",)
    assert moved.converging


def test_a_claim_whose_test_resolves_to_nothing_is_further_away(tmp_path) -> None:
    (tmp_path / "config").mkdir()
    document = {"claims": [_claim("real"), _claim("ghost", test="tests/test_nobody_wrote_this.py")]}

    weights = {s.key: s.weight for s in capability_signals(document, root=ROOT)}

    assert weights["ghost"] > weights["real"]


def test_an_environment_needing_provisioning_is_further_away() -> None:
    document = {
        "claims": [
            _claim("plain", environment="Ubuntu 24.04 with the runtime installed"),
            _claim("gated", environment="requires ISLO_API_KEY and a hosted account"),
        ]
    }

    weights = {s.key: s.weight for s in capability_signals(document, root=ROOT)}

    assert weights["gated"] > weights["plain"]


def test_github_hosted_is_not_mistaken_for_needing_provisioning() -> None:
    """The false positive this cost: "GitHub-hosted Docker" matched a "hosted" term and scored a
    claim CI already runs as if it were blocked on infrastructure."""
    document = {
        "claims": [
            _claim("docker", environment="GitHub-hosted Docker"),
            _claim("plain", environment="Ubuntu 24.04"),
        ]
    }

    weights = {s.key: s.weight for s in capability_signals(document, root=ROOT)}

    assert weights["docker"] == weights["plain"]


def test_without_a_root_the_scoring_falls_back_to_state_alone() -> None:
    """No tree to resolve against is not a reason to invent distance."""
    document = {"claims": [_claim("a", test="tests/test_nobody_wrote_this.py", environment="needs kvm hardware")]}

    (signal,) = capability_signals(document)

    assert signal.weight == 2.0  # experimental(1) + environment(1); the test term needs a root
