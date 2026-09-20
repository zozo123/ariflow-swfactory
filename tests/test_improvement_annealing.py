"""Annealing the improvement loop's own schedule: when to exploit, when to explore.

`self_improvement` proposed the same way whether it was retiring debt every cycle or had retired
none in twenty -- demote whatever stalled, propose the next-heaviest, forever. That is a loop with
no escape from a local minimum. These tests pin the escape and, more importantly, pin that it stays
a diagnostic: temperature shapes a proposal and can never weaken a gate.
"""

from __future__ import annotations

import pytest

from swfactory.improvement_annealing import (
    EXPLORING_AT,
    READMIT_AT,
    LoopObservation,
    evaluate,
    observe,
)


def _obs(**kw) -> LoopObservation:
    base = {"cycles": 10, "cycles_since_retirement": 0, "stalled": 0, "carried": 7, "sources": 2}
    return LoopObservation(**{**base, **kw})


# ------------------------------------------------------------------ exploit while it is working


def test_a_loop_that_is_retiring_debt_stays_cool_and_narrow() -> None:
    heat = evaluate(_obs(cycles_since_retirement=0), base_budget=5)

    assert heat.mode == "exploiting"
    assert heat.budget == 5
    assert heat.readmit_stalled is False


def test_a_hard_backlog_is_not_by_itself_a_reason_to_thrash() -> None:
    """A loop can carry plenty of stalled debt and still be cool while it retires something each
    cycle. Progress is the evidence that matters; a difficult backlog is not a crisis."""
    heat = evaluate(_obs(cycles_since_retirement=0, stalled=6, carried=7))

    assert heat.mode == "exploiting"


def test_with_no_trajectory_the_loop_does_not_guess() -> None:
    """One recorded assessment has no predecessor, so nothing is known about retirement yet."""
    assert "no trajectory yet" in evaluate(_obs(cycles=1)).reason
    assert evaluate(_obs(cycles=0)).temperature == 0.0


# ------------------------------------------------------------------------- explore when stuck


def test_barren_cycles_heat_the_loop_monotonically() -> None:
    temps = [evaluate(_obs(cycles_since_retirement=n)).temperature for n in range(0, 9)]

    assert temps == sorted(temps)
    assert temps[0] < temps[-1]


def test_it_starts_exploring_once_it_has_stopped_retiring_anything() -> None:
    cold = evaluate(_obs(cycles_since_retirement=1))
    hot = evaluate(_obs(cycles_since_retirement=6, stalled=6, carried=7))

    assert cold.mode == "exploiting"
    assert hot.mode == "exploring"
    assert hot.budget > cold.budget


def test_re_admission_needs_a_higher_bar_than_widening() -> None:
    """Widening the search is cheap. Returning to an item already proven stuck should need real
    evidence, so the two thresholds are deliberately not the same number."""
    assert READMIT_AT > EXPLORING_AT

    warm = evaluate(_obs(cycles_since_retirement=4, stalled=2, carried=7))
    stuck = evaluate(_obs(cycles_since_retirement=9, stalled=7, carried=7))

    assert warm.readmit_stalled is False
    assert stuck.readmit_stalled is True


def test_heat_saturates_rather_than_running_away() -> None:
    """Twenty barren cycles are not twenty times hotter than four: past a point the loop is simply
    stuck, and more heat buys nothing."""
    assert evaluate(_obs(cycles_since_retirement=500, stalled=7, carried=7)).temperature <= 1.0


def test_the_budget_never_more_than_doubles() -> None:
    """An unbounded budget is not exploration; it is the whole backlog proposed at once, which is
    the same as proposing nothing."""
    hottest = evaluate(_obs(cycles_since_retirement=999, stalled=7, carried=7), base_budget=5)

    assert hottest.budget <= 10


# ------------------------------------------------------------------------------- it is a diagnostic


def test_the_policy_declares_it_shapes_proposals_and_nothing_else() -> None:
    """The same discipline `liquid_annealing` and `exploration_entropy` hold: these numbers carry
    no authority. A hotter loop asks for different work; it never lowers the bar for finishing it."""
    assert evaluate(_obs()).to_dict()["authority"] == "proposal-shaping-only"


def test_evaluate_is_deterministic() -> None:
    """No clock, no randomness, no I/O -- the same trajectory must always yield the same policy."""
    assert evaluate(_obs(cycles_since_retirement=4)) == evaluate(_obs(cycles_since_retirement=4))


def test_counts_cannot_be_negative() -> None:
    with pytest.raises(ValueError, match="counts"):
        evaluate(_obs(stalled=-1))


def test_a_budget_below_one_is_refused() -> None:
    with pytest.raises(ValueError, match="at least one"):
        evaluate(_obs(), base_budget=0)


# ------------------------------------------------------------------ reading a real trajectory


def _entry(n: int) -> dict:
    return {"signals": [{"source": "capability", "key": f"k{i}", "weight": 1} for i in range(n)]}


def test_observe_counts_back_to_the_last_time_debt_shrank() -> None:
    """Retirement seen from outside: an assessment carrying strictly more debt than the next one."""
    history = [_entry(9), _entry(7), _entry(7), _entry(7)]

    assert observe(history, carried=7, stalled=0, sources=1).cycles_since_retirement == 2


def test_observe_reports_a_fresh_retirement_as_zero() -> None:
    history = [_entry(9), _entry(8)]

    assert observe(history, carried=8, stalled=0, sources=1).cycles_since_retirement == 0


def test_observe_on_an_empty_trajectory_is_cold() -> None:
    assert evaluate(observe([], carried=7, stalled=0, sources=1)).temperature == 0.0


def test_growing_debt_does_not_count_as_retirement() -> None:
    history = [_entry(5), _entry(6), _entry(7)]

    assert observe(history, carried=7, stalled=0, sources=1).cycles_since_retirement == 2


def test_a_quiet_queue_changes_nothing_about_the_annealed_budget() -> None:
    """The default must be a no-op: an operator who says nothing has not said the queue is empty."""
    for since in (0, 2, 5, 20):
        observation = LoopObservation(cycles=8, cycles_since_retirement=since, stalled=1, carried=6)
        assert observation.enrolled == 0
        heat = evaluate(observation, base_budget=5)
        assert heat.budget == heat.annealed_budget
        assert heat.pressure == 0.0
        assert heat.enrol_allowed is True
        assert "budget cut" not in heat.reason


def test_a_full_queue_narrows_a_loop_that_heat_alone_would_widen() -> None:
    """The whole point of the term: stagnation says widen, an undrained queue says stop asking."""
    stuck = LoopObservation(cycles=9, cycles_since_retirement=8, stalled=5, carried=6)
    hot = evaluate(stuck, base_budget=5)
    assert hot.mode == "exploring"
    assert hot.budget > 5, "a stagnant loop should widen when nothing is queued against it"

    flooded = evaluate(
        LoopObservation(cycles=9, cycles_since_retirement=8, stalled=5, carried=6, enrolled=55, enrol_cap=10),
        base_budget=5,
    )
    assert flooded.annealed_budget == hot.budget, "pressure must not change what the heat asked for"
    assert flooded.budget == 1, (
        "at a full queue the budget is one WHATEVER the heat says. Asserting only `< base_budget` "
        "let a subtractive governor pass: `annealed - pressure * base` also lands under base here, "
        "while still widening with heat. Pressure scales the budget, it does not offset it."
    )
    assert flooded.temperature == hot.temperature, "pressure is not heat; collapsing them hides why"
    assert flooded.readmit_stalled is hot.readmit_stalled, "pressure must not stop re-scoping what is stuck"


def test_the_governor_never_proposes_nothing() -> None:
    """A budget of zero is a loop gone dark, which is worse than the flood it is governing."""
    for enrolled in (10, 50, 5000):
        heat = evaluate(
            LoopObservation(cycles=4, cycles_since_retirement=0, stalled=0, carried=3, enrolled=enrolled, enrol_cap=10),
            base_budget=5,
        )
        assert heat.budget >= 1
        assert heat.pressure == 1.0
        assert heat.enrol_allowed is False


def test_pressure_scales_the_budget_rather_than_offsetting_it() -> None:
    """Half a queue must cost half the budget at any heat -- not a fixed number of orders.

    A subtractive governor (`annealed - k`) narrows too, so narrowing alone proves nothing. What
    separates them is whether the cut tracks the budget: under a subtractive rule a hot loop keeps
    most of its widening at half pressure, which is exactly the flood this term exists to stop.
    """
    for since, stalled in ((0, 0), (4, 2), (12, 6)):
        quiet = evaluate(
            LoopObservation(cycles=9, cycles_since_retirement=since, stalled=stalled, carried=8),
            base_budget=10,
        )
        half = evaluate(
            LoopObservation(
                cycles=9, cycles_since_retirement=since, stalled=stalled, carried=8, enrolled=5, enrol_cap=10
            ),
            base_budget=10,
        )
        assert half.budget == round(quiet.budget * 0.5), (
            f"at heat {quiet.temperature} a half-full queue must halve {quiet.budget}, not shave a constant"
        )


def test_pressure_is_linear_rather_than_a_cliff() -> None:
    """The loop should narrow as the queue fills, not run at full width until one issue tips it."""
    budgets = [
        evaluate(
            LoopObservation(cycles=4, cycles_since_retirement=0, stalled=0, carried=3, enrolled=n, enrol_cap=10),
            base_budget=10,
        ).budget
        for n in range(0, 11)
    ]
    assert budgets[0] == 10
    assert budgets[-1] == 1
    assert budgets == sorted(budgets, reverse=True), "adding an open order must never widen the budget"
    assert len(set(budgets)) > 3, "a term with only two values is a cliff wearing a ramp's name"


def test_the_cap_boundary_is_at_the_cap_not_past_it() -> None:
    at_cap = LoopObservation(cycles=4, cycles_since_retirement=0, stalled=0, carried=3, enrolled=10, enrol_cap=10)
    under = LoopObservation(cycles=4, cycles_since_retirement=0, stalled=0, carried=3, enrolled=9, enrol_cap=10)
    assert evaluate(at_cap).enrol_allowed is False
    assert evaluate(under).enrol_allowed is True


def test_an_enrolment_cap_below_one_is_refused() -> None:
    """A cap of zero would refuse every proposal forever, which is a dead loop, not a governed one."""
    with pytest.raises(ValueError, match="refuse every proposal"):
        LoopObservation(cycles=1, cycles_since_retirement=0, stalled=0, carried=1, enrol_cap=0).validate()
    with pytest.raises(ValueError, match="counts and cannot be negative"):
        LoopObservation(cycles=1, cycles_since_retirement=0, stalled=0, carried=1, enrolled=-1).validate()


def test_observe_carries_the_queue_through_because_a_trajectory_cannot_know_it() -> None:
    history = [{"signals": [1, 2, 3]}, {"signals": [1, 2, 3]}]
    assert observe(history, carried=3, stalled=0, sources=1).enrolled == 0
    passed = observe(history, carried=3, stalled=0, sources=1, enrolled=7, enrol_cap=12)
    assert (passed.enrolled, passed.enrol_cap) == (7, 12)


def test_the_reason_says_the_budget_was_cut_and_by_how_much() -> None:
    """A governor that silently narrows is indistinguishable from a loop with nothing left to say."""
    heat = evaluate(
        LoopObservation(cycles=9, cycles_since_retirement=8, stalled=5, carried=6, enrolled=8, enrol_cap=10),
        base_budget=5,
    )
    assert "re-admit what is being avoided" in heat.reason, "the heat reason must survive"
    assert f"budget cut {heat.annealed_budget}->{heat.budget}" in heat.reason
    assert "8/10 enrolled" in heat.reason
    assert heat.enrol_allowed is True
    assert "acknowledge the queue" not in heat.reason, "under the cap, enrolment is not refused"


def test_the_receipt_carries_the_queue_it_was_governed_by() -> None:
    heat = evaluate(
        LoopObservation(cycles=4, cycles_since_retirement=0, stalled=0, carried=3, enrolled=6, enrol_cap=10),
        base_budget=5,
    )
    receipt = heat.to_dict()
    assert receipt["authority"] == "proposal-shaping-only"
    assert receipt["schema_version"] == 3 and "mode" in receipt and "phase" not in receipt
    assert receipt["enrolled"] == 6 and receipt["enrol_cap"] == 10
    assert receipt["annealed_budget"] == 5 and receipt["budget"] == 2
    assert receipt["pressure"] == 0.6
