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

    assert heat.phase == "exploiting"
    assert heat.budget == 5
    assert heat.readmit_stalled is False


def test_a_hard_backlog_is_not_by_itself_a_reason_to_thrash() -> None:
    """A loop can carry plenty of stalled debt and still be cool while it retires something each
    cycle. Progress is the evidence that matters; a difficult backlog is not a crisis."""
    heat = evaluate(_obs(cycles_since_retirement=0, stalled=6, carried=7))

    assert heat.phase == "exploiting"


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

    assert cold.phase == "exploiting"
    assert hot.phase == "exploring"
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
