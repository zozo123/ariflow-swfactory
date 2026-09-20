"""The enrolment path refuses a full queue; the measurement behind it never stops.

`improve` reads the factory's own evidence and prints work orders. Nothing stops it printing the
same orders every cycle, and an operator running `--as-issues` files them -- so a loop whose queue
is not draining can bury its own human gates under its own proposals. The governor lives in
`improvement_annealing`; these tests pin the two things the CLI owes it: that the queue reaches the
annealer at all, and that a refusal lands on enrolment WITHOUT taking the assessment down with it.
"""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from swfactory.cli import app
from swfactory.improvement_annealing import DEFAULT_ENROL_CAP

ROOT = Path(__file__).resolve().parents[1]


def _improve(*args: str):
    return CliRunner().invoke(app, ["improve", "--root", str(ROOT), *args])


def test_the_default_says_nothing_about_the_queue_and_so_changes_nothing() -> None:
    quiet = _improve("--budget", "5")
    assert quiet.exit_code == 0, quiet.output
    assert "budget=5" in quiet.output
    assert "budget cut" not in quiet.output


def test_an_undrained_queue_narrows_the_proposal() -> None:
    flooded = _improve("--budget", "5", "--enrolled", "55")
    assert flooded.exit_code == 0, flooded.output
    assert "budget=1" in flooded.output
    assert "55/10 enrolled orders still open" in flooded.output
    assert "budget cut 5->1" in flooded.output


def test_a_full_queue_refuses_enrolment_but_not_assessment() -> None:
    """The distinction the issue turns on: suppress enrolment, never measurement."""
    refused = _improve("--budget", "5", "--enrolled", "55", "--as-issues")
    assert refused.exit_code == 1, refused.output
    assert "Close or drain them" in refused.output
    assert "gh issue create" not in refused.output

    # Same queue, no --as-issues: the assessment is still produced in full.
    measured = _improve("--budget", "5", "--enrolled", "55")
    assert measured.exit_code == 0, measured.output
    assert len(measured.output.strip().splitlines()) > 1, "a governed loop still reports what it measured"


def test_an_acknowledged_queue_enrols_anyway() -> None:
    acked = _improve("--budget", "5", "--enrolled", "55", "--as-issues", "--ack-queue")
    assert acked.exit_code == 0, acked.output
    assert "Close or drain them" not in acked.output


def test_under_the_cap_enrolment_needs_no_acknowledgement() -> None:
    ok = _improve("--budget", "5", "--enrolled", str(DEFAULT_ENROL_CAP - 1), "--as-issues")
    assert ok.exit_code == 0, ok.output
    assert "Close or drain them" not in ok.output


def test_the_cap_is_operator_settable_in_both_directions() -> None:
    tight = _improve("--budget", "5", "--enrolled", "3", "--enrol-cap", "3", "--as-issues")
    assert tight.exit_code == 1, tight.output
    loose = _improve("--budget", "5", "--enrolled", "3", "--enrol-cap", "99", "--as-issues")
    assert loose.exit_code == 0, loose.output
