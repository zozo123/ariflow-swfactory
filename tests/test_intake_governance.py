from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from swfactory.intake_governance import (
    BacklogCandidate,
    ScheduleLimits,
    require_complete_bindings,
    select_backlog,
)


def test_backlog_selection_is_bounded_and_explains_skips() -> None:
    result = select_backlog(
        [
            BacklogCandidate(1, "a", "closed", 0),
            BacklogCandidate(2, "b", "open", 0, prerequisites=(9,)),
            BacklogCandidate(3, "c", "open", 1, active=True),
            BacklogCandidate(4, "d", "open", 1),
            BacklogCandidate(5, "e", "open", 2),
        ],
        completed={9},
        limit=2,
    )
    assert [item.issue for item in result.selected] == [2, 4]
    assert result.skipped[1] == "not-open"
    assert result.skipped[3] == "active-cell"
    assert result.skipped[5] == "batch-limit"


def test_schedule_limits_are_explicit_and_timezone_aware() -> None:
    limits = ScheduleLimits(
        origin=datetime(2026, 1, 1, tzinfo=UTC),
        max_active_runs=1,
        max_cells=4,
        run_timeout=timedelta(hours=20),
    )
    assert limits.origin is not None and limits.origin.tzinfo is UTC
    with pytest.raises(ValueError, match="timezone-aware"):
        ScheduleLimits(origin=datetime(2026, 1, 1), max_active_runs=1, max_cells=4, run_timeout=timedelta(hours=1))
    with pytest.raises(ValueError, match="positive"):
        ScheduleLimits(origin=None, max_active_runs=0, max_cells=4, run_timeout=timedelta(hours=1))
    with pytest.raises(ValueError, match="positive"):
        ScheduleLimits(origin=None, max_active_runs=1, max_cells=4, run_timeout=timedelta(0))


def test_the_shipped_lines_declare_their_schedule_limits() -> None:
    """The bounds ``dags/blueprints.py`` hands Airflow come from here (#2070): a cron line has an
    origin and one run at a time, a manual line has no origin and Airflow's declared default, and
    every run is capped at the life of its own sandbox."""
    from swfactory.blueprint import load

    liquid = load("liquid").schedule_limits()
    assert liquid.origin is not None and liquid.origin.tzinfo is not None
    assert liquid.max_active_runs == 1
    assert liquid.max_cells == 1
    assert liquid.run_timeout == timedelta(seconds=load("liquid").sandbox.ttl_s)
    manual = load("factory").schedule_limits()
    assert (manual.origin, manual.max_active_runs) == (None, 16)
    assert manual.run_timeout == timedelta(seconds=load("factory").sandbox.ttl_s)


def test_managed_bindings_must_cover_every_job() -> None:
    snapshot_digest = "a" * 64
    rows = require_complete_bindings(
        [
            {
                "job_idx": 0,
                "cell_id": "cell-a",
                "epoch": 1,
                "repo": "a",
                "snapshot_digest": snapshot_digest,
            },
            {
                "job_idx": 1,
                "cell_id": "cell-b",
                "epoch": 1,
                "repo": "b",
                "snapshot_digest": snapshot_digest,
            },
        ],
        2,
    )
    assert len(rows) == 2
    with pytest.raises(ValueError, match="partial"):
        require_complete_bindings([{"job_idx": 0}], 2)
