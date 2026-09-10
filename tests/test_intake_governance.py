from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from swfactory.intake_governance import (
    AcceptedSnapshot,
    BacklogCandidate,
    CellBinding,
    GateResponse,
    HumanGate,
    ManagedWorkOrder,
    ScheduleLimits,
    WorkOrderState,
    authorize_gate,
    cross_channel_key,
    require_complete_bindings,
    select_backlog,
)


def snapshot() -> AcceptedSnapshot:
    return AcceptedSnapshot(
        issue_ref="2034",
        issue_revision="rev-1",
        issue_body="body",
        blueprint="liquid",
        blueprint_revision="bp-1",
        policy={"human_gate": True},
        target="zozo123/ariflow-swfactory",
        base_revision="deadbeef",
    )


def test_managed_work_order_binds_every_cell_to_one_snapshot() -> None:
    accepted = snapshot()
    order = ManagedWorkOrder(
        request_id="req-1",
        actor="operator",
        source="cron",
        snapshot=accepted,
        bindings=(CellBinding(0, "cell-1", 1, "repo", accepted.digest),),
    )
    order.validate_bindings()
    assert order.transition(WorkOrderState.ADMITTED).state == WorkOrderState.ADMITTED
    with pytest.raises(ValueError):
        order.transition(WorkOrderState.BOUND)


def test_human_gate_rejects_auto_and_stale_artifacts() -> None:
    policy = HumanGate("publish", True, "cell-1", 2, "a" * 64)
    now = datetime.now(UTC)
    with pytest.raises(PermissionError, match="automatically"):
        authorize_gate(policy, GateResponse("publish", "auto", "approve", "cell-1", 2, "a" * 64, now))
    with pytest.raises(PermissionError, match="stale artifacts"):
        authorize_gate(policy, GateResponse("publish", "yossi", "approve", "cell-1", 2, "b" * 64, now))
    assert (
        authorize_gate(policy, GateResponse("publish", "yossi", "approve", "cell-1", 2, "a" * 64, now)).actor == "yossi"
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


def test_cross_channel_dedupe_and_complete_bindings() -> None:
    accepted = snapshot()
    assert cross_channel_key("liquid", accepted.digest) == cross_channel_key("liquid", accepted.digest)
    rows = require_complete_bindings(
        [
            {
                "job_idx": 0,
                "cell_id": "cell-a",
                "epoch": 1,
                "repo": "a",
                "snapshot_digest": accepted.digest,
            },
            {
                "job_idx": 1,
                "cell_id": "cell-b",
                "epoch": 1,
                "repo": "b",
                "snapshot_digest": accepted.digest,
            },
        ],
        2,
    )
    assert len(rows) == 2
    with pytest.raises(ValueError, match="partial"):
        require_complete_bindings([{"job_idx": 0}], 2)
