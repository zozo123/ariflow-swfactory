from __future__ import annotations

from datetime import datetime, timezone

import pytest

from swfactory.intake_governance import GateResponse, HumanGate, authorize_gate
from swfactory.recovery_accounting import (
    Observation,
    Outcome,
    RecoveryAction,
    RemoteIdentity,
    classify_observation,
)
from swfactory.sandbox_governance import (
    CleanupDecision,
    ResourceObservation,
    SandboxIdentity,
    WorkNodeResult,
    authorize_cleanup,
    authorize_fan_in,
)


def test_stale_human_response_cannot_publish_after_cell_epoch_moves() -> None:
    policy = HumanGate("publish", True, "cell-1", 3, "a" * 64)
    stale = GateResponse(
        "publish",
        "operator",
        "approve",
        "cell-1",
        2,
        "a" * 64,
        datetime.now(timezone.utc),
    )
    with pytest.raises(PermissionError, match="stale Cell authority"):
        authorize_gate(policy, stale)


def test_lost_remote_response_is_observed_before_replay() -> None:
    expected = RemoteIdentity("github-pr", "repo", "delivery-1", "f" * 64)
    assert classify_observation(expected, Observation(Outcome.IN_DOUBT)) == RecoveryAction.OBSERVE
    assert classify_observation(expected, Observation(Outcome.COMMITTED, "0" * 64)) == RecoveryAction.REFUSE


def test_cleanup_never_deletes_unowned_resource_even_when_old_cell_is_terminal() -> None:
    identity = SandboxIdentity("islo", "cell_0123456789abcdef01234567", 5, "attempt-2")
    foreign = ResourceObservation(
        "box-9",
        {"swfactory.owned": "true", "swfactory.cell": "other"},
        False,
    )
    assert authorize_cleanup(identity, foreign, current_epoch=6, active=False) == CleanupDecision.REFUSE


def test_parallel_workers_with_observed_overlap_cannot_fan_in() -> None:
    left = WorkNodeResult(
        "left",
        True,
        frozenset({"src/x.py"}),
        frozenset({"src/x.py"}),
        "base",
        "a",
        "cell",
        1,
    )
    right = WorkNodeResult(
        "right",
        True,
        frozenset({"src/x.py"}),
        frozenset({"src/x.py"}),
        "base",
        "b",
        "cell",
        1,
    )
    with pytest.raises(RuntimeError, match="sibling-overlap"):
        authorize_fan_in([left, right])


def test_parallel_workers_from_different_cell_authority_cannot_fan_in() -> None:
    left = WorkNodeResult(
        "left",
        True,
        frozenset({"a"}),
        frozenset({"a"}),
        "base",
        "x",
        "cell-a",
        1,
    )
    right = WorkNodeResult(
        "right",
        True,
        frozenset({"b"}),
        frozenset({"b"}),
        "base",
        "y",
        "cell-b",
        1,
    )
    with pytest.raises(RuntimeError, match="authority-divergence"):
        authorize_fan_in([left, right])
