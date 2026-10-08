from __future__ import annotations

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
    authorize_cleanup,
)


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
