from __future__ import annotations

import pytest

from swfactory.sandbox_governance import (
    CleanupDebt,
    CleanupDecision,
    ResourceObservation,
    SandboxIdentity,
    authorize_cleanup,
)


def identity() -> SandboxIdentity:
    return SandboxIdentity("docker", "cell_0123456789abcdef01234567", 2, "attempt-1")


def observation(item: SandboxIdentity, *, running: bool = False) -> ResourceObservation:
    return ResourceObservation("container-1", item.labels, running)


def test_owned_resource_cleanup_is_epoch_and_activity_fenced() -> None:
    item = identity()
    assert authorize_cleanup(item, observation(item), current_epoch=2, active=True) == CleanupDecision.KEEP
    assert authorize_cleanup(item, observation(item), current_epoch=2, active=False) == CleanupDecision.REMOVE
    assert authorize_cleanup(item, observation(item), current_epoch=3, active=False) == CleanupDecision.REMOVE
    assert (
        authorize_cleanup(item, ResourceObservation("x", {}, False), current_epoch=2, active=False)
        == CleanupDecision.REFUSE
    )


def test_cleanup_debt_cannot_be_settled_by_another_cell() -> None:
    debt = CleanupDebt()
    item = identity()
    debt.record("container-1", item)
    with pytest.raises(RuntimeError, match="does not match"):
        debt.settle("container-1", SandboxIdentity("docker", "cell_4ce269b99ed3c09c564e4735", 2, "attempt-1"))
    debt.settle("container-1", item)
    assert not debt.outstanding
