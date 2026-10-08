from __future__ import annotations

from swfactory.recovery_accounting import (
    CallbackDebt,
    Observation,
    Outcome,
    PublicationReceipt,
    RecoveryAction,
    RemoteIdentity,
    classify_observation,
    reconcile_callback,
)


def identity() -> RemoteIdentity:
    return RemoteIdentity("publication", "zozo123/ariflow-swfactory", "issue-1", "f" * 64)


def test_existing_remote_effect_is_adopted_only_when_identity_matches() -> None:
    expected = identity()
    assert classify_observation(expected, Observation(Outcome.COMMITTED, expected.digest)) == RecoveryAction.ADOPT
    assert classify_observation(expected, Observation(Outcome.COMMITTED, "0" * 64)) == RecoveryAction.REFUSE
    assert classify_observation(expected, Observation(Outcome.IN_DOUBT)) == RecoveryAction.OBSERVE


def test_publication_receipt_binds_immutable_git_identity() -> None:
    expected = identity()
    receipt = PublicationReceipt(
        repository=expected.repository,
        base_revision="base",
        head_revision="head",
        content_digest=expected.immutable_content,
        branch="factory/1",
        pr_number=1,
        pr_state="merged",
    )
    assert receipt.verify(expected)
    bad = PublicationReceipt(expected.repository, "base", "head", "0" * 64, "factory/1", 1, "open")
    assert not bad.verify(expected)


def test_callback_reconciliation_is_epoch_fenced_and_idempotent() -> None:
    debt = CallbackDebt("cell", 2, "run", "build", "failed")
    assert reconcile_callback(2, debt, "failed") == RecoveryAction.ADOPT
    assert reconcile_callback(2, debt, None) == RecoveryAction.OBSERVE
    assert reconcile_callback(3, debt, "failed") == RecoveryAction.REFUSE
