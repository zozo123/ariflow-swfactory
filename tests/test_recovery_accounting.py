from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from swfactory.recovery_accounting import (
    AttemptLedger,
    BackupManifest,
    CallbackDebt,
    Observation,
    Outcome,
    PublicationReceipt,
    RecoveryAction,
    RemoteIdentity,
    RetryPolicy,
    SpendLedger,
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


def test_retry_policy_respects_authority_budget_backoff_and_replay_safety() -> None:
    absent = Observation(Outcome.ABSENT)
    future = datetime.now(UTC) + timedelta(minutes=1)
    assert RetryPolicy(0, 3, future, True, True).next_action(absent) == RecoveryAction.WAIT
    assert RetryPolicy(0, 3, None, False, True).next_action(absent) == RecoveryAction.REFUSE
    assert RetryPolicy(3, 3, None, True, True).next_action(absent) == RecoveryAction.REFUSE
    assert RetryPolicy(0, 3, None, True, False).next_action(absent) == RecoveryAction.REFUSE
    assert RetryPolicy(0, 3, None, True, True).next_action(absent) == RecoveryAction.RETRY


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


def test_attempt_ledger_never_replays_known_completed_attempt() -> None:
    ledger = AttemptLedger("op", 2)
    ledger.reserve("a1", input_head="h0", budget_usd=1.0)
    ledger.settle("a1", output_head="h1", receipt={"ok": True})
    with pytest.raises(ValueError, match="already exists"):
        ledger.reserve("a1", input_head="h0", budget_usd=1.0)
    ledger.reserve("a2", input_head="h1", budget_usd=1.0)
    ledger.mark_unknown("a2", "worker died after provider return")
    assert ledger.attempts["a2"]["state"] == "unknown"


def test_spend_reservation_survives_unknown_result() -> None:
    ledger = SpendLedger(10.0)
    ledger.reserve("call-1", 4.0)
    ledger.settle("call-1", 3.0)
    ledger.reserve("call-2", 4.0)
    ledger.lose_result("call-2")
    assert ledger.committed_usd == 3.0
    assert "call-2" in ledger.unknown
    with pytest.raises(ValueError, match="duplicate"):
        ledger.reserve("call-2", 1.0)


def test_callback_reconciliation_is_epoch_fenced_and_idempotent() -> None:
    debt = CallbackDebt("cell", 2, "run", "build", "failed")
    assert reconcile_callback(2, debt, "failed") == RecoveryAction.ADOPT
    assert reconcile_callback(2, debt, None) == RecoveryAction.OBSERVE
    assert reconcile_callback(3, debt, "failed") == RecoveryAction.REFUSE


def test_backup_manifest_refuses_partial_or_mixed_schema() -> None:
    manifest = BackupManifest(2, datetime.now(UTC), {"cells": "a", "operations": "b"}, "f" * 64)
    manifest.validate(expected_schema=2, required_stores={"cells", "operations"})
    with pytest.raises(RuntimeError, match="unsupported"):
        manifest.validate(expected_schema=3, required_stores={"cells"})
    with pytest.raises(RuntimeError, match="missing stores"):
        manifest.validate(expected_schema=2, required_stores={"cells", "admission", "operations"})
