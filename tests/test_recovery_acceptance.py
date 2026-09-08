"""Acceptance for the `recovery.external-effects` claim, over the two modules it names.

The claim is: *ambiguous effects remain in_doubt until observation proves committed or definitely
absent; stale epochs cannot repair or replay them.*  Proving that needs both halves of the runtime
entry together — a real ``OperationJournal`` writing the durable rows, and ``plan_recovery`` reading
those exact rows back, as ``swfactory.backend.core_service.operator_projection`` does — because the
dangerous gap is between them: a decision function that classifies a journal state it never sees.

Every row here therefore comes out of the journal, never out of a hand-written dictionary.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from swfactory.idempotency import (
    MutationOutcome,
    OperationInDoubt,
    OperationJournal,
    OperationRef,
    RetryBudget,
    RetryBudgetExhausted,
)
from swfactory.operation_recovery import RecoveryAction, plan_recovery

CELL = "cell_acceptance"
EPOCH = 1


@pytest.fixture
def journal(tmp_path: Path) -> Iterator[OperationJournal]:
    value = OperationJournal(tmp_path / "operations.db")
    try:
        yield value
    finally:
        value.close()


def _ref(kind: str = "github_publish") -> OperationRef:
    return OperationRef.build(CELL, EPOCH, kind, "pull-request")


def _row(journal: OperationJournal, ref: OperationRef) -> dict:
    """The unresolved row exactly as the operator projection hands it to ``plan_recovery``."""
    rows = [row for row in journal.unresolved() if row["operation_key"] == ref.key]
    assert rows, f"{ref.key} is no longer unresolved"
    return rows[0]


def _interrupt(journal: OperationJournal, ref: OperationRef, *, budget: RetryBudget | None = None) -> None:
    """Drive one external effect that dies mid-flight: committed remotely or not, nobody knows."""

    def publish() -> dict:
        raise ConnectionError("socket closed after the request was sent")

    with pytest.raises(ConnectionError):
        journal.execute(ref, publish, budget=budget)


def test_an_interrupted_effect_is_durably_in_doubt_and_waits_out_its_backoff(journal: OperationJournal) -> None:
    ref = _ref()
    _interrupt(journal, ref)

    row = _row(journal, ref)
    assert row["state"] == "in_doubt"

    decision = plan_recovery(row, current_epoch=EPOCH, now=row["next_attempt_at"] - 1)

    assert decision.action is RecoveryAction.WAIT
    assert decision.retry_at == row["next_attempt_at"]


def test_an_in_doubt_effect_must_be_observed_before_anything_replays_it(journal: OperationJournal) -> None:
    """The whole claim. ``in_doubt`` is the state the journal writes when an effect may have
    committed remotely; planning a plain retry for it would replay a live GitHub mutation."""
    ref = _ref()
    _interrupt(journal, ref)
    row = _row(journal, ref)

    decision = plan_recovery(row, current_epoch=EPOCH, now=row["next_attempt_at"] + 1)

    assert decision.action is RecoveryAction.OBSERVE
    assert decision.reason == "must_observe_before_retry"


def test_an_observation_that_stays_ambiguous_never_becomes_a_retry(journal: OperationJournal) -> None:
    """An observation is only progress when it is conclusive; ambiguity has to loop back."""
    ref = _ref()
    _interrupt(journal, ref)
    journal.mark_observation(ref, MutationOutcome("ambiguous", detail="search returned two candidates"))

    row = _row(journal, ref)
    decision = plan_recovery(row, current_epoch=EPOCH, now=row["next_attempt_at"] + 1)

    assert row["state"] == "reconciling"
    assert decision.action is RecoveryAction.OBSERVE


def test_an_observation_proving_absence_releases_a_bounded_retry(journal: OperationJournal) -> None:
    ref = _ref()
    _interrupt(journal, ref)
    journal.mark_observation(ref, MutationOutcome("definitely_absent", detail="no pull request with that head"))

    row = _row(journal, ref)
    decision = plan_recovery(row, current_epoch=EPOCH, now=row["next_attempt_at"] + 1)

    assert decision.action is RecoveryAction.RETRY
    assert decision.reason == "retryable_pending_operation"


def test_a_replay_after_proven_absence_runs_the_effect_exactly_once(journal: OperationJournal) -> None:
    """The end-to-end shape of a recovered publication: one external call, one committed result."""
    ref = _ref()
    calls = 0

    def publish() -> dict:
        nonlocal calls
        calls += 1
        return {"url": "https://github.com/acme/repo/pull/7"}

    _interrupt(journal, ref)
    with pytest.raises(OperationInDoubt):
        journal.execute(ref, publish, replay_safe=True, reconcile=lambda: MutationOutcome("ambiguous"))

    result = journal.execute(
        ref,
        publish,
        replay_safe=True,
        reconcile=lambda: MutationOutcome("definitely_absent"),
    )

    assert calls == 1
    assert result["url"].endswith("/7")
    assert journal.get(ref.key)["state"] == "committed"
    assert not [row for row in journal.unresolved() if row["operation_key"] == ref.key]


def test_an_effect_observed_as_committed_is_adopted_rather_than_repeated(journal: OperationJournal) -> None:
    ref = _ref()
    _interrupt(journal, ref)

    outcome = journal.observe(ref, lambda: MutationOutcome("committed", {"url": "https://example/pull/9"}))
    row = journal.get(ref.key)

    assert outcome.status == "committed"
    assert row["state"] == "committed"
    assert plan_recovery(row, current_epoch=EPOCH).action is RecoveryAction.COMMITTED


def test_a_stale_epoch_can_neither_repair_nor_replay_the_operation(journal: OperationJournal) -> None:
    """Fencing outranks recovery: a superseded worker must not resume someone else's mutation."""
    ref = _ref()
    _interrupt(journal, ref)

    decision = plan_recovery(_row(journal, ref), current_epoch=EPOCH + 1)

    assert decision.action is RecoveryAction.REFUSE
    assert decision.reason == "stale_epoch"


def test_an_operation_the_journal_has_exhausted_is_dead_not_retried(journal: OperationJournal) -> None:
    """The planner has to speak the journal's vocabulary: ``exhausted`` is a terminal row, and
    planning another attempt for it only hands the reconciler work the journal will refuse."""
    ref = _ref("sandbox_cleanup")
    budget = RetryBudget(max_attempts=1, base_delay_s=0.0)
    _interrupt(journal, ref, budget=budget)
    with pytest.raises(RetryBudgetExhausted):
        journal.start_attempt(ref, budget=budget)

    row = _row(journal, ref)
    decision = plan_recovery(row, current_epoch=EPOCH, now=row["next_attempt_at"] + 1)

    assert row["state"] == "exhausted"
    assert decision.action is RecoveryAction.DEAD
    assert decision.reason == "exhausted"


def test_the_exhausted_marker_survives_the_process_that_wrote_it(tmp_path: Path) -> None:
    """Recovery runs after a restart, so a terminal state that lives only in a rolled-back
    transaction is the same as no state at all: the row would look retryable forever."""
    path = tmp_path / "operations.db"
    ref = _ref("sandbox_cleanup")
    budget = RetryBudget(max_attempts=1, base_delay_s=0.0)
    first = OperationJournal(path)
    try:
        _interrupt(first, ref, budget=budget)
        with pytest.raises(RetryBudgetExhausted):
            first.start_attempt(ref, budget=budget)
    finally:
        first.close()

    reopened = OperationJournal(path)
    try:
        row = reopened.get(ref.key)
    finally:
        reopened.close()

    assert row["state"] == "exhausted"
    assert plan_recovery(row, current_epoch=EPOCH).action is RecoveryAction.DEAD


def test_a_spent_retry_budget_in_the_projected_row_is_dead(journal: OperationJournal) -> None:
    ref = _ref()
    _interrupt(journal, ref)
    row = _row(journal, ref)

    decision = plan_recovery({**row, "max_attempts": 1}, current_epoch=EPOCH, now=row["next_attempt_at"] + 1)

    assert row["attempts"] == 1
    assert decision.action is RecoveryAction.DEAD
    assert decision.reason == "retry_budget_exhausted"


def test_a_row_without_a_usable_epoch_is_dead_not_retryable() -> None:
    """Recovery reads durable rows; a row that lost its fencing identity cannot be trusted."""
    decision = plan_recovery({"operation_key": "github_publish:abc", "epoch": None}, current_epoch=EPOCH)

    assert decision.action is RecoveryAction.DEAD
    assert decision.reason == "invalid_epoch"


def test_the_recovery_plan_carries_the_target_an_operator_has_to_look_at(journal: OperationJournal) -> None:
    ref = _ref()
    _interrupt(journal, ref)
    row = _row(journal, ref)
    target = {"kind": "github_pull_request", "identity": {"repo": "acme/repo", "head": "factory/42"}}

    decision = plan_recovery({**row, "target": target}, current_epoch=EPOCH, now=row["next_attempt_at"] + 1)

    assert decision.target is not None
    assert decision.to_dict()["target"] == target
    assert decision.target.digest().startswith("target:")


def test_an_expired_lease_is_recorded_before_the_refusal_is_raised(tmp_path: Path) -> None:
    """The same defect as the exhausted marker, in the other branch of `start_attempt`.

    `with self.db` rolls back on any exception, so a branch that writes a durable fact and then
    raises loses the fact. `start_attempt` records `in_doubt` when it finds an expired lease and
    then refuses the caller -- and the refusal used to discard the record. The next process to open
    the journal would see the operation as it was before, with a stale owner and no evidence that
    anyone had noticed, which is how an ambiguous remote effect turns back into a blind retry.
    """
    db = tmp_path / "ops.db"
    ref = OperationRef("cell_1", 1, "github_publish", "publish:lease")

    first = OperationJournal(db)
    first.begin(ref)
    _, owner = first.start_attempt(ref, lease_s=1.0)
    # Expire the lease the way a dead process would leave it: the row keeps its owner.
    first.db.execute("UPDATE operations SET attempt_lease_until=? WHERE operation_key=?", (time.time() - 60, ref.key))
    first.db.commit()

    with pytest.raises(OperationInDoubt):
        first.start_attempt(ref)
    first.close()

    reopened = OperationJournal(db)
    try:
        row = reopened.get(ref.key)
        assert row["state"] == "in_doubt", f"the refusal rolled its own record back: {row['state']!r}"
        assert "lease expired" in str(row["last_error"])
        assert row["attempt_owner"] == owner, "the dead owner must stay visible until it is reconciled"
    finally:
        reopened.close()
