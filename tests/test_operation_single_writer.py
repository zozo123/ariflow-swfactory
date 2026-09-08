from __future__ import annotations

import threading
from pathlib import Path

import pytest

from swfactory.idempotency import (
    MutationOutcome,
    OperationIdentityConflict,
    OperationInDoubt,
    OperationJournal,
    OperationRef,
)


def _digest(ch: str) -> str:
    return "sha256:" + ch * 64


def test_two_independent_journals_execute_one_provider_effect(tmp_path: Path) -> None:
    db = tmp_path / "operations.sqlite3"
    first = OperationJournal(db)
    second = OperationJournal(db)
    ref = OperationRef("cell_concurrent", 1, "github_publish", "publish:concurrent")
    entered = threading.Event()
    release = threading.Event()
    calls: list[str] = []
    result: list[dict[str, str]] = []

    def provider() -> dict[str, str]:
        calls.append("provider")
        entered.set()
        assert release.wait(5)
        return {"url": "https://example.invalid/pr/1"}

    def run_first() -> None:
        result.append(
            first.execute(
                ref,
                provider,
                replay_safe=True,
                reconcile=lambda: MutationOutcome("definitely_absent"),
                intent_digest=_digest("a"),
            )
        )

    thread = threading.Thread(target=run_first)
    thread.start()
    assert entered.wait(5)
    try:
        with pytest.raises(OperationInDoubt, match="attempt_in_progress"):
            second.execute(
                ref,
                lambda: calls.append("duplicate") or {"url": "https://example.invalid/pr/2"},
                replay_safe=True,
                reconcile=lambda: MutationOutcome("definitely_absent"),
                intent_digest=_digest("a"),
            )
        assert calls == ["provider"]
    finally:
        release.set()
        thread.join(5)
        first.close()
        second.close()

    assert result == [{"url": "https://example.invalid/pr/1"}]

    replay = OperationJournal(db)
    try:
        assert replay.execute(ref, lambda: pytest.fail("provider replayed"), intent_digest=_digest("a")) == result[0]
    finally:
        replay.close()


def test_committed_receipt_is_immutable(tmp_path: Path) -> None:
    journal = OperationJournal(tmp_path / "operations.sqlite3")
    ref = OperationRef("cell_receipt", 1, "github_publish", "publish:receipt")
    try:
        journal.begin(ref, intent_digest=_digest("b"))
        journal.commit(ref, {"url": "one"})
        journal.commit(ref, {"url": "one"})
        with pytest.raises(OperationIdentityConflict, match="cannot be overwritten"):
            journal.commit(ref, {"url": "two"})
        assert journal.get(ref.key)["result"] == {"url": "one"}
    finally:
        journal.close()


def test_expired_attempt_stays_in_doubt_until_observed(tmp_path: Path) -> None:
    journal = OperationJournal(tmp_path / "operations.sqlite3")
    ref = OperationRef("cell_expired", 1, "github_publish", "publish:expired")
    try:
        journal.begin(ref, intent_digest=_digest("c"))
        attempt, owner = journal.start_attempt(ref, lease_s=1.0)
        assert attempt == 1 and owner
        with journal.lock, journal.db:
            journal.db.execute(
                "UPDATE operations SET attempt_lease_until=0 WHERE operation_key=?",
                (ref.key,),
            )

        observed: list[str] = []
        with pytest.raises(OperationInDoubt, match="definitely_absent"):
            journal.execute(
                ref,
                lambda: pytest.fail("expired owner must not be blindly replaced"),
                replay_safe=False,
                reconcile=lambda: observed.append("observed") or MutationOutcome("definitely_absent"),
                intent_digest=_digest("c"),
            )
        assert observed == ["observed"]
        assert journal.get(ref.key)["state"] == "reconciling"
    finally:
        journal.close()
