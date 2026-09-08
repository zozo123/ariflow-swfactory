from pathlib import Path

P = Path('src/swfactory/idempotency.py')
text = P.read_text(encoding='utf-8')


def replace_once(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f'expected exactly one match, got {count}: {old[:120]!r}')
    text = text.replace(old, new, 1)


replace_once(
    'import threading\nimport time\n',
    'import threading\nimport time\nimport uuid\n',
)

replace_once(
'''            additions = {
                "attempts": "INTEGER NOT NULL DEFAULT 0",
                "last_error": "TEXT",
                "observation_json": "TEXT",
                "next_attempt_at": "REAL",
                "intent_digest": "TEXT",
            }
''',
'''            additions = {
                "attempts": "INTEGER NOT NULL DEFAULT 0",
                "last_error": "TEXT",
                "observation_json": "TEXT",
                "next_attempt_at": "REAL",
                "intent_digest": "TEXT",
                "attempt_owner": "TEXT",
                "attempt_lease_until": "REAL",
            }
''')

start = text.index('    def start_attempt(')
end = text.index('    def mark_in_doubt(', start)
text = text[:start] + '''    def start_attempt(
        self,
        ref: OperationRef,
        *,
        budget: RetryBudget | None = None,
        lease_s: float = 300.0,
    ) -> tuple[int, str]:
        """Atomically claim the one live provider attempt for an operation.

        The claim lives in SQLite rather than a process lock, so independent backend processes and
        journal connections see the same owner. An unexpired owner is never displaced. An expired
        owner is treated as an ambiguous external outcome and must be reconciled before another
        provider callback is allowed to start.
        """
        budget = budget or budget_for(ref.kind)
        owner = uuid.uuid4().hex
        now = time.time()
        with self.lock, self.db:
            row = self.get(ref.key)
            self._assert_identity(ref, row)
            if row["state"] == "committed":
                raise OperationInDoubt(ref.key, "already_committed")
            incumbent = row.get("attempt_owner")
            lease_until = float(row.get("attempt_lease_until") or 0.0)
            if incumbent:
                state = "attempt_in_progress" if lease_until > now else "expired_attempt_in_doubt"
                if lease_until <= now:
                    self.db.execute(
                        """UPDATE operations SET state='in_doubt',last_error=?,updated_at=?
                           WHERE operation_key=? AND attempt_owner=?""",
                        ("attempt lease expired; reconcile before another effect", now, ref.key, incumbent),
                    )
                raise OperationInDoubt(ref.key, state)
            retry_at = row.get("next_attempt_at")
            if retry_at is not None and float(retry_at) > now:
                raise OperationInDoubt(ref.key, "retry_not_due")
            attempts = int(row.get("attempts") or 0)
            if attempts >= budget.max_attempts:
                self.db.execute(
                    "UPDATE operations SET state='exhausted', updated_at=? WHERE operation_key=?",
                    (now, ref.key),
                )
                raise RetryBudgetExhausted(f"{ref.key}: retry budget {budget.max_attempts} exhausted")
            attempt = attempts + 1
            cur = self.db.execute(
                """UPDATE operations SET attempts=?,state='intent',last_error=NULL,next_attempt_at=NULL,
                   attempt_owner=?,attempt_lease_until=?,updated_at=?
                   WHERE operation_key=? AND state!='committed' AND attempt_owner IS NULL""",
                (attempt, owner, now + max(1.0, lease_s), now, ref.key),
            )
            if cur.rowcount != 1:
                raise OperationInDoubt(ref.key, "attempt_in_progress")
            return attempt, owner

''' + text[end:]

start = text.index('    def mark_in_doubt(')
end = text.index('    def mark_observation(', start)
text = text[:start] + '''    def mark_in_doubt(self, ref: OperationRef, error: BaseException | str, *, owner: str | None = None) -> None:
        detail = str(error)[:2000]
        with self.lock, self.db:
            self._assert_identity(ref, self.get(ref.key))
            if owner is None:
                self.db.execute(
                    """UPDATE operations SET state='in_doubt',last_error=?,updated_at=?
                       WHERE operation_key=? AND state!='committed'""",
                    (detail, time.time(), ref.key),
                )
            else:
                self.db.execute(
                    """UPDATE operations SET state='in_doubt',last_error=?,attempt_owner=NULL,
                       attempt_lease_until=NULL,updated_at=?
                       WHERE operation_key=? AND state!='committed' AND attempt_owner=?""",
                    (detail, time.time(), ref.key, owner),
                )

''' + text[end:]

start = text.index('    def commit(')
end = text.index('    def get(', start)
text = text[:start] + '''    def commit(self, ref: OperationRef, result: Any, *, owner: str | None = None) -> None:
        payload = json.dumps(result, sort_keys=True, separators=(",", ":"))
        with self.lock, self.db:
            row = self.get(ref.key)
            self._assert_identity(ref, row)
            if row["state"] == "committed":
                existing = json.dumps(row["result"], sort_keys=True, separators=(",", ":"))
                if existing != payload:
                    raise OperationIdentityConflict(f"committed operation {ref.key!r} cannot be overwritten")
                return
            if owner is not None and row.get("attempt_owner") != owner:
                raise OperationInDoubt(ref.key, "attempt_owner_changed")
            where = "operation_key=? AND state!='committed'"
            args: list[Any] = [payload, time.time(), ref.key]
            if owner is not None:
                where += " AND attempt_owner=?"
                args.append(owner)
            cur = self.db.execute(
                f"""UPDATE operations SET state='committed',result_json=?,last_error=NULL,next_attempt_at=NULL,
                    attempt_owner=NULL,attempt_lease_until=NULL,updated_at=? WHERE {where}""",
                args,
            )
            if cur.rowcount != 1:
                latest = self.get(ref.key)
                if latest["state"] == "committed" and latest["result"] == result:
                    return
                raise OperationInDoubt(ref.key, "commit_lost_attempt_owner")

''' + text[end:]

start = text.index('    def execute(')
end = text.index('    def observe(', start)
text = text[:start] + '''    def execute(
        self,
        ref: OperationRef,
        fn: Callable[[], Any],
        *,
        replay_safe: bool = False,
        reconcile: Callable[[], MutationOutcome] | None = None,
        budget: RetryBudget | None = None,
        intent_digest: str | None = None,
        observe_before_first_attempt: bool = False,
    ) -> Any:
        """Execute one provider effect with a durable cross-process attempt claim.

        A follower never interprets an in-flight owner's temporary absence from the provider as
        permission to start another effect. It either replays an immutable committed receipt or
        reports the operation in doubt. Reconciliation is required before a retry of any prior
        attempt, including an expired claim.
        """
        existed = True
        try:
            row = self.get(ref.key)
        except KeyError:
            existed = False
            self.begin(ref, intent_digest=intent_digest)
            row = self.get(ref.key)
        else:
            self._assert_identity(ref, row, intent_digest=intent_digest)
        if row["state"] == "committed":
            return row["result"]

        now = time.time()
        incumbent = row.get("attempt_owner")
        if incumbent:
            lease_until = float(row.get("attempt_lease_until") or 0.0)
            if lease_until > now:
                raise OperationInDoubt(ref.key, "attempt_in_progress")
            self.mark_in_doubt(ref, "attempt lease expired; external outcome is unknown", owner=incumbent)
            row = self.get(ref.key)
            existed = True

        should_observe = existed or observe_before_first_attempt
        if should_observe:
            if reconcile is None:
                raise OperationInDoubt(ref.key, str(row["state"]))
            outcome = reconcile()
            self.mark_observation(ref, outcome)
            if outcome.status == "committed":
                self.commit(ref, outcome.result)
                return outcome.result
            if outcome.status != "definitely_absent" or not replay_safe:
                raise OperationInDoubt(ref.key, outcome.status)

        attempt, owner = self.start_attempt(ref, budget=budget)
        try:
            result = fn()
        except BaseException as exc:
            self.mark_in_doubt(ref, exc, owner=owner)
            self.schedule_retry(ref, attempt, budget=budget)
            raise
        self.commit(ref, result, owner=owner)
        return result

''' + text[end:]

P.write_text(text, encoding='utf-8')

T = Path('tests/test_operation_single_writer.py')
T.write_text('''from __future__ import annotations

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
''', encoding='utf-8')
