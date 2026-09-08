"""Durable idempotency and reconciliation primitives for external factory side effects.

The important rule is deliberately conservative: once an intent exists, its external outcome is
*unknown* until either a committed result is present or a reconciler proves what happened. A retry
may repeat a request only when the caller explicitly marks the operation replay-safe and a
reconciler has proved that the previous attempt is definitely absent.

Operation identity is immutable. Reusing one operation key for different content is rejected even
before reconciliation, because idempotency without an intent digest can silently turn a caller bug
into the wrong durable receipt. Deterministic remote resources may additionally require observation
before the first local attempt so a new epoch adopts an already-created effect instead of duplicating
it.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

OutcomeStatus = Literal["committed", "definitely_absent", "ambiguous", "divergent", "refused"]
OperationState = Literal["intent", "in_doubt", "reconciling", "committed", "exhausted", "refused"]


class OperationError(RuntimeError):
    pass


class OperationIdentityConflict(OperationError):
    """An operation key was reused for a different Cell/epoch/kind or request intent."""


class OperationInDoubt(OperationError):
    """The previous attempt may have committed externally; blind replay is unsafe."""

    def __init__(self, key: str, state: str = "in_doubt") -> None:
        super().__init__(f"operation {key} is {state}; reconcile external state before retrying")
        self.key = key
        self.state = state


class RetryBudgetExhausted(OperationError):
    pass


@dataclass(frozen=True)
class OperationRef:
    cell_id: str
    epoch: int
    kind: str
    key: str

    @classmethod
    def build(cls, cell_id: str, epoch: int, kind: str, *parts: str) -> OperationRef:
        digest = hashlib.sha256("\0".join((cell_id, str(epoch), kind, *parts)).encode()).hexdigest()[:24]
        return cls(cell_id, epoch, kind, f"{kind}:{digest}")


@dataclass(frozen=True)
class MutationOutcome:
    status: OutcomeStatus
    result: Any = None
    evidence: dict[str, Any] | None = None
    detail: str | None = None

    @property
    def terminal(self) -> bool:
        return self.status in {"committed", "divergent", "refused"}


@dataclass(frozen=True)
class RetryBudget:
    max_attempts: int = 3
    base_delay_s: float = 1.0
    max_delay_s: float = 60.0

    def delay(self, attempt: int, operation_key: str) -> float:
        """Deterministic exponential backoff with key-derived jitter (no process RNG)."""
        exponent = max(0, attempt - 1)
        raw = min(self.max_delay_s, self.base_delay_s * (2**exponent))
        digest = hashlib.sha256(f"{operation_key}:{attempt}".encode()).digest()
        jitter = int.from_bytes(digest[:2], "big") / 65535.0
        return min(self.max_delay_s, raw * (0.75 + 0.5 * jitter))


DEFAULT_BUDGETS: dict[str, RetryBudget] = {
    "airflow_dispatch": RetryBudget(3, 1.0, 30.0),
    "github_publish": RetryBudget(4, 1.0, 60.0),
    "github_issue": RetryBudget(4, 1.0, 60.0),
    "sandbox_cleanup": RetryBudget(5, 1.0, 60.0),
}


def budget_for(kind: str) -> RetryBudget:
    return DEFAULT_BUDGETS.get(kind, RetryBudget())


class OperationJournal:
    """SQLite local journal with crash-safe intents and bounded reconciliation metadata.

    The API intentionally stays storage-shaped so a Postgres implementation can preserve the same
    state machine. A committed operation is replayed from its stored result. Any non-terminal
    pre-existing intent is fail-closed by default.
    """

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, timeout=30, isolation_level="IMMEDIATE", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self._migrate()

    def close(self) -> None:
        with self.lock:
            self.db.close()

    def _migrate(self) -> None:
        with self.lock, self.db:
            self.db.execute(
                """CREATE TABLE IF NOT EXISTS operations(
                    operation_key TEXT PRIMARY KEY,
                    cell_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    result_json TEXT,
                    updated_at REAL NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    observation_json TEXT,
                    next_attempt_at REAL,
                    intent_digest TEXT
                )"""
            )
            columns = {row["name"] for row in self.db.execute("PRAGMA table_info(operations)")}
            additions = {
                "attempts": "INTEGER NOT NULL DEFAULT 0",
                "last_error": "TEXT",
                "observation_json": "TEXT",
                "next_attempt_at": "REAL",
                "intent_digest": "TEXT",
                "attempt_owner": "TEXT",
                "attempt_lease_until": "REAL",
            }
            for name, ddl in additions.items():
                if name not in columns:
                    self.db.execute(f"ALTER TABLE operations ADD COLUMN {name} {ddl}")
            self.db.execute("CREATE INDEX IF NOT EXISTS operations_unresolved ON operations(state, updated_at)")

    def begin(self, ref: OperationRef, *, intent_digest: str | None = None) -> str:
        """Create a durable intent if absent and verify immutable operation identity."""
        self._validate_ref(ref)
        self._validate_digest(intent_digest)
        now = time.time()
        with self.lock, self.db:
            self.db.execute(
                """INSERT OR IGNORE INTO operations(
                    operation_key,cell_id,epoch,kind,state,result_json,updated_at,attempts,intent_digest
                ) VALUES(?,?,?,?,?,?,?,?,?)""",
                (ref.key, ref.cell_id, ref.epoch, ref.kind, "intent", None, now, 0, intent_digest),
            )
            row = self.get(ref.key)
            self._assert_identity(ref, row, intent_digest=intent_digest)
            if intent_digest is not None and row.get("intent_digest") is None:
                self.db.execute(
                    "UPDATE operations SET intent_digest=?,updated_at=? WHERE operation_key=?",
                    (intent_digest, now, ref.key),
                )
        return str(self.get(ref.key)["state"])

    def start_attempt(
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

    def mark_in_doubt(self, ref: OperationRef, error: BaseException | str, *, owner: str | None = None) -> None:
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

    def mark_observation(self, ref: OperationRef, outcome: MutationOutcome) -> None:
        payload = json.dumps(
            {"status": outcome.status, "evidence": outcome.evidence, "detail": outcome.detail},
            sort_keys=True,
            separators=(",", ":"),
        )
        state: OperationState = "refused" if outcome.status == "refused" else "reconciling"
        with self.lock, self.db:
            self._assert_identity(ref, self.get(ref.key))
            self.db.execute(
                """UPDATE operations SET state=?, observation_json=?, updated_at=?
                   WHERE operation_key=? AND state!='committed'""",
                (state, payload, time.time(), ref.key),
            )

    def schedule_retry(self, ref: OperationRef, attempt: int, *, budget: RetryBudget | None = None) -> float:
        budget = budget or budget_for(ref.kind)
        when = time.time() + budget.delay(attempt, ref.key)
        with self.lock, self.db:
            self._assert_identity(ref, self.get(ref.key))
            self.db.execute(
                "UPDATE operations SET next_attempt_at=?, updated_at=? WHERE operation_key=?",
                (when, time.time(), ref.key),
            )
        return when

    def commit(self, ref: OperationRef, result: Any, *, owner: str | None = None) -> None:
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

    def get(self, key: str) -> dict[str, Any]:
        with self.lock:
            row = self.db.execute("SELECT * FROM operations WHERE operation_key=?", (key,)).fetchone()
            if row is None:
                raise KeyError(key)
            out = dict(row)
        out["result"] = json.loads(out.pop("result_json")) if out.get("result_json") else None
        out["observation"] = json.loads(out.pop("observation_json")) if out.get("observation_json") else None
        return out

    def execute(
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

    def observe(self, ref: OperationRef, observer: Callable[[], MutationOutcome]) -> MutationOutcome:
        """Record one external observation without guessing or automatically replaying the effect."""
        row = self.get(ref.key)
        self._assert_identity(ref, row)
        if row["state"] == "committed":
            return MutationOutcome("committed", row["result"], row.get("observation"), "journal is committed")
        outcome = observer()
        self.mark_observation(ref, outcome)
        if outcome.status == "committed":
            self.commit(ref, outcome.result)
        return outcome

    def unresolved(self, *, limit: int = 100, due_only: bool = False) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        query = "SELECT * FROM operations WHERE state!='committed'"
        args: list[Any] = []
        if due_only:
            query += " AND (next_attempt_at IS NULL OR next_attempt_at<=?)"
            args.append(time.time())
        query += " ORDER BY updated_at LIMIT ?"
        args.append(limit)
        with self.lock:
            rows = self.db.execute(query, args).fetchall()
        result = []
        for raw in rows:
            row = dict(raw)
            row["result"] = json.loads(row.pop("result_json")) if row.get("result_json") else None
            row["observation"] = json.loads(row.pop("observation_json")) if row.get("observation_json") else None
            result.append(row)
        return result

    @staticmethod
    def _validate_ref(ref: OperationRef) -> None:
        if not ref.cell_id.startswith("cell_"):
            raise OperationIdentityConflict("operation must carry a Factory Cell id")
        if ref.epoch < 1 or not ref.kind.strip() or not ref.key.strip():
            raise OperationIdentityConflict("operation reference is incomplete")

    @staticmethod
    def _validate_digest(intent_digest: str | None) -> None:
        if intent_digest is not None and not intent_digest.startswith("sha256:"):
            raise OperationIdentityConflict("intent digest must use sha256:<hex>")
        if intent_digest is not None:
            value = intent_digest.removeprefix("sha256:")
            if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
                raise OperationIdentityConflict("intent digest must be a lowercase SHA-256 digest")

    def _assert_identity(
        self,
        ref: OperationRef,
        row: dict[str, Any],
        *,
        intent_digest: str | None = None,
    ) -> None:
        self._validate_ref(ref)
        self._validate_digest(intent_digest)
        observed = (str(row.get("cell_id")), int(row.get("epoch", -1)), str(row.get("kind")))
        expected = (ref.cell_id, ref.epoch, ref.kind)
        if observed != expected:
            raise OperationIdentityConflict(
                f"operation key {ref.key!r} is already bound to {observed!r}, not {expected!r}"
            )
        stored_digest = row.get("intent_digest")
        if intent_digest is not None and stored_digest is not None and stored_digest != intent_digest:
            raise OperationIdentityConflict(f"operation key {ref.key!r} was reused with divergent intent")
