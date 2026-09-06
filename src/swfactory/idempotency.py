"""Durable idempotency and reconciliation primitives for external factory side effects.

The important rule is deliberately conservative: once an intent exists, its external outcome is
*unknown* until either a committed result is present or a reconciler proves what happened.  A
retry may repeat a request only when the caller explicitly marks the operation replay-safe and a
reconciler has proved that the previous attempt is definitely absent.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

OutcomeStatus = Literal["committed", "definitely_absent", "ambiguous", "divergent", "refused"]
OperationState = Literal["intent", "in_doubt", "reconciling", "committed", "exhausted", "refused"]


class OperationError(RuntimeError):
    pass


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
        digest = hashlib.sha256(
            "\0".join((cell_id, str(epoch), kind, *parts)).encode()
        ).hexdigest()[:24]
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
    "sandbox_cleanup": RetryBudget(5, 1.0, 60.0),
}


def budget_for(kind: str) -> RetryBudget:
    return DEFAULT_BUDGETS.get(kind, RetryBudget())


class OperationJournal:
    """SQLite local journal with crash-safe intents and bounded reconciliation metadata.

    The API intentionally stays storage-shaped so a Postgres implementation can preserve the same
    state machine.  A committed operation is replayed from its stored result.  Any non-terminal
    pre-existing intent is fail-closed by default.
    """

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self.db = sqlite3.connect(
            path, timeout=30, isolation_level="IMMEDIATE", check_same_thread=False
        )
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
                    next_attempt_at REAL
                )"""
            )
            columns = {row["name"] for row in self.db.execute("PRAGMA table_info(operations)")}
            additions = {
                "attempts": "INTEGER NOT NULL DEFAULT 0",
                "last_error": "TEXT",
                "observation_json": "TEXT",
                "next_attempt_at": "REAL",
            }
            for name, ddl in additions.items():
                if name not in columns:
                    self.db.execute(f"ALTER TABLE operations ADD COLUMN {name} {ddl}")
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS operations_unresolved ON operations(state, updated_at)"
            )

    def begin(self, ref: OperationRef) -> str:
        """Create the durable intent if absent and return the current state."""
        now = time.time()
        with self.lock, self.db:
            self.db.execute(
                """INSERT OR IGNORE INTO operations(
                    operation_key,cell_id,epoch,kind,state,result_json,updated_at,attempts
                ) VALUES(?,?,?,?,?,?,?,0)""",
                (ref.key, ref.cell_id, ref.epoch, ref.kind, "intent", None, now),
            )
        return str(self.get(ref.key)["state"])

    def start_attempt(self, ref: OperationRef, *, budget: RetryBudget | None = None) -> int:
        budget = budget or budget_for(ref.kind)
        with self.lock, self.db:
            row = self.get(ref.key)
            attempts = int(row.get("attempts") or 0)
            if attempts >= budget.max_attempts:
                self.db.execute(
                    "UPDATE operations SET state='exhausted', updated_at=? WHERE operation_key=?",
                    (time.time(), ref.key),
                )
                raise RetryBudgetExhausted(
                    f"{ref.key}: retry budget {budget.max_attempts} exhausted"
                )
            attempt = attempts + 1
            self.db.execute(
                """UPDATE operations SET attempts=?, state='intent', last_error=NULL,
                   next_attempt_at=NULL, updated_at=? WHERE operation_key=?""",
                (attempt, time.time(), ref.key),
            )
            return attempt

    def mark_in_doubt(self, ref: OperationRef, error: BaseException | str) -> None:
        detail = str(error)[:2000]
        with self.lock, self.db:
            self.db.execute(
                """UPDATE operations SET state='in_doubt', last_error=?, updated_at=?
                   WHERE operation_key=? AND state!='committed'""",
                (detail, time.time(), ref.key),
            )

    def mark_observation(self, ref: OperationRef, outcome: MutationOutcome) -> None:
        payload = json.dumps(
            {
                "status": outcome.status,
                "evidence": outcome.evidence,
                "detail": outcome.detail,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        state: OperationState = "refused" if outcome.status == "refused" else "reconciling"
        with self.lock, self.db:
            self.db.execute(
                """UPDATE operations SET state=?, observation_json=?, updated_at=?
                   WHERE operation_key=? AND state!='committed'""",
                (state, payload, time.time(), ref.key),
            )

    def schedule_retry(
        self, ref: OperationRef, attempt: int, *, budget: RetryBudget | None = None
    ) -> float:
        budget = budget or budget_for(ref.kind)
        when = time.time() + budget.delay(attempt, ref.key)
        with self.lock, self.db:
            self.db.execute(
                "UPDATE operations SET next_attempt_at=?, updated_at=? WHERE operation_key=?",
                (when, time.time(), ref.key),
            )
        return when

    def commit(self, ref: OperationRef, result: Any) -> None:
        payload = json.dumps(result, sort_keys=True, separators=(",", ":"))
        with self.lock, self.db:
            cur = self.db.execute(
                """UPDATE operations SET state='committed', result_json=?, last_error=NULL,
                   next_attempt_at=NULL, updated_at=? WHERE operation_key=?""",
                (payload, time.time(), ref.key),
            )
            if cur.rowcount != 1:
                raise KeyError(ref.key)

    def get(self, key: str) -> dict[str, Any]:
        with self.lock:
            row = self.db.execute(
                "SELECT * FROM operations WHERE operation_key=?", (key,)
            ).fetchone()
            if row is None:
                raise KeyError(key)
            out = dict(row)
        out["result"] = json.loads(out.pop("result_json")) if out.get("result_json") else None
        out["observation"] = (
            json.loads(out.pop("observation_json")) if out.get("observation_json") else None
        )
        return out

    def execute(
        self,
        ref: OperationRef,
        fn: Callable[[], Any],
        *,
        replay_safe: bool = False,
        reconcile: Callable[[], MutationOutcome] | None = None,
        budget: RetryBudget | None = None,
    ) -> Any:
        """Execute once, replay committed output, otherwise reconcile before any retry.

        `replay_safe=True` alone is intentionally insufficient: a previous attempt can be replayed
        only after `reconcile()` proves it is definitely absent.
        """
        existed = True
        try:
            row = self.get(ref.key)
        except KeyError:
            existed = False
            self.begin(ref)
            row = self.get(ref.key)
        if row["state"] == "committed":
            return row["result"]
        if existed:
            if reconcile is None:
                raise OperationInDoubt(ref.key, str(row["state"]))
            outcome = reconcile()
            self.mark_observation(ref, outcome)
            if outcome.status == "committed":
                self.commit(ref, outcome.result)
                return outcome.result
            if outcome.status != "definitely_absent" or not replay_safe:
                raise OperationInDoubt(ref.key, outcome.status)

        attempt = self.start_attempt(ref, budget=budget)
        try:
            result = fn()
        except BaseException as exc:
            self.mark_in_doubt(ref, exc)
            self.schedule_retry(ref, attempt, budget=budget)
            raise
        self.commit(ref, result)
        return result

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
            row["observation"] = (
                json.loads(row.pop("observation_json")) if row.get("observation_json") else None
            )
            result.append(row)
        return result
