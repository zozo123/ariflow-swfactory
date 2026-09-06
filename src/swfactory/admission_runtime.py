"""Crash-safe admission runtime with deterministic fairness and explicit pressure evidence.

This module is the durable successor to the small in-memory `AdmissionController`.  It does not
schedule work: it decides which already-submitted work may be dispatched to Airflow.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from swfactory.admission import Limits, Priority

AdmissionState = Literal["queued", "active", "rejected", "completed", "cancelled"]


@dataclass(frozen=True)
class AdmissionRecord:
    work_id: str
    repo: str
    actor: str
    blueprint: str
    priority: Priority
    sequence: int
    state: AdmissionState
    reason: str
    enqueued_at: float
    updated_at: float
    limiting_dimension: str | None = None
    limiting_current: int | None = None
    limiting_limit: int | None = None


@dataclass(frozen=True)
class ThrottleWindow:
    key: str
    upstream: str
    scope: str
    until: float
    reason: str


@dataclass(frozen=True)
class DurableDecision:
    work_id: str
    state: AdmissionState
    reason: str
    position: int | None = None
    limiting_dimension: str | None = None
    limiting_current: int | None = None
    limiting_limit: int | None = None
    throttle_until: float | None = None

    @property
    def admitted(self) -> bool:
        return self.state == "active"


class AdmissionStore:
    """SQLite projection of queue/active/decision state.

    Sequence allocation, capacity snapshots, selection and state transitions happen under one
    SQLite transaction plus an in-process lock for the shared connection.  Storage is intentionally
    simple enough to reproduce with Postgres transactions later.
    """

    WEIGHTS: dict[Priority, int] = {
        Priority.HOTFIX: 8,
        Priority.MANUAL: 4,
        Priority.NORMAL: 2,
        Priority.BACKGROUND: 1,
    }

    def __init__(self, path: Path, limits: Limits = Limits()):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.limits = limits
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
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS admission_records(
                    work_id TEXT PRIMARY KEY,
                    repo TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    blueprint TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    sequence INTEGER NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    enqueued_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    limiting_dimension TEXT,
                    limiting_current INTEGER,
                    limiting_limit INTEGER
                );
                CREATE INDEX IF NOT EXISTS admission_state_seq
                    ON admission_records(state, sequence);
                CREATE TABLE IF NOT EXISTS admission_meta(
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS admission_throttles(
                    throttle_key TEXT PRIMARY KEY,
                    upstream TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    until REAL NOT NULL,
                    reason TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            for priority in Priority:
                key = f"served:{priority.name.lower()}"
                self.db.execute(
                    "INSERT OR IGNORE INTO admission_meta(key,value_json) VALUES(?,?)",
                    (key, "0"),
                )

    def submit(
        self,
        work_id: str,
        repo: str,
        actor: str,
        blueprint: str,
        priority: Priority = Priority.NORMAL,
    ) -> DurableDecision:
        now = time.time()
        with self.lock, self.db:
            existing = self.db.execute(
                "SELECT * FROM admission_records WHERE work_id=?", (work_id,)
            ).fetchone()
            if existing is not None:
                return self._decision(self._decode(existing))
            sequence = int(
                self.db.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM admission_records").fetchone()[0]
            )
            throttle = self._matching_throttle(repo, now)
            limiting = self._limiting(repo, actor, blueprint)
            if throttle is None and limiting is None:
                state: AdmissionState = "active"
                reason = "admitted"
                dim = current = limit = None
            else:
                queued = self.db.execute(
                    "SELECT COUNT(*) FROM admission_records WHERE state='queued'"
                ).fetchone()[0]
                if int(queued) >= self.limits.queue_size:
                    state = "rejected"
                    reason = "queue_full"
                    dim, current, limit = "queue", int(queued), self.limits.queue_size
                else:
                    state = "queued"
                    if throttle is not None:
                        reason = f"throttled:{throttle.upstream}"
                        dim, current, limit = "throttle", None, None
                    else:
                        assert limiting is not None
                        dim, current, limit = limiting
                        reason = f"capacity:{dim}"
            self.db.execute(
                """INSERT INTO admission_records VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    work_id,
                    repo,
                    actor,
                    blueprint,
                    int(priority),
                    sequence,
                    state,
                    reason,
                    now,
                    now,
                    dim,
                    current,
                    limit,
                    None,
                )[:13],
            )
            return self._decision(self.get(work_id), throttle=throttle)

    def get(self, work_id: str) -> AdmissionRecord:
        with self.lock:
            row = self.db.execute(
                "SELECT * FROM admission_records WHERE work_id=?", (work_id,)
            ).fetchone()
        if row is None:
            raise KeyError(work_id)
        return self._decode(row)

    def complete(self, work_id: str, *, reason: str = "completed") -> list[str]:
        with self.lock, self.db:
            row = self.db.execute(
                "SELECT state FROM admission_records WHERE work_id=?", (work_id,)
            ).fetchone()
            if row is None:
                raise KeyError(work_id)
            if row["state"] == "active":
                self.db.execute(
                    "UPDATE admission_records SET state='completed',reason=?,updated_at=? WHERE work_id=?",
                    (reason, time.time(), work_id),
                )
            return self._drain_locked()

    def cancel(self, work_id: str, *, reason: str = "cancelled") -> list[str]:
        with self.lock, self.db:
            self.db.execute(
                """UPDATE admission_records SET state='cancelled',reason=?,updated_at=?
                   WHERE work_id=? AND state IN ('queued','active')""",
                (reason, time.time(), work_id),
            )
            return self._drain_locked()

    def set_throttle(
        self,
        upstream: str,
        scope: str,
        until: float,
        *,
        reason: str,
    ) -> ThrottleWindow:
        key = f"{upstream}:{scope}"
        now = time.time()
        with self.lock, self.db:
            self.db.execute(
                """INSERT INTO admission_throttles VALUES(?,?,?,?,?,?)
                   ON CONFLICT(throttle_key) DO UPDATE SET until=excluded.until,
                   reason=excluded.reason,updated_at=excluded.updated_at""",
                (key, upstream, scope, float(until), reason, now),
            )
        return ThrottleWindow(key, upstream, scope, float(until), reason)

    def clear_expired_throttles(self, now: float | None = None) -> int:
        now = time.time() if now is None else now
        with self.lock, self.db:
            cur = self.db.execute("DELETE FROM admission_throttles WHERE until<=?", (now,))
            return cur.rowcount

    def drain(self) -> list[str]:
        with self.lock, self.db:
            return self._drain_locked()

    def _drain_locked(self) -> list[str]:
        admitted: list[str] = []
        now = time.time()
        self.db.execute("DELETE FROM admission_throttles WHERE until<=?", (now,))
        while True:
            candidates = [
                self._decode(row)
                for row in self.db.execute(
                    "SELECT * FROM admission_records WHERE state='queued' ORDER BY sequence"
                ).fetchall()
            ]
            eligible = [
                row
                for row in candidates
                if self._matching_throttle(row.repo, now) is None
                and self._limiting(row.repo, row.actor, row.blueprint) is None
            ]
            if not eligible:
                break
            selected = min(eligible, key=lambda row: self._fair_key(row, now))
            self.db.execute(
                """UPDATE admission_records SET state='active',reason='admitted_from_queue',
                   limiting_dimension=NULL,limiting_current=NULL,limiting_limit=NULL,updated_at=?
                   WHERE work_id=? AND state='queued'""",
                (now, selected.work_id),
            )
            self._bump_served(selected.priority)
            admitted.append(selected.work_id)
        return admitted

    def _fair_key(self, record: AdmissionRecord, now: float) -> tuple[float, int, int]:
        served = self._served(record.priority)
        weight = self.WEIGHTS[record.priority]
        # Age can improve service by at most one weighted quantum every 5 minutes.  Original
        # priority is preserved in the record; only selection changes.
        age_quanta = int(max(0.0, now - record.enqueued_at) // 300.0)
        virtual = (served - min(served, age_quanta)) / weight
        return (virtual, int(record.priority), record.sequence)

    def _served(self, priority: Priority) -> int:
        row = self.db.execute(
            "SELECT value_json FROM admission_meta WHERE key=?",
            (f"served:{priority.name.lower()}",),
        ).fetchone()
        return int(json.loads(row[0])) if row else 0

    def _bump_served(self, priority: Priority) -> None:
        value = self._served(priority) + 1
        self.db.execute(
            "UPDATE admission_meta SET value_json=? WHERE key=?",
            (json.dumps(value), f"served:{priority.name.lower()}"),
        )

    def _matching_throttle(self, repo: str, now: float) -> ThrottleWindow | None:
        rows = self.db.execute(
            """SELECT * FROM admission_throttles
               WHERE until>? AND (scope='*' OR scope=?) ORDER BY until DESC""",
            (now, repo),
        ).fetchall()
        if not rows:
            return None
        row = rows[0]
        return ThrottleWindow(
            str(row["throttle_key"]),
            str(row["upstream"]),
            str(row["scope"]),
            float(row["until"]),
            str(row["reason"]),
        )

    def _limiting(self, repo: str, actor: str, blueprint: str) -> tuple[str, int, int] | None:
        rows = self.db.execute(
            "SELECT repo,actor,blueprint FROM admission_records WHERE state='active'"
        ).fetchall()
        checks = (
            ("global", len(rows), self.limits.global_active),
            ("repo", sum(row["repo"] == repo for row in rows), self.limits.per_repo_active),
            ("actor", sum(row["actor"] == actor for row in rows), self.limits.per_actor_active),
            (
                "blueprint",
                sum(row["blueprint"] == blueprint for row in rows),
                self.limits.per_blueprint_active,
            ),
        )
        return next((item for item in checks if item[1] >= item[2]), None)

    def list(self, *, state: AdmissionState | None = None, limit: int = 200) -> list[AdmissionRecord]:
        limit = max(1, min(int(limit), 1000))
        sql = "SELECT * FROM admission_records"
        args: list[Any] = []
        if state is not None:
            sql += " WHERE state=?"
            args.append(state)
        sql += " ORDER BY sequence LIMIT ?"
        args.append(limit)
        with self.lock:
            return [self._decode(row) for row in self.db.execute(sql, args).fetchall()]

    def pressure(self) -> dict[str, Any]:
        now = time.time()
        records = self.list(limit=1000)
        queued = [r for r in records if r.state == "queued"]
        active = [r for r in records if r.state == "active"]
        ages = sorted(max(0.0, now - r.enqueued_at) for r in queued)

        def percentile(p: float) -> float:
            if not ages:
                return 0.0
            index = min(len(ages) - 1, int(round((len(ages) - 1) * p)))
            return ages[index]

        with self.lock:
            throttles = int(
                self.db.execute("SELECT COUNT(*) FROM admission_throttles WHERE until>?", (now,)).fetchone()[0]
            )
        limiting: dict[str, int] = {}
        for record in queued:
            if record.limiting_dimension:
                limiting[record.limiting_dimension] = limiting.get(record.limiting_dimension, 0) + 1
        return {
            "active": len(active),
            "queued": len(queued),
            "queue_age_p50_s": percentile(0.50),
            "queue_age_p95_s": percentile(0.95),
            "throttles": throttles,
            "limiting_dimensions": dict(sorted(limiting.items())),
            "limits": asdict(self.limits),
        }

    @staticmethod
    def _decode(row: sqlite3.Row) -> AdmissionRecord:
        return AdmissionRecord(
            work_id=str(row["work_id"]),
            repo=str(row["repo"]),
            actor=str(row["actor"]),
            blueprint=str(row["blueprint"]),
            priority=Priority(int(row["priority"])),
            sequence=int(row["sequence"]),
            state=str(row["state"]),  # type: ignore[arg-type]
            reason=str(row["reason"]),
            enqueued_at=float(row["enqueued_at"]),
            updated_at=float(row["updated_at"]),
            limiting_dimension=row["limiting_dimension"],
            limiting_current=row["limiting_current"],
            limiting_limit=row["limiting_limit"],
        )

    def _decision(
        self, record: AdmissionRecord, *, throttle: ThrottleWindow | None = None
    ) -> DurableDecision:
        position = None
        if record.state == "queued":
            queued = self.list(state="queued", limit=1000)
            position = next((i for i, item in enumerate(queued, 1) if item.work_id == record.work_id), None)
        return DurableDecision(
            work_id=record.work_id,
            state=record.state,
            reason=record.reason,
            position=position,
            limiting_dimension=record.limiting_dimension,
            limiting_current=record.limiting_current,
            limiting_limit=record.limiting_limit,
            throttle_until=throttle.until if throttle else None,
        )
