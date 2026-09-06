"""Crash-safe admission queue with deterministic weighted fairness and throttle windows.

Airflow remains the scheduler.  This store decides whether a validated work order may be dispatched;
it does not execute work.  State is durable so a backend restart cannot forget queued/active work or
reset fairness counters.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from swfactory.admission import Limits, Priority


@dataclass(frozen=True)
class CapacityBlock:
    dimension: str
    current: int
    limit: int
    key: str = ""


@dataclass(frozen=True)
class AdmissionDecision:
    state: str
    reason: str
    position: int | None = None
    limiting: CapacityBlock | None = None


@dataclass(frozen=True)
class QueueItem:
    work_id: str
    repo: str
    actor: str
    blueprint: str
    priority: Priority
    sequence: int
    state: str
    enqueued_at: float
    updated_at: float


WEIGHTS: dict[Priority, int] = {
    Priority.HOTFIX: 8,
    Priority.MANUAL: 4,
    Priority.NORMAL: 2,
    Priority.BACKGROUND: 1,
}


class DurableAdmission:
    """SQLite-backed local admission implementation.

    Selection uses weighted deficit round-robin across declared priority classes.  Within a class,
    FIFO sequence is stable.  Capacity dimensions have deterministic precedence so an operator gets
    one reproducible explanation for a queue decision.
    """

    def __init__(self, path: Path, limits: Limits | None = None):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.limits = limits or Limits()
        self.db = sqlite3.connect(
            path, timeout=30, isolation_level="IMMEDIATE", check_same_thread=False
        )
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self._migrate()

    def _migrate(self) -> None:
        with self.db:
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS admission_work(
                    work_id TEXT PRIMARY KEY,
                    repo TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    blueprint TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    sequence INTEGER NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    limiting_json TEXT,
                    cell_id TEXT,
                    cell_epoch INTEGER,
                    enqueued_at REAL NOT NULL,
                    admitted_at REAL,
                    terminal_at REAL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS admission_state_seq
                    ON admission_work(state, sequence);
                CREATE TABLE IF NOT EXISTS admission_meta(
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS admission_fairness(
                    priority INTEGER PRIMARY KEY,
                    deficit INTEGER NOT NULL,
                    served INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS admission_throttles(
                    scope TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    until_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(scope, scope_key)
                );
                """
            )
            self.db.execute("INSERT OR IGNORE INTO admission_meta VALUES('sequence',0)")
            for priority in Priority:
                self.db.execute(
                    "INSERT OR IGNORE INTO admission_fairness VALUES(?,?,0)",
                    (int(priority), 0),
                )

    def close(self) -> None:
        self.db.close()

    def submit(
        self,
        *,
        work_id: str,
        repo: str,
        actor: str,
        blueprint: str,
        priority: Priority = Priority.NORMAL,
    ) -> AdmissionDecision:
        existing = self._row(work_id)
        if existing is not None:
            if existing["state"] == "active":
                return AdmissionDecision("active", "duplicate_active")
            if existing["state"] == "queued":
                return AdmissionDecision("queued", "duplicate_queued", self.position(work_id))
            return AdmissionDecision(existing["state"], "duplicate_terminal")

        block = self.capacity_block(repo=repo, actor=actor, blueprint=blueprint)
        throttle = self._throttle(repo)
        now = time.time()
        if throttle is not None:
            block = CapacityBlock("rate_limit", 1, 0, throttle)
        if block is None:
            seq = self._next_sequence()
            with self.db:
                self.db.execute(
                    """INSERT INTO admission_work(
                        work_id,repo,actor,blueprint,priority,sequence,state,reason,
                        enqueued_at,admitted_at,updated_at
                    ) VALUES(?,?,?,?,?,?, 'active','admitted',?,?,?)""",
                    (work_id, repo, actor, blueprint, int(priority), seq, now, now, now),
                )
            return AdmissionDecision("active", "admitted")

        queued = self._count("queued")
        if queued >= self.limits.queue_size:
            return AdmissionDecision(
                "rejected",
                "queue_full",
                limiting=CapacityBlock("queue", queued, self.limits.queue_size),
            )
        seq = self._next_sequence()
        limiting_json = json.dumps(block.__dict__, sort_keys=True, separators=(",", ":"))
        with self.db:
            self.db.execute(
                """INSERT INTO admission_work(
                    work_id,repo,actor,blueprint,priority,sequence,state,reason,limiting_json,
                    enqueued_at,updated_at
                ) VALUES(?,?,?,?,?,?, 'queued','capacity',?,?,?)""",
                (work_id, repo, actor, blueprint, int(priority), seq, limiting_json, now, now),
            )
        return AdmissionDecision("queued", "capacity", self.position(work_id), block)

    def bind_cell(self, work_id: str, cell_id: str, epoch: int) -> None:
        if epoch < 1:
            raise ValueError("epoch must be positive")
        with self.db:
            cur = self.db.execute(
                "UPDATE admission_work SET cell_id=?,cell_epoch=?,updated_at=? WHERE work_id=?",
                (cell_id, epoch, time.time(), work_id),
            )
        if cur.rowcount != 1:
            raise KeyError(work_id)

    def complete(self, work_id: str, *, cell_id: str, epoch: int, state: str) -> list[str]:
        """Release capacity only from the authoritative bound cell epoch."""
        if state not in {"success", "failed", "cancelled", "rejected", "cleaned"}:
            raise ValueError("non-terminal completion state")
        now = time.time()
        with self.db:
            cur = self.db.execute(
                "UPDATE admission_work SET state=?,reason='cell_terminal',"
                "terminal_at=?,updated_at=? "
                "WHERE work_id=? AND state='active' AND cell_id=? AND cell_epoch=?",
                (state, now, now, work_id, cell_id, epoch),
            )
        if cur.rowcount != 1:
            return []
        return self.drain()

    def capacity_block(self, *, repo: str, actor: str, blueprint: str) -> CapacityBlock | None:
        active = self._active_rows()
        checks = (
            ("global", "", len(active), self.limits.global_active),
            ("repo", repo, sum(r["repo"] == repo for r in active), self.limits.per_repo_active),
            (
                "actor",
                actor,
                sum(r["actor"] == actor for r in active),
                self.limits.per_actor_active,
            ),
            (
                "blueprint",
                blueprint,
                sum(r["blueprint"] == blueprint for r in active),
                self.limits.per_blueprint_active,
            ),
        )
        for dimension, key, current, limit in checks:
            if current >= limit:
                return CapacityBlock(dimension, current, limit, key)
        return None

    def set_throttle(self, scope: str, scope_key: str, *, until_at: float, reason: str) -> None:
        if until_at <= time.time():
            self.clear_throttle(scope, scope_key)
            return
        with self.db:
            self.db.execute(
                """INSERT INTO admission_throttles(scope,scope_key,reason,until_at,updated_at)
                   VALUES(?,?,?,?,?) ON CONFLICT(scope,scope_key) DO UPDATE SET
                   reason=excluded.reason,until_at=excluded.until_at,updated_at=excluded.updated_at""",
                (scope, scope_key, reason[:512], until_at, time.time()),
            )

    def clear_throttle(self, scope: str, scope_key: str) -> None:
        with self.db:
            self.db.execute(
                "DELETE FROM admission_throttles WHERE scope=? AND scope_key=?",
                (scope, scope_key),
            )

    def drain(self, limit: int | None = None) -> list[str]:
        admitted: list[str] = []
        max_items = limit if limit is not None else self.limits.global_active
        for _ in range(max(0, max_items)):
            item = self._next_fair_candidate()
            if item is None:
                break
            block = self.capacity_block(repo=item.repo, actor=item.actor, blueprint=item.blueprint)
            throttle = self._throttle(item.repo)
            if throttle is not None:
                block = CapacityBlock("rate_limit", 1, 0, throttle)
            if block is not None:
                self._set_limiting(item.work_id, block)
                # Another class may still fit, so temporarily mark this sequence
                # skipped for this pass.
                if not self._any_other_candidate(item.work_id):
                    break
                self._defer_sequence(item.work_id)
                continue
            now = time.time()
            with self.db:
                cur = self.db.execute(
                    """UPDATE admission_work SET state='active',reason='drained',limiting_json=NULL,
                       admitted_at=?,updated_at=? WHERE work_id=? AND state='queued'""",
                    (now, now, item.work_id),
                )
            if cur.rowcount == 1:
                self._record_service(item.priority)
                admitted.append(item.work_id)
        return admitted

    def position(self, work_id: str) -> int | None:
        rows = self._queued_rows()
        for i, row in enumerate(rows, 1):
            if row["work_id"] == work_id:
                return i
        return None

    def snapshot(self, *, limit: int = 200) -> dict:
        now = time.time()
        queued = self._queued_rows()[: max(1, min(limit, 1000))]
        active = self._active_rows()[: max(1, min(limit, 1000))]
        ages = sorted(max(0.0, now - float(r["enqueued_at"])) for r in queued)
        return {
            "active": [self._public(r, now) for r in active],
            "queued": [dict(self._public(r, now), position=i + 1) for i, r in enumerate(queued)],
            "limits": self.limits.__dict__,
            "pressure": {
                "active": len(active),
                "queued": self._count("queued"),
                "oldest_wait_s": max(ages, default=0.0),
                "p50_wait_s": self._percentile(ages, 0.50),
                "p95_wait_s": self._percentile(ages, 0.95),
                "throttles": self._active_throttle_count(),
            },
        }

    def _next_fair_candidate(self) -> QueueItem | None:
        rows = self._queued_rows()
        if not rows:
            return None
        by_priority: dict[Priority, list[sqlite3.Row]] = {p: [] for p in Priority}
        for row in rows:
            by_priority[Priority(int(row["priority"]))].append(row)
        fairness = {
            Priority(int(r["priority"])): int(r["deficit"])
            for r in self.db.execute("SELECT priority,deficit FROM admission_fairness")
        }
        available = [p for p in Priority if by_priority[p]]
        if not any(fairness.get(p, 0) > 0 for p in available):
            with self.db:
                for p in available:
                    self.db.execute(
                        "UPDATE admission_fairness SET deficit=deficit+? WHERE priority=?",
                        (WEIGHTS[p], int(p)),
                    )
                    fairness[p] = fairness.get(p, 0) + WEIGHTS[p]
        selected = min(
            (p for p in available if fairness.get(p, 0) > 0),
            key=lambda p: (int(p), by_priority[p][0]["sequence"]),
        )
        return self._decode(by_priority[selected][0])

    def _record_service(self, priority: Priority) -> None:
        with self.db:
            self.db.execute(
                "UPDATE admission_fairness SET deficit=MAX(deficit-1,0),served=served+1 "
                "WHERE priority=?",
                (int(priority),),
            )

    def _set_limiting(self, work_id: str, block: CapacityBlock) -> None:
        payload = json.dumps(block.__dict__, sort_keys=True, separators=(",", ":"))
        with self.db:
            self.db.execute(
                "UPDATE admission_work SET limiting_json=?,updated_at=? WHERE work_id=?",
                (payload, time.time(), work_id),
            )

    def _defer_sequence(self, work_id: str) -> None:
        # Stable across restarts: move a temporarily blocked item behind current queued items while
        # preserving declared priority.  Evidence still carries its original enqueue timestamp.
        seq = self._next_sequence()
        with self.db:
            self.db.execute(
                "UPDATE admission_work SET sequence=?,updated_at=? "
                "WHERE work_id=? AND state='queued'",
                (seq, time.time(), work_id),
            )

    def _throttle(self, repo: str) -> str | None:
        now = time.time()
        with self.db:
            self.db.execute("DELETE FROM admission_throttles WHERE until_at<=?", (now,))
            rows = self.db.execute(
                "SELECT scope,scope_key,reason FROM admission_throttles WHERE until_at>?",
                (now,),
            ).fetchall()
        for row in rows:
            if row["scope"] == "global" or (row["scope"] == "repo" and row["scope_key"] == repo):
                return f"{row['scope']}:{row['scope_key']}:{row['reason']}"
        return None

    def _next_sequence(self) -> int:
        with self.db:
            self.db.execute("UPDATE admission_meta SET value=value+1 WHERE key='sequence'")
            return int(
                self.db.execute("SELECT value FROM admission_meta WHERE key='sequence'").fetchone()[
                    0
                ]
            )

    def _row(self, work_id: str) -> sqlite3.Row | None:
        return self.db.execute(
            "SELECT * FROM admission_work WHERE work_id=?", (work_id,)
        ).fetchone()

    def _active_rows(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM admission_work WHERE state='active' ORDER BY admitted_at,sequence"
        ).fetchall()

    def _queued_rows(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM admission_work WHERE state='queued' ORDER BY priority,sequence"
        ).fetchall()

    def _count(self, state: str) -> int:
        return int(
            self.db.execute(
                "SELECT count(*) FROM admission_work WHERE state=?", (state,)
            ).fetchone()[0]
        )

    def _any_other_candidate(self, work_id: str) -> bool:
        return bool(
            self.db.execute(
                "SELECT 1 FROM admission_work WHERE state='queued' AND work_id!=? LIMIT 1",
                (work_id,),
            ).fetchone()
        )

    def _active_throttle_count(self) -> int:
        return int(
            self.db.execute(
                "SELECT count(*) FROM admission_throttles WHERE until_at>?", (time.time(),)
            ).fetchone()[0]
        )

    @staticmethod
    def _decode(row: sqlite3.Row) -> QueueItem:
        return QueueItem(
            work_id=row["work_id"],
            repo=row["repo"],
            actor=row["actor"],
            blueprint=row["blueprint"],
            priority=Priority(int(row["priority"])),
            sequence=int(row["sequence"]),
            state=row["state"],
            enqueued_at=float(row["enqueued_at"]),
            updated_at=float(row["updated_at"]),
        )

    @staticmethod
    def _public(row: sqlite3.Row, now: float) -> dict:
        limiting = json.loads(row["limiting_json"]) if row["limiting_json"] else None
        return {
            "work_id": row["work_id"],
            "repo": row["repo"],
            "actor": row["actor"],
            "blueprint": row["blueprint"],
            "priority": Priority(int(row["priority"])).name.lower(),
            "state": row["state"],
            "reason": row["reason"],
            "limiting": limiting,
            "wait_s": max(0.0, now - float(row["enqueued_at"])),
            "cell_id": row["cell_id"],
            "cell_epoch": row["cell_epoch"],
        }

    @staticmethod
    def _percentile(values: Iterable[float], q: float) -> float:
        ordered = list(values)
        if not ordered:
            return 0.0
        index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
        return float(ordered[index])
