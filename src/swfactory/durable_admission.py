"""Crash-safe admission, durable work orders and a resumable dispatch outbox.

Airflow remains the scheduler.  This store decides whether a validated work order may be dispatched
and remembers, durably, everything needed to hand that one already-admitted command to Airflow again
after a restart.  It never schedules stages and it never retries on a timer of its own: the outbox is
pumped by the request paths that create or release capacity, and each delivery goes through the
operation journal, which observes an ambiguous remote outcome before it replays anything.

States are real, not overloaded.  ``active`` used to mean four different things at once, which is why
a drained item could sit forever with no Cell behind it:

    queued       waiting for capacity; nothing exists remotely yet
    admitted     capacity is reserved and a dispatch intent is open; nothing delivered yet
    dispatching  one leased delivery attempt owns the intent
    bound        the Airflow run exists and every member Cell is bound to it
    terminal     success/failed/cancelled/rejected/cleaned; capacity released

Delivery attempts are bounded, so the outbox has to say what happens when they run out.  Spending
the budget must never mean "keep the unit and go quiet": that is the same strand in a new costume.
Either the remote outcome is proven absent, in which case the order is failed and its unit released,
or it is unknown, in which case the unit stays held and the dispatch intent is marked
``undeliverable`` so an operator sees one stuck reservation instead of a queue that looks busy.

The capacity unit is one Factory Cell activation: one (repo, target, issue) at one epoch.  A
submission that fans out to N cells consumes N units, counts against each affected repository, and
holds its reservation until all N members are terminal.  Releasing on the first member's terminal
transition is exactly what used to strand that member's siblings.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from swfactory.admission import Limits, Priority
from swfactory.store_schema import ensure_named_schema, guard_before_ddl

WORK_ORDER_SCHEMA = 1
HOLDING_STATES = ("admitted", "dispatching", "bound")
TERMINAL_STATES = frozenset({"success", "failed", "cancelled", "rejected", "cleaned"})
# When one work order's members end differently the operator must be shown the worst outcome; a
# rejected sibling must never be summarised as a success.
TERMINAL_PRECEDENCE = ("rejected", "failed", "cancelled", "cleaned", "success")
DISPATCH_LEASE_S = 300.0
MAX_DISPATCH_ATTEMPTS = 5

WEIGHTS: dict[Priority, int] = {
    Priority.HOTFIX: 8,
    Priority.MANUAL: 4,
    Priority.NORMAL: 2,
    Priority.BACKGROUND: 1,
}


class WorkOrderConflict(ValueError):
    """One immutable request identity was reused for a different work order payload."""


class DispatchLeaseLost(RuntimeError):
    """The delivery attempt no longer owns the dispatch intent (cancelled, or leased elsewhere)."""


@dataclass(frozen=True)
class DispatchIntent:
    work_id: str
    attempt: int
    lease_token: str
    order: WorkOrder
    members: tuple[Member, ...]


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
    # A fresh admission hands its delivery straight to the submitter, inside the transaction that
    # created it. Without that, the intent is briefly claimable by any concurrent request, and the
    # submitter can end up reporting on a delivery some other request is in the middle of.
    intent: DispatchIntent | None = None


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


@dataclass(frozen=True)
class MemberSpec:
    """One declared capacity unit of a submission, known before any Cell is activated."""

    job_idx: int
    repo: str
    cell_id: str


@dataclass(frozen=True)
class Member:
    job_idx: int
    repo: str
    cell_id: str
    cell_epoch: int | None
    state: str

    def as_dict(self) -> dict:
        return {
            "job_idx": self.job_idx,
            "repo": self.repo,
            "cell_id": self.cell_id,
            "cell_epoch": self.cell_epoch,
            "state": self.state,
        }


@dataclass(frozen=True)
class WorkOrder:
    """The versioned, immutable payload a dispatch is reconstructed from after a restart."""

    work_id: str
    schema_version: int
    request_digest: str
    payload: dict
    created_at: float


def request_digest(payload: dict) -> str:
    """Request identity: the digest of the immutable payload, independent of retry/epoch identity."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


class DurableAdmission:
    """SQLite-backed local admission implementation.

    Selection uses weighted deficit round-robin across declared priority classes.  Within a class,
    FIFO sequence is stable.  Capacity dimensions have deterministic precedence so an operator gets
    one reproducible explanation for a queue decision.
    """

    def __init__(self, path: Path, limits: Limits | None = None):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.limits = limits or Limits()
        # How long one delivery attempt owns its intent. A process that dies mid-dispatch cannot
        # release the lease itself, so redelivery waits for it to expire rather than racing it.
        self.dispatch_lease_s = DISPATCH_LEASE_S
        self.lock = threading.RLock()
        # Work ids this *process* is delivering right now. The lease clock exists to decide that a
        # process which died mid-dispatch is never coming back; it is a guess, and with a short
        # lease it is a wrong one for an attempt that is merely slow. About our own live threads we
        # do not have to guess, so an in-process attempt excludes another one outright.
        self._inflight: dict[str, str] = {}
        # Autocommit plus explicit BEGIN IMMEDIATE: capacity is inspected and reserved inside one
        # write transaction, so two concurrent submissions cannot both read the same free slot.
        self.db = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self._migrate()

    @contextmanager
    def _txn(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self.db.execute("ROLLBACK")
                raise
            self.db.execute("COMMIT")

    def _migrate(self) -> None:
        guard_before_ddl(self.db, "admission")
        # executescript() commits any open transaction, so the DDL runs on its own before the
        # seeding and legacy repair that must be one atomic step.
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
            CREATE TABLE IF NOT EXISTS admission_order(
                work_id TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                request_digest TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS admission_members(
                work_id TEXT NOT NULL,
                job_idx INTEGER NOT NULL,
                repo TEXT NOT NULL,
                cell_id TEXT NOT NULL,
                cell_epoch INTEGER,
                state TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(work_id, job_idx)
            );
            CREATE INDEX IF NOT EXISTS admission_members_cell
                ON admission_members(cell_id, cell_epoch);
            CREATE TABLE IF NOT EXISTS admission_dispatch(
                work_id TEXT PRIMARY KEY,
                attempt INTEGER NOT NULL,
                state TEXT NOT NULL,
                lease_token TEXT,
                lease_until REAL,
                dag_run_id TEXT,
                last_error TEXT,
                observation_json TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )
        with self._txn():
            # Inside the same transaction as the seeding: a store this binary may not write to must
            # be refused before it is given a sequence counter and a fairness table.
            ensure_named_schema(self.db, "admission")
            self.db.execute("INSERT OR IGNORE INTO admission_meta VALUES('sequence',0)")
            for priority in Priority:
                self.db.execute(
                    "INSERT OR IGNORE INTO admission_fairness VALUES(?,?,0)",
                    (int(priority), 0),
                )
            self._migrate_legacy_active()

    def _migrate_legacy_active(self) -> None:
        """Retire the overloaded ``active`` state without leaking or inventing capacity.

        A legacy row that carries a Cell binding really was dispatched, so it becomes ``bound`` and
        keeps holding its unit through that member.  A legacy row with no binding is precisely the
        stranded admission this store exists to prevent (#2058): it has no work order to dispatch
        from, so it is closed as cancelled rather than left holding a unit nothing can release.
        """
        now = time.time()
        rows = self.db.execute("SELECT * FROM admission_work WHERE state='active'").fetchall()
        for row in rows:
            if row["cell_id"] and row["cell_epoch"] is not None:
                self.db.execute(
                    "UPDATE admission_work SET state='bound',updated_at=? WHERE work_id=?",
                    (now, row["work_id"]),
                )
                self.db.execute(
                    """INSERT OR IGNORE INTO admission_members(
                        work_id,job_idx,repo,cell_id,cell_epoch,state,created_at,updated_at
                    ) VALUES(?,?,?,?,?,'held',?,?)""",
                    (row["work_id"], 0, row["repo"], row["cell_id"], row["cell_epoch"], now, now),
                )
            else:
                self.db.execute(
                    """UPDATE admission_work SET state='cancelled',reason='orphaned_admission',
                       terminal_at=?,updated_at=? WHERE work_id=?""",
                    (now, now, row["work_id"]),
                )

    def close(self) -> None:
        with self.lock:
            self.db.close()

    # ------------------------------------------------------------------ admission

    def submit(
        self,
        *,
        work_id: str,
        actor: str,
        blueprint: str,
        order: dict,
        members: Sequence[MemberSpec],
        priority: Priority = Priority.NORMAL,
    ) -> AdmissionDecision:
        """Record an immutable work order and reserve capacity for every one of its members."""
        if not members:
            raise ValueError("a work order must declare at least one capacity member")
        if len({m.job_idx for m in members}) != len(members) or len({m.cell_id for m in members}) != len(members):
            raise ValueError("work order members must be unique by job index and Factory Cell")
        if int(order.get("schema_version", 0)) != WORK_ORDER_SCHEMA:
            raise ValueError(f"work order payload must declare schema_version {WORK_ORDER_SCHEMA}")
        digest = request_digest(order)
        repos = sorted(m.repo for m in members)
        # The display key stays one string for the operator view; capacity itself is counted per
        # member repository below, so a multi-repo order can no longer hide behind one synthetic key.
        repo_key = repos[0] if len(set(repos)) == 1 else _multi_repo_key(repos)
        now = time.time()
        with self._txn():
            existing = self._row(work_id)
            if existing is not None:
                self._assert_same_order(work_id, digest)
                state = str(existing["state"])
                if state in HOLDING_STATES:
                    return AdmissionDecision(state, f"duplicate_{state}")
                if state == "queued":
                    return AdmissionDecision("queued", "duplicate_queued", self._position(work_id))
                return AdmissionDecision(state, "duplicate_terminal")

            impossible = self._impossible(repos=repos, actor=actor, blueprint=blueprint)
            if impossible is not None:
                # A request that would not fit even on an idle factory can never drain, so queueing
                # it would be a promise the factory cannot keep.
                return AdmissionDecision("rejected", "capacity_impossible", limiting=impossible)
            block = self._capacity_block(repos=repos, actor=actor, blueprint=blueprint)
            throttle = self._throttle(repo_key)
            if throttle is not None:
                block = CapacityBlock("rate_limit", 1, 0, throttle)
            if block is not None:
                queued = self._count("queued")
                if queued >= self.limits.queue_size:
                    return AdmissionDecision(
                        "rejected",
                        "queue_full",
                        limiting=CapacityBlock("queue", queued, self.limits.queue_size),
                    )
            state = "queued" if block is not None else "admitted"
            reason = "capacity" if block is not None else "admitted"
            seq = self._next_sequence()
            limiting_json = json.dumps(block.__dict__, sort_keys=True, separators=(",", ":")) if block else None
            self.db.execute(
                """INSERT INTO admission_work(
                    work_id,repo,actor,blueprint,priority,sequence,state,reason,limiting_json,
                    cell_id,cell_epoch,enqueued_at,admitted_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    work_id,
                    repo_key,
                    actor,
                    blueprint,
                    int(priority),
                    seq,
                    state,
                    reason,
                    limiting_json,
                    min(m.cell_id for m in members),
                    None,
                    now,
                    now if block is None else None,
                    now,
                ),
            )
            self.db.execute(
                """INSERT INTO admission_order(work_id,schema_version,request_digest,payload_json,created_at)
                   VALUES(?,?,?,?,?)""",
                (
                    work_id,
                    WORK_ORDER_SCHEMA,
                    digest,
                    json.dumps(order, sort_keys=True, separators=(",", ":"), allow_nan=False),
                    now,
                ),
            )
            for member in sorted(members, key=lambda m: m.job_idx):
                self.db.execute(
                    """INSERT INTO admission_members(
                        work_id,job_idx,repo,cell_id,cell_epoch,state,created_at,updated_at
                    ) VALUES(?,?,?,?,NULL,'held',?,?)""",
                    (work_id, member.job_idx, member.repo, member.cell_id, now, now),
                )
            if block is None:
                self._open_dispatch(work_id, now)
                return AdmissionDecision("admitted", "admitted", intent=self._claim(work_id))
            return AdmissionDecision("queued", "capacity", self._position(work_id), block)

    def drain(self, limit: int | None = None) -> list[str]:
        """Move queued work to ``admitted`` and open its dispatch intent. Delivery is a separate step."""
        admitted: list[str] = []
        max_items = limit if limit is not None else self.limits.global_active
        with self._txn():
            for _ in range(max(0, max_items)):
                item = self._next_fair_candidate()
                if item is None:
                    break
                repos = [m.repo for m in self._members(item.work_id)]
                block = self._capacity_block(repos=repos, actor=item.actor, blueprint=item.blueprint)
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
                cur = self.db.execute(
                    """UPDATE admission_work SET state='admitted',reason='drained',limiting_json=NULL,
                       admitted_at=?,updated_at=? WHERE work_id=? AND state='queued'""",
                    (now, now, item.work_id),
                )
                if cur.rowcount == 1:
                    self._open_dispatch(item.work_id, now)
                    self._record_service(item.priority)
                    admitted.append(item.work_id)
        return admitted

    def cancel(self, work_id: str, *, reason: str, state: str = "cancelled") -> list[str]:
        """Close an undelivered admission and release every unit it still holds.

        Cancelling a ``dispatching`` order also invalidates the outstanding lease token, so a
        delivery attempt that returns later cannot bind Cells to an order the operator withdrew.
        A ``bound`` order is deliberately not cancellable here: its run exists, so its capacity is
        released by the Cell lifecycle that owns it, never by a second opinion on this side.
        """
        if state not in TERMINAL_STATES:
            raise ValueError("cancellation must end in a terminal state")
        now = time.time()
        with self._txn():
            cur = self.db.execute(
                """UPDATE admission_work SET state=?,reason=?,terminal_at=?,updated_at=?
                   WHERE work_id=? AND state IN ('queued','admitted','dispatching')""",
                (state, reason[:512], now, now, work_id),
            )
            if cur.rowcount != 1:
                return []
            self.db.execute(
                "UPDATE admission_members SET state=?,updated_at=? WHERE work_id=? AND state='held'",
                (state, now, work_id),
            )
            self.db.execute(
                """UPDATE admission_dispatch SET state='abandoned',lease_token=NULL,lease_until=NULL,
                   last_error=?,updated_at=? WHERE work_id=? AND state!='delivered'""",
                (reason[:512], now, work_id),
            )
            self._inflight.pop(work_id, None)
        return self.drain()

    def complete(self, work_id: str, *, cell_id: str, epoch: int, state: str) -> list[str]:
        """Release one member's unit; the reservation survives until every member is terminal."""
        if state not in TERMINAL_STATES:
            raise ValueError("non-terminal completion state")
        now = time.time()
        with self._txn():
            cur = self.db.execute(
                """UPDATE admission_members SET state=?,updated_at=?
                   WHERE work_id=? AND cell_id=? AND cell_epoch=? AND state='held'""",
                (state, now, work_id, cell_id, epoch),
            )
            if cur.rowcount != 1:
                return []
            remaining = int(
                self.db.execute(
                    "SELECT count(*) FROM admission_members WHERE work_id=? AND state='held'",
                    (work_id,),
                ).fetchone()[0]
            )
            if remaining:
                self.db.execute(
                    "UPDATE admission_work SET reason='member_terminal',updated_at=? WHERE work_id=?",
                    (now, work_id),
                )
                return []
            outcomes = [
                str(row["state"])
                for row in self.db.execute("SELECT state FROM admission_members WHERE work_id=?", (work_id,)).fetchall()
            ]
            self.db.execute(
                """UPDATE admission_work SET state=?,reason='members_terminal',terminal_at=?,updated_at=?
                   WHERE work_id=? AND state IN ('admitted','dispatching','bound')""",
                (_worst(outcomes), now, now, work_id),
            )
        return self.drain()

    def held_memberships(self, *, limit: int = 200) -> list[tuple[str, str, int]]:
        """Every unit currently held against a named Cell epoch, for reconciliation against Cell truth.

        A unit is released when the Cell that holds it reports a terminal transition.  If that report
        never arrives -- the Cell was ended by a compensation, an external cleanup, or a process that
        died between closing a Cell and closing its reservation -- the unit is held by a Cell that is
        already finished, and nothing will ever ask for it back.  This is the read a caller that can
        see Cells needs in order to notice that and repair it.
        """
        with self.lock:
            rows = self.db.execute(
                f"""SELECT m.work_id AS work_id, m.cell_id AS cell_id, m.cell_epoch AS cell_epoch
                    FROM admission_members m JOIN admission_work w ON w.work_id=m.work_id
                    WHERE m.state='held' AND m.cell_epoch IS NOT NULL
                      AND w.state IN ({_placeholders(HOLDING_STATES)})
                    ORDER BY w.sequence LIMIT ?""",  # noqa: S608
                (*HOLDING_STATES, max(1, min(int(limit), 1000))),
            ).fetchall()
        return [(str(row["work_id"]), str(row["cell_id"]), int(row["cell_epoch"])) for row in rows]

    def members_for_cell(self, cell_id: str, epoch: int) -> list[str]:
        """Every work order still holding a unit for this exact Cell epoch, in admission order."""
        with self.lock:
            rows = self.db.execute(
                """SELECT m.work_id AS work_id FROM admission_members m
                   JOIN admission_work w ON w.work_id=m.work_id
                   WHERE m.cell_id=? AND m.cell_epoch=? AND m.state='held'
                   ORDER BY w.sequence""",
                (cell_id, epoch),
            ).fetchall()
        return [str(row["work_id"]) for row in rows]

    # ------------------------------------------------------------------ work orders

    def work_order(self, work_id: str) -> WorkOrder:
        with self.lock:
            row = self.db.execute("SELECT * FROM admission_order WHERE work_id=?", (work_id,)).fetchone()
        if row is None:
            raise KeyError(work_id)
        return WorkOrder(
            work_id=work_id,
            schema_version=int(row["schema_version"]),
            request_digest=str(row["request_digest"]),
            payload=json.loads(row["payload_json"]),
            created_at=float(row["created_at"]),
        )

    def members(self, work_id: str) -> list[Member]:
        with self.lock:
            return self._members(work_id)

    def state_of(self, work_id: str) -> str | None:
        with self.lock:
            row = self._row(work_id)
        return None if row is None else str(row["state"])

    # ------------------------------------------------------------------ dispatch outbox

    def pending_dispatch(self, *, limit: int = 32) -> list[str]:
        """Admitted work whose command has not been delivered, in the one deterministic order.

        Includes an attempt whose lease expired: the process that held it died, and the command is
        still owed to Airflow.  This is a redelivery queue for one already-admitted command, not a
        second scheduler -- nothing here decides *when* a stage runs.
        """
        now = time.time()
        with self.lock:
            rows = self.db.execute(
                """SELECT w.work_id AS work_id FROM admission_work w
                   JOIN admission_dispatch d ON d.work_id=w.work_id
                   WHERE d.state IN ('pending','inflight')
                     AND (w.state='admitted' OR (w.state='dispatching' AND (d.lease_until IS NULL OR d.lease_until<=?)))
                   ORDER BY w.priority, w.sequence LIMIT ?""",
                (now, max(1, min(int(limit), 1000))),
            ).fetchall()
        return [str(row["work_id"]) for row in rows]

    def claim_dispatch(self, work_id: str, *, lease_s: float | None = None) -> DispatchIntent | None:
        """Take exclusive, time-limited ownership of one dispatch intent."""
        with self._txn():
            return self._claim(work_id, lease_s=lease_s)

    def _claim(self, work_id: str, *, lease_s: float | None = None) -> DispatchIntent | None:
        """Claim one dispatch intent.  Must be called with this store's write transaction open.

        Claiming inside the caller's transaction is what lets a fresh admission hand its delivery
        to its own submitter atomically: the intent is never visible as claimable in between.
        """
        now = time.time()
        lease = self.dispatch_lease_s if lease_s is None else lease_s
        row = self.db.execute("SELECT * FROM admission_dispatch WHERE work_id=?", (work_id,)).fetchone()
        work = self._row(work_id)
        if row is None or work is None or row["state"] in {"delivered", "abandoned", "undeliverable"}:
            return None
        if str(work["state"]) not in {"admitted", "dispatching"}:
            return None
        if work_id in self._inflight:
            # Another thread here is still delivering this exact intent.  Its lease may already have
            # expired, but expiry is a guess that the holder died, and about our own threads we know.
            return None
        leased = row["lease_until"] is not None and float(row["lease_until"]) > now
        if str(work["state"]) == "dispatching" and leased:
            return None
        attempt = int(row["attempt"]) + 1
        if attempt > MAX_DISPATCH_ATTEMPTS:
            return None
        token = hashlib.sha256(f"{work_id}\0{attempt}\0{now!r}".encode()).hexdigest()[:32]
        self.db.execute(
            """UPDATE admission_dispatch SET attempt=?,state='inflight',lease_token=?,lease_until=?,
               updated_at=? WHERE work_id=?""",
            (attempt, token, now + max(0.0, lease), now, work_id),
        )
        self.db.execute(
            "UPDATE admission_work SET state='dispatching',reason='dispatching',updated_at=? WHERE work_id=?",
            (now, work_id),
        )
        members = tuple(self._members(work_id))
        self._inflight[work_id] = token
        try:
            return DispatchIntent(work_id, attempt, token, self.work_order(work_id), members)
        except BaseException:
            # The claim is only real once the caller holds the intent it can act on.
            self._inflight.pop(work_id, None)
            raise

    def assert_lease(self, work_id: str, token: str) -> None:
        """Raise unless this attempt still owns the intent; nothing it decides is valid otherwise."""
        with self.lock:
            self._assert_lease(work_id, token)

    def attempts_exhausted(self, work_id: str) -> bool:
        with self.lock:
            row = self.db.execute("SELECT attempt FROM admission_dispatch WHERE work_id=?", (work_id,)).fetchone()
        return row is not None and int(row["attempt"]) >= MAX_DISPATCH_ATTEMPTS

    def record_member_epoch(self, work_id: str, job_idx: int, epoch: int, *, token: str) -> None:
        """Persist which Cell epoch this order owns, before and after the activation that creates it.

        Written before the activation so a crash in that window cannot leave a Cell that no work
        order admits to having activated, and rewritten after so the recorded epoch is the real one.
        """
        if epoch < 1:
            raise ValueError("epoch must be positive")
        now = time.time()
        with self._txn():
            self._assert_lease(work_id, token)
            cur = self.db.execute(
                "UPDATE admission_members SET cell_epoch=?,updated_at=? WHERE work_id=? AND job_idx=?",
                (epoch, now, work_id, job_idx),
            )
            if cur.rowcount != 1:
                raise KeyError(f"{work_id}:{job_idx}")

    def record_dispatch(self, work_id: str, *, token: str, dag_run_id: str) -> None:
        """Close the intent: the command was delivered and every member Cell is bound to the run."""
        now = time.time()
        with self._txn():
            self._assert_lease(work_id, token)
            missing = int(
                self.db.execute(
                    "SELECT count(*) FROM admission_members WHERE work_id=? AND cell_epoch IS NULL",
                    (work_id,),
                ).fetchone()[0]
            )
            if missing:
                raise DispatchLeaseLost(f"{work_id}: {missing} member(s) have no Factory Cell epoch")
            self.db.execute(
                """UPDATE admission_dispatch SET state='delivered',lease_token=NULL,lease_until=NULL,
                   dag_run_id=?,last_error=NULL,updated_at=? WHERE work_id=?""",
                (dag_run_id, now, work_id),
            )
            self.db.execute(
                """UPDATE admission_work SET state='bound',reason='dispatched',updated_at=?,
                   cell_epoch=(SELECT cell_epoch FROM admission_members
                               WHERE work_id=? ORDER BY cell_id LIMIT 1),
                   cell_id=(SELECT cell_id FROM admission_members WHERE work_id=? ORDER BY cell_id LIMIT 1)
                   WHERE work_id=? AND state='dispatching'""",
                (now, work_id, work_id, work_id),
            )
        self._release_inflight(work_id)

    def release_dispatch(self, work_id: str, *, token: str, error: str, observation: dict | None = None) -> None:
        """Hand the intent back for a later, observed redelivery; the reservation is kept."""
        now = time.time()
        with self._txn():
            self._assert_lease(work_id, token)
            self.db.execute(
                """UPDATE admission_dispatch SET state='pending',lease_token=NULL,lease_until=NULL,
                   last_error=?,observation_json=?,updated_at=? WHERE work_id=?""",
                (
                    error[:2000],
                    json.dumps(observation, sort_keys=True, separators=(",", ":")) if observation else None,
                    now,
                    work_id,
                ),
            )
            self.db.execute(
                """UPDATE admission_work SET state='admitted',reason='dispatch_retry',updated_at=?
                   WHERE work_id=? AND state='dispatching'""",
                (now, work_id),
            )
        self._release_inflight(work_id)

    def mark_undeliverable(self, work_id: str, *, error: str, observation: dict | None = None) -> None:
        """Retire an intent whose delivery budget is spent while its remote outcome is unproven.

        The reservation keeps its unit, because releasing capacity a possibly-running Airflow run
        still uses would be a guess.  What changes is that it stops looking deliverable: it leaves
        the redelivery queue and is counted as ``undeliverable`` pressure, so a stuck unit is
        visible to an operator instead of being an ``admitted`` row nothing will ever claim.
        """
        now = time.time()
        with self._txn():
            self.db.execute(
                """UPDATE admission_dispatch SET state='undeliverable',lease_token=NULL,lease_until=NULL,
                   last_error=?,observation_json=?,updated_at=? WHERE work_id=? AND state!='delivered'""",
                (
                    error[:2000],
                    json.dumps(observation, sort_keys=True, separators=(",", ":")) if observation else None,
                    now,
                    work_id,
                ),
            )
            self.db.execute(
                """UPDATE admission_work SET state='admitted',reason='undeliverable',updated_at=?
                   WHERE work_id=? AND state='dispatching'""",
                (now, work_id),
            )
        self._release_inflight(work_id)

    def note_outcome(self, work_id: str, *, error: str, observation: dict | None = None) -> None:
        """Record an unproven remote outcome on an intent this process no longer owns.

        A delivery that lost its lease -- the order was cancelled underneath it -- still has to say
        what it could not prove, or the cancellation would read as clean when it was not.
        """
        now = time.time()
        with self._txn():
            self.db.execute(
                "UPDATE admission_dispatch SET last_error=?,observation_json=?,updated_at=? WHERE work_id=?",
                (
                    error[:2000],
                    json.dumps(observation, sort_keys=True, separators=(",", ":")) if observation else None,
                    now,
                    work_id,
                ),
            )
        self._release_inflight(work_id)

    def dispatch_row(self, work_id: str) -> dict | None:
        with self.lock:
            row = self.db.execute("SELECT * FROM admission_dispatch WHERE work_id=?", (work_id,)).fetchone()
        return None if row is None else self._public_dispatch(row)

    # ------------------------------------------------------------------ capacity

    def capacity_block(self, *, repos: Sequence[str], actor: str, blueprint: str) -> CapacityBlock | None:
        with self.lock:
            return self._capacity_block(repos=repos, actor=actor, blueprint=blueprint)

    def _capacity_block(self, *, repos: Sequence[str], actor: str, blueprint: str) -> CapacityBlock | None:
        """Count every unit the request needs, per dimension, against what is already held."""
        return _first_block(self._checks(repos, actor, blueprint, self._held_members()))

    def _impossible(self, *, repos: Sequence[str], actor: str, blueprint: str) -> CapacityBlock | None:
        """A block that an idle factory would still report: waiting for capacity cannot fix it."""
        return _first_block(self._checks(repos, actor, blueprint, []))

    def _checks(
        self,
        repos: Sequence[str],
        actor: str,
        blueprint: str,
        held: Sequence[sqlite3.Row],
    ) -> list[tuple[str, str, int, int, int]]:
        """Deterministic dimension precedence: global, then repository, then actor, then blueprint."""
        wanted = list(repos) or [""]
        checks: list[tuple[str, str, int, int, int]] = [
            ("global", "", len(held), len(wanted), self.limits.global_active)
        ]
        checks += [
            (
                "repo",
                repo,
                sum(row["repo"] == repo for row in held),
                sum(item == repo for item in wanted),
                self.limits.per_repo_active,
            )
            for repo in sorted(set(wanted))
        ]
        checks.append(
            ("actor", actor, sum(row["actor"] == actor for row in held), len(wanted), self.limits.per_actor_active)
        )
        checks.append(
            (
                "blueprint",
                blueprint,
                sum(row["blueprint"] == blueprint for row in held),
                len(wanted),
                self.limits.per_blueprint_active,
            )
        )
        return checks

    def _held_members(self) -> list[sqlite3.Row]:
        return self.db.execute(
            f"""SELECT m.repo AS repo, w.actor AS actor, w.blueprint AS blueprint
                FROM admission_members m JOIN admission_work w ON w.work_id=m.work_id
                WHERE m.state='held' AND w.state IN ({_placeholders(HOLDING_STATES)})""",  # noqa: S608
            HOLDING_STATES,
        ).fetchall()

    # ------------------------------------------------------------------ throttles

    def set_throttle(self, scope: str, scope_key: str, *, until_at: float, reason: str) -> None:
        if until_at <= time.time():
            self.clear_throttle(scope, scope_key)
            return
        with self._txn():
            self.db.execute(
                """INSERT INTO admission_throttles(scope,scope_key,reason,until_at,updated_at)
                   VALUES(?,?,?,?,?) ON CONFLICT(scope,scope_key) DO UPDATE SET
                   reason=excluded.reason,until_at=excluded.until_at,updated_at=excluded.updated_at""",
                (scope, scope_key, reason[:512], until_at, time.time()),
            )

    def clear_throttle(self, scope: str, scope_key: str) -> None:
        with self._txn():
            self.db.execute(
                "DELETE FROM admission_throttles WHERE scope=? AND scope_key=?",
                (scope, scope_key),
            )

    # ------------------------------------------------------------------ views

    def position(self, work_id: str) -> int | None:
        with self.lock:
            return self._position(work_id)

    def snapshot(self, *, limit: int = 200) -> dict:
        now = time.time()
        with self.lock:
            queued = self._queued_rows()[: max(1, min(limit, 1000))]
            active = self._holding_rows()[: max(1, min(limit, 1000))]
            counts = {
                str(row["state"]): int(row["n"])
                for row in self.db.execute("SELECT state, count(*) AS n FROM admission_work GROUP BY state").fetchall()
            }
            units = len(self._held_members())
            undeliverable = int(
                self.db.execute(
                    f"""SELECT count(*) FROM admission_work w JOIN admission_dispatch d ON d.work_id=w.work_id
                        WHERE d.state='undeliverable' AND w.state IN ({_placeholders(HOLDING_STATES)})""",  # noqa: S608
                    HOLDING_STATES,
                ).fetchone()[0]
            )
            active_rows = [self._public(row, now) for row in active]
            queued_rows = [dict(self._public(row, now), position=i + 1) for i, row in enumerate(queued)]
            throttles = self._active_throttle_count()
        ages = sorted(max(0.0, now - float(row["enqueued_at"])) for row in queued)
        return {
            "active": active_rows,
            "queued": queued_rows,
            "limits": self.limits.__dict__,
            "capacity_unit": "factory_cell_activation",
            "pressure": {
                "active": len(active),
                "queued": counts.get("queued", 0),
                # Waiting for capacity, waiting for delivery and actually running are three
                # different things; an operator who cannot tell them apart cannot tell a moving
                # queue from a stuck one.
                "awaiting_dispatch": counts.get("admitted", 0),
                "dispatching": counts.get("dispatching", 0),
                "bound": counts.get("bound", 0),
                "held_units": units,
                # A unit whose delivery budget is spent and whose remote outcome is unproven. It is
                # still held on purpose; it is counted separately so it cannot hide inside
                # "awaiting_dispatch", which is what a healthy, claimable reservation looks like.
                "undeliverable": undeliverable,
                "oldest_wait_s": max(ages, default=0.0),
                "p50_wait_s": self._percentile(ages, 0.50),
                "p95_wait_s": self._percentile(ages, 0.95),
                "throttles": throttles,
            },
        }

    # ------------------------------------------------------------------ internals

    def _assert_same_order(self, work_id: str, digest: str) -> None:
        row = self.db.execute("SELECT request_digest FROM admission_order WHERE work_id=?", (work_id,)).fetchone()
        if row is not None and str(row["request_digest"]) != digest:
            raise WorkOrderConflict(f"{work_id} already exists with a different immutable work order")

    def _release_inflight(self, work_id: str) -> None:
        with self.lock:
            self._inflight.pop(work_id, None)

    def _assert_lease(self, work_id: str, token: str) -> None:
        row = self.db.execute("SELECT * FROM admission_dispatch WHERE work_id=?", (work_id,)).fetchone()
        if row is None or row["lease_token"] != token or row["state"] != "inflight":
            raise DispatchLeaseLost(f"{work_id}: dispatch lease is no longer held")

    def _open_dispatch(self, work_id: str, now: float) -> None:
        self.db.execute(
            """INSERT INTO admission_dispatch(work_id,attempt,state,created_at,updated_at)
               VALUES(?,0,'pending',?,?)
               ON CONFLICT(work_id) DO UPDATE SET state='pending',updated_at=excluded.updated_at
               WHERE admission_dispatch.state='abandoned'""",
            (work_id, now, now),
        )

    def _members(self, work_id: str) -> list[Member]:
        rows = self.db.execute(
            "SELECT * FROM admission_members WHERE work_id=? ORDER BY job_idx", (work_id,)
        ).fetchall()
        return [
            Member(
                job_idx=int(row["job_idx"]),
                repo=str(row["repo"]),
                cell_id=str(row["cell_id"]),
                cell_epoch=None if row["cell_epoch"] is None else int(row["cell_epoch"]),
                state=str(row["state"]),
            )
            for row in rows
        ]

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
        self.db.execute(
            "UPDATE admission_fairness SET deficit=MAX(deficit-1,0),served=served+1 WHERE priority=?",
            (int(priority),),
        )

    def _set_limiting(self, work_id: str, block: CapacityBlock) -> None:
        payload = json.dumps(block.__dict__, sort_keys=True, separators=(",", ":"))
        self.db.execute(
            "UPDATE admission_work SET limiting_json=?,updated_at=? WHERE work_id=?",
            (payload, time.time(), work_id),
        )

    def _defer_sequence(self, work_id: str) -> None:
        # Stable across restarts: move a temporarily blocked item behind current queued items while
        # preserving declared priority.  Evidence still carries its original enqueue timestamp.
        seq = self._next_sequence()
        self.db.execute(
            "UPDATE admission_work SET sequence=?,updated_at=? WHERE work_id=? AND state='queued'",
            (seq, time.time(), work_id),
        )

    def _throttle(self, repo: str) -> str | None:
        now = time.time()
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
        self.db.execute("UPDATE admission_meta SET value=value+1 WHERE key='sequence'")
        return int(self.db.execute("SELECT value FROM admission_meta WHERE key='sequence'").fetchone()[0])

    def _row(self, work_id: str) -> sqlite3.Row | None:
        return self.db.execute("SELECT * FROM admission_work WHERE work_id=?", (work_id,)).fetchone()

    def _holding_rows(self) -> list[sqlite3.Row]:
        return self.db.execute(
            f"SELECT * FROM admission_work WHERE state IN ({_placeholders(HOLDING_STATES)}) "  # noqa: S608
            "ORDER BY admitted_at,sequence",
            HOLDING_STATES,
        ).fetchall()

    def _queued_rows(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM admission_work WHERE state='queued' ORDER BY priority,sequence"
        ).fetchall()

    def _position(self, work_id: str) -> int | None:
        for i, row in enumerate(self._queued_rows(), 1):
            if row["work_id"] == work_id:
                return i
        return None

    def _count(self, state: str) -> int:
        return int(self.db.execute("SELECT count(*) FROM admission_work WHERE state=?", (state,)).fetchone()[0])

    def _any_other_candidate(self, work_id: str) -> bool:
        return bool(
            self.db.execute(
                "SELECT 1 FROM admission_work WHERE state='queued' AND work_id!=? LIMIT 1",
                (work_id,),
            ).fetchone()
        )

    def _active_throttle_count(self) -> int:
        return int(
            self.db.execute("SELECT count(*) FROM admission_throttles WHERE until_at>?", (time.time(),)).fetchone()[0]
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

    def _public(self, row: sqlite3.Row, now: float) -> dict:
        limiting = json.loads(row["limiting_json"]) if row["limiting_json"] else None
        work_id = str(row["work_id"])
        dispatch = self.db.execute("SELECT * FROM admission_dispatch WHERE work_id=?", (work_id,)).fetchone()
        return {
            "work_id": work_id,
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
            "members": [member.as_dict() for member in self._members(work_id)],
            "dispatch": None if dispatch is None else self._public_dispatch(dispatch),
        }

    @staticmethod
    def _public_dispatch(row: sqlite3.Row) -> dict:
        return {
            "state": row["state"],
            "attempt": int(row["attempt"]),
            "dag_run_id": row["dag_run_id"],
            "last_error": row["last_error"],
            "observation": json.loads(row["observation_json"]) if row["observation_json"] else None,
        }

    @staticmethod
    def _percentile(values: Iterable[float], q: float) -> float:
        ordered = list(values)
        if not ordered:
            return 0.0
        index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
        return float(ordered[index])


def _multi_repo_key(repos: Sequence[str]) -> str:
    return "multi:" + hashlib.sha256("\0".join(repos).encode()).hexdigest()[:16]


def _first_block(checks: Sequence[tuple[str, str, int, int, int]]) -> CapacityBlock | None:
    for dimension, key, current, needed, limit in checks:
        if current + needed > limit:
            return CapacityBlock(dimension, current, limit, key)
    return None


def _placeholders(values: Sequence[str]) -> str:
    return ",".join("?" for _ in values)


def _worst(states: Sequence[str]) -> str:
    for candidate in TERMINAL_PRECEDENCE:
        if candidate in states:
            return candidate
    return "success"
