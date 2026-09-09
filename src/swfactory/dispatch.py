"""Durable webhook intake, independent of Airflow availability and receiver processes.

Only the dispatch envelope is stored: never issue bodies, signatures or credentials. SQLite
serializes intake and claims; a lease token fences stale workers. Airflow's stable run identity
closes the gap between a successful POST and committing its receipt here.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import urllib.error
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from swfactory.webhook import Opener, TokenProvider, Trigger, WorkOrders

DEFAULT_INBOX = Path(".factory/webhooks/inbox.sqlite3")
STATES = ("pending", "dispatching", "dispatched", "dead")
_DELIVERY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


class DeliveryConflict(ValueError):
    """The same delivery identity cannot authorize different work."""


class InboxFull(RuntimeError):
    """Refuse new work before acknowledging more than this receiver can retain."""


@dataclass(frozen=True)
class Delivery:
    delivery_id: str
    repository: str
    event: str
    payload_sha256: str
    dag_id: str
    dag_run_id: str
    conf: dict[str, Any]
    state: str
    attempts: int
    total_attempts: int
    created_at: float
    updated_at: float
    next_attempt_at: float
    lease_until: float
    lease_token: str | None
    last_error: str | None
    work_order_id: str | None
    admission_state: str | None

    def public(self) -> dict[str, Any]:
        """Operator-visible receipt; the fencing token is internal to the worker."""
        return {key: value for key, value in vars(self).items() if key != "lease_token"}


class DeliveryInbox:
    """One local durable database per Airflow endpoint; connections are never shared by threads.

    Keep this file and its WAL on a persistent local filesystem. Several receiver processes on
    that host may share it; it is not a distributed queue on NFS. Receipts are retained so an old
    redelivery cannot silently become new work.
    """

    def __init__(self, path: Path, *, max_pending: int = 10_000) -> None:
        if max_pending < 1:
            raise ValueError("max_pending must be positive")
        self.path = Path(path).expanduser().absolute()
        self.max_pending = max_pending
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            if self.path.is_symlink() or not self.path.is_file():
                raise ValueError("webhook inbox must be a regular file") from None
        else:
            os.close(fd)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2):
                raise ValueError(f"unsupported webhook inbox schema {version}")
            # v1 receipts only ever named an Airflow run. In managed mode the durable answer is a
            # backend work order and its admission state, and adding the columns before the version
            # bump keeps a crash between the two from leaving a "v2" table without them.
            present = {row[1] for row in db.execute("PRAGMA table_info(deliveries)")}
            for column in ("work_order_id", "admission_state"):
                if present and column not in present:
                    db.execute(f"ALTER TABLE deliveries ADD COLUMN {column} TEXT")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS deliveries (
                    delivery_id TEXT PRIMARY KEY,
                    repository TEXT NOT NULL,
                    event TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    dag_id TEXT NOT NULL,
                    dag_run_id TEXT NOT NULL,
                    conf TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN
                        ('pending', 'dispatching', 'dispatched', 'dead')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    total_attempts INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    next_attempt_at REAL NOT NULL,
                    lease_until REAL NOT NULL DEFAULT 0,
                    lease_token TEXT,
                    last_error TEXT,
                    work_order_id TEXT,
                    admission_state TEXT
                );
                CREATE INDEX IF NOT EXISTS deliveries_due
                    ON deliveries (state, next_attempt_at, created_at);
                CREATE INDEX IF NOT EXISTS deliveries_leases ON deliveries (state, lease_until);
                PRAGMA user_version=2;
            """)

    @contextmanager
    def _connect(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=3, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous=FULL")
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if db.in_transaction:
                db.commit()
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _delivery(row: sqlite3.Row) -> Delivery:
        values = dict(row)
        values["conf"] = json.loads(values["conf"])
        return Delivery(**values)

    def bind(self, airflow_url: str | None = None, work_order_url: str | None = None) -> None:
        """A changed URL must never replay this database's pending work at another factory.

        Only the endpoint actually dispatched to is fenced. Managed mode fences the backend --
        sending these receipts to a different one would be a second admission of the same work at a
        factory that never saw it -- and deliberately does not fence the unused Airflow URL, so
        moving a receiver from legacy to managed is not a permanent conflict over a dead setting.
        """
        endpoints = {
            key: value
            for key, value in (("airflow_url", airflow_url), ("work_order_url", work_order_url))
            if value is not None
        }
        with self._connect(write=True) as db:
            for key, value in endpoints.items():
                row = db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
                if row is not None and row[0] != value:
                    raise DeliveryConflict(f"webhook inbox belongs to a different {key}")
                db.execute("INSERT OR IGNORE INTO metadata (key, value) VALUES (?, ?)", (key, value))

    def enqueue(
        self, delivery_id: str, event: str, body: bytes, repository: str, trigger: Trigger
    ) -> tuple[Delivery, bool]:
        """Commit before acknowledging, or return the existing immutable receipt on redelivery."""
        if not _DELIVERY_RE.fullmatch(delivery_id):
            raise ValueError("a valid X-GitHub-Delivery header is required")
        digest = hashlib.sha256(event.encode() + b"\0" + body).hexdigest()
        identity = json.dumps([repository.casefold(), delivery_id], separators=(",", ":"))
        run_id = "swf_webhook__" + hashlib.sha256(identity.encode()).hexdigest()
        conf = {
            **trigger.conf,
            "_swfactory_webhook": {
                "version": 1,
                "delivery_id": delivery_id,
                "repository": repository,
                "event": event,
                "payload_sha256": digest,
            },
        }
        now = time.time()
        with self._connect(write=True) as db:
            row = db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
            if row is not None:
                old = self._delivery(row)
                if old.payload_sha256 != digest or old.repository != repository:
                    raise DeliveryConflict("delivery id was already used for different content")
                # Routing is frozen at admission. A blueprint edit does not redirect a replay.
                return old, False
            count = db.execute(
                "SELECT count(*) FROM deliveries WHERE state IN ('pending', 'dispatching', 'dead')"
            ).fetchone()[0]
            if count >= self.max_pending:
                raise InboxFull("webhook inbox is full; resolve pending or dead deliveries")
            db.execute(
                """INSERT INTO deliveries
                   (delivery_id, repository, event, payload_sha256, dag_id, dag_run_id, conf,
                    state, created_at, updated_at, next_attempt_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)""",
                (
                    delivery_id,
                    repository,
                    event,
                    digest,
                    trigger.dag_id,
                    run_id,
                    json.dumps(conf, sort_keys=True, separators=(",", ":")),
                    now,
                    now,
                    now,
                ),
            )
            row = db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
            return self._delivery(row), True

    def claim(self, *, lease_s: float = 120, max_attempts: int = 12) -> Delivery | None:
        """Reclaim expired leases with a new token; never trust the old worker's completion."""
        if lease_s <= 0 or max_attempts < 1:
            raise ValueError("lease and attempt limits must be positive")
        now = time.time()
        with self._connect(write=True) as db:
            db.execute(
                """UPDATE deliveries SET state='dead', lease_token=NULL, lease_until=0,
                   updated_at=?, last_error='dispatch_attempts_exhausted'
                   WHERE attempts >= ? AND (state='pending' OR
                       (state='dispatching' AND lease_until <= ?))""",
                (now, max_attempts, now),
            )
            row = db.execute(
                """SELECT * FROM deliveries WHERE
                   (state='pending' AND next_attempt_at <= ?) OR
                   (state='dispatching' AND lease_until <= ?)
                   ORDER BY next_attempt_at, created_at, delivery_id LIMIT 1""",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            db.execute(
                """UPDATE deliveries SET state='dispatching', attempts=attempts+1,
                   total_attempts=total_attempts+1, lease_token=?, lease_until=?, updated_at=?
                   WHERE delivery_id=?""",
                (uuid.uuid4().hex, now + lease_s, now, row["delivery_id"]),
            )
            row = db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (row["delivery_id"],)).fetchone()
            return self._delivery(row)

    def complete(
        self,
        delivery: Delivery,
        *,
        work_order_id: str | None = None,
        admission_state: str | None = None,
    ) -> bool:
        """Record the handover. ``dispatched`` means the far side accepted the work, never that it
        ran: in managed mode ``admission_state`` is what separates a queued order -- durable, still
        waiting for capacity -- from one already handed to Airflow."""
        with self._connect(write=True) as db:
            changed = db.execute(
                """UPDATE deliveries SET state='dispatched', lease_token=NULL, lease_until=0,
                   last_error=NULL, updated_at=?, work_order_id=?, admission_state=?
                   WHERE delivery_id=? AND state='dispatching' AND lease_token=?""",
                (time.time(), work_order_id, admission_state, delivery.delivery_id, delivery.lease_token),
            )
            return changed.rowcount == 1

    def fail(
        self,
        delivery: Delivery,
        error: str,
        *,
        retryable: bool,
        max_attempts: int = 12,
        retry_after_s: float = 0,
    ) -> bool:
        """Bound retries, keep diagnostic codes only, and fence against a replacement claimant."""
        now = time.time()
        state = "pending" if retryable and delivery.attempts < max_attempts else "dead"
        delay = min(300, 5 * 2 ** min(delivery.attempts - 1, 8))
        # Stable jitter spreads retries without changing the receipt's identity.
        jitter = int(delivery.payload_sha256[:4], 16) % (max(1, delay // 5) + 1)
        due = now + max(delay + jitter, min(max(retry_after_s, 0), 3600))
        with self._connect(write=True) as db:
            changed = db.execute(
                """UPDATE deliveries SET state=?, last_error=?, next_attempt_at=?, updated_at=?,
                   lease_token=NULL, lease_until=0 WHERE delivery_id=? AND state='dispatching'
                   AND lease_token=?""",
                (state, error, due, now, delivery.delivery_id, delivery.lease_token),
            )
            return changed.rowcount == 1

    def retry(self, delivery_id: str) -> Delivery:
        """Explicitly reopen a dead dispatch; preserve the run id and lifetime attempt count."""
        with self._connect(write=True) as db:
            row = db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
            if row is None:
                raise KeyError(delivery_id)
            if row["state"] != "dead":
                raise DeliveryConflict("only dead deliveries can be retried")
            now = time.time()
            db.execute(
                """UPDATE deliveries SET state='pending', attempts=0, next_attempt_at=?,
                   updated_at=?, lease_token=NULL, lease_until=0 WHERE delivery_id=?""",
                (now, now, delivery_id),
            )
            row = db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
            return self._delivery(row)

    def get(self, delivery_id: str) -> Delivery:
        with self._connect() as db:
            row = db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
        if row is None:
            raise KeyError(delivery_id)
        return self._delivery(row)

    def list(self, *, state: str | None = None, limit: int = 50) -> list[Delivery]:
        if state is not None and state not in STATES:
            raise ValueError(f"state must be one of {', '.join(STATES)}")
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self._connect() as db:
            rows = db.execute(
                """SELECT * FROM deliveries WHERE (? IS NULL OR state=?)
                   ORDER BY created_at DESC, delivery_id LIMIT ?""",
                (state, state, limit),
            ).fetchall()
        return [self._delivery(row) for row in rows]

    def summary(self) -> dict[str, Any]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT state, count(*) AS count, min(created_at) AS oldest FROM deliveries GROUP BY state"
            ).fetchall()
        counts = dict.fromkeys(STATES, 0)
        oldest = time.time()
        for row in rows:
            counts[row["state"]] = row["count"]
            if row["state"] in {"pending", "dispatching"}:
                oldest = min(oldest, row["oldest"])
        return {"counts": counts, "oldest_pending_age_s": round(max(0, time.time() - oldest), 1)}


def _retry_after(headers: Mapping[str, str] | None) -> float:
    value = headers.get("Retry-After", "") if headers else ""
    if len(value) > 128:
        return 0
    try:
        return float(min(max(int(value), 0), 3600))
    except (ValueError, OverflowError):
        try:
            return parsedate_to_datetime(value).timestamp() - time.time()
        except (ValueError, TypeError, OverflowError):
            return 0


class Dispatcher:
    """One background worker; the SQLite lease permits multiple processes without double claims."""

    def __init__(
        self,
        inbox: DeliveryInbox,
        *,
        airflow_url: str,
        token_provider: TokenProvider | None,
        opener: Opener,
        log: Callable[[str], None],
        max_attempts: int = 12,
        work_orders: WorkOrders | None = None,
    ) -> None:
        from swfactory.webhook import _safe_airflow_base, _safe_backend_base

        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.airflow_url = _safe_airflow_base(airflow_url)
        # Managed mode: the receiver's only outbound mutation is a work order. Legacy direct-Airflow
        # mode stays reachable for a sandbox with no backend, and is the only mode that needs an
        # Airflow credential at all.
        self.work_orders = (
            None if work_orders is None else replace(work_orders, url=_safe_backend_base(work_orders.url))
        )
        if self.work_orders is None and token_provider is None:
            raise ValueError("legacy direct-Airflow dispatch needs an Airflow token provider")
        if self.work_orders is None:
            inbox.bind(airflow_url=self.airflow_url)
        else:
            inbox.bind(work_order_url=self.work_orders.url)
        self.inbox = inbox
        self.token_provider = token_provider
        self.opener = opener
        self.log = log
        self.max_attempts = max_attempts
        self.stopped = threading.Event()
        self.wakeup = threading.Event()
        self.thread = threading.Thread(target=self._run, name="swfactory-dispatch", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.stopped.set()
        self.wakeup.set()
        # If an API call is still in flight, its durable lease is recoverable on restart.
        self.thread.join(timeout=5)

    def _submit(self, delivery: Delivery) -> dict[str, Any]:
        """One attempt at the configured boundary. Returns the backend receipt, empty in legacy."""
        from swfactory.webhook import Trigger, submit_work_order, trigger_airflow

        trigger = Trigger(delivery.dag_id, delivery.conf, delivery.dag_run_id)
        if self.work_orders is not None:
            return submit_work_order(trigger, self.work_orders, opener=self.opener)
        assert self.token_provider is not None  # enforced in __init__
        trigger_airflow(
            trigger,
            airflow_url=self.airflow_url,
            token=self.token_provider(),
            opener=self.opener,
        )
        return {}

    def dispatch_one(self) -> bool:
        delivery = self.inbox.claim(max_attempts=self.max_attempts)
        if delivery is None:
            return False
        # A refusal from the managed boundary is the answer, not a reason to reach past it: there is
        # no branch below that falls back to Airflow, so a drain or a capacity refusal can never
        # produce a run the backend never admitted.
        channel = "work_order" if self.work_orders is not None else "airflow"
        retryable, delay, error, receipt = True, 0.0, "", {}
        try:
            receipt = self._submit(delivery)
        except DeliveryConflict:
            error, retryable = f"{channel}_identity_conflict", False
        except urllib.error.HTTPError as exc:
            error = f"{channel}_http_{exc.code}"
            retryable = exc.code in {408, 429} or 500 <= exc.code < 600
            delay = _retry_after(exc.headers)
            exc.close()
        except (urllib.error.URLError, OSError):
            error = f"{channel}_transport_error"
        except ValueError:
            error, retryable = "dispatch_configuration_error", False
        except RuntimeError:
            error = f"{channel}_invalid_response"
        except Exception:  # noqa: BLE001 - retain work and keep intake alive on adapter failures
            error = "dispatch_internal_error"
        if error:
            changed = self.inbox.fail(
                delivery,
                error,
                retryable=retryable,
                max_attempts=self.max_attempts,
                retry_after_s=delay,
            )
        else:
            changed = self.inbox.complete(
                delivery,
                work_order_id=str(receipt["submission_id"]) if receipt else None,
                admission_state=str(receipt["state"]) if receipt.get("state") else None,
            )
        admission = receipt.get("state")
        self.log(
            f"dispatch delivery={delivery.delivery_id} dag={delivery.dag_id} "
            f"attempt={delivery.total_attempts} outcome={error or 'dispatched'} "
            f"admission={admission or '-'} receipt={'saved' if changed else 'lease_lost'}"
        )
        return True

    def _run(self) -> None:
        while not self.stopped.is_set():
            self.wakeup.clear()
            try:
                if self.dispatch_one():
                    continue
            except (sqlite3.Error, OSError):
                # Never print database statements or upstream responses (both may hold data).
                self.log("dispatch outcome=inbox_unavailable; will retry")
            self.wakeup.wait(timeout=1)
