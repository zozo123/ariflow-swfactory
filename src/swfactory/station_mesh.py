"""Durable coordination between independent software-factory stations.

The mesh is deliberately *not* a scheduler.  Airflow still owns lifecycle scheduling and Factory
Cell epochs still own mutation authority.  A mesh signal is only a durable knock telling another
station what to re-read from an authoritative source.

A shared backend can expose one :class:`StationMesh` to many independently operated Airflow
stations working the same repository.  Station leases fence stale processes; coordination claims
prevent duplicate station-level ownership attempts; signals carry human/agent context and artifact
references without becoming executable commands.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
DEFAULT_LEASE_TTL_S = 120
DEFAULT_CLAIM_TTL_S = 900
DEFAULT_SIGNAL_TTL_S = 3600
MAX_TTL_S = 86_400
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}\Z")


class MeshError(RuntimeError):
    """The station-coordination contract was violated."""


class StaleStationLease(MeshError):
    """A process tried to act with a station lease that has been replaced or expired."""


class ClaimConflict(MeshError):
    """Another live station already owns the repo-level coordination claim."""

    def __init__(self, claim: dict[str, Any]):
        self.claim = claim
        super().__init__(
            f"{claim['cell_id']} is coordinated by {claim['station_id']} "
            f"at claim epoch {claim['claim_epoch']}"
        )


class SignalKind(StrEnum):
    INTENT = "intent"
    OBSERVATION = "observation"
    REQUEST = "request"
    OFFER = "offer"
    HANDOFF = "handoff"
    CONFLICT = "conflict"
    RESULT = "result"
    NOTE = "note"


@dataclass(frozen=True)
class StationLease:
    station_id: str
    repo: str
    incarnation_id: str
    lease_epoch: int
    operator: str
    generation: str
    capabilities: tuple[str, ...]
    metadata: dict[str, Any]
    heartbeat_at: float
    expires_at: float


@dataclass(frozen=True)
class MeshSignal:
    seq: int
    message_id: str
    repo: str
    station_id: str
    station_lease_epoch: int
    kind: str
    topic: str
    summary: str
    cell_id: str | None
    cell_epoch: int | None
    to_station: str | None
    reply_to: str | None
    artifacts: tuple[str, ...]
    payload: dict[str, Any]
    created_at: float
    expires_at: float


@dataclass(frozen=True)
class MeshClaim:
    repo: str
    cell_id: str
    cell_epoch: int
    claim_epoch: int
    station_id: str
    station_lease_epoch: int
    purpose: str
    created_at: float
    updated_at: float
    expires_at: float


def station_id(repo: str, operator: str, host: str) -> str:
    """Return a stable non-secret station id for one operator/host/repository tuple."""
    digest = hashlib.sha256(f"{repo}\0{operator}\0{host}".encode()).hexdigest()[:20]
    return f"station_{digest}"


def new_incarnation_id() -> str:
    """Return a process-incarnation token.  It is an identity fence, not a credential."""
    return "inc_" + uuid.uuid4().hex


def _required(value: Any, field: str, *, max_len: int = 256) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > max_len:
        raise ValueError(f"{field} must be a nonempty string of at most {max_len} characters")
    return value.strip()


def _identifier(value: Any, field: str) -> str:
    value = _required(value, field)
    if not _ID.fullmatch(value):
        raise ValueError(f"{field} contains unsupported characters")
    return value


def _ttl(value: Any, default: int) -> int:
    if value is None:
        return default
    if type(value) is not int or not 10 <= value <= MAX_TTL_S:
        raise ValueError(f"ttl_s must be an integer between 10 and {MAX_TTL_S}")
    return value


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object")
    # Validate serializability now instead of failing halfway through a transaction.
    json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return value


def _strings(value: Any, field: str, *, limit: int = 64) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)) or len(value) > limit:
        raise ValueError(f"{field} must be an array with at most {limit} values")
    result = tuple(_required(item, field, max_len=512) for item in value)
    return tuple(dict.fromkeys(result))


class StationMesh:
    """SQLite rendezvous for independent stations sharing one repository.

    The database may sit behind one shared backend HTTP endpoint.  Callers never acquire a Python
    lock as authority: SQLite transactions and explicit lease/claim epochs are the concurrency
    boundary, so a second process using the same store sees the same fences.
    """

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, timeout=30, isolation_level="IMMEDIATE", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def close(self) -> None:
        with self.lock:
            self.db.close()

    def _migrate(self) -> None:
        with self.lock, self.db:
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS stations (
                    station_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    repo TEXT NOT NULL,
                    incarnation_id TEXT NOT NULL,
                    lease_epoch INTEGER NOT NULL,
                    operator TEXT NOT NULL,
                    generation TEXT NOT NULL,
                    capabilities_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    heartbeat_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS stations_repo_expiry ON stations(repo, expires_at);
                CREATE TABLE IF NOT EXISTS signals (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT UNIQUE NOT NULL,
                    schema_version INTEGER NOT NULL,
                    repo TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    station_lease_epoch INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    cell_id TEXT,
                    cell_epoch INTEGER,
                    to_station TEXT,
                    reply_to TEXT,
                    artifacts_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS signals_repo_seq ON signals(repo, seq);
                CREATE INDEX IF NOT EXISTS signals_cell ON signals(repo, cell_id, cell_epoch, seq);
                CREATE TABLE IF NOT EXISTS claims (
                    repo TEXT NOT NULL,
                    cell_id TEXT NOT NULL,
                    cell_epoch INTEGER NOT NULL,
                    claim_epoch INTEGER NOT NULL,
                    station_id TEXT NOT NULL,
                    station_lease_epoch INTEGER NOT NULL,
                    purpose TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY(repo, cell_id)
                );
                CREATE INDEX IF NOT EXISTS claims_expiry ON claims(repo, expires_at);
                """
            )

    def join(
        self,
        *,
        station_id: str,
        repo: str,
        incarnation_id: str,
        operator: str,
        generation: str = "stable",
        capabilities: Any = None,
        metadata: Any = None,
        ttl_s: int | None = None,
    ) -> StationLease:
        station_id = _identifier(station_id, "station_id")
        repo = _required(repo, "repo")
        incarnation_id = _identifier(incarnation_id, "incarnation_id")
        operator = _required(operator, "operator")
        generation = _identifier(generation, "generation")
        capabilities = _strings(capabilities, "capabilities")
        metadata = _mapping(metadata, "metadata")
        ttl = _ttl(ttl_s, DEFAULT_LEASE_TTL_S)
        now = time.time()
        expires = now + ttl
        with self.lock, self.db:
            row = self.db.execute("SELECT * FROM stations WHERE station_id=?", (station_id,)).fetchone()
            lease_epoch = 1 if row is None else int(row["lease_epoch"])
            created = now if row is None else float(row["created_at"])
            if row is not None and row["incarnation_id"] != incarnation_id:
                lease_epoch += 1
            self.db.execute(
                """INSERT INTO stations(
                    station_id,schema_version,repo,incarnation_id,lease_epoch,operator,generation,
                    capabilities_json,metadata_json,heartbeat_at,expires_at,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(station_id) DO UPDATE SET
                    schema_version=excluded.schema_version,repo=excluded.repo,
                    incarnation_id=excluded.incarnation_id,lease_epoch=excluded.lease_epoch,
                    operator=excluded.operator,generation=excluded.generation,
                    capabilities_json=excluded.capabilities_json,metadata_json=excluded.metadata_json,
                    heartbeat_at=excluded.heartbeat_at,expires_at=excluded.expires_at,
                    updated_at=excluded.updated_at""",
                (
                    station_id,
                    SCHEMA_VERSION,
                    repo,
                    incarnation_id,
                    lease_epoch,
                    operator,
                    generation,
                    json.dumps(capabilities),
                    json.dumps(metadata, sort_keys=True, separators=(",", ":")),
                    now,
                    expires,
                    created,
                    now,
                ),
            )
            return self._station(station_id)

    def heartbeat(
        self,
        station_id: str,
        lease_epoch: int,
        incarnation_id: str,
        *,
        ttl_s: int | None = None,
    ) -> StationLease:
        ttl = _ttl(ttl_s, DEFAULT_LEASE_TTL_S)
        now = time.time()
        with self.lock, self.db:
            cur = self.db.execute(
                """UPDATE stations SET heartbeat_at=?,expires_at=?,updated_at=?
                   WHERE station_id=? AND lease_epoch=? AND incarnation_id=?""",
                (now, now + ttl, now, station_id, lease_epoch, incarnation_id),
            )
            if cur.rowcount != 1:
                raise StaleStationLease(f"{station_id}: station lease is stale")
            return self._station(station_id)

    def leave(self, station_id: str, lease_epoch: int, incarnation_id: str) -> StationLease:
        now = time.time()
        with self.lock, self.db:
            cur = self.db.execute(
                """UPDATE stations SET heartbeat_at=?,expires_at=?,updated_at=?
                   WHERE station_id=? AND lease_epoch=? AND incarnation_id=?""",
                (now, now, now, station_id, lease_epoch, incarnation_id),
            )
            if cur.rowcount != 1:
                raise StaleStationLease(f"{station_id}: station lease is stale")
            return self._station(station_id)

    def peers(self, repo: str, *, include_expired: bool = False, limit: int = 100) -> list[StationLease]:
        repo = _required(repo, "repo")
        limit = max(1, min(int(limit), 1000))
        now = time.time()
        with self.lock:
            query = "SELECT station_id FROM stations WHERE repo=?"
            args: list[Any] = [repo]
            if not include_expired:
                query += " AND expires_at>?"
                args.append(now)
            query += " ORDER BY updated_at DESC LIMIT ?"
            args.append(limit)
            rows = self.db.execute(query, args).fetchall()
            return [self._station(str(row["station_id"])) for row in rows]

    def say(
        self,
        *,
        station_id: str,
        station_lease_epoch: int,
        repo: str,
        kind: str,
        topic: str,
        summary: str,
        cell_id: str | None = None,
        cell_epoch: int | None = None,
        to_station: str | None = None,
        reply_to: str | None = None,
        artifacts: Any = None,
        payload: Any = None,
        ttl_s: int | None = None,
        message_id: str | None = None,
    ) -> MeshSignal:
        repo = _required(repo, "repo")
        kind = SignalKind(kind).value
        topic = _required(topic, "topic")
        summary = _required(summary, "summary", max_len=2000)
        if cell_id is not None:
            cell_id = _identifier(cell_id, "cell_id")
            if not cell_id.startswith("cell_"):
                raise ValueError("cell_id must be a Factory Cell id")
            if type(cell_epoch) is not int or cell_epoch < 1:
                raise ValueError("cell_epoch must be positive when cell_id is set")
        elif cell_epoch is not None:
            raise ValueError("cell_epoch requires cell_id")
        to_station = _identifier(to_station, "to_station") if to_station else None
        reply_to = _identifier(reply_to, "reply_to") if reply_to else None
        artifacts = _strings(artifacts, "artifacts")
        payload = _mapping(payload, "payload")
        ttl = _ttl(ttl_s, DEFAULT_SIGNAL_TTL_S)
        message_id = _identifier(message_id or ("msg_" + uuid.uuid4().hex), "message_id")
        now = time.time()
        with self.lock, self.db:
            self._assert_live_station(station_id, station_lease_epoch, repo, now)
            canonical = (
                repo,
                station_id,
                station_lease_epoch,
                kind,
                topic,
                summary,
                cell_id,
                cell_epoch,
                to_station,
                reply_to,
                json.dumps(artifacts),
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
            )
            try:
                self.db.execute(
                    """INSERT INTO signals(
                        message_id,schema_version,repo,station_id,station_lease_epoch,kind,topic,summary,
                        cell_id,cell_epoch,to_station,reply_to,artifacts_json,payload_json,created_at,expires_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (message_id, SCHEMA_VERSION, *canonical, now, now + ttl),
                )
            except sqlite3.IntegrityError:
                row = self.db.execute("SELECT * FROM signals WHERE message_id=?", (message_id,)).fetchone()
                if row is None or self._signal_canonical(row) != canonical:
                    raise MeshError(f"message_id {message_id} was reused with different content") from None
            return self._signal_by_id(message_id)

    def signals(
        self,
        repo: str,
        *,
        after_seq: int = 0,
        topic: str | None = None,
        cell_id: str | None = None,
        to_station: str | None = None,
        include_expired: bool = False,
        limit: int = 100,
    ) -> list[MeshSignal]:
        repo = _required(repo, "repo")
        if type(after_seq) is not int or after_seq < 0:
            raise ValueError("after_seq must be a nonnegative integer")
        limit = max(1, min(int(limit), 1000))
        query = "SELECT * FROM signals WHERE repo=? AND seq>?"
        args: list[Any] = [repo, after_seq]
        if not include_expired:
            query += " AND expires_at>?"
            args.append(time.time())
        if topic:
            query += " AND topic=?"
            args.append(_required(topic, "topic"))
        if cell_id:
            query += " AND cell_id=?"
            args.append(_identifier(cell_id, "cell_id"))
        if to_station:
            query += " AND (to_station IS NULL OR to_station=?)"
            args.append(_identifier(to_station, "to_station"))
        query += " ORDER BY seq ASC LIMIT ?"
        args.append(limit)
        with self.lock:
            return [self._decode_signal(row) for row in self.db.execute(query, args).fetchall()]

    def claim(
        self,
        *,
        repo: str,
        cell_id: str,
        cell_epoch: int,
        station_id: str,
        station_lease_epoch: int,
        purpose: str = "work",
        ttl_s: int | None = None,
    ) -> MeshClaim:
        repo = _required(repo, "repo")
        cell_id = _identifier(cell_id, "cell_id")
        if not cell_id.startswith("cell_"):
            raise ValueError("cell_id must be a Factory Cell id")
        if type(cell_epoch) is not int or cell_epoch < 1:
            raise ValueError("cell_epoch must be positive")
        purpose = _identifier(purpose, "purpose")
        ttl = _ttl(ttl_s, DEFAULT_CLAIM_TTL_S)
        now = time.time()
        with self.lock, self.db:
            self._assert_live_station(station_id, station_lease_epoch, repo, now)
            row = self.db.execute("SELECT * FROM claims WHERE repo=? AND cell_id=?", (repo, cell_id)).fetchone()
            if row is None:
                claim_epoch = 1
                created = now
            else:
                current = self._decode_claim(row)
                same_owner = (
                    current.station_id == station_id
                    and current.station_lease_epoch == station_lease_epoch
                    and current.cell_epoch == cell_epoch
                )
                if current.expires_at > now and not same_owner:
                    raise ClaimConflict(self._claim_dict(current))
                claim_epoch = current.claim_epoch if same_owner else current.claim_epoch + 1
                created = current.created_at if same_owner else now
            self.db.execute(
                """INSERT INTO claims(
                    repo,cell_id,cell_epoch,claim_epoch,station_id,station_lease_epoch,purpose,
                    created_at,updated_at,expires_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(repo,cell_id) DO UPDATE SET
                    cell_epoch=excluded.cell_epoch,claim_epoch=excluded.claim_epoch,
                    station_id=excluded.station_id,station_lease_epoch=excluded.station_lease_epoch,
                    purpose=excluded.purpose,created_at=excluded.created_at,
                    updated_at=excluded.updated_at,expires_at=excluded.expires_at""",
                (
                    repo,
                    cell_id,
                    cell_epoch,
                    claim_epoch,
                    station_id,
                    station_lease_epoch,
                    purpose,
                    created,
                    now,
                    now + ttl,
                ),
            )
            return self._claim(repo, cell_id)

    def release(
        self,
        *,
        repo: str,
        cell_id: str,
        station_id: str,
        station_lease_epoch: int,
        claim_epoch: int,
    ) -> None:
        now = time.time()
        with self.lock, self.db:
            self._assert_live_station(station_id, station_lease_epoch, _required(repo, "repo"), now)
            cur = self.db.execute(
                """DELETE FROM claims WHERE repo=? AND cell_id=? AND station_id=?
                   AND station_lease_epoch=? AND claim_epoch=?""",
                (repo, cell_id, station_id, station_lease_epoch, claim_epoch),
            )
            if cur.rowcount != 1:
                raise ClaimConflict(self._claim_dict(self._claim(repo, cell_id)))

    def claims(self, repo: str, *, include_expired: bool = False, limit: int = 100) -> list[MeshClaim]:
        repo = _required(repo, "repo")
        limit = max(1, min(int(limit), 1000))
        query = "SELECT * FROM claims WHERE repo=?"
        args: list[Any] = [repo]
        if not include_expired:
            query += " AND expires_at>?"
            args.append(time.time())
        query += " ORDER BY updated_at DESC LIMIT ?"
        args.append(limit)
        with self.lock:
            return [self._decode_claim(row) for row in self.db.execute(query, args).fetchall()]

    def conflicts(self, repo: str) -> list[dict[str, Any]]:
        """Return coordination anomalies that need a human/station response.

        Claims are single-row CAS leases, so two live claims cannot exist.  The useful anomalies are
        an unexpired claim owned by a station whose lease died, and competing live ``intent``
        signals emitted before a claim converged.
        """
        repo = _required(repo, "repo")
        now = time.time()
        live = {peer.station_id for peer in self.peers(repo, limit=1000)}
        out: list[dict[str, Any]] = []
        for claim in self.claims(repo, limit=1000):
            if claim.station_id not in live:
                out.append(
                    {
                        "kind": "orphaned_claim",
                        "cell_id": claim.cell_id,
                        "cell_epoch": claim.cell_epoch,
                        "station_id": claim.station_id,
                        "claim_epoch": claim.claim_epoch,
                        "expires_at": claim.expires_at,
                    }
                )
        intents: dict[tuple[str, int], set[str]] = {}
        with self.lock:
            rows = self.db.execute(
                """SELECT cell_id,cell_epoch,station_id FROM signals
                   WHERE repo=? AND kind=? AND cell_id IS NOT NULL AND expires_at>?""",
                (repo, SignalKind.INTENT.value, now),
            ).fetchall()
        for row in rows:
            if row["station_id"] in live:
                intents.setdefault((str(row["cell_id"]), int(row["cell_epoch"])), set()).add(str(row["station_id"]))
        for (cell_id, cell_epoch), stations in sorted(intents.items()):
            if len(stations) > 1:
                out.append(
                    {
                        "kind": "competing_intents",
                        "cell_id": cell_id,
                        "cell_epoch": cell_epoch,
                        "stations": sorted(stations),
                    }
                )
        return out

    def _assert_live_station(self, station_id: str, lease_epoch: int, repo: str, now: float) -> sqlite3.Row:
        if type(lease_epoch) is not int or lease_epoch < 1:
            raise ValueError("station_lease_epoch must be positive")
        row = self.db.execute(
            """SELECT * FROM stations WHERE station_id=? AND lease_epoch=? AND repo=? AND expires_at>?""",
            (station_id, lease_epoch, repo, now),
        ).fetchone()
        if row is None:
            raise StaleStationLease(f"{station_id}: no live station lease at epoch {lease_epoch}")
        return row

    def _station(self, station_id: str) -> StationLease:
        row = self.db.execute("SELECT * FROM stations WHERE station_id=?", (station_id,)).fetchone()
        if row is None:
            raise KeyError(station_id)
        return StationLease(
            station_id=str(row["station_id"]),
            repo=str(row["repo"]),
            incarnation_id=str(row["incarnation_id"]),
            lease_epoch=int(row["lease_epoch"]),
            operator=str(row["operator"]),
            generation=str(row["generation"]),
            capabilities=tuple(json.loads(row["capabilities_json"])),
            metadata=dict(json.loads(row["metadata_json"])),
            heartbeat_at=float(row["heartbeat_at"]),
            expires_at=float(row["expires_at"]),
        )

    def _signal_by_id(self, message_id: str) -> MeshSignal:
        row = self.db.execute("SELECT * FROM signals WHERE message_id=?", (message_id,)).fetchone()
        if row is None:
            raise KeyError(message_id)
        return self._decode_signal(row)

    @staticmethod
    def _decode_signal(row: sqlite3.Row) -> MeshSignal:
        return MeshSignal(
            seq=int(row["seq"]),
            message_id=str(row["message_id"]),
            repo=str(row["repo"]),
            station_id=str(row["station_id"]),
            station_lease_epoch=int(row["station_lease_epoch"]),
            kind=str(row["kind"]),
            topic=str(row["topic"]),
            summary=str(row["summary"]),
            cell_id=str(row["cell_id"]) if row["cell_id"] is not None else None,
            cell_epoch=int(row["cell_epoch"]) if row["cell_epoch"] is not None else None,
            to_station=str(row["to_station"]) if row["to_station"] is not None else None,
            reply_to=str(row["reply_to"]) if row["reply_to"] is not None else None,
            artifacts=tuple(json.loads(row["artifacts_json"])),
            payload=dict(json.loads(row["payload_json"])),
            created_at=float(row["created_at"]),
            expires_at=float(row["expires_at"]),
        )

    @staticmethod
    def _signal_canonical(row: sqlite3.Row) -> tuple[Any, ...]:
        return (
            str(row["repo"]),
            str(row["station_id"]),
            int(row["station_lease_epoch"]),
            str(row["kind"]),
            str(row["topic"]),
            str(row["summary"]),
            str(row["cell_id"]) if row["cell_id"] is not None else None,
            int(row["cell_epoch"]) if row["cell_epoch"] is not None else None,
            str(row["to_station"]) if row["to_station"] is not None else None,
            str(row["reply_to"]) if row["reply_to"] is not None else None,
            str(row["artifacts_json"]),
            str(row["payload_json"]),
        )

    def _claim(self, repo: str, cell_id: str) -> MeshClaim:
        row = self.db.execute("SELECT * FROM claims WHERE repo=? AND cell_id=?", (repo, cell_id)).fetchone()
        if row is None:
            raise KeyError(cell_id)
        return self._decode_claim(row)

    @staticmethod
    def _decode_claim(row: sqlite3.Row) -> MeshClaim:
        return MeshClaim(
            repo=str(row["repo"]),
            cell_id=str(row["cell_id"]),
            cell_epoch=int(row["cell_epoch"]),
            claim_epoch=int(row["claim_epoch"]),
            station_id=str(row["station_id"]),
            station_lease_epoch=int(row["station_lease_epoch"]),
            purpose=str(row["purpose"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            expires_at=float(row["expires_at"]),
        )

    @staticmethod
    def _claim_dict(claim: MeshClaim) -> dict[str, Any]:
        return {
            "repo": claim.repo,
            "cell_id": claim.cell_id,
            "cell_epoch": claim.cell_epoch,
            "claim_epoch": claim.claim_epoch,
            "station_id": claim.station_id,
            "station_lease_epoch": claim.station_lease_epoch,
            "purpose": claim.purpose,
            "created_at": claim.created_at,
            "updated_at": claim.updated_at,
            "expires_at": claim.expires_at,
        }
