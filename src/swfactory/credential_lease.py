"""Opaque, attempt-bound credential leases for trusted control-plane capabilities.

Bearer material is never persisted.  The durable store keeps only a hash of the opaque handle,
immutable execution binding, process binding, expiry/revocation state, and negative provenance.
Airflow workers and coding sandboxes are not permitted to redeem leases; the broker is a backend
control-plane primitive.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from swfactory.cells import is_cell_id

LEASE_SCHEMA_VERSION = 1
_MIN_NONCE_LEN = 16


class CredentialLeaseError(PermissionError):
    """A credential lease could not be minted or redeemed safely."""


@dataclass(frozen=True)
class LeaseBinding:
    factory_run_id: str
    dag_run_id: str
    task_instance_id: str
    stage_id: str
    sandbox_id: str
    attempt_number: int
    cell_id: str
    epoch: int
    operation_key: str
    policy_digest: str

    def validate(self) -> None:
        strings = {
            "factory_run_id": self.factory_run_id,
            "dag_run_id": self.dag_run_id,
            "task_instance_id": self.task_instance_id,
            "stage_id": self.stage_id,
            "sandbox_id": self.sandbox_id,
            "operation_key": self.operation_key,
            "policy_digest": self.policy_digest,
        }
        for name, value in strings.items():
            if not isinstance(value, str) or not value.strip() or len(value) > 512:
                raise ValueError(f"{name} must be a nonempty bounded string")
        if not is_cell_id(self.cell_id):
            raise ValueError("credential lease requires a valid Factory Cell id")
        if self.epoch < 1:
            raise ValueError("credential lease epoch must be positive")
        if self.attempt_number < 1:
            raise ValueError("credential lease attempt_number must be positive")
        if not self.policy_digest.startswith("policy:"):
            raise ValueError("credential lease requires a canonical policy digest")

    def canonical(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)

    def digest(self) -> str:
        payload = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":")).encode()
        return "lease-binding:" + hashlib.sha256(payload).hexdigest()

    @classmethod
    def from_untrusted(cls, raw: Mapping[str, Any]) -> LeaseBinding:
        """Parse a scheduler/artifact projection without accepting extra authority-bearing fields."""
        allowed = {
            "factory_run_id",
            "dag_run_id",
            "task_instance_id",
            "stage_id",
            "sandbox_id",
            "attempt_number",
            "cell_id",
            "epoch",
            "operation_key",
            "policy_digest",
        }
        unknown = sorted(set(raw) - allowed)
        if unknown:
            raise CredentialLeaseError(
                "untrusted lease binding carries unsupported fields: " + ", ".join(unknown)
            )
        try:
            binding = cls(
                factory_run_id=str(raw["factory_run_id"]),
                dag_run_id=str(raw["dag_run_id"]),
                task_instance_id=str(raw["task_instance_id"]),
                stage_id=str(raw["stage_id"]),
                sandbox_id=str(raw["sandbox_id"]),
                attempt_number=int(raw["attempt_number"]),
                cell_id=str(raw["cell_id"]),
                epoch=int(raw["epoch"]),
                operation_key=str(raw["operation_key"]),
                policy_digest=str(raw["policy_digest"]),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise CredentialLeaseError(f"invalid untrusted lease binding: {error}") from error
        binding.validate()
        return binding


@dataclass(frozen=True, repr=False)
class LeaseHandle:
    lease_id: str
    bearer: str

    def __repr__(self) -> str:
        return f"LeaseHandle(lease_id={self.lease_id!r}, bearer='[REDACTED]')"


@dataclass(frozen=True)
class LeaseDenial:
    lease_id: str
    reason: str
    binding_digest: str | None
    capability: str | None
    created_at: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


CredentialProvider = Callable[[LeaseBinding], str]


class CredentialLeaseBroker:
    """Durable lease authority with opaque handles and process-bound redeem semantics."""

    def __init__(
        self,
        path: Path,
        *,
        providers: Mapping[str, CredentialProvider] | None = None,
        on_denial: Callable[[LeaseDenial], None] | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.providers = dict(providers or {})
        self.on_denial = on_denial
        self.lock = threading.RLock()
        self.db = sqlite3.connect(
            self.path,
            timeout=30,
            isolation_level="IMMEDIATE",
            check_same_thread=False,
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
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS credential_leases (
                    lease_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    secret_hash TEXT NOT NULL,
                    capability TEXT NOT NULL,
                    binding_json TEXT NOT NULL,
                    binding_digest TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    process_hash TEXT,
                    revoked_at REAL,
                    revoke_reason TEXT,
                    created_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS credential_lease_epoch
                    ON credential_leases(cell_id, epoch);
                """
                .replace(
                    "CREATE INDEX IF NOT EXISTS credential_lease_epoch\n                    ON credential_leases(cell_id, epoch);",
                    "",
                )
            )
            columns = {row["name"] for row in self.db.execute("PRAGMA table_info(credential_leases)")}
            additions = {
                "cell_id": "TEXT",
                "epoch": "INTEGER",
                "attempt_number": "INTEGER",
            }
            for name, ddl in additions.items():
                if name not in columns:
                    self.db.execute(f"ALTER TABLE credential_leases ADD COLUMN {name} {ddl}")
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS credential_lease_epoch "
                "ON credential_leases(cell_id, epoch)"
            )
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS credential_denials (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    lease_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    binding_digest TEXT,
                    capability TEXT,
                    created_at REAL NOT NULL
                );
                """
            )

    def mint(
        self,
        binding: LeaseBinding,
        *,
        capability: str,
        ttl_s: float = 300.0,
        now: float | None = None,
    ) -> LeaseHandle:
        binding.validate()
        capability = capability.strip()
        if capability not in self.providers:
            raise CredentialLeaseError(f"capability {capability!r} has no trusted provider")
        if ttl_s <= 0 or ttl_s > 3600:
            raise ValueError("credential lease ttl_s must be in (0, 3600]")
        clock = time.time() if now is None else now
        lease_id = "lease_" + secrets.token_hex(16)
        bearer = "swfl_" + secrets.token_urlsafe(32)
        secret_hash = _hash_secret(bearer)
        document = json.dumps(binding.canonical(), sort_keys=True, separators=(",", ":"))
        with self.lock, self.db:
            self.db.execute(
                """INSERT INTO credential_leases(
                    lease_id,schema_version,secret_hash,capability,binding_json,binding_digest,
                    expires_at,process_hash,revoked_at,revoke_reason,created_at,
                    cell_id,epoch,attempt_number
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    lease_id,
                    LEASE_SCHEMA_VERSION,
                    secret_hash,
                    capability,
                    document,
                    binding.digest(),
                    clock + ttl_s,
                    None,
                    None,
                    None,
                    clock,
                    binding.cell_id,
                    binding.epoch,
                    binding.attempt_number,
                ),
            )
        return LeaseHandle(lease_id, bearer)

    def redeem(
        self,
        handle: LeaseHandle,
        binding: LeaseBinding,
        *,
        process_nonce: str,
        now: float | None = None,
    ) -> str:
        binding.validate()
        if len(process_nonce) < _MIN_NONCE_LEN:
            raise ValueError(f"process_nonce must contain at least {_MIN_NONCE_LEN} characters")
        clock = time.time() if now is None else now
        with self.lock, self.db:
            row = self.db.execute(
                "SELECT * FROM credential_leases WHERE lease_id=?",
                (handle.lease_id,),
            ).fetchone()
            if row is None:
                self._deny(handle.lease_id, "unknown_lease", binding=binding, capability=None, now=clock)
            assert row is not None
            capability = str(row["capability"])
            if not hmac.compare_digest(str(row["secret_hash"]), _hash_secret(handle.bearer)):
                self._deny(handle.lease_id, "invalid_bearer", binding=binding, capability=capability, now=clock)
            if row["revoked_at"] is not None:
                self._deny(handle.lease_id, "revoked", binding=binding, capability=capability, now=clock)
            if float(row["expires_at"]) <= clock:
                self._deny(handle.lease_id, "expired", binding=binding, capability=capability, now=clock)
            if not hmac.compare_digest(str(row["binding_digest"]), binding.digest()):
                self._deny(
                    handle.lease_id,
                    "binding_mismatch",
                    binding=binding,
                    capability=capability,
                    now=clock,
                )
            process_hash = _hash_secret(process_nonce)
            existing = row["process_hash"]
            if existing is None:
                self.db.execute(
                    "UPDATE credential_leases SET process_hash=? "
                    "WHERE lease_id=? AND process_hash IS NULL",
                    (process_hash, handle.lease_id),
                )
                row = self.db.execute(
                    "SELECT * FROM credential_leases WHERE lease_id=?",
                    (handle.lease_id,),
                ).fetchone()
                existing = row["process_hash"] if row is not None else None
            if existing is None or not hmac.compare_digest(str(existing), process_hash):
                self._deny(
                    handle.lease_id,
                    "process_mismatch",
                    binding=binding,
                    capability=capability,
                    now=clock,
                )
            provider = self.providers.get(capability)
            if provider is None:
                self._deny(
                    handle.lease_id,
                    "provider_unavailable",
                    binding=binding,
                    capability=capability,
                    now=clock,
                )
        value = provider(binding)
        if not isinstance(value, str) or not value:
            raise CredentialLeaseError(f"trusted provider for {capability!r} returned no credential")
        return value

    def revoke_epoch(
        self,
        cell_id: str,
        epoch: int,
        *,
        reason: str = "epoch_advanced",
        now: float | None = None,
    ) -> int:
        """Synchronously revoke every still-live lease issued under a superseded fence."""
        clock = time.time() if now is None else now
        with self.lock, self.db:
            cur = self.db.execute(
                """UPDATE credential_leases
                   SET revoked_at=?, revoke_reason=?
                   WHERE cell_id=? AND epoch=? AND revoked_at IS NULL""",
                (clock, reason, cell_id, epoch),
            )
            return int(cur.rowcount)

    def denials(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.db.execute(
                "SELECT lease_id,reason,binding_digest,capability,created_at "
                "FROM credential_denials ORDER BY seq DESC LIMIT ?",
                (max(1, min(int(limit), 1000)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def inspect(self, lease_id: str) -> dict[str, Any]:
        """Return metadata only. Bearer hashes and provider values are never operator surface."""
        with self.lock:
            row = self.db.execute(
                "SELECT lease_id,capability,binding_digest,expires_at,revoked_at,revoke_reason,"
                "created_at,cell_id,epoch,attempt_number,process_hash IS NOT NULL AS process_bound "
                "FROM credential_leases WHERE lease_id=?",
                (lease_id,),
            ).fetchone()
        if row is None:
            raise KeyError(lease_id)
        return dict(row)

    def _deny(
        self,
        lease_id: str,
        reason: str,
        *,
        binding: LeaseBinding | None,
        capability: str | None,
        now: float,
    ) -> None:
        event = LeaseDenial(
            lease_id=lease_id,
            reason=reason,
            binding_digest=binding.digest() if binding is not None else None,
            capability=capability,
            created_at=now,
        )
        self.db.execute(
            """INSERT INTO credential_denials(
                lease_id,reason,binding_digest,capability,created_at
            ) VALUES(?,?,?,?,?)""",
            (
                event.lease_id,
                event.reason,
                event.binding_digest,
                event.capability,
                event.created_at,
            ),
        )
        if self.on_denial is not None:
            self.on_denial(event)
        raise CredentialLeaseError(f"credential lease denied: {reason}")


def _hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()
