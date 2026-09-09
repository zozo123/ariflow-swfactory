"""Explicit schema identity, compatibility checks and forward migrations for factory state.

The factory's authoritative state is five things on one local filesystem: the Factory Cell store,
the operation journal, the admission/dispatch store, the repair-lease store, and the evidence tree.
Before this module each of them created missing tables on open and carried no version anyone could
compare, so two failures were silent:

* an *older* binary opening state a newer one wrote would happily add its own tables next to the
  newer ones and keep writing -- a rollback that corrupts rather than refuses;
* a partially restored or hand-assembled state root looked identical to a healthy one, because
  "the tables exist" was the only check anything made.

Every store therefore stamps ``PRAGMA user_version`` with the schema version this binary writes and
refuses anything higher.  ``user_version`` is chosen deliberately over a metadata row: it is
readable and enforceable before any table is trusted, and it survives on the file even when the
tables are unreadable.  Legacy stores carry version 0 and are adopted only after their required
tables are proven present, so an empty file cannot be mistaken for a migrated one.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# The Cell row still carries a ``schema_version`` column. It is kept in step with the store version
# here rather than in ``cells.py`` so the row field and the file stamp can never disagree about what
# this binary writes.
CELL_ROW_SCHEMA_VERSION = 1
EVIDENCE_SCHEMA_VERSION = 1
EVIDENCE_MARKER = ".schema.json"


class StoreSchemaError(RuntimeError):
    """A durable store cannot be used at its current schema."""


class StoreSchemaTooNew(StoreSchemaError):
    """The store was written by a newer binary; an older one must refuse it, not downgrade it."""


class StoreSchemaIncomplete(StoreSchemaError):
    """The file exists but does not hold the tables its declared schema promises."""


@dataclass(frozen=True)
class StoreSchema:
    """One authoritative store: where it lives, what this binary writes, what must be inside it."""

    name: str
    relative_path: str
    version: int
    required_tables: frozenset[str] = frozenset()
    kind: str = "sqlite"

    def path(self, state_root: Path) -> Path:
        return Path(state_root) / self.relative_path


# Every authoritative store of one factory, addressed relative to the shared local state root. A
# backup that does not cover all of these is not a backup of the factory.
FACTORY_STORES: tuple[StoreSchema, ...] = (
    StoreSchema("cells", "cells.sqlite3", 1, frozenset({"cells", "cell_events"})),
    StoreSchema("operations", "control/operations.sqlite3", 1, frozenset({"operations"})),
    StoreSchema(
        "admission",
        "control/admission.sqlite3",
        1,
        frozenset({"admission_work", "admission_order", "admission_members", "admission_dispatch"}),
    ),
    StoreSchema("repairs", "control/repairs.sqlite3", 1, frozenset({"repair_leases"})),
    StoreSchema("evidence", "evidence", EVIDENCE_SCHEMA_VERSION, kind="evidence"),
)

STORES_BY_NAME: dict[str, StoreSchema] = {store.name: store for store in FACTORY_STORES}


def supported_versions() -> dict[str, int]:
    """The store versions this binary writes; a restore compares a manifest against exactly this."""
    return {store.name: store.version for store in FACTORY_STORES}


# ------------------------------------------------------------------ forward migrations


def _migrate_cells_rows(db: sqlite3.Connection) -> None:
    """Adopt an unstamped Cell store only if no row claims a schema this binary cannot read.

    The row field is the older half of Cell versioning and the only place a newer binary's data can
    hide once the file stamp is missing, so it is checked before the file is stamped as ours.
    """
    row = db.execute("SELECT max(schema_version) FROM cells").fetchone()
    highest = int(row[0]) if row and row[0] is not None else 0
    if highest > CELL_ROW_SCHEMA_VERSION:
        raise StoreSchemaTooNew(
            f"cells store holds rows at cell schema {highest}; this binary writes "
            f"{CELL_ROW_SCHEMA_VERSION}. Run the newer binary; an older one would write rows the "
            "newer one cannot interpret."
        )


# ``ADOPTIONS`` runs when an unstamped (version 0) legacy store is first stamped. ``MIGRATIONS``
# holds real version-to-version steps; a gap with no registered step is a refusal, never a guess.
ADOPTIONS: dict[str, Callable[[sqlite3.Connection], None]] = {"cells": _migrate_cells_rows}
MIGRATIONS: dict[str, dict[int, Callable[[sqlite3.Connection], None]]] = {}


# ------------------------------------------------------------------ enforcement at open


def connect_read(path: Path) -> sqlite3.Connection:
    """Open a store for inspection without any intent to write it.

    ``mode=ro`` states the intent, but a WAL database whose shared-memory index does not yet exist
    cannot always be opened read-only. Falling back keeps a healthy factory from being reported as
    unreadable -- a false refusal is not a safe default here, it just teaches operators to ignore
    the check.
    """
    try:
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error:
        return sqlite3.connect(path)


def read_user_version(db: sqlite3.Connection) -> int:
    return int(db.execute("PRAGMA user_version").fetchone()[0])


def table_names(db: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def guard_before_ddl(db: sqlite3.Connection, name: str) -> int:
    """Refuse a store this binary must not own, *before* its own DDL papers over the damage.

    Every store opens by running ``CREATE TABLE IF NOT EXISTS``. On a store that has already been
    stamped, that DDL will happily recreate a table that went missing and hand back something that
    looks healthy and has lost its rows -- which is exactly how a half-restored file becomes an
    empty factory nobody notices. So a stamped store proves its tables before anything is created;
    an unstamped one (a fresh file, or a legacy store) is allowed to have them created.
    """
    schema = STORES_BY_NAME[name]
    version = read_user_version(db)
    if version > schema.version:
        raise StoreSchemaTooNew(
            f"{schema.name} store is at schema {version}; this binary supports {schema.version}. "
            "Restore with the newer binary, or roll the binary forward: an older one writing this "
            "store would silently downgrade state the newer one already committed."
        )
    if version >= 1:
        missing = sorted(schema.required_tables - table_names(db))
        if missing:
            raise StoreSchemaIncomplete(
                f"{schema.name} store is stamped at schema {version} but is missing {missing}; it is "
                "damaged or partially restored. Restore it from a verified backup rather than "
                "letting the factory create empty tables over it."
            )
    return version


def ensure_sqlite_schema(db: sqlite3.Connection, schema: StoreSchema) -> int:
    """Verify, migrate and stamp one open store. Call it after the store's own DDL has run.

    Returns the version now stamped on the file.
    """
    version = read_user_version(db)
    if version > schema.version:
        raise StoreSchemaTooNew(
            f"{schema.name} store is at schema {version}; this binary supports {schema.version}. "
            "Restore with the newer binary, or roll the binary forward: an older one writing this "
            "store would silently downgrade state the newer one already committed."
        )
    missing = sorted(schema.required_tables - table_names(db))
    if missing:
        raise StoreSchemaIncomplete(
            f"{schema.name} store is missing required tables {missing}; it is not a usable "
            f"{schema.name} store. Restore it from a verified backup rather than letting the "
            "factory create empty tables over a partial file."
        )
    if version == schema.version:
        return version
    if version == 0:
        adopt = ADOPTIONS.get(schema.name)
        if adopt is not None:
            adopt(db)
    else:
        steps = MIGRATIONS.get(schema.name, {})
        for step in range(version, schema.version):
            migrate = steps.get(step)
            if migrate is None:
                raise StoreSchemaError(
                    f"{schema.name} store is at schema {step} and this binary has no migration to "
                    f"{step + 1}; refusing to write to state it cannot upgrade."
                )
            migrate(db)
    db.execute(f"PRAGMA user_version={int(schema.version)}")
    return schema.version


def ensure_named_schema(db: sqlite3.Connection, name: str) -> int:
    """``ensure_sqlite_schema`` addressed by store name, for stores that own their own path."""
    return ensure_sqlite_schema(db, STORES_BY_NAME[name])


def ensure_evidence_schema(root: Path) -> int:
    """Stamp/verify the evidence tree, which is files rather than a database."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    marker = root / EVIDENCE_MARKER
    if marker.is_file():
        try:
            declared = int(json.loads(marker.read_text(encoding="utf-8"))["schema_version"])
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise StoreSchemaIncomplete(f"evidence schema marker {marker} is unreadable: {error}") from error
        if declared > EVIDENCE_SCHEMA_VERSION:
            raise StoreSchemaTooNew(
                f"evidence tree is at schema {declared}; this binary supports {EVIDENCE_SCHEMA_VERSION}."
            )
        return declared
    marker.write_text(json.dumps({"schema_version": EVIDENCE_SCHEMA_VERSION}) + "\n", encoding="utf-8")
    return EVIDENCE_SCHEMA_VERSION


# ------------------------------------------------------------------ read-only inspection


@dataclass(frozen=True)
class StoreStatus:
    """What one store looks like right now, without opening it for writing or migrating it."""

    name: str
    path: str
    present: bool
    version: int | None
    expected: int
    status: str
    detail: str = ""

    @property
    def usable(self) -> bool:
        return self.status in {"ok", "absent", "migratable"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "present": self.present,
            "version": self.version,
            "expected": self.expected,
            "status": self.status,
            "detail": self.detail,
        }


def inspect_store(state_root: Path, schema: StoreSchema) -> StoreStatus:
    path = schema.path(Path(state_root))
    if schema.kind == "evidence":
        return _inspect_evidence(path, schema)
    if not path.is_file():
        return StoreStatus(schema.name, str(path), False, None, schema.version, "absent", "no store file")
    try:
        db = connect_read(path)
    except sqlite3.Error as error:  # pragma: no cover - unreadable file is environmental
        return StoreStatus(schema.name, str(path), True, None, schema.version, "unreadable", str(error))
    try:
        version = read_user_version(db)
        missing = sorted(schema.required_tables - table_names(db))
    except sqlite3.DatabaseError as error:
        return StoreStatus(schema.name, str(path), True, None, schema.version, "unreadable", str(error))
    finally:
        db.close()
    if missing:
        return StoreStatus(
            schema.name, str(path), True, version, schema.version, "incomplete", f"missing tables {missing}"
        )
    if version > schema.version:
        return StoreStatus(
            schema.name,
            str(path),
            True,
            version,
            schema.version,
            "too_new",
            "written by a newer binary; roll the binary forward instead of downgrading the store",
        )
    if version < schema.version:
        return StoreStatus(
            schema.name, str(path), True, version, schema.version, "migratable", "will migrate forward on open"
        )
    return StoreStatus(schema.name, str(path), True, version, schema.version, "ok")


def _inspect_evidence(path: Path, schema: StoreSchema) -> StoreStatus:
    if not path.is_dir():
        return StoreStatus(schema.name, str(path), False, None, schema.version, "absent", "no evidence directory")
    marker = path / EVIDENCE_MARKER
    if not marker.is_file():
        return StoreStatus(schema.name, str(path), True, 0, schema.version, "migratable", "unstamped evidence tree")
    try:
        declared = int(json.loads(marker.read_text(encoding="utf-8"))["schema_version"])
    except (OSError, ValueError, KeyError, TypeError) as error:
        return StoreStatus(schema.name, str(path), True, None, schema.version, "unreadable", str(error))
    if declared > schema.version:
        return StoreStatus(schema.name, str(path), True, declared, schema.version, "too_new", "newer evidence schema")
    status = "ok" if declared == schema.version else "migratable"
    return StoreStatus(schema.name, str(path), True, declared, schema.version, status)


def inspect_state_root(state_root: Path, stores: Iterable[StoreSchema] = FACTORY_STORES) -> list[StoreStatus]:
    return [inspect_store(Path(state_root), store) for store in stores]


def assert_compatible(state_root: Path, *, require_present: bool = False) -> list[StoreStatus]:
    """Refuse a state root this binary must not write to, with one actionable diagnostic.

    ``require_present`` is for the moment after a restore: a store that is merely *absent* is a
    fresh factory, but a restored factory missing one of its five stores is a partial restore, and
    a partial restore that starts mutating is how a factory replays what it cannot see.
    """
    statuses = inspect_state_root(state_root)
    problems = [status for status in statuses if not status.usable]
    if require_present:
        problems += [status for status in statuses if status.status == "absent"]
    if problems:
        detail = "; ".join(f"{s.name} ({s.status}): {s.detail or s.path}" for s in problems)
        raise StoreSchemaError(f"factory state at {state_root} is not usable by this binary: {detail}")
    return statuses
