"""Coordinated backup, validated restore, and reconciliation before replay.

The failure this module exists for is the worst one a factory has. Restore a snapshot taken an hour
before a publication, and the operation journal believes the publication is still owed while the
outside world already has the pull request. Nothing in the restored state can tell the difference,
so the factory helpfully opens a second one.

Four rules follow, and they are the whole design:

1. **A backup covers every authoritative store or it is not taken.** Three of four stores captured
   mid-write is worse than no backup at all, because it looks restorable. Every store is fenced by
   a write transaction for the length of the copy, and because those locks are taken one at a time
   the window is *proved* rather than assumed: any store that moved while the rest were being
   fenced abandons the backup. Run directories go in for the same reason the databases do -- a
   restore replaces the whole state root, so what is not in the manifest is what a restore deletes.
   The manifest is written last: a directory without a complete manifest is a copy, not a backup.
2. **Restore validates before mutations resume.** Digests, completeness, evidence hash chains,
   store schema versions and the joins *between* the stores are checked before any byte is placed,
   and a store written by a newer binary is refused rather than downgraded. Each store being
   individually perfect is not the same fact as the set of them coming from one instant.
3. **Reconciliation precedes replay.** A restored factory does not know what happened after its
   snapshot, so until an operator has closed the restore window, *every* Cell must observe remote
   state before it re-drives anything. Every Cell, not only the restored ones: Cell identity is a
   hash of repo/target/issue, so a Cell first activated after the backup was taken is rebuilt under
   the same id with no journal row to stop it, and no list built from restored rows can name it.
   That observation is #2042's operation journal (``observe_before_first_attempt`` and the in-doubt
   states) and #2058's dispatch outbox, not a parallel notion of "probably fine".
4. **Ending the window is a decision, not an arithmetic result.** Reconciling everything the
   snapshot could name says nothing about the effects it could not, so the window closes only when
   an operator says the interval itself was reviewed -- and the fact that one is open is recorded
   in the stores as well as in a file, because deleting the file that refuses is the most natural
   wrong move available.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import sqlite3
import time
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from swfactory.deployment_profile import assert_supported_state_root
from swfactory.lifecycle_evidence import verify_chain
from swfactory.operation_recovery import plan_recovery
from swfactory.paths import validate_run_id
from swfactory.store_schema import (
    FACTORY_STORES,
    StoreSchemaError,
    assert_compatible,
    connect_read,
    inspect_state_root,
    supported_versions,
)

BACKUP_SCHEMA_VERSION = 1
MANIFEST_NAME = "manifest.json"
PAYLOAD_DIR = "state"
RESTORE_DIR = "restore"
MARKER_NAME = "pending.json"
GATE_PENDING = "pending"
# Written into the admission store as well as the marker file, so deleting the marker to unstick a
# factory does not silently re-enable blind replay. ``admission_meta`` holds integers, so the stamp
# is the restore's own timestamp: non-zero means a restore is open.
RESTORE_OPEN_KEY = "restore_open"
GATE_RECONCILING = "reconciling"
GATE_CLEAR = "clear"
# Long enough that a slow but healthy writer finishes, short enough that an operator learns the
# factory is busy instead of watching a backup hang overnight.
QUIESCE_TIMEOUT_S = 30.0


class BackupRefused(RuntimeError):
    """Refuse to write a backup that would look restorable and not be."""


class RestoreRefused(RuntimeError):
    """Refuse to place state this binary must not own."""


class MutationsWithheld(RuntimeError):
    """A restored factory has not been validated and reconciled; external effects stay withheld."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)


class ReconciliationIncomplete(RuntimeError):
    """A reconciliation claim is not backed by a recorded observation."""


# ---------------------------------------------------------------- backup


def _data_version(db: sqlite3.Connection) -> int:
    """SQLite's own counter of commits made by *other* connections to this database."""
    return int(db.execute("PRAGMA data_version").fetchone()[0])


@contextmanager
def quiesce(paths: Sequence[Path], *, timeout_s: float = QUIESCE_TIMEOUT_S) -> Iterator[None]:
    """Hold a write transaction on every store at once, so all of them stop at one instant.

    Taken in a fixed (sorted) order because two processes quiescing in different orders deadlock,
    and a deadlocked backup is indistinguishable to an operator from a slow one. If any store
    cannot be fenced the whole window is abandoned: a backup of four stores where the fifth kept
    committing is precisely the torn snapshot this contract refuses to produce.

    Locks are taken one after another, so "all of them stop at one instant" is a claim about the
    window *after* the last one is held -- a writer can still commit to a store that has not been
    reached yet, which is a torn snapshot that looks quiesced. ``PRAGMA data_version`` counts the
    commits other connections have made, so it is read on every store before the first lock and
    again once the last one is held: if any store moved while the window was closing, the window
    never existed and the backup is refused rather than written.
    """
    ordered = sorted(paths)
    with ExitStack() as stack:
        connections: list[tuple[Path, sqlite3.Connection]] = []
        for path in ordered:
            try:
                db = sqlite3.connect(path, timeout=max(0.1, timeout_s), isolation_level=None)
            except sqlite3.Error as error:
                raise BackupRefused(f"cannot open {path} to quiesce it: {error}") from error
            stack.callback(db.close)
            connections.append((path, db))
        before = {path: _data_version(db) for path, db in connections}
        for path, db in connections:
            try:
                db.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as error:
                raise BackupRefused(
                    f"cannot quiesce {path} within {timeout_s:g}s ({error}); refusing to write a "
                    "backup that catches some stores mid-write. Retry when the factory is idle."
                ) from error
            stack.callback(_rollback, db)
        moved = [str(path) for path, db in connections if _data_version(db) != before[path]]
        if moved:
            raise BackupRefused(
                f"{', '.join(moved)} committed while the other stores were being fenced, so this "
                "would not be one instant: the Cell store would be older than the journal beside "
                "it. Retry when the factory is idle."
            )
        yield


def _rollback(db: sqlite3.Connection) -> None:
    with suppress(sqlite3.Error):  # a connection already closed by an earlier failure holds nothing
        db.execute("ROLLBACK")


def create_backup(
    state_root: Path,
    dest: Path,
    *,
    actor: str,
    timeout_s: float = QUIESCE_TIMEOUT_S,
) -> dict[str, Any]:
    """Write a coordinated, self-describing backup of every authoritative store plus evidence."""
    state_root = Path(state_root).resolve()
    dest = Path(dest)
    if dest.exists() and any(dest.iterdir()):
        raise BackupRefused(f"{dest} is not empty; write each backup into its own directory")
    assert_supported_state_root(state_root)
    statuses = {status.name: status for status in inspect_state_root(state_root)}
    unusable = [s for s in statuses.values() if not s.usable]
    if unusable:
        detail = "; ".join(f"{s.name}: {s.status} {s.detail}" for s in unusable)
        raise BackupRefused(f"refusing to back up unusable state: {detail}")

    payload = dest / PAYLOAD_DIR
    payload.mkdir(parents=True)
    sqlite_stores = [s for s in FACTORY_STORES if s.kind == "sqlite" and s.path(state_root).is_file()]
    stores: list[dict[str, Any]] = []
    evidence: dict[str, Any] = {"present": False, "cells": []}
    runs: list[dict[str, Any]] = []
    with quiesce([s.path(state_root) for s in sqlite_stores], timeout_s=timeout_s):
        runs = _copy_runs(state_root, payload)
        for schema in FACTORY_STORES:
            source = schema.path(state_root)
            if schema.kind == "evidence":
                if source.is_dir():
                    evidence = _copy_evidence(source, payload / schema.relative_path)
                stores.append(
                    {
                        "name": schema.name,
                        "relative_path": schema.relative_path,
                        "present": evidence["present"],
                        "schema_version": statuses[schema.name].version,
                        "kind": "evidence",
                    }
                )
                continue
            present = source.is_file()
            if present:
                _backup_sqlite(source, payload / schema.relative_path)
            stores.append(
                {
                    "name": schema.name,
                    "relative_path": schema.relative_path,
                    "present": present,
                    "schema_version": statuses[schema.name].version,
                    "kind": "sqlite",
                    # Recorded so an operator can see the write-ahead log was folded into the copy
                    # by SQLite's own backup API rather than skipped, which is the mistake that
                    # makes `cp *.sqlite3` lose committed rows.
                    "source_wal_bytes": _size(source.with_name(source.name + "-wal")),
                }
            )

    # The joins are checked on the copy rather than the source: a backup is judged by what it will
    # restore, and a copy that lost a receipt is refused before it is given a manifest to look
    # trustworthy with.
    incoherent = coherence_problems(payload)
    if incoherent:
        raise BackupRefused(
            "refusing to write a backup whose stores do not agree with each other: "
            + "; ".join(incoherent)
            + ". Copy the state root aside for forensics instead; this would restore as a factory "
            "that re-drives effects it has already made."
        )

    files = _hash_tree(dest, skip={MANIFEST_NAME})
    body = {
        "schema_version": BACKUP_SCHEMA_VERSION,
        "created_at": time.time(),
        "actor": actor,
        "host": socket.gethostname(),
        "state_root": str(state_root),
        "quiesced": True,
        "binary_store_versions": supported_versions(),
        "stores": stores,
        "evidence": evidence,
        # Run directories are the sixth authoritative thing on this filesystem: the accepted-inputs
        # pin (#2065), recorded approvals (#2066), stage verdicts and cost accounting all live
        # there, and a restore replaces the whole state root. A backup that skipped them would
        # verify clean and still delete the record of what a human authorised.
        "runs": runs,
        "files": files,
    }
    manifest = {**body, "manifest_digest": _digest(body), "complete": True}
    # Written last and atomically: a run that dies mid-copy leaves a directory with no manifest,
    # which `verify_backup` refuses, instead of a plausible-looking partial backup.
    _atomic_json(dest / MANIFEST_NAME, manifest)
    return manifest


def _backup_sqlite(source: Path, target: Path) -> None:
    """Copy one store through SQLite's online backup API, WAL contents included."""
    target.parent.mkdir(parents=True, exist_ok=True)
    src = connect_read(source)
    dst = sqlite3.connect(target)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def _copy_evidence(source: Path, target: Path) -> dict[str, Any]:
    """Copy the evidence tree and record each Cell's hash-chain tail as it was captured."""
    shutil.copytree(source, target, dirs_exist_ok=True)
    cells = []
    for cell_dir in sorted(p for p in target.iterdir() if p.is_dir()):
        records = _read_events(cell_dir / "events.jsonl")
        intact, tail = verify_chain(records)
        cells.append(
            {
                "cell_id": cell_dir.name,
                "events": len(records),
                "chain_intact": intact,
                "tail_digest": tail if intact else "",
                "detail": "" if intact else tail,
            }
        )
    return {"present": True, "cells": cells}


def run_directories(state_root: Path) -> list[Path]:
    """Every saved run directory below the state root, using ``RunState``'s own layout.

    A run is ``<state_root>/<run_id>/state``; the store directories and the restore marker are not
    runs. Discovery is by shape rather than by a list of names so a new run cannot be missed.
    """
    state_root = Path(state_root)
    if not state_root.is_dir():
        return []
    reserved = {"control", "evidence", RESTORE_DIR}
    found = []
    for path in sorted(state_root.iterdir()):
        if not path.is_dir() or path.name in reserved or path.name.startswith("."):
            continue
        try:
            validate_run_id(path.name)
        except ValueError:
            continue
        if (path / "state").is_dir():
            found.append(path)
    return found


def _copy_runs(state_root: Path, payload: Path) -> list[dict[str, Any]]:
    """Copy the run directories into the payload, recorded so a restore puts them back."""
    captured = []
    for directory in run_directories(state_root):
        target = payload / directory.name
        shutil.copytree(directory, target, dirs_exist_ok=True)
        captured.append(
            {
                "run_id": directory.name,
                "files": sum(1 for path in target.rglob("*") if path.is_file()),
            }
        )
    return captured


def _read_events(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                rows.append(json.loads(line))
            except ValueError:
                # A torn tail is a finding, not an exception: it must reach the manifest so the
                # verifier refuses the backup rather than a stack trace hiding the reason.
                rows.append({"digest": "unparseable", "previous_digest": None})
    return rows


# ---------------------------------------------------------------- verification


@dataclass(frozen=True)
class BackupVerification:
    ok: bool
    problems: list[str] = field(default_factory=list)
    manifest: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "problems": list(self.problems), "manifest": self.manifest}


def verify_backup(backup_dir: Path) -> BackupVerification:
    """Prove a directory is a complete, unmodified backup before anything depends on it."""
    backup_dir = Path(backup_dir)
    manifest_path = backup_dir / MANIFEST_NAME
    if not manifest_path.is_file():
        return BackupVerification(
            False,
            [f"no {MANIFEST_NAME}: {backup_dir} is a copy of some files, not a backup of a factory"],
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except ValueError as error:
        return BackupVerification(False, [f"{MANIFEST_NAME} is not readable JSON: {error}"])

    problems: list[str] = []
    if manifest.get("schema_version") != BACKUP_SCHEMA_VERSION:
        problems.append(f"backup schema {manifest.get('schema_version')!r} is not {BACKUP_SCHEMA_VERSION}")
        return BackupVerification(False, problems, manifest)
    if manifest.get("complete") is not True:
        problems.append("manifest is not marked complete; the backup run did not finish")
    body = {key: value for key, value in manifest.items() if key not in {"manifest_digest", "complete"}}
    if manifest.get("manifest_digest") != _digest(body):
        problems.append("manifest digest does not match its own contents; the manifest was edited")

    declared = {entry["path"]: entry for entry in manifest.get("files", [])}
    for relative, entry in sorted(declared.items()):
        path = backup_dir / relative
        if not path.is_file():
            problems.append(f"declared file is missing: {relative}")
            continue
        data = path.read_bytes()
        if len(data) != entry.get("bytes") or hashlib.sha256(data).hexdigest() != entry.get("sha256"):
            problems.append(f"file does not match its recorded digest: {relative}")
    for relative in sorted(_relative_files(backup_dir, skip={MANIFEST_NAME})):
        if relative not in declared:
            problems.append(f"undeclared file in backup: {relative}")

    for store in manifest.get("stores", []):
        if not store.get("present"):
            problems.append(
                f"store {store.get('name')!r} was absent when the backup was taken; a factory "
                "restored from it would start with a store it never had"
            )
    problems += [f"stores disagree: {problem}" for problem in coherence_problems(backup_dir / PAYLOAD_DIR)]
    for cell in manifest.get("evidence", {}).get("cells", []):
        if not cell.get("chain_intact"):
            problems.append(f"evidence chain for {cell.get('cell_id')} is broken: {cell.get('detail')}")
        else:
            records = _read_events(backup_dir / PAYLOAD_DIR / "evidence" / str(cell["cell_id"]) / "events.jsonl")
            intact, tail = verify_chain(records)
            if not intact or tail != cell.get("tail_digest"):
                problems.append(f"evidence for {cell.get('cell_id')} no longer matches its recorded tail")
    return BackupVerification(not problems, problems, manifest)


# ---------------------------------------------------------------- cross-store coherence


def coherence_problems(state_root: Path) -> list[str]:
    """Joins between the stores that a per-file digest cannot see.

    Each store can be internally valid and byte-perfect while the set of them comes from two
    different instants -- a Cell store from after a takeover beside a journal from before it. That
    is the restore that looks like a success: every file verifies, and the receipts that prove what
    the factory already did to the outside world are simply not there. Two joins are checked, in the
    two directions state can be torn:

    * an operation bound to a Cell the Cell store does not have, or to an epoch ahead of it -- the
      journal is newer than the Cells;
    * a Cell whose own history records an external mutation the journal has no receipt for -- the
      Cells are newer than the journal, which is how a committed publication becomes invisible.
    """
    state_root = Path(state_root)
    cells_path = state_root / "cells.sqlite3"
    journal_path = state_root / "control" / "operations.sqlite3"
    if not cells_path.is_file() or not journal_path.is_file():
        return []
    problems: list[str] = []
    try:
        with _inspect_only(cells_path) as db:
            epochs = {str(row[0]): int(row[1]) for row in db.execute("SELECT cell_id, epoch FROM cells")}
            effects = [
                (str(row[0]), str(row[1]), int(row[2]))
                for row in db.execute(
                    "SELECT cell_id, operation_key, epoch FROM cell_events WHERE kind='external_mutation'"
                )
            ]
        with _inspect_only(journal_path) as db:
            committed = {
                str(row[0]): (str(row[1]), int(row[2]), str(row[3]))
                for row in db.execute("SELECT operation_key, cell_id, epoch, state FROM operations")
            }
    except sqlite3.DatabaseError as error:  # pragma: no cover - unreadable store is its own refusal
        return [f"cannot read the stores to check their joins: {error}"]

    for key, (cell_id, epoch, _state) in sorted(committed.items()):
        if cell_id not in epochs:
            problems.append(
                f"operation {key} belongs to Cell {cell_id}, which the Cell store does not hold; "
                "these two stores are not from the same instant"
            )
        elif epoch > epochs[cell_id]:
            problems.append(
                f"operation {key} is at epoch {epoch} but Cell {cell_id} is at epoch {epochs[cell_id]}; "
                "the operation journal is newer than the Cell store"
            )
    for cell_id, key, epoch in sorted(effects):
        if key not in committed:
            problems.append(
                f"Cell {cell_id} records external effect {key} at epoch {epoch}, and the operation "
                "journal holds no receipt for it; the Cell store is newer than the journal, so the "
                "factory would re-drive an effect it has already made"
            )
    return problems


# ---------------------------------------------------------------- restore


def restore(
    backup_dir: Path,
    state_root: Path,
    *,
    actor: str,
    reason: str,
    replace_existing: bool = False,
) -> dict[str, Any]:
    """Validate a backup, refuse an old-binary rollback, place it, and withhold mutations."""
    backup_dir = Path(backup_dir)
    state_root = Path(state_root)
    verification = verify_backup(backup_dir)
    if not verification.ok:
        raise RestoreRefused(
            "backup is not restorable: " + "; ".join(verification.problems),
        )
    manifest = verification.manifest
    supported = supported_versions()
    for store in manifest.get("stores", []):
        name = str(store.get("name"))
        version = store.get("schema_version")
        limit = supported.get(name)
        if limit is None:
            raise RestoreRefused(
                f"backup carries store {name!r}, which this binary does not know; restore it with "
                "the binary that wrote it."
            )
        if isinstance(version, int) and version > limit:
            # The old-binary rollback refusal. Placing this state and running would let a reader
            # that cannot interpret the newer rows overwrite them with its own interpretation.
            raise RestoreRefused(
                f"{name} store in this backup is at schema {version}; this binary supports {limit}. "
                "Roll the binary forward and restore with it; an older binary must not own newer "
                "factory state."
            )

    payload = backup_dir / PAYLOAD_DIR
    # Read the fences the live state root currently holds, before it is moved aside. A restore
    # rolls every Cell's epoch back to the snapshot's, so a process that the live factory had
    # already fenced out at a higher epoch has its writes accepted again. Nothing local can stop
    # that -- the restored rows are the authority now -- so it is at least named.
    superseded_epochs = _current_epochs(state_root)
    if state_root.exists() and any(state_root.iterdir()):
        if not replace_existing:
            raise RestoreRefused(
                f"{state_root} already holds factory state; pass replace_existing to set it aside "
                "first. Restoring over live state loses the very evidence needed to reconcile."
            )
        superseded = state_root.with_name(f"{state_root.name}.superseded-{int(time.time())}")
        # Moved, never deleted: after a bad restore the superseded tree is the only record of what
        # the factory actually did while the backup was ageing.
        state_root.rename(superseded)
    state_root.mkdir(parents=True, exist_ok=True)
    shutil.copytree(payload, state_root, dirs_exist_ok=True)

    marker = _build_marker(state_root, manifest, actor=actor, reason=reason, superseded_epochs=superseded_epochs)
    _write_marker(state_root, marker)
    # Second copy of the same fact, inside a store the factory cannot open without noticing. The
    # marker is a file in an operator-writable directory, and "the factory is stuck, delete the
    # restore directory" is the most natural wrong move available after a restore.
    _set_restore_stamp(state_root, int(marker["restored_at"]))
    return marker


def _restore_stamp(state_root: Path) -> int:
    """The restore stamp the admission store carries, or 0 when no restore is open."""
    path = Path(state_root) / "control" / "admission.sqlite3"
    if not path.is_file():
        return 0
    try:
        with _readonly(path) as db:
            row = db.execute("SELECT value FROM admission_meta WHERE key=?", (RESTORE_OPEN_KEY,)).fetchone()
    except sqlite3.DatabaseError:  # pragma: no cover - an unreadable store refuses elsewhere
        return 0
    return int(row[0]) if row and row[0] is not None else 0


def _set_restore_stamp(state_root: Path, value: int) -> None:
    path = Path(state_root) / "control" / "admission.sqlite3"
    if not path.is_file():
        return
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    try:
        db.execute(
            "INSERT INTO admission_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (RESTORE_OPEN_KEY, int(value)),
        )
    finally:
        db.close()


def _fence_rollbacks(state_root: Path, superseded_epochs: dict[str, int]) -> list[dict[str, Any]]:
    """Cells whose mutation fence (#2041) moved backwards because of this restore."""
    restored = _current_epochs(state_root)
    return [
        {"cell_id": cell_id, "was": was, "now": restored[cell_id]}
        for cell_id, was in sorted(superseded_epochs.items())
        if cell_id in restored and was > restored[cell_id]
    ]


def _build_marker(
    state_root: Path,
    manifest: dict[str, Any],
    *,
    actor: str,
    reason: str,
    superseded_epochs: dict[str, int] | None = None,
) -> dict[str, Any]:
    cells = _restored_live_cells(state_root)
    dispatch = _inflight_dispatch(state_root)
    rollbacks = _fence_rollbacks(state_root, superseded_epochs or {})
    return {
        "schema_version": 1,
        "state": GATE_PENDING,
        "restored_at": time.time(),
        "actor": actor,
        "reason": reason,
        "manifest_digest": manifest.get("manifest_digest"),
        "backup_created_at": manifest.get("created_at"),
        # Every Cell the snapshot left non-terminal. Each of them may hold an external effect that
        # completed after the snapshot was taken, so none of them may re-drive one unobserved.
        "restored_cells": cells,
        "unreconciled_cells": list(cells),
        "unreconciled_dispatch": dispatch,
        # Fences this restore moved backwards. Any process still holding the higher epoch can write
        # to these Cells again, so an operator has to know which ones before work resumes.
        "fence_rollbacks": rollbacks,
        "history": [{"event": "restored", "at": time.time(), "actor": actor, "reason": reason}],
    }


def _restored_live_cells(state_root: Path) -> list[str]:
    """The Cells the snapshot itself says are owed an observation.

    Two sources, because either alone lies. A non-terminal Cell may hold an effect that completed
    after the snapshot; and a Cell the snapshot recorded as terminal can still own an operation the
    journal never resolved, which a terminal state does not settle. This is the operator's worklist,
    not the set of Cells that must observe -- that is every Cell, for as long as the window is open.
    """
    owed: set[str] = set()
    path = Path(state_root) / "cells.sqlite3"
    if path.is_file():
        terminal = ("success", "failed", "cancelled", "rejected", "cleaned")
        placeholders = ",".join("?" for _ in terminal)
        with _readonly(path) as db:
            rows = db.execute(
                f"SELECT cell_id FROM cells WHERE state NOT IN ({placeholders})",  # noqa: S608
                terminal,
            ).fetchall()
        owed.update(str(row[0]) for row in rows)
    journal = Path(state_root) / "control" / "operations.sqlite3"
    if journal.is_file():
        with _readonly(journal) as db:
            rows = db.execute("SELECT DISTINCT cell_id FROM operations WHERE state!='committed'").fetchall()
        owed.update(str(row[0]) for row in rows)
    return sorted(owed)


def _inflight_dispatch(state_root: Path) -> list[str]:
    """#2058's outbox rows whose delivery may already have reached Airflow."""
    path = Path(state_root) / "control" / "admission.sqlite3"
    if not path.is_file():
        return []
    with _readonly(path) as db:
        rows = db.execute("SELECT work_id FROM admission_dispatch WHERE state='inflight' ORDER BY work_id").fetchall()
    return [str(row[0]) for row in rows]


@contextmanager
def _readonly(path: Path) -> Iterator[sqlite3.Connection]:
    db = connect_read(path)
    try:
        yield db
    finally:
        db.close()


@contextmanager
def _inspect_only(path: Path) -> Iterator[sqlite3.Connection]:
    """Read a store without touching the directory that holds it.

    ``connect_read`` falls back to a writable connection when a WAL database has no shared-memory
    index, which creates ``-wal``/``-shm`` beside the file. Doing that inside a backup would add
    undeclared files to it -- a verifier that damages what it verifies. ``immutable=1`` promises
    SQLite the bytes will not change under it, which is exactly true of a backup payload.
    """
    try:
        db = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    except sqlite3.Error:  # pragma: no cover - environmental
        db = connect_read(path)
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------- the gate


def marker_path(state_root: Path) -> Path:
    return Path(state_root) / RESTORE_DIR / MARKER_NAME


def _write_marker(state_root: Path, marker: dict[str, Any]) -> None:
    _atomic_json(marker_path(state_root), marker)


class RestoreGate:
    """Whether this factory may mutate the outside world yet, and what it must observe first.

    The marker is re-read from disk whenever it changes, because the operator clearing a restore
    runs in a different process from the backend that must stop refusing.
    """

    def __init__(self, state_root: Path):
        self.state_root = Path(state_root)
        self._stamp: tuple[int, int] | None = None
        self._marker: dict[str, Any] | None = None
        self._orphaned: bool | None = None

    @property
    def path(self) -> Path:
        return marker_path(self.state_root)

    def marker(self) -> dict[str, Any] | None:
        try:
            stat = self.path.stat()
        except OSError:
            self._stamp, self._marker = None, None
            return None
        stamp = (stat.st_mtime_ns, stat.st_size)
        if stamp != self._stamp:
            try:
                self._marker = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as error:
                raise MutationsWithheld(
                    f"restore marker {self.path} is unreadable ({error}); a factory that cannot read "
                    "its own restore state must not mutate."
                ) from error
            self._stamp = stamp
        return self._marker

    def orphaned(self) -> bool:
        """The stores record an open restore and the marker that governs it is gone.

        A restore can only be opened while the factory is stopped, so a negative answer is cached
        for the life of this gate: the cost of asking is one store open, and this is asked on the
        path every external effect takes.
        """
        if self.marker() is not None:
            return False
        if self._orphaned is None:
            self._orphaned = _restore_stamp(self.state_root) != 0
        return self._orphaned

    @property
    def state(self) -> str:
        marker = self.marker()
        if marker is not None:
            return str(marker.get("state", GATE_PENDING))
        return GATE_PENDING if self.orphaned() else GATE_CLEAR

    def assert_mutations_allowed(self) -> None:
        if self.marker() is None and self.orphaned():
            raise MutationsWithheld(
                f"the restore marker {self.path} is missing, and this factory's stores still record "
                "an open restore. Deleting the marker does not end a restore, it only hides which "
                "effects were never reconciled. Restore the marker from "
                f"{self.state_root / RESTORE_DIR} or restore the factory again."
            )
        if self.state == GATE_PENDING:
            marker = self.marker() or {}
            raise MutationsWithheld(
                "this factory was restored from a backup and has not been validated for mutation "
                f"(restored at {marker.get('restored_at')} by {marker.get('actor')!r}). Run "
                "`swfactory backup status` and then `swfactory backup resume` once the restore is "
                "checked; external effects stay withheld until then."
            )

    def requires_observation(self, cell_id: str) -> bool:
        """True for *every* Cell while a restore window is open.

        Deliberately not a lookup in ``unreconciled_cells``. That list holds the Cells the snapshot
        knew about, and the effects a restore duplicates are precisely the ones the snapshot never
        recorded: a Cell first activated after the backup was taken has a deterministic identity, so
        a restored factory rebuilds it under the same id, finds no journal row for its publication,
        and opens a second pull request. An allowlist derived from restored rows cannot name a Cell
        those rows never had, so while the window is open every first attempt observes the remote,
        and the window is closed by an operator rather than by arithmetic over what came back.
        """
        marker = self.marker()
        if marker is None:
            return self.orphaned()
        return str(marker.get("state", GATE_PENDING)) != GATE_CLEAR

    def window(self) -> dict[str, Any]:
        """The interval a restore cannot see: snapshot taken, then restored. Effects landed here."""
        marker = self.marker() or {}
        created = marker.get("backup_created_at")
        restored = marker.get("restored_at")
        span = None
        if isinstance(created, int | float) and isinstance(restored, int | float):
            span = max(0.0, float(restored) - float(created))
        return {"backup_created_at": created, "restored_at": restored, "unobserved_seconds": span}

    def outstanding(self) -> dict[str, Any]:
        marker = self.marker()
        if marker is None:
            if self.orphaned():
                return {
                    "state": GATE_PENDING,
                    "cells": [],
                    "dispatch": [],
                    "observation_required": True,
                    "window": {"backup_created_at": None, "restored_at": None, "unobserved_seconds": None},
                    "fence_rollbacks": [],
                    "detail": "the restore marker is missing while the stores record an open restore",
                }
            return {
                "state": GATE_CLEAR,
                "cells": [],
                "dispatch": [],
                "observation_required": False,
                "window": {"backup_created_at": None, "restored_at": None, "unobserved_seconds": None},
                "fence_rollbacks": [],
            }
        return {
            "state": marker.get("state", GATE_PENDING),
            "cells": list(marker.get("unreconciled_cells", [])),
            "dispatch": list(marker.get("unreconciled_dispatch", [])),
            # True while *any* Cell must observe the remote before its first attempt, which is the
            # whole time the window is open -- not only for the Cells the snapshot could name.
            "observation_required": str(marker.get("state", GATE_PENDING)) != GATE_CLEAR,
            "window": self.window(),
            "fence_rollbacks": list(marker.get("fence_rollbacks", [])),
        }

    def resume(self, *, actor: str, reason: str) -> dict[str, Any]:
        """Validate the restored state root, then allow mutations that observe before they act."""
        marker = self.marker()
        if marker is None:
            raise MutationsWithheld("there is no pending restore to resume")
        if marker.get("state") != GATE_PENDING:
            return marker
        try:
            assert_compatible(self.state_root, require_present=True)
        except StoreSchemaError as error:
            raise RestoreRefused(f"restored state is not ready for mutation: {error}") from error
        incoherent = coherence_problems(self.state_root)
        if incoherent:
            raise RestoreRefused("restored stores do not agree with each other: " + "; ".join(incoherent))
        owed = _cells_owing_evidence(self.state_root)
        for cell_id in marker.get("restored_cells", []):
            events = self.state_root / "evidence" / str(cell_id) / "events.jsonl"
            if not events.is_file():
                # A Cell that never reached outside itself has no history to lose; one that has
                # operations in the journal does, and without it cannot prove what it already did.
                if cell_id in owed:
                    raise RestoreRefused(
                        f"evidence for restored Cell {cell_id} is missing, but its operation journal "
                        "records external effects; a Cell whose history cannot be read cannot prove "
                        "what it already did to the outside world."
                    )
                continue
            intact, detail = verify_chain(_read_events(events))
            if not intact:
                raise RestoreRefused(f"evidence for restored Cell {cell_id} is damaged: {detail}")
        updated = dict(marker)
        updated["state"] = GATE_RECONCILING
        updated["history"] = [
            *marker.get("history", []),
            {"event": "resumed", "at": time.time(), "actor": actor, "reason": reason},
        ]
        self._store(updated)
        return updated

    def mark_reconciled(self, *, cell_id: str | None = None, work_id: str | None = None) -> dict[str, Any]:
        """Drop one Cell or dispatch intent from the gate, and only on recorded evidence."""
        marker = self.marker()
        if marker is None:
            raise ReconciliationIncomplete("there is no pending restore to reconcile")
        if marker.get("state") == GATE_PENDING:
            raise ReconciliationIncomplete("validate the restore with `backup resume` before reconciling")
        updated = dict(marker)
        if cell_id is not None:
            self._assert_cell_observed(cell_id)
            updated["unreconciled_cells"] = [c for c in marker.get("unreconciled_cells", []) if c != cell_id]
        if work_id is not None:
            self._assert_dispatch_settled(work_id)
            updated["unreconciled_dispatch"] = [w for w in marker.get("unreconciled_dispatch", []) if w != work_id]
        updated["history"] = [
            *marker.get("history", []),
            {"event": "reconciled", "at": time.time(), "cell_id": cell_id, "work_id": work_id},
        ]
        # Emptying the worklist does not close the window. The worklist is what the snapshot could
        # name; the window also contains every Cell and operation created after it, which no local
        # record mentions. Ending it is an operator's statement, made below.
        self._store(updated)
        return updated

    def close(self, *, actor: str, reason: str, window_reviewed: bool = False) -> dict[str, Any]:
        """End the restore window, on an operator's explicit statement about the lost interval.

        Every known obligation must already be reconciled, and the operator must say they have
        reviewed the interval itself -- because the duplicate this contract exists to prevent lives
        in the effects the snapshot never recorded, and nothing on this host can enumerate those.
        """
        marker = self.marker()
        if marker is None:
            raise ReconciliationIncomplete("there is no restore window to close")
        if marker.get("state") == GATE_PENDING:
            raise ReconciliationIncomplete("validate the restore with `backup resume` before closing the window")
        outstanding = [*marker.get("unreconciled_cells", []), *marker.get("unreconciled_dispatch", [])]
        if outstanding:
            raise ReconciliationIncomplete(
                f"{len(outstanding)} restored items are still unreconciled ({sorted(outstanding)}); "
                "reconcile each one before closing the window."
            )
        if not window_reviewed:
            span = self.window()["unobserved_seconds"]
            length = f"{span:.0f}s" if isinstance(span, float) else "an unknown interval"
            raise ReconciliationIncomplete(
                f"the restore window covers {length} between the backup and the restore. Every Cell "
                "and every external effect created inside it is absent from this state, so the "
                "factory cannot list them and will re-drive them unobserved once the window is "
                "closed. Confirm the interval was reviewed against the remote before closing."
            )
        updated = dict(marker)
        updated["state"] = GATE_CLEAR
        updated["history"] = [
            *marker.get("history", []),
            {"event": "closed", "at": time.time(), "actor": actor, "reason": reason},
        ]
        self._archive(updated)
        return updated

    def _assert_cell_observed(self, cell_id: str) -> None:
        """Refuse a reconciliation claim the operation journal does not back up.

        The journal (#2042) already records what an observation found. Trusting the claim instead of
        the record is how "we checked" becomes the reason a second pull request exists.
        """
        path = self.state_root / "control" / "operations.sqlite3"
        if not path.is_file():
            return
        with _readonly(path) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT * FROM operations WHERE cell_id=? AND state!='committed'", (cell_id,)).fetchall()
        unobserved = [dict(row) for row in rows if not row["observation_json"]]
        if unobserved:
            keys = sorted(str(row["operation_key"]) for row in unobserved)
            raise ReconciliationIncomplete(
                f"Cell {cell_id} still has unresolved operations with no recorded observation: {keys}. "
                "Observe the remote and record the outcome through the operation journal first."
            )

    def _assert_dispatch_settled(self, work_id: str) -> None:
        path = self.state_root / "control" / "admission.sqlite3"
        if not path.is_file():
            return
        with _readonly(path) as db:
            row = db.execute(
                "SELECT state, observation_json FROM admission_dispatch WHERE work_id=?", (work_id,)
            ).fetchone()
        if row is not None and str(row[0]) == "inflight" and not row[1]:
            raise ReconciliationIncomplete(
                f"dispatch intent {work_id} is still in flight with no recorded observation; check "
                "whether Airflow already has its run before letting the factory redeliver it."
            )

    def _store(self, marker: dict[str, Any]) -> None:
        _write_marker(self.state_root, marker)
        self._stamp = None

    def _archive(self, marker: dict[str, Any]) -> None:
        digest = str(marker.get("manifest_digest") or "unknown")[-16:]
        archive = self.state_root / RESTORE_DIR / f"completed-{int(marker['restored_at'])}-{digest}.json"
        _atomic_json(archive, marker)
        # The stores stop recording an open restore before the marker goes, so a crash between the
        # two leaves the gate closed rather than orphaned.
        _set_restore_stamp(self.state_root, 0)
        self.path.unlink(missing_ok=True)
        self._stamp, self._marker, self._orphaned = None, None, False


def _current_epochs(state_root: Path) -> dict[str, int]:
    path = Path(state_root) / "cells.sqlite3"
    if not path.is_file():
        return {}
    with _readonly(path) as db:
        return {str(row[0]): int(row[1]) for row in db.execute("SELECT cell_id, epoch FROM cells")}


def _cells_owing_evidence(state_root: Path) -> set[str]:
    """Cells the operation journal says reached outside themselves, and therefore owe evidence."""
    path = Path(state_root) / "control" / "operations.sqlite3"
    if not path.is_file():
        return set()
    with _readonly(path) as db:
        return {str(row[0]) for row in db.execute("SELECT DISTINCT cell_id FROM operations")}


def plan_reconciliation(state_root: Path) -> dict[str, Any]:
    """What a restored factory must observe before it re-drives anything, and why."""
    state_root = Path(state_root)
    gate = RestoreGate(state_root)
    outstanding = gate.outstanding()
    operations: list[dict[str, Any]] = []
    journal = state_root / "control" / "operations.sqlite3"
    if journal.is_file():
        with _readonly(journal) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT * FROM operations WHERE state!='committed' ORDER BY updated_at").fetchall()
        epochs = _current_epochs(state_root)
        for raw in rows:
            row = dict(raw)
            row["observation"] = json.loads(row["observation_json"]) if row.get("observation_json") else None
            # The Cell store decides which epoch is current, not the operation row. Passing the row's
            # own epoch back to the planner would make every stale operation look repairable, which
            # is the opposite of what an operator needs to see after a restore.
            decision = plan_recovery(row, current_epoch=epochs.get(str(row["cell_id"]), int(row["epoch"])))
            action, why = decision.action.value, decision.reason
            if action == "retry" and outstanding["state"] != GATE_CLEAR:
                # The recovery planner reasons about a factory that watched its own attempts. This
                # one did not: the snapshot may have been taken mid-attempt, so a row that looks
                # merely pending can already have reached the provider. Observe, never retry.
                action, why = "observe", "restored_state_cannot_prove_this_attempt_never_landed"
            operations.append(
                {
                    "operation_key": row["operation_key"],
                    "cell_id": row["cell_id"],
                    "kind": row["kind"],
                    "state": row["state"],
                    "action": action,
                    "reason": why,
                }
            )
    return {
        "schema_version": 1,
        "gate": outstanding,
        "operations": operations,
        "dispatch": outstanding["dispatch"],
    }


def status(state_root: Path) -> dict[str, Any]:
    """One operator-readable answer to 'may this factory mutate, and what is it waiting on?'."""
    state_root = Path(state_root)
    statuses = [item.to_dict() for item in inspect_state_root(state_root)]
    plan = plan_reconciliation(state_root)
    compatible = all(item["status"] in {"ok", "absent", "migratable"} for item in statuses)
    # Checked here as well as at restore time, because this is the command an operator runs before
    # starting a factory on state someone assembled by hand.
    incoherent = coherence_problems(state_root)
    return {
        "schema_version": 1,
        "state_root": str(state_root),
        "stores": statuses,
        "binary_store_versions": supported_versions(),
        "schema_compatible": compatible,
        "stores_agree": not incoherent,
        "coherence_problems": incoherent,
        "restore": plan["gate"],
        "mutations_allowed": compatible and not incoherent and plan["gate"]["state"] != GATE_PENDING,
        # Mutations may be allowed and still be unsafe to make blindly: while the window is open
        # every one of them must observe the remote first, so an operator reading this needs both.
        "observation_required": bool(plan["gate"].get("observation_required")),
        "restore_window": plan["gate"].get("window", {}),
        "reconciliation": plan["operations"],
    }


# ---------------------------------------------------------------- small shared helpers


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _relative_files(root: Path, *, skip: set[str]) -> list[str]:
    return [
        str(path.relative_to(root))
        for path in sorted(root.rglob("*"))
        if path.is_file() and str(path.relative_to(root)) not in skip
    ]


def _hash_tree(root: Path, *, skip: set[str]) -> list[dict[str, Any]]:
    entries = []
    for relative in _relative_files(root, skip=skip):
        data = (root / relative).read_bytes()
        entries.append({"path": relative, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    return entries


def _digest(body: dict[str, Any]) -> str:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


__all__ = [
    "BACKUP_SCHEMA_VERSION",
    "BackupRefused",
    "BackupVerification",
    "MutationsWithheld",
    "ReconciliationIncomplete",
    "RestoreGate",
    "RestoreRefused",
    "coherence_problems",
    "create_backup",
    "marker_path",
    "plan_reconciliation",
    "restore",
    "status",
    "verify_backup",
]
