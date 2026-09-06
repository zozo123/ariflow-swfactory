"""Host-owned run state.

The sandbox is where untrusted work happens, so it cannot also be the source of truth for gates,
review verdicts, cost accounting, or the baseline used to create a delivery patch. ``RunState``
keeps those values below the orchestrator's run directory and mirrors publishable artifacts into
the sandbox only when needed.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import tempfile
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO

from swfactory.paths import confined_path, normalize_relative_path

if TYPE_CHECKING:
    from swfactory.sandbox import Sandbox

try:
    import fcntl
except ImportError:  # pragma: no cover - swfactory targets Linux/macOS, kept import-safe elsewhere
    fcntl = None  # type: ignore[assignment]

OPERATIONS_LOG = "operations.jsonl"
RUN_LOCK = "run.lock"


class RunBusyError(RuntimeError):
    """Another process already owns this run's mutation boundary."""


class JournalCorruption(ValueError):
    """A committed record is corrupt; repairing a trailing append cannot resolve it."""


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def _sync_directory(path: Path) -> None:
    """A durable file rename also needs its directory entry to survive a host crash."""
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _ensure_directory(path: Path) -> None:
    """Persist each newly created directory before writing evidence beneath it."""
    if path.is_dir():
        return
    if path.exists():
        raise NotADirectoryError(str(path))
    _ensure_directory(path.parent)
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        if not path.is_dir():
            raise
    _sync_directory(path.parent)


def _open_append(path: Path) -> BinaryIO:
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_APPEND, 0o600)
    return os.fdopen(fd, "a+b")


def _decode_journal(raw: bytes, path: Path) -> tuple[list[Any], int]:
    """Return complete records and the start of a torn tail, or the byte length if intact.

    Decode records separately so a multibyte UTF-8 character interrupted mid-write is treated as
    a torn final record. A complete final JSON value without a newline is retained for older logs.
    Committed corruption is never skipped, including malformed records preceding a torn tail.
    """
    records: list[Any] = []
    lines = raw.split(b"\n")
    offset = 0
    for index, line in enumerate(lines):
        if line.strip():
            try:
                records.append(json.loads(line))
            except ValueError:
                if index == len(lines) - 1 and not raw.endswith(b"\n"):
                    return records, offset
                raise JournalCorruption(f"corrupt run journal at {path}:{index + 1}") from None
        offset += len(line) + 1
    return records, len(raw)


class RunState:
    """Atomic files and an append-only journal owned by the orchestrator."""

    def __init__(self, run_dir: Path) -> None:
        self.root = confined_path(Path(run_dir).resolve(), "state")
        self.artifacts = confined_path(self.root, "artifacts")

    def _path(self, relative: str, *, artifacts: bool = False) -> Path:
        clean = normalize_relative_path(relative, field="run state path")
        return confined_path(self.artifacts if artifacts else self.root, clean)

    @staticmethod
    def _atomic_write(path: Path, content: str) -> None:
        RunState._atomic_write_bytes(path, content.encode("utf-8"))

    @staticmethod
    def _atomic_write_bytes(path: Path, content: bytes) -> None:
        _ensure_directory(path.parent)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
            _sync_directory(path.parent)
        finally:
            if tmp.exists():
                tmp.unlink()

    def write_control(self, name: str, content: str) -> None:
        self._atomic_write(self._path(name), content)

    def read_control(self, name: str) -> str:
        return self._path(name).read_text(encoding="utf-8")

    def has_control(self, name: str) -> bool:
        return self._path(name).is_file()

    def clear_control(self, name: str) -> None:
        path = self._path(name)
        try:
            path.unlink()
        except FileNotFoundError:
            return
        _sync_directory(path.parent)

    @contextmanager
    def exclusive(self, operation: str) -> Iterator[None]:
        """Own the whole mutation, not only its final journal append. No lease can expire live.

        The kernel releases ownership when the process exits. Never remove the lock file: another
        inode would allow two owners. Attempts are recorded separately from stage results, so a
        competing caller cannot add a failure that supersedes the real owner's completed stage.
        """
        if fcntl is None:
            raise RuntimeError("run ownership requires POSIX file locking")
        path = self._path(RUN_LOCK)
        _ensure_directory(path.parent)
        with _open_append(path) as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RunBusyError(f"run {self.root.parent.name!r} is already in use") from None
            # Closing the descriptor releases ownership even if journal I/O itself fails.
            previous = self.operation_records()
            if previous and previous[-1].get("event") == "started":
                self.append_json(
                    OPERATIONS_LOG,
                    {
                        **previous[-1],
                        "event": "interrupted",
                        "finished_at": _now(),
                    },
                )
            attempt = {
                "attempt_id": uuid.uuid4().hex,
                "operation": operation,
                "pid": os.getpid(),
                "host": socket.gethostname(),
                "started_at": _now(),
            }
            self.append_json(OPERATIONS_LOG, {**attempt, "event": "started"})
            try:
                yield
            except BaseException as error:
                try:
                    self.append_json(
                        OPERATIONS_LOG,
                        {
                            **attempt,
                            "event": "failed",
                            "finished_at": _now(),
                            "error_type": type(error).__name__,
                        },
                    )
                except Exception:
                    error.add_note("the final operation receipt could not be saved")
                raise
            else:
                self.append_json(
                    OPERATIONS_LOG,
                    {
                        **attempt,
                        "event": "completed",
                        "finished_at": _now(),
                    },
                )

    def ownership(self) -> dict[str, Any]:
        """Read the kernel lock and last attempt without creating state or clearing a lock."""
        path = self._path(RUN_LOCK)
        held: bool | None = None
        try:
            handle = path.open("rb")
        except FileNotFoundError:
            records = self.operation_records()
            # Missing locks on legacy runs are normal. If operation records do exist, avoid
            # declaring an interruption while another process creates the first lock file.
            held = None if records else False
        else:
            with handle:
                if fcntl is not None:
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                    except BlockingIOError:
                        held = True
                    else:
                        held = False
                # Keep the shared lock through this read when acquired: otherwise a fresh owner
                # could start between the lock check and journal read and appear interrupted.
                records = self.operation_records()
        last = records[-1] if records else None
        interrupted = bool(held is False and last and last.get("event") == "started")
        return {"held": held, "interrupted": interrupted, "last_operation": last}

    def operation_records(self) -> list[dict[str, Any]]:
        records = self.read_jsonl(OPERATIONS_LOG)
        for index, record in enumerate(records, start=1):
            if (
                not isinstance(record, dict)
                or not isinstance(record.get("event"), str)
                or record["event"] not in {"started", "completed", "failed", "interrupted"}
                or any(
                    not isinstance(record.get(key), str)
                    for key in ("attempt_id", "operation", "started_at")
                )
            ):
                raise JournalCorruption(f"invalid operation record at {self.root}:{index}")
        return records

    def write_artifact(self, relative: str, content: str) -> None:
        self._atomic_write(self._path(relative, artifacts=True), content)

    def read_artifact(self, relative: str) -> str:
        return self._path(relative, artifacts=True).read_text(encoding="utf-8")

    def has_artifact(self, relative: str) -> bool:
        return self._path(relative, artifacts=True).is_file()

    def mirror_artifact(self, sandbox: Sandbox, relative: str) -> None:
        sandbox.write(relative, self.read_artifact(relative))

    def mirror_all(self, sandbox: Sandbox) -> None:
        if not self.artifacts.is_dir():
            return
        for path in sorted(p for p in self.artifacts.rglob("*") if p.is_file()):
            sandbox.write(path.relative_to(self.artifacts).as_posix(), path.read_text("utf-8"))

    def append_json(self, name: str, value: Any) -> str:
        """Validate, preserve a torn tail, then append under the same exclusive file lock.

        Merely tolerating a torn read is insufficient: appending onto those bytes would turn a
        recoverable final fragment into committed corruption. Save it before truncating, then
        fsync the new record and directory. Corruption in newline-terminated records fails closed.
        """

        line = json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n"
        path = self._path(name)
        _ensure_directory(path.parent)
        if fcntl is None:
            raise RuntimeError("journal appends require POSIX file locking")
        with _open_append(path) as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            handle.seek(0)
            raw = handle.read()
            _, intact = _decode_journal(raw, path)
            if intact < len(raw):
                digest = hashlib.sha256(name.encode() + b"\0" + raw).hexdigest()
                recovery = self._path(f"recovery/{path.name}.{digest}.tail")
                self._atomic_write_bytes(recovery, raw[intact:])
                handle.seek(intact)
                handle.truncate()
                raw = raw[:intact]
            handle.seek(0, os.SEEK_END)
            if raw and not raw.endswith(b"\n"):
                handle.write(b"\n")
            handle.write(line.encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
            _sync_directory(path.parent)
        return line

    def read_jsonl(self, name: str) -> list[Any]:
        """Take a coherent journal read; never mutate a recoverable tail while inspecting it."""
        raw = self._read_journal(name)
        records, _ = _decode_journal(raw, self._path(name))
        return records

    def _read_journal(self, name: str) -> bytes:
        path = self._path(name)
        try:
            handle = path.open("rb")
        except FileNotFoundError:
            return b""
        with handle:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
            return handle.read()

    def journal_status(self, name: str) -> dict[str, Any]:
        """Report incomplete bytes for operators without hiding corrupt committed records."""
        raw = self._read_journal(name)
        records, intact = _decode_journal(raw, self._path(name))
        return {"records": len(records), "bytes": len(raw), "torn_tail_bytes": len(raw) - intact}
