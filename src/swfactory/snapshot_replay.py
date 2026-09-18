"""Replay an immutable source snapshot under an explicit execution recipe.

This is a local verification primitive, not a scheduler. It reconstructs one
recorded source archive into a disposable directory, executes one argv vector
without a shell, retains stdout/stderr, and seals the result under digests.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from swfactory.source_snapshot import SourceSnapshot, verify_source_snapshot

_ENV_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SnapshotReplayError(RuntimeError):
    """A replay recipe or its retained evidence is unsafe, invalid, or stale."""


@dataclass(frozen=True)
class SnapshotRunRecipe:
    argv: tuple[str, ...]
    cwd: str = "."
    env: tuple[tuple[str, str], ...] = ()
    timeout_s: float = 300.0
    schema_version: int = 1

    def validate(self) -> None:
        if self.schema_version != 1:
            raise SnapshotReplayError(f"unsupported replay recipe schema {self.schema_version}")
        if not self.argv or any(not arg or "\x00" in arg for arg in self.argv):
            raise SnapshotReplayError("replay argv must contain nonempty NUL-free arguments")
        cwd = PurePosixPath(self.cwd)
        if cwd.is_absolute() or ".." in cwd.parts:
            raise SnapshotReplayError("replay cwd must stay inside the extracted snapshot")
        if not (0 < self.timeout_s <= 3600):
            raise SnapshotReplayError("replay timeout must be in (0, 3600] seconds")
        keys: set[str] = set()
        for key, value in self.env:
            if not _ENV_KEY.fullmatch(key):
                raise SnapshotReplayError(f"invalid environment key: {key!r}")
            if key in keys:
                raise SnapshotReplayError(f"duplicate environment key: {key}")
            if "\x00" in value:
                raise SnapshotReplayError(f"environment value for {key} contains NUL")
            keys.add(key)

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "argv": list(self.argv),
            "cwd": self.cwd,
            "env": [[key, value] for key, value in sorted(self.env)],
            "timeout_s": self.timeout_s,
        }

    @property
    def digest(self) -> str:
        payload = json.dumps(self.canonical_dict(), sort_keys=True, separators=(",", ":")).encode()
        return "sha256:" + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class RetainedStream:
    path: str
    sha256: str
    size_bytes: int

    def validate(self) -> None:
        path = PurePosixPath(self.path)
        if path.is_absolute() or ".." in path.parts:
            raise SnapshotReplayError(f"unsafe retained stream path: {self.path}")
        if len(self.sha256) != 64 or any(ch not in "0123456789abcdef" for ch in self.sha256):
            raise SnapshotReplayError("retained stream digest is not lowercase sha256")
        if self.size_bytes < 0:
            raise SnapshotReplayError("retained stream size is negative")


@dataclass(frozen=True)
class SnapshotRunReceipt:
    commit_sha: str
    snapshot_sha256: str
    snapshot_size_bytes: int
    recipe_digest: str
    exit_code: int | None
    timed_out: bool
    duration_s: float
    stdout: RetainedStream
    stderr: RetainedStream
    schema_version: int = 1

    def validate(self) -> None:
        if self.schema_version != 1:
            raise SnapshotReplayError(f"unsupported replay receipt schema {self.schema_version}")
        if len(self.commit_sha) != 40 or any(ch not in "0123456789abcdef" for ch in self.commit_sha):
            raise SnapshotReplayError("receipt commit is not a full lowercase Git SHA")
        if len(self.snapshot_sha256) != 64:
            raise SnapshotReplayError("receipt snapshot digest is invalid")
        if self.snapshot_size_bytes <= 0:
            raise SnapshotReplayError("receipt snapshot size is invalid")
        if not self.recipe_digest.startswith("sha256:") or len(self.recipe_digest) != 71:
            raise SnapshotReplayError("receipt recipe digest is invalid")
        if self.timed_out and self.exit_code is not None:
            raise SnapshotReplayError("timed-out replay cannot claim an exit code")
        if not self.timed_out and self.exit_code is None:
            raise SnapshotReplayError("completed replay must record an exit code")
        if self.duration_s < 0:
            raise SnapshotReplayError("receipt duration is negative")
        self.stdout.validate()
        self.stderr.validate()

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "commit_sha": self.commit_sha,
            "snapshot_sha256": self.snapshot_sha256,
            "snapshot_size_bytes": self.snapshot_size_bytes,
            "recipe_digest": self.recipe_digest,
            "exit_code": self.exit_code,
            "timed_out": self.timed_out,
            "duration_s": self.duration_s,
            "stdout": asdict(self.stdout),
            "stderr": asdict(self.stderr),
        }

    @property
    def digest(self) -> str:
        payload = json.dumps(self.canonical_dict(), sort_keys=True, separators=(",", ":")).encode()
        return "sha256:" + hashlib.sha256(payload).hexdigest()


def run_snapshot_recipe(
    snapshot: SourceSnapshot,
    recipe: SnapshotRunRecipe,
    destination: Path,
) -> SnapshotRunReceipt:
    """Execute one explicit recipe from one verified immutable source archive."""

    recipe.validate()
    verify_source_snapshot(snapshot)
    destination = _prepare_destination(destination)
    work_parent = destination / ".work"
    work_parent.mkdir(mode=0o700)
    extracted = Path(tempfile.mkdtemp(prefix="snapshot-", dir=work_parent))
    try:
        _extract_snapshot(Path(snapshot.archive_path), extracted)
        cwd = _resolve_cwd(extracted, recipe.cwd)
        stdout_path = destination / "stdout.bin"
        stderr_path = destination / "stderr.bin"
        env = {key: value for key, value in recipe.env}
        started = time.monotonic()
        timed_out = False
        exit_code: int | None = None
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            _restrict_file(stdout_path)
            _restrict_file(stderr_path)
            try:
                proc = subprocess.run(
                    list(recipe.argv),
                    cwd=cwd,
                    env=env,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    check=False,
                    timeout=recipe.timeout_s,
                )
                exit_code = proc.returncode
            except subprocess.TimeoutExpired:
                timed_out = True
        duration = round(time.monotonic() - started, 6)
        stdout_receipt = _retained_stream(stdout_path, destination)
        stderr_receipt = _retained_stream(stderr_path, destination)
        receipt = SnapshotRunReceipt(
            commit_sha=snapshot.commit_sha,
            snapshot_sha256=snapshot.sha256,
            snapshot_size_bytes=snapshot.size_bytes,
            recipe_digest=recipe.digest,
            exit_code=exit_code,
            timed_out=timed_out,
            duration_s=duration,
            stdout=stdout_receipt,
            stderr=stderr_receipt,
        )
        recipe_document = recipe.canonical_dict()
        recipe_document["recipe_digest"] = recipe.digest
        _atomic_json(destination / "recipe.json", recipe_document)
        receipt_document = receipt.canonical_dict()
        receipt_document["receipt_digest"] = receipt.digest
        _atomic_json(destination / "receipt.json", receipt_document)
        verify_snapshot_run(destination, snapshot=snapshot)
        return receipt
    finally:
        shutil.rmtree(work_parent, ignore_errors=True)


def verify_snapshot_run(
    destination: Path,
    *,
    snapshot: SourceSnapshot | None = None,
) -> tuple[SnapshotRunRecipe, SnapshotRunReceipt]:
    """Verify recipe, receipt and retained streams; optionally re-bind source bytes."""

    destination = Path(destination)
    if destination.is_symlink() or not destination.is_dir():
        raise SnapshotReplayError(f"replay destination is absent or redirected: {destination}")
    recipe_document = _load_json(destination / "recipe.json")
    receipt_document = _load_json(destination / "receipt.json")
    try:
        recipe = SnapshotRunRecipe(
            argv=tuple(str(value) for value in recipe_document["argv"]),
            cwd=str(recipe_document.get("cwd", ".")),
            env=tuple((str(row[0]), str(row[1])) for row in recipe_document.get("env", ())),
            timeout_s=float(recipe_document.get("timeout_s", 300.0)),
            schema_version=int(recipe_document.get("schema_version", 1)),
        )
        receipt = SnapshotRunReceipt(
            commit_sha=str(receipt_document["commit_sha"]),
            snapshot_sha256=str(receipt_document["snapshot_sha256"]),
            snapshot_size_bytes=int(receipt_document["snapshot_size_bytes"]),
            recipe_digest=str(receipt_document["recipe_digest"]),
            exit_code=(
                None if receipt_document.get("exit_code") is None else int(receipt_document["exit_code"])
            ),
            timed_out=bool(receipt_document["timed_out"]),
            duration_s=float(receipt_document["duration_s"]),
            stdout=RetainedStream(**receipt_document["stdout"]),
            stderr=RetainedStream(**receipt_document["stderr"]),
            schema_version=int(receipt_document.get("schema_version", 1)),
        )
    except (KeyError, TypeError, ValueError, IndexError) as error:
        raise SnapshotReplayError(f"invalid replay manifest: {error}") from error
    if recipe_document.get("recipe_digest") != recipe.digest:
        raise SnapshotReplayError("replay recipe digest mismatch")
    if receipt_document.get("receipt_digest") != receipt.digest:
        raise SnapshotReplayError("replay receipt digest mismatch")
    if receipt.recipe_digest != recipe.digest:
        raise SnapshotReplayError("receipt targets a different replay recipe")
    for stream in (receipt.stdout, receipt.stderr):
        path = destination / stream.path
        if path.is_symlink() or not path.is_file():
            raise SnapshotReplayError(f"retained replay stream is absent or redirected: {stream.path}")
        digest, size = _digest_file(path)
        if digest != stream.sha256 or size != stream.size_bytes:
            raise SnapshotReplayError(
                f"retained replay stream changed: {stream.path} "
                f"expected {stream.sha256}/{stream.size_bytes}, observed {digest}/{size}"
            )
    if snapshot is not None:
        verify_source_snapshot(snapshot)
        if (
            receipt.commit_sha != snapshot.commit_sha
            or receipt.snapshot_sha256 != snapshot.sha256
            or receipt.snapshot_size_bytes != snapshot.size_bytes
        ):
            raise SnapshotReplayError("replay receipt targets different source snapshot bytes")
    return recipe, receipt


def _prepare_destination(destination: Path) -> Path:
    raw = Path(destination)
    if raw.is_symlink():
        raise SnapshotReplayError(f"replay destination is a symlink: {raw}")
    destination = raw.resolve()
    if destination.exists() and not destination.is_dir():
        raise SnapshotReplayError(f"replay destination is not a directory: {destination}")
    if destination.exists() and any(destination.iterdir()):
        raise SnapshotReplayError(f"replay destination is not empty: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    if os.name == "posix":
        destination.chmod(0o700)
    return destination


def _extract_snapshot(archive_path: Path, destination: Path) -> None:
    try:
        with tarfile.open(archive_path, "r:") as archive:
            members = archive.getmembers()
            for member in members:
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts or not member.name:
                    raise SnapshotReplayError(f"unsafe archive member path: {member.name!r}")
                if not (member.isdir() or member.isfile()):
                    raise SnapshotReplayError(
                        f"replay refuses non-regular archive member: {member.name!r}"
                    )
            archive.extractall(destination, members=members, filter="data")
    except (tarfile.TarError, OSError) as error:
        raise SnapshotReplayError(f"cannot extract source snapshot: {error}") from error


def _resolve_cwd(root: Path, cwd: str) -> Path:
    relative = PurePosixPath(cwd)
    if relative.is_absolute() or ".." in relative.parts:
        raise SnapshotReplayError("replay cwd escapes extracted snapshot")
    target = (root / Path(*relative.parts)).resolve()
    try:
        target.relative_to(root.resolve())
    except ValueError as error:
        raise SnapshotReplayError("replay cwd resolves outside extracted snapshot") from error
    if not target.is_dir():
        raise SnapshotReplayError(f"replay cwd does not exist: {cwd}")
    return target


def _retained_stream(path: Path, root: Path) -> RetainedStream:
    digest, size = _digest_file(path)
    return RetainedStream(path.relative_to(root).as_posix(), digest, size)


def _digest_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(128 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _load_json(path: Path) -> Mapping[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise SnapshotReplayError(f"replay manifest is absent or redirected: {path.name}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SnapshotReplayError(f"cannot load replay manifest {path.name}: {error}") from error
    if not isinstance(value, dict):
        raise SnapshotReplayError(f"replay manifest {path.name} is not an object")
    return value


def _atomic_json(path: Path, document: Mapping[str, Any]) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        temporary.write_text(json.dumps(dict(document), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _restrict_file(temporary)
        os.replace(temporary, path)
        _restrict_file(path)
    finally:
        temporary.unlink(missing_ok=True)


def _restrict_file(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o600)
