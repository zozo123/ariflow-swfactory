"""Replay one immutable source snapshot under its commit-bound execution recipe.

This module is an evidence/reproduction primitive, not a scheduler or sandbox.
It reconstructs recorded source bytes, executes the recipe loaded from the same
Git commit, retains stdout/stderr, and seals the observed result under digests.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from swfactory.execution_recipe import (
    BoundExecutionRecipe,
    ExecutionRecipeError,
    load_execution_recipe,
    parse_execution_recipe,
)
from swfactory.source_snapshot import SourceSnapshot, verify_source_snapshot


class SnapshotReplayError(RuntimeError):
    """A replay input or retained result is unsafe, inconsistent, or stale."""


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
    execution_recipe_sha256: str
    execution_recipe_commit_sha: str
    execution_recipe_path: str
    requested_cpus: int
    requested_memory_mb: int
    resource_enforcement: str
    executable_path: str
    executable_sha256: str
    exit_code: int | None
    timed_out: bool
    duration_s: float
    stdout: RetainedStream
    stderr: RetainedStream
    schema_version: int = 1

    def validate(self) -> None:
        if self.schema_version != 1:
            raise SnapshotReplayError(f"unsupported replay receipt schema {self.schema_version}")
        for label, sha in (
            ("commit", self.commit_sha),
            ("recipe commit", self.execution_recipe_commit_sha),
        ):
            if len(sha) != 40 or any(ch not in "0123456789abcdef" for ch in sha):
                raise SnapshotReplayError(f"{label} is not a full lowercase Git SHA")
        for label, digest in (
            ("snapshot", self.snapshot_sha256),
            ("execution recipe", self.execution_recipe_sha256),
            ("executable", self.executable_sha256),
        ):
            if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
                raise SnapshotReplayError(f"{label} digest is not lowercase sha256")
        if self.commit_sha != self.execution_recipe_commit_sha:
            raise SnapshotReplayError("replay source and execution recipe commits differ")
        if self.snapshot_size_bytes <= 0:
            raise SnapshotReplayError("replay snapshot size is invalid")
        if self.requested_cpus <= 0 or self.requested_memory_mb <= 0:
            raise SnapshotReplayError("replay resource request is invalid")
        if self.resource_enforcement != "declared-not-enforced-local":
            raise SnapshotReplayError("unexpected local replay resource enforcement mode")
        if not self.execution_recipe_path:
            raise SnapshotReplayError("replay execution recipe path is empty")
        if self.timed_out and self.exit_code is not None:
            raise SnapshotReplayError("timed-out replay cannot claim an exit code")
        if not self.timed_out and self.exit_code is None:
            raise SnapshotReplayError("completed replay must record an exit code")
        if self.duration_s < 0:
            raise SnapshotReplayError("replay duration is negative")
        self.stdout.validate()
        self.stderr.validate()

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "commit_sha": self.commit_sha,
            "snapshot_sha256": self.snapshot_sha256,
            "snapshot_size_bytes": self.snapshot_size_bytes,
            "execution_recipe_sha256": self.execution_recipe_sha256,
            "execution_recipe_commit_sha": self.execution_recipe_commit_sha,
            "execution_recipe_path": self.execution_recipe_path,
            "requested_cpus": self.requested_cpus,
            "requested_memory_mb": self.requested_memory_mb,
            "resource_enforcement": self.resource_enforcement,
            "executable_path": self.executable_path,
            "executable_sha256": self.executable_sha256,
            "exit_code": self.exit_code,
            "timed_out": self.timed_out,
            "duration_s": self.duration_s,
            "stdout": asdict(self.stdout),
            "stderr": asdict(self.stderr),
        }

    @property
    def digest(self) -> str:
        payload = json.dumps(self.canonical_dict(), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()


def run_snapshot_recipe(
    snapshot: SourceSnapshot,
    bound_recipe: BoundExecutionRecipe,
    destination: Path,
) -> SnapshotRunReceipt:
    """Execute one commit-bound recipe against the exact snapshot for that commit."""

    verify_source_snapshot(snapshot)
    if snapshot.commit_sha != bound_recipe.commit_sha:
        raise SnapshotReplayError(
            f"source snapshot commit {snapshot.commit_sha} != execution recipe commit {bound_recipe.commit_sha}"
        )
    recipe = bound_recipe.recipe
    if recipe.secret_env:
        raise SnapshotReplayError(
            "local replay does not inject secret_env values; use an isolated provider with secret injection"
        )

    destination = _prepare_destination(destination)
    work_parent = destination / ".work"
    work_parent.mkdir(mode=0o700)
    extracted = Path(tempfile.mkdtemp(prefix="snapshot-", dir=work_parent))
    try:
        _extract_snapshot(Path(snapshot.archive_path), extracted)
        cwd = _resolve_cwd(extracted, recipe.cwd)
        executable = _resolve_executable(recipe.argv[0], cwd)
        executable_digest, _ = _digest_file(executable)
        argv = [str(executable), *recipe.argv[1:]]
        stdout_path = destination / "stdout.bin"
        stderr_path = destination / "stderr.bin"
        env = dict(recipe.environment)

        started = time.monotonic()
        timed_out = False
        exit_code: int | None = None
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            _restrict_file(stdout_path)
            _restrict_file(stderr_path)
            try:
                proc = subprocess.run(
                    argv,
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

        receipt = SnapshotRunReceipt(
            commit_sha=snapshot.commit_sha,
            snapshot_sha256=snapshot.sha256,
            snapshot_size_bytes=snapshot.size_bytes,
            execution_recipe_sha256=bound_recipe.digest,
            execution_recipe_commit_sha=bound_recipe.commit_sha,
            execution_recipe_path=bound_recipe.path,
            requested_cpus=recipe.cpus,
            requested_memory_mb=recipe.memory_mb,
            resource_enforcement="declared-not-enforced-local",
            executable_path=str(executable),
            executable_sha256=executable_digest,
            exit_code=exit_code,
            timed_out=timed_out,
            duration_s=duration,
            stdout=_retained_stream(stdout_path, destination),
            stderr=_retained_stream(stderr_path, destination),
        )
        recipe_document = _bound_recipe_document(bound_recipe)
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
    repo: Path | None = None,
) -> tuple[BoundExecutionRecipe, SnapshotRunReceipt]:
    """Re-hash replay evidence and optionally re-bind source and recipe Git objects."""

    destination = Path(destination)
    if destination.is_symlink() or not destination.is_dir():
        raise SnapshotReplayError(f"replay destination is absent or redirected: {destination}")
    recipe_document = _load_json(destination / "recipe.json")
    receipt_document = _load_json(destination / "receipt.json")
    try:
        recipe_payload = recipe_document["recipe"]
        if not isinstance(recipe_payload, dict):
            raise TypeError("recipe must be an object")
        recipe_payload = {key: value for key, value in recipe_payload.items() if key != "digest"}
        recipe = parse_execution_recipe(recipe_payload)
        bound_recipe = BoundExecutionRecipe(
            commit_sha=str(recipe_document["commit_sha"]),
            path=str(recipe_document["path"]),
            recipe=recipe,
            schema_version=int(recipe_document.get("schema_version", 1)),
        )
        receipt = SnapshotRunReceipt(
            commit_sha=str(receipt_document["commit_sha"]),
            snapshot_sha256=str(receipt_document["snapshot_sha256"]),
            snapshot_size_bytes=int(receipt_document["snapshot_size_bytes"]),
            execution_recipe_sha256=str(receipt_document["execution_recipe_sha256"]),
            execution_recipe_commit_sha=str(receipt_document["execution_recipe_commit_sha"]),
            execution_recipe_path=str(receipt_document["execution_recipe_path"]),
            requested_cpus=int(receipt_document["requested_cpus"]),
            requested_memory_mb=int(receipt_document["requested_memory_mb"]),
            resource_enforcement=str(receipt_document["resource_enforcement"]),
            executable_path=str(receipt_document["executable_path"]),
            executable_sha256=str(receipt_document["executable_sha256"]),
            exit_code=None if receipt_document.get("exit_code") is None else int(receipt_document["exit_code"]),
            timed_out=bool(receipt_document["timed_out"]),
            duration_s=float(receipt_document["duration_s"]),
            stdout=RetainedStream(**receipt_document["stdout"]),
            stderr=RetainedStream(**receipt_document["stderr"]),
            schema_version=int(receipt_document.get("schema_version", 1)),
        )
    except (ExecutionRecipeError, KeyError, TypeError, ValueError, IndexError) as error:
        raise SnapshotReplayError(f"invalid replay manifest: {error}") from error

    if recipe_document.get("digest") != bound_recipe.digest:
        raise SnapshotReplayError("retained execution recipe digest mismatch")
    if receipt_document.get("receipt_digest") != receipt.digest:
        raise SnapshotReplayError("replay receipt digest mismatch")
    if receipt.execution_recipe_sha256 != bound_recipe.digest:
        raise SnapshotReplayError("receipt targets a different execution recipe")
    if receipt.execution_recipe_commit_sha != bound_recipe.commit_sha:
        raise SnapshotReplayError("receipt execution recipe commit mismatch")
    if receipt.execution_recipe_path != bound_recipe.path:
        raise SnapshotReplayError("receipt execution recipe path mismatch")

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

    executable = Path(receipt.executable_path)
    if executable.is_symlink() or not executable.is_file():
        raise SnapshotReplayError(f"recorded replay executable is absent or redirected: {executable}")
    executable_digest, _ = _digest_file(executable)
    if executable_digest != receipt.executable_sha256:
        raise SnapshotReplayError("recorded replay executable bytes changed")

    if snapshot is not None:
        verify_source_snapshot(snapshot)
        if (
            receipt.commit_sha != snapshot.commit_sha
            or receipt.snapshot_sha256 != snapshot.sha256
            or receipt.snapshot_size_bytes != snapshot.size_bytes
        ):
            raise SnapshotReplayError("replay receipt targets different source snapshot bytes")

    if repo is not None:
        try:
            observed = load_execution_recipe(repo, bound_recipe.commit_sha, path=bound_recipe.path)
        except ExecutionRecipeError as error:
            raise SnapshotReplayError(f"cannot re-load committed execution recipe: {error}") from error
        if observed.digest != bound_recipe.digest:
            raise SnapshotReplayError("retained recipe differs from recipe in recorded Git commit")

    return bound_recipe, receipt


def _bound_recipe_document(bound_recipe: BoundExecutionRecipe) -> dict[str, Any]:
    recipe = bound_recipe.recipe
    return {
        "schema_version": bound_recipe.schema_version,
        "commit_sha": bound_recipe.commit_sha,
        "path": bound_recipe.path,
        "recipe": {
            "schema_version": recipe.schema_version,
            "argv": list(recipe.argv),
            "cwd": recipe.cwd,
            "timeout_s": recipe.timeout_s,
            "resources": {"cpus": recipe.cpus, "memory_mb": recipe.memory_mb},
            "environment": dict(recipe.environment),
            "secret_env": list(recipe.secret_env),
        },
        "digest": bound_recipe.digest,
    }


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
                        f"local replay refuses non-regular archive member: {member.name!r}"
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


def _resolve_executable(argv0: str, cwd: Path) -> Path:
    candidate = Path(argv0)
    if candidate.is_absolute():
        resolved = candidate.resolve()
    elif "/" in argv0 or "\\" in argv0:
        resolved = (cwd / candidate).resolve()
    else:
        found = shutil.which(argv0)
        if found is None:
            raise SnapshotReplayError(f"replay executable is not available on host: {argv0}")
        resolved = Path(found).resolve()
    if not resolved.is_file():
        raise SnapshotReplayError(f"replay executable is not a regular file: {resolved}")
    return resolved


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
