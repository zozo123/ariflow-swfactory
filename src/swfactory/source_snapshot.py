"""Immutable, content-addressed source archives for candidate execution.

A candidate must run the code revision it claims to evaluate, not whatever bytes
happen to be in a mutable checkout when a provider starts.  This module turns a
Git revision into a tar archive of the exact recorded commit, hashes the archive,
and stores it under its content digest.

Only committed Git content enters the archive.  Dirty tracked files and untracked
files are deliberately absent because `git archive` reads the commit object, not
the worktree.  A small commit-keyed index makes reuse cheap, but every reused
object is re-hashed before it is trusted.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from swfactory.repo_runtime import immutable_cache_key


class SourceSnapshotError(RuntimeError):
    """A source revision could not be resolved, archived, or verified."""


@dataclass(frozen=True)
class SourceSnapshot:
    commit_sha: str
    sha256: str
    size_bytes: int
    path: str
    cache_key: str
    format: str = "tar"
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def resolve_commit(repo: Path, revision: str) -> str:
    """Resolve a user-facing revision to the exact commit object it names."""
    repo = repo.resolve()
    proc = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", f"{revision}^{{commit}}"],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip() or "unknown revision"
        raise SourceSnapshotError(f"cannot resolve {revision!r} in {repo}: {detail}")
    sha = proc.stdout.strip()
    if not sha:
        raise SourceSnapshotError(f"git returned an empty commit for {revision!r}")
    return sha


def create_source_snapshot(
    repo: Path,
    revision: str = "HEAD",
    *,
    cache_root: Path = Path(".factory/source-snapshots"),
) -> SourceSnapshot:
    """Create or reuse a verified archive for one exact Git commit."""
    repo = repo.resolve()
    commit_sha = resolve_commit(repo, revision)
    cache_root = cache_root.resolve()
    objects = cache_root / "objects"
    indexes = cache_root / "index"
    quarantine = cache_root / "quarantine"
    for directory in (cache_root, objects, indexes, quarantine):
        directory.mkdir(parents=True, exist_ok=True)
        _restrict_dir(directory)

    key = immutable_cache_key("source-snapshot", {"commit": commit_sha, "format": "tar"})
    index_path = indexes / f"{key.digest}.json"
    cached = _load_cached(index_path, objects, quarantine, commit_sha, key.key)
    if cached is not None:
        return cached

    fd, temporary_name = tempfile.mkstemp(prefix=".snapshot-", suffix=".tar", dir=objects)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        _archive(repo, commit_sha, temporary)
        digest, size = _digest_file(temporary)
        destination = objects / f"{digest}.tar"
        if destination.exists():
            observed_digest, observed_size = _digest_file(destination)
            if observed_digest != digest or observed_size != size:
                _quarantine(destination, quarantine, key.key)
                os.replace(temporary, destination)
            else:
                temporary.unlink()
        else:
            os.replace(temporary, destination)
        _restrict_file(destination)

        snapshot = SourceSnapshot(
            commit_sha=commit_sha,
            sha256=digest,
            size_bytes=size,
            path=str(destination),
            cache_key=key.key,
        )
        verify_source_snapshot(snapshot)
        _write_index(index_path, snapshot)
        return snapshot
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def verify_source_snapshot(snapshot: SourceSnapshot) -> None:
    """Fail closed if the retained object no longer matches its evidence."""
    path = Path(snapshot.path)
    if not path.is_file():
        raise SourceSnapshotError(f"source snapshot is absent: {path}")
    digest, size = _digest_file(path)
    if digest != snapshot.sha256:
        raise SourceSnapshotError(
            f"source snapshot digest mismatch: expected {snapshot.sha256}, observed {digest}"
        )
    if size != snapshot.size_bytes:
        raise SourceSnapshotError(
            f"source snapshot size mismatch: expected {snapshot.size_bytes}, observed {size}"
        )


def _load_cached(
    index_path: Path,
    objects: Path,
    quarantine: Path,
    commit_sha: str,
    cache_key: str,
) -> SourceSnapshot | None:
    if not index_path.is_file():
        return None
    try:
        document = json.loads(index_path.read_text(encoding="utf-8"))
        snapshot = SourceSnapshot(
            commit_sha=str(document["commit_sha"]),
            sha256=str(document["sha256"]),
            size_bytes=int(document["size_bytes"]),
            path=str(objects / f"{document['sha256']}.tar"),
            cache_key=str(document["cache_key"]),
            format=str(document.get("format", "tar")),
            schema_version=int(document.get("schema_version", 1)),
        )
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        index_path.unlink(missing_ok=True)
        return None

    if snapshot.commit_sha != commit_sha or snapshot.cache_key != cache_key or snapshot.format != "tar":
        index_path.unlink(missing_ok=True)
        return None

    try:
        verify_source_snapshot(snapshot)
    except SourceSnapshotError:
        path = Path(snapshot.path)
        if path.exists():
            _quarantine(path, quarantine, cache_key)
        index_path.unlink(missing_ok=True)
        return None
    return snapshot


def _archive(repo: Path, commit_sha: str, destination: Path) -> None:
    with destination.open("wb") as output:
        proc = subprocess.run(
            ["git", "-C", str(repo), "archive", "--format=tar", commit_sha],
            stdout=output,
            stderr=subprocess.PIPE,
            check=False,
            timeout=600,
        )
    if proc.returncode == 0:
        _restrict_file(destination)
        return
    destination.unlink(missing_ok=True)
    detail = proc.stderr.decode(errors="replace").strip()
    raise SourceSnapshotError(f"git archive failed for {commit_sha}: {detail}")


def _digest_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(128 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _write_index(path: Path, snapshot: SourceSnapshot) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=".index-", suffix=".json", dir=path.parent)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        temporary.write_text(json.dumps(snapshot.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _restrict_file(temporary)
        os.replace(temporary, path)
        _restrict_file(path)
    finally:
        temporary.unlink(missing_ok=True)


def _quarantine(path: Path, quarantine: Path, cache_key: str) -> Path:
    suffix = hashlib.sha256(cache_key.encode()).hexdigest()[:16]
    target = quarantine / f"{path.name}.{suffix}.bad"
    counter = 1
    while target.exists():
        target = quarantine / f"{path.name}.{suffix}.{counter}.bad"
        counter += 1
    os.replace(path, target)
    _restrict_file(target)
    return target


def _restrict_dir(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o700)


def _restrict_file(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o600)
