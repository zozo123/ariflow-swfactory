"""Immutable source snapshots from exact Git commits.

Candidate execution must not depend on whatever happens to be present in a mutable
working tree. A source snapshot is therefore built with ``git archive`` from one
resolved commit, hashed, and installed under a content-addressed name.

This module owns no scheduling or promotion authority. It is a transport/evidence
primitive: callers receive bytes that are provably tied to a recorded revision.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path


class SourceSnapshotError(RuntimeError):
    """An exact source snapshot could not be created or verified."""


@dataclass(frozen=True)
class SourceSnapshot:
    commit_sha: str
    sha256: str
    size_bytes: int
    archive_path: str
    cache_hit: bool
    format: str = "tar"
    schema_version: int = 1

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def create_source_snapshot(repo: Path, revision: str, cache_root: Path) -> SourceSnapshot:
    """Archive exactly ``revision^{commit}`` and install it by content digest.

    Untracked files and dirty working-tree edits cannot enter the archive because
    Git reads the recorded tree object, not the checkout. Existing cache entries
    are reused only after their digest and byte size are re-verified.
    """

    repo = Path(repo).resolve()
    cache_root = Path(cache_root).resolve()
    if not repo.is_dir():
        raise SourceSnapshotError(f"repository does not exist: {repo}")
    commit = _git(repo, "rev-parse", "--verify", f"{revision}^{{commit}}").strip()
    if not commit:
        raise SourceSnapshotError(f"revision did not resolve to a commit: {revision}")

    _prepare_cache_root(cache_root)
    fd, temporary_name = tempfile.mkstemp(prefix=".source-", suffix=".tar", dir=cache_root)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        with temporary.open("wb") as stream:
            proc = subprocess.run(
                ["git", "-C", str(repo), "archive", "--format=tar", commit],
                stdout=stream,
                stderr=subprocess.PIPE,
                check=False,
                timeout=300,
            )
        if proc.returncode != 0:
            detail = proc.stderr.decode(errors="replace").strip()
            raise SourceSnapshotError(f"git archive failed for {commit}: {detail}")

        digest, size = _digest_file(temporary)
        destination = cache_root / f"{digest}.tar"
        cache_hit = _install_content_addressed(temporary, destination, digest, size)
        return SourceSnapshot(
            commit_sha=commit,
            sha256=digest,
            size_bytes=size,
            archive_path=str(destination),
            cache_hit=cache_hit,
        )
    finally:
        temporary.unlink(missing_ok=True)


def verify_source_snapshot(snapshot: SourceSnapshot) -> None:
    """Fail closed when retained snapshot bytes no longer match their receipt."""

    path = Path(snapshot.archive_path)
    if not path.is_file():
        raise SourceSnapshotError(f"source snapshot is absent: {path}")
    digest, size = _digest_file(path)
    if digest != snapshot.sha256 or size != snapshot.size_bytes:
        raise SourceSnapshotError(
            "source snapshot integrity mismatch: "
            f"expected {snapshot.sha256}/{snapshot.size_bytes}, observed {digest}/{size}"
        )


def _prepare_cache_root(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise SourceSnapshotError(f"snapshot cache root is not a directory: {path}")
    if os.name == "posix":
        path.chmod(0o700)


def _install_content_addressed(
    temporary: Path,
    destination: Path,
    expected_digest: str,
    expected_size: int,
) -> bool:
    if destination.exists():
        _verify_existing(destination, expected_digest, expected_size)
        return True

    try:
        os.link(temporary, destination)
    except FileExistsError:
        _verify_existing(destination, expected_digest, expected_size)
        return True
    except OSError as error:
        raise SourceSnapshotError(f"could not install source snapshot: {error}") from error

    if os.name == "posix":
        destination.chmod(0o600)
    _verify_existing(destination, expected_digest, expected_size)
    return False


def _verify_existing(path: Path, expected_digest: str, expected_size: int) -> None:
    if not path.is_file():
        raise SourceSnapshotError(f"content-addressed snapshot is not a file: {path}")
    digest, size = _digest_file(path)
    if digest != expected_digest or size != expected_size:
        raise SourceSnapshotError(
            "content-addressed snapshot collision or corruption: "
            f"{path} expected {expected_digest}/{expected_size}, observed {digest}/{size}"
        )


def _digest_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(128 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _git(repo: Path, *args: str) -> str:
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
        env=env,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip()
        raise SourceSnapshotError(f"git {' '.join(args)} failed: {detail}")
    return proc.stdout
