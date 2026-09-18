"""Immutable source snapshots bind execution to committed Git bytes."""

from __future__ import annotations

import subprocess
import tarfile
from pathlib import Path

import pytest

from swfactory.source_snapshot import (
    SourceSnapshotError,
    create_source_snapshot,
    resolve_commit,
    verify_source_snapshot,
)

IDENTITY = [
    "-c",
    "user.name=Snapshot Test",
    "-c",
    "user.email=snapshot@example.invalid",
]


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *IDENTITY, *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-q", "-b", "main")
    (root / "tracked.txt").write_text("committed\n", encoding="utf-8")
    git(root, "add", "tracked.txt")
    git(root, "commit", "-qm", "base")
    return root


def archive_files(path: Path) -> dict[str, bytes]:
    with tarfile.open(path, "r") as archive:
        return {
            member.name: archive.extractfile(member).read()
            for member in archive.getmembers()
            if member.isfile() and archive.extractfile(member) is not None
        }


def test_snapshot_contains_only_the_recorded_commit(repo: Path, tmp_path: Path) -> None:
    commit = git(repo, "rev-parse", "HEAD")
    (repo / "tracked.txt").write_text("dirty worktree\n", encoding="utf-8")
    (repo / "untracked.txt").write_text("must never run\n", encoding="utf-8")

    snapshot = create_source_snapshot(repo, cache_root=tmp_path / "cache")
    files = archive_files(Path(snapshot.path))

    assert snapshot.commit_sha == commit
    assert files["tracked.txt"] == b"committed\n"
    assert "untracked.txt" not in files
    verify_source_snapshot(snapshot)


def test_verified_cache_reuse_does_not_archive_again(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = tmp_path / "cache"
    first = create_source_snapshot(repo, cache_root=cache)

    def fail_archive(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("a verified cached snapshot should be reused")

    monkeypatch.setattr("swfactory.source_snapshot._archive", fail_archive)
    second = create_source_snapshot(repo, cache_root=cache)

    assert second == first


def test_corrupt_cached_object_is_quarantined_and_rebuilt(repo: Path, tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    first = create_source_snapshot(repo, cache_root=cache)
    Path(first.path).write_bytes(b"tampered")

    rebuilt = create_source_snapshot(repo, cache_root=cache)

    assert rebuilt.sha256 == first.sha256
    assert rebuilt.size_bytes == first.size_bytes
    verify_source_snapshot(rebuilt)
    quarantined = list((cache / "quarantine").glob("*.bad"))
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == b"tampered"


def test_old_commit_snapshot_does_not_move_when_branch_moves(repo: Path, tmp_path: Path) -> None:
    first_sha = git(repo, "rev-parse", "HEAD")
    (repo / "tracked.txt").write_text("second\n", encoding="utf-8")
    git(repo, "add", "tracked.txt")
    git(repo, "commit", "-qm", "second")

    snapshot = create_source_snapshot(repo, first_sha, cache_root=tmp_path / "cache")
    files = archive_files(Path(snapshot.path))

    assert snapshot.commit_sha == first_sha
    assert files["tracked.txt"] == b"committed\n"


def test_snapshot_digest_detects_later_tampering(repo: Path, tmp_path: Path) -> None:
    snapshot = create_source_snapshot(repo, cache_root=tmp_path / "cache")
    Path(snapshot.path).write_bytes(b"tampered")

    with pytest.raises(SourceSnapshotError, match="digest mismatch"):
        verify_source_snapshot(snapshot)


def test_invalid_revision_fails_before_writing_snapshot(repo: Path, tmp_path: Path) -> None:
    cache = tmp_path / "cache"

    with pytest.raises(SourceSnapshotError, match="cannot resolve"):
        create_source_snapshot(repo, "not-a-revision", cache_root=cache)

    assert not cache.exists()


def test_resolve_commit_rejects_non_commit_object(repo: Path) -> None:
    blob = subprocess.run(
        ["git", "hash-object", "-w", "--stdin"],
        cwd=repo,
        input=b"blob",
        capture_output=True,
        check=True,
    ).stdout.decode().strip()

    with pytest.raises(SourceSnapshotError, match="cannot resolve"):
        resolve_commit(repo, blob)
