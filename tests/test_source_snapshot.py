"""Exact source snapshots: committed bytes only, content-addressed and verified."""

from __future__ import annotations

import subprocess
import tarfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory.cli import app
from swfactory.source_snapshot import (
    SourceSnapshotError,
    create_source_snapshot,
    verify_source_snapshot,
)


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Factory Test")
    _git(repo, "config", "user.email", "factory@example.test")
    (repo / "tracked.txt").write_text("committed\n", encoding="utf-8")
    _git(repo, "add", "tracked.txt")
    _git(repo, "commit", "-q", "-m", "base")
    return repo, _git(repo, "rev-parse", "HEAD")


def _archive_text(path: Path, member: str) -> str:
    with tarfile.open(path, "r:") as archive:
        stream = archive.extractfile(member)
        assert stream is not None
        return stream.read().decode()


def test_snapshot_contains_recorded_commit_not_dirty_worktree(tmp_path: Path) -> None:
    repo, commit = _repo(tmp_path)
    (repo / "tracked.txt").write_text("dirty edit\n", encoding="utf-8")
    (repo / "untracked.txt").write_text("never committed\n", encoding="utf-8")

    snapshot = create_source_snapshot(repo, "HEAD", tmp_path / "snapshots")

    assert snapshot.commit_sha == commit
    assert snapshot.cache_hit is False
    assert _archive_text(Path(snapshot.archive_path), "tracked.txt") == "committed\n"
    with tarfile.open(snapshot.archive_path, "r:") as archive:
        assert "untracked.txt" not in archive.getnames()
    verify_source_snapshot(snapshot)


def test_same_commit_reuses_the_same_verified_content_addressed_archive(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    cache = tmp_path / "snapshots"

    first = create_source_snapshot(repo, "HEAD", cache)
    (repo / "tracked.txt").write_text("a different dirty edit\n", encoding="utf-8")
    second = create_source_snapshot(repo, "HEAD", cache)

    assert first.sha256 == second.sha256
    assert first.size_bytes == second.size_bytes
    assert first.archive_path == second.archive_path
    assert first.cache_hit is False
    assert second.cache_hit is True


def test_new_commit_gets_a_new_snapshot_identity(tmp_path: Path) -> None:
    repo, first_commit = _repo(tmp_path)
    cache = tmp_path / "snapshots"
    first = create_source_snapshot(repo, first_commit, cache)

    (repo / "tracked.txt").write_text("second commit\n", encoding="utf-8")
    _git(repo, "add", "tracked.txt")
    _git(repo, "commit", "-q", "-m", "second")
    second_commit = _git(repo, "rev-parse", "HEAD")
    second = create_source_snapshot(repo, second_commit, cache)

    assert second.commit_sha == second_commit
    assert second.commit_sha != first.commit_sha
    assert second.sha256 != first.sha256
    assert second.archive_path != first.archive_path


def test_invalid_revision_fails_without_retaining_partial_archive(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    cache = tmp_path / "snapshots"

    with pytest.raises(SourceSnapshotError, match="rev-parse"):
        create_source_snapshot(repo, "does-not-exist", cache)

    assert not cache.exists() or list(cache.iterdir()) == []


def test_corrupt_content_addressed_entry_is_never_silently_reused(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    cache = tmp_path / "snapshots"
    snapshot = create_source_snapshot(repo, "HEAD", cache)
    path = Path(snapshot.archive_path)
    path.write_bytes(b"tampered")

    with pytest.raises(SourceSnapshotError, match="collision or corruption"):
        create_source_snapshot(repo, "HEAD", cache)


def test_verifier_detects_retained_snapshot_tampering(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    snapshot = create_source_snapshot(repo, "HEAD", tmp_path / "snapshots")
    Path(snapshot.archive_path).write_bytes(b"tampered")

    with pytest.raises(SourceSnapshotError, match="integrity mismatch"):
        verify_source_snapshot(snapshot)


@pytest.mark.skipif(__import__("os").name != "posix", reason="POSIX permission contract")
def test_snapshot_cache_and_archive_are_private_on_posix(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    cache = tmp_path / "snapshots"

    snapshot = create_source_snapshot(repo, "HEAD", cache)

    assert cache.stat().st_mode & 0o777 == 0o700
    assert Path(snapshot.archive_path).stat().st_mode & 0o777 == 0o600



def test_revision_that_looks_like_a_git_option_is_refused(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)

    with pytest.raises(SourceSnapshotError, match="invalid Git revision"):
        create_source_snapshot(repo, "--help", tmp_path / "snapshots")


@pytest.mark.skipif(__import__("os").name != "posix", reason="symlink cache attack is POSIX-specific")
def test_symlink_at_content_address_is_refused(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    cache = tmp_path / "snapshots"
    first = create_source_snapshot(repo, "HEAD", cache)
    path = Path(first.archive_path)
    victim = tmp_path / "victim.tar"
    victim.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(victim)

    with pytest.raises(SourceSnapshotError, match="not a regular file"):
        create_source_snapshot(repo, "HEAD", cache)


def test_cli_emits_a_machine_readable_verified_receipt(tmp_path: Path) -> None:
    repo, commit = _repo(tmp_path)
    cache = tmp_path / "snapshots"

    result = CliRunner().invoke(
        app,
        ["source-snapshot", str(repo), "--cache-root", str(cache), "--json"],
    )

    assert result.exit_code == 0, result.output
    document = __import__("json").loads(result.stdout)
    assert document["commit_sha"] == commit
    assert document["sha256"]
    assert document["size_bytes"] > 0
    assert Path(document["archive_path"]).is_file()
