"""Promotion-window retention keeps candidate evidence available until it is safe to collect."""

from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from swfactory.candidate_evidence import build_candidate_evidence_bundle
from swfactory.candidate_retention import (
    CandidateRetentionError,
    pin_candidate_evidence,
    retain_candidate_evidence,
    sweep_candidate_evidence,
    unpin_candidate_evidence,
    verify_retained_candidate_evidence,
)
from swfactory.candidate_worktree import (
    create_candidate_worktree,
    freeze_candidate_worktree,
    remove_candidate_worktree,
)
from swfactory.source_snapshot import create_source_snapshot

IDENTITY = [
    "-c",
    "user.name=Retention Test",
    "-c",
    "user.email=retention@example.invalid",
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
    (root / "value.txt").write_text("base\n", encoding="utf-8")
    git(root, "add", "value.txt")
    git(root, "commit", "-qm", "base")
    return root


def candidate_bundle(repo: Path, tmp_path: Path) -> tuple[Path, object]:
    base = git(repo, "rev-parse", "HEAD")
    source = create_source_snapshot(repo, base, tmp_path / "source-cache")
    worktree = create_candidate_worktree(repo, "retained-candidate", base, root=tmp_path / "worktrees")
    path = Path(worktree.path)
    (path / "value.txt").write_text("answer\n", encoding="utf-8")
    git(path, "add", "value.txt")
    git(path, "commit", "-qm", "answer")
    revision = freeze_candidate_worktree(worktree)
    log = tmp_path / "agent.log"
    log.write_text("passed\n", encoding="utf-8")
    bundle = tmp_path / "candidate-bundle"
    build_candidate_evidence_bundle(
        repo,
        revision,
        source,
        artifacts={"agent-log": log},
        destination=bundle,
    )
    remove_candidate_worktree(worktree)
    return bundle, revision


def test_retention_import_is_content_addressed_and_verifiable(repo: Path, tmp_path: Path) -> None:
    bundle, revision = candidate_bundle(repo, tmp_path)
    store = tmp_path / "retention"
    now = datetime(2026, 9, 18, 1, 0, tzinfo=UTC)

    lease = retain_candidate_evidence(
        bundle,
        repo=repo,
        store=store,
        ttl=timedelta(days=7),
        now=now,
    )
    retained = store / "objects" / lease.digest

    assert retained.is_dir()
    assert lease.output_head == revision.output_head
    assert lease.expires_at == "2026-09-25T01:00:00Z"
    assert verify_retained_candidate_evidence(store, lease.digest, repo=repo).output_head == revision.output_head


def test_sweep_removes_only_after_expiry(repo: Path, tmp_path: Path) -> None:
    bundle, _ = candidate_bundle(repo, tmp_path)
    store = tmp_path / "retention"
    now = datetime(2026, 9, 18, 1, 0, tzinfo=UTC)
    lease = retain_candidate_evidence(
        bundle,
        repo=repo,
        store=store,
        ttl=timedelta(hours=2),
        now=now,
    )

    early = sweep_candidate_evidence(store, now=now + timedelta(minutes=30))
    assert early.retained == (lease.digest,)
    assert (store / "objects" / lease.digest).is_dir()

    expired = sweep_candidate_evidence(store, now=now + timedelta(hours=2))
    assert expired.removed == (lease.digest,)
    assert not (store / "objects" / lease.digest).exists()
    assert not (store / "leases" / f"{lease.digest}.json").exists()


def test_pin_survives_expiry_until_unpinned(repo: Path, tmp_path: Path) -> None:
    bundle, _ = candidate_bundle(repo, tmp_path)
    store = tmp_path / "retention"
    now = datetime(2026, 9, 18, 1, 0, tzinfo=UTC)
    lease = retain_candidate_evidence(
        bundle,
        repo=repo,
        store=store,
        ttl=timedelta(minutes=5),
        now=now,
    )

    assert pin_candidate_evidence(store, lease.digest).pinned is True
    report = sweep_candidate_evidence(store, now=now + timedelta(days=1))
    assert report.retained == (lease.digest,)
    assert (store / "objects" / lease.digest).is_dir()

    assert unpin_candidate_evidence(store, lease.digest).pinned is False
    report = sweep_candidate_evidence(store, now=now + timedelta(days=1))
    assert report.removed == (lease.digest,)


def test_refresh_extends_but_never_shortens_lease(repo: Path, tmp_path: Path) -> None:
    bundle, _ = candidate_bundle(repo, tmp_path)
    store = tmp_path / "retention"
    now = datetime(2026, 9, 18, 1, 0, tzinfo=UTC)
    first = retain_candidate_evidence(
        bundle,
        repo=repo,
        store=store,
        ttl=timedelta(days=7),
        now=now,
    )
    shorter = retain_candidate_evidence(
        bundle,
        repo=repo,
        store=store,
        ttl=timedelta(minutes=1),
        now=now + timedelta(hours=1),
    )
    longer = retain_candidate_evidence(
        bundle,
        repo=repo,
        store=store,
        ttl=timedelta(days=14),
        now=now + timedelta(hours=1),
    )

    assert shorter.expires_at == first.expires_at
    assert longer.expires_at == "2026-10-02T02:00:00Z"
    assert longer.retained_at == first.retained_at


def test_malformed_lease_is_reported_not_deleted(repo: Path, tmp_path: Path) -> None:
    bundle, _ = candidate_bundle(repo, tmp_path)
    store = tmp_path / "retention"
    now = datetime(2026, 9, 18, 1, 0, tzinfo=UTC)
    lease = retain_candidate_evidence(
        bundle,
        repo=repo,
        store=store,
        ttl=timedelta(minutes=1),
        now=now,
    )
    lease_path = store / "leases" / f"{lease.digest}.json"
    lease_path.write_text("{broken", encoding="utf-8")

    report = sweep_candidate_evidence(store, now=now + timedelta(days=1))

    assert report.malformed == (lease.digest,)
    assert (store / "objects" / lease.digest).is_dir()


@pytest.mark.skipif(os.name != "posix", reason="symlink redirection test is POSIX-specific")
def test_redirected_object_is_reported_not_followed(repo: Path, tmp_path: Path) -> None:
    bundle, _ = candidate_bundle(repo, tmp_path)
    store = tmp_path / "retention"
    now = datetime(2026, 9, 18, 1, 0, tzinfo=UTC)
    lease = retain_candidate_evidence(
        bundle,
        repo=repo,
        store=store,
        ttl=timedelta(minutes=1),
        now=now,
    )
    target = tmp_path / "must-survive"
    target.mkdir()
    (target / "important.txt").write_text("keep\n", encoding="utf-8")
    retained = store / "objects" / lease.digest
    retained.rename(tmp_path / "original-retained")
    retained.symlink_to(target, target_is_directory=True)

    report = sweep_candidate_evidence(store, now=now + timedelta(days=1))

    assert report.malformed == (lease.digest,)
    assert (target / "important.txt").read_text(encoding="utf-8") == "keep\n"


def test_naive_retention_time_is_refused(repo: Path, tmp_path: Path) -> None:
    bundle, _ = candidate_bundle(repo, tmp_path)

    with pytest.raises(CandidateRetentionError, match="timezone-aware"):
        retain_candidate_evidence(
            bundle,
            repo=repo,
            store=tmp_path / "retention",
            ttl=timedelta(days=1),
            now=datetime(2026, 9, 18, 1, 0),
        )


def test_retention_lease_is_plain_inspectable_json(repo: Path, tmp_path: Path) -> None:
    bundle, _ = candidate_bundle(repo, tmp_path)
    store = tmp_path / "retention"
    lease = retain_candidate_evidence(
        bundle,
        repo=repo,
        store=store,
        ttl=timedelta(days=1),
        now=datetime(2026, 9, 18, 1, 0, tzinfo=UTC),
    )

    document = json.loads((store / "leases" / f"{lease.digest}.json").read_text(encoding="utf-8"))
    assert document["candidate_id"] == "retained-candidate"
    assert document["pinned"] is False
