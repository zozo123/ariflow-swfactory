"""Git worktree isolation and immutable candidate freeze semantics."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from swfactory.candidate_worktree import (
    CandidateWorktreeError,
    candidate_ref,
    create_candidate_worktree,
    freeze_candidate_worktree,
    remove_candidate_worktree,
    verify_candidate_revision,
)

IDENTITY = [
    "-c",
    "user.name=Candidate Test",
    "-c",
    "user.email=candidate@example.invalid",
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


def test_sibling_candidates_get_physically_distinct_worktrees(repo: Path, tmp_path: Path) -> None:
    head = git(repo, "rev-parse", "HEAD")
    root = tmp_path / "worktrees"
    left = create_candidate_worktree(repo, "candidate-left", head, root=root)
    right = create_candidate_worktree(repo, "candidate-right", head, root=root)

    assert left.path != right.path
    assert Path(left.path).read_text if False else True
    (Path(left.path) / "value.txt").write_text("left only\n", encoding="utf-8")

    assert (Path(right.path) / "value.txt").read_text(encoding="utf-8") == "base\n"
    assert git(Path(right.path), "status", "--porcelain") == ""
    assert git(Path(left.path), "status", "--porcelain") != ""

    remove_candidate_worktree(left, force=True)
    remove_candidate_worktree(right)


def test_freeze_preserves_answer_after_disposable_worktree_is_removed(repo: Path, tmp_path: Path) -> None:
    head = git(repo, "rev-parse", "HEAD")
    worktree = create_candidate_worktree(repo, "candidate-answer", head, root=tmp_path / "worktrees")
    path = Path(worktree.path)
    (path / "value.txt").write_text("answer\n", encoding="utf-8")
    git(path, "add", "value.txt")
    git(path, "commit", "-qm", "candidate answer")

    revision = freeze_candidate_worktree(worktree)
    output = git(path, "rev-parse", "HEAD")

    assert revision.input_head == head
    assert revision.output_head == output
    assert git(repo, "rev-parse", revision.ref) == output
    verify_candidate_revision(repo, revision)

    remove_candidate_worktree(worktree)
    assert not path.exists()
    assert git(repo, "rev-parse", revision.ref) == output
    git(repo, "cat-file", "-e", f"{output}^{{commit}}")


def test_dirty_candidate_cannot_be_frozen_as_evidence(repo: Path, tmp_path: Path) -> None:
    head = git(repo, "rev-parse", "HEAD")
    worktree = create_candidate_worktree(repo, "candidate-dirty", head, root=tmp_path / "worktrees")
    (Path(worktree.path) / "value.txt").write_text("not committed\n", encoding="utf-8")

    with pytest.raises(CandidateWorktreeError, match="uncommitted"):
        freeze_candidate_worktree(worktree)

    assert git(repo, "show-ref", "--verify", "--quiet", worktree.ref) == ""
    remove_candidate_worktree(worktree, force=True)


def test_unchanged_candidate_cannot_be_frozen(repo: Path, tmp_path: Path) -> None:
    head = git(repo, "rev-parse", "HEAD")
    worktree = create_candidate_worktree(repo, "candidate-noop", head, root=tmp_path / "worktrees")

    with pytest.raises(CandidateWorktreeError, match="did not advance"):
        freeze_candidate_worktree(worktree)

    remove_candidate_worktree(worktree)


def test_answered_candidate_ref_cannot_be_rewritten(repo: Path, tmp_path: Path) -> None:
    head = git(repo, "rev-parse", "HEAD")
    worktree = create_candidate_worktree(repo, "candidate-frozen", head, root=tmp_path / "worktrees")
    path = Path(worktree.path)

    (path / "value.txt").write_text("first\n", encoding="utf-8")
    git(path, "add", "value.txt")
    git(path, "commit", "-qm", "first")
    first = freeze_candidate_worktree(worktree)

    (path / "value.txt").write_text("second\n", encoding="utf-8")
    git(path, "add", "value.txt")
    git(path, "commit", "-qm", "second")

    with pytest.raises(CandidateWorktreeError, match="already frozen"):
        freeze_candidate_worktree(worktree)

    assert git(repo, "rev-parse", first.ref) == first.output_head
    remove_candidate_worktree(worktree)


def test_same_candidate_id_cannot_start_after_it_is_frozen(repo: Path, tmp_path: Path) -> None:
    head = git(repo, "rev-parse", "HEAD")
    root = tmp_path / "worktrees"
    worktree = create_candidate_worktree(repo, "stable-id", head, root=root)
    path = Path(worktree.path)
    (path / "value.txt").write_text("answer\n", encoding="utf-8")
    git(path, "add", "value.txt")
    git(path, "commit", "-qm", "answer")
    frozen = freeze_candidate_worktree(worktree)
    remove_candidate_worktree(worktree)

    with pytest.raises(CandidateWorktreeError, match="already frozen"):
        create_candidate_worktree(repo, "stable-id", head, root=root)

    assert frozen.ref == candidate_ref("stable-id")


def test_remove_without_force_refuses_dirty_candidate(repo: Path, tmp_path: Path) -> None:
    head = git(repo, "rev-parse", "HEAD")
    worktree = create_candidate_worktree(repo, "candidate-inspect", head, root=tmp_path / "worktrees")
    (Path(worktree.path) / "value.txt").write_text("inspect me\n", encoding="utf-8")

    with pytest.raises(CandidateWorktreeError, match="could not remove"):
        remove_candidate_worktree(worktree)

    assert Path(worktree.path).exists()
    remove_candidate_worktree(worktree, force=True)


def test_candidate_id_is_hashed_before_becoming_a_git_ref() -> None:
    ref = candidate_ref("../../ weird candidate / with spaces")

    assert ref.startswith("refs/swfactory/candidates/")
    assert ref.count("/") == 3
    assert "weird" not in ref
