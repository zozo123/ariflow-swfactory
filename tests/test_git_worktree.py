"""Local candidate fan-out through detached Git worktrees."""

from __future__ import annotations

import subprocess
from pathlib import Path

from swfactory.evolution import (
    CandidateOutcome,
    Strategy,
    evaluation,
    plan_requests,
    run_campaign,
    worktree_candidate_runner,
)
from swfactory.generations import Dimension
from swfactory.git_worktree import (
    CandidateWorktreeError,
    create_candidate_worktree,
    recorded_candidate_head,
    remove_candidate_worktree,
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
    (repo / "value.txt").write_text("base\n", encoding="utf-8")
    _git(repo, "add", "value.txt")
    _git(repo, "commit", "-q", "-m", "base")
    return repo, _git(repo, "rev-parse", "HEAD")


def _passed(request, *, head: str | None = None) -> CandidateOutcome:
    return CandidateOutcome(
        logical_id=request.logical_id,
        strategy=request.strategy,
        state="ok",
        input_head=request.input_head,
        output_head=head,
        evaluations=(
            evaluation(Dimension.CORRECTNESS, passed=True, evidence="tests"),
            evaluation(Dimension.EVIDENCE, passed=True, evidence="receipt"),
        ),
    )


def test_sibling_worktrees_start_from_same_commit_without_sharing_edits(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    root = tmp_path / "worktrees"
    left = create_candidate_worktree(repo, "left", base, root)
    right = create_candidate_worktree(repo, "right", base, root)
    left_path = Path(left.path)
    right_path = Path(right.path)

    (left_path / "value.txt").write_text("left\n", encoding="utf-8")
    _git(left_path, "add", "value.txt")
    _git(left_path, "commit", "-q", "-m", "left")

    assert (right_path / "value.txt").read_text(encoding="utf-8") == "base\n"
    assert recorded_candidate_head(left) != base
    assert recorded_candidate_head(right) == base

    remove_candidate_worktree(repo, left)
    remove_candidate_worktree(repo, right)
    assert not left_path.exists()
    assert not right_path.exists()


def test_workspace_path_is_derived_from_digest_not_candidate_text(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    worktree = create_candidate_worktree(repo, "../../candidate with spaces", base, tmp_path / "worktrees")

    assert worktree.workspace_key in Path(worktree.path).name
    assert "candidate with spaces" not in worktree.path

    remove_candidate_worktree(repo, worktree)


def test_recorded_head_refuses_dirty_or_untracked_state(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    worktree = create_candidate_worktree(repo, "dirty", base, tmp_path / "worktrees")
    path = Path(worktree.path)
    (path / "untracked.txt").write_text("not evidence\n", encoding="utf-8")

    try:
        recorded_candidate_head(worktree)
    except CandidateWorktreeError as error:
        assert "uncommitted state" in str(error)
    else:
        raise AssertionError("dirty candidate workspace was recorded as evidence")

    remove_candidate_worktree(repo, worktree, force=True)


def test_campaign_adapter_records_observed_commit_and_cleans_workspace(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    root = tmp_path / "worktrees"
    requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head=base,
        strategies=(Strategy.REPAIR,),
    )

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("candidate\n", encoding="utf-8")
        _git(workspace, "add", "value.txt")
        _git(workspace, "commit", "-q", "-m", "candidate")
        return _passed(request)

    report = run_campaign(
        worktree_candidate_runner(repo, root, runner),
        requests,
        parallel=False,
        human_approved=True,
    )

    outcome = report.outcomes[0]
    assert outcome.state == "ok"
    assert outcome.output_head
    assert outcome.output_head != base
    assert outcome.workspace_key
    assert report.selection.winner == requests[0].logical_id
    assert list(root.glob("cand-*")) == []


def test_uncommitted_candidate_becomes_failure_and_workspace_is_reclaimed(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    root = tmp_path / "worktrees"
    requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head=base,
        strategies=(Strategy.REPAIR,),
    )

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("dirty\n", encoding="utf-8")
        return _passed(request)

    report = run_campaign(
        worktree_candidate_runner(repo, root, runner),
        requests,
        parallel=False,
        human_approved=True,
    )

    assert report.outcomes[0].state == "failed"
    assert "uncommitted state" in report.outcomes[0].detail
    assert report.selection.winner is None
    assert list(root.glob("cand-*")) == []


def test_candidate_cannot_lie_about_recorded_output_head(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    root = tmp_path / "worktrees"
    requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head=base,
        strategies=(Strategy.REPAIR,),
    )

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("candidate\n", encoding="utf-8")
        _git(workspace, "add", "value.txt")
        _git(workspace, "commit", "-q", "-m", "candidate")
        return _passed(request, head="f" * 40)

    report = run_campaign(
        worktree_candidate_runner(repo, root, runner),
        requests,
        parallel=False,
        human_approved=True,
    )

    assert report.outcomes[0].state == "failed"
    assert "claimed output" in report.outcomes[0].detail
    assert report.selection.winner is None
