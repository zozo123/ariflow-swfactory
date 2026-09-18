"""Campaign adapter over immutable candidate worktree revisions."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from swfactory.candidate_evidence import verify_candidate_evidence_bundle
from swfactory.evolution import (
    CandidateOutcome,
    Strategy,
    evaluation,
    plan_requests,
    run_campaign,
    worktree_candidate_runner,
)
from swfactory.generations import Dimension


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


def _request(base: str):
    return plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head=base,
        strategies=(Strategy.REPAIR,),
    )


def _passing(request, head: str | None = None) -> CandidateOutcome:
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


def test_adapter_freezes_exact_candidate_revision_then_removes_workspace(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    root = tmp_path / "worktrees"
    requests = _request(base)

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("candidate\n", encoding="utf-8")
        _git(workspace, "add", "value.txt")
        _git(workspace, "commit", "-q", "-m", "candidate")
        return _passing(request)

    report = run_campaign(
        worktree_candidate_runner(repo, root, runner),
        requests,
        parallel=False,
        human_approved=True,
    )

    outcome = report.outcomes[0]
    assert outcome.state == "ok"
    assert outcome.output_head and outcome.output_head != base
    assert outcome.candidate_ref
    assert outcome.evidence_bundle_path
    assert outcome.evidence_digest
    assert _git(repo, "rev-parse", outcome.candidate_ref) == outcome.output_head
    bundle = verify_candidate_evidence_bundle(Path(outcome.evidence_bundle_path), repo=repo)
    assert bundle.digest() == outcome.evidence_digest
    assert report.selection.winner == requests[0].logical_id
    assert not any(root.iterdir())


def test_dirty_success_is_failed_instead_of_becoming_candidate_evidence(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    root = tmp_path / "worktrees"
    requests = _request(base)

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("dirty\n", encoding="utf-8")
        return _passing(request)

    report = run_campaign(
        worktree_candidate_runner(repo, root, runner),
        requests,
        parallel=False,
        human_approved=True,
    )

    assert report.outcomes[0].state == "failed"
    assert "uncommitted" in report.outcomes[0].detail
    assert report.outcomes[0].candidate_ref is None
    assert report.selection.winner is None
    assert not any(root.iterdir())


def test_failed_runner_does_not_freeze_candidate_ref(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    root = tmp_path / "worktrees"
    requests = _request(base)

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "debug.txt").write_text("partial\n", encoding="utf-8")
        return CandidateOutcome(
            request.logical_id,
            request.strategy,
            "failed",
            request.input_head,
            detail="agent failed",
        )

    report = run_campaign(
        worktree_candidate_runner(repo, root, runner),
        requests,
        parallel=False,
        human_approved=True,
    )

    outcome = report.outcomes[0]
    assert outcome.state == "failed"
    assert outcome.candidate_ref is None
    assert "refs/swfactory/candidates/" not in _git(repo, "show-ref")
    assert not any(root.iterdir())


def test_worker_cannot_lie_about_frozen_output_sha(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    root = tmp_path / "worktrees"
    requests = _request(base)

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("candidate\n", encoding="utf-8")
        _git(workspace, "add", "value.txt")
        _git(workspace, "commit", "-q", "-m", "candidate")
        return _passing(request, "f" * 40)

    report = run_campaign(
        worktree_candidate_runner(repo, root, runner),
        requests,
        parallel=False,
        human_approved=True,
    )

    assert report.outcomes[0].state == "failed"
    assert "claimed output" in report.outcomes[0].detail
    assert report.selection.winner is None


def test_experiment_node_retains_frozen_candidate_ref_as_evidence(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    requests = _request(base)

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("candidate\n", encoding="utf-8")
        _git(workspace, "add", "value.txt")
        _git(workspace, "commit", "-q", "-m", "candidate")
        return _passing(request)

    report = run_campaign(
        worktree_candidate_runner(repo, tmp_path / "worktrees", runner),
        requests,
        parallel=False,
        human_approved=True,
    )

    node = report.experiment_round.nodes[0]
    assert any(item.startswith("candidate-ref:refs/swfactory/candidates/") for item in node.evidence)


def test_adapter_retains_workspace_artifacts_before_cleanup(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    root = tmp_path / "worktrees"
    requests = _request(base)

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("candidate\n", encoding="utf-8")
        (workspace / "agent.log").write_text("proof from disposable workspace\n", encoding="utf-8")
        _git(workspace, "add", "value.txt", "agent.log")
        _git(workspace, "commit", "-q", "-m", "candidate")
        return _passing(request)

    def artifacts(_request, workspace: Path, _outcome: CandidateOutcome):
        return {"agent-log": workspace / "agent.log"}

    report = run_campaign(
        worktree_candidate_runner(repo, root, runner, artifact_collector=artifacts),
        requests,
        parallel=False,
        human_approved=True,
    )

    outcome = report.outcomes[0]
    assert outcome.evidence_bundle_path
    bundle = verify_candidate_evidence_bundle(Path(outcome.evidence_bundle_path), repo=repo)
    retained = Path(outcome.evidence_bundle_path) / bundle.artifacts[0].path
    assert retained.read_text(encoding="utf-8") == "proof from disposable workspace\n"
    assert not any(root.iterdir())


def test_evidence_capture_failure_refuses_but_retains_frozen_revision(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    root = tmp_path / "worktrees"
    requests = _request(base)

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("candidate\n", encoding="utf-8")
        _git(workspace, "add", "value.txt")
        _git(workspace, "commit", "-q", "-m", "candidate")
        return _passing(request)

    def broken_artifacts(_request, _workspace: Path, _outcome: CandidateOutcome):
        raise RuntimeError("artifact collector unavailable")

    report = run_campaign(
        worktree_candidate_runner(repo, root, runner, artifact_collector=broken_artifacts),
        requests,
        parallel=False,
        human_approved=True,
    )

    outcome = report.outcomes[0]
    assert outcome.state == "refused"
    assert outcome.candidate_ref
    assert outcome.output_head
    assert _git(repo, "rev-parse", outcome.candidate_ref) == outcome.output_head
    assert outcome.evidence_digest is None
    assert "evidence capture failed" in outcome.detail
    assert report.selection.winner is None
    assert not any(root.iterdir())


def test_frozen_candidate_without_bundle_cannot_be_selected() -> None:
    request = _request("a" * 40)[0]
    outcome = CandidateOutcome(
        logical_id=request.logical_id,
        strategy=request.strategy,
        state="ok",
        input_head=request.input_head,
        output_head="b" * 40,
        candidate_ref="refs/swfactory/candidates/" + "c" * 24,
        evaluations=(
            evaluation(Dimension.CORRECTNESS, passed=True, evidence="tests"),
            evaluation(Dimension.EVIDENCE, passed=True, evidence="claimed evidence"),
        ),
    )

    report = run_campaign(lambda _request: outcome, (request,), parallel=False, human_approved=True)

    assert report.selection.winner is None
    assert any("missing_candidate_evidence" in item for item in report.selection.refusals)


def test_experiment_node_retains_candidate_evidence_digest(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    requests = _request(base)

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("candidate\n", encoding="utf-8")
        _git(workspace, "add", "value.txt")
        _git(workspace, "commit", "-q", "-m", "candidate")
        return _passing(request)

    report = run_campaign(
        worktree_candidate_runner(repo, tmp_path / "worktrees", runner),
        requests,
        parallel=False,
        human_approved=True,
    )

    outcome = report.outcomes[0]
    assert outcome.evidence_digest
    node = report.experiment_round.nodes[0]
    assert f"candidate-evidence:{outcome.evidence_digest}" in node.evidence


def test_campaign_can_require_inherited_input_recipe(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    recipe_dir = repo / ".swfactory"
    recipe_dir.mkdir()
    (recipe_dir / "candidate-run.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "argv": ["uv", "run", "pytest", "-q"],
                "cwd": ".",
                "timeout_s": 900,
                "resources": {"cpus": 2, "memory_mb": 4096},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "trusted candidate recipe")
    base = _git(repo, "rev-parse", "HEAD")
    requests = _request(base)

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("candidate\n", encoding="utf-8")
        _git(workspace, "add", "value.txt")
        _git(workspace, "commit", "-q", "-m", "candidate")
        return _passing(request)

    report = run_campaign(
        worktree_candidate_runner(
            repo,
            tmp_path / "worktrees",
            runner,
            inherited_recipe_path=".swfactory/candidate-run.json",
        ),
        requests,
        parallel=False,
        human_approved=True,
    )

    outcome = report.outcomes[0]
    assert outcome.state == "ok"
    assert outcome.inherited_recipe_digest
    assert outcome.evidence_bundle_path
    bundle = verify_candidate_evidence_bundle(Path(outcome.evidence_bundle_path), repo=repo)
    assert bundle.inherited_recipe_sha256 == outcome.inherited_recipe_digest
    assert bundle.inherited_recipe_commit_sha == base
    node = report.experiment_round.nodes[0]
    assert f"inherited-recipe:{outcome.inherited_recipe_digest}" in node.evidence


def test_required_inherited_recipe_missing_refuses_candidate(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    requests = _request(base)

    def runner(request, workspace: Path) -> CandidateOutcome:
        (workspace / "value.txt").write_text("candidate\n", encoding="utf-8")
        _git(workspace, "add", "value.txt")
        _git(workspace, "commit", "-q", "-m", "candidate")
        return _passing(request)

    report = run_campaign(
        worktree_candidate_runner(
            repo,
            tmp_path / "worktrees",
            runner,
            inherited_recipe_path=".swfactory/candidate-run.json",
        ),
        requests,
        parallel=False,
        human_approved=True,
    )

    outcome = report.outcomes[0]
    assert outcome.state == "refused"
    assert outcome.candidate_ref
    assert outcome.inherited_recipe_digest is None
    assert "ExecutionRecipeError" in outcome.detail
    assert ".swfactory/candidate-run.json" in outcome.detail
    assert report.selection.winner is None
