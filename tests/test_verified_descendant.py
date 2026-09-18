"""Verified evidence-backed parent to child experiment descent."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from swfactory.campaign_decision import CampaignDecisionError, plan_descendant_campaign
from swfactory.candidate_evidence import verify_candidate_evidence_bundle
from swfactory.evolution import (
    CandidateOutcome,
    Strategy,
    evaluation,
    plan_requests,
    run_campaign,
    worktree_candidate_runner,
)
from swfactory.generations import CampaignBudget, Dimension


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _successful_report(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Factory Test")
    _git(repo, "config", "user.email", "factory@example.test")
    (repo / "value.txt").write_text("base\n", encoding="utf-8")
    _git(repo, "add", "value.txt")
    _git(repo, "commit", "-q", "-m", "base")
    base = _git(repo, "rev-parse", "HEAD")
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
        return CandidateOutcome(
            logical_id=request.logical_id,
            strategy=request.strategy,
            state="ok",
            input_head=request.input_head,
            evaluations=(
                evaluation(Dimension.CORRECTNESS, passed=True, evidence="tests"),
                evaluation(Dimension.EVIDENCE, passed=True, evidence="receipt"),
            ),
        )

    report = run_campaign(
        worktree_candidate_runner(repo, tmp_path / "worktrees", runner),
        requests,
        parallel=False,
        human_approved=False,
    )
    return repo, report


def test_descendant_binds_to_verified_exploration_decision(tmp_path: Path) -> None:
    repo, report = _successful_report(tmp_path)
    winner = report.outcomes[0]
    assert report.selection.winner is None
    assert report.exploration_selection.winner == winner.logical_id
    assert winner.output_head
    assert winner.evidence_digest

    plan = plan_descendant_campaign(
        report,
        repo=repo,
        campaign_id="round-1",
        strategies=(Strategy.RETHINK, Strategy.SCRATCH),
    )

    assert plan.parent_candidate == winner.logical_id
    assert plan.parent_evidence_digest == winner.evidence_digest
    assert plan.input_head == winner.output_head
    assert plan.depth == 1
    assert plan.parent_decision_digest.startswith("sha256:")
    assert all(request.input_head == winner.output_head for request in plan.requests)
    assert all(request.parent_candidate == winner.logical_id for request in plan.requests)
    assert all(
        request.parent_decision_digest == plan.parent_decision_digest for request in plan.requests
    )


def test_descendant_refuses_tampered_sibling_evidence(tmp_path: Path) -> None:
    repo, report = _successful_report(tmp_path)
    outcome = report.outcomes[0]
    assert outcome.evidence_bundle_path
    bundle = verify_candidate_evidence_bundle(Path(outcome.evidence_bundle_path), repo=repo)
    retained = Path(outcome.evidence_bundle_path) / bundle.diff.path
    retained.write_bytes(retained.read_bytes() + b"tamper")

    with pytest.raises(CampaignDecisionError, match="candidate evidence verification failed"):
        plan_descendant_campaign(report, repo=repo, campaign_id="round-1")


def test_descendant_refuses_tree_selection_drift(tmp_path: Path) -> None:
    from dataclasses import replace

    repo, report = _successful_report(tmp_path)
    assert report.experiment_round is not None
    report.experiment_round = replace(report.experiment_round, winner_id=None)

    with pytest.raises(CampaignDecisionError, match="experiment-tree winner differs"):
        plan_descendant_campaign(report, repo=repo, campaign_id="round-1")


def test_descendant_preserves_depth_budget(tmp_path: Path) -> None:
    repo, report = _successful_report(tmp_path)

    with pytest.raises(CampaignDecisionError, match="descendant campaign is inadmissible"):
        plan_descendant_campaign(
            report,
            repo=repo,
            campaign_id="round-1",
            strategies=(Strategy.REPAIR,),
            budget=CampaignBudget(max_depth=0),
        )


def test_child_identity_includes_parent_decision_digest() -> None:
    kwargs = {
        "campaign_id": "round-1",
        "cell_id": "cell",
        "epoch": 1,
        "input_head": "b" * 40,
        "strategies": (Strategy.REPAIR,),
        "parent_candidate": "cand-parent",
        "depth": 1,
    }
    first = plan_requests(parent_decision_digest="sha256:" + "1" * 64, **kwargs)[0]
    second = plan_requests(parent_decision_digest="sha256:" + "2" * 64, **kwargs)[0]

    assert first.logical_id != second.logical_id


def test_invalid_parent_decision_digest_is_refused() -> None:
    with pytest.raises(Exception, match="canonical sha256"):
        plan_requests(
            campaign_id="round-1",
            cell_id="cell",
            epoch=1,
            input_head="b" * 40,
            strategies=(Strategy.REPAIR,),
            parent_candidate="cand-parent",
            parent_decision_digest="not-a-digest",
            depth=1,
        )
