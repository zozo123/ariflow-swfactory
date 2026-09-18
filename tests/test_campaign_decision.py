"""Campaign fan-in is digest-bound to every answered candidate's retained evidence."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory.campaign_decision import (
    CampaignDecisionError,
    build_campaign_decision,
    load_campaign_decision,
    verify_campaign_decision,
    write_campaign_decision,
)
from swfactory.candidate_evidence import build_candidate_evidence_bundle
from swfactory.candidate_worktree import (
    create_candidate_worktree,
    freeze_candidate_worktree,
    remove_candidate_worktree,
)
from swfactory.cli import app
from swfactory.evolution import CampaignReport, CandidateOutcome, Selection, Strategy, evaluation
from swfactory.generations import Dimension
from swfactory.source_snapshot import create_source_snapshot

IDENTITY = [
    "-c",
    "user.name=Decision Test",
    "-c",
    "user.email=decision@example.invalid",
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


def answered(
    repo: Path,
    tmp_path: Path,
    *,
    candidate_id: str,
    strategy: Strategy,
    value: str,
    cost: float,
):
    base = git(repo, "rev-parse", "main")
    source = create_source_snapshot(repo, base, tmp_path / "source-cache")
    worktree = create_candidate_worktree(repo, candidate_id, base, root=tmp_path / "worktrees")
    path = Path(worktree.path)
    (path / "value.txt").write_text(value + "\n", encoding="utf-8")
    git(path, "add", "value.txt")
    git(path, "commit", "-qm", candidate_id)
    revision = freeze_candidate_worktree(worktree)
    log = tmp_path / f"{candidate_id}.log"
    log.write_text(f"{candidate_id} completed\n", encoding="utf-8")
    destination = tmp_path / f"{candidate_id}-evidence"
    bundle = build_candidate_evidence_bundle(
        repo,
        revision,
        source,
        artifacts={"agent-log": log},
        destination=destination,
    )
    outcome = CandidateOutcome(
        logical_id=candidate_id,
        strategy=strategy,
        state="ok",
        input_head=base,
        output_head=revision.output_head,
        evaluations=(
            evaluation(Dimension.CORRECTNESS, passed=True, evidence="target test command"),
            evaluation(Dimension.EVIDENCE, passed=True, evidence="candidate evidence bundle"),
        ),
        cost_usd=cost,
        duration_s=1.0,
        candidate_ref=revision.ref,
    )
    remove_candidate_worktree(worktree)
    return outcome, bundle, destination


def campaign(repo: Path, tmp_path: Path):
    first, first_bundle, first_path = answered(
        repo,
        tmp_path,
        candidate_id="cand_repair",
        strategy=Strategy.REPAIR,
        value="repair",
        cost=1.0,
    )
    second, second_bundle, second_path = answered(
        repo,
        tmp_path,
        candidate_id="cand_rethink",
        strategy=Strategy.RETHINK,
        value="rethink",
        cost=2.0,
    )
    report = CampaignReport(
        campaign_id="round-0",
        cell_id="cell-1",
        epoch=7,
        input_head=first.input_head,
        strategies=(Strategy.REPAIR.value, Strategy.RETHINK.value),
        parallel=True,
        outcomes=(first, second),
        selection=Selection(
            winner=first.logical_id,
            reason="promotable:repair",
            ranking=(first.logical_id, second.logical_id),
            refusals=(),
        ),
    )
    bundles = {first.logical_id: first_bundle, second.logical_id: second_bundle}
    paths = {first.logical_id: first_path, second.logical_id: second_path}
    return report, bundles, paths


def test_decision_binds_winner_ranking_evaluations_and_every_answered_bundle(repo: Path, tmp_path: Path) -> None:
    report, bundles, _ = campaign(repo, tmp_path)

    manifest = build_campaign_decision(report, bundles)

    assert manifest.selection.winner == "cand_repair"
    assert tuple(candidate.candidate_id for candidate in manifest.candidates) == (
        "cand_repair",
        "cand_rethink",
    )
    assert all(candidate.evidence_bundle_digest for candidate in manifest.candidates)
    assert manifest.candidates[0].evaluations[0].dimension == "correctness"
    assert manifest.digest().startswith("sha256:")


def test_evidence_mapping_order_does_not_change_decision_digest(repo: Path, tmp_path: Path) -> None:
    report, bundles, _ = campaign(repo, tmp_path)

    forward = build_campaign_decision(report, bundles)
    reverse = build_campaign_decision(report, dict(reversed(tuple(bundles.items()))))

    assert forward.digest() == reverse.digest()


def test_losing_answered_sibling_cannot_disappear_from_fan_in(repo: Path, tmp_path: Path) -> None:
    report, bundles, _ = campaign(repo, tmp_path)
    bundles.pop("cand_rethink")

    with pytest.raises(CampaignDecisionError, match="missing retained evidence"):
        build_campaign_decision(report, bundles)


def test_candidate_cannot_claim_a_siblings_evidence_bundle(repo: Path, tmp_path: Path) -> None:
    report, bundles, _ = campaign(repo, tmp_path)
    bundles["cand_repair"] = bundles["cand_rethink"]

    with pytest.raises(CampaignDecisionError, match="bundle belongs"):
        build_campaign_decision(report, bundles)


def test_manifest_tampering_is_detected(repo: Path, tmp_path: Path) -> None:
    report, bundles, _ = campaign(repo, tmp_path)
    path = tmp_path / "decision.json"
    write_campaign_decision(path, build_campaign_decision(report, bundles))
    document = json.loads(path.read_text(encoding="utf-8"))
    document["selection"]["winner"] = "cand_rethink"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(CampaignDecisionError, match="manifest digest mismatch"):
        load_campaign_decision(path)


def test_verification_rehashes_bound_candidate_evidence(repo: Path, tmp_path: Path) -> None:
    report, bundles, paths = campaign(repo, tmp_path)
    path = tmp_path / "decision.json"
    write_campaign_decision(path, build_campaign_decision(report, bundles))
    evidence = paths["cand_rethink"]
    retained = next((evidence / "artifacts").iterdir())
    retained.write_text("tampered\n", encoding="utf-8")

    with pytest.raises(Exception, match="changed"):
        verify_campaign_decision(path, paths, repo=repo)


def test_frozen_ref_drift_invalidates_campaign_decision(repo: Path, tmp_path: Path) -> None:
    report, bundles, paths = campaign(repo, tmp_path)
    path = tmp_path / "decision.json"
    manifest = build_campaign_decision(report, bundles)
    write_campaign_decision(path, manifest)
    loser = next(candidate for candidate in manifest.candidates if candidate.candidate_id == "cand_rethink")
    git(repo, "update-ref", loser.candidate_ref, report.input_head)

    with pytest.raises(Exception, match="candidate ref"):
        verify_campaign_decision(path, paths, repo=repo)


def test_cli_builds_and_verifies_campaign_fan_in(repo: Path, tmp_path: Path) -> None:
    report, _, paths = campaign(repo, tmp_path)
    report_path = tmp_path / "campaign.json"
    report_path.write_text(json.dumps(report.to_dict()), encoding="utf-8")
    decision_path = tmp_path / "decision.json"
    evidence_args = []
    for candidate_id, path in paths.items():
        evidence_args += ["--candidate-evidence", f"{candidate_id}={path}"]

    built = CliRunner().invoke(
        app,
        [
            "campaign-decision",
            "build",
            str(report_path),
            str(decision_path),
            "--repo",
            str(repo),
            *evidence_args,
        ],
    )

    assert built.exit_code == 0, built.output
    verified = CliRunner().invoke(
        app,
        [
            "campaign-decision",
            "verify",
            str(decision_path),
            "--repo",
            str(repo),
            *evidence_args,
            "--json",
        ],
    )
    assert verified.exit_code == 0, verified.output
    document = json.loads(verified.stdout)
    assert document["selection"]["winner"] == "cand_repair"
    assert document["manifest_digest"].startswith("sha256:")
