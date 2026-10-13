"""Fusion preserves exact sibling contributions without transferring their acceptance."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
from support import git, make_repo
from typer.testing import CliRunner

from swfactory.campaign_decision import (
    CampaignDecisionError,
    CampaignDecisionManifest,
    DecisionCandidate,
    DecisionSelection,
    write_campaign_decision,
)
from swfactory.campaign_fusion import CampaignFusionError, build_fusion_proposal, verify_fusion_proposal
from swfactory.candidate_evidence import CandidateEvidenceError, build_candidate_evidence_bundle
from swfactory.candidate_worktree import (
    create_candidate_worktree,
    freeze_candidate_worktree,
    remove_candidate_worktree,
)
from swfactory.cli import app
from swfactory.source_snapshot import create_source_snapshot


def siblings(
    tmp_path: Path,
    paths: tuple[str, str] = ("left.py", "right.py"),
    *,
    protected: str = "",
    symlink_last: bool = False,
):
    repo, base = make_repo(
        tmp_path,
        {
            "factory.toml": '[commands]\ntest = "pytest"\n[paths]\nprotected = ' + protected + "\n"
            if protected
            else '[commands]\ntest = "pytest"\n',
            "left.py": "left = False\n",
            "right.py": "right = False\n",
        },
    )
    source = create_source_snapshot(repo, base, tmp_path / "sources")
    candidates = []
    evidence = {}
    for index, path in enumerate(paths):
        candidate_id = f"repair-{index}"
        worktree = create_candidate_worktree(repo, candidate_id, base, root=tmp_path / "worktrees")
        workspace = Path(worktree.path)
        target = workspace / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if symlink_last and index == 1:
            target.unlink(missing_ok=True)
            target.symlink_to("left.py")
        else:
            target.write_text(f"fixed_{index} = True\n", encoding="utf-8")
        git(workspace, "add", "-A")
        git(workspace, "commit", "-qm", candidate_id)
        revision = freeze_candidate_worktree(worktree)
        evidence[candidate_id] = tmp_path / f"evidence-{index}"
        bundle = build_candidate_evidence_bundle(
            repo, revision, source, artifacts={}, destination=evidence[candidate_id]
        )
        candidates.append(
            DecisionCandidate(
                candidate_id, "repair", "ok", base, revision.output_head, revision.ref, bundle.digest(), (), 1, 1
            )
        )
        remove_candidate_worktree(worktree)
    decision = CampaignDecisionManifest(
        "campaign",
        "cell",
        7,
        base,
        tuple(candidates),
        DecisionSelection("repair-0", "search selection", tuple(evidence), ()),
        (),
        False,
    )
    decision_path = tmp_path / "decision.json"
    write_campaign_decision(decision_path, decision)
    return repo, decision, decision_path, evidence


def observations(tmp_path: Path, decision: CampaignDecisionManifest, *, duplicate: bool = True) -> Path:
    first = {
        "evidence_id": "shared-reproducer",
        "producer_id": "diagnostic-without-patch",
        "kind": "reproducer",
        "statement": "Both obligations are required; either partial repair is insufficient.",
        "subject_head": decision.input_head,
        "source_evidence_ids": ["original-failure"],
    }
    rows = [first, {**first, "producer_id": "challenger"}] if duplicate else [first]
    path = tmp_path / "observations.json"
    path.write_text(json.dumps({"campaign_decision_digest": decision.digest(), "observations": rows}))
    return path


def test_composes_exact_files_and_leaves_head_index_refs_and_dirty_working_tree_unchanged(tmp_path: Path) -> None:
    repo, decision, decision_path, evidence = siblings(tmp_path)
    (repo / "left.py").write_text("user edit\n")
    git(repo, "add", "left.py")
    before = (
        git(repo, "rev-parse", "HEAD"),
        git(repo, "write-tree"),
        git(repo, "show-ref"),
        git(repo, "diff", "--cached"),
    )
    destination = tmp_path / "fusion"

    manifest = build_fusion_proposal(repo, decision_path, evidence, list(reversed(evidence)), destination)

    assert manifest["authority"] == "search-only"
    assert manifest["verification"] == "required"
    assert manifest["existing_selection"] == decision.selection.winner
    assert git(repo, "show", f"{manifest['output_tree']}:left.py") == "fixed_0 = True"
    assert git(repo, "show", f"{manifest['output_tree']}:right.py") == "fixed_1 = True"
    assert (repo / "left.py").read_text() == "user edit\n"
    assert before == (
        git(repo, "rev-parse", "HEAD"),
        git(repo, "write-tree"),
        git(repo, "show-ref"),
        git(repo, "diff", "--cached"),
    )
    assert verify_fusion_proposal(repo, decision_path, evidence, destination) == manifest
    other = build_fusion_proposal(repo, decision_path, evidence, list(evidence), tmp_path / "other")
    assert other == manifest


def test_diagnostic_without_patch_is_retained_and_shared_evidence_is_counted_once(tmp_path: Path) -> None:
    repo, decision, decision_path, evidence = siblings(tmp_path)
    witness = observations(tmp_path, decision)
    destination = tmp_path / "fusion"
    manifest = build_fusion_proposal(
        repo, decision_path, evidence, list(evidence), destination, observations_path=witness
    )

    assert len(manifest["observations"]) == 1
    assert manifest["observations"][0]["producer_ids"] == ["challenger", "diagnostic-without-patch"]
    assert manifest["observations"][0]["authority"] == "untrusted-observations"
    assert verify_fusion_proposal(repo, decision_path, evidence, destination, observations_path=witness) == manifest
    with pytest.raises(CampaignFusionError, match="changed"):
        verify_fusion_proposal(repo, decision_path, evidence, destination)


@pytest.mark.parametrize("paths", [("left.py", "left.py"), ("new", "new/child.py")])
def test_overlap_and_file_directory_collisions_require_semantic_synthesis(
    tmp_path: Path, paths: tuple[str, str]
) -> None:
    repo, _, decision_path, evidence = siblings(tmp_path, paths)
    with pytest.raises(CampaignFusionError, match="overlapping"):
        build_fusion_proposal(repo, decision_path, evidence, list(evidence), tmp_path / "fusion")
    assert not (tmp_path / "fusion").exists()


@pytest.mark.parametrize("path", ["factory.toml", ".github/workflows/check.yml", "private/control.py"])
def test_base_contract_and_fixed_authority_paths_are_protected(tmp_path: Path, path: str) -> None:
    repo, _, decision_path, evidence = siblings(tmp_path, ("left.py", path), protected='["private/**"]')
    with pytest.raises(CampaignFusionError, match="protected"):
        build_fusion_proposal(repo, decision_path, evidence, list(evidence), tmp_path / "fusion")


def test_conflicting_evidence_identity_and_cross_campaign_observations_are_refused(tmp_path: Path) -> None:
    repo, decision, decision_path, evidence = siblings(tmp_path)
    witness = observations(tmp_path, decision)
    document = json.loads(witness.read_text())
    document["observations"][1]["statement"] = "A contradictory observation"
    witness.write_text(json.dumps(document))
    with pytest.raises(CampaignFusionError, match="conflicting evidence"):
        build_fusion_proposal(
            repo, decision_path, evidence, list(evidence), tmp_path / "fusion", observations_path=witness
        )
    document["campaign_decision_digest"] = "sha256:" + "0" * 64
    witness.write_text(json.dumps(document))
    with pytest.raises(CampaignFusionError, match="another campaign"):
        build_fusion_proposal(
            repo, decision_path, evidence, list(evidence), tmp_path / "fusion", observations_path=witness
        )


@pytest.mark.parametrize("artifact", ["candidate.diff", "manifest.json"])
def test_tampered_fusion_artifacts_are_refused(tmp_path: Path, artifact: str) -> None:
    repo, _, decision_path, evidence = siblings(tmp_path)
    destination = tmp_path / "fusion"
    build_fusion_proposal(repo, decision_path, evidence, list(evidence), destination)
    path = destination / artifact
    if artifact.endswith("json"):
        document = json.loads(path.read_text())
        document["verification"] = "passed"
        path.write_text(json.dumps(document))
    else:
        path.write_bytes(path.read_bytes() + b"tampered")
    with pytest.raises(CampaignFusionError, match="changed"):
        verify_fusion_proposal(repo, decision_path, evidence, destination)


def test_missing_sibling_evidence_and_changed_contributor_ref_are_refused(tmp_path: Path) -> None:
    repo, decision, decision_path, evidence = siblings(tmp_path)
    with pytest.raises(CampaignDecisionError, match="membership"):
        build_fusion_proposal(
            repo, decision_path, {"repair-0": evidence["repair-0"]}, list(evidence), tmp_path / "fusion"
        )
    git(repo, "update-ref", decision.candidates[0].candidate_ref, decision.input_head)
    with pytest.raises(CandidateEvidenceError, match="ref"):
        build_fusion_proposal(repo, decision_path, evidence, list(evidence), tmp_path / "fusion")


def test_symlink_edits_are_refused(tmp_path: Path) -> None:
    repo, _, decision_path, evidence = siblings(tmp_path, symlink_last=True)
    with pytest.raises(CampaignFusionError, match="symlinks"):
        build_fusion_proposal(repo, decision_path, evidence, list(evidence), tmp_path / "fusion")


@pytest.mark.parametrize("ids", [["repair-0"], ["repair-0", "repair-0"], ["repair-0", "missing"]])
def test_invalid_contributor_selection_is_refused(tmp_path: Path, ids: list[str]) -> None:
    repo, _, decision_path, evidence = siblings(tmp_path)
    with pytest.raises(CampaignFusionError):
        build_fusion_proposal(repo, decision_path, evidence, ids, tmp_path / "fusion")


def test_cancelled_campaigns_and_existing_output_directories_are_refused(tmp_path: Path) -> None:
    repo, decision, decision_path, evidence = siblings(tmp_path)
    destination = tmp_path / "fusion"
    destination.mkdir()
    marker = destination / "user.txt"
    marker.write_text("preserve me")
    with pytest.raises(CampaignFusionError, match="must not exist"):
        build_fusion_proposal(repo, decision_path, evidence, list(evidence), destination)
    assert marker.read_text() == "preserve me"
    write_campaign_decision(decision_path, replace(decision, cancelled=True))
    with pytest.raises(CampaignFusionError, match="cancelled"):
        build_fusion_proposal(repo, decision_path, evidence, list(evidence), tmp_path / "other")


def test_cli_build_and_integrity_verify_explain_fresh_verification_requirement(tmp_path: Path) -> None:
    repo, _, decision_path, evidence = siblings(tmp_path)
    destination = tmp_path / "fusion"
    common = ["--repo", str(repo)]
    for candidate_id, path in evidence.items():
        common.extend(["--candidate-evidence", f"{candidate_id}={path}"])
    runner = CliRunner()
    built = runner.invoke(
        app,
        [
            "campaign-fusion",
            "build",
            str(decision_path),
            str(destination),
            "--include",
            "repair-0",
            "--include",
            "repair-1",
            *common,
        ],
    )
    assert built.exit_code == 0, built.output
    assert "fresh verification required" in built.output
    verified = runner.invoke(app, ["campaign-fusion", "verify", str(decision_path), str(destination), *common])
    assert verified.exit_code == 0, verified.output
    assert "program verification still required" in verified.output
