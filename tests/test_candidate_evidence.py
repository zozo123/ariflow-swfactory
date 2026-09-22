"""Candidate-local evidence binds source, diff, artifacts, and frozen output."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory.candidate_evidence import (
    CandidateEvidenceError,
    build_candidate_evidence_bundle,
    verify_candidate_evidence_bundle,
)
from swfactory.candidate_worktree import (
    create_candidate_worktree,
    freeze_candidate_worktree,
    remove_candidate_worktree,
)
from swfactory.cli import app
from swfactory.execution_recipe import load_execution_recipe
from swfactory.source_snapshot import create_source_snapshot

IDENTITY = [
    "-c",
    "user.name=Evidence Test",
    "-c",
    "user.email=evidence@example.invalid",
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


def reseal_manifest(path: Path) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    canonical = {key: value for key, value in document.items() if key != "manifest_digest"}
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    document["manifest_digest"] = "sha256:" + hashlib.sha256(payload).hexdigest()
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def answered_candidate(repo: Path, tmp_path: Path):
    base = git(repo, "rev-parse", "HEAD")
    source = create_source_snapshot(repo, base, tmp_path / "source-cache")
    worktree = create_candidate_worktree(repo, "candidate-evidence", base, root=tmp_path / "worktrees")
    path = Path(worktree.path)
    (path / "value.txt").write_text("answer\n", encoding="utf-8")
    git(path, "add", "value.txt")
    git(path, "commit", "-qm", "answer")
    revision = freeze_candidate_worktree(worktree)
    return source, worktree, revision


def test_bundle_retains_exact_diff_and_named_artifacts(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    log = tmp_path / "agent.log"
    log.write_text("candidate completed\n", encoding="utf-8")
    destination = tmp_path / "bundle"

    search_provenance = "sha256:" + "a" * 64
    bundle = build_candidate_evidence_bundle(
        repo,
        revision,
        source,
        artifacts={"agent-log": log},
        destination=destination,
        search_provenance_digest=search_provenance,
    )
    verified = verify_candidate_evidence_bundle(destination, repo=repo)

    assert verified == bundle
    assert (destination / bundle.diff.path).read_text(encoding="utf-8").find("+answer") >= 0
    retained = destination / bundle.artifacts[0].path
    assert retained.read_text(encoding="utf-8") == "candidate completed\n"
    result = (destination / "RESULT.md").read_text(encoding="utf-8")
    assert revision.output_head in result
    assert bundle.digest() in result
    assert bundle.search_provenance_digest == search_provenance
    manifest = json.loads((destination / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["search_provenance_digest"] == search_provenance

    remove_candidate_worktree(worktree)


def test_tampered_retained_artifact_fails_verification(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    log = tmp_path / "agent.log"
    log.write_text("good\n", encoding="utf-8")
    destination = tmp_path / "bundle"
    bundle = build_candidate_evidence_bundle(
        repo,
        revision,
        source,
        artifacts={"agent-log": log},
        destination=destination,
    )

    (destination / bundle.artifacts[0].path).write_text("tampered\n", encoding="utf-8")

    with pytest.raises(CandidateEvidenceError, match="changed"):
        verify_candidate_evidence_bundle(destination, repo=repo)

    remove_candidate_worktree(worktree)


def test_tampered_diff_fails_verification(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    destination = tmp_path / "bundle"
    bundle = build_candidate_evidence_bundle(repo, revision, source, artifacts={}, destination=destination)

    (destination / bundle.diff.path).write_bytes(b"fake patch")

    with pytest.raises(CandidateEvidenceError, match="git-diff.*changed"):
        verify_candidate_evidence_bundle(destination, repo=repo)

    remove_candidate_worktree(worktree)


def test_source_snapshot_must_match_candidate_input(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    wrong_source = create_source_snapshot(repo, revision.output_head, tmp_path / "wrong-cache")

    with pytest.raises(CandidateEvidenceError, match="source snapshot commit"):
        build_candidate_evidence_bundle(
            repo,
            revision,
            wrong_source,
            artifacts={},
            destination=tmp_path / "bundle",
        )

    assert source.commit_sha == revision.input_head
    remove_candidate_worktree(worktree)


def test_frozen_ref_drift_invalidates_bundle(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    destination = tmp_path / "bundle"
    build_candidate_evidence_bundle(repo, revision, source, artifacts={}, destination=destination)
    git(repo, "update-ref", revision.ref, revision.input_head)

    with pytest.raises(CandidateEvidenceError, match="candidate ref verification failed"):
        verify_candidate_evidence_bundle(destination, repo=repo)

    remove_candidate_worktree(worktree)


def test_nonempty_destination_is_refused(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    destination = tmp_path / "bundle"
    destination.mkdir()
    (destination / "old.txt").write_text("old\n", encoding="utf-8")

    with pytest.raises(CandidateEvidenceError, match="destination is not empty"):
        build_candidate_evidence_bundle(repo, revision, source, artifacts={}, destination=destination)

    remove_candidate_worktree(worktree)


@pytest.mark.skipif(os.name != "posix", reason="symlink artifact test is POSIX-specific")
def test_symlink_artifact_is_refused(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    target = tmp_path / "real.log"
    target.write_text("secret-ish output\n", encoding="utf-8")
    link = tmp_path / "agent.log"
    link.symlink_to(target)

    with pytest.raises(CandidateEvidenceError, match="symlink"):
        build_candidate_evidence_bundle(
            repo,
            revision,
            source,
            artifacts={"agent-log": link},
            destination=tmp_path / "bundle",
        )

    remove_candidate_worktree(worktree)


def test_cli_builds_and_verifies_candidate_evidence(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    frozen_receipt = tmp_path / "candidate.frozen.json"
    source_receipt = tmp_path / "source.json"
    frozen_receipt.write_text(json.dumps(revision.to_dict()), encoding="utf-8")
    source_receipt.write_text(json.dumps(source.to_dict()), encoding="utf-8")
    log = tmp_path / "agent.log"
    log.write_text("cli evidence\n", encoding="utf-8")
    destination = tmp_path / "cli-bundle"

    built = CliRunner().invoke(
        app,
        [
            "candidate-evidence",
            "build",
            str(frozen_receipt),
            str(source_receipt),
            str(destination),
            "--repo",
            str(repo),
            "--artifact",
            f"agent-log={log}",
        ],
    )

    assert built.exit_code == 0, built.output
    assert (destination / "manifest.json").is_file()
    verified = CliRunner().invoke(
        app,
        ["candidate-evidence", "verify", str(destination), "--repo", str(repo), "--json"],
    )
    assert verified.exit_code == 0, verified.output
    assert json.loads(verified.stdout)["output_head"] == revision.output_head

    remove_candidate_worktree(worktree)


@pytest.mark.skipif(os.name != "posix", reason="symlink destination test is POSIX-specific")
def test_symlink_bundle_destination_is_refused(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    real = tmp_path / "real-bundle"
    real.mkdir()
    link = tmp_path / "bundle"
    link.symlink_to(real, target_is_directory=True)

    with pytest.raises(CandidateEvidenceError, match="destination is a symlink"):
        build_candidate_evidence_bundle(repo, revision, source, artifacts={}, destination=link)

    remove_candidate_worktree(worktree)


def test_resealed_manifest_cannot_substitute_another_candidate_ref(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    destination = tmp_path / "bundle"
    build_candidate_evidence_bundle(repo, revision, source, artifacts={}, destination=destination)
    manifest = destination / "manifest.json"
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["candidate_ref"] = "refs/swfactory/candidates/" + "0" * 24
    manifest.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    reseal_manifest(manifest)

    with pytest.raises(CandidateEvidenceError, match="deterministic ref"):
        verify_candidate_evidence_bundle(destination, repo=repo)

    remove_candidate_worktree(worktree)


def test_resealed_diff_cannot_claim_different_semantics(repo: Path, tmp_path: Path) -> None:
    source, worktree, revision = answered_candidate(repo, tmp_path)
    destination = tmp_path / "bundle"
    build_candidate_evidence_bundle(repo, revision, source, artifacts={}, destination=destination)
    fake = b"diff --git a/value.txt b/value.txt\n# different but self-consistent evidence\n"
    diff_path = destination / "candidate.diff"
    diff_path.write_bytes(fake)

    manifest = destination / "manifest.json"
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["diff"]["sha256"] = hashlib.sha256(fake).hexdigest()
    document["diff"]["size_bytes"] = len(fake)
    manifest.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    reseal_manifest(manifest)

    with pytest.raises(CandidateEvidenceError, match="does not match the recorded input/output revisions"):
        verify_candidate_evidence_bundle(destination, repo=repo)

    remove_candidate_worktree(worktree)


def test_bundle_binds_inherited_recipe_to_candidate_input(repo: Path, tmp_path: Path) -> None:
    recipe_dir = repo / ".swfactory"
    recipe_dir.mkdir()
    recipe_path = recipe_dir / "candidate-run.json"
    recipe_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "argv": ["uv", "run", "pytest", "-q"],
                "cwd": ".",
                "timeout_s": 900,
                "resources": {"cpus": 2, "memory_mb": 4096},
                "environment": {"PYTHONHASHSEED": "0"},
                "secret_env": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "add inherited recipe")

    source, worktree, revision = answered_candidate(repo, tmp_path)
    recipe = load_execution_recipe(repo, revision.input_head)
    destination = tmp_path / "recipe-bundle"
    bundle = build_candidate_evidence_bundle(
        repo,
        revision,
        source,
        artifacts={},
        destination=destination,
        inherited_recipe=recipe,
    )

    assert bundle.inherited_recipe_sha256 == recipe.digest
    assert bundle.inherited_recipe_commit_sha == revision.input_head
    assert bundle.inherited_recipe_path == ".swfactory/candidate-run.json"

    recipe_path.write_text('{"schema_version":1,"argv":["dirty"]}\n', encoding="utf-8")
    verified = verify_candidate_evidence_bundle(destination, repo=repo)
    assert verified.inherited_recipe_sha256 == recipe.digest
    remove_candidate_worktree(worktree)


def test_resealed_manifest_cannot_lie_about_inherited_recipe(repo: Path, tmp_path: Path) -> None:
    recipe_dir = repo / ".swfactory"
    recipe_dir.mkdir()
    (recipe_dir / "candidate-run.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "argv": ["python", "-m", "pytest"],
                "cwd": ".",
                "timeout_s": 600,
                "resources": {"cpus": 1, "memory_mb": 1024},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "add recipe")

    source, worktree, revision = answered_candidate(repo, tmp_path)
    recipe = load_execution_recipe(repo, revision.input_head)
    destination = tmp_path / "recipe-bundle"
    build_candidate_evidence_bundle(
        repo,
        revision,
        source,
        artifacts={},
        destination=destination,
        inherited_recipe=recipe,
    )
    manifest = destination / "manifest.json"
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["inherited_recipe_sha256"] = "0" * 64
    manifest.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    reseal_manifest(manifest)

    with pytest.raises(CandidateEvidenceError, match="recipe digest"):
        verify_candidate_evidence_bundle(destination, repo=repo)

    remove_candidate_worktree(worktree)
