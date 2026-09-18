"""Candidate evidence stays bound to one frozen Git answer and retained bytes."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from swfactory.candidate_evidence import (
    CandidateEvidenceError,
    build_candidate_evidence,
    verify_candidate_evidence,
)
from swfactory.candidate_worktree import (
    create_candidate_worktree,
    freeze_candidate_worktree,
    remove_candidate_worktree,
)

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


def frozen(repo: Path, tmp_path: Path, candidate_id: str = "candidate-a"):
    head = git(repo, "rev-parse", "HEAD")
    worktree = create_candidate_worktree(repo, candidate_id, head, root=tmp_path / "worktrees")
    path = Path(worktree.path)
    (path / "value.txt").write_text("answer\n", encoding="utf-8")
    git(path, "add", "value.txt")
    git(path, "commit", "-qm", "answer")
    revision = freeze_candidate_worktree(worktree)
    remove_candidate_worktree(worktree)
    return revision


def test_bundle_keeps_diff_result_and_copied_artifacts_together(repo: Path, tmp_path: Path) -> None:
    revision = frozen(repo, tmp_path)
    log = tmp_path / "pytest.log"
    log.write_text("42 passed\n", encoding="utf-8")

    manifest, path = build_candidate_evidence(
        repo,
        revision,
        root=tmp_path / "evidence",
        result={"state": "ok", "strategy": "repair", "score": 1.0},
        artifacts={"pytest.log": log},
    )

    assert path.is_file()
    assert manifest.candidate_id == revision.candidate_id
    assert manifest.input_head == revision.input_head
    assert manifest.output_head == revision.output_head
    assert manifest.candidate_ref == revision.ref
    assert {item.name for item in manifest.artifacts} == {"changes.patch", "result.json", "pytest.log"}

    bundle = path.parent
    patch = (bundle / "changes.patch").read_text(encoding="utf-8")
    assert "-base" in patch
    assert "+answer" in patch
    assert json.loads((bundle / "result.json").read_text())["strategy"] == "repair"
    assert (bundle / "artifacts" / "pytest.log").read_text() == "42 passed\n"

    verified = verify_candidate_evidence(repo, path)
    assert verified.digest() == manifest.digest()


def test_bundle_survives_source_artifact_and_worktree_disappearance(repo: Path, tmp_path: Path) -> None:
    revision = frozen(repo, tmp_path)
    source = tmp_path / "trace.json"
    source.write_text('{"events": 3}\n', encoding="utf-8")
    _, path = build_candidate_evidence(
        repo,
        revision,
        root=tmp_path / "evidence",
        result={"state": "ok"},
        artifacts={"trace.json": source},
    )

    source.unlink()

    verified = verify_candidate_evidence(repo, path)
    assert verified.output_head == revision.output_head
    assert (path.parent / "artifacts" / "trace.json").is_file()


def test_existing_verified_bundle_is_idempotently_reused(repo: Path, tmp_path: Path) -> None:
    revision = frozen(repo, tmp_path)
    root = tmp_path / "evidence"
    first, first_path = build_candidate_evidence(repo, revision, root=root, result={"state": "ok"})
    second, second_path = build_candidate_evidence(
        repo,
        revision,
        root=root,
        result={"state": "different result is not allowed to rewrite evidence"},
    )

    assert second_path == first_path
    assert second.digest() == first.digest()
    assert json.loads((first_path.parent / "result.json").read_text()) == {"state": "ok"}


def test_tampered_result_is_detected(repo: Path, tmp_path: Path) -> None:
    revision = frozen(repo, tmp_path)
    _, path = build_candidate_evidence(repo, revision, root=tmp_path / "evidence", result={"state": "ok"})
    (path.parent / "result.json").write_text('{"state":"forged"}\n', encoding="utf-8")

    with pytest.raises(CandidateEvidenceError, match="integrity mismatch"):
        verify_candidate_evidence(repo, path)


def test_tampered_manifest_is_detected(repo: Path, tmp_path: Path) -> None:
    revision = frozen(repo, tmp_path)
    _, path = build_candidate_evidence(repo, revision, root=tmp_path / "evidence", result={"state": "ok"})
    document = json.loads(path.read_text())
    document["output_head"] = revision.input_head
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(CandidateEvidenceError, match="manifest digest mismatch"):
        verify_candidate_evidence(repo, path)


def test_candidate_ref_drift_is_detected(repo: Path, tmp_path: Path) -> None:
    revision = frozen(repo, tmp_path)
    _, path = build_candidate_evidence(repo, revision, root=tmp_path / "evidence", result={"state": "ok"})
    subprocess.run(["git", "update-ref", "-d", revision.ref], cwd=repo, check=True)

    with pytest.raises(Exception, match="candidate ref drift"):
        verify_candidate_evidence(repo, path)


def test_external_artifact_names_cannot_escape_or_replace_internal_evidence(repo: Path, tmp_path: Path) -> None:
    revision = frozen(repo, tmp_path)
    source = tmp_path / "x"
    source.write_text("x", encoding="utf-8")

    for name in ("../escape", "changes.patch", "result.json", "manifest.json", "a/b"):
        with pytest.raises(CandidateEvidenceError):
            build_candidate_evidence(
                repo,
                revision,
                root=tmp_path / f"evidence-{name.replace('/', '-')}",
                result={"state": "ok"},
                artifacts={name: source},
            )


@pytest.mark.skipif(__import__("os").name != "posix", reason="symlink replacement is POSIX-specific")
def test_artifact_symlink_swap_is_detected(repo: Path, tmp_path: Path) -> None:
    revision = frozen(repo, tmp_path)
    source = tmp_path / "trace.txt"
    source.write_text("trusted\n", encoding="utf-8")
    _, path = build_candidate_evidence(
        repo,
        revision,
        root=tmp_path / "evidence",
        result={"state": "ok"},
        artifacts={"trace.txt": source},
    )
    retained = path.parent / "artifacts" / "trace.txt"
    retained.unlink()
    retained.symlink_to(source)

    with pytest.raises(CandidateEvidenceError, match="not regular"):
        verify_candidate_evidence(repo, path)


def test_bundle_refuses_a_different_revision_at_same_candidate_identity(repo: Path, tmp_path: Path) -> None:
    revision = frozen(repo, tmp_path)
    root = tmp_path / "evidence"
    build_candidate_evidence(repo, revision, root=root, result={"state": "ok"})

    # The same candidate id cannot be rebound to the input commit through evidence reuse.
    forged = revision.__class__(
        candidate_id=revision.candidate_id,
        input_head=revision.input_head,
        output_head=revision.input_head,
        ref=revision.ref,
    )
    with pytest.raises(Exception):
        build_candidate_evidence(repo, forged, root=root, result={"state": "ok"})
