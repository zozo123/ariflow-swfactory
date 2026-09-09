from __future__ import annotations

from pathlib import Path

import pytest

from swfactory.evidence_release import (
    CandidateManifest,
    CapabilityClaim,
    ProducerEvidence,
    ReleaseArtifact,
    ReleaseAttestation,
    assert_public_claim_not_stronger,
    parse_workflow_jobs,
)


def producer(name: str, sha: str) -> ProducerEvidence:
    return ProducerEvidence(name, "run-1", "job-1", sha, {"python": "3.13"}, "success", "a" * 64, "https://logs")


def test_candidate_manifest_requires_exact_producer_sha() -> None:
    manifest = CandidateManifest("deadbeef", "base", (producer("test", "deadbeef"),), {"wheel": "b" * 64})
    manifest.validate({"test"})
    broken = CandidateManifest("other", "base", manifest.producers, manifest.artifact_digests)
    with pytest.raises(RuntimeError, match="tested"):
        broken.validate({"test"})


def test_release_attestation_binds_exact_candidate_and_artifact() -> None:
    manifest = CandidateManifest("deadbeef", "base", (producer("test", "deadbeef"),), {"wheel": "b" * 64})
    artifact = ReleaseArtifact("wheel", "deadbeef", "b" * 64, "any", "1.0.0")
    ReleaseAttestation(manifest.digest, (artifact,)).validate(manifest=manifest, expected_names={"wheel"})
    bad = ReleaseArtifact("wheel", "deadbeef", "c" * 64, "any", "1.0.0")
    with pytest.raises(RuntimeError, match="digest differs"):
        ReleaseAttestation(manifest.digest, (bad,)).validate(manifest=manifest, expected_names={"wheel"})


def test_public_docs_cannot_overstate_inventory() -> None:
    claim = CapabilityClaim("provider-fork", "experimental", "src/swfactory/work_executor.py", "tests/x.py", "test")
    assert_public_claim_not_stronger(claim, "experimental")
    with pytest.raises(RuntimeError, match="overstates"):
        assert_public_claim_not_stronger(claim, "supported")


def test_workflow_job_parser_finds_top_level_jobs() -> None:
    workflow = """
name: ci
jobs:
  test:
    runs-on: ubuntu-latest
  rust:
    runs-on: ubuntu-latest
"""
    assert parse_workflow_jobs([workflow]) == {"test", "rust"}


def test_claim_requires_resolvable_runtime_test_and_job(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "src" / "runtime.py").write_text("x = 1\n")
    (tmp_path / "tests" / "test_runtime.py").write_text("def test_x(): pass\n")
    claim = CapabilityClaim("runtime", "supported", "src/runtime.py", "tests/test_runtime.py", "test")
    claim.validate(tmp_path, {"test"})
    with pytest.raises(RuntimeError, match="evidence job"):
        claim.validate(tmp_path, {"rust"})
