from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence


SUPPORT_ORDER = {"unsupported": 0, "test_only": 1, "experimental": 2, "supported": 3}


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


@dataclass(frozen=True)
class ProducerEvidence:
    producer: str
    run_id: str
    job_id: str
    tested_sha: str
    environment: Mapping[str, str]
    result: str
    report_digest: str
    log_url: str

    def validate(self, expected_sha: str) -> None:
        if self.tested_sha != expected_sha:
            raise RuntimeError(f"{self.producer} tested {self.tested_sha}, expected {expected_sha}")
        if self.result != "success":
            raise RuntimeError(f"{self.producer} result is {self.result}")
        if len(self.report_digest) != 64:
            raise RuntimeError(f"{self.producer} report digest is not sha256")
        if not self.run_id or not self.job_id or not self.log_url:
            raise RuntimeError(f"{self.producer} evidence is incomplete")


@dataclass(frozen=True)
class CandidateManifest:
    source_sha: str
    base_sha: str
    producers: tuple[ProducerEvidence, ...]
    artifact_digests: Mapping[str, str]

    @property
    def digest(self) -> str:
        payload = {
            "source_sha": self.source_sha,
            "base_sha": self.base_sha,
            "producers": [producer.__dict__ for producer in self.producers],
            "artifact_digests": dict(sorted(self.artifact_digests.items())),
        }
        return sha256_bytes(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=dict).encode())

    def validate(self, mandatory_producers: set[str]) -> None:
        names = {producer.producer for producer in self.producers}
        missing = mandatory_producers - names
        if missing:
            raise RuntimeError("missing producer evidence: " + ",".join(sorted(missing)))
        duplicates = [name for name in names if sum(1 for row in self.producers if row.producer == name) != 1]
        if duplicates:
            raise RuntimeError("duplicate producer evidence: " + ",".join(sorted(duplicates)))
        for producer in self.producers:
            producer.validate(self.source_sha)
        for name, value in self.artifact_digests.items():
            if len(value) != 64:
                raise RuntimeError(f"artifact {name} digest is not sha256")


@dataclass(frozen=True)
class CapabilityClaim:
    claim: str
    support: str
    runtime_entry: str
    test: str
    evidence: str

    def validate(self, root: Path, workflow_jobs: set[str]) -> None:
        if self.support not in SUPPORT_ORDER:
            raise RuntimeError(f"unknown support level {self.support}")
        runtime = root / self.runtime_entry
        test = root / self.test
        if not runtime.exists():
            raise RuntimeError(f"claim {self.claim} runtime does not resolve: {self.runtime_entry}")
        if not test.exists():
            raise RuntimeError(f"claim {self.claim} test does not resolve: {self.test}")
        if self.evidence not in workflow_jobs:
            raise RuntimeError(f"claim {self.claim} evidence job does not resolve: {self.evidence}")


def assert_public_claim_not_stronger(claim: CapabilityClaim, advertised_support: str) -> None:
    if advertised_support not in SUPPORT_ORDER:
        raise RuntimeError(f"unknown advertised support level {advertised_support}")
    if SUPPORT_ORDER[advertised_support] > SUPPORT_ORDER[claim.support]:
        raise RuntimeError(
            f"public claim {claim.claim} overstates support: advertised={advertised_support} inventory={claim.support}"
        )


@dataclass(frozen=True)
class ReleaseArtifact:
    name: str
    source_sha: str
    digest: str
    platform: str
    version: str

    def validate(self, expected_sha: str) -> None:
        if self.source_sha != expected_sha:
            raise RuntimeError(f"artifact {self.name} comes from the wrong source revision")
        if len(self.digest) != 64:
            raise RuntimeError(f"artifact {self.name} has invalid digest")
        if not self.platform or not self.version:
            raise RuntimeError(f"artifact {self.name} is missing platform/version")


@dataclass(frozen=True)
class ReleaseAttestation:
    candidate_digest: str
    artifacts: tuple[ReleaseArtifact, ...]

    def validate(
        self,
        *,
        manifest: CandidateManifest,
        expected_names: set[str],
    ) -> None:
        manifest.validate({producer.producer for producer in manifest.producers})
        if self.candidate_digest != manifest.digest:
            raise RuntimeError("release attestation targets a different candidate")
        names = {artifact.name for artifact in self.artifacts}
        if names != expected_names:
            missing = expected_names - names
            extra = names - expected_names
            raise RuntimeError(f"release artifact membership mismatch missing={sorted(missing)} extra={sorted(extra)}")
        for artifact in self.artifacts:
            artifact.validate(manifest.source_sha)
            expected_digest = manifest.artifact_digests.get(artifact.name)
            if expected_digest is None:
                raise RuntimeError(f"candidate manifest does not bind artifact {artifact.name}")
            if expected_digest != artifact.digest:
                raise RuntimeError(f"artifact {artifact.name} digest differs from candidate manifest")


def parse_workflow_jobs(workflow_texts: Iterable[str]) -> set[str]:
    # A deliberately tiny YAML-independent parser sufficient for validating inventory references.
    jobs: set[str] = set()
    in_jobs = False
    base_indent: int | None = None
    for text in workflow_texts:
        in_jobs = False
        base_indent = None
        for raw in text.splitlines():
            stripped = raw.strip()
            if not stripped or stripped.startswith("#"):
                continue
            indent = len(raw) - len(raw.lstrip())
            if stripped == "jobs:":
                in_jobs = True
                base_indent = indent
                continue
            if in_jobs and base_indent is not None:
                if indent <= base_indent:
                    in_jobs = False
                    continue
                if indent == base_indent + 2 and stripped.endswith(":") and not stripped.startswith("-"):
                    jobs.add(stripped[:-1])
    return jobs


def validate_claims(
    claims: Sequence[CapabilityClaim],
    *,
    root: Path,
    workflow_texts: Iterable[str],
) -> None:
    jobs = parse_workflow_jobs(workflow_texts)
    seen: set[str] = set()
    for claim in claims:
        if claim.claim in seen:
            raise RuntimeError(f"duplicate capability claim {claim.claim}")
        seen.add(claim.claim)
        claim.validate(root, jobs)
