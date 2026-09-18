"""Candidate-local evidence bundles: one answer, one immutable context.

OpenResearch keeps logs, diffs, files, results, and artifacts next to the experiment
that produced them.  This module adapts that idea to the Liquid factory's existing
Git authority model: a frozen candidate revision gets one self-contained,
content-verified bundle.

The bundle is evidence only.  It schedules nothing and grants no promotion authority.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from swfactory.candidate_worktree import CandidateRevision, verify_candidate_revision

_ARTIFACT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class CandidateEvidenceError(RuntimeError):
    """Candidate evidence is missing, mutable, ambiguous, or corrupt."""


@dataclass(frozen=True)
class EvidenceArtifact:
    name: str
    path: str
    sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateEvidenceManifest:
    candidate_id: str
    input_head: str
    output_head: str
    candidate_ref: str
    artifacts: tuple[EvidenceArtifact, ...]
    schema_version: int = 1

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "input_head": self.input_head,
            "output_head": self.output_head,
            "candidate_ref": self.candidate_ref,
            "artifacts": [artifact.to_dict() for artifact in sorted(self.artifacts, key=lambda item: item.name)],
        }

    def digest(self) -> str:
        raw = json.dumps(self.canonical_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        return "sha256:" + hashlib.sha256(raw).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        document = self.canonical_dict()
        document["manifest_digest"] = self.digest()
        return document

    @classmethod
    def from_dict(cls, document: Mapping[str, Any]) -> CandidateEvidenceManifest:
        artifacts = tuple(
            EvidenceArtifact(
                name=str(item["name"]),
                path=str(item["path"]),
                sha256=str(item["sha256"]),
                size_bytes=int(item["size_bytes"]),
            )
            for item in document.get("artifacts", ())
        )
        return cls(
            candidate_id=str(document["candidate_id"]),
            input_head=str(document["input_head"]),
            output_head=str(document["output_head"]),
            candidate_ref=str(document["candidate_ref"]),
            artifacts=artifacts,
            schema_version=int(document.get("schema_version", 1)),
        )


def build_candidate_evidence(
    repo: Path,
    revision: CandidateRevision,
    *,
    root: Path,
    result: Mapping[str, Any],
    artifacts: Mapping[str, Path] | None = None,
) -> tuple[CandidateEvidenceManifest, Path]:
    """Build or verify the immutable evidence bundle for one frozen candidate."""
    repo = repo.resolve()
    verify_candidate_revision(repo, revision)
    _verify_lineage(repo, revision.input_head, revision.output_head)

    bundle = root.resolve() / _candidate_token(revision.candidate_id)
    manifest_path = bundle / "manifest.json"
    if manifest_path.exists():
        manifest = load_candidate_evidence(manifest_path)
        verify_candidate_evidence(repo, manifest_path)
        _assert_identity(manifest, revision)
        return manifest, manifest_path
    if bundle.exists():
        raise CandidateEvidenceError(f"candidate evidence directory exists without a manifest: {bundle}")

    bundle.parent.mkdir(parents=True, exist_ok=True)
    _restrict_dir(bundle.parent)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}-", dir=bundle.parent))
    try:
        _restrict_dir(staging)
        retained: list[EvidenceArtifact] = []

        patch_path = staging / "changes.patch"
        _git_diff(repo, revision.input_head, revision.output_head, patch_path)
        retained.append(_artifact("changes.patch", patch_path, staging))

        result_path = staging / "result.json"
        _write_json(result_path, dict(result))
        retained.append(_artifact("result.json", result_path, staging))

        artifact_dir = staging / "artifacts"
        if artifacts:
            artifact_dir.mkdir()
            _restrict_dir(artifact_dir)
        for name, source in sorted((artifacts or {}).items()):
            _validate_artifact_name(name)
            source = Path(source)
            if source.is_symlink() or not source.is_file():
                raise CandidateEvidenceError(f"candidate artifact {name!r} is not a regular file: {source}")
            destination = artifact_dir / name
            _copy_file(source, destination)
            retained.append(_artifact(name, destination, staging))

        manifest = CandidateEvidenceManifest(
            candidate_id=revision.candidate_id,
            input_head=revision.input_head,
            output_head=revision.output_head,
            candidate_ref=revision.ref,
            artifacts=tuple(retained),
        )
        _write_json(staging / "manifest.json", manifest.to_dict())
        os.replace(staging, bundle)
        _fsync_dir(bundle.parent)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    verify_candidate_evidence(repo, manifest_path)
    return manifest, manifest_path


def load_candidate_evidence(manifest_path: Path) -> CandidateEvidenceManifest:
    """Read a candidate manifest and validate its self-digest."""
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest = CandidateEvidenceManifest.from_dict(document)
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise CandidateEvidenceError(f"invalid candidate evidence manifest {manifest_path}: {error}") from error
    expected = manifest.digest()
    observed = str(document.get("manifest_digest", ""))
    if observed != expected:
        raise CandidateEvidenceError(f"candidate evidence manifest digest mismatch: expected {expected}, observed {observed}")
    return manifest


def verify_candidate_evidence(repo: Path, manifest_path: Path) -> CandidateEvidenceManifest:
    """Recompute Git identity and every retained artifact digest."""
    manifest_path = manifest_path.resolve()
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise CandidateEvidenceError(f"candidate evidence manifest is not a regular file: {manifest_path}")
    manifest = load_candidate_evidence(manifest_path)
    bundle = manifest_path.parent

    revision = CandidateRevision(
        candidate_id=manifest.candidate_id,
        input_head=manifest.input_head,
        output_head=manifest.output_head,
        ref=manifest.candidate_ref,
    )
    verify_candidate_revision(repo.resolve(), revision)
    _verify_lineage(repo.resolve(), manifest.input_head, manifest.output_head)

    names: set[str] = set()
    for artifact in manifest.artifacts:
        _validate_stored_artifact_name(artifact.name)
        if artifact.name in names:
            raise CandidateEvidenceError(f"duplicate candidate artifact name: {artifact.name}")
        names.add(artifact.name)
        raw_path = bundle / artifact.path
        if raw_path.is_symlink():
            raise CandidateEvidenceError(f"candidate artifact is missing or not regular: {artifact.path}")
        path = raw_path.resolve()
        try:
            path.relative_to(bundle.resolve())
        except ValueError as error:
            raise CandidateEvidenceError(f"candidate artifact escapes bundle: {artifact.path}") from error
        if path.is_symlink() or not path.is_file():
            raise CandidateEvidenceError(f"candidate artifact is missing or not regular: {artifact.path}")
        digest, size = _digest_file(path)
        if digest != artifact.sha256 or size != artifact.size_bytes:
            raise CandidateEvidenceError(
                f"candidate artifact {artifact.name!r} integrity mismatch: "
                f"expected {artifact.sha256}/{artifact.size_bytes}, observed {digest}/{size}"
            )

    required = {"changes.patch", "result.json"}
    if not required.issubset(names):
        raise CandidateEvidenceError(f"candidate evidence missing required artifacts: {sorted(required - names)}")
    return manifest


def _assert_identity(manifest: CandidateEvidenceManifest, revision: CandidateRevision) -> None:
    actual = (manifest.candidate_id, manifest.input_head, manifest.output_head, manifest.candidate_ref)
    expected = (revision.candidate_id, revision.input_head, revision.output_head, revision.ref)
    if actual != expected:
        raise CandidateEvidenceError("existing candidate evidence belongs to a different frozen revision")


def _candidate_token(candidate_id: str) -> str:
    if not candidate_id.strip():
        raise CandidateEvidenceError("candidate id must be nonempty")
    return hashlib.sha256(candidate_id.encode()).hexdigest()[:24]


def _validate_stored_artifact_name(name: str) -> None:
    if not _ARTIFACT_NAME.fullmatch(name):
        raise CandidateEvidenceError(f"invalid candidate artifact name: {name!r}")


def _validate_artifact_name(name: str) -> None:
    _validate_stored_artifact_name(name)
    if name in {"changes.patch", "result.json", "manifest.json"}:
        raise CandidateEvidenceError(f"artifact name is reserved: {name}")


def _artifact(name: str, path: Path, bundle: Path) -> EvidenceArtifact:
    digest, size = _digest_file(path)
    return EvidenceArtifact(name=name, path=str(path.relative_to(bundle)), sha256=digest, size_bytes=size)


def _copy_file(source: Path, destination: Path) -> None:
    with source.open("rb") as src, destination.open("xb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
        dst.flush()
        os.fsync(dst.fileno())
    _restrict_file(destination)


def _write_json(path: Path, document: Mapping[str, Any]) -> None:
    data = json.dumps(document, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    with path.open("x", encoding="utf-8") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    _restrict_file(path)


def _git_diff(repo: Path, input_head: str, output_head: str, destination: Path) -> None:
    with destination.open("xb") as stream:
        proc = subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "diff",
                "--binary",
                "--full-index",
                "--no-ext-diff",
                input_head,
                output_head,
                "--",
            ],
            stdout=stream,
            stderr=subprocess.PIPE,
            check=False,
            timeout=120,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )
        stream.flush()
        os.fsync(stream.fileno())
    if proc.returncode != 0:
        destination.unlink(missing_ok=True)
        detail = proc.stderr.decode(errors="replace").strip()
        raise CandidateEvidenceError(f"could not render candidate diff: {detail}")
    _restrict_file(destination)


def _verify_lineage(repo: Path, input_head: str, output_head: str) -> None:
    proc = subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", input_head, output_head],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    if proc.returncode != 0:
        raise CandidateEvidenceError(f"candidate output {output_head} does not descend from input {input_head}")


def _digest_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return "sha256:" + digest.hexdigest(), size


def _restrict_dir(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o700)


def _restrict_file(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o600)


def _fsync_dir(path: Path) -> None:
    if os.name != "posix":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
