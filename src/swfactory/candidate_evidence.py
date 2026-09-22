"""Candidate-local evidence bundles for experiment selection and review.

OpenResearch keeps logs, diffs, files, results, and artifacts next to the work that
produced them. The factory already has cell-wide evidence and release readiness;
this module fills the narrower candidate seam: one immutable manifest binding a
frozen candidate revision to its source snapshot, exact Git diff, and retained
candidate artifacts.

The bundle is evidence only. It cannot schedule, approve, publish, or promote.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from swfactory.candidate_worktree import (
    CandidateRevision,
    CandidateWorktreeError,
    candidate_ref,
    verify_candidate_revision,
)
from swfactory.execution_recipe import BoundExecutionRecipe, load_execution_recipe
from swfactory.source_snapshot import SourceSnapshot, verify_source_snapshot

_SHA = re.compile(r"^[0-9a-f]{40}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class CandidateEvidenceError(RuntimeError):
    """Candidate evidence is incomplete, stale, or has changed after capture."""


@dataclass(frozen=True)
class RetainedArtifact:
    name: str
    path: str
    sha256: str
    size_bytes: int

    def validate(self) -> None:
        if not self.name.strip():
            raise CandidateEvidenceError("artifact name must be nonempty")
        if not self.path.strip() or Path(self.path).is_absolute() or ".." in Path(self.path).parts:
            raise CandidateEvidenceError(f"artifact {self.name!r} has an unsafe retained path")
        if not _DIGEST.fullmatch(self.sha256):
            raise CandidateEvidenceError(f"artifact {self.name!r} has an invalid sha256")
        if self.size_bytes < 0:
            raise CandidateEvidenceError(f"artifact {self.name!r} has a negative size")


@dataclass(frozen=True)
class CandidateEvidenceBundle:
    candidate_id: str
    input_head: str
    output_head: str
    candidate_ref: str
    source_sha256: str
    source_size_bytes: int
    diff: RetainedArtifact
    artifacts: tuple[RetainedArtifact, ...]
    inherited_recipe_sha256: str | None = None
    inherited_recipe_commit_sha: str | None = None
    inherited_recipe_path: str | None = None
    search_provenance_digest: str | None = None
    schema_version: int = 1

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        document = {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "input_head": self.input_head,
            "output_head": self.output_head,
            "candidate_ref": self.candidate_ref,
            "source_sha256": self.source_sha256,
            "source_size_bytes": self.source_size_bytes,
            "diff": asdict(self.diff),
            "artifacts": [asdict(item) for item in sorted(self.artifacts, key=lambda item: item.name)],
        }
        recipe_fields = (
            self.inherited_recipe_sha256,
            self.inherited_recipe_commit_sha,
            self.inherited_recipe_path,
        )
        if any(value is not None for value in recipe_fields):
            document["inherited_recipe_sha256"] = self.inherited_recipe_sha256
            document["inherited_recipe_commit_sha"] = self.inherited_recipe_commit_sha
            document["inherited_recipe_path"] = self.inherited_recipe_path
        if self.search_provenance_digest is not None:
            document["search_provenance_digest"] = self.search_provenance_digest
        return document

    def validate(self) -> None:
        if self.schema_version != 1:
            raise CandidateEvidenceError(f"unsupported candidate evidence schema {self.schema_version}")
        if not self.candidate_id.strip():
            raise CandidateEvidenceError("candidate id must be nonempty")
        for label, sha in (("input", self.input_head), ("output", self.output_head)):
            if not _SHA.fullmatch(sha):
                raise CandidateEvidenceError(f"{label} head must be a full lowercase Git SHA")
        if self.input_head == self.output_head:
            raise CandidateEvidenceError("candidate evidence requires a distinct output head")
        try:
            expected_ref = candidate_ref(self.candidate_id)
        except CandidateWorktreeError as error:
            raise CandidateEvidenceError(f"candidate id is invalid: {error}") from error
        if self.candidate_ref != expected_ref:
            raise CandidateEvidenceError(
                f"candidate evidence ref {self.candidate_ref!r} != deterministic ref {expected_ref!r}"
            )
        if not _DIGEST.fullmatch(self.source_sha256):
            raise CandidateEvidenceError("source snapshot sha256 is invalid")
        if self.source_size_bytes < 0:
            raise CandidateEvidenceError("source snapshot size is invalid")
        recipe_fields = (
            self.inherited_recipe_sha256,
            self.inherited_recipe_commit_sha,
            self.inherited_recipe_path,
        )
        if any(value is None for value in recipe_fields) and any(value is not None for value in recipe_fields):
            raise CandidateEvidenceError("inherited execution recipe fields must be recorded together")
        if self.search_provenance_digest is not None:
            search_digest = self.search_provenance_digest.removeprefix("sha256:")
            if not _DIGEST.fullmatch(search_digest):
                raise CandidateEvidenceError("search provenance digest is invalid")
        if self.inherited_recipe_sha256 is not None:
            if not _DIGEST.fullmatch(self.inherited_recipe_sha256):
                raise CandidateEvidenceError("inherited execution recipe digest is invalid")
            if self.inherited_recipe_commit_sha != self.input_head:
                raise CandidateEvidenceError("inherited execution recipe commit must equal the candidate input head")
            if not self.inherited_recipe_path:
                raise CandidateEvidenceError("inherited execution recipe path is empty")
        self.diff.validate()
        names = [item.name for item in self.artifacts]
        if len(names) != len(set(names)):
            raise CandidateEvidenceError("candidate evidence contains duplicate artifact names")
        for artifact in self.artifacts:
            artifact.validate()

    def digest(self) -> str:
        payload = json.dumps(self.canonical_dict(), sort_keys=True, separators=(",", ":")).encode()
        return "sha256:" + hashlib.sha256(payload).hexdigest()


def build_candidate_evidence_bundle(
    repo: Path,
    revision: CandidateRevision,
    source: SourceSnapshot,
    *,
    artifacts: Mapping[str, Path],
    destination: Path,
    inherited_recipe: BoundExecutionRecipe | None = None,
    search_provenance_digest: str | None = None,
) -> CandidateEvidenceBundle:
    """Retain a frozen candidate's diff and named artifacts under one manifest."""
    repo = repo.resolve()
    raw_destination = destination
    if raw_destination.is_symlink():
        raise CandidateEvidenceError(f"candidate evidence destination is a symlink: {raw_destination}")
    destination = raw_destination.resolve()
    verify_candidate_revision(repo, revision)
    verify_source_snapshot(source)
    if source.commit_sha != revision.input_head:
        raise CandidateEvidenceError(
            f"source snapshot commit {source.commit_sha} != candidate input {revision.input_head}"
        )
    if inherited_recipe is not None and inherited_recipe.commit_sha != revision.input_head:
        raise CandidateEvidenceError(
            f"inherited execution recipe commit {inherited_recipe.commit_sha} != candidate input {revision.input_head}"
        )
    if destination.exists() and not destination.is_dir():
        raise CandidateEvidenceError(f"candidate evidence destination is not a directory: {destination}")
    if destination.exists() and any(destination.iterdir()):
        raise CandidateEvidenceError(f"candidate evidence destination is not empty: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    _restrict_dir(destination)

    diff_path = destination / "candidate.diff"
    diff_bytes = _git_bytes(repo, "diff", "--binary", "--full-index", revision.input_head, revision.output_head)
    _atomic_bytes(diff_path, diff_bytes)
    diff_artifact = _retained("git-diff", diff_path, destination)

    retained: list[RetainedArtifact] = []
    artifact_dir = destination / "artifacts"
    artifact_dir.mkdir(exist_ok=True)
    _restrict_dir(artifact_dir)
    for index, (name, source_path) in enumerate(sorted(artifacts.items())):
        if not name.strip():
            raise CandidateEvidenceError("artifact name must be nonempty")
        raw_source = Path(source_path)
        if raw_source.is_symlink() or not raw_source.is_file():
            raise CandidateEvidenceError(f"artifact {name!r} is absent, not regular, or a symlink: {raw_source}")
        source_path = raw_source.resolve()
        digest, _ = _digest_file(source_path)
        retained_path = artifact_dir / f"{index:03d}-{digest[:16]}"
        _copy_atomic(source_path, retained_path)
        retained.append(_retained(name, retained_path, destination))

    bundle = CandidateEvidenceBundle(
        candidate_id=revision.candidate_id,
        input_head=revision.input_head,
        output_head=revision.output_head,
        candidate_ref=revision.ref,
        source_sha256=source.sha256,
        source_size_bytes=source.size_bytes,
        diff=diff_artifact,
        artifacts=tuple(retained),
        inherited_recipe_sha256=inherited_recipe.digest if inherited_recipe is not None else None,
        inherited_recipe_commit_sha=inherited_recipe.commit_sha if inherited_recipe is not None else None,
        inherited_recipe_path=inherited_recipe.path if inherited_recipe is not None else None,
        search_provenance_digest=search_provenance_digest,
    )
    document = bundle.canonical_dict()
    document["manifest_digest"] = bundle.digest()
    _atomic_json(destination / "manifest.json", document)
    _atomic_bytes(destination / "RESULT.md", render_candidate_result(bundle).encode())
    verify_candidate_evidence_bundle(destination, repo=repo)
    return bundle


def load_candidate_evidence_bundle(destination: Path) -> CandidateEvidenceBundle:
    path = destination / "manifest.json"
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        diff = RetainedArtifact(**document["diff"])
        artifacts = tuple(RetainedArtifact(**item) for item in document.get("artifacts", ()))
        bundle = CandidateEvidenceBundle(
            candidate_id=str(document["candidate_id"]),
            input_head=str(document["input_head"]),
            output_head=str(document["output_head"]),
            candidate_ref=str(document["candidate_ref"]),
            source_sha256=str(document["source_sha256"]),
            source_size_bytes=int(document["source_size_bytes"]),
            diff=diff,
            artifacts=artifacts,
            inherited_recipe_sha256=(
                str(document["inherited_recipe_sha256"])
                if document.get("inherited_recipe_sha256") is not None
                else None
            ),
            inherited_recipe_commit_sha=(
                str(document["inherited_recipe_commit_sha"])
                if document.get("inherited_recipe_commit_sha") is not None
                else None
            ),
            inherited_recipe_path=(
                str(document["inherited_recipe_path"]) if document.get("inherited_recipe_path") is not None else None
            ),
            search_provenance_digest=(
                str(document["search_provenance_digest"])
                if document.get("search_provenance_digest") is not None
                else None
            ),
            schema_version=int(document.get("schema_version", 1)),
        )
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise CandidateEvidenceError(f"cannot load candidate evidence manifest: {error}") from error
    expected = str(document.get("manifest_digest", ""))
    if expected != bundle.digest():
        raise CandidateEvidenceError(
            f"candidate evidence manifest digest mismatch: expected {expected!r}, observed {bundle.digest()}"
        )
    return bundle


def verify_candidate_evidence_bundle(destination: Path, *, repo: Path | None = None) -> CandidateEvidenceBundle:
    """Re-hash every retained byte and optionally re-check the immutable Git ref."""
    raw_destination = destination
    if raw_destination.is_symlink():
        raise CandidateEvidenceError(f"candidate evidence destination is a symlink: {raw_destination}")
    destination = raw_destination.resolve()
    bundle = load_candidate_evidence_bundle(destination)
    for artifact in (bundle.diff, *bundle.artifacts):
        path = destination / artifact.path
        if path.is_symlink() or not path.is_file():
            raise CandidateEvidenceError(f"retained artifact {artifact.name!r} is absent or not regular")
        digest, size = _digest_file(path)
        if digest != artifact.sha256 or size != artifact.size_bytes:
            raise CandidateEvidenceError(
                f"retained artifact {artifact.name!r} changed: "
                f"expected {artifact.sha256}/{artifact.size_bytes}, observed {digest}/{size}"
            )
    if repo is not None:
        revision = CandidateRevision(
            candidate_id=bundle.candidate_id,
            input_head=bundle.input_head,
            output_head=bundle.output_head,
            ref=bundle.candidate_ref,
        )
        repo = repo.resolve()
        try:
            verify_candidate_revision(repo, revision)
        except RuntimeError as error:
            raise CandidateEvidenceError(f"candidate ref verification failed: {error}") from error
        if bundle.inherited_recipe_sha256 is not None:
            try:
                recipe = load_execution_recipe(
                    repo,
                    bundle.inherited_recipe_commit_sha or "",
                    path=bundle.inherited_recipe_path or "",
                )
            except RuntimeError as error:
                raise CandidateEvidenceError(f"inherited execution recipe verification failed: {error}") from error
            if recipe.digest != bundle.inherited_recipe_sha256:
                raise CandidateEvidenceError("inherited execution recipe digest does not match the recorded Git object")
        expected_diff = _git_bytes(
            repo,
            "diff",
            "--binary",
            "--full-index",
            bundle.input_head,
            bundle.output_head,
        )
        expected_digest = hashlib.sha256(expected_diff).hexdigest()
        if expected_digest != bundle.diff.sha256 or len(expected_diff) != bundle.diff.size_bytes:
            raise CandidateEvidenceError(
                "retained git diff does not match the recorded input/output revisions: "
                f"expected {expected_digest}/{len(expected_diff)}, "
                f"manifest has {bundle.diff.sha256}/{bundle.diff.size_bytes}"
            )
    return bundle


def render_candidate_result(bundle: CandidateEvidenceBundle) -> str:
    """Human-readable index for the same machine-verifiable manifest."""
    rows = [
        "# Candidate result",
        "",
        f"- Candidate: `{bundle.candidate_id}`",
        f"- Input: `{bundle.input_head}`",
        f"- Output: `{bundle.output_head}`",
        f"- Frozen ref: `{bundle.candidate_ref}`",
        f"- Source snapshot: `sha256:{bundle.source_sha256}` ({bundle.source_size_bytes} bytes)",
        *(
            [
                f"- Inherited execution recipe: `sha256:{bundle.inherited_recipe_sha256}` "
                f"from `{bundle.inherited_recipe_commit_sha}:{bundle.inherited_recipe_path}`"
            ]
            if bundle.inherited_recipe_sha256 is not None
            else []
        ),
        f"- Evidence manifest: `{bundle.digest()}`",
        "",
        "## Retained evidence",
        "",
        "| Name | Bytes | SHA-256 |",
        "| --- | ---: | --- |",
        f"| {bundle.diff.name} | {bundle.diff.size_bytes} | `{bundle.diff.sha256}` |",
    ]
    rows.extend(
        f"| {item.name} | {item.size_bytes} | `{item.sha256}` |"
        for item in sorted(bundle.artifacts, key=lambda item: item.name)
    )
    return "\n".join(rows) + "\n"


def _retained(name: str, path: Path, root: Path) -> RetainedArtifact:
    digest, size = _digest_file(path)
    return RetainedArtifact(name, path.relative_to(root).as_posix(), digest, size)


def _digest_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(128 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _copy_atomic(source: Path, destination: Path) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=".artifact-", dir=destination.parent)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        shutil.copyfile(source, temporary)
        _restrict_file(temporary)
        os.replace(temporary, destination)
        _restrict_file(destination)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_bytes(path: Path, content: bytes) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        temporary.write_bytes(content)
        _restrict_file(temporary)
        os.replace(temporary, path)
        _restrict_file(path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, document: Mapping[str, Any]) -> None:
    _atomic_bytes(path, (json.dumps(dict(document), indent=2, sort_keys=True) + "\n").encode())


def _git_bytes(repo: Path, *args: str) -> bytes:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        check=False,
        timeout=120,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode(errors="replace").strip()
        raise CandidateEvidenceError(f"git {' '.join(args)} failed: {detail}")
    return proc.stdout


def _restrict_dir(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o700)


def _restrict_file(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o600)
