"""Evidence-bound, deterministic patch proposals from sibling candidate experiments.

This is a controller-side search primitive. It uses a private Git index, never a
coding worktree, and produces an unverified patch rather than a commit or approval.
Airflow scheduling, verification, review and publication remain with their owners.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator

from swfactory.campaign_decision import verify_campaign_decision
from swfactory.candidate_evidence import verify_candidate_evidence_bundle
from swfactory.canonical import atomic_json, json_digest, restrict_dir, restrict_file
from swfactory.config import TargetContract, protected_for
from swfactory.paths import normalize_relative_path

MAX_CONTRIBUTORS = 4
MAX_PATCH_BYTES = 16 * 1024 * 1024
MAX_OBSERVATION_BYTES = 1024 * 1024
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_CHANGED_FILES = 512
FIXED_PROTECTED = (
    ".git",
    ".github",
    ".gitmodules",
    ".claude",
    ".factory",
    ".factory-runs",
    ".swfactory/candidate-run.json",
    "docs/factory",
    "factory.toml",
    "AGENTS.md",
    "CLAUDE.md",
    "REVIEW.md",
    "REVIEW_LIQUID.md",
)


class CampaignFusionError(ValueError):
    """Inputs cannot support an exact, bounded fusion proposal."""


class FusionObservation(BaseModel):
    """An untrusted discovery; producer identity does not make it verified."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_id: StrictStr = Field(min_length=1, max_length=256)
    producer_id: StrictStr = Field(min_length=1, max_length=256)
    kind: Literal["hypothesis", "reproducer", "counterexample"]
    statement: StrictStr = Field(min_length=1, max_length=4096)
    subject_head: StrictStr = Field(pattern=r"^[0-9a-f]{40}$")
    artifact_digest: StrictStr | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    source_evidence_ids: tuple[StrictStr, ...] = Field(default=(), max_length=64)

    @field_validator("evidence_id", "producer_id", "statement")
    @classmethod
    def _nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("observation fields must not be blank")
        return value

    @field_validator("source_evidence_ids")
    @classmethod
    def _sources(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value.strip() or len(value) > 256 for value in values):
            raise ValueError("source evidence identities must be nonempty and bounded")
        return tuple(sorted(set(values)))


class FusionObservations(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    authority: Literal["untrusted-observations"] = "untrusted-observations"
    campaign_decision_digest: StrictStr = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    observations: tuple[FusionObservation, ...] = Field(max_length=256)


def _git(repo: Path, *args: str, index: Path | None = None, data: bytes | None = None) -> bytes:
    environment = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    if index is not None:
        environment["GIT_INDEX_FILE"] = str(index)
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            input=data,
            capture_output=True,
            env=environment,
            check=False,
            timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise CampaignFusionError(f"fusion Git operation failed: {error}") from error
    if result.returncode:
        raise CampaignFusionError(result.stderr.decode(errors="replace").strip())
    return result.stdout


def _ledger(path: Path | None, decision_digest: str, heads: set[str]) -> list[dict]:
    if path is None:
        return []
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_OBSERVATION_BYTES:
        raise CampaignFusionError("observations must be a regular file of at most 1 MiB")
    document = FusionObservations.model_validate_json(path.read_bytes())
    if document.campaign_decision_digest != decision_digest:
        raise CampaignFusionError("observations belong to another campaign decision")
    records: dict[str, dict] = {}
    producers: dict[str, set[str]] = {}
    for observation in document.observations:
        if observation.subject_head not in heads:
            raise CampaignFusionError("observation subject is outside the campaign")
        payload = observation.model_dump(mode="json", exclude={"producer_id"})
        previous = records.get(observation.evidence_id)
        if previous is not None and previous != payload:
            raise CampaignFusionError(f"conflicting evidence identity: {observation.evidence_id}")
        records[observation.evidence_id] = payload
        producers.setdefault(observation.evidence_id, set()).add(observation.producer_id)
    return [
        {**records[key], "producer_ids": sorted(producers[key]), "authority": "untrusted-observations"}
        for key in sorted(records)
    ]


def _is_protected(path: str, rules: Sequence[str]) -> bool:
    if any(part.casefold() == ".git" for part in path.split("/")):
        return True
    for rule in rules:
        # Match globs and their descendants; also preserve the conservative literal
        # directory boundary used by the sandbox for patterns such as src/**/*.py.
        segments = rule.rstrip("/").split("/")
        literal: list[str] = []
        for segment in segments:
            if any(character in segment for character in "*?["):
                break
            literal.append(segment)
        prefix = "/".join(literal)
        ancestors = ["/".join(path.split("/")[:count]) for count in range(1, len(path.split("/")) + 1)]
        if (prefix and (path == prefix or path.startswith(prefix + "/"))) or any(
            fnmatch.fnmatchcase(ancestor, rule.rstrip("/")) for ancestor in ancestors
        ):
            return True
    return False


def _changes(repo: Path, base: str, head: str) -> list[tuple[str, str, str]]:
    _git(repo, "merge-base", "--is-ancestor", base, head)
    raw = _git(repo, "diff-tree", "--no-commit-id", "-r", "--no-renames", "--raw", "-z", base, head)
    fields = raw.split(b"\0")
    changes = []
    for offset in range(0, len(fields) - 1, 2):
        old_mode, mode, _old_object, object_id, _status = fields[offset].decode("ascii").split()
        path = fields[offset + 1].decode("utf-8")
        if normalize_relative_path(path) != path:
            raise CampaignFusionError("candidate path is not canonical")
        if old_mode[1:] not in {"000000", "100644", "100755"} or mode not in {"000000", "100644", "100755"}:
            raise CampaignFusionError("fusion refuses symlinks, submodules and non-regular entries")
        changes.append((path, mode, object_id))
        if len(changes) > MAX_CHANGED_FILES:
            raise CampaignFusionError("contributor exceeds the 512-file fusion limit")
    if not changes:
        raise CampaignFusionError("contributor has no source changes")
    return changes


def _proposal(
    repo: Path,
    decision_path: Path,
    evidence_paths: Mapping[str, Path],
    candidate_ids: Sequence[str],
    observations_path: Path | None,
) -> tuple[dict, bytes]:
    decision = verify_campaign_decision(decision_path, evidence_paths, repo=repo)
    if decision.cancelled:
        raise CampaignFusionError("cancelled campaigns cannot produce fusion proposals")
    ids = tuple(sorted(candidate_ids))
    if not 2 <= len(ids) <= MAX_CONTRIBUTORS or len(set(ids)) != len(ids):
        raise CampaignFusionError("select two to four distinct contributors")
    candidates = {candidate.candidate_id: candidate for candidate in decision.candidates}
    if any(candidate_id not in evidence_paths for candidate_id in ids):
        raise CampaignFusionError("every contributor must be an answered, retained campaign candidate")
    base = decision.input_head
    if _git(repo, "rev-parse", "--verify", f"{base}^{{commit}}").decode().strip() != base:
        raise CampaignFusionError("campaign base must be an exact commit SHA")
    contract = TargetContract.parse(_git(repo, "show", f"{base}:factory.toml").decode("utf-8"))
    protected = tuple(sorted(set((*FIXED_PROTECTED, *protected_for(contract, "build")))))
    heads = {base, *(candidate.output_head for candidate in candidates.values() if candidate.answered)}
    ledger = _ledger(observations_path, decision.digest(), heads)
    contributors = []
    edits: dict[str, tuple[str, str]] = {}
    for candidate_id in ids:
        candidate = candidates[candidate_id]
        bundle = verify_candidate_evidence_bundle(evidence_paths[candidate_id], repo=repo)
        changes = _changes(repo, base, bundle.output_head)
        for path, mode, object_id in changes:
            if _is_protected(path, protected):
                raise CampaignFusionError(f"protected candidate path: {path}")
            if any(path == other or path.startswith(other + "/") or other.startswith(path + "/") for other in edits):
                raise CampaignFusionError(f"overlapping candidate path: {path}; semantic synthesis is required")
            edits[path] = (mode, object_id)
        contributors.append(
            {
                "candidate_id": candidate_id,
                "output_head": candidate.output_head,
                "evidence_bundle_digest": bundle.digest(),
                "paths": sorted(path for path, _, _ in changes),
            }
        )
    with tempfile.TemporaryDirectory(prefix="swfactory-fusion-") as scratch:
        index = Path(scratch) / "index"
        _git(repo, "read-tree", base, index=index)
        entries = b"".join(
            f"{mode} {object_id}\t{path}".encode("utf-8") + b"\0" for path, (mode, object_id) in sorted(edits.items())
        )
        _git(repo, "update-index", "-z", "--index-info", index=index, data=entries)
        tree = _git(repo, "write-tree", index=index).decode().strip()
    patch = _git(
        repo,
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--no-renames",
        "--no-color",
        "--no-relative",
        "--src-prefix=a/",
        "--dst-prefix=b/",
        "--diff-algorithm=myers",
        "--no-indent-heuristic",
        "--binary",
        "--full-index",
        base,
        tree,
    )
    if not patch or len(patch) > MAX_PATCH_BYTES:
        raise CampaignFusionError("fusion patch must be nonempty and at most 16 MiB")
    manifest = {
        "schema_version": 1,
        "authority": "search-only",
        "verification": "required",
        "campaign_id": decision.campaign_id,
        "cell_id": decision.cell_id,
        "epoch": decision.epoch,
        "campaign_decision_digest": decision.digest(),
        "input_head": base,
        "output_tree": tree,
        "contributors": contributors,
        "observations": ledger,
        "observations_digest": json_digest(ledger),
        "protected_paths": list(protected),
        "existing_selection": decision.selection.winner,
        "patch": {"path": "candidate.diff", "sha256": hashlib.sha256(patch).hexdigest(), "size_bytes": len(patch)},
    }
    return manifest, patch


def build_fusion_proposal(
    repo: Path,
    decision_path: Path,
    evidence_paths: Mapping[str, Path],
    candidate_ids: Sequence[str],
    destination: Path,
    *,
    observations_path: Path | None = None,
) -> dict:
    """Retain a new patch proposal without changing HEAD, the working tree, index or refs."""
    manifest, patch = _proposal(repo.resolve(), decision_path, evidence_paths, candidate_ids, observations_path)
    if destination.exists() or destination.is_symlink():
        raise CampaignFusionError("fusion destination must not exist")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.mkdir(mode=0o700)
    restrict_dir(destination)
    patch_path = destination / "candidate.diff"
    with patch_path.open("xb") as stream:
        stream.write(patch)
    restrict_file(patch_path)
    atomic_json(destination / "manifest.json", {**manifest, "manifest_digest": json_digest(manifest)})
    return manifest


def verify_fusion_proposal(
    repo: Path,
    decision_path: Path,
    evidence_paths: Mapping[str, Path],
    destination: Path,
    *,
    observations_path: Path | None = None,
) -> dict:
    """Reconstruct exact proposal bytes and lineage; this does not verify program behavior."""
    if destination.is_symlink() or not destination.is_dir():
        raise CampaignFusionError("fusion destination must be a regular directory")
    manifest_path = destination / "manifest.json"
    patch_path = destination / "candidate.diff"
    if any(path.is_symlink() or not path.is_file() for path in (manifest_path, patch_path)):
        raise CampaignFusionError("fusion manifest and patch must be regular files")
    if manifest_path.stat().st_size > MAX_MANIFEST_BYTES or patch_path.stat().st_size > MAX_PATCH_BYTES:
        raise CampaignFusionError("retained fusion artifact exceeds its size limit")
    document = json.loads(manifest_path.read_bytes())
    ids = [row["candidate_id"] for row in document["contributors"]]
    manifest, patch = _proposal(repo.resolve(), decision_path, evidence_paths, ids, observations_path)
    if document != {**manifest, "manifest_digest": json_digest(manifest)} or patch_path.read_bytes() != patch:
        raise CampaignFusionError("fusion proposal bytes or lineage changed")
    return manifest
