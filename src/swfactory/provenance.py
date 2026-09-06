"""Release artifact provenance and verification manifest helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class ArtifactDigest:
    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True)
class ReleaseProvenance:
    source_sha: str
    builder: str
    workflow: str
    artifacts: tuple[ArtifactDigest, ...]
    sboms: tuple[str, ...] = ()
    attestations: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "schema_version": 1,
            "source_sha": self.source_sha,
            "builder": self.builder,
            "workflow": self.workflow,
            "artifacts": [asdict(a) for a in self.artifacts],
            "sboms": list(self.sboms),
            "attestations": list(self.attestations),
        }


def digest(path: Path) -> ArtifactDigest:
    data = path.read_bytes()
    return ArtifactDigest(str(path), hashlib.sha256(data).hexdigest(), len(data))


def manifest(
    source_sha: str, builder: str, workflow: str, paths: Iterable[Path]
) -> ReleaseProvenance:
    return ReleaseProvenance(source_sha, builder, workflow, tuple(digest(p) for p in paths))


def write(path: Path, provenance: ReleaseProvenance) -> None:
    path.write_text(
        json.dumps(provenance.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def verify(root: Path, provenance: ReleaseProvenance) -> tuple[bool, tuple[str, ...]]:
    failures = []
    for artifact in provenance.artifacts:
        path = root / artifact.path
        if not path.exists():
            failures.append(f"missing:{artifact.path}")
            continue
        actual = digest(path)
        if actual.sha256 != artifact.sha256 or actual.bytes != artifact.bytes:
            failures.append(f"mismatch:{artifact.path}")
    return not failures, tuple(failures)
