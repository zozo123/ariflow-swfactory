"""SHA-bound candidate readiness evidence.

A green label is not evidence by itself.  This module records exactly which checkout was exercised,
which base it was integrated against, and the digest of each required artifact.  Missing, stale,
skipped or wrong-identity evidence fails closed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
SCHEMA_VERSION = 1
PASSING = frozenset({"success"})


class CandidateNotReady(RuntimeError):
    """Required candidate evidence is absent, stale or unsuccessful."""


@dataclass(frozen=True)
class CheckEvidence:
    name: str
    status: str
    head_sha: str
    base_sha: str
    tested_sha: str
    artifact: str
    artifact_digest: str
    required: bool = True

    def validate(self, *, head_sha: str, base_sha: str, tested_sha: str) -> None:
        if not self.name.strip():
            raise CandidateNotReady("check name is required")
        if self.required and self.status not in PASSING:
            raise CandidateNotReady(f"required check {self.name!r} is {self.status!r}")
        if self.head_sha != head_sha or self.base_sha != base_sha or self.tested_sha != tested_sha:
            raise CandidateNotReady(f"check {self.name!r} belongs to a different candidate/base checkout")
        if not self.artifact.strip() or not DIGEST_RE.fullmatch(self.artifact_digest):
            raise CandidateNotReady(f"check {self.name!r} has no retained artifact digest")


@dataclass(frozen=True)
class CandidateManifest:
    head_sha: str
    base_sha: str
    tested_sha: str
    checks: tuple[CheckEvidence, ...]
    schema_version: int = SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise CandidateNotReady(f"unsupported readiness schema {self.schema_version}")
        for label, sha in (("head", self.head_sha), ("base", self.base_sha), ("tested", self.tested_sha)):
            if not SHA_RE.fullmatch(sha):
                raise CandidateNotReady(f"{label} SHA must be a full lowercase git SHA")
        if not self.checks:
            raise CandidateNotReady("candidate has no evidence checks")
        names = [check.name for check in self.checks]
        if len(names) != len(set(names)):
            raise CandidateNotReady("candidate evidence contains duplicate check names")
        for check in self.checks:
            check.validate(head_sha=self.head_sha, base_sha=self.base_sha, tested_sha=self.tested_sha)

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "head_sha": self.head_sha,
            "base_sha": self.base_sha,
            "tested_sha": self.tested_sha,
            "checks": [asdict(check) for check in sorted(self.checks, key=lambda item: item.name)],
        }

    def digest(self) -> str:
        raw = json.dumps(self.canonical_dict(), sort_keys=True, separators=(",", ":")).encode()
        return "sha256:" + hashlib.sha256(raw).hexdigest()


def file_digest(path: Path) -> str:
    if not path.is_file():
        raise CandidateNotReady(f"required evidence artifact does not exist: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def build_manifest(
    *,
    head_sha: str,
    base_sha: str,
    tested_sha: str,
    required: list[tuple[str, Path]],
    advisory: list[tuple[str, str, Path]] | None = None,
) -> CandidateManifest:
    checks = [
        CheckEvidence(name, "success", head_sha, base_sha, tested_sha, str(path), file_digest(path), True)
        for name, path in required
    ]
    checks.extend(
        CheckEvidence(name, status, head_sha, base_sha, tested_sha, str(path), file_digest(path), False)
        for name, status, path in (advisory or [])
    )
    manifest = CandidateManifest(head_sha, base_sha, tested_sha, tuple(checks))
    manifest.validate()
    return manifest


def _required(value: str) -> tuple[str, Path]:
    name, sep, path = value.partition("=")
    if not sep or not name.strip() or not path.strip():
        raise argparse.ArgumentTypeError("required evidence must be NAME=PATH")
    return name.strip(), Path(path)


def _advisory(value: str) -> tuple[str, str, Path]:
    name, sep, tail = value.partition("=")
    status, sep2, path = tail.partition(":")
    if not sep or not sep2 or not name.strip() or not status.strip() or not path.strip():
        raise argparse.ArgumentTypeError("advisory evidence must be NAME=STATUS:PATH")
    return name.strip(), status.strip(), Path(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create and verify SHA-bound candidate evidence")
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--tested-sha", required=True)
    parser.add_argument("--required", action="append", default=[], type=_required)
    parser.add_argument("--advisory", action="append", default=[], type=_advisory)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    manifest = build_manifest(
        head_sha=args.head_sha,
        base_sha=args.base_sha,
        tested_sha=args.tested_sha,
        required=args.required,
        advisory=args.advisory,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    document = manifest.canonical_dict()
    document["manifest_digest"] = manifest.digest()
    args.out.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(document["manifest_digest"])
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI seam
    raise SystemExit(main())
