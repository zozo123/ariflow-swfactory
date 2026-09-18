"""Canonical convergence decisions for evidence-backed fan-in."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


@dataclass(frozen=True)
class ConvergenceDecision:
    winner: str
    candidate_ids: tuple[str, ...]
    evidence_digests: tuple[str, ...]
    required_dimensions: tuple[str, ...]

    @property
    def digest(self) -> str:
        return "sha256:" + hashlib.sha256(_canonical(self.to_dict(include_digest=False))).hexdigest()

    def to_dict(self, *, include_digest: bool = True) -> dict[str, object]:
        data: dict[str, object] = {
            "schema_version": 1,
            "authority": "deterministic-fan-in",
            "winner": self.winner,
            "candidate_ids": list(self.candidate_ids),
            "evidence_digests": list(self.evidence_digests),
            "required_dimensions": list(self.required_dimensions),
        }
        if include_digest:
            data["digest"] = self.digest
        return data


def build_convergence_decision(
    rows: Iterable[Mapping[str, object]],
    *,
    winner: str,
    required_dimensions: Iterable[str],
) -> ConvergenceDecision:
    """Build a completion-order-independent fan-in record from retained evidence."""
    normalized: list[tuple[str, str]] = []
    for row in rows:
        candidate = str(row.get("logical_id") or "")
        evidence = str(row.get("evidence_digest") or "")
        if not candidate:
            raise ValueError("candidate logical_id is required")
        if not evidence.startswith("sha256:") or len(evidence) != 71:
            raise ValueError(f"candidate {candidate} lacks a canonical evidence digest")
        normalized.append((candidate, evidence))

    if not normalized:
        raise ValueError("convergence requires at least one candidate")
    if len({candidate for candidate, _ in normalized}) != len(normalized):
        raise ValueError("candidate identities must be unique")

    normalized.sort()
    candidate_ids = tuple(candidate for candidate, _ in normalized)
    if winner not in candidate_ids:
        raise ValueError("winner must be one of the candidate identities")

    dimensions = tuple(sorted(set(required_dimensions)))
    if not dimensions:
        raise ValueError("convergence requires at least one required dimension")

    return ConvergenceDecision(
        winner=winner,
        candidate_ids=candidate_ids,
        evidence_digests=tuple(evidence for _, evidence in normalized),
        required_dimensions=dimensions,
    )
