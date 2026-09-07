"""Rough append-only evidence, SLO and cost runtime for Liquid issue families."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EvidenceRecord:
    cell_id: str
    epoch: int
    kind: str
    payload: dict[str, Any]
    previous_digest: str = ""

    def digest(self) -> str:
        body = json.dumps(
            {
                "cell_id": self.cell_id,
                "epoch": self.epoch,
                "kind": self.kind,
                "payload": self.payload,
                "previous_digest": self.previous_digest,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(body).hexdigest()


class EvidenceChain:
    def __init__(self) -> None:
        self._records: list[EvidenceRecord] = []

    def append(self, *, cell_id: str, epoch: int, kind: str, payload: dict[str, Any]) -> str:
        previous = self._records[-1].digest() if self._records else ""
        record = EvidenceRecord(cell_id, epoch, kind, dict(payload), previous)
        self._records.append(record)
        return record.digest()

    def verify(self) -> bool:
        previous = ""
        for record in self._records:
            if record.previous_digest != previous:
                return False
            previous = record.digest()
        return True

    def export(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            {
                "cell_id": record.cell_id,
                "epoch": record.epoch,
                "kind": record.kind,
                "payload": record.payload,
                "previous_digest": record.previous_digest,
                "digest": record.digest(),
            }
            for record in self._records
        )


def slo_burn(*, failures: int, total: int, target: float) -> float:
    if total <= 0:
        return 0.0
    if not 0.0 < target < 1.0:
        raise ValueError("target must be between zero and one")
    actual_error = max(failures, 0) / total
    budget = 1.0 - target
    return actual_error / budget


def attributed_cost(*, compute: float = 0.0, storage: float = 0.0, model: float = 0.0, ci: float = 0.0) -> float:
    return sum(max(value, 0.0) for value in (compute, storage, model, ci))


def capability_claim(*, evidence_digest: str, observed: bool, measured_value: float | None = None) -> dict[str, Any]:
    if not evidence_digest:
        raise ValueError("capability claims require retained evidence")
    return {
        "evidence_digest": evidence_digest,
        "observed": observed,
        "measured_value": measured_value,
        "publishable": bool(observed),
    }
