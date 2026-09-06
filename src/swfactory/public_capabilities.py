"""Generate provenance-backed public capability/benchmark data from retained evidence."""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

VALID_STATES = {"supported", "experimental", "not_tested", "not_supported", "non_blocking_ci"}


def validate_capability(row: dict[str, Any]) -> None:
    required = {"provider", "capability", "state", "source_sha", "measured_at", "evidence"}
    missing = required - set(row)
    if missing:
        raise ValueError(f"missing capability provenance fields: {sorted(missing)}")
    if row["state"] not in VALID_STATES:
        raise ValueError(f"invalid capability state: {row['state']}")
    if row["state"] == "supported" and not row["evidence"]:
        raise ValueError("supported capabilities require evidence")


def validate_benchmark(row: dict[str, Any]) -> None:
    required = {
        "name",
        "source_sha",
        "measured_at",
        "environment",
        "workload",
        "samples",
        "methodology",
        "evidence",
        "value",
    }
    missing = required - set(row)
    if missing:
        raise ValueError(f"missing benchmark provenance fields: {sorted(missing)}")
    if int(row["samples"]) <= 0:
        raise ValueError("benchmark samples must be positive")


def build_public_document(
    capabilities: Iterable[dict[str, Any]], benchmarks: Iterable[dict[str, Any]]
) -> dict[str, Any]:
    capabilities = list(capabilities)
    benchmarks = list(benchmarks)
    for row in capabilities:
        validate_capability(row)
    for row in benchmarks:
        validate_benchmark(row)
    return {
        "schema_version": 1,
        "capabilities": sorted(capabilities, key=lambda r: (r["provider"], r["capability"])),
        "benchmarks": sorted(benchmarks, key=lambda r: r["name"]),
    }


def write_public_data(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
