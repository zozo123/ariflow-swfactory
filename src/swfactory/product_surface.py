"""Level-1 product contract shared by backend, Rust operator and deployment health.

The document is intentionally boring/versioned: operators and deployment probes should consume the
same readiness/feature truth instead of inferring capabilities from a process version or route
presence.  Blueprint preview is read-only and creates neither Factory Cells nor Airflow runs.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class ContractVersions:
    api: int = 1
    cell: int = 1
    evidence: int = 1
    mutation: int = 1
    generation: int = 1


DEFAULT_FEATURES = (
    "factory_cells",
    "cell_history",
    "durable_admission",
    "operation_debt",
    "provider_capabilities",
    "blueprint_preview",
    "offline_evidence",
)


@dataclass(frozen=True)
class CapabilityDocument:
    schema_version: int
    contracts: ContractVersions
    features: tuple[str, ...]
    read_ready: bool
    mutation_ready: bool
    serving_generation: str | None
    draining_generation: str | None
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["features"] = sorted(set(self.features))
        return value


def capability_document(
    *,
    read_ready: bool,
    storage_authoritative: bool,
    schema_compatible: bool,
    draining: bool,
    serving_generation: str | None,
    draining_generation: str | None = None,
    features: Iterable[str] = DEFAULT_FEATURES,
    detail: str = "",
) -> CapabilityDocument:
    mutation_ready = bool(read_ready and storage_authoritative and schema_compatible and not draining)
    return CapabilityDocument(
        schema_version=1,
        contracts=ContractVersions(),
        features=tuple(sorted(set(features))),
        read_ready=read_ready,
        mutation_ready=mutation_ready,
        serving_generation=serving_generation,
        draining_generation=draining_generation,
        detail=detail[:1000],
    )


@dataclass(frozen=True)
class PreviewJob:
    job_idx: int
    issue: str
    target: str
    required_capabilities: tuple[str, ...]
    policy_inputs: tuple[str, ...]
    predicted_checks: tuple[str, ...] = ()


@dataclass(frozen=True)
class BlueprintPreview:
    schema_version: int
    line: str
    request_digest: str
    jobs: tuple[PreviewJob, ...]
    creates_state: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_preview(
    *,
    line: str,
    jobs: Iterable[Mapping[str, Any]],
    required_capabilities: Iterable[str] = (),
    policy_inputs: Iterable[str] = (),
    predicted_checks_by_target: Mapping[str, Iterable[str]] | None = None,
) -> BlueprintPreview:
    required = tuple(sorted(set(required_capabilities)))
    policy = tuple(sorted(set(policy_inputs)))
    checks = predicted_checks_by_target or {}
    rows: list[PreviewJob] = []
    normalized_request: list[dict[str, Any]] = []
    for index, raw in enumerate(jobs):
        issue = str(raw.get("issue") or raw.get("issue_ref") or "").strip()
        target = str(raw.get("target") or raw.get("repo") or "").strip()
        if not issue or not target:
            raise ValueError("preview jobs require issue and target")
        job_idx = int(raw.get("job_idx", index))
        predicted = tuple(sorted(set(str(v) for v in checks.get(target, ()))))
        rows.append(PreviewJob(job_idx, issue, target, required, policy, predicted))
        normalized_request.append({"job_idx": job_idx, "issue": issue, "target": target, "checks": predicted})
    payload = {
        "line": line,
        "jobs": normalized_request,
        "required_capabilities": required,
        "policy_inputs": policy,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return BlueprintPreview(1, line, digest, tuple(rows), False)
