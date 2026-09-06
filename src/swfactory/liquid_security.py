"""Executable Liquid400 coverage for trust, isolation, policy and provenance domains."""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass

ROLE = "security"
SCHEDULER = "airflow"
CONCERNS = (
    "canonical invariant",
    "durable persistence",
    "versioned API",
    "runtime integration",
    "operator surface",
    "security hardening",
    "failure recovery",
    "scale pressure",
    "evidence and SLO",
    "E2E entropy collapse",
)
PRIMARY = {
    "artifact-integrity": "provenance",
    "secret-scope": "security_contract",
    "policy-drift": "security_boundary",
    "tenant-isolation": "worker_security",
    "provenance-sbom": "provenance",
    "backend-versioning": "contracts",
}
CONCERN_ANCHORS = {
    "durable persistence": "trust_evidence",
    "versioned API": "contracts",
    "runtime integration": "worker_security",
    "operator surface": "doctor",
    "security hardening": "security_contract",
    "failure recovery": "reconcile",
    "scale pressure": "parallel_workers",
    "evidence and SLO": "trust_evidence",
    "E2E entropy collapse": "ci_topology",
}


@dataclass(frozen=True)
class AcceptanceCell:
    domain: str
    concern: str
    owner_role: str
    scheduler: str
    module: str


def coverage() -> tuple[AcceptanceCell, ...]:
    return tuple(
        AcceptanceCell(
            domain,
            concern,
            ROLE,
            SCHEDULER,
            primary if concern == "canonical invariant" else CONCERN_ANCHORS[concern],
        )
        for domain, primary in PRIMARY.items()
        for concern in CONCERNS
    )


def validate() -> tuple[str, ...]:
    rows = coverage()
    failures: list[str] = []
    if len(rows) != 60 or len({(row.domain, row.concern) for row in rows}) != len(rows):
        failures.append("coverage_cardinality")
    for module in sorted({row.module for row in rows}):
        if importlib.util.find_spec(f"swfactory.{module}") is None:
            failures.append(f"missing_module:{module}")
    if any(row.scheduler != "airflow" for row in rows):
        failures.append("scheduler_authority")
    return tuple(failures)
