"""Executable Liquid400 coverage for evidence, observability, SLO and cost domains."""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass

ROLE = "evidence"
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
    "evidence-sealing": "evidence_store",
    "otel-correlation": "metrics",
    "slo-budget": "metrics",
    "cost-attribution": "metrics",
    "benchmarks": "evals",
    "stabilization-health": "trust_evidence",
}
CONCERN_ANCHORS = {
    "durable persistence": "evidence_store",
    "versioned API": "backend",
    "runtime integration": "lifecycle_evidence",
    "operator surface": "inspection",
    "security hardening": "trust_evidence",
    "failure recovery": "fault_evidence",
    "scale pressure": "metrics",
    "evidence and SLO": "metrics",
    "E2E entropy collapse": "evals",
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
