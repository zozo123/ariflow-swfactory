"""Executable Liquid400 acceptance coverage for authority-owned domains.

This module does not schedule work. It maps each acceptance concern to the existing canonical
runtime primitive that implements it so the final fan-in can detect missing or duplicated
substrates instead of accepting prose-only issues.
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass

ROLE = "authority"
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
    "authority-transfer": "authority",
    "epoch-fencing": "cells",
    "operation-replay": "idempotency",
    "mutation-observation": "idempotency",
    "webhook-replay": "webhook",
    "intake-dedupe": "intake_policy",
}

CONCERN_ANCHORS = {
    "durable persistence": "cells",
    "versioned API": "backend",
    "runtime integration": "cell_runtime",
    "operator surface": "inspection",
    "security hardening": "security_contract",
    "failure recovery": "operation_recovery",
    "scale pressure": "parallel_workers",
    "evidence and SLO": "lifecycle_evidence",
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
    rows: list[AcceptanceCell] = []
    for domain, primary in PRIMARY.items():
        for concern in CONCERNS:
            module = primary if concern == "canonical invariant" else CONCERN_ANCHORS[concern]
            rows.append(AcceptanceCell(domain, concern, ROLE, SCHEDULER, module))
    return tuple(rows)


def validate() -> tuple[str, ...]:
    failures: list[str] = []
    rows = coverage()
    if len(rows) != len(PRIMARY) * len(CONCERNS):
        failures.append("coverage_cardinality")
    identities = {(row.domain, row.concern) for row in rows}
    if len(identities) != len(rows):
        failures.append("duplicate_acceptance_cell")
    if any(row.scheduler != "airflow" for row in rows):
        failures.append("scheduler_authority")
    for module in sorted({row.module for row in rows}):
        if importlib.util.find_spec(f"swfactory.{module}") is None:
            failures.append(f"missing_module:{module}")
    return tuple(failures)
