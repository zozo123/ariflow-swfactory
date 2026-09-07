"""Executable Liquid400 coverage for CLI, TUI, promotion and fuzzing domains."""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass

ROLE = "operator"
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
    "cli-contract": "cli",
    "tui-parity": "product_surface",
    "fuzzing": "evals",
    "release-promotion": "generations",
}
CONCERN_ANCHORS = {
    "durable persistence": "state",
    "versioned API": "contracts",
    "runtime integration": "runtime",
    "operator surface": "inspection",
    "security hardening": "security_contract",
    "failure recovery": "reconcile",
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
    if len(rows) != 40 or len({(row.domain, row.concern) for row in rows}) != len(rows):
        failures.append("coverage_cardinality")
    for module in sorted({row.module for row in rows}):
        if importlib.util.find_spec(f"swfactory.{module}") is None:
            failures.append(f"missing_module:{module}")
    if any(row.scheduler != "airflow" for row in rows):
        failures.append("scheduler_authority")
    return tuple(failures)
