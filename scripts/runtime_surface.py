#!/usr/bin/env python3
"""Inspect the explicit secondary runtime surfaces kept outside the hot path.

The software factory has a small primary execution path, plus a set of bounded
control, evidence, repository, provider, and experimental helpers.  This script
is the explicit entrypoint for those secondary surfaces: it makes their presence
intentional and inspectable without pretending that importing them puts them in
the managed Airflow lifecycle.

Use ``python scripts/runtime_surface.py`` for a compact inventory or ``--json``
for machine-readable output.  A module listed here is *reachable*, not
necessarily supported; capability support remains governed by
``config/capability-inventory.json``.
"""

from __future__ import annotations

import argparse
import json
from types import ModuleType

from swfactory import (
    airflow_binding,
    backend_store,
    ci_topology,
    contracts,
    evidence_release,
    evidence_store,
    evolution,
    fault_evidence,
    generations,
    harness_conformance,
    intake_policy,
    line_authoring,
    maintenance_incidents,
    parallel_workers,
    provider_conformance,
    public_capabilities,
    reconcile,
    repo_coordination,
    repo_runtime,
    repo_topology_runtime,
    worker_evidence,
    worker_security,
    workspace_materialization,
)

SURFACES: tuple[tuple[str, ModuleType, str], ...] = (
    ("airflow_binding", airflow_binding, "Airflow/Cell lifecycle binding helpers"),
    ("backend_store", backend_store, "optimistic backend state/lease store"),
    ("ci_topology", ci_topology, "targeted CI topology model"),
    ("contracts", contracts, "versioned compatibility contracts"),
    ("evidence_release", evidence_release, "candidate/release evidence contracts"),
    ("evidence_store", evidence_store, "durable operator-readable evidence bundles"),
    ("evolution", evolution, "bounded candidate campaigns"),
    ("fault_evidence", fault_evidence, "seeded fault/evidence plans"),
    ("generations", generations, "factory generation and promotion policy"),
    ("harness_conformance", harness_conformance, "multi-harness conformance scenarios"),
    ("intake_policy", intake_policy, "typed intake and deduplication policy"),
    ("line_authoring", line_authoring, "offline production-line validation/diff"),
    ("maintenance_incidents", maintenance_incidents, "maintenance incident identity/receipts"),
    ("parallel_workers", parallel_workers, "bounded seven-role inner worker pool"),
    ("provider_conformance", provider_conformance, "sandbox-provider conformance contract"),
    ("public_capabilities", public_capabilities, "provenance-backed public capability data"),
    ("reconcile", reconcile, "bounded mutation reconciliation"),
    ("repo_coordination", repo_coordination, "concurrent repository coordination"),
    ("repo_runtime", repo_runtime, "repository topology/materialization runtime"),
    ("repo_topology_runtime", repo_topology_runtime, "read-only topology discovery/planning"),
    ("worker_evidence", worker_evidence, "bounded-worker evidence ledger"),
    ("worker_security", worker_security, "least-privilege worker-role policy"),
    ("workspace_materialization", workspace_materialization, "workspace/cache planning"),
)


def document() -> dict[str, object]:
    rows = []
    for name, module, purpose in SURFACES:
        summary = ((module.__doc__ or "").strip().splitlines() or [purpose])[0]
        rows.append(
            {
                "module": f"swfactory.{name}",
                "purpose": purpose,
                "summary": summary,
            }
        )
    return {
        "schema_version": 1,
        "meaning": "reachable secondary surface; support is declared separately",
        "surfaces": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit one JSON document")
    args = parser.parse_args()
    payload = document()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    for row in payload["surfaces"]:
        print(f"{row['module']:<42} {row['purpose']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
