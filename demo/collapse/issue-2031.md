---
id: COLLAPSE-2031
title: Delete the dead Liquid runtime pairs and the single-test shims
labels: [factory, selfhost]
---
## Problem

Fourteen modules in the `liquid_*` family have no live consumer.

**Dead — no consumer at all** (`grep -rl` over `src tests dags scripts`, excluding the family itself):
`liquid_airflow_runtime.py`, `liquid_authority_runtime.py`, `liquid_evidence_runtime.py`, `liquid_operator_runtime.py`, `liquid_recovery_runtime.py` (386 lines).

**Test-only shims** — each imported by exactly one ~380-byte test and nothing else:
`liquid_airflow.py`, `liquid_authority.py`, `liquid_evidence.py`, `liquid_operator.py`, `liquid_recovery.py`, `liquid_security.py`, `liquid_workgraph.py` (535 lines), with `tests/test_liquid_*.py`.

**`liquid_release.py` is imported by nothing at all.**

## Must survive

Two modules in this family are load-bearing and must not be touched:

- `liquid_security_runtime.py` — `Capability`, `SecurityContext` are imported by `src/swfactory/core_capabilities.py` and `src/swfactory/backend/scm_service.py`
- `liquid_workgraph_runtime.py` — `WorkNode` is imported by `src/swfactory/core_capabilities.py`

## Acceptance

- The five dead runtimes, the seven shims, their seven tests, and `liquid_release.py` are deleted.
- Any invariant sentence a deleted shim held is carried into the `invariant` field of the corresponding row in `config/liquid-spec.yaml` rather than lost.
- `uv run pytest`, `ruff check`, `ruff format --check` and `python -m swfactory.capability_inventory` all stay green.
- No `runtime_entry` or `test` field in `config/capability-inventory.json`, and no link in `docs/`, points at a deleted module.

Depends on the declarative spec landing first, so the invariants have somewhere to go.
