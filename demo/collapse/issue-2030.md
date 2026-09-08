---
id: COLLAPSE-2030
title: Collapse the numbered Liquid bundles into a declarative spec + CI checker
labels: [factory, selfhost]
---
## Problem

`src/swfactory/liquid_bundle_01..18.py` are declarative coverage metadata expressed as Python modules. Each one is a `BundleSpec(id, lo, hi, (DomainSpec(slug, owner, runtime_module) x5), family)`, a `BUNDLE.validate()` call, and a two-line `run()` that forwards to `liquid_bundle_engine.execute`. They carry no behaviour of their own.

**Nothing imports them.** A grep over `src tests dags scripts` finds no importer outside the family. `liquid_bundle_engine.py` is imported only by those bundles, `liquid_release.py`, `physics_wave_runtime.py` and `tests/test_physics_wave_runtime.py`.

This is the thing `docs/liquid-methodology.md` C10 forbids: permanent Python architecture proportional to the number of issue slices. 58 of the 131 modules in `src/swfactory/` are `liquid_*`/`physics_*`/`legacy_*` — 44% of the module count for 11% of the lines.

## Acceptance

- `config/liquid-spec.yaml` declares the matrix once: the concern axis declared a single time, and per-domain rows carrying `slug`, `owner` (one of the seven canonical roles), `invariant`, `runtime_anchor`, `evidence`, and a `state` field so a row can be honestly marked aspirational.
- `src/swfactory/liquid_spec.py` loads and validates it, and **resolves every `runtime_anchor` to a real module/attribute under `src/swfactory/`** — this is the point: it makes the matrix falsifiable instead of decorative.
- Runs as `python -m swfactory.liquid_spec`, one-line JSON summary, non-zero exit on violation — the same contract as `python -m swfactory.capability_inventory`.
- Wired into the required `test` job in `.github/workflows/ci.yml`.
- `tests/test_liquid_spec.py`: the shipped spec validates; every anchor resolves; a bad owner / duplicate slug / dangling anchor / missing field is rejected.
- The 18 `liquid_bundle_*.py` and `liquid_bundle_engine.py` are deleted, and the declared coverage they encoded survives as data.

## Constraint

`blueprints/`, `dags/`, `config/capability-inventory.json` and the seven confinement modules are protected paths for a self-hosted work cell (see `factory.toml`). This issue only adds to `config/` and `src/swfactory/`, so it is inside a cell's writable surface — but `.github/workflows/ci.yml` is not. Land that one hunk as a human edit.
