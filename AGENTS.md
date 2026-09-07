# AI harness entrypoint

This repository is an Apache Airflow software factory. When acting as an **outer coding harness**
(Codex CLI, Claude Code, Grok, or another agent runner), do not create a second lifecycle loop and
do not impersonate an inner stage worker.

## Submit factory work

Use one stable factory-session identity for the lifetime of the outer harness session:

```bash
scripts/swf_harness.sh codex codex-session-17 --issue 1204
scripts/swf_harness.sh codex codex-session-17 --issue 1205 --target owner/repo
```

The wrapper delegates to `swf submit --harness ... --factory-id ...`. Reuse the same factory id for
retries from the same session; use a different id for an independent session.

## Authority rules

- Apache Airflow is the only lifecycle scheduler.
- Factory Cell identity + positive epoch is the external-mutation authority.
- The outer harness submits, inspects, answers human gates, and verifies evidence; it does not run a
  competing stage scheduler.
- Inner stage agents follow the factory artifacts and **never push, commit, or open PRs directly**.
  The factory owns publication.
- Never pass backend/service credentials into stage sandboxes.
- If harness identity cannot be preserved, fail rather than silently submit as an anonymous actor.

## Parallel physics/control models

The repository intentionally evaluates multiple advisory models over the same Cell trajectory:
hydrodynamics, phase transitions, equilibrium and non-equilibrium statistical mechanics,
information theory, critical phenomena, control theory, complex reaction/protein networks,
runaway-chain/nuclear criticality, discrete operator algebra, and high-assurance safety analysis.

These models may disagree. Preserve that disagreement as evidence; do not average away a hard
failure. They may recommend expand/mix/hold/throttle/repair/isolate/crystallize, but **none of them
owns lifecycle scheduling or external mutation authority**. Hard Cell/epoch/security/evidence rules
win before any statistical score. Read `docs/physics-operating-model.md` before extending these
models, and use `src/swfactory/physics_registry.py` plus `src/swfactory/physics_fusion.py` rather than
creating a competing control framework.

For concrete Codex/Claude/Grok/custom examples, read `docs/harnesses.md`.
