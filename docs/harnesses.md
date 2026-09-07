# AI harness sessions

Codex CLI, Claude Code, Grok, and custom agent runners are **outer factory clients**. They submit
work into the same governed backend and Airflow DAGs; they do not become lifecycle schedulers.

## Session identity

A session is the pair `(harness, factory_id)`. Keep it stable for retries and multiple issue
submissions from the same outer agent session. Independent sessions must use different factory ids.

```bash
# Codex
scripts/swf_harness.sh codex codex-2026-09-07-a --issue 1204

# Claude Code
scripts/swf_harness.sh claude claude-2026-09-07-a --issue 1205

# Grok
scripts/swf_harness.sh grok grok-2026-09-07-a --issue 1207

# Generic/custom harness
scripts/swf_harness.sh custom bench-worker-03 --issue 1210 --target owner/repo
```

The wrapper is intentionally tiny. It only validates the two identity components and delegates to:

```bash
swf submit --harness <name> --factory-id <stable-id> ...
```

`SWF_BIN=/path/to/swf` selects a different binary. All ordinary `swf submit` arguments after the
first two wrapper arguments are passed through unchanged.

## What the outer harness may do

1. Submit one or more issues with its stable session identity.
2. Inspect runs/Cells through the normal `swf` operator surfaces.
3. Answer authenticated human-in-the-loop gates when explicitly authorized.
4. Verify retained delivery/evidence after the factory publishes.

It must not schedule stage transitions itself, mutate a stale Cell epoch, or give stage sandboxes
backend/service credentials.

## Inner-stage boundary

A process launched *inside* a factory stage is not an outer harness even if the executable happens
to be Claude, Codex, or another coding agent. Inner stages follow the factory's intent/spec/plan
artifacts and leave commit/PR/publication to the factory. This keeps one publication authority and
one Airflow lifecycle authority regardless of which model or agent program performs a stage.
