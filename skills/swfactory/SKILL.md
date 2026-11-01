---
name: swfactory
description: Drive SWFactory from an outer AI coding harness through the swf CLI. Use when Codex, Claude Code, Cursor, Grok, OpenCode, or a custom agent should submit, inspect, gate, and verify Airflow-governed work with a stable (harness, factory_id) session without becoming a second scheduler.
---

# Software Factory harness

The AI harness is the entry point. `swf` is its control surface. Apache Airflow is the only lifecycle
scheduler. Factory Cells own durable issue × target identity and epoch fencing. Sandboxes are
disposable execution. The factory owns publication.

## When to use

- an outer agent should submit GitHub issues into an already configured SWFactory deployment
- the user mentions `swf submit`, harness identity, Factory Cells, gates, or delivery verification
- multiple harness sessions must hammer one repository without inventing locks outside the factory
- Airflow already launched work and the outer session must only observe or answer authorized gates

If Airflow launched this agent **inside** a Factory Cell, switch to the inner stage instructions in
`.claude/skills/swfactory/SKILL.md` instead. Do not schedule later stages or publish from inside a
cell.

## Install

```bash
npx skills add zozo123/ariflow-swfactory --skill swfactory
```

Companion adoption/operations skill:

```bash
npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory
```

## Verified local baseline

Without a deployed backend, prove the factory line with the keyless replay from the repository root:

```bash
uv sync --locked
uv run swfactory demo
uv run swfactory state list
```

That path uses `swfactory`, not `swf`. Use `swf` only after a backend context exists (`swf doctor`
must pass).

## Outer harness mode

A session is the pair `(harness, factory_id)`. Keep it stable for the lifetime of the outer agent
session, across retries and every issue it submits. Different harness processes use different
factory ids even against the same repository.

```bash
swf doctor
swf submit --harness codex --factory-id codex-session-17 --issue 1203
swf submit --harness claude --factory-id claude-session-22 --issue 1205 --target owner/repo
swf runs list
swf cells list
swf attention
```

`swf submit` refuses half a pair, a harness name over 48 or a factory id over 64 characters, and
anything but ASCII letters, digits, dot, underscore, and hyphen. The identity needs a
factory-backend context; direct Airflow submission refuses it rather than dropping it silently.

An outer harness may:

1. submit one or more issues with its stable session identity
2. inspect runs, jobs, and Cells through `swf`
3. answer authenticated human-in-the-loop gates when the user explicitly authorizes the answer
4. verify retained delivery and evidence after the factory publishes

### Rules

- Reuse the exact session identity on retries so submission dedupe is deterministic.
- Many sessions may submit different issue × target Cells against the same repository concurrently.
- The same issue × target Cell has one writer. A competing session must be queued, fenced, or refused.
- Never invent stage transitions in the harness. Airflow schedules spec, plan, build, test, review,
  approval, delivery, and cleanup.
- Never mutate a Cell at a stale epoch.
- Never pass backend, GitHub publication, or service credentials into stage sandboxes.
- Never push, force-push, open a PR, or publish from an inner stage agent. Publication belongs to the
  factory authority.
- Read status from `swf` instead of scraping Airflow internals.

## Driving hard on one repository

`SWF_HARNESS` and `SWF_FACTORY_ID` supply the pair when the flags are absent:

```bash
SWF_HARNESS=codex SWF_FACTORY_ID=codex-a swf submit --issue 101
SWF_HARNESS=claude SWF_FACTORY_ID=claude-b swf submit --issue 102
SWF_HARNESS=custom SWF_FACTORY_ID=worker-03 swf submit --issue 103
```

Use `swf cells list`, `swf runs list`, `swf jobs list`, and `swf attention` as the shared observation
surface. Do not coordinate with shared mutable checkouts or ad-hoc lock files outside the factory.

## Examples

**User:** "Submit issue 1204 from this Codex session."

```bash
swf submit --harness codex --factory-id codex-session-17 --issue 1204
swf attention
```

Reuse the same `--harness` / `--factory-id` on every retry in that session.

**User:** "Did the factory actually publish?"

```bash
swf deliveries verify --clone
```

Treat terminal Cell/run state plus retained delivery evidence as completion. A successful-looking
agent transcript is not publication evidence.

## Completion

Outer-harness work is complete only when the terminal Cell/run state and retained delivery evidence
agree.
