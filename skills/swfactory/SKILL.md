---
name: swfactory
description: Drive the Airflow software factory from any AI coding harness through the swf CLI. Use for Codex, Claude Code, Grok, Cursor, OpenCode, custom agents, or another outer harness that should submit, inspect, gate, and verify governed work without becoming a second scheduler.
---

# Software Factory

The AI harness is the entry point. `swf` is its control surface. Apache Airflow is the only lifecycle
scheduler. Factory Cells own durable issue x target identity and epoch fencing. Sandboxes are
disposable execution. The factory owns publication.

Install this skill with any skills.sh-compatible harness:

```bash
npx skills add zozo123/ariflow-swfactory --skill swfactory
```

## Outer harness mode

A session is the pair `(harness, factory_id)`. Keep it stable for the lifetime of the outer agent
session, across retries and every issue it submits. Different harness processes or independent
sessions use different factory ids, even when they drive the same repository.

```bash
swf doctor
swf submit --harness codex --factory-id codex-session-17 --issue 1203
swf submit --harness claude --factory-id claude-session-22 --issue 1205 --target owner/repo
swf runs list
swf cells list
swf attention
```

`swf submit` refuses half a pair, a harness name over 48 or a factory id over 64 characters, and
anything but ASCII letters, digits, dot, underscore and hyphen. The identity needs a factory-backend
context; direct Airflow submission refuses it rather than dropping it silently.

An outer harness may:

1. submit one or more issues with its stable session identity;
2. inspect runs, jobs and Cells through `swf`;
3. answer authenticated human-in-the-loop gates when the user explicitly authorizes the answer;
4. verify retained delivery and evidence after the factory publishes.

### Rules

- Reuse the exact session identity on retries so submission dedupe is deterministic.
- Many sessions may submit different issue x target Cells against the same repository concurrently.
- The same issue x target Cell has one writer. A competing session must be queued, fenced, or refused.
- Never invent stage transitions in the harness. Airflow schedules spec, plan, build, test, review,
  approval, delivery, and cleanup.
- Never mutate a Cell at a stale epoch.
- Never pass backend, GitHub publication, or service credentials into stage sandboxes.
- Never push, force-push, open a PR, or publish from an inner stage agent. Publication belongs to the
  factory authority.
- Read status from `swf` instead of scraping Airflow internals.

## Driving hard on one repository

Independent harnesses can hammer one repository safely by keeping identity explicit.
`SWF_HARNESS` and `SWF_FACTORY_ID` supply the pair when the flags are absent:

```bash
SWF_HARNESS=codex SWF_FACTORY_ID=codex-a swf submit --issue 101
SWF_HARNESS=claude SWF_FACTORY_ID=claude-b swf submit --issue 102
SWF_HARNESS=custom SWF_FACTORY_ID=worker-03 swf submit --issue 103
```

Use `swf cells list`, `swf runs list`, `swf jobs list`, and `swf attention` as the shared observation
surface. Do not coordinate with shared mutable checkouts or ad-hoc lock files outside the factory.

## Inner stage mode

If Airflow launched the agent inside a Factory Cell, it is no longer an outer harness, even when the
executable is Claude, Codex or another coding agent. Follow the intent/spec/plan artifacts for that
Cell, stay inside its sandbox/workspace, use only stage-granted tools and credentials, run the
required checks, and return the stage artifact. Do not schedule later stages or publish. Confusing
the two roles is an authority bug.

## Completion

Outer-harness work is complete only when the terminal Cell/run state and retained delivery evidence
agree. A successful-looking agent transcript is not evidence of publication.
