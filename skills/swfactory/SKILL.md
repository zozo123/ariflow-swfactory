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

Use one stable session pair `(harness, factory_id)` for the lifetime of the outer agent session.
Different harness processes or independent sessions use different factory ids, even when they drive
the same repository.

```bash
swf doctor
swf submit --harness codex --factory-id codex-session-17 --issue 1203
swf submit --harness claude --factory-id claude-session-22 --issue 1205
swf runs list
swf cells list
swf attention
```

The convenience wrapper is equivalent:

```bash
scripts/swf_harness.sh <harness> <factory-id> --issue <issue> [submit args...]
```

### Rules

- Reuse the exact session identity on retries so submission dedupe is deterministic.
- Many sessions may submit different issue x target Cells against the same repository concurrently.
- The same issue x target Cell has one writer. A competing session must be queued, fenced, or refused.
- Never invent stage transitions in the harness. Airflow schedules spec, plan, build, test, review,
  approval, delivery, and cleanup.
- Never pass backend, GitHub publication, or service credentials into stage sandboxes.
- Never push, force-push, open a PR, or publish from an inner stage agent. Publication belongs to the
  factory authority.
- Read status from `swf` instead of scraping Airflow internals.
- Answer HITL gates only when the user explicitly authorizes the answer.

## Driving hard on one repository

Independent harnesses can hammer one repository safely by keeping identity explicit:

```bash
SWF_HARNESS=codex SWF_FACTORY_ID=codex-a swf submit --issue 101
SWF_HARNESS=claude SWF_FACTORY_ID=claude-b swf submit --issue 102
SWF_HARNESS=custom SWF_FACTORY_ID=worker-03 swf submit --issue 103
```

Use `swf cells list`, `swf runs list`, `swf jobs list`, and `swf attention` as the shared observation
surface. Do not coordinate with shared mutable checkouts or ad-hoc lock files outside the factory.

## Inner stage mode

If Airflow launched the agent inside a Factory Cell, it is no longer an outer harness. Follow the
intent/spec/plan artifacts for that Cell, stay inside its sandbox/workspace, run the required checks,
and return the stage artifact. Do not schedule later stages or publish.

## Completion

Outer-harness work is complete only when the terminal Cell/run state and retained delivery evidence
agree. A successful-looking agent transcript is not evidence of publication.
