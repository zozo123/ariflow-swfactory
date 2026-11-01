---
name: airflow-software-factory
description: "Operate SWFactory, an Apache Airflow software factory that turns GitHub issues into tested, reviewed PRs with human gates, Factory Cells, sandboxes, and evidence. Use when adopting, configuring, running, debugging, deploying, auditing, or extending swfactory, factory.toml, Airflow blueprints, GitHub publication, agent sandboxes, or agentic issue-to-PR delivery. Keep Airflow as the sole lifecycle scheduler."
---

# Airflow software factory

Teach an agent how to adopt and operate **SWFactory**: GitHub issues become tested, reviewed pull
requests under Apache Airflow, with human approval gates and retained evidence.

```text
WORK ORDER -> BLUEPRINT ROUTE -> INTENT GATE -> SPEC -> PLAN GATE -> WORK CELL
           -> TESTS + REVIEW -> VALIDATED PATCH -> PULL REQUEST -> HUMAN MERGE
```

Airflow schedules the line. Factory Cells own durable issue × target identity and epoch fencing.
Coding agents run inside sandboxes. The factory backend owns publication. Humans keep the merge key
on the default line.

## When to use

Load this skill when the user asks to:

- install, demo, adopt, or deploy SWFactory / `ariflow-swfactory` / `swfactory`
- write or change `factory.toml`, a blueprint under `blueprints/`, or protected paths
- run, approve, inspect, retry, reject, or clean up factory work
- choose or debug a sandbox (`local`, `islo`, `srt`, `docker`, `toolset`, `boat`)
- audit evidence, gates, delivery verification, or concurrency fences
- extend the factory with a stage policy, adapter, or metric response band
- keep Airflow as the only lifecycle scheduler while coding agents do bounded stage work

Do **not** use this skill to invent a second scheduler, push from an inner stage agent, or put
backend/GitHub credentials into a coding sandbox. For driving an already-deployed factory from an
outer coding harness through `swf`, prefer the companion skill `swfactory`.

## Install

```bash
npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory
```

Use without installing:

```bash
npx skills use zozo123/ariflow-swfactory@airflow-software-factory
```

Catalog: https://skills.sh/zozo123/ariflow-swfactory/airflow-software-factory

## Verified quickstart

Requires Git, Python 3.12+, and [uv](https://docs.astral.sh/uv/). No model key, GitHub token,
Airflow, Docker, or Rust.

```bash
git clone https://github.com/zozo123/ariflow-swfactory.git
cd ariflow-swfactory
uv sync --locked
uv run swfactory demo
uv run swfactory state list
```

Expected outcome: the scripted replay changes a calculator target, fails a build, repairs it,
reviews the result, and delivers to a local Git remote. Demo gates auto-approve. The terminal prints
the stage table; machine-readable evidence lands under `.factory/<run_id>/` (including `report.json`
and `pr.md`). `state list` shows the completed local run.

## Choose a mode, then follow its steps

Infer the narrowest useful mode from the request.

### Demo

1. Run the quickstart above.
2. Open `.factory/<run_id>/pr.md` and `docs/factory/DEMO-1/` in the demo target checkout for the
   evidence chain (`intent.md`, `spec.md`, `plan.json`, review, approvals, metrics).
3. Stop here unless the user asked to adopt or operate a live factory.

### Adopt

1. Read the target's existing `factory.toml` (or add one). Do not guess `[commands].test`,
   `[commands].lint`, or `[paths].{junit,source,tests,protected}`.
2. Pick a blueprint under `blueprints/` (`factory` is the default SDLC line; `selfhost` targets this
   repository; others are specialized). Confirm sandbox and SCM choices in that TOML.
3. Connect one target repository and verify readiness:

```bash
uv run swfactory doctor --blueprint factory --repo owner/name
```

4. Ask before merging, weakening protected paths, or exposing credentials.

### Operate

1. Prefer repository commands over copied runbooks:

```bash
uv run swfactory doctor
uv run swfactory run --blueprint factory --issue 42
uv run swfactory herd --once --json
```

2. Answer intent/plan gates only with explicit user authorization (`swfactory approve` against a
   live Airflow HITL task, or `swf gates approve` / `swfactory herd --approve-all` when that is the
   intended control surface).
3. Inspect with `swfactory state …`, `swf attention`, `swf runs list`, `swf cells list`, and
   `swf deliveries verify --clone` when verifying retained delivery.
4. Never invent stage transitions in the harness. Airflow owns spec, plan, build, test, review,
   approval, delivery, and cleanup.

`swf` is the same operations layer as a native binary (no Python required on the operator laptop).
`swf snapshot --json` and `swfactory herd --once --json` must describe the same factory;
`scripts/snapshot_diff.py` diffs them against one live backend. See `docs/swf.md`.

### Audit

1. Confirm trust boundaries: credentials stay on the orchestrator; sandboxes stay credential-free
   for publication; protected paths match `factory.toml` and CI gates.
2. Trace one job under `docs/factory/<issue>/`: intent → spec → plan → approvals → review → metrics.
3. Check delivery evidence at the level actually reached: reported, published, or independently
   verified (three fields, never collapsed into one).
4. Read [references/concurrency-and-durability.md](references/concurrency-and-durability.md) before
   judging admission, epoch, publication, or restore behavior.

### Extend

1. Keep stage semantics in code; keep route and deployment choices in blueprint TOML.
2. Add or compare sandboxes only after reading [references/sandboxes.md](references/sandboxes.md).
   Never silently downgrade an unenforceable sandbox request: fail closed with a configuration error.
3. Compose an existing line from Astronomer Blueprint / dag-factory YAML only as an outer trigger;
   do not redefine gates or delivery authority. Read `docs/astronomer-blueprint.md`.
4. Preserve the invariants below.

## Production-route invariants

A blueprint is executable governance. Require:

- `intent` is the originator's text, preserved verbatim
- `spec` and `plan` are read-only agent stages
- build loops and review-fix loops are bounded
- every build or review line includes a typed plan (`plan.json` / `Plan` schema)
- verification fails closed when fresh JUnit evidence is absent, empty, or malformed
- review reads a baseline-to-HEAD diff and cannot approve around a blocker
- gates bind approver, decision, timestamp, and exact artifact digest
- delivery accepts only reviewed commits plus orchestrator-owned evidence
- reported / published / independently verified stay three distinct fields
- rejected and blocked work stays visible; never relabel it as success

## Examples

**User:** "Show me SWFactory without API keys."

```bash
uv sync --locked && uv run swfactory demo && uv run swfactory state list
```

Report the run id, stage table, and paths to `.factory/<run_id>/pr.md` and the evidence under
`docs/factory/DEMO-1/`.

**User:** "Wire issue 42 through the default factory line for `acme/widgets`."

1. `uv run swfactory doctor --blueprint factory --repo acme/widgets`
2. `uv run swfactory run --blueprint factory --issue 42 --repo acme/widgets` (or submit via
   `swf submit --harness <name> --factory-id <stable-session> --issue 42` when acting as an outer
   harness)
3. Watch gates with `swfactory herd` / `swf attention`; approve only with explicit authorization
4. After delivery, verify with `swf deliveries verify --clone` and cite the evidence level reached

**User:** "Is boat.dev a drop-in replacement for islo?"

Read [references/sandboxes.md](references/sandboxes.md). Compare boundary (agent cell vs command
tool), credential path, and what the repository actually ships. Do not claim live Claude-on-boat
unless the current adapter provides a scoped model credential path; scripted boat demos exercise
hosted VMs with fixtures.

## Failure handling

Classify before retrying:

- retry transient sandbox, network, or SCM transport failures within configured bounds
- do not retry invalid policy, missing evidence, rejected credentials, or unenforceable sandbox specs
- publish a clearly blocked or rejected result when policy allows failure evidence
- stop before publication if patch scope, artifact hashes, baseline, or workspace cleanliness cannot
  be proven

When reporting, state the route, target, boundary, gate decisions, review disposition, PR URL if
created, anything intentionally not executed, and the verification level actually reached.

## Operator surface and skill discovery

Every operator face (CLI, TUI, script, chat responder) should sit on one shared operations layer.
Read [references/operator-interface.md](references/operator-interface.md) before building or auditing
one.

Keep skill discovery on the trusted operator side. Skills never gain lifecycle or publication
authority:

```bash
npx skills add vercel-labs/skills@find-skills -y
npx skills find airflow --owner vercel-labs
```

`swfactory.skills_connector` exposes canonical identities for this skill, the outer-harness
`swfactory` skill, and `vercel-labs/skills@find-skills`. See
[references/skills-sh.md](references/skills-sh.md).
