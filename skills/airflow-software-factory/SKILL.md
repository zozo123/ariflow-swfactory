---
name: airflow-software-factory
description: "Operate and extend an Apache Airflow software factory for coding agents: turn GitHub issues and work orders into tested, reviewed pull requests with human approval gates, durable Factory Cells, sandbox isolation, bounded work graphs, recovery, evidence and provenance, CI/CD, and Rust/Python operator surfaces. Use when adopting, configuring, running, debugging, deploying, auditing, or improving swfactory, factory.toml, Airflow blueprints, GitHub publication, agent sandboxes, or agentic software-delivery workflows. Keep Airflow as the sole lifecycle scheduler."
---

# Airflow software factory

Build a repeatable software production system on Apache Airflow.

Treat the GitHub issue as a work order, the blueprint as a production route, Airflow as the plant
scheduler, and each sandbox as one work cell. Airflow expands issue-to-target jobs, pauses for
human decisions, retries infrastructure failures, records state, and exposes operations. A coding
agent performs the bounded job. Git keeps the trace record. A human keeps the merge key.

```text
WORK ORDER -> ROUTE -> INTENT GATE -> SPEC -> PLAN GATE -> WORK CELL
           -> TESTS + REVIEW -> VALIDATED PATCH -> PULL REQUEST -> HUMAN MERGE
                                                               |
                                                               v
                                                     CONTINUOUS IMPROVEMENT
```

Install this skill from the public repository with Vercel's open `skills` CLI:

```bash
npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory
```

Use it without installing:

```bash
npx skills use zozo123/ariflow-swfactory@airflow-software-factory
```

Browse the public listing at `https://skills.sh/zozo123/ariflow-swfactory/airflow-software-factory`.

## Choose the job

Infer the narrowest useful mode from the request:

- **Demo:** run the scripted, keyless route to inspect behavior without model calls.
- **Adopt:** add `factory.toml`, select a blueprint, and connect one target repository.
- **Operate:** trigger, approve, inspect, retry, reject, or clean up existing runs.
- **Audit:** inspect trust boundaries, evidence, failure behavior, and deployment configuration.
- **Extend:** add a blueprint, stage policy, sandbox adapter, SCM adapter, or metric response band.

Before changing a repository, inspect `factory.toml`, the chosen blueprint, protected paths, the
test command, branch policy, and the deployment boundary. Preserve explicit user scope. Ask for
separate authorization before merging, weakening protected paths, or exposing credentials.

## Define the production route

A blueprint is executable governance. Keep stage behavior in code and deployment choices in TOML.
Require these invariants:

- `intent` is the originator's text, preserved verbatim.
- `spec` and `plan` are read-only agent stages.
- build loops and review-fix loops are bounded.
- every build or review line includes a typed plan.
- verification fails closed when its fresh JUnit evidence is absent, empty, or malformed.
- review reads a baseline-to-HEAD diff and cannot approve around a blocker.
- gates bind the approver, decision, timestamp, and exact artifact digest.
- delivery accepts only reviewed commits plus orchestrator-owned evidence.
- reported, published, and independently verified are recorded as three fields, never one.
- rejected and blocked work remains visible; it is never relabeled as success.

The swfactory blueprint defines one production route. Astronomer Blueprint can compose that route
inside a larger workflow. Its `software_factory` template selects an existing route and passes
issues or target filters to the child DAG, preserving the child's approvals. Read
`docs/astronomer-blueprint.md` before adding this composition layer.

Use the target's `factory.toml` as the command contract. Do not guess package managers, test
commands, source paths, or protected paths. Keep generated JUnit below `.factory/`.

## Pick the boundary deliberately

Read [references/sandboxes.md](references/sandboxes.md) whenever selecting, adding, or comparing a
sandbox provider. The critical question is what crosses the boundary: one command, the agent
process, or the whole Airflow task.

Never silently downgrade a requested sandbox. If a backend cannot enforce a requested network,
filesystem, lifetime, credential, or resource rule, stop with a configuration error.

## Keep a trace record

For each job, preserve a reviewable chain under `docs/factory/<issue>/`:

- `intent.md`, `spec.md`, `plan.json`, and `plan.md`
- artifact-bound approvals
- structured review findings and verdict
- bounded agent envelopes and guard decisions
- verification counts, iterations, timings, cost, and final disposition

Keep authoritative gate, baseline, review, and cost state outside the agent-writable checkout until
delivery. Commit through the factory identity with run, stage, and agent provenance. The delivery
credential stays in the orchestrator, never in the coding cell.

## Operate through one operations layer

Every operator face — a script, a terminal UI, a chat responder, a monitoring probe — belongs over
one shared operations layer that owns validation, readiness, and the write itself. A face that
decides for itself is a second policy, and only one of two policies ever gets fixed. Read
[references/operator-interface.md](references/operator-interface.md) before building or auditing
one: it covers gate readiness, the three levels of delivery evidence, exit codes a script can trust,
and paging a control plane that clamps `limit` without saying so.

Prefer the repository's own commands and documentation over copied instructions:

```bash
uv sync
uv run swfactory doctor                            # readiness, before any live run
uv run swfactory demo
uv run swfactory run --blueprint factory --issue 42
uv run swfactory herd                              # the Python control room
```

`swf` is the same factory from a native binary that needs no Python — `swf doctor`, `swf submit`,
`swf attention`, `swf gates approve`, `swf deliveries verify --clone`, `swf tui`. Its commands and
its keystrokes share one `Ops` layer (`rust/crates/swf-app/`), and `swf snapshot --json` must
describe the same factory as `swfactory herd --once --json` — `scripts/snapshot_diff.py` diffs the
two against one live server — so the two control rooms are held together by a diff rather than by
intent. See `docs/swf.md`.

For production, use Airflow's API or GitHub webhook path, keep approval tasks assigned, and retain
Airflow logs plus the committed evidence chain. Do not run the real agent in the local sandbox
unless the user explicitly accepts that development escape hatch.

## Handle failure as product output

Classify failures before retrying:

- retry transient sandbox, network, or SCM transport failures within the configured bound;
- do not retry invalid policy, missing evidence, rejected credentials, or an unenforceable sandbox
  specification;
- publish a clearly blocked or rejected result when policy allows delivery of failure evidence;
- stop before publication if the patch scope, artifact hashes, baseline, or workspace cleanliness
  cannot be proven.

When reporting the result, state the route used, target, boundary, gate decisions, review
disposition, PR URL if created, and anything intentionally not executed. State verification
evidence at the level it actually reaches — reported, published, or independently verified.

## Discover and install reusable skills

Keep skill discovery and installation on the trusted operator/orchestrator side. Skills are reusable
instructions and tools; they do not gain lifecycle, publication, or credential authority.

Use Vercel's canonical discovery skill when a reusable capability may already exist:

```bash
npx skills add vercel-labs/skills@find-skills -y
npx skills find <query> --owner vercel-labs
```

The repository exposes the same identities through `swfactory.skills_connector`, including the
factory skill, the outer-harness skill, and `vercel-labs/skills@find-skills`. Read
[references/skills-sh.md](references/skills-sh.md) before changing distribution metadata,
installing networked skills as part of an operator workflow, or diagnosing skills.sh indexing.
