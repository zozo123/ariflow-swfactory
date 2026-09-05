---
name: airflow-software-factory
description: Design, configure, operate, or audit a governed AI software delivery line on Apache Airflow that turns issues into reviewed pull requests. Use for swfactory blueprints, approval gates, agent sandbox selection, evidence chains, and Airflow deployment; not for ordinary ETL DAGs.
metadata:
  short-description: Build governed software factories on Airflow
---

# Airflow software factory

Build a change-manufacturing line, not a chatbot wrapped in a DAG.

Airflow is the control plane. It schedules work, expands issue-to-target jobs, pauses for human
decisions, retries infrastructure failures, records state, and exposes operations. The coding
agent is a replaceable worker inside a bounded cell. Git is the durable ledger. A human keeps the
merge key.

```text
ISSUE -> INTENT GATE -> SPEC -> PLAN GATE -> BUILD / VERIFY -> REVIEW
      -> VALIDATED PATCH -> PULL REQUEST -> HUMAN MERGE -> RUN METRICS
```

## Choose the job

Infer the narrowest useful mode from the request:

- **Demo:** run the scripted, keyless line to inspect behavior without model calls.
- **Adopt:** add `factory.toml`, select a blueprint, and connect one target repository.
- **Operate:** trigger, approve, inspect, retry, reject, or clean up existing runs.
- **Audit:** inspect trust boundaries, evidence, failure behavior, and deployment configuration.
- **Extend:** add a blueprint, stage policy, sandbox adapter, SCM adapter, or metric response band.

Before changing a repository, inspect `factory.toml`, the chosen blueprint, protected paths, the
test command, branch policy, and the deployment boundary. Preserve explicit user scope. Opening a
pull request does not imply permission to merge it; running a factory does not imply permission to
weaken protected paths or expose credentials.

## Model the line as policy

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
- rejected and blocked work remains visible; it is never relabeled as success.

Do not confuse that policy blueprint with Astronomer Blueprint. When a team uses Astronomer's
composer or Astro IDE, put its `software_factory` template outside the line: it may select an
existing line and pass issues/target filters, but it must trigger the governed child DAG rather
than recreate or remove its gates. Read `docs/astronomer-blueprint.md` in a swfactory checkout
before adding this composition layer.

Use the target's `factory.toml` as the command contract. Do not guess package managers, test
commands, source paths, or protected paths. Keep generated JUnit below `.factory/`.

## Pick the boundary deliberately

Read [references/sandboxes.md](references/sandboxes.md) whenever selecting, adding, or comparing a
sandbox provider. The critical question is what crosses the boundary: one command, the agent
process, or the whole Airflow task.

Never silently downgrade a requested sandbox. If a backend cannot enforce a requested network,
filesystem, lifetime, credential, or resource rule, stop with a configuration error.

## Produce evidence, not theater

For each job, preserve a reviewable chain under `docs/factory/<issue>/`:

- `intent.md`, `spec.md`, `plan.json`, and `plan.md`
- artifact-bound approvals
- structured review findings and verdict
- bounded agent envelopes and guard decisions
- verification counts, iterations, timings, cost, and final disposition

Keep authoritative gate, baseline, review, and cost state outside the agent-writable checkout until
delivery. Commit through the factory identity with run, stage, and agent provenance. The delivery
credential stays in the orchestrator, never in the coding cell.

## Operate the repository

Prefer the repository's own commands and documentation over copied instructions:

```bash
uv sync
uv run swfactory doctor
uv run swfactory demo
uv run swfactory run --blueprint factory --issue 42
uv run swfactory herd
uv run swfactory metrics
```

For production, use Airflow's API or GitHub webhook path, keep approval tasks assigned, and retain
Airflow logs plus the committed evidence chain. Use `swfactory doctor` before a live run. Do not run
the real agent in the local sandbox unless the user explicitly accepts that development escape
hatch.

## Handle failure as product output

Classify failures before retrying:

- retry transient sandbox, network, or SCM transport failures within the configured bound;
- do not retry invalid policy, missing evidence, rejected credentials, or an unenforceable sandbox
  specification;
- publish a clearly blocked or rejected result when policy allows delivery of failure evidence;
- stop before publication if the patch scope, artifact hashes, baseline, or workspace cleanliness
  cannot be proven.

When reporting the result, state the line used, target, boundary, gate decisions, verification
evidence, review disposition, PR URL if created, and anything intentionally not executed.
