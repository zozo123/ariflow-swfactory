---
name: airflow-software-factory
description: "Design, configure, operate, audit, or extend the Airflow Software Factory, and use it from AI coding harnesses such as Codex, Claude Code, Grok, Cursor, or custom agents. Use when an agent must submit, inspect, approve, repair, review, or deliver governed factory work, or when it is launched inside a Factory Cell to produce specification, plan, implementation, test, fix, or review artifacts without bypassing Airflow scheduling, Cell epoch fencing, evidence, or publication authority."
---

# Airflow Software Factory

Treat the GitHub issue as the work order, the blueprint as the production route, Apache Airflow as the only lifecycle scheduler, and each issue x target Factory Cell as the durable identity/evidence boundary. Keep publication in the factory and keep the human merge key outside coding cells.

```text
WORK ORDER -> AIRFLOW ROUTE -> INTENT GATE -> SPEC -> PLAN GATE -> WORK CELL
           -> TESTS + REVIEW -> VALIDATED PATCH -> PULL REQUEST -> HUMAN MERGE
```

## Choose the operating mode

Use the narrowest mode that matches the request:

- **Harness:** submit and inspect governed work from Codex, Claude Code, Grok, Cursor, or another outer agent.
- **Inner stage:** execute one bounded spec, plan, build, fix, test, or review stage inside a Factory Cell.
- **Demo:** run the scripted keyless route to inspect behavior without model calls.
- **Adopt:** add `factory.toml`, select a blueprint, and connect one target repository.
- **Operate:** trigger, approve, inspect, retry, reject, reconcile, or clean up existing runs.
- **Audit:** inspect trust boundaries, evidence, failure behavior, and deployment configuration.
- **Extend:** add a blueprint, stage policy, sandbox adapter, SCM adapter, metric response band, or operator surface.

Before changing a repository, inspect `factory.toml`, the selected blueprint, protected paths, the test command, branch policy, and the deployment boundary. Preserve explicit user scope. Require separate authorization before merging, weakening protected paths, or exposing credentials.

## Harness mode

Use one stable factory-session identity and preserve it through retries.

```sh
scripts/swf_harness.sh <harness> <factory-id> --issue <issue> [submit args...]
```

Then use the normal operator surface:

```sh
swf jobs list
swf gates list
swf runs list
swf attention
swf deliveries list
```

Follow these invariants:

1. Reuse the same `factory-id` for retries of the same harness session.
2. Keep harness/session identity in admission, Cell evidence, and Airflow run configuration.
3. Never create a second lifecycle loop in the harness. Airflow owns lifecycle scheduling.
4. Never pass backend, GitHub, or provider credentials into coding sandboxes.
5. Answer human gates only when explicitly authorized.
6. Verify terminal evidence and delivery instead of trusting an agent's success message.

Read `docs/harnesses.md` for Codex, Claude, Grok, and custom-harness examples.

## Inner Factory Cell mode

When launched as one stage of a durable Cell:

1. Read `docs/factory/<issue>/intent.md` first and treat it as the scope authority.
2. Read `factory.toml`; obey protected paths, tool limits, and verification commands.
3. Stay within the current `cell_id` and positive epoch. Never mutate or reclaim another incarnation.
4. Touch only files authorized by the approved plan.
5. Do not commit, push, open a PR, publish, or promote yourself. The factory owns publication; parent authority owns promotion.

### Specification

Produce `spec.md` with numbered testable requirements, API behavior, error cases, edge conditions, backwards-compatibility expectations, and concrete correctness/security/performance/maintainability risks. Map every requirement to at least one planned test. Add no implementation code during this stage.

### Planning

Treat `plan.json` as the typed source and `plan.md` as its rendering. Include the complete file allowlist, ordered steps, named tests, and explicit risks. Keep `Plan.work` as a bounded issue-specific inner work graph; never turn it into another scheduler.

```json
{
  "files": ["src/calc/core.py", "tests/test_core.py"],
  "steps": ["implement percent_change", "add edge-case tests"],
  "tests": ["test_percent_change_increase", "test_percent_change_zero_old"],
  "risks": ["float comparisons -> use approximate assertions"]
}
```

### Build and fix

Implement only the approved plan. Prefer the smallest coherent change. Run the configured tests and record failures accurately. A fix may repair implementation or tests only when the approved intent requires it; never widen scope just to make CI green.

### Review

Read `REVIEW.md` at the target root and follow its output contract exactly. Review in this order: correctness, tests, security, plan fidelity, style. Use the repository's configured severities and nit cap. Do not soften blocking findings to force delivery.

## Define the production route

Treat a blueprint as executable governance. Keep stage behavior in code and deployment choices in TOML. Require these invariants:

- preserve originator intent verbatim;
- keep spec and plan read-only stages;
- bound build and review-fix loops;
- require typed plans for build/review work;
- fail closed when fresh verification evidence is absent, empty, or malformed;
- bind gates to approver, decision, timestamp, and artifact digest;
- allow delivery only from reviewed commits plus orchestrator-owned evidence;
- record reported, published, and independently verified as separate states;
- keep rejected and blocked work visible instead of relabeling it success.

Astronomer Blueprint may compose a software-factory route inside a larger workflow, but the child Airflow DAG keeps its approvals and lifecycle authority. Read `docs/astronomer-blueprint.md` before adding composition.

## Pick the sandbox boundary deliberately

Read [references/sandboxes.md](references/sandboxes.md) when selecting, adding, or comparing a provider. Never infer behavior from provider names. Admit only explicitly proven capabilities.

Never silently downgrade a requested sandbox. If a backend cannot enforce a requested filesystem, network, lifetime, credential, identity, cancellation, or teardown rule, fail closed with evidence.

## Preserve mutation and recovery authority

Treat `(cell_id, epoch, operation_key)` as the identity of every external mutation.

- Refuse stale epochs.
- Make exact replays idempotent.
- Reconcile ambiguous outcomes before another write.
- Reclaim only resources proven to belong to the current or terminal incarnation.
- Keep cleanup debt and unresolved mutation evidence visible.
- Never let a child factory self-promote.

## Keep a trace record

Preserve a reviewable chain under `docs/factory/<issue>/`:

- `intent.md`, `spec.md`, `plan.json`, and `plan.md`;
- artifact-bound approvals;
- structured review findings and verdict;
- bounded agent envelopes and guard decisions;
- verification counts, iterations, timings, cost, and final disposition.

Keep authoritative gate, baseline, review, and cost state outside the agent-writable checkout until delivery. Keep the publishing credential in the orchestrator, never in the coding cell.

## Operate through one operations layer

Every operator face belongs over one shared operations layer that owns validation, readiness, and writes. A face that implements its own policy creates a second authority. Read [references/operator-interface.md](references/operator-interface.md) before building or auditing an operator surface.

Prefer repository commands over copied instructions:

```sh
uv sync
uv run swfactory doctor
uv run swfactory demo
uv run swfactory run --blueprint factory --issue 42
uv run swfactory herd

swf doctor
swf submit --blueprint factory --issue 42
swf attention
swf gates list
swf deliveries verify --clone
swf tui
```

For production, use Airflow's API or the GitHub webhook path, retain Airflow logs plus committed evidence, and keep approval tasks durable. Do not run real agents in the local sandbox unless the user explicitly accepts that development escape hatch.

## Handle failure as product output

Classify failures before retrying:

- retry transient sandbox, network, or SCM transport failures within the configured bound;
- do not retry invalid policy, missing evidence, rejected credentials, stale epochs, or unenforceable sandbox requirements;
- publish clearly blocked/rejected evidence only when policy permits it;
- stop before publication if patch scope, artifact hashes, baseline, or workspace cleanliness cannot be proven.

Report the route, target, boundary, gate decisions, review disposition, delivery URL if created, and anything intentionally not executed. State evidence at the level it actually reached: reported, published, or independently verified.
