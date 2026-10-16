# Managed lifecycle graphs

`swfactory` uses **fixed Airflow DAGs for lifecycle control** and a **validated dynamic graph as
issue data**. It does not write a new Python DAG file for every GitHub issue.

That separation is deliberate. Airflow remains responsible for scheduling, retries, human gates,
worker limits and recovery. The issue graph describes the work that is different for each change.
Keeping runtime graph shape out of DAG parsing avoids scheduler churn, unbounded DAG cardinality and
per-issue deployment races on large repositories.

## The managed roles

The default production line maps to explicit responsibilities:

| Role | Factory responsibility | Execution today |
| --- | --- | --- |
| `issue_maker` | Supplies the GitHub work order / intent | External source (human, GitHub automation or governed maintenance producer) |
| `groomer` | Turns intent into a bounded specification | `spec` Airflow stage |
| `planner` | Produces and validates `plan.json` | `plan` Airflow stage |
| `code_writer` | Implements one node of the issue graph | Inside the bounded `build_and_test` stage |
| `reviewer` | Reviews diff, tests and plan fidelity | `review` Airflow stage |
| `improver` | Repairs blocking review findings | Bounded fix loop inside `review` |
| `deliverer` | Publishes the evidence chain and PR | `deliver` Airflow stage |

Automatic GitHub issue creation is intentionally **not** an implicit side effect of the planner.
An issue-maker integration must be an explicit producer with its own credentials, quotas and
policy. Existing GitHub/webhook intake remains the source of truth for normal work orders.

## Fixed control graph, dynamic issue graph

The scheduler graph stays stable:

```text
fan_out
  -> job[setup -> intent -> gate -> spec -> plan -> gate
         -> build_and_test -> review -> deliver -> metrics -> teardown]
```

`plan.json` may now include a `work` DAG:

```json
{
  "files": ["src/api.py", "tests/test_api.py", "docs/api.md"],
  "steps": ["add failing tests", "implement endpoint", "document behavior"],
  "tests": ["R1: endpoint returns the required schema"],
  "risks": ["API compatibility"],
  "work": [
    {
      "id": "tests",
      "title": "Add contract tests",
      "role": "code_writer",
      "depends_on": [],
      "files": ["tests/test_api.py"],
      "tests": ["R1: endpoint returns the required schema"],
      "parallel_safe": true
    },
    {
      "id": "api",
      "title": "Implement the endpoint",
      "role": "code_writer",
      "depends_on": ["tests"],
      "files": ["src/api.py"],
      "tests": ["R1: endpoint returns the required schema"],
      "parallel_safe": false
    },
    {
      "id": "docs",
      "title": "Document the endpoint",
      "role": "code_writer",
      "depends_on": ["tests"],
      "files": ["docs/api.md"],
      "tests": [],
      "parallel_safe": true
    }
  ]
}
```

The boundary rejects duplicate ids, missing dependencies, cycles and work-node files that were not
already declared in `plan.files`. Legacy plans without `work` remain valid and are represented as
one serial code-writer node.

Render any stored plan as the full managed lifecycle graph:

```sh
uv run python -m swfactory.lifecycle \
  docs/factory/42/plan.json --issue 42 --format mermaid
```

The projection includes the fixed issue-maker, groomer, planner, reviewer, improver and deliverer
nodes around the dynamic work nodes. JSON output is the default.

## Forkable sandboxes: capability, not fiction

`parallel_safe = true` is a **fork hint**. Multiple parallel-safe work nodes in the same
topological wave are candidates to start from the same approved plan state. A hint is always safe
to ignore: serial execution in the one governed workspace remains semantically correct, and it is
the default.

`swfactory.fork_wave` is the experimental executor for the hint. Inside the Airflow build task it
fans one wave out over provider forks and back in. A wave forks only when every gate holds, and
the reason is retained either way in `workgraph-execution.json` (`wave_decisions`):

- the line opted in with `[limits] max_parallel_forks` (or `SWF_MAX_PARALLEL_FORKS`) above 1;
- the configured provider's capability document says `fork` **and** its adapter implements
  `ForkableSandbox` (today only `boat`, whose fork was observed against a live account);
- the wave has at least two pending nodes, every one `parallel_safe`, with no declared overlap;
- the agent is not the external launcher;
- the run budget can pay every sibling's full per-stage grant at once.

How each rule a native-fork executor must satisfy is held:

1. **Immutable parent.** The parent is parked at the wave's checkpoint SHA (boat forks only a
   completed snapshot, so parking means `stop`). Every child proves `HEAD == checkpoint` on a
   clean tree before an agent runs; a child forked from an older periodic snapshot is refused.
2. **Own lease, budget and provenance.** Each child gets an explicit TTL no longer than the stage,
   its own agent call number and budget reservation, and an `Idempotency-Key` bound to run, wave,
   node, call and checkpoint.
3. **No publishing credentials.** Children are forks of a credential-free cell (boat `noEnv`);
   published commits and publication happen only in the parent.
4. **Lineage.** The journal records parent, checkpoint, child id, key, input/output HEAD and the
   patch blob of every child; each fanned-in node receipt carries that `fork` lineage.
5. **No false agreement.** Siblings do different work from one shared parent; receipts name the
   shared `fork_parent`, and nothing counts siblings as independent evidence.
6. **Deterministic merge.** `WorkExecutor` classifies stale bases, undeclared or protected writes
   and sibling overlap before any merge. Patches are applied in node-id order, and each applied
   result must reproduce the child's paths, modes and blob ids, or the merge is refused.
7. **Complete verification.** Nothing is verified in a child; the stage's test and repair loop
   runs on the merged head, then review runs as for any other change.
8. **Teardown.** Each child is reaped in its runner's `finally`; a failed node halts its live
   siblings at the provider; a retried task reaps every journalled child (one whose id was lost
   is found again by its idempotency key) and unparks the parent before anything else runs.

`tests/test_fork_wave_e2e.py` drives the real setup and build stages over boat's measured
lifecycle, and runs the same scenario on boat.dev with `SWF_TEST_LIVE_BOAT=1`. Support stays
`experimental` until retained managed workloads show the wall-clock and spend trade against serial
execution and a second provider implements the same contract.

The lineage/evidence rule follows the core warning in *Evidence-Aware MapReduce for Forkable
Compute*: cheap snapshot branches do not make reused evidence independent. The managed graph marks
native forks as `lineage-required` so the executor has an explicit compatibility contract.

The existing `sandbox.snapshot` option is a warm-start image, not a live worktree fork. The two
concepts must not be conflated.

## Scale model

For a repository with many developers, issues and CI flows, scale comes from bounded mapped work,
not DAG proliferation:

- DAG definitions are O(number of installed lifecycle blueprints), not O(number of issues).
- One run fans out `issue × target` jobs with `max_parallel_jobs` enforcing cell concurrency.
- The issue graph is capped at 64 work nodes and validated before use.
- Airflow owns retries and recovery; Rust remains an operator/control surface, not a scheduler.
- Every issue retains its own artifact chain, approvals, budget and sandbox identity.
- Fork hints are safe to ignore: serial execution remains semantically correct.
- A forked wave holds at most `max_parallel_forks` child cells, each with its own lease.

This keeps the default path KISS while forked waves remain an opt-in, evidence-bound experiment.
