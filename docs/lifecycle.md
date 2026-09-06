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
topological wave are candidates to start from the same approved plan state. The graph reports them,
but the current executor still runs the governed build/review cell without claiming a provider can
clone a live sandbox.

A future native-fork executor must satisfy all of these before enabling parallel branches:

1. fork from an immutable, content-addressed parent state;
2. give every branch its own lease/TTL, budget and provenance;
3. never copy publishing credentials into a coding cell;
4. record the immutable parent checkpoint, branch id and evidence ids for every child;
5. never count sibling branches that reuse the same evidence as independent agreement;
6. merge branches deterministically and surface conflicts as evidence, not silent retries;
7. run the complete verification and review stream on the merged result;
8. tear down every child on success, failure, cancellation or scheduler recovery.

The lineage/evidence rule follows the core warning in *Evidence-Aware MapReduce for Forkable
Compute*: cheap snapshot branches do not make reused evidence independent. The managed graph marks
native forks as `lineage-required` so a future executor has an explicit compatibility contract.

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

This keeps the system KISS today while preserving a clean path to provider-native forks and more
parallel execution later.
