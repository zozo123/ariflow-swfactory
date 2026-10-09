# Compose factory lines with Astronomer Blueprint or dag-factory

Astronomer Blueprint and swfactory solve different layers of the system, so they compose cleanly:

```text
Astro IDE / DAG YAML
        |
        v
software_factory Blueprint step
        |
        | TriggerDagRunOperator (deferrable)
        v
swfactory line DAG
        |
        +-- issues x targets
        +-- human gates
        +-- isolated agent cells
        +-- evidence + reviewed pull request
```

The outer Blueprint is workflow composition. The inner swfactory line is delivery governance.
YAML can choose an existing line and its inputs; it cannot redefine stage semantics, budgets,
protected paths, approvals, or delivery authority.

## Install and load

The bridge targets `airflow-blueprint==0.4.0` and the repository's pinned Airflow stack:

```bash
uv sync --group airflow --group astronomer-blueprint
mkdir -p dags/composed
cp examples/astronomer-blueprint/loader.py dags/composed/
cp examples/astronomer-blueprint/product-change.dag.yaml dags/composed/
uv run blueprint lint
```

The package entry point publishes `SoftwareFactory` to Blueprint's registry as
`software_factory`. A composed YAML step looks like this:

```yaml
steps:
  manufacture_change:
    blueprint: software_factory
    line: factory
    issues: ["42", "43"]
    targets: ["your-org/your-repo"]
    wait_for_completion: true
```

| Field | Meaning |
|---|---|
| `line` | an existing swfactory DAG id generated from `blueprints/*.toml` |
| `issues` | one or more GitHub issue numbers or repo-relative Markdown issue files |
| `targets` | optional `owner/name` filter over targets declared by the line |
| `wait_for_completion` | defer the parent task until the child line finishes |
| `poke_interval_s` | child-state polling interval, 5–3600 seconds |

## Why a child DAG

- Dynamic mapping and HITL remain native, addressable Airflow tasks.
- The factory run has an independent retry, audit, and operator history.
- A visual composer cannot bypass policy by deleting an inner gate.
- The parent worker is released while a human decision is pending.
- Existing webhook, CLI, herd, and direct Airflow triggers keep one runtime path.

`wait_for_completion: true` reports whether the governed line completed operationally. A rejected
line deliberately completes after publishing rejection evidence, so this signal does not mean a
change was accepted or merged. Downstream release automation must inspect the factory disposition,
not equate child-DAG success with approval.

The Astronomer no-code Blueprint UI is currently preview and `airflow-blueprint` identifies itself
as alpha. The bridge is therefore an optional integration, not the factory's only DAG surface. It
was statically audited but not live-executed as part of this 2.0 delivery.

## dag-factory

[astronomer/dag-factory](https://github.com/astronomer/dag-factory) builds Airflow DAGs from YAML
and meets the line at the same boundary: its YAML owns the outer composition, and one
`TriggerDagRunOperator` (deferrable, waiting) starts the line DAG.

```bash
uv sync --group airflow
uv pip install "dag-factory>=1.0"
mkdir -p dags/composed
cp examples/dag-factory/loader.py examples/dag-factory/software_factory.yml dags/composed/
airflow dags list | grep product_change
```

`examples/dag-factory/software_factory.yml` triggers the `factory` line for `demo/issue.md` and
succeeds when the line finishes; as above, read the pull request for the verdict. dag-factory is
not a dependency of this repository and no test imports it; the example file is the whole
integration.

The line itself is not generated from dag-factory YAML. It is a mapped task group over
`(issue x target)` whose stages, gates and evidence tasks are derived from one blueprint and
checked against the runtime's approval policy (`tests/test_dag_parity.py`); a composition layer
that could express the line could also express it without its human gate. A direct trigger from a
composed DAG is an *unmanaged* submission, without the durable admission and Cell fencing of a
backend work order (`/v1/work-orders`): compose for convenience, admit through the backend.
