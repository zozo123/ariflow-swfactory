# Compose factory lines with dag-factory

[astronomer/dag-factory](https://github.com/astronomer/dag-factory) builds Airflow DAGs from YAML.
swfactory already has a declarative surface of its own — `blueprints/*.toml`, from which
`dags/blueprints.py` emits one line DAG per file — so the two meet the same way the
[Astronomer Blueprint bridge](astronomer-blueprint.md) does: dag-factory owns the **outer**
composition, the line DAG owns **delivery governance**, and the boundary between them is a
`TriggerDagRunOperator`.

```text
dag-factory YAML  ->  run_factory_line (TriggerDagRunOperator, deferrable)  ->  swfactory line DAG
                                                                                 issues x targets
                                                                                 human gates
                                                                                 isolated agent cells
                                                                                 evidence + reviewed PR
```

## Why not generate the line itself from dag-factory YAML

The question was considered and answered no, for three reasons that are each sufficient:

- **The line is not a task list.** It is a mapped task group over `(issue x target)` whose stages,
  gates (`ApprovalOperator` / HITL) and evidence tasks are derived from one blueprint, checked at
  parse time to agree with the runtime's approval policy (`tests/test_dag_parity.py`). dag-factory's
  vocabulary describes operators and dependencies; the line's invariants live in the derivation.
- **YAML must not be able to remove a gate.** A composition layer that can express the line can also
  express the line minus its human gate. Keeping the line behind a trigger makes every gate, budget,
  protected path and delivery credential unaddressable from the outside.
- **One scheduler, one authority.** Airflow remains the only lifecycle scheduler; the backend's
  managed work orders (`/v1/work-orders`) remain the admission authority. A direct trigger from a
  composed DAG is an *unmanaged* submission — accepted, marked so, and given none of the durable
  admission or Cell fencing a managed one has. Compose for convenience; admit through the boundary.

## Use it

```bash
uv sync --group airflow
uv pip install "dag-factory>=1.0"
mkdir -p dags/composed
cp examples/dag-factory/loader.py examples/dag-factory/software_factory.yml dags/composed/
airflow dags list | grep product_change
```

`examples/dag-factory/software_factory.yml` triggers the `factory` line for `demo/issue.md`, waits
deferrably, and succeeds when the line finishes. Read the pull request for the verdict: a line that
ended **blocked** or **rejected** has still run to completion and published its evidence.

dag-factory is not a dependency of this repository and no test here imports it; the example is a
file you copy, and it is the whole integration.
