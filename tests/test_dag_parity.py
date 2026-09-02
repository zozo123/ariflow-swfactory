"""The DAG is the pipeline: task ids mirror ``stages.PIPELINE``; gates are ApprovalOperators.

Runs only with the ``airflow`` dependency group:
``uv run --group airflow pytest tests/test_dag_parity.py``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

REPO = Path(__file__).resolve().parents[1]
DAGS = REPO / "dags"

EXPECTED_TASKS = [
    "setup",
    "intent",
    "approve_intent",
    "record_intent",
    "spec",
    "plan",
    "approve_plan",
    "record_plan",
    "build_and_test",
    "review",
    "deliver",
    "metrics",
    "teardown",
]
STAGE_TASKS = ["intent", "spec", "plan", "build_and_test", "review", "deliver"]


def _dagbag():
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:  # pragma: no cover - older 3.x layout
        from airflow.models.dagbag import DagBag
    # Airflow 3.3 dropped `include_examples`; AIRFLOW__CORE__LOAD_EXAMPLES=False covers it.
    return DagBag(dag_folder=str(DAGS))


@pytest.fixture(scope="module")
def dagbag(tmp_path_factory: pytest.TempPathFactory):
    os.environ["AIRFLOW_HOME"] = str(tmp_path_factory.mktemp("airflow_home"))
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    return _dagbag()


@pytest.fixture(scope="module")
def factory_dag(dagbag):
    assert not dagbag.import_errors, dagbag.import_errors
    return dagbag.dags["factory"]  # .dags, not get_dag(): no metadata DB in unit tests


def _linear_order(dag) -> list[str]:
    """Follow the single downstream edge from the root; the DAG must be a straight line.

    The teardown task is excluded from the walk (Airflow wires it to its setup task as well)
    and appended last after checking it hangs off the final task.
    """
    roots = [t for t in dag.tasks if not t.upstream_task_ids]
    assert len(roots) == 1, [t.task_id for t in roots]
    order, current = [], roots[0]
    for _ in range(len(dag.tasks)):
        order.append(current.task_id)
        nxt = sorted(t for t in current.downstream_task_ids if t != "teardown")
        if not nxt:
            break
        assert len(nxt) == 1, f"{current.task_id} fans out to {nxt}"
        current = dag.get_task(nxt[0])
    teardown = dag.get_task("teardown")
    assert current.task_id in teardown.upstream_task_ids
    return [*order, "teardown"]


def test_dags_import_cleanly(dagbag) -> None:
    assert not dagbag.import_errors, dagbag.import_errors
    assert {"factory", "maintain"} <= set(dagbag.dag_ids)


def test_factory_task_ids_in_order(factory_dag) -> None:
    assert _linear_order(factory_dag) == EXPECTED_TASKS
    assert sorted(factory_dag.task_ids) == sorted(EXPECTED_TASKS)


def test_factory_dag_shape(factory_dag) -> None:
    assert factory_dag.schedule is None
    assert factory_dag.catchup is False
    assert "issue" in factory_dag.params
    teardown = factory_dag.get_task("teardown")
    assert teardown.is_teardown  # survives an ApprovalOperator reject (teardowns are not skipped)
    assert str(teardown.trigger_rule.value) in {"all_done", "all_done_setup_success"}
    assert {"setup", "metrics"} <= set(teardown.upstream_task_ids)
    assert factory_dag.get_task("setup").retries == 2
    assert factory_dag.get_task("deliver").retries == 2
    for name in ("intent", "spec", "plan", "build_and_test", "review"):
        assert factory_dag.get_task(name).retries == 0, name


def test_gates_are_approval_operators_with_timeout(factory_dag) -> None:
    from airflow.providers.standard.operators.hitl import ApprovalOperator

    for gate in ("intent", "plan"):
        op = factory_dag.get_task(f"approve_{gate}")
        assert isinstance(op, ApprovalOperator), type(op)
        assert op.response_timeout is not None and op.response_timeout.total_seconds() > 0
        assert op.ignore_downstream_trigger_rules is True
        assert op.fail_on_reject is False
        assert op.options == ["Approve", "Reject"]
        assert f"record_{gate}" in op.downstream_task_ids


def test_stage_tasks_match_pipeline(factory_dag) -> None:
    try:
        from swfactory import stages
    except ImportError as e:  # stages.py is being written concurrently
        pytest.xfail(f"swfactory.stages not importable yet: {e}")
    pipeline_names = [f.__name__ for f in stages.PIPELINE if callable(f)]
    assert pipeline_names == STAGE_TASKS
    assert [t for t in _linear_order(factory_dag) if t in pipeline_names] == STAGE_TASKS
    gates = [g for g in stages.PIPELINE if not callable(g)]
    assert [g.name for g in gates] == ["intent", "plan"]
    for gate in gates:
        assert f"approve_{gate.name}" in factory_dag.task_ids
        assert f"record_{gate.name}" in factory_dag.task_ids
    for fn in ("setup", "record_approval"):
        assert callable(getattr(stages, fn)), fn


def test_dag_modules_do_not_import_swfactory_at_parse_time() -> None:
    import ast

    for path in DAGS.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        top_level = [n for n in tree.body if isinstance(n, ast.Import | ast.ImportFrom)]
        for node in top_level:
            names = (
                [node.module or ""]
                if isinstance(node, ast.ImportFrom)
                else [a.name for a in node.names]
            )
            assert not any(n.startswith("swfactory") for n in names), f"{path.name}: {names}"


def test_maintain_dag_is_daily(dagbag) -> None:
    dag = dagbag.dags["maintain"]
    assert "check_bands" in dag.task_ids and "sweep_sandboxes" in dag.task_ids
    assert dag.schedule == "@daily"
