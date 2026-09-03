"""The DAGs are the blueprints: one DAG per ``blueprints/*.toml`` whose mapped ``job`` group
mirrors the blueprint's stage order and gates (ApprovalOperators); ``maintain`` is asset+cron.

Runs only with the ``airflow`` dependency group:
``uv run --group airflow pytest tests/test_dag_parity.py``.
"""

from __future__ import annotations

import ast
import os
import shutil
import tomllib
from datetime import timedelta
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

REPO = Path(__file__).resolve().parents[1]
DAGS = REPO / "dags"
BLUEPRINTS = sorted((REPO / "blueprints").glob("*.toml"))
BLUEPRINT_IDS = [p.stem for p in BLUEPRINTS]

# A synthetic blueprint exercising the branches the shipped ones may not: cron trigger, a gate
# that auto-approves, a non-default parallelism, and a shortened stage order.
NIGHTLY_TOML = """
[blueprint]
name = "nightly"
[trigger]
kind = "cron"
cron = "0 6 * * 1"
[[targets]]
repo = "acme/app"
[stages]
order = ["intent", "plan", "build_and_test", "review", "deliver"]
[[gates]]
after = "plan"
artifact = "plan.md"
timeout_h = 2
auto = true
[limits]
stage_timeout_h = 5
max_parallel_jobs = 2
[sandbox]
ttl_s = 86400
"""


def _dagbag(folder: Path):
    from airflow.dag_processing.dagbag import DagBag

    # Airflow 3.3 dropped `include_examples`; AIRFLOW__CORE__LOAD_EXAMPLES=False covers it.
    return DagBag(dag_folder=str(folder))


def _shape(path: Path) -> dict:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return {
        "name": data["blueprint"]["name"],
        "order": data["stages"]["order"],
        "gates": data.get("gates", []),
        "limits": data.get("limits", {}),
        "trigger": data.get("trigger", {}),
    }


def _job_task_ids(shape: dict) -> list[str]:
    """Expected task ids under ``job.`` in pipeline order (setup first, metrics/teardown last)."""
    gates = {g["after"] for g in shape["gates"]}
    ids = ["job.setup"]
    for stage in shape["order"]:
        ids.append(f"job.{stage}")
        if stage in gates:
            ids += [f"job.approve_{stage}", f"job.record_{stage}"]
    return [*ids, "job.metrics", "job.teardown"]


def _linear_order(dag) -> list[str]:
    """``fan_out`` then the single downstream chain inside the mapped group. Every group task
    takes the mapped ``job`` argument, so ``fan_out`` has an edge to each of them; those edges
    are ignored by the walk. ``job.teardown`` is excluded from the walk (Airflow wires it to its
    setup task as well) and appended last after checking it hangs off the final task."""
    roots = [t for t in dag.tasks if not t.upstream_task_ids]
    assert [t.task_id for t in roots] == ["fan_out"]
    heads = [
        t for t in dag.tasks if t.upstream_task_ids == {"fan_out"} and t.task_id != "job.teardown"
    ]
    assert [t.task_id for t in heads] == ["job.setup"], [t.task_id for t in heads]
    order, current = ["fan_out"], heads[0]
    for _ in range(len(dag.tasks)):
        order.append(current.task_id)
        nxt = sorted(t for t in current.downstream_task_ids if t != "job.teardown")
        if not nxt:
            break
        assert len(nxt) == 1, f"{current.task_id} fans out to {nxt}"
        current = dag.get_task(nxt[0])
    assert current.task_id in dag.get_task("job.teardown").upstream_task_ids
    return [*order, "job.teardown"]


# ---------------------------------------------------------------- fixtures


@pytest.fixture(scope="module", autouse=True)
def _airflow_env(tmp_path_factory: pytest.TempPathFactory):
    os.environ["AIRFLOW_HOME"] = str(tmp_path_factory.mktemp("airflow_home"))
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ.pop("SWF_APPROVE", None)  # parse-time knob: the shipped blueprints decide


@pytest.fixture(scope="module")
def dagbag():
    bag = _dagbag(DAGS)
    assert not bag.import_errors, bag.import_errors
    return bag


@pytest.fixture(scope="module")
def nightly_dag(tmp_path_factory: pytest.TempPathFactory):
    """The generator run against a synthetic ``blueprints/`` dir: copy ``dags/blueprints.py``
    next to a tmp ``blueprints/nightly.toml`` (the module resolves ``../blueprints``)."""
    root = tmp_path_factory.mktemp("factory")
    (root / "dags").mkdir()
    (root / "blueprints").mkdir()
    shutil.copy(DAGS / "blueprints.py", root / "dags" / "blueprints.py")
    (root / "blueprints" / "nightly.toml").write_text(NIGHTLY_TOML, encoding="utf-8")
    bag = _dagbag(root / "dags")
    assert not bag.import_errors, bag.import_errors
    assert set(bag.dag_ids) == {"nightly"}
    return bag.dags["nightly"]


# ---------------------------------------------------------------- one DAG per blueprint


def test_shipped_blueprints_exist() -> None:
    assert BLUEPRINT_IDS, "no blueprints/*.toml"
    assert "default" in BLUEPRINT_IDS


def test_dags_import_cleanly(dagbag) -> None:
    names = {_shape(p)["name"] for p in BLUEPRINTS}
    assert names | {"maintain"} == set(dagbag.dag_ids)
    assert "factory" in names  # dag_id "factory" keeps v1 URLs, dispatch.yml and the CLI default


@pytest.mark.parametrize("path", BLUEPRINTS, ids=BLUEPRINT_IDS)
def test_task_ids_mirror_blueprint(dagbag, path: Path) -> None:
    shape = _shape(path)
    dag = dagbag.dags[shape["name"]]
    expected = ["fan_out", *_job_task_ids(shape)]
    assert _linear_order(dag) == expected
    assert sorted(dag.task_ids) == sorted(expected)


@pytest.mark.parametrize("path", BLUEPRINTS, ids=BLUEPRINT_IDS)
def test_dag_shape(dagbag, path: Path) -> None:
    from airflow.sdk.definitions.mappedoperator import MappedOperator

    shape = _shape(path)
    dag = dagbag.dags[shape["name"]]
    assert dag.schedule is None if shape["trigger"].get("kind", "manual") == "manual" else True
    assert dag.catchup is False
    assert {"issues", "issue"} <= set(dag.params)
    assert "swfactory" in dag.tags
    group = dag.task_group.get_child_by_label("job")
    assert type(group).__name__ == "MappedTaskGroup"
    assert isinstance(dag.get_task("fan_out"), MappedOperator) is False
    assert dag.get_task("job.setup").retries == 2
    assert dag.get_task("job.deliver").retries == 2
    timeout = timedelta(hours=shape["limits"].get("stage_timeout_h", 3))
    parallel = shape["limits"].get("max_parallel_jobs", 4)
    for stage in shape["order"]:
        t = dag.get_task(f"job.{stage}")
        assert t.retries == (2 if stage == "deliver" else 0), stage
        assert t.execution_timeout == timeout, stage
        assert t.max_active_tis_per_dagrun == parallel, stage
    outlets = dag.get_task("job.deliver").outlets
    assert [a.name for a in outlets] == [f"swf.metrics.{shape['name']}"]
    for stage in shape["order"]:
        if stage != "deliver":
            assert not dag.get_task(f"job.{stage}").outlets, stage
    teardown = dag.get_task("job.teardown")
    assert teardown.is_teardown  # survives an ApprovalOperator reject (teardowns never skip)
    assert str(teardown.trigger_rule.value) in {"all_done", "all_done_setup_success"}
    assert {"job.setup", "job.metrics"} <= set(teardown.upstream_task_ids)


@pytest.mark.parametrize("path", BLUEPRINTS, ids=BLUEPRINT_IDS)
def test_gates_are_approval_operators(dagbag, path: Path) -> None:
    from airflow.providers.standard.operators.hitl import ApprovalOperator

    shape = _shape(path)
    dag = dagbag.dags[shape["name"]]
    for gate in shape["gates"]:
        stage = gate["after"]
        op = dag.get_task(f"job.approve_{stage}")
        assert isinstance(op, ApprovalOperator), type(op)
        assert op.response_timeout == timedelta(hours=gate.get("timeout_h", 24))
        assert op.defaults == (["Approve"] if gate.get("auto") else None)
        assert op.ignore_downstream_trigger_rules is True
        assert op.fail_on_reject is False
        assert op.options == ["Approve", "Reject"]
        assert gate["artifact"] in op.subject and f"[{shape['name']}]" in op.subject
        assert "fan_out" in op.subject  # issue rendered from the fan_out XCom
        assert f"job.{stage}" in op.body and "preview" in op.body
        assert op.downstream_task_ids == {f"job.record_{stage}"}
        assert op.upstream_task_ids == {f"job.{stage}"}


@pytest.mark.parametrize("path", BLUEPRINTS, ids=BLUEPRINT_IDS)
def test_dag_serializes(dagbag, path: Path) -> None:
    from airflow.serialization.serialized_objects import DagSerialization

    dag = dagbag.dags[_shape(path)["name"]]
    data = DagSerialization.serialize_dag(dag)
    assert data["dag_id"] == dag.dag_id
    DagSerialization.deserialize_dag(data)


@pytest.mark.parametrize("path", BLUEPRINTS, ids=BLUEPRINT_IDS)
def test_job_tasks_equal_blueprint_pipeline(dagbag, path: Path) -> None:
    try:
        from swfactory.blueprint import load
        from swfactory.stages import Gate
    except ImportError as e:  # blueprint.py / stages.py are being written concurrently
        pytest.xfail(f"swfactory not importable yet: {e}")
    bp = load(str(path))
    dag = dagbag.dags[bp.name]
    expected = ["fan_out", "job.setup"]
    for item in bp.pipeline():
        if isinstance(item, Gate):
            expected += [f"job.approve_{item.name}", f"job.record_{item.name}"]
        else:
            expected.append(f"job.{item.__name__}")
    expected += ["job.metrics", "job.teardown"]
    assert _linear_order(dag) == expected


# ---------------------------------------------------------------- synthetic blueprint


def test_synthetic_blueprint_cron_and_auto_gate(nightly_dag) -> None:
    from airflow.providers.standard.operators.hitl import ApprovalOperator

    dag = nightly_dag
    assert dag.schedule == "0 6 * * 1"
    assert sorted(dag.task_ids) == sorted(
        [
            "fan_out",
            "job.setup",
            "job.intent",
            "job.plan",
            "job.approve_plan",
            "job.record_plan",
            "job.build_and_test",
            "job.review",
            "job.deliver",
            "job.metrics",
            "job.teardown",
        ]
    )
    approve = dag.get_task("job.approve_plan")
    assert isinstance(approve, ApprovalOperator)
    assert approve.defaults == [ApprovalOperator.APPROVE]
    assert approve.response_timeout == timedelta(hours=2)
    assert dag.get_task("job.build_and_test").execution_timeout == timedelta(hours=5)
    assert dag.get_task("job.build_and_test").max_active_tis_per_dagrun == 2
    assert [a.name for a in dag.get_task("job.deliver").outlets] == ["swf.metrics.nightly"]


# ---------------------------------------------------------------- maintain + hygiene


def test_maintain_dag_is_asset_or_time(dagbag) -> None:
    from airflow.timetables.assets import AssetOrTimeSchedule

    dag = dagbag.dags["maintain"]
    assert "check_bands" in dag.task_ids and "sweep_sandboxes" in dag.task_ids
    assert isinstance(dag.timetable, AssetOrTimeSchedule)
    names = {_shape(p)["name"] for p in BLUEPRINTS}
    summary = dag.timetable.summary
    assert "0 3 * * *" in summary or "Asset" in summary
    asset_names = {a.name for _key, a in dag.timetable.asset_condition.iter_assets()}
    assert asset_names == {f"swf.metrics.{n}" for n in names}


def test_dag_modules_do_not_import_swfactory_at_parse_time() -> None:
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
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            if line.startswith(("from swfactory", "import swfactory")):
                pytest.fail(f"{path.name}: top-level swfactory import: {line}")
    assert not (DAGS / "factory.py").exists(), "dags/factory.py was replaced by dags/blueprints.py"
