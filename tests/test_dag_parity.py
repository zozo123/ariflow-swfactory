"""The DAGs are the blueprints: one DAG per ``blueprints/*.toml`` whose mapped ``job`` group
mirrors the blueprint's stage order and gates (ApprovalOperators); ``maintain`` is asset+cron.

Runs only with the ``airflow`` dependency group:
``uv run --group airflow pytest tests/test_dag_parity.py``.
"""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
import shutil
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace

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
start = 2026-01-05T00:00:00+00:00
issues = ["ops/nightly.md"]
[[targets]]
repo = "acme/app"
[stages]
order = ["intent", "plan", "build_and_test", "review", "deliver"]
[[gates]]
after = "plan"
artifact = "plan.md"
timeout_h = 2
mode = "auto"
assigned = ["alice", "bob"]
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


def _load_module(path: Path) -> ModuleType:
    """Import a DAG file as a module (DagBag hands back DAGs, not the helpers around them)."""
    spec = importlib.util.spec_from_file_location(f"swf_dag_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _shape(path: Path) -> dict:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return {
        "name": data["blueprint"]["name"],
        "order": data["stages"]["order"],
        "gates": data.get("gates", []),
        "limits": data.get("limits", {}),
        "trigger": data.get("trigger", {}),
    }


def _gate_is_auto(gate: dict) -> bool:
    """The generator's own resolution of ``gates[].mode``: the parity test must not keep a second
    opinion about who owns a gate (``tests/test_approval_authority.py`` pins it to swfactory's)."""
    return _load_module(DAGS / "blueprints.py").gate_mode(gate) == "auto"


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
    heads = [t for t in dag.tasks if t.upstream_task_ids == {"fan_out"} and t.task_id != "job.teardown"]
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
    os.environ.pop("SWF_APPROVE", None)  # not a gate knob any more; kept out for a clean parse


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


@pytest.fixture(scope="module")
def blueprints_mod() -> ModuleType:
    return _load_module(DAGS / "blueprints.py")


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
    # setup is where a cell comes to exist, so it is bounded like the stages that use it (#2070)
    assert dag.get_task("job.setup").max_active_tis_per_dagrun == parallel
    for stage in shape["order"]:
        t = dag.get_task(f"job.{stage}")
        assert t.retries == (2 if stage == "deliver" else 0), stage
        assert t.execution_timeout == timeout, stage
        assert t.max_active_tis_per_dagrun == parallel, stage
        # a rejected gate skips the work stages; deliver (and metrics) still publish the refusal
        rule = "none_failed" if stage == "deliver" else "all_success"
        assert str(t.trigger_rule.value) == rule, stage
    assert str(dag.get_task("job.metrics").trigger_rule.value) == "none_failed"
    for stage in shape["order"]:
        if stage in {g["after"] for g in shape["gates"]}:
            record = dag.get_task(f"job.record_{stage}")
            assert str(record.trigger_rule.value) == "all_success"
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
        assert op.defaults == (["Approve"] if _gate_is_auto(gate) else None)
        # Reject must reach record_<stage>: the gate never skips on its own (see GateOperator)
        assert type(op).__name__ == "GateOperator"
        assert op.fail_on_reject is False and op.ignore_downstream_trigger_rules is False
        assigned = gate.get("assigned") or []
        assert op.assigned_users == ([{"id": u, "name": u} for u in assigned] or None)
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


@pytest.mark.parametrize("path", BLUEPRINTS, ids=BLUEPRINT_IDS)
def test_dag_schedule_limits_mirror_the_blueprint(dagbag, path: Path) -> None:
    """``read_shape`` derives the run bounds with stdlib only; ``Blueprint.schedule_limits`` is the
    validated declaration. They must be one answer (#2070), the way ``gate_mode`` is pinned."""
    from swfactory.blueprint import load

    bp = load(str(path))
    dag = dagbag.dags[bp.name]
    limits = bp.schedule_limits()
    assert dag.catchup is False
    assert dag.max_active_runs == limits.max_active_runs
    assert dag.dagrun_timeout == limits.run_timeout == timedelta(seconds=bp.sandbox.ttl_s)
    assert dag.start_date == limits.origin
    if bp.trigger.kind == "cron":
        assert dag.start_date is not None and dag.start_date.tzinfo is not None
        assert dag.max_active_runs == 1, "a scheduled line never stacks runs behind an unanswered gate"
    else:
        assert dag.start_date is None and dag.schedule is None


def test_a_cron_line_first_automated_run_follows_from_its_declared_origin(dagbag) -> None:
    """Airflow's own timetable, asked from the blueprint's origin: the first run is the first tick
    at or after it. Airflow stays the only party that says *when*; the blueprint only says *from*."""
    from airflow.timetables.base import TimeRestriction
    from airflow.timetables.trigger import CronTriggerTimetable

    dag = dagbag.dags["liquid"]
    assert dag.start_date == datetime(2026, 9, 1, tzinfo=UTC)
    # The DAG holds the SDK's declaration of the timetable; the scheduler's implementation of the
    # same cron and timezone is what answers "when".
    timetable = CronTriggerTimetable(dag.schedule, timezone=dag.timezone)
    info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=dag.start_date, latest=None, catchup=True),
    )
    assert info is not None and info.run_after == datetime(2026, 9, 1, 6, 17, tzinfo=UTC)
    # and the line itself never back-fills the ticks it slept through
    assert dag.catchup is False


def test_a_cron_line_without_an_origin_does_not_parse(blueprints_mod, tmp_path: Path) -> None:
    """A scheduled DAG with no start is a run that fails in fan_out every tick; refuse it at parse time."""
    toml = NIGHTLY_TOML.replace("start = 2026-01-05T00:00:00+00:00\n", "")
    (tmp_path / "no_origin.toml").write_text(toml, encoding="utf-8")
    with pytest.raises(ValueError, match="trigger.start"):
        blueprints_mod.read_shape(tmp_path / "no_origin.toml")
    naive = NIGHTLY_TOML.replace("2026-01-05T00:00:00+00:00", "2026-01-05T00:00:00")
    (tmp_path / "naive.toml").write_text(naive, encoding="utf-8")
    with pytest.raises(ValueError, match="timezone-aware"):
        blueprints_mod.read_shape(tmp_path / "naive.toml")


def _fan_out(dagbag, name: str):
    return dagbag.dags[name].get_task("fan_out").python_callable


def _scheduled(run_id: str = "scheduled__2026-09-10T06:17:00+00:00") -> SimpleNamespace:
    return SimpleNamespace(conf=None, run_id=run_id, run_type="scheduled")


def test_a_scheduled_run_is_admitted_through_the_backend_or_stops_in_fan_out(
    dagbag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#2067: an empty-conf scheduled run must leave ``fan_out`` with positive-epoch managed Cells
    for every job, or not leave it at all. Nothing downstream can run unmanaged by falling back."""
    pytest.importorskip("swfactory")
    # liquid drains a label-selected backlog through the SCM at fan-out (#2069); stand in for the
    # SCM here so the run has two issues to admit without a network.
    import swfactory.blueprint as blueprint_mod
    from swfactory import cell_runtime
    from swfactory.blueprint import load
    from swfactory.cell_callback import CellCallbackError
    from swfactory.cell_runtime import identity_for_job

    monkeypatch.setattr(blueprint_mod, "_drain", lambda line: ["2034", "2035"])
    fan_out = _fan_out(dagbag, "liquid")
    jobs = load("liquid").jobs(None)
    assert len(jobs) == 2

    # No backend configured on the worker: the run stops here, before any sandbox exists.
    monkeypatch.delenv("SWF_BACKEND_URL", raising=False)
    monkeypatch.delenv("SWF_BACKEND_TOKEN", raising=False)
    with pytest.raises(CellCallbackError, match="SWF_BACKEND_URL"):
        fan_out(dag_run=_scheduled())

    # The backend answers with complete bindings for this run: every job is managed at its epoch.
    asked: list[tuple[str, str, int]] = []
    digest = "a" * 64

    def admit(line: str, dag_run_id: str, wanted: list[dict]) -> list[dict]:
        asked.append((line, dag_run_id, len(wanted)))
        return [
            {
                "job_idx": job["job_idx"],
                "cell_id": identity_for_job(job).stable_id(),
                "epoch": 3,
                "repo": job["repo"],
                "snapshot_digest": digest,
                "policy_digest": "policy:x",
                "factory_generation": "stable",
            }
            for job in wanted
        ]

    monkeypatch.setattr(cell_runtime, "admit_scheduled_run", admit)
    bound = fan_out(dag_run=_scheduled())
    assert asked == [("liquid", "scheduled__2026-09-10T06:17:00+00:00", 2)]
    assert [(job["cell_managed"], job["cell_epoch"]) for job in bound] == [(True, 3), (True, 3)]
    assert [job["issue"] for job in bound] == [job["issue"] for job in jobs]

    # A partial answer binds nothing: half a managed run is an unmanaged run.
    monkeypatch.setattr(cell_runtime, "admit_scheduled_run", lambda *args: admit(*args)[:1])
    with pytest.raises(ValueError, match="partial"):
        fan_out(dag_run=_scheduled())

    # A manual direct submission keeps its explicitly unmanaged legacy shape; that path is unchanged.
    manual = fan_out(dag_run=SimpleNamespace(conf={"issues": ["2034"]}, run_id="manual__x", run_type="manual"))
    assert [(job["cell_managed"], job["cell_epoch"]) for job in manual] == [(False, 1)]


# ---------------------------------------------------------------- synthetic blueprint


def test_synthetic_blueprint_cron_and_auto_gate(nightly_dag) -> None:
    from airflow.providers.standard.operators.hitl import ApprovalOperator

    dag = nightly_dag
    assert dag.schedule == "0 6 * * 1"
    assert dag.start_date == datetime(2026, 1, 5, tzinfo=UTC)
    assert dag.max_active_runs == 1 and dag.dagrun_timeout == timedelta(seconds=86400)
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
    assert approve.assigned_users == [
        {"id": "alice", "name": "alice"},
        {"id": "bob", "name": "bob"},
    ]
    assert dag.get_task("job.build_and_test").execution_timeout == timedelta(hours=5)
    assert dag.get_task("job.build_and_test").max_active_tis_per_dagrun == 2
    assert [a.name for a in dag.get_task("job.deliver").outlets] == ["swf.metrics.nightly"]


# ---------------------------------------------------------------- record_<stage> on Reject


class _Ti:
    map_index = 0

    def __init__(self, response: dict | None) -> None:
        self.response = response
        self.pulled: list[tuple] = []

    def xcom_pull(self, *, task_ids: str, map_indexes: int):
        self.pulled.append((task_ids, map_indexes))
        return self.response


def _ctx_on(tmp_path: Path):
    """A real ``Ctx`` over a LocalSandbox, so record/metrics write to files we can read back."""
    from swfactory import accepted_inputs
    from swfactory.config import Config
    from swfactory.models import Issue
    from swfactory.sandbox import LocalSandbox
    from swfactory.stages import Ctx

    cfg = Config(issue="demo/issue.md", run_id="abcd1234")
    issue = Issue(id="DEMO-1", title="t", body="")
    ctx = Ctx(
        cfg=cfg,
        sb=LocalSandbox(tmp_path / "work"),
        agent=SimpleNamespace(kind="scripted"),
        scm=SimpleNamespace(kind="local"),
        issue=issue,
        run_dir=tmp_path / "run",
    )
    # Pin the run the way `_prepare_ctx` does on its first context. Recording reads the accepted
    # inputs fail-closed, so a Ctx straight from the constructor would be an unpinned run that no
    # production path can produce.
    accepted_inputs.admit(ctx.state, accepted_inputs.snapshot(cfg, None, issue))
    return ctx


@pytest.mark.parametrize("stage", ["intent", "plan"])
def test_record_task_persists_rejection_then_skips_the_line(
    blueprints_mod, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    from airflow.sdk.exceptions import AirflowSkipException

    pytest.importorskip("swfactory")
    from swfactory.models import StageError

    ctx = _ctx_on(tmp_path)
    gate_artifact = f"{ctx.art}/{stage}.md"
    ctx.write_artifact(gate_artifact, f"# {stage}\n")
    monkeypatch.setattr(blueprints_mod, "_ctx", lambda name, job, run_id: ctx)
    record = blueprints_mod._record_task("factory", stage, "human").function
    dag_run = SimpleNamespace(run_id="manual__2026-09-02T03:00:00+00:00")
    rejected = {
        "chosen_options": ["Reject"],
        "params_input": {},
        "responded_by_user": {"id": "u1", "name": "alice"},
        # The HITL event's own answer time. Recording refuses a stored response without one,
        # because that is what dates the answer to the inputs the run admitted.
        "responded_at": "2099-01-01T00:00:00Z",
    }
    with pytest.raises(AirflowSkipException, match=f"{stage} rejected by alice"):
        record({"job_idx": 0}, ti=_Ti(rejected), dag_run=dag_run)
    art = tmp_path / "work" / "docs" / "factory" / "DEMO-1"
    approvals = json.loads((art / "approvals.json").read_text())
    assert [(a["gate"], a["decision"], a["actor"]) for a in approvals] == [(stage, "reject", "alice")]
    assert approvals[0]["artifact_sha256"] == hashlib.sha256(f"# {stage}\n".encode()).hexdigest()

    # A later decision replaces the same gate entry, so task retries cannot duplicate approvals.
    ti = _Ti({**rejected, "chosen_options": ["Approve"]})
    out = record({"job_idx": 0}, ti=ti, dag_run=dag_run)
    assert (out["decision"], out["actor"], out["mode"]) == ("approve", "alice", "human")
    assert ti.pulled == [(f"job.approve_{stage}", 0)]
    saved = json.loads((art / "approvals.json").read_text())
    assert [(a["decision"], a["actor"]) for a in saved] == [("approve", "alice")]
    # ... and a gate with no response at all is refused rather than recorded as actor "auto";
    # tests/test_approval_authority.py owns that boundary in full.
    with pytest.raises(StageError, match="no recorded response"):
        record({"job_idx": 0}, ti=_Ti(None), dag_run=dag_run)
    assert [a["actor"] for a in json.loads((art / "approvals.json").read_text())] == ["alice"]


def test_run_ids_are_hex8_and_stable(blueprints_mod) -> None:
    from swfactory.config import Config

    maintain_mod = _load_module(DAGS / "maintain.py")
    airflow_run_id = "scheduled__2026-09-02T03:00:00+00:00"  # every Airflow run id ends like this
    for rid in (
        blueprints_mod.run_id_for(airflow_run_id, 0),
        blueprints_mod.run_id_for(airflow_run_id, 1),
        maintain_mod.run_id_for(airflow_run_id),
    ):
        assert len(rid) == 8 and int(rid, 16) >= 0
        Config(issue="maintain", run_id=rid, agent="claude", sandbox="islo").sandbox_name("maintain")
    assert blueprints_mod.run_id_for(airflow_run_id, 0) != blueprints_mod.run_id_for(airflow_run_id, 1)
    assert maintain_mod.run_id_for(airflow_run_id) == maintain_mod.run_id_for(airflow_run_id)


def test_dag_ctx_config_matches_runtime_job_config(
    blueprints_mod, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every task rebuilds its ``Ctx`` with ``swfactory.runtime.build_ctx``, so a task runs on the
    config ``swfactory run`` would build for the same (blueprint, job, run id). The CLI half is
    ``tests/test_runtime.py::test_cli_run_derives_its_config_with_job_config``."""
    pytest.importorskip("swfactory")
    from swfactory import runtime
    from swfactory.blueprint import load

    monkeypatch.chdir(tmp_path)  # a worker's cwd is not the factory checkout
    seen: list[tuple] = []
    monkeypatch.setattr(runtime, "ctx_for", lambda cfg, **kw: seen.append((cfg, kw)))
    dag_run_id = "manual__2026-09-02T03:00:00+00:00"
    (job,) = load("factory").jobs({"issues": ["demo/issue.md"]})

    blueprints_mod._ctx("factory", job, dag_run_id)

    run_id = blueprints_mod.run_id_for(dag_run_id, 0)
    assert run_id == runtime.run_id_for(dag_run_id, 0)
    ((cfg, kw),) = seen
    assert cfg == runtime.job_config(load("factory"), job, run_id=run_id)
    assert kw["run_dir"] == runtime.job_run_dir(cfg) and kw["agent"] is None
    assert kw["blueprint"].name == "factory"


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
            names = [node.module or ""] if isinstance(node, ast.ImportFrom) else [a.name for a in node.names]
            assert not any(n.startswith("swfactory") for n in names), f"{path.name}: {names}"
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            if line.startswith(("from swfactory", "import swfactory")):
                pytest.fail(f"{path.name}: top-level swfactory import: {line}")
    assert not (DAGS / "factory.py").exists(), "dags/factory.py was replaced by dags/blueprints.py"
