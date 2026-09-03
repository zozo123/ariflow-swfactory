"""Fan-out proof: ``blueprints/stress.toml`` really runs on Airflow, and its jobs never collide.

``tests/test_dag_smoke.py`` shows one job of one DAG run reaching a PR. This shows the other half
of the contract — that ``jobs = issues x targets`` are *independent*: two issues over two targets
is four mapped jobs, each with its own run id, run dir, workdir, bare remote, ``approvals.json``
and ``metrics.json``, and no job can see another's artifact chain. That is the property that
breaks first when the ``(blueprint, job, run_id) -> Ctx`` assembly (``swfactory.runtime``) grows a
path that is not keyed by the job, so it is worth a test that a human can also run by hand
(``scripts/stress_airflow.sh`` does the same run against a live ``airflow standalone``).

Two things the DAG needs and this file provides: ``demo/issue2.md`` (DEMO-2, whose acceptance
criteria the recorded ``demo/scripted`` fixtures already satisfy, so fan-out costs no new
fixtures) and ``demo/target-b`` — the blueprint's second target dir, materialised into the run's
cwd with ``stages.seed_local_workdir``, the same call a job uses to seed its own workdir. Nothing
is written inside the checkout.

``airflow dags test`` never resolves HITL tasks, so the gates are marked success with
``mark_success_pattern=r"job\\.approve_.*"`` and ``record_<stage>`` records actor "auto"; the live
script answers them through the HITL API instead. Everything runs in a subprocess with a
throwaway ``AIRFLOW_HOME`` and cwd (Airflow reads its config at import time), scripted agent,
local sandbox, local git remote: no keys, no network, ~40 s.

Runs only with the ``airflow`` dependency group and is marked ``slow``:
``uv run --group airflow pytest tests/test_dag_stress.py``.
"""

from __future__ import annotations

import functools
import importlib.util
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

airflow = pytest.importorskip("airflow")

pytestmark = pytest.mark.slow

REPO = Path(__file__).resolve().parents[1]
DAGS = REPO / "dags"
BLUEPRINT = REPO / "blueprints" / "stress.toml"
DAG_ID = "stress"
ISSUES = ["demo/issue.md", "demo/issue2.md"]
ISSUE_IDS = {"demo/issue.md": "DEMO-1", "demo/issue2.md": "DEMO-2"}
CONF: dict[str, Any] = {"issues": ISSUES}
MARK_SUCCESS = r"job\.approve_.*"
MATERIALISED_TARGET = "demo/target-b"  # blueprints/stress.toml's second [[targets]].dir
CHAIN = ("intent.md", "spec.md", "plan.md", "plan.json", "review.json", "metrics.json")

# Own interpreter, own AIRFLOW_HOME: run the DAG, then dump the run, the fan_out XCom and the
# shape of every task in one JSON line. Reading the shape here (rather than building a second
# DagBag in the pytest process) keeps this file's Airflow state confined to the subprocess.
DRIVER = """
import json
import sys

from airflow import settings

settings.configure_orm()

from airflow.dag_processing.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.utils.session import create_session
from sqlalchemy import select

dags_folder, dag_id, conf, mark = sys.argv[1:5]
bag = DagBag(dag_folder=dags_folder)
assert not bag.import_errors, bag.import_errors
dag = bag.dags[dag_id]
run = dag.test(run_conf=json.loads(conf), mark_success_pattern=mark)


def hours(td):
    return None if td is None else round(td.total_seconds() / 3600.0, 6)


tasks = {
    t.task_id: {
        "cls": type(t).__name__,
        "is_approval": isinstance(t, ApprovalOperator),
        "max_active_tis_per_dagrun": t.max_active_tis_per_dagrun,
        "trigger_rule": str(t.trigger_rule.value),
        "is_teardown": bool(t.is_teardown),
        "response_timeout_h": hours(getattr(t, "response_timeout", None)),
        "defaults": getattr(t, "defaults", None),
    }
    for t in dag.tasks
}

with create_session() as session:
    tis = sorted(
        [ti.task_id, ti.map_index, str(ti.state)]
        for ti in session.scalars(
            select(TaskInstance).where(TaskInstance.run_id == run.run_id)
        )
    )
    rows = session.scalars(
        XComModel.get_many(
            run_id=run.run_id, key="return_value", task_ids="fan_out", dag_ids=dag_id
        )
    ).all()
    fan_out = [XComModel.deserialize_value(r) for r in rows]

print(
    "STRESS_RESULT "
    + json.dumps(
        {
            "state": str(run.state),
            "run_id": run.run_id,
            "tis": tis,
            "fan_out": fan_out[0] if fan_out else None,
            "group_type": type(dag.task_group.get_child_by_label("job")).__name__,
            "tasks": tasks,
        }
    )
)
"""


def _shape() -> dict[str, Any]:
    """The blueprint's own numbers, read the way the DAG generator reads them (stdlib tomllib)."""
    data = tomllib.loads(BLUEPRINT.read_text(encoding="utf-8"))
    return {
        "order": list(data["stages"]["order"]),
        "targets": list(data["targets"]),
        "gates": {g["after"]: g for g in data.get("gates", [])},
        "limits": data.get("limits", {}),
    }


def _env(home: Path) -> dict[str, str]:
    """Clean process env: throwaway AIRFLOW_HOME, no inherited SWF_* knobs, scripted/local run."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SWF_", "AIRFLOW"))}
    env.update(
        AIRFLOW_HOME=str(home),
        AIRFLOW__CORE__DAGS_FOLDER=str(DAGS),
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        SWF_AGENT="scripted",
        SWF_SANDBOX="local",
        SWF_SCM="local",
    )
    return env


def _run(argv: list[str], *, cwd: Path, env: dict[str, str], timeout: int) -> str:
    proc = subprocess.run(
        argv, cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout, check=False
    )
    tail = f"{proc.stdout[-4000:]}\n{proc.stderr[-4000:]}"
    assert proc.returncode == 0, f"{argv[-3:]} rc={proc.returncode}\n{tail}"
    return proc.stdout + proc.stderr


@functools.cache
def _blueprints_mod() -> ModuleType:
    """``dags/blueprints.py`` as a module (importing it builds DAGs, so do it once)."""
    spec = importlib.util.spec_from_file_location("swf_dags_blueprints", DAGS / "blueprints.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_id(dag_run_id: str, job_idx: int) -> str:
    """``dags/blueprints.py::run_id_for`` — the DAG module itself, not a copy kept in sync."""
    return str(_blueprints_mod().run_id_for(dag_run_id, job_idx))


@functools.cache
def _expected_jobs() -> tuple[dict[str, Any], ...]:
    from swfactory.blueprint import load

    return tuple(load(DAG_ID).jobs(CONF))


def _job_dirs(stress: dict, job_idx: int) -> tuple[Path, Path]:
    """``(run_dir, workdir)`` of one job, derived exactly as ``swfactory.runtime`` derives them."""
    from swfactory import runtime
    from swfactory.blueprint import load

    bp = load(DAG_ID)
    job = _expected_jobs()[job_idx]
    run_id = _run_id(stress["run_id"], job_idx)
    cfg = runtime.job_config(bp, job, run_id=run_id, root=stress["cwd"])
    assert cfg.target_dir == job["dir"]  # the job's target decides what its workdir was seeded from
    return runtime.job_run_dir(cfg, stress["cwd"]), Path(cfg.workdir)


# ---------------------------------------------------------------- the run


@pytest.fixture(scope="module")
def stress(tmp_path_factory: pytest.TempPathFactory) -> dict:
    """One ``dag.test()`` of the stress DAG: 2 issues x 2 targets = 4 mapped jobs."""
    from swfactory.stages import seed_local_workdir

    root = tmp_path_factory.mktemp("stress")
    home, cwd = root / "airflow_home", root / "cwd"
    home.mkdir()
    cwd.mkdir()
    # The blueprint's second target: the harness materialises it, so no byte-identical copy of
    # demo/target has to be committed to double the fan-out (see blueprints/stress.toml).
    assert seed_local_workdir(cwd / MATERIALISED_TARGET, "demo/target")
    env = _env(home)
    _run([sys.executable, "-m", "airflow", "db", "migrate"], cwd=cwd, env=env, timeout=300)
    driver = root / "driver.py"
    driver.write_text(DRIVER, encoding="utf-8")
    argv = [sys.executable, str(driver), str(DAGS), DAG_ID, json.dumps(CONF), MARK_SUCCESS]
    log = _run(argv, cwd=cwd, env=env, timeout=1800)
    line = next(ln for ln in log.splitlines() if ln.startswith("STRESS_RESULT "))
    return {"cwd": cwd, "log": log, **json.loads(line.removeprefix("STRESS_RESULT "))}


def test_every_mapped_job_succeeds(stress: dict) -> None:
    """The whole fan-out is green, and both gates of every job were the marked-success HITL ones."""
    assert stress["state"] == "success", stress["log"][-6000:]
    failed = [ti for ti in stress["tis"] if ti[2] != "success"]
    assert not failed, failed
    jobs = _expected_jobs()
    mapped = {tuple(ti[:2]) for ti in stress["tis"] if ti[0].startswith("job.")}
    assert {mi for _tid, mi in mapped} == set(range(len(jobs)))
    marked = [ln for ln in stress["log"].splitlines() if "[DAG TEST] Marking success" in ln]
    assert len(marked) == 2 * len(jobs) and all("job.approve_" in ln for ln in marked), marked


def test_fan_out_returned_issues_x_targets(stress: dict) -> None:
    """``fan_out``'s XCom is exactly ``Blueprint.jobs(conf)``: the DAG expands over nothing else."""
    assert stress["fan_out"] == list(_expected_jobs())


def test_each_job_owns_its_run_dir_and_workdir(stress: dict) -> None:
    """No cross-job leakage: distinct workdirs, each holding only its own artifact chain."""
    jobs = _expected_jobs()
    seen: dict[Path, int] = {}
    for idx, job in enumerate(jobs):
        run_dir, workdir = _job_dirs(stress, idx)
        assert run_dir.is_dir(), sorted(str(p) for p in stress["cwd"].glob(".factory/*"))
        assert workdir.is_dir() and workdir.parent == run_dir
        assert workdir not in seen, f"job {idx} shares a workdir with job {seen[workdir]}"
        seen[workdir] = idx
        issue_id = ISSUE_IDS[job["issue"]]
        chain = workdir / "docs" / "factory" / issue_id
        assert [p.name for p in sorted((workdir / "docs" / "factory").iterdir())] == [issue_id]
        for artifact in CHAIN:
            assert (chain / artifact).is_file(), f"job {idx}: {artifact}"
        assert (run_dir / "stages.jsonl").is_file()  # the orchestrator's own per-job stage log
    assert len(seen) == len(jobs)


def test_each_job_publishes_its_own_pr_on_its_own_remote(stress: dict) -> None:
    """``deliver`` per job: its own ``pr.md`` and its own bare remote carrying only its branch."""
    for idx, job in enumerate(_expected_jobs()):
        run_dir, _workdir = _job_dirs(stress, idx)
        issue_id = ISSUE_IDS[job["issue"]]
        pr = (run_dir / "pr.md").read_text(encoding="utf-8")
        assert pr.startswith(f"# {issue_id}: "), pr[:200]
        assert "labels: factory, agent-authored, stress" in pr  # blueprint [deliver].labels
        refs = subprocess.run(
            ["git", "for-each-ref", "--format=%(refname)"],
            cwd=run_dir / "remote.git",
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
        assert sorted(refs) == sorted(
            ["refs/heads/main", f"refs/heads/factory/{issue_id}-{run_dir.name}"]
        )


def test_each_job_records_both_gates_and_its_own_metrics(stress: dict) -> None:
    """``approvals.json`` holds both gates and ``metrics.json`` names this blueprint, per job."""
    for idx, job in enumerate(_expected_jobs()):
        _run_dir, workdir = _job_dirs(stress, idx)
        issue_id = ISSUE_IDS[job["issue"]]
        chain = workdir / "docs" / "factory" / issue_id
        approvals = json.loads((chain / "approvals.json").read_text(encoding="utf-8"))
        assert [a["gate"] for a in approvals] == ["intent", "plan"]
        assert [a["decision"] for a in approvals] == ["approve", "approve"]
        assert [a["actor"] for a in approvals] == ["auto", "auto"]  # dag.test marks HITL success
        metrics = json.loads((chain / "metrics.json").read_text(encoding="utf-8"))
        assert metrics["blueprint"] == DAG_ID
        assert (metrics["issue_id"], metrics["run_id"]) == (issue_id, workdir.parent.name)
        assert metrics["agent"] == "scripted" and metrics["tests_passed"] is True
        assert metrics["approvers"] == ["auto", "auto"]


# ---------------------------------------------------------------- the DAG's shape


def test_dag_structure_mirrors_the_stress_blueprint(stress: dict) -> None:
    """The knobs that make fan-out safe are the blueprint's, not the generator's defaults."""
    shape, tasks = _shape(), stress["tasks"]
    assert stress["group_type"] == "MappedTaskGroup"
    parallel = shape["limits"]["max_parallel_jobs"]
    assert parallel == 2  # the point of this blueprint: fan-out is throttled, not unbounded
    for stage in shape["order"]:
        assert tasks[f"job.{stage}"]["max_active_tis_per_dagrun"] == parallel, stage
    for stage, gate in shape["gates"].items():
        op = tasks[f"job.approve_{stage}"]
        assert op["is_approval"] and op["cls"] == "GateOperator", stage
        assert op["response_timeout_h"] == gate["timeout_h"], stage
        assert op["defaults"] == ["Approve"], stage  # gates[].auto: unattended runs still finish
    teardown = tasks["job.teardown"]
    # `all_done` as written in the DAG; Airflow narrows it for a teardown wired to a setup task
    # (`as_teardown(setups=setup)`). Either way the sandbox is closed after a rejected or failed
    # job, which is what fan-out needs. Same pair as tests/test_dag_parity.py::test_dag_shape.
    assert teardown["is_teardown"]
    assert teardown["trigger_rule"] in {"all_done", "all_done_setup_success"}


def test_gates_are_addressable_per_map_index(stress: dict) -> None:
    """One approval per (job, gate): ``swfactory approve <run> <gate> --map-index <i>``."""
    jobs = _expected_jobs()
    for stage in _shape()["gates"]:
        answered = sorted(mi for tid, mi, _s in stress["tis"] if tid == f"job.approve_{stage}")
        assert answered == list(range(len(jobs))), stage


def test_multi_target_blueprint_expands_to_issues_x_targets(stress: dict) -> None:
    """``Blueprint.jobs`` and the DAG's expansion agree, and both are issues x targets.

    The mapped-group expansion is the number of distinct ``map_index`` values Airflow actually
    ran, so this pins the DAG against the blueprint rather than against ``jobs()`` alone.
    """
    from swfactory.blueprint import load

    bp = load(DAG_ID)
    targets = len(bp.targets)
    assert targets == len(_shape()["targets"]) > 1
    for issues in ([ISSUES[0]], ISSUES):
        jobs = bp.jobs({"issues": issues})
        assert len(jobs) == len(issues) * targets
        assert [j["job_idx"] for j in jobs] == list(range(len(jobs)))
        # issue-major, target-minor: (i0,t0), (i0,t1), (i1,t0), ...
        assert [(j["issue"], j["dir"]) for j in jobs] == [
            (i, t.dir) for i in issues for t in bp.targets
        ]
    expanded = {mi for tid, mi, _s in stress["tis"] if tid.startswith("job.")}
    assert len(expanded) == len(ISSUES) * targets == len(bp.jobs(CONF))
