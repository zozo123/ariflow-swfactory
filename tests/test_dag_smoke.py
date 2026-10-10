"""End-to-end smoke of the ``factory`` DAG through ``dag.test()`` (docs/design.md, "Airflow").

``airflow dags test`` never resolves HITL tasks, so the gates are marked success with
``mark_success_pattern=r"job\\.approve_.*"``. Marking a gate successful is NOT an approval: the
``record_*`` tasks find no response and refuse (issue #2066), so this file drives the line through
the one thing allowed to stand in for a person — the declared replay fixture
``demo/gate-replay.json``, pointed at by ``SWF_GATE_REPLAY``. The second run here is the proof that
the fixture is load-bearing: with it removed, the same marked-success run fails at the first gate
instead of delivering. ``swfactory.approval_policy`` refuses the fixture outright for
backend-managed cells, so this path cannot authorize managed production work.

Everything runs in a subprocess with a throwaway ``AIRFLOW_HOME`` and cwd (Airflow reads its config
at import time, so the process must not share the parity test's interpreter), parameterized over
the scripted agent and an independently executable no-charge external fixture, with the local
sandbox and local git remote: no keys, no network.

Runs only with the ``airflow`` dependency group:
``uv run --group airflow pytest tests/test_dag_smoke.py``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from support import DAGS, airflow_env, load_dag_module, run_checked

airflow = pytest.importorskip("airflow")

ISSUE = "demo/issue.md"
MARK_SUCCESS = r"job\.approve_.*"
DRIVER = f"""
import json, sys
from airflow import settings
settings.configure_orm()
from airflow.dag_processing.dagbag import DagBag
bag = DagBag(dag_folder=sys.argv[1])
assert not bag.import_errors, bag.import_errors
dag = bag.dags["factory"]
dr = dag.test(run_conf={{"issues": [{ISSUE!r}]}}, mark_success_pattern={MARK_SUCCESS!r})
print("SMOKE_RESULT " + json.dumps({{"state": str(dr.state), "run_id": dr.run_id}}))
"""


def _result(log: str) -> dict:
    line = next(ln for ln in log.splitlines() if ln.startswith("SMOKE_RESULT "))
    return json.loads(line.removeprefix("SMOKE_RESULT "))


@pytest.fixture(scope="module", params=["scripted", "external"])
def smoke(tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest) -> dict:
    agent = request.param
    root = tmp_path_factory.mktemp(f"smoke-{agent}")
    home, cwd, bare_cwd = root / "airflow_home", root / "cwd", root / "cwd_no_fixture"
    home.mkdir()
    cwd.mkdir()
    bare_cwd.mkdir()
    env = airflow_env(home)
    if agent == "external":
        profile = root / "profile.json"
        executable = Path(__file__).resolve().parent / "fixtures" / "external_agent" / "fixture.py"
        profile.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "id": "external-fixture",
                    "version": "1",
                    "argv": [str(executable)],
                    "model": "fixture",
                    "credential_env": [],
                    "budget_mode": "no_charge",
                    "enforces_usd_limit": False,
                }
            ),
            encoding="utf-8",
        )
        env.update(SWF_AGENT="external", SWF_AGENT_PROFILE=str(profile), SWF_ALLOW_LOCAL_AGENT="1")
    run_checked([sys.executable, "-m", "airflow", "db", "migrate"], cwd=cwd, env=env, timeout=300)
    driver = root / "driver.py"
    driver.write_text(DRIVER, encoding="utf-8")
    log = run_checked([sys.executable, str(driver), str(DAGS)], cwd=cwd, env=env, timeout=900)
    # The same marked-success run with the fixture withdrawn: nothing else changes.
    bare_env = {key: value for key, value in env.items() if key != "SWF_GATE_REPLAY"}
    bare_log = run_checked([sys.executable, str(driver), str(DAGS)], cwd=bare_cwd, env=bare_env, timeout=900)
    return {
        "agent": agent,
        "cwd": cwd,
        "log": log,
        **_result(log),
        "bare": {"cwd": bare_cwd, "log": bare_log, **_result(bare_log)},
    }


def test_dag_run_succeeds(smoke: dict) -> None:
    assert smoke["state"] == "success", smoke["log"][-6000:]
    marked = [ln for ln in smoke["log"].splitlines() if "[DAG TEST] Marking success" in ln]
    assert len(marked) == 2 and all("job.approve_" in ln for ln in marked), marked


def test_marking_a_gate_successful_alone_authorizes_nothing(smoke: dict) -> None:
    """Without the declared replay fixture the identical run fails at the first gate and never
    delivers — the smoke harness cannot approve anything by itself."""
    bare = smoke["bare"]
    assert bare["state"] == "failed", bare["log"][-6000:]
    assert "a gate nobody answered is not approved" in bare["log"]
    run_dir = bare["cwd"] / ".factory" / load_dag_module().run_id_for(bare["run_id"], 0)
    assert not (run_dir / "pr.md").exists()
    assert not (run_dir / "work" / "docs" / "factory" / "DEMO-1" / "approvals.json").exists()


def test_approvals_record_the_replay_fixture_as_its_own_authority(smoke: dict) -> None:
    run_dir = smoke["cwd"] / ".factory" / load_dag_module().run_id_for(smoke["run_id"], 0)
    approvals_path = run_dir / "work" / "docs" / "factory" / "DEMO-1" / "approvals.json"
    assert approvals_path.is_file(), sorted(str(p) for p in (smoke["cwd"] / ".factory").rglob("*"))
    approvals = json.loads(approvals_path.read_text(encoding="utf-8"))
    assert [a["gate"] for a in approvals] == ["intent", "plan"]
    assert [a["decision"] for a in approvals] == ["approve", "approve"]
    # "replay:*" and mode "replay" so the chain never reads as a person, and so continuation
    # refuses this decision the moment the same run is backend-managed.
    assert [a["actor"] for a in approvals] == ["replay:scripted-replay"] * 2
    assert [a["mode"] for a in approvals] == ["replay", "replay"]
    assert all(len(a["artifact_sha256"]) == 64 for a in approvals)
    assert (run_dir / "pr.md").is_file()  # deliver published to the local bare remote
    metrics = json.loads((run_dir / "work" / "docs" / "factory" / "DEMO-1" / "metrics.json").read_text("utf-8"))
    assert metrics["agent"] == smoke["agent"] and metrics["tests_passed"] is True
    assert metrics["approvers"] == ["replay:scripted-replay"] * 2
