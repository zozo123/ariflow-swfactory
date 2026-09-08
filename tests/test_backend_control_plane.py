from __future__ import annotations

from pathlib import Path

import pytest

from swfactory.backend.service import Factory, Refused
from swfactory.cell_runtime import identity_for_job

TOKEN = "t" * 40


def _factory(tmp_path: Path) -> Factory:
    return Factory(token=TOKEN, airflow_url="http://127.0.0.1:8080", state_root=tmp_path / ".factory")


def _job() -> dict[str, object]:
    return {
        "issue": "42",
        "repo": "acme/widgets",
        "dir": "",
        "base_branch": "main",
        "job_idx": 0,
    }


def test_drain_guard_is_shared_by_submit_and_compatibility_without_side_effects(tmp_path: Path, monkeypatch) -> None:
    factory = _factory(tmp_path)
    monkeypatch.setenv("SWF_DRAIN", "1")
    remote_calls: list[tuple] = []
    factory.airflow = lambda *args: remote_calls.append(args) or (500, {})  # type: ignore[method-assign]
    try:
        with pytest.raises(Refused, match="draining or not mutation-ready") as canonical:
            factory.submit({"line": "factory", "issues": ["42"]})
        assert canonical.value.status == 503

        with pytest.raises(Refused, match="draining or not mutation-ready") as compat:
            factory.compatibility(
                "POST",
                "/dags/factory/dagRuns",
                {"conf": {"issues": ["42"]}},
            )
        assert compat.value.status == 503
        assert remote_calls == []
        assert factory.cell_store.list(limit=100) == []
        snapshot = factory.control.admission.snapshot(limit=100)
        assert snapshot["active"] == [] and snapshot["queued"] == []
    finally:
        factory.close()


def _bound_cell(factory: Factory) -> dict:
    job = _job()
    identity = identity_for_job(job)
    cell = factory.cell_store.activate(identity, actor="test")
    cell = factory.cell_store.patch(
        cell["cell_id"],
        int(cell["epoch"]),
        "bind:test",
        state="queued",
        airflow_dag_id="factory",
        airflow_run_id="run-1",
        map_index=0,
    )
    decision = factory.control.submit(
        work_id="work-1",
        repo="acme/widgets",
        actor="operator",
        blueprint="factory",
    )
    assert decision.state == "active"
    factory.control.bind_cell("work-1", cell["cell_id"], int(cell["epoch"]))
    return cell


def test_managed_run_cancel_persists_cell_before_remote_stop_and_then_releases_capacity(tmp_path: Path) -> None:
    factory = _factory(tmp_path)
    cell = _bound_cell(factory)
    calls: list[tuple[str, str]] = []

    def airflow(method: str, path: str, body):
        calls.append((method, path))
        if method == "PATCH":
            assert factory.cell_store.get(cell["cell_id"])["state"] == "cancelled"
            assert factory.control.admission.snapshot(limit=10)["active"], "capacity released before stop receipt"
            return 200, {"state": "failed", "dag_run_id": "run-1"}
        raise AssertionError((method, path, body))

    factory.airflow = airflow  # type: ignore[method-assign]
    try:
        status, payload = factory.compatibility(
            "PATCH",
            "/dags/factory/dagRuns/run-1",
            {"state": "failed"},
        )
        assert status == 200 and payload["managed_cells"] == 1
        assert factory.cell_store.get(cell["cell_id"])["state"] == "cancelled"
        assert factory.control.admission.snapshot(limit=10)["active"] == []
        assert calls == [("PATCH", "/dags/factory/dagRuns/run-1")]
        assert any(
            event["kind"] == "patch" and event["payload"].get("state") == "cancelled"
            for event in factory.cell_store.history(cell["cell_id"])
        )
    finally:
        factory.close()


def test_ambiguous_managed_cancel_keeps_capacity_until_observation_converges(tmp_path: Path) -> None:
    factory = _factory(tmp_path)
    cell = _bound_cell(factory)
    mode = {"value": "fail"}

    def airflow(method: str, path: str, body):
        if method == "PATCH":
            return 503, {"detail": "lost response"}
        if method == "GET" and mode["value"] == "observed":
            return 200, {"state": "failed", "dag_run_id": "run-1"}
        return 503, {}

    factory.airflow = airflow  # type: ignore[method-assign]
    request = {
        "cell_id": cell["cell_id"],
        "epoch": int(cell["epoch"]),
        "state": "cancelled",
        "operation_key": "operator:cancel:test",
    }
    try:
        with pytest.raises(Refused):
            factory._transition(request)
        assert factory.cell_store.get(cell["cell_id"])["state"] == "cancelled"
        assert factory.control.admission.snapshot(limit=10)["active"], "ambiguous stop released capacity"
        assert any(row["kind"] == "airflow_cancel" for row in factory.control.operations.unresolved(limit=10))

        mode["value"] = "observed"
        result = factory._transition(request)
        assert result["cell"]["state"] == "cancelled"
        assert factory.control.admission.snapshot(limit=10)["active"] == []
    finally:
        factory.close()
