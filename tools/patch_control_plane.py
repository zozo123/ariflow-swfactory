from pathlib import Path

P = Path('src/swfactory/backend/service.py')
text = P.read_text(encoding='utf-8')


def replace_once(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f'expected exactly one match, got {count}: {old[:140]!r}')
    text = text.replace(old, new, 1)


replace_once(
'''    def submit(self, body: dict[str, Any]) -> dict[str, Any]:
        line = self._line(text(body, "line"))
''',
'''    def submit(self, body: dict[str, Any]) -> dict[str, Any]:
        # Admission readiness belongs to the shared use case, not one HTTP route. Every caller --
        # canonical work-orders, the Airflow compatibility mount, and future internal callers --
        # must cross this guard before a reservation, Cell activation, or remote mutation exists.
        if not self.capabilities()["mutation_ready"]:
            raise Refused(503, "backend is draining or not mutation-ready")
        line = self._line(text(body, "line"))
''')

replace_once(
'''        operation_key = text(
            {"operation_key": body.get("operation_key") or f"airflow:{requested}"},
''',
'''        operation_key = text(
            {"operation_key": body.get("operation_key") or f"operator:{requested}"},
''')

marker = '    def _transition(self, body: dict[str, Any]) -> dict[str, Any]:\n'
idx = text.index(marker)
helper = '''    def _journal_airflow_cancel(self, cell: dict[str, Any]) -> dict[str, Any]:
        """Stop one managed Airflow run through the same durable operation journal as dispatch.

        The Cell is cancelled *before* this method is called. Capacity is released only after this
        operation is committed or observation proves the run is already terminal, so an ambiguous
        stop can never admit replacement work while the old run may still be alive.
        """
        dag_id = str(cell.get("airflow_dag_id") or "")
        run_id = str(cell.get("airflow_run_id") or "")
        if not dag_id or not run_id:
            return {"state": "no_remote_run"}
        path = "/dags/" + urllib.parse.quote(dag_id, safe="") + "/dagRuns/" + urllib.parse.quote(run_id, safe="")
        ref = OperationRef.build(str(cell["cell_id"]), int(cell["epoch"]), "airflow_cancel", dag_id, run_id)

        def apply() -> dict[str, Any]:
            payload = self._checked_airflow("PATCH", path, {"state": "failed"})
            return payload if isinstance(payload, dict) else {"state": "failed"}

        def reconcile() -> MutationOutcome:
            status, payload = self.airflow("GET", path, None)
            if status == 404:
                return MutationOutcome(
                    "committed",
                    {"state": "absent", "dag_run_id": run_id},
                    {"status": status, "dag_run_id": run_id},
                    "managed Airflow run is absent",
                )
            if status == 200 and isinstance(payload, dict):
                state = str(payload.get("state") or "")
                if state in {"failed", "success"}:
                    return MutationOutcome(
                        "committed",
                        payload,
                        {"status": status, "state": state, "dag_run_id": run_id},
                        "managed Airflow run is terminal",
                    )
                return MutationOutcome(
                    "definitely_absent",
                    None,
                    {"status": status, "state": state, "dag_run_id": run_id},
                    "managed Airflow run is still live",
                )
            return MutationOutcome(
                "ambiguous",
                None,
                {"status": status, "dag_run_id": run_id},
                "managed Airflow cancellation outcome is unknown",
            )

        result = self.control.mutate(ref, apply, replay_safe=True, reconcile=reconcile)
        return result if isinstance(result, dict) else {"state": "failed"}

'''
text = text[:idx] + helper + text[idx:]

replace_once(
'''            released = (
                self.control.release_cell(cell_id, epoch=epoch, state=requested) if requested in TERMINAL_STATES else []
            )
            next_state = requested
''',
'''            if (
                requested == "cancelled"
                and not operation_key.startswith("airflow:")
                and updated.get("airflow_dag_id")
                and updated.get("airflow_run_id")
            ):
                self._journal_airflow_cancel(updated)
            released = (
                self.control.release_cell(cell_id, epoch=epoch, state=requested) if requested in TERMINAL_STATES else []
            )
            next_state = requested
''')

replace_once(
'''        if method == "PATCH" and len(segments) == 4 and segments[2] == "dagRuns" and body == {"state": "failed"}:
            return self.airflow(method, path, body)
''',
'''        if method == "PATCH" and len(segments) == 4 and segments[2] == "dagRuns" and body == {"state": "failed"}:
            dag_id, run_id = segments[1], segments[3]
            managed = [
                cell
                for cell in self.cell_store.list(limit=1000)
                if cell.get("airflow_dag_id") == dag_id
                and cell.get("airflow_run_id") == run_id
                and (cell.get("state") not in TERMINAL_STATES or cell.get("state") == "cancelled")
            ]
            if not managed:
                return self.airflow(method, path, body)
            for cell in managed:
                self._transition(
                    {
                        "cell_id": cell["cell_id"],
                        "epoch": int(cell["epoch"]),
                        "state": "cancelled",
                        "operation_key": f"operator:airflow-stop:{run_id}:{cell['cell_id']}",
                    }
                )
            return 200, {"state": "failed", "managed_cells": len(managed)}
''')

P.write_text(text, encoding='utf-8')

T = Path('tests/test_backend_control_plane.py')
T.write_text('''from __future__ import annotations

from pathlib import Path

import pytest

from swfactory.cell_runtime import identity_for_job
from swfactory.backend.service import Factory, Refused


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
''', encoding='utf-8')
