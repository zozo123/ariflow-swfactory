from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from swfactory.backend.service import Factory, Refused

TOKEN = "t" * 32


def factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Factory:
    monkeypatch.setenv("AIRFLOW_TOKEN", "airflow-test-token")
    monkeypatch.setenv("SWF_BACKEND_TOKEN", TOKEN)
    return Factory(
        token=TOKEN,
        airflow_url="http://localhost:8080",
        root=tmp_path,
        state_root=tmp_path / ".factory",
    )


def test_compatibility_rejects_encoded_path_traversal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    backend = factory(tmp_path, monkeypatch)
    try:
        with pytest.raises(Refused, match="invalid path segment") as caught:
            backend.compatibility("GET", "/dags/%2e%2e/dagRuns", None)
        assert caught.value.status == 400
    finally:
        backend.close()


def test_work_orders_refuse_drain_before_submit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    backend = factory(tmp_path, monkeypatch)
    monkeypatch.setattr(backend, "capabilities", lambda: {"mutation_ready": False})
    monkeypatch.setattr(backend, "submit", lambda body: pytest.fail(f"submit reached during drain: {body}"))
    try:
        with pytest.raises(Refused, match="draining or not mutation-ready") as caught:
            backend.operation("/work-orders", {"line": "factory", "issues": ["1"]})
        assert caught.value.status == 503
    finally:
        backend.close()


def test_doctor_reports_managed_worker_callback_contract(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    backend = factory(tmp_path, monkeypatch)
    monkeypatch.delenv("SWF_BACKEND_URL", raising=False)
    monkeypatch.setattr(shutil, "which", lambda _tool: "/bin/true")

    def airflow(method: str, path: str, body=None):
        assert method == "GET"
        if path == "/monitor/health":
            return {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}
        if path == "/dags?limit=1":
            return {"dags": []}
        raise AssertionError(path)

    monkeypatch.setattr(backend, "_checked_airflow", airflow)
    try:
        rows = backend.operation("/doctor", {})
        callback = next(row for row in rows if row["name"] == "managed worker callback")
        assert callback["ok"] is False
        # Informational, not required. This test first asserted `required is True`, and the row it
        # demanded failed `swf doctor` against a healthy backend in the live e2e harness: the backend
        # host never sets its own SWF_BACKEND_URL (it does not call itself), so a required row here
        # can only ever be red on a correct deployment. That is #1217, re-created by a test.
        assert callback["required"] is False and callback["status"] == "warn"
        assert "SWF_BACKEND_URL" in callback["fix"]
        assert "SWF_BACKEND_TOKEN" in callback["fix"]

        monkeypatch.setenv("SWF_BACKEND_URL", "http://backend:8082")
        rows = backend.operation("/doctor", {})
        callback = next(row for row in rows if row["name"] == "managed worker callback")
        assert callback["ok"] is True
        assert callback["fix"] == ""
    finally:
        backend.close()
