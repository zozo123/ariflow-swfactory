"""Drain must refuse new work on *every* submission route, not just the canonical one.

Draining is how a backend is taken out of service before an upgrade: it stops admitting work so
that nothing half-finished is stranded by the restart. The console does not submit through
``POST /v1/work-orders`` — it submits through the Airflow compatibility mount
(``POST /v1/airflow/api/v2/dags/<line>/dagRuns``), which reaches the same
:meth:`Factory.submit`. A guard that lives on one route and not the other is not a drain.

The fake opener stands in for Airflow so that "no Airflow write happened" is an assertion about a
recorded list rather than a hope.
"""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from swfactory.backend.service import Factory, Refused

AF = "http://localhost:8080"
TOKEN = "t" * 32


class _Resp:
    def __init__(self, code: int, payload: Any) -> None:
        self.code = code
        self._raw = b"" if payload is None else json.dumps(payload).encode()

    def read(self, _limit: int = -1) -> bytes:
        return self._raw

    def close(self) -> None:
        pass

    def __enter__(self) -> _Resp:
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False


class FakeAirflow:
    """Answers ``(METHOD, path)`` from ``routes`` and records every request that reaches it."""

    def __init__(self, routes: dict[tuple[str, str], Any] | None = None) -> None:
        self.routes = routes or {}
        self.requests: list[tuple[str, str]] = []

    def open(self, request: urllib.request.Request, timeout: float = 0) -> _Resp:
        path = request.full_url.removeprefix(AF + "/api/v2")
        self.requests.append((request.get_method(), path))
        payload = self.routes.get((request.get_method(), path))
        if payload is None:
            raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, io.BytesIO(b'{"detail":"no route"}'))
        return _Resp(200, payload)

    @property
    def writes(self) -> list[tuple[str, str]]:
        return [call for call in self.requests if call[0] != "GET"]


@pytest.fixture
def factory(tmp_path: Path) -> Any:
    served = Factory(token=TOKEN, airflow_url=AF, root=tmp_path, state_root=tmp_path / "state")
    served.opener = FakeAirflow(
        {
            ("PATCH", "/dags/factory"): {"is_paused": False},
            ("POST", "/dags/factory/dagRuns"): {"dag_run_id": "swf__run"},
            ("PATCH", "/dags/factory/dagRuns/swf__run"): {"state": "failed"},
        }
    )
    try:
        yield served
    finally:
        served.close()


def _quiet(factory: Factory) -> bool:
    """Nothing was admitted: no Airflow write, no Factory Cell, no admission slot."""
    snapshot = factory.control.admission.snapshot(limit=100)
    return (
        not factory.opener.writes
        and not factory.cell_store.list(limit=100)
        and not snapshot["active"]
        and not snapshot["queued"]
    )


def test_both_submission_routes_refuse_while_the_backend_is_draining(
    factory: Factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guard is backend-wide and fires before the line is resolved, so the refusal names the
    backend rather than a line. `mutation_ready` is not a per-line property; a message claiming
    otherwise would tell an operator to look at the wrong thing."""
    monkeypatch.setenv("SWF_DRAIN", "1")
    assert factory.capabilities()["mutation_ready"] is False

    with pytest.raises(Refused) as caught:
        factory.operation("/work-orders", {"line": "factory", "issues": ["42"]})
    canonical = caught.value
    with pytest.raises(Refused) as caught:
        factory.compatibility("POST", "/dags/factory/dagRuns", {"conf": {"issues": ["42"]}})
    compat = caught.value

    for error in (canonical, compat):
        assert error.status == 503
        assert "draining" in str(error)
    assert str(canonical) == str(compat), "one drain, one refusal"
    assert _quiet(factory), "a refused submission must leave no trace"


def test_a_ready_backend_still_admits_through_the_compatibility_mount(factory: Factory) -> None:
    status, payload = factory.compatibility("POST", "/dags/factory/dagRuns", {"conf": {"issues": ["42"]}})
    assert status == 201 and payload["dag_run_id"]
    assert ("POST", "/dags/factory/dagRuns") in factory.opener.writes


def test_a_drain_still_lets_an_operator_stop_work_that_already_exists(
    factory: Factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A drain that also blocked cancellation would strand the very runs it exists to protect."""
    monkeypatch.setenv("SWF_DRAIN", "1")
    status, _ = factory.compatibility("PATCH", "/dags/factory/dagRuns/swf__run", {"state": "failed"})
    assert status == 200
