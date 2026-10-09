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

import pytest
from backend_support import LINE, FakeAirflow

from swfactory.backend.service import Factory, Refused


def _quiet(factory: Factory, airflow: FakeAirflow) -> bool:
    """Nothing was admitted: no Airflow write, no Factory Cell, no admission slot."""
    snapshot = factory.control.admission.snapshot(limit=100)
    return (
        all(method == "GET" for method, _path, _body in airflow.requests)
        and not factory.cell_store.list(limit=100)
        and not snapshot["active"]
        and not snapshot["queued"]
    )


def test_both_submission_routes_refuse_while_the_backend_is_draining(
    factory: Factory, airflow: FakeAirflow, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guard is backend-wide, so the refusal names the backend rather than a line. On
    ``/work-orders`` it fires before the line is resolved (pinned: resolving it fails the test); the
    compatibility mount resolves the line from its path first, then reaches the same guard.
    `mutation_ready` is not a per-line property; a message claiming otherwise would tell an operator
    to look at the wrong thing."""
    monkeypatch.setenv("SWF_DRAIN", "1")
    assert factory.capabilities()["mutation_ready"] is False

    with pytest.MonkeyPatch.context() as mp, pytest.raises(Refused) as caught:
        mp.setattr(factory, "_line", lambda name: pytest.fail(f"/work-orders resolved line {name!r} before draining"))
        factory.operation("/work-orders", {"line": LINE, "issues": ["42"]})
    canonical = caught.value
    with pytest.raises(Refused) as caught:
        factory.compatibility("POST", f"/dags/{LINE}/dagRuns", {"conf": {"issues": ["42"]}})
    compat = caught.value

    for error in (canonical, compat):
        assert error.status == 503
        assert "draining" in str(error)
    assert str(canonical) == str(compat), "one drain, one refusal"
    assert _quiet(factory, airflow), "a refused submission must leave no trace"


def test_a_ready_backend_still_admits_through_the_compatibility_mount(factory: Factory, airflow: FakeAirflow) -> None:
    status, payload = factory.compatibility("POST", f"/dags/{LINE}/dagRuns", {"conf": {"issues": ["42"]}})
    assert status == 201 and payload["dag_run_id"]
    assert [path for _method, path, _body in airflow.calls("POST")] == [f"/dags/{LINE}/dagRuns"]


def test_a_drain_still_lets_an_operator_stop_work_that_already_exists(
    factory: Factory, airflow: FakeAirflow, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A drain that also blocked cancellation would strand the very runs it exists to protect."""
    airflow.routes[("PATCH", f"/dags/{LINE}/dagRuns/swf__run")] = {"state": "failed"}
    monkeypatch.setenv("SWF_DRAIN", "1")
    status, _ = factory.compatibility("PATCH", f"/dags/{LINE}/dagRuns/swf__run", {"state": "failed"})
    assert status == 200
