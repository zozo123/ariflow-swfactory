"""The worker's half of #2071: a lifecycle report that cannot be delivered is written down, not lost.

Every scenario here goes through a real HTTP round trip (a local backend stand-in, or a port with
nothing listening) rather than a monkeypatched ``urlopen``: the failures that lost callbacks in
production were transport failures, and a fake that never opens a socket cannot produce one.
"""

from __future__ import annotations

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any

import pytest

from swfactory import cell_callback
from swfactory.cell_callback import DEBT_XCOM_KEY, CellCallbackError
from swfactory.recovery_accounting import CallbackDebt

TOKEN = "b" * 40
_UNSET = object()


class FakeTI:
    """Just enough of Airflow 3's ``RuntimeTaskInstance`` XCom surface, with its exact shapes:
    ``task_ids=None`` means the calling task, a list of task ids returns a list, a missing entry
    is ``None``, and an unspecified ``map_indexes`` returns every index of a task."""

    def __init__(self, task_id: str, *, map_index: int = 0, try_number: int = 1) -> None:
        self.task_id = task_id
        self.map_index = map_index
        self.try_number = try_number
        self.store: dict[tuple[str, int, str], Any] = {}

    def xcom_pull(self, task_ids: Any = None, key: str = "return_value", map_indexes: Any = _UNSET) -> Any:
        single = isinstance(task_ids, (str, type(None)))
        ids = [self.task_id] if task_ids is None else [task_ids] if isinstance(task_ids, str) else list(task_ids)
        values: list[Any] = []
        for task_id in ids:
            if map_indexes is _UNSET:
                values.extend(v for (t, _i, k), v in self.store.items() if t == task_id and k == key)
            else:
                values.append(self.store.get((task_id, int(map_indexes), key)))
        if single and (len(values) == 1 or map_indexes is not _UNSET):
            return values[0] if values else None
        return values

    def xcom_push(self, key: str, value: Any) -> None:
        self.store[(self.task_id, self.map_index, key)] = value


def _job(idx: int = 0, *, managed: bool = True) -> dict[str, Any]:
    return {"cell_managed": managed, "cell_id": "cell_abc", "cell_epoch": 2, "job_idx": idx, "issue": "1"}


def _context(ti: FakeTI, *, task_ids: list[str] | None = None) -> dict[str, Any]:
    return {
        "ti": ti,
        "dag_run": SimpleNamespace(run_id="swf__run"),
        "dag": SimpleNamespace(task_ids=task_ids or ["fan_out", "job.setup", "job.build", "job.teardown"]),
    }


class Backend:
    """A backend stand-in on a real port. ``routes[path](body) -> (status, payload)``."""

    def __init__(self) -> None:
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self.routes: dict[str, Any] = {}
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_a: Any) -> None:
                pass

            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
                outer.posts.append((self.path, body))
                route = outer.routes.get(self.path)
                status, payload = route(body) if route else (404, {"detail": "no route"})
                raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def echo_transitions(self) -> None:
        self.routes["/v1/cells/transition"] = lambda body: (
            200,
            {"cell": {"cell_id": body["cell_id"], "epoch": body["epoch"], "state": body["state"]}, "released_work": []},
        )

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch):
    box = Backend()
    monkeypatch.setenv("SWF_BACKEND_URL", box.url)
    monkeypatch.setenv("SWF_BACKEND_TOKEN", TOKEN)
    yield box
    box.close()


@pytest.fixture
def no_backend(monkeypatch: pytest.MonkeyPatch) -> str:
    """A port nothing listens on: the outage that produced the lost callbacks in the issue."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        url = f"http://127.0.0.1:{probe.getsockname()[1]}"
    monkeypatch.setenv("SWF_BACKEND_URL", url)
    monkeypatch.setenv("SWF_BACKEND_TOKEN", TOKEN)
    return url


def _debts(ti: FakeTI) -> list[CallbackDebt]:
    rows = ti.store.get((ti.task_id, ti.map_index, DEBT_XCOM_KEY)) or []
    return [CallbackDebt(**row) for row in rows]


# ---------------------------------------------------------------------- recording the debt


def test_a_report_that_cannot_reach_the_backend_records_its_debt_and_fails_closed(no_backend: str) -> None:
    ti = FakeTI("job.deliver", try_number=2)
    with pytest.raises(CellCallbackError, match="unavailable"):
        cell_callback.report(_job(), "success", _context(ti), "delivered")
    assert _debts(ti) == [CallbackDebt("cell_abc", 2, "swf__run", "job.deliver", "success", attempt=2)]


def test_a_refused_report_records_its_debt(backend: Backend) -> None:
    backend.routes["/v1/cells/transition"] = lambda body: (503, {"detail": "draining"})
    ti = FakeTI("job.setup")
    with pytest.raises(CellCallbackError, match="refused: draining"):
        cell_callback.report(_job(), "running", _context(ti), "setup")
    assert [d.desired_state for d in _debts(ti)] == ["running"]


def test_the_same_lost_report_is_recorded_once(no_backend: str) -> None:
    ti = FakeTI("job.setup")
    for _ in range(2):
        with pytest.raises(CellCallbackError):
            cell_callback.report(_job(), "running", _context(ti), "setup")
    assert len(_debts(ti)) == 1


def test_report_failure_records_the_failed_debt_without_raising(no_backend: str) -> None:
    """The on_failure_callback body. It used to swallow this; now the loss is written down."""
    ti = FakeTI("job.build", map_index=1)
    ti.store[("fan_out", -1, "return_value")] = [_job(0), _job(1)]
    cell_callback.report_failure(_context(ti))
    assert _debts(ti) == [CallbackDebt("cell_abc", 2, "swf__run", "job.build", "failed", attempt=1)]


def test_report_failure_leaves_unmanaged_and_unknown_jobs_alone(no_backend: str) -> None:
    ti = FakeTI("job.build", map_index=0)
    ti.store[("fan_out", -1, "return_value")] = [_job(0, managed=False)]
    cell_callback.report_failure(_context(ti))
    ti.map_index = 5
    cell_callback.report_failure(_context(ti))
    assert not any(k[2] == DEBT_XCOM_KEY for k in ti.store)


def test_report_failure_lets_a_programming_error_reach_the_airflow_log(no_backend: str) -> None:
    """Only the delivery failure is caught (it is recorded); a broken job is not silently dropped."""
    ti = FakeTI("job.build")
    broken = _job()
    del broken["job_idx"]
    ti.store[("fan_out", -1, "return_value")] = [broken]
    with pytest.raises(KeyError, match="job_idx"):
        cell_callback.report_failure(_context(ti))


# ------------------------------------------------------------------------ settling the debt


def _owed(ti: FakeTI, desired: str = "failed", *, task_id: str = "job.build", epoch: int = 2) -> CallbackDebt:
    debt = CallbackDebt("cell_abc", epoch, "swf__run", task_id, desired, attempt=1)
    ti.store[(task_id, ti.map_index, DEBT_XCOM_KEY)] = [debt.__dict__]
    return debt


def test_outstanding_debt_is_replayed_before_the_next_report_while_the_cell_is_live(backend: Backend) -> None:
    """Teardown always runs; its report is where a job's earlier lost ``failed`` gets delivered."""
    backend.echo_transitions()
    backend.routes["/v1/cells/inspect"] = lambda body: (200, {"cell_id": "cell_abc", "epoch": 2, "state": "running"})
    ti = FakeTI("job.teardown")
    debt = _owed(ti, "failed")
    cell_callback.report(_job(), "cleaned", _context(ti), "teardown")
    assert [(p, b.get("state"), b.get("operation_key")) for p, b in backend.posts] == [
        ("/v1/cells/inspect", None, None),
        ("/v1/cells/transition", "failed", f"airflow-debt:{debt.key}"),
        ("/v1/cells/transition", "cleaned", "airflow:swf__run:0:job.teardown:1:teardown"),
    ]


@pytest.mark.parametrize(
    ("answer", "why"),
    [
        ((200, {"cell_id": "cell_abc", "epoch": 2, "state": "failed"}), "the report already landed (ADOPT)"),
        ((200, {"cell_id": "cell_abc", "epoch": 3, "state": "running"}), "the epoch moved on (REFUSE)"),
        ((200, {"cell_id": "cell_abc", "epoch": 2, "state": "success"}), "the Cell ended differently (REFUSE)"),
        ((404, {"detail": "no Factory Cell"}), "the Cell is gone (REFUSE)"),
    ],
)
def test_a_settled_or_voided_debt_is_not_replayed(backend: Backend, answer: tuple, why: str) -> None:
    backend.echo_transitions()
    backend.routes["/v1/cells/inspect"] = lambda body: answer
    ti = FakeTI("job.teardown")
    _owed(ti, "failed")
    cell_callback.report(_job(), "cleaned", _context(ti), "teardown")
    states = [b.get("state") for p, b in backend.posts if p == "/v1/cells/transition"]
    assert states == ["cleaned"], f"{why}: {states}"


def test_a_debt_that_cannot_be_settled_fails_the_report_closed_and_is_itself_recorded(backend: Backend) -> None:
    backend.echo_transitions()
    backend.routes["/v1/cells/inspect"] = lambda body: (503, {"detail": "down"})
    ti = FakeTI("job.teardown")
    _owed(ti, "failed")
    with pytest.raises(CellCallbackError, match="cannot be inspected"):
        cell_callback.report(_job(), "cleaned", _context(ti), "teardown")
    assert [b.get("state") for p, b in backend.posts if p == "/v1/cells/transition"] == []
    assert [d.desired_state for d in _debts(ti)] == ["cleaned"]


def test_debt_is_scoped_to_the_job_group_and_map_index(backend: Backend) -> None:
    backend.echo_transitions()
    backend.routes["/v1/cells/inspect"] = lambda body: (200, {"cell_id": "cell_abc", "epoch": 2, "state": "running"})
    ti = FakeTI("job.teardown", map_index=1)
    other = FakeTI("job.build", map_index=0)
    other_debt = CallbackDebt("cell_other", 2, "swf__run", "job.build", "failed", attempt=1)
    ti.store[("job.build", 0, DEBT_XCOM_KEY)] = [other_debt.__dict__]
    ti.store[("fan_out", -1, DEBT_XCOM_KEY)] = [_owed(other, "failed").__dict__]  # outside the group
    cell_callback.report(_job(1), "cleaned", _context(ti), "teardown")
    assert [p for p, _ in backend.posts] == ["/v1/cells/transition"], "a sibling job's debt is not this job's"


def test_unmanaged_jobs_never_touch_the_backend(backend: Backend) -> None:
    ti = FakeTI("job.setup")
    assert cell_callback.report(_job(managed=False), "running", _context(ti), "setup") is None
    assert backend.posts == []


# ---------------------------------------------------------------------- the maintain path


def test_resume_backend_asks_the_queue_to_resume(backend: Backend) -> None:
    backend.routes["/v1/queue/resume"] = lambda body: (200, {"resumed": [{"work_id": "submit_b", "dispatched": True}]})
    assert cell_callback.resume_backend() == {"resumed": [{"work_id": "submit_b", "dispatched": True}]}
    assert backend.posts == [("/v1/queue/resume", {})]


def test_resume_backend_is_a_no_op_without_a_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SWF_BACKEND_URL", raising=False)
    assert cell_callback.resume_backend() is None


def test_resume_backend_reports_an_unreachable_backend(no_backend: str) -> None:
    with pytest.raises(CellCallbackError, match="unavailable"):
        cell_callback.resume_backend()
