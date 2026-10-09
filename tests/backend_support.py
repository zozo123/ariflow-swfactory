"""The backend test harness: fake Airflow transports, a loopback client and a restartable backend.

A plain module (pytest collects only ``test_*.py``); test files import it as ``backend_support``.
The fixtures over it (``env``, ``airflow``, ``factory``, ``client``, ``backend``) live in
``conftest.py``.
"""

from __future__ import annotations

import http.client
import io
import json
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

from swfactory.admission import Limits
from swfactory.backend.service import Factory

TOKEN = "t" * 40  # Factory demands >= 32 non-whitespace characters
REPO = "zozo123/ariflow-swfactory"  # the repo blueprints/default.toml targets
AF = "http://localhost:8080"
LINE = "factory"


class FakeResponse(io.BytesIO):
    """Enough of ``http.client.HTTPResponse``: ``code``/``status``, a sized ``read``, a context.

    A payload is sent as JSON; bytes pass through, so a test can hand back the non-JSON body a
    proxy really returns."""

    def __init__(self, payload: Any = None, status: int = 200) -> None:
        if not isinstance(payload, bytes):
            payload = b"" if payload is None else json.dumps(payload).encode()
        super().__init__(payload)
        self.code = self.status = status


# ---------------------------------------------------------------- Airflow behind the opener


class FakeAirflow:
    """Answers ``(METHOD, path-after-/api/v2)`` from ``routes``; records every request.

    A route is a payload, a ``(status, payload)`` pair, or an exception to raise."""

    def __init__(self, routes: dict[tuple[str, str], Any] | None = None) -> None:
        self.routes = dict(routes or {})
        self.requests: list[tuple[str, str, Any]] = []

    def open(self, request: urllib.request.Request, timeout: float = 0) -> FakeResponse:
        path = request.full_url.removeprefix(AF + "/api/v2")
        body = json.loads(request.data) if request.data else None
        self.requests.append((request.get_method(), path, body))
        entry = self.routes.get((request.get_method(), path))
        if entry is None:
            raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, io.BytesIO(b'{"detail":"no route"}'))
        if isinstance(entry, Exception):
            raise entry
        status, payload = entry if isinstance(entry, tuple) else (200, entry)
        return FakeResponse(payload, status)

    def calls(self, method: str) -> list[tuple[str, str, Any]]:
        return [row for row in self.requests if row[0] == method]


DAG_HEALTHY = {
    "metadatabase": {"status": "healthy"},
    "scheduler": {"status": "healthy", "latest_scheduler_heartbeat": "2026-09-08T00:00:00Z"},
    "triggerer": {"status": "healthy"},
}
SUBMIT_ROUTES: dict[tuple[str, str], Any] = {
    ("GET", "/monitor/health"): DAG_HEALTHY,
    ("GET", "/dags?limit=1"): {"dags": [{"dag_id": LINE}], "total_entries": 1},
    ("GET", f"/dags/{LINE}"): {"dag_id": LINE, "is_paused": False},
    ("PATCH", f"/dags/{LINE}"): {"dag_id": LINE, "is_paused": False},
    ("POST", f"/dags/{LINE}/dagRuns"): (
        200,
        {"dag_run_id": "swf__run", "dag_id": LINE, "state": "queued"},
    ),
}


class Client:
    """Loopback driver for one live backend; ``raw`` exists for headers http.client will not send."""

    def __init__(self, server: ThreadingHTTPServer, airflow: FakeAirflow) -> None:
        self.host, self.port = server.server_address[0], server.server_address[1]
        self.airflow = airflow

    def call(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        token: str | None = TOKEN,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, Any]:
        raw = b"" if body is None else json.dumps(body).encode()
        sent = {"Content-Length": str(len(raw))}
        if token is not None:
            sent["Authorization"] = f"Bearer {token}"
        sent.update(headers or {})
        return self.raw(method, path, raw, sent)

    def raw(self, method: str, path: str, body: bytes, headers: dict[str, str]) -> tuple[int, Any]:
        conn = http.client.HTTPConnection(self.host, self.port, timeout=10)
        try:
            conn.putrequest(method, path, skip_accept_encoding=True)
            for name, value in headers.items():
                conn.putheader(name, value)
            conn.endheaders()
            if body:
                conn.send(body)
            response = conn.getresponse()
            payload = response.read()
            return response.status, (json.loads(payload) if payload else None)
        finally:
            conn.close()


# ---------------------------------------------------------------- a restartable backend (durable dispatch)

LINE_TOML = """
[blueprint]
name = "line"
version = 1
description = "test line"

[trigger]
kind = "manual"

[[targets]]
repo = "owner/one"
dir = "a"
base_branch = "main"

[stages]
order = ["intent", "spec", "plan", "build_and_test", "review", "deliver"]

[[gates]]
after = "intent"
artifact = "intent.md"
timeout_h = 1
assigned = []
auto = true

[limits]
max_build_iterations = 3
max_review_fixes = 1
max_turns = 40
budget_usd_per_stage = 2.0
budget_usd = 8.0
stage_timeout_h = 1
max_parallel_jobs = 2

[review]
policy = "REVIEW.md"
nit_cap = 3

[sandbox]
kind = "local"
ttl_s = 7200
idle_s = 900

[deliver]
labels = ["factory"]
"""

SECOND_TARGET = """
[[targets]]
repo = "owner/two"
dir = "b"
base_branch = "main"
"""


def write_line(root: Path, *extra: str) -> None:
    """``root/blueprints/line.toml``: ``LINE_TOML`` plus ``extra`` TOML tables."""
    (root / "blueprints").mkdir(parents=True, exist_ok=True)
    (root / "blueprints" / "line.toml").write_text(LINE_TOML + "".join(extra))


class AirflowRuns:
    """The four calls the backend makes, plus a truthful record of which runs really exist.

    Stands in for ``Factory.airflow``. ``posts`` counts attempts and ``runs`` holds the runs a POST
    actually created; a refused POST creates nothing, which is what makes the next attempt's
    reconcile answer ``definitely_absent``.
    """

    def __init__(self) -> None:
        self.posts: list[dict[str, Any]] = []
        self.created: list[dict[str, Any]] = []
        self.runs: dict[str, dict[str, Any]] = {}
        self.post_hook = None
        self.post_status = 200

    def __call__(self, method: str, path: str, body: dict | None) -> tuple[int, Any]:
        if path.count("/") == 2 and method in {"GET", "PATCH"}:
            return 200, {"is_paused": False}
        if method == "POST" and path.endswith("/dagRuns"):
            run_id = str(body["dag_run_id"])
            work_id = str(body["conf"]["_factory_submission_id"])
            self.posts.append({"dag_run_id": run_id, "work_id": work_id, "conf": body["conf"]})
            if self.post_hook is not None:
                self.post_hook(run_id)
            if self.post_status >= 300:
                return self.post_status, {"detail": "airflow refused"}
            if run_id in self.runs:
                # Airflow refuses a duplicate deterministic run id; a second dispatch must be
                # visible as a conflict rather than as a silently repeated run.
                return 409, {"detail": "duplicate dag_run_id"}
            self.runs[run_id] = {"dag_run_id": run_id, "state": "queued"}
            self.created.append({"dag_run_id": run_id, "work_id": work_id})
            return 200, self.runs[run_id]
        if method == "GET" and "/dagRuns/" in path:
            run_id = urllib.parse.unquote(path.rsplit("/", 1)[1])
            if run_id in self.runs:
                return 200, self.runs[run_id]
            return 404, {"detail": "absent"}
        raise AssertionError(f"unexpected Airflow call {method} {path}")

    def created_for(self, work_id: str) -> list[str]:
        return [run["dag_run_id"] for run in self.created if run["work_id"] == work_id]


class Backend:
    """One backend on one state directory, restartable in place."""

    def __init__(self, root: Path, limits: Limits) -> None:
        self.root = root
        self.limits = limits
        self.airflow = AirflowRuns()
        self.factory = self._open()

    def _open(self) -> Factory:
        factory = Factory(
            token=TOKEN, airflow_url="https://airflow.invalid:8080", root=self.root, state_root=self.root / ".factory"
        )
        factory.control.admission.limits = self.limits
        # A crashed process cannot hand its dispatch lease back, so redelivery waits for the lease
        # to expire. These tests restart instantly, so the lease has to expire instantly too.
        factory.control.admission.dispatch_lease_s = 0.0
        # Likewise a lost report is only looked for once the Cell's last write is older than the
        # read floor; these tests lose it and restart within the same second.
        factory.reconcile_interval_s = 0.0
        factory.airflow = self.airflow  # type: ignore[method-assign]
        return factory

    def restart(self) -> Factory:
        self.factory.close()
        self.factory = self._open()
        return self.factory

    def close(self) -> None:
        self.factory.close()

    def submit(self, *issues: str, actor: str = "op", targets: list[str] | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"line": "line", "issues": list(issues), "actor": actor}
        if targets is not None:
            body["targets"] = targets
        return self.factory.submit(body)

    def finish(self, cell_id: str, state: str = "success") -> dict[str, Any]:
        epoch = int(self.factory.cell_store.get(cell_id)["epoch"])
        return self.factory._transition(
            {"cell_id": cell_id, "epoch": epoch, "state": state, "operation_key": f"airflow:{state}:{cell_id}"}
        )

    def admission_state(self, work_id: str) -> str | None:
        return self.factory.control.admission.state_of(work_id)
