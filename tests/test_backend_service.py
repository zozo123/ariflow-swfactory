"""``swfactory.backend``: the authenticated HTTP boundary over fake Airflow/GitHub transports.

This is the privileged process — it holds the Airflow, GitHub and worker-provider credentials the
console deliberately does not — so every assertion here is about what the boundary *refuses*, not
only about what it returns.

Hermetic, in the house style of ``tests/test_control.py``: the Airflow transport is an ``opener``
answering a canned ``(method, path)`` table, ``gh``/``islo`` are fake client classes swapped into
the service module, and no test reaches a real network or a live Airflow. The server itself is
real: ``make_server`` on an ephemeral loopback port, driven with ``http.client``, because the
header-level guards (bearer compare, ``Content-Length`` bounds, ``Transfer-Encoding``) only exist
inside ``BaseHTTPRequestHandler`` and cannot be exercised by calling ``Factory`` directly.
"""

from __future__ import annotations

import http.client
import io
import json
import subprocess
import threading
import types
import urllib.error
import urllib.request
from collections.abc import Iterator
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from swfactory.backend import Factory, make_server
from swfactory.backend import scm_service as scm_mod
from swfactory.backend import service as service_mod
from swfactory.control import AirflowClient

TOKEN = "t" * 40  # Factory demands >= 32 non-whitespace characters
REPO = "zozo123/ariflow-swfactory"  # the repo blueprints/default.toml targets
AF = "http://localhost:8080"
LINE = "factory"


# ---------------------------------------------------------------- fakes


class _Resp:
    """Enough of ``http.client.HTTPResponse`` for ``Factory.airflow``: code, sized read, context."""

    def __init__(self, code: int, payload: Any) -> None:
        self.code = code
        # bytes pass through so a test can hand back the non-JSON body a proxy really returns
        if isinstance(payload, bytes):
            self._raw = payload
        else:
            self._raw = b"" if payload is None else json.dumps(payload).encode()

    def read(self, amount: int | None = None) -> bytes:
        return self._raw if amount is None else self._raw[:amount]

    def close(self) -> None:
        return None

    def __enter__(self) -> _Resp:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class FakeAirflow:
    """Answers ``(METHOD, path-after-/api/v2)`` from ``routes``; records every request."""

    def __init__(self, routes: dict[tuple[str, str], Any] | None = None) -> None:
        self.routes = dict(routes or {})
        self.requests: list[tuple[str, str, Any]] = []

    def open(self, request: urllib.request.Request, timeout: float = 0) -> _Resp:
        path = request.full_url.removeprefix(AF + "/api/v2")
        body = json.loads(request.data) if request.data else None
        self.requests.append((request.get_method(), path, body))
        entry = self.routes.get((request.get_method(), path))
        if entry is None:
            raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, io.BytesIO(b'{"detail":"no route"}'))
        if isinstance(entry, Exception):
            raise entry
        status, payload = entry if isinstance(entry, tuple) else (200, entry)
        return _Resp(status, payload)

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


class FakeGitHubClient:
    """Stands in for ``swfactory.control.GitHubClient``, whose runner default is bound at def time."""

    prs_returned: list[dict[str, Any]] = [{"number": 7, "title": "wired", "url": "https://x/7"}]
    issues_returned: list[dict[str, Any]] = [{"number": 9, "title": "filed", "url": "https://x/9"}]

    def __init__(self, repo: str, *_a: Any, **_kw: Any) -> None:
        self.repo = repo

    def prs(self, label: str = "factory", limit: int = 30) -> list[dict[str, Any]]:
        return self.prs_returned

    def issues(self, label: str = "factory", limit: int = 30) -> list[dict[str, Any]]:
        return self.issues_returned


class FakeIsloClient:
    def __init__(self, owner: str, *_a: Any, **_kw: Any) -> None:
        self.owner = owner

    def own_sandboxes(self) -> list[dict[str, Any]]:
        return [{"name": "swf-1", "status": "running", "created_by": self.owner}]

    def remove(self, name: str) -> dict[str, Any]:
        return {"removed": name}


# ---------------------------------------------------------------- harness


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


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ambient credentials/flags decide backend behavior; pin them so the suite is order-free."""
    for name in ("AIRFLOW_TOKEN", "AIRFLOW_USER", "AIRFLOW_PASSWORD", "SWF_DRAIN", "SWF_GENERATION"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def airflow() -> FakeAirflow:
    return FakeAirflow(SUBMIT_ROUTES)


@pytest.fixture
def factory(tmp_path: Path, airflow: FakeAirflow, env: None) -> Iterator[Factory]:
    made = Factory(
        token=TOKEN,
        airflow_url=AF,
        repo=REPO,
        owner="operator",
        root=tmp_path / "metrics",
        state_root=tmp_path / "state",
    )
    # Replace the transport *after* construction, the way test_control fakes an opener: the real
    # one is a urllib opener that would dial 127.0.0.1:8080 on the first read route.
    made.opener = types.SimpleNamespace(open=airflow.open)  # type: ignore[assignment]
    made.credentials = AirflowClient(AF, token="airflow-token", opener=airflow.open)
    try:
        yield made
    finally:
        made.close()


@pytest.fixture
def client(factory: Factory, airflow: FakeAirflow) -> Iterator[Client]:
    server = make_server(factory, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield Client(server, airflow)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def gh(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Fake every ``gh`` path: the injected clients and ``Factory._gh``'s direct ``subprocess.run``."""
    calls: list[list[str]] = []
    rows = {
        "pr list": [
            {
                "url": "https://x/7",
                "state": "OPEN",
                "title": "wired",
                "labels": [{"name": "factory"}],
                "headRefOid": "abc",
                "baseRefName": "main",
            }
        ],
        "pr view": {"url": "https://x/7", "statusCheckRollup": []},
    }

    def run(argv: list[str], **_kw: Any) -> subprocess.CompletedProcess[str]:
        calls.append(list(argv))
        key = " ".join(argv[1:3])
        return subprocess.CompletedProcess(argv, 0, json.dumps(rows.get(key, [])), "")

    monkeypatch.setattr(service_mod, "GitHubClient", FakeGitHubClient)
    monkeypatch.setattr(service_mod, "IsloClient", FakeIsloClient)
    monkeypatch.setattr(
        service_mod,
        "subprocess",
        types.SimpleNamespace(run=run, SubprocessError=subprocess.SubprocessError),
    )
    return calls


def submit(client: Client, issues: list[str] | None = None) -> dict[str, Any]:
    status, payload = client.call("POST", "/v1/work-orders", {"line": LINE, "issues": issues or ["101"]})
    assert status == 200, payload
    assert payload["state"] == "submitted", payload
    return payload


# ---------------------------------------------------------------- liveness / readiness / health


def test_public_probes_answer_without_a_token(client: Client) -> None:
    """The three probe documents are the only unauthenticated surface, and they carry no state."""
    status, live = client.call("GET", "/v1/liveness", token=None)
    assert (status, live) == (200, {"service": "swfactory", "live": True, "api_version": 1})

    status, ready = client.call("GET", "/v1/readiness", token=None)
    assert status == 200
    assert ready["read_ready"] is True and ready["mutation_ready"] is True

    # `/health` is NOT public. It returns the full capability document -- drain state, serving
    # generation, every readiness flag -- and #2088 moved it behind the token for that reason.
    # `/readiness` above is the minimal unauthenticated subset an orchestrator needs. An earlier
    # version of this test asserted 200 here, which pinned the leak as the contract.
    status, payload = client.call("GET", "/v1/health", token=None)
    assert status == 401, payload
    status, health = client.call("GET", "/v1/health")
    assert status == 200
    assert health["service"] == "swfactory" and isinstance(health["api_version"], int)
    assert health["mutation_ready"] is True


def test_probe_paths_are_public_only_for_get(client: Client) -> None:
    """`_public_probe` is method-scoped; a POST to a probe path must still meet the token."""
    status, payload = client.call("POST", "/v1/liveness", {}, token=None)
    assert status == 401 and payload == {"detail": "factory backend token required"}


def test_readiness_reports_draining(client: Client, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWF_DRAIN", "1")
    status, ready = client.call("GET", "/v1/readiness", token=None)
    assert status == 200  # still readable while draining
    assert ready["read_ready"] is True and ready["mutation_ready"] is False


# ---------------------------------------------------------------- authentication


def test_factory_refuses_a_weak_token_at_construction(tmp_path: Path, env: None) -> None:
    """A short or whitespace-bearing token is the difference between a guard and a formality."""
    for bad in ("", "short", "t" * 31, "t" * 20 + " " + "t" * 20):
        with pytest.raises(ValueError, match="at least 32 non-whitespace"):
            Factory(token=bad, airflow_url=AF, state_root=tmp_path / "s")


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({}, id="absent"),
        pytest.param({"Authorization": ""}, id="empty-header"),
        pytest.param({"Authorization": "Bearer "}, id="empty-token"),
        pytest.param({"Authorization": "Bearer"}, id="scheme-only"),
        pytest.param({"Authorization": "Bearer " + "x" * 40}, id="wrong-token"),
        pytest.param({"Authorization": "Bearer " + TOKEN[:-1]}, id="truncated-token"),
        pytest.param({"Authorization": "Bearer " + TOKEN + "x"}, id="extended-token"),
        pytest.param({"Authorization": TOKEN}, id="scheme-missing"),
        pytest.param({"Authorization": "bearer " + TOKEN}, id="scheme-lowercased"),
        pytest.param({"Authorization": "Basic " + TOKEN}, id="wrong-scheme"),
    ],
)
def test_every_credential_that_is_not_the_token_is_refused(client: Client, headers: dict[str, str]) -> None:
    status, payload = client.call("POST", "/v1/fleet", {}, token=None, headers=headers)
    assert status == 401
    assert payload == {"detail": "factory backend token required"}


def test_unauthenticated_mutation_never_reaches_airflow(client: Client, factory: Factory) -> None:
    """A 401 must be refused *before* the body is interpreted, not after the DAG run is created."""
    status, payload = client.call("POST", "/v1/work-orders", {"line": LINE, "issues": ["404"]}, token=None)
    assert status == 401 and payload == {"detail": "factory backend token required"}

    status, payload = client.call(
        "POST", f"/v1/airflow/api/v2/dags/{LINE}/dagRuns", {"conf": {"issues": ["404"]}}, token=None
    )
    assert status == 401 and payload == {"detail": "factory backend token required"}

    assert client.airflow.requests == []  # nothing was dispatched
    assert factory.cell_store.list(limit=10) == []  # and no Factory Cell was claimed


# ---------------------------------------------------------------- /v1/doctor wire shape


def _assert_console_check(row: Any) -> None:
    """One row against ``swf_domain::doctor::Check``.

    ``ok: bool`` has no ``#[serde(default)]`` and no alias, and ``detail``/``fix`` are ``String``:
    a row carrying only ``status``, or a ``detail`` object, fails to deserialize, the whole response
    is discarded, and ``swf doctor`` prints one fabricated failure blaming the operator's token.
    `doctor` is what someone runs when nothing else works, so this shape is pinned, not assumed.
    """
    assert isinstance(row, dict), row
    assert isinstance(row.get("name"), str) and row["name"], row
    assert "ok" in row, row
    assert isinstance(row["ok"], bool), row  # not "ok"/1/None — serde bool is strict
    for optional in ("detail", "fix"):
        if optional in row:
            assert isinstance(row[optional], str), row
    if "required" in row:
        assert isinstance(row["required"], bool), row


def test_doctor_rows_deserialize_into_the_console_check_shape(client: Client, gh: list[list[str]]) -> None:
    status, checks = client.call("POST", "/v1/doctor", {})
    assert status == 200
    assert isinstance(checks, list) and checks
    for row in checks:
        _assert_console_check(row)
    names = [row["name"] for row in checks]
    assert {"factory backend", "factory cells", "mutation readiness"} <= set(names)
    assert {"metadatabase", "scheduler", "airflow auth"} <= set(names)  # healthy Airflow was reached
    assert all(row["ok"] for row in checks if row["name"] in {"metadatabase", "scheduler"})


def test_doctor_reports_airflow_failure_without_breaking_the_shape(
    client: Client, airflow: FakeAirflow, gh: list[list[str]]
) -> None:
    """The degraded branch is the one an operator actually sees; it must still parse."""
    airflow.routes.pop(("GET", "/monitor/health"))
    status, checks = client.call("POST", "/v1/doctor", {})
    assert status == 200
    for row in checks:
        _assert_console_check(row)
    airflow_row = next(row for row in checks if row["name"] == "airflow")
    assert airflow_row["ok"] is False
    assert airflow_row["fix"] == "check AIRFLOW_URL and credentials on the backend"
    assert "metadatabase" not in {row["name"] for row in checks}


def test_doctor_detail_is_text_even_for_the_capability_document(client: Client, gh: list[list[str]]) -> None:
    """`mutation readiness` renders the capability mapping as a string; an object here broke Rust."""
    _, checks = client.call("POST", "/v1/doctor", {})
    row = next(r for r in checks if r["name"] == "mutation readiness")
    assert isinstance(row["detail"], str) and "mutation_ready=" in row["detail"]


# ---------------------------------------------------------------- read routes


def test_capability_and_fleet_reads(client: Client) -> None:
    status, caps = client.call("POST", "/v1/compatibility", {})
    assert status == 200
    assert caps["mutation_ready"] is True and caps["contracts"]["api"] >= 1

    status, fleet = client.call("POST", "/v1/fleet", {})
    assert status == 200
    assert fleet["cells_active"] == 0 and fleet["queue_depth"] == 0
    assert fleet["bottlenecks"] == [] and fleet["unresolved_operations"] == 0


def test_empty_projections_read_before_any_work_exists(client: Client) -> None:
    """A fresh backend must answer these, not 500 on an empty store."""
    for path in ("/v1/queue", "/v1/operations", "/v1/cells", "/v1/state/runs", "/v1/metrics/runs"):
        status, payload = client.call("POST", path, {})
        assert status == 200, (path, payload)
    status, summary = client.call("POST", "/v1/metrics/summary", {})
    assert status == 200 and isinstance(summary, dict)


def test_lines_lists_installed_blueprints(client: Client) -> None:
    status, lines = client.call("POST", "/v1/lines", {})
    assert status == 200
    names = {row["name"] for row in lines}
    assert LINE in names
    row = next(r for r in lines if r["name"] == LINE)
    assert row["route"][0] == "intent" and row["route"][-1] == "deliver"
    assert REPO in row["targets"]


def test_blueprint_preview_resolves_jobs_without_touching_airflow(client: Client) -> None:
    status, preview = client.call("POST", "/v1/blueprints/preview", {"line": LINE, "issues": ["101"]})
    assert status == 200
    assert preview["line"] == LINE and preview["jobs"]
    assert client.airflow.requests == []


def test_delivery_and_worker_reads_go_through_backend_credentials(client: Client, gh: list[list[str]]) -> None:
    status, prs = client.call("POST", "/v1/deliveries/prs", {})
    assert status == 200 and prs == FakeGitHubClient.prs_returned

    status, issues = client.call("POST", "/v1/deliveries/issues", {})
    assert status == 200 and issues == FakeGitHubClient.issues_returned

    status, head = client.call("POST", "/v1/deliveries/head", {"branch": "swf/101"})
    assert status == 200
    assert head["url"] == "https://x/7" and head["head_sha"] == "abc" and head["labels"] == ["factory"]

    status, url = client.call("POST", "/v1/deliveries/url", {"number": 7})
    assert status == 200 and url == "https://x/7"

    status, checks = client.call("POST", "/v1/deliveries/checks", {"number": 7})
    assert status == 200

    status, workers = client.call("POST", "/v1/workers", {})
    assert status == 200 and workers[0]["created_by"] == "operator"

    status, removed = client.call("POST", "/v1/workers/remove", {"name": "swf-1"})
    assert status == 200 and removed == {"removed": "swf-1"}
    assert all("--repo" in call for call in gh)  # every gh call is pinned to the backend's repo


def test_deliveries_are_empty_when_no_repo_is_configured(tmp_path: Path, env: None) -> None:
    """No repo means no credential; the read answers empty rather than shelling out."""
    bare = Factory(token=TOKEN, airflow_url=AF, state_root=tmp_path / "s")
    try:
        assert bare.operation("/deliveries/prs", {}) == []
        assert bare.operation("/deliveries/issues", {}) == []
    finally:
        bare.close()


# ---------------------------------------------------------------- /v1/work-orders


def test_work_order_dispatches_one_airflow_run_and_binds_a_cell(client: Client, factory: Factory) -> None:
    payload = submit(client, ["101"])
    assert payload["dag_id"] == LINE and payload["run_id"] == "swf__run"
    assert payload["jobs"] == len(payload["cells"]) == 1
    assert payload["submission_id"].startswith("submit_")

    dispatched = client.airflow.calls("POST")
    assert [path for _m, path, _b in dispatched] == [f"/dags/{LINE}/dagRuns"]
    conf = dispatched[0][2]["conf"]
    assert conf["issues"] == ["101"]
    assert conf["_factory_submission_id"] == payload["submission_id"]
    assert [b["cell_id"] for b in conf["_factory_cells"]] == payload["cells"]

    cell = factory.cell_store.get(payload["cells"][0])
    assert cell["state"] == "queued"
    assert cell["airflow_dag_id"] == LINE and cell["airflow_run_id"] == "swf__run"
    assert factory.evidence.verify(cell["cell_id"])[0] is True


def test_work_order_replay_is_idempotent(client: Client) -> None:
    """Retrying the identical submission must reuse the deterministic id, not fan out a second run."""
    first = submit(client, ["101"])
    second = submit(client, ["101"])
    assert second["submission_id"] == first["submission_id"]
    assert second["cells"] == first["cells"]
    assert len(client.airflow.calls("POST")) == 1


def test_work_order_is_refused_while_draining(
    client: Client, factory: Factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The drain guard is what stands between a rolling upgrade and a stranded half-run.

    Asserted at the HTTP boundary: the refusal must arrive as a 503 that says why, and it must
    arrive *before* a Factory Cell is activated or an Airflow run is created — a drain that refuses
    after the write has already gone out has drained nothing. (Whether the compatibility mount
    refuses identically is ``tests/test_backend_drain.py``'s subject, not this file's.)
    """
    monkeypatch.setenv("SWF_DRAIN", "true")
    status, payload = client.call("POST", "/v1/work-orders", {"line": LINE, "issues": ["101"]})
    assert status == 503, payload
    assert "drain" in payload["detail"].lower(), payload
    assert client.airflow.requests == []
    assert factory.cell_store.list(limit=10) == []


def test_work_order_refuses_an_uninstalled_or_malformed_line(client: Client) -> None:
    status, payload = client.call("POST", "/v1/work-orders", {"line": "../../etc/passwd", "issues": ["1"]})
    assert status == 400 and payload["detail"] == "line must be an installed blueprint name"

    status, payload = client.call("POST", "/v1/work-orders", {"line": "a" * 200, "issues": ["1"]})
    assert status == 400 and payload["detail"] == "line must be an installed blueprint name"

    status, payload = client.call("POST", "/v1/work-orders", {"line": "no-such-line", "issues": ["1"]})
    assert status == 404
    assert payload == {"detail": "backend resource or required tool not found"}
    assert client.airflow.requests == []


@pytest.mark.parametrize(
    ("body", "detail"),
    [
        ({"issues": ["1"]}, "line must be a nonempty string of at most 512 characters"),
        ({"line": LINE}, "issues must contain between 1 and 1000 references"),
        ({"line": LINE, "issues": []}, "issues must contain between 1 and 1000 references"),
        ({"line": LINE, "issues": "101"}, "issues must contain between 1 and 1000 references"),
        ({"line": LINE, "issues": ["1"] * 1001}, "issues must contain between 1 and 1000 references"),
        ({"line": LINE, "issues": [7]}, "issue references must be nonempty strings"),
        ({"line": LINE, "issues": [" "]}, "issue references must be nonempty strings"),
        ({"line": LINE, "issues": ["x" * 129]}, "issue references must be nonempty strings"),
        ({"line": LINE, "issues": ["1", "1"]}, "duplicate issue references are not allowed"),
        ({"line": LINE, "issues": ["1"], "targets": "repo"}, "targets must be an array"),
        ({"line": LINE, "issues": ["1"], "targets": [3]}, "targets must be an array"),
        ({"line": LINE, "issues": ["1"], "actor": "a" * 129}, "actor must be a nonempty string"),
        ({"line": LINE, "issues": ["1"], "priority": 3}, "priority must be hotfix, manual, normal"),
        ({"line": LINE, "issues": ["1"], "priority": "urgent"}, "priority must be hotfix, manual, normal"),
    ],
)
def test_work_order_input_bounds_refuse_with_a_useful_message(
    client: Client, body: dict[str, Any], detail: str
) -> None:
    """Every one of these is a 400 naming the field — never a 500, and never a dispatch."""
    status, payload = client.call("POST", "/v1/work-orders", body)
    assert status == 400, payload
    assert detail in payload["detail"], payload
    assert client.airflow.requests == []


# ---------------------------------------------------------------- Airflow compatibility mount

MOUNT = "/v1/airflow/api/v2"


def test_compat_read_route_is_proxied_verbatim(client: Client, airflow: FakeAirflow) -> None:
    airflow.routes[("GET", f"/dags/{LINE}/dagRuns?limit=5")] = {"dag_runs": [], "total_entries": 0}
    status, payload = client.call("GET", f"{MOUNT}/dags/{LINE}/dagRuns?limit=5")
    assert status == 200 and payload == {"dag_runs": [], "total_entries": 0}
    assert airflow.requests[-1][:2] == ("GET", f"/dags/{LINE}/dagRuns?limit=5")


def test_compat_read_route_forwards_the_upstream_status(client: Client, airflow: FakeAirflow) -> None:
    """A 404 from Airflow stays a 404; the mount must not launder it into a backend error."""
    airflow.routes[("GET", f"/dags/{LINE}/dagRuns/ghost")] = (404, {"detail": "not found"})
    status, payload = client.call("GET", f"{MOUNT}/dags/{LINE}/dagRuns/ghost")
    assert status == 404 and payload == {"detail": "not found"}


@pytest.mark.parametrize(
    "target",
    [
        pytest.param("/dags/../../../etc/passwd", id="dotdot"),
        pytest.param("/dags/%2e%2e/secrets", id="percent-encoded-dotdot"),
        pytest.param("/dags/%2fetc%2fpasswd", id="encoded-slash"),
        pytest.param("/dags/a%5Cb", id="encoded-backslash"),
        pytest.param("/dags/./factory", id="dot"),
    ],
)
def test_compat_rejects_traversal_segments(client: Client, target: str) -> None:
    """Segments are decoded *before* the check, so an encoded `..` cannot smuggle past it."""
    status, payload = client.call("GET", MOUNT + target)
    assert status == 400 and payload == {"detail": "invalid path segment"}
    assert client.airflow.requests == []


def test_compat_rejects_a_query_on_a_mutation(client: Client) -> None:
    status, payload = client.call("PATCH", f"{MOUNT}/dags/{LINE}?force=1", {"is_paused": False})
    assert status == 400 and payload == {"detail": "mutation queries are not supported"}
    assert client.airflow.requests == []


def test_compat_rejects_routes_outside_dags(client: Client) -> None:
    for target in ("/variables/secret", "/connections", "/"):
        status, payload = client.call("PATCH", MOUNT + target, {"is_paused": False})
        assert status == 404, (target, payload)
        assert payload == {"detail": "unknown control route"}
    assert client.airflow.requests == []


def test_compat_unpause_and_run_failure_are_the_only_shapes_allowed(client: Client, airflow: FakeAirflow) -> None:
    airflow.routes[("PATCH", f"/dags/{LINE}/dagRuns/run-1")] = {"state": "failed"}
    status, _ = client.call("PATCH", f"{MOUNT}/dags/{LINE}", {"is_paused": False})
    assert status == 200
    status, _ = client.call("PATCH", f"{MOUNT}/dags/{LINE}/dagRuns/run-1", {"state": "failed"})
    assert status == 200

    # Anything else on the same paths is outside the control surface.
    for target, body in (
        (f"{MOUNT}/dags/{LINE}", {"is_paused": True}),
        (f"{MOUNT}/dags/{LINE}", {"is_paused": False, "tags": ["x"]}),
        (f"{MOUNT}/dags/{LINE}/dagRuns/run-1", {"state": "success"}),
        (f"{MOUNT}/dags/{LINE}/dagRuns/run-1", {"note": "hi"}),
    ):
        status, payload = client.call("PATCH", target, body)
        assert status == 403, (target, payload)
        assert payload == {"detail": "operation is outside the factory control surface"}


def test_compat_submit_goes_through_factory_admission(client: Client, factory: Factory) -> None:
    status, payload = client.call("POST", f"{MOUNT}/dags/{LINE}/dagRuns", {"conf": {"issues": ["101"]}})
    assert status == 201 and payload == {"dag_run_id": "swf__run"}
    assert len(factory.cell_store.list(limit=10)) == 1


def test_compat_submit_refuses_conf_keys_outside_issues_and_targets(client: Client) -> None:
    """`conf` is the operator's only lever here; anything else would reach the DAG's internals."""
    for conf in ({"issues": ["1"], "_factory_cells": []}, {"cmd": "rm -rf /"}, "issues"):
        status, payload = client.call("POST", f"{MOUNT}/dags/{LINE}/dagRuns", {"conf": conf})
        assert status == 400, (conf, payload)
        assert payload == {"detail": "only issues and installed targets can be submitted"}
    assert client.airflow.requests == []


def test_compat_gate_answer_requires_a_waiting_gate(client: Client, airflow: FakeAirflow) -> None:
    """A gate answer is a human decision; replaying it onto a task that is not waiting is a forgery."""
    hitl = f"{MOUNT}/dags/{LINE}/dagRuns/run-1/taskInstances/job.approve_intent/0/hitlDetails"
    airflow.routes[("GET", f"/dags/{LINE}/dagRuns/run-1/taskInstances?limit=100&offset=0")] = {
        "task_instances": [{"task_id": "job.approve_intent", "map_index": 0, "state": "success", "try_number": 1}],
        "total_entries": 1,
    }
    status, payload = client.call("PATCH", hitl, {"chosen_options": ["Approve"], "params_input": {}})
    assert status == 409 and payload == {"detail": "gate is not waiting for operator input"}
    assert airflow.calls("PATCH") == []


def test_compat_gate_answer_is_forwarded_when_the_gate_is_waiting(client: Client, airflow: FakeAirflow) -> None:
    hitl_path = f"/dags/{LINE}/dagRuns/run-1/taskInstances/job.approve_intent/0/hitlDetails"
    airflow.routes[("GET", f"/dags/{LINE}/dagRuns/run-1/taskInstances?limit=100&offset=0")] = {
        "task_instances": [{"task_id": "job.approve_intent", "map_index": 0, "state": "deferred", "try_number": 1}],
        "total_entries": 1,
    }
    airflow.routes[("PATCH", hitl_path)] = {"response_received": True}
    status, payload = client.call("PATCH", MOUNT + hitl_path, {"chosen_options": ["Approve"], "params_input": {}})
    assert status == 200 and payload == {"response_received": True}
    assert airflow.calls("PATCH")[-1][1] == hitl_path


def test_compat_gate_answer_refuses_anything_but_approve_or_reject(client: Client) -> None:
    hitl = f"{MOUNT}/dags/{LINE}/dagRuns/run-1/taskInstances/job.approve_intent/0/hitlDetails"
    for body in (
        {"chosen_options": ["Approve"]},
        {"chosen_options": ["Approve", "Reject"], "params_input": {}},
        {"chosen_options": ["Approve"], "params_input": {"force": True}},
        {"chosen_options": [], "params_input": {}},
    ):
        status, payload = client.call("PATCH", hitl, body)
        assert status == 400, (body, payload)
        assert payload == {"detail": "only explicit Approve or Reject answers are supported"}
    assert client.airflow.requests == []


# ---------------------------------------------------------------- cells and lifecycle


def test_cell_reads_and_transition_walk_one_cell_to_terminal(client: Client, factory: Factory) -> None:
    cell_id = submit(client)["cells"][0]

    status, listed = client.call("POST", "/v1/cells", {"limit": 5})
    assert status == 200 and [row["cell_id"] for row in listed] == [cell_id]

    status, cell = client.call("POST", "/v1/cells/inspect", {"cell_id": cell_id})
    assert status == 200 and cell["state"] == "queued"

    status, history = client.call("POST", "/v1/cells/history", {"cell_id": cell_id})
    assert status == 200 and history

    status, moved = client.call(
        "POST", "/v1/cells/transition", {"cell_id": cell_id, "epoch": cell["epoch"], "state": "running"}
    )
    assert status == 200 and moved["cell"]["state"] == "running"

    status, done = client.call(
        "POST", "/v1/cells/transition", {"cell_id": cell_id, "epoch": cell["epoch"], "state": "success"}
    )
    assert status == 200 and done["cell"]["state"] == "success"

    status, verified = client.call("POST", "/v1/evidence/verify", {"cell_id": cell_id})
    assert status == 200 and verified["verified"] is True and verified["tail_digest"]

    status, checkpoint = client.call("POST", "/v1/evidence/checkpoint", {"cell_id": cell_id})
    assert status == 200 and checkpoint["cell_id"] == cell_id


def test_transition_refuses_a_stale_epoch_and_an_illegal_move(client: Client) -> None:
    """Epoch is the fence: a worker from a previous generation must not steer a live Cell."""
    cell_id = submit(client)["cells"][0]

    status, payload = client.call("POST", "/v1/cells/transition", {"cell_id": cell_id, "epoch": 99, "state": "running"})
    assert status == 409 and "stale Factory Cell epoch 99" in payload["detail"]

    client.call("POST", "/v1/cells/transition", {"cell_id": cell_id, "epoch": 1, "state": "failed"})
    status, payload = client.call("POST", "/v1/cells/transition", {"cell_id": cell_id, "epoch": 1, "state": "running"})
    assert status == 409 and "terminal Factory Cell cannot transition failed -> running" in payload["detail"]


def test_unknown_cell_is_a_404_not_a_crash(client: Client) -> None:
    for path in ("/v1/cells/inspect", "/v1/cells/history"):
        status, payload = client.call("POST", path, {"cell_id": "cell-does-not-exist"})
        assert status == 404, (path, payload)
        assert "cell-does-not-exist" in payload["detail"]


def test_core_inspect_of_an_unknown_cell_is_a_404_and_not_a_500(client: Client) -> None:
    """`CoreCapabilityRuntime.inspect` reads the Cell store directly, so an unknown id used to
    escape as a bare KeyError and reach server.py's catch-all as 500 "internal backend error" —
    telling an operator who typed a cell id wrong that the backend is broken."""
    status, payload = client.call("POST", "/v1/core/inspect", {"cell_id": "cell-does-not-exist"})
    assert status == 404, payload
    assert "cell-does-not-exist" in payload["detail"]


def test_core_projection_joins_cell_operations_and_evidence(client: Client) -> None:
    cell_id = submit(client)["cells"][0]
    status, projection = client.call("POST", "/v1/core/inspect", {"cell_id": cell_id})
    assert status == 200
    assert projection["operator_truth"]["cell_id"] == cell_id
    assert projection["operator_truth"]["airflow"]["run_id"] == "swf__run"
    assert projection["operator_truth"]["evidence_verified"] is True
    assert projection["recovery"] == [] and projection["cleanup_debt"] is False


def test_queue_and_operation_lookups_answer_404_for_unknown_keys(client: Client) -> None:
    status, payload = client.call("POST", "/v1/queue/inspect", {"work_id": "submit_nope"})
    assert status == 404 and payload == {"detail": "no admission work submit_nope"}

    status, payload = client.call("POST", "/v1/operations/inspect", {"operation_key": "nope"})
    assert status == 404 and payload == {"detail": "no such operation"}

    status, payload = client.call("POST", "/v1/core/recovery-plan", {"operation_key": "nope"})
    assert status == 404 and payload == {"detail": "no such operation"}


def test_queue_inspect_finds_the_admitted_submission(client: Client) -> None:
    work_id = submit(client)["submission_id"]
    status, row = client.call("POST", "/v1/queue/inspect", {"work_id": work_id})
    assert status == 200 and row["work_id"] == work_id


def test_unknown_routes_are_404_on_every_namespace(client: Client) -> None:
    for path in ("/v1/nope", "/v1/core/nope", "/v1/scm/nope"):
        status, payload = client.call("POST", path, {})
        assert status == 404, (path, payload)
    status, payload = client.call("GET", "/v1/fleet")  # reads are POST-only outside the mount
    assert status == 404 and payload == {"detail": "unknown factory route"}
    status, payload = client.call("PATCH", "/v1/fleet", {})
    assert status == 404 and payload == {"detail": "unknown factory route"}


# ---------------------------------------------------------------- /v1/scm


class FakeScm:
    """Stands in for ``GitHubScm``; records what the backend decided to do with its credential."""

    instances: list[FakeScm] = []

    def __init__(self, repo: str, base: str) -> None:
        self.repo, self.base = repo, base
        self.fetched: list[str] = []
        self.published: list[dict[str, Any]] = []
        self.issues: list[dict[str, Any]] = []
        FakeScm.instances.append(self)

    def fetch_issue(self, ref: str) -> Any:
        self.fetched.append(ref)
        return types.SimpleNamespace(
            model_dump=lambda mode="json": {"number": int(ref), "title": "an issue", "body": "text"}
        )

    def publish(self, **kwargs: Any) -> str:
        self.published.append(kwargs)
        return "https://x/pr/1"

    def open_issue(self, **kwargs: Any) -> str:
        self.issues.append(kwargs)
        return "https://x/issue/1"


@pytest.fixture
def scm(monkeypatch: pytest.MonkeyPatch) -> type[FakeScm]:
    FakeScm.instances = []
    monkeypatch.setattr(scm_mod, "GitHubScm", FakeScm)
    return FakeScm


@pytest.mark.parametrize(
    "ref",
    [
        pytest.param("demo/issue.md", id="relative-path"),
        pytest.param("/etc/passwd", id="absolute-path"),
        pytest.param("../../.env", id="traversal"),
        pytest.param("~/.config/gh/hosts.yml", id="home-relative"),
        pytest.param("12a", id="digits-then-letters"),
        pytest.param("-1", id="signed"),
        pytest.param("1 2", id="two-numbers"),
        pytest.param("1_2", id="underscored"),
        pytest.param("0x10", id="hex"),
    ],
)
def test_scm_issue_refuses_anything_that_is_not_an_issue_number(client: Client, scm: type[FakeScm], ref: str) -> None:
    """A ``ref`` that is not digits is a path, and a path on this host is an arbitrary read of the
    process that holds the GitHub and Airflow credentials. The refusal is asserted twice: the status
    the caller sees, and the fake SCM never being asked to resolve it."""
    status, payload = client.call("POST", "/v1/scm/issue", {"ref": ref})
    assert status == 400, payload
    assert payload == {"detail": "ref must be an issue number over the API; a path is local-only"}
    assert all(instance.fetched == [] for instance in scm.instances)


def test_scm_issue_resolves_a_numeric_ref(client: Client, scm: type[FakeScm]) -> None:
    status, payload = client.call("POST", "/v1/scm/issue", {"ref": " 1220 "})
    assert status == 200 and payload["number"] == 1220
    assert scm.instances[-1].fetched == ["1220"]
    assert scm.instances[-1].repo == REPO  # the backend's repo, never the caller's


def test_scm_issue_bounds_the_ref(client: Client, scm: type[FakeScm]) -> None:
    for ref, detail in ((None, "ref must be"), ("", "ref must be"), ("9" * 129, "ref must be"), (7, "ref must be")):
        status, payload = client.call("POST", "/v1/scm/issue", {"ref": ref})
        assert status == 400, (ref, payload)
        assert detail in payload["detail"]


def test_scm_refuses_when_no_repo_is_configured(tmp_path: Path, env: None, scm: type[FakeScm]) -> None:
    bare = Factory(token=TOKEN, airflow_url=AF, state_root=tmp_path / "s")
    try:
        with pytest.raises(service_mod.Refused) as caught:
            scm_mod.operation(bare, "/scm/issue", {"ref": "1"})
        assert caught.value.status == 503
    finally:
        bare.close()


def _publish_body(cell: dict[str, Any], **changes: Any) -> dict[str, Any]:
    body = {
        "cell_id": cell["cell_id"],
        "epoch": int(cell["epoch"]),
        "policy_digest": cell["policy_digest"],
        "operation_key": "publish:1",
        "branch": "swf/101",
        "title": "factory: 101",
        "body": "what changed",
        "labels": ["factory"],
        "patch_b64": "ZGlmZg==",
    }
    body.update(changes)
    return body


def test_scm_publish_requires_a_live_bound_cell(client: Client, factory: Factory, scm: type[FakeScm]) -> None:
    """Managed publication is gated on identity, not on the caller's word: unknown cell, stale
    epoch, changed policy and an unbound Cell each refuse before a credential is used."""
    cell = factory.cell_store.get(submit(client)["cells"][0])

    status, payload = client.call("POST", "/v1/scm/publish", _publish_body(cell, cell_id="ghost"))
    assert status == 404 and "ghost" in payload["detail"]

    status, payload = client.call("POST", "/v1/scm/publish", _publish_body(cell, epoch=99))
    assert status == 409 and "stale Factory Cell epoch 99" in payload["detail"]

    status, payload = client.call("POST", "/v1/scm/publish", _publish_body(cell, policy_digest="sha256:0"))
    assert status == 409
    assert payload == {"detail": "Factory Cell policy digest changed; publication is stale"}

    assert all(instance.published == [] for instance in scm.instances)


def test_scm_publish_refuses_a_cell_with_no_airflow_binding(
    client: Client, factory: Factory, scm: type[FakeScm]
) -> None:
    """Without a bound run there is no authority for the write, so it must fail closed."""
    from swfactory.cell_runtime import identity_for_job

    identity = identity_for_job({"issue": "9", "repo": REPO, "dir": "", "base_branch": "main", "job_idx": 0})
    cell = factory.cell_store.activate(identity, actor="test")
    cell = factory.cell_store.patch(cell["cell_id"], int(cell["epoch"]), "policy:test", policy_digest="sha256:x")
    status, payload = client.call("POST", "/v1/scm/publish", _publish_body(cell))
    assert status == 409
    assert payload == {"detail": "Factory Cell is not bound to an authoritative Airflow run"}
    assert all(instance.published == [] for instance in scm.instances)


@pytest.mark.parametrize(
    ("changes", "detail"),
    [
        pytest.param({"cell_id": None}, "cell_id must be", id="cell_id-missing"),
        pytest.param({"epoch": "1"}, "epoch must be a positive integer", id="epoch-string"),
        pytest.param({"epoch": 0}, "epoch must be a positive integer", id="epoch-zero"),
        pytest.param({"epoch": True}, "epoch must be a positive integer", id="epoch-bool"),
        pytest.param({"operation_key": "k" * 257}, "operation_key must be", id="operation_key-long"),
        pytest.param({"branch": "b" * 513}, "branch must be", id="branch-long"),
        pytest.param({"title": "t" * 513}, "title must be", id="title-long"),
        pytest.param({"body": 7}, "body must be a string", id="body-type"),
        pytest.param({"labels": "factory"}, "labels must be an array", id="labels-type"),
        pytest.param({"labels": ["l" * 129]}, "labels must be an array", id="label-long"),
        pytest.param({"allowed_prefixes": "src/"}, "allowed_prefixes must be", id="prefixes-type"),
        pytest.param({"patch_b64": None}, "patch_b64 is required", id="patch-missing"),
        pytest.param({"patch_b64": "not base64!"}, "patch_b64 is invalid base64", id="patch-not-base64"),
    ],
)
def test_scm_publish_input_bounds_refuse_with_a_useful_message(
    client: Client, factory: Factory, scm: type[FakeScm], changes: dict[str, Any], detail: str
) -> None:
    cell = factory.cell_store.get(submit(client)["cells"][0])
    status, payload = client.call("POST", "/v1/scm/publish", _publish_body(cell, **changes))
    assert status == 400, (changes, payload)
    assert detail in payload["detail"], payload
    assert all(instance.published == [] for instance in scm.instances)


def test_scm_publish_and_open_issue_reach_the_credential_once_identity_holds(
    client: Client, factory: Factory, scm: type[FakeScm], gh: list[list[str]]
) -> None:
    cell = factory.cell_store.get(submit(client)["cells"][0])

    status, payload = client.call("POST", "/v1/scm/publish", _publish_body(cell))
    assert status == 200, payload
    assert payload["url"] == "https://x/pr/1" and payload["branch"] == "swf/101"
    published = scm.instances[-1].published[-1]
    assert published["branch"] == "swf/101" and published["patch"] == b"diff"
    # The digest marker is what a replay reconciles against; it must be in the published body.
    assert "swfactory-patch-sha256:" + payload["patch_sha256"] in published["body"]

    status, payload = client.call(
        "POST",
        "/v1/scm/open-issue",
        {
            "cell_id": cell["cell_id"],
            "epoch": int(cell["epoch"]),
            "policy_digest": cell["policy_digest"],
            "operation_key": "issue:1",
            "title": "blocked",
            "body": "why",
            "labels": ["factory"],
        },
    )
    assert status == 200 and payload["url"] == "https://x/issue/1"
    assert "swfactory-operation:issue:1" in scm.instances[-1].issues[-1]["body"]


# ---------------------------------------------------------------- transport bounds


def test_body_larger_than_the_backend_limit_is_refused_before_it_is_read(client: Client) -> None:
    """The length is checked against ``MAX_BODY`` before ``rfile.read``, so an attacker cannot make
    the backend buffer a gigabyte by claiming one. Asserted by announcing a length we never send."""
    status, payload = client.raw(
        "POST",
        "/v1/fleet",
        b"",
        {"Authorization": f"Bearer {TOKEN}", "Content-Length": str(1024**3)},
    )
    assert status == 413 and payload == {"detail": "request exceeds backend limit"}


def test_a_negative_content_length_is_refused(client: Client) -> None:
    status, payload = client.raw("POST", "/v1/fleet", b"", {"Authorization": f"Bearer {TOKEN}", "Content-Length": "-1"})
    assert status == 413 and payload == {"detail": "request exceeds backend limit"}


def test_a_non_numeric_content_length_is_a_400_not_a_500(client: Client) -> None:
    status, payload = client.raw(
        "POST", "/v1/fleet", b"", {"Authorization": f"Bearer {TOKEN}", "Content-Length": "many"}
    )
    assert status == 400 and payload["detail"] != "internal backend error"


def test_chunked_requests_are_refused(client: Client) -> None:
    """A chunked body has no declared length, so the size guard above could not bound it."""
    status, payload = client.raw(
        "POST",
        "/v1/fleet",
        b"",
        {"Authorization": f"Bearer {TOKEN}", "Content-Length": "0", "Transfer-Encoding": "chunked"},
    )
    assert status == 400 and payload == {"detail": "chunked requests are not supported"}


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(b"{not json", id="malformed"),
        pytest.param(b"[]", id="array"),
        pytest.param(b'"line"', id="string"),
        pytest.param(b"7", id="number"),
        pytest.param(b"null", id="null"),
        pytest.param(b"true", id="bool"),
    ],
)
def test_a_body_that_is_not_a_json_object_is_a_400_with_a_reason(client: Client, raw: bytes) -> None:
    status, payload = client.raw(
        "POST",
        "/v1/fleet",
        raw,
        {"Authorization": f"Bearer {TOKEN}", "Content-Length": str(len(raw))},
    )
    assert status == 400, payload
    assert payload["detail"] and payload["detail"] != "internal backend error"
    assert len(payload["detail"]) <= 500  # the reason is truncated, never a raw dump


def test_an_absent_body_is_treated_as_an_empty_object(client: Client) -> None:
    status, payload = client.raw("POST", "/v1/fleet", b"", {"Authorization": f"Bearer {TOKEN}"})
    assert status == 200 and payload["cells_active"] == 0


@pytest.mark.parametrize(
    ("path", "body", "detail"),
    [
        ("/v1/cells", {"limit": "30"}, "limit must be between 1 and 1000"),
        ("/v1/cells", {"limit": 0}, "limit must be between 1 and 1000"),
        ("/v1/cells", {"limit": 1001}, "limit must be between 1 and 1000"),
        ("/v1/cells", {"limit": 1.0}, "limit must be between 1 and 1000"),
        ("/v1/cells", {"limit": True}, "limit must be between 1 and 1000"),
        ("/v1/cells/inspect", {}, "cell_id must be a nonempty string"),
        ("/v1/cells/inspect", {"cell_id": 7}, "cell_id must be a nonempty string"),
        ("/v1/cells/inspect", {"cell_id": "   "}, "cell_id must be a nonempty string"),
        ("/v1/cells/inspect", {"cell_id": "c" * 513}, "cell_id must be a nonempty string"),
        ("/v1/cells/transition", {"cell_id": "c", "epoch": 1, "state": "gone"}, "invalid lifecycle state"),
        ("/v1/cells/transition", {"cell_id": "c", "epoch": 1}, "state must be a nonempty string"),
        ("/v1/queue/inspect", {}, "work_id must be a nonempty string"),
        ("/v1/operations/inspect", {}, "operation_key must be a nonempty string"),
        ("/v1/evidence/verify", {}, "cell_id must be a nonempty string"),
        ("/v1/deliveries/checks", {"number": "7"}, "number must be a positive integer"),
        ("/v1/deliveries/checks", {"number": 0}, "number must be a positive integer"),
        ("/v1/deliveries/url", {"number": -1}, "number must be a positive integer"),
        ("/v1/deliveries/head", {}, "branch must be a nonempty string"),
        ("/v1/workers/remove", {}, "name must be a nonempty string"),
        ("/v1/state/inspect", {}, "run_id must be a nonempty string"),
        ("/v1/blueprints/preview", {}, "line must be a nonempty string"),
    ],
)
def test_wrong_types_and_missing_fields_name_the_field_instead_of_500ing(
    client: Client, gh: list[list[str]], path: str, body: dict[str, Any], detail: str
) -> None:
    """Validation happens before any store or credential is touched, so the message can be specific."""
    status, payload = client.call("POST", path, body)
    assert status == 400, (path, body, payload)
    assert detail in payload["detail"], (path, payload)
    assert client.airflow.requests == [] and gh == []


# ---------------------------------------------------------------- response envelope and errors


def test_every_reply_carries_the_hardening_headers(client: Client) -> None:
    """The console parses JSON; a sniffed or cached control-plane document is a bug either way."""
    conn = http.client.HTTPConnection(client.host, client.port, timeout=10)
    try:
        conn.request("GET", "/v1/liveness")
        response = conn.getresponse()
        response.read()
        assert response.getheader("Content-Type") == "application/json"
        assert response.getheader("Cache-Control") == "no-store"
        assert response.getheader("X-Content-Type-Options") == "nosniff"
    finally:
        conn.close()


def test_non_json_from_airflow_becomes_a_502_not_a_500(client: Client, airflow: FakeAirflow) -> None:
    """A proxy error page in front of Airflow must read as "upstream", not "backend bug"."""
    airflow.routes[("GET", f"/dags/{LINE}/dagRuns")] = b"<html>502 Bad Gateway</html>"
    status, payload = client.call("GET", f"{MOUNT}/dags/{LINE}/dagRuns")
    assert status == 502 and payload == {"detail": "Airflow returned invalid JSON"}


def test_an_unreachable_airflow_becomes_a_502_naming_the_uncertainty(client: Client, airflow: FakeAirflow) -> None:
    """A mutation whose outcome is unknown must say so; silence would invite a blind retry."""
    airflow.routes[("PATCH", f"/dags/{LINE}")] = OSError("connection refused")
    status, payload = client.call("PATCH", f"{MOUNT}/dags/{LINE}", {"is_paused": False})
    assert status == 502
    assert payload == {"detail": "backend service unavailable; mutation outcome may be unknown"}


def test_state_inspect_of_an_unknown_run_is_not_a_500(client: Client) -> None:
    status, payload = client.call("POST", "/v1/state/inspect", {"run_id": "no-such-run"})
    assert status in (400, 404), payload
    assert payload["detail"] != "internal backend error"


def test_a_control_plane_refusal_reaches_the_operator_instead_of_a_500(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`OperationError` and `CellError` both subclass RuntimeError.

    The transport catches Refused, PermissionError, ValueError/TypeError, ControlError and OSError
    by name and sinks everything else into 500 "internal backend error". Both durable control-plane
    families fell into that sink, so a stale epoch, a busy Cell, a duplicate operation key or an
    in-doubt outcome — each with a specific remedy — reached the operator as "the backend is
    broken".
    """
    from swfactory.cells import StaleEpoch
    from swfactory.idempotency import OperationInDoubt

    for error, expected in (
        (StaleEpoch("cell_x: expected epoch 2, current 3"), "expected epoch 2"),
        (OperationInDoubt("publish:abc", "attempt_in_progress"), "attempt_in_progress"),
    ):

        def boom(*_a: object, _raise: BaseException = error, **_k: object) -> None:
            raise _raise

        monkeypatch.setattr(service_mod.Factory, "capabilities", boom)
        status, payload = client.call("POST", "/v1/doctor", {})
        assert status == 409, (status, payload)
        assert expected in payload["detail"]
        assert "internal backend error" not in payload["detail"]


def test_an_unhandled_error_is_logged_with_the_id_the_operator_is_given(
    client: Client, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    """A 500 must leave a trace. `log_message` is suppressed and the traceback used to be
    discarded, so an intermittent failure produced a zero-byte backend log and could not be
    diagnosed at all. The id ties the operator's response to that traceback."""

    def boom(*_a: object, **_k: object) -> None:
        raise ZeroDivisionError("a genuine defect, not a refusal")

    monkeypatch.setattr(service_mod.Factory, "capabilities", boom)
    status, payload = client.call("POST", "/v1/doctor", {})
    assert status == 500 and payload["detail"] == "internal backend error"
    error_id = payload["error_id"]

    logged = capfd.readouterr().err
    assert error_id in logged, "the id handed to the operator does not appear in the log"
    assert "ZeroDivisionError" in logged and "a genuine defect" in logged
