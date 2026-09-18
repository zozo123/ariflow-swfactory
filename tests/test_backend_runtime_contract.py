from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from swfactory.backend.server import LARGE_SCM_ROUTES, MAX_BODY, MAX_SCM_BODY, make_server

TOKEN = "t" * 32


class FakeFactory:
    token = TOKEN

    def __init__(self, *, mutation_ready: bool = True) -> None:
        self.mutation_ready = mutation_ready
        self.compatibility_calls = 0

    def capabilities(self) -> dict[str, Any]:
        return {
            "read_ready": True,
            "mutation_ready": self.mutation_ready,
            "serving_generation": "stable",
            "contracts": {"api": 1, "cell": 1},
        }

    def compatibility(self, _method: str, _path: str, _body: dict[str, Any]) -> tuple[int, Any]:
        self.compatibility_calls += 1
        return 201, {"dag_run_id": "run"}

    def operation(self, _path: str, _body: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True}


def request(url: str, path: str, *, method: str = "GET", body: bytes | None = None, token: str | None = None):
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url + path, data=body, headers=headers, method=method)
    try:
        return urllib.request.urlopen(req, timeout=3)
    except urllib.error.HTTPError as error:
        return error


def running_server(factory: FakeFactory):
    server = make_server(factory, "127.0.0.1", 0)  # type: ignore[arg-type]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}"


def test_only_liveness_and_readiness_are_public() -> None:
    server, thread, url = running_server(FakeFactory())
    try:
        assert request(url, "/v1/liveness").status == 200
        assert request(url, "/v1/readiness").status == 200
        assert request(url, "/v1/health").status == 401
        assert request(url, "/v1/health", token="wrong").status == 401
        response = request(url, "/v1/health", token=TOKEN)
        assert response.status == 200
        assert json.load(response)["mutation_ready"] is True
    finally:
        server.shutdown()
        thread.join(timeout=3)
        server.server_close()


def test_drain_refuses_compatibility_submit_before_it_reaches_factory() -> None:
    factory = FakeFactory(mutation_ready=False)
    server, thread, url = running_server(factory)
    try:
        response = request(
            url,
            "/v1/airflow/api/v2/dags/factory/dagRuns",
            method="POST",
            token=TOKEN,
            body=json.dumps({"conf": {"issues": ["1"]}}).encode(),
        )
        assert response.status == 503
        assert factory.compatibility_calls == 0
        assert "draining or not mutation-ready" in response.read().decode()
    finally:
        server.shutdown()
        thread.join(timeout=3)
        server.server_close()


def test_default_request_bound_is_64k_and_only_scm_patch_routes_get_16m() -> None:
    assert MAX_BODY == 64 * 1024
    assert MAX_SCM_BODY == 16 * 1024 * 1024
    assert {"/v1/scm/publish", "/v1/scm/open-issue"} == LARGE_SCM_ROUTES


def test_compose_wires_backend_callback_contract_into_airflow_workers() -> None:
    compose = Path("deploy/docker/compose.yml").read_text()
    airflow = compose.split("  airflow:", 1)[1].split("  webhook:", 1)[0]
    assert "SWF_BACKEND_URL: ${SWF_BACKEND_URL:-http://backend:8082}" in airflow
    assert "SWF_BACKEND_TOKEN: ${SWF_BACKEND_TOKEN:-}" in airflow
