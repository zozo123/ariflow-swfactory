"""GitHub -> Airflow webhook receiver. Hermetic: fake urlopen, server on an ephemeral port."""

from __future__ import annotations

import hashlib
import hmac
import http.client
import io
import json
import threading
import urllib.error
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from swfactory import webhook
from swfactory.cli import app
from swfactory.webhook import Trigger, route, trigger_airflow, verify_signature

SECRET = "s3cret"
AIRFLOW = "http://127.0.0.1:8080"


# ---------------------------------------------------------------- fixtures / fakes


def issue_payload(action: str, *, label: str | None = None, number: int = 42, pr: bool = False):
    issue: dict[str, Any] = {"number": number, "title": "x"}
    if pr:
        issue["pull_request"] = {"url": "https://api.github.com/repos/o/r/pulls/42"}
    payload: dict[str, Any] = {"action": action, "issue": issue}
    if label is not None:
        payload["label"] = {"name": label}
    return payload


def comment_payload(body: str, *, action: str = "created", number: int = 7, pr: bool = False):
    payload = issue_payload(action, number=number, pr=pr)
    payload["comment"] = {"body": body}
    return payload


class FakeResponse(io.BytesIO):
    def __init__(self, data: bytes, status: int = 200) -> None:
        super().__init__(data)
        self.status = status

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class FakeOpener:
    """Records every request; answers by URL suffix. Raises when told to."""

    def __init__(self, *, fail: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fail = fail

    def __call__(self, req, timeout=None):  # noqa: ANN001 - urlopen signature
        self.calls.append(
            {
                "url": req.full_url,
                "method": req.get_method(),
                "headers": {k.lower(): v for k, v in req.header_items()},
                "body": json.loads(req.data.decode()) if req.data else None,
                "timeout": timeout,
            }
        )
        if self.fail is not None:
            raise self.fail
        if req.full_url.endswith("/auth/token"):
            return FakeResponse(json.dumps({"access_token": "jwt-123"}).encode(), 201)
        return FakeResponse(
            json.dumps({"dag_run_id": "manual__2026-01-01", "state": "queued"}).encode(), 200
        )


def sign(body: bytes, secret: str = SECRET) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


# ---------------------------------------------------------------- route()


@pytest.mark.parametrize(
    ("event", "payload", "expected"),
    [
        (
            "issues",
            issue_payload("labeled", label="factory"),
            Trigger("factory", {"issues": ["42"]}),
        ),
        (
            "issues",
            issue_payload("labeled", label="factory:hotfix"),
            Trigger("hotfix", {"issues": ["42"]}),
        ),
        ("issues", issue_payload("labeled", label="factory:blocked"), None),
        ("issues", issue_payload("labeled", label="factory:rejected"), None),
        ("issues", issue_payload("labeled", label="bug"), None),
        ("issues", issue_payload("labeled", label="factory:"), None),
        ("issues", issue_payload("labeled", label="factory:../evil"), None),
        ("issues", issue_payload("labeled", label="factory", pr=True), None),
        ("issues", issue_payload("unlabeled", label="factory"), None),
        ("issues", issue_payload("opened"), None),
        ("issues", issue_payload("labeled"), None),
        (
            "issue_comment",
            comment_payload("@factory run"),
            Trigger("factory", {"issues": ["7"]}),
        ),
        (
            "issue_comment",
            comment_payload("  @factory run hotfix \nplease"),
            Trigger("hotfix", {"issues": ["7"]}),
        ),
        ("issue_comment", comment_payload("@factory run hotfix now"), None),
        ("issue_comment", comment_payload("@factory runner"), None),
        ("issue_comment", comment_payload("please @factory run"), None),
        ("issue_comment", comment_payload("thanks"), None),
        ("issue_comment", comment_payload(""), None),
        ("issue_comment", comment_payload("@factory run", action="edited"), None),
        ("issue_comment", comment_payload("@factory run", pr=True), None),
        ("pull_request", {"action": "labeled", "number": 3, "label": {"name": "factory"}}, None),
        ("pull_request", issue_payload("labeled", label="factory"), None),
        ("ping", {"zen": "Keep it logically awesome."}, None),
        ("issues", {"action": "labeled", "label": {"name": "factory"}}, None),
        (
            "issues",
            {"action": "labeled", "label": {"name": "factory"}, "issue": {"number": "1"}},
            None,
        ),
        (
            "issues",
            {"action": "labeled", "label": {"name": "factory"}, "issue": {"number": 0}},
            None,
        ),
        (
            "issues",
            {"action": "labeled", "label": {"name": "factory"}, "issue": {"number": True}},
            None,
        ),
        ("issues", {"action": "labeled", "label": "factory", "issue": {"number": 1}}, None),
        ("issues", "not a dict", None),
    ],
)
def test_route(event: str, payload: Any, expected: Trigger | None) -> None:
    assert route(event, payload) == expected


def test_trigger_body_matches_airflow_post_body() -> None:
    body = Trigger("factory", {"issues": ["42"]}).body()
    assert body == {"conf": {"issues": ["42"]}, "logical_date": None}


# ---------------------------------------------------------------- verify_signature()


def test_verify_signature_accepts_github_scheme() -> None:
    body = b'{"action":"labeled"}'
    assert verify_signature(SECRET, body, sign(body))
    assert verify_signature(SECRET, body, sign(body).upper().replace("SHA256=", "sha256="))


@pytest.mark.parametrize(
    "header",
    [None, "", "sha256=", "sha1=abc", "deadbeef", "sha256=" + "0" * 64],
)
def test_verify_signature_rejects(header: str | None) -> None:
    assert not verify_signature(SECRET, b"{}", header)


def test_verify_signature_wrong_secret_or_body() -> None:
    body = b"{}"
    assert not verify_signature("other", body, sign(body))
    assert not verify_signature(SECRET, b"{} ", sign(body))


# ---------------------------------------------------------------- Airflow calls (fake opener)


@pytest.mark.parametrize(
    "url", ["http://airflow.example:8080", "ftp://airflow.example", "http://user:pw@127.0.0.1:8080"]
)
def test_airflow_rejects_unsafe_credential_transport(url: str) -> None:
    with pytest.raises(ValueError):
        webhook.airflow_token(url, "admin", "pw", FakeOpener())
    with pytest.raises(ValueError):
        trigger_airflow(Trigger("factory"), airflow_url=url, token="T", opener=FakeOpener())


def test_authenticated_default_opener_refuses_redirects() -> None:
    assert (
        webhook._NoRedirect().redirect_request(None, None, 302, "Found", {}, "https://evil.example")
        is None
    )


def test_airflow_token_posts_credentials() -> None:
    opener = FakeOpener()
    token = webhook.airflow_token(AIRFLOW + "/", "admin", "pw", opener)
    assert token == "jwt-123"
    (call,) = opener.calls
    assert call["url"] == f"{AIRFLOW}/auth/token"
    assert call["method"] == "POST"
    assert call["body"] == {"username": "admin", "password": "pw"}
    assert call["headers"]["content-type"] == "application/json"
    assert "authorization" not in call["headers"]


def test_airflow_token_rejects_bad_response() -> None:
    class Opener(FakeOpener):
        def __call__(self, req, timeout=None):  # noqa: ANN001
            return FakeResponse(b"<html>login</html>")

    with pytest.raises(RuntimeError, match="/auth/token"):
        webhook.airflow_token(AIRFLOW, "admin", "pw", Opener())


def test_trigger_airflow_url_headers_and_body() -> None:
    opener = FakeOpener()
    run_id = trigger_airflow(
        Trigger("hotfix", {"issues": ["42"]}), airflow_url=AIRFLOW + "/", token="T", opener=opener
    )
    assert run_id == "manual__2026-01-01"
    (call,) = opener.calls
    assert call["url"] == f"{AIRFLOW}/api/v2/dags/hotfix/dagRuns"
    assert call["method"] == "POST"
    assert call["headers"]["authorization"] == "Bearer T"
    assert call["headers"]["content-type"] == "application/json"
    assert call["body"] == {"conf": {"issues": ["42"]}, "logical_date": None}


def test_trigger_airflow_propagates_http_error() -> None:
    err = urllib.error.HTTPError(AIRFLOW, 404, "not found", None, io.BytesIO(b'{"detail":"x"}'))
    opener = FakeOpener(fail=err)
    with pytest.raises(urllib.error.HTTPError):
        trigger_airflow(Trigger("nope"), airflow_url=AIRFLOW, token="T", opener=opener)


def test_token_provider_from_env_static_token() -> None:
    provider = webhook.token_provider_from_env(AIRFLOW, {"AIRFLOW_TOKEN": "static"})
    assert provider() == "static"


def test_token_provider_from_env_logs_in_per_call() -> None:
    opener = FakeOpener()
    provider = webhook.token_provider_from_env(
        AIRFLOW, {"AIRFLOW_USER": "admin", "AIRFLOW_PASSWORD": "pw"}, opener
    )
    assert provider() == "jwt-123"
    assert provider() == "jwt-123"
    assert [c["url"] for c in opener.calls] == [f"{AIRFLOW}/auth/token"] * 2


def test_token_provider_from_env_requires_credentials() -> None:
    with pytest.raises(ValueError, match="AIRFLOW_TOKEN"):
        webhook.token_provider_from_env(AIRFLOW, {"AIRFLOW_USER": "admin"})


# ---------------------------------------------------------------- HTTP server end to end


@pytest.fixture
def served() -> Iterator[tuple[int, FakeOpener, list[str]]]:
    opener = FakeOpener()
    lines: list[str] = []
    server = webhook.make_server(
        0,
        airflow_url=AIRFLOW,
        token_provider=lambda: "T",
        secret=SECRET,
        opener=opener,
        host="127.0.0.1",
        log=lines.append,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1], opener, lines
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def post(port: int, body: bytes, headers: dict[str, str], path: str = "/webhooks/github"):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("POST", path, body=body, headers={"Content-Type": "application/json", **headers})
    resp = conn.getresponse()
    data = json.loads(resp.read().decode())
    conn.close()
    return resp.status, data


def github_headers(body: bytes, event: str, *, delivery: str = "d-1") -> dict[str, str]:
    return {
        "X-GitHub-Event": event,
        "X-GitHub-Delivery": delivery,
        "X-Hub-Signature-256": sign(body),
    }


def test_server_routes_labeled_issue_to_airflow(served) -> None:
    port, opener, lines = served
    body = json.dumps(issue_payload("labeled", label="factory:hotfix", number=9)).encode()
    status, data = post(port, body, github_headers(body, "issues", delivery="abc"))
    assert status == 202
    assert data == {
        "routed": True,
        "dag_id": "hotfix",
        "conf": {"issues": ["9"]},
        "dag_run_id": "manual__2026-01-01",
    }
    (call,) = opener.calls
    assert call["url"] == f"{AIRFLOW}/api/v2/dags/hotfix/dagRuns"
    assert call["headers"]["authorization"] == "Bearer T"
    assert call["body"] == {"conf": {"issues": ["9"]}, "logical_date": None}
    assert any("delivery=abc" in line and "hotfix" in line for line in lines)
    assert not any(SECRET in line for line in lines)


def test_server_ignores_unrouted_events(served) -> None:
    port, opener, _ = served
    body = json.dumps({"zen": "Design for failure."}).encode()
    status, data = post(port, body, github_headers(body, "ping"))
    assert (status, data["routed"]) == (200, False)
    body = json.dumps(issue_payload("labeled", label="factory", pr=True)).encode()
    status, data = post(port, body, github_headers(body, "issues"))
    assert (status, data["routed"]) == (200, False)
    assert opener.calls == []


def test_server_rejects_bad_signature(served) -> None:
    port, opener, _ = served
    body = json.dumps(issue_payload("labeled", label="factory")).encode()
    headers = github_headers(body, "issues")
    headers["X-Hub-Signature-256"] = sign(body, "wrong")
    assert post(port, body, headers)[0] == 401
    del headers["X-Hub-Signature-256"]
    assert post(port, body, headers)[0] == 401
    assert opener.calls == []


def test_server_rejects_invalid_json(served) -> None:
    port, _, _ = served
    body = b"{not json"
    assert post(port, body, github_headers(body, "issues"))[0] == 400


def test_server_unknown_paths_and_health(served) -> None:
    port, _, _ = served
    body = b"{}"
    assert post(port, body, github_headers(body, "issues"), path="/other")[0] == 404
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("GET", "/healthz")
    resp = conn.getresponse()
    assert resp.status == 200
    assert json.loads(resp.read()) == {"ok": True}
    conn.request("GET", "/")
    assert conn.getresponse().status == 404
    conn.close()


def test_server_maps_airflow_failure_to_502() -> None:
    err = urllib.error.HTTPError(AIRFLOW, 409, "conflict", None, io.BytesIO(b'{"detail":"dup"}'))
    server = webhook.make_server(
        0,
        airflow_url=AIRFLOW,
        token_provider=lambda: "T",
        secret=None,
        opener=FakeOpener(fail=err),
        host="127.0.0.1",
        log=lambda _line: None,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        body = json.dumps(issue_payload("labeled", label="factory")).encode()
        # secret=None: islo verified upstream, so no signature header is needed here.
        status, data = post(port, body, {"X-GitHub-Event": "issues"})
        assert status == 502
        assert data["error"] == "airflow HTTP 409"
        assert data["detail"] == '{"detail":"dup"}'
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


# ---------------------------------------------------------------- CLI


def test_cli_webhook_route(tmp_path: Path) -> None:
    payload = tmp_path / "event.json"
    payload.write_text(json.dumps(issue_payload("labeled", label="factory:hotfix")))
    result = CliRunner().invoke(app, ["webhook", "route", "issues", str(payload)])
    assert result.exit_code == 0, result.output
    assert "POST /api/v2/dags/hotfix/dagRuns" in result.output
    assert '"issues": ["42"]' in result.output
    payload.write_text(json.dumps(issue_payload("labeled", label="bug")))
    result = CliRunner().invoke(app, ["webhook", "route", "issues", str(payload)])
    assert result.exit_code == 1
    assert "ignored" in result.output


def test_cli_webhook_serve_requires_airflow_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("AIRFLOW_TOKEN", "AIRFLOW_USER", "AIRFLOW_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    result = CliRunner().invoke(app, ["webhook", "serve", "--port", "0"])
    assert result.exit_code == 2
    assert "AIRFLOW_TOKEN" in result.output
