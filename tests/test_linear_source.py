from __future__ import annotations

import copy
import io
import json
import urllib.error
import urllib.request
from dataclasses import FrozenInstanceError
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pytest
from typer.testing import CliRunner

from swfactory import linear_source
from swfactory.cli import app
from swfactory.linear_source import (
    ENDPOINT,
    MAX_RESPONSE_BYTES,
    LinearSource,
    LinearSourceError,
    parse_preview,
)

WORKSPACE = "12345678-1234-4234-8234-123456789abc"
PROJECT = "22345678-1234-4234-8234-123456789abc"
ISSUE = "32345678-1234-4234-8234-123456789abc"
TEAM = "42345678-1234-4234-8234-123456789abc"
KEY = "lin_api_fixture_not_a_credential"


@pytest.fixture
def document():
    return {
        "data": {
            "organization": {"id": WORKSPACE, "urlKey": "factory-fixture"},
            "issue": {
                "id": ISSUE,
                "identifier": "YOS-104",
                "url": "https://linear.app/factory-fixture/issue/YOS-104/source-preview",
                "title": "Preserve the originator's intent",
                "description": "Exact text.\n\n- Include Unicode: 世界\n- Keep all acceptance criteria.\n",
                "updatedAt": "2026-10-06T12:00:00Z",
                "archivedAt": None,
                "team": {"id": TEAM},
                "project": {"id": PROJECT},
                "state": {"type": "started"},
            },
        }
    }


def parse(document):
    return parse_preview(document, workspace_id=WORKSPACE, project_id=PROJECT, issue_id=ISSUE)


def test_preserves_original_text_and_immutable_source_identity(document):
    preview = parse(document)
    assert preview.source_key == f"linear:{WORKSPACE}:{ISSUE}"
    assert preview.description == "Exact text.\n\n- Include Unicode: 世界\n- Keep all acceptance criteria.\n"
    assert preview.title == "Preserve the originator's intent"
    assert preview.to_dict()["admission_ready"] is False
    with pytest.raises(FrozenInstanceError):
        preview.title = "changed"
    document["data"]["issue"]["title"] = "changed after parsing"
    assert preview.title == "Preserve the originator's intent"


def test_status_identifier_and_revision_metadata_do_not_change_intent(document):
    before = parse(document)
    issue = document["data"]["issue"]
    issue.update(
        identifier="YOS-999",
        url="https://linear.app/factory-fixture/issue/YOS-999/renamed",
        updatedAt="2026-10-07T12:00:00Z",
    )
    issue["state"]["type"] = "completed"
    after = parse(document)
    assert after.intent_digest == before.intent_digest
    assert after.source_key == before.source_key
    assert after.state_type == "completed"
    assert after.to_dict()["admission_ready"] is False


@pytest.mark.parametrize("field,value", [("title", "Changed intent"), ("description", "Changed acceptance criteria")])
def test_intent_edits_change_digest_but_preserve_source_identity(document, field, value):
    before = parse(document)
    document["data"]["issue"][field] = value
    after = parse(document)
    assert after.source_key == before.source_key
    assert after.intent_digest != before.intent_digest


def test_repeated_reads_and_json_roundtrip_preserve_identity(document):
    first = parse(document)
    second = parse(json.loads(json.dumps(document)))
    assert first.to_dict() == second.to_dict()


@pytest.mark.parametrize(
    "path,value,match",
    [
        (("organization", "id"), TEAM, "workspace differs"),
        (("issue", "id"), TEAM, "requested UUID"),
        (("issue", "project", "id"), TEAM, "project differs"),
        (("issue", "team", "id"), "YOS-104", "immutable UUID"),
        (("issue", "project"), None, "missing required"),
        (("issue", "state", "type"), "unknown", "unsupported workflow state"),
        (("issue", "updatedAt"), "2026-10-06", "timezone"),
        (("issue", "updatedAt"), "yesterday", "invalid timestamp"),
        (("issue", "title"), " ", "invalid text"),
        (("issue", "description"), {"text": "wrong type"}, "invalid text"),
        (("issue", "identifier"), "../../YOS-104", "display identifier"),
        (("organization", "urlKey"), "../escape", "workspace URL key"),
    ],
)
def test_rejects_malformed_or_unexpected_source(document, path, value, match):
    node = document["data"]
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(LinearSourceError, match=match):
        parse(document)


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/factory-fixture/issue/YOS-104",
        "https://linear.app@evil.example/factory-fixture/issue/YOS-104",
        "http://linear.app/factory-fixture/issue/YOS-104",
        "https://linear.app/other-workspace/issue/YOS-104",
        "https://linear.app/factory-fixture/issue/YOS-1040",
        "https://linear.app/factory-fixture/issue/YOS-104?token=oops",
        "https://[",
        "https://linear.app/\nfactory-fixture/issue/YOS-104",
    ],
)
def test_rejects_noncanonical_urls(document, url):
    document["data"]["issue"]["url"] = url
    with pytest.raises(LinearSourceError, match="canonical issue URL"):
        parse(document)


@pytest.mark.parametrize("payload", [None, [], {}, {"data": None}, {"data": {"organization": {}}}])
def test_incomplete_read_never_becomes_a_preview(payload):
    with pytest.raises(LinearSourceError):
        parse(payload)


def test_rejects_partial_graphql_success_without_echoing_errors(document):
    document["errors"] = [{"message": KEY}]
    with pytest.raises(LinearSourceError, match="GraphQL errors") as error:
        parse(document)
    assert KEY not in str(error.value)


@pytest.mark.parametrize("secret", [KEY, "Bearer synthetic_sensitive_token", "ghp_" + "a" * 30])
def test_credential_content_refused_without_echo(document, secret):
    document["data"]["issue"]["description"] = secret
    with pytest.raises(LinearSourceError, match="credential-like content") as error:
        parse(document)
    assert secret not in str(error.value)


def test_null_description_is_empty_and_archived_state_remains_observational(document):
    document["data"]["issue"].update(description=None, archivedAt="2026-10-06T12:00:00Z")
    preview = parse(document)
    assert preview.description == ""
    assert preview.archived_at == "2026-10-06T12:00:00Z"
    assert preview.to_dict()["admission_ready"] is False


@pytest.mark.parametrize("state", ["completed", "canceled", "duplicate"])
def test_terminal_linear_states_do_not_prove_delivery(document, state):
    document["data"]["issue"]["state"]["type"] = state
    preview = parse(document)
    assert preview.state_type == state
    assert preview.to_dict()["admission_ready"] is False


def transport(monkeypatch, *, raw=None, error=None):
    calls = []

    class Opener:
        def open(self, request, timeout):
            calls.append((request, timeout))
            if error is not None:
                raise error
            return io.BytesIO(raw)

    monkeypatch.setattr(urllib.request, "build_opener", lambda *handlers: Opener())
    return calls


def test_controller_makes_only_one_graphql_query_without_mutation(monkeypatch, document):
    calls = transport(monkeypatch, raw=json.dumps(document).encode())
    source = LinearSource(KEY, WORKSPACE, PROJECT)
    preview = source.preview(ISSUE)
    assert preview.issue_id == ISSUE
    assert KEY not in repr(source)
    assert KEY not in json.dumps(preview.to_dict())
    assert len(calls) == 1
    request, timeout = calls[0]
    assert request.full_url == ENDPOINT
    assert request.get_header("Authorization") == KEY
    assert timeout == 15
    body = json.loads(request.data)
    assert body["query"].startswith("query FactoryLinearPreview")
    assert "mutation" not in body["query"]
    assert body["variables"] == {"id": ISSUE}


@pytest.mark.parametrize("issue_id", ["YOS-104", "https://linear.app/issue/YOS-104", "0" * 32, "not-a-uuid"])
def test_bad_reference_is_refused_before_network(monkeypatch, issue_id):
    calls = transport(monkeypatch)
    with pytest.raises(LinearSourceError):
        LinearSource(KEY, WORKSPACE, PROJECT).preview(issue_id)
    assert calls == []


@pytest.mark.parametrize(
    "raw", [b"invalid", b"\xff", b"[]", b"{}", pytest.param(b"a" * (MAX_RESPONSE_BYTES + 1), id="oversized")]
)
def test_controller_refuses_invalid_response(monkeypatch, raw):
    transport(monkeypatch, raw=raw)
    with pytest.raises(LinearSourceError):
        LinearSource(KEY, WORKSPACE, PROJECT).preview(ISSUE)


@pytest.mark.parametrize("code", [302, 401, 403, 429, 500])
def test_controller_http_errors_do_not_expose_remote_body_or_retry(monkeypatch, code):
    error = urllib.error.HTTPError(ENDPOINT, code, KEY, {}, io.BytesIO(KEY.encode()))
    calls = transport(monkeypatch, error=error)
    with pytest.raises(LinearSourceError, match=f"HTTP {code}") as raised:
        LinearSource(KEY, WORKSPACE, PROJECT).preview(ISSUE)
    assert KEY not in str(raised.value)
    assert len(calls) == 1


def test_controller_refuses_credential_echo_anywhere_in_payload(monkeypatch, document):
    document = copy.deepcopy(document)
    document["extension"] = KEY
    transport(monkeypatch, raw=json.dumps(document).encode())
    with pytest.raises(LinearSourceError, match="controller credential"):
        LinearSource(KEY, WORKSPACE, PROJECT).preview(ISSUE)


@pytest.mark.parametrize("method", ["preview", "resolve_for_admission"])
@pytest.mark.parametrize("field", ["title", "description"])
def test_controller_refuses_json_escaped_credential_in_source(monkeypatch, document, method, field):
    key = "synthetic-controller-secret-92847"
    document["data"]["issue"][field] = f"Source text containing {key}."
    document["data"]["issue"]["inverseRelations"] = {"nodes": [], "pageInfo": {"hasNextPage": False}}
    escaped_key = "".join(f"\\u{ord(character):04x}" for character in key)
    raw = json.dumps(document).replace(key, escaped_key).encode()
    assert key.encode() not in raw
    transport(monkeypatch, raw=raw)
    source = LinearSource(key, WORKSPACE, PROJECT)
    with pytest.raises(LinearSourceError, match="controller credential") as raised:
        getattr(source, method)(ISSUE)
    assert key not in str(raised.value)


def test_real_transport_never_follows_redirect_with_controller_key(monkeypatch):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(self.path)
            self.send_response(302)
            self.send_header("Location", "/redirect-target")
            self.end_headers()

        def do_GET(self):
            requests.append(self.path)
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        monkeypatch.setattr(linear_source, "ENDPOINT", f"http://127.0.0.1:{server.server_port}/graphql")
        try:
            with pytest.raises(LinearSourceError, match="HTTP 302"):
                LinearSource(KEY, WORKSPACE, PROJECT).preview(ISSUE)
        finally:
            server.shutdown()
            thread.join(timeout=2)
    assert requests == ["/graphql"]


def test_transport_failure_is_sanitized_without_retry(monkeypatch):
    calls = transport(monkeypatch, error=urllib.error.URLError(KEY))
    with pytest.raises(LinearSourceError, match="checking connectivity") as raised:
        LinearSource(KEY, WORKSPACE, PROJECT).preview(ISSUE)
    assert KEY not in str(raised.value)
    assert len(calls) == 1


def test_cli_prints_preview_without_admission(monkeypatch, document):
    calls = transport(monkeypatch, raw=json.dumps(document).encode())
    monkeypatch.setenv("SWF_LINEAR_API_KEY", KEY)
    result = CliRunner().invoke(app, ["linear-preview", ISSUE, "--workspace-id", WORKSPACE, "--project-id", PROJECT])
    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert output["source_key"] == f"linear:{WORKSPACE}:{ISSUE}"
    assert output["admission_ready"] is False
    assert "submission_id" not in output
    assert len(calls) == 1
    assert KEY not in result.output


def test_cli_missing_controller_key_fails_before_network(monkeypatch):
    calls = transport(monkeypatch)
    monkeypatch.delenv("SWF_LINEAR_API_KEY", raising=False)
    result = CliRunner().invoke(app, ["linear-preview", ISSUE, "--workspace-id", WORKSPACE, "--project-id", PROJECT])
    assert result.exit_code == 2
    assert "trusted controller" in result.output
    assert calls == []
