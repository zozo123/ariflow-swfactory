from __future__ import annotations

import io
import json
import threading
import urllib.request
from dataclasses import replace

import pytest
from test_durable_dispatch import LINE, Backend
from typer.testing import CliRunner

from swfactory import blueprint, cell_callback, runtime
from swfactory.admission import Limits
from swfactory.backend import make_server
from swfactory.backend.service import Refused
from swfactory.backend_scm import BackendScm
from swfactory.cell_runtime import bind_jobs
from swfactory.cli import app
from swfactory.linear_intake import LinearWorkRequest
from swfactory.linear_source import LinearPreview, LinearSource, LinearSourceError
from swfactory.models import StageError
from swfactory.scm import GitHubScm
from swfactory.security_contract import policy_digest_for_mapping

WORKSPACE = "12345678-1234-4234-8234-123456789abc"
PROJECT = "22345678-1234-4234-8234-123456789abc"
ISSUE = "32345678-1234-4234-8234-123456789abc"
TEAM = "42345678-1234-4234-8234-123456789abc"
PREVIEW = LinearPreview(
    workspace_id=WORKSPACE,
    issue_id=ISSUE,
    identifier="YOS-104",
    url="https://linear.app/factory-fixture/issue/YOS-104/native-intake",
    team_id=TEAM,
    project_id=PROJECT,
    title="Original intent",
    description="Original acceptance criteria.\n",
    updated_at="2026-10-06T12:00:00Z",
    state_type="unstarted",
    archived_at=None,
)
NATIVE_LINE = (
    LINE.replace("auto = true", 'mode = "human"')
    + f'''
[[gates]]
after = "plan"
artifact = "plan.md"
mode = "human"
timeout_h = 1

[work_source]
kind = "linear"
workspace_id = "{WORKSPACE}"
project_id = "{PROJECT}"
'''
)


@pytest.fixture
def box(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for key in ("SWF_DRAIN", "SWF_GENERATION"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("SWF_LINEAR_API_KEY", "lin_api_synthetic_fixture")
    (tmp_path / "blueprints").mkdir()
    (tmp_path / "blueprints/line.toml").write_text(NATIVE_LINE)

    class NativeBackend(Backend):
        def _open(self):
            factory = super()._open()
            factory.repo = "owner/one"
            return factory

    made = NativeBackend(tmp_path, Limits())
    made.source_reads = []

    def resolve(source, issue_id):
        made.source_reads.append(issue_id)
        assert source.workspace_id == WORKSPACE and source.project_id == PROJECT
        return PREVIEW

    monkeypatch.setattr(LinearSource, "resolve_for_admission", resolve)
    monkeypatch.setattr(GitHubScm, "fetch_issue", lambda *a, **kw: pytest.fail("GitHub issue lookup forbidden"))
    monkeypatch.setattr(GitHubScm, "open_issue", lambda *a, **kw: pytest.fail("GitHub issue creation forbidden"))
    try:
        yield made
    finally:
        made.close()


def request(preview=PREVIEW, *, attempt="initial"):
    return {
        "line": "line",
        "work_source": {
            "schema_version": 1,
            "kind": "linear",
            "issue_id": preview.issue_id,
            "intent_digest": preview.intent_digest,
            "attempt": attempt,
        },
    }


def source_request(box, receipt):
    cell = box.factory.cell_store.get(receipt["cells"][0])
    return {
        "cell_id": cell["cell_id"],
        "epoch": cell["epoch"],
        "policy_digest": cell["policy_digest"],
        "ref": cell["issue"],
        "blueprint_digest": policy_digest_for_mapping(blueprint.load("line").model_dump(mode="json")),
    }


def test_native_source_enters_existing_admission_without_github_issues(box):
    receipt = box.factory.submit(request())
    assert receipt["state"] == "submitted"
    assert len(receipt["cells"]) == 1
    assert receipt["accepted_source_digest"] == PREVIEW.intent_digest
    order = box.factory.control.admission.work_order(receipt["submission_id"]).payload
    assert order["accepted_source"]["snapshot"]["description"] == "Original acceptance criteria.\n"
    assert order["work_source"]["issue_id"] == ISSUE
    assert len(box.airflow.created) == 1
    conf = box.airflow.posts[0]["conf"]
    assert "accepted_source" not in conf
    assert "lin_api_synthetic_fixture" not in json.dumps(conf)
    assert len(bind_jobs(blueprint.load("line").jobs(conf), conf["_factory_cells"])) == 1


def test_operator_cli_uses_real_backend_http_boundary_and_retries_same_receipt(box, monkeypatch):
    server = make_server(box.factory, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("SWF_BACKEND_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("SWF_BACKEND_TOKEN", "t" * 40)
    try:
        args = ["linear-submit", ISSUE, "--line", "line", "--intent-digest", PREVIEW.intent_digest]
        first, repeated = [CliRunner().invoke(app, args) for _ in range(2)]
        assert first.exit_code == 0, first.output
        assert repeated.exit_code == 0, repeated.output
        assert json.loads(first.output) == json.loads(repeated.output)
        assert len(box.airflow.created) == 1
        assert "lin_api_synthetic_fixture" not in first.output
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_duplicate_and_lost_client_response_recover_same_receipt_after_restart(box, monkeypatch):
    original = box.factory.submit(request())
    box.restart()
    monkeypatch.setattr(LinearSource, "resolve_for_admission", lambda *a: pytest.fail("retry re-read mutable Linear"))
    monkeypatch.delenv("SWF_LINEAR_API_KEY")
    repeated = box.factory.submit(request())
    assert repeated == original
    assert len(box.airflow.created) == 1
    assert len(box.factory.cell_store.list()) == 1


def test_lost_airflow_response_is_observed_after_restart_without_second_dispatch(box):
    def lose_response(method, path, body):
        response = box.airflow(method, path, body)
        if method == "POST" and path.endswith("/dagRuns"):
            raise OSError("lost Airflow response after remote commit")
        return response

    box.factory.airflow = lose_response
    with pytest.raises(Exception, match="lost Airflow response"):
        box.factory.submit(request())
    assert len(box.airflow.created) == 1
    box.restart()
    receipt = box.factory.submit(request())
    assert receipt["state"] == "submitted"
    assert len(box.airflow.created) == 1
    assert len(box.airflow.posts) == 1


def test_queued_native_snapshot_survives_restart_and_capacity_release(box, monkeypatch):
    box.factory.control.admission.limits = box.limits = Limits(global_active=1)
    first = box.factory.submit(request())
    second_source = replace(PREVIEW, issue_id=TEAM, identifier="YOS-105", url=PREVIEW.url.replace("YOS-104", "YOS-105"))
    monkeypatch.setattr(LinearSource, "resolve_for_admission", lambda *a: second_source)
    queued = box.factory.submit(request(second_source))
    assert queued["state"] == "queued"
    assert queued["accepted_source_digest"] == second_source.intent_digest
    box.restart()
    monkeypatch.setattr(
        LinearSource, "resolve_for_admission", lambda *a: pytest.fail("queued retry read mutable source")
    )
    assert box.factory.submit(request(second_source)) == queued
    box.finish(first["cells"][0])
    bound = box.factory.submit(request(second_source))
    assert bound["state"] == "submitted"
    assert bound["submission_id"] == queued["submission_id"]
    assert len(box.airflow.created) == 2


def test_terminal_duplicate_never_rearms_cell_without_explicit_attempt(box):
    first = box.factory.submit(request())
    box.finish(first["cells"][0])
    repeated = box.factory.submit(request())
    assert repeated["submission_id"] == first["submission_id"]
    assert repeated["state"] == "success"
    assert box.factory.cell_store.get(first["cells"][0])["epoch"] == 1
    assert len(box.airflow.created) == 1
    next_attempt = box.factory.submit(request(attempt="operator-retry-2"))
    assert next_attempt["submission_id"] != first["submission_id"]
    assert next_attempt["cells"] == first["cells"]
    assert box.factory.cell_store.get(first["cells"][0])["epoch"] == 2


def test_status_only_updates_keep_receipt_and_original_snapshot(box, monkeypatch):
    first = box.factory.submit(request())
    changed = replace(PREVIEW, state_type="completed", updated_at="2026-10-07T12:00:00Z")
    monkeypatch.setattr(LinearSource, "resolve_for_admission", lambda *a: changed)
    second = box.factory.submit(request(changed))
    assert first == second
    assert len(box.airflow.created) == 1
    issue = box.factory.operation("/scm/linear-source", source_request(box, first))
    assert issue["body"] == "Original acceptance criteria.\n"
    assert issue["state"] == "open"


def test_intent_edit_requires_explicit_digest_and_new_epoch(box, monkeypatch):
    first = box.factory.submit(request())
    changed = replace(PREVIEW, description="Explicitly accepted new criteria")
    monkeypatch.setattr(LinearSource, "resolve_for_admission", lambda *a: changed)
    with pytest.raises(Refused, match="intent changed"):
        box.factory.submit(request(attempt="new-intent"))
    box.finish(first["cells"][0])
    second = box.factory.submit(request(changed))
    assert second["submission_id"] != first["submission_id"]
    assert box.factory.cell_store.get(first["cells"][0])["epoch"] == 2
    issue = box.factory.operation("/scm/linear-source", source_request(box, second))
    assert issue["body"] == "Explicitly accepted new criteria"


def test_source_read_is_fenced_by_cell_epoch_policy_and_worker_blueprint(box):
    receipt = box.factory.submit(request())
    body = source_request(box, receipt)
    for field, value in [
        ("epoch", 99),
        ("policy_digest", "policy:wrong"),
        ("blueprint_digest", "policy:wrong"),
        ("ref", "101"),
    ]:
        with pytest.raises(Refused):
            box.factory.operation("/scm/linear-source", {**body, field: value})
    assert len(box.airflow.created) == 1


def test_source_integrity_failure_refuses_worker_read(box):
    receipt = box.factory.submit(request())
    order = box.factory.control.admission.work_order(receipt["submission_id"]).payload
    order["accepted_source"]["snapshot"]["description"] = "tampered"
    box.factory.control.admission.db.execute(
        "UPDATE admission_order SET payload_json=? WHERE work_id=?", (json.dumps(order), receipt["submission_id"])
    )
    with pytest.raises(Refused, match="integrity verification"):
        box.factory.operation("/scm/linear-source", source_request(box, receipt))


@pytest.mark.parametrize(
    "changes",
    [
        {"issues": ["104"]},
        {"airflow_run_id": "scheduled"},
        {"work_source": {"schema_version": 2, "kind": "linear"}},
        {"work_source": {"schema_version": 1, "kind": "github"}},
    ],
)
def test_unsupported_native_contract_refuses_before_side_effects(box, changes):
    with pytest.raises(ValueError):
        box.factory.submit({**request(), **changes})
    assert not box.airflow.posts and not box.source_reads
    assert not box.factory.cell_store.list()


def test_source_read_failure_never_reserves_a_cell(box, monkeypatch):
    def denied(*a):
        raise LinearSourceError("workspace unauthorized")

    monkeypatch.setattr(LinearSource, "resolve_for_admission", denied)
    with pytest.raises(Refused, match="workspace unauthorized"):
        box.factory.submit(request())
    assert not box.airflow.posts
    assert not box.factory.cell_store.list()
    assert box.factory.control.admission.snapshot()["active"] == []


def test_unconfigured_publication_target_refuses_before_source_read(box):
    box.factory.repo = "different/repository"
    with pytest.raises(Refused, match="publication repository"):
        box.factory.submit(request())
    assert not box.source_reads and not box.airflow.posts


def test_linear_cell_cannot_create_a_github_issue(box):
    receipt = box.factory.submit(request())
    body = {**source_request(box, receipt), "operation_key": "attempted-bridge", "title": "bridge", "body": "bridge"}
    with pytest.raises(Refused, match="cannot create GitHub issues"):
        box.factory.operation("/scm/open-issue", body)


def test_native_worker_uses_backend_snapshot_after_restart_without_linear_credentials(box, monkeypatch, tmp_path):
    receipt = box.factory.submit(request())
    conf = box.airflow.posts[0]["conf"]
    bp = blueprint.load("line")
    job = bind_jobs(bp.jobs(conf), conf["_factory_cells"])[0]
    box.restart()
    monkeypatch.delenv("SWF_LINEAR_API_KEY")
    monkeypatch.setenv("SWF_BACKEND_URL", "https://backend.invalid")
    monkeypatch.setenv("SWF_BACKEND_TOKEN", "t" * 40)
    monkeypatch.setattr(cell_callback, "post", lambda path, body: box.factory.operation(path, body))
    monkeypatch.setattr(BackendScm, "_post", lambda self, path, body, **kw: box.factory.operation(path, body))
    ctx = runtime.build_ctx(
        bp,
        job,
        run_id="native001",
        root=tmp_path,
        overrides={"scm": "github", "agent": "scripted", "sandbox": "islo"},
    )
    assert ctx.issue.body == PREVIEW.description
    assert ctx.issue.url == PREVIEW.url
    assert ctx.state.read_control("accepted-inputs.json")
    assert receipt["cells"][0] == json.loads(ctx.state.read_control("cell.json"))["cell_id"]


def test_native_blueprint_refuses_unmanaged_worker_before_sandbox(box, monkeypatch, tmp_path):
    bp = blueprint.load("line")
    ref = LinearWorkRequest.model_validate(request()["work_source"]).issue_ref(bp.work_source)
    job = bp.jobs({"issues": [ref]})[0]
    monkeypatch.setattr(runtime, "make_sandbox", lambda *a, **kw: pytest.fail("unmanaged sandbox"))
    with pytest.raises(StageError, match="backend-managed Cell"):
        runtime.build_ctx(bp, job, run_id="native002", root=tmp_path, overrides={"scm": "github"})


def test_legacy_blueprint_serialization_is_unchanged():
    assert "work_source" not in blueprint.loads(LINE).model_dump(mode="json")


@pytest.mark.parametrize(
    "replacement",
    [
        NATIVE_LINE.replace('mode = "human"', 'mode = "auto"'),
        NATIVE_LINE.replace('kind = "linear"', 'kind = "other"'),
        NATIVE_LINE.replace(WORKSPACE, "YOS"),
    ],
)
def test_source_configuration_and_human_gates_fail_closed(replacement):
    with pytest.raises(ValueError):
        blueprint.loads(replacement)


def test_concurrent_submissions_converge_on_one_cell_and_run(box):
    barrier = threading.Barrier(2)
    results, errors = [], []

    def submit():
        try:
            barrier.wait(timeout=2)
            results.append(box.factory.submit(request()))
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert not errors
    assert len(results) == 2
    assert len({result["submission_id"] for result in results}) == 1
    assert len(box.airflow.created) == 1
    assert len(box.factory.cell_store.list()) == 1


def test_admission_query_refuses_unverified_dependencies(monkeypatch):
    payload = {
        "data": {
            "organization": {"id": WORKSPACE, "urlKey": "factory-fixture"},
            "issue": {
                "id": ISSUE,
                "identifier": PREVIEW.identifier,
                "url": PREVIEW.url,
                "title": PREVIEW.title,
                "description": PREVIEW.description,
                "updatedAt": PREVIEW.updated_at,
                "archivedAt": None,
                "team": {"id": TEAM},
                "project": {"id": PROJECT},
                "state": {"type": "unstarted"},
                "inverseRelations": {"nodes": [{"type": "blocks"}], "pageInfo": {"hasNextPage": False}},
            },
        }
    }

    class Opener:
        def open(self, req, **kw):
            assert "inverseRelations" in json.loads(req.data)["query"]
            return io.BytesIO(json.dumps(payload).encode())

    monkeypatch.setattr(urllib.request, "build_opener", lambda *a: Opener())
    source = LinearSource("synthetic-key", WORKSPACE, PROJECT)
    with pytest.raises(LinearSourceError, match="dependency delivery receipts"):
        source.resolve_for_admission(ISSUE)
    payload["data"]["issue"]["inverseRelations"]["nodes"] = [{"type": "unknown"}]
    with pytest.raises(LinearSourceError, match="eligibility is unknown"):
        source.resolve_for_admission(ISSUE)
    payload["data"]["issue"]["inverseRelations"]["nodes"] = []
    assert source.resolve_for_admission(ISSUE).intent_digest == PREVIEW.intent_digest
    payload["data"]["issue"]["inverseRelations"]["pageInfo"]["hasNextPage"] = True
    with pytest.raises(LinearSourceError, match="incomplete"):
        source.resolve_for_admission(ISSUE)


def test_missing_dependency_evidence_and_terminal_source_refuse(monkeypatch):
    payload = {
        "data": {
            "organization": {"id": WORKSPACE, "urlKey": "factory-fixture"},
            "issue": {
                "id": ISSUE,
                "identifier": PREVIEW.identifier,
                "url": PREVIEW.url,
                "title": PREVIEW.title,
                "description": PREVIEW.description,
                "updatedAt": PREVIEW.updated_at,
                "archivedAt": None,
                "team": {"id": TEAM},
                "project": {"id": PROJECT},
                "state": {"type": "unstarted"},
            },
        }
    }

    class Opener:
        def open(self, *a, **kw):
            return io.BytesIO(json.dumps(payload).encode())

    monkeypatch.setattr(urllib.request, "build_opener", lambda *a: Opener())
    source = LinearSource("synthetic-key", WORKSPACE, PROJECT)
    with pytest.raises(LinearSourceError, match="evidence is missing"):
        source.resolve_for_admission(ISSUE)
    payload["data"]["issue"]["state"]["type"] = "completed"
    with pytest.raises(LinearSourceError, match="terminal"):
        source.resolve_for_admission(ISSUE)
