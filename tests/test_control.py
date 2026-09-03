"""``swfactory.control``: clients over fake transports, the sandbox-removal guard, ``collect``.

Hermetic: the ``opener`` records ``urllib.request.Request`` objects and answers from a canned
``(method, path)`` table; the ``runner`` records argv and answers with ``CompletedProcess``.
Response shapes mirror what Airflow 3.3.1 (``/api/v2``) and ``gh --json`` really return.
"""

from __future__ import annotations

import io
import json
import subprocess
import urllib.error
import urllib.request
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from swfactory.control import (
    AirflowClient,
    ControlError,
    Gate,
    GitHubClient,
    IsloClient,
    JobRow,
    MetricsSource,
    Run,
    Sandbox,
    TaskState,
    collapsed_job,
    collect,
    group_jobs,
    job_state,
    summarize_checks,
)

AF = "http://af:8080"
RUN_ID = "manual__2026-09-03T08:04:02.456739+00:00"
RUN_SEG = "manual__2026-09-03T08%3A04%3A02.456739%2B00%3A00"


# ---------------------------------------------------------------- fakes


class _Resp:
    def __init__(self, payload: Any) -> None:
        self._raw = b"" if payload is None else json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._raw

    def close(self) -> None:
        pass


class FakeOpener:
    """Answers ``(METHOD, path-with-query)`` from ``routes``; records every request."""

    def __init__(self, routes: dict[tuple[str, str], Any] | None = None) -> None:
        self.routes = routes or {}
        self.requests: list[urllib.request.Request] = []

    def __call__(self, request: urllib.request.Request, *, timeout: float) -> _Resp:
        self.requests.append(request)
        path = request.full_url.removeprefix(AF)
        key = (request.get_method(), path)
        if key not in self.routes:
            raise urllib.error.HTTPError(
                request.full_url, 404, "Not Found", {}, io.BytesIO(b'{"detail":"no route"}')
            )
        payload = self.routes[key]
        if isinstance(payload, Exception):
            raise payload
        return _Resp(payload)

    @property
    def last(self) -> urllib.request.Request:
        return self.requests[-1]

    def body(self, i: int = -1) -> Any:
        data = self.requests[i].data
        return None if data is None else json.loads(data)


class FakeRunner:
    """``subprocess.run`` stand-in keyed on the first two argv words."""

    def __init__(self, outputs: dict[str, str | Exception] | None = None) -> None:
        self.outputs = outputs or {}
        self.calls: list[list[str]] = []

    def __call__(self, argv: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        argv = list(argv)
        self.calls.append(argv)
        key = " ".join(argv[:2])
        out = self.outputs.get(key, "")
        if isinstance(out, Exception):
            raise out
        if out == "FAIL":
            return subprocess.CompletedProcess(argv, 1, "", "boom")
        return subprocess.CompletedProcess(argv, 0, out, "")


def _gate(**kw: Any) -> Gate:
    base: dict[str, Any] = dict(
        dag_id="factory",
        run_id=RUN_ID,
        task_id="job.approve_plan",
        map_index=0,
        subject="[factory] approve plan.md for 42",
        body="",
        created_at=None,
        options=["Approve", "Reject"],
    )
    base.update(kw)
    return Gate(**base)


HITL_LIVE = {  # verbatim shape of GET /api/v2/dags/~/dagRuns/~/hitlDetails on Airflow 3.3.1
    "hitl_details": [
        {
            "options": ["Approve", "Reject"],
            "subject": "[factory] approve intent.md for demo/issue.md",
            "body": "Run manual__... · job 0 · `intent.md`",
            "defaults": None,
            "multiple": False,
            "params": {},
            "assigned_users": [],
            "created_at": "2026-09-03T08:19:12.857207Z",
            "responded_by_user": None,
            "responded_at": None,
            "chosen_options": None,
            "params_input": {},
            "response_received": False,
            "task_instance": {
                "task_id": "job.approve_intent",
                "dag_id": "factory",
                "dag_run_id": RUN_ID,
                "map_index": 0,
                "state": "deferred",
            },
        }
    ],
    "total_entries": 1,
}

ISLO_LS = json.dumps(
    [
        {"name": "swf-42-abcd1234", "status": "running", "created_by": "Me@x.io"},
        {"name": "swf-99-deadbeef", "status": "running", "created_by": "teammate@x.io"},
        {"name": "swf-77-0badf00d", "status": "deleted", "created_by": "me@x.io"},
        {"name": "my-personal-box", "status": "running", "created_by": "me@x.io"},
    ]
)


# ---------------------------------------------------------------- Airflow


def test_airflow_reads_send_bearer_and_hit_v2_routes() -> None:
    opener = FakeOpener(
        {
            ("GET", "/api/v2/dags?tags=swfactory&limit=100"): {
                "dags": [{"dag_id": "factory"}, {"dag_id": "hotfix"}]
            },
            ("GET", "/api/v2/dags/factory/dagRuns?limit=2&order_by=-run_after"): {
                "dag_runs": [
                    {
                        "dag_run_id": RUN_ID,
                        "dag_id": "factory",
                        "state": "success",
                        "start_date": "2026-09-03T08:18:24.904858Z",
                        "end_date": "2026-09-03T08:21:10.613775Z",
                        "conf": {"issues": ["demo/issue.md"]},
                    }
                ]
            },
            (
                "GET",
                f"/api/v2/dags/factory/dagRuns/{RUN_SEG}/taskInstances?limit=500",
            ): {
                "task_instances": [
                    {"task_id": "fan_out", "map_index": -1, "state": "success"},
                    {"task_id": "job.build_and_test", "map_index": 0, "state": "running"},
                ]
            },
            (
                "GET",
                "/api/v2/dags/~/dagRuns/~/hitlDetails?response_received=false&limit=100",
            ): HITL_LIVE,
        }
    )
    af = AirflowClient(AF + "/", token="tok", opener=opener)

    assert af.list_dags() == ["factory", "hotfix"]
    assert opener.last.get_header("Authorization") == "Bearer tok"
    assert opener.last.get_header("Accept") == "application/json"

    runs = af.list_runs("factory", limit=2)
    assert len(runs) == 1 and runs[0].run_id == RUN_ID and runs[0].state == "success"
    assert runs[0].start == datetime(2026, 9, 3, 8, 18, 24, 904858, tzinfo=UTC)
    assert runs[0].issues == ["demo/issue.md"] and not runs[0].active

    tasks = af.task_states("factory", RUN_ID)
    assert [(t.task_id, t.map_index, t.state) for t in tasks] == [
        ("fan_out", -1, "success"),
        ("job.build_and_test", 0, "running"),
    ]

    gates = af.pending_gates()
    assert len(gates) == 1
    g = gates[0]
    assert (g.dag_id, g.run_id, g.task_id, g.map_index) == (
        "factory",
        RUN_ID,
        "job.approve_intent",
        0,
    )
    assert g.options == ["Approve", "Reject"] and g.subject.startswith("[factory] approve intent")
    assert g.created_at == datetime(2026, 9, 3, 8, 19, 12, 857207, tzinfo=UTC)
    assert all(r.get_method() == "GET" and r.data is None for r in opener.requests)


def test_airflow_respond_patches_hitl_details_with_chosen_option() -> None:
    path = f"/api/v2/dags/factory/dagRuns/{RUN_SEG}/taskInstances/job.approve_plan/0/hitlDetails"
    opener = FakeOpener({("PATCH", path): {"chosen_options": ["Approve"]}})
    af = AirflowClient(AF, token="tok", opener=opener)

    assert af.respond(_gate(), approve=True) == {"chosen_options": ["Approve"]}
    assert opener.last.get_method() == "PATCH"
    assert opener.body() == {"chosen_options": ["Approve"], "params_input": {}}
    assert opener.last.get_header("Content-type") == "application/json"
    assert opener.last.get_header("Authorization") == "Bearer tok"

    af.respond(_gate(), approve=False)
    assert opener.body() == {"chosen_options": ["Reject"], "params_input": {}}

    with pytest.raises(ValueError, match="offers"):
        af.respond(_gate(options=["Yes", "No"]), approve=True)
    assert len(opener.requests) == 2  # the refused call never reached the wire


def test_airflow_trigger_and_stop_and_run_url() -> None:
    opener = FakeOpener(
        {
            ("POST", "/api/v2/dags/factory/dagRuns"): {
                "dag_run_id": "manual__x",
                "state": "queued",
            },
            ("POST", "/api/v2/dags/hotfix/dagRuns"): {"dag_run_id": "manual__h"},
            ("PATCH", f"/api/v2/dags/factory/dagRuns/{RUN_SEG}"): {"state": "failed"},
        }
    )
    af = AirflowClient(AF, token="tok", opener=opener)

    assert af.trigger("factory", ["42", " 43 ", ""]) == "manual__x"
    assert opener.last.get_method() == "POST"
    assert opener.last.full_url == f"{AF}/api/v2/dags/factory/dagRuns"
    assert opener.body() == {"logical_date": None, "conf": {"issues": ["42", "43"]}}
    with pytest.raises(ValueError, match="at least one issue"):
        af.trigger("factory", ["", "  "])

    # The blueprint the operator picked decides the dag id: same call, other line.
    assert af.trigger("hotfix", ["7"]) == "manual__h"
    assert opener.last.full_url == f"{AF}/api/v2/dags/hotfix/dagRuns"
    assert opener.body() == {"logical_date": None, "conf": {"issues": ["7"]}}

    assert af.stop_run("factory", RUN_ID) == {"state": "failed"}
    assert opener.last.get_method() == "PATCH" and opener.body() == {"state": "failed"}

    assert af.run_url("factory", RUN_ID) == f"{AF}/dags/factory/runs/{RUN_SEG}"


# ---------------------------------------------------------------- per-job rows

TI_PATH = f"/api/v2/dags/factory/dagRuns/{RUN_SEG}/taskInstances?limit=500"
XCOM_PATH = (
    f"/api/v2/dags/factory/dagRuns/{RUN_SEG}/taskInstances/fan_out"
    "/xcomEntries/return_value?map_index=-1"
)
TWO_JOBS = [  # verbatim shape of fan_out's return value (Blueprint.jobs)
    {"issue": "42", "repo": "o/r", "dir": "", "base_branch": "main", "job_idx": 0},
    {"issue": "demo/issue.md", "repo": "o/r", "dir": "", "base_branch": "main", "job_idx": 1},
]
TWO_JOB_TIS = {
    "task_instances": [
        {"task_id": "fan_out", "map_index": -1, "state": "success"},
        {"task_id": "job.setup", "map_index": 0, "state": "success"},
        {"task_id": "job.intent", "map_index": 0, "state": "success"},
        {"task_id": "job.approve_intent", "map_index": 0, "state": "deferred"},
        {"task_id": "job.setup", "map_index": 1, "state": "success"},
        {"task_id": "job.intent", "map_index": 1, "state": "failed"},
        {"task_id": "job.deliver", "map_index": 1, "state": None},
    ]
}


def test_job_rows_group_by_map_index_and_name_the_issue_from_fan_out_xcom() -> None:
    opener = FakeOpener(
        {
            ("GET", TI_PATH): TWO_JOB_TIS,
            # /api/v2 hands the XCom back as the serialized JSON string it holds in the DB.
            ("GET", XCOM_PATH): {"key": "return_value", "value": json.dumps(TWO_JOBS)},
        }
    )
    af = AirflowClient(AF, token="tok", opener=opener)

    rows = af.job_rows("factory", RUN_ID)
    assert [(r.map_index, r.issue, r.state) for r in rows] == [
        (0, "42", "running"),  # deferred at its gate
        (1, "demo/issue.md", "failed"),
    ]
    assert all(r.dag_id == "factory" and r.run_id == RUN_ID and r.mapped for r in rows)
    # ``fan_out`` itself is not a job: it is dropped once mapped tasks exist.
    assert [t.task_id for t in rows[0].tasks] == ["job.setup", "job.intent", "job.approve_intent"]
    assert [r.get_method() for r in opener.requests] == ["GET", "GET"]


def test_job_rows_without_xcom_fall_back_to_conf_then_to_dash() -> None:
    opener = FakeOpener({("GET", TI_PATH): TWO_JOB_TIS})  # no XCom entry -> 404
    af = AirflowClient(AF, token="tok", opener=opener)

    with pytest.raises(ControlError, match="HTTP 404"):
        af.fan_out_jobs("factory", RUN_ID)  # the client itself is honest about the miss

    plain = af.job_rows("factory", RUN_ID)  # job_rows degrades instead of failing
    assert [(r.map_index, r.issue) for r in plain] == [(0, "-"), (1, "-")]

    # One issue x N targets means every job carries that issue: exact without the XCom.
    one = af.job_rows("factory", RUN_ID, fallback_issues=["42"])
    assert [(r.map_index, r.issue) for r in one] == [(0, "42"), (1, "42")]
    # Two issues and the mapping is ambiguous without fan_out: say so rather than guess.
    many = af.job_rows("factory", RUN_ID, fallback_issues=["42", "43"])
    assert [r.issue for r in many] == ["-", "-"]


def test_job_rows_before_fan_out_and_with_junk_xcom() -> None:
    tis = {"task_instances": [{"task_id": "fan_out", "map_index": -1, "state": "running"}]}
    opener = FakeOpener({("GET", TI_PATH): tis, ("GET", XCOM_PATH): {"value": "not json at all"}})
    af = AirflowClient(AF, token="tok", opener=opener)

    rows = af.job_rows("factory", RUN_ID, fallback_issues=["42"])
    assert [(r.map_index, r.issue, r.state) for r in rows] == [(-1, "42", "running")]
    assert not rows[0].mapped  # a placeholder row, not "job -1"
    assert af.fan_out_jobs("factory", RUN_ID) == []  # unparseable XCom is no issue list

    already_list = AirflowClient(
        AF, opener=FakeOpener({("GET", XCOM_PATH): {"value": [*TWO_JOBS, "junk"]}})
    )
    assert already_list.fan_out_jobs("factory", RUN_ID) == TWO_JOBS  # non-dict entries dropped

    empty = AirflowClient(AF, opener=FakeOpener({("GET", TI_PATH): {"task_instances": []}}))
    assert empty.job_rows("factory", RUN_ID) == []


def test_job_state_and_group_jobs_are_pure() -> None:
    def ts(task_id: str, state: str | None, idx: int = 0) -> TaskState:
        return TaskState(task_id=task_id, map_index=idx, state=state)

    assert job_state([]) == "queued"
    assert job_state([ts("job.setup", None), ts("job.intent", None)]) == "queued"
    assert job_state([ts("job.setup", "success"), ts("job.intent", None)]) == "running"
    assert job_state([ts("job.setup", "success"), ts("job.intent", "running")]) == "running"
    assert job_state([ts("job.intent", "upstream_failed"), ts("job.spec", "running")]) == "failed"
    assert job_state([ts("job.setup", "success"), ts("job.deliver", "success")]) == "success"
    assert job_state([ts("job.spec", "skipped"), ts("job.plan", "skipped")]) == "skipped"
    # A rejected gate skips the work stages but deliver still publishes: that is a success.
    assert job_state([ts("job.spec", "skipped"), ts("job.deliver", "success")]) == "success"

    rows = group_jobs("factory", "r1", [ts("job.setup", "success", 2), ts("job.setup", None, 0)])
    assert [r.map_index for r in rows] == [0, 2]  # ascending, gaps kept as they come
    assert group_jobs("factory", "r1", []) == []

    run = Run("factory", "r1", "success", None, None, {"issues": ["42", "43"]})
    collapsed = collapsed_job(run)
    assert (collapsed.map_index, collapsed.issue, collapsed.state) == (-1, "42, 43", "success")
    assert collapsed_job(Run("f", "r", "queued", None, None)).issue == "-"


def test_a_job_parked_on_a_human_gate_counts_as_active() -> None:
    """``awaiting_input`` is how Airflow 3.3 parks a HITL task, so it must roll up as in-flight.

    Regression for a live-only bug: with that state missing from ``ACTIVE_TASK_STATES`` a job
    waiting on a gate had no active task, so both the state roll-up and the herd frontier fell
    back to its last *finished* task and the Runs tab named ``intent`` — never the gate the
    operator has to answer.
    """
    from swfactory.control import ACTIVE_TASK_STATES

    assert {"awaiting_input", "deferred"} <= ACTIVE_TASK_STATES
    parked = [
        TaskState("job.setup", 0, "success"),
        TaskState("job.intent", 0, "success"),
        TaskState("job.approve_intent", 0, "awaiting_input"),
        TaskState("job.deliver", 0, None),
    ]
    assert job_state(parked) == "running"


def test_airflow_username_password_mints_token_once() -> None:
    opener = FakeOpener(
        {
            ("POST", "/auth/token"): {"access_token": "minted"},
            ("GET", "/api/v2/dags?tags=swfactory&limit=100"): {"dags": []},
        }
    )
    af = AirflowClient(AF, username="admin", password="pw", opener=opener)
    assert af.list_dags() == []
    assert af.list_dags() == []
    methods = [(r.get_method(), r.full_url.removeprefix(AF)) for r in opener.requests]
    assert methods[0] == ("POST", "/auth/token")
    assert methods.count(("POST", "/auth/token")) == 1  # cached
    assert opener.body(0) == {"username": "admin", "password": "pw"}
    assert opener.requests[0].get_header("Authorization") is None
    assert opener.requests[1].get_header("Authorization") == "Bearer minted"

    bad = AirflowClient(
        AF, username="a", password="b", opener=FakeOpener({("POST", "/auth/token"): {}})
    )
    with pytest.raises(ControlError, match="no access_token"):
        bad.list_dags()


def test_airflow_errors_become_control_errors() -> None:
    af = AirflowClient(AF, token="t", opener=FakeOpener({}))
    with pytest.raises(ControlError, match="HTTP 404"):
        af.list_dags()
    down = FakeOpener(
        {("GET", "/api/v2/dags?tags=swfactory&limit=100"): urllib.error.URLError("refused")}
    )
    with pytest.raises(ControlError, match="refused"):
        AirflowClient(AF, token="t", opener=down).list_dags()
    junk = AirflowClient(AF, opener=lambda req, timeout: _RawResp(b"<html>"))
    with pytest.raises(ControlError, match="non-JSON"):
        junk.list_dags()
    assert junk.token() is None  # no credential at all: unauthenticated requests


class _RawResp:
    def __init__(self, raw: bytes) -> None:
        self.raw = raw

    def read(self) -> bytes:
        return self.raw

    def close(self) -> None:
        pass


# ---------------------------------------------------------------- GitHub


def test_github_prs_and_issues_use_gh_json_and_summarize_checks() -> None:
    prs = [
        {
            "number": 7,
            "title": "feat: percent_change",
            "url": "https://github.com/o/r/pull/7",
            "labels": [{"name": "factory"}, {"name": "hotfix"}],
            "state": "OPEN",
            "headRefName": "factory/42-abcd1234",
            "statusCheckRollup": [
                {"status": "COMPLETED", "conclusion": "SUCCESS"},
                {"status": "COMPLETED", "conclusion": "FAILURE"},
                {"status": "IN_PROGRESS", "conclusion": None},
                {"state": "SUCCESS"},
            ],
        }
    ]
    issues = [
        {"number": 42, "title": "Add percent_change", "url": "https://x/42", "labels": ["factory"]}
    ]
    runner = FakeRunner({"gh pr": json.dumps(prs), "gh issue": json.dumps(issues)})
    gh = GitHubClient("o/r", runner=runner)

    got = gh.prs()
    assert len(got) == 1 and got[0].number == 7 and got[0].labels == ["factory", "hotfix"]
    assert got[0].checks == "2 pass / 1 fail / 1 pending" and got[0].head == "factory/42-abcd1234"
    argv = runner.calls[0]
    assert argv[:4] == ["gh", "pr", "list", "--repo"] and argv[4] == "o/r"
    assert "--label" in argv and argv[argv.index("--label") + 1] == "factory"
    assert "--json" in argv and "statusCheckRollup" in argv[argv.index("--json") + 1]

    refs = gh.issues(label="factory:hotfix")
    assert [(i.number, i.labels) for i in refs] == [(42, ["factory"])]
    assert runner.calls[1][:3] == ["gh", "issue", "list"]
    assert runner.calls[1][runner.calls[1].index("--label") + 1] == "factory:hotfix"

    assert gh.open_pr_in_browser(7) == ["gh", "pr", "view", "7", "--repo", "o/r", "--web"]
    assert runner.calls[-1] == ["gh", "pr", "view", "7", "--repo", "o/r", "--web"]

    assert summarize_checks(None) == "none" and summarize_checks([]) == "none"
    assert (
        summarize_checks([{"status": "COMPLETED", "conclusion": "NEUTRAL"}])
        == "1 pass / 0 fail / 0 pending"
    )


def test_github_failures_are_control_errors() -> None:
    gh = GitHubClient("o/r", runner=FakeRunner({"gh pr": "FAIL"}))
    with pytest.raises(ControlError, match="rc=1"):
        gh.prs()
    missing = GitHubClient("o/r", runner=FakeRunner({"gh pr": FileNotFoundError("gh")}))
    with pytest.raises(ControlError, match="gh"):
        missing.prs()
    with pytest.raises(ControlError, match="non-JSON"):
        GitHubClient("o/r", runner=FakeRunner({"gh pr": "not json"})).prs()


# ---------------------------------------------------------------- islo


def test_islo_lists_own_sandboxes_without_all_flag() -> None:
    runner = FakeRunner({"islo ls": ISLO_LS})
    islo = IsloClient("me@x.io", runner=runner)
    own = islo.own_sandboxes()
    assert [s.name for s in own] == [
        "swf-42-abcd1234",
        "my-personal-box",
    ]  # deleted + foreign dropped
    assert [s.factory_named for s in own] == [True, False]
    assert runner.calls == [["islo", "ls", "--output", "json"]]
    assert all("--all" not in argv for argv in runner.calls)
    assert IsloClient("", runner=runner).own_sandboxes() == []


def test_islo_remove_only_own_factory_named_sandbox() -> None:
    runner = FakeRunner({"islo ls": ISLO_LS, "islo rm": ""})
    islo = IsloClient("me@x.io", runner=runner)

    assert islo.remove("swf-42-abcd1234") == ["islo", "rm", "swf-42-abcd1234", "--output", "plain"]
    assert runner.calls == [
        ["islo", "ls", "--output", "json"],  # fresh listing right before the rm
        ["islo", "rm", "swf-42-abcd1234", "--output", "plain"],
    ]

    runner.calls.clear()
    with pytest.raises(PermissionError, match="not created by"):
        islo.remove("swf-99-deadbeef")  # a teammate's, factory-named
    with pytest.raises(PermissionError, match="not a factory-named"):
        islo.remove("my-personal-box")  # own, but not factory-named: never listed, never rm'd
    with pytest.raises(PermissionError, match="not created by"):
        islo.remove("swf-77-0badf00d")  # deleted
    with pytest.raises(PermissionError, match="not created by"):
        islo.remove("swf-00-00000000")  # unknown
    assert all(argv[:2] != ["islo", "rm"] for argv in runner.calls)
    assert all("--all" not in argv for argv in runner.calls)

    with pytest.raises(PermissionError, match="no sandbox owner"):
        IsloClient("", runner=runner).remove("swf-42-abcd1234")


# ---------------------------------------------------------------- metrics


def test_metrics_source_summarizes_checkout(tmp_path: Path) -> None:
    d = tmp_path / "demo" / "target" / "docs" / "factory" / "42"
    d.mkdir(parents=True)
    (d / "metrics.json").write_text(
        json.dumps({"run_id": "a", "first_pass_ci": True, "cycle_s": 10, "iterations": 1}),
        encoding="utf-8",
    )
    src = MetricsSource(tmp_path)
    assert len(src.runs()) == 1
    summary = src.summary()
    assert summary["runs"] == 1 and summary["first_pass_rate"] == 1.0
    assert MetricsSource(tmp_path / "empty").summary()["runs"] == 0


# ---------------------------------------------------------------- collect


class _Airflow:
    def __init__(self, *, fail: set[str] = frozenset()) -> None:
        self.fail = fail
        self.calls: list[str] = []

    def _maybe(self, what: str) -> None:
        self.calls.append(what)
        if what in self.fail:
            raise ControlError(f"{what} down")

    def list_dags(self, tag: str = "swfactory") -> list[str]:
        self._maybe("list_dags")
        return ["factory"]

    def list_runs(self, dag_id: str, limit: int = 20) -> list[Run]:
        self._maybe("list_runs")
        return [
            Run(dag_id, "r-running", "running", None, None, {"issues": ["42"]}),
            Run(dag_id, "r-done", "success", None, None, {"issues": ["43"]}),
        ]

    def job_rows(
        self, dag_id: str, run_id: str, *, fallback_issues: Sequence[str] = ()
    ) -> list[JobRow]:
        self._maybe(f"job_rows:{run_id}")
        return [JobRow(dag_id, run_id, 0, next(iter(fallback_issues), "-"), "running")]

    def pending_gates(self) -> list[Gate]:
        self._maybe("pending_gates")
        return [_gate()]


class _GitHub:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail

    def prs(self, label: str = "factory", limit: int = 30) -> list:
        if self.fail:
            raise ControlError("gh: not logged in")
        return ["pr"]


class _Islo:
    def own_sandboxes(self) -> list[Sandbox]:
        return [Sandbox("swf-42-abcd1234", "running", "me", None)]


class _Metrics:
    def summary(self) -> dict:
        raise OSError("no checkout")


def test_collect_reads_every_source_and_hands_each_run_its_jobs() -> None:
    af = _Airflow()
    now = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
    snap = collect(af, _GitHub(), _Islo(), None, now=lambda: now)
    assert snap.collected_at == now and snap.errors == {}
    assert [r.run_id for r in snap.runs] == ["r-running", "r-done"]
    assert af.calls == [
        "list_dags",
        "list_runs",
        "job_rows:r-running",
        "job_rows:r-done",
        "pending_gates",
    ]
    # ``conf`` rides along as the issue fallback, so a row names its issue even without XCom.
    assert [(j.run_id, j.map_index, j.issue) for j in snap.jobs] == [
        ("r-running", 0, "42"),
        ("r-done", 0, "43"),
    ]
    assert len(snap.gates) == 1 and snap.prs == ["pr"]
    assert [s.name for s in snap.sandboxes] == ["swf-42-abcd1234"]
    assert snap.metrics == {}

    explicit = _Airflow()
    collect(explicit, None, None, None, dag_ids=["hotfix"], runs_per_dag=5)
    assert explicit.calls[:2] == ["list_runs", "job_rows:r-running"]  # no list_dags call


def test_collect_bounds_job_reads_and_collapses_the_rest() -> None:
    """``jobs_per_dag`` caps the poll: older finished runs collapse to one row, never vanish."""
    af = _Airflow()
    snap = collect(af, None, None, None, jobs_per_dag=0)
    assert af.calls == ["list_dags", "list_runs", "job_rows:r-running", "pending_gates"]
    done = next(r for r in snap.runs if r.run_id == "r-done")
    assert [(j.map_index, j.issue, j.state) for j in done.jobs] == [(-1, "43", "success")]


def test_collect_degrades_per_source() -> None:
    af = _Airflow(fail={"list_dags", "pending_gates"})
    snap = collect(af, _GitHub(fail=True), _Islo(), _Metrics())
    assert snap.runs == [] and snap.gates == [] and snap.prs == [] and snap.metrics == {}
    assert [s.name for s in snap.sandboxes] == ["swf-42-abcd1234"]  # the healthy source survives
    assert set(snap.errors) == {"airflow", "gates", "github", "metrics"}
    assert snap.errors["airflow"] == "list_dags down"
    assert snap.errors["github"] == "gh: not logged in"
    assert "no checkout" in snap.errors["metrics"]

    partial = collect(_Airflow(fail={"job_rows:r-running"}), None, None, None)
    assert [r.run_id for r in partial.runs] == ["r-running", "r-done"]  # runs kept
    assert set(partial.errors) == {"airflow:factory/r-running"}
    broken = next(r for r in partial.runs if r.run_id == "r-running")
    assert [(j.map_index, j.state) for j in broken.jobs] == [(-1, "running")]  # collapsed row
    assert len(partial.gates) == 1

    assert collect(None, None, None, None).errors == {}


# ---------------------------------------------------------------- data helpers


def test_run_issues_and_sandbox_naming() -> None:
    assert Run("f", "r", "queued", None, None, {"issues": [42, "43"], "issue": 42}).issues == [
        "42",
        "43",
    ]
    assert Run("f", "r", "queued", None, None, {"issue": 7}).issues == ["7"]
    assert Run("f", "r", "queued", None, None, {}).issues == []
    assert Run("f", "r", "queued", None, None).active
    assert Sandbox("swf-42-abcd1234", "running", "me", None).factory_named
    assert not Sandbox("SWF-42-ABCD1234", "running", "me", None).factory_named
    assert not Sandbox("prod-db", "running", "me", None).factory_named
