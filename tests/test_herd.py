"""herd: fake collector + fake actions drive ``HerdApp`` through ``App.run_test``, and the same
fakes drive the headless flags (``--once --json``, ``--approve-all``).

Hermetic: no network, no browser, no ``gh``/``islo``. The row/snapshot dataclasses are the real
``swfactory.control`` ones — they are plain data, so using them keeps the fixtures honest about
what the TUI is handed.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from textual.widgets import DataTable, Static
from typer.testing import CliRunner

from swfactory import herd
from swfactory.cli import app as cli_app
from swfactory.control import Gate, JobRow, PullRequest, Run, Sandbox, Snapshot, TaskState
from swfactory.herd import (
    Clients,
    Confirm,
    ControlActions,
    HerdApp,
    HerdInfo,
    Picker,
    Prompt,
    age,
    approve_all,
    drive_once,
    job_index,
    parse_issues,
    snapshot_data,
    stage_progress,
)

NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
TRIGGERED_RUN = "manual__2026-09-03T12:00:00+00:00"


# ---------------------------------------------------------------- fakes


def make_snapshot(*, errors: dict[str, str] | None = None, owner: str = "me") -> Snapshot:
    """Two runs: ``factory`` fanned out over two jobs (42 building, 43 at its plan gate) and a
    ``hotfix`` run whose single job failed in ``intent``."""
    job_0 = [
        TaskState("job.setup", 0, "success"),
        TaskState("job.intent", 0, "success"),
        TaskState("job.approve_intent", 0, "success"),
        TaskState("job.record_intent", 0, "success"),
        TaskState("job.spec", 0, "success"),
        TaskState("job.plan", 0, "success"),
        TaskState("job.approve_plan", 0, "success"),
        TaskState("job.record_plan", 0, "success"),
        TaskState("job.build_and_test", 0, "running"),
    ]
    job_1 = [
        TaskState("job.setup", 1, "success"),
        TaskState("job.intent", 1, "success"),
        TaskState("job.record_intent", 1, "success"),
        TaskState("job.spec", 1, "success"),
        TaskState("job.plan", 1, "success"),
        TaskState("job.approve_plan", 1, "deferred"),
    ]
    job_h = [TaskState("job.setup", 0, "success"), TaskState("job.intent", 0, "failed")]
    gate = Gate(
        dag_id="factory",
        run_id="manual__1",
        task_id="job.approve_plan",
        map_index=1,
        subject="[factory] approve plan.md for 43",
        body="Run manual__1 · job 1",
        created_at=NOW - timedelta(minutes=5),
        options=["Approve", "Reject"],
    )
    return Snapshot(
        collected_at=NOW,
        runs=[
            Run(
                "factory",
                "manual__1",
                "running",
                NOW - timedelta(hours=1),
                None,
                {"issues": ["42", "43"]},
                [
                    JobRow("factory", "manual__1", 0, "42", "running", job_0),
                    JobRow("factory", "manual__1", 1, "43", "running", job_1),
                ],
            ),
            Run(
                "hotfix",
                "manual__2",
                "failed",
                NOW - timedelta(days=2),
                NOW,
                {"issue": "9"},
                [JobRow("hotfix", "manual__2", 0, "9", "failed", job_h)],
            ),
        ],
        gates=[gate],
        prs=[
            PullRequest(
                7, "feat: percent_change", "https://x/pull/7", ["factory"], "open", "pass", "f/42"
            )
        ],
        sandboxes=[
            Sandbox("swf-42-abcd1234", "running", owner, NOW - timedelta(hours=3)),
            Sandbox("swf-99-deadbeef", "paused", "someone-else", NOW - timedelta(days=1)),
        ],
        metrics={"runs": 2, "first_pass_rate": 0.5, "findings_by_severity": {"blocker": 1}},
        errors=errors or {},
    )


class FakeCollector:
    def __init__(self, snapshot: Snapshot, *, fail: Exception | None = None) -> None:
        self.snapshot = snapshot
        self.fail = fail
        self.calls = 0

    def collect(self) -> Snapshot:
        self.calls += 1
        if self.fail is not None:
            raise self.fail
        return self.snapshot


class FakeActions:
    """Records every call; ``remove_sandbox`` refuses anything not created by ``owner``."""

    def __init__(self, owner: str = "me", *, fail_gate: str | None = None) -> None:
        self.owner = owner
        self.fail_gate = fail_gate  # run_id whose gate answer blows up (error-path tests)
        self.calls: list[tuple[str, Any]] = []

    def approve(self, gate: Gate) -> None:
        self._gate("approve", gate)

    def reject(self, gate: Gate) -> None:
        self._gate("reject", gate)

    def _gate(self, verb: str, gate: Gate) -> None:
        if self.fail_gate is not None and gate.run_id == self.fail_gate:
            raise RuntimeError("HTTP 409")
        self.calls.append((verb, gate))

    def trigger(self, dag_id: str, issues: Sequence[str]) -> str:
        self.calls.append(("trigger", (dag_id, list(issues))))
        return TRIGGERED_RUN

    def stop_run(self, run: Any) -> None:
        self.calls.append(("stop_run", run))

    def open_run(self, run: Any) -> None:
        self.calls.append(("open_run", run))

    def run_url(self, dag_id: str, run_id: str) -> str:
        return f"http://af:8080/dags/{dag_id}/runs/{run_id}"

    def open_pr(self, pr: PullRequest) -> None:
        self.calls.append(("open_pr", pr))

    def remove_sandbox(self, sandbox: Sandbox) -> None:
        if sandbox.created_by != self.owner:
            raise PermissionError(f"{sandbox.name} was created by {sandbox.created_by}")
        self.calls.append(("remove_sandbox", sandbox))


def build(
    snapshot: Snapshot | None = None, **kw: Any
) -> tuple[HerdApp, FakeCollector, FakeActions]:
    collector = FakeCollector(snapshot or make_snapshot(), fail=kw.pop("fail", None))
    actions = FakeActions()
    info = HerdInfo(
        repo="o/r",
        airflow_url="http://af:8080",
        owner="me",
        actor="admin",
        dag_ids=tuple(kw.pop("dag_ids", ("factory",))),
    )
    app = HerdApp(
        collector, actions, info=info, refresh_s=kw.pop("refresh_s", 0), clock=lambda: NOW
    )
    return app, collector, actions


def drive(body: Callable[[], Awaitable[None]]) -> None:
    """Run an async test body (no pytest-asyncio in the dev group)."""
    asyncio.run(body())


async def settle(app: HerdApp, pilot: Any) -> None:
    """Let worker threads finish and their ``call_from_thread`` results land."""
    await app.workers.wait_for_complete()
    await pilot.pause()
    await pilot.pause()


def rows(app: HerdApp, table_id: str) -> list[list[str]]:
    table = app.query_one(table_id, DataTable)
    return [[str(c) for c in table.get_row_at(i)] for i in range(table.row_count)]


# ---------------------------------------------------------------- pure helpers


def test_stage_progress_picks_active_then_last_done() -> None:
    snap = make_snapshot()
    assert stage_progress(snap.runs[0].jobs[0].tasks) == "build_and_test"
    assert stage_progress(snap.runs[0].jobs[1].tasks) == "approve_plan"
    assert stage_progress(snap.runs[1].jobs[0].tasks) == "intent:failed"
    assert stage_progress([]) == "-"
    two_jobs = [
        TaskState("job.setup", 0, "success"),
        TaskState("job.spec", 0, "running"),
        TaskState("job.setup", 1, "success"),
        TaskState("job.approve_intent", 1, "deferred"),
    ]
    assert stage_progress(two_jobs) == "spec, approve_intent"
    assert stage_progress([TaskState("fan_out", -1, "success")]) == "fan_out"
    assert stage_progress([TaskState("job.setup", 0, "none")]) == "pending"
    # Airflow 3.3 parks a HITL task in `awaiting_input`, not `deferred`: the frontier of a job
    # waiting on a gate is that gate, not the stage before it (see test_control.py).
    assert (
        stage_progress(
            [
                TaskState("job.setup", 0, "success"),
                TaskState("job.intent", 0, "success"),
                TaskState("job.approve_intent", 0, "awaiting_input"),
            ]
        )
        == "approve_intent"
    )


def test_parse_issues_and_age() -> None:
    assert parse_issues(" 42, 43,,demo/issue.md ") == ["42", "43", "demo/issue.md"]
    assert parse_issues("") == []
    assert (job_index(0), job_index(3), job_index(-1), job_index(None)) == ("0", "3", "-", "-")
    assert age(NOW - timedelta(seconds=30), NOW) == "30s"
    assert age(NOW - timedelta(minutes=5), NOW) == "5m"
    assert age((NOW - timedelta(hours=3)).isoformat(), NOW) == "3h"
    assert age("2026-09-01T12:00:00Z", NOW) == "2d"
    assert age(None, NOW) == "-"
    assert age("not a date", NOW) == "-"


# ---------------------------------------------------------------- the app


def test_tables_populate_and_header_renders() -> None:
    app, collector, _ = build()

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            assert collector.calls == 1
            gates = rows(app, "#gates-table")
            # The gate names its job: map_index 1 and the issue that job is working on.
            assert gates == [["factory", "manual__1", "approve_plan", "1", "43", gates[0][5], "5m"]]
            assert "approve plan.md" in gates[0][5]
            # One row per JOB, not per run: the two jobs of manual__1 are separate rows.
            assert rows(app, "#runs-table") == [
                ["factory", "manual__1", "0", "42", "build_and_test", "running"],
                ["factory", "manual__1", "1", "43", "approve_plan", "running"],
                ["hotfix", "manual__2", "0", "9", "intent:failed", "failed"],
            ]
            assert app.query_one("TabbedContent").get_tab("runs").label_text == "Runs (3)"
            assert rows(app, "#prs-table") == [
                ["7", "feat: percent_change", "factory", "pass", "open"]
            ]
            sandboxes = rows(app, "#sandboxes-table")
            assert [s[0] for s in sandboxes] == ["swf-42-abcd1234", "swf-99-deadbeef"]
            assert sandboxes[0][1] == "running"
            metrics = str(app.query_one("#metrics", Static).render())
            assert "first-pass rate" in metrics and "50%" in metrics
            status = str(app.query_one("#status", Static).render())
            assert "o/r" in status and "http://af:8080" in status and "me" in status
            assert "never" not in status
            footer = str(app.query_one("#actor").query_one(Static).render())
            assert "admin" in footer and "Airflow token" in footer

    drive(body)


def test_approve_asks_confirmation_then_calls_action() -> None:
    app, _, actions = build()

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            app.query_one("#gates-table", DataTable).focus()
            await pilot.pause()
            await pilot.press("a")
            await pilot.pause()
            assert isinstance(app.screen, Confirm)
            assert "approve approve_plan" in app.screen.question
            assert "admin" in app.screen.question
            await pilot.press("y")
            await settle(app, pilot)
            assert [c[0] for c in actions.calls] == ["approve"]
            assert actions.calls[0][1].run_id == "manual__1"
            assert any("ok" in e and "approve" in e for e in app.events)

    drive(body)


def test_reject_cancelled_records_nothing() -> None:
    app, _, actions = build()

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            app.query_one("#gates-table", DataTable).focus()
            await pilot.press("r")
            await pilot.pause()
            assert isinstance(app.screen, Confirm)
            assert "reject" in app.screen.question
            await pilot.press("n")
            await settle(app, pilot)
            assert actions.calls == []
            assert any("cancelled" in e for e in app.events)

    drive(body)


def test_remove_foreign_sandbox_surfaces_permission_error() -> None:
    app, _, actions = build()

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            table = app.query_one("#sandboxes-table", DataTable)
            table.focus()
            table.move_cursor(row=1)  # the teammate's sandbox
            await pilot.pause()
            await pilot.press("x")
            await pilot.pause()
            assert isinstance(app.screen, Confirm)
            assert "swf-99-deadbeef" in app.screen.question
            await pilot.press("enter")
            await settle(app, pilot)
            assert actions.calls == []
            assert any(n.startswith("warning:") and "someone-else" in n for n in app.notices)
            assert any("refused" in e for e in app.events)
            assert app.is_running

    drive(body)


def test_remove_own_sandbox_calls_action() -> None:
    app, _, actions = build()

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            app.query_one("#sandboxes-table", DataTable).focus()
            await pilot.press("x", "y")
            await settle(app, pilot)
            assert [c[0] for c in actions.calls] == ["remove_sandbox"]
            assert actions.calls[0][1].name == "swf-42-abcd1234"

    drive(body)


def test_refresh_key_collects_again_and_gates_r_rejects() -> None:
    app, collector, actions = build()

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            assert collector.calls == 1
            app.query_one("#runs-table", DataTable).focus()
            await pilot.press("r")
            await settle(app, pilot)
            assert collector.calls == 2
            await pilot.press("f5")
            await settle(app, pilot)
            assert collector.calls == 3
            app.query_one("#gates-table", DataTable).focus()
            await pilot.press("r")  # on the Gates tab `r` is reject, not refresh
            await pilot.pause()
            assert isinstance(app.screen, Confirm)
            await pilot.press("y")
            await settle(app, pilot)
            assert [c[0] for c in actions.calls] == ["reject"]

    drive(body)


def test_errors_render_badges_and_collector_failure_is_survivable() -> None:
    app, _, _ = build(make_snapshot(errors={"github": "gh: not logged in"}))

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            status = str(app.query_one("#status", Static).render())
            assert "github" in status
            assert any("gh: not logged in" in e for e in app.events)

    drive(body)

    failing, _, _ = build(fail=ConnectionError("airflow down"))

    async def body_fail() -> None:
        async with failing.run_test() as pilot:
            await settle(failing, pilot)
            assert failing.is_running
            assert any("airflow down" in n for n in failing.notices)
            assert "collect" in str(failing.query_one("#status", Static).render())

    drive(body_fail)


def test_run_keys_open_and_stop_the_selected_job_s_run() -> None:
    app, _, actions = build()

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            table = app.query_one("#runs-table", DataTable)
            table.focus()
            table.move_cursor(row=1)  # job 1 of manual__1
            await pilot.pause()
            await pilot.press("o")
            await settle(app, pilot)
            await pilot.press("s")
            await pilot.pause()
            assert isinstance(app.screen, Confirm)
            assert "the whole run" in app.screen.question  # stopping is per run, not per job
            await pilot.press("y")
            await settle(app, pilot)
            kinds = [c[0] for c in actions.calls]
            assert kinds[-2:] == ["open_run", "stop_run"]
            assert actions.calls[-1][1].run_id == "manual__1"
            assert actions.calls[-1][1].map_index == 1  # the job the cursor was on

    drive(body)


def test_trigger_one_blueprint_skips_the_picker_and_shows_an_optimistic_row() -> None:
    app, _, actions = build()  # dag_ids == ("factory",)

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            app.query_one("#runs-table", DataTable).focus()
            await pilot.press("t")
            await pilot.pause()
            assert isinstance(app.screen, Prompt)  # one line: nothing to pick
            assert "factory" in app.screen.question
            await pilot.press(*"42, 43", "enter")
            await settle(app, pilot)
            assert ("trigger", ("factory", ["42", "43"])) in actions.calls
            # The new run is on screen before the next poll, with the Airflow UI link logged.
            assert rows(app, "#runs-table")[0] == [
                "factory",
                TRIGGERED_RUN,
                "-",
                "42, 43",
                "-",
                "queued",
            ]
            url = f"http://af:8080/dags/factory/runs/{TRIGGERED_RUN}"
            assert any(url in e for e in app.events)
            assert any(url in n for n in app.notices)

    drive(body)


def test_trigger_picks_the_blueprint_then_the_issues() -> None:
    app, _, actions = build(dag_ids=("factory", "hotfix"))

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            app.query_one("#runs-table", DataTable).focus()
            await pilot.press("t")
            await pilot.pause()
            assert isinstance(app.screen, Picker)
            assert app.screen.choices == ["factory", "hotfix"]
            assert app.screen.initial == 0  # the cursor is on a factory row
            await pilot.press("2")  # hotfix
            await pilot.pause()
            assert isinstance(app.screen, Prompt)
            assert "hotfix" in app.screen.question
            await pilot.press(*"demo/issue.md", "enter")
            await settle(app, pilot)
            assert ("trigger", ("hotfix", ["demo/issue.md"])) in actions.calls
            assert rows(app, "#runs-table")[0][:2] == ["hotfix", TRIGGERED_RUN]

    drive(body)


def test_trigger_picker_and_prompt_can_both_be_cancelled() -> None:
    app, _, actions = build(dag_ids=("factory", "hotfix"))

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            app.query_one("#runs-table", DataTable).focus()
            await pilot.press("t")
            await pilot.pause()
            await pilot.press("escape")
            await settle(app, pilot)
            assert actions.calls == []
            assert any("no blueprint" in e for e in app.events)
            await pilot.press("t")
            await pilot.pause()
            await pilot.press("1", "enter")  # picked factory, then submitted nothing
            await settle(app, pilot)
            assert actions.calls == []
            assert any("no issues" in e for e in app.events)
            assert len(rows(app, "#runs-table")) == 3  # no optimistic row for a cancelled trigger

    drive(body)


def test_trigger_preselects_the_blueprint_under_the_cursor() -> None:
    app, _, _ = build(dag_ids=("factory", "hotfix"))

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            table = app.query_one("#runs-table", DataTable)
            table.focus()
            table.move_cursor(row=2)  # the hotfix run
            await pilot.pause()
            assert app.cursor_dag_id() == "hotfix"
            await pilot.press("t")
            await pilot.pause()
            assert isinstance(app.screen, Picker) and app.screen.initial == 1
            await pilot.press("enter")  # take the highlighted one
            await pilot.pause()
            assert isinstance(app.screen, Prompt) and "hotfix" in app.screen.question

    drive(body)


def test_open_pr_and_help() -> None:
    app, _, actions = build()

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            app.query_one("#prs-table", DataTable).focus()
            await pilot.press("o")
            await settle(app, pilot)
            assert actions.calls == [("open_pr", actions.calls[0][1])]
            assert actions.calls[0][1].number == 7
            await pilot.press("question_mark")
            await pilot.pause()
            assert any("approve" in n and "refresh" in n for n in app.notices)

    drive(body)


def test_action_exception_is_notified_not_fatal() -> None:
    app, _, actions = build()
    actions.open_pr = lambda pr: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            app.query_one("#prs-table", DataTable).focus()
            await pilot.press("o")
            await settle(app, pilot)
            assert app.is_running
            assert any(n.startswith("error:") and "boom" in n for n in app.notices)

    drive(body)


# ---------------------------------------------------------------- control adapters (no clients)


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def __getattr__(self, name: str) -> Any:
        def _call(*a: Any, **kw: Any) -> Any:
            self.calls.append((name, a, kw))
            if name == "run_url":
                return f"http://af/dags/{a[0]}/runs/{a[1]}"
            return "manual__new" if name == "trigger" else None

        return _call


def test_control_actions_delegate_to_clients() -> None:
    airflow, github, islo = _Recorder(), _Recorder(), _Recorder()
    opened: list[str] = []
    acts = ControlActions(airflow, github, islo, opener=opened.append)
    snap = make_snapshot()
    acts.approve(snap.gates[0])
    acts.reject(snap.gates[0])
    assert acts.trigger("factory", ("42",)) == "manual__new"
    acts.stop_run(snap.runs[0])
    acts.open_run(snap.jobs[1])  # a job addresses its run just as well
    acts.open_pr(snap.prs[0])
    acts.remove_sandbox(snap.sandboxes[0])
    assert [c[0] for c in airflow.calls] == [
        "respond",
        "respond",
        "trigger",
        "stop_run",
        "run_url",
    ]
    assert airflow.calls[0][2] == {"approve": True} and airflow.calls[1][2] == {"approve": False}
    assert airflow.calls[2][1] == ("factory", ["42"])
    assert airflow.calls[3][1] == ("factory", "manual__1")
    assert opened == ["http://af/dags/factory/runs/manual__1"]
    assert acts.run_url("hotfix", "r9") == "http://af/dags/hotfix/runs/r9"
    assert github.calls == [("open_pr_in_browser", (7,), {})]
    assert islo.calls == [("remove", ("swf-42-abcd1234",), {})]


def test_make_app_and_make_clients_share_one_stack() -> None:
    clients = herd.make_clients(
        airflow_url="http://af:8080", repo="o/r", owner="me", token="t", dag_ids=["factory"]
    )
    assert clients.dag_ids == ("factory",)
    # One AirflowClient behind both seams: a headless answer and a keystroke are the same call.
    assert clients.collector.airflow is clients.actions.airflow
    assert clients.collector.islo is clients.actions.islo

    app = herd.make_app(
        airflow_url="http://af:8080",
        repo="o/r",
        owner="me",
        token="t",
        metrics_root=".",
        refresh_s=9,
        dag_ids=["factory"],
    )
    assert isinstance(app, HerdApp)
    assert app.refresh_s == 9
    assert app.info.actor == "Airflow token owner"
    assert app.info.dag_ids == ("factory",)


def test_blueprint_dag_ids_reads_the_blueprints_dir() -> None:
    """``t``'s choices are the real ``blueprints/*.toml`` names (= the DAG ids)."""
    ids = herd.blueprint_dag_ids()
    assert "factory" in ids and "hotfix" in ids


# ---------------------------------------------------------------- headless drive mode


def test_snapshot_data_carries_runs_with_their_jobs() -> None:
    data = snapshot_data(make_snapshot(errors={"github": "gh: not logged in"}))
    assert set(data) == {
        "collected_at",
        "runs",
        "gates",
        "prs",
        "sandboxes",
        "metrics",
        "errors",
    }
    assert data["collected_at"] == NOW.isoformat()
    assert [r["run_id"] for r in data["runs"]] == ["manual__1", "manual__2"]
    assert data["runs"][0]["issues"] == ["42", "43"]
    assert data["runs"][0]["jobs"] == [
        {"map_index": 0, "issue": "42", "stage": "build_and_test", "state": "running"},
        {"map_index": 1, "issue": "43", "stage": "approve_plan", "state": "running"},
    ]
    assert data["gates"][0]["map_index"] == 1 and data["gates"][0]["gate"] == "approve_plan"
    assert data["prs"][0]["number"] == 7 and data["sandboxes"][0]["name"] == "swf-42-abcd1234"
    assert data["metrics"]["runs"] == 2
    assert data["errors"] == {"github": "gh: not logged in"}
    json.dumps(data)  # the whole snapshot is JSON-safe


def test_drive_once_prints_one_json_snapshot() -> None:
    collector = FakeCollector(make_snapshot())
    printed: list[str] = []
    assert drive_once(collector, out=printed.append) == 0
    assert collector.calls == 1 and len(printed) == 1  # ONE snapshot, then exit
    assert json.loads(printed[0])["runs"][0]["jobs"][1]["issue"] == "43"

    text: list[str] = []
    assert drive_once(FakeCollector(make_snapshot()), as_json=False, out=text.append) == 0
    assert "job 1 43 approve_plan running" in text[0]
    assert "gate factory/manual__1[1] approve_plan" in text[0]


def test_approve_all_answers_every_pending_gate_of_the_configured_blueprints() -> None:
    snapshot = make_snapshot()
    snapshot.gates = [
        snapshot.gates[0],
        Gate("hotfix", "manual__2", "job.approve_intent", 0, "s", "", NOW, ["Approve", "Reject"]),
        Gate("other", "manual__3", "job.approve_plan", 0, "s", "", NOW, ["Approve", "Reject"]),
    ]
    actions = FakeActions()
    printed: list[str] = []
    code = approve_all(
        FakeCollector(snapshot),
        actions,
        dag_ids=("factory", "hotfix"),
        actor="admin",
        out=printed.append,
    )
    assert code == 0
    assert [(c[0], c[1].dag_id, c[1].map_index) for c in actions.calls] == [
        ("approve", "factory", 1),
        ("approve", "hotfix", 0),
    ]  # one respond() per pending gate, on the right map_index
    assert printed[0] == "approve factory/manual__1[1] approve_plan as admin"
    assert printed[1] == "approve hotfix/manual__2[0] approve_intent as admin"
    assert "2/2 pending gates" in printed[-1] and "1 outside" in printed[-1]


def test_approve_all_honours_reject_and_reports_failures() -> None:
    actions = FakeActions()
    printed: list[str] = []
    assert (
        approve_all(
            FakeCollector(make_snapshot()),
            actions,
            dag_ids=("factory",),
            reject=True,
            out=printed.append,
        )
        == 0
    )
    assert [c[0] for c in actions.calls] == ["reject"]
    assert printed[0].startswith("reject factory/manual__1[1] approve_plan as ")

    failing = FakeActions(fail_gate="manual__1")
    lines: list[str] = []
    assert approve_all(FakeCollector(make_snapshot()), failing, out=lines.append) == 1
    assert failing.calls == [] and "HTTP 409" in lines[0]

    blind = FakeCollector(make_snapshot(errors={"gates": "airflow down"}))
    out: list[str] = []
    assert approve_all(blind, FakeActions(), out=out.append) == 1  # cannot claim it answered all
    assert "error gates: airflow down" in out[0]


def _fake_clients(monkeypatch: pytest.MonkeyPatch, **kw: Any) -> tuple[FakeCollector, FakeActions]:
    """Point ``herd.main`` at fakes: the CLI wiring under test, no clients built."""
    collector = FakeCollector(kw.pop("snapshot", None) or make_snapshot())
    actions = FakeActions()
    info = HerdInfo(repo="o/r", airflow_url="http://af:8080", actor="admin", dag_ids=("factory",))
    monkeypatch.setattr(
        herd,
        "make_clients",
        lambda **_: Clients(collector, actions, info),  # type: ignore[arg-type]
    )
    return collector, actions


def test_cli_herd_once_json_prints_a_snapshot_and_exits_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector, actions = _fake_clients(monkeypatch)
    result = CliRunner().invoke(cli_app, ["herd", "--repo", "o/r", "--once", "--json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert set(data) == {
        "collected_at",
        "runs",
        "gates",
        "prs",
        "sandboxes",
        "metrics",
        "errors",
    }
    assert [j["map_index"] for j in data["runs"][0]["jobs"]] == [0, 1]
    assert collector.calls == 1 and actions.calls == []  # a read, exactly once


def test_cli_herd_approve_all_answers_the_gates(monkeypatch: pytest.MonkeyPatch) -> None:
    _, actions = _fake_clients(monkeypatch)
    result = CliRunner().invoke(cli_app, ["herd", "--repo", "o/r", "--approve-all"])
    assert result.exit_code == 0, result.output
    assert [c[0] for c in actions.calls] == ["approve"]
    assert "approve factory/manual__1[1] approve_plan as admin" in result.stdout

    _, rejecting = _fake_clients(monkeypatch)
    result = CliRunner().invoke(cli_app, ["herd", "--repo", "o/r", "--approve-all", "--reject"])
    assert result.exit_code == 0, result.output
    assert [c[0] for c in rejecting.calls] == ["reject"]
