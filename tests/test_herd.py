"""herd TUI: fake collector + fake actions drive ``HerdApp`` through ``App.run_test``.

Hermetic: no network, no browser, no ``gh``/``islo``. The dataclasses come from
``swfactory.control`` when it is importable; until then structurally identical stand-ins keep
the tests runnable (``herd`` only ever reads attributes off them).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from textual.widgets import DataTable, Static

from swfactory import herd
from swfactory.herd import (
    Confirm,
    ControlActions,
    HerdApp,
    HerdInfo,
    Prompt,
    age,
    parse_issues,
    stage_progress,
)

try:  # control.py lands concurrently; fall back to stand-ins with the agreed field names
    from swfactory.control import Gate, PullRequest, Run, Sandbox, Snapshot, TaskState
except ImportError:  # pragma: no cover - only before control.py exists

    @dataclass
    class TaskState:  # type: ignore[no-redef]
        task_id: str
        map_index: int
        state: str

    @dataclass
    class Run:  # type: ignore[no-redef]
        dag_id: str
        run_id: str
        state: str
        start: Any
        end: Any
        conf: dict
        tasks: list[TaskState] = field(default_factory=list)

    @dataclass
    class Gate:  # type: ignore[no-redef]
        dag_id: str
        run_id: str
        task_id: str
        map_index: int
        subject: str
        body: str
        created_at: Any
        options: list[str] = field(default_factory=list)

    @dataclass
    class PullRequest:  # type: ignore[no-redef]
        number: int
        title: str
        url: str
        labels: list[str]
        state: str
        checks: Any
        head: str

    @dataclass
    class Sandbox:  # type: ignore[no-redef]
        name: str
        status: str
        created_by: str | None
        created_at: Any

    @dataclass
    class Snapshot:  # type: ignore[no-redef]
        collected_at: Any
        runs: list[Run]
        gates: list[Gate]
        prs: list[PullRequest]
        sandboxes: list[Sandbox]
        metrics: Any
        errors: dict[str, str] = field(default_factory=dict)


NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)


# ---------------------------------------------------------------- fakes


def make_snapshot(*, errors: dict[str, str] | None = None, owner: str = "me") -> Snapshot:
    tasks_a = [
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
    tasks_b = [TaskState("job.setup", 0, "success"), TaskState("job.intent", 0, "failed")]
    gate = Gate(
        dag_id="factory",
        run_id="manual__1",
        task_id="job.approve_plan",
        map_index=0,
        subject="[factory] approve plan.md for 42",
        body="Run manual__1 · job 0",
        created_at=NOW - timedelta(minutes=5),
        options=["Approve", "Reject"],
    )
    return Snapshot(
        collected_at=NOW,
        runs=[
            Run("factory", "manual__1", "running", NOW - timedelta(hours=1), None, {}, tasks_a),
            Run("hotfix", "manual__2", "failed", NOW - timedelta(days=2), NOW, {}, tasks_b),
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

    def __init__(self, owner: str = "me") -> None:
        self.owner = owner
        self.calls: list[tuple[str, Any]] = []

    def approve(self, gate: Gate) -> None:
        self.calls.append(("approve", gate))

    def reject(self, gate: Gate) -> None:
        self.calls.append(("reject", gate))

    def trigger(self, dag_id: str, issues: list[str]) -> None:
        self.calls.append(("trigger", (dag_id, list(issues))))

    def stop_run(self, run: Run) -> None:
        self.calls.append(("stop_run", run))

    def open_run(self, run: Run) -> None:
        self.calls.append(("open_run", run))

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
    info = HerdInfo(repo="o/r", airflow_url="http://af:8080", owner="me", actor="admin")
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
    assert stage_progress(snap.runs[0].tasks) == "build_and_test"
    assert stage_progress(snap.runs[1].tasks) == "intent:failed"
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


def test_parse_issues_and_age() -> None:
    assert parse_issues(" 42, 43,,demo/issue.md ") == ["42", "43", "demo/issue.md"]
    assert parse_issues("") == []
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
            assert gates == [["factory", "manual__1", "approve_plan", "0", gates[0][4], "5m"]]
            assert "approve plan.md" in gates[0][4]
            runs = rows(app, "#runs-table")
            assert [r[:3] for r in runs] == [
                ["factory", "manual__1", "running"],
                ["hotfix", "manual__2", "failed"],
            ]
            assert [r[4] for r in runs] == ["build_and_test", "intent:failed"]
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


def test_trigger_prompts_for_issues_and_run_keys() -> None:
    app, _, actions = build()

    async def body() -> None:
        async with app.run_test() as pilot:
            await settle(app, pilot)
            app.query_one("#runs-table", DataTable).focus()
            await pilot.press("t")
            await pilot.pause()
            assert isinstance(app.screen, Prompt)
            assert "factory" in app.screen.question
            await pilot.press(*"42, 43", "enter")
            await settle(app, pilot)
            assert ("trigger", ("factory", ["42", "43"])) in actions.calls
            app.query_one("#runs-table", DataTable).focus()
            await pilot.press("o")
            await settle(app, pilot)
            await pilot.press("s")
            await pilot.pause()
            assert isinstance(app.screen, Confirm)
            await pilot.press("y")
            await settle(app, pilot)
            kinds = [c[0] for c in actions.calls]
            assert kinds[-2:] == ["open_run", "stop_run"]
            assert actions.calls[-1][1].run_id == "manual__1"

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
            return "http://af/run" if name == "open_url" else None

        return _call


def test_control_actions_delegate_to_clients() -> None:
    airflow, github, islo = _Recorder(), _Recorder(), _Recorder()
    opened: list[str] = []
    acts = ControlActions(airflow, github, islo, opener=opened.append)
    snap = make_snapshot()
    acts.approve(snap.gates[0])
    acts.reject(snap.gates[0])
    acts.trigger("factory", ("42",))
    acts.stop_run(snap.runs[0])
    acts.open_run(snap.runs[0])
    acts.open_pr(snap.prs[0])
    acts.remove_sandbox(snap.sandboxes[0])
    assert [c[0] for c in airflow.calls] == [
        "respond",
        "respond",
        "trigger",
        "stop_run",
        "open_url",
    ]
    assert airflow.calls[0][2] == {"approve": True} and airflow.calls[1][2] == {"approve": False}
    assert airflow.calls[2][1] == ("factory", ["42"])
    assert airflow.calls[3][1] == ("factory", "manual__1")
    assert opened == ["http://af/run"]
    assert github.calls == [("open_pr_in_browser", (7,), {})]
    assert islo.calls == [("remove", ("swf-42-abcd1234",), {})]


def test_make_app_needs_control(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("swfactory.control")
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
