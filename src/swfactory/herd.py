"""herd: the factory's driver's seat — a Textual TUI (and a headless twin) over
``swfactory.control``.

One screen, five tabs (Gates, Runs, PRs, Sandboxes, Metrics), one log pane. The app never talks
to Airflow, GitHub or islo itself: it reads a :class:`Snapshot` from a :class:`Collector` and
mutates through an :class:`Actions` object, both injected, so tests drive it with fakes while
:func:`make_clients` wires the real ones. Collection runs in a worker thread so the UI never
blocks; every mutation asks for a one-line confirmation and lands in the log pane.

The **job** — one ``(run_id, map_index)`` of a mapped ``job`` task group — is the unit the
operator sees and acts on: the Runs tab has a row per job (dag, run, job index, issue, stage,
state) and the Gates tab names the job a gate belongs to. ``t`` picks the blueprint (DAG) to
trigger from ``blueprints/*.toml``, asks for issues and posts the run.

:func:`main` is the whole ``swfactory herd`` command, TUI and headless alike, over one set of
clients (:func:`make_clients`): ``--once [--json]`` prints one snapshot and exits, and
``--approve-all [--reject]`` answers every pending gate of the configured blueprints. There is
no second client stack behind the headless flags — that is the point of them: what CI proves is
exactly what the TUI does.

Key map (also ``?`` inside the app):

* global: ``r`` refresh (``f5`` everywhere, including the Gates tab), ``q`` quit, ``?`` help
* Gates: ``a`` approve, ``r`` reject — the actor is the user the Airflow token belongs to
* Runs: ``t`` trigger (pick a blueprint, then issue ids), ``s`` stop the run, ``o`` open it
* PRs: ``o`` open in browser
* Sandboxes: ``x`` remove (own sandboxes only; a foreign one is refused, never crashes)
"""

from __future__ import annotations

import contextlib
import json
import webbrowser
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from rich.markup import escape
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.notifications import SeverityLevel
from textual.screen import ModalScreen
from textual.widgets import (
    DataTable,
    Footer,
    Input,
    Label,
    OptionList,
    RichLog,
    Static,
    TabbedContent,
    TabPane,
)

# The task-state vocabulary is Airflow's, so it is defined once, in the module that speaks to
# Airflow; herd only renders it.
from swfactory.control import ACTIVE_TASK_STATES as _ACTIVE
from swfactory.control import FINAL_TASK_STATES as _DONE

if TYPE_CHECKING:
    from swfactory.control import Gate, JobRow, PullRequest, Sandbox, Snapshot

# Canonical walk of a job's tasks inside the mapped ``job`` task group (``dags/blueprints.py``).
TASK_ORDER: tuple[str, ...] = (
    "setup",
    "intent",
    "approve_intent",
    "record_intent",
    "spec",
    "plan",
    "approve_plan",
    "record_plan",
    "build_and_test",
    "review",
    "deliver",
    "metrics",
    "teardown",
)
DEFAULT_DAG_ID = "factory"
HELP = """\
[b]herd[/b] — global: [b]r[/b]/[b]f5[/b] refresh · [b]q[/b] quit · [b]?[/b] this help
[b]Gates[/b]  a approve · r reject (f5 refreshes here)
[b]Runs[/b]   t trigger (pick blueprint, then issue ids) · s stop run · o open in browser
[b]PRs[/b]    o open in browser
[b]Sandboxes[/b]  x remove (own only)
Every mutation asks for confirmation and is recorded in the log pane."""


# ---------------------------------------------------------------- seams


class Collector(Protocol):
    """Anything that produces a :class:`swfactory.control.Snapshot` (blocking, thread-safe)."""

    def collect(self) -> Snapshot: ...


class RunLike(Protocol):
    """A run *or* one of its jobs: both address the DAG run the operator stops or opens."""

    dag_id: str
    run_id: str


class Actions(Protocol):
    """Every mutation the TUI can perform. Implementations may raise; the app reports and
    carries on. ``remove_sandbox`` raises ``PermissionError`` for a sandbox the user does not own.
    """

    def approve(self, gate: Gate) -> None: ...

    def reject(self, gate: Gate) -> None: ...

    def trigger(self, dag_id: str, issues: Sequence[str]) -> str | None: ...

    def stop_run(self, run: RunLike) -> None: ...

    def open_run(self, run: RunLike) -> None: ...

    def run_url(self, dag_id: str, run_id: str) -> str: ...

    def open_pr(self, pr: PullRequest) -> None: ...

    def remove_sandbox(self, sandbox: Sandbox) -> None: ...


@dataclass(frozen=True)
class HerdInfo:
    """What the header and footer say about where the app points and who acts."""

    repo: str = "-"
    airflow_url: str = "-"
    owner: str | None = None
    actor: str = "Airflow token owner"
    dag_ids: tuple[str, ...] = (DEFAULT_DAG_ID,)


# ---------------------------------------------------------------- pure helpers (unit-tested)


def stage_progress(tasks: Iterable[Any]) -> str:
    """Summarise a run's task states as its current stage, e.g. ``build_and_test``.

    Per job (map index) the frontier is the first active task in :data:`TASK_ORDER`, else the
    last finished one (annotated with its state when it is not ``success``). Jobs sharing a
    frontier are collapsed; several are joined with ``, ``.
    """
    by_job: dict[int, dict[str, str]] = {}
    for t in tasks:
        stage = str(getattr(t, "task_id", "")).rsplit(".", 1)[-1]
        idx = getattr(t, "map_index", -1)
        by_job.setdefault(-1 if idx is None else int(idx), {})[stage] = str(
            getattr(t, "state", "") or "none"
        )
    frontiers: list[str] = []
    for states in by_job.values():
        ordered = [s for s in TASK_ORDER if s in states] + sorted(set(states) - set(TASK_ORDER))
        active = next((s for s in ordered if states[s] in _ACTIVE), None)
        if active is not None:
            label = active
        else:
            done = [s for s in ordered if states[s] in _DONE]
            if not done:
                label = "pending"
            else:
                last = done[-1]
                label = last if states[last] == "success" else f"{last}:{states[last]}"
        if label not in frontiers:
            frontiers.append(label)
    return ", ".join(frontiers) or "-"


def parse_issues(text: str) -> list[str]:
    """``"42, 43,,7"`` -> ``["42", "43", "7"]``."""
    return [part.strip() for part in text.split(",") if part.strip()]


def job_index(map_index: Any) -> str:
    """``map_index`` as a column: ``-`` when the run has not fanned out into jobs yet."""
    try:
        idx = int(map_index)
    except (TypeError, ValueError):
        return "-"
    return "-" if idx < 0 else str(idx)


def age(value: Any, now: datetime | None = None) -> str:
    """Compact age (``3m``, ``2h``, ``5d``) of an ISO string or datetime; ``-`` when unknown."""
    then = _as_datetime(value)
    if then is None:
        return "-"
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    seconds = max(0, int((now - then).total_seconds()))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86_400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86_400}d"


def when(value: Any) -> str:
    """``HH:MM`` (UTC, with the date when not today) of an ISO string or datetime; ``-`` if none."""
    dt = _as_datetime(value)
    if dt is None:
        return "-"
    dt = dt.astimezone(UTC)
    fmt = "%H:%M" if dt.date() == datetime.now(UTC).date() else "%m-%d %H:%M"
    return dt.strftime(fmt)


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _join(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        return value or "-"
    if isinstance(value, dict):
        return ", ".join(f"{k}={v}" for k, v in value.items()) or "-"
    if isinstance(value, (list, tuple, set, frozenset)):
        return ", ".join(str(v) for v in value) or "-"
    return str(value)


def _metrics_text(metrics: Any) -> str:
    if isinstance(metrics, dict):
        from swfactory.metrics import table

        return table(metrics) if metrics else "(no metrics yet)"
    return str(metrics) if metrics else "(no metrics yet)"


# ---------------------------------------------------------------- modal prompts


class Confirm(ModalScreen[bool]):
    """One-line yes/no question: ``y``/``enter`` confirms, ``n``/``escape`` cancels."""

    DEFAULT_CSS = """
    Confirm { align: center middle; }
    Confirm > Vertical { width: auto; max-width: 90%; height: auto; padding: 1 2;
        border: thick $accent; background: $surface; }
    """
    BINDINGS = [
        Binding("y", "answer(True)", "yes"),
        Binding("enter", "answer(True)", "yes", show=False),
        Binding("n", "answer(False)", "no"),
        Binding("escape", "answer(False)", "no", show=False),
    ]

    def __init__(self, question: str) -> None:
        super().__init__()
        self.question = question

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label(escape(self.question))
            yield Label("[b]y[/b] confirm · [b]n[/b] cancel")

    def action_answer(self, value: bool) -> None:
        self.dismiss(value)


class Prompt(ModalScreen[str | None]):
    """One-line text prompt: ``enter`` submits, ``escape`` cancels (``None``)."""

    DEFAULT_CSS = """
    Prompt { align: center middle; }
    Prompt > Vertical { width: 70; height: auto; padding: 1 2;
        border: thick $accent; background: $surface; }
    """
    BINDINGS = [Binding("escape", "cancel", "cancel", show=False)]

    def __init__(self, question: str, placeholder: str = "") -> None:
        super().__init__()
        self.question = question
        self.placeholder = placeholder

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label(escape(self.question))
            yield Input(placeholder=self.placeholder, id="prompt-input")

    def on_mount(self) -> None:
        self.query_one(Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value)

    def action_cancel(self) -> None:
        self.dismiss(None)


class Picker(ModalScreen[str | None]):
    """Pick one of a short list (the blueprints): ``1``-``9``, or arrows + ``enter``.

    Digits are bound as well as the arrows because the list is the set of ``blueprints/*.toml``
    — a handful of lines — and one keystroke should be enough to start a run.
    """

    DEFAULT_CSS = """
    Picker { align: center middle; }
    Picker > Vertical { width: 60; height: auto; padding: 1 2;
        border: thick $accent; background: $surface; }
    Picker OptionList { height: auto; max-height: 12; }
    """
    BINDINGS = [
        Binding("escape", "cancel", "cancel", show=False),
        *[Binding(str(n), f"choose({n - 1})", f"{n}", show=False) for n in range(1, 10)],
    ]

    def __init__(self, question: str, choices: Sequence[str], initial: int = 0) -> None:
        super().__init__()
        self.question = question
        self.choices = [str(c) for c in choices]
        self.initial = initial if 0 <= initial < len(self.choices) else 0

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label(escape(self.question))
            yield OptionList(
                *[f"{i + 1}. {escape(c)}" for i, c in enumerate(self.choices)], id="picker-list"
            )
            yield Label("[b]1[/b]-[b]9[/b] or [b]enter[/b] · [b]escape[/b] cancel")

    def on_mount(self) -> None:
        options = self.query_one(OptionList)
        options.highlighted = self.initial
        options.focus()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        self.action_choose(event.option_index)

    def action_choose(self, index: int) -> None:
        if 0 <= index < len(self.choices):
            self.dismiss(self.choices[index])

    def action_cancel(self) -> None:
        self.dismiss(None)


# ---------------------------------------------------------------- tables (tab-local key maps)


class GatesTable(DataTable):
    BINDINGS = [
        Binding("a", "app.approve_gate", "approve"),
        Binding("r", "app.reject_gate", "reject"),
        Binding("f5", "app.refresh", "refresh", show=False),
    ]


class RunsTable(DataTable):
    BINDINGS = [
        Binding("t", "app.trigger_run", "trigger"),
        Binding("s", "app.stop_run", "stop"),
        Binding("o", "app.open_run", "open"),
    ]


class PRsTable(DataTable):
    BINDINGS = [Binding("o", "app.open_pr", "open")]


class SandboxesTable(DataTable):
    BINDINGS = [Binding("x", "app.remove_sandbox", "remove")]


# ---------------------------------------------------------------- the app


class HerdApp(App[None]):
    """Management TUI. ``collector`` and ``actions`` are the only way in or out."""

    TITLE = "swfactory herd"
    CSS = """
    #status { height: auto; padding: 0 1; background: $primary-background; }
    #actor { height: auto; padding: 0 1; color: $text-muted; }
    TabbedContent { height: 1fr; }
    #log { height: 7; border-top: solid $accent; }
    #metrics { padding: 1 2; }
    """
    BINDINGS = [
        Binding("r", "refresh", "refresh"),
        Binding("f5", "refresh", "refresh", show=False),
        Binding("q", "quit", "quit"),
        Binding("question_mark", "help", "help", key_display="?"),
    ]

    def __init__(
        self,
        collector: Collector,
        actions: Actions,
        *,
        info: HerdInfo | None = None,
        refresh_s: float = 5.0,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        super().__init__()
        self.collector = collector
        self.actions = actions
        self.info = info or HerdInfo()
        self.refresh_s = refresh_s
        self.clock = clock  # injectable "now" (ages in the tables, log stamps)
        self.snapshot: Snapshot | None = None
        self.last_refresh: datetime | None = None
        self.collect_calls = 0
        self.events: list[str] = []  # log pane mirror (tests)
        self.notices: list[str] = []  # notification mirror (tests)
        self._gates: list[Gate] = []
        self._jobs: list[JobRow] = []  # the Runs table: one row per job, snapshot order
        self._optimistic: list[JobRow] = []  # runs just triggered, until the poll catches up
        self._prs: list[PullRequest] = []
        self._sandboxes: list[Sandbox] = []

    # -- layout

    def compose(self) -> ComposeResult:
        yield Static(id="status")
        with TabbedContent(initial="gates"):
            with TabPane("Gates", id="gates"):
                yield GatesTable(id="gates-table", cursor_type="row", zebra_stripes=True)
            with TabPane("Runs", id="runs"):
                yield RunsTable(id="runs-table", cursor_type="row", zebra_stripes=True)
            with TabPane("PRs", id="prs"):
                yield PRsTable(id="prs-table", cursor_type="row", zebra_stripes=True)
            with TabPane("Sandboxes", id="sandboxes"):
                yield SandboxesTable(id="sandboxes-table", cursor_type="row", zebra_stripes=True)
            with TabPane("Metrics", id="metrics-pane"):
                yield Static("(no metrics yet)", id="metrics")
        yield RichLog(id="log", markup=True, wrap=True)
        with Horizontal(id="actor"):
            yield Static(
                f"actions are recorded as [b]{escape(self.info.actor)}[/b] "
                "(the user the Airflow token belongs to)"
            )
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#gates-table", DataTable).add_columns(
            "dag", "run", "gate", "job", "issue", "subject", "age"
        )
        self.query_one("#runs-table", DataTable).add_columns(
            "dag", "run_id", "job", "issue", "stage", "state"
        )
        self.query_one("#prs-table", DataTable).add_columns(
            "#", "title", "labels", "checks", "state"
        )
        self.query_one("#sandboxes-table", DataTable).add_columns(
            "name", "status", "created", "age"
        )
        self._render_status()
        if self.refresh_s > 0:
            self.set_interval(self.refresh_s, self.collect_snapshot, name="auto-refresh")
        self.collect_snapshot()

    # -- collection (worker thread -> UI thread)

    @work(thread=True, exclusive=True, group="collect", exit_on_error=False)
    def collect_snapshot(self) -> None:
        """Pull a fresh snapshot off the UI thread and hand it back with ``call_from_thread``."""
        try:
            snapshot = self.collector.collect()
        except Exception as e:  # noqa: BLE001 - the UI must survive any collector failure
            self.call_from_thread(self._collect_failed, e)
            return
        self.call_from_thread(self.apply_snapshot, snapshot)

    def _collect_failed(self, exc: Exception) -> None:
        self.collect_calls += 1
        self.log_event(f"[red]refresh failed:[/red] {escape(f'{type(exc).__name__}: {exc}')}")
        self.notify(f"refresh failed: {exc}", severity="error")
        self._render_status(extra_error="collect")

    def apply_snapshot(self, snapshot: Snapshot) -> None:
        """Repaint every tab from ``snapshot`` (UI thread only)."""
        self.collect_calls += 1
        self.snapshot = snapshot
        self.last_refresh = self.clock()
        self._fill_runs(list(snapshot.jobs))  # before the gates: they name their job's issue
        self._fill_gates(list(snapshot.gates))
        self._fill_prs(list(snapshot.prs))
        self._fill_sandboxes(list(snapshot.sandboxes))
        self.query_one("#metrics", Static).update(escape(_metrics_text(snapshot.metrics)))
        self._render_status()
        for source, msg in (snapshot.errors or {}).items():
            self.log_event(f"[red]{escape(str(source))}:[/red] {escape(str(msg))}")

    def _render_status(self, *, extra_error: str | None = None) -> None:
        errors = dict((self.snapshot.errors or {}) if self.snapshot else {})
        if extra_error:
            errors.setdefault(extra_error, "collector raised")
        refreshed = self.last_refresh.strftime("%H:%M:%S") if self.last_refresh else "never"
        parts = [
            f"[b]repo[/b] {escape(self.info.repo)}",
            f"[b]airflow[/b] {escape(self.info.airflow_url)}",
            f"[b]owner[/b] {escape(self.info.owner or '-')}",
            f"[b]refreshed[/b] {refreshed}",
        ]
        badges = " ".join(f"[reverse red] {escape(str(k))} [/]" for k in sorted(errors))
        if badges:
            parts.append(badges)
        self.query_one("#status", Static).update("  ·  ".join(parts))

    def _refill(self, table_id: str, rows: Iterable[tuple[Any, ...]]) -> None:
        table = self.query_one(table_id, DataTable)
        row = table.cursor_row
        table.clear()
        for cells in rows:
            table.add_row(*cells)
        if table.row_count:
            table.move_cursor(row=min(row, table.row_count - 1))

    def _fill_gates(self, gates: list[Gate]) -> None:
        self._gates = gates
        self._refill(
            "#gates-table",
            (
                (
                    g.dag_id,
                    g.run_id,
                    str(g.task_id).rsplit(".", 1)[-1],
                    job_index(g.map_index),
                    self.job_issue(g.dag_id, g.run_id, g.map_index),
                    (g.subject or "")[:60],
                    age(g.created_at, self.clock()),
                )
                for g in gates
            ),
        )
        self.query_one(TabbedContent).get_tab("gates").label = f"Gates ({len(gates)})"

    def _fill_runs(self, jobs: list[JobRow]) -> None:
        """One row per job. Optimistic rows (a run just triggered) stay on top until the poll
        returns that run, so the operator sees the effect of ``t`` without waiting for it."""
        self._jobs = jobs
        known = {(j.dag_id, j.run_id) for j in jobs}
        self._optimistic = [o for o in self._optimistic if (o.dag_id, o.run_id) not in known]
        rows = self.run_rows()
        self._refill(
            "#runs-table",
            (
                (
                    j.dag_id,
                    j.run_id,
                    job_index(j.map_index),
                    j.issue or "-",
                    stage_progress(j.tasks or ()),
                    str(j.state),
                )
                for j in rows
            ),
        )
        self.query_one(TabbedContent).get_tab("runs").label = f"Runs ({len(rows)})"

    def run_rows(self) -> list[JobRow]:
        """What the Runs table shows, in order: optimistic rows first, then the snapshot's."""
        return [*self._optimistic, *self._jobs]

    def job_issue(self, dag_id: str, run_id: str, map_index: int) -> str:
        """The issue of one job, as the Runs tab knows it (``-`` when no row matches yet)."""
        return next(
            (
                j.issue
                for j in self.run_rows()
                if (j.dag_id, j.run_id, j.map_index) == (dag_id, run_id, map_index)
            ),
            "-",
        )

    def _fill_prs(self, prs: list[PullRequest]) -> None:
        self._prs = prs
        self._refill(
            "#prs-table",
            (
                (str(p.number), (p.title or "")[:60], _join(p.labels), _join(p.checks), p.state)
                for p in prs
            ),
        )
        self.query_one(TabbedContent).get_tab("prs").label = f"PRs ({len(prs)})"

    def _fill_sandboxes(self, sandboxes: list[Sandbox]) -> None:
        self._sandboxes = sandboxes
        self._refill(
            "#sandboxes-table",
            (
                (s.name, s.status, when(s.created_at), age(s.created_at, self.clock()))
                for s in sandboxes
            ),
        )
        self.query_one(TabbedContent).get_tab("sandboxes").label = f"Sandboxes ({len(sandboxes)})"

    # -- selection helpers

    def _selected(self, table_id: str, items: list[Any], what: str) -> Any | None:
        table = self.query_one(table_id, DataTable)
        if not items or table.row_count == 0:
            self.notify(f"no {what} selected", severity="warning")
            return None
        return items[min(table.cursor_row, len(items) - 1)]

    def selected_gate(self) -> Gate | None:
        return self._selected("#gates-table", self._gates, "gate")

    def selected_job(self) -> JobRow | None:
        """The job under the cursor on the Runs tab; its ``dag_id``/``run_id`` are the run."""
        return self._selected("#runs-table", self.run_rows(), "run")

    def selected_pr(self) -> PullRequest | None:
        return self._selected("#prs-table", self._prs, "PR")

    def selected_sandbox(self) -> Sandbox | None:
        return self._selected("#sandboxes-table", self._sandboxes, "sandbox")

    # -- log / notify

    def log_event(self, text: str) -> None:
        stamp = self.clock().strftime("%H:%M:%S")
        line = f"[dim]{stamp}[/dim] {text}"
        self.events.append(line)
        self.query_one("#log", RichLog).write(line)

    def notify(
        self,
        message: str,
        *,
        title: str = "",
        severity: SeverityLevel = "information",
        timeout: float | None = None,
        markup: bool = True,
    ) -> None:
        """``App.notify`` that also mirrors the toast into :attr:`notices` (tests read it)."""
        self.notices.append(f"{severity}: {message}")
        super().notify(message, title=title, severity=severity, timeout=timeout, markup=markup)

    # -- global actions

    def action_refresh(self) -> None:
        self.log_event("refresh requested")
        self.collect_snapshot()

    def action_help(self) -> None:
        self.notify(HELP, title="keys", timeout=12)
        self.log_event(HELP.replace("\n", " · "))

    # -- gates

    def action_approve_gate(self) -> None:
        self._respond(approve=True)

    def action_reject_gate(self) -> None:
        self._respond(approve=False)

    def _respond(self, *, approve: bool) -> None:
        gate = self.selected_gate()
        if gate is None:
            return
        verb = "approve" if approve else "reject"
        name = str(gate.task_id).rsplit(".", 1)[-1]
        question = (
            f"{verb} {name} of {gate.dag_id}/{gate.run_id}[{gate.map_index}] as {self.info.actor}?"
        )
        fn = self.actions.approve if approve else self.actions.reject
        self._confirm(
            question, f"{verb} {gate.dag_id}/{gate.run_id}[{gate.map_index}] {name}", fn, gate
        )

    # -- runs

    def action_trigger_run(self) -> None:
        """``t``: pick the blueprint (skipped when the factory runs a single line), then issues.

        The DAG id **is** the blueprint name, so the choices are ``info.dag_ids`` — read from
        ``blueprints/*.toml`` by :func:`blueprint_dag_ids` — and the row under the cursor only
        preselects one; the operator can trigger any line from any tab position.
        """
        dags = list(self.info.dag_ids) or [DEFAULT_DAG_ID]
        if len(dags) == 1:
            self._ask_issues(dags[0])
            return
        current = self.cursor_dag_id()
        initial = dags.index(current) if current in dags else 0

        def _picked(dag_id: str | None) -> None:
            if dag_id:
                self._ask_issues(dag_id)
            else:
                self.log_event("trigger cancelled (no blueprint)")

        self.push_screen(Picker("trigger which blueprint?", dags, initial), _picked)

    def cursor_dag_id(self) -> str | None:
        """The DAG of the Runs row under the cursor, without nagging when the table is empty."""
        rows = self.run_rows()
        if not rows:
            return None
        return rows[min(self.query_one("#runs-table", DataTable).cursor_row, len(rows) - 1)].dag_id

    def _ask_issues(self, dag_id: str) -> None:
        def _go(text: str | None) -> None:
            issues = parse_issues(text or "")
            if not issues:
                self.log_event("trigger cancelled (no issues)")
                return
            self._perform(
                f"trigger {dag_id} {issues}",
                self.actions.trigger,
                dag_id,
                issues,
                on_success=lambda run_id: self._triggered(dag_id, issues, run_id),
            )

        self.push_screen(
            Prompt(f"trigger {dag_id}: issue ids or paths, comma separated", "42, 43"), _go
        )

    def _triggered(self, dag_id: str, issues: Sequence[str], run_id: Any) -> None:
        """Show the new run at once (optimistic row) with its Airflow UI link (UI thread)."""
        from swfactory.control import JobRow

        rid = str(run_id) if run_id else "(queued)"
        self._optimistic = [
            JobRow(
                dag_id=dag_id,
                run_id=rid,
                map_index=-1,
                issue=", ".join(str(i) for i in issues),
                state="queued",
            ),
            *[o for o in self._optimistic if (o.dag_id, o.run_id) != (dag_id, rid)],
        ]
        self._fill_runs(self._jobs)
        self.query_one("#runs-table", DataTable).move_cursor(row=0)  # the new run is the subject
        url = ""
        if run_id:
            with contextlib.suppress(Exception):  # a link is a nicety, never a failure
                url = str(self.actions.run_url(dag_id, rid) or "")
        self.log_event(f"[green]run[/green] {escape(dag_id)}/{escape(rid)} {escape(url)}".rstrip())
        if url:
            self.notify(f"{dag_id}/{rid}\n{url}", title="triggered", timeout=12)

    def action_stop_run(self) -> None:
        job = self.selected_job()
        if job is None:
            return
        self._confirm(
            f"stop {job.dag_id}/{job.run_id} — the whole run, every job (state {job.state})?",
            f"stop {job.dag_id}/{job.run_id}",
            self.actions.stop_run,
            job,
        )

    def action_open_run(self) -> None:
        job = self.selected_job()
        if job is not None:
            self._perform(f"open {job.dag_id}/{job.run_id}", self.actions.open_run, job)

    # -- PRs / sandboxes

    def action_open_pr(self) -> None:
        pr = self.selected_pr()
        if pr is not None:
            self._perform(f"open PR #{pr.number}", self.actions.open_pr, pr)

    def action_remove_sandbox(self) -> None:
        sb = self.selected_sandbox()
        if sb is None:
            return
        self._confirm(
            f"remove sandbox {sb.name} (created by {sb.created_by or '?'})?",
            f"remove sandbox {sb.name}",
            self.actions.remove_sandbox,
            sb,
        )

    # -- mutation plumbing

    def _confirm(self, question: str, label: str, fn: Callable[..., None], *args: Any) -> None:
        def _answered(yes: bool | None) -> None:
            if yes:
                self._perform(label, fn, *args)
            else:
                self.log_event(f"cancelled: {escape(label)}")

        self.push_screen(Confirm(question), _answered)

    def _perform(
        self,
        label: str,
        fn: Callable[..., Any],
        *args: Any,
        on_success: Callable[[Any], None] | None = None,
    ) -> None:
        """Run ``fn(*args)`` off the UI thread; ``on_success`` gets its return value back on
        the UI thread (that is how ``trigger`` turns a new run id into a row and a link)."""
        self.log_event(f"[yellow]->[/yellow] {escape(label)}")
        self._run_action(label, fn, on_success, *args)

    @work(thread=True, exit_on_error=False)
    def _run_action(
        self,
        label: str,
        fn: Callable[..., Any],
        on_success: Callable[[Any], None] | None,
        *args: Any,
    ) -> None:
        try:
            result = fn(*args)
        except PermissionError as e:
            self.call_from_thread(self._action_refused, label, e)
        except Exception as e:  # noqa: BLE001 - surfaced, never fatal
            self.call_from_thread(self._action_failed, label, e)
        else:
            self.call_from_thread(self._action_done, label, result, on_success)

    def _action_done(
        self,
        label: str,
        result: Any = None,
        on_success: Callable[[Any], None] | None = None,
    ) -> None:
        self.log_event(f"[green]ok[/green] {escape(label)}")
        self.notify(label)
        if on_success is not None:
            on_success(result)
        self.collect_snapshot()

    def _action_refused(self, label: str, exc: Exception) -> None:
        self.log_event(f"[yellow]refused[/yellow] {escape(label)}: {escape(str(exc))}")
        self.notify(f"refused: {exc}", title=label, severity="warning")

    def _action_failed(self, label: str, exc: Exception) -> None:
        self.log_event(
            f"[red]failed[/red] {escape(label)}: {escape(f'{type(exc).__name__}: {exc}')}"
        )
        self.notify(f"{type(exc).__name__}: {exc}", title=label, severity="error")


# ---------------------------------------------------------------- wiring for the CLI


@dataclass
class ControlCollector:
    """:class:`Collector` over ``swfactory.control.collect`` with the real clients."""

    airflow: Any
    github: Any
    islo: Any
    metrics: Any
    dag_ids: tuple[str, ...] = (DEFAULT_DAG_ID,)

    def collect(self) -> Snapshot:
        from swfactory.control import collect

        return collect(
            self.airflow, self.github, self.islo, self.metrics, dag_ids=list(self.dag_ids)
        )


@dataclass
class ControlActions:
    """:class:`Actions` over the ``swfactory.control`` clients."""

    airflow: Any
    github: Any
    islo: Any
    opener: Callable[[str], Any] = field(default=webbrowser.open)

    def approve(self, gate: Gate) -> None:
        self.airflow.respond(gate, approve=True)

    def reject(self, gate: Gate) -> None:
        self.airflow.respond(gate, approve=False)

    def trigger(self, dag_id: str, issues: Sequence[str]) -> str | None:
        """Post the run for **this** dag id and hand back its run id (the one trigger call)."""
        return self.airflow.trigger(dag_id, list(issues))

    def stop_run(self, run: RunLike) -> None:
        self.airflow.stop_run(run.dag_id, run.run_id)

    def open_run(self, run: RunLike) -> None:
        url = self.run_url(run.dag_id, run.run_id)
        if url:
            self.opener(url)

    def run_url(self, dag_id: str, run_id: str) -> str:
        url = self.airflow.run_url(dag_id, run_id)
        return url if isinstance(url, str) else ""

    def open_pr(self, pr: PullRequest) -> None:
        self.github.open_pr_in_browser(pr.number)

    def remove_sandbox(self, sandbox: Sandbox) -> None:
        self.islo.remove(sandbox.name)


def blueprint_dag_ids() -> tuple[str, ...]:
    """DAG ids of every ``blueprints/*.toml`` (``factory`` when none can be read)."""
    try:
        from swfactory.blueprint import blueprint_paths, load

        names = tuple(load(str(p)).name for p in blueprint_paths())
    except Exception:  # noqa: BLE001 - the TUI must start even with a broken blueprint dir
        names = ()
    return names or (DEFAULT_DAG_ID,)


@dataclass(frozen=True)
class Clients:
    """The one client stack of the ``herd`` command: TUI, ``--once`` and ``--approve-all``
    all read and write through this, so a headless run proves the interactive path."""

    collector: ControlCollector
    actions: ControlActions
    info: HerdInfo

    @property
    def dag_ids(self) -> tuple[str, ...]:
        return self.info.dag_ids


def make_clients(
    *,
    airflow_url: str,
    repo: str,
    owner: str | None,
    token: str | None = None,
    username: str | None = None,
    password: str | None = None,
    metrics_root: str = ".",
    dag_ids: Sequence[str] | None = None,
) -> Clients:
    """Build the real ``swfactory.control`` clients (imported here, lazily)."""
    from pathlib import Path

    from swfactory.control import AirflowClient, GitHubClient, IsloClient, MetricsSource

    airflow = AirflowClient(airflow_url, token=token, username=username, password=password)
    github = GitHubClient(repo)
    islo = IsloClient(owner)
    metrics = MetricsSource(Path(metrics_root))
    ids = tuple(dag_ids) if dag_ids else blueprint_dag_ids()
    return Clients(
        collector=ControlCollector(airflow, github, islo, metrics, ids),
        actions=ControlActions(airflow, github, islo),
        info=HerdInfo(
            repo=repo,
            airflow_url=airflow_url,
            owner=owner,
            actor=username or "Airflow token owner",
            dag_ids=ids,
        ),
    )


def make_app(
    *,
    airflow_url: str,
    repo: str,
    owner: str | None,
    token: str | None = None,
    username: str | None = None,
    password: str | None = None,
    metrics_root: str = ".",
    refresh_s: float = 5.0,
    dag_ids: Sequence[str] | None = None,
) -> HerdApp:
    """The TUI over the real clients."""
    clients = make_clients(
        airflow_url=airflow_url,
        repo=repo,
        owner=owner,
        token=token,
        username=username,
        password=password,
        metrics_root=metrics_root,
        dag_ids=dag_ids,
    )
    return HerdApp(clients.collector, clients.actions, info=clients.info, refresh_s=refresh_s)


def run_herd(
    collector: Collector,
    actions: Actions,
    *,
    info: HerdInfo | None = None,
    refresh_s: float = 5.0,
) -> None:
    """Run the TUI until the user quits."""
    HerdApp(collector, actions, info=info, refresh_s=refresh_s).run()


# ---------------------------------------------------------------- headless drive mode


def snapshot_data(snapshot: Snapshot) -> dict:
    """A :class:`Snapshot` as plain JSON-safe data — the same rows the TUI paints.

    Runs carry their jobs (``map_index``, issue, stage, state), so a script or a CI job reads
    exactly what an operator would see, without a terminal.
    """
    return {
        "collected_at": _iso(getattr(snapshot, "collected_at", None)),
        "runs": [
            {
                "dag_id": r.dag_id,
                "run_id": r.run_id,
                "state": r.state,
                "start": _iso(r.start),
                "end": _iso(r.end),
                "issues": list(r.issues),
                "jobs": [
                    {
                        "map_index": j.map_index,
                        "issue": j.issue,
                        "stage": stage_progress(j.tasks or ()),
                        "state": j.state,
                    }
                    for j in r.jobs
                ],
            }
            for r in snapshot.runs
        ],
        "gates": [
            {
                "dag_id": g.dag_id,
                "run_id": g.run_id,
                "task_id": g.task_id,
                "gate": str(g.task_id).rsplit(".", 1)[-1],
                "map_index": g.map_index,
                "subject": g.subject,
                "options": list(g.options or []),
                "created_at": _iso(g.created_at),
            }
            for g in snapshot.gates
        ],
        "prs": [
            {
                "number": p.number,
                "title": p.title,
                "url": p.url,
                "labels": list(p.labels or []),
                "state": p.state,
                "checks": p.checks,
                "head": p.head,
            }
            for p in snapshot.prs
        ],
        "sandboxes": [
            {
                "name": s.name,
                "status": s.status,
                "created_by": s.created_by,
                "created_at": _iso(s.created_at),
            }
            for s in snapshot.sandboxes
        ],
        "metrics": dict(snapshot.metrics or {}),
        "errors": dict(snapshot.errors or {}),
    }


def snapshot_text(snapshot: Snapshot) -> str:
    """The same snapshot as a few lines for a CI log (``--once`` without ``--json``)."""
    data = snapshot_data(snapshot)
    lines = [
        f"collected {data['collected_at']}  "
        f"runs {len(data['runs'])}  jobs {sum(len(r['jobs']) for r in data['runs'])}  "
        f"gates {len(data['gates'])}  prs {len(data['prs'])}  sandboxes {len(data['sandboxes'])}"
    ]
    lines += [
        f"run  {r['dag_id']}/{r['run_id']} {r['state']}"
        + "".join(
            f"\n  job {job_index(j['map_index'])} {j['issue']} {j['stage']} {j['state']}"
            for j in r["jobs"]
        )
        for r in data["runs"]
    ]
    lines += [
        f"gate {g['dag_id']}/{g['run_id']}[{g['map_index']}] {g['gate']} {g['subject']}"
        for g in data["gates"]
    ]
    lines += [f"error {source}: {msg}" for source, msg in data["errors"].items()]
    return "\n".join(lines)


def drive_once(
    collector: Collector, *, as_json: bool = True, out: Callable[[str], Any] = print
) -> int:
    """Print exactly one snapshot and return the exit code (always 0: this is a read)."""
    snapshot = collector.collect()
    out(json.dumps(snapshot_data(snapshot), indent=2) if as_json else snapshot_text(snapshot))
    return 0


def approve_all(
    collector: Collector,
    actions: Actions,
    *,
    dag_ids: Sequence[str] = (),
    reject: bool = False,
    actor: str = "Airflow token owner",
    out: Callable[[str], Any] = print,
) -> int:
    """Answer every pending gate of ``dag_ids`` and echo each answer; exit code 1 on any failure.

    This is the action layer under test in CI: one collect through the same clients the TUI
    uses, then one ``respond`` per gate. The recorded approver is the Airflow API user, never
    ``actor`` — that string is only echoed so the operator can see whose token answered.
    """
    snapshot = collector.collect()
    wanted = {str(d) for d in dag_ids}
    failures = 0
    for source, msg in (snapshot.errors or {}).items():
        out(f"error {source}: {msg}")
        if source == "gates":  # the gate list is the one error that voids the whole answer
            failures += 1
    gates = [g for g in snapshot.gates if not wanted or g.dag_id in wanted]
    skipped = len(snapshot.gates) - len(gates)
    verb = "reject" if reject else "approve"
    answer = actions.reject if reject else actions.approve
    answered = 0
    for gate in gates:
        name = str(gate.task_id).rsplit(".", 1)[-1]
        where = f"{gate.dag_id}/{gate.run_id}[{gate.map_index}] {name}"
        try:
            answer(gate)
        except Exception as e:  # noqa: BLE001 - one bad gate must not skip the rest
            failures += 1
            out(f"failed {verb} {where}: {type(e).__name__}: {e}")
        else:
            answered += 1
            out(f"{verb} {where} as {actor}")
    outside = f" ({skipped} outside {', '.join(sorted(wanted))})" if skipped else ""
    out(
        f"{verb}d {answered}/{len(gates)} pending gates{outside}"
        if gates
        else f"no pending gates{outside}"
    )
    return 1 if failures else 0


def main(
    *,
    airflow_url: str,
    repo: str,
    owner: str | None,
    token: str | None = None,
    username: str | None = None,
    password: str | None = None,
    metrics_root: str = ".",
    refresh_s: float = 5.0,
    dag_ids: Sequence[str] | None = None,
    once: bool = False,
    json_out: bool = False,
    approve_all_gates: bool = False,
    reject: bool = False,
    out: Callable[[str], Any] = print,
) -> int:
    """The whole ``swfactory herd`` command: exit code for the headless flags, 0 after the TUI.

    ``--json`` implies ``--once`` (a JSON stream out of a live TUI is nobody's format), and
    ``--approve-all`` never opens a screen: both are the same clients, driven headlessly.
    """
    clients = make_clients(
        airflow_url=airflow_url,
        repo=repo,
        owner=owner,
        token=token,
        username=username,
        password=password,
        metrics_root=metrics_root,
        dag_ids=dag_ids,
    )
    if approve_all_gates:
        return approve_all(
            clients.collector,
            clients.actions,
            dag_ids=clients.dag_ids,
            reject=reject,
            actor=clients.info.actor,
            out=out,
        )
    if once or json_out:
        return drive_once(clients.collector, as_json=json_out, out=out)
    HerdApp(clients.collector, clients.actions, info=clients.info, refresh_s=refresh_s).run()
    return 0


def _iso(value: Any) -> str | None:
    """A datetime as ISO-8601 for the JSON snapshot; ``None`` stays ``None``."""
    dt = _as_datetime(value)
    return dt.isoformat() if dt is not None else None
