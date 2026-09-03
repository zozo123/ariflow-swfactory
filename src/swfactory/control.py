"""Control-room data layer: read-mostly clients for Airflow, GitHub and islo plus one snapshot.

The TUI (and any future ``swfactory control`` verb) never talks to the network itself. It asks
:func:`collect` for a :class:`Snapshot`, which fans out to small clients that each take an
injectable transport (``opener`` for HTTP, ``runner`` for subprocesses) so tests are hermetic.
Every source failure lands in :attr:`Snapshot.errors` instead of raising, so a dead Airflow does
not hide the PR list and vice versa.

Only three writes exist, all explicit and narrow:

* :meth:`AirflowClient.respond` — answer one HITL gate (Approve / Reject);
* :meth:`AirflowClient.trigger` / :meth:`AirflowClient.stop_run` — start or fail a DAG run;
* :meth:`IsloClient.remove` — ``islo rm`` of a sandbox this factory named **and** this owner
  created. Anything else raises :class:`PermissionError` (teammates' sandboxes are never touched).

The addressable unit is the **job**, not the run: ``dags/blueprints.py`` expands the ``job`` task
group over ``fan_out`` (issues x targets), so a gate, a sandbox and a PR all belong to one
``(run_id, map_index)`` pair. :class:`JobRow` is that unit and :meth:`AirflowClient.job_rows`
builds it from one ``taskInstances`` read plus ``fan_out``'s XCom (absent XCom = a ``-`` issue,
never an error).

Route and payload shapes were checked against the installed Airflow 3.3.1
``airflow.api_fastapi`` package; the UI path against the bundled ``airflow/ui`` router.
"""

from __future__ import annotations

import contextlib
import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from swfactory import metrics as _metrics
from swfactory.maintain import SANDBOX_NAME_RE, owned_sandboxes
from swfactory.sandbox import owns_sandbox

API_PREFIX = "/api/v2"
TOKEN_PATH = "/auth/token"
GATE_APPROVE = "Approve"  # ApprovalOperator.APPROVE / REJECT in airflow.providers.standard
GATE_REJECT = "Reject"
ACTIVE_RUN_STATES = frozenset({"queued", "running"})
DEFAULT_TIMEOUT_S = 15.0
_SUBPROCESS_TIMEOUT_S = 120
FAN_OUT_TASK_ID = "fan_out"  # dags/blueprints.py: the task whose XCom lists the jobs
XCOM_RETURN_KEY = "return_value"
NO_ISSUE = "-"  # a job row whose issue cannot be known yet (no fan_out XCom)
# Airflow task-instance states, split the three ways every roll-up needs. One definition:
# ``swfactory.herd`` imports these instead of keeping its own copy.
#
# ``awaiting_input`` is the state Airflow 3.3 parks a HITL task in (older builds used
# ``deferred``, kept here for them). It has to count as ACTIVE or the frontier of a job waiting
# on a gate falls back to its last finished task, and the Runs tab shows ``intent`` instead of
# ``approve_intent`` for exactly the jobs an operator has to act on — observed on a live
# standalone, where four jobs sat in ``awaiting_input`` while every row still read ``intent``.
ACTIVE_TASK_STATES = frozenset(
    {"running", "queued", "scheduled", "deferred", "restarting", "awaiting_input"}
    | {"up_for_retry", "up_for_reschedule"}
)
FAILED_TASK_STATES = frozenset({"failed", "upstream_failed"})
FINAL_TASK_STATES = FAILED_TASK_STATES | frozenset({"success", "skipped", "removed"})

_CHECK_PASS = frozenset({"SUCCESS", "NEUTRAL", "SKIPPED"})
_CHECK_FAIL = frozenset(
    {"FAILURE", "ERROR", "TIMED_OUT", "CANCELLED", "ACTION_REQUIRED", "STARTUP_FAILURE", "STALE"}
)


class ControlError(RuntimeError):
    """A source call failed (HTTP status, transport error, non-zero subprocess)."""


# ---------------------------------------------------------------- data


@dataclass(frozen=True)
class TaskState:
    task_id: str  # ``fan_out``, ``job.<stage>``, ``job.approve_<gate>`` ...
    map_index: int  # -1 for unmapped tasks, else the job index from ``fan_out``
    state: str | None


@dataclass(frozen=True)
class JobRow:
    """One mapped job of a DAG run — the unit a gate, a sandbox and a PR belong to.

    ``map_index`` is ``-1`` only before ``fan_out`` has produced the job list (or for a run whose
    task instances were not fetched): there is no job index yet, not a job numbered -1.
    """

    dag_id: str
    run_id: str
    map_index: int
    issue: str = NO_ISSUE
    state: str = "queued"
    tasks: list[TaskState] = field(default_factory=list)

    @property
    def mapped(self) -> bool:
        return self.map_index >= 0


@dataclass
class Run:
    dag_id: str
    run_id: str
    state: str
    start: datetime | None
    end: datetime | None
    conf: dict = field(default_factory=dict)
    jobs: list[JobRow] = field(default_factory=list)

    @property
    def active(self) -> bool:
        return self.state in ACTIVE_RUN_STATES

    @property
    def issues(self) -> list[str]:
        """Issue refs from ``conf`` (``{"issues": [...]}`` or the ``{"issue": N}`` compat form)."""
        many = self.conf.get("issues") or []
        one = self.conf.get("issue")
        refs = [str(i) for i in many] if isinstance(many, list) else []
        if one not in (None, "") and str(one) not in refs:
            refs.append(str(one))
        return refs


@dataclass(frozen=True)
class Gate:
    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    subject: str
    body: str
    created_at: datetime | None
    options: list[str]


@dataclass(frozen=True)
class PullRequest:
    number: int
    title: str
    url: str
    labels: list[str]
    state: str  # OPEN / MERGED / CLOSED
    checks: str  # ``summarize_checks`` text
    head: str


@dataclass(frozen=True)
class IssueRef:
    number: int
    title: str
    url: str
    labels: list[str]


@dataclass(frozen=True)
class Sandbox:
    name: str
    status: str
    created_by: str
    created_at: datetime | None

    @property
    def factory_named(self) -> bool:
        return bool(SANDBOX_NAME_RE.match(self.name))


@dataclass
class Snapshot:
    collected_at: datetime
    runs: list[Run] = field(default_factory=list)
    gates: list[Gate] = field(default_factory=list)
    prs: list[PullRequest] = field(default_factory=list)
    sandboxes: list[Sandbox] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)  # source -> failure text

    @property
    def jobs(self) -> list[JobRow]:
        """Every run's job rows, flattened in run order (what the Runs table shows)."""
        return [job for run in self.runs for job in run.jobs]


# ---------------------------------------------------------------- job grouping (pure)


def job_state(tasks: Sequence[TaskState]) -> str:
    """Roll one job's task states up into a single word for the Runs table.

    Any failure wins (that is what the operator must see), then anything in flight; a job whose
    tasks all still have a ``None`` state has not started, and one that is entirely skipped is
    reported as such (a rejected gate skips the work stages).
    """
    states = [t.state or "none" for t in tasks]
    if not states:
        return "queued"
    if any(s in FAILED_TASK_STATES for s in states):
        return "failed"
    if any(s in ACTIVE_TASK_STATES for s in states):
        return "running"
    unfinished = [s for s in states if s not in FINAL_TASK_STATES]
    if unfinished:
        return "running" if len(unfinished) < len(states) else "queued"
    return "skipped" if all(s == "skipped" for s in states) else "success"


def group_jobs(
    dag_id: str,
    run_id: str,
    tasks: Sequence[TaskState],
    fan_out: Sequence[dict] = (),
    *,
    fallback_issues: Sequence[str] = (),
) -> list[JobRow]:
    """Task instances of one run -> one :class:`JobRow` per ``map_index``, ascending.

    ``fan_out`` is the job list from the ``fan_out`` XCom (``[{"issue": ..., "repo": ...}, ...]``)
    and names each row's issue. Unmapped tasks (``fan_out`` itself, ``map_index == -1``) are
    dropped once any mapped task exists, so a fanned-out run shows exactly its jobs and a run
    still fanning out shows one placeholder row instead of nothing.
    """
    by_index: dict[int, list[TaskState]] = {}
    for t in tasks:
        idx = -1 if t.map_index is None else int(t.map_index)
        by_index.setdefault(idx, []).append(t)
    mapped = sorted(i for i in by_index if i >= 0)
    return [
        JobRow(
            dag_id=dag_id,
            run_id=run_id,
            map_index=idx,
            issue=_issue_of(idx, fan_out, fallback_issues),
            state=job_state(by_index[idx]),
            tasks=by_index[idx],
        )
        for idx in (mapped or sorted(by_index))
    ]


def _issue_of(idx: int, fan_out: Sequence[dict], fallback: Sequence[str]) -> str:
    """The issue of job ``idx``: from the XCom, else from ``conf`` when that is unambiguous.

    Jobs are issues x targets, so a run carrying exactly one issue gives every job that issue —
    exact without the XCom. More than one issue and the mapping needs ``fan_out``: say ``-``.
    """
    if 0 <= idx < len(fan_out):
        issue = (fan_out[idx] or {}).get("issue")
        if issue:
            return str(issue)
    return str(fallback[0]) if len(fallback) == 1 else NO_ISSUE


def collapsed_job(run: Run) -> JobRow:
    """The single row that stands for a run whose task instances were not fetched."""
    return JobRow(
        dag_id=run.dag_id,
        run_id=run.run_id,
        map_index=-1,
        issue=", ".join(run.issues) or NO_ISSUE,
        state=run.state,
    )


# ---------------------------------------------------------------- transports


class Opener(Protocol):
    """``urllib.request.urlopen`` shape: returns something with ``read()`` and ``close()``."""

    def __call__(self, request: urllib.request.Request, *, timeout: float) -> Any: ...


class Runner(Protocol):
    """``subprocess.run`` shape; fakes return a ``CompletedProcess``."""

    def __call__(self, argv: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]: ...


def _exec(runner: Runner, argv: Sequence[str]) -> str:
    """Run ``argv`` and return stdout; a non-zero exit raises :class:`ControlError`."""
    argv = list(argv)
    try:
        proc = runner(
            argv, capture_output=True, text=True, check=False, timeout=_SUBPROCESS_TIMEOUT_S
        )
    except (OSError, subprocess.SubprocessError) as e:
        raise ControlError(f"{argv[0]}: {e}") from e
    if proc.returncode != 0:
        err = (proc.stderr or "").strip() or (proc.stdout or "").strip()
        raise ControlError(f"{' '.join(argv)} failed rc={proc.returncode}: {err[:300]}")
    return proc.stdout or ""


def _json(text: str, what: str) -> Any:
    try:
        return json.loads(text or "null")
    except ValueError as e:
        raise ControlError(f"{what} returned non-JSON: {text[:200]!r}") from e


def _seg(value: str | int) -> str:
    """One URL path segment (run ids carry ``:`` and ``+``)."""
    return urllib.parse.quote(str(value), safe="")


# ---------------------------------------------------------------- Airflow


class AirflowClient:
    """Airflow 3 REST API (``/api/v2``) over ``urllib``; token via ``POST /auth/token``.

    ``token`` wins when given; otherwise ``username``/``password`` are exchanged lazily on the
    first call and cached. Without any credential requests go out unauthenticated (fine for a
    dev server with ``simple_auth_manager_all_admins``).
    """

    def __init__(
        self,
        url: str,
        *,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        opener: Opener = urllib.request.urlopen,
        timeout: float = DEFAULT_TIMEOUT_S,
    ) -> None:
        self.url = url.rstrip("/")
        self._token = token
        self._username = username
        self._password = password
        self._opener = opener
        self.timeout = timeout

    # -- auth / transport

    def token(self) -> str | None:
        """Bearer token: the configured one, or one minted from username/password (cached)."""
        if self._token is None and self._username and self._password is not None:
            data = self._http(
                "POST",
                self.url + TOKEN_PATH,
                body={"username": self._username, "password": self._password},
                auth=False,
            )
            token = (data or {}).get("access_token") if isinstance(data, dict) else None
            if not token:
                raise ControlError(f"{TOKEN_PATH} returned no access_token")
            self._token = str(token)
        return self._token

    def _api(
        self,
        method: str,
        path: str,
        *,
        body: dict | None = None,
        query: dict[str, Any] | None = None,
    ) -> Any:
        url = self.url + API_PREFIX + path
        if query:
            url += "?" + urllib.parse.urlencode(query, doseq=True)
        return self._http(method, url, body=body, auth=True)

    def _http(self, method: str, url: str, *, body: dict | None, auth: bool) -> Any:
        headers = {"Accept": "application/json"}
        data: bytes | None = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if auth:
            token = self.token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            resp = self._opener(req, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            detail = ""
            with contextlib.suppress(Exception):  # the status alone is still useful
                detail = e.read().decode("utf-8", "replace")[:300]
            raise ControlError(f"{method} {url} -> HTTP {e.code} {detail}".rstrip()) from e
        except (urllib.error.URLError, OSError) as e:
            raise ControlError(f"{method} {url}: {e}") from e
        try:
            raw = resp.read()
        finally:
            resp.close()
        text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
        return _json(text, f"{method} {url}") if text.strip() else None

    # -- reads

    def list_dags(self, tag: str = "swfactory") -> list[str]:
        """Ids of the DAGs tagged ``tag`` (``dags/blueprints.py`` tags every line ``swfactory``)."""
        data = self._api("GET", "/dags", query={"tags": tag, "limit": 100})
        return [str(d["dag_id"]) for d in (data or {}).get("dags", []) if "dag_id" in d]

    def list_runs(self, dag_id: str, limit: int = 20) -> list[Run]:
        """Most recent ``limit`` runs of ``dag_id``, newest first, without task states."""
        data = self._api(
            "GET",
            f"/dags/{_seg(dag_id)}/dagRuns",
            query={"limit": limit, "order_by": "-run_after"},
        )
        return [_run_from(d) for d in (data or {}).get("dag_runs", [])]

    def task_states(self, dag_id: str, run_id: str) -> list[TaskState]:
        """Every task instance of one run (``job.<stage>`` mapped per job index)."""
        data = self._api(
            "GET",
            f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}/taskInstances",
            query={"limit": 500},
        )
        return [
            TaskState(
                task_id=str(ti.get("task_id", "")),
                map_index=int(ti.get("map_index", -1)),
                state=ti.get("state"),
            )
            for ti in (data or {}).get("task_instances", [])
        ]

    def fan_out_jobs(self, dag_id: str, run_id: str) -> list[dict]:
        """``fan_out``'s ``return_value`` XCom: the job list, index = ``map_index``.

        Raises :class:`ControlError` when the entry is missing (a run that has not fanned out
        yet answers 404) — :meth:`job_rows` treats that as "issue unknown", never as a failure.
        """
        data = self._api(
            "GET",
            f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}"
            f"/taskInstances/{_seg(FAN_OUT_TASK_ID)}/xcomEntries/{XCOM_RETURN_KEY}",
            query={"map_index": -1},
        )
        value = (data or {}).get("value")
        if isinstance(value, str):  # /api/v2 hands back the DB's serialized JSON as a string
            try:
                value = json.loads(value)
            except ValueError:
                return []
        return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []

    def job_rows(
        self, dag_id: str, run_id: str, *, fallback_issues: Sequence[str] = ()
    ) -> list[JobRow]:
        """One row per job of ``run_id``: task instances grouped by ``map_index``, issue named.

        Two bounded reads: ``taskInstances`` (the states) and ``fan_out``'s XCom (the issues).
        A missing or unreadable XCom degrades to ``fallback_issues`` / ``-``.
        """
        tasks = self.task_states(dag_id, run_id)
        try:
            fan_out = self.fan_out_jobs(dag_id, run_id)
        except ControlError:
            fan_out = []
        return group_jobs(dag_id, run_id, tasks, fan_out, fallback_issues=fallback_issues)

    def pending_gates(self) -> list[Gate]:
        """Unanswered HITL gates across all DAGs (``GET /dags/~/dagRuns/~/hitlDetails``)."""
        data = self._api(
            "GET",
            "/dags/~/dagRuns/~/hitlDetails",
            query={"response_received": "false", "limit": 100},
        )
        return [_gate_from(h) for h in (data or {}).get("hitl_details", [])]

    # -- writes

    def respond(self, gate: Gate, approve: bool) -> dict:
        """Answer ``gate`` with ``Approve``/``Reject`` (the option must exist on the gate)."""
        choice = GATE_APPROVE if approve else GATE_REJECT
        if gate.options and choice not in gate.options:
            raise ValueError(f"gate {gate.task_id} offers {gate.options}, not {choice!r}")
        path = (
            f"/dags/{_seg(gate.dag_id)}/dagRuns/{_seg(gate.run_id)}"
            f"/taskInstances/{_seg(gate.task_id)}/{gate.map_index}/hitlDetails"
        )
        return self._api("PATCH", path, body={"chosen_options": [choice], "params_input": {}})

    def trigger(self, dag_id: str, issues: Sequence[str]) -> str:
        """``POST /dags/{dag_id}/dagRuns`` with ``conf={"issues": [...]}``; returns the run id."""
        refs = [str(i).strip() for i in issues if str(i).strip()]
        if not refs:
            raise ValueError("trigger needs at least one issue")
        data = self._api(
            "POST",
            f"/dags/{_seg(dag_id)}/dagRuns",
            body={"logical_date": None, "conf": {"issues": refs}},
        )
        run_id = (data or {}).get("dag_run_id") or (data or {}).get("run_id")
        if not run_id:
            raise ControlError(f"trigger {dag_id}: response carried no dag_run_id")
        return str(run_id)

    def stop_run(self, dag_id: str, run_id: str) -> dict:
        """Mark a run failed (``PATCH dagRuns/{run_id} {"state": "failed"}``)."""
        return self._api(
            "PATCH", f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}", body={"state": "failed"}
        )

    # -- links

    def run_url(self, dag_id: str, run_id: str) -> str:
        """Airflow UI page of one run (router: ``dags/:dagId/runs/:runId``).

        Takes the ids, not a :class:`Run`, so a run that was just triggered (and is not in any
        snapshot yet) can be linked straight away.
        """
        return f"{self.url}/dags/{_seg(dag_id)}/runs/{_seg(run_id)}"


def _run_from(d: dict) -> Run:
    conf = d.get("conf")
    return Run(
        dag_id=str(d.get("dag_id", "")),
        run_id=str(d.get("dag_run_id") or d.get("run_id") or ""),
        state=str(d.get("state") or "unknown"),
        start=_metrics.parse_ts(d.get("start_date")),
        end=_metrics.parse_ts(d.get("end_date")),
        conf=conf if isinstance(conf, dict) else {},
    )


def _gate_from(h: dict) -> Gate:
    ti = h.get("task_instance") or {}
    return Gate(
        dag_id=str(ti.get("dag_id") or h.get("dag_id") or ""),
        run_id=str(ti.get("dag_run_id") or ti.get("run_id") or h.get("run_id") or ""),
        task_id=str(ti.get("task_id") or h.get("task_id") or ""),
        map_index=int(ti.get("map_index", h.get("map_index", -1))),
        subject=str(h.get("subject") or ""),
        body=str(h.get("body") or ""),
        created_at=_metrics.parse_ts(h.get("created_at")),
        options=[str(o) for o in h.get("options") or []],
    )


# ---------------------------------------------------------------- GitHub


class GitHubClient:
    """Factory PRs and issues through ``gh`` (the orchestrator's credential, never argv)."""

    def __init__(self, repo: str, runner: Runner = subprocess.run) -> None:
        self.repo = repo
        self._runner = runner

    def prs(self, label: str = "factory", limit: int = 30) -> list[PullRequest]:
        out = _exec(
            self._runner,
            [
                "gh", "pr", "list", "--repo", self.repo, "--label", label, "--state", "all",
                "--limit", str(limit),
                "--json", "number,title,url,labels,state,headRefName,statusCheckRollup",
            ],
        )  # fmt: skip
        return [
            PullRequest(
                number=int(p["number"]),
                title=str(p.get("title") or ""),
                url=str(p.get("url") or ""),
                labels=_label_names(p.get("labels")),
                state=str(p.get("state") or ""),
                checks=summarize_checks(p.get("statusCheckRollup")),
                head=str(p.get("headRefName") or ""),
            )
            for p in _json(out, "gh pr list") or []
        ]

    def issues(self, label: str = "factory", limit: int = 30) -> list[IssueRef]:
        out = _exec(
            self._runner,
            [
                "gh", "issue", "list", "--repo", self.repo, "--label", label,
                "--limit", str(limit), "--json", "number,title,url,labels",
            ],
        )  # fmt: skip
        return [
            IssueRef(
                number=int(i["number"]),
                title=str(i.get("title") or ""),
                url=str(i.get("url") or ""),
                labels=_label_names(i.get("labels")),
            )
            for i in _json(out, "gh issue list") or []
        ]

    def open_pr_in_browser(self, number: int) -> list[str]:
        """``gh pr view N --repo R --web``; returns the argv it ran."""
        argv = ["gh", "pr", "view", str(int(number)), "--repo", self.repo, "--web"]
        _exec(self._runner, argv)
        return argv


def _label_names(labels: Any) -> list[str]:
    if not isinstance(labels, list):
        return []
    return [str(x["name"]) if isinstance(x, dict) else str(x) for x in labels]


def summarize_checks(rollup: Any) -> str:
    """``statusCheckRollup`` -> ``"2 pass / 1 fail / 0 pending"`` (``"none"`` when empty).

    Handles both ``CheckRun`` (``status``/``conclusion``) and ``StatusContext`` (``state``).
    """
    if not isinstance(rollup, list) or not rollup:
        return "none"
    passed = failed = pending = 0
    for item in rollup:
        if not isinstance(item, dict):
            continue
        verdict = str(item.get("conclusion") or item.get("state") or "").upper()
        status = str(item.get("status") or "").upper()
        if status and status != "COMPLETED" and not item.get("conclusion"):
            pending += 1
        elif verdict in _CHECK_PASS:
            passed += 1
        elif verdict in _CHECK_FAIL:
            failed += 1
        else:
            pending += 1
    return f"{passed} pass / {failed} fail / {pending} pending"


# ---------------------------------------------------------------- islo


class IsloClient:
    """This owner's sandboxes via plain ``islo ls`` (never ``--all``) and a guarded ``islo rm``."""

    def __init__(self, owner: str, runner: Runner = subprocess.run) -> None:
        self.owner = (owner or "").strip()
        self._runner = runner

    def listing(self) -> str:
        """Raw ``islo ls --output json`` text (own scope)."""
        return _exec(self._runner, ["islo", "ls", "--output", "json"])

    def own_sandboxes(self) -> list[Sandbox]:
        """Live sandboxes whose ``created_by`` equals ``owner`` (case-insensitive); else ``[]``."""
        if not self.owner:
            return []
        return [
            Sandbox(
                name=str(item.get("name") or ""),
                status=str(item.get("status") or ""),
                created_by=str(item.get("created_by") or ""),
                created_at=_metrics.first_timestamp(item, _metrics.CREATED_KEYS),
            )
            for item in owned_sandboxes(self.listing(), self.owner)
        ]

    def remove(self, name: str) -> list[str]:
        """``islo rm name`` — ONLY for a factory-named sandbox this owner created.

        Re-lists right before removing so the decision is made on fresh data. Raises
        :class:`PermissionError` otherwise; returns the argv it ran.
        """
        if not self.owner:
            raise PermissionError("no sandbox owner configured; refusing to remove anything")
        if not SANDBOX_NAME_RE.match(name):
            raise PermissionError(f"{name!r} is not a factory-named sandbox (swf-<slug>-<run8>)")
        if not owns_sandbox(self.listing(), name, owner=self.owner):
            raise PermissionError(f"{name!r} was not created by {self.owner!r}; refusing")
        argv = ["islo", "rm", name, "--output", "plain"]
        _exec(self._runner, argv)
        return argv


# ---------------------------------------------------------------- metrics


class MetricsSource:
    """``metrics.summarize(metrics.load_all(root))`` over a checkout holding artifact chains."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def runs(self) -> list[dict]:
        return _metrics.load_all(self.root)

    def summary(self) -> dict:
        return _metrics.summarize(self.runs())


# ---------------------------------------------------------------- snapshot


class RunSource(Protocol):
    def list_dags(self, tag: str = ...) -> list[str]: ...
    def list_runs(self, dag_id: str, limit: int = ...) -> list[Run]: ...
    def job_rows(
        self, dag_id: str, run_id: str, *, fallback_issues: Sequence[str] = ...
    ) -> list[JobRow]: ...
    def pending_gates(self) -> list[Gate]: ...


class PullRequestSource(Protocol):
    def prs(self, label: str = ..., limit: int = ...) -> list[PullRequest]: ...


class SandboxSource(Protocol):
    def own_sandboxes(self) -> list[Sandbox]: ...


class SummarySource(Protocol):
    def summary(self) -> dict: ...


def collect(
    airflow: RunSource | None,
    github: PullRequestSource | None,
    islo: SandboxSource | None,
    metrics: SummarySource | None,
    *,
    dag_ids: Sequence[str] | None = None,
    runs_per_dag: int = 20,
    jobs_per_dag: int = 5,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Snapshot:
    """One consistent picture of the factory; a failing source fills ``errors`` and nothing else.

    ``dag_ids=None`` asks Airflow for every ``swfactory``-tagged DAG. Per-job rows cost two
    reads per run, so they are fetched for every active run plus the ``jobs_per_dag`` newest
    runs of each DAG — the ones an operator acts on. Every other run (and every run whose read
    failed) still contributes exactly one :func:`collapsed_job` row, so the table never lies by
    omission. A ``None`` client is skipped.
    """
    snap = Snapshot(collected_at=now())
    if airflow is not None:
        try:
            ids = list(dag_ids) if dag_ids is not None else airflow.list_dags()
            for dag_id in ids:
                snap.runs.extend(airflow.list_runs(dag_id, runs_per_dag))
        except Exception as e:  # noqa: BLE001 - per-source degradation is the contract
            snap.errors["airflow"] = str(e)
        position: dict[str, int] = {}
        for run in snap.runs:
            pos = position.get(run.dag_id, 0)
            position[run.dag_id] = pos + 1
            if not (run.active or pos < jobs_per_dag):
                run.jobs = [collapsed_job(run)]
                continue
            try:
                run.jobs = airflow.job_rows(run.dag_id, run.run_id, fallback_issues=run.issues) or [
                    collapsed_job(run)
                ]
            except Exception as e:  # noqa: BLE001
                snap.errors[f"airflow:{run.dag_id}/{run.run_id}"] = str(e)
                run.jobs = [collapsed_job(run)]
        try:
            snap.gates = airflow.pending_gates()
        except Exception as e:  # noqa: BLE001
            snap.errors["gates"] = str(e)
    if github is not None:
        try:
            snap.prs = github.prs()
        except Exception as e:  # noqa: BLE001
            snap.errors["github"] = str(e)
    if islo is not None:
        try:
            snap.sandboxes = islo.own_sandboxes()
        except Exception as e:  # noqa: BLE001
            snap.errors["islo"] = str(e)
    if metrics is not None:
        try:
            snap.metrics = metrics.summary()
        except Exception as e:  # noqa: BLE001
            snap.errors["metrics"] = str(e)
    return snap
