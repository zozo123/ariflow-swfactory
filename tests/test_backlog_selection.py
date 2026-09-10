"""Backlog selection is wired, not decorative: the scheduled fan-out and ``swfactory run`` without
``--issue`` both draw their work from ``intake_governance.drain_line``, every non-selected issue
is recorded with its reason, and a closed issue is refused at intake however it got there."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace

import pytest

from swfactory import cli, runtime
from swfactory import scm as scm_mod
from swfactory.blueprint import Backlog, Blueprint, load, loads
from swfactory.intake_governance import digest, drain_line, prerequisites_of, priority_of
from swfactory.models import Issue, StageError

ROOT = Path(__file__).resolve().parents[1]
LOCAL = {"agent": "scripted", "sandbox": "local", "scm": "local", "approve": "auto"}

BACKLOG_TOML = """
[blueprint]
name = "drain"

[trigger]
kind = "cron"
cron = "0 6 * * *"
start = 2026-01-01T00:00:00+00:00

[trigger.backlog]
label = "liquid"
batch = 1

[[targets]]
repo = "o/r"
dir = ""

[stages]
order = ["intent", "deliver"]

[sandbox]
kind = "local"
"""


def issue(number: int, title: str, body: str = "", *, state: str = "open") -> Issue:
    return Issue(id=str(number), title=title, body=body, url=f"https://github.com/o/r/issues/{number}", state=state)


class FakeSource:
    """What ``GitHubScm`` answers for one repository, without ``gh``."""

    def __init__(self, listed: Sequence[Issue], others: Sequence[Issue] = (), heads: Sequence[str] = ()) -> None:
        self.listed = list(listed)
        self.by_id = {i.id: i for i in [*listed, *others]}
        self.heads = list(heads)
        self.calls: list[tuple] = []

    def list_open_issues(self, label: str, *, limit: int) -> list[Issue]:
        self.calls.append(("list", label, limit))
        return self.listed

    def list_open_pr_heads(self, *, limit: int) -> list[str]:
        self.calls.append(("heads", limit))
        return self.heads

    def fetch_issue(self, ref: str) -> Issue:
        self.calls.append(("fetch", ref))
        return self.by_id[ref]


def blueprint() -> Blueprint:
    return loads(BACKLOG_TOML)


# ---------------------------------------------------------------- the eligibility inputs


def test_priority_and_prerequisites_come_from_the_repo_conventions() -> None:
    assert priority_of("[bug] [P0] Admit cron-triggered work") == 0
    assert priority_of("[P2] Share one backend client") == 2
    assert priority_of("no priority at all") == 9, "unprioritised work sorts last, never first"
    assert prerequisites_of("Parent: #2040\n\nPriority: **P1**.\nDependencies: #1219, #2058, #2065, #2066.\n") == (
        1219,
        2058,
        2065,
        2066,
    )
    assert prerequisites_of("Parent: #2040. Experimental; builds on #2045.") == ()


# ---------------------------------------------------------------- drain_line


def test_drain_line_selects_eligible_open_work_and_records_every_skip(tmp_path: Path) -> None:
    source = FakeSource(
        listed=[
            issue(2067, "[P0] Admit cron work", "Dependencies: #2058, #2065."),
            issue(2069, "[P1] Select eligible backlog", "Dependencies: #2058."),
            issue(2071, "[P1] Reconcile callbacks"),
            issue(2070, "[P1] Bound scheduled runs"),
            issue(2078, "Time machine"),
        ],
        others=[issue(2058, "done", state="closed"), issue(2065, "still open")],
        heads=["factory/2069-0123456789ab", "fix/2048-promotion-gate"],
    )
    selection = drain_line(blueprint(), source=source, root=tmp_path)

    assert [c.issue for c in selection.selected] == [2070], "P1, lowest number, no PR, no open prerequisite"
    assert dict(selection.skipped) == {
        2067: "blocked:2065",
        2069: "implementation-pr-open",
        2071: "batch-limit",
        2078: "batch-limit",
    }
    # the selection's revision is the digest admission pins, so the two can be compared later
    (selected,) = selection.selected
    assert selected.revision == digest({"id": "2070", "title": "[P1] Bound scheduled runs", "body": "", "labels": []})
    # bounded, deterministic reads: one list, one PR scan, one fetch per prerequisite not listed
    assert source.calls == [("list", "liquid", 100), ("heads", 100), ("fetch", "2058"), ("fetch", "2065")]

    record = tmp_path / ".factory" / "backlog" / "drain.jsonl"
    (row,) = [json.loads(line) for line in record.read_text(encoding="utf-8").splitlines()]
    assert row["line"] == "drain" and row["label"] == "liquid" and row["batch"] == 1
    assert row["selected"] == [{"issue": 2070, "priority": 1, "revision": selected.revision}]
    assert row["skipped"] == {
        "2067": "blocked:2065",
        "2069": "implementation-pr-open",
        "2071": "batch-limit",
        "2078": "batch-limit",
    }
    assert row["at"].endswith("Z")

    # a second selection appends: the file is the line's history, not its latest state
    drain_line(blueprint(), source=source, root=tmp_path)
    assert len(record.read_text(encoding="utf-8").splitlines()) == 2


def test_drain_line_treats_an_active_cell_as_taken(tmp_path: Path) -> None:
    source = FakeSource(listed=[issue(2070, "[P1] a"), issue(2071, "[P1] b")])
    selection = drain_line(blueprint(), source=source, root=tmp_path, active={2070})
    assert [c.issue for c in selection.selected] == [2071]
    assert dict(selection.skipped) == {2070: "active-cell"}


def test_drain_line_lets_an_scm_failure_surface_instead_of_an_empty_backlog(tmp_path: Path) -> None:
    class Outage(FakeSource):
        def list_open_issues(self, label: str, *, limit: int) -> list[Issue]:
            raise StageError("scm", "gh issue list failed (rc=1): connection reset", retryable=True)

    with pytest.raises(StageError, match="connection reset"):
        drain_line(blueprint(), source=Outage([]), root=tmp_path)
    assert not (tmp_path / ".factory").exists(), "an outage records nothing: there was no selection"


# ---------------------------------------------------------------- the blueprint seam


def test_blueprint_refuses_two_sources_of_scheduled_work() -> None:
    with pytest.raises(ValueError, match="trigger.issues and trigger.backlog"):
        loads(BACKLOG_TOML.replace('cron = "0 6 * * *"', 'cron = "0 6 * * *"\nissues = ["1"]'))
    with pytest.raises(ValueError, match="one repository"):
        loads(BACKLOG_TOML.replace('repo = "o/r"\ndir = ""', 'repo = "o/r"\ndir = ""\n\n[[targets]]\nrepo = "o/other"'))
    assert Backlog(label="x").scan == 100 and Backlog(label="x").batch == 1


def test_jobs_without_conf_drain_the_backlog_and_an_empty_selection_is_a_no_op() -> None:
    bp = blueprint()
    seen: list[Blueprint] = []

    def fake(line: Blueprint) -> list[str]:
        seen.append(line)
        return ["2070"]

    assert [job["issue"] for job in bp.jobs({}, backlog=fake)] == ["2070"]
    assert [job["issue"] for job in bp.jobs({"issues": []}, backlog=fake)] == ["2070"]
    assert seen == [bp, bp]
    # explicit runtime issues are the manual override: the backlog is not consulted
    assert [job["issue"] for job in bp.jobs({"issues": ["7"]}, backlog=fake)] == ["7"] and len(seen) == 2
    assert bp.jobs({}, backlog=lambda line: []) == [], "nothing eligible is a recorded no-op, not a broken run"


def test_the_liquid_line_declares_a_backlog_instead_of_a_pair_of_numbers() -> None:
    bp = load(ROOT / "blueprints" / "liquid.toml")
    assert bp.trigger.issues == [], "a fixed list goes stale the day one of its issues closes"
    assert bp.trigger.backlog is not None and bp.trigger.backlog.label == "liquid"
    assert bp.trigger.backlog.batch == bp.limits.max_parallel_jobs == 1
    assert not bp.trigger.backlog.label.startswith("factory"), (
        "factory / factory:<line> labels are the webhook's immediate-intake route; a label that "
        "both submits now and enrols for the next tick would start the same issue twice"
    )


# ---------------------------------------------------------------- the CLI submit path


def test_cli_run_without_issues_submits_the_selection_and_prints_the_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    bp = blueprint()
    source = FakeSource(listed=[issue(2070, "[P1] a"), issue(2071, "[P1] b")])
    monkeypatch.setattr(scm_mod, "GitHubScm", lambda repo, base_branch: source)
    ran: list[str] = []
    monkeypatch.setattr(runtime, "ctx_for", lambda cfg, **kw: (ran.append(cfg.issue), SimpleNamespace(cfg=cfg))[1])
    report = SimpleNamespace(stages=[], tests_passed=True, table=lambda: "ok")
    monkeypatch.setattr(cli, "run_ctx", lambda ctx, *args: report)

    cli._run_jobs(bp, [], {**LOCAL, "run_id": "dra1n001"})

    assert ran == ["2070"]
    out = capsys.readouterr().out
    assert "skipped 2071: batch-limit" in out
    assert (tmp_path / ".factory" / "backlog" / "drain.jsonl").is_file()


def test_cli_run_with_nothing_eligible_exits_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(scm_mod, "GitHubScm", lambda repo, base_branch: FakeSource(listed=[]))
    cli._run_jobs(blueprint(), [], {**LOCAL})
    assert "nothing eligible" in capsys.readouterr().out


# ---------------------------------------------------------------- intake refuses closed work


class ClosedScm:
    kind = "local"

    def fetch_issue(self, ref: str) -> Issue:
        return issue(2034, "already merged", state="closed")

    def publish(self, **kw):  # pragma: no cover - never reached
        raise NotImplementedError

    def open_issue(self, **kw):  # pragma: no cover - never reached
        raise NotImplementedError


def test_intake_refuses_a_closed_issue_before_a_sandbox_exists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The race policy: selection saw the issue open, a human closed it before the task ran. The
    task refuses, non-retryable, with no MicroVM started -- and teardown can still run."""
    monkeypatch.chdir(tmp_path)
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["2034"]})
    cfg = runtime.job_config(bp, job, run_id="cl0sed01", overrides=LOCAL, root=tmp_path)
    monkeypatch.setattr(runtime, "make_sandbox", lambda *a, **kw: pytest.fail("a refused issue must not get a sandbox"))
    with pytest.raises(StageError, match="issue 2034 is closed") as info:
        runtime.ctx_for(cfg, blueprint=bp, run_dir=tmp_path / "run", scm_override=ClosedScm())
    assert info.value.kind == "scm" and not info.value.retryable

    monkeypatch.setattr(runtime, "make_sandbox", lambda *a, **kw: SimpleNamespace(name="stub"))
    ctx = runtime.ctx_for(cfg, blueprint=bp, run_dir=tmp_path / "run", scm_override=ClosedScm(), enforce_inputs=False)
    assert ctx.issue.state == "closed", "cleanup still sees the issue: a cell whose issue closed mid-run is not leaked"
