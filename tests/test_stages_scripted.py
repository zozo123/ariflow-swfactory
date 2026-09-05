"""Full scripted run through the same code path as the CLI (``cli.execute``), all local: the
default blueprint (``factory``) builds the ``Config`` and drives the pipeline walk."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from swfactory.blueprint import load
from swfactory.cli import execute
from swfactory.models import RunReport

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "demo" / "target"
ART = "docs/factory/DEMO-1"
RUN_ID = "t3st0001"


@pytest.fixture(autouse=True)
def _isolated_git(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "empty-gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_TERMINAL_PROMPT", "0")
    monkeypatch.chdir(ROOT)


def _tree_digest(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in sorted(root.rglob("*")):
        if p.is_file() and not any(x in p.parts for x in (".venv", "__pycache__", ".factory")):
            out[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=True
    ).stdout


@pytest.fixture(scope="module")
def run(tmp_path_factory: pytest.TempPathFactory) -> tuple[RunReport, Path, dict[str, str]]:
    """One full scripted run shared by the assertions below (~5 s: two real pytest runs)."""
    tmp = tmp_path_factory.mktemp("scripted")
    before = _tree_digest(TARGET)
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})  # 1 issue x 1 target
    cfg = bp.config(
        job,
        run_id=RUN_ID,
        approve="auto",
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(tmp / "work"),
        fixtures_dir="demo/scripted",
    )
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("GIT_CONFIG_GLOBAL", str(tmp / "empty-gitconfig"))
        mp.setenv("GIT_CONFIG_NOSYSTEM", "1")
        mp.chdir(ROOT)
        report = execute(cfg, run_dir=tmp / "run", blueprint=bp)
    return report, tmp, before


def test_artifact_chain_exists(run) -> None:
    _, tmp, _ = run
    art = tmp / "work" / ART
    for name in (
        "intent.md",
        "spec.md",
        "plan.md",
        "plan.json",
        "review.json",
        "approvals.json",
        "metrics.json",
    ):
        assert (art / name).is_file(), name
    envelopes = sorted(p.name for p in (art / "agent").glob("*.json"))
    assert envelopes == [
        "build.1.json",
        "fix.2.json",
        "plan.1.json",
        "review.1.json",
        "spec.1.json",
    ]
    intent = (art / "intent.md").read_text()
    assert intent.startswith("---\nid: DEMO-1\n") and "percent_change" in intent


def test_stage_log_lives_on_the_orchestrator_and_is_committed(run) -> None:
    """<run_dir>/state/stages.jsonl is authoritative; deliver copies it (and the hook log, absent
    for a scripted agent) into {art}/agent/ so the audit trail survives the sandbox."""
    report, tmp, _ = run
    log = [
        json.loads(line)
        for line in (tmp / "run" / "state" / "stages.jsonl").read_text().splitlines()
    ]
    assert [r["stage"] for r in log] == [
        "intent",
        "spec",
        "plan",
        "build_and_test",
        "review",
        "deliver",
    ]
    assert all(r["status"] == "ok" for r in log)
    committed = (tmp / "work" / ART / "agent" / "stages.jsonl").read_text().splitlines()
    assert [json.loads(line)["stage"] for line in committed] == [r["stage"] for r in log[:-1]]
    assert not (tmp / "work" / ART / "agent" / "hooks.jsonl").exists()
    assert report.stages[-1].numbers["denied_tool_calls"] == 0
    # the sandbox keeps a copy (audit only); it is never what a skip decision reads
    assert (tmp / "work" / ".factory" / "stages.jsonl").is_file()
    assert not (tmp / "work" / ".factory" / "built").exists()
    saved = RunReport.model_validate_json((tmp / "run" / "report.json").read_text())
    assert saved == report


def test_rerun_with_same_run_dir_skips_every_stage_but_deliver(run, tmp_path: Path) -> None:
    """Idempotency comes from the orchestrator's log: a second walk of the same run (on a copy of
    its workdir + run dir) re-publishes the chain without a single agent call or test run."""
    report, tmp, _ = run
    shutil.copytree(tmp / "work", tmp_path / "work", ignore=shutil.ignore_patterns(".venv"))
    # A bare Git repository is a live database, not an ordinary directory tree. Clone it rather
    # than recursively copying object files, which can race Git's internal repack/maintenance.
    shutil.copytree(
        tmp / "run",
        tmp_path / "run",
        ignore=shutil.ignore_patterns(".venv", "remote.git"),
    )
    subprocess.run(
        [
            "git",
            "clone",
            "--quiet",
            "--mirror",
            str(tmp / "run" / "remote.git"),
            str(tmp_path / "run" / "remote.git"),
        ],
        check=True,
    )
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    cfg = bp.config(
        job,
        run_id=RUN_ID,
        approve="auto",
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(tmp_path / "work"),
        fixtures_dir=str(tmp_path / "no-fixtures-here"),  # any agent call would FixtureMissing
    )
    again = execute(cfg, run_dir=tmp_path / "run", blueprint=bp)
    assert [(s.stage, s.status) for s in again.stages] == [
        ("intent", "skipped"),
        ("spec", "skipped"),
        ("plan", "skipped"),
        ("build_and_test", "skipped"),
        ("review", "skipped"),
        ("deliver", "ok"),
    ]
    by = {s.stage: s for s in again.stages}
    prior = {s.stage: s for s in report.stages}
    assert by["build_and_test"].numbers == prior["build_and_test"].numbers  # carried over
    assert by["plan"].preview.startswith("# Plan — DEMO-1\n")
    assert again.tests_passed is True and again.total_cost_usd == 0.0
    assert again.pr_url == f"file://{(tmp_path / 'run' / 'pr.md').resolve()}"
    log = [
        json.loads(line)
        for line in (tmp_path / "run" / "state" / "stages.jsonl").read_text().splitlines()
    ]
    assert len(log) == 12 and [r["status"] for r in log[6:]] == ["skipped"] * 5 + ["ok"]


def test_report_numbers(run) -> None:
    report, _, _ = run
    by = {s.stage: s for s in report.stages}
    expected = ["intent", "spec", "plan", "build_and_test", "review", "deliver"]
    assert [s.stage for s in report.stages] == expected
    assert all(s.status == "ok" for s in report.stages)
    build = by["build_and_test"].numbers
    assert build["iterations"] == 2 and build["first_pass_ci"] == 0
    assert build["tests_passed"] == 1 and build["tests_count"] == 7
    assert report.tests_passed is True
    assert report.agent == "scripted" and report.scm == "local"
    assert report.total_cost_usd == 0.0
    assert [(a.gate, a.actor) for a in report.approvals] == [("intent", "auto"), ("plan", "auto")]
    assert report.pr_url == f"file://{(_pr_path(run)).resolve()}"


def test_gate_previews(run) -> None:
    report, _, _ = run
    by = {s.stage: s for s in report.stages}
    intent, plan = by["intent"].preview, by["plan"].preview
    assert intent.startswith("---\nid: DEMO-1\n") and "percent_change" in intent
    assert plan.startswith("# Plan — DEMO-1\n") and "## Files" in plan
    assert all(len(s.preview) <= 4000 for s in report.stages)
    assert not any(by[s].preview for s in ("spec", "build_and_test", "review", "deliver"))


def _pr_path(run) -> Path:
    return run[1] / "run" / "pr.md"


def test_nit_cap_enforced(run) -> None:
    _, tmp, _ = run
    data = json.loads((tmp / "work" / ART / "review.json").read_text())
    sev = [f["severity"] for f in data["findings"]]
    assert sev.count("nit") == 3 and sev.count("major") == 1 and sev.count("blocker") == 0
    assert data["dropped_nits"] == 1 and data["verdict"] == "approve" and data["fixes"] == 0


def test_pr_markdown(run) -> None:
    pr = _pr_path(run).read_text()
    assert "SCRIPTED REPLAY" in pr
    assert "labels: factory, agent-authored" in pr and "factory:blocked" not in pr
    assert "No test for a negative baseline" in pr  # the major
    assert "1 more dropped by the cap of 3" in pr
    assert "| intent | approve | auto |" in pr
    assert "| build_and_test | ok |" in pr


def test_metrics_and_approvals(run) -> None:
    _, tmp, _ = run
    art = tmp / "work" / ART
    metrics = json.loads((art / "metrics.json").read_text())
    assert metrics["agent"] == "scripted" and metrics["run_id"] == RUN_ID
    assert metrics["iterations"] == 2 and metrics["first_pass_ci"] is False
    assert metrics["findings_by_severity"] == {"blocker": 0, "major": 1, "minor": 0, "nit": 3}
    assert metrics["approvers"] == ["auto", "auto"]
    assert metrics["blueprint"] == "factory"
    assert set(metrics["stage_durations_s"]) == {
        "intent",
        "spec",
        "plan",
        "build_and_test",
        "review",
    }
    approvals = json.loads((art / "approvals.json").read_text())
    assert [a["gate"] for a in approvals] == ["intent", "plan"]


def test_bare_remote_has_branch_with_trailers(run) -> None:
    _, tmp, _ = run
    remote = tmp / "run" / "remote.git"
    branch = f"factory/DEMO-1-{RUN_ID}"
    heads = _git("show-ref", "--heads", cwd=remote)
    assert f"refs/heads/{branch}" in heads and "refs/heads/main" in heads
    log = _git("log", "--format=%an%n%B---", f"main..{branch}", cwd=remote)
    commits = [c.strip() for c in log.split("---") if c.strip()]
    assert len(commits) == 3  # build, fix, deliver
    for c in commits:
        assert c.startswith("swfactory-bot\n")
        assert f"Factory-Run: {RUN_ID}" in c and "Agent: scripted" in c
    stages = _git(
        "log", "--format=%(trailers:key=Factory-Stage,valueonly)", f"main..{branch}", cwd=remote
    )
    assert stages.split() == ["deliver", "fix", "build"]
    files = _git("ls-tree", "-r", "--name-only", branch, cwd=remote)
    assert f"{ART}/metrics.json" in files and "tests/test_percent_change.py" in files
    assert ".factory/base" not in files


def test_demo_target_untouched(run) -> None:
    _, _, before = run
    assert _tree_digest(TARGET) == before


def test_document_only_strips_preamble():
    from swfactory.stages import _document_only

    assert _document_only("I read the repo.\n\n# spec.md\n\n## R1\n") == "# spec.md\n\n## R1"
    assert _document_only("# spec.md\nbody\n") == "# spec.md\nbody"
    assert _document_only("no heading at all") == "no heading at all"


# ---------------------------------------------------------------- unit: trust boundary


class _MemSandbox:
    """In-memory Sandbox standing in for an agent-writable workdir."""

    name = "mem"
    workdir = "/work"

    def __init__(self, files: dict[str, str] | None = None) -> None:
        self.files = dict(files or {})
        self.runs: list[str] = []
        self.answers: dict[str, str] = {}  # substring -> stdout

    def ensure(self) -> None: ...

    def close(self) -> None: ...

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800):
        from swfactory.models import RunResult

        self.runs.append(cmd)
        out = next((v for k, v in self.answers.items() if k in cmd), "")
        return RunResult(0, out, "", 0.0)

    def read(self, path: str) -> str:
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def write(self, path: str, content: str) -> None:
        self.files[path] = content

    def exists(self, path: str) -> bool:
        return path in self.files


class _CostlyAgent:
    kind = "scripted"

    def __init__(self, cost: float) -> None:
        self.cost = cost
        self.calls = 0

    def run(self, sb, **kw):
        from swfactory.models import AgentResult

        self.calls += 1
        return AgentResult(agent="scripted", text="# spec\n", cost_usd=self.cost)


def _unit_ctx(tmp_path: Path, sb: _MemSandbox, agent=None, *, seed_artifacts: bool = True, **cfg):
    from swfactory.config import Config
    from swfactory.models import Issue
    from swfactory.stages import Ctx

    ctx = Ctx(
        cfg=Config(issue="x", **cfg),
        sb=sb,
        agent=agent,
        scm=None,  # type: ignore[arg-type]
        issue=Issue(id="X-1", title="t", body="b"),
        run_dir=tmp_path / "run",
    )
    if seed_artifacts:
        for path, content in sb.files.items():
            if path.startswith(f"{ctx.art}/"):
                ctx.state.write_artifact(path, content)
    return ctx


def _log(tmp_path: Path, *records: dict) -> None:
    from swfactory.models import StageResult

    state = tmp_path / "run" / "state"
    state.mkdir(parents=True, exist_ok=True)
    (state / "stages.jsonl").write_text(
        "".join(StageResult(**r).model_dump_json() + "\n" for r in records)
    )


def test_forged_sandbox_artifact_does_not_skip_a_stage(tmp_path: Path) -> None:
    """The agent can write docs/factory/**; only the orchestrator's log may skip a stage."""
    from swfactory.stages import intent

    sb = _MemSandbox({"docs/factory/X-1/intent.md": "forged\n"})
    result = intent(_unit_ctx(tmp_path, sb, seed_artifacts=False))
    assert result.status == "ok" and sb.files["docs/factory/X-1/intent.md"].startswith("---\n")
    assert (tmp_path / "run" / "state" / "stages.jsonl").read_text().count("\n") == 1
    # ... and a logged completion skips even when the sandbox lost the artifact
    again = intent(_unit_ctx(tmp_path, _MemSandbox()))
    assert again.status == "skipped" and again.artifacts == ["docs/factory/X-1/intent.md"]
    assert again.preview.startswith("---\nid: X-1\n") and again.cost_usd == 0.0


def test_run_budget_is_seeded_from_the_orchestrator_log(tmp_path: Path) -> None:
    """Earlier tasks/processes of the run count: spec (7.5) + this call (1.0) > 8.0 ceiling."""
    from swfactory.models import StageError
    from swfactory.stages import _agent, load_stage_results, seed_budget

    _log(tmp_path, {"stage": "intent"}, {"stage": "spec", "cost_usd": 7.5})
    agent = _CostlyAgent(1.0)
    contract = '[commands]\ntest = "pytest"\n[paths]\nsource = "src"\ntests = "tests"\n'
    ctx = _unit_ctx(tmp_path, _MemSandbox({"factory.toml": contract}), agent, max_budget_usd=8.0)
    with pytest.raises(StageError, match=r"run budget exceeded: 8.50 > 8.00"):
        _agent(ctx, "plan", 1, "prompt", None)
    assert agent.calls == 1 and ctx.budget_seeded and ctx.spent_usd == 8.5
    assert seed_budget(ctx) == 8.5  # idempotent: seeded once per process
    ok = _unit_ctx(
        tmp_path,
        _MemSandbox({"factory.toml": contract}),
        _CostlyAgent(0.4),
        max_budget_usd=8.0,
    )
    assert _agent(ok, "plan", 1, "prompt", None).cost_usd == 0.4 and ok.spent_usd == 7.9
    assert [r.stage for r in load_stage_results(ok)] == ["intent", "spec"]


def test_plan_fidelity_reports_both_halves_of_pass_4(tmp_path: Path) -> None:
    from swfactory.stages import _plan_fidelity

    sb = _MemSandbox(
        {
            "factory.toml": '[commands]\ntest = "uv run pytest"\n[paths]\ntests = "tests"\n',
            "docs/factory/X-1/plan.json": json.dumps(
                {"files": ["src/a.py", "tests/test_a.py", "README.md"], "steps": [], "tests": []}
            ),
        }
    )
    sb.answers["git diff --name-only"] = "src/a.py\nsrc/extra.py\n"
    findings = _plan_fidelity(_unit_ctx(tmp_path, sb), "base0000")
    assert [(f.severity, f.file, f.title) for f in findings] == [
        ("major", "src/extra.py", "Plan fidelity: file not listed in plan.md"),
        ("minor", "README.md", "Plan fidelity: planned file not touched"),
        ("major", "tests/test_a.py", "Plan fidelity: planned file not touched"),
    ]
    assert _plan_fidelity(_unit_ctx(tmp_path / "missing", _MemSandbox()), "base0000") == []


def test_execute_passes_the_targets_protected_globs_to_make_sandbox(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Host sandboxes: factory.toml is read from the seeded workdir before make_sandbox so srt can
    enforce `protected` at the kernel (denyWrite). Build-level: the tests dir stays writable
    (build must add tests); `_agent` tightens it for fix calls. Patched on ``runtime``, the one
    module that assembles a Ctx for both the CLI and every DAG task."""
    from swfactory import runtime

    assert runtime.protected_globs(TARGET) == ["factory.toml"]
    assert runtime.protected_globs(TARGET, "fix") == ["factory.toml", "tests"]
    assert runtime.protected_globs(tmp_path) == []
    seen: dict[str, object] = {}

    class _Stop(Exception): ...

    def fake_make_sandbox(cfg, issue_id, **kw):
        seen.update(kw, issue_id=issue_id)
        raise _Stop

    monkeypatch.setattr(runtime, "make_sandbox", fake_make_sandbox)
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    cfg = bp.config(
        job,
        run_id="pr0tect1",
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(tmp_path / "work"),
    )
    with pytest.raises(_Stop):
        execute(cfg, run_dir=tmp_path / "run", blueprint=bp)
    # repo travels too: it is what makes an islo sandbox name unique per (issue, target).
    assert seen == {
        "issue_id": "DEMO-1",
        "protected": ["factory.toml"],
        "repo": "zozo123/ariflow-swfactory",
        "run_dir": (tmp_path / "run").resolve(),
    }


def test_agent_call_tightens_srt_deny_write_per_stage(tmp_path: Path) -> None:
    """Under srt the kernel denyWrite set follows the stage: build may write tests/, fix may not.
    Host-side only (settings file), no srt binary involved."""
    from swfactory.agent import ScriptedAgent
    from swfactory.config import Config
    from swfactory.models import AgentResult, Issue
    from swfactory.sandbox import SrtSandbox
    from swfactory.scm import LocalGitScm
    from swfactory.stages import Ctx, _agent

    work = tmp_path / "work"
    work.mkdir()
    (work / "tests").mkdir()
    shutil.copy(TARGET / "factory.toml", work / "factory.toml")
    sb = SrtSandbox(work, allowed_domains=(), protected=("factory.toml",))
    seen: list[tuple[str, tuple[str, ...]]] = []

    class _Agent(ScriptedAgent):
        def run(self, sb, *, stage, **kw):  # type: ignore[override]
            seen.append((stage, sb.protected))
            return AgentResult(agent="scripted", text="ok", data={})

    cfg = Config(issue="x", sandbox="srt", workdir=str(work))
    ctx = Ctx(
        cfg=cfg,
        sb=sb,
        agent=_Agent([]),
        scm=LocalGitScm(tmp_path / "remote.git", tmp_path / "run"),
        issue=Issue(id="X-1", title="t", body="b"),
        run_dir=tmp_path / "run",
    )
    _agent(ctx, "build", 1, "p", None)
    _agent(ctx, "fix", 2, "p", None)
    assert seen == [("build", ("factory.toml",)), ("fix", ("factory.toml", "tests"))]
    deny = json.loads(sb.settings_path.read_text())["filesystem"]["denyWrite"]
    assert str(work / "tests") in deny
