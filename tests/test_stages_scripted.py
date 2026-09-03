"""Full scripted run through the same code path as the CLI (``cli.execute``), all local: the
default blueprint (``factory``) builds the ``Config`` and drives the pipeline walk."""

from __future__ import annotations

import hashlib
import json
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
