"""Review fix path and the factory:blocked path, driven by tests/fixtures/blocker."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from swfactory.agent import ScriptedAgent
from swfactory.cli import execute
from swfactory.config import Config
from swfactory.models import RunReport

ROOT = Path(__file__).resolve().parents[1]
ART = "docs/factory/DEMO-1"
FIXTURES = [ROOT / "tests" / "fixtures" / "blocker", ROOT / "demo" / "scripted"]


@pytest.fixture(autouse=True)
def _isolated_git(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "empty-gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.chdir(ROOT)


def _run(tmp_path: Path, **overrides: object) -> tuple[RunReport, Path]:
    cfg = Config(
        issue="demo/issue.md",
        approve="auto",
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(tmp_path / "work"),
        run_id="b10ck3r1",
        **overrides,
    )
    report = execute(cfg, run_dir=tmp_path / "run", agent=ScriptedAgent(FIXTURES))
    return report, tmp_path


def test_blocker_is_fixed_and_re_reviewed(tmp_path: Path) -> None:
    report, tmp = _run(tmp_path)  # max_review_fixes defaults to 1
    by = {s.stage: s for s in report.stages}
    assert by["review"].status == "ok" and by["deliver"].status == "ok"
    assert by["review"].numbers["fixes"] == 1 and by["review"].numbers["blockers"] == 0
    assert by["review"].numbers["tests_passed"] == 1
    assert report.tests_passed is True
    art = tmp / "work" / ART
    review = json.loads((art / "review.json").read_text())
    assert review["verdict"] == "approve" and review["fixes"] == 1
    assert [f["severity"] for f in review["findings"]] == ["nit"]
    envelopes = sorted(p.name for p in (art / "agent").glob("*.json"))
    assert "fix.1.json" in envelopes and "review.2.json" in envelopes
    core = (tmp / "work" / "src" / "calc" / "core.py").read_text()
    assert "A decrease is negative" in core  # fix.1.patch landed
    pr = (tmp / "run" / "pr.md").read_text()
    assert "factory:blocked" not in pr and "[BLOCKED]" not in pr
    assert "| fix | " in pr or "fixes=1" in pr


def test_exhausted_fixes_deliver_blocked(tmp_path: Path) -> None:
    report, tmp = _run(tmp_path, max_review_fixes=0)
    by = {s.stage: s for s in report.stages}
    assert by["review"].status == "blocked" and by["deliver"].status == "blocked"
    assert by["review"].numbers["blockers"] == 1 and by["review"].numbers["fixes"] == 0
    assert report.tests_passed is True  # build passed; the block is a review verdict
    review = json.loads((tmp / "work" / ART / "review.json").read_text())
    assert review["verdict"] == "request_changes"
    pr = (tmp / "run" / "pr.md").read_text()
    assert pr.startswith("# [BLOCKED] DEMO-1:")
    assert "labels: factory, agent-authored, factory:blocked" in pr
    assert "### Blocker (1)" in pr
    assert pr.index("### Blocker") < pr.index("### Nit")
