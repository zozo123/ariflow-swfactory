"""An installed external executable traverses the real factory stages and local delivery.

The fixture is a subprocess with no swfactory imports: unlike ScriptedAgent, it crosses the
request/response boundary that an independently installed coding harness must implement.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from support import git

from swfactory import stages
from swfactory.approval_policy import SCRIPTED_REPLAY_FIXTURE
from swfactory.blueprint import load
from swfactory.call_accounting import CallLedger
from swfactory.cli import execute
from swfactory.models import RunReport
from swfactory.models import TestResult as VerificationResult
from swfactory.publication_identity import publication_key
from swfactory.state import RunState

ROOT = Path(__file__).resolve().parents[1]
EXECUTABLE = ROOT / "tests" / "fixtures" / "external_agent" / "fixture.py"
ART = "docs/factory/DEMO-1"
CALLS = [("spec", 1), ("plan", 1), ("build", 1), ("fix", 2), ("review", 1)]
RUN_ID = "external001"


def write_profile(path: Path) -> Path:
    """Operator-owned test profile; its no-charge fixture never reaches a model provider."""
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "id": "external-fixture",
                "version": "1",
                "argv": [str(EXECUTABLE)],
                "model": "fixture",
                "credential_env": [],
                "budget_mode": "no_charge",
                "enforces_usd_limit": False,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


@pytest.fixture(scope="module")
def external_run(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[RunReport, Path, list[tuple[VerificationResult, str]]]:
    tmp = tmp_path_factory.mktemp("external")
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    cfg = bp.config(
        job,
        run_id=RUN_ID,
        agent="external",
        agent_profile=str(write_profile(tmp / "profile.json")),
        sandbox="local",
        allow_local_agent=True,
        scm="local",
        gate_replay=str(SCRIPTED_REPLAY_FIXTURE),
        workdir=str(tmp / "work"),
    )
    observations: list[tuple[VerificationResult, str]] = []
    real_tests = stages.run_tests

    def observe_tests(ctx):
        result, output = real_tests(ctx)
        assert result.junit_path is not None
        observations.append((result, ctx.sb.read(result.junit_path)))
        return result, output

    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(ROOT)
        mp.setattr(stages, "run_tests", observe_tests)
        report = execute(cfg, run_dir=tmp / "run", blueprint=bp)
    return report, tmp, observations


def test_external_executable_builds_repairs_and_delivers(external_run) -> None:
    report, tmp, observations = external_run
    assert report.agent == "external" and report.scm == "local"
    assert [(stage.stage, stage.status) for stage in report.stages] == [
        (name, "ok") for name in ("intent", "spec", "plan", "build_and_test", "review", "deliver")
    ]
    build = next(stage for stage in report.stages if stage.stage == "build_and_test")
    assert build.numbers["iterations"] == 2 and build.numbers["first_pass_ci"] == 0
    assert report.tests_passed and report.total_cost_usd == 0
    assert report.unreconciled_cost_usd == 0
    # These are the real target subprocess runs and freshly parsed reports, including a failed
    # first build. A success string from the external executable cannot supply this evidence.
    first, first_xml = observations[0]
    repaired, repaired_xml = observations[1]
    assert first.report_valid and not first.ok and first.failed == 2
    assert repaired.report_valid and repaired.ok and repaired.passed == 7
    assert first_xml != repaired_xml and "<failure" in first_xml and "<failure" not in repaired_xml
    assert all(result.ok for result, _ in observations[1:])
    art = tmp / "work" / ART
    assert "return (new - old) / old" in (tmp / "work" / "src" / "calc" / "core.py").read_text()
    assert sorted(path.name for path in (art / "agent").glob("*.json")) == [
        "build.1.json",
        "fix.2.json",
        "plan.1.json",
        "review.1.json",
        "spec.1.json",
    ]
    assert [(approval.gate, approval.actor, approval.mode) for approval in report.approvals] == [
        ("intent", "replay:scripted-replay", "replay"),
        ("plan", "replay:scripted-replay", "replay"),
    ]
    assert (tmp / "run" / "pr.md").is_file()
    assert RunReport.model_validate_json((tmp / "run" / "report.json").read_text()) == report


def test_external_call_ids_and_profile_identity_are_host_receipts(external_run) -> None:
    report, tmp, _ = external_run
    state = RunState(tmp / "run")
    ledger = CallLedger(state)
    records = ledger.records()
    assert [(record.stage, record.iteration) for record in records] == CALLS
    assert len({record.call_id for record in records}) == len(CALLS)
    assert ledger.unreconciled() == [] and ledger.charged_usd() == 0
    for record in records:
        envelope = json.loads(state.read_artifact(f"{ART}/agent/{record.stage}.{record.iteration}.json"))
        assert envelope["authority"] == "factory" and envelope["agent"] == "external"
        assert envelope["call_id"] == record.call_id
        assert envelope["accepted_inputs_digest"] == report.inputs_digest
        binding = envelope["profile"]
        assert binding["manifest"]["id"] == "external-fixture"
        assert binding["manifest"]["model"] == "fixture" and binding["manifest"]["version"] == "1"
        assert binding["executable_sha256"] == hashlib.sha256(EXECUTABLE.read_bytes()).hexdigest()
        assert len(binding["manifest_sha256"]) == 64 and binding["docker_image"] is None
        assert "text" not in envelope and "usage" not in envelope
        receipt_name = f"external-agent/{hashlib.sha256(record.call_id.encode()).hexdigest()}.json"
        receipt = json.loads(state.read_control(receipt_name))
        assert receipt["call_id"] == record.call_id and receipt["profile_id"] == "external-fixture"
        assert receipt["candidate"]["call_id"] == record.call_id
        assert receipt["candidate"]["profile_id"] == "external-fixture"
        assert receipt["candidate"]["status"] == "success"
        assert record.settled_usd == 0


def test_factory_owns_external_run_commits_and_bare_remote(external_run) -> None:
    _, tmp, _ = external_run
    remote = tmp / "run" / "remote.git"
    branch = f"factory/DEMO-1-{publication_key('zozo123/ariflow-swfactory', 'demo/target', 'DEMO-1')}"
    assert f"refs/heads/{branch}" in git(remote, "show-ref", "--heads")
    commits = git(remote, "log", "--format=%an%n%B---", f"main..{branch}").split("---")
    commits = [commit.strip() for commit in commits if commit.strip()]
    assert len(commits) == 3  # factory-managed build, fix, and delivery
    for commit in commits:
        assert commit.startswith("swfactory-bot\n")
        assert f"Factory-Run: {RUN_ID}" in commit and "Agent: external" in commit
    assert git(remote, "log", "--format=%(trailers:key=Factory-Stage,valueonly)", f"main..{branch}").split() == [
        "deliver",
        "fix",
        "build",
    ]
    published = git(remote, "ls-tree", "-r", "--name-only", branch).split()
    assert f"{ART}/metrics.json" in published and "tests/test_percent_change.py" in published
