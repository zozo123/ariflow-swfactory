"""The self-host line: the factory's own target contract and the line that builds it.

Self-hosting removes the separation every other line relies on. Normally the factory's control
surfaces are unreachable from a work cell because the target is a different repository; with
``dir = ""`` the guard hook, the blueprints, the sandbox implementation and the stage semantics
are all inside the agent's workspace. These tests pin the two things that keep that survivable:
the root ``factory.toml`` refuses the paths that govern future runs, and every entry in it
actually reaches the sandbox instead of being silently reduced to something wider or narrower.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path

import pytest

from swfactory.agent import GUARD_PATH_DENY, guard_deny_rules
from swfactory.blueprint import load
from swfactory.config import TargetContract, protected_for
from swfactory.sandbox import _literal_prefix

ROOT = Path(__file__).resolve().parents[1]

# Editing any of these from inside a work cell is how a self-hosted run would widen its own cage:
# they define the tool policy, the sandbox confinement, and the stage that applies the protected
# list. They must be refused for both writing stages, not just for ``fix``.
CONFINEMENT = (
    "src/swfactory/agent.py",
    "src/swfactory/sandbox.py",
    "src/swfactory/sandbox_contract.py",
    "src/swfactory/stages.py",
    "src/swfactory/security_boundary.py",
    "src/swfactory/security_contract.py",
    "src/swfactory/worker_security.py",
)

# What governs every later run rather than this one: a cell that edits these changes the rules the
# next cell is judged by.
FUTURE_RUNS = (
    "blueprints",
    "dags",
    "scripts",
    "pyproject.toml",
    "uv.lock",
    "CLAUDE.md",
    "config/capability-inventory.json",
)


@pytest.fixture(scope="module")
def contract() -> TargetContract:
    return TargetContract.parse((ROOT / "factory.toml").read_text(encoding="utf-8"))


def test_the_repo_root_is_a_valid_factory_target(contract: TargetContract) -> None:
    """Without this file the factory refuses a root target outright, so it is the keystone."""
    assert contract.test.startswith("uv run")
    assert ".factory/" in contract.junit, "run_tests deletes and re-parses this path"
    assert contract.source == "src"
    assert contract.tests_dir == "tests"


def test_the_test_command_stays_python_only(contract: TargetContract) -> None:
    """``cargo`` cannot run in an islo cell: the gateway allow-list has no crates.io host, and
    CLAUDE.md keeps Rust out of a work cell entirely. Rust is the operator's client, covered by
    CI's own ``rust`` job rather than by the target contract."""
    assert "cargo" not in contract.test
    assert "--junitxml" in contract.test, "a missing report fails the stage even on exit 0"


@pytest.mark.parametrize("entry", CONFINEMENT)
def test_confinement_modules_are_refused_for_both_writing_stages(contract: TargetContract, entry: str) -> None:
    for stage in ("build", "fix"):
        assert entry in protected_for(contract, stage), f"{entry} writable during {stage}"


@pytest.mark.parametrize("entry", FUTURE_RUNS)
def test_paths_governing_future_runs_are_refused(contract: TargetContract, entry: str) -> None:
    assert entry in protected_for(contract, "build")


def test_no_protected_entry_is_widened_by_literal_prefix_reduction(
    contract: TargetContract,
) -> None:
    """srt and docker reduce each entry to its longest wildcard-free prefix, so a glob like
    ``src/**/*.py`` silently becomes ``src`` -- the whole package would go read-only and no build
    stage could edit any code. Every entry here must survive that reduction unchanged."""
    for entry in contract.protected:
        assert _literal_prefix(entry) == entry, (
            f"{entry!r} reduces to {_literal_prefix(entry)!r}: use an exact path, not a glob"
        )


def test_the_trap_this_repo_must_never_fall_into() -> None:
    """Pins the reduction itself, so the guard above cannot rot into a tautology."""
    assert _literal_prefix("src/**/*.py") == "src"
    assert _literal_prefix("src/swfactory/sandbox.py") == "src/swfactory/sandbox.py"


def test_tests_are_writable_for_build_and_refused_for_fix(contract: TargetContract) -> None:
    """ "Fix the code, not the gate" -- but a build stage must still be able to add tests."""
    assert "tests" not in protected_for(contract, "build")
    assert "tests" in protected_for(contract, "fix")


def test_the_protected_list_reaches_the_agent_on_every_backend(contract: TargetContract) -> None:
    """islo and toolset have no ``set_protected``, so kernel enforcement is srt/docker only. The
    list still reaches every backend as Claude Code deny rules, which is what makes it meaningful
    on the production sandbox."""
    rules = guard_deny_rules(protected_for(contract, "build"))
    for entry in CONFINEMENT:
        assert any(entry in rule for rule in rules), f"{entry} absent from deny rules"
    # The hard-coded rules are independent of this contract and must stay that way.
    assert "Edit(factory.toml)" in GUARD_PATH_DENY
    assert "Edit(.github/**)" in GUARD_PATH_DENY


def test_the_selfhost_line_targets_the_repo_root_with_both_gates_human() -> None:
    bp = load(ROOT / "blueprints" / "selfhost.toml")
    assert bp.name == "selfhost"
    assert [(t.repo, t.dir) for t in bp.targets] == [("zozo123/ariflow-swfactory", "")]
    assert [g.after for g in bp.gates] == ["intent", "plan"]
    assert not any(g.auto for g in bp.gates), (
        "an unattended self-edit is the one failure this repo cannot recover from on its own"
    )
    assert bp.sandbox.ttl_s > max(g.timeout_h for g in bp.gates) * 3600


def test_one_line_serves_islo_and_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    """The whole "islo and local docker" requirement rests on ``sandbox`` being an operational
    knob rather than job identity, so the same blueprint runs on both without a second file."""
    bp = load(ROOT / "blueprints" / "selfhost.toml")
    job = bp.jobs({"issues": ["demo/issue.md"]})[0]
    monkeypatch.delenv("SWF_SANDBOX", raising=False)
    assert bp.config(job, run_id="selfhost__manual__t").sandbox == "islo"
    monkeypatch.setenv("SWF_SANDBOX", "docker")
    flipped = bp.config(job, run_id="selfhost__manual__t")
    assert flipped.sandbox == "docker"
    assert flipped.target_dir == "", "the backend must not change which tree is the target"


def test_both_sandbox_backends_are_declared_in_the_capability_inventory() -> None:
    """A self-host line makes an unclaimed production backend visible, so the inventory has to
    name islo even while it stays experimental."""
    claims = json.loads((ROOT / "config" / "capability-inventory.json").read_text(encoding="utf-8"))["claims"]
    by_id = {c["id"]: c for c in claims}
    for claim_id in ("sandbox.islo", "sandbox.docker", "selfhost.factory"):
        assert claim_id in by_id, f"{claim_id} missing from the capability inventory"
        assert by_id[claim_id]["state"] == "experimental"
        assert by_id[claim_id]["follow_up"], "an experimental claim owes a follow_up"
    assert by_id["selfhost.factory"]["evidence"] == [
        "advisory check: control-plane-gate (base-revision protected list vs the PR diff; not required on main)"
    ], "no self-hosted run has been archived yet; do not claim end-to-end evidence"


def test_the_control_plane_gate_reads_the_base_revision() -> None:
    """The gate exists to catch diffs the in-sandbox guard did not produce, and those diffs can
    edit ``factory.toml`` freely. Reading the pull request's own copy would let one commit shrink
    the protected list and edit the newly-unprotected file, passing green."""
    workflow = (ROOT / ".github" / "workflows" / "control-plane-gate.yml").read_text(encoding="utf-8")
    assert 'git show "${BASE_SHA}:factory.toml"' in workflow
    assert 'git cat-file -e "${BASE_SHA}:factory.toml"' in workflow, (
        "a deleted factory.toml must fail the gate, not disarm it"
    )


def test_every_protected_entry_exists(contract: TargetContract) -> None:
    """A stale entry is worse than no entry: docker only mounts paths that exist, so a renamed
    module would drop out of kernel enforcement silently."""
    for entry in contract.protected:
        assert (ROOT / entry).exists(), f"{entry} is protected but not present in the tree"


def _run_gate(tmp_path: Path, changed: str) -> subprocess.CompletedProcess[str]:
    """Execute the gate's embedded matcher against a synthetic diff.

    The logic lives in a YAML heredoc, so it is invisible to this suite unless it is lifted out
    and run. Without this the *enforcement* branch is only ever exercised on a factory-authored
    branch -- which is exactly the branch nobody opens by hand, so a broken gate would sit green
    for as long as it took someone to notice.
    """
    workflow = (ROOT / ".github" / "workflows" / "control-plane-gate.yml").read_text(encoding="utf-8")
    body = workflow.split("python3 - <<'PY'\n", 1)[1].split("\n          PY", 1)[0]
    code = textwrap.dedent(body)
    base = tmp_path / "base-factory.toml"
    base.write_text((ROOT / "factory.toml").read_text(encoding="utf-8"), encoding="utf-8")
    changed_file = tmp_path / "changed.txt"
    changed_file.write_text(changed, encoding="utf-8")
    code = code.replace("/tmp/base-factory.toml", str(base)).replace("/tmp/changed.txt", str(changed_file))
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=repo,
        check=True,
        capture_output=True,
    )


def test_the_gate_ignores_base_drift(tmp_path: Path) -> None:
    """The gate must diff the merge-base, not the base tip.

    A two-dot ``git diff BASE HEAD`` reports commits that are on the base but absent from the head
    as if the pull request had made them, so an un-rebased branch is blamed for files it never
    touched. That is not hypothetical: the collapse PR modifies ``.github/workflows/ci.yml`` and
    ``pyproject.toml``, both protected entries, so once it lands every un-rebased ``factory/*``
    branch would fail a two-dot gate. The lifted-matcher tests cannot catch this because they feed
    the matcher a changed-file list instead of running git, so this one drives real repositories.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    (repo / "factory.toml").write_text((ROOT / "factory.toml").read_text(encoding="utf-8"))
    (repo / "untouched.py").write_text("x = 1\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "base")
    merge_base = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()

    # The pull request: one innocuous file, no protected path.
    _git(repo, "checkout", "-q", "-b", "factory/work")
    (repo / "docs_note.md").write_text("note\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "pr work")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()

    # Meanwhile the base advances, touching a PROTECTED path the branch never saw.
    _git(repo, "checkout", "-q", "main")
    (repo / "pyproject.toml").write_text("[project]\nname='x'\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "base drift into a protected path")
    base_tip = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()

    assert base_tip != merge_base, "the base must have advanced for this test to mean anything"

    def changed(spec: str) -> list[str]:
        out = subprocess.run(
            ["git", "diff", "--name-only", spec],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
        )
        return [line for line in out.stdout.splitlines() if line.strip()]

    two_dot = changed(f"{base_tip}..{head}")
    three_dot = changed(f"{base_tip}...{head}")

    # This is the bug, pinned: two dots invent a protected-path change out of base drift.
    assert "pyproject.toml" in two_dot
    # Three dots see only what the branch actually did.
    assert three_dot == ["docs_note.md"], three_dot
    assert "pyproject.toml" not in three_dot

    workflow = (ROOT / ".github" / "workflows" / "control-plane-gate.yml").read_text(encoding="utf-8")
    assert '"${BASE_SHA}...${HEAD_SHA}"' in workflow, "the gate must use a three-dot diff"


def test_the_gate_refuses_a_factory_authored_control_plane_change(tmp_path: Path) -> None:
    done = _run_gate(tmp_path, "src/swfactory/sandbox.py\nblueprints/selfhost.toml\n")
    assert done.returncode == 1, done.stdout + done.stderr
    assert "src/swfactory/sandbox.py" in done.stdout
    assert "protected by 'blueprints'" in done.stdout, "a directory prefix must match too"


def test_the_gate_permits_ordinary_work(tmp_path: Path) -> None:
    done = _run_gate(tmp_path, "docs/selfhost.md\nsrc/swfactory/metrics.py\n")
    assert done.returncode == 0, done.stdout + done.stderr
    assert "no protected path touched" in done.stdout


def test_the_gate_waives_tests_because_build_may_add_them(tmp_path: Path) -> None:
    """``tests/`` is protected only for ``fix``, so a factory-authored diff may add tests."""
    done = _run_gate(tmp_path, "tests/test_new_thing.py\n")
    assert done.returncode == 0, done.stdout + done.stderr


def test_the_root_contract_is_the_only_new_target_and_demo_still_works() -> None:
    """Self-hosting must not disturb the demo line every other test and the eval suite use."""
    demo = TargetContract.parse((ROOT / "demo" / "target" / "factory.toml").read_text(encoding="utf-8"))
    assert demo.source == "src"
    default = tomllib.loads((ROOT / "blueprints" / "default.toml").read_text(encoding="utf-8"))
    assert default["targets"][0]["dir"] == "demo/target", (
        "the default line must keep pointing at the calculator; selfhost.toml is the root target"
    )


# --------------------------------------------------------------------------------------------
# The liquid line: the same root target on a schedule. Continuous intake, human promotion.
# --------------------------------------------------------------------------------------------


def test_the_liquid_line_is_scheduled_but_cannot_self_approve() -> None:
    """A cron line that rewrites the factory's own source is only inside the doctrine while the
    human release gate holds. `docs/liquid-methodology.md` forbids self-PROMOTION, not self-work,
    so the schedule may decide when work starts and never who decides it ships."""
    bp = load(ROOT / "blueprints" / "liquid.toml")
    assert bp.trigger.kind == "cron"
    assert bp.trigger.cron == "17 6 * * *"
    assert [(t.repo, t.dir) for t in bp.targets] == [("zozo123/ariflow-swfactory", "")]
    assert not any(gate.auto for gate in bp.gates), "a scheduled line must never self-approve"
    assert bp.limits.max_parallel_jobs == 1, "two cells editing our own tree manufacture conflicts"


def test_the_liquid_gates_expire_inside_the_schedule_period() -> None:
    """`dags/blueprints.py` sets catchup=False but no max_active_runs, so gates that outlive the
    period stack runs faster than a human answers them. Shorter gates fail visibly instead."""
    bp = load(ROOT / "blueprints" / "liquid.toml")
    longest_gate_s = max(gate.timeout_h for gate in bp.gates) * 3600
    assert longest_gate_s < 24 * 3600, "a daily line needs sub-daily gates"
    assert bp.sandbox.ttl_s > longest_gate_s, "the cell must outlive its longest gate"


def test_a_scheduled_liquid_run_draws_its_work_from_the_trigger() -> None:
    """A scheduled run has no conf, so the line falls back to its declared backlog."""
    bp = load(ROOT / "blueprints" / "liquid.toml")
    jobs = bp.jobs(None)
    assert [job["issue"] for job in jobs] == bp.trigger.issues
    assert all(job["dir"] == "" for job in jobs), "every job targets the repo root"
