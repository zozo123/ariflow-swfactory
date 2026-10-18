"""End to end: a parallel-safe ``Plan.work`` wave fans out over boat.dev forks and back in.

Everything real except the network: the real ``stages.setup`` provisions a real ``BoatSandbox``,
the real ``work_stage.build_and_test`` decides per wave, ``fork_wave`` parks the parent, forks one
child per node, ``WorkExecutor`` fans out and back in, real git applies every patch, and the
target's own test command verifies the merged head. ``HostBoatClient`` is the only fake: it is
boat.dev's lifecycle as it was measured live (see ``swfactory.boat``) over host directories and
real bash -- a running sandbox has no completed snapshot and refuses to fork, ``stop`` takes one,
each fork copies it, an ``Idempotency-Key`` returns the child it already made.

The same scenario runs against the real service when asked to::

    SWF_TEST_LIVE_BOAT=1 BOAT_API_KEY=... uv run pytest tests/test_fork_wave_e2e.py -k live

It clones this repository's ``SWF_TEST_LIVE_BOAT_BRANCH`` (default ``main``) on boat.dev, so the
fixture target must be pushed there; it creates four small VMs and confirms every one archived.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from swfactory import fork_wave, stages, work_stage
from swfactory.boat import BoatError, BoatExecResult, BoatSandbox, boat_client_from_env
from swfactory.call_accounting import CallLedger
from swfactory.config import Config
from swfactory.models import AgentResult, Issue, Plan, PlanTask, StageError
from swfactory.stages import Ctx
from swfactory.state import RunState

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = "tests/fixtures/fork_target"
REPO = "zozo123/ariflow-swfactory"
ISSUE_ID = "FORK-1"
ART = f"docs/factory/{ISSUE_ID}"
SIBLINGS = ("alpha", "beta", "gamma")
VM_ROOT = "/swf-boat-vm"  # every BoatSandbox path starts here; HostBoatClient maps it per VM


# ---------------------------------------------------------------- boat.dev, as measured


class HostBoatClient:
    """The ``BoatClient`` surface over host directories: one directory per VM, real bash per exec.

    ``stale_snapshot`` makes forks come from the snapshot before the latest one -- what a
    periodic snapshot older than HEAD would hand a child.
    """

    def __init__(self, base: Path, *, sources: dict[str, str]) -> None:
        self.base = base
        self.sources = sources
        self.vms: dict[str, dict[str, Any]] = {}
        self.idempotent: dict[str, str] = {}
        self.forks: list[dict[str, Any]] = []
        self.resumes: list[str] = []
        self.stale_snapshot = False
        self.refuse_forks: int | None = None  # answer every fork with this 4xx, as a start-rate limit does
        self.lock = threading.Lock()

    def _new(self, *, origin: Path | None = None, parent: str | None = None) -> str:
        with self.lock:
            sandbox_id = f"bx_{len(self.vms) + 1:04d}"
            home = self.base / sandbox_id
            if origin is None:
                home.mkdir(parents=True)
            else:
                shutil.copytree(origin, home, symlinks=True)
            self.vms[sandbox_id] = {"state": "ready", "dir": home, "snapshots": [], "parent": parent}
        return sandbox_id

    def _vm(self, sandbox_id: str) -> dict[str, Any]:
        if sandbox_id not in self.vms:
            raise BoatError("boat.dev get failed with HTTP 404", status=404)
        return self.vms[sandbox_id]

    def _host(self, sandbox_id: str, text: str) -> str:
        text = text.replace(VM_ROOT, str(self._vm(sandbox_id)["dir"]))
        for url, local in self.sources.items():
            text = text.replace(url, local)
        return text

    def _guest(self, sandbox_id: str, text: str) -> str:
        return text.replace(str(self._vm(sandbox_id)["dir"]), VM_ROOT)

    def create(self, *, ttl_s: int, machine_type: str = "small") -> str:
        return self._new()

    def wait_ready(self, sandbox_id: str) -> None:
        state = self._vm(sandbox_id)["state"]
        if state != "ready":
            raise BoatError(f"boat sandbox {sandbox_id} entered state {state}")

    def exec(self, sandbox_id: str, command: str, *, cwd: str | None = None, timeout_s: int | None = None):
        vm = self._vm(sandbox_id)
        if vm["state"] != "ready":
            raise BoatError("boat.dev exec failed with HTTP 409", status=409)
        # boat: "Path must resolve under /home/user or /tmp" and "cwd must be an existing directory".
        if cwd is not None and not (cwd == VM_ROOT or cwd.startswith(f"{VM_ROOT}/")):
            raise BoatError("boat.dev exec failed with HTTP 400", status=400)
        where = self._host(sandbox_id, cwd) if cwd else str(vm["dir"])
        if not Path(where).is_dir():
            raise BoatError("boat.dev exec failed with HTTP 400", status=400)
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(vm["dir"]),
            "LANG": "C.UTF-8",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
        }
        done = subprocess.run(
            ["bash", "-c", self._host(sandbox_id, command)],
            cwd=where,
            env=env,
            capture_output=True,
            text=True,
            timeout=min(timeout_s or 600, 600),
            check=False,
        )
        return BoatExecResult(
            exit_code=done.returncode,
            stdout=self._guest(sandbox_id, done.stdout),
            stderr=self._guest(sandbox_id, done.stderr),
        )

    def read_file(self, sandbox_id: str, path: str) -> str:
        target = Path(self._host(sandbox_id, path))
        if not target.is_file():
            raise BoatError("boat.dev read file failed with HTTP 404", status=404)
        return target.read_text(encoding="utf-8")

    def write_file(self, sandbox_id: str, path: str, content: bytes) -> None:
        Path(self._host(sandbox_id, path)).write_bytes(content)

    def stop(self, sandbox_id: str) -> None:
        vm = self._vm(sandbox_id)
        with self.lock:
            if vm["state"] == "archived":
                return
            snapshot = self.base / f"{sandbox_id}.snap{len(vm['snapshots'])}"
            shutil.copytree(vm["dir"], snapshot, symlinks=True)
            vm["snapshots"].append(snapshot)
            vm["state"] = "archived"

    def wait_stopped(self, sandbox_id: str) -> None:
        if self._vm(sandbox_id)["state"] != "archived":
            raise BoatError(f"boat sandbox {sandbox_id} is still running")

    def fork(self, sandbox_id: str, *, ttl_s: int, idempotency_key: str) -> str:
        if self.refuse_forks is not None:
            raise BoatError(f"boat.dev fork failed with HTTP {self.refuse_forks}", status=self.refuse_forks)
        with self.lock:
            if idempotency_key in self.idempotent:
                return self.idempotent[idempotency_key]
        vm = self._vm(sandbox_id)
        snapshots = vm["snapshots"]
        if vm["state"] != "archived" or not snapshots:
            raise BoatError("boat.dev fork failed with HTTP 409", status=409)
        origin = snapshots[-2] if self.stale_snapshot and len(snapshots) > 1 else snapshots[-1]
        child = self._new(origin=origin, parent=sandbox_id)
        with self.lock:
            self.idempotent[idempotency_key] = child
            self.forks.append({"parent": sandbox_id, "child": child, "ttl_s": ttl_s, "key": idempotency_key})
        return child

    def resume(self, sandbox_id: str) -> None:
        vm = self._vm(sandbox_id)
        with self.lock:
            self.resumes.append(sandbox_id)
            vm["state"] = "ready"

    # -- test views ------------------------------------------------------
    def children(self) -> list[str]:
        return [row["child"] for row in self.forks]

    def state(self, sandbox_id: str) -> str:
        return self._vm(sandbox_id)["state"]


class ForkAgent:
    """Writes its node's file into whichever cell it is handed. The siblings meet at a barrier, so
    a run that did not really execute them at once cannot pass; ``index`` writes what fanned in."""

    kind = "scripted"

    def __init__(
        self,
        *,
        fail: str | None = None,
        stray: str | None = None,
        rename: str | None = None,
        timeout_s: float = 60.0,
    ) -> None:
        self.barrier = threading.Barrier(len(SIBLINGS), timeout=timeout_s)
        self.fail = fail
        self.stray = stray
        self.rename = rename
        self.calls: list[tuple[str, int, str | None]] = []
        self.lock = threading.Lock()

    def run(self, sb: Any, *, stage: str, iteration: int, prompt: str = "", **_: Any) -> AgentResult:
        node = match.group(1) if (match := re.search(r"Node: `([^`]+)`", prompt)) else stage
        with self.lock:
            self.calls.append((node, iteration, getattr(sb, "sandbox_id", None)))
        if node in SIBLINGS:
            self.barrier.wait()
            if self.fail == node:
                return AgentResult(agent="scripted", is_error=True, subtype="error", text=f"{node} broke")
            if self.fail is not None:
                _until_halted(sb)  # a real agent is a process in the VM; halting the VM is what stops it
            if self.rename == node:  # "creates" its file by moving one nobody declared
                sb.run(f"git mv src/README src/{node}.txt")
            sb.write(f"src/{node}.txt", f"written by {node} in a forked cell\n")
            if self.stray == node:
                sb.write("src/stray.txt", "outside the declared scope\n")
        elif node == "index":
            present = [name for name in SIBLINGS if sb.exists(f"src/{name}.txt")]
            sb.write("src/index.txt", " ".join(present) + "\n")
        return AgentResult(agent="scripted", data={"summary": f"{node} done"}, cost_usd=0.05)


def _until_halted(sb: Any, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            sb.run("true")
        except BoatError:
            return
        time.sleep(0.02)
    raise AssertionError("a sibling of a failed node was never halted")


class NoScm:
    kind = "local"


def _plan() -> Plan:
    work = [
        PlanTask(id=name, title=f"write {name}", files=[f"src/{name}.txt"], parallel_safe=True) for name in SIBLINGS
    ]
    work.append(PlanTask(id="index", title="index what fanned in", depends_on=list(SIBLINGS), files=["src/index.txt"]))
    return Plan(
        files=sorted({path for task in work for path in task.files}), steps=["fan out"], tests=["check"], work=work
    )


def _source_repo(tmp_path: Path) -> str:
    """A git repository shaped like this one where it matters: the target in the same subdir."""
    work = tmp_path / "source"
    shutil.copytree(ROOT / FIXTURE, work / FIXTURE)
    env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"]
    subprocess.run(["git", "init", "-q", "-b", "main", str(work)], check=True, env=env)
    subprocess.run([*git, "-C", str(work), "add", "-A"], check=True, env=env)
    subprocess.run([*git, "-C", str(work), "commit", "-qm", "fixture"], check=True, env=env)
    return f"file://{work}"


def _ctx(tmp_path: Path, client: Any, agent: ForkAgent, *, branch: str = "main", **cfg: Any) -> Ctx:
    config = Config(
        issue=ISSUE_ID,
        run_id="forkrun1",
        sandbox="boat",
        agent="scripted",
        repo=REPO,
        target_dir=FIXTURE,
        base_branch=branch,
        max_build_iterations=1,
        **{"max_parallel_forks": 3, **cfg},
    )
    run_dir = tmp_path / "run"
    sandbox = BoatSandbox(
        "swf-fork-1",
        client=client,
        repo=REPO,
        base_branch=branch,
        target_dir=FIXTURE,
        ttl_s=1800,
        state=RunState(run_dir),
        **({"workdir_root": f"{VM_ROOT}/workspace"} if isinstance(client, HostBoatClient) else {}),
    )
    ctx = Ctx(
        cfg=config,
        sb=sandbox,
        agent=agent,  # type: ignore[arg-type]
        scm=NoScm(),  # type: ignore[arg-type]
        issue=Issue(id=ISSUE_ID, title="fan out", body="three siblings, one index"),
        run_dir=run_dir,
    )
    stages.setup(ctx)
    plan = _plan()
    ctx.write_artifact(f"{ART}/spec.md", "# spec\n")
    ctx.write_artifact(f"{ART}/plan.md", "# plan\n")
    ctx.write_artifact(f"{ART}/plan.json", plan.model_dump_json())
    return ctx


def _host_ctx(tmp_path: Path, agent: ForkAgent, **cfg: Any) -> tuple[Ctx, HostBoatClient]:
    client = HostBoatClient(tmp_path / "vms", sources={f"https://github.com/{REPO}.git": _source_repo(tmp_path)})
    return _ctx(tmp_path, client, agent, **cfg), client


def _report(ctx: Ctx) -> dict[str, Any]:
    return json.loads(ctx.state.read_artifact(f"{ART}/workgraph-execution.json"))


def _progress(ctx: Ctx) -> dict[str, Any]:
    return json.loads(ctx.state.read_control(fork_wave.PROGRESS_FILE))


def _log(ctx: Ctx) -> list[str]:
    return stages._sh(ctx, "git log --format=%s -5").splitlines()


def _assert_fanned_out_and_in(ctx: Ctx, agent: ForkAgent, parent: str, children: list[str]) -> dict[str, Any]:
    report = _report(ctx)
    assert report["mode"] == "fork_wave_parallel" and report["parallel"] is True
    assert report["provider"] == "boat" and report["reason"] == "forked_parallel_safe_waves"
    assert report["parallel_nodes"] == 3
    assert report["verified"]["ok"] is True and report["verified"]["head"] == report["final_head"]

    decisions = {row["layer"]: row for row in report["wave_decisions"]}
    assert decisions[0]["fork"] is True and decisions[0]["nodes"] == list(SIBLINGS)
    assert decisions[1] == {"layer": 1, "nodes": ["index"], "fork": False, "reason": "single_node_wave"}

    (wave,) = report["fork_waves"]
    checkpoint = wave["checkpoint"]
    assert wave["parallel"] is True and wave["teardown"] == "confirmed" and wave["fork_parent"] == parent
    assert [m["node_id"] for m in wave["merges"]] == list(SIBLINGS)  # stable fan-in, not completion order
    assert all(m["transform"] == "patch_apply" and m["requires_reverify"] for m in wave["merges"])
    assert wave["merges"][0]["target_before"] == checkpoint
    rows = {row["node_id"]: row for row in wave["children"]}
    assert sorted(rows) == list(SIBLINGS)
    assert sorted(row["child"] for row in rows.values()) == sorted(children)
    assert len(set(children)) == 3 and parent not in children
    for name, row in rows.items():
        assert row["reaped"] is True and row["child_input_head"] == checkpoint
        assert row["touched"] == [f"src/{name}.txt"] and row["key"].startswith("swf-fork-")

    nodes = {row["node_id"]: row for row in report["nodes"]}
    for name in SIBLINGS:
        lineage = nodes[name]["fork"]
        assert lineage["fork_parent"] == parent and lineage["checkpoint"] == checkpoint
        assert lineage["child"] == rows[name]["child"] and lineage["patch_blob"] == rows[name]["patch_blob"]
        assert nodes[name]["changed_files"] == [f"src/{name}.txt"]
    assert "fork" not in nodes["index"]  # wave 1 ran serially, in the parent, after the fan-in

    ran_in = {node: sandbox for node, _, sandbox in agent.calls}
    assert {ran_in[name] for name in SIBLINGS} == set(children)
    assert ran_in["index"] == parent
    calls = [record.iteration for record in CallLedger(ctx.state).records()]
    assert sorted(calls) == [1, 2, 3, 4]  # one ledger row per agent call, none shared by siblings
    assert _log(ctx)[:4] == [
        "work index: index done",
        "work gamma: gamma done",
        "work beta: beta done",
        "work alpha: alpha done",
    ]
    assert ctx.sb.read("src/index.txt") == "alpha beta gamma\n"
    return report


# ---------------------------------------------------------------- hermetic end to end


def test_a_parallel_safe_wave_forks_runs_at_once_fans_in_by_id_and_verifies_the_merge(tmp_path: Path) -> None:
    agent = ForkAgent()
    ctx, client = _host_ctx(tmp_path, agent)
    parent = ctx.sb.sandbox_id

    result = work_stage.build_and_test(ctx)

    assert result.numbers["parallel_nodes"] == 3 and result.numbers["work_nodes"] == 4
    _assert_fanned_out_and_in(ctx, agent, parent, client.children())
    assert client.resumes == [parent]  # parked once, for the one forked wave
    assert all(client.state(child) == "archived" for child in client.children())
    assert client.state(parent) == "ready"
    # Each child's lease is what is left of the stage: never the 48 h parent default, never unbounded.
    stage_s = ctx.cfg.stage_timeout_h * 3600
    assert all(stage_s - 600 < row["ttl_s"] <= stage_s for row in client.forks)
    assert len({row["key"] for row in client.forks}) == 3
    assert _progress(ctx)["fork_wave"] is None


class PromptRecordingForkAgent(ForkAgent):
    def __init__(self) -> None:
        super().__init__()
        self.prompts: dict[str, str] = {}

    def run(self, sb: Any, *, stage: str, iteration: int, prompt: str = "", **kw: Any) -> AgentResult:
        node = match.group(1) if (match := re.search(r"Node: `([^`]+)`", prompt)) else stage
        with self.lock:
            self.prompts[node] = prompt
        return super().run(sb, stage=stage, iteration=iteration, prompt=prompt, **kw)


def test_every_forked_and_serial_node_prompt_carries_the_runs_one_dialect_card(tmp_path: Path) -> None:
    """Fork-wave prompts are rendered on the parent before fan-out, so each child is told the same
    stored card as the serial node that runs after the fan-in moved HEAD."""
    agent = PromptRecordingForkAgent()
    ctx, _client = _host_ctx(tmp_path, agent)

    work_stage.build_and_test(ctx)

    card = ctx.state.read_control(stages.DIALECT_CONTROL).rstrip("\n")
    assert card.startswith("## Target dialect") and "You have no shell." in card
    assert sorted(agent.prompts) == [*SIBLINGS, "index"]
    assert all(card in prompt for prompt in agent.prompts.values())
    assert not any("Run the target's test command" in prompt for prompt in agent.prompts.values())
    # Every declared file is new at its node's checkpoint: nothing to snapshot, so no pack section.
    assert not any("## Snapshot of the declared files" in prompt for prompt in agent.prompts.values())


def test_the_default_keeps_every_node_in_the_one_shared_workspace(tmp_path: Path) -> None:
    """``max_parallel_forks = 1`` is the default: a forkable provider changes nothing until asked."""
    agent = ForkAgent(timeout_s=0.01)
    agent.barrier = threading.Barrier(1)  # serial siblings never meet
    ctx, client = _host_ctx(tmp_path, agent, max_parallel_forks=1)

    work_stage.build_and_test(ctx)

    report = _report(ctx)
    assert report["mode"] == "shared_workspace_serial" and report["parallel"] is False
    assert report["parallel_hint"] is True and report["reason"] == "max_parallel_forks_is_1"
    assert report["wave_decisions"][0]["reason"] == "max_parallel_forks_is_1"
    assert client.forks == [] and client.resumes == []
    assert ctx.sb.read("src/index.txt") == "alpha beta gamma\n"


def test_a_budget_that_cannot_pay_every_sibling_at_once_stays_serial(tmp_path: Path) -> None:
    agent = ForkAgent()
    agent.barrier = threading.Barrier(1)
    ctx, client = _host_ctx(tmp_path, agent, max_budget_usd=5.0, max_budget_usd_per_stage=2.0)

    work_stage.build_and_test(ctx)

    assert _report(ctx)["wave_decisions"][0]["reason"] == "budget_cannot_cover_siblings"
    assert client.forks == []


def test_a_provider_that_refuses_every_fork_costs_the_wave_its_parallelism_not_its_run(tmp_path: Path) -> None:
    """Observed live: boat answers ``429 rate_limited`` once an account spends its hourly starts.
    A refusal is definitive -- nothing was created, nothing is lost -- so the wave runs serially."""
    agent = ForkAgent()
    agent.barrier = threading.Barrier(1)  # serial siblings never meet
    ctx, client = _host_ctx(tmp_path, agent)
    parent = ctx.sb.sandbox_id
    client.refuse_forks = 429

    work_stage.build_and_test(ctx)

    report = _report(ctx)
    assert report["mode"] == "shared_workspace_serial" and report["parallel"] is False
    decision = report["wave_decisions"][0]
    assert decision["fork"] is False and decision["reason"].startswith("provider_refused_fork")
    assert "HTTP 429" in decision["reason"]
    (attempted,) = report["fork_waves"]
    assert attempted["teardown"] == "confirmed" and attempted["merges"] == []
    assert all(row["fork_refused"] and row["reaped"] for row in attempted["children"])
    assert {sandbox for _, _, sandbox in agent.calls} == {parent}  # every node ran in the parent
    assert client.resumes == [parent] and client.state(parent) == "ready"
    assert ctx.sb.read("src/index.txt") == "alpha beta gamma\n"


def test_a_failing_sibling_halts_the_wave_reaps_every_child_and_merges_nothing(tmp_path: Path) -> None:
    agent = ForkAgent(fail="beta")
    ctx, client = _host_ctx(tmp_path, agent)
    parent = ctx.sb.sandbox_id
    checkpoint = stages._workspace_head(ctx)

    with pytest.raises(StageError, match="beta broke"):
        work_stage.build_and_test(ctx)

    progress = _progress(ctx)
    (wave,) = progress["fork_waves"]
    assert wave["merges"] == [] and wave["teardown"] == "confirmed"
    assert {row["node_id"]: row["state"] for row in wave["results"]}["beta"] == "failed"
    assert progress["fork_wave"] is None and progress["nodes"] == []
    assert all(client.state(child) == "archived" for child in client.children())
    assert client.state(parent) == "ready" and stages._workspace_head(ctx) == checkpoint
    assert not ctx.sb.exists("src/alpha.txt")  # a sibling's work is not a candidate after a failure


def test_a_write_outside_a_nodes_declared_files_is_refused_before_any_merge(tmp_path: Path) -> None:
    ctx, client = _host_ctx(tmp_path, ForkAgent(stray="gamma"))
    checkpoint = stages._workspace_head(ctx)

    with pytest.raises(StageError, match="refused before fan-in: undeclared gamma/plan"):
        work_stage.build_and_test(ctx)

    (wave,) = _progress(ctx)["fork_waves"]
    assert [c["kind"] for c in wave["conflicts"]] == ["undeclared"] and wave["merges"] == []
    assert stages._workspace_head(ctx) == checkpoint
    assert all(client.state(child) == "archived" for child in client.children())


def test_a_rename_cannot_hide_the_deletion_of_an_undeclared_file(tmp_path: Path) -> None:
    """``git diff --name-only`` reports only a rename's new path; the deleted one must count too."""
    ctx, client = _host_ctx(tmp_path, ForkAgent(rename="alpha"))

    with pytest.raises(StageError, match=r"undeclared alpha/plan \['src/README'\]"):
        work_stage.build_and_test(ctx)

    assert ctx.sb.exists("src/README")
    assert all(client.state(child) == "archived" for child in client.children())


def test_a_failed_wave_still_charges_the_stage_what_its_children_committed(tmp_path: Path) -> None:
    ctx, _ = _host_ctx(tmp_path, ForkAgent(fail="beta"))

    with pytest.raises(StageError):
        work_stage.build_and_test(ctx)

    (record,) = [row for row in stages.load_stage_results(ctx) if row.stage == "build_and_test"]
    assert record.status == "failed"
    assert record.cost_usd == pytest.approx(CallLedger(ctx.state).charged_usd()) and record.cost_usd > 0


def test_a_child_forked_from_a_stale_snapshot_is_refused_before_its_agent_runs(tmp_path: Path) -> None:
    """A periodic snapshot can be older than HEAD. Lineage is proved in the child, not assumed."""
    agent = ForkAgent()
    ctx, client = _host_ctx(tmp_path, agent)
    client.stop(ctx.sb.sandbox_id)  # an older snapshot exists ...
    client.resume(ctx.sb.sandbox_id)
    ctx.sb.write("src/advance.txt", "x\n")
    stages.commit(ctx, stage="setup", msg="advance HEAD past that snapshot")
    client.stale_snapshot = True  # ... and the provider hands it out

    with pytest.raises(StageError, match="a stale snapshot is never a parent"):
        work_stage.build_and_test(ctx)

    assert agent.calls == []
    assert all(client.state(child) == "archived" for child in client.children())


def test_a_retry_after_a_dead_process_reaps_the_orphans_unparks_the_parent_and_reruns_the_wave(
    tmp_path: Path,
) -> None:
    """The orchestrator died mid-wave: the parent is parked, one child id was journalled, and one
    fork call's answer never arrived. The retry finds that child again by its idempotency key."""
    agent = ForkAgent()
    ctx, client = _host_ctx(tmp_path, agent)
    parent = ctx.sb.sandbox_id
    checkpoint = stages._workspace_head(ctx)
    ctx.sb.fork_parent()
    seen = client.fork(parent, ttl_s=600, idempotency_key="swf-fork-dead-seen")
    lost = client.fork(parent, ttl_s=600, idempotency_key="swf-fork-dead-lost")
    ctx.state.write_control(
        fork_wave.PROGRESS_FILE,
        json.dumps(
            {
                "schema_version": 1,
                "plan_digest": work_stage._digest_plan(_plan()),
                "input_head": checkpoint,
                "head": checkpoint,
                "agent_calls": 3,
                "attempt": None,
                "nodes": [],
                "repairs": [],
                "verified": None,
                "fork_waves": [],
                "wave_decisions": [],
                "fork_wave": {
                    "layer": 0,
                    "checkpoint": checkpoint,
                    "parent": parent,
                    "parked": True,
                    "started_at": 0.0,
                    "children": [
                        {
                            "node_id": "alpha",
                            "agent_call": 1,
                            "key": "swf-fork-dead-seen",
                            "ttl_s": 600,
                            "child": seen,
                            "reaped": False,
                        },
                        {
                            "node_id": "beta",
                            "agent_call": 2,
                            "key": "swf-fork-dead-lost",
                            "ttl_s": 600,
                            "child": None,
                            "reaped": False,
                            "fork_requested": True,
                        },
                        {  # journalled, but the process died before it asked for a fork
                            "node_id": "gamma",
                            "agent_call": 3,
                            "key": "swf-fork-dead-never",
                            "ttl_s": 600,
                            "child": None,
                            "reaped": False,
                        },
                    ],
                },
            }
        ),
    )
    forks_before = len(client.forks)

    work_stage.build_and_test(ctx)

    assert client.state(seen) == "archived" and client.state(lost) == "archived"
    # The reclaim reused the lost child, and the never-requested key made no VM at all.
    assert len(client.forks) == forks_before + 3
    assert "swf-fork-dead-never" not in client.idempotent
    report = _report(ctx)
    recovered, wave = report["fork_waves"]
    assert recovered["recovered"] is True and recovered["teardown"] == "confirmed"
    assert {row["child"] for row in recovered["children"]} == {seen, lost, None}
    assert wave["parallel"] is True and [m["node_id"] for m in wave["merges"]] == list(SIBLINGS)
    assert report["agent_calls"] == 7  # the dead attempts' identities are never reused
    assert ctx.sb.read("src/index.txt") == "alpha beta gamma\n"


def _dead_wave(ctx: Ctx, client: HostBoatClient, *, lost_key: str) -> tuple[str, str, str]:
    """The journal a process leaves when it dies mid-wave: parent parked, one child journalled,
    one fork requested whose answer never arrived."""
    parent = ctx.sb.sandbox_id
    checkpoint = stages._workspace_head(ctx)
    ctx.sb.fork_parent()
    seen = client.fork(parent, ttl_s=600, idempotency_key="swf-fork-dead-seen")
    lost = client.fork(parent, ttl_s=600, idempotency_key=lost_key)
    now = time.time()
    children = [
        {"node_id": "alpha", "agent_call": 1, "key": "swf-fork-dead-seen", "ttl_s": 600, "child": seen},
        {"node_id": "beta", "agent_call": 2, "key": lost_key, "ttl_s": 600, "child": None},
    ]
    for row in children:
        row |= {"reaped": False, "fork_requested": True, "requested_at": now}
    progress = {
        "schema_version": 1,
        "plan_digest": work_stage._digest_plan(_plan()),
        "input_head": checkpoint,
        "head": checkpoint,
        "agent_calls": 2,
        "attempt": None,
        "nodes": [],
        "repairs": [],
        "verified": None,
        "fork_waves": [],
        "wave_decisions": [],
        "fork_wave": {
            "layer": 0,
            "checkpoint": checkpoint,
            "parent": parent,
            "parked": True,
            "started_at": now,
            "children": children,
        },
    }
    ctx.state.write_control(fork_wave.PROGRESS_FILE, json.dumps(progress))
    return parent, seen, lost


def test_the_teardown_task_reaps_a_dead_waves_children_even_if_the_build_is_never_retried(tmp_path: Path) -> None:
    ctx, client = _host_ctx(tmp_path, ForkAgent())
    parent, seen, lost = _dead_wave(ctx, client, lost_key="swf-fork-dead-lost")

    stages.teardown(ctx)

    assert client.state(seen) == "archived" and client.state(lost) == "archived"
    assert client.state(parent) == "archived"  # closed, not resumed
    progress = _progress(ctx)
    assert progress["fork_wave"] is None and progress["fork_waves"][-1]["torn_down"] is True


def test_a_lost_fork_whose_reclaim_fails_is_debt_and_keeps_the_parent_parked_for_the_next_try(
    tmp_path: Path,
) -> None:
    """Unknown is not absent. A reclaim needs a parked parent -- a running one could fork anew from
    a periodic snapshot -- so the parent stays parked until a later attempt finds the child."""
    ctx, client = _host_ctx(tmp_path, ForkAgent())
    parent, seen, lost = _dead_wave(ctx, client, lost_key="swf-fork-dead-lost")
    real_fork = client.fork

    def flaky(sandbox_id: str, *, ttl_s: int, idempotency_key: str) -> str:
        raise BoatError("boat.dev fork failed; check connectivity", retryable=True)

    client.fork = flaky  # type: ignore[method-assign]
    with pytest.raises(StageError, match="key swf-fork-dead-lost"):
        work_stage.build_and_test(ctx)
    assert client.state(seen) == "archived" and client.state(lost) == "ready"
    assert client.state(parent) == "archived" and _progress(ctx)["fork_wave"]["parked"] is True

    client.fork = real_fork  # type: ignore[method-assign]
    work_stage.build_and_test(ctx)  # the next attempt reclaims it by key, unparks, and runs the wave

    assert client.state(lost) == "archived"
    assert ctx.sb.read("src/index.txt") == "alpha beta gamma\n"


# ---------------------------------------------------------------- the real service


@pytest.mark.skipif(
    os.environ.get("SWF_TEST_LIVE_BOAT") != "1" or not os.environ.get("BOAT_API_KEY"),
    reason="opt-in live boat.dev test: SWF_TEST_LIVE_BOAT=1 and BOAT_API_KEY",
)
def test_live_boat_fork_wave_end_to_end(tmp_path: Path) -> None:
    client = boat_client_from_env()
    agent = ForkAgent(timeout_s=300.0)
    ctx = _ctx(tmp_path, client, agent, branch=os.environ.get("SWF_TEST_LIVE_BOAT_BRANCH", "main"))
    parent = ctx.sb.sandbox_id
    children: list[str] = []
    try:
        work_stage.build_and_test(ctx)
        decision = _report(ctx)["wave_decisions"][0]
        if not decision["fork"]:  # e.g. the account's hourly start budget: say so, do not pass
            pytest.fail(f"boat did not fan the wave out: {decision['reason']}")
        (wave,) = _report(ctx)["fork_waves"]
        children = [row["child"] for row in wave["children"]]
        _assert_fanned_out_and_in(ctx, agent, parent, children)
        for child in children:  # boat.dev itself, not the journal, says each child is gone
            assert client._state(child) in {"archived", "cancelled"}
    finally:
        for child in children:
            ctx.sb.reap(child)
        ctx.sb.close()
    assert client._state(parent) in {"archived", "cancelled"}
