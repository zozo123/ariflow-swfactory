"""Helpers shared across test files: real git repos, swarm populations and the DAG modules.

A plain module (pytest collects only ``test_*.py``); test files import it as ``support``.
"""

from __future__ import annotations

import functools
import importlib.util
import os
import subprocess
from pathlib import Path
from types import ModuleType

from swfactory.population_manifest import PopulationManifest, build_population_manifest
from swfactory.provider_binding import ProviderBindingManifest, ProviderChoiceSet, bind_population_manifest
from swfactory.swarm_dynamics import AgentRole, ComputeTier, ContextPolicy, PopulationLane, SwarmPlan

REPO = Path(__file__).resolve().parents[1]
DAGS = REPO / "dags"

# ---------------------------------------------------------------- git


def git(cwd: Path, *args: str) -> str:
    """Run git in ``cwd`` under the suite's hermetic config (``conftest.py``); stripped stdout."""
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout.strip()


def make_repo(tmp_path: Path, files: dict[str, str] | None = None) -> tuple[Path, str]:
    """``tmp_path/repo`` with ``files`` (default ``value.txt``) committed on ``main`` -> (repo, sha)."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    for name, text in (files or {"value.txt": "base\n"}).items():
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text(text, encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "base")
    return repo, git(repo, "rev-parse", "HEAD")


# ---------------------------------------------------------------- populations


def lane(
    role: AgentRole,
    tier: ComputeTier,
    count: int,
    *,
    temperature: float,
    verifier: bool = False,
    axes: tuple[str, ...] = ("model", "prompt", "runtime"),
    focus: tuple[str, ...] = (),
    context: ContextPolicy = ContextPolicy.FRESH,
) -> PopulationLane:
    return PopulationLane(
        role=role,
        compute_tier=tier,
        count=count,
        context=context,
        temperature=temperature,
        independent_verification=verifier,
        diversity_axes=axes,
        focus_hotspots=focus,
    )


def swarm_plan(
    *lanes: PopulationLane, phase: str = "liquid", mode: str = "coordinate", units: float, reason: str
) -> SwarmPlan:
    return SwarmPlan(
        phase=phase,
        mode=mode,
        lanes=lanes,
        selected_hotspots=(),
        crystals_to_verify=(),
        stop_new_work=False,
        estimated_compute_units=units,
        reason=reason,
    )


def bound_population(plan: SwarmPlan, choices: ProviderChoiceSet) -> tuple[PopulationManifest, ProviderBindingManifest]:
    manifest = build_population_manifest(plan, search_provenance_digest="sha256:" + "a" * 64)
    return manifest, bind_population_manifest(manifest, choices=choices)


# ---------------------------------------------------------------- DAGs


@functools.cache
def load_dag_module(name: str = "blueprints") -> ModuleType:
    """``dags/<name>.py`` as a module: the DAG file itself, not a copy kept in sync. Importing it
    builds DAGs, so it is loaded once; tests patch it with ``monkeypatch``, which undoes itself."""
    spec = importlib.util.spec_from_file_location(f"swf_dag_{name}", DAGS / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def airflow_env(home: Path, *, replay: bool = True) -> dict[str, str]:
    """Clean process env: throwaway AIRFLOW_HOME, no inherited SWF_* knobs, scripted/local run.

    Marking the HITL tasks successful leaves no response; only the declared replay fixture may stand
    in for one, and never for backend-managed work."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SWF_", "AIRFLOW"))}
    env.update(
        AIRFLOW_HOME=str(home),
        AIRFLOW__CORE__DAGS_FOLDER=str(DAGS),
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        SWF_AGENT="scripted",
        SWF_SANDBOX="local",
        SWF_SCM="local",
    )
    if replay:
        env["SWF_GATE_REPLAY"] = str(REPO / "demo" / "gate-replay.json")
    return env


def run_checked(argv: list[str], *, cwd: Path, env: dict[str, str], timeout: int) -> str:
    proc = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout, check=False)
    tail = f"{proc.stdout[-4000:]}\n{proc.stderr[-4000:]}"
    assert proc.returncode == 0, f"{argv[-3:]} rc={proc.returncode}\n{tail}"
    return proc.stdout + proc.stderr
