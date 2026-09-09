"""The one way from a ``(blueprint, job, run id)`` triple to a ready ``Ctx``.

CLI and Airflow tasks must produce the same run identity/workspace. Backend-managed Airflow jobs
add one extra trust boundary: their cell binding is persisted in host-owned control state and their
GitHub SCM is a credential-free backend proxy from the first issue read onward. Airflow still
schedules the work; the backend alone owns GitHub publication credentials and mutation fencing.

Every context also passes through ``swfactory.accepted_inputs``: the first one admits an immutable
snapshot of the issue, blueprint, policy and target this epoch is executing, and every later one is
checked against it. Because Airflow rebuilds the whole Ctx per task -- reloading the blueprint,
re-fetching the issue and re-reading the worker's ``SWF_*`` environment each time -- this is the
only place where drift can be caught before a stage body, an agent call or a publication.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from swfactory import accepted_inputs
from swfactory.config import FACTORY_ROOT, Config, protected_globs
from swfactory.models import StageError
from swfactory.paths import (
    normalize_relative_path,
    validate_git_ref,
    validate_repo,
    validate_run_id,
)
from swfactory.sandbox import HOST_SANDBOXES, make_sandbox
from swfactory.scm import make_scm
from swfactory.stages import Ctx, seed_local_workdir
from swfactory.state import JournalCorruption, RunBusyError, RunState

if TYPE_CHECKING:
    from swfactory.agent import Agent
    from swfactory.blueprint import Blueprint
    from swfactory.scm import Scm


def run_id_for(seed: str, job_idx: int = 0) -> str:
    return hashlib.sha1(f"{seed}:{job_idx}".encode()).hexdigest()[:8]


def locate(rel: str) -> str:
    if Path(rel).is_absolute() or Path(rel).exists():
        return rel
    alt = FACTORY_ROOT / rel
    return str(alt) if alt.exists() else rel


def job_run_dir(cfg: Config, root: Path | None = None) -> Path:
    return ((Path(root) if root is not None else Path()) / ".factory" / cfg.run_id).resolve()


def job_config(
    bp: Blueprint,
    job: dict[str, Any],
    *,
    run_id: str,
    overrides: dict[str, Any] | None = None,
    root: Path | None = None,
) -> Config:
    over = overrides or {}
    cfg = bp.config(job, run_id=run_id, **over)
    if cfg.agent == "scripted" and over.get("sandbox") is None and cfg.sandbox != "local":
        cfg = bp.config(job, run_id=run_id, **{**over, "sandbox": "local"})
    identity: dict[str, Any] = {
        "issue": str(job["issue"]),
        "repo": validate_repo(str(job.get("repo", bp.targets[0].repo))),
        "target_dir": normalize_relative_path(
            str(job.get("dir", bp.targets[0].dir)), field="target_dir", allow_empty=True
        ),
        "base_branch": validate_git_ref(str(job.get("base_branch", bp.targets[0].base_branch)), field="base_branch"),
        "run_id": validate_run_id(run_id),
        "blueprint": bp.name,
    }
    cfg = cfg.model_copy(update=identity)
    update: dict[str, Any] = {"fixtures_dir": locate(cfg.fixtures_dir)}
    if cfg.sandbox in HOST_SANDBOXES:
        update["workdir"] = str(job_run_dir(cfg, root) / "work")
    return cfg.model_copy(update=update)


def _cell_binding(job: dict[str, Any]) -> dict[str, Any] | None:
    raw_id = job.get("cell_id")
    if raw_id in (None, ""):
        return None
    cell_id = str(raw_id).strip()
    if not cell_id.startswith("cell_") or len(cell_id) != 29:
        raise StageError("policy", "mapped job carries an invalid Factory Cell id")
    epoch = job.get("cell_epoch")
    if type(epoch) is not int or epoch < 1:
        raise StageError("policy", "mapped job carries an invalid Factory Cell epoch")
    managed = job.get("cell_managed", False)
    if type(managed) is not bool:
        raise StageError("policy", "mapped job carries an invalid Factory Cell managed flag")
    policy_digest = job.get("cell_policy_digest")
    if policy_digest is not None and (not isinstance(policy_digest, str) or not policy_digest.startswith("policy:")):
        raise StageError("policy", "mapped job carries an invalid Factory Cell policy digest")
    generation = job.get("cell_generation")
    if generation is not None and not isinstance(generation, str):
        raise StageError("policy", "mapped job carries an invalid Factory Cell generation")
    if managed and not policy_digest:
        raise StageError("policy", "managed Factory Cell is missing its policy digest")
    return {
        "schema_version": 1,
        "cell_id": cell_id,
        "epoch": epoch,
        "managed": managed,
        "job_idx": int(job.get("job_idx", 0)),
        "policy_digest": policy_digest,
        "factory_generation": generation,
    }


def _managed_scm(cfg: Config, binding: dict[str, Any] | None) -> Scm | None:
    if binding is None or not binding["managed"] or cfg.scm != "github":
        return None
    from swfactory.backend_scm import BackendScm

    return BackendScm(
        repo=cfg.repo,
        base_branch=cfg.base_branch,
        backend_url=os.getenv("SWF_BACKEND_URL") or "",
        backend_token=os.getenv("SWF_BACKEND_TOKEN") or "",
        cell_id=binding["cell_id"],
        epoch=binding["epoch"],
        policy_digest=str(binding["policy_digest"]),
    )


def build_ctx(
    bp: Blueprint,
    job: dict[str, Any],
    *,
    run_id: str,
    overrides: dict[str, Any] | None = None,
    agent: Agent | None = None,
    root: Path | None = None,
    enforce_inputs: bool = True,
) -> Ctx:
    """``enforce_inputs=False`` is for cleanup only -- see ``ctx_for``."""
    cfg = job_config(bp, job, run_id=run_id, overrides=overrides, root=root)
    binding = _cell_binding(job)
    return ctx_for(
        cfg,
        blueprint=bp,
        run_dir=job_run_dir(cfg, root),
        agent=agent,
        scm_override=_managed_scm(cfg, binding),
        cell_binding=binding,
        enforce_inputs=enforce_inputs,
    )


def ctx_for(
    cfg: Config,
    *,
    blueprint: Blueprint,
    run_dir: Path,
    agent: Agent | None = None,
    scm_override: Scm | None = None,
    cell_binding: dict[str, Any] | None = None,
    enforce_inputs: bool = True,
) -> Ctx:
    """Build the run's Ctx and admit (or re-check) the inputs this epoch accepted.

    ``enforce_inputs=False`` neither admits nor refuses: it exists for teardown, which must still be
    able to close a sandbox belonging to an epoch whose inputs drifted -- a refusal there would leak
    the very cell cleanup exists to close. Every path that can produce work or publish it leaves the
    flag at its default.
    """
    run_dir = Path(run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    try:
        with RunState(run_dir).exclusive("prepare"):
            return _prepare_ctx(
                cfg,
                blueprint=blueprint,
                run_dir=run_dir,
                agent=agent,
                scm_override=scm_override,
                cell_binding=cell_binding,
                enforce_inputs=enforce_inputs,
            )
    except RunBusyError as error:
        raise StageError("sandbox", str(error), retryable=True) from error
    except JournalCorruption as error:
        raise StageError("policy", str(error)) from error


def _prepare_ctx(
    cfg: Config,
    *,
    blueprint: Blueprint,
    run_dir: Path,
    agent: Agent | None,
    scm_override: Scm | None,
    cell_binding: dict[str, Any] | None = None,
    enforce_inputs: bool = True,
) -> Ctx:
    from swfactory.agent import make_agent

    base_repo: Path | None = None
    protected: list[str] = []
    if cfg.sandbox in HOST_SANDBOXES:
        base_repo = Path(cfg.workdir).resolve()
        seed_local_workdir(base_repo, cfg.target_dir)
        protected = protected_globs(base_repo)
    scm = scm_override or make_scm(cfg, run_dir, base_repo=base_repo, base_ref=cfg.base_branch)
    issue = scm.fetch_issue(cfg.issue if cfg.issue.strip().isdigit() else locate(cfg.issue))
    state = RunState(run_dir)
    # Admission BEFORE make_sandbox/make_agent: a refused task must not have started a MicroVM or
    # constructed an agent, let alone reached a stage body. Reading the issue above is the only I/O
    # that has to precede this, because the issue is one of the things being compared.
    current = accepted_inputs.snapshot(
        cfg,
        blueprint,
        issue,
        cell_id=(cell_binding or {}).get("cell_id"),
        cell_epoch=(cell_binding or {}).get("epoch"),
        managed=bool((cell_binding or {}).get("managed")),
    )
    if enforce_inputs:
        accepted_inputs.admit(state, current)
    # After the fence, not before. A refused task used to rewrite this run's cell binding on its way
    # out, so the record of which Cell epoch owns the directory reflected a task that was turned
    # away. `managed` is inside the snapshot for the same reason: it decides who owns the epoch, and
    # a fence that pins the epoch while leaving that flag loose is pinning the wrong half. Flipping
    # it off used to be admitted silently, and it lifts both the local re-accept refusal and the
    # ban on a replay fixture authorizing managed work.
    if cell_binding is not None:
        state.write_control(
            "cell.json",
            json.dumps(cell_binding, sort_keys=True, separators=(",", ":")) + "\n",
        )
    return Ctx(
        cfg=cfg,
        sb=make_sandbox(cfg, issue.id, protected=protected, repo=cfg.repo, run_dir=run_dir),
        agent=agent or make_agent(cfg),
        scm=scm,
        issue=issue,
        run_dir=run_dir,
        blueprint=blueprint,
    )
