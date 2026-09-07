"""The one way from a ``(blueprint, job, run id)`` triple to a ready ``Ctx``.

CLI and Airflow tasks must produce the same run identity/workspace. Backend-managed Airflow jobs
add one extra trust boundary: their cell binding is persisted in host-owned control state and their
GitHub SCM is a credential-free backend proxy from the first issue read onward. Airflow still
schedules the work; the backend alone owns GitHub publication credentials and mutation fencing.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

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
) -> Ctx:
    cfg = job_config(bp, job, run_id=run_id, overrides=overrides, root=root)
    binding = _cell_binding(job)
    ctx = ctx_for(
        cfg,
        blueprint=bp,
        run_dir=job_run_dir(cfg, root),
        agent=agent,
        scm_override=_managed_scm(cfg, binding),
    )
    if binding is not None:
        ctx.state.write_control(
            "cell.json",
            json.dumps(binding, sort_keys=True, separators=(",", ":")) + "\n",
        )
    return ctx


def ctx_for(
    cfg: Config,
    *,
    blueprint: Blueprint,
    run_dir: Path,
    agent: Agent | None = None,
    scm_override: Scm | None = None,
) -> Ctx:
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
    return Ctx(
        cfg=cfg,
        sb=make_sandbox(cfg, issue.id, protected=protected, repo=cfg.repo, run_dir=run_dir),
        agent=agent or make_agent(cfg),
        scm=scm,
        issue=issue,
        run_dir=run_dir,
        blueprint=blueprint,
    )
