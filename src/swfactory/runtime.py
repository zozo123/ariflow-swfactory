"""The one way from a ``(blueprint, job, run id)`` triple to a ready ``Ctx``.

WHY one place: the CLI runs a job in a single process, while every Airflow task rebuilds the same
job from scratch on a worker. Both must come out identical — same run id, run dir, seeded workdir
and sandbox name — or a retried task talks to a different sandbox than its predecessor and the
run forks. ``cli.execute`` and ``dags._ctx`` each had a copy and had already drifted (seeding,
protected globs, sandbox naming). No Airflow import here: ``dags/blueprints.py`` calls in from
inside its task callables, so DAG parsing still needs nothing but Airflow.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, Any

from swfactory.config import FACTORY_ROOT, Config, protected_globs
from swfactory.sandbox import HOST_SANDBOXES, make_sandbox
from swfactory.scm import make_scm
from swfactory.stages import Ctx, seed_local_workdir

if TYPE_CHECKING:
    from swfactory.agent import Agent
    from swfactory.blueprint import Blueprint


def run_id_for(seed: str, job_idx: int = 0) -> str:
    """First 8 hex of ``sha1(<seed>:<job_idx>)`` — the run id of one job of one run.

    Pure, so every task and every retry of that job derives the same run id, hence the same run
    dir and sandbox name, and resumes the work instead of duplicating it.
    """
    return hashlib.sha1(f"{seed}:{job_idx}".encode()).hexdigest()[:8]


def locate(rel: str) -> str:
    """Resolve a repo-relative path (issue file, fixtures dir) against cwd, then the checkout:
    neither a worker nor a ``swfactory run`` elsewhere runs from the factory root."""
    if Path(rel).is_absolute() or Path(rel).exists():
        return rel
    alt = FACTORY_ROOT / rel
    return str(alt) if alt.exists() else rel


def job_run_dir(cfg: Config, root: Path | None = None) -> Path:
    """``<root>/.factory/<run_id>``: the run's host scratch (local remote, stage log, pr.md)."""
    return ((Path(root) if root is not None else Path()) / ".factory" / cfg.run_id).resolve()


def job_config(
    bp: Blueprint,
    job: dict[str, Any],
    *,
    run_id: str,
    overrides: dict[str, Any] | None = None,
    root: Path | None = None,
) -> Config:
    """``Config`` for one job: blueprint, then ``overrides`` (CLI flags, ``None`` ignored), then
    ``SWF_*`` env (``Config`` orders env before init), then this run's local paths.

    A blueprint's ``[sandbox]`` says where the REAL agent runs; a scripted replay never needs a
    MicroVM, so without an explicit ``sandbox`` it falls back to ``local`` (``SWF_SANDBOX`` still
    wins, as it wins over every init value). Host sandboxes get one workdir per run, under the run
    dir, so concurrent jobs never share a checkout.
    """
    over = overrides or {}  # Blueprint.config drops the None entries (flags the user did not pass)
    cfg = bp.config(job, run_id=run_id, **over)
    if cfg.agent == "scripted" and over.get("sandbox") is None and cfg.sandbox != "local":
        cfg = bp.config(job, run_id=run_id, **{**over, "sandbox": "local"})
    update: dict[str, Any] = {"fixtures_dir": locate(cfg.fixtures_dir)}
    if cfg.sandbox in HOST_SANDBOXES:
        update["workdir"] = str(job_run_dir(cfg, root) / "work")
    return cfg.model_copy(update=update)


def build_ctx(
    bp: Blueprint,
    job: dict[str, Any],
    *,
    run_id: str,
    overrides: dict[str, Any] | None = None,
    agent: Agent | None = None,
    root: Path | None = None,
) -> Ctx:
    """Everything a stage needs for one job of one run: ``job_config`` then ``ctx_for``."""
    cfg = job_config(bp, job, run_id=run_id, overrides=overrides, root=root)
    return ctx_for(cfg, blueprint=bp, run_dir=job_run_dir(cfg, root), agent=agent)


def ctx_for(cfg: Config, *, blueprint: Blueprint, run_dir: Path, agent: Agent | None = None) -> Ctx:
    """Assemble the ``Ctx`` of a job whose paths are already decided.

    Split from ``build_ctx`` only because ``cli.execute`` is handed a ready ``Config`` and run dir
    (tests and fixtures build both). Order matters: a host workdir is seeded from the target dir
    *before* ``make_sandbox``, so srt can turn the target's ``factory.toml`` ``protected`` globs
    into kernel-level ``denyWrite`` from the very first command, and before ``make_scm``, whose
    local remote is seeded from that workdir. ``agent`` overrides ``make_agent(cfg)`` (tests
    inject a ``ScriptedAgent`` with extra fixture dirs).
    """
    from swfactory.agent import make_agent  # runtime import: agent loads the prompt templates

    run_dir = Path(run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    base_repo: Path | None = None
    protected: list[str] = []
    if cfg.sandbox in HOST_SANDBOXES:
        base_repo = Path(cfg.workdir).resolve()
        seed_local_workdir(base_repo, cfg.target_dir)
        protected = protected_globs(base_repo)  # build level: the tests dir stays writable
    scm = make_scm(cfg, run_dir, base_repo=base_repo, base_ref=cfg.base_branch)
    issue = scm.fetch_issue(cfg.issue if cfg.issue.strip().isdigit() else locate(cfg.issue))
    return Ctx(  # contract stays lazy: setup() seeds the workdir before reading factory.toml
        cfg=cfg,
        sb=make_sandbox(cfg, issue.id, protected=protected, repo=cfg.repo),
        agent=agent or make_agent(cfg),
        scm=scm,
        issue=issue,
        run_dir=run_dir,
        blueprint=blueprint,
    )
