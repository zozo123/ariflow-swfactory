"""``swfactory`` command line: run, demo, metrics, approve, maintain. Thin wiring only.

``run``/``demo`` build one ``Config`` per job from a blueprint (``Blueprint.config``) and then apply
the flags the user actually passed; ``SWF_*`` env vars win over both (``Config`` orders env before
init). Jobs (issues x targets) run sequentially, each in its own run dir and workdir.
"""

from __future__ import annotations

import json
import shutil
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Annotated, Any

import typer

from swfactory import blueprint as blueprint_mod
from swfactory import metrics as metrics_mod
from swfactory.agent import Agent, make_agent
from swfactory.blueprint import Blueprint
from swfactory.config import Config
from swfactory.models import RunReport, StageError
from swfactory.sandbox import HOST_SANDBOXES, make_sandbox
from swfactory.scm import make_scm
from swfactory.stages import (
    Approver,
    Ctx,
    cli_approver,
    run_pipeline,
    seed_local_workdir,
    setup,
)

app = typer.Typer(help="AI-native software factory.", no_args_is_help=True, add_completion=False)

FACTORY_ROOT = Path(__file__).resolve().parents[2]
COPY_IGNORE = shutil.ignore_patterns(
    ".venv", ".factory", ".git", "__pycache__", "*.pyc", ".pytest_cache", ".ruff_cache"
)
SCRIPTED_BANNER = "SCRIPTED REPLAY — agent=scripted, no model calls"


# ---------------------------------------------------------------- wiring shared by run/demo


def _locate(rel: str) -> str:
    """Resolve a repo-relative path against cwd, then against the factory checkout."""
    if Path(rel).exists():
        return rel
    alt = FACTORY_ROOT / rel
    return str(alt) if alt.exists() else rel


def _seed_workdir(workdir: Path, target_dir: str) -> None:
    """Delegates to stages.seed_local_workdir (kept so demo/run seed before scm.fetch_issue)."""
    seed_local_workdir(workdir, target_dir if Path(target_dir).is_dir() else _locate(target_dir))


def execute(
    cfg: Config,
    *,
    approver: Approver = cli_approver,
    run_dir: Path | None = None,
    agent: Agent | None = None,
    blueprint: Blueprint | None = None,
) -> RunReport:
    """Build the context for ``cfg`` and run the whole pipeline. Used by ``run``, ``demo``, tests.

    ``agent`` overrides ``make_agent(cfg)`` (tests inject a ScriptedAgent with extra fixture dirs).
    ``blueprint`` defaults to ``load(cfg.blueprint)``; its ``pipeline()`` is the walk order.
    """
    bp = blueprint if blueprint is not None else blueprint_mod.load(cfg.blueprint)
    run_dir = (Path(run_dir) if run_dir is not None else Path(".factory") / cfg.run_id).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    base_repo: Path | None = None
    if cfg.sandbox in HOST_SANDBOXES:
        base_repo = Path(cfg.workdir).resolve()
        _seed_workdir(base_repo, cfg.target_dir)
    scm = make_scm(cfg, run_dir, base_repo=base_repo, base_ref=cfg.base_branch)
    issue = scm.fetch_issue(_locate(cfg.issue) if not cfg.issue.strip().isdigit() else cfg.issue)
    sb = make_sandbox(cfg, issue.id)
    ctx = Ctx(
        cfg=cfg,
        sb=sb,
        agent=agent or make_agent(cfg),
        scm=scm,
        issue=issue,
        run_dir=run_dir,
        blueprint=bp,
        target={"repo": cfg.repo, "dir": cfg.target_dir, "base_branch": cfg.base_branch},
    )
    result = setup(ctx)
    print(f"{'setup':<16} {result.status:<8} {result.duration_s:6.1f}s  sandbox={sb.name}")
    return run_pipeline(ctx, approver)


def _load_blueprint(name_or_path: str) -> Blueprint:
    try:
        return blueprint_mod.load(name_or_path)
    except (OSError, ValueError) as e:
        typer.echo(f"blueprint error: {e}", err=True)
        raise typer.Exit(2) from e


def _job_config(bp: Blueprint, job: dict, run_id: str, overrides: dict[str, Any]) -> Config:
    """``Blueprint.config`` for one job. A blueprint's ``[sandbox]`` describes where the REAL
    agent runs; a scripted replay never needs a MicroVM, so without an explicit ``--sandbox`` it
    runs in ``LocalSandbox`` (as v1's ``run`` did). ``SWF_SANDBOX`` in the env still wins."""
    cfg = bp.config(job, run_id=run_id, **overrides)
    if cfg.agent == "scripted" and overrides.get("sandbox") is None and cfg.sandbox != "local":
        cfg = bp.config(job, run_id=run_id, **{**overrides, "sandbox": "local"})
    return cfg


def _run_jobs(
    bp: Blueprint, issues: list[str], overrides: dict[str, Any], *, targets: list[str] | None = None
) -> None:
    """Run every (issue x target) job of ``bp`` in sequence. ``overrides`` are the CLI flags the
    user passed (``None`` = not passed). Exit 1 if any job blocks, fails its tests or errors."""
    run_id = overrides.pop("run_id", None) or uuid.uuid4().hex[:8]
    try:
        jobs = bp.jobs({"issues": issues, **({"targets": targets} if targets else {})})
    except ValueError as e:
        typer.echo(f"config error: {e}", err=True)
        raise typer.Exit(2) from e
    failed = False
    for job in jobs:
        job_run_id = run_id if len(jobs) == 1 else f"{run_id}-{job['job_idx']}"
        try:
            cfg = _job_config(bp, job, job_run_id, overrides)
        except ValueError as e:
            typer.echo(f"config error: {e}", err=True)
            raise typer.Exit(2) from e
        if cfg.sandbox in HOST_SANDBOXES:
            cfg = cfg.model_copy(update={"workdir": str(Path(".factory") / cfg.run_id / "work")})
        if len(jobs) > 1:
            where = f"{job['repo']}/{job['dir']}".rstrip("/")
            typer.echo(f"\n=== job {job['job_idx'] + 1}/{len(jobs)}: {job['issue']} -> {where}")
        if cfg.agent == "scripted":
            typer.echo(f"{SCRIPTED_BANNER}; fixtures: {cfg.fixtures_dir}")
        try:
            report = execute(cfg, blueprint=bp)
        except StageError as e:
            typer.echo(f"stage failed: {e}", err=True)
            failed = True
            continue
        typer.echo("\n" + report.table())
        blocked = any(s.status == "blocked" for s in report.stages)
        failed = failed or blocked or not report.tests_passed
    if failed:
        raise typer.Exit(1)


# ---------------------------------------------------------------- commands


@app.command()
def run(
    issue: Annotated[
        list[str],
        typer.Option(help="GitHub issue number or path to a front-matter .md (repeatable)"),
    ],
    blueprint: Annotated[
        str, typer.Option(help="blueprints/<name>.toml or a path")
    ] = blueprint_mod.DEFAULT_BLUEPRINT,
    target: Annotated[
        list[str] | None, typer.Option(help="only these blueprint targets (owner/name, repeatable)")
    ] = None,
    repo: Annotated[str | None, typer.Option(help="owner/name of the target repo")] = None,
    target_dir: Annotated[str | None, typer.Option(help="subdir the factory operates on")] = None,
    agent: Annotated[str | None, typer.Option(help="claude | scripted")] = None,
    sandbox: Annotated[str | None, typer.Option(help="local | islo | srt")] = None,
    scm: Annotated[str | None, typer.Option(help="local | github")] = None,
    approve: Annotated[str | None, typer.Option(help="auto | prompt")] = None,
    tests: Annotated[str | None, typer.Option(help="sandbox | crabbox")] = None,
    crabbox_provider: Annotated[str | None, typer.Option()] = None,
    max_build_iterations: Annotated[int | None, typer.Option()] = None,
    record: Annotated[str | None, typer.Option(help="dump real agent outputs as fixtures")] = None,
    allow_local_agent: Annotated[bool, typer.Option(help="DEV: real agent outside islo")] = False,
    run_id: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Run a blueprint's line on one or more issues (one PR per issue x target)."""
    _run_jobs(
        _load_blueprint(blueprint),
        issue,
        {
            "repo": repo,
            "target_dir": target_dir,
            "agent": agent,
            "sandbox": sandbox,
            "scm": scm,
            "approve": approve,
            "tests": tests,
            "crabbox_provider": crabbox_provider,
            "max_build_iterations": max_build_iterations,
            "record_dir": record,
            "allow_local_agent": allow_local_agent or None,
            "run_id": run_id,
        },
        targets=target,
    )


@app.command()
def demo(
    real: Annotated[
        bool, typer.Option(help="claude agent, islo sandbox, github scm, prompt")
    ] = False,
    agent: Annotated[str | None, typer.Option()] = None,
    sandbox: Annotated[str | None, typer.Option()] = None,
    scm: Annotated[str | None, typer.Option()] = None,
    approve: Annotated[str | None, typer.Option()] = None,
    tests: Annotated[str | None, typer.Option()] = None,
    crabbox_provider: Annotated[str | None, typer.Option()] = None,
    record: Annotated[str | None, typer.Option()] = None,
    allow_local_agent: Annotated[bool, typer.Option()] = False,
    run_id: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Run the default line on demo/issue.md: scripted replay by default, the real path with
    --real."""
    preset = (
        {"agent": "claude", "sandbox": "islo", "scm": "github", "approve": "prompt"}
        if real
        else {"agent": "scripted", "sandbox": "local", "scm": "local", "approve": "auto"}
    )
    _run_jobs(
        _load_blueprint(blueprint_mod.DEFAULT_BLUEPRINT),
        ["demo/issue.md"],
        {
            "fixtures_dir": _locate("demo/scripted"),
            **preset,
            **{
                k: v
                for k, v in {
                    "agent": agent,
                    "sandbox": sandbox,
                    "scm": scm,
                    "approve": approve,
                    "tests": tests,
                    "crabbox_provider": crabbox_provider,
                    "record_dir": record,
                    "allow_local_agent": allow_local_agent or None,
                    "run_id": run_id,
                }.items()
                if v is not None
            },
        },
    )


@app.command()
def metrics(
    root: Annotated[
        Path, typer.Option(help="checkout to scan for docs/factory/*/metrics.json")
    ] = Path("."),
) -> None:
    """Summarise every committed run: first-pass rate, iterations, cycle time, findings, cost."""
    runs = metrics_mod.load_all(root)
    typer.echo(metrics_mod.table(metrics_mod.summarize(runs)))


@app.command()
def approve(
    dag_run_id: Annotated[str, typer.Argument()],
    gate: Annotated[str, typer.Argument(help="intent | plan")],
    reject: Annotated[bool, typer.Option(help="reject instead of approve")] = False,
    blueprint: Annotated[
        str, typer.Option(help="blueprint name (= DAG id) or blueprints/*.toml path")
    ] = blueprint_mod.DEFAULT_BLUEPRINT,
    map_index: Annotated[int, typer.Option(help="job index within the run (issues x targets)")] = 0,
    airflow_url: Annotated[str, typer.Option(envvar="AIRFLOW_URL")] = "http://localhost:8080",
    token: Annotated[str | None, typer.Option(envvar="AIRFLOW_TOKEN", help="API JWT")] = None,
) -> None:
    """Answer a running DAG's approval gate (mapped task ``job.approve_<gate>``) through the
    Airflow HITL API."""
    if gate not in ("intent", "plan"):
        typer.echo("gate must be 'intent' or 'plan'", err=True)
        raise typer.Exit(2)
    dag_id = _load_blueprint(blueprint).name if blueprint.endswith(".toml") else blueprint
    url = (
        f"{airflow_url.rstrip('/')}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"
        f"/taskInstances/job.approve_{gate}/{map_index}/hitlDetails"
    )
    payload = {"chosen_options": ["Reject" if reject else "Approve"], "params_input": {}}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    typer.echo(f"PATCH {url}\n{json.dumps(payload)}")
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(), headers=headers, method="PATCH"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            typer.echo(f"{resp.status} {resp.read().decode()[:2000]}")
    except urllib.error.HTTPError as e:
        typer.echo(f"HTTP {e.code}: {e.read().decode()[:2000]}", err=True)
        raise typer.Exit(1) from e
    except urllib.error.URLError as e:
        typer.echo(f"cannot reach {airflow_url}: {e.reason}", err=True)
        raise typer.Exit(1) from e


if __name__ == "__main__":  # pragma: no cover
    app()


@app.command()
def maintain(
    bands: Annotated[Path, typer.Option(help="Response tiers (sigma bands).")] = Path("bands.yaml"),
    root: Annotated[Path, typer.Option(help="Repo root with docs/factory/*/metrics.json")] = Path(),
    scm: Annotated[str, typer.Option(help="local|github")] = "local",
    sweep_ttl_s: Annotated[
        int, typer.Option(help="Also remove orphan swf-* islo sandboxes older than this (0=skip).")
    ] = 0,
) -> None:
    """Maintain stage: detect metric breaches per bands.yaml; act by tier (log/diagnose/propose)."""
    from swfactory import maintain as maintain_mod

    cfg = Config(issue="maintain", scm=scm, approve="auto")  # type: ignore[arg-type]
    run_dir = Path(".factory") / cfg.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    breaches = maintain_mod.run(
        cfg, scm=make_scm(cfg, run_dir), agent=None, sb=None, bands_path=bands, root=root
    )
    for b in breaches:
        typer.echo(
            f"{b.action:8s} {b.metric}: {b.value:g} vs mean {b.mean:g}±{b.stdev:g} ({b.sigma}σ)"
        )
    if not breaches:
        typer.echo("no breaches")
    if sweep_ttl_s:
        for name in maintain_mod.sweep_sandboxes(sweep_ttl_s):
            typer.echo(f"removed orphan sandbox {name}")
