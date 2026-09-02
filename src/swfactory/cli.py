"""``swfactory`` command line: run, demo, metrics, approve. Thin wiring only."""

from __future__ import annotations

import json
import shutil
import urllib.error
import urllib.request
from pathlib import Path
from typing import Annotated, Any

import typer

from swfactory import metrics as metrics_mod
from swfactory.agent import Agent, make_agent
from swfactory.config import Config
from swfactory.models import RunReport, StageError
from swfactory.sandbox import make_sandbox
from swfactory.scm import make_scm
from swfactory.stages import Approver, Ctx, cli_approver, run_pipeline, setup

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
    """Copy the target dir into an empty local workdir (never touches the source tree)."""
    workdir.mkdir(parents=True, exist_ok=True)
    if any(workdir.iterdir()):
        return
    src = Path(_locate(target_dir)) if target_dir else None
    if src and src.is_dir():
        shutil.copytree(src, workdir, ignore=COPY_IGNORE, dirs_exist_ok=True)


def execute(
    cfg: Config,
    *,
    approver: Approver = cli_approver,
    run_dir: Path | None = None,
    agent: Agent | None = None,
) -> RunReport:
    """Build the context for ``cfg`` and run the whole pipeline. Used by ``run``, ``demo``, tests.

    ``agent`` overrides ``make_agent(cfg)`` (tests inject a ScriptedAgent with extra fixture dirs).
    """
    run_dir = (Path(run_dir) if run_dir is not None else Path(".factory") / cfg.run_id).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    base_repo: Path | None = None
    if cfg.sandbox == "local":
        base_repo = Path(cfg.workdir).resolve()
        _seed_workdir(base_repo, cfg.target_dir)
    scm = make_scm(cfg, run_dir, base_repo=base_repo, base_ref=cfg.base_branch)
    issue = scm.fetch_issue(_locate(cfg.issue) if not cfg.issue.strip().isdigit() else cfg.issue)
    sb = make_sandbox(cfg, issue.id)
    ctx = Ctx(cfg=cfg, sb=sb, agent=agent or make_agent(cfg), scm=scm, issue=issue, run_dir=run_dir)
    result = setup(ctx)
    print(f"{'setup':<16} {result.status:<8} {result.duration_s:6.1f}s  sandbox={sb.name}")
    return run_pipeline(ctx, approver)


def _run_and_report(kwargs: dict[str, Any]) -> None:
    try:
        cfg = Config(**{k: v for k, v in kwargs.items() if v is not None})
    except ValueError as e:
        typer.echo(f"config error: {e}", err=True)
        raise typer.Exit(2) from e
    if cfg.sandbox == "local":
        cfg = cfg.model_copy(update={"workdir": str(Path(".factory") / cfg.run_id / "work")})
    if cfg.agent == "scripted":
        typer.echo(f"{SCRIPTED_BANNER}; fixtures: {cfg.fixtures_dir}")
    try:
        report = execute(cfg)
    except StageError as e:
        typer.echo(f"stage failed: {e}", err=True)
        raise typer.Exit(1) from e
    typer.echo("\n" + report.table())
    blocked = any(s.status == "blocked" for s in report.stages)
    if blocked or not report.tests_passed:
        raise typer.Exit(1)


# ---------------------------------------------------------------- commands


@app.command()
def run(
    issue: Annotated[str, typer.Option(help="GitHub issue number or path to a front-matter .md")],
    repo: Annotated[str | None, typer.Option(help="owner/name of the target repo")] = None,
    target_dir: Annotated[str | None, typer.Option(help="subdir the factory operates on")] = None,
    agent: Annotated[str | None, typer.Option(help="claude | scripted")] = None,
    sandbox: Annotated[str | None, typer.Option(help="local | islo")] = None,
    scm: Annotated[str | None, typer.Option(help="local | github")] = None,
    approve: Annotated[str | None, typer.Option(help="auto | prompt")] = None,
    tests: Annotated[str | None, typer.Option(help="sandbox | crabbox")] = None,
    crabbox_provider: Annotated[str | None, typer.Option()] = None,
    max_build_iterations: Annotated[int | None, typer.Option()] = None,
    record: Annotated[str | None, typer.Option(help="dump real agent outputs as fixtures")] = None,
    allow_local_agent: Annotated[bool, typer.Option(help="DEV: real agent outside islo")] = False,
    run_id: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Run the factory on one issue and open a PR."""
    _run_and_report(
        {
            "issue": issue,
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
        }
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
    """Run the factory on demo/issue.md: scripted replay by default, the real path with --real."""
    preset = (
        {"agent": "claude", "sandbox": "islo", "scm": "github", "approve": "prompt"}
        if real
        else {"agent": "scripted", "sandbox": "local", "scm": "local", "approve": "auto"}
    )
    _run_and_report(
        {
            "issue": "demo/issue.md",
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
        }
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
    airflow_url: Annotated[str, typer.Option(envvar="AIRFLOW_URL")] = "http://localhost:8080",
    token: Annotated[str | None, typer.Option(envvar="AIRFLOW_TOKEN", help="API JWT")] = None,
) -> None:
    """Answer a running DAG's approval gate through the Airflow HITL API."""
    if gate not in ("intent", "plan"):
        typer.echo("gate must be 'intent' or 'plan'", err=True)
        raise typer.Exit(2)
    url = (
        f"{airflow_url.rstrip('/')}/api/v2/dags/factory/dagRuns/{dag_run_id}"
        f"/taskInstances/approve_{gate}/-1/hitlDetails"
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
