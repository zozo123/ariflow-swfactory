"""``swfactory`` command line: run, demo, metrics, approve, maintain. Thin wiring only.

``run``/``demo`` turn each job (issue x target) into a ``Ctx`` with ``swfactory.runtime``, exactly
as the Airflow tasks do, and walk the blueprint's pipeline over it. Jobs run sequentially, each in
its own run dir and workdir. The flags the user passed are the ``overrides``; ``SWF_*`` env vars
win over them (see ``runtime.job_config``).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Annotated, Any

import typer

from swfactory import blueprint as blueprint_mod
from swfactory import metrics as metrics_mod
from swfactory.agent import Agent
from swfactory.blueprint import Blueprint
from swfactory.config import FACTORY_ROOT, Config
from swfactory.models import RunReport, StageError
from swfactory.runtime import build_ctx, ctx_for, job_run_dir
from swfactory.scm import make_scm
from swfactory.stages import Approver, Ctx, cli_approver, run_pipeline, setup

app = typer.Typer(help="AI-native software factory.", no_args_is_help=True, add_completion=False)

SCRIPTED_BANNER = "SCRIPTED REPLAY — agent=scripted, no model calls"


# ---------------------------------------------------------------- wiring shared by run/demo


def run_ctx(ctx: Ctx, approver: Approver = cli_approver) -> RunReport:
    """``setup`` then the blueprint's pipeline walk; the report also lands in
    ``<run_dir>/report.json``."""
    result = setup(ctx)
    print(f"{'setup':<16} {result.status:<8} {result.duration_s:6.1f}s  sandbox={ctx.sb.name}")
    report = run_pipeline(ctx, approver)
    (ctx.run_dir / "report.json").write_text(
        report.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )
    return report


def execute(
    cfg: Config,
    *,
    approver: Approver = cli_approver,
    run_dir: Path | None = None,
    agent: Agent | None = None,
    blueprint: Blueprint | None = None,
) -> RunReport:
    """Run the whole pipeline for an already-built ``Config`` (tests and fixtures build one).

    ``agent`` overrides ``make_agent(cfg)``; ``blueprint`` defaults to ``load(cfg.blueprint)`` and
    its ``pipeline()`` is the walk order. ``run``/``demo`` go through ``_run_jobs`` instead, which
    derives the config from the (blueprint, job) pair.
    """
    bp = blueprint if blueprint is not None else blueprint_mod.load(cfg.blueprint)
    ctx = ctx_for(cfg, blueprint=bp, run_dir=run_dir or job_run_dir(cfg), agent=agent)
    return run_ctx(ctx, approver)


def _load_blueprint(name_or_path: str) -> Blueprint:
    try:
        return blueprint_mod.load(name_or_path)
    except (OSError, ValueError) as e:
        typer.echo(f"blueprint error: {e}", err=True)
        raise typer.Exit(2) from e


def _run_jobs(
    bp: Blueprint, issues: list[str], overrides: dict[str, Any], *, targets: list[str] | None = None
) -> None:
    """Run every (issue x target) job of ``bp`` in sequence. ``overrides`` are the CLI flags the
    user passed (``None`` = not passed). Each job's report is printed as a table and written to
    ``.factory/<run_id>/report.json``. Exit 1 if any job blocks, fails its tests or errors."""
    run_id = overrides.pop("run_id", None) or uuid.uuid4().hex[:8]
    try:
        jobs = bp.jobs({"issues": issues, **({"targets": targets} if targets else {})})
    except ValueError as e:
        typer.echo(f"config error: {e}", err=True)
        raise typer.Exit(2) from e
    failed = False
    for job in jobs:
        if len(jobs) > 1:
            where = f"{job['repo']}/{job['dir']}".rstrip("/")
            typer.echo(f"\n=== job {job['job_idx'] + 1}/{len(jobs)}: {job['issue']} -> {where}")
        job_run_id = run_id if len(jobs) == 1 else f"{run_id}-{job['job_idx']}"
        try:
            ctx = build_ctx(bp, job, run_id=job_run_id, overrides=overrides)
        except ValueError as e:
            typer.echo(f"config error: {e}", err=True)
            raise typer.Exit(2) from e
        if ctx.cfg.agent == "scripted":
            typer.echo(f"{SCRIPTED_BANNER}; fixtures: {ctx.cfg.fixtures_dir}")
        try:
            report = run_ctx(ctx)
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
    sandbox: Annotated[
        str | None, typer.Option(help="local | islo | srt | docker | toolset")
    ] = None,
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


@app.command()
def maintain(
    bands: Annotated[Path, typer.Option(help="Response tiers (sigma bands).")] = (
        FACTORY_ROOT / "bands.yaml"
    ),
    root: Annotated[Path, typer.Option(help="Repo root with docs/factory/*/metrics.json")] = Path(),
    scm: Annotated[str, typer.Option(help="local|github")] = "local",
    sweep_ttl_s: Annotated[
        int, typer.Option(help="Also remove orphan swf-* islo sandboxes older than this (0=skip).")
    ] = 0,
    owner: Annotated[
        str | None,
        typer.Option(help="Only sweep sandboxes created_by this email (or $SWF_SANDBOX_OWNER)."),
    ] = None,
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
        for name in maintain_mod.sweep_sandboxes(sweep_ttl_s, owner=owner):
            typer.echo(f"removed orphan sandbox {name}")


# ---------------------------------------------------------------- webhook (orchestrator on islo)

webhook_app = typer.Typer(
    help="GitHub -> Airflow webhook receiver (runs inside the swf-orchestrator islo sandbox).",
    no_args_is_help=True,
)
app.add_typer(webhook_app, name="webhook")


@webhook_app.command("serve")
def webhook_serve(
    port: Annotated[int, typer.Option(help="listen port (islo delivers to it)")] = 8081,
    airflow_url: Annotated[
        str, typer.Option(envvar="AIRFLOW_URL", help="Airflow API base URL")
    ] = "http://localhost:8080",
    secret_env: Annotated[
        str,
        typer.Option(
            help="env var holding the GitHub webhook secret; unset var = trust islo's upstream "
            "HMAC check and skip local verification"
        ),
    ] = "SWF_WEBHOOK_SECRET",
    host: Annotated[str, typer.Option(help="bind address")] = "0.0.0.0",
) -> None:
    """Serve POST /webhooks/github and GET /healthz; Airflow creds from AIRFLOW_TOKEN or
    AIRFLOW_USER + AIRFLOW_PASSWORD."""
    import os

    from swfactory import webhook as webhook_mod

    try:
        provider = webhook_mod.token_provider_from_env(airflow_url)
    except ValueError as e:
        typer.echo(f"webhook: {e}", err=True)
        raise typer.Exit(2) from e
    webhook_mod.serve(
        port,
        airflow_url=airflow_url,
        token_provider=provider,
        secret=os.environ.get(secret_env) or None,
        host=host,
    )


@webhook_app.command("route")
def webhook_route(
    event: Annotated[str, typer.Argument(help="X-GitHub-Event value: issues | issue_comment")],
    payload: Annotated[Path, typer.Argument(help="path to the event payload JSON")],
) -> None:
    """Dry run: print the DAG run a payload would trigger (exit 1 when it would be ignored)."""
    from swfactory import webhook as webhook_mod

    try:
        data = json.loads(payload.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        typer.echo(f"payload error: {e}", err=True)
        raise typer.Exit(2) from e
    trigger = webhook_mod.route(event, data if isinstance(data, dict) else {})
    if trigger is None:
        typer.echo(f"{event}: ignored")
        raise typer.Exit(1)
    typer.echo(
        f"POST /api/v2/dags/{trigger.dag_id}/dagRuns {json.dumps(trigger.body(), sort_keys=True)}"
    )


@app.command()
def doctor(
    blueprint: Annotated[
        str, typer.Option(help="blueprints/<name>.toml or a path whose sandbox/targets to check")
    ] = blueprint_mod.DEFAULT_BLUEPRINT,
    json_out: Annotated[bool, typer.Option("--json", help="machine-readable report")] = False,
) -> None:
    """Pre-flight the real path: islo auth + integrations, gateway profile, environment,
    snapshot, gh auth + repo, claude, srt, blueprint, factory.toml. Exit 1 on any failure."""
    from swfactory import doctor as doctor_mod

    try:
        bp = blueprint_mod.load(blueprint)
        cfg = bp.config(bp.jobs({"issues": ["doctor"]})[0], run_id="doctor")
    except (OSError, ValueError) as e:
        # A broken blueprint is itself a finding: report it with Config defaults instead of dying.
        typer.echo(f"blueprint error: {e}", err=True)
        cfg = Config(issue="doctor", blueprint=blueprint)
    checks = doctor_mod.run_doctor(cfg)
    typer.echo(doctor_mod.to_json(checks) if json_out else doctor_mod.table(checks))
    raise typer.Exit(doctor_mod.exit_code(checks))


@app.command()
def herd(
    airflow_url: Annotated[str, typer.Option(envvar="AIRFLOW_URL")] = "http://localhost:8080",
    repo: Annotated[str | None, typer.Option(help="owner/name (default: blueprint target)")] = None,
    owner: Annotated[
        str | None, typer.Option(envvar="SWF_SANDBOX_OWNER", help="only this creator's sandboxes")
    ] = None,
    token: Annotated[str | None, typer.Option(envvar="AIRFLOW_TOKEN", help="API JWT")] = None,
    username: Annotated[str | None, typer.Option(envvar="AIRFLOW_USER")] = None,
    password: Annotated[str | None, typer.Option(envvar="AIRFLOW_PASSWORD")] = None,
    metrics_root: Annotated[Path, typer.Option(help="root with docs/factory/*/metrics.json")] = (
        Path()
    ),
    refresh_s: Annotated[float, typer.Option(help="auto-refresh interval")] = 5.0,
    once: Annotated[
        bool, typer.Option("--once", help="print one snapshot and exit, no TUI (CI, scripts)")
    ] = False,
    json_out: Annotated[
        bool, typer.Option("--json", help="machine-readable snapshot (implies --once)")
    ] = False,
    approve_all: Annotated[
        bool,
        typer.Option(
            "--approve-all",
            help="answer every pending gate of the configured blueprints, then exit (no TUI)",
        ),
    ] = False,
    reject: Annotated[
        bool, typer.Option("--reject", help="with --approve-all: reject every pending gate")
    ] = False,
) -> None:
    """Control room: pending gates (approve/reject), runs and their jobs, PRs, own sandboxes,
    metrics. A TUI by default; `--once [--json]` prints one snapshot and `--approve-all
    [--reject]` answers every pending gate, both over the same clients. Exit 1 if a gate answer
    failed."""
    from swfactory.blueprint import load
    from swfactory.herd import main as herd_main

    target_repo = repo or load("factory").targets[0].repo
    code = herd_main(
        airflow_url=airflow_url,
        repo=target_repo,
        owner=owner,
        token=token,
        username=username,
        password=password,
        metrics_root=str(metrics_root),
        refresh_s=refresh_s,
        once=once,
        json_out=json_out,
        approve_all_gates=approve_all,
        reject=reject,
        out=typer.echo,
    )
    raise typer.Exit(code)


if __name__ == "__main__":  # pragma: no cover
    app()
