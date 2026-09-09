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
from swfactory.approval_policy import SCRIPTED_REPLAY_FIXTURE
from swfactory.blueprint import Blueprint
from swfactory.config import FACTORY_ROOT, Config
from swfactory.dispatch import DEFAULT_INBOX, DeliveryConflict, DeliveryInbox
from swfactory.models import RunReport, StageError
from swfactory.runtime import build_ctx, ctx_for, job_run_dir
from swfactory.scm import make_scm
from swfactory.stages import Approver, Ctx, cli_approver, run_pipeline, setup

app = typer.Typer(help="AI-native software factory.", no_args_is_help=True, add_completion=False)


@app.command("backend")
def backend_serve(host: str = "127.0.0.1", port: int = 8082) -> None:
    """Serve the factory API for the Rust console; credentials come from backend environment."""
    from swfactory.backend import serve

    try:
        serve(host, port)
    except (ValueError, OSError) as error:
        typer.echo(f"backend: {error}", err=True)
        raise typer.Exit(1) from error


SCRIPTED_BANNER = "SCRIPTED REPLAY — agent=scripted, no model calls"


# ---------------------------------------------------------------- wiring shared by run/demo


def run_ctx(ctx: Ctx, approver: Approver = cli_approver) -> RunReport:
    """``setup`` then the blueprint's pipeline walk; the report also lands in
    ``<run_dir>/report.json``."""
    result = setup(ctx)
    print(f"{'setup':<16} {result.status:<8} {result.duration_s:6.1f}s  sandbox={ctx.sb.name}")
    report = run_pipeline(ctx, approver)
    (ctx.run_dir / "report.json").write_text(report.model_dump_json(indent=2) + "\n", encoding="utf-8")
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


def _run_jobs(bp: Blueprint, issues: list[str], overrides: dict[str, Any], *, targets: list[str] | None = None) -> None:
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
        except StageError as e:
            typer.echo(f"run unavailable: {e}", err=True)
            failed = True
            continue
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
    blueprint: Annotated[str, typer.Option(help="blueprints/<name>.toml or a path")] = blueprint_mod.DEFAULT_BLUEPRINT,
    target: Annotated[
        list[str] | None, typer.Option(help="only these blueprint targets (owner/name, repeatable)")
    ] = None,
    repo: Annotated[str | None, typer.Option(help="owner/name of the target repo")] = None,
    target_dir: Annotated[str | None, typer.Option(help="subdir the factory operates on")] = None,
    agent: Annotated[str | None, typer.Option(help="claude | scripted")] = None,
    sandbox: Annotated[str | None, typer.Option(help="local | islo | srt | docker | toolset")] = None,
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
    real: Annotated[bool, typer.Option(help="claude agent, islo sandbox, github scm, prompt")] = False,
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
        # The scripted demo is a replay, so it answers the human gates through the declared replay
        # fixture. ``approve="auto"`` cannot do this any more (#2066): configuration is not an
        # approver, and the fixture is refused for backend-managed work.
        else {
            "agent": "scripted",
            "sandbox": "local",
            "scm": "local",
            "gate_replay": str(SCRIPTED_REPLAY_FIXTURE),
        }
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
    root: Annotated[Path, typer.Option(help="checkout to scan for docs/factory/*/metrics.json")] = Path("."),
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
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method="PATCH")
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
    bands: Annotated[Path, typer.Option(help="Response tiers (sigma bands).")] = (FACTORY_ROOT / "bands.yaml"),
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
    breaches = maintain_mod.run(cfg, scm=make_scm(cfg, run_dir), agent=None, sb=None, bands_path=bands, root=root)
    for b in breaches:
        typer.echo(f"{b.action:8s} {b.metric}: {b.value:g} vs mean {b.mean:g}±{b.stdev:g} ({b.sigma}σ)")
    if not breaches:
        typer.echo("no breaches")
    if sweep_ttl_s:
        for name in maintain_mod.sweep_sandboxes(sweep_ttl_s, owner=owner):
            typer.echo(f"removed orphan sandbox {name}")


# ---------------------------------------------------------------- local host-owned evidence

state_app = typer.Typer(help="Inspect saved host run evidence without reconnecting to sandboxes.", no_args_is_help=True)
app.add_typer(state_app, name="state")


@state_app.command("list")
def state_list(
    root: Annotated[Path, typer.Option(help="directory containing saved run directories")] = Path(".factory"),
    limit: Annotated[int, typer.Option(min=1, max=1000)] = 50,
    attention: Annotated[bool, typer.Option(help="only interrupted, failed or damaged runs")] = False,
    as_json: Annotated[bool, typer.Option("--json", help="one JSON document")] = False,
) -> None:
    """List the most recently changed local runs, their ownership and recorded spend."""
    from swfactory.inspection import list_runs

    try:
        runs = list_runs(root, limit=limit)
    except (OSError, ValueError) as error:
        typer.echo(f"saved state unavailable: {error}", err=True)
        raise typer.Exit(2) from error
    if attention:
        runs = [
            run
            for run in runs
            if run["interrupted"] or run["errors"] or run["torn_tail_bytes"] or run["last_event"] == "failed"
        ]
    if as_json:
        typer.echo(json.dumps({"runs": runs}))
        return
    if not runs:
        typer.echo("No matching saved runs.")
    for run in runs:
        status = (
            "damaged"
            if run["errors"]
            else "active"
            if run["held"]
            else "interrupted"
            if run["interrupted"]
            else run["last_event"] or "saved"
        )
        cost = run["recorded_cost_usd"]
        amount = f"${cost:.4f}" if cost is not None else "unknown"
        typer.echo(
            f"{run['run_id']}  {status:<11}  {run['repo'] or '-'} "
            f"#{run['issue_id'] or '-'}  {run['last_operation'] or '-'}  {amount}"
        )


@state_app.command("inspect")
def state_inspect(
    run_id: Annotated[str, typer.Argument(help="saved factory run ID")],
    root: Annotated[Path, typer.Option(help="directory containing saved run directories")] = Path(".factory"),
    events: Annotated[int, typer.Option(min=1, max=1000, help="recent operation records")] = 50,
) -> None:
    """Print identity, stage evidence, operation ownership and journal health as JSON."""
    from swfactory.inspection import inspect_run

    try:
        details = inspect_run(root, run_id, event_limit=events)
    except FileNotFoundError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(3) from error
    except (OSError, ValueError) as error:
        typer.echo(f"saved state unavailable: {error}", err=True)
        raise typer.Exit(2) from error
    typer.echo(json.dumps(details, indent=2))
    if details["errors"]:
        raise typer.Exit(1)


@state_app.command("reaccept")
def state_reaccept(
    run_id: Annotated[str, typer.Argument(help="saved factory run ID")],
    actor: Annotated[str, typer.Option(help="who is re-opening this epoch's inputs")],
    reason: Annotated[str, typer.Option(help="why they changed, e.g. 'issue #7 edited by author'")],
    root: Annotated[Path, typer.Option(help="directory containing saved run directories")] = Path(".factory"),
) -> None:
    """Retire this run's accepted inputs so the next task admits the current ones, on the record.

    The documented answer to "the issue was edited mid-run": without it a changed input is a stuck
    Cell. Every earlier approval keeps the digest it was given for, so delivery refuses on it and
    every gate must be answered again -- this re-opens the inputs, it does not re-authorize them.
    Backend-managed Cells re-accept by advancing their epoch through the backend instead.
    """
    from swfactory import accepted_inputs
    from swfactory.paths import confined_path, validate_run_id
    from swfactory.state import RunState

    try:
        state = RunState(confined_path(Path(root).expanduser().resolve(), validate_run_id(run_id)))
        retired = accepted_inputs.reaccept(state, actor=actor, reason=reason)
    except (OSError, ValueError) as error:
        typer.echo(f"saved state unavailable: {error}", err=True)
        raise typer.Exit(2) from error
    except StageError as error:
        typer.echo(f"cannot re-accept: {error}", err=True)
        raise typer.Exit(1) from error
    typer.echo(f"retired {retired.digest}; the next task admits the current inputs.")
    typer.echo("Re-answer every gate: approvals given for the retired inputs no longer publish.")


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
    inbox: Annotated[
        Path, typer.Option(envvar="SWF_WEBHOOK_INBOX", help="persistent webhook SQLite database")
    ] = DEFAULT_INBOX,
    max_pending: Annotated[
        int,
        typer.Option(min=1, envvar="SWF_WEBHOOK_MAX_PENDING", help="undispatched receipt limit"),
    ] = 10_000,
    max_attempts: Annotated[
        int,
        typer.Option(min=1, envvar="SWF_WEBHOOK_MAX_ATTEMPTS", help="dispatch attempts per cycle"),
    ] = 12,
    backend_url: Annotated[
        str,
        typer.Option(
            envvar="SWF_BACKEND_URL",
            help="managed work-order boundary; unset = LEGACY unmanaged direct-Airflow dispatch",
        ),
    ] = "",
    backend_token_env: Annotated[
        str, typer.Option(help="env var holding the backend bearer token")
    ] = "SWF_BACKEND_TOKEN",
) -> None:
    """Persist signed work before replying, then submit it through the backend's work-order
    boundary with retries. GET /readyz reports intake capacity. Without --backend-url this falls
    back to legacy direct-Airflow dispatch, whose runs carry no Factory Cell bindings; that mode
    needs AIRFLOW_TOKEN or AIRFLOW_USER + AIRFLOW_PASSWORD."""
    import os
    import sqlite3

    from swfactory import webhook as webhook_mod

    try:
        # Managed mode holds no Airflow credential at all: the only mutation this process can make
        # is a work order, so a bug here cannot become a run the backend never admitted.
        orders = None
        provider = None
        if backend_url:
            token = os.environ.get(backend_token_env, "")
            if not token:
                raise ValueError(f"set {backend_token_env} to submit work orders to {backend_url}")
            orders = webhook_mod.WorkOrders(webhook_mod._safe_backend_base(backend_url), token)
        else:
            provider = webhook_mod.token_provider_from_env(airflow_url)
        queue = DeliveryInbox(inbox, max_pending=max_pending)
        if orders is None:
            queue.bind(airflow_url=webhook_mod._safe_airflow_base(airflow_url))
        else:
            queue.bind(work_order_url=orders.url)
    except (ValueError, OSError, sqlite3.Error) as e:
        typer.echo(f"webhook: {e}", err=True)
        raise typer.Exit(2) from e
    if orders is None:
        typer.echo(
            "webhook: LEGACY mode -- dispatching straight to Airflow, so these runs get no managed "
            "admission, capacity accounting or Factory Cell bindings. Set --backend-url.",
            err=True,
        )
    webhook_mod.serve(
        port,
        airflow_url=airflow_url,
        token_provider=provider,
        secret=os.environ.get(secret_env) or None,
        host=host,
        inbox=queue,
        max_attempts=max_attempts,
        work_orders=orders,
    )


def _webhook_inbox(path: Path) -> DeliveryInbox:
    """Operator reads must not create an empty database because a path was mistyped."""
    import sqlite3

    if not path.expanduser().is_file():
        typer.echo(f"webhook inbox does not exist: {path}", err=True)
        raise typer.Exit(3)
    try:
        return DeliveryInbox(path)
    except (OSError, ValueError, sqlite3.Error) as exc:
        typer.echo(f"webhook inbox unavailable: {exc}", err=True)
        raise typer.Exit(1) from exc


@webhook_app.command("deliveries")
def webhook_deliveries(
    inbox: Annotated[Path, typer.Option(envvar="SWF_WEBHOOK_INBOX", help="receiver's SQLite database")] = DEFAULT_INBOX,
    state: Annotated[str | None, typer.Option(help="pending | dispatching | dispatched | dead")] = None,
    limit: Annotated[int, typer.Option(min=1, max=1000)] = 50,
    as_json: Annotated[bool, typer.Option("--json", help="one JSON document")] = False,
) -> None:
    """Inspect dispatch receipts.

    "dispatched" means the far side accepted the work, not that its run passed. In managed mode the
    admission column is the one that separates a queued order from an executing one.
    """
    import sqlite3

    queue = _webhook_inbox(inbox)
    try:
        deliveries = queue.list(state=state, limit=limit)
        summary = queue.summary()
    except (ValueError, sqlite3.Error) as exc:
        typer.echo(f"webhook: {exc}", err=True)
        raise typer.Exit(2) from exc
    if as_json:
        typer.echo(json.dumps({**summary, "deliveries": [d.public() for d in deliveries]}))
        return
    typer.echo("  ".join(f"{key}={value}" for key, value in summary["counts"].items()))
    for delivery in deliveries:
        typer.echo(
            f"{delivery.delivery_id}  {delivery.state:<11}  {delivery.repository}  "
            f"{delivery.dag_id}  attempts={delivery.attempts}/{delivery.total_attempts}  "
            f"admission={delivery.admission_state or '-'}  "
            f"{delivery.last_error or delivery.work_order_id or delivery.dag_run_id}"
        )


@webhook_app.command("inspect")
def webhook_inspect(
    delivery_id: Annotated[str, typer.Argument(help="X-GitHub-Delivery identity")],
    inbox: Annotated[Path, typer.Option(envvar="SWF_WEBHOOK_INBOX", help="receiver's SQLite database")] = DEFAULT_INBOX,
) -> None:
    """Print one receipt and its frozen Airflow configuration as JSON."""
    import sqlite3

    try:
        receipt = _webhook_inbox(inbox).get(delivery_id)
    except KeyError as exc:
        typer.echo("webhook delivery not found", err=True)
        raise typer.Exit(3) from exc
    except sqlite3.Error as exc:
        typer.echo("webhook inbox unavailable", err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(receipt.public(), indent=2))


@webhook_app.command("retry")
def webhook_retry(
    delivery_id: Annotated[str, typer.Argument(help="dead dispatch to requeue after repairing cause")],
    inbox: Annotated[Path, typer.Option(envvar="SWF_WEBHOOK_INBOX", help="receiver's SQLite database")] = DEFAULT_INBOX,
) -> None:
    """Requeue one dead delivery with the SAME run id. Does not rerun an existing Airflow job."""
    import sqlite3

    try:
        receipt = _webhook_inbox(inbox).retry(delivery_id)
    except KeyError as exc:
        typer.echo("webhook delivery not found", err=True)
        raise typer.Exit(3) from exc
    except DeliveryConflict as exc:
        typer.echo(f"webhook: {exc}", err=True)
        raise typer.Exit(6) from exc
    except sqlite3.Error as exc:
        typer.echo("webhook inbox unavailable", err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(receipt.public(), indent=2))


@webhook_app.command("route")
def webhook_route(
    event: Annotated[str, typer.Argument(help="X-GitHub-Event value: issues | issue_comment")],
    payload: Annotated[Path, typer.Argument(help="path to the event payload JSON")],
    repository_check: Annotated[
        bool, typer.Option(help="validate repository.full_name against the installed blueprint")
    ] = False,
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
    if repository_check:
        try:
            trigger = webhook_mod.repository_trigger(trigger, webhook_mod._repository(data))
        except (OSError, ValueError) as exc:
            typer.echo(f"webhook route rejected: {exc}", err=True)
            raise typer.Exit(2) from exc
    typer.echo(f"POST /api/v2/dags/{trigger.dag_id}/dagRuns {json.dumps(trigger.body(), sort_keys=True)}")


# ---------------------------------------------------------------- whole-factory backup and restore

backup_app = typer.Typer(
    help="Coordinated backup, validated restore and reconciliation for the factory state root.",
    no_args_is_help=True,
)
app.add_typer(backup_app, name="backup")

StateRoot = Annotated[Path, typer.Option(help="the one shared local factory state root")]


@backup_app.command("create")
def backup_create(
    dest: Annotated[Path, typer.Argument(help="empty directory to write this backup into")],
    state_root: StateRoot = Path(".factory"),
    actor: Annotated[str, typer.Option(help="who is taking this backup")] = "operator",
) -> None:
    """Quiesce every authoritative store and write one manifest-covered backup."""
    from swfactory.restore_contract import BackupRefused, create_backup

    try:
        manifest = create_backup(state_root, dest, actor=actor)
    except (BackupRefused, OSError, ValueError) as error:
        typer.echo(f"backup refused: {error}", err=True)
        raise typer.Exit(1) from error
    stores = ", ".join(f"{s['name']}@{s['schema_version']}" for s in manifest["stores"])
    typer.echo(f"{dest}: {len(manifest['files'])} files, {stores}")
    typer.echo(manifest["manifest_digest"])


@backup_app.command("verify")
def backup_verify(
    backup_dir: Annotated[Path, typer.Argument(help="a directory written by `backup create`")],
) -> None:
    """Prove a backup is complete and unmodified before anything depends on it."""
    from swfactory.restore_contract import verify_backup

    result = verify_backup(backup_dir)
    for problem in result.problems:
        typer.echo(f"problem: {problem}", err=True)
    if not result.ok:
        raise typer.Exit(1)
    typer.echo(f"{backup_dir}: verified {result.manifest['manifest_digest']}")


@backup_app.command("restore")
def backup_restore(
    backup_dir: Annotated[Path, typer.Argument(help="a verified backup directory")],
    state_root: StateRoot = Path(".factory"),
    actor: Annotated[str, typer.Option(help="who is restoring")] = "operator",
    reason: Annotated[str, typer.Option(help="why this restore is happening")] = "",
    replace_existing: Annotated[
        bool, typer.Option(help="set aside existing state (it is moved, never deleted)")
    ] = False,
) -> None:
    """Validate a backup, refuse an old-binary rollback, and restore with mutations withheld."""
    from swfactory.restore_contract import RestoreRefused, restore

    if not reason.strip():
        typer.echo("--reason is required: a restore is an operator decision that must be on the record", err=True)
        raise typer.Exit(2)
    try:
        marker = restore(backup_dir, state_root, actor=actor, reason=reason, replace_existing=replace_existing)
    except (RestoreRefused, OSError) as error:
        typer.echo(f"restore refused: {error}", err=True)
        raise typer.Exit(1) from error
    typer.echo(f"restored into {state_root}; external effects are WITHHELD until validated.")
    typer.echo(f"cells needing reconciliation: {len(marker['unreconciled_cells'])}")
    typer.echo("next: swfactory backup status, then swfactory backup resume")


@backup_app.command("status")
def backup_status(
    state_root: StateRoot = Path(".factory"),
    json_out: Annotated[bool, typer.Option("--json", help="one JSON document")] = False,
) -> None:
    """Report store schema versions, the restore gate and what must be reconciled first."""
    from swfactory.restore_contract import status as restore_status

    report = restore_status(state_root)
    if json_out:
        typer.echo(json.dumps(report, indent=2, sort_keys=True))
    else:
        for store in report["stores"]:
            typer.echo(f"{store['name']:<11} {str(store['version']):>4} / {store['expected']:<4} {store['status']}")
        for problem in report["coherence_problems"]:
            typer.echo(f"stores disagree: {problem}")
        typer.echo(f"restore gate: {report['restore']['state']}")
        if report["observation_required"]:
            span = report["restore_window"].get("unobserved_seconds")
            length = f"{span:.0f}s" if isinstance(span, float) else "an unknown interval"
            typer.echo(
                f"  every Cell must observe the remote before its first attempt: the restore window "
                f"covers {length} this state cannot see. Close it with `swfactory backup close`."
            )
        for rollback in report["restore"].get("fence_rollbacks", []):
            typer.echo(
                f"  fence rolled back: {rollback['cell_id']} epoch {rollback['was']} -> "
                f"{rollback['now']}; a process still holding epoch {rollback['was']} can write again"
            )
        for cell in report["restore"]["cells"]:
            typer.echo(f"  awaiting observation: {cell}")
        for row in report["reconciliation"]:
            typer.echo(f"  {row['operation_key']} {row['kind']} -> {row['action']} ({row['reason']})")
    if not report["schema_compatible"] or not report["stores_agree"]:
        raise typer.Exit(2)
    if not report["mutations_allowed"]:
        raise typer.Exit(1)


@backup_app.command("resume")
def backup_resume(
    state_root: StateRoot = Path(".factory"),
    actor: Annotated[str, typer.Option(help="who validated this restore")] = "operator",
    reason: Annotated[str, typer.Option(help="what was checked")] = "",
) -> None:
    """Allow mutations again after a restore -- each restored Cell must still observe before acting."""
    from swfactory.restore_contract import MutationsWithheld, RestoreGate, RestoreRefused

    if not reason.strip():
        typer.echo("--reason is required: resuming mutations is an operator decision", err=True)
        raise typer.Exit(2)
    gate = RestoreGate(state_root)
    try:
        marker = gate.resume(actor=actor, reason=reason)
    except (RestoreRefused, MutationsWithheld) as error:
        typer.echo(f"resume refused: {error}", err=True)
        raise typer.Exit(1) from error
    typer.echo(f"restore gate: {marker['state']}")
    typer.echo(f"{len(marker['unreconciled_cells'])} cells must observe the remote before re-driving anything")


@backup_app.command("reconciled")
def backup_reconciled(
    state_root: StateRoot = Path(".factory"),
    cell_id: Annotated[str | None, typer.Option(help="Cell whose remote state was observed")] = None,
    work_id: Annotated[str | None, typer.Option(help="dispatch intent whose delivery was observed")] = None,
) -> None:
    """Clear one Cell or dispatch intent from the gate, on recorded observations only."""
    from swfactory.restore_contract import ReconciliationIncomplete, RestoreGate

    if not cell_id and not work_id:
        typer.echo("pass --cell-id or --work-id", err=True)
        raise typer.Exit(2)
    try:
        marker = RestoreGate(state_root).mark_reconciled(cell_id=cell_id, work_id=work_id)
    except ReconciliationIncomplete as error:
        typer.echo(f"not reconciled: {error}", err=True)
        raise typer.Exit(1) from error
    typer.echo(f"restore gate: {marker['state']}; {len(marker['unreconciled_cells'])} cells outstanding")
    if not marker["unreconciled_cells"] and not marker["unreconciled_dispatch"]:
        typer.echo("every restored item is reconciled; close the window with `swfactory backup close`")


@backup_app.command("close")
def backup_close(
    state_root: StateRoot = Path(".factory"),
    actor: Annotated[str, typer.Option(help="who is closing the restore window")] = "operator",
    reason: Annotated[str, typer.Option(help="what was reviewed")] = "",
    window_reviewed: Annotated[
        bool,
        typer.Option(help="the interval between the backup and the restore was checked against the remote"),
    ] = False,
) -> None:
    """End the restore window, after which Cells stop observing the remote before they act.

    The interval between the backup and the restore holds effects no local record mentions, so
    ending it is an operator's statement rather than something the factory can conclude on its own.
    """
    from swfactory.restore_contract import ReconciliationIncomplete, RestoreGate

    if not reason.strip():
        typer.echo("--reason is required: closing the restore window is an operator decision", err=True)
        raise typer.Exit(2)
    try:
        marker = RestoreGate(state_root).close(actor=actor, reason=reason, window_reviewed=window_reviewed)
    except ReconciliationIncomplete as error:
        typer.echo(f"window stays open: {error}", err=True)
        raise typer.Exit(1) from error
    typer.echo(f"restore gate: {marker['state']}; observation before first attempt is no longer required")


@app.command("claim")
def claim_cmd(
    issue: Annotated[str, typer.Argument(help="issue id or path, as passed to `run`")],
    repo: Annotated[str | None, typer.Option(help="owner/name of the target repo")] = None,
    target_dir: Annotated[str | None, typer.Option(help="subdir in the target repo")] = None,
    state_root: StateRoot = Path(".factory"),
    json_out: Annotated[bool, typer.Option("--json", help="machine-readable")] = False,
) -> None:
    """Print this session's identity and the claim ref for one issue x target.

    Many harness sessions loop over one backlog. `publication_identity` already stops two of them
    publishing twice, but by then both have burned a full agent loop -- two sandboxes, two model
    budgets, two sets of provider calls. The claim is where a session decides whether to spend that
    energy at all: `refs/swf/claims/<key>` is created by exactly one pusher, because git's ref
    creation is a compare-and-swap the server enforces.

    This prints the coordinates rather than taking the claim: the claim is taken by the push, and a
    command that both reports and mutates would hide which of the two happened.
    """
    from swfactory import work_claim
    from swfactory.publication_identity import instance_id, publication_key
    from swfactory.runtime import locate
    from swfactory.scm import parse_issue_file

    cfg = Config(issue=issue, **{k: v for k, v in (("repo", repo), ("target_dir", target_dir)) if v})
    # The issue's declared id, not its filename. `demo/issue.md` carries `id: DEMO-1` in its front
    # matter, and the branch a run publishes is keyed on that -- printing the stem would send an
    # operator to a ref that does not exist.
    issue_id = issue if issue.strip().isdigit() else parse_issue_file(Path(locate(issue))).id
    key = publication_key(cfg.repo, cfg.target_dir, issue_id)
    document = {
        "issue": issue,
        "repo": cfg.repo,
        "target_dir": cfg.target_dir,
        "publication_key": key,
        "claim_ref": work_claim.claim_ref(key),
        "issue_id": issue_id,
        "branch": f"factory/{issue_id}-{key}",
        "instance": instance_id(state_root, create=True),
        "lease_s": work_claim.DEFAULT_LEASE_S,
    }
    if json_out:
        typer.echo(json.dumps(document, indent=2))
        return
    width = max(len(k) for k in document)
    for name, value in document.items():
        typer.echo(f"{name.replace('_', ' '):<{width}}  {value}")


@app.command()
def doctor(
    blueprint: Annotated[
        str, typer.Option(help="blueprints/<name>.toml or a path whose sandbox/targets to check")
    ] = blueprint_mod.DEFAULT_BLUEPRINT,
    repo: Annotated[str | None, typer.Option(help="owner/name of the target repo")] = None,
    target_dir: Annotated[str | None, typer.Option(help="subdir in the target repo")] = None,
    agent: Annotated[str, typer.Option(help="claude | scripted")] = "claude",
    sandbox: Annotated[str | None, typer.Option(help="local | islo | srt | docker | toolset")] = None,
    scm: Annotated[str, typer.Option(help="local | github")] = "github",
    allow_local_agent: Annotated[bool, typer.Option(help="DEV: allow a real agent outside a sandbox")] = False,
    json_out: Annotated[bool, typer.Option("--json", help="machine-readable report")] = False,
) -> None:
    """Pre-flight the selected agent, sandbox, SCM, blueprint, and target contract."""
    from swfactory import doctor as doctor_mod

    try:
        bp = blueprint_mod.load(blueprint)
        job = bp.jobs({"issues": ["doctor"]})[0]
    except (OSError, ValueError) as e:
        # A broken blueprint is itself a finding: report it with Config defaults instead of dying.
        typer.echo(f"blueprint error: {e}", err=True)
        cfg = Config(issue="doctor", blueprint=blueprint)
    else:
        try:
            cfg = bp.config(
                job,
                run_id="doctor",
                repo=repo,
                target_dir=target_dir,
                agent=agent,
                sandbox=sandbox,
                scm=scm,
                allow_local_agent=allow_local_agent or None,
            )
        except ValueError as e:
            typer.echo(f"config error: {e}", err=True)
            raise typer.Exit(2) from e
    checks = doctor_mod.run_doctor(cfg)
    typer.echo(doctor_mod.to_json(checks) if json_out else doctor_mod.table(checks))
    raise typer.Exit(doctor_mod.exit_code(checks))


@app.command()
def herd(
    airflow_url: Annotated[str, typer.Option(envvar="AIRFLOW_URL")] = "http://localhost:8080",
    repo: Annotated[str | None, typer.Option(help="owner/name (default: blueprint target)")] = None,
    owner: Annotated[str | None, typer.Option(envvar="SWF_SANDBOX_OWNER", help="only this creator's sandboxes")] = None,
    token: Annotated[str | None, typer.Option(envvar="AIRFLOW_TOKEN", help="API JWT")] = None,
    username: Annotated[str | None, typer.Option(envvar="AIRFLOW_USER")] = None,
    password: Annotated[str | None, typer.Option(envvar="AIRFLOW_PASSWORD")] = None,
    metrics_root: Annotated[Path, typer.Option(help="root with docs/factory/*/metrics.json")] = (Path()),
    refresh_s: Annotated[float, typer.Option(help="auto-refresh interval")] = 5.0,
    once: Annotated[bool, typer.Option("--once", help="print one snapshot and exit, no TUI (CI, scripts)")] = False,
    json_out: Annotated[bool, typer.Option("--json", help="machine-readable snapshot (implies --once)")] = False,
    approve_all: Annotated[
        bool,
        typer.Option(
            "--approve-all",
            help="answer every pending gate of the configured blueprints, then exit (no TUI)",
        ),
    ] = False,
    reject: Annotated[bool, typer.Option("--reject", help="with --approve-all: reject every pending gate")] = False,
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
