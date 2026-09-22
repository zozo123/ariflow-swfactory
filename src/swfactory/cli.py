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
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer

from swfactory import blueprint as blueprint_mod
from swfactory import metrics as metrics_mod
from swfactory import runtime as runtime_mod
from swfactory.agent import Agent
from swfactory.approval_policy import SCRIPTED_REPLAY_FIXTURE
from swfactory.blueprint import Blueprint
from swfactory.config import FACTORY_ROOT, Config
from swfactory.dispatch import DEFAULT_INBOX, DeliveryConflict, DeliveryInbox

# Imported at module level, not deferred like the rest of `improve`: it is the default of a
# typer option, which is evaluated when the command is declared. The module is stdlib-only.
from swfactory.improvement_annealing import DEFAULT_ENROL_CAP
from swfactory.models import RunReport, StageError
from swfactory.runtime import build_ctx, ctx_for, job_config, job_run_dir
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
    ``.factory/<run_id>/report.json``. Exit 1 if any job blocks, fails its tests or errors.

    No ``issues`` means the line's ``trigger.backlog`` decides -- the same selection the scheduled
    fan-out makes, so an operator can run by hand exactly what the cron would have started, with
    every skipped issue and its reason on the terminal (and in ``.factory/backlog/<line>.jsonl``).
    """
    run_id = overrides.pop("run_id", None) or uuid.uuid4().hex[:8]
    if not issues and bp.trigger.backlog is not None:
        from swfactory.intake_governance import drain_line

        try:
            selection = drain_line(bp)
        except StageError as e:
            typer.echo(f"backlog unavailable: {e}", err=True)
            raise typer.Exit(1) from e
        for number, reason in sorted(selection.skipped.items()):
            typer.echo(f"skipped {number}: {reason}")
        issues = [str(candidate.issue) for candidate in selection.selected]
        if not issues:
            typer.echo(f"backlog {bp.trigger.backlog.label!r}: nothing eligible")
            return
    try:
        jobs = bp.jobs({"issues": issues, **({"targets": targets} if targets else {})})
    except ValueError as e:
        typer.echo(f"config error: {e}", err=True)
        raise typer.Exit(2) from e
    # Before the first job provisions anything: every job in one invocation shares the sandbox
    # provider, so one check answers for all of them. What this replaces is the provider's own
    # error several stages in, after a cell already existed.
    try:
        runtime_mod._preflight(job_config(bp, jobs[0], run_id=run_id, overrides=overrides))
    except StageError as e:
        typer.echo(f"run unavailable: {e}", err=True)
        raise typer.Exit(1) from e

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
        list[str] | None,
        typer.Option(help="GitHub issue number or path to a front-matter .md (repeatable); omit to drain the backlog"),
    ] = None,
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
    """Run a blueprint's line on one or more issues (one PR per issue x target), or without
    --issue on whatever the line's `trigger.backlog` selects."""
    _run_jobs(
        _load_blueprint(blueprint),
        issue or [],
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
def improve(
    budget: Annotated[int, typer.Option(help="how many work orders to propose")] = 5,
    as_json: Annotated[bool, typer.Option("--json", help="machine-readable proposal")] = False,
    as_issues: Annotated[
        bool, typer.Option("--as-issues", help="print the gh commands that would enrol these on the liquid line")
    ] = False,
    enrolled: Annotated[
        int,
        typer.Option(
            help="open work orders this loop already enrolled; count them with "
            "`gh issue list --label liquid --state open --json number --jq length`"
        ),
    ] = 0,
    enrol_cap: Annotated[
        int, typer.Option("--enrol-cap", help="soft cap on open enrolled orders before enrolment needs an ack")
    ] = DEFAULT_ENROL_CAP,
    ack_queue: Annotated[
        bool,
        typer.Option("--ack-queue", help="acknowledge a full queue and print the enrolment commands anyway"),
    ] = False,
    record_to: Annotated[
        Path | None, typer.Option("--record", help="append this assessment to a trajectory directory")
    ] = None,
    root: Annotated[Path | None, typer.Option(help="repository root (default: cwd)")] = None,
) -> None:
    """Propose the work the factory's own evidence says it needs, ranked and falsifiable.

    Reads what the line has already measured about itself -- unreachable modules, capability claims
    that never reached `validated`, and delivery metrics that miss their target -- and emits work
    orders the liquid line can drain. Every order names the existing check that retires it, and one
    whose done-condition cites no such check is refused rather than emitted.

    It proposes only. The gates and the merge button are untouched.

    ``--enrolled`` is the loop's second observation, and it opposes the first: the annealer widens
    the budget when nothing is retiring, and enrolment pressure narrows it when the queue the loop
    already filed is not draining. Measurement is never suppressed -- every signal is assessed and
    reported at any pressure -- but at the cap ``--as-issues`` refuses until ``--ack-queue`` says a
    human has looked at the queue.
    """
    from swfactory import metrics as improve_metrics
    from swfactory.improvement_annealing import evaluate as anneal
    from swfactory.improvement_annealing import observe
    from swfactory.self_improvement import (
        assess,
        history,
        issue_commands,
        propose,
        record,
        report,
        stalled,
        trajectory_report,
    )

    where = Path(root) if root else Path.cwd()
    ledger = json.loads((where / "config" / "not-yet-wired.json").read_text())["modules"]
    try:
        summary = improve_metrics.summarize(improve_metrics.load_all(where))
    except (OSError, ValueError):
        summary = {}
    # History first: what has stalled decides how this proposal is ranked, so a blocked order stops
    # taking the top slot every cycle. Without a trajectory there is nothing stalled to know about.
    past = history(record_to) if record_to is not None else []
    signals = assess(where, ledger=ledger, summary=summary)
    # Only debt the factory still carries can be stalled; anything retired has already been paid.
    carried = [f"{signal.source.value}:{signal.key}" for signal in signals]
    stuck = stalled(past, present=carried)
    # Annealing reads the trajectory and decides explore-vs-exploit: a loop retiring nothing widens
    # its budget and stops sidestepping the item it keeps avoiding. It shapes the proposal only.
    heat = anneal(
        observe(
            past,
            carried=len(carried),
            stalled=len(stuck),
            sources=len({s.source for s in signals}),
            enrolled=enrolled,
            enrol_cap=enrol_cap,
        ),
        base_budget=budget,
    )
    assessment = propose(signals, budget=heat.budget, stalled_keys=stuck, readmit_stalled=heat.readmit_stalled)
    typer.echo(f"[{heat.mode} T={heat.temperature:.2f} budget={heat.budget}] {heat.reason}\n")
    if as_json:
        typer.echo(json.dumps(assessment.to_dict(), indent=2, sort_keys=True))
        return
    if as_issues:
        if not heat.enrol_allowed and not ack_queue:
            # Refuse the enrolment path, not the assessment: `improve` without --as-issues still
            # prints every signal. A loop whose queue is full has already said what it needs; what
            # it needs next is for someone to drain it, not for it to say the same thing louder.
            typer.echo(
                f"{heat.enrolled} enrolled orders are still open (cap {heat.enrol_cap}). "
                "Close or drain them, or pass --ack-queue to enrol anyway.",
                err=True,
            )
            raise typer.Exit(1)
        # Printed, never run: filing is an outward effect, and the loop proposes rather than acts.
        typer.echo("\n\n".join(issue_commands(assessment.orders)))
        return
    typer.echo(report(assessment.orders))
    if record_to is not None:
        typer.echo("\n" + trajectory_report(past, assessment))
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
        typer.echo(f"recorded: {record(assessment, record_to, at=stamp)}")
    for refusal in assessment.refused:
        typer.echo(f"refused: {refusal}", err=True)


provenance_app = typer.Typer(help="Release artifact provenance: record digests, and verify downloads against them.")


@provenance_app.command("manifest")
def provenance_manifest(
    artifact: Annotated[list[str], typer.Option(help="artifact path (repeatable)")],
    source_sha: Annotated[str, typer.Option(help="commit the artifacts were built from")],
    builder: Annotated[str, typer.Option(help="who built them")] = "github-actions",
    workflow: Annotated[str, typer.Option(help="workflow that built them")] = "release.yml",
    out: Annotated[Path, typer.Option(help="where to write the manifest")] = Path("provenance.json"),
) -> None:
    """Digest release artifacts into a manifest published beside them."""
    from swfactory import provenance as provenance_mod

    paths = [Path(a) for a in artifact]
    missing = [str(p) for p in paths if not p.is_file()]
    if missing:
        typer.echo(f"no such artifact: {', '.join(missing)}", err=True)
        raise typer.Exit(2)
    document = provenance_mod.manifest(source_sha, builder, workflow, paths)
    provenance_mod.write(out, document)
    typer.echo(f"{out}: {len(document.artifacts)} artifacts from {source_sha}")


@provenance_app.command("verify")
def provenance_verify(
    manifest_path: Annotated[Path, typer.Option("--manifest", help="manifest published with the release")],
    root: Annotated[Path, typer.Option(help="directory holding the downloaded artifacts")] = Path("."),
) -> None:
    """Check downloaded artifacts against the manifest published with them. Exit 1 on any mismatch."""
    from swfactory import provenance as provenance_mod

    try:
        document = provenance_mod.load(manifest_path)
    except (OSError, KeyError, ValueError) as error:
        typer.echo(f"unreadable manifest: {error}", err=True)
        raise typer.Exit(2) from error
    ok, failures = provenance_mod.verify(root, document)
    if ok:
        typer.echo(f"verified {len(document.artifacts)} artifacts against {manifest_path}")
        return
    for failure in failures:
        typer.echo(failure, err=True)
    raise typer.Exit(1)


app.add_typer(provenance_app, name="provenance")


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
        int,
        typer.Option(help="Also have the backend ($SWF_BACKEND_URL) remove orphan swf-* sandboxes older than this."),
    ] = 0,
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
        from swfactory.cell_callback import CellCallbackError

        try:
            report = maintain_mod.request_sweep(sweep_ttl_s)
        except CellCallbackError as e:
            typer.echo(f"sweep refused: {e}", err=True)
            raise typer.Exit(1) from e
        for key in ("removed", "kept", "debt", "reconciled"):
            for name in report.get(key, []):
                typer.echo(f"{key:10s} {name}")


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


candidate_worktree_app = typer.Typer(
    help="Create, freeze, and remove isolated Git worktrees for candidate exploration.",
    no_args_is_help=True,
)
app.add_typer(candidate_worktree_app, name="candidate-worktree")


@candidate_worktree_app.command("create")
def candidate_worktree_create(
    candidate_id: Annotated[str, typer.Argument(help="stable candidate logical id")],
    repo_path: Annotated[Path, typer.Option("--repo", help="local Git repository")] = Path("."),
    input_head: Annotated[str, typer.Option("--input-head", help="exact input revision")] = "HEAD",
    root: Annotated[
        Path,
        typer.Option(help="directory that owns disposable candidate worktrees"),
    ] = Path(".factory/candidate-worktrees"),
    receipt: Annotated[
        Path | None,
        typer.Option(help="receipt path; default is next to the candidate worktree"),
    ] = None,
    json_out: Annotated[bool, typer.Option("--json", help="print the receipt as JSON")] = False,
) -> None:
    """Create one detached candidate checkout at the exact input commit."""
    from swfactory.candidate_worktree import CandidateWorktreeError, create_candidate_worktree

    try:
        worktree = create_candidate_worktree(repo_path, candidate_id, input_head, root=root)
    except (OSError, CandidateWorktreeError) as error:
        typer.echo(f"candidate worktree: {error}", err=True)
        raise typer.Exit(2) from error
    receipt_path = receipt or Path(worktree.path).with_suffix(".json")
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(worktree.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if json_out:
        typer.echo(json.dumps(worktree.to_dict(), indent=2, sort_keys=True))
    else:
        typer.echo(f"{receipt_path}: {worktree.candidate_id} @ {worktree.input_head}")
        typer.echo(worktree.path)


@candidate_worktree_app.command("freeze")
def candidate_worktree_freeze(
    receipt: Annotated[Path, typer.Argument(help="receipt written by candidate-worktree create")],
    json_out: Annotated[bool, typer.Option("--json", help="print the frozen revision as JSON")] = False,
) -> None:
    """Freeze a clean, committed candidate answer under its immutable factory ref."""
    from swfactory.candidate_worktree import CandidateWorktree, CandidateWorktreeError, freeze_candidate_worktree

    try:
        worktree = CandidateWorktree.from_dict(json.loads(receipt.read_text(encoding="utf-8")))
        revision = freeze_candidate_worktree(worktree)
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError, CandidateWorktreeError) as error:
        typer.echo(f"candidate worktree: {error}", err=True)
        raise typer.Exit(2) from error
    frozen_receipt = receipt.with_suffix(".frozen.json")
    frozen_receipt.write_text(json.dumps(revision.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if json_out:
        typer.echo(json.dumps(revision.to_dict(), indent=2, sort_keys=True))
    else:
        typer.echo(f"{frozen_receipt}: {revision.output_head}")
        typer.echo(revision.ref)


@candidate_worktree_app.command("remove")
def candidate_worktree_remove(
    receipt: Annotated[Path, typer.Argument(help="receipt written by candidate-worktree create")],
    force: Annotated[bool, typer.Option(help="discard dirty workspace state too")] = False,
) -> None:
    """Remove disposable candidate files; a frozen candidate ref is retained."""
    from swfactory.candidate_worktree import CandidateWorktree, CandidateWorktreeError, remove_candidate_worktree

    try:
        worktree = CandidateWorktree.from_dict(json.loads(receipt.read_text(encoding="utf-8")))
        remove_candidate_worktree(worktree, force=force)
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError, CandidateWorktreeError) as error:
        typer.echo(f"candidate worktree: {error}", err=True)
        raise typer.Exit(2) from error
    typer.echo(f"removed {worktree.path}; retained ref {worktree.ref} if frozen")


candidate_evidence_app = typer.Typer(
    help="Build and verify candidate-local diffs, logs, and artifact evidence.",
    no_args_is_help=True,
)
app.add_typer(candidate_evidence_app, name="candidate-evidence")


@candidate_evidence_app.command("build")
def candidate_evidence_build(
    frozen_receipt: Annotated[Path, typer.Argument(help="candidate-worktree .frozen.json receipt")],
    source_receipt: Annotated[Path, typer.Argument(help="JSON receipt from source-snapshot --json")],
    destination: Annotated[Path, typer.Argument(help="empty directory for retained evidence")],
    repo_path: Annotated[Path, typer.Option("--repo", help="local Git repository")] = Path("."),
    artifact: Annotated[
        list[str] | None,
        typer.Option("--artifact", help="named evidence NAME=PATH; repeatable"),
    ] = None,
) -> None:
    """Bind a frozen candidate to its source snapshot, binary diff, and named evidence files."""
    from swfactory.candidate_evidence import CandidateEvidenceError, build_candidate_evidence_bundle
    from swfactory.candidate_worktree import CandidateRevision, CandidateWorktreeError
    from swfactory.source_snapshot import SourceSnapshot, SourceSnapshotError

    try:
        revision_doc = json.loads(frozen_receipt.read_text(encoding="utf-8"))
        source_doc = json.loads(source_receipt.read_text(encoding="utf-8"))
        revision = CandidateRevision(
            candidate_id=str(revision_doc["candidate_id"]),
            input_head=str(revision_doc["input_head"]),
            output_head=str(revision_doc["output_head"]),
            ref=str(revision_doc["ref"]),
            schema_version=int(revision_doc.get("schema_version", 1)),
        )
        source = SourceSnapshot(**source_doc)
        named: dict[str, Path] = {}
        for value in artifact or []:
            name, separator, path = value.partition("=")
            if not separator or not name.strip() or not path.strip():
                raise ValueError("--artifact must be NAME=PATH")
            if name in named:
                raise ValueError(f"duplicate artifact name: {name}")
            named[name] = Path(path)
        bundle = build_candidate_evidence_bundle(
            repo_path,
            revision,
            source,
            artifacts=named,
            destination=destination,
        )
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        CandidateEvidenceError,
        CandidateWorktreeError,
        SourceSnapshotError,
    ) as error:
        typer.echo(f"candidate evidence: {error}", err=True)
        raise typer.Exit(2) from error
    typer.echo(f"{destination / 'manifest.json'}  {bundle.digest()}")
    typer.echo(destination / "RESULT.md")


@candidate_evidence_app.command("verify")
def candidate_evidence_verify(
    destination: Annotated[Path, typer.Argument(help="candidate evidence bundle directory")],
    repo_path: Annotated[Path, typer.Option("--repo", help="local Git repository")] = Path("."),
    json_out: Annotated[bool, typer.Option("--json", help="print the canonical manifest")] = False,
) -> None:
    """Re-hash retained evidence and re-check the immutable candidate ref."""
    from swfactory.candidate_evidence import CandidateEvidenceError, verify_candidate_evidence_bundle

    try:
        bundle = verify_candidate_evidence_bundle(destination, repo=repo_path)
    except (OSError, CandidateEvidenceError) as error:
        typer.echo(f"candidate evidence: {error}", err=True)
        raise typer.Exit(2) from error
    if json_out:
        document = bundle.canonical_dict()
        document["manifest_digest"] = bundle.digest()
        typer.echo(json.dumps(document, indent=2, sort_keys=True))
    else:
        typer.echo(f"verified {bundle.candidate_id} {bundle.output_head} {bundle.digest()}")


campaign_decision_app = typer.Typer(
    help="Bind deterministic campaign fan-in to retained candidate evidence.",
    no_args_is_help=True,
)
app.add_typer(campaign_decision_app, name="campaign-decision")


def _candidate_evidence_paths(values: list[str] | None) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for value in values or []:
        candidate_id, separator, path = value.partition("=")
        if not separator or not candidate_id.strip() or not path.strip():
            raise ValueError("--candidate-evidence must be CANDIDATE_ID=PATH")
        if candidate_id in paths:
            raise ValueError(f"duplicate candidate evidence: {candidate_id}")
        paths[candidate_id] = Path(path)
    return paths


@campaign_decision_app.command("build")
def campaign_decision_build(
    report: Annotated[Path, typer.Argument(help="stored CampaignReport JSON")],
    destination: Annotated[Path, typer.Argument(help="campaign decision manifest JSON")],
    repo_path: Annotated[Path, typer.Option("--repo", help="local Git repository")] = Path("."),
    candidate_evidence: Annotated[
        list[str] | None,
        typer.Option("--candidate-evidence", help="CANDIDATE_ID=BUNDLE_DIR; repeat for every answered sibling"),
    ] = None,
) -> None:
    """Cryptographically join deterministic selection to every answered sibling's retained evidence."""
    from swfactory.campaign_decision import (
        CampaignDecisionError,
        build_campaign_decision_from_document,
        write_campaign_decision,
    )
    from swfactory.candidate_evidence import CandidateEvidenceError, verify_candidate_evidence_bundle

    try:
        document = json.loads(report.read_text(encoding="utf-8"))
        paths = _candidate_evidence_paths(candidate_evidence)
        bundles = {
            candidate_id: verify_candidate_evidence_bundle(path, repo=repo_path) for candidate_id, path in paths.items()
        }
        manifest = build_campaign_decision_from_document(document, bundles)
        write_campaign_decision(destination, manifest)
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        CampaignDecisionError,
        CandidateEvidenceError,
    ) as error:
        typer.echo(f"campaign decision: {error}", err=True)
        raise typer.Exit(2) from error
    typer.echo(f"{destination}  {manifest.digest()}")


@campaign_decision_app.command("verify")
def campaign_decision_verify(
    manifest_path: Annotated[Path, typer.Argument(help="campaign decision manifest JSON")],
    repo_path: Annotated[Path, typer.Option("--repo", help="local Git repository")] = Path("."),
    candidate_evidence: Annotated[
        list[str] | None,
        typer.Option("--candidate-evidence", help="CANDIDATE_ID=BUNDLE_DIR; repeat for every answered sibling"),
    ] = None,
    json_out: Annotated[bool, typer.Option("--json", help="print the canonical verified manifest")] = False,
) -> None:
    """Re-hash fan-in and every bound answered-candidate evidence bundle."""
    from swfactory.campaign_decision import CampaignDecisionError, verify_campaign_decision
    from swfactory.candidate_evidence import CandidateEvidenceError

    try:
        paths = _candidate_evidence_paths(candidate_evidence)
        manifest = verify_campaign_decision(manifest_path, paths, repo=repo_path)
    except (OSError, ValueError, CampaignDecisionError, CandidateEvidenceError) as error:
        typer.echo(f"campaign decision: {error}", err=True)
        raise typer.Exit(2) from error
    if json_out:
        document = manifest.canonical_dict()
        document["manifest_digest"] = manifest.digest()
        typer.echo(json.dumps(document, indent=2, sort_keys=True))
    else:
        typer.echo(f"verified {manifest.campaign_id} {manifest.digest()}")


@app.command("phase-assess")
def phase_assess_cmd(
    observation_path: Annotated[Path, typer.Argument(help="JSON file containing phase order parameters")],
    previous_phase: Annotated[
        str | None,
        typer.Option("--previous", help="previous phase for hysteresis"),
    ] = None,
    json_out: Annotated[bool, typer.Option("--json", help="machine-readable phase assessment")] = False,
) -> None:
    """Classify factory state and print a search-only control posture."""

    from typing import cast

    from swfactory.phase_control import Phase, PhaseObservation, assess

    phases = {"gas", "liquid", "critical", "crystal", "glass", "jammed"}
    if previous_phase is not None and previous_phase not in phases:
        typer.echo(f"phase assess: unknown previous phase {previous_phase!r}", err=True)
        raise typer.Exit(2)
    try:
        raw = json.loads(observation_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("phase input must be a JSON object")
        payload = raw.get("observation", raw)
        if not isinstance(payload, dict):
            raise ValueError("phase observation must be a JSON object")
        observation = PhaseObservation(**payload)
        assessment = assess(
            observation,
            previous_phase=cast(Phase, previous_phase) if previous_phase is not None else None,
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        typer.echo(f"phase assess: {error}", err=True)
        raise typer.Exit(2) from error

    document = assessment.as_dict()
    if json_out:
        typer.echo(json.dumps(document, indent=2, sort_keys=True))
        return
    recommendation = assessment.recommendation
    typer.echo(
        f"{assessment.phase} -> {recommendation.mode} "
        f"(authority={assessment.authority}, spawn={recommendation.spawn}, "
        f"trajectory={recommendation.trajectory}, verification={recommendation.verification})"
    )
    typer.echo(recommendation.reason)


@app.command("research-schedule")
def research_schedule_cmd(
    max_depth: Annotated[int, typer.Option(help="deepest descendant experiment round")] = 1,
    max_candidates: Annotated[int, typer.Option(help="maximum sibling candidates per round")] = 4,
    json_out: Annotated[bool, typer.Option("--json", help="machine-readable cooling schedule")] = False,
) -> None:
    """Show the deterministic exploration-width cooling schedule."""
    from swfactory.evolution import CampaignError
    from swfactory.research_loop import annealed_strategy_schedule

    try:
        schedule = annealed_strategy_schedule(max_depth, max_candidates=max_candidates)
    except CampaignError as error:
        typer.echo(f"research schedule: {error}", err=True)
        raise typer.Exit(2) from error
    document = {
        "authority": "exploration-only",
        "scheduler": "airflow",
        "rounds": [
            {
                "depth": depth,
                "strategies": [strategy.value for strategy in strategies],
            }
            for depth, strategies in enumerate(schedule)
        ],
    }
    if json_out:
        typer.echo(json.dumps(document, indent=2, sort_keys=True))
        return
    for round_ in document["rounds"]:
        typer.echo(f"depth {round_['depth']}: {' '.join(round_['strategies'])}")


@app.command("research-adapt")
def research_adapt_cmd(
    reports: Annotated[
        list[Path],
        typer.Argument(help="stored CampaignReport JSON files in chronological order"),
    ],
    max_candidates: Annotated[int, typer.Option(help="maximum sibling candidates in the next round")] = 4,
    max_parallel: Annotated[int, typer.Option(help="maximum next-round parallelism")] = 3,
    blackboard_path: Annotated[
        Path | None,
        typer.Option("--blackboard", help="optional recursive-search artifact blackboard JSON"),
    ] = None,
    population_telemetry_path: Annotated[
        Path | None,
        typer.Option(
            "--population-telemetry",
            help="optional retained population telemetry JSON from the previous swarm",
        ),
    ] = None,
    population_execution_report_path: Annotated[
        Path | None,
        typer.Option(
            "--population-execution-report",
            help="optional retained managed population execution report; uses its verified telemetry",
        ),
    ] = None,
    json_out: Annotated[bool, typer.Option("--json", help="machine-readable recursive search plan")] = False,
) -> None:
    """Compress prior campaigns into search laws and adapt the next experiment round."""

    from swfactory.evolution import CampaignError
    from swfactory.population_execution import load_population_execution_report
    from swfactory.population_manifest import population_telemetry_from_document
    from swfactory.recursive_search import (
        ArtifactBlackboard,
        extract_search_laws,
        load_blackboard,
        plan_adaptive_round,
        signal_from_document,
    )

    try:
        if not reports:
            raise CampaignError("research adapt needs at least one campaign report")
        documents = []
        for report_path in reports:
            raw = json.loads(report_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise CampaignError(f"{report_path}: campaign report must be a JSON object")
            documents.append(raw)

        signals = tuple(signal_from_document(document) for document in documents)
        last = documents[-1]
        input_head = str(last.get("input_head") or "")
        selection = last.get("exploration_selection")
        winner_id = selection.get("winner") if isinstance(selection, dict) else None
        if winner_id is not None:
            outcomes = last.get("outcomes")
            if isinstance(outcomes, list):
                winner = next(
                    (row for row in outcomes if isinstance(row, dict) and str(row.get("logical_id")) == str(winner_id)),
                    None,
                )
                if winner is not None and winner.get("output_head"):
                    input_head = str(winner["output_head"])
        if not input_head:
            raise CampaignError("latest campaign does not identify a next input head")

        blackboard = load_blackboard(blackboard_path) if blackboard_path is not None else ArtifactBlackboard()
        if population_telemetry_path is not None and population_execution_report_path is not None:
            raise CampaignError(
                "--population-telemetry and --population-execution-report are mutually exclusive"
            )
        population_telemetry = None
        if population_execution_report_path is not None:
            population_telemetry = load_population_execution_report(
                population_execution_report_path
            ).telemetry
        elif population_telemetry_path is not None:
            raw_telemetry = json.loads(population_telemetry_path.read_text(encoding="utf-8"))
            if not isinstance(raw_telemetry, dict):
                raise CampaignError("population telemetry must be a JSON object")
            population_telemetry = population_telemetry_from_document(raw_telemetry)

        plan = plan_adaptive_round(
            signals,
            depth=signals[-1].depth + 1,
            input_head=input_head,
            blackboard=blackboard,
            max_candidates=max_candidates,
            max_parallel=max_parallel,
            population_telemetry=population_telemetry,
        )
        laws = extract_search_laws(signals)
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError, CampaignError) as error:
        typer.echo(f"research adapt: {error}", err=True)
        raise typer.Exit(2) from error

    document = {
        "authority": "exploration-only",
        "scheduler": "airflow",
        "plan": plan.to_dict(),
        "laws": [
            {
                "law_id": law.law_id,
                "kind": law.kind.value,
                "statement": law.statement,
                "confidence": law.confidence,
                "support": law.support,
                "strategies": [strategy.value for strategy in law.strategies],
                "evidence_digests": list(law.evidence_digests),
                "digest": law.digest(),
            }
            for law in laws
        ],
        "blackboard_digest": blackboard.digest(),
    }
    if json_out:
        typer.echo(json.dumps(document, indent=2, sort_keys=True))
        return

    typer.echo(
        f"depth {plan.depth}: {plan.posture.value} "
        f"strategies={' '.join(strategy.value for strategy in plan.strategies)} "
        f"parallel={plan.max_parallel} phase={plan.phase or '-'} "
        f"compute={plan.estimated_compute_units:.1f}"
    )
    typer.echo(plan.reason)
    if plan.swarm_plan is not None:
        lanes = ", ".join(
            f"{lane.role.value}:{lane.count}@{lane.compute_tier.value}/{lane.context.value}"
            for lane in plan.swarm_plan.lanes
        )
        typer.echo(f"swarm: {lanes}")
    if plan.population_manifest is not None and plan.population_manifest_digest is not None:
        typer.echo(
            f"population: tasks={len(plan.population_manifest.tasks)} manifest={plan.population_manifest_digest}"
        )
    for law in laws:
        typer.echo(f"{law.kind.value}: {law.statement} ({law.confidence:.2f}, n={law.support})")


@app.command("population-bind")
def population_bind(
    plan_path: Annotated[
        Path,
        typer.Argument(help="research-adapt JSON or a direct population-manifest JSON"),
    ],
    choices_path: Annotated[
        Path,
        typer.Argument(help="JSON allowlist for provider/model/runtime diversity choices"),
    ],
    json_out: Annotated[bool, typer.Option("--json", help="emit the bound population manifest")] = False,
) -> None:
    """Bind provider-neutral population tasks to deterministic allowlisted provider choices."""

    from swfactory.population_manifest import (
        PopulationManifestError,
        population_manifest_from_document,
    )
    from swfactory.provider_binding import (
        bind_population_manifest,
        provider_choices_from_document,
    )

    try:
        raw_plan = json.loads(plan_path.read_text(encoding="utf-8"))
        raw_choices = json.loads(choices_path.read_text(encoding="utf-8"))
        if not isinstance(raw_plan, dict) or not isinstance(raw_choices, dict):
            raise PopulationManifestError("population plan and choices must be JSON objects")

        manifest_document = raw_plan
        if isinstance(raw_plan.get("plan"), dict):
            manifest_document = raw_plan["plan"].get("population_manifest")
        if not isinstance(manifest_document, dict):
            raise PopulationManifestError("input does not contain a population_manifest object")

        manifest = population_manifest_from_document(manifest_document)
        choices = provider_choices_from_document(raw_choices)
        bound = bind_population_manifest(manifest, choices=choices)
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        PopulationManifestError,
    ) as error:
        typer.echo(f"population bind: {error}", err=True)
        raise typer.Exit(2) from error

    document = {
        "authority": bound.authority,
        "scheduler": bound.scheduler,
        "population_manifest_digest": manifest.digest(),
        "provider_binding_digest": bound.digest(),
        "binding": bound.canonical_dict(),
    }
    if json_out:
        typer.echo(json.dumps(document, indent=2, sort_keys=True))
        return

    typer.echo(f"population {manifest.digest()} -> binding {bound.digest()} tasks={len(bound.tasks)}")
    for task in bound.tasks:
        typer.echo(
            f"{task.task_id}: provider={task.provider or '-'} model={task.model or '-'} "
            f"runtime={task.runtime or '-'} prompt={task.prompt_variant or '-'}"
        )


@app.command("population-summarize")
def population_summarize(
    plan_path: Annotated[
        Path,
        typer.Argument(help="research-adapt JSON or a direct population-manifest JSON"),
    ],
    receipts_path: Annotated[
        Path,
        typer.Argument(help="JSON array of retained provider BehaviorReceipt documents"),
    ],
    require_complete: Annotated[
        bool,
        typer.Option("--require-complete", help="refuse unless every population task has a receipt"),
    ] = False,
    json_out: Annotated[bool, typer.Option("--json", help="emit population telemetry JSON")] = False,
) -> None:
    """Reduce provider behavior receipts into replayable population telemetry."""

    from swfactory.population_manifest import (
        PopulationManifestError,
        behavior_receipt_from_document,
        population_manifest_from_document,
        summarize_population,
    )

    try:
        raw_plan = json.loads(plan_path.read_text(encoding="utf-8"))
        raw_receipts = json.loads(receipts_path.read_text(encoding="utf-8"))
        if not isinstance(raw_plan, dict):
            raise PopulationManifestError("population plan must be a JSON object")
        if not isinstance(raw_receipts, list):
            raise PopulationManifestError("population receipts must be a JSON array")

        manifest_document = raw_plan
        if isinstance(raw_plan.get("plan"), dict):
            manifest_document = raw_plan["plan"].get("population_manifest")
        if not isinstance(manifest_document, dict):
            raise PopulationManifestError("input does not contain a population_manifest object")

        manifest = population_manifest_from_document(manifest_document)
        receipts = tuple(behavior_receipt_from_document(row) for row in raw_receipts if isinstance(row, dict))
        if len(receipts) != len(raw_receipts):
            raise PopulationManifestError("every population receipt must be a JSON object")
        telemetry = summarize_population(
            manifest,
            receipts,
            require_complete=require_complete,
        )
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        PopulationManifestError,
    ) as error:
        typer.echo(f"population summarize: {error}", err=True)
        raise typer.Exit(2) from error

    document = telemetry.canonical_dict()
    document["telemetry_digest"] = telemetry.digest()
    if json_out:
        typer.echo(json.dumps(document, indent=2, sort_keys=True))
        return

    typer.echo(
        f"population {telemetry.manifest_digest}: answered={telemetry.answered}/"
        f"{telemetry.total_tasks} effective={telemetry.effective_independent_search:.3f} "
        f"correlation={telemetry.mean_correlation:.3f} "
        f"disagreement={telemetry.candidate_disagreement:.3f}"
    )
    typer.echo(f"telemetry={telemetry.digest()}")


@candidate_evidence_app.command("retain")
def candidate_evidence_retain(
    destination: Annotated[Path, typer.Argument(help="verified candidate evidence bundle directory")],
    repo_path: Annotated[Path, typer.Option("--repo", help="local Git repository")] = Path("."),
    store: Annotated[
        Path,
        typer.Option(help="factory-owned content-addressed retention store"),
    ] = Path(".factory/candidate-retention"),
    ttl_hours: Annotated[
        int,
        typer.Option("--ttl-hours", min=1, help="promotion-window retention lease in hours"),
    ] = 168,
    json_out: Annotated[bool, typer.Option("--json", help="print the retention lease")] = False,
) -> None:
    """Import verified evidence into the promotion-window retention store."""
    from datetime import timedelta

    from swfactory.candidate_retention import CandidateRetentionError, retain_candidate_evidence

    try:
        lease = retain_candidate_evidence(
            destination,
            repo=repo_path,
            store=store,
            ttl=timedelta(hours=ttl_hours),
        )
    except (OSError, CandidateRetentionError) as error:
        typer.echo(f"candidate retention: {error}", err=True)
        raise typer.Exit(2) from error
    if json_out:
        typer.echo(json.dumps(lease.to_dict(), indent=2, sort_keys=True))
    else:
        typer.echo(f"retained sha256:{lease.digest} until {lease.expires_at}")


@candidate_evidence_app.command("pin")
def candidate_evidence_pin(
    digest: Annotated[str, typer.Argument(help="candidate evidence digest, with or without sha256:")],
    store: Annotated[
        Path,
        typer.Option(help="factory-owned content-addressed retention store"),
    ] = Path(".factory/candidate-retention"),
) -> None:
    """Prevent a retained candidate bundle from being collected after expiry."""
    from swfactory.candidate_retention import CandidateRetentionError, pin_candidate_evidence

    token = digest.removeprefix("sha256:")
    try:
        lease = pin_candidate_evidence(store, token)
    except (OSError, CandidateRetentionError) as error:
        typer.echo(f"candidate retention: {error}", err=True)
        raise typer.Exit(2) from error
    typer.echo(f"pinned sha256:{lease.digest}")


@candidate_evidence_app.command("unpin")
def candidate_evidence_unpin(
    digest: Annotated[str, typer.Argument(help="candidate evidence digest, with or without sha256:")],
    store: Annotated[
        Path,
        typer.Option(help="factory-owned content-addressed retention store"),
    ] = Path(".factory/candidate-retention"),
) -> None:
    """Return a retained candidate bundle to ordinary promotion-window expiry."""
    from swfactory.candidate_retention import CandidateRetentionError, unpin_candidate_evidence

    token = digest.removeprefix("sha256:")
    try:
        lease = unpin_candidate_evidence(store, token)
    except (OSError, CandidateRetentionError) as error:
        typer.echo(f"candidate retention: {error}", err=True)
        raise typer.Exit(2) from error
    typer.echo(f"unpinned sha256:{lease.digest}; expires {lease.expires_at}")


@candidate_evidence_app.command("gc")
def candidate_evidence_gc(
    store: Annotated[
        Path,
        typer.Option(help="factory-owned content-addressed retention store"),
    ] = Path(".factory/candidate-retention"),
    dry_run: Annotated[bool, typer.Option("--dry-run", help="report expired evidence without deleting it")] = False,
    json_out: Annotated[bool, typer.Option("--json", help="print the sweep report")] = False,
) -> None:
    """Sweep only expired, unpinned candidate evidence owned by the retention store."""
    from swfactory.candidate_retention import CandidateRetentionError, sweep_candidate_evidence

    try:
        report = sweep_candidate_evidence(store, dry_run=dry_run)
    except (OSError, CandidateRetentionError) as error:
        typer.echo(f"candidate retention: {error}", err=True)
        raise typer.Exit(2) from error
    if json_out:
        typer.echo(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    typer.echo(f"removed={len(report.removed)} retained={len(report.retained)} malformed={len(report.malformed)}")
    if report.malformed:
        typer.echo("refused malformed: " + ", ".join(report.malformed), err=True)


@app.command("experiment-tree")
def experiment_tree_cmd(
    reports: Annotated[
        list[Path],
        typer.Argument(help="campaign report JSON files, in experiment depth order"),
    ],
    json_out: Annotated[bool, typer.Option("--json", help="machine-readable validated tree")] = False,
    mermaid: Annotated[bool, typer.Option("--mermaid", help="GitHub-compatible Mermaid flowchart")] = False,
) -> None:
    """Validate and render stacked candidate campaign lineage."""
    from swfactory.experiment_tree import load_round, render, render_mermaid, stack_rounds

    if json_out and mermaid:
        typer.echo("experiment tree: choose only one of --json or --mermaid", err=True)
        raise typer.Exit(2)
    try:
        tree = stack_rounds(load_round(path) for path in reports)
    except (OSError, KeyError, TypeError, ValueError) as error:
        typer.echo(f"experiment tree: {error}", err=True)
        raise typer.Exit(2) from error
    if json_out:
        typer.echo(json.dumps(tree.to_dict(), indent=2, sort_keys=True))
    elif mermaid:
        typer.echo(render_mermaid(tree))
    else:
        typer.echo(render(tree))


@app.command("source-snapshot")
def source_snapshot_cmd(
    repo: Annotated[Path, typer.Argument(help="local Git repository to snapshot")] = Path("."),
    revision: Annotated[str, typer.Option(help="commit-ish to resolve and archive")] = "HEAD",
    cache_root: Annotated[
        Path,
        typer.Option(help="content-addressed snapshot cache"),
    ] = Path(".factory/source-snapshots"),
    json_out: Annotated[bool, typer.Option("--json", help="machine-readable receipt")] = False,
) -> None:
    """Create and verify the immutable source archive for one recorded Git revision."""
    from swfactory.repo_runtime import snapshot_source
    from swfactory.source_snapshot import SourceSnapshotError, verify_source_snapshot

    try:
        snapshot = snapshot_source(repo, revision, cache_root)
        verify_source_snapshot(snapshot)
    except (OSError, SourceSnapshotError) as error:
        typer.echo(f"source snapshot: {error}", err=True)
        raise typer.Exit(2) from error
    document = snapshot.to_dict()
    if json_out:
        typer.echo(json.dumps(document, indent=2, sort_keys=True))
        return
    width = max(len(key) for key in document)
    for name, value in document.items():
        typer.echo(f"{name.replace('_', ' '):<{width}}  {value}")


@app.command("snapshot-replay")
def snapshot_replay_cmd(
    repo: Annotated[Path, typer.Argument(help="local Git repository containing the recorded commit")],
    source_receipt: Annotated[Path, typer.Argument(help="JSON receipt from source-snapshot --json")],
    destination: Annotated[Path, typer.Argument(help="empty directory for replay evidence")],
    recipe_path: Annotated[
        str,
        typer.Option(help="recipe path loaded from the exact source commit"),
    ] = ".swfactory/candidate-run.json",
) -> None:
    """Replay exact source bytes using the recipe committed with those bytes."""
    from swfactory.execution_recipe import ExecutionRecipeError, load_execution_recipe
    from swfactory.snapshot_replay import SnapshotReplayError, run_snapshot_recipe
    from swfactory.source_snapshot import SourceSnapshot, SourceSnapshotError

    try:
        source = SourceSnapshot(**json.loads(source_receipt.read_text(encoding="utf-8")))
        recipe = load_execution_recipe(repo, source.commit_sha, path=recipe_path)
        receipt = run_snapshot_recipe(source, recipe, destination)
    except (
        OSError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        ExecutionRecipeError,
        SnapshotReplayError,
        SourceSnapshotError,
    ) as error:
        typer.echo(f"snapshot replay: {error}", err=True)
        raise typer.Exit(2) from error
    typer.echo(f"{destination / 'receipt.json'}  {receipt.digest}")
    typer.echo(f"exit={receipt.exit_code} timed_out={str(receipt.timed_out).lower()}")


@app.command("snapshot-replay-verify")
def snapshot_replay_verify_cmd(
    repo: Annotated[Path, typer.Argument(help="local Git repository containing the recorded commit")],
    source_receipt: Annotated[Path, typer.Argument(help="JSON receipt from source-snapshot --json")],
    destination: Annotated[Path, typer.Argument(help="replay evidence directory")],
    json_out: Annotated[bool, typer.Option("--json", help="print the canonical verified receipt")] = False,
) -> None:
    """Re-hash replay evidence and re-bind it to source and recipe Git objects."""
    from swfactory.snapshot_replay import SnapshotReplayError, verify_snapshot_run
    from swfactory.source_snapshot import SourceSnapshot, SourceSnapshotError

    try:
        source = SourceSnapshot(**json.loads(source_receipt.read_text(encoding="utf-8")))
        recipe, receipt = verify_snapshot_run(destination, snapshot=source, repo=repo)
    except (
        OSError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        SnapshotReplayError,
        SourceSnapshotError,
    ) as error:
        typer.echo(f"snapshot replay: {error}", err=True)
        raise typer.Exit(2) from error
    if json_out:
        document = receipt.canonical_dict()
        document["receipt_digest"] = receipt.digest
        document["execution_recipe_sha256"] = recipe.digest
        typer.echo(json.dumps(document, indent=2, sort_keys=True))
    else:
        typer.echo(
            f"verified {receipt.commit_sha} recipe={recipe.digest} "
            f"exit={receipt.exit_code} timed_out={str(receipt.timed_out).lower()}"
        )


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
