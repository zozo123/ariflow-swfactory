"""Per-run metrics (``docs/factory/<issue>/metrics.json``) and their aggregation.

Git is the durable store: every run commits its metrics next to the artifact chain, and
``swfactory metrics`` / ``maintain`` read them back from a checkout with ``load_all``.
"""

from __future__ import annotations

import json
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from swfactory.models import Approval, Review, StageResult

if TYPE_CHECKING:
    from swfactory.stages import Ctx

SEVERITIES: tuple[str, ...] = ("blocker", "major", "minor", "nit")
_MAX_RUNS = 10_000  # bound for load_all


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat(timespec="seconds")


def write_run_metrics(ctx: Ctx, stages: list[StageResult], approvals: list[Approval]) -> dict:
    """Assemble the run's metrics and write ``{art}/metrics.json`` into the sandbox."""
    from swfactory.stages import denied_tool_calls  # runtime: stages imports this module

    by_stage = {s.stage: s for s in stages}
    build = by_stage.get("build_and_test", StageResult(stage="build_and_test")).numbers
    review = by_stage.get("review", StageResult(stage="review")).numbers
    findings = _findings_by_severity(ctx, review)
    try:
        started = ctx.sb.read(".factory/started").strip() or _iso(ctx.started_at)
    except FileNotFoundError:
        started = _iso(ctx.started_at)
    tests = [s.numbers["tests_passed"] for s in stages if "tests_passed" in s.numbers]
    data = {
        "run_id": ctx.cfg.run_id,
        "issue_id": ctx.issue.id,
        "blueprint": ctx.cfg.blueprint,
        "agent": ctx.agent.kind,
        "sandbox": ctx.cfg.sandbox,
        "sandbox_name": ctx.sb.name,
        "scm": ctx.scm.kind,
        "started": started,
        "finished": _iso(datetime.now(UTC)),
        "stage_durations_s": {s.stage: s.duration_s for s in stages},
        "stage_status": {s.stage: s.status for s in stages},
        "cycle_s": round(sum(s.duration_s for s in stages), 3),
        "iterations": int(build.get("iterations", 0)),
        "first_pass_ci": bool(build.get("first_pass_ci", 0.0)),
        "tests_passed": bool(tests) and tests[-1] == 1.0,
        "findings_by_severity": findings,
        "blockers": findings["blocker"],
        "review_fixes": int(review.get("fixes", 0)),
        "denied_tool_calls": denied_tool_calls(ctx),
        "total_cost_usd": round(sum(s.cost_usd for s in stages), 6),
        "approvers": [a.actor for a in approvals],
        "approvals": [a.model_dump(mode="json") for a in approvals],
    }
    ctx.sb.write(f"{ctx.art}/metrics.json", json.dumps(data, indent=2) + "\n")
    return data


def _findings_by_severity(ctx: Ctx, review_numbers: dict[str, float]) -> dict[str, int]:
    try:
        rv = Review.model_validate(json.loads(ctx.sb.read(f"{ctx.art}/review.json")))
    except (FileNotFoundError, ValueError):
        return {s: int(review_numbers.get(s, 0)) for s in SEVERITIES}
    return {s: sum(1 for f in rv.findings if f.severity == s) for s in SEVERITIES}


def load_all(root: Path) -> list[dict]:
    """Every ``**/docs/factory/*/metrics.json`` under ``root``, oldest first."""
    runs: list[dict] = []
    for i, path in enumerate(sorted(Path(root).glob("**/docs/factory/*/metrics.json"))):
        if i >= _MAX_RUNS:
            break
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(data, dict) and "run_id" in data:
            runs.append(data)
    runs.sort(key=lambda r: str(r.get("finished", "")))
    return runs


def summarize(runs: list[dict]) -> dict:
    """Aggregate: first-pass rate, mean iterations, p50 cycle time, findings, cost."""
    n = len(runs)
    findings = {s: 0 for s in SEVERITIES}
    for r in runs:
        for s in SEVERITIES:
            findings[s] += int((r.get("findings_by_severity") or {}).get(s, 0))
    cycles = [float(r.get("cycle_s", 0.0)) for r in runs]
    iterations = [int(r.get("iterations", 0)) for r in runs if r.get("iterations")]
    return {
        "runs": n,
        "scripted_runs": sum(1 for r in runs if r.get("agent") == "scripted"),
        "first_pass_rate": (sum(1 for r in runs if r.get("first_pass_ci")) / n) if n else 0.0,
        "mean_iterations": statistics.fmean(iterations) if iterations else 0.0,
        "p50_cycle_s": statistics.median(cycles) if cycles else 0.0,
        "tests_pass_rate": (sum(1 for r in runs if r.get("tests_passed")) / n) if n else 0.0,
        "findings_by_severity": findings,
        "blockers": findings["blocker"],
        "total_cost_usd": round(sum(float(r.get("total_cost_usd", 0.0)) for r in runs), 6),
    }


def table(summary: dict) -> str:
    """Two-column text table of ``summarize()`` output."""
    f = summary.get("findings_by_severity") or {}
    rows = [
        ("runs", f"{summary.get('runs', 0)} ({summary.get('scripted_runs', 0)} scripted)"),
        ("first-pass rate", f"{summary.get('first_pass_rate', 0.0):.0%}"),
        ("mean build iterations", f"{summary.get('mean_iterations', 0.0):.2f}"),
        ("p50 cycle time", f"{summary.get('p50_cycle_s', 0.0):.1f}s"),
        ("tests pass rate", f"{summary.get('tests_pass_rate', 0.0):.0%}"),
        ("findings", ", ".join(f"{s}={f.get(s, 0)}" for s in SEVERITIES)),
        ("total cost", f"${summary.get('total_cost_usd', 0.0):.4f}"),
    ]
    width = max(len(k) for k, _ in rows)
    return "\n".join(f"{k.ljust(width)}  {v}" for k, v in rows)
