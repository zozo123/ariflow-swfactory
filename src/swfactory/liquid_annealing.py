"""Annealed review convergence for the Liquid Software Factory line.

This module borrows one useful idea from statistical mechanics without turning physics vocabulary
into authority: exploration may be broad, but promotion must converge on retained evidence. The
review stage therefore runs independent read-only specialist lanes, deterministically fans their
findings in, repairs material defects inside the existing bounded review loop, and records a small
set of dimensionless diagnostics that explain whether the candidate has crystallized.

Apache Airflow remains the lifecycle scheduler. Factory Cells remain the durable identity and
fencing authority. This module creates no tasks, branches, sandboxes, approvals or publications.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass

from swfactory import stages
from swfactory.models import BuildSummary, Finding, Review, StageResult
from swfactory.phase_control import ControlMode, Phase

LANES: tuple[str, ...] = ("correctness", "verification", "risk")
RISK_PREFIXES: tuple[str, ...] = (
    ".github/",
    "dags/",
    "deploy/",
    "scripts/",
    "src/swfactory/agent.py",
    "src/swfactory/backend",
    "src/swfactory/cells.py",
    "src/swfactory/core_capabilities.py",
    "src/swfactory/runtime.py",
    "src/swfactory/sandbox",
    "src/swfactory/security",
    "src/swfactory/stages.py",
)


@dataclass(frozen=True)
class AnnealingObservation:
    round: int
    changed_files: int
    risky_files: int
    blockers: int
    majors: int
    minors: int
    nits: int
    tests_green: bool
    lane_coverage: float = 1.0
    stagnated: bool = False
    exhausted: bool = False


@dataclass(frozen=True)
class AnnealingState:
    round: int
    phase: Phase
    control_mode: ControlMode
    temperature: float
    beta: float
    defect_energy: float
    surface_penalty: float
    driving_force: float
    nucleation_barrier: float
    crossing_probability: float
    order_parameter: float
    crystallized: bool
    actionable_findings: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _review_control_mode(phase: Phase) -> ControlMode:
    """Translate review-local material state into a bounded posture.

    The review stage never widens the lifecycle graph. In particular, a review-local gas state means
    "measure what is missing", not "spawn arbitrary new implementation lanes".
    """

    return {
        "gas": ControlMode.MEASURE,
        "liquid": ControlMode.ANNEAL,
        "critical": ControlMode.MEASURE,
        "crystal": ControlMode.VERIFY,
        "glass": ControlMode.PERTURB,
        "jammed": ControlMode.DRAIN,
    }[phase]


def evaluate(observation: AnnealingObservation) -> AnnealingState:
    """Map retained review evidence to bounded, dimensionless annealing diagnostics.

    The numbers are intentionally simple and falsifiable. They are not physical units and they do
    not grant authority. Findings and a red test suite raise effective defect energy; wider or
    trust-boundary-heavy diffs raise the interface/surface penalty; complete independent review and
    green tests raise the driving force. A candidate crystallizes only when the ordinary software
    invariants are already true: tests are green, every lane completed, and no blocker/major remains.
    """

    defect_energy = (
        8.0 * observation.blockers
        + 3.0 * observation.majors
        + 0.75 * observation.minors
        + 0.10 * observation.nits
        + (8.0 if not observation.tests_green else 0.0)
    )
    breadth = min(observation.changed_files / 20.0, 1.0)
    risk = min(observation.risky_files / 6.0, 1.0)
    surface_penalty = min(1.0, 0.65 * breadth + 0.35 * risk)

    # Defects keep the system hot. Breadth adds some exploration temperature but can never by
    # itself override clean evidence. beta is bounded to keep the diagnostic numerically stable.
    temperature = min(1.0, defect_energy / (defect_energy + 5.0) + 0.25 * surface_penalty)
    beta = min(20.0, 1.0 / max(temperature, 0.05))
    order_parameter = max(0.0, 1.0 - temperature)

    cleanliness = 1.0 / (1.0 + defect_energy)
    driving_force = max(
        0.05,
        min(
            1.0,
            (float(observation.tests_green) + observation.lane_coverage + cleanliness) / 3.0,
        ),
    )
    nucleation_barrier = ((surface_penalty + 0.05) ** 3) / (driving_force**2)
    crossing_probability = math.exp(-nucleation_barrier / max(temperature, 0.05))

    actionable = observation.blockers + observation.majors
    complete = observation.lane_coverage >= 1.0
    crystallized = observation.tests_green and complete and actionable == 0

    if observation.exhausted and not observation.tests_green:
        phase: Phase = "jammed"
    elif observation.blockers or not observation.tests_green:
        phase = "critical"
    elif observation.stagnated and actionable:
        phase = "glass"
    elif crystallized:
        phase = "crystal"
    elif actionable:
        phase = "liquid"
    else:
        phase = "gas"

    return AnnealingState(
        round=observation.round,
        phase=phase,
        control_mode=_review_control_mode(phase),
        temperature=round(temperature, 6),
        beta=round(beta, 6),
        defect_energy=round(defect_energy, 6),
        surface_penalty=round(surface_penalty, 6),
        driving_force=round(driving_force, 6),
        nucleation_barrier=round(nucleation_barrier, 6),
        crossing_probability=round(crossing_probability, 6),
        order_parameter=round(order_parameter, 6),
        crystallized=crystallized,
        actionable_findings=actionable,
    )


def merge_findings(groups: list[list[Finding]]) -> list[Finding]:
    """Deterministically de-duplicate specialist output without hiding severity disagreements."""

    rank = {"blocker": 0, "major": 1, "minor": 2, "nit": 3}
    by_key: dict[tuple[str, int | None, str], Finding] = {}
    for finding in (item for group in groups for item in group):
        key = (finding.file, finding.line, finding.title.strip().casefold())
        previous = by_key.get(key)
        if previous is None or rank[finding.severity] < rank[previous.severity]:
            by_key[key] = finding
    return sorted(
        by_key.values(),
        key=lambda item: (rank[item.severity], item.file, item.line or 0, item.title.casefold()),
    )


def _risk_count(paths: tuple[str, ...]) -> int:
    return sum(any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in RISK_PREFIXES) for path in paths)


def _signature(findings: list[Finding]) -> tuple[tuple[str, str, int | None, str], ...]:
    return tuple(
        sorted(
            (finding.severity, finding.file, finding.line, finding.title.casefold())
            for finding in findings
            if finding.severity in {"blocker", "major"}
        )
    )


def _review_prompt(ctx: stages.Ctx, *, lane: str, spec: str, plan: str, diff: str) -> str:
    policy = (
        stages._review_policy(ctx)
        + f"\n\n## Assigned Liquid review lane\n{lane}\n"
        + "Apply only the specialist lane with this name from the policy above.\n"
    )
    return stages.render_prompt(
        "review",
        issue_id=ctx.issue.id,
        review_policy=policy,
        spec=spec,
        plan=plan,
        diff=diff,
    )


def _round(
    ctx: stages.Ctx,
    *,
    round_index: int,
    spec: str,
    plan: str,
    diff: str,
) -> tuple[list[Finding], int, list[dict[str, object]]]:
    groups: list[list[Finding]] = []
    lane_records: list[dict[str, object]] = []
    for lane_index, lane in enumerate(LANES, start=1):
        iteration = round_index * 10 + lane_index
        result = stages._agent(
            ctx,
            "review",
            iteration,
            _review_prompt(ctx, lane=lane, spec=spec, plan=plan, diff=diff),
            Review,
        )
        review = Review.model_validate(result.data)
        groups.append(review.findings)
        lane_records.append(
            {
                "lane": lane,
                "iteration": iteration,
                "verdict": review.verdict,
                "findings": len(review.findings),
                "blockers": len(review.blockers),
            }
        )
    merged = merge_findings(groups)
    review = Review(
        verdict="request_changes" if any(f.severity == "blocker" for f in merged) else "approve",
        findings=merged,
    )
    review, dropped = stages.cap_nits(review, stages._nit_cap(ctx))
    return review.findings, dropped, lane_records


def _initial_tests_green(ctx: stages.Ctx) -> bool:
    build = next((item for item in stages.load_stage_results(ctx) if item.stage == "build_and_test"), None)
    return build is not None and build.status == "ok" and build.numbers.get("tests_passed") == 1.0


def _run_annealed_review(ctx: stages.Ctx) -> StageResult:
    review_path = f"{ctx.art}/review.json"
    annealing_path = f"{ctx.art}/annealing.json"
    if prior := stages._done(ctx, "review"):
        return stages._skipped(prior)

    stages._assert_workspace_head(ctx, "review")
    base = ctx.state.read_control("base").strip()
    spec = stages._read_or(ctx, f"{ctx.art}/spec.md")
    plan = ctx.read_artifact(f"{ctx.art}/plan.md")
    changed = tuple(
        line.strip()
        for line in stages._sh(
            ctx,
            f"git diff --name-only --relative {base}..HEAD -- . {stages._exclude(ctx)}",
        ).splitlines()
        if line.strip()
    )
    risky_files = _risk_count(changed)
    tests_green = _initial_tests_green(ctx)
    tests_blocker: Finding | None = None
    previous_signature: tuple[tuple[str, str, int | None, str], ...] = ()
    trace: list[dict[str, object]] = []
    numbers: dict[str, float] = {}
    fixes = 0
    dropped = 0
    final_findings: list[Finding] = []
    final_lanes: list[dict[str, object]] = []
    final_state = evaluate(
        AnnealingObservation(0, len(changed), risky_files, 1, 0, 0, 0, tests_green, lane_coverage=0.0)
    )

    for round_index in range(ctx.cfg.max_review_fixes + 1):
        diff = stages._sh(ctx, f"git diff {base}..HEAD -- . {stages._exclude(ctx)}")
        from swfactory.harness_efficiency import pack_review_diff

        packed_diff = pack_review_diff(
            ctx,
            diff=diff,
            base_sha=base,
            head_sha=stages._assert_workspace_head(ctx, "review context"),
            fanout=len(LANES),
        )
        review_diff = packed_diff.prompt_text if packed_diff is not None else diff
        if packed_diff is not None:
            numbers["review_context_source_bytes"] = numbers.get("review_context_source_bytes", 0.0) + float(
                packed_diff.source_bytes
            )
            numbers["review_context_prompt_bytes"] = numbers.get("review_context_prompt_bytes", 0.0) + float(
                packed_diff.prompt_bytes
            )
            numbers["review_context_replayed_bytes_avoided"] = numbers.get(
                "review_context_replayed_bytes_avoided", 0.0
            ) + float(packed_diff.estimated_replayed_bytes_avoided)
        findings, dropped, lane_records = _round(
            ctx,
            round_index=round_index,
            spec=spec,
            plan=plan,
            diff=review_diff,
        )
        findings.extend(stages._plan_fidelity(ctx, base))
        if tests_blocker is not None:
            findings.append(tests_blocker)
        findings = merge_findings([findings])
        review = Review(
            verdict="request_changes" if any(item.severity == "blocker" for item in findings) else "approve",
            findings=findings,
        )
        review, extra_dropped = stages.cap_nits(review, stages._nit_cap(ctx))
        dropped += extra_dropped
        findings = review.findings

        counts = {severity: sum(item.severity == severity for item in findings) for severity in stages.SEVERITIES}
        signature = _signature(findings)
        stagnated = bool(signature) and signature == previous_signature
        exhausted = round_index == ctx.cfg.max_review_fixes
        final_state = evaluate(
            AnnealingObservation(
                round=round_index,
                changed_files=len(changed),
                risky_files=risky_files,
                blockers=counts["blocker"],
                majors=counts["major"],
                minors=counts["minor"],
                nits=counts["nit"],
                tests_green=tests_green,
                lane_coverage=len(lane_records) / len(LANES),
                stagnated=stagnated,
                exhausted=exhausted,
            )
        )
        trace.append(
            {
                "round": round_index,
                "lanes": lane_records,
                "findings": [item.model_dump() for item in findings],
                "review_context": (
                    {
                        "handle": packed_diff.handle,
                        "source_bytes": packed_diff.source_bytes,
                        "prompt_bytes": packed_diff.prompt_bytes,
                        "fanout": packed_diff.fanout,
                    }
                    if packed_diff is not None
                    else {"mode": "full-diff"}
                ),
                "state": final_state.as_dict(),
            }
        )
        final_findings, final_lanes = findings, lane_records
        if final_state.crystallized or exhausted:
            break

        actionable = [item for item in findings if item.severity in {"blocker", "major"}]
        if not actionable:
            break
        fixes = round_index + 1
        fix_prompt = stages.render_prompt(
            "fix",
            issue_id=ctx.issue.id,
            plan=plan,
            failures="Liquid annealing defects requiring relaxation:\n" + stages._format_findings(actionable),
            protected=stages._protected(ctx, "fix"),
        )
        iteration = ctx.cfg.max_build_iterations + fixes
        fix_result = stages._agent(ctx, "fix", iteration, fix_prompt, BuildSummary)
        stages.commit(ctx, stage="fix", msg=f"fix: {stages._summary_line(fix_result, 'relax annealing defects')}")
        test_result, output = stages.run_tests(ctx)
        numbers.update(stages._test_numbers(test_result))
        tests_green = test_result.ok
        tests_blocker = None if test_result.ok else stages._tests_blocker(ctx, test_result, output)
        previous_signature = signature

    counts = {severity: sum(item.severity == severity for item in final_findings) for severity in stages.SEVERITIES}
    review_record = {
        "verdict": "request_changes" if counts["blocker"] else "approve",
        "findings": [item.model_dump() for item in final_findings],
        "dropped_nits": dropped,
        "fixes": fixes,
        "annealing": final_state.as_dict(),
        "lanes": final_lanes,
    }
    annealing_record = {
        "schema_version": 1,
        "strategy": "liquid-relaxation-annealing",
        "authority": "advisory-review-only",
        "phase_contract": "factory-phase/v1",
        "changed_files": list(changed),
        "risky_files": risky_files,
        "specialist_lanes": list(LANES),
        "trace": trace,
        "final": final_state.as_dict(),
    }
    ctx.write_artifact(review_path, stages._dumps(review_record))
    ctx.write_artifact(annealing_path, stages._dumps(annealing_record))

    numbers.update(
        {
            "blockers": float(counts["blocker"]),
            "findings": float(len(final_findings)),
            "dropped_nits": float(dropped),
            "fixes": float(fixes),
            "review_lanes": float(len(LANES)),
            "crystallized": float(final_state.crystallized),
            "anneal_temperature": final_state.temperature,
            "anneal_beta": final_state.beta,
            "anneal_barrier": final_state.nucleation_barrier,
            "anneal_crossing": final_state.crossing_probability,
            **{key: float(value) for key, value in counts.items()},
        }
    )
    status = "ok" if final_state.crystallized else "blocked"
    return StageResult(stage="review", status=status, artifacts=[review_path, annealing_path], numbers=numbers)


@stages._timed
def review(ctx: stages.Ctx) -> StageResult:
    """Liquid-line review: independent specialist lanes -> deterministic fan-in -> bounded relaxation."""

    return _run_annealed_review(ctx)
