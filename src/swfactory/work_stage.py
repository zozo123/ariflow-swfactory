"""Managed ``Plan.work`` execution inside Airflow's existing build stage.

This module is deliberately not a scheduler. Apache Airflow still owns the lifecycle task. Within
that task, a validated issue-local DAG is expanded into stable topological waves and executed against
one governed workspace. Shared-workspace execution is serial by design; provider fork/clone fan-out
remains experimental until a provider can prove isolated lineage, cancellation and teardown.

Progress is checkpointed in host-owned RunState after every node and before every agent attempt. An
Airflow task retry therefore resumes from durable node receipts without replaying completed work or
reusing a failed agent-envelope identity, while workspace-head fencing refuses an unexpected checkout.
"""

from __future__ import annotations

import hashlib
import json
import shlex
import time
from typing import Any

from swfactory import stages
from swfactory.models import BuildSummary, Plan, PlanTask, StageError, StageResult
from swfactory.workgraph import WorkNode, conflict_set

_PROGRESS = "workgraph-progress.json"


def _digest_plan(plan: Plan) -> str:
    raw = json.dumps(plan.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _nodes(plan: Plan) -> tuple[WorkNode, ...]:
    return tuple(
        WorkNode(
            id=node.id,
            depends_on=tuple(node.depends_on),
            files=tuple(node.files),
            parallel_safe=node.parallel_safe,
        )
        for node in plan.work
    )


def _load_progress(ctx: stages.Ctx, plan: Plan, current_head: str) -> dict[str, Any]:
    expected = _digest_plan(plan)
    if not ctx.state.has_control(_PROGRESS):
        return {
            "schema_version": 1,
            "plan_digest": expected,
            "input_head": current_head,
            "head": current_head,
            "agent_calls": 0,
            "nodes": [],
        }
    try:
        progress = json.loads(ctx.state.read_control(_PROGRESS))
    except (ValueError, OSError) as error:
        raise StageError("policy", f"workgraph progress is corrupt: {error}") from error
    if not isinstance(progress, dict) or progress.get("schema_version") != 1:
        raise StageError("policy", "workgraph progress has an unsupported schema")
    if progress.get("plan_digest") != expected:
        raise StageError("policy", "workgraph progress belongs to a different plan")
    if not isinstance(progress.get("nodes"), list):
        raise StageError("policy", "workgraph progress node receipts are invalid")
    input_head = progress.get("input_head")
    checkpoint_head = progress.get("head")
    if not isinstance(input_head, str) or not input_head or not isinstance(checkpoint_head, str) or not checkpoint_head:
        raise StageError("policy", "workgraph progress is incomplete")
    if checkpoint_head != current_head:
        raise StageError(
            "policy",
            f"workgraph checkpoint expects HEAD {checkpoint_head} but workspace is {current_head}",
        )
    agent_calls = progress.setdefault("agent_calls", len(progress["nodes"]))
    if type(agent_calls) is not int or agent_calls < len(progress["nodes"]):
        raise StageError("policy", "workgraph attempt counter is invalid")
    return progress


def _store_progress(ctx: stages.Ctx, progress: dict[str, Any]) -> None:
    ctx.state.write_control(_PROGRESS, json.dumps(progress, indent=2, sort_keys=True) + "\n")


def _restore(ctx: stages.Ctx, head: str) -> None:
    stages._sh(ctx, f"git reset --hard {shlex.quote(head)}")
    ctx.sb.run("git clean -fd -e .factory -e docs/factory")
    stages._record_workspace_head(ctx, head)


def _node_prompt(ctx: stages.Ctx, plan_text: str, spec_text: str, node: PlanTask) -> str:
    base = stages.render_prompt(
        "build",
        issue_id=ctx.issue.id,
        spec=spec_text,
        plan=plan_text,
        failures="",
        protected=stages._protected(ctx, "build"),
    )
    # A template, not a Python literal. As a literal this instruction was outside the accepted-
    # inputs pin (#2098): two swfactory builds differing only in these lines admitted the same
    # digest and told the model different things. `prompts/build_node.md` is digested with the rest.
    return (
        base
        + "\n"
        + stages.render_prompt(
            "build_node",
            node_id=node.id,
            node_title=node.title,
            node_depends=", ".join(node.depends_on),
            node_files=", ".join(node.files),
            node_tests="; ".join(node.tests),
        )
    )


def _execute_nodes(
    ctx: stages.Ctx,
    plan: Plan,
    spec_text: str,
    plan_text: str,
) -> tuple[int, dict[str, Any]]:
    declared_conflicts = [
        {"left": left, "right": right, "files": list(files)} for left, right, files in conflict_set(_nodes(plan))
    ]
    current_head = stages._assert_workspace_head(ctx, "workgraph start")
    progress = _load_progress(ctx, plan, current_head)
    completed = {
        str(row["node_id"]): row
        for row in progress["nodes"]
        if isinstance(row, dict) and row.get("state") == "ok" and row.get("node_id")
    }

    for layer_index, layer in enumerate(plan.work_layers()):
        for node in sorted(layer, key=lambda item: item.id):
            if node.id in completed:
                continue
            missing = sorted(dep for dep in node.depends_on if dep not in completed)
            if missing:
                raise StageError("policy", f"Plan.work node {node.id!r} has incomplete dependencies: {missing}")
            before = stages._assert_workspace_head(ctx, f"workgraph node {node.id}")
            agent_call = int(progress["agent_calls"]) + 1
            progress["agent_calls"] = agent_call
            _store_progress(ctx, progress)
            started = time.monotonic()
            try:
                result = stages._agent(
                    ctx,
                    "build",
                    agent_call,
                    _node_prompt(ctx, plan_text, spec_text, node),
                    BuildSummary,
                )
                after = stages.commit(
                    ctx,
                    stage=f"work:{node.id}",
                    msg=f"work {node.id}: {stages._summary_line(result, node.title)}",
                )
                changed_text = stages._sh(
                    ctx,
                    f"git diff --name-only {shlex.quote(before)}..{shlex.quote(after)} -- .",
                )
                changed = tuple(sorted(line.strip() for line in changed_text.splitlines() if line.strip()))
                unexpected = sorted(set(changed) - set(node.files))
                if unexpected:
                    _restore(ctx, before)
                    raise StageError(
                        "policy",
                        f"Plan.work node {node.id!r} changed files outside its declared scope: {unexpected}",
                    )
            except BaseException:
                _restore(ctx, before)
                raise
            receipt = {
                "node_id": node.id,
                "state": "ok",
                "layer": layer_index,
                "agent_call": agent_call,
                "input_head": before,
                "output_head": after,
                "depends_on": list(node.depends_on),
                "declared_files": list(node.files),
                "changed_files": list(changed),
                "parallel_safe_hint": node.parallel_safe,
                "duration_s": round(time.monotonic() - started, 3),
            }
            progress["nodes"].append(receipt)
            progress["head"] = after
            _store_progress(ctx, progress)
            completed[node.id] = receipt

    ordered_receipts = [completed[node.id] for node in plan.work]
    report = {
        "schema_version": 1,
        "mode": "shared_workspace_serial",
        "scheduler": "airflow",
        "parallel": False,
        "reason": "supported shared-workspace path serializes nodes; provider fork remains experimental",
        "input_head": progress["input_head"],
        "final_head": progress["head"],
        "plan_digest": progress["plan_digest"],
        "agent_calls": progress["agent_calls"],
        "declared_conflicts": declared_conflicts,
        "nodes": ordered_receipts,
    }
    ctx.write_artifact(
        f"{ctx.art}/workgraph-execution.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return int(progress["agent_calls"]), report


def _legacy_build(ctx: stages.Ctx, spec_text: str, plan_text: str) -> StageResult:
    failures = ""
    for iteration in range(1, ctx.cfg.max_build_iterations + 1):
        stage = "build" if iteration == 1 else "fix"
        prompt = stages.render_prompt(
            stage,
            issue_id=ctx.issue.id,
            spec=spec_text,
            plan=plan_text,
            failures=failures,
            protected=stages._protected(ctx, stage),
        )
        result = stages._agent(ctx, stage, iteration, prompt, BuildSummary)
        stages.commit(ctx, stage=stage, msg=f"{stage}: {stages._summary_line(result, f'iteration {iteration}')}")
        tests, output = stages.run_tests(ctx)
        if tests.ok:
            return StageResult(
                stage="build_and_test",
                numbers={
                    "iterations": float(iteration),
                    "first_pass_ci": float(iteration == 1),
                    **stages._test_numbers(tests),
                },
            )
        failures = f"exit code {tests.exit_code}; failed={tests.failed} errors={tests.errors}\n\n{output}"
    raise StageError(
        "policy",
        f"tests still failing after {ctx.cfg.max_build_iterations} build iterations; last failure:\n{failures[-1500:]}",
    )


@stages._timed
def build_and_test(ctx: stages.Ctx) -> StageResult:
    """Airflow-managed build stage with real ``Plan.work`` semantics and legacy fallback."""
    if prior := stages._done(ctx, "build_and_test"):
        return stages._skipped(prior)
    spec_text = stages._read_or(ctx, f"{ctx.art}/spec.md")
    plan_text = ctx.read_artifact(f"{ctx.art}/plan.md")
    try:
        plan = Plan.model_validate_json(ctx.read_artifact(f"{ctx.art}/plan.json"))
    except (ValueError, OSError) as error:
        raise StageError("policy", f"plan.json is invalid: {error}") from error

    if not plan.work:
        return _legacy_build(ctx, spec_text, plan_text)

    node_calls, report = _execute_nodes(ctx, plan, spec_text, plan_text)
    tests, output = stages.run_tests(ctx)
    if tests.ok:
        return StageResult(
            stage="build_and_test",
            artifacts=[f"{ctx.art}/workgraph-execution.json"],
            numbers={
                "work_nodes": float(len(plan.work)),
                "work_conflicts": float(len(report["declared_conflicts"])),
                "parallel_nodes": 0.0,
                "repair_iterations": 0.0,
                "first_pass_ci": 1.0,
                **stages._test_numbers(tests),
            },
        )

    failures = f"exit code {tests.exit_code}; failed={tests.failed} errors={tests.errors}\n\n{output}"
    for repair in range(1, ctx.cfg.max_build_iterations):
        iteration = node_calls + repair
        result = stages._agent(
            ctx,
            "fix",
            iteration,
            stages.render_prompt(
                "fix",
                issue_id=ctx.issue.id,
                spec=spec_text,
                plan=plan_text,
                failures=failures,
                protected=stages._protected(ctx, "fix"),
            ),
            BuildSummary,
        )
        stages.commit(ctx, stage="fix", msg=f"fix: {stages._summary_line(result, f'workgraph repair {repair}')}")
        tests, output = stages.run_tests(ctx)
        if tests.ok:
            return StageResult(
                stage="build_and_test",
                artifacts=[f"{ctx.art}/workgraph-execution.json"],
                numbers={
                    "work_nodes": float(len(plan.work)),
                    "work_conflicts": float(len(report["declared_conflicts"])),
                    "parallel_nodes": 0.0,
                    "repair_iterations": float(repair),
                    "first_pass_ci": 0.0,
                    **stages._test_numbers(tests),
                },
            )
        failures = f"exit code {tests.exit_code}; failed={tests.failed} errors={tests.errors}\n\n{output}"
    raise StageError(
        "policy",
        f"Plan.work tests still failing after bounded repair; last failure:\n{failures[-1500:]}",
    )
