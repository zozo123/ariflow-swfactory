"""Managed ``Plan.work`` execution inside Airflow's existing build stage.

This module is deliberately not a scheduler. Apache Airflow still owns the lifecycle task. Within
that task, a validated issue-local DAG is expanded into stable topological waves and executed against
one governed workspace. Shared-workspace execution is serial by design; provider fork/clone fan-out
remains experimental until a provider can prove isolated lineage, cancellation and teardown.

Progress is journalled in host-owned RunState: an attempt row before every agent call (node or
repair), a receipt after its commit, and the verdict of every test run. An Airflow task retry
therefore resumes from the last committed candidate without replaying completed work or reusing a
dead attempt's identity, while workspace-head fencing refuses a checkout the journal cannot explain.
"""

from __future__ import annotations

import hashlib
import json
import shlex
import time
from typing import Any

from swfactory import stages
from swfactory.models import BuildSummary, Plan, PlanTask, StageError, StageResult, TestResult
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
            "attempt": None,
            "nodes": [],
            "repairs": [],
            "verified": None,
        }
    try:
        progress = json.loads(ctx.state.read_control(_PROGRESS))
    except (ValueError, OSError) as error:
        raise StageError("policy", f"workgraph progress is corrupt: {error}") from error
    if not isinstance(progress, dict) or progress.get("schema_version") != 1:
        raise StageError("policy", "workgraph progress has an unsupported schema")
    if progress.get("plan_digest") != expected:
        raise StageError("policy", "workgraph progress belongs to a different plan")
    progress.setdefault("repairs", [])
    if not isinstance(progress.get("nodes"), list) or not isinstance(progress["repairs"], list):
        raise StageError("policy", "workgraph progress receipts are invalid")
    input_head = progress.get("input_head")
    checkpoint_head = progress.get("head")
    if not isinstance(input_head, str) or not input_head or not isinstance(checkpoint_head, str) or not checkpoint_head:
        raise StageError("policy", "workgraph progress is incomplete")
    attempt = progress.setdefault("attempt", None)
    progress.setdefault("verified", None)
    # The one drift a retry may explain: the factory's own commit for a journalled attempt whose
    # receipt never landed. `commit()` recorded that HEAD in host state and `_assert_workspace_head`
    # has just proved the sandbox still sits on it; `_resume` settles it. Anything else is somebody
    # else's checkout.
    own_commit = isinstance(attempt, dict) and attempt.get("input_head") == checkpoint_head
    if checkpoint_head != current_head and not own_commit:
        raise StageError(
            "policy",
            f"workgraph checkpoint expects HEAD {checkpoint_head} but workspace is {current_head}",
        )
    agent_calls = progress.setdefault("agent_calls", len(progress["nodes"]))
    if type(agent_calls) is not int or agent_calls < len(progress["nodes"]) + len(progress["repairs"]):
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


def _open_progress(ctx: stages.Ctx, plan: Plan) -> dict[str, Any]:
    """Fence the workspace, load the journal, and settle whatever a dead process left in flight."""
    current_head = stages._assert_workspace_head(ctx, "workgraph start")
    progress = _load_progress(ctx, plan, current_head)
    attempt = progress["attempt"]
    if attempt is not None:
        if current_head == progress["head"]:
            # Died before its commit: whatever the agent left uncommitted was never verified and is
            # discarded, so the retry starts its fresh identity from the checkpointed candidate.
            _abandon(ctx, progress, current_head)
        else:
            _settle(ctx, progress, plan, current_head)
    return progress


def _attempt(
    ctx: stages.Ctx,
    progress: dict[str, Any],
    *,
    kind: str,
    ident: str | int,
    stage: str,
    prompt: str,
    commit_stage: str,
    label: str,
    title: str,
    **extra: Any,
) -> str:
    """Journal one paid attempt, invoke the agent, commit what it produced; returns the new HEAD.

    The attempt row is durable before the provider is called: it carries the identity a retry must
    not reuse and the input HEAD that lets `_load_progress` tell the factory's own commit from
    foreign drift. The caller settles it with `_settle`.
    """
    before = stages._assert_workspace_head(ctx, f"workgraph {kind} {ident}")
    agent_call = int(progress["agent_calls"]) + 1
    progress["agent_calls"] = agent_call
    progress["attempt"] = {
        "kind": kind,
        "id": ident,
        "agent_call": agent_call,
        "input_head": before,
        "started_at": time.time(),
        **extra,
    }
    _store_progress(ctx, progress)
    try:
        result = stages._agent(ctx, stage, agent_call, prompt, BuildSummary)
        return stages.commit(ctx, stage=commit_stage, msg=f"{label}: {stages._summary_line(result, title)}")
    except BaseException:
        _abandon(ctx, progress, before)
        raise


def _abandon(ctx: stages.Ctx, progress: dict[str, Any], head: str) -> None:
    """Restore the attempt's input HEAD and strike the attempt: nothing of it is a candidate."""
    _restore(ctx, head)
    progress["attempt"] = None
    _store_progress(ctx, progress)


def _settle(ctx: stages.Ctx, progress: dict[str, Any], plan: Plan, after: str) -> dict[str, Any]:
    """Turn the in-flight attempt's commit into a receipt and advance the checkpoint to it."""
    attempt = progress["attempt"]
    before = attempt["input_head"]
    receipt: dict[str, Any] = {
        "agent_call": attempt["agent_call"],
        "input_head": before,
        "output_head": after,
        "duration_s": round(time.time() - attempt["started_at"], 3),
    }
    if attempt["kind"] == "node":
        node = next(row for row in plan.work if row.id == attempt["id"])
        changed_text = stages._sh(ctx, f"git diff --name-only {shlex.quote(before)}..{shlex.quote(after)} -- .")
        changed = tuple(sorted(line.strip() for line in changed_text.splitlines() if line.strip()))
        unexpected = sorted(set(changed) - set(node.files))
        if unexpected:
            _abandon(ctx, progress, before)
            raise StageError(
                "policy",
                f"Plan.work node {node.id!r} changed files outside its declared scope: {unexpected}",
            )
        receipt = {
            "node_id": node.id,
            "state": "ok",
            "layer": attempt["layer"],
            **receipt,
            "depends_on": list(node.depends_on),
            "declared_files": list(node.files),
            "changed_files": list(changed),
            "parallel_safe_hint": node.parallel_safe,
        }
        progress["nodes"].append(receipt)
    else:
        receipt = {"repair": attempt["id"], **receipt}
        progress["repairs"].append(receipt)
    progress["head"] = after
    progress["attempt"] = None
    _store_progress(ctx, progress)
    return receipt


def _execute_nodes(ctx: stages.Ctx, plan: Plan, spec_text: str, plan_text: str, progress: dict[str, Any]) -> None:
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
            after = _attempt(
                ctx,
                progress,
                kind="node",
                ident=node.id,
                stage="build",
                prompt=_node_prompt(ctx, plan_text, spec_text, node),
                commit_stage=f"work:{node.id}",
                label=f"work {node.id}",
                title=node.title,
                layer=layer_index,
            )
            completed[node.id] = _settle(ctx, progress, plan, after)


def _verify(
    ctx: stages.Ctx, progress: dict[str, Any], plan: Plan, conflicts: list[dict[str, Any]]
) -> tuple[TestResult, str]:
    """Test the checkpointed candidate, journal the verdict, and derive the execution report from it.

    Written after every run, not once after fan-in: the report's ``final_head`` is the HEAD the
    suite actually ran on, which after a repair is not the HEAD the nodes fanned in to.
    """
    tests, output = stages.run_tests(ctx)
    progress["verified"] = {"head": progress["head"], "ok": tests.ok, **stages._test_numbers(tests)}
    _store_progress(ctx, progress)
    by_id = {row["node_id"]: row for row in progress["nodes"]}
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
        "declared_conflicts": conflicts,
        "nodes": [by_id[node.id] for node in plan.work],
        "repairs": progress["repairs"],
        "verified": progress["verified"],
    }
    ctx.write_artifact(f"{ctx.art}/workgraph-execution.json", json.dumps(report, indent=2, sort_keys=True) + "\n")
    return tests, output


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

    conflicts = [
        {"left": left, "right": right, "files": list(files)} for left, right, files in conflict_set(_nodes(plan))
    ]
    progress = _open_progress(ctx, plan)
    _execute_nodes(ctx, plan, spec_text, plan_text, progress)

    def outcome(tests: TestResult) -> StageResult:
        return StageResult(
            stage="build_and_test",
            artifacts=[f"{ctx.art}/workgraph-execution.json"],
            numbers={
                "work_nodes": float(len(plan.work)),
                "work_conflicts": float(len(conflicts)),
                "parallel_nodes": 0.0,
                "repair_iterations": float(len(progress["repairs"])),
                "first_pass_ci": float(not progress["repairs"]),
                **stages._test_numbers(tests),
            },
        )

    tests, output = _verify(ctx, progress, plan, conflicts)
    if tests.ok:
        return outcome(tests)
    failures = f"exit code {tests.exit_code}; failed={tests.failed} errors={tests.errors}\n\n{output}"
    # The bound counts journalled repairs, so a retry continues the budget rather than reopening it.
    while len(progress["repairs"]) < ctx.cfg.max_build_iterations - 1:
        repair = len(progress["repairs"]) + 1
        after = _attempt(
            ctx,
            progress,
            kind="repair",
            ident=repair,
            stage="fix",
            prompt=stages.render_prompt(
                "fix",
                issue_id=ctx.issue.id,
                spec=spec_text,
                plan=plan_text,
                failures=failures,
                protected=stages._protected(ctx, "fix"),
            ),
            commit_stage="fix",
            label="fix",
            title=f"workgraph repair {repair}",
        )
        _settle(ctx, progress, plan, after)
        tests, output = _verify(ctx, progress, plan, conflicts)
        if tests.ok:
            return outcome(tests)
        failures = f"exit code {tests.exit_code}; failed={tests.failed} errors={tests.errors}\n\n{output}"
    raise StageError(
        "policy",
        f"Plan.work tests still failing after bounded repair; last failure:\n{failures[-1500:]}",
    )
