---
name: swfactory
description: "Operate the Airflow Software Factory from AI coding harnesses and execute bounded inner Factory Cell stages. Use when an agent must submit, inspect, approve, repair, review, or deliver work through zozo123/ariflow-swfactory, or when it is launched inside a Factory Cell and must produce specification, plan, implementation, test, fix, or review artifacts without bypassing Airflow lifecycle scheduling, Cell epoch fencing, or factory publication authority."
---

# Airflow Software Factory

Determine whether you are outside the factory as a harness or inside one Factory Cell as a stage worker. Preserve the authority boundary in both modes.

## Outer harness mode

Use this mode when driving the repository from Codex, Claude Code, Grok, Cursor, or another agent harness.

1. Choose a stable `factory-id` for the harness session and reuse it for retries.
2. Submit through the governed harness adapter:

   ```sh
   scripts/swf_harness.sh <harness> <factory-id> --issue <issue> [submit args...]
   ```

3. Use `swf` operator commands to inspect runs, Cells, gates, evidence, and deliveries.
4. Answer human gates only when the user has explicitly authorized that decision.
5. Verify terminal evidence and the published delivery instead of inferring success from an agent message.

Never create a parallel lifecycle scheduler. Apache Airflow is the only lifecycle scheduler. Never silently drop harness/session identity. Never pass backend, GitHub, or provider credentials into stage sandboxes. Retries of the same logical session should preserve the same harness identity.

Read `docs/harnesses.md` in the repository for concrete Codex, Claude, Grok, and custom-harness examples.

## Inner Factory Cell mode

Use this mode when the factory launched you to perform a specific stage.

1. Read `docs/factory/<issue>/intent.md` first. Treat it as the scope authority.
2. Read `factory.toml` and obey protected paths, tool limits, and stage constraints.
3. Keep work within the current Cell and epoch. Do not mutate or clean up resources belonging to another epoch.
4. Do not commit, push, open a pull request, publish, or promote yourself. The factory owns publication and parent authority owns promotion.
5. Touch only files authorized by the current plan. Keep tests green after a fix.

### Specification stage

Produce `spec.md` with:

- numbered testable requirements `R1`, `R2`, ...;
- exported API names, signatures, types, return values, and errors;
- correctness, security, performance, and maintainability risks with mitigations;
- explicit assumptions for unresolved ambiguity.

Map every requirement to at least one planned test. Add no implementation code during the specification stage.

### Planning stage

Treat `plan.json` as the typed source and `plan.md` as its rendering. Include the complete file allowlist, ordered implementation steps, named tests, and concrete risks. Keep `Plan.work` bounded to the issue-specific inner work graph; it is not another scheduler.

Example shape:

```json
{
  "files": ["src/calc/core.py", "tests/test_core.py"],
  "steps": ["implement percent_change", "add edge-case tests"],
  "tests": ["test_percent_change_increase", "test_percent_change_zero_old"],
  "risks": ["float comparisons -> use approximate assertions"]
}
```

### Build and fix stages

Implement only the approved plan. Prefer the smallest coherent change. Run the relevant tests and record failures accurately. A fix may repair implementation or tests only when the approved intent requires it; it may not widen scope to make CI green.

### Review stage

Read `REVIEW.md` at the target root and follow its output contract exactly. Review in this order: correctness, tests, security, plan fidelity, style. Use the repository's configured severities and nit cap. Request changes when the review contract requires it; do not soften a blocking finding to force delivery.

## Failure and recovery rules

- Treat `(cell_id, epoch, operation_key)` as the external-mutation identity.
- Refuse stale epochs rather than attempting best-effort writes.
- Reconcile ambiguous external mutation outcomes before retrying.
- Reclaim only resources proven to belong to the current or terminal incarnation.
- Keep evidence for claims, benchmarks, promotion decisions, and cleanup.

## Definition of done

A harness-side task is done only when the authoritative run reaches the intended terminal state and the expected evidence/delivery is verifiable. An inner-stage task is done when its stage artifact or code change satisfies the stage contract; leave lifecycle transitions and publication to the factory.
