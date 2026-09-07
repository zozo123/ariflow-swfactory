---
name: swfactory
description: Operate this Airflow software factory from Claude Code and perform inner factory stages. Use when Claude is an outer harness submitting or inspecting factory work, or when Claude is executing a spec, plan, build, fix, or review stage inside a Factory Cell.
---

# Software factory

First determine which side of the factory boundary you are on.

## Outer harness mode

Use this mode when the user is driving the repository from Claude Code and wants the factory to do
work. Submit through the governed factory; do not create a parallel lifecycle loop.

1. Choose one stable factory-session id and reuse it for retries and multiple issues in this Claude
   session.
2. Submit with `scripts/swf_harness.sh claude <factory-id> --issue <issue> [submit args...]`.
3. Inspect progress and evidence through normal `swf` operator commands.
4. Answer human gates only when explicitly authorized.
5. Leave stage scheduling to Apache Airflow and publication to the factory.

Never pass backend/service credentials into stage sandboxes. Never silently drop harness/session
identity; fail if the configured submission path cannot preserve it. Read `docs/harnesses.md` for
examples shared with Codex, Grok, and custom harnesses.

## Inner stage mode

Use this mode when the factory launched you as one stage of a durable Factory Cell. Treat the
originator's words in `docs/factory/<issue>/intent.md` as the source of scope. Never invent scope,
never touch files listed as `protected` in `factory.toml`, and never push, open a PR, or commit
yourself: the factory commits and delivers.

### spec.md

Write:

```text
# spec.md
## Requirements       numbered R1, R2, ...; each testable in one assertion and traceable to intent.md
                      ("percent_change(100, 125) == 0.25"); include error cases, edge conditions,
                      backwards compatibility
## API                exported names, signatures, types, return values, raised errors
## Concerns           correctness / security / performance / maintainability risks, each with a mitigation
## Open questions     anything ambiguous in the intent, with the assumption you are making
```

Map every requirement to at least one test in plan.md. Add no code and no scope beyond the intent.
Read the repository instead of guessing. Keep the spec under one page and output only the document.

### plan.md / plan.json

Treat `plan.json` as the typed source (`Plan` schema) and `plan.md` as its rendering.

```json
{
  "files": ["src/calc/core.py", "src/calc/__init__.py", "tests/test_core.py"],
  "steps": ["add percent_change to core.py raising ValueError on old == 0", "export it", "add tests"],
  "tests": ["test_percent_change_increase", "test_percent_change_decrease", "test_percent_change_zero_old"],
  "risks": ["float rounding in equality assertions -> use pytest.approx"]
}
```

Make `files` the complete list the diff may touch. Keep `steps` ordered and one commit's worth of
work each. Name real test functions in `tests`. List risks honestly; `[]` is acceptable.

### Review

Read `REVIEW.md` at the target root and follow it literally. Run the five passes in order:
correctness, tests, security, plan fidelity, style. Use severities blocker/major/minor/nit and obey
the configured nit cap. Set `verdict` to `request_changes` iff any blocker exists. Return only the
JSON contract `REVIEW.md` specifies.

Do not style-review `docs/factory/**`, lockfiles, or generated files. Keep the test suite green after
a fix; a red suite introduced by the fix is itself a blocker.
