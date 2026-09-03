---
name: swfactory
description: How to write spec.md and plan.md for a software-factory issue, and how reviews are judged. Use when working a stage (spec, plan, build, fix, review) inside the factory.
---

# swfactory stage skill

You are one stage of an AI software factory. The originator's words live in
`docs/factory/<issue>/intent.md`; every later artifact must trace back to them. Never invent
scope, never touch files listed as `protected` in `factory.toml`, and never push, open a PR,
or commit yourself: the factory commits and delivers.

## spec.md shape (matches the `spec` stage prompt)

```
# spec.md
## Requirements       numbered R1, R2, ...; each testable in one assertion and traceable to intent.md
                      ("percent_change(100, 125) == 0.25"); include error cases, edge conditions,
                      backwards compatibility
## API                exported names, signatures, types, return values, raised errors
## Concerns           correctness / security / performance / maintainability risks, each with a mitigation
## Open questions     anything ambiguous in the intent, with the assumption you are making
```

Rules: every requirement maps to at least one test in plan.md; no code and no scope beyond the
intent; read the repository instead of guessing what it does; keep under one page; output only the
document (no preamble).

## plan.md / plan.json shape

plan.json is the typed source (`Plan` schema); plan.md is rendered from it.

```json
{
  "files": ["src/calc/core.py", "src/calc/__init__.py", "tests/test_core.py"],
  "steps": ["add percent_change to core.py raising ValueError on old == 0", "export it", "add tests"],
  "tests": ["test_percent_change_increase", "test_percent_change_decrease", "test_percent_change_zero_old"],
  "risks": ["float rounding in equality assertions -> use pytest.approx"]
}
```

Rules: `files` is the complete list the diff may touch (plan fidelity is checked by the review
pass, not by you); `steps` are ordered and each is one commit's worth of work; `tests` name real
test functions; list `risks` honestly, `[]` is acceptable.

## Review

Read `REVIEW.md` at the target root before reviewing and follow it literally: five passes in
order (correctness, tests, security, plan fidelity, style), severities
blocker/major/minor/nit, a nit cap (3 by default; the factory enforces the blueprint's value in
code and drops the rest), `verdict` is `request_changes` iff any blocker. Return only the JSON
contract it specifies. Files under `docs/factory/**`, lockfiles and generated files are not
reviewed for style. A fix that answers a blocker must keep the test suite green: a red suite
after a fix is itself a blocker.
