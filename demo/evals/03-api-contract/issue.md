---
id: EVAL-CONTRACT
title: Docstrings do not say what simple_interest returns
labels: [factory]
expect:
  stages: [intent, spec, plan, build_and_test, review, deliver]
  tests_pass: true
  max_build_iterations: 1
  blockers: 0
  artifacts_contain:
    spec.md: ["## Requirements"]
---
Two teams read `simple_interest` as "principal plus interest" and shipped the wrong number.
`compound` really does return the final amount, `simple_interest` returns the interest only —
the docstrings do not say so, and nothing pins them.

Acceptance:
- Both docstrings state exactly what is returned and that a rate is a fraction, not a percent.
- The contract is pinned by tests, so a future edit that drops it fails CI.
- No behaviour change: the arithmetic and the signatures stay as they are.
