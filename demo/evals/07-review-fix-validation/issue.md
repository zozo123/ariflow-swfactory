---
id: EVAL-VARIANCE
title: Add variance(values) to calc
labels: [factory]
expect:
  stages: [intent, spec, plan, build_and_test, review, deliver]
  tests_pass: true
  max_build_iterations: 1
  max_review_fixes: 1
  blockers: 0
  exports: [calc.variance]
---
The report wants to say how spread out the weekly figures are, so add the population variance
next to the mean.

Acceptance:
- `variance(values) -> float` is importable from `calc` and listed in `__all__`.
- Population variance (divide by `n`), not the sample one: `variance([1, 2, 3])` is
  `0.6666...`.
- All-equal input gives `0.0`.
- An empty sequence raises `ValueError`, like the rest of `calc`.
- Docstring + tests, existing `calc` tests keep passing.
