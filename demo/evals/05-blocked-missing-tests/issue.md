---
id: EVAL-MEDIAN
title: Add median(values) to calc
labels: [factory]
expect:
  stages: [intent, spec, plan, build_and_test]
  tests_pass: true
  max_build_iterations: 1
  blockers: 1
  label: factory:blocked
  exports: [calc.median]
---
The report needs a typical figure that a single outlier week cannot drag around, so add a median
next to the mean.

Acceptance:
- `median(values) -> float` is importable from `calc` and listed in `__all__`.
- Odd length returns the middle value, even length the mean of the two middle values.
- An empty sequence raises `ValueError`.
- Docstring + tests, existing `calc` tests keep passing.
