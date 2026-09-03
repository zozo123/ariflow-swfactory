---
id: EVAL-AVERAGE
title: Add average(values) to calc
labels: [factory]
expect:
  stages: [intent, spec, plan, build_and_test, review, deliver]
  tests_pass: true
  max_build_iterations: 1
  blockers: 0
  exports: [calc.average]
---
The weekly report averages a list of figures by hand in two places. Put the one implementation
in `calc`.

Acceptance:
- `average(values) -> float` is importable from `calc` and listed in `__all__`.
- Returns the arithmetic mean; `average([1, 2, 3])` is `2.0`.
- Negative values are allowed: `average([-1, 1])` is `0.0`.
- An empty sequence raises `ValueError`: the mean of nothing is undefined.
- Docstring + tests, existing `calc` tests keep passing.
