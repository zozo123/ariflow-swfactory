---
id: EVAL-MEDIAN
title: Add median(values) to calc
labels: [factory]
expect:
  stages: [intent, spec, plan, build_and_test]
  tests_pass: true
  max_build_iterations: 1
  # One code-decided blocker per planned acceptance test (R1-R4) that never ran; the scripted
  # reviewer approves, so the block comes from the requirement trace, not a hand-written verdict.
  blockers: 4
  label: factory:blocked
  exports: [calc.median]
  artifacts_contain:
    review.json: ["Acceptance test missing: test_odd_length", "R2 is planned to be proven by"]
    trace.json: ["\"evidence\": \"junit\"", "\"test\": \"test_empty_raises\"", "\"status\": \"missing\""]
---
The report needs a typical figure that a single outlier week cannot drag around, so add a median
next to the mean.

Acceptance:
- `median(values) -> float` is importable from `calc` and listed in `__all__`.
- Odd length returns the middle value, even length the mean of the two middle values.
- An empty sequence raises `ValueError`.
- Docstring + tests, existing `calc` tests keep passing.
