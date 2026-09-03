---
id: EVAL-ROUNDING
title: The report's change figures are unreadable
labels: [factory]
expect:
  stages: [intent, spec, plan, build_and_test, review, deliver]
  tests_pass: true
  max_build_iterations: 1
  blockers: 0
  exports: [calc.round_fraction]
  artifacts_contain:
    spec.md: ["## Open questions", "?"]
---
The weekly report prints change figures like `0.25348712456`. Make them presentable.
