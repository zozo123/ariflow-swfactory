---
id: EVAL-FINITE
title: calc silently accepts nan and inf
labels: [factory]
expect:
  stages: [intent, spec, plan, build_and_test, review, deliver]
  tests_pass: true
  max_build_iterations: 1
  blockers: 0
---
A bad row in the report source gave us `simple_interest(nan, 0.05, 2)` and the whole weekly
total came out `nan` with no error anywhere. The negative checks are there, but `nan` and `inf`
slip through every comparison.

Acceptance:
- `simple_interest` and `compound` raise `ValueError` when any numeric argument is not finite.
- The error message names the offending argument.
- Finite arguments keep their current results, and the existing tests keep passing.
