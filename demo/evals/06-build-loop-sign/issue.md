---
id: EVAL-DRAWDOWN
title: Add drawdown(peak, trough) to calc
labels: [factory]
expect:
  stages: [intent, spec, plan, build_and_test, review, deliver]
  tests_pass: true
  max_build_iterations: 2
  blockers: 0
  exports: [calc.drawdown]
---
The report wants the size of the worst decline, as a positive fraction of the peak, so
`drawdown(100, 75)` should read `0.25` — a quarter lost.

Acceptance:
- `drawdown(peak: float, trough: float) -> float` is importable from `calc` and in `__all__`.
- Returns `(peak - trough) / peak`, so a decline is POSITIVE.
- `peak == 0` raises `ValueError`; there is no decline from nothing.
- A trough above the peak yields a negative number (no decline happened).
- Docstring + tests, existing `calc` tests keep passing.
