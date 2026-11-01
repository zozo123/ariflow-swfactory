---
id: DEMO-1
title: Add percent_change(old, new) to calc
labels: [factory]
---
As a user of `calc` I want a `percent_change(old: float, new: float) -> float` that returns the
relative change from `old` to `new` as a fraction (e.g. 100 -> 125 gives 0.25).
The formula is `(new - old) / old`; the denominator retains its sign, rather than using `abs(old)`.

Acceptance:
- Exported from `calc`.
- `old == 0` raises `ValueError` (undefined).
- Negative values allowed (falling prices), e.g. 200 -> 150 gives -0.25.
- A negative baseline uses the same formula: -100 -> -50 gives -0.5.
- Docstring + tests, existing tests keep passing.
