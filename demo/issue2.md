---
id: DEMO-2
title: calc should expose the relative change used by the weekly report
labels: [factory]
---
The weekly report recomputes "change vs last week" by hand in three places and the three copies
already disagree about the sign. Please put the one implementation in `calc` so the report can
import it.

Acceptance:
- `percent_change(old: float, new: float) -> float` is importable from `calc` and listed in
  `__all__`.
- The result is the fraction `(new - old) / old`, not a percentage: 100 -> 125 is `0.25`.
- A fall is negative, so 200 -> 150 is `-0.25`.
- `old == 0` raises `ValueError`: the relative change has no meaning without a baseline.
- Docstring says the result is a fraction; tests cover the four cases above and the existing
  `calc` tests keep passing.
