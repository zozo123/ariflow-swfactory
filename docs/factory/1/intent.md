---
id: '1'
title: Add percent_change(old, new) to calc
labels:
- factory
url: https://github.com/zozo123/ariflow-swfactory/issues/1
run_id: real0001
---
As a user of `calc` I want a `percent_change(old: float, new: float) -> float` that returns the
relative change from `old` to `new` as a fraction (e.g. 100 -> 125 gives 0.25).

Acceptance:
- Exported from `calc`.
- `old == 0` raises `ValueError` (undefined).
- Negative values allowed (falling prices), e.g. 200 -> 150 gives -0.25.
- Docstring + tests, existing tests keep passing.
