# Spec — EVAL-ROUNDING: `round_fraction`

The issue does not say how presentable. This spec implements the narrowest reading that cannot
be wrong for the caller — rounding, in `calc`, leaving formatting to the report — and records
what a human must answer before anything wider ships.

## Requirements
1. `round_fraction(value: float, places: int = 2) -> float` is exported from `calc`.
2. Returns `value` rounded to `places` decimal places, still a fraction (0.2534 -> 0.25).
3. `places` must be >= 0; anything else raises `ValueError`.
4. Rounding is Python's `round`, i.e. banker's rounding — the docstring says so.
5. Nothing existing changes: no formatting, no percent conversion, no report code touched.

## API
```python
def round_fraction(value: float, places: int = 2) -> float: ...
```

## Concerns
Presentation belongs to the report, not to `calc`; returning a `float` keeps `calc` free of
locale and formatting concerns. A `str` return would have been the wider, harder-to-undo choice.

## Open questions
1. How many decimal places does the report want — 2 (0.25) or 1 (0.3)?
2. Should the report show a percent (`25.35%`) instead of a fraction? That is a formatting
   helper (`str`), not a rounding one, and would live in the report or in a new `calc.format`.
3. Is banker's rounding acceptable for money-adjacent figures, or must this be
   `ROUND_HALF_UP` via `decimal`?
4. Do the three copies of "change vs last week" in the report all want the same precision?
