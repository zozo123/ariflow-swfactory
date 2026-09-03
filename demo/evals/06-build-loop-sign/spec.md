# Spec — EVAL-DRAWDOWN: `drawdown`

## Requirements
1. `drawdown(peak: float, trough: float) -> float` is exported from `calc` and in `__all__`.
2. Returns `(peak - trough) / peak`: a decline is a POSITIVE fraction, `drawdown(100, 75)` is
   `0.25`. This is the opposite sign convention from `percent_change`-style helpers.
3. `peak == 0` raises `ValueError`.
4. `trough > peak` returns a negative number: no decline happened.
5. Docstring states the sign convention; tests cover R2-R4.

## API
```python
def drawdown(peak: float, trough: float) -> float: ...
```

## Concerns
The sign convention is the whole risk of this change: reviewers and callers disagree by habit.
R2 and R4 pin it in both directions.

## Open questions
None.
