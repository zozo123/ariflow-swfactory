# Spec — DEMO-1: `percent_change`

## Requirements
1. `percent_change(old: float, new: float) -> float` is exported from `calc`.
2. Returns `(new - old) / old`.
3. `old == 0` raises `ValueError("old must be non-zero")`.
4. Negative and decreasing values are allowed: `percent_change(200, 150) == -0.25`.
5. Docstring states that the result is a fraction, not a percent.

## API
```python
def percent_change(old: float, new: float) -> float: ...
```

## Concerns
None: no new dependencies, no network egress, no data handling changes.

## Open questions
None.
