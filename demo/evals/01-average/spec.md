# Spec — EVAL-AVERAGE: `average`

## Requirements
1. `average(values: Sequence[float]) -> float` is exported from `calc` and in `__all__`.
2. Returns the arithmetic mean of `values`.
3. Negative and mixed-sign values are allowed: `average([-1, 1]) == 0.0`.
4. An empty sequence raises `ValueError`.
5. Docstring states the return is the arithmetic mean and names the `ValueError`.

## API
```python
def average(values: Sequence[float]) -> float: ...
```

## Concerns
No new dependencies (`math.fsum` is stdlib), no network egress, no data handling changes.

## Open questions
None: the four cases above are stated in the issue.
