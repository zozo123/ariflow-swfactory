# Spec — EVAL-MEDIAN: `median`

## Requirements
1. `median(values: Sequence[float]) -> float` is exported from `calc` and in `__all__`.
2. Odd length: the middle value of the sorted input.
3. Even length: the mean of the two middle values.
4. An empty sequence raises `ValueError`.
5. Docstring + at least two tests (the target's CLAUDE.md requires them for public functions).

## API
```python
def median(values: Sequence[float]) -> float: ...
```

## Concerns
None: sorting a copy leaves the caller's sequence untouched.

## Open questions
None.
