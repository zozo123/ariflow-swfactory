# Spec — EVAL-VARIANCE: `variance`

## Requirements
1. `variance(values: Sequence[float]) -> float` is exported from `calc` and in `__all__`.
2. Population variance: `sum((x - mean) ** 2) / n`. `variance([1, 2, 3]) == 2 / 3`.
3. All-equal input gives `0.0`.
4. An empty sequence raises `ValueError`, matching the rest of the module.
5. Docstring states population (÷ n), not sample (÷ n-1).

## API
```python
def variance(values: Sequence[float]) -> float: ...
```

## Concerns
Population vs sample is the classic mistake here; R2's exact value pins it.

## Open questions
None.
