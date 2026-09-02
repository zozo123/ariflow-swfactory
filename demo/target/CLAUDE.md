# calc — agent notes
- Pure functions in `src/calc/core.py`; export new public functions from `src/calc/__init__.py`.
- Rates are fractions (0.05), never percents. Validate inputs and raise `ValueError`.
- Every public function needs a docstring and at least two tests in `tests/`.
- Commands: see `factory.toml`. Healthy test output ends with `passed`.
