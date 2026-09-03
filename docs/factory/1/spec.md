# spec.md

## Requirements

- **R1** `calc.percent_change(old, new)` returns `(new - old) / old` as a fraction: `percent_change(100, 125) == pytest.approx(0.25)`.
- **R2** A fall is a negative fraction: `percent_change(200, 150) == pytest.approx(-0.25)`.
- **R3** No change returns `0.0`: `percent_change(50, 50) == pytest.approx(0.0)`.
- **R4** `old == 0` raises `ValueError` (the relative change is undefined), including `0.0` and `-0.0`.
- **R5** Negative `old` is accepted and computed by the same formula: `percent_change(-100, -50) == pytest.approx(-0.5)`; the function does not reject negative inputs the way `simple_interest`/`compound` do, because falling or negative prices are explicitly in scope.
- **R6** `new == 0` is valid and returns `-1.0`: `percent_change(100, 0) == pytest.approx(-1.0)`.
- **R7** Integer inputs are accepted and the result is a `float`: `isinstance(percent_change(1, 2), float)`.
- **R8** `percent_change` is importable from the package root: `from calc import percent_change` succeeds and `"percent_change" in calc.__all__`.
- **R9** The function has a non-empty docstring stating that the result is a fraction, not a percent (per `CLAUDE.md`).
- **R10** At least two new tests exist in `tests/` covering the rise (R1) and the `old == 0` error (R4); the fall case (R2) is also covered.
- **R11** Backwards compatibility: `simple_interest` and `compound` signatures, behavior and the three existing tests in `tests/test_core.py` are unchanged and still pass.
- **R12** `uv run --group dev pytest` output ends with `passed`, and `python -m compileall -q src` succeeds (the `factory.toml` lint command).

## API

- **Module:** `src/calc/core.py` (pure function, alongside `simple_interest` and `compound`).
- **Export:** added to the import line and `__all__` in `src/calc/__init__.py`.
- **Signature:** `percent_change(old: float, new: float) -> float`
- **Returns:** `(new - old) / old`, a fraction (`0.25` means +25 %).
- **Raises:** `ValueError` when `old == 0`. No other validation; any real `float`/`int` is accepted.
- **No new dependencies**, no changes to `factory.toml`, `pyproject.toml` or `tests/` protected paths beyond adding tests.

## Concerns

- **Correctness / float equality:** `old == 0` must be tested with `==` (catches `0`, `0.0`, `-0.0`), not `is` or truthiness tricks; tests use `pytest.approx` for results, matching the existing style.
- **Correctness / sign convention:** with negative `old` the sign of the result follows the raw formula (R5). This is documented in the docstring so callers are not surprised; no attempt to take `abs(old)` since the intent does not ask for it.
- **Correctness / NaN and infinity:** `float("nan")` and `inf` propagate per IEEE rules and are not rejected; out of scope of the intent, and consistent with existing functions which also do not guard them.
- **Security:** pure arithmetic on numbers, no I/O, no user-controlled strings; no risk.
- **Performance:** one subtraction and one division; negligible.
- **Maintainability:** follows the repository conventions (pure function in `core.py`, re-exported from `__init__.py`, docstring, rate-as-fraction). Adding the name to `__all__` keeps `from calc import *` consistent. Tests go in the existing `tests/test_core.py` to keep the single-file layout.

## Open questions

- **Q1 Should negative `old` be rejected?** The intent says "negative values allowed (falling prices)" and its example only shows a negative *result*. Assumption: both negative inputs and negative results are allowed; only `old == 0` raises (R4, R5).
- **Q2 Should the ValueError message be prescribed?** Not stated. Assumption: any `ValueError` satisfies acceptance; the message should say the change from zero is undefined.
- **Q3 Should NaN/inf be validated?** Not mentioned. Assumption: no, to match `simple_interest`/`compound`.
- **Q4 Where do new tests live?** Not stated. Assumption: `tests/test_core.py`, next to the existing three tests, with at least two tests as `CLAUDE.md` requires.
