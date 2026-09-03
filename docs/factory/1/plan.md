# Plan — 1

## Files
- tests/test_core.py
- src/calc/core.py
- src/calc/__init__.py

## Order
- Step 1 (tests, R1-R10): append new tests to tests/test_core.py, importing percent_change from calc alongside compound and simple_interest; cover rise 100->125 == 0.25 (R1), fall 200->150 == -0.25 (R2), no change 50->50 == 0.0 (R3), old in {0, 0.0, -0.0} raises ValueError via pytest.raises (R4), negative old -100->-50 == -0.5 (R5), new == 0 gives -1.0 (R6), integer inputs return a float (R7), name present in calc.__all__ (R8), docstring non-empty and mentions 'fraction' (R9). Use pytest.approx for numeric results, matching existing style.
- Step 2 (verify red): run `uv run --group dev pytest`; confirm the new tests fail with ImportError/AttributeError while the three existing tests still pass (R11 baseline).
- Step 3 (implementation, R1-R7): add `percent_change(old: float, new: float) -> float` to src/calc/core.py after `compound`; body: `if old == 0: raise ValueError("percent_change is undefined when old == 0")` then `return (new - old) / old`. No other validation; do not touch simple_interest or compound (R11).
- Step 4 (docs, R9): give percent_change a docstring stating it returns the relative change from old to new as a fraction (0.25 == +25 %), not a percent; that negative results mean a fall; that negative old is accepted and the sign follows the raw formula; and that old == 0 raises ValueError.
- Step 5 (export, R8): in src/calc/__init__.py extend the import line to `from calc.core import compound, percent_change, simple_interest` and add "percent_change" to `__all__` (keep alphabetical order).
- Step 6 (verify green, R11, R12): run `uv run --group dev pytest --junitxml=.factory/junit.xml` and confirm output ends with `passed` with all tests (3 existing + new) green; run `uv run --group dev python -m compileall -q src` and confirm exit 0. Do not modify factory.toml or pyproject.toml.

## Tests
- R1: test_percent_change_rise — percent_change(100, 125) == pytest.approx(0.25)
- R2: test_percent_change_fall — percent_change(200, 150) == pytest.approx(-0.25)
- R3: test_percent_change_no_change — percent_change(50, 50) == pytest.approx(0.0)
- R4: test_percent_change_zero_old_rejected — parametrized over old in (0, 0.0, -0.0): pytest.raises(ValueError) on percent_change(old, 10)
- R5: test_percent_change_negative_old — percent_change(-100, -50) == pytest.approx(-0.5)
- R6: test_percent_change_to_zero — percent_change(100, 0) == pytest.approx(-1.0)
- R7: test_percent_change_returns_float — isinstance(percent_change(1, 2), float) and value == pytest.approx(1.0)
- R8: test_percent_change_exported — `from calc import percent_change` at module top succeeds and "percent_change" in calc.__all__
- R9: test_percent_change_docstring — percent_change.__doc__ is non-empty and contains 'fraction'
- R10: satisfied by the union of the tests above (rise, fall and old == 0 error are each covered)
- R11: existing test_simple_interest, test_compound_annual, test_negative_rejected remain untouched and pass
- R12: full `uv run --group dev pytest` run ends with `passed`; `python -m compileall -q src` exits 0

## Risks
- Zero check written as truthiness (`if not old`) or identity (`old is 0`) instead of `old == 0` would mishandle -0.0 or ints; bounded by the parametrized R4 test over 0, 0.0 and -0.0.
- Integer division semantics: Python 3 `/` always yields float, so R7 holds without an explicit float() cast; the R7 isinstance test guards against accidental use of `//`.
- Forgetting the `__all__` update while the import works would let `from calc import percent_change` pass but break `from calc import *`; bounded by the explicit `__all__` membership test (R8).
- Accidentally adding non-negativity validation by copy-pasting from simple_interest would reject falling prices; bounded by the R5 negative-old test.
- Protected paths: tests/ is listed as protected in factory.toml for fix tasks; all test additions happen in the build stage (step 1) so later fix iterations only touch src/. factory.toml and pyproject.toml are never edited.
- Float precision: results compared with pytest.approx, never exact equality, so platform float differences cannot cause flakes.
