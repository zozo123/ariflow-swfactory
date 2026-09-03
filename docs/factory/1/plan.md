# Plan — 1

## Files
- tests/test_core.py
- src/calc/core.py
- src/calc/__init__.py

## Order
- Append percent_change tests to tests/test_core.py: import percent_change from calc alongside compound and simple_interest; add success-path tests (R1, R2, R3, R6, R7, R8) using pytest.approx, ValueError tests (R4, R5) using pytest.raises, an export test (R9) asserting 'percent_change' in calc.__all__, a docstring test (R10) asserting the docstring is non-empty and contains 'fraction', and leave the three existing tests unmodified (R11).
- Run `uv run --group dev pytest` and confirm the new tests fail with ImportError/AttributeError while the three existing tests still pass (R11, R12).
- Implement percent_change(old: float, new: float) -> float in src/calc/core.py after compound(): one-line docstring stating the result is a fraction (0.25 == +25%), guard `if old == 0: raise ValueError("old must be non-zero")`, then `return (new - old) / old` with no abs() and no other validation (R1-R8, R10).
- Export percent_change from src/calc/__init__.py: extend the import from calc.core and set __all__ = ["compound", "percent_change", "simple_interest"], keeping the list alphabetically sorted (R9, R11).
- Run `uv run --group dev pytest` and confirm output ends with `passed` with all old and new tests green (R11, R12); run `uv run --group dev python -m compileall -q src` to confirm the lint command passes.
- Docs: verify the module docstring of core.py still reads correctly ("Core interest calculations.") and that the percent_change docstring mentions fraction semantics; no other documentation files exist in the target, so no further docs changes.

## Tests
- R1: test_percent_change_rise — percent_change(100, 125) == pytest.approx(0.25)
- R2: test_percent_change_fall — percent_change(200, 150) == pytest.approx(-0.25)
- R3: test_percent_change_no_change — percent_change(50, 50) == pytest.approx(0.0)
- R4: test_percent_change_zero_old_rejected — pytest.raises(ValueError) for percent_change(0, 10)
- R5: test_percent_change_zero_old_zero_new_rejected — pytest.raises(ValueError) for percent_change(0.0, 0.0)
- R6: test_percent_change_negative_baseline — percent_change(-100, -50) == pytest.approx(0.5)
- R7: test_percent_change_total_loss — percent_change(100, 0) == pytest.approx(-1.0)
- R8: test_percent_change_int_inputs_return_float — result = percent_change(4, 5); isinstance(result, float) and result == pytest.approx(0.25)
- R9: test_percent_change_exported — 'percent_change' in calc.__all__ and `from calc import percent_change` at module top succeeds
- R10: test_percent_change_docstring — percent_change.__doc__ is a non-empty str containing 'fraction'
- R11: existing test_simple_interest, test_compound_annual, test_negative_rejected remain unmodified and pass; additionally assert calc.__all__ still contains 'compound' and 'simple_interest'
- R12: full `uv run --group dev pytest` run ends with `passed` (at least two new percent_change tests: one success path, one ValueError path)

## Risks
- Float equality: results like 0.25 and -0.25 are exact in binary but others may not be; all numeric assertions use pytest.approx as the existing tests do, so no false failures.
- Zero check: `old == 0` catches both int 0 and float 0.0 (and -0.0); tests R4 and R5 cover both literal forms so a stricter `is` or type-based check would be caught.
- Negative baseline sign convention: the literal formula flips sign for old < 0 (R6). This is a recorded assumption from the spec's open questions; a test pins the behaviour so any later switch to abs(old) is a visible, deliberate change.
- Breaking existing API: only additive changes to core.py and __init__.py; existing tests remain byte-identical and are run before and after implementation to prove R11.
- Protected paths: tests/test_core.py is edited only in the build stage (new tests are part of the deliverable); no changes to factory.toml, and no test edits during any fix iteration, per the factory's deny rules.
- Scope creep: no handling of inf/nan, no rounding, no percent scaling, no extra parameters; the implementation is one guard plus one return line, matching the spec's API section exactly.
