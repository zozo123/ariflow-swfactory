I have everything I need: `calc` is a two-function pure module (`simple_interest`, `compound`) in `src/calc/core.py`, re-exported via `__all__`, with three pytest tests. Here is the spec.

# spec.md — percent_change(old, new)

## Requirements

- **R1** `calc.percent_change(100, 125)` returns `0.25` (relative change as a fraction, not a percent, per CLAUDE.md "rates are fractions").
- **R2** `calc.percent_change(200, 150)` returns `-0.25` (falling values allowed, negative result).
- **R3** `calc.percent_change(50, 50)` returns `0.0` (no change).
- **R4** `calc.percent_change(0, 10)` raises `ValueError` (undefined baseline).
- **R5** `calc.percent_change(0.0, 0.0)` raises `ValueError` (zero baseline even when `new` is also zero).
- **R6** `calc.percent_change(-100, -50)` returns `0.5`, i.e. `(new - old) / old` is computed literally with no `abs()` on the denominator; negative `old` is allowed, matching "negative values allowed".
- **R7** `calc.percent_change(100, 0)` returns `-1.0` (`new == 0` is a valid total loss, not an error).
- **R8** Integer inputs are accepted and the result is a `float` (e.g. `percent_change(4, 5) == 0.25`).
- **R9** `"percent_change" in calc.__all__` and `from calc import percent_change` succeeds.
- **R10** `calc.percent_change.__doc__` is a non-empty string that states the result is a fraction.
- **R11** `simple_interest` and `compound` signatures, behaviour and `__all__` membership are unchanged; the three existing tests in `tests/test_core.py` pass unmodified.
- **R12** At least two new tests for `percent_change` exist under `tests/` (one success path, one `ValueError` path), so `uv run --group dev pytest` still ends with `passed`.

## API

- **Location**: `src/calc/core.py` (pure function, no I/O, stdlib only). Re-exported from `src/calc/__init__.py` and appended to `__all__` (keep the list alphabetically sorted: `compound`, `percent_change`, `simple_interest`).
- **Signature**: `percent_change(old: float, new: float) -> float`
- **Returns**: `(new - old) / old` as a `float`.
- **Raises**: `ValueError` when `old == 0` (message names the argument, e.g. "old must be non-zero"). No other validation: `new` may be any finite number, including zero or negative.
- **Does not** accept keyword-only tricks, extra parameters, rounding, or percent scaling. Consistent with the existing style: one guard clause, one return line, one-line docstring.

## Concerns

- **Correctness / float equality**: `old == 0` should be checked with a plain `==` so both `0` and `0.0` are caught; tests compare results with `pytest.approx` as the existing tests do, to avoid binary-fraction surprises.
- **Correctness / sign convention with negative `old`**: dividing by a negative baseline flips the sign (R6). This is the mathematically literal definition and the intent explicitly allows negatives; the assumption is recorded in Open questions rather than silently using `abs(old)`.
- **Security**: none. Pure arithmetic, no I/O, no external input parsing, no new dependencies.
- **Performance**: O(1); nothing to mitigate.
- **Maintainability**: follow existing conventions (module docstring style, `ValueError` guard first, fraction semantics). Keep `__all__` in sync so the public surface remains the single source of truth. Do not touch `factory.toml` or `tests/` during a fix stage; new tests belong in the build stage.
- **Non-finite inputs**: `inf`/`nan` are passed through to arithmetic as the rest of `calc` does; no special handling, to avoid scope creep.

## Open questions

- **Negative baseline sign**: Should `old < 0` use `abs(old)` in the denominator (common in finance reporting) or the literal formula? *Assumption*: literal `(new - old) / old`, because the intent gives the formula by example and says only "negative values allowed" without redefining it.
- **`new == 0`**: Is a change to zero an error or a valid `-1.0`? *Assumption*: valid, since the intent restricts the error to `old == 0` only.
- **Test file placement**: New tests in the existing `tests/test_core.py` or a new file? *Assumption*: append to `tests/test_core.py`, matching the single-file layout of the repo.
- **Error message text**: Not specified. *Assumption*: any `ValueError` with a message mentioning `old` satisfies acceptance; tests assert on the exception type only.
