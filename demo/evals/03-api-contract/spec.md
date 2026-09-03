# Spec — EVAL-CONTRACT: pin the documented return contract

## Requirements
1. `simple_interest.__doc__` states the result is the **interest only** (principal excluded).
2. `compound.__doc__` states the result is the **final amount** (principal included).
3. Both docstrings state that a rate is a fraction, not a percent.
4. `tests/test_contract.py` asserts R1-R3 against `__doc__`, so dropping a clause fails CI.
5. No signature or arithmetic change: the existing tests keep passing untouched.

## API
Unchanged. This is a documentation contract change plus the test that pins it.

## Concerns
Docstring assertions are brittle if they match whole sentences; the tests match the load-bearing
phrases only ("interest only", "final amount", "fraction").

## Open questions
None.
