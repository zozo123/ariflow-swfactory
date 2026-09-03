# Spec — EVAL-FINITE: reject non-finite arguments

## Requirements
1. `simple_interest(principal, rate, years)` raises `ValueError` when any argument is `nan`
   or `±inf`.
2. `compound(principal, rate, years, periods_per_year)` does the same for its float arguments.
3. The message names the argument, e.g. `principal must be a finite number`.
4. The finite path is unchanged: `simple_interest(1000, 0.05, 2) == 100.0`.
5. The check runs BEFORE the existing sign checks: `nan < 0` is `False`, so a comparison-based
   guard can never catch it.

## API
Unchanged. One private helper (`_require_finite`) is added; nothing new is exported.

## Concerns
`math.isfinite` is stdlib. No behaviour change for finite input, so no caller migration.

## Open questions
None.
