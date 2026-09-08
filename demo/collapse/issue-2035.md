---
id: COLLAPSE-2035
title: One brutal chaos acceptance suite for the kernel's safety invariant
labels: [factory, selfhost]
---
## Problem

The kernel's real promise is a safety property, and it is currently asserted by unit tests rather than by adversarial scenarios. The promise should be split into the two things it actually is:

- **Safety** (provable): no mutation is ever performed without current authority, and no logical operation is ever committed twice.
- **Progress** (bounded): every operation is always classifiable as committed / definitely-absent / in-doubt, and in-doubt always names a deterministic repair.

Stating it as "converges to one authorized outcome" overclaims, because an ambiguous observation may stay `in_doubt` indefinitely - which the model correctly permits. Safety is provable; liveness is only ever bounded.

## Scenarios

Each must hold the same four acceptance conditions: **no unauthorized mutation, no silent duplicate, the operator can explain the state, a deterministic repair exists.**

1. Kill an agent mid-write.
2. Lose the GitHub response after the PR was created.
3. Restart the backend after the journal commit but before the receipt.
4. Advance the Cell epoch while an old worker is still running.
5. Cancel during repair.
6. Kill Airflow mid-stage.
7. Leave a sandbox orphaned.
8. Introduce target-head drift between evidence and merge.

## Acceptance

- Scenarios live as a runnable suite, not prose, and each asserts all four conditions.
- Scenario 4 is the sharpest: it is the whole reason epochs exist, and it must assert that the stale worker's mutation is **refused**, not merely that it loses a race.
- Scenario 8 should note that GitHub's own `BEHIND` merge state models the same concept the evidence binding already carries, so the test can cross-check that the two agree.
- Scenario 7 has a known gap to close first: `sandbox.docker` teardown is `--rm` only, `close()` is a no-op, and argv sets no `--name`/`--label`, so an orphan is currently unidentifiable. That is a prerequisite, not part of the assertion.

