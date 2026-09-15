# REVIEW_LIQUID.md — annealed Liquid review policy

The Liquid line deliberately creates implementation freedom early and destroys uncertainty before
promotion. Review is therefore an **evidence fan-out / deterministic fan-in** step, not another
scheduler and not a vote. Apache Airflow still owns the one `review` stage; Factory Cell identity,
epoch fencing, budgets, publication authority, and the human merge decision remain unchanged.

The managed Liquid line runs three independent read-only review lanes over the same candidate. A
finding survives fan-in by deterministic identity (`file`, `line`, `title`); when lanes disagree on
severity, the stronger severity wins. A repair round must re-run the target verification command
before another review round.

## Specialist lanes

### correctness
Review correctness and architecture only:
- broken requirements or invariants from `spec.md`;
- logic and state-machine errors, edge cases, concurrency and race conditions;
- recovery/idempotency/fencing mistakes;
- API and data-contract breakage;
- implementations that satisfy the happy path but not the declared plan.

### verification
Review proof and test quality only:
- changed behavior with no meaningful test;
- tests that can pass without exercising the behavior;
- missing failure/retry/recovery coverage;
- edited or weakened tests not justified by `plan.md`;
- plan fidelity: unplanned changed files or planned work that never landed;
- evidence that is stale, candidate-unbound, or incapable of proving the claim.

### risk
Review trust and operational risk only:
- secrets, injection, unsafe deserialization, path traversal, unexpected network egress;
- privilege/capability widening and tenant-boundary mistakes;
- new external mutations without identity, fencing, idempotency, reconciliation, or retained evidence;
- violations of the rule that Airflow is the only lifecycle scheduler;
- rollback/recovery gaps, retry amplification, unbounded cost or resource growth;
- changes to control-plane, CI, deployment, sandbox, security, publication, or lifecycle code whose
  verification is not commensurate with their blast radius.

## Relaxation and annealing

The host combines the three lane outputs and treats **blocker + major** findings as material defects.
While the blueprint's bounded `max_review_fixes` budget remains, material defects are sent through
the existing write-scoped `fix` policy, committed by the trusted factory, re-tested, and reviewed
again. `minor` and `nit` findings remain visible but do not by themselves trigger another repair.

The host also records dimensionless diagnostics in `annealing.json`: defect energy, effective
exploration temperature, beta, interface/surface penalty, driving force, nucleation-barrier proxy,
crossing-probability proxy, order parameter, and the phase label. These are **explanations**, never
authority. No numeric score can override a red test, a material finding, an approval, a Cell fence,
or a human promotion decision.

A candidate is `crystal` only when all of these ordinary software conditions are true:
1. the target verification command is green for the current candidate;
2. all specialist lanes completed for the round;
3. zero `blocker` findings remain;
4. zero `major` findings remain.

If the repair budget is exhausted first, the review stage is `blocked` and delivery may publish a
`[BLOCKED]` evidence PR. Publication is not promotion.

## Severity

- `blocker` — correctness/security/recovery failure that makes promotion unsafe. Must be repaired;
  if unresolved, the candidate is blocked.
- `major` — material defect or missing proof that should be repaired before promotion. On the Liquid
  line this is part of the relaxation loop even though the base `Review` JSON contract reserves
  `request_changes` for blockers.
- `minor` — useful non-material improvement; retain in the PR evidence.
- `nit` — small readability/style point; retain at most the blueprint's configured nit cap.

## Output contract

Each specialist returns the existing strict JSON review shape:
`{"verdict":"approve"|"request_changes","findings":[{"severity","file","line","title","detail"}]}`.
For an individual specialist response, `verdict` is `request_changes` iff that response contains a
`blocker`. The trusted host — not an agent — performs fan-in, decides whether material defects remain,
runs the bounded relaxation loop, and writes the final `review.json` plus `annealing.json`.

## Exclusions

- Do not review generated files, lockfiles, or `docs/factory/**` for style.
- Do not invent a lifecycle action, approval, deployment, or merge. Review produces evidence only.
- Do not soften a finding because another lane is likely to notice it; lane independence is part of
  the evidence.
