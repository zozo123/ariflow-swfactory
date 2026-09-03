# REVIEW.md — automated PR review policy

Every factory PR gets the same review passes. Findings are ranked; humans spend attention on intent and risk, not typos.

## Passes (in order)
1. **Correctness** — logic errors, unhandled edge cases, broken invariants named in `spec.md`.
2. **Tests** — new behaviour without tests; tests that assert nothing; edited existing tests (must be justified in `plan.md`).
3. **Security** — secrets, injection, unsafe deserialization, path traversal, new network egress.
4. **Plan fidelity** — diff touches files not listed in `plan.md` (major), or skips listed work (minor; major when the skipped file is a test). Both halves are also checked deterministically in code.
5. **Style** — only if it hurts readability. Never block on style.

## Severity
- `blocker` — merge must not happen; sent back to a fix + test + re-review round (bounded by
  `max_review_fixes`). A fix that leaves the test suite red is itself a blocker. Unresolved
  blockers ship as a `[BLOCKED]` PR labeled `factory:blocked`, never as a merge.
- `major` — should fix before merge; PR is opened, finding is listed at top of PR body.
- `minor` — listed in PR body.
- `nit` — at most **3** per review by default (`[review] nit_cap` per blueprint); the rest are dropped.

## Output contract
The reviewer returns JSON: `{"verdict": "approve"|"request_changes", "findings": [{"severity","file","line","title","detail"}]}`.
`verdict` is `request_changes` iff any finding is `blocker`.

## Exclusions
- Generated files, lockfiles, and `docs/factory/**` artifacts are not reviewed for style.
