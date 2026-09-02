# REVIEW.md — automated PR review policy

Every factory PR gets the same review passes. Findings are ranked; humans spend attention on intent and risk, not typos.

## Passes (in order)
1. **Correctness** — logic errors, unhandled edge cases, broken invariants named in `spec.md`.
2. **Tests** — new behaviour without tests; tests that assert nothing; edited existing tests (must be justified in `plan.md`).
3. **Security** — secrets, injection, unsafe deserialization, path traversal, new network egress.
4. **Plan fidelity** — diff touches files not listed in `plan.md`, or skips listed work.
5. **Style** — only if it hurts readability. Never block on style.

## Severity
- `blocker` — merge must not happen; sent back to Build (bounded by `max_build_iterations`).
- `major` — should fix before merge; PR is opened, finding is listed at top of PR body.
- `minor` — listed in PR body.
- `nit` — at most **3** per review; the rest are dropped.

## Output contract
The reviewer returns JSON: `{"verdict": "approve"|"request_changes", "findings": [{"severity","file","line","title","detail"}]}`.
`verdict` is `request_changes` iff any finding is `blocker`.

## Exclusions
- Generated files, lockfiles, and `docs/factory/**` artifacts are not reviewed for style.
