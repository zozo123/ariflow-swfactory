# Managed software factory implementation map

This file is the review/merge map for epic #22. Airflow remains the only scheduler; all issue-specific execution stays bounded inside fixed lifecycle DAGs.

## Foundation

| Issue | PR | Surface |
|---|---:|---|
| #21 | #23 | lifecycle graph, roles, visual demo |
| #24 | #58 | deterministic `Plan.work` execution planning |
| #25 | #45 | sandbox capability negotiation and lineage |
| #26 | #46 | admission control and backpressure |
| #27 | #48 | exactly-once operation journal |
| #28 | #49 | trust-zone policy and redaction |
| #29 | #50 | optimistic durable backend store |
| #30 | #51 | durable evidence bundles |
| #31 | #52 | managed intake and deduplication |
| #32 | #53 | CI topology impact analysis |
| #33 | #47 | same-repository coordination |
| #34 | #54 | seeded fault/evidence plans |
| #35 | #55 | provider conformance contract |
| #36 | #56 | large-repository materialization |
| #37 | #57 | versioned compatibility contracts |
| #38 | #59 | release provenance verification |
| #39 | #60 | deployment/drain contracts |
| #40 | #61 | line validation/graph/semantic diff |
| #41 | #44 | Factory Cell durability and epoch fencing |
| #42 | #62 | bounded factory generations/promotion |
| #43 | #63 | provenance-backed public capability data |

## Recommended review/merge sequence

1. #23 — current lifecycle foundation and Rust CLI integration fixture fix.
2. #49 + #48 — security and idempotent mutation boundary.
3. #44 — durable Factory Cell identity/fencing.
4. #45 + #47 + #46 — compute lineage, repository coordination, admission.
5. #58 — bounded `Plan.work` parallelism/merge planning.
6. #50 + #51 + #57 — scalable persistence, evidence, compatibility.
7. #52 + #53 + #56 — managed intake, topology awareness, materialization.
8. #54 + #55 + #63 — fault evidence, provider conformance, public claims.
9. #59 + #60 + #61 — provenance, deployment, operator authoring.
10. #62 — factory-of-factories only after the trusted boundary is stable.

## Cross-cutting invariants

Every merge should preserve:

- Airflow is authoritative for scheduling, task retries and HITL gates.
- Factory Cell identity outlives any sandbox/process.
- External mutation authority is fenced by `(cell_id, epoch)`.
- Retryable side effects use stable operation keys.
- Provider possession is never publication/control-plane authority.
- Parallel work cannot silently last-writer-win.
- Public capability claims require retained evidence.
- Candidate factory generations cannot self-promote or access parent production credentials.

## Session policy

The PRs listed above were intentionally opened without merge. Their descriptions state that tests were not run in the coding session by explicit request. A later integration session should review, rebase as needed, add integration wiring where branches overlap, run the full Python/Rust/Airflow test matrix, then merge in dependency order.
