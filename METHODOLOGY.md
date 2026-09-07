# Airflow Software Factory Methodology

The canonical development and execution methodology is documented in
[`docs/liquid-methodology.md`](docs/liquid-methodology.md).

The governing idea is:

> **Fan out ideas aggressively. Fan in contracts deterministically. Keep authority singular. Keep
> compute disposable. Keep identity durable. Fence mutations. Require evidence. Delete superseded
> abstractions.**

The methodology defines:

- Apache Airflow as the only lifecycle scheduler.
- Factory Cell identity as the durable issue x target ownership boundary.
- Positive Cell epochs and external mutation identity `(cell_id, epoch, operation_key)`.
- Disposable sandbox/VM incarnations recovered from durable intent.
- `Plan.work` as a bounded issue-specific inner graph, never a second scheduler.
- Singular scheduling, Cell, publication, and promotion authorities.
- Seven implementation lanes: authority, airflow, workgraph, recovery, security, evidence, operator.
- The Liquid concern matrix C01-C10: invariant, persistence, API, runtime, operator, security,
  recovery, scale, evidence, stabilization.
- Parallel fan-out followed by explicit contract fan-in and deletion of superseded implementations.
- Bundled implementation PRs: normally 50 generated issues (5 domains x 10 concerns), and 10-50
  issue tranches for irregular legacy work.
- Fail-closed policy/evidence semantics, durable recovery, cleanup reconciliation, explicit
  publication/promotion separation, bounded factory generations, and exact-head CI/merge fencing.

For the full flow, rationale, diagrams, anti-patterns, review checklist, and implementation map, read
[`docs/liquid-methodology.md`](docs/liquid-methodology.md).
