# Airflow Software Factory Methodology

The methodology has two canonical documents:

1. [`docs/liquid-methodology.md`](docs/liquid-methodology.md) - the core execution and Liquid Development constitution.
2. [`docs/harness-concurrency-methodology.md`](docs/harness-concurrency-methodology.md) - the multi-harness concurrency extension for many AI coding sessions driving one repository.

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
- Stable outer-harness session identity `(harness, factory_id)` for deterministic submission dedupe.
- Many concurrent harnesses per repository, while the same issue x target Cell retains one current writer/epoch.
- Admission and backpressure as pressure control, never as a hidden second scheduler.
- Fail-closed policy/evidence semantics, durable recovery, cleanup reconciliation, explicit
  publication/promotion separation, bounded factory generations, and exact-head CI/merge fencing.

For the full flow, rationale, diagrams, anti-patterns, review checklists, concurrency scenarios, and implementation maps, read both documents above.
