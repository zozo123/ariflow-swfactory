# Runtime ownership map

This file is the entropy-collapse contract for the stable factory. Parallel implementation may fan
out aggressively, but each durable state machine and mutation has exactly one owner at fan-in.

| Boundary | Sole authority | Primary implementation | Forbidden duplication |
| --- | --- | --- | --- |
| Lifecycle scheduling | Apache Airflow | `dags/` composition roots | no second scheduler in backend/CLI/workgraph |
| Factory Cell identity and epoch | domain/control kernel | `cells.py`, `control_kernel.py`, authority helpers | no adapter-local ownership counters |
| Admission and durable work-order state | application/backend | `durable_admission.py`, backend submit path | no direct Airflow intake that bypasses admission |
| External mutation serialization | operation journal | `idempotency.py`, control kernel mutation path | no adapter-local retry loops that can double-commit |
| Accepted issue/plan/policy identity | application intake | governed intake snapshot contract | no per-task re-resolution that changes one epoch |
| Publication credentials | backend SCM adapter | `backend/scm_service.py` | inner sandboxes never receive promotion authority |
| Build-stage execution | application stage registry | managed stage implementation selected by Airflow | scripted replay stays test-only, not a second product path |
| Sandbox lifecycle | sandbox adapter + cleanup receipts | `sandbox.py` plus ownership/cleanup contracts | no age/name-only destructive cleanup |
| Recovery projection | operation journal state | backend recovery/read model | no second state vocabulary or independent retry budget |
| Evidence and candidate readiness | evidence writer/fan-in | candidate/evidence contracts + CI producers | no success strings without producer evidence |
| Release provenance | release dependency graph | release workflow over exact candidate artifacts | no detached release-event workflow as sole provenance path |
| Operator transport | `swf-app` | shared `BackendContext` and application operations | CLI/TUI may render differently but not reconnect differently |
| Advisory/research models | research/catalog | declarative/experimental modules | never own mutation, scheduling or promotion |

## Dependency direction

`domain -> application -> adapters/interfaces` is the operational direction. Storage implements
domain/application ports and owns atomic persistence semantics. `dags/` are composition roots. The
five Rust crates retain the same rule: `swf-domain` is pure, `swf-adapters` owns I/O, `swf-app` owns
use cases, and CLI/TUI are renderers over the application layer.

## Fan-out / fan-in rule

During exploration, workers may create temporary adapters, branches and alternative algorithms.
Before merge to `main`, stabilization must choose one authority for every row above, migrate callers,
and delete or explicitly mark experimental any superseded implementation. A compatibility shim may
exist only with a named removal path; it cannot become a second durable authority.

## Change checklist

Every boundary-changing PR must state: owner being changed, callers migrated, persisted/wire
compatibility, rollback strategy, duplicate code deleted or retained intentionally, and the test or
evidence proving that the stable authority remains singular.
