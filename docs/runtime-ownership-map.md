# Runtime ownership map

This map is the architectural authority for operational state machines, stores, use cases, and mutation entrypoints. It advances #2054 by naming one owner per concern without introducing another scheduler, service, or datastore.

| Concern | Sole owner | Durable state / store | Managed mutation entrypoint | Adapters / projections |
| --- | --- | --- | --- | --- |
| Cell identity, epoch and lifecycle state | domain + application Cell runtime | Cell store | backend/application Cell operations | Airflow task composition, Rust operator projections |
| Admission and dispatch | application admission runtime | durable admission store | backend admission API | Airflow trigger/fan-out, CLI/TUI reads |
| External mutation idempotency | application operation journal | operations journal | control-kernel / core-capability external-effect path | GitHub, sandbox and callback adapters |
| Recovery and reconciliation | application recovery runtime | operation journal + restore marker/evidence | recovery planner / reconciliation actions | operator status, CLI/TUI attention |
| Human approval | application approval policy | approval/evidence records | managed gate response path | Airflow HITL + replay fixture adapter |
| Stage execution | application stage registry | run state + accepted-input evidence | one stage-registry resolution path | Airflow DAG and scripted replay composition |
| Workgraph execution | application experimental executor | candidate/evidence records | bounded executor + deterministic fan-in | sandbox/provider adapters |
| Publication | application delivery/runtime authority | operation journal + publication receipt | managed delivery mutation path | GitHub adapter |
| Cleanup | application cleanup policy | cleanup receipts/debt | fenced cleanup path | Docker/islo/provider adapters |
| Scheduling | Airflow only | Airflow metadata DB | DAG/timetable | factory code supplies declarative limits only |
| HTTP/operator interface | interface layer | none beyond backend-owned stores | backend service use cases | Rust CLI/TUI and HTTP |
| Advisory/research catalogs | research layer | advisory artifacts only | no managed mutation authority | experiments/evals/reporting |

## Boundary rules

1. **Domain** owns identities, state vocabulary, immutable inputs, plans, policies and receipts. It does not call providers.
2. **Application** owns admission, stage dispatch, mutation authorization, recovery, publication and cleanup decisions. It may depend on domain contracts, never on advisory research implementations.
3. **Storage** owns transactional persistence and schema migration. Callers do not bypass stores with ad-hoc side files for authoritative state.
4. **Adapters** own Airflow, GitHub, sandbox/provider and callback I/O. Provider-specific behavior cannot become scheduling or promotion authority.
5. **Interfaces** own HTTP/CLI/TUI rendering and request translation. They do not invent state or execute factory stages.
6. **Research/catalog** code may propose or evaluate. It never becomes a publication, scheduling or mutation authority.

## Managed mutation entrypoints

Every externally visible mutation must pass through one of these application-owned seams:

- Cell/admission mutation through the backend control path.
- External effect execution through the operation journal/control kernel.
- Human-gate continuation through approval policy.
- Delivery/publication through the managed delivery path.
- Cleanup through fenced resource identity + receipt handling.
- Experimental workgraph promotion through conflict classification and deterministic fan-in.

Direct provider calls outside an adapter are architectural violations. Direct adapter calls from UI code are architectural violations.

## Compatibility and rollback

Boundary migration PRs must state whether they change:

- public CLI/HTTP behavior;
- Python/Rust import paths;
- persisted schema or serialization;
- capability-inventory claims;
- scheduler ownership;
- publication/credential ownership.

A migration should preserve persisted formats until readers and writers are migrated together. Rollback means restoring the prior projection/adapter while leaving authoritative stores readable; it must not fork a second scheduler or datastore.

## Deletion rule

Compatibility imports, duplicate stage implementations, and duplicate operation vocabularies are deleted only after all managed callers use the canonical owner and CI proves the explicit projection. Moving files without deleting the duplicate authority does not count as convergence.
