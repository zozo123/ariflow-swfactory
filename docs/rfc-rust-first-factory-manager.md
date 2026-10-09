# RFC: Rust-first factory manager, Airflow as scheduler

Status: proposed.

Today `swf` is an operator client of the Python backend, Airflow runs Python stages, and Rust
holds the operator CLI/TUI plus the shared domain contracts; the [system map](system-map.md)
records current ownership. An earlier Rust scaffold for this design (the `swf-domain` `factory`,
`manager_protocol` and `worker` types and the `swf-app` `stage_runtime`) had no caller and was
removed in #2367. Of the `swf factory` commands below, `run` and `status` exist *today*: `run`
delegates to the `swf submit` path, and `status` inspects one `dag/run`. Unless a line says
*today*, every other type, endpoint and command below is a proposal.

## Product principle

The public product is **`swf`**.

A human, Codex, Claude Code, Cursor, CI, or another harness should need exactly one stable interface:

```text
swf factory run ...
swf factory status ...
swf factory watch ...
swf factory stop ...
swf factory replay ...
swf factory serve
```

The CLI is not a thin collection of scripts. It is the primary product surface and the manager of
factory application state. The same Rust application layer backs CLI, TUI, API and Airflow callbacks.

The intended developer experience is closer to Pydantic/FastAPI than to a pile of operators:

- strong typed models at every boundary;
- one obvious composition path;
- excellent errors;
- JSON is a first-class stable protocol;
- local use is simple;
- production use adds adapters, not different semantics;
- escape hatches are explicit and never silently broaden authority.

## Architectural inversion

Today the normal path is roughly:

```text
harness -> swf Rust client -> Python backend -> Airflow -> Python stages -> sandbox
```

Target:

```text
                          +----------------------+
harness/human ----------> | swf CLI / TUI        |
                          |                      |
Airflow task -----------> | Rust FactoryManager  | <----> GitHub / sandboxes / evidence
                          | application runtime  |
                          +----------+-----------+
                                     |
                                     | Airflow REST API v2
                                     v
                               Apache Airflow
                             lifecycle scheduler
```

Airflow remains the sole lifecycle scheduler. Rust owns factory semantics.

That distinction is strict:

**Airflow owns**
- DAG scheduling;
- retries and deferrals;
- mapped task lifecycle;
- timers;
- HITL waiting points;
- task concurrency.

**Rust owns**
- work-order validation;
- admission policy;
- Factory Cell identity and epochs;
- stage contracts;
- sandbox execution;
- external-effect idempotency;
- recovery/reconciliation;
- evidence;
- publication policy;
- operator read models;
- deterministic fan-in.

Airflow does not own business state. Rust does not invent another scheduler.

The end state in one line: **Rust is the factory. Airflow is the scheduler. `swf` is the harness.
The manager protocol is the binding.**

## One Rust application core

Today's workspace has five crates: `swf-domain`, `swf-app`, `swf-adapters`, `swf-cli` and
`swf-tui`. `swf-api` and `swf-runtime` are proposed. The dependency direction becomes:

```text
swf-cli / swf-tui / swf-api
          |
        swf-app
          |
      swf-runtime
      /    |    \
 domain  stores  adapters
                 |
         airflow github sandbox
```

### swf-domain

Pure versioned types only.

Proposed types:
- `FactorySpec`
- `FactoryId`
- `FactoryRunRequest`
- `FactoryRunId`
- `FactoryRunStatus`
- `CellId` / `CellEpoch`
- `StageSpec`
- `OperationReceipt`
- `EvidenceDigest`

No network, filesystem, Tokio runtime, Airflow types, GitHub types, or CLI flags.

### swf-app

Use cases.

Examples:
- create/load factory;
- validate run request;
- admit run;
- start run;
- inspect run;
- approve gate;
- reconcile interruption;
- replay evidence;
- publish candidate.

The CLI and TUI call these operations directly. The HTTP API exposes them. Airflow callbacks call
the same operations. No semantic rule exists in a renderer or DAG.

### swf-runtime

Long-lived manager composition.

It owns:
- store handles;
- adapter construction;
- background reconciliation;
- API listener;
- shutdown;
- one process-scoped manager identity.

The runtime can be embedded in `swf factory serve` or started as a service.

## Smart Airflow binding

Do **not** bind Rust to Airflow internals, metadata tables, Python objects or provider implementation
details. Bind only to Airflow's public REST API and an intentionally tiny callback protocol.

There are two directions.

### Rust -> Airflow

The manager starts or observes lifecycle work using Airflow REST v2.

```text
FactoryManager::start(request)
  -> validate/admit in Rust
  -> bind FactoryRunId to dag_id + dag_run_id
  -> POST Airflow /api/v2/dags/{dag}/dagRuns
  -> persist the binding
```

The binding is data, not authority.

### Airflow -> Rust

A DAG task does not import factory business logic. It calls the Rust manager.

Preferred production path:

```text
Airflow task -> HTTP/Unix socket -> swf manager API -> swf-app use case
```

Conceptual endpoint:

```text
POST /v1/manager/stages/execute
StageInvocation -> StageReceipt
```

The invocation is a versioned envelope carrying the factory/run identity, the Factory Cell id and
epoch, the stage and attempt, and the Airflow dag/run/task/map/try identity. The manager validates
that identity, executes the use case and returns a receipt with an explicit disposition and
evidence digest. The disposition alone decides the Airflow task result:

| Disposition | Airflow result |
| --- | --- |
| `completed` | task success |
| `waiting_approval` | human-in-the-loop wait |
| `retryable` | retry under Airflow policy |
| `failed` | task failure |
| `in_doubt` | stop automatic replay and surface recovery debt |

Airflow never reconstructs those semantics itself.

The HTTP path is preferred because it gives:
- cancellation/deadline propagation;
- typed JSON contracts;
- one process owning stores and credentials;
- no shell quoting surface;
- no Python/Rust semantic duplication.

Unix-domain sockets may be used on a single host. TCP + authenticated HTTPS is used across hosts.

Acceptable local/transition path:

```text
Airflow BashOperator -> swf internal stage execute --run ... --stage ...
```

## Python end state

Python is not deleted on day one.

Python shrinks to:
- Airflow DAG declarations;
- very small Airflow operators/hooks/sensors where native Airflow Python integration is useful;
- compatibility fixtures during migration.

Python Airflow code should be boring: build the invocation from task context, call the manager, and
map the returned disposition to Airflow behavior. It must not contain admission policy, stage
implementations, publication credentials, retry-safety logic, Cell epoch decisions, evidence
fan-in, or provider-specific sandbox semantics, and it must not retain an independent
implementation of recovery, publication, stage selection or operation identity.

A Python module that contains factory semantics is migration debt.

## CLI as manager

The CLI gets a first-class `factory` namespace.

```text
swf factory init
swf factory list
swf factory show NAME
swf factory run NAME --issue 42 --target owner/repo
swf factory status RUN
swf factory watch RUN
swf factory stop RUN
swf factory replay RUN
swf factory serve
```

*Today* `swf factory run` and `swf factory status` exist: `run` resolves NAME to the installed
blueprint of that name and delegates to the submit path, and `status` takes the `dag/run`
identity until FactoryRunId lookup lands. `swf submit` remains a compatibility alias for
`swf factory run` during migration.

### Harness contract

Outer harnesses talk only to `swf`.

```text
swf factory run default \
  --harness codex \
  --factory-id session-123 \
  --issue 42 \
  --json
```

The stable JSON response returns:
- factory id;
- logical run id;
- Airflow binding;
- admitted Cell identities;
- evidence/status URLs or local handles.

A harness never needs to know Python package names or DAG internals. Today the same session
identity travels as `swf factory run --harness <h> --factory-id <id>` (or its `swf submit`
alias), and its JSON is today's submission document, not yet the fields above.

## Factory as a typed object

A factory is a persisted declaration, not just a DAG name.

```rust
FactorySpec {
    name,
    line,
    targets,
    stages,
    gates,
    limits,
    sandbox_policy,
    evidence_policy,
    publication_policy,
}
```

The declaration can be loaded from TOML/YAML/JSON, but once parsed it becomes one Rust type.

Think Pydantic model semantics:
- validation is immediate and exhaustive;
- unknown/invalid fields fail early;
- serialization is canonical;
- schemas are versioned;
- errors identify exact fields and fixes.

## Run state

One logical run exists independently of scheduler transport.

```text
FactoryRunId
  -> admitted
  -> scheduled(AirflowBinding)
  -> running
  -> waiting_approval
  -> verifying
  -> delivered | failed | cancelled | in_doubt
```

Airflow state is an observation used to reconcile this state machine, never the canonical identity.

## Migration

Do not rewrite everything in one flag day. The migration unit is a **use case**, not a file. For
each slice:

1. freeze the current external contract;
2. implement it in Rust;
3. run Python and Rust against the same fixtures and evidence;
4. switch the Airflow stage shim to call the Rust manager;
5. remove the Python implementation;
6. keep Airflow scheduling unchanged.

Suggested order:

1. Rust factory/run contracts and the `swf factory` namespace, over the current backend;
2. reads and validation: FactorySpec, work orders, admission decisions, Cell identity, operator
   projections;
3. stage registry and the execution envelope;
4. operation journal and recovery;
5. evidence and candidate fan-in;
6. sandbox and provider adapters;
7. publication;
8. the backend HTTP surface (`swf factory serve`), leaving Python with DAG composition only.

Every mutation needs restart, crash and lost-response tests before its Python authority is deleted.
Delete a Python implementation only when current-main E2E is green, Airflow scheduling is intact,
restart/recovery tests are retained, API/CLI compatibility is documented, and no second scheduler
has appeared.

## Invariants

- There is exactly one lifecycle scheduler: Airflow.
- After a slice migrates, there is exactly one application authority for it: Rust.
- The Rust CLI is the public harness entrypoint.
- Airflow talks to Rust through a versioned protocol, not through imports or the Airflow metadata
  DB.
- Python DAG code may compose tasks but cannot implement factory state machines.
- Every external mutation stays bound to Cell + epoch + operation identity.
- A stage receipt is evidence-bearing and replay-safe; Python cannot turn an `in_doubt` receipt
  into an automatic retry.
- Final candidate promotion stays deterministic and human/policy gated.

## Non-goals

- Rewriting Airflow.
- Reimplementing the Airflow scheduler in Rust.
- Reading Airflow's metadata DB directly.
- Requiring a daemon for purely local inspection commands.
- Embedding Python in the Rust process.
- Letting the CLI directly mutate GitHub or sandboxes when manager mode is configured.

## Success criteria

The migration is complete when:

1. a machine with `swf` can create, run, inspect, approve, recover and verify a factory;
2. Airflow is still visibly the lifecycle scheduler;
3. Python contains orchestration glue but no unique factory semantics;
4. CLI/TUI/API/Airflow callbacks share one Rust application layer;
5. a harness only needs the `swf` command and JSON contract;
6. one crash/restart test proves the manager can recover without duplicate external mutation;
7. one clean E2E run proves `swf factory run -> Airflow -> Rust stage execution -> evidence -> delivery`.
