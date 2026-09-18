# RFC: Rust-first factory manager, Airflow as scheduler

Status: proposed

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
harness -> swf Rust client -> Python backend -> Airflow -> Python stages -> sandbox```

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

## One Rust application core

The dependency direction becomes:

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

Examples:
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

Acceptable local/transition path:

```text
Airflow BashOperator -> swf internal stage execute --run ... --stage ...
```

The HTTP path is preferred because it gives:
- cancellation/deadline propagation;
- typed JSON contracts;
- one process owning stores and credentials;
- no shell quoting surface;
- no Python/Rust semantic duplication.

Unix-domain sockets may be used on a single host. TCP + authenticated HTTPS is used across hosts.

## Python end state

Python is not deleted on day one.

Python shrinks to:
- Airflow DAG declarations;
- very small Airflow operators/hooks/sensors where native Airflow Python integration is useful;
- compatibility fixtures during migration.

It must not retain an independent implementation of admission, recovery, publication, stage
selection or operation identity.

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

`swf submit` remains a compatibility alias during migration.

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

A harness never needs to know Python package names or DAG internals.

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

## Migration strategy

### Phase 1: define the Rust target
- add Rust factory/run contracts;
- add `swf factory` CLI namespace;
- preserve current backend path underneath.

### Phase 2: move reads and validation
- FactorySpec parsing/validation;
- work-order validation;
- admission decisions;
- Cell identity;
- operator projections.

Each migrated contract gets Python/Rust fixture equivalence until Python is removed.

### Phase 3: move mutations
- operation journal;
- stage execution;
- sandbox ownership;
- recovery;
- publication;
- evidence retention.

Every mutation must have restart/crash/lost-response tests before Python authority is deleted.

### Phase 4: replace Python backend
- `swf factory serve` exposes the manager API;
- Airflow callbacks point to Rust;
- CLI talks to Rust manager;
- Python backend becomes compatibility-only.

### Phase 5: delete duplicate authority
Delete Python implementations only after:
- current-main E2E is green;
- Airflow scheduling remains intact;
- restart/recovery tests are retained;
- API/CLI compatibility is documented;
- no second scheduler has appeared.

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
