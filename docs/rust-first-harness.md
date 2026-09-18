# Rust-first harness with Airflow lifecycle binding

The target architecture is deliberately asymmetric:

- **Rust owns the harness, control-plane application logic, execution contracts, recovery, evidence, CLI/TUI and provider adapters.**
- **Apache Airflow remains the lifecycle scheduler.**
- **Python is reduced to thin DAG/composition glue while migration is in progress.**
- **The operator entrypoint is `swf`, the Rust binary.**

Airflow must not become the factory implementation, and Rust must not become a second scheduler.

## Process boundary

```text
operator / agent harness
        |
        v
      swf CLI  (Rust)
        |
        +--------------------+
        |                    |
        v                    v
 Rust factory manager     Airflow /api/v2
 (application authority)  (lifecycle authority)
        ^                    |
        |                    |
        +--- stage invoke ----+
             versioned API
```

The existing `swf-domain::manager_protocol` is the wire contract. It already carries:

- factory/run identity;
- Factory Cell id and epoch;
- stage and attempt;
- Airflow dag/run/task/map/try identity;
- versioned request envelope;
- explicit stage disposition and evidence digest.

That contract is the binding between Airflow scheduling and Rust execution.

## Desired runtime

### 1. Rust CLI is the harness entrypoint

Every human, agent, automation harness or external tool begins with `swf`.

Examples:

```sh
swf submit --issue 123
swf run inspect <run>
swf gates list
swf operations inspect <operation>
swf doctor
```

The CLI delegates use cases to `swf-app`; it must not contain scheduling or provider policy.

### 2. Airflow owns lifecycle scheduling only

Airflow owns:

- DAG/run/task lifecycle;
- retries and mapped lifecycle tasks;
- human wait points;
- concurrency/pools;
- cron/event scheduling.

Airflow does **not** own:

- factory business logic;
- operation journals;
- provider credentials;
- publication policy;
- recovery semantics;
- candidate ranking;
- sandbox authority.

### 3. Airflow invokes Rust through one manager API

A DAG task converts Airflow runtime context into
`ManagerEnvelope<StageInvocation>` and sends it to the Rust manager.

Preferred transports:

1. HTTP over loopback/service network for ordinary deployment.
2. Unix-domain socket for colocated deployments.

Both carry exactly the same serialized contract.

Conceptual endpoint:

```text
POST /v1/manager/stages/execute
Content-Type: application/json

ManagerEnvelope<StageInvocation>
```

Response:

```text
StageReceipt
```

The response disposition determines the Airflow task result:

- `completed` -> task success;
- `waiting_approval` -> Airflow/HITL wait;
- `retryable` -> retry according to Airflow policy;
- `failed` -> task failure;
- `in_doubt` -> stop automatic replay and surface recovery debt.

Airflow never reconstructs those semantics itself.

## Rust crate ownership

```text
swf-domain
  identities, immutable contracts, state vocabulary, manager protocol

swf-app
  use cases, admission, stage execution, recovery, evidence, publication decisions

swf-adapters
  Airflow REST, GitHub, sandbox/provider I/O, manager transport

swf-cli
  process entrypoint / harness surface

swf-tui
  alternate renderer over the same application layer
```

No Python module is allowed to become a second implementation of a Rust-owned state machine after
that state machine is migrated.

## Migration rule

Do not rewrite everything in one flag day.

For each slice:

1. freeze the current external contract;
2. implement it in Rust;
3. run Python and Rust against the same fixtures/evidence;
4. switch the Airflow stage shim to call the Rust manager;
5. remove the Python implementation;
6. keep Airflow scheduling unchanged.

The migration unit is a **use case**, not a file.

Suggested order:

1. immutable domain/contracts;
2. stage registry and execution envelope;
3. operation journal/recovery;
4. evidence/candidate fan-in;
5. sandbox/provider adapters;
6. publication;
7. backend/control-plane HTTP surface;
8. delete Python runtime implementations, leaving DAG composition only.

## Non-negotiable invariants

- There is exactly one lifecycle scheduler: Airflow.
- There is exactly one factory application authority: Rust.
- The Rust CLI is the public harness entrypoint.
- Airflow talks to Rust through a versioned protocol, not imports or the Airflow metadata DB.
- Python DAG code may compose tasks but cannot implement factory state machines.
- All external mutations remain bound to Cell + epoch + operation identity.
- A stage receipt is evidence-bearing and replay-safe; an `in_doubt` receipt cannot be converted into an automatic retry by Python.
- Final candidate promotion remains deterministic and human/policy gated.

## End state

The intended end state is not “Rust replaces Airflow.”

It is:

> **Rust is the factory. Airflow is the scheduler. `swf` is the harness. The manager protocol is the binding.**
