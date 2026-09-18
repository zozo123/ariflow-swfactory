# ADR: Rust-first factory runtime on Airflow

Status: proposed migration architecture

## Decision

The software factory becomes **Rust-first end to end** while Apache Airflow remains the sole
lifecycle scheduler and human-in-the-loop authority.

The outer harness entrypoint is the native `swf` binary. Python is retained only where Airflow
currently requires Python to register DAG topology or bootstrap a language coordinator.

```text
Codex / Claude / Cursor / CI / human
                |
                v
          swf (Rust CLI)
                |
                v
      Factory API / work order
                |
                v
       Airflow REST API v2
                |
                v
  Airflow scheduler + HITL + retries
                |
       +--------+---------+
       |                  |
 phase 1             phase 2
       |                  |
 thin Python          Rust Airflow
 task shim            coordinator
       |                  |
       +--------+---------+
                |
                v
        swf task-runtime
             (Rust)
                |
                v
 domain / app / adapters / evidence
             (Rust)
```

## Non-negotiable ownership

| Concern | Owner |
| --- | --- |
| Harness entrypoint | Rust `swf` CLI |
| Admission/work-order client | Rust |
| Domain identities/state/policy | Rust |
| Stage application logic | Rust |
| Agent/sandbox/provider adapters | Rust |
| Evidence/replay/candidate fan-in | Rust |
| Operator CLI/TUI | Rust |
| Lifecycle scheduling | Airflow |
| Retries / task mapping | Airflow |
| HITL presentation + responder identity | Airflow |
| DAG registration during migration | minimal Python |
| Publication authority | backend-managed, singular |

Python must not retain an alternative implementation of a migrated state machine.

## Why not rewrite Airflow

Airflow 3 already provides the boundary we need:

- stable REST API for external management clients;
- a Task Execution API separating workers from scheduler/database internals;
- language-specific task execution through `BaseCoordinator`;
- a runtime protocol for non-Python workers using a communication channel and task-scoped
  operations such as Variable, Connection and XCom access.

The factory should implement *against* those boundaries, not duplicate the scheduler.

## Phase 0: native harness entry

Every outer harness executes `swf submit --harness ... --factory-id ...`.
The shell wrapper is compatibility only and may be deleted once callers move.

No outer harness talks to Airflow directly.

## Phase 1: Rust stage executable

Airflow retains the DAG graph but stage bodies become tiny launchers of the native runtime:

```text
fan_out
  -> swf task setup
  -> swf task intent
  -> Airflow HITL
  -> swf task record-gate
  -> swf task plan
  -> Airflow HITL
  -> swf task build-and-test
  -> swf task review
  -> swf task deliver
  -> swf task metrics
  -> swf task teardown
```

The command receives only stable identifiers:

- DAG id;
- DAG run id;
- task id / stage;
- mapped job index;
- Factory Cell id + epoch when managed.

It resolves the rest from the backend. Large payloads are never encoded into argv.

## Phase 2: Airflow Rust coordinator

Implement an Airflow Task SDK coordinator for Rust.

The Python side is only the coordinator plugin that starts the Rust runtime and gives it the
communication/log addresses. The Rust process:

1. accepts `--comm=host:port` and `--logs=host:port`;
2. connects to both channels;
3. reads Airflow startup details;
4. resolves the Rust task by DAG/task id;
5. executes the task;
6. proxies Variable / Connection / XCom requests over the Task SDK protocol;
7. sends the terminal task state and exits.

This removes Python stage wrappers without changing Airflow core.

## Phase 3: delete Python product runtime

Delete Python implementations only after Rust contract/e2e equivalence is green. Keep only:

- DAG registration/composition required by Airflow;
- the Rust coordinator bootstrap;
- compatibility fixtures needed for migration verification.

The target is not "two equivalent runtimes." The target is **one Rust runtime with an Airflow
scheduler boundary**.

## Transport rules

### Operator/client side

`swf` uses the Airflow stable REST API or the factory backend API. It never reads the metadata DB.

### Task side

Prefer the Airflow task-runtime protocol through the Rust coordinator. During phase 1, the Rust
stage process talks to the factory backend using a task-scoped identity minted by the orchestrator.

### Secrets

No GitHub publication credential enters the stage sandbox. Airflow/task credentials are scoped to
the running task. Publication stays behind the managed backend.

## Migration rule

A component moves to Rust only when all four exist:

1. Rust implementation;
2. contract-equivalence test against retained fixtures;
3. live Airflow acceptance path;
4. deletion or permanent deactivation of the Python authority it replaces.

Porting code without deleting duplicate authority is not migration.

## Desired end state

```text
swf CLI/TUI (Rust)
   |
Factory application/runtime (Rust)
   |
Airflow public interfaces
   |
Airflow scheduler / HITL / retries / mapping
   |
Rust task coordinator/runtime
   |
Rust sandboxes, agents, evidence, delivery
```

Airflow schedules. Rust reasons and executes.
