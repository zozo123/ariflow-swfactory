# Credential authority

The software factory deliberately allows high-entropy search inside disposable cells. Credential
authority is the opposite: explicit, short-lived, fenced, and auditable.

## Contract

A credential lease binds exactly:

```text
(factory_run_id, dag_run_id, task_instance_id, stage_id, sandbox_id,
 attempt_number, cell_id, epoch, operation_key, policy_digest)
```

The broker returns an opaque handle. It persists only a hash of that handle and binding metadata.
The raw provider credential is materialized only inside the trusted backend process for the scoped
operation. It is never a Cell environment variable, XCom value, artifact, receipt, or log field.

Capabilities are deny-by-default. Broad audiences such as `github` or `*` are invalid; callers
request an operation-shaped capability such as `github.publish` or `github.read`.

## TTL, revocation, and retry

TTL limits how long a lease can exist if nothing else changes. **Revocation is authority removal.**
It is synchronous with a Cell epoch advance and with terminal Cell transitions; redeem also reads
the current Cell epoch so a stale handle fails even if a revocation projection is interrupted.

An Airflow retry is a new execution identity. Attempt N+1 mints a new lease and cannot redeem an
attempt-N handle. A successful one-shot operation revokes its lease immediately after the trusted
SCM callback completes.

## Negative provenance

Denied redeem attempts are durable evidence, not debug noise. The broker records capability,
purpose, binding digest, run/Cell/epoch/attempt identity, reason and timestamp, but never a bearer or
raw provider token. The backend projects those denials into the Cell evidence chain and exposes
metadata-only `/v1/leases/denials` and `/v1/leases/inspect` reads.

## XCom and recipes

Airflow XCom is scheduler scratch. It may carry Cell identity/epoch and untrusted HITL responses,
but never policy seals, lease handles, patch bytes or credentials. See
[xcom-ownership.md](xcom-ownership.md).

Public execution recipes no longer accept non-empty `secret_env`. Search cells inherit an
allow-listed process environment; production model credentials are projected by the sandbox
gateway, not copied from the orchestrator environment.

**Chaos is allowed under leases; raw credentials are not part of the search state.**
