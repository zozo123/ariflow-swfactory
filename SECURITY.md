# Security policy

## Supported versions

Security fixes are applied to the latest tagged release and the latest commit on `main`, then
shipped in the next release. Older releases, commits, and development branches are not supported.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting for this repository:

1. Open the repository's **Security** tab.
2. Choose **Report a vulnerability**.
3. Include the affected revision, impact, reproduction steps, and any suggested mitigation.

Please do not disclose the issue publicly until a fix is available. You can expect an initial
acknowledgement within seven days. If private reporting is unavailable, contact the repository owner
through their GitHub profile without including exploit details in a public issue.

## Security model

swfactory treats agent-generated code as untrusted. The production design separates the agent
sandbox from the orchestrator that holds source-control credentials, validates patch paths, scans
for secret-shaped values, and alone performs delivery. See the trust-boundary diagram and sandbox
comparison in [README.md](README.md) before operating the real-agent path.

## Credential authority

Search inside a disposable work cell may be high-entropy; credential authority is the opposite:
explicit, short-lived, fenced and auditable. Managed publication credentials stay in the trusted
backend, which mints one-shot opaque leases bound to exactly:

```text
(factory_run_id, dag_run_id, task_instance_id, stage_id, sandbox_id,
 attempt_number, cell_id, epoch, operation_key, policy_digest)
```

The broker persists only a hash of the handle and the binding metadata. The raw provider token is
materialized only inside the trusted SCM process for the scoped operation, never in a coding-cell
environment, XCom, run artifact, receipt or log. Capabilities are deny-by-default and
operation-shaped (`github.publish`, `github.read`); broad audiences such as `github` or `*` are
invalid.

TTL only limits how long an unused lease can exist; **revocation is authority removal**. Cell
epoch changes and terminal transitions revoke outstanding leases synchronously, and redeem
re-reads the durable current epoch, so a stale handle fails even if a revocation projection was
interrupted. An Airflow retry is a new execution identity: attempt N+1 mints its own lease and
cannot redeem attempt N's handle. A one-shot operation revokes its lease as soon as the trusted SCM
callback completes. Denied redeems are durable evidence: the broker records capability, purpose,
binding digest, run/Cell/epoch/attempt identity, reason and time, never a bearer or provider token,
and the backend projects them into the Cell evidence chain and serves metadata-only
`/v1/leases/denials` and `/v1/leases/inspect` reads.

Coding-cell environments are allow-listed, so a new ambient credential name fails closed rather
than relying on a secret-name denylist. Public execution recipes cannot request `secret_env`;
production model credentials are projected by the sandbox gateway, not copied from the
orchestrator environment.

Airflow XCom is scheduler scratch, not authority:

| Datum | XCom | Source of truth |
| --- | --- | --- |
| issue/target routing, Cell id + epoch | allowed | backend work order + CellStore |
| bounded stage preview / stage result | allowed, untrusted | run artifact store |
| HITL approve/reject response | allowed as an untrusted response | sealed `approvals.json` + Cell/inputs/artifact checks |
| policy digest / generation seal | **never** | CellStore |
| credential lease / handle / redeem material | **never** | credential broker |
| raw patch bytes | **never** | sandbox/worktree then BackendScm request |
| GitHub/backend bearer tokens | **never** | trusted backend/gateway |

`swfactory.xcom_contract.validate_xcom_document` enforces the forbidden fields. Managed mapped jobs
carry only Cell identity and epoch, and every task re-reads the current policy from the backend
before creating its runtime context. A HITL answer pulled from XCom is input, not a seal:
`record_approval` binds it to the current Cell epoch, accepted-input digest and exact artifact
digest, and `deliver` re-validates that sealed record before any external effect, so corrupted
scheduler scratch cannot substitute a policy digest or a credential capability.
