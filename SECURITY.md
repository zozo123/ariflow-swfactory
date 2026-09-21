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

Managed publication credentials stay in the trusted backend. The backend mints one-shot opaque
credential leases bound to the exact run, task, stage, sandbox, attempt, Cell epoch, operation and
policy digest; raw provider tokens are materialized only inside the trusted SCM process and are
never written to XCom, run artifacts, receipts, or coding-cell environments. Cell epoch changes and
terminal transitions synchronously revoke outstanding leases, and redeem independently re-checks
the durable current epoch.

Coding-cell environments are allow-listed. Public execution recipes cannot request `secret_env`;
new ambient credential names fail closed rather than relying on a secret-name denylist. See
[docs/credential-authority.md](docs/credential-authority.md) and
[docs/xcom-ownership.md](docs/xcom-ownership.md).
