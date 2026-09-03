# Security policy

## Supported versions

swfactory is pre-1.0. Security fixes are applied to the latest commit on `main` and released in the
next version. Older commits and development branches are not supported.

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
