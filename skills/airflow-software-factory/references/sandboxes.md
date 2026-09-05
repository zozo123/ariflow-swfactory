# Sandbox boundaries and providers

Read this reference before selecting or implementing a sandbox.

## First choose the boundary

| Boundary | What runs inside | Use it for | Main risk |
|---|---|---|---|
| command/tool | shell and file operations only | analysis tools and generated scripts | the agent loop and its other tools still run on the worker |
| agent cell | agent loop, checkout, tools, and tests | untrusted coding work | the cell needs narrowly scoped model access |
| whole task | Airflow task and everything it starts | maximum worker isolation | heavier scheduling and state handoff |

`swfactory` expects an **agent cell**. Airflow's `SandboxToolset` is a **command/tool** boundary.
They share create, execute, file, and destroy primitives, but they are not security-equivalent.

## Provider fit

| Provider | Shape | Strong fit | Integration note |
|---|---|---|---|
| local | host directory | scripted replay only | never use for a real agent without the explicit dev override |
| Anthropic Sandbox Runtime | host process with OS confinement | keyed development box | enforce protected writes and a destination allowlist |
| Docker | shared-kernel container | local stack and reproducible builds | Docker socket access is host-root-equivalent; not a MicroVM |
| islo | remote MicroVM with stable name and TTL | production agent cell | keep GitHub delivery credentials in the orchestrator |
| Docker Sandboxes `sbx` | local MicroVM backend | local Airflow `SandboxBackend` work | declare the host network policy and sweep orphans |
| Daytona | hosted stateful sandbox | fast persistent agent cells | use SDK lifecycle, TTL, network controls, and idempotent identity |
| E2B | hosted cloud sandbox | ephemeral tool or agent cells | set secure mode, bounded timeout, and explicit internet policy |
| Tensorlake | hosted MicroVM sandbox | persistent cells and scalable verification | use named sandboxes, termination/timeout, and explicit egress rules |
| Box by ASCII | persistent full Linux VM | repositories needing Docker, desktop, or long-lived state | use `no_env` unless account secrets are intentionally required |

The repository ships direct `local`, `srt`, `docker`, and `islo` runtimes. Its `toolset` seam ships
the Apache Airflow `sbx` backend and accepts a custom backend as
`SWF_TOOLSET_BACKEND=package.module:Class`. Daytona, E2B, Tensorlake, and Box by ASCII should enter
through that adapter seam only after the adapter proves every requested policy field. Do not claim
a provider is supported merely because its SDK can execute a command.

## Adapter contract

A backend must provide:

```python
class SandboxBackend:
    def create(self, *, spec=None) -> str: ...
    def run_command(self, sandbox, command, *, timeout, max_output_bytes): ...
    def read_file(self, sandbox, path, *, max_bytes) -> bytes: ...
    def write_file(self, sandbox, path, content) -> None: ...
    def destroy(self, sandbox) -> None: ...
```

Before wiring it into a live factory, prove all of these:

1. Creation is idempotent or recoverable after a lost response.
2. A later Airflow task can reconnect to the same named sandbox or checkpoint.
3. Command timeouts kill the full process tree and output is bounded per stream.
4. File paths cannot escape the repository root through `..`, absolute paths, or symlinks.
5. Requested egress policy is enforced, not ignored.
6. No host environment crosses by default; credentials are explicit and minimal.
7. Destroy is idempotent and a server-side TTL handles worker death.
8. The checkout is provisioned at the expected branch and the target directory exists.
9. Delivery credentials never enter the sandbox.
10. Provider errors distinguish retryable transport failures from terminal policy failures.

If a capability is missing, reject the configuration. Never weaken `SandboxSpec` to make creation
succeed.

## Official references

- Apache Airflow SandboxToolset: https://airflow.apache.org/docs/apache-airflow-providers-common-ai/stable/toolsets.html
- Docker Sandboxes backend: https://airflow.apache.org/docs/apache-airflow-providers-common-ai/stable/_api/airflow/providers/common/ai/sandbox/sbx/index.html
- Daytona: https://www.daytona.io/docs/en/
- E2B: https://e2b.dev/docs
- Tensorlake: https://docs.tensorlake.ai/sandboxes/introduction
- Box by ASCII: https://docs.ascii.dev/box
