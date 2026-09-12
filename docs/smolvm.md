# SmolVM sandbox backend (experimental)

The factory can use [SmolVM](https://github.com/smol-machines/smolvm) through its existing
`ToolsetSandbox` adapter. `SmolvmSandboxBackend` implements the same method/result contract as
Airflow common.ai's `SandboxBackend` without importing Airflow during module loading. Airflow
continues to schedule stages; the factory retains approval, evidence and publication authority.

This is a basic execution integration. Live branching, snapshots, pause/resume and native TTL
are not advertised. The supported work-stage path remains serial.

## Configure a worker

Start with a dedicated Linux/KVM host (`/dev/kvm`) and an operator-installed SmolVM version.
The implementation was checked against upstream commit
`a311ec2be7d2fc18ee1606476f675e0294f20975`. Pin and qualify your runtime and image before use.
No runtime download or installation happens while parsing a DAG.

Run `smolvm serve start --listen /run/smolvm/api.sock` as the worker's dedicated host user,
after provisioning that user-owned directory. Restrict its directory/socket permissions to
that user. The backend opens HTTP over this Unix socket. It does not support remote HTTP or
mTLS in this first integration. Do not mount the socket into a coding guest.
Every task for a run must reach the same daemon and host-owned journal: use a dedicated worker
host/queue. A socket pathname alone is not a fleet-wide daemon identity; transparent host failover
is not implemented. The socket, image and resource settings are pinned in accepted-input policy.
Upgrading adds policy fields, so existing accepted epochs may require deliberate reacceptance or
a new epoch through the normal recovery procedure.

```sh
export SWF_TOOLSET_BACKEND=smolvm
export SWF_TOOLSET_SMOLVM_SOCKET=/run/smolvm/api.sock
export SWF_TOOLSET_SMOLVM_IMAGE=ghcr.io/zozo123/swfactory-sandbox:latest
export SWF_TOOLSET_SMOLVM_CPUS=2
export SWF_TOOLSET_SMOLVM_MEMORY_MB=2048

uv run --no-sync swfactory doctor --blueprint your-product --sandbox toolset --agent claude --scm github
```

Use an immutable image digest for a reproducible deployment. The default factory image provides
Git, Python, uv, Claude Code, bash and GNU coreutils. File helpers need bash `pipefail`, `head`,
`base64` and `find`. Prewarm images on the host before imposing guest egress restrictions.
`doctor` checks that the adapter loads; the live tests below check an actual runtime.

Set the blueprint's existing sandbox selection to `toolset` for managed Airflow runs. A direct
CLI rehearsal can use `swfactory run --blueprint your-product --issue 42 --sandbox toolset
--agent claude --scm github --approve prompt` after configuring the normal model/SCM environment.
It can spend model budget and publish a PR; use the normal approval gates.

For direct Python use, construct `SmolvmSandboxBackend(socket_path=..., state=RunState(run_dir))`
and pass it to `ToolsetSandbox` or Airflow's `SandboxToolset`. Construction performs no I/O.
Persist the returned handle and serialize mutations. Factory-managed construction automatically
binds a stable machine name to repository, target, issue and run ID and provides its run journal.

## Execution and network semantics

- Only explicitly supplied model environment values enter the guest. The factory wiring supplies
  `ANTHROPIC_API_KEY` for Claude; backend and GitHub credentials remain outside. Host journals
  store a provisioning digest, not environment values. Environment values are visible to guest
  code and to the trusted SmolVM service; do not put publication authority there.
- No host mounts, published ports, GPU or CUDA capabilities are requested.
- A blocked network with no exceptions maps to `network=false`. Hostname exceptions map to
  `network=true`, `allowedHosts`, an empty `allowedCidrs`, and the `virtio-net` backend. Returned
  isolation settings are checked. SmolVM hostname exceptions also allow subdomains; they are
  domain scopes, not exact-host or URL-path restrictions. The normal toolset path supplies its
  existing model/package/GitHub domain list. Enforcement still needs qualification on your host.
- Commands use `/bin/sh -c` with `timeoutSecs`, plus an adapter wall-clock deadline. The adapter
  consumes SSE incrementally, bounds each event, and retains at most the caller's limit per
  stream. Provider errors, malformed events and missing exit events fail the attempt.
- Exit 124 is conservatively treated as timeout, even if user code deliberately returned 124:
  upstream does not supply an unambiguous timeout flag. Timeout or an ambiguous command destroys
  the VM; a terminated attempt cannot continue with an empty replacement.
- Native file downloads buffer the file on the server, so bounded reads run a capped `head`
  inside the same filesystem as exec. Uploads use the native API. File operations are limited
  to 8 MiB; traversal paths are refused. UTF-8 command output is textual; binary files use base64.

## Recovery and cleanup

The host records `state/smolvm/<machine-name>.json` **before** provisioning. It records the
socket, intended configuration, phase and PID, and marks operations in flight before execution.
On restart it observes an existing machine instead of blindly repeating creation. Changed PIDs,
missing machines and interrupted commands cannot silently resume completed stage journals.
The observation and exec request are separate API calls; upstream can auto-start between them.
This remains experimental until that runtime race is addressed and qualified.

A failed DELETE leaves cleanup debt and fences further work. Cleanup succeeds only after GET
confirms absence. A pending create with a missing machine stays in doubt: a delayed server
operation could still complete. A create name collision is neither adopted nor deleted.

Ordinary SmolVM machines expose no native TTL. A worker killed outright can leave a VM running.
Retain the journal, reconcile interrupted runs, and explicitly reclaim the exact recorded handle.
For example, after stopping the run and confirming no legitimate work should continue:

```python
import json
from pathlib import Path
from swfactory.state import RunState
from swfactory.smolvm_backend import SmolvmSandboxBackend

state = RunState(Path(".factory/REPLACE_WITH_RUN_ID"))
with state.exclusive("smolvm-cleanup"):
    for path in (state.root / "smolvm").glob("*.json"):
        record = json.loads(path.read_text())
        backend = SmolvmSandboxBackend(socket_path=record["socket"], state=state)
        backend.destroy(record["name"])
```

This deliberately refuses a colliding identity or an absent pending create. Inspect the SmolVM
service before resolving those cases; do not delete the journal to make the refusal disappear.
The Unix socket is a trusted host control surface, not a multi-tenant authorization boundary.

## Verification

```sh
uv run --no-sync pytest tests/test_smolvm_backend.py
SWF_TEST_LIVE_SMOLVM=1 uv run --no-sync pytest tests/test_smolvm_live.py
```

Hermetic tests cover the HTTP/SSE contract, bounded output, trickling streams, ambiguous creation,
restart fencing, isolated credential maps and failed cleanup. They use both in-memory HTTP byte
streams and real Unix sockets; socket cases skip only when the runner forbids socket creation.
The explicit live tests create real VMs, exercise file/exec persistence and reconnection, probe
network denial, verify timeout deletion, and raise if cleanup cannot be confirmed. They need no
model key and publish nothing. Passing hermetic tests is not evidence of hypervisor isolation.

## Branching follow-up

After qualifying ordinary execution, add a separate provider-fork integration. Prepare a parent
at the wave's exact Git SHA, finish dependency setup, and park at the branch-ready boundary before
launching agents or injecting their credentials. Keep checkouts on guest-owned disks. Record
Cell/epoch/node/attempt/parent/child/input SHA outside the guest; bind cancellation to actual VM
termination. Merge patches in stable order, reverify the combined candidate, then prepare the
next wave from that new SHA. The current `WorkExecutor` callback does not directly cancel an
in-flight runner; enabling `fork=True` alone would not implement this contract.
