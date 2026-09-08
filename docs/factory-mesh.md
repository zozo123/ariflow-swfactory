# Factory Mesh — station-to-station coordination

A software factory becomes more interesting when **several independently operated stations point at
the same repository**. One person may run an Airflow stack in Tel Aviv, another in Boston, a third
on a CI host. They should be able to see each other, avoid duplicating the same Cell, hand work off,
and leave durable context without turning a chat room into a second scheduler.

Factory Mesh is that rendezvous layer.

The design borrows one useful idea from Cydonia: live agent sessions are not the durable truth;
project artifacts are. Cydonia also treats a filesystem event as a knock that causes a re-read,
rather than trusting the event as the state itself. Factory Mesh applies the same rule to distributed
software factories:

> **A signal is a knock, never the news.**

A station may say “main moved”, “I intend to work on Cell X”, “please inspect this evidence”, or
“handoff to station B”. The receiver must then re-read the authoritative source — Airflow, the
Factory Cell, GitHub, the evidence ledger, or the provider receipt — before it acts.

## What is authoritative

Factory Mesh adds no lifecycle authority.

| Question | Authority |
| --- | --- |
| What runs next inside an admitted station lifecycle? | **Airflow** |
| Which lifecycle incarnation may mutate? | **Factory Cell epoch** |
| Did GitHub/provider mutation commit? | **operation journal + observed remote state** |
| Is a claim about a result true? | **retained evidence** |
| Which cooperating station is currently volunteering for a repo Cell? | **Factory Mesh coordination claim** |
| What did another station want me to notice? | **Factory Mesh signal** |

A coordination claim is deliberately weaker than a Cell. It prevents two stations that cooperate
through the same rendezvous from independently deciding to start the same repo-level work. It does
**not** authorize a sandbox mutation, advance a Cell, dispatch an Airflow task, publish a branch, or
merge a PR. Each station may have its own Airflow installation; the invariant is that lifecycle work
is scheduled by Airflow, never by the mesh, an agent chat loop, GitHub Actions, or a provider.

## Topology

```text
                         same GitHub repository
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
        station alice       station bob         station ci-east
        Airflow + backend    Airflow + backend   Airflow + backend
              │                   │                   │
              └──────────────┬────┴────┬──────────────┘
                             │         │
                    shared Factory Mesh endpoint
                         /v1/mesh/*
                             │
                   station-mesh.sqlite3
                  leases • claims • signals
```

The rendezvous can be one existing `swfactory backend`; it does not need to be the backend that owns
any station's Airflow installation. For a shared/multi-person deployment, configure **one dedicated
rendezvous backend process** and point every participant's `SWF_MESH_URL` at it.

Give peers a dedicated `SWF_MESH_TOKEN` (at least 32 non-whitespace characters) that is **different
from `SWF_BACKEND_TOKEN`**. The server accepts that credential only on `/v1/mesh/*`; it cannot
transition Cells, proxy Airflow, publish through SCM, or use the other privileged backend routes. A
backend administrator's normal backend token may still call mesh routes. Do not distribute the
backend token merely to let people coordinate.

For a single-machine/operator setup, `SWF_MESH_TOKEN` may be left unset and the normal backend token
continues to work for mesh routes at `http://localhost:8082`.

## The three primitives

### 1. Station lease

A station joins with a stable `station_id` and a fresh `incarnation_id`. Restarting the same station
advances `lease_epoch`; the old process can no longer heartbeat, signal, claim, or release work.
A station id is repository-bound, so it cannot silently move between repositories while keeping the
same lease identity.

This is process fencing, not authentication. HTTP bearer authentication is a separate boundary.

### 2. Coordination claim

A claim is keyed by `(repo, cell_id)` and carries both `cell_epoch` and its own monotonically
increasing `claim_epoch`. One current claim row exists at a time. Renewal by the same live station
incarnation preserves the claim epoch. A takeover increments it when either the claim TTL expires
**or the owning station lease dies/is replaced**. A stale releaser cannot delete the new owner's
claim.

That station-lease rule matters in practice: restarting a station does not make it wait for the
longer claim TTL before it can resume the work. The old claim becomes visible as `orphaned_claim`,
and a new live incarnation may immediately take it over with a higher claim epoch.

Claims are useful before a station submits/activates work and during explicit handoff. They are not
used as a substitute for Cell epoch fencing after lifecycle work starts.

### 3. Typed signal

Signals are durable, ordered by a monotonically increasing `seq`, TTL-bounded, and optionally
addressed to one station. Supported kinds are:

- `intent` — “I am about to work here”; useful for early duplicate detection.
- `observation` — “something changed; re-read the source of truth”.
- `request` — ask another station for a bounded action or inspection.
- `offer` — advertise available capability/capacity.
- `handoff` — point another station at the Cell/evidence/artifacts needed to continue.
- `conflict` — record an observed coordination disagreement.
- `result` — point at retained evidence for a completed attempt.
- `note` — human context that does not fit the stricter kinds.

A signal may carry `cell_id`, `cell_epoch`, artifact references, a small JSON payload, `reply_to`, and
`to_station`. Payloads are context only; consumers must never execute them as commands. Signals from
a replaced/expired station lease remain historical records but no longer count as live competing
intent.

`to_station` is **routing, not confidentiality**. Holders of the same mesh credential are cooperating
participants and can query the shared signal log (including with the client's `inbox --all`). Never
put secrets, raw credentials, private keys, or other confidential payloads in mesh signals.

## Operator flow

The client intentionally stores no bearer token. `.factory/station.json` contains only the local
station id, incarnation id, lease epoch, repo and rendezvous URL.

```bash
export SWF_MESH_URL=https://mesh.example.internal
export SWF_MESH_TOKEN='...dedicated peer-only token, distinct from backend token...'

# Announce one installation/process incarnation.
uv run python -m swfactory.station_client join \
  --repo acme/widgets \
  --capability islo \
  --capability rust

# Who else is live?
uv run python -m swfactory.station_client peers

# Publish early intent before expensive work.
uv run python -m swfactory.station_client say intent issue:42 \
  'Planning issue 42; re-read the Cell before acting' \
  --cell cell_0123456789abcdef \
  --cell-epoch 3 \
  --artifact docs/factory/42/intent.md

# Acquire the repo-level coordination lease.
uv run python -m swfactory.station_client claim cell_0123456789abcdef 3

# Ask a specific station to inspect evidence.
uv run python -m swfactory.station_client say request cell:cell_0123456789abcdef \
  'Please independently verify the candidate evidence' \
  --to station_abc123 \
  --cell cell_0123456789abcdef \
  --cell-epoch 3 \
  --artifact docs/factory/42/evidence.json

# Read broadcast + messages addressed to this station.
uv run python -m swfactory.station_client inbox --after 0

# Surface orphaned claims and multiple live intents for the same Cell/epoch.
uv run python -m swfactory.station_client conflicts
```

A long-running station should heartbeat at an interval comfortably below its lease TTL. A supervisor
can do that; the mesh itself intentionally does not create a background scheduler.

## HTTP and credential contract

All mesh routes are authenticated `POST`s on the existing backend:

```text
/v1/mesh/join
/v1/mesh/heartbeat
/v1/mesh/leave
/v1/mesh/peers
/v1/mesh/say
/v1/mesh/inbox
/v1/mesh/claim
/v1/mesh/release
/v1/mesh/claims
/v1/mesh/conflicts
```

When `SWF_MESH_TOKEN` is configured, it must be strong and distinct from `SWF_BACKEND_TOKEN`, and it
is accepted only for the routes above. This is intentionally a coarse **repo-cooperator**
credential, not per-message secrecy or per-user RBAC.

The shared store lives at:

```text
$SWF_STATE_ROOT/station-mesh.sqlite3
```

SQLite is the current single-rendezvous implementation. Run one mesh backend process for a given
state file and let all stations use its HTTP endpoint; do not mount one SQLite file under several
multi-primary backend processes. The API is intentionally narrow enough to move to Postgres (or
another transactional shared store) without changing station semantics when horizontal mesh-server
scaling is actually needed.

## Failure semantics

- **Station restart:** new incarnation advances `lease_epoch`; stale process writes are refused; its
  old claim is orphaned and immediately takeoverable with a higher `claim_epoch`.
- **Lost heartbeat:** station disappears from live peers; its still-unexpired claim is reported as
  `orphaned_claim` and may be taken over rather than blocking until the claim TTL.
- **Claim expiry:** a live station may take over; `claim_epoch` increments.
- **Late release:** fenced by station lease + claim epoch; it cannot delete a takeover. Releasing a
  claim that no longer exists is a coordination conflict, not an internal server error.
- **Duplicate signal retry:** same `message_id` + same content returns the original signal; reusing
  the id with different content is refused.
- **Station replacement:** signals from the old lease remain durable history but do not count as
  live intent for the new incarnation.
- **Two early intents:** both are retained and `conflicts` reports `competing_intents`; operators can
  converge before expensive work begins.
- **Mesh outage:** already-running lifecycle work does not change authority. Airflow and Cell epochs
  continue to define lifecycle/mutation truth; only optional station coordination is unavailable.

## The important boundary

Do **not** turn Factory Mesh into a queue of executable commands. If a future feature wants to say
“run this issue”, the signal may announce the request, but the actual request must still enter the
normal authenticated admission path and be scheduled by Airflow.

That keeps the system liquid at the edges — many stations, many humans, many agents — while the
center remains deterministic: Airflow-only lifecycle scheduling, fenced Cell mutation authority,
one operation journal per mutation authority, and retained proof of what happened.
