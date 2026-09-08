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
| What runs next? | **Airflow** |
| Which lifecycle incarnation may mutate? | **Factory Cell epoch** |
| Did GitHub/provider mutation commit? | **operation journal + observed remote state** |
| Is a claim about a result true? | **retained evidence** |
| Which station is currently volunteering for a repo Cell? | **Factory Mesh coordination claim** |
| What did another station want me to notice? | **Factory Mesh signal** |

A coordination claim is deliberately weaker than a Cell. It prevents two stations that cooperate
through the same rendezvous from independently deciding to start the same repo-level work. It does
**not** authorize a sandbox mutation, advance a Cell, dispatch an Airflow task, publish a branch, or
merge a PR.

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
any station's Airflow installation. Set every participant's `SWF_MESH_URL` and `SWF_MESH_TOKEN` to
the same authenticated endpoint.

For a single-machine setup, the normal backend at `http://localhost:8082` is enough.

## The three primitives

### 1. Station lease

A station joins with a stable `station_id` and a fresh `incarnation_id`. Restarting the same station
advances `lease_epoch`; the old process can no longer heartbeat, signal, claim, or release work.

This is process fencing, not authentication. The backend bearer token is still the credential.

### 2. Coordination claim

A claim is keyed by `(repo, cell_id)` and carries both `cell_epoch` and its own monotonically
increasing `claim_epoch`. One live claim exists at a time. Renewal by the same station preserves the
claim epoch; takeover after expiry increments it. A stale releaser cannot delete the new owner's
claim.

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
`to_station`. Payloads are context only; consumers must never execute them as commands.

## Operator flow

The client intentionally stores no bearer token. `.factory/station.json` contains only the local
station id, incarnation id, lease epoch, repo and rendezvous URL.

```bash
export SWF_MESH_URL=https://mesh.example.internal
export SWF_MESH_TOKEN='...same rendezvous token...'

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

## HTTP contract

All routes are authenticated `POST`s on the existing backend:

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

The shared store lives at:

```text
$SWF_STATE_ROOT/station-mesh.sqlite3
```

SQLite is the current rendezvous implementation because the backend is already the serialized,
authenticated control-plane boundary. The API is intentionally narrow enough to move to Postgres
without changing station semantics.

## Failure semantics

- **Station restart:** new incarnation advances `lease_epoch`; stale process writes are refused.
- **Lost heartbeat:** station disappears from live peers; its still-unexpired claim is reported as
  `orphaned_claim`, not silently stolen.
- **Claim expiry:** a live station may take over; `claim_epoch` increments.
- **Late release:** fenced by station lease + claim epoch; it cannot delete a takeover.
- **Duplicate signal retry:** same `message_id` + same content returns the original signal; reusing
  the id with different content is refused.
- **Two early intents:** both are retained and `conflicts` reports `competing_intents`; operators can
  converge before expensive work begins.
- **Mesh outage:** already-running lifecycle work does not change authority. Airflow and Cell epochs
  continue to define lifecycle/mutation truth; only optional station coordination is unavailable.

## The important boundary

Do **not** turn Factory Mesh into a queue of executable commands. If a future feature wants to say
“run this issue”, the signal may announce the request, but the actual request must still enter the
normal authenticated admission path and be scheduled by Airflow.

That keeps the system liquid at the edges — many stations, many humans, many agents — while the
center remains deterministic: one scheduler, one Cell epoch, one mutation journal, one retained
proof of what happened.
