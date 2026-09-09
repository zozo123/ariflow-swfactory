# Durable GitHub intake

`swfactory webhook serve` commits each admitted delivery to SQLite before returning HTTP 202.
The request does not wait for the backend. A background worker drains the inbox, including work
accepted before the receiver restarted. This follows GitHub's recommendation to
[respond within ten seconds and process deliveries asynchronously](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks).

The worker submits one **work order** per delivery to the backend's managed boundary
(`POST /v1/work-orders`). The backend owns everything past that call: durable admission, capacity
accounting, Factory Cell activation and the Airflow write itself, so a webhook-triggered run is
managed exactly like one submitted from the console. Airflow remains the only lifecycle scheduler;
the receiver decides admission, never timing.

```sh
export SWF_WEBHOOK_INBOX=/var/lib/swfactory/webhooks/inbox.sqlite3
export SWF_BACKEND_URL=https://factory-backend.example.com
export SWF_BACKEND_TOKEN=...    # the backend bearer token
# SWF_WEBHOOK_SECRET enables local HMAC verification.
uv run swfactory webhook serve
```

The default inbox is `.factory/webhooks/inbox.sqlite3` relative to the receiver's working
directory. Both Docker and islo entrypoints set it to `$AIRFLOW_HOME/webhooks/inbox.sqlite3`,
beside the persistent Airflow state.

Every GitHub-shaped intake — this receiver and `.github/workflows/dispatch.yml` — submits under the
actor `github`, so the same label arriving through both channels hashes to one work order and meets
the same immutable-work and Factory Cell checks instead of racing two admissions.

### Legacy direct-Airflow mode

Without `--backend-url` the receiver posts DAG runs straight to Airflow (`AIRFLOW_TOKEN`, or
`AIRFLOW_USER` + `AIRFLOW_PASSWORD`) and says so on startup. Those runs carry no `_factory_cells`,
so they get no admission record, no capacity accounting and no Cell fencing. It exists only for a
sandbox with no backend in front of it.

## What gets admitted

The existing event policy applies: a `factory` or `factory:<route>` issue label, or a new
`@factory run [<route>]` comment from an OWNER, MEMBER or COLLABORATOR. Pull requests and delivery
status labels do not start work. `SWF_WEBHOOK_SECRET` enables local HMAC verification; without it,
the receiver still requires a trusted upstream verifier such as islo's incoming webhook.

A routed request must carry `X-GitHub-Delivery` and `repository.full_name`. The named blueprint
must be installed locally, its DAG name must match, and the source repository must be one of its
targets. The saved configuration sets `targets` to that repository's configured spelling. All
of its configured monorepo directories still participate; other repositories do not receive the
source issue's number. Multi-repository work remains available through explicit CLI/API submission.

Preview this admission decision without starting work:

```sh
uv run swfactory webhook route issues event.json --repository-check
```

The persisted envelope contains the issue number, selected targets, route, delivery ID, source
repository, event type and a SHA-256 digest of the event type plus original body. Issue text,
comment text, HMAC signatures, API tokens and passwords are not stored. Airflow receives the
provenance in `conf._swfactory_webhook` alongside `issues` and `targets`.

## Receipts and recovery

Each delivery gets `swf_webhook__<sha256(repository, delivery-id)>` as its Airflow run ID. Attempts
always submit that identity and the frozen configuration. On HTTP 409, the worker reads the exact
run and compares its DAG ID, run ID and complete configuration. Only matching evidence counts as
dispatched; it never clears, stops or recreates the existing run.

This covers an Airflow commit followed by a network timeout, and a worker crash after creating
the run but before saving the receipt. A new attempt can recover the same receipt. It does not
make the tasks inside the Airflow run execute exactly once. Deleting both the inbox receipt and
the Airflow run removes their duplicate protection.

| State | Meaning | Next step |
| --- | --- | --- |
| `pending` | Saved; waiting for dispatch or backoff | Worker retries automatically |
| `dispatching` | A worker owns a 120-second lease | Expired leases are reclaimed automatically |
| `dispatched` | Airflow returned matching run evidence | Follow that run through Airflow or `swf` |
| `dead` | Permanent failure or attempt limit reached | Repair the cause, inspect, then explicitly retry |

Claims use SQLite write transactions. Each claim has a new fencing token; a worker that lost
its claim cannot overwrite a newer worker's receipt. The HTTP operation runs outside the
transaction so a slow API does not hold the intake lock.

Transport errors, HTTP 408/429 and HTTP 5xx retry with exponential backoff from five seconds to
five minutes, plus deterministic jitter. `Retry-After` seconds and HTTP dates are respected up to
one hour. Other HTTP failures become dead deliveries; a missing route or bad credentials need an
operator's attention. Retries are limited to 12 claims by default. A crash consumes an attempt,
and an expired final attempt becomes dead instead of looping forever.

```sh
uv run swfactory webhook deliveries --state dead
uv run swfactory webhook deliveries --json
uv run swfactory webhook inspect <delivery-id>
# After fixing the reported problem:
uv run swfactory webhook retry <delivery-id>
```

Use the same `SWF_WEBHOOK_INBOX` or `--inbox` as the running receiver. Docker operators can run
these commands with `docker compose -f deploy/docker/compose.yml exec webhook uv run swfactory ...`.
`inspect` and `retry` print one JSON receipt; `deliveries --json` includes counts and pending age.
`retry` resets the current attempt cycle, preserves lifetime attempts, and requeues only a dead
delivery with its original run ID. It cannot rerun a completed Airflow job. To request new work,
create a new authorized GitHub event with a new delivery identity.

Reposting the same delivery returns its saved receipt without reopening a dead or dispatched
entry, even if the blueprint was subsequently changed or removed. Reusing the identity with a
different event/body is HTTP 409. A saved dispatch is not redirected by later blueprint edits;
the child DAG's normal validation still applies when Airflow runs it.

## Health and operating limits

| Setting | Default | Purpose |
| --- | --- | --- |
| `SWF_WEBHOOK_INBOX` / `--inbox` | `.factory/webhooks/inbox.sqlite3` | Persistent database path |
| `SWF_WEBHOOK_MAX_PENDING` / `--max-pending` | `10000` | Maximum pending, dispatching and dead receipts combined |
| `SWF_WEBHOOK_MAX_ATTEMPTS` / `--max-attempts` | `12` | Claims before a delivery becomes dead |
| `SWF_BACKEND_URL` / `--backend-url` | unset (legacy Airflow mode) | Managed work-order boundary |
| `SWF_BACKEND_TOKEN` / `--backend-token-env` | `SWF_BACKEND_TOKEN` | Env var holding the backend bearer token |

`GET /healthz` is process liveness. `GET /readyz` checks database readability, worker liveness
and intake capacity, and returns counts plus the oldest pending age. It intentionally does not
require backend connectivity: the inbox can accept work during an outage. Monitor dead counts
and pending age separately. A successful 202 means durable admission, never that a job passed.

The response's `state` is the receipt snapshot at admission; inspect again for current progress.

A receipt names the work order the backend admitted (`work_order_id`) and the state it answered
with (`admission_state`). `submitted` means the order was handed to Airflow; `queued` means it is
durably admitted and waiting for capacity, and no run exists yet. `swfactory webhook inspect
<delivery-id>` prints both. A backend refusal is never answered by writing to Airflow behind its
back: a drain (503) and a capacity refusal (429) retry with backoff, while a conflict (409) or an
unsupported line (422) goes dead for an operator to look at.

Intake returns 503 before acknowledgment when storage is unavailable or capacity is exhausted.
Invalid repository routing or a missing delivery identity returns 422. A failed HMAC is 401.
An unrouted event remains 200 with `routed: false`. Failed or unreceived deliveries must be
[redelivered by the sender](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries);
the internal worker can only retry work the receiver actually committed.

Use a persistent local filesystem. SQLite WAL with full synchronization provides durability and
permits concurrent receiver processes on the same host; this is not a queue for shared NFS or
independent replica disks. New database files are mode 0600. A database binds to one Airflow URL
and refuses to open for dispatch against a different endpoint. Supply a separate database for
another environment, and keep credential access restricted to the control plane.

Receipts are retained without automatic pruning, so plan disk capacity. Back up with SQLite's
online backup API, or stop all receiver processes before copying the database and any WAL files.
Deleting Docker volumes or destroying an islo control-plane VM without preserving this state
loses accepted work. No credentials are embedded in the database, but receipt metadata still
belongs in the trusted control plane.

The low-level `make_handler` / `make_server` Python APIs retain synchronous behavior when called
without an inbox. The shipped CLI and deployment entrypoints always use durable admission.
