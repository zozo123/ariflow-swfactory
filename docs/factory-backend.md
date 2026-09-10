# One console, one backend, one scheduler

The normal path is **Rust console → Python factory backend → Airflow → Python stages → isolated worker**.
Start here when deciding which component should own a change.

| Factory term | Meaning | Owner |
| --- | --- | --- |
| Work order | Issue references and selected repositories | Python backend validates and admits it |
| Production line | Installed blueprint: route, targets, limits and gates | Python blueprint model |
| Batch | One submitted Airflow DAG run | Airflow schedules and retries it |
| Work cell | One mapped issue × target job, with its sandbox and journal | Python runtime and stages |
| Quality station | Build/test, review, or human approval | Python stages; Airflow pauses for human input |
| Delivery | Published branch/PR with evidence | Python control plane publishes; a human merges |
| Control room | Terminal views, selection and explicit operator confirmation | Rust `swf` |

These terms describe the existing manufacturing flow. There is no second queue or scheduler in
the console. A stopped batch means Airflow marks the run failed; it does not kill an executing
process or remove its work cell. Cleanup is a separate, ownership-checked action.

## What runs where

`src/swfactory/backend.py` is the network boundary. It loads the installed Python blueprints,
validates work orders and target selections, unpauses the selected line, and submits to Airflow's
public API. It also serves delivery evidence, metrics, worker operations and saved run inspection.
Airflow, GitHub and worker-provider credentials stay in this process or the Python execution
control plane. The terminal needs only an operator token for this backend.

`dags/blueprints.py` describes scheduling and mapping. `runtime.py` creates execution contexts.
`stages.py` performs intent, specification, planning, build/test, review and publication. The
worker sandbox receives the agent's restricted credentials, never the GitHub publishing token.
`state.py` serializes mutations and records interrupted operations; [run recovery](run-recovery.md)
explains the limits of that evidence.

`rust/crates/swf-adapters/src/factory.rs` is the backend client. `swf-app` owns operator selection,
presentation, cancellation, gate review and bulk-confirmation bookkeeping. The Rust application
does not execute factory stages. Local stack administration, opening a browser and explicitly
requested delivery re-verification remain local actions. Re-verification may require a checkout,
git and that repository's test tools; ordinary factory operation requires none of them.

## Start the backend

Run in the factory checkout on the trusted control-plane host, with Python dependencies installed:

```sh
# Use a secret manager or generate a token once; give the same value to authorized operators.
export SWF_BACKEND_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export AIRFLOW_URL=http://localhost:8080
export AIRFLOW_USER=admin
# Set AIRFLOW_PASSWORD securely, or set AIRFLOW_TOKEN instead.
export SWF_REPO=acme/widgets
export SWF_SANDBOX_OWNER=operator@example.com
uv run swfactory backend
```

The default bind is `127.0.0.1:8082`. For remote operators, put an authenticated deployment behind
HTTPS and run `swfactory backend --host 0.0.0.0` on its private network. The API always requires
`Authorization: Bearer <SWF_BACKEND_TOKEN>`, including health. The token must be at least 32
non-whitespace characters. It grants factory operator authority, including approvals and cleanup;
this is a single trusted-operator deployment, not a multi-tenant authorization system.

`SWF_METRICS_ROOT` defaults to the backend checkout. `SWF_STATE_ROOT` defaults to `.factory`.
Optional integrations use the backend's `gh` and `islo` installations and credentials. Missing
integrations remain visible in `swf doctor`; an empty configured fleet is a valid result.

## Backend-host variables are not worker variables

`SWF_BACKEND_URL` and `SWF_BACKEND_TOKEN` are read by three different processes. Setting them once,
on the backend host, configures **only** that host:

| Variable | Backend host (`swfactory backend`) | Airflow worker (managed stages) | Operator machine (`swf`) |
| --- | --- | --- | --- |
| `SWF_BACKEND_URL` | not read — `--host`/`--port` bind the listener | **required**: where a managed work cell reports | optional; overrides the context's `backend_url` |
| `SWF_BACKEND_TOKEN` | **required**: the token the API accepts | **required**: the same secret, sent as `Authorization: Bearer` | **required**: the same secret |

A backend-managed work cell reports every lifecycle transition
(`src/swfactory/cell_callback.py`) and publishes (`backend_scm.py`) from inside the Airflow worker
process, using the worker's own copy of the pair. Both fail closed when a value is missing or the
token is under 32 non-whitespace characters, so a stack whose workers do not carry them accepts
work orders and then fails every one of them in its first stage: the submission succeeds, no gate
ever appears, and the only evidence is in a task log. `swfactory doctor` reports that wiring as its
`managed workers` row — read it before admitting a work order. Direct/unmanaged Airflow runs never
call the backend and need neither variable.

For Docker, export the token in the shell that starts Compose, then:

```sh
export SWF_BACKEND_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
docker compose -f deploy/docker/compose.yml up -d
```

Compose gives that one secret to both the `backend` and the `airflow` service, and gives the
workers `SWF_BACKEND_URL=http://backend:8082`. That URL is fixed in the file, not read from your
shell: `http://localhost:8082` is the *console's* address and reaches nothing from inside the
compose network. Workers that must reach a backend published elsewhere need a compose override.

The `backend` service starts with the default stack, because the built-in console context and every
managed work cell address it; a stack without it is the failure above. It reads the generated
Airflow login from the shared volume when explicit Airflow credentials are absent, and publishes
only loopback port 8082. An Airflow-only deployment that wants no backend names its services
explicitly: `docker compose -f deploy/docker/compose.yml up -d airflow webhook`.
Worker discovery/removal currently uses islo; Docker work cells still clean up through their
Python stage lifecycle. The Docker image does not install islo, so that optional integration must
be provisioned separately if needed.

## Connect the Rust console

On the operator machine, set `SWF_BACKEND_TOKEN` to the same secret and run:

```sh
swf context add local --backend-url http://localhost:8082 \
  --airflow-url http://localhost:8080 --repo acme/widgets --use
swf doctor
swf submit --blueprint factory --issue 42 --target acme/widgets
swf tui
```

`--airflow-url` is the public browser address in backend mode. The backend uses its own
`AIRFLOW_URL` for service requests. A context stores `backend_url`; `SWF_BACKEND_URL` can override
it for the current process. The repository and owner configured **on the backend** determine the
delivery and worker scope. Context values cannot widen those permissions. `--repo` on a context
is still useful for explicitly requested local delivery verification.

New CLI contexts and the built-in fallback use `http://localhost:8082`. Existing context files
without `backend_url` retain direct mode so migration is explicit. To keep a direct connection,
create its context with
`swf context add ... --direct --force`, or set `backend_url = ""` in its TOML. Direct mode uses
the previous context Airflow credentials, local `gh`/`islo`, and local metrics. There is no silent
fallback from an unavailable backend to broader local credentials.

## API v1 contract

All bodies are JSON objects. Service errors use an HTTP status and `{"detail": "..."}`.
Read operations below use POST with explicit JSON arguments so identities never become arbitrary
filesystem paths or command strings. No route accepts a caller-provided executable or upstream URL.

| Route | Request | Response |
| --- | --- | --- |
| `GET /v1/health` | none | service name and API version |
| `POST /v1/doctor` | `{}` | readiness checks from the backend host |
| `POST /v1/lines` | `{}` | installed line names, routes, gates and targets |
| `POST /v1/work-orders` | `line`, `issues`, optional `targets`; `airflow_run_id` from actor `airflow-schedule` binds a run Airflow's scheduler already created instead of dispatching one | run identity, validated blueprint, mapped job count and, once bound, the complete Cell `bindings` |
| `POST /v1/workers` | `{}` | configured owner's active worker references |
| `POST /v1/workers/remove` | `name` | argv executed after fresh server-side ownership checks |
| `POST /v1/deliveries/prs` / `issues` | optional `label`, `limit` | normalized repository records |
| `POST /v1/deliveries/head` | `branch` | PR publication evidence, or null |
| `POST /v1/deliveries/checks` / `url` | `number` | check summary / browser URL |
| `POST /v1/metrics/runs` / `summary` | `{}` | committed metrics / aggregate |
| `POST /v1/state/runs` | optional `limit` | saved run summaries |
| `POST /v1/state/inspect` | `run_id` | ownership, journal health and interruption evidence |

The compatibility mount `/v1/airflow/api/v2/...` preserves Airflow's published JSON, pagination,
log continuation and conflict responses for the existing console. It allows the console's reads
and only four mutations: submit an installed line, unpause it, mark its batch failed, and answer
an approval. It cannot change variables, connections, users or DAG code. Read access covers the
configured Airflow deployment; keep unrelated tenants on separate deployments. Submission and
approval mutations validate in Python, including a fresh task-readiness check for approvals.
Rust keeps its additional evidence review and repeated readiness observations. `--force` cannot
bypass the backend's readiness check.

Requests are bounded to 64 KiB and upstream responses to 16 MiB, with deadlines and redirects
disabled. Airflow authentication can refresh once after an explicit 401. A timeout or server error
after a write remains an **unknown outcome**: the backend does not replay it. Inspect the run/gate
before retrying. Interactive submission has no durable deduplication key; webhook submissions use
the separate [durable inbox](webhooks.md).

## From work order to finish line

Submit an installed line, watch mapped work cells, inspect logs and artifacts, explicitly answer
its quality gates, then inspect the delivery and its checks. A completed Airflow run, a published
PR, and independently verified code are three different claims. The console preserves that
distinction. Publication is the factory's finish line; merging remains a human decision.
