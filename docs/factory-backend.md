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

`src/swfactory/backend/` is the network boundary package. It loads the installed Python blueprints,
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
# Managed Airflow stages call the backend at every Cell boundary. Export these SAME values in the
# scheduler/worker environment before Airflow starts (use the workers' reachable backend URL).
export SWF_BACKEND_URL=http://localhost:8082
export AIRFLOW_URL=http://localhost:8080
export AIRFLOW_USER=admin
# Set AIRFLOW_PASSWORD securely, or set AIRFLOW_TOKEN instead.
export SWF_REPO=acme/widgets
export SWF_SANDBOX_OWNER=operator@example.com
uv run swfactory backend
```

The default bind is `127.0.0.1:8082`. For remote operators, put an authenticated deployment behind
HTTPS and run `swfactory backend --host 0.0.0.0` on its private network. `GET /v1/liveness` and
`GET /v1/readiness` are deliberately unauthenticated probe endpoints; every other route, including
`GET /v1/health`, requires `Authorization: Bearer <SWF_BACKEND_TOKEN>`. The token must be at least
32 non-whitespace characters. It grants factory operator authority, including approvals, Cell
transitions and publication; this is a single trusted-operator deployment, not a multi-tenant
authorization system.

For a host deployment, start Airflow with `SWF_BACKEND_URL` and the same `SWF_BACKEND_TOKEN` in its
scheduler/worker environment. `swf doctor` reports `managed worker callback` red if this declared
callback contract is missing. In Compose the `airflow` service receives both values automatically;
the backend container keeps the same declarations so its doctor report and worker configuration
come from one environment.

`SWF_METRICS_ROOT` defaults to the backend checkout. `SWF_STATE_ROOT` defaults to `.factory`.
Optional integrations use the backend's `gh` and `islo` installations and credentials. Missing
integrations remain visible in `swf doctor`; an empty configured fleet is a valid result.

For Docker, set the token in your shell, then:

```sh
docker compose -f deploy/docker/compose.yml --profile console up -d
```

The `backend` service reads the generated Airflow login from the shared volume when explicit
Airflow credentials are absent. It publishes only loopback port 8082. The console profile keeps
existing Airflow-only deployments usable; starting the backend requires opting into the profile.
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

| Route | Authority / response |
| --- | --- |
| `GET /v1/liveness` | public process liveness probe |
| `GET /v1/readiness` | public read/mutation readiness and serving generation probe |
| `GET /v1/health` | authenticated service/API/Cell schema versions and mutation readiness |
| `POST /v1/doctor` | backend, Airflow, managed-worker callback and optional-tool checks |
| `POST /v1/compatibility` | versioned capability/contract document |
| `POST /v1/lines` | installed production lines, targets, stages and gates |
| `POST /v1/blueprints/preview` | deterministic mapping preview for a validated line |
| `POST /v1/work-orders` | admit and submit a governed line; creates/owns Factory Cells |
| `POST /v1/fleet` | configured provider/fleet capability document |
| `POST /v1/queue` / `queue/inspect` | admission state and one admitted/queued work item |
| `POST /v1/operations` / `operations/inspect` | unresolved mutation journal and one operation |
| `POST /v1/cells` / `cells/inspect` / `cells/history` | durable Cell projections and history |
| `POST /v1/cells/transition` | **mutation:** epoch-fenced Cell lifecycle transition/cleanup |
| `POST /v1/evidence/verify` / `evidence/checkpoint` | evidence-chain verification/checkpoint |
| `POST /v1/scm/issue` | read one numeric GitHub issue through backend credentials |
| `POST /v1/scm/publish` | **mutation:** epoch/policy-fenced patch push + PR publication |
| `POST /v1/scm/open-issue` | **mutation:** epoch/policy-fenced GitHub issue creation |
| `POST /v1/deliveries/prs` / `deliveries/issues` | normalized repository records |
| `POST /v1/deliveries/head` | publication evidence for a deterministic branch |
| `POST /v1/deliveries/checks` / `deliveries/url` | check summary / browser URL |
| `POST /v1/workers` | configured owner's active worker references |
| `POST /v1/workers/remove` | **mutation:** ownership-checked worker removal |
| `POST /v1/metrics/runs` / `metrics/summary` | committed metrics / aggregate |
| `POST /v1/state/runs` / `state/inspect` | saved run summaries / journal evidence |

The compatibility mount `/v1/airflow/api/v2/...` preserves Airflow's published JSON, pagination,
log continuation and conflict responses for the existing console. **Inside that compatibility
mount only**, the allow-list contains four mutations: submit an installed line, unpause it, mark
its batch failed, and answer an approval. The backend API also has the explicitly documented Cell,
SCM and worker mutations in the table above. The compatibility mount cannot change variables,
connections, users or DAG code. Read access covers the
configured Airflow deployment; keep unrelated tenants on separate deployments. Submission and
approval mutations validate in Python, including a fresh task-readiness check for approvals.
Rust keeps its additional evidence review and repeated readiness observations. `--force` cannot
bypass the backend's readiness check.

Ordinary requests are bounded to 64 KiB. The two SCM publication routes
`/v1/scm/publish` and `/v1/scm/open-issue` allow up to 16 MiB so bounded patches and issue bodies can
cross the trusted boundary; upstream responses are capped at 16 MiB. Deadlines are enforced and
redirects are disabled. Airflow authentication can refresh once after an explicit 401. A timeout or server error
after a write remains an **unknown outcome**: the backend does not replay it. Inspect the run/gate
before retrying. Interactive submission has no durable deduplication key; webhook submissions use
the separate [durable inbox](webhooks.md).

## From work order to finish line

Submit an installed line, watch mapped work cells, inspect logs and artifacts, explicitly answer
its quality gates, then inspect the delivery and its checks. A completed Airflow run, a published
PR, and independently verified code are three different claims. The console preserves that
distinction. Publication is the factory's finish line; merging remains a human decision.
