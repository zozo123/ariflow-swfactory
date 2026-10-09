# Operations

The operator's runbook: connect a real repository, keep the factory running, and operate it day to
day. Start with the [README](README.md) for what the factory is, and with the
[factory backend](docs/factory-backend.md) for what runs where. This project is alpha; read
[SECURITY.md](SECURITY.md) before connecting a production repository.

## Run it on a real GitHub repository

### 1. Prepare the product repository

Use an existing repository or create one:

```bash
gh repo create your-org/your-product --private --clone
```

Add `factory.toml` at the target directory's root. Commands must be non-interactive. The test
command must return a failure code when tests fail and write fresh JUnit XML to
`.factory/junit.xml`.

```toml
[commands]
test = "./scripts/ci-test.sh --junit .factory/junit.xml"
lint = "./scripts/ci-lint.sh"

[paths]
source = "src"
tests = "tests"
junit = ".factory/junit.xml"
protected = ["factory.toml", ".github/"]
```

For a monorepo, put one contract in each target directory and set that directory in the route.

### 2. Create a production route

Fork or clone this control repository. Copy `blueprints/default.toml` to
`blueprints/your-product.toml`, then update its name and target:

```toml
[blueprint]
name = "your-product"
version = 1
description = "Normal product change"

[trigger]
kind = "manual"

[[targets]]
repo = "your-org/your-product"
dir = ""
base_branch = "main"

[stages]
order = ["intent", "spec", "plan", "build_and_test", "review", "deliver"]

[[gates]]
after = "intent"
artifact = "intent.md"
timeout_h = 24

[[gates]]
after = "plan"
artifact = "plan.md"
timeout_h = 24

[limits]
max_build_iterations = 3
max_review_fixes = 1
max_turns = 40
budget_usd_per_stage = 2.0
budget_usd = 8.0
stage_timeout_h = 3
max_parallel_jobs = 4

[sandbox]
kind = "islo"
gateway_profile = "swfactory"
environment = "swfactory"
ttl_s = 172800
idle_s = 900

[deliver]
labels = ["factory", "agent-authored"]
```

The file name, blueprint name, and Airflow DAG id must match.

### 3. Check access and run one work order

Install the Airflow dependencies, authenticate GitHub and the selected sandbox provider, then run
the preflight:

```bash
uv sync --group airflow
gh auth status
islo login && islo login --tool github && islo login --tool claude
uv run swfactory doctor \
  --blueprint your-product \
  --agent claude \
  --sandbox islo \
  --scm github
```

Start issue 42 and answer both approvals in the terminal:

```bash
uv run swfactory run \
  --blueprint your-product \
  --issue 42 \
  --agent claude \
  --sandbox islo \
  --scm github \
  --approve prompt
```

The run ends with a pull request or an explicitly blocked or rejected pull request. Inspect the
pull request, `docs/factory/42/`, test report, review findings, approvals, cost, and patch before a
person merges it.

## Keep the factory running

The durable deployment is a small trusted control plane (Airflow, the webhook receiver and the
factory backend) that holds the GitHub delivery credentials, plus one short-lived work cell per
job.

- [Docker](docs/docker.md): the whole stack on one host, bound to localhost. Exposing it needs
  TLS, webhook HMAC verification, durable Airflow storage, authentication, backups and a
  restricted network, and Docker socket access gives the Airflow worker control of the host.
- [islo](docs/islo.md): the hosted control plane with MicroVM work cells and a signed GitHub
  webhook.
- [Backend-host variables are not worker variables](docs/factory-backend.md#backend-host-variables-are-not-worker-variables):
  the Airflow workers need their own `SWF_BACKEND_URL` and `SWF_BACKEND_TOKEN`; check the
  `managed workers` row of `swfactory doctor` before submitting.
- Other sandbox providers: `config/capability-inventory.json` records what each one carries, and
  an adapter must meet the
  [sandbox provider contract](skills/airflow-software-factory/references/sandboxes.md).

## Daily operation

1. **Send work.** Label an issue `factory:<route>`, comment `@factory run <route>`, trigger the DAG,
   use the CLI, or submit one Linear issue with `swfactory linear-submit`
   ([Linear intake](docs/native-linear-intake.md)).
2. **Approve.** Read `intent.md` and `plan.md` in Airflow, or run `swf gates review` then
   `swf gates approve`, or `swfactory approve <dag_run_id> intent|plan`.
3. **Watch.** Use `swf attention`, `swf tui`, `swfactory herd`, Airflow, and GitHub checks to
   follow active work.
4. **Release.** Review the final patch and evidence, then merge through normal branch protection.
5. **Improve.** Let `maintain` compare merged run metrics with `bands.yaml`; investigate incidents
   and admit useful proposals as new work orders.

Delivery metrics cover cycle time, iterations, first-pass verification, review findings, denied
tools, and cost. Production health, security, customer impact, and business results can feed the
same system by creating GitHub issues from the tools that already observe those signals.

## Routes, repositories, and versions

| Need | Configuration |
|---|---|
| Normal feature or repair | manual route with intent and plan approvals |
| Urgent repair | shorter `hotfix` route with tighter limits |
| Recurring maintenance | `[trigger] kind = "cron"`, `cron = "…"`, and `issues = ["path/to/work-order.md"]` — or `[trigger.backlog] label = "…"` to drain the open issues carrying a label, one `batch` per run, with every skip and its reason in `.factory/backlog/<line>.jsonl` |
| Several repositories | multiple `[[targets]]`; each run maps issues × targets |
| Monorepo | set each target's `dir` and keep a `factory.toml` there |
| Larger outer workflow | use the optional [Astronomer Blueprint step](docs/astronomer-blueprint.md) |

Python and Airflow are pinned in `pyproject.toml`, with an upstream-main canary in CI; `swf` needs
Rust 1.88 or newer to build and is prebuilt for macOS and Linux
([docs/swf.md](docs/swf.md#install)). Schema and package versions move independently: a blueprint
schema `version` changes only when an older blueprint can no longer be read.

## Commands

| Command | Purpose |
|---|---|
| `swfactory demo` | run the keyless end-to-end replay |
| `swfactory run` | run one route over issues × targets |
| `swfactory doctor` | check a live deployment before work starts |
| `swfactory herd` | view runs, approvals, pull requests, and sandboxes |
| `swfactory approve` | answer an Airflow approval gate |
| `swfactory state list / inspect` | inspect local run ownership, interrupted attempts, journal health and recorded spend |
| `swfactory webhook serve` | route trusted GitHub events into Airflow |
| `swfactory webhook deliveries / inspect / retry` | inspect durable dispatch receipts and recover failed submissions |
| `swfactory linear-preview / linear-submit` | read one Linear issue on the controller, then submit its accepted text through the backend ([Linear intake](docs/native-linear-intake.md)) |
| `swfactory metrics` | aggregate committed run evidence |
| `swfactory maintain` | detect metric drift and sweep owned sandboxes |
| `swf` | the same connect, submit, watch, approve, and verify operations as one native binary, plus `swf tui` ([docs/swf.md](docs/swf.md)) |

Webhook intake persists accepted work before replying and retries Airflow submission in the
background. Repository-bound routes and stable run IDs keep redelivery from creating unrelated
jobs. See [durable webhook intake](docs/webhooks.md) for receipts, recovery, health and storage.

Run mutations share a host ownership lock, and interrupted journal appends preserve their damaged
tail before recovery. See [run recovery](docs/run-recovery.md) for operation history, local
inspection, concurrent attempts and budget accounting.

The factory's five authoritative stores live in one state root on one host. `swfactory backup
create|verify|restore|status|resume|reconciled|close` takes coordinated backups and restores them with
mutations withheld until the restore is validated and every restored Cell has observed remote
state. See [backup, restore and upgrade](docs/backup-restore.md) for the supported deployment
boundary, the schema/rollback rules and the operator drill. Multi-replica and Postgres operation is
unqualified and refused.

## Repository map

```text
blueprints/        production routes and their limits
dags/              generated Airflow DAGs, approvals, and maintenance
src/swfactory/     runtime, stages, adapters, state, policy, and CLI
rust/              the swf operator binary: domain, adapters, operations, TUI, CLI
skills/            installable agent skills (airflow-software-factory, swfactory)
deploy/docker/     local Airflow and Docker work-cell stack
deploy/islo/       hosted control plane and MicroVM work cells
docs/              design, deployment, evaluation, and operations guides
site/              GitHub Pages guide
```

[Design and schema](docs/design.md) · [Docker](docs/docker.md) · [islo](docs/islo.md) ·
[Factory floor](docs/herd.md) · [swf binary](docs/swf.md) · [Evaluation](docs/evals.md) ·
[Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)
