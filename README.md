<p align="center">
  <img src="site/factory-line.webp" alt="A software change moving through isolated factory cells from issue to reviewed pull request" width="1200" />
</p>

<h1 align="center">swfactory</h1>

<p align="center"><strong>Run software work like a production system.</strong></p>

<p align="center">
  <a href="https://github.com/zozo123/ariflow-swfactory/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/zozo123/ariflow-swfactory/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://airflow.apache.org/docs/apache-airflow/3.3.1/"><img alt="Airflow 3.3.1" src="https://img.shields.io/badge/Airflow-3.3.1-017CEE?logo=apacheairflow&logoColor=white" /></a>
  <a href="https://www.python.org/downloads/release/python-3120/"><img alt="Python 3.12" src="https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white" /></a>
  <a href="docs/swf.md"><img alt="Rust 1.82" src="https://img.shields.io/badge/Rust-1.82-000000?logo=rust&logoColor=white" /></a>
  <a href="LICENSE"><img alt="Apache-2.0" src="https://img.shields.io/badge/License-Apache--2.0-D22128" /></a>
  <a href="https://skills.sh/zozo123/ariflow-swfactory/airflow-software-factory"><img alt="skills.sh" src="https://skills.sh/b/zozo123/ariflow-swfactory" /></a>
</p>

`swfactory` turns a GitHub issue into a reviewed pull request. The issue is the work order. A
blueprint selects the production route. Airflow schedules the steps. A coding agent works in one
isolated work cell. Tests, review, and people decide whether the change moves forward.

```text
WORK ORDER -> ROUTE -> AIRFLOW -> WORK CELL -> QUALITY -> PULL REQUEST -> HUMAN MERGE
                              \________________________________________/
                                      saved evidence and history
```

[Open the live guide](https://zozo123.github.io/ariflow-swfactory/) ·
[Read the design](docs/design.md) ·
[See a completed factory run](https://github.com/zozo123/ariflow-swfactory/pull/3)

## Run against the latest Airflow main

Requires Python 3.12, uv, Git, Node 22+ and pnpm 10.28.1.

```sh
./scripts/airflow_main.sh
uv run --no-sync pytest
uv run --no-sync swfactory demo
SWF_AIRFLOW_NO_SYNC=1 scripts/stress_airflow.sh
```

The installer resolves `apache/airflow@main` once, installs core, task SDK, standard and common.ai
providers plus the Airflow metapackage from that commit, checks their provenance, and builds both
web UIs. Set `AIRFLOW_REF=<commit>` to reproduce a run. Keep `--no-sync` on subsequent commands;
`uv sync --group airflow` intentionally returns to the pinned release. `--islo` explicitly selects
the experimental islo provider fork; the default sandbox provider comes from upstream main.

The live E2E starts a temporary Airflow instance, runs two issues across two targets, answers eight
real approval gates as admin, and checks each job's test results and delivery artifacts. It then
clones each published branch and reruns its tests independently. It uses
the scripted agent and local Git remotes. To separately test real sbx microVM transport and
cleanup on an authenticated Docker Sandboxes host:

```sh
SWF_TEST_LIVE_TOOLSET=1 uv run --no-sync pytest tests/test_toolset_live.py
```

This opt-in microVM test requests open networking and does not change host policy. Factory toolset
runs retain network restrictions: set `SWF_TOOLSET_SBX_HOST_NETWORK_POLICY=deny-all` only after
configuring that policy on a dedicated worker, and set `SWF_TOOLSET_SBX_IMAGE` to an image with Git,
the target's test tools, and the selected agent. The provider's default Python image suffices for
the transport test but does not contain all factory tools.

## Factory map

| Factory term | In this project | Job |
|---|---|---|
| Work order | GitHub issue | says what needs to change |
| Production route | `blueprints/*.toml` | selects stages, approvals, limits, target, and sandbox |
| Work instructions | target repository's `factory.toml` | names test, lint, source, test, and protected paths |
| Plant scheduler | Apache Airflow | starts, pauses, retries, maps, and records each job |
| Work cell | one sandbox per issue and target | contains the checkout, agent, and tools |
| Operator | the coding agent | writes or reviews within the current stage |
| Quality checks | fresh tests, review, and patch validation | decide whether work may advance |
| Trace record | `docs/factory/<issue>/` | keeps intent, plan, approvals, review, metrics, and receipts |
| Factory floor | `swfactory herd` | shows active runs, gates, pull requests, and sandboxes |
| Continuous improvement | `metrics.json`, `bands.yaml`, and `maintain` | turns delivery drift into logs, incidents, or new work orders |

People choose the work, approve intent and plan, and merge. Airflow owns run state. The agent owns
only the task inside its current work cell.

## Try the complete flow locally

The demo runs every production stage, including a failed build and repair, with recorded agent
outputs. It needs no model key and publishes only to a temporary local Git remote.

```bash
git clone https://github.com/zozo123/ariflow-swfactory.git
cd ariflow-swfactory
uv sync
uv run swfactory demo
```

The final report links the generated artifacts and local pull-request record.

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

The durable deployment has a small trusted control plane and many short-lived work cells:

```text
GitHub -> signed webhook -> Airflow control plane -> sandbox provider -> one work cell per job
   ^                              |                         |
   |                              v                         v
   +-------- pull request <- quality checks <- patch + evidence
```

The control plane can run inside a container, VM, or hosted sandbox and create remote work cells
through a provider API. Keep Airflow state and the webhook receiver on persistent storage. Keep
GitHub delivery credentials in the control plane. Give each work cell only the model access and
network destinations required for its job.

### Local Docker rehearsal

```bash
docker build -f deploy/docker/sandbox.Dockerfile -t swfactory-sandbox:local .
SWF_AGENT=claude SWF_SCM=github \
ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" GH_TOKEN="$GH_TOKEN" \
docker compose -f deploy/docker/compose.yml up -d
```

The stack binds to localhost. Production exposure needs TLS, webhook HMAC verification, durable
Airflow storage, authentication, backups, and a restricted network. Docker socket access gives the
Airflow worker control of the Docker host. See [the Docker guide](docs/docker.md).

### Hosted islo deployment

Bootstrap the product repository's agent environment once, then deploy the control repository and
attach its webhook to the product repository:

```bash
REPO=your-org/your-product TARGET_DIR= deploy/islo/bootstrap.sh

export SWF_CONTROL_REPO=your-org/swfactory-control
export SWF_TARGET_REPO=your-org/your-product
export SWF_CONTROL_BRANCH=main
export GITHUB_WEBHOOK_SECRET="$(openssl rand -hex 32)"
deploy/islo/deploy.sh
```

Store and reuse the webhook secret on later deployments. The script starts Airflow and the
receiver, creates a signed incoming webhook, and installs the repository hook. See
[the islo production guide](docs/islo.md).

### Other sandbox providers

The built-in choices are `local`, `srt`, `docker`, `islo`, and Airflow `toolset`. The toolset path
can load an Airflow-compatible backend for Docker Sandboxes (`sbx`) or a custom Daytona, E2B,
Tensorlake, or Box by ASCII adapter:

```bash
SWF_SANDBOX=toolset \
SWF_TOOLSET_BACKEND=your_package.backends:YourSandboxBackend \
uv run swfactory run --blueprint your-product --issue 42 --agent claude --scm github
```

A provider adapter must support reconnecting, timeouts, bounded output, path isolation, network
rules, limited credentials, cleanup, and expiry. Read the
[sandbox provider contract](skills/airflow-software-factory/references/sandboxes.md).

## Daily operation

1. **Send work.** Label an issue `factory:<route>`, comment `@factory run <route>`, trigger the DAG,
   or use the CLI.
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

## Operate it from one binary

`swf` is the operator's client: a single native executable that connects to a factory, submits work,
shows every mapped job, answers approval gates, verifies deliveries and removes owned sandboxes,
with an interactive screen over the same operations. It needs no Python, no `uv` and no virtualenv
on the operator's machine, and it holds no authority — it reads Airflow's public API and re-reads
every decision from the service that owns it before acting.

```sh
swf context add prod --airflow-url https://airflow.example.com --repo acme/widgets --use
swf doctor                              # one line per readiness check, a fix: for every failure
swf submit --issue 42                   # governed work to Airflow
swf attention                           # approvals waiting, failures, blocked deliveries
swf gates approve 'factory/manual__2026-09-06T08:04:02+00:00#1:plan'
swf deliveries verify --clone           # re-derive the delivery instead of trusting the report
swf tui                                 # the same operations, interactively
```

Every command answers `--json` with exactly one document on stdout, every diagnostic on stderr, and
a stable exit code: 1 operational, 2 usage, 3 not found, 4 authentication, 5 unreachable, 6
conflict. The config file stores the *name* of the environment variable holding a credential and
never a value. `swf` runs no stage: Airflow schedules and Python executes, exactly as before. Read
[docs/swf.md](docs/swf.md) for the full command table, the JSON contract, the key map and the
security posture. Release tarballs are attached to each GitHub Release, one per platform, with a
`SHA256SUMS` file.

## Grow useful information and remove noise

Healthy software production runs two loops:

- **Explore:** issues, specs, plans, evaluations, and telemetry add useful choices and evidence.
- **Standardize:** tests, budgets, work-in-progress limits, protected paths, review, and cleanup
  remove failed ideas, duplication, stale code, and stale documentation.

Use several narrow routes instead of one universal route. A normal feature can use `factory`; an
urgent repair can use `hotfix`; dependency upkeep can run on a schedule. Keep each job bounded with
`max_parallel_jobs`, iteration limits, timeouts, and cost limits. Promote a repeated good practice
into `factory.toml`, a blueprint, a test, or a reusable skill.

## Routes, repositories, and versions

| Need | Configuration |
|---|---|
| Normal feature or repair | manual route with intent and plan approvals |
| Urgent repair | shorter `hotfix` route with tighter limits |
| Recurring maintenance | `[trigger] kind = "cron"`, `cron = "…"`, and `issues = ["path/to/work-order.md"]` |
| Several repositories | multiple `[[targets]]`; each run maps issues × targets |
| Monorepo | set each target's `dir` and keep a `factory.toml` there |
| Larger outer workflow | use the optional [Astronomer Blueprint step](docs/astronomer-blueprint.md) |

Astronomer Blueprint can assemble several reusable factory routes in YAML or the Astro IDE. This
package exposes `software_factory`, which triggers an existing route DAG while that child DAG
keeps its mapped jobs, approvals, evidence, and run history. Install it with
`uv sync --group airflow --group astronomer-blueprint`.

Compatibility is explicit:

- Python `>=3.12,<3.13`
- Apache Airflow `3.3.1`, with an upstream-main canary in CI
- blueprint schema `version = 1`, read by swfactory `2.0.x`
- the `swf` operator binary: Rust `1.82` or newer to build, prebuilt for macOS (Apple silicon and
  Intel) and Linux (`x86_64` and `aarch64`)
- GitHub delivery and a local Git remote for the keyless demo
- target projects in any language whose contract can produce JUnit XML

Schema and package versions move independently. A blueprint schema version changes when an older
blueprint can no longer be read. Releases follow semantic versioning and are published only after
lint, unit tests, the scripted end-to-end run, Airflow parity and smoke, and package build pass.

## Commands

| Command | Purpose |
|---|---|
| `swfactory demo` | run the keyless end-to-end replay |
| `swfactory run` | run one route over issues × targets |
| `swfactory doctor` | check a live deployment before work starts |
| `swfactory herd` | view runs, approvals, pull requests, and sandboxes |
| `swfactory approve` | answer an Airflow approval gate |
| `swfactory webhook serve` | route trusted GitHub events into Airflow |
| `swfactory webhook deliveries / inspect / retry` | inspect durable dispatch receipts and recover failed submissions |
| `swfactory metrics` | aggregate committed run evidence |
| `swfactory maintain` | detect metric drift and sweep owned sandboxes |
| `swf` | the same connect, submit, watch, approve, and verify operations as one native binary, plus `swf tui` ([docs/swf.md](docs/swf.md)) |

Webhook intake persists accepted work before replying and retries Airflow submission in the
background. Repository-bound routes and stable run IDs keep redelivery from creating unrelated
jobs. See [durable webhook intake](docs/webhooks.md) for receipts, recovery, health and storage.

## Install the factory skill

```bash
npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory
```

The public skill teaches an agent how to design, adopt, operate, and audit this production system.

## Evidence and project status

The current release is `2.0.1`. [PR #2](https://github.com/zozo123/ariflow-swfactory/pull/2)
records a blocked run; [PR #3](https://github.com/zozo123/ariflow-swfactory/pull/3) records a clean
run that stopped at human merge. This project is alpha. Read [SECURITY.md](SECURITY.md) before
connecting a production repository.

## Repository map

```text
blueprints/        production routes and their limits
dags/              generated Airflow DAGs, approvals, and maintenance
src/swfactory/     runtime, stages, adapters, state, policy, and CLI
rust/              the swf operator binary: domain, adapters, operations, TUI, CLI
skills/            installable Airflow software factory skill
deploy/docker/     local Airflow and Docker work-cell stack
deploy/islo/       hosted control plane and MicroVM work cells
docs/              design, deployment, evaluation, and operations guides
site/              GitHub Pages guide
```

[Design and schema](docs/design.md) · [Docker](docs/docker.md) · [islo](docs/islo.md) ·
[Factory floor](docs/herd.md) · [swf binary](docs/swf.md) · [Evaluation](docs/evals.md) ·
[Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

Apache-2.0
