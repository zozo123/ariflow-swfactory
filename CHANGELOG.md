# Changelog

All notable changes to this project will be documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.2.0] - 2026-09-08

### Removed

- Collapsed the Liquid/physics/legacy vocabulary layer: 67 files, 2,445 lines. The 18
  `liquid_bundle_*`, 18 `physics_bundle_*` and 4 `legacy_bundle_*` modules were declarative
  coverage metadata expressed as Python — `execute`, `execute_wave` and `run` were pure functions
  returning frozen dataclasses, so nothing executed. With them go `liquid_bundle_engine`,
  `physics_wave_runtime`, `legacy_issue_runtime`, `liquid_release`, `non_equilibrium`, the five
  dead `liquid_*_runtime` modules, the seven single-test `liquid_*` shims, and their tests. This is
  the methodology's own C10 applied to itself: more issue slices must not imply more permanent
  abstractions. `liquid_security_runtime` and `liquid_workgraph_runtime` are retained — they are
  imported by `core_capabilities` and `backend.scm_service`.
- The statistical-mechanics vocabulary (Jarzynski, Crooks, Onsager, nucleation, the
  Ocean120/Phase240/StatMech360 waves) is out of the product path and documented in
  [docs/research/](docs/research/README.md) with the falsifiable-prediction bar it must clear to
  return. The one idea worth keeping needs no equations: create entropy where exploration benefits
  from it, destroy it before promotion.

### Added

- `config/liquid-spec.yaml` and `python -m swfactory.liquid_spec`: the 90-domain x 10-concern
  matrix as data, with a checker that resolves all 45 distinct `runtime_anchor` values to real code
  under `src/swfactory/` and every `capability_claim` to `config/capability-inventory.json`. The
  matrix is now falsifiable rather than decorative, and `state`/`support` keep declared scope
  separate from validated behaviour. It replaces `swfactory.liquid_release` as the required check.
- The checker reports `duplicate_slugs`, which surfaces a defect the old gate could not see: the
  90 domain slots hold only 84 distinct slugs, and five of the six duplicates carried contradicting
  owners. `BundleSpec.validate` checked uniqueness only within one bundle and the manifest counted
  slots, so "one owner per domain" was never an invariant.
- Shared CLI/DAG graphic in the README and website, plus an interactive 11-station walkthrough
  that pauses at both simulated human gates. It is explicitly illustrative and makes no live calls.
- Rewritten README with a concise console/backend setup; the full deployment reference is preserved
  in `OPERATIONS.md`.

- Shared CLI/DAG graphic in the README and website, plus an interactive 11-station walkthrough
  that pauses at both simulated human gates. It is explicitly illustrative and makes no live calls.
- Rewritten README with a concise console/backend setup; the full deployment reference is preserved
  in `OPERATIONS.md`.

### Changed

- The Rust console now connects to a Python factory backend by default. Backend API v1 owns
  service credentials, installed-line validation, work-order submission with target selection,
  fresh approval readiness checks, worker cleanup, metrics and run-recovery inspection.
- Added `swfactory backend`, context `--backend-url` and explicit `--direct` migration mode,
  plus a Docker `console` profile. See [the backend contract](docs/factory-backend.md).
- Rust preserves the existing paginated Airflow response contract and bulk approval behavior;
  backend transport never retries uncertain mutations or silently falls back to local credentials.

## [2.1.0] - 2026-09-06

The operator gets a native binary. `swf` is one executable that drives a factory environment over
its API — submit work, watch runs and mapped jobs, read a log, answer an approval gate, verify a
delivery, sweep owned sandboxes — with no Python, uv or virtualenv on the machine it runs from, and
it ships as a release asset for macOS and Linux. Airflow still schedules and Python still executes
every stage, and nothing on the 2.0 public surface changes, so this is a minor release. Operators
running a wide fan-out should read the control-room pagination fix below: it was returning short
tables.

### Added

- Host run ownership across preparation, setup, stages, approvals and cleanup, with durable
  operation-attempt history and interrupted-attempt detection. Competing mutations fail before
  changing the sandbox or the authoritative stage result; teardown has two Airflow retries.
- `swfactory state list` and `state inspect` expose local ownership, journal health, recent
  operations and recorded spend without contacting a sandbox or agent.
- Journal recovery now archives torn trailing bytes before the next append, including split UTF-8
  characters, while refusing corruption in committed records. Atomic state writes also sync
  directory entries, and new state directories and files use private permissions.
- Agent budgets are capped to the remaining run allowance, refreshed under stage ownership,
  and charged before downloading result envelopes. See [run recovery](docs/run-recovery.md).

- Durable webhook intake with a persistent SQLite inbox, leased worker claims, bounded retries,
  backoff and dead dispatch receipts. Accepted events survive receiver restarts and Airflow
  outages; stable run IDs and verified conflict recovery preserve submission identity.
- `swfactory webhook deliveries`, `inspect` and `retry` expose dispatch state and recover one
  failed submission without creating a new run. `/readyz` reports intake readiness, queue counts
  and pending age; inbox path, capacity and attempts have explicit CLI/environment settings.
- Webhook admission validates source repositories against installed blueprints, restricts mapped
  targets to the originating repository and records minimal provenance in Airflow configuration.
  Docker and islo keep receipts beside Airflow's persistent state and can start intake before
  the Airflow API is reachable when credentials are available. See [webhooks](docs/webhooks.md).

- `swf`, a native operator binary, is now built and released alongside the Python package. One
  executable connects to a factory environment, submits governed work, lists runs and mapped jobs,
  reads a task attempt's log, reviews and answers approval gates, verifies deliveries, removes
  owned sandboxes and drives the local Docker stack, with no Python, uv or virtualenv on the
  operator's machine. `swf tui` is a full-screen interface over the same operations layer, so a
  command and a keystroke cannot answer a gate differently. Airflow still schedules and Python
  still executes every stage; `swf` is a client and holds no authority. See [docs/swf.md](docs/swf.md).
- `swf`'s command names, `--json` document keys and exit-code table are public surface under the
  project's semantic versioning policy: 1 operational, 2 usage, 3 not found, 4 authentication,
  5 unreachable, 6 conflict. `--json` prints exactly one document to stdout and every diagnostic to
  stderr, and a failure still prints a parseable `{"error": {…}}` envelope carrying its own
  `exit_code`.
- `swf context` records factory environments in a `config.toml` written at mode `0600` that holds
  the *name* of the environment variable a credential lives in and never a value. A literal
  `password`, `token` or `secret` key anywhere in that file is a load error, not a warning, and
  `swf context show` redacts to the variable name.
- `swf gates approve` re-reads the gate immediately before answering, refuses one whose task has
  not parked in `awaiting_input` on two consecutive polls unless `--force` is given, and accepts
  `--expect <revision>` so an approval fails with a conflict when the evidence moved under the
  operator.
- `swf deliveries verify` keeps "the workflow said so", "a branch was published" and "we re-derived
  it" as three separate verdicts rather than one tick; `--clone` is what makes the third reachable
  at all. `swf runs stop` says what Airflow actually does — it marks the run failed — in its help,
  its output and its JSON, which records `processes_stopped` and `sandboxes_removed` as false.
- Two CI jobs cover the binary: `rust` runs format, clippy as errors, the workspace suite and the
  release build; `contract-equivalence` asserts that both languages produce the recorded answers in
  `tests/fixtures/contract/`, in one job over one checkout. `scripts/swf_e2e.sh` drives a live
  Airflow through submit, eight authenticated approvals and four independently verified deliveries
  with `swf` as the only client.
- Each GitHub Release now attaches a `swf` tarball for macOS (Apple silicon and Intel) and Linux
  (`x86_64` and `aarch64`), each with shell completions generated by the binary itself, plus a
  `SHA256SUMS` file covering every asset in the release.
- Live E2E now clones every published branch, checks remote branch isolation and published
  approvals, and reruns the target's tests from that clean delivered checkout.
- Reproducible Airflow-main installation with commit provenance checks and both web UIs built
  from source. The upstream common.ai sandbox provider is now the default development provider;
  `--islo` explicitly selects the experimental fork.
- Main CI runs the complete suite and live scheduler/HITL E2E without reinstalling the release.
  Live E2E now verifies gate decisions and admin attribution, not just DAG success.
- Configurable sbx host policy declaration and factory image, plus an opt-in real microVM
  file/command/cleanup test.
- Scheduled blueprints can declare `trigger.issues`; cron-created DAG runs use those issue inputs
  when no runtime configuration is present, while explicit run configuration still wins.
- `swfactory doctor` now validates only the selected sandbox, agent, and SCM providers. It checks
  Docker and Airflow Toolset backends directly, treats SRT as required when selected, and reads a
  target repository's `factory.toml` through GitHub when it is not in the control checkout.

### Changed

- The design reference's "No Rust" decision is now stated precisely instead of broadly. The
  reasoning it recorded still holds and is unchanged: the sandbox guard hook stays `python3`,
  because the islo work-cell image has `/usr/bin/python3` and no pip or uv, and Claude Code's
  native deny rules are the primary gate. What that bullet never covered is the operator's own
  machine, which is what `swf` serves. Stage semantics stay in `stages.py`.
- Toolset reconnect failures preserve the existing sandbox handle and run state instead of
  silently provisioning an empty replacement. Backend-reported termination is persisted across
  task restarts and blocks continuation of the old run while retaining cleanup support.
- The README, site, design reference, and public skill now use a practical factory vocabulary:
  work orders, production routes, plant scheduling, work cells, quality checks, release, and
  continuous improvement. The README includes a full real-repository setup and deployment guide.
- The feedback-loop language states exactly which delivery signals it measures and when the
  default maintainer sees merged evidence.
- The hosted islo deployment now separates the factory control repository
  (`SWF_CONTROL_REPO`) from the product repository receiving webhooks and pull requests
  (`SWF_TARGET_REPO`). The previous `SWF_REPO` / `SWF_BRANCH` inputs remain compatible aliases.
- GitHub workflows use the current Node 24 checkout action.

### Fixed

- The Python control room now pages every Airflow collection instead of trusting one request.
  Airflow silently clamps `limit` to `[api] maximum_page_limit` (default 100), so the old
  `limit=500` read of a run's task instances returned the first 100 and dropped the rest — the
  wide run an operator opened the control room for was exactly the one whose jobs went missing.
  Runs, DAGs, task instances and pending gates are walked by `offset` until the collection is
  exhausted; a read that hits the page cap keeps its rows and reports `airflow:truncated` in the
  snapshot rather than passing a short table off as the whole factory. This is the behavior `swf`
  already had, so the two clients now agree on a large fan-out.
- `TARGET_DIR=` now selects a repository-root target during islo bootstrap, matching the documented
  empty-directory behavior instead of falling back to `demo/target`.
- The README's Docker rehearsal uses the shipped image and Compose file names.
- The islo bootstrap preflight now checks the requested target repository and directory instead of
  the demonstration target.

## [2.0.1] - 2026-09-06

### Fixed

- Linux Sandbox Runtime runs now ignore only its absent, mandatory auto-protected mount
  placeholders. Real project dotfiles remain tracked, and the SRT scripted end-to-end lane passes.
- Eval issues use safe factory-relative references, duplicate gate definitions fail before their
  artifact details are inspected, and missing Toolset files preserve the sandbox protocol's
  `FileNotFoundError` contract.
- Airflow gate tests seed the authoritative host artifact chain and assert its approval digest,
  matching production task isolation and retry behavior.

## [2.0.0] - 2026-09-05

The factory now treats every model, sandbox, identifier, and artifact as an explicit trust-boundary
input. This release intentionally tightens configuration and adapter contracts rather than
preserving permissive 1.x behavior.

### Validation

- The 2.0 changes were reviewed statically before tagging. The required version-tag workflow runs
  lint, the hermetic suite, scripted e2e demo, Airflow parity and smoke, and the package build; it
  publishes the release only after all gates pass. Live hosted-provider and Astronomer Blueprint
  runs remain deployment validation, while the 1.1 results below are historical evidence.

### Added

- Atomic host-owned run state for identity, baseline, target contract, review policy, approvals,
  review verdicts, sandbox handles, invocation costs, and stage journals. Delivery reconstructs
  its audit artifacts from this state instead of trusting files in the agent checkout.
- Strict, shared validation for external identifiers, GitHub repositories, Git refs, local paths,
  and remote POSIX paths; boundary models reject unknown fields and invalid numeric values.
- Reconnectable Airflow `SandboxBackend` cells with a persisted provider identity and handle,
  concrete network and environment requirements, output limits, target checkout provisioning,
  confined file access, timeout propagation, and custom `package.module:Class` adapters for
  Daytona, E2B, Tensorlake, Box by ASCII, and future providers.
- `airflow-software-factory`, a public installable agent skill with the line, evidence, failure,
  and sandbox-provider contracts.
- An optional Astronomer Blueprint integration. The registered `software_factory` template turns
  an existing governed line into a deferrable composable step for DAG YAML and the Astro IDE while
  retaining the child DAG's mapping, HITL gates, and authority boundary.
- A new control-room site and repository visual system, including the generated factory-line hero,
  provider fabric, operating model, trust topology, and concise onboarding path.

### Changed

- Claude Code runs in restricted mode with an explicit tool inventory and no permission prompts.
  Spec, plan, and review are read-only. Build and fix may edit files but receive no shell; the
  trusted stage runner owns verification, commits, and delivery.
- Guard settings and the hook live below ignored `.factory/`. The factory no longer writes an
  internal skill or exclusion rules into a target's `.claude/` tree.
- Operational environment settings may override line defaults, but mapped job identity always
  wins for issue, repo, target, branch, run id, and line name.
- Factory assets resolve from either a source checkout or the installed wheel. Shipped blueprints,
  demo fixtures, guard, policy, and deployment files are included in the distribution.
- Blueprint shape, stage dependencies, gate artifacts, target contracts, review verdicts, and
  additive tool overrides are fail-closed. Shell escalation and writes in read-only stages are
  rejected during configuration.

### Fixed

- A zero exit status without fresh, non-empty, internally consistent JUnit is no longer green.
- Verification cannot mutate source outside factory artifacts, and delivery refuses unreviewed
  workspace dirt or broad `git add` behavior.
- Failed agent calls retain their spend and journal entry, malformed trailing journal records are
  handled safely, and sandbox-side copies cannot skip stages or forge approvals.
- Root local targets no longer become convincing empty repositories: they must be launched from a
  checkout containing `factory.toml`; missing target directories fail before Git initialization.
- Webhook routing now constrains trusted forwarding hosts, parses loopback addresses correctly,
  caps request bodies, and rejects credential-bearing or query-bearing Airflow URLs.
- Credential scrubbing covers major provider families, exact secret variables, and conventional
  secret-name suffixes.

### Security

- Approvals bind ordered gates to exact artifact digests. Delivery validates the required stage
  sequence, approvals, host-recorded review, baseline, clean workspace, patch scope, symlink modes,
  and secret scan before the first publication call.
- Toolset backends must enforce requested network policy or reject creation; truncation, timeout,
  and terminated cells can no longer masquerade as successful commands.

## [1.1.0] - 2026-09-03

Airflow grew a sandbox abstraction of its own, and this release adopts it instead of competing with
it: the factory gains a sandbox kind and a dependency, and nothing on the 1.0 public surface
changes, so this is a minor release.

### Added

- `--sandbox toolset`: `ToolsetSandbox` adapts the `SandboxBackend` that ships in Airflow's
  `common.ai` provider — its `create` / `destroy` / `run_command` / `read_file` / `write_file` is
  the same shape as this factory's `Sandbox` protocol — so every backend the Airflow community
  ships becomes a swfactory sandbox with no change to the trust boundary. The backend is chosen by
  `toolset_backend` (`SWF_TOOLSET_BACKEND`, default `sbx`) and the sandbox root by
  `toolset_workdir`; four backends are registered. `sbx` resolves from the released provider today;
  `islo`, `opensandbox` and `asciibox` are pending upstream
  ([apache/airflow#71672](https://github.com/apache/airflow/pull/71672),
  [#71676](https://github.com/apache/airflow/pull/71676),
  [#71725](https://github.com/apache/airflow/pull/71725)) and a pending backend raises a
  `StageError` naming its pull request rather than crashing on import.
- `apache-airflow-providers-common-ai>=0.8.0` joins the `airflow` dependency group, so
  `uv sync --group airflow` plus `--sandbox toolset` works out of the box against the pinned
  `apache-airflow==3.3.1` release.
- `scripts/airflow_main.sh` puts a checkout on `apache/airflow@main` plus the pending islo backend
  in one command (`--pypi` takes the released provider instead), and the optional CI job
  `airflow-main-sandbox-toolset` runs that same overlay. A script rather than a locked dependency
  group on purpose: locking a git dependency on the Airflow monorepo clones roughly a gigabyte and
  pins a commit that is stale the next day.
- [docs/design.md](docs/design.md) records the factory proven on Airflow built from source, not only
  against the release: airflow 3.4.0 with task-sdk 1.4.0 and providers-standard 1.18.0 from main,
  plus common-ai 0.7.0 from the islo backend PR branch, `islo` resolving to
  `IsloSandboxBackend`, 31 Airflow tests
  passing, and `airflow dags test factory --mark-success-pattern 'job\.approve_.*'` finishing all
  14 tasks `state=success`.

### Fixed

- `ToolsetSandbox` no longer requires Airflow to be installed: `_spec()` hard-imported the
  `common.ai` provider, so injecting a backend directly — what the tests do — crashed in an
  Airflow-free environment. It now returns `None` and the backend is created with `spec=None`.
  Local runs hid this because the dev venv has Airflow and CI's default job does not.
- The experimental main-track install asked uv for two different git URLs for the same package
  (`apache/airflow@main` and the PR branch), which uv rejects. Every Airflow distribution now comes
  from one repository; the PR branch is `main` plus the backend anyway.

## [1.0.0] - 2026-09-03

First stable release: the public surface (blueprint schema, `SWF_*` config, the `Sandbox` /
`Agent` / `Scm` protocols, the CLI verbs, the committed artifact chain) is now covered by
semantic versioning — see "Versioning and release" in [docs/design.md](docs/design.md). The
sections below cover the whole history since the initial scaffold, because `0.1.0` was a
development snapshot that was never tagged or published.

### Added

- Blueprint-driven lines: `blueprints/<name>.toml` (stage order, human gates, limits, targets,
  sandbox profile, PR labels) is validated by `swfactory.blueprint.Blueprint` and becomes both one
  `swfactory run --blueprint <name>` line and one Airflow 3 DAG. Two lines ship with zero Python
  between them: `default.toml` (`factory`) and `hotfix.toml` (no `spec` stage, self-approving
  intent gate, extra `hotfix` label).
- Generated DAGs: `dags/blueprints.py` emits one DAG per blueprint and fans `issues x targets` out
  into a mapped `job` task group with dynamic task mapping, `max_parallel_jobs` at a time, so each
  (issue, target) pair gets its own sandbox, PR and addressable approval. `tests/test_dag_parity.py`
  asserts every DAG mirrors its blueprint; `tests/test_dag_smoke.py` runs the default line end to
  end through `dag.test()`.
- Human gates as first-class state: `GateOperator` (an `ApprovalOperator` that never skips its own
  child) records Airflow's `responded_by_user` into `approvals.json`, and a rejected gate still
  delivers a `[REJECTED]` PR labeled `factory:rejected` instead of vanishing.
- Six stage functions (`intent`, `spec`, `plan`, `build_and_test`, `review`, `deliver`) in
  `stages.py`, with the build and review-fix loops inside the stage functions and every loop bounded
  by `Config`; exhaustion is a `StageError(kind="policy")` or a `factory:blocked` PR, never a retry.
- Four sandbox kinds behind one `Sandbox` protocol: `local` (no boundary; demo, tests, CI), `srt`
  (Anthropic Sandbox Runtime — macOS Seatbelt / Linux bubblewrap, per-stage kernel `denyWrite`,
  egress domain allowlist), `docker` (a bind-mounted container per command with protected prefixes
  mounted `:ro` per stage), and `islo` (Firecracker-class MicroVM behind a deny-by-default gateway
  with a phantom `ANTHROPIC_API_KEY`) — the production boundary.
- `swfactory herd`: a Textual control-room TUI over `control.py` for pending gates
  (approve/reject), runs, factory PRs, your own sandboxes and metrics; `control.py` owns the
  Airflow, `gh` and `islo` clients and `herd.py` is presentation only, so the whole TUI is
  unit-tested with fakes and no network.
- `swfactory doctor [--json]`: read-only preflight of the real path (islo/gh/claude/srt CLIs,
  `islo login` and tool integrations, gateway profile, environment, snapshot, `gh auth` and repo,
  blueprint, target `factory.toml`), exit 1 per required red row, each carrying its own `fix:`.
- `swfactory webhook serve|route`: a stdlib GitHub issue/comment receiver on the orchestrator that
  POSTs `/api/v2/dags/<name>/dagRuns`, with `GET /healthz` and a pure, unit-tested `route`.
  `.github/workflows/dispatch.yml` is the hosted-Airflow-free alternative.
- islo orchestrator deploy: `deploy/islo/bootstrap.sh` (gateway profile, environment, snapshot,
  knowledge items) then `deploy.sh` (orchestrator sandbox running Airflow plus the webhook receiver,
  wired to an islo incoming webhook), plus `knowledge.sh` and [docs/islo.md](docs/islo.md).
- `runtime.py`: the single `(blueprint, job, run id) -> Ctx` assembly used by both the CLI and every
  Airflow task, so a retried task lands on the same run dir and sandbox.
- Delivery that the agent cannot perform: `deliver` pulls `git format-patch <base>..HEAD` out of the
  sandbox and applies it on the orchestrator (`git am`, push `factory/<issue>-<run>`,
  `gh pr create`). Delivery is retry-safe — `factory/*` is the bot-owned branch namespace,
  force-updated on a retried `deliver`, and an open PR for the branch is edited in place.
- Run state on the orchestrator: every `StageResult` is appended to `.factory/<run_id>/stages.jsonl`
  and a stage is `skipped` only when that log holds a completed record, so an artifact forged in the
  agent-writable sandbox cannot skip a stage; `budget_usd` is seeded from the same log and holds
  across tasks and workers.
- Committed artifact chain under `docs/factory/<issue>/` authored by `swfactory-bot` with
  `Factory-Run` / `Factory-Stage` / `Agent` trailers: `intent.md`, `spec.md`, `plan.json` +
  `plan.md`, `review.json`, `approvals.json`, `metrics.json`, and `agent/` copies of the stage and
  hook logs.
- Metrics and maintenance: `swfactory metrics --root` summarises committed runs (first-pass rate,
  mean iterations, p50 cycle, findings, cost); `swfactory maintain --root` applies the three
  `bands.yaml` tiers (log / diagnose / propose) and sweeps orphan `swf-*` sandboxes; `dags/maintain.py`
  runs nightly at 03:00 UTC and after every delivery via the `swf.metrics.<blueprint>` asset.
- Keyless end-to-end replay: `swfactory demo [--sandbox srt|docker] [--real]` replays recorded
  fixtures from `demo/scripted` against `demo/target` in about ten seconds, and `--record <dir>`
  produces them from a real run. The hermetic test suite (fake subprocess, tmp git repos, no
  network) is the same path.
- CI in `.github/workflows/`: `ci.yml` (`test` = ruff + pytest + scripted demo, `airflow-parity`,
  `srt-smoke`, `docker-smoke`, and an optional `airflow-main` canary that runs DAG parity and smoke
  against upstream `apache/airflow@main`), and `evals.yml`, which runs the real Claude agent weekly
  on `demo/issue.md` in two jobs — `real-demo` under srt and `evals-islo` in an islo MicroVM with no
  Anthropic key on the runner — asserting on the same `report.json`.
- Fully local Docker stack: `deploy/docker/compose.yml` with Airflow and the webhook receiver plus
  sandbox and Airflow images ([docs/docker.md](docs/docker.md)).
- The cyanotype project site at `site/` — responsive, accessible, SEO and social assets, a custom
  404, static contract tests (`tests/test_site.py`) and GitHub Pages deployment
  (`.github/workflows/pages.yml`), published at
  <https://zozo123.github.io/ariflow-swfactory/>.
- Repository hygiene: complete Apache-2.0 license text, `CONTRIBUTING.md`, `SECURITY.md`,
  `.github/CODEOWNERS`, structured issue forms and a pull-request template, and Python package
  metadata with a `swfactory` console script.

### Changed

- One runtime assembly: 114 lines of duplicated wiring (run ids, run dirs, workdir seeding,
  protected globs, scm/sandbox/agent construction, issue fetch) were deleted from `cli.py` and
  `dags/blueprints.py` in favour of `runtime.py`. This resolved three real CLI/DAG divergences —
  DAG runs now seed the host workdir and pass `protected` before `make_sandbox` (srt and docker
  previously got neither), and `repo` always reaches the sandbox name.
- One metrics reader: `metrics.load_all(root, include_scripted=, newest_first=)` is the only glob
  over `docs/factory/*/metrics.json`, and `maintain.load_runs` is a call into it; `maintain`'s
  private timestamp and glob helpers are gone.
- Docs are focused: `README.md` is an entry point (368 -> 214 lines) and the long form lives in
  `docs/design.md`, `docs/islo.md`, `docs/docker.md` and `docs/herd.md`; the `deploy/*` READMEs are
  commands plus a link.
- `SWF_*` environment variables now override blueprint values and CLI flags alike (env > init), the
  documented dev and smoke escape hatch.
- `setup` seeds the local workdir so the CLI and the DAG share one path, and the target contract is
  read lazily so a per-run workdir works under `airflow dags test`.
- The `spec` stage keeps only the document, stripping any agent preamble before the first heading;
  the prompt says so.
- Airflow is pinned to `apache-airflow==3.3.1` with `apache-airflow-providers-standard==1.18.0`.

### Fixed

- The build stage may write tests again: the `tests/` protection is applied only during `fix` tasks.
  A live run found this — a review had correctly reported "no tests" as a blocker the agent could
  not resolve.
- srt on Linux: deny rules cover only existing paths (bubblewrap refuses to bind a missing one), and
  neither `setup` nor `commit` stages srt's Linux shell-rc stubs (`.bash_profile`, `.bashrc`,
  `.profile`, `.gitconfig`, `.npmrc`, `.zshrc`, `.inputrc`) or any other non-regular root dotfile;
  `.git/info/exclude` is seeded with `sb.write` rather than a confined shell.
- Docker sandbox on Linux runs as the host uid with a writable `/tmp` home, and the sandbox image
  installs `uv` system-wide so any uid can use it.
- Local SCM snapshots and the srt uv cache are stable, which unblocked the `srt-smoke` and
  `docker-smoke` CI jobs; srt on Linux also needs `ripgrep` installed.
- The `maintain` DAG never reads metrics relative to a worker's cwd: it uses `$SWF_MAINTAIN_ROOT` or
  a shallow read-only clone of the target's base branch, and fails loudly when there is no
  `docs/factory/`. Its run id is derived the same way as every other task's.
- Production-path and srt startup hardening, gate `assigned` users reaching Airflow's HITL
  `assigned_users`, and a run-level (not per-task) budget ceiling.
- The Docker sandbox row is back in the README's sandbox comparison table, and the stale hard-coded
  test count is gone from the development instructions.

### Security

- `deliver` never skips, and before any git or network call it runs `scm.validate_patch` (no
  absolute paths, no `..`, nothing under `.git/`, no symlink modes, every path under the target dir
  or `docs/factory/`) and a secret scan. A hit is `StageError("policy")` and nothing is published.
- The sandbox holds no GitHub credential in any of the four kinds, so "the agent never pushes or
  merges" is structural rather than a prompt: `Scm` has no merge method and the sandbox has nothing
  to push with.
- No `--env` or `--env-file` on islo argv and no tokens inside a sandbox; `scrub_env` strips
  `ANTHROPIC_*`, `GH_TOKEN`, `GITHUB_TOKEN`, `AWS_*` and `ISLO_API*`, and only the islo path gives
  the agent a phantom `ANTHROPIC_API_KEY` swapped on egress by the gateway.
- Sandbox ownership safety: the sweep deletes only sandboxes it owns — own-scope listing, a
  `created_by` owner match and the `swf-*` factory name pattern — and refuses to run at all without
  a resolvable owner.
- Claude Code's native `permissions.deny` rules are the primary gate (checked before hooks and not
  bypassable by hook output): the artifact chain, the stage scratch, `REVIEW.md`, `factory.toml`,
  `.claude/**`, `.github/**` and `.env*` are unwritable or unreadable, and `git push`, `git commit`,
  `gh pr`, `curl` and `wget` are denied by substring. The `swf_guard.py` PreToolUse hook is
  defense-in-depth and writes the `hooks.jsonl` audit log; denied calls surface as
  `denied_tool_calls` in `metrics.json` and in the PR body.
- `dispatch.yml` requires an `https://` Airflow URL and refuses to follow redirects while carrying
  the token.

## [0.1.0] - 2026-09-03

### Added

- Blueprint-driven Airflow 3 software factory with human approval gates.
- Local, SRT, Docker, and islo execution paths.
- Scripted keyless end-to-end replay and hermetic test suite.
- GitHub delivery, issue dispatch, control room, maintenance bands, and webhook receiver.

[Unreleased]: https://github.com/zozo123/ariflow-swfactory/compare/v2.1.0...HEAD
[2.1.0]: https://github.com/zozo123/ariflow-swfactory/releases/tag/v2.1.0
[2.0.1]: https://github.com/zozo123/ariflow-swfactory/releases/tag/v2.0.1
[2.0.0]: https://github.com/zozo123/ariflow-swfactory/releases/tag/v2.0.0
[1.1.0]: https://github.com/zozo123/ariflow-swfactory/releases/tag/v1.1.0
[1.0.0]: https://github.com/zozo123/ariflow-swfactory/releases/tag/v1.0.0
[0.1.0]: https://github.com/zozo123/ariflow-swfactory/releases/tag/v0.1.0
