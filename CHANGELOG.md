# Changelog

All notable changes to this project will be documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Nothing yet.

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

[Unreleased]: https://github.com/zozo123/ariflow-swfactory/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/zozo123/ariflow-swfactory/releases/tag/v1.0.0
[0.1.0]: https://github.com/zozo123/ariflow-swfactory/releases/tag/v0.1.0
