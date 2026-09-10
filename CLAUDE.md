# swfactory — institutional knowledge (one page)

A blueprint (`blueprints/<name>.toml`: stage order, gates, limits, targets, sandbox, labels) is one
Airflow 3 DAG (`dags/blueprints.py`, jobs = issues x targets via task mapping) and one
`swfactory run --blueprint <name>` line. Claude Code works in a sandbox holding no GitHub credential
(islo MicroVM in production, srt or a container on a dev box); the orchestrator alone talks to
GitHub (`git am` a format-patch stream, push, `gh pr create`); a human merges. Stage semantics live
in `stages.py`. Details: README + docs/*.md.

## Commands
- `uv sync`; `uv run pytest` — the hermetic suite (fake subprocess, tmp git repos, no network).
  `addopts` already carries `-q`; a second `-q` hides the pass/fail summary line, and `| tail`
  hides the exit code — read `${pipestatus[1]}` (zsh) before calling a run green.
- `uv run ruff check . && uv run ruff format --check .` — line length 100; E,F,I,B,UP,SIM.
- `uv run swfactory demo [--sandbox srt|docker] [--real]` — scripted replay, no keys, ~10 s;
  `--real` runs claude in an islo sandbox and opens a real PR.
- `uv run swfactory run --issue <n|path> --agent claude --sandbox srt --scm local|github`
  — direct CLI path without Airflow. `--blueprint hotfix --issue demo/issue.md` = second line.
  A gate declared `mode = "human"` must be answered; `SWF_GATE_REPLAY=demo/gate-replay.json` is the
  only unattended substitute, and it is refused for any run that can reach outside itself — a
  backend-managed Cell, or `--scm github`. `SWF_APPROVE=auto` can no longer satisfy a human gate.
- `uv run swfactory approve <dag_run_id> intent|plan [--reject] [--map-index <j>]`; `doctor
  [--json]` (exit 1 per red row, with a `fix:`); `metrics|maintain --root .`; `herd`; `webhook`.
- `cargo test --manifest-path rust/Cargo.toml --workspace`, `cargo fmt`/`clippy -- -D warnings` —
  run these after ANY change to `blueprints/*.toml`, backend HTTP shapes or CLI surfaces, not only
  to `rust/`: the crates are a second reader of those contracts (`deny_unknown_fields`, and a test
  that parses every shipped blueprint), and two PRs went red in CI for skipping them.
  the `swf` operator binary in `rust/` (docs/swf.md). It drives the same Airflow/`gh`/`islo`
  interfaces as `control.py` only in explicit `--direct` mode. Normally it connects to the Python
  `backend.py` API; service credentials and work-order validation live there. It runs no stage. `contract-equivalence` CI asserts both languages
  produce `tests/fixtures/contract/`; `scripts/swf_e2e.sh` is the live acceptance test.
- `swfactory backend` serves API v1 on loopback:8082; requires `SWF_BACKEND_TOKEN`. See
  docs/factory-backend.md for the Rust/Python boundary, service credentials and migration.
- `swf context add <name> --backend-url <url> --airflow-url <public-ui-url>` (the config holds env var NAMES,
  never values), then `doctor | submit --issue <n> | attention | jobs list | gates review|approve
  <dag/run#i:gate> | deliveries verify --clone | snapshot --json | tui`. Exit codes are contract:
  1 operational, 2 usage, 3 not found, 4 auth, 5 unreachable, 6 conflict. `runs stop` only marks
  the Airflow run failed — it stops no process and removes no sandbox.
- `uv run python -m swfactory.evals [--only <slug>] [--update-baseline]` — the eval suite in
  `demo/evals/**` scored against its `baseline.json`; a regression fails CI (docs/evals.md).
- `uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py` — DAG tests;
  `airflow dags test` never resolves HITL gates (add `--mark-success-pattern 'job\.approve_.*'`).
- `deploy/islo/bootstrap.sh` (gateway, environment, snapshot, knowledge) then `deploy.sh`
  (orchestrator sandbox + webhooks) — docs/islo.md.

- `./scripts/airflow_main.sh` — install one verified apache/airflow@main snapshot and build both
  UIs (Node 22+, pnpm 10.28.1). `--islo` opts into the pending provider fork. Use `uv run --no-sync`
  afterwards; `SWF_AIRFLOW_NO_SYNC=1 scripts/stress_airflow.sh` tests live HITL gates on that stack.
  `uv sync --group airflow` returns to the pinned release.
- Toolset reconnect failures must preserve the existing handle: never recreate an empty VM while
  the run journal still records completed stages. Persist termination and require a new run.
- Webhook CLI intake commits to `SWF_WEBHOOK_INBOX` before 202; the background dispatcher uses
  leased claims and stable Airflow run IDs. Never acknowledge a 409 without reading that exact
  run and comparing its complete conf. Source-repository targets and provenance are frozen in
  the receipt. `webhook retry <delivery-id>` reopens only dead dispatches and preserves identity;
  it must never clear or rerun a delivered Airflow job. See docs/webhooks.md.
- Toolset sbx: `SWF_TOOLSET_SBX_HOST_NETWORK_POLICY=deny-all` declares an already-configured worker
  policy; it never changes the host. `SWF_TOOLSET_SBX_IMAGE` selects the factory-ready image.
  `SWF_TEST_LIVE_TOOLSET=1 uv run --no-sync pytest tests/test_toolset_live.py` exercises a real
  microVM with explicitly open networking; normal factory runs keep their restrictive spec.
- Release = push tag `v<pyproject version>` (a mismatch fails the gate before anything builds) and
  a `## [X.Y.Z]` CHANGELOG section, which IS the body. `workflow_dispatch` dry-runs every leg and
  publishes nothing. Assets: `swf-<v>-<target>.tar.gz` x4 + wheel + sdist + one `SHA256SUMS`;
  completions come from the built binary, whose `--version` must equal `rust/Cargo.toml`'s
  `[workspace.package]` — bump both. Install: docs/swf.md#install.

## Conventions
- Experimental `WorkExecutor` enables forks only when every node sets `parallel_safe=True`;
  default or mixed plans use the conservative serial fallback even on fork-capable providers.
- Python 3.12, `from __future__ import annotations`, type hints, docstrings that say WHY. Stdlib
  first (`subprocess`, `tomllib`, `statistics`, `xml.etree`). No Airflow import under `src/`;
  `dags/*.py` import swfactory only inside task callables (the parity test asserts it).
- Protocols: `Sandbox` (local/srt/docker/islo/toolset), `Agent` (claude/scripted), `Scm`
  (local/github).
  Stages are functions `Ctx -> StageResult` in `STAGES`; loops live inside stage functions, never in
  the DAG; task mapping fans out over jobs only (nested expansion is unsupported in Airflow 3.3.1).
- `runtime.py` is the only place a `(blueprint, job, run id)` triple becomes a `Ctx`; the CLI and
  every Airflow task call it, so a retried task lands on the same run dir and sandbox.
- Blueprints may only ADD allowed tools or set a model per stage; gates only after `intent`/`plan`;
  `ttl_s > max gate timeout`. Operational `SWF_*` settings override line defaults; issue, repo,
  target, branch, run id, and line identity always come from the mapped job.
- Artifacts are committed under `docs/factory/<issue>/` by `swfactory-bot` with `Factory-Run`/
  `Factory-Stage`/`Agent` trailers; `.factory/` is uncommitted orchestrator scratch.
- Every stage is idempotent: a completed record in the orchestrator's log
  `.factory/<run_id>/state/stages.jsonl` -> `status="skipped"`. The agent-writable sandbox is never
  consulted for skips or the budget. Loops are `Config`-bounded; exhaustion is
  `StageError(kind="policy")` or a `factory:blocked` PR, never a retry.
- Preparation, setup, stages, approval recording and teardown hold the same `state/run.lock`
  through their mutation. Never delete that file to recover ownership; POSIX releases the lock
  when its owner exits. `operations.jsonl` records attempts separately from authoritative stage
  results. `swfactory state list|inspect` reads local ownership and journal evidence without
  reconnecting to a cell. See docs/run-recovery.md for interrupted attempts and archived tails.
- The five authoritative stores (cells / operations / admission / repairs + evidence) are ONE unit
  in one state root on ONE host: `replicas=1`, local filesystem, no Postgres — `deployment_profile`
  refuses the rest. Each store stamps `PRAGMA user_version` and refuses a newer one (rollback gate);
  a stamped store missing a table refuses instead of recreating it. `swfactory backup
  create|verify|restore|status|resume|reconciled|close`; a restore withholds every external effect
  until `resume`, then EVERY Cell must observe the remote before its first attempt until an operator
  runs `backup close --window-reviewed` — a Cell the snapshot never had is rebuilt under the same
  deterministic id and would otherwise republish. Run directories are backed up with the stores. See
  docs/backup-restore.md. Never hand-copy `*.sqlite3` — that drops WAL-resident commits.
- `deliver` never skips: `validate_patch` (no `..`/absolute/`.git`/symlink; paths under the target
  dir + `docs/factory/`) and `scan_secrets` run before any git or network call; only the bot-owned
  `factory/*` branch is force-updated and an open PR is edited in place. A rejected gate still
  delivers: `[REJECTED]` PR + `factory:rejected`.
- Protected paths come from the target's `factory.toml`, re-applied per stage (srt kernel
  `denyWrite`, docker `:ro`): tests writable for `build`, denied for `fix`; `Edit(docs/factory/**)`
  and `Edit(.factory/**)` denied always.
- Claude Code runs in restricted mode with an explicit tool inventory. Spec, plan, and review are
  read-only; build and fix have file tools but no shell. The trusted orchestrator alone runs the
  target's test command, Git, and delivery operations.
- No `--env`/`--env-file` on islo argv, no tokens in a sandbox; `scrub_env` strips `ANTHROPIC_*`/
  `GH_TOKEN`/`GITHUB_TOKEN`/`AWS_*`/`ISLO_API*`; srt, docker and toolset forward
  `ANTHROPIC_API_KEY` only for `agent=claude`, which needs `sandbox=islo|srt|docker|toolset` unless
  `--allow-local-agent`.
- Git identity travels as `git -c user.name=...`, never `git config` (srt makes `.git/config`
  read-only). Never `claude --bare`/`--dangerously-skip-permissions`. Typed where machines consume
  it (`Plan`, `Review`, `Diagnosis`), prose where humans do (intent.md, spec.md).

## Common mistakes
- Editing `tests/`, `factory.toml`, `REVIEW.md`, `.claude/`, `.github/` in a build/fix stage: the
  native `Edit(...)` deny rules refuse it and `swf_guard.py` logs it. Fix the code, not the gate.
  Never point the hook at anything but `python3` — the islo image has no pip and no uv.
- `git commit`, `git push`, `gh pr`, `curl`, `wget` in a stage's Bash call: denied by substring, so
  even a heredoc containing those words is refused. Use Write/Edit for content.
- Scripted replay against `[sandbox] kind = "islo"`: the CLI already downgrades `agent=scripted` to
  `LocalSandbox` unless `--sandbox` is explicit (the DAG smoke path sets `SWF_SANDBOX=local`).
- Fixtures are `{stage}.{iteration}.{patch|json|md}`; iteration >= 2 of the build loop is stage
  `fix` (`fix.2.patch`, not `build.2.patch`), review fixes continue at `fix.<max_build_iterations
  + k>`. A patch keeping a file's byte length can be masked by a stale `__pycache__` entry (mtime
  seconds + size): change the size too.
- `maintain` on a worker: never read metrics relative to cwd — `$SWF_MAINTAIN_ROOT` or the shallow
  clone of the target's base branch, and fail when there is no `docs/factory/`.
- crabbox: never `-artifact-glob` (use `-download`), default provider `local-container` (islo needs
  the `ISLO_API_KEY` that `scrub_env` strips), `.crabbox.yaml` jobs are maps; `tests=crabbox` only
  with `sandbox=local`. A target without `factory.toml` is refused: never guess.
- No Rust *inside a work cell* — the guard hook stays `python3`, and stage semantics stay
  `stages.py`; `rust/` is the operator's client only. No `CrabboxSandbox`, no `SandboxExecutor`, no
  blueprint -> `line.toml` compiler. The Astronomer Blueprint bridge (`uv sync --group airflow
  --group astronomer-blueprint`, `examples/astronomer-blueprint/`) composes by triggering a governed
  child DAG; it never recompiles or weakens the line. See docs/design.md.
