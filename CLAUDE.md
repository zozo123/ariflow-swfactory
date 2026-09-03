# swfactory — institutional knowledge (one page)

A blueprint (`blueprints/<name>.toml`: stage order, gates, limits, targets, sandbox, labels) is one
Airflow 3 DAG (`dags/blueprints.py`, jobs = issues x targets via task mapping) and one
`swfactory run --blueprint <name>` line. Claude Code works in a sandbox holding no GitHub credential
(islo MicroVM in production, srt or a container on a dev box); the orchestrator alone talks to
GitHub (`git am` a format-patch stream, push, `gh pr create`); a human merges. Stage semantics live
in `stages.py`. Details: README + docs/*.md.

## Commands
- `uv sync`; `uv run pytest` — 340 hermetic tests (fake subprocess, tmp git repos, no network).
- `uv run ruff check . && uv run ruff format --check .` — line length 100; E,F,I,B,UP,SIM.
- `uv run swfactory demo [--sandbox srt|docker] [--real]` — scripted replay, no keys, ~10 s;
  `--real` runs claude in an islo sandbox and opens a real PR.
- `uv run swfactory run --issue <n|path> --agent claude --sandbox srt --scm local|github`
  — real agent, cloudless. `--blueprint hotfix --issue demo/issue.md --approve auto` = second line.
- `uv run swfactory approve <dag_run_id> intent|plan [--reject] [--map-index <j>]`; `doctor
  [--json]` (exit 1 per red row, with a `fix:`); `metrics|maintain --root .`; `herd`; `webhook`.
- `uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py` — DAG tests;
  `airflow dags test` never resolves HITL gates (add `--mark-success-pattern 'job\.approve_.*'`).
- `deploy/islo/bootstrap.sh` (gateway, environment, snapshot, knowledge) then `deploy.sh`
  (orchestrator sandbox + webhooks) — docs/islo.md.

## Conventions
- Python 3.12, `from __future__ import annotations`, type hints, docstrings that say WHY. Stdlib
  first (`subprocess`, `tomllib`, `statistics`, `xml.etree`). No Airflow import under `src/`;
  `dags/*.py` import swfactory only inside task callables (the parity test asserts it).
- Protocols: `Sandbox` (local/srt/docker/islo), `Agent` (claude/scripted), `Scm` (local/github).
  Stages are functions `Ctx -> StageResult` in `STAGES`; loops live inside stage functions, never in
  the DAG; task mapping fans out over jobs only (nested expansion is unsupported in Airflow 3.3.1).
- `runtime.py` is the only place a `(blueprint, job, run id)` triple becomes a `Ctx`; the CLI and
  every Airflow task call it, so a retried task lands on the same run dir and sandbox.
- Blueprints may only ADD allowed tools or set a model per stage; gates only after `intent`/`plan`;
  `ttl_s > max gate timeout`. `SWF_*` env overrides blueprint values and CLI flags (env > init).
- Artifacts are committed under `docs/factory/<issue>/` by `swfactory-bot` with `Factory-Run`/
  `Factory-Stage`/`Agent` trailers; `.factory/` is uncommitted orchestrator scratch.
- Every stage is idempotent: a completed record in the orchestrator's log
  `.factory/<run_id>/stages.jsonl` -> `status="skipped"`. The agent-writable sandbox is never
  consulted for skips or the budget. Loops are `Config`-bounded; exhaustion is
  `StageError(kind="policy")` or a `factory:blocked` PR, never a retry.
- `deliver` never skips: `validate_patch` (no `..`/absolute/`.git`/symlink; paths under the target
  dir + `docs/factory/`) and `scan_secrets` run before any git or network call; only the bot-owned
  `factory/*` branch is force-updated and an open PR is edited in place. A rejected gate still
  delivers: `[REJECTED]` PR + `factory:rejected`.
- Protected paths come from the target's `factory.toml`, re-applied per stage (srt kernel
  `denyWrite`, docker `:ro`): tests writable for `build`, denied for `fix`; `Edit(docs/factory/**)`
  and `Edit(.factory/**)` denied always.
- No `--env`/`--env-file` on islo argv, no tokens in a sandbox; `scrub_env` strips `ANTHROPIC_*`/
  `GH_TOKEN`/`GITHUB_TOKEN`/`AWS_*`/`ISLO_API*`; srt and docker forward `ANTHROPIC_API_KEY` only
  for `agent=claude`, which needs `sandbox=islo|srt|docker` unless `--allow-local-agent`.
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
- No Rust, no `CrabboxSandbox`, no `SandboxExecutor`, no blueprint -> `line.toml` compiler, Docker
  Sandboxes documented-not-wired: docs/design.md says why.
