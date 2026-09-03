# swfactory — institutional knowledge (keep under one page)

## What this is
An AI-native software factory. A blueprint (`blueprints/<name>.toml`: stage order, gates, limits,
targets, sandbox, labels) is one Airflow 3 DAG (`dags/blueprints.py`, jobs = issues x targets via
dynamic task mapping) and one `swfactory run --blueprint <name>` line. Claude Code does stage work
inside a sandbox that holds no GitHub credential (islo MicroVM in production, srt-confined dir on a
dev box); the orchestrator alone talks to GitHub (`git am` a format-patch stream, push, `gh pr
create`); a human merges. Stage semantics live in `stages.py`; a blueprint only picks the walk.

## Commands
- `uv sync` — install. `uv run pytest` — 235 hermetic tests (fake subprocess, tmp git, no network).
- `uv run ruff check . && uv run ruff format --check .` — lint (line length 100; E,F,I,B,UP,SIM).
- `uv run swfactory demo` — scripted replay, local sandbox, local bare remote. No keys, ~10 s.
  `--sandbox srt` confines the same replay with the Anthropic Sandbox Runtime (needs `npx`).
- `uv run swfactory run --blueprint hotfix --issue demo/issue.md --approve auto` — second line, scripted.
- `uv run swfactory run --issue <n|path> --agent claude --sandbox srt --scm local|github --approve prompt`
  — real agent, cloudless (Claude login or `ANTHROPIC_API_KEY` in your shell).
- `uv run swfactory demo --real` — claude agent in an islo sandbox, real PR (needs `islo login`,
  `GH_TOKEN`; README bootstrap + snapshot recipe).
- `uv run swfactory approve <dag_run_id> intent|plan [--reject] [--blueprint <name>] [--map-index <job>]`.
- `uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py` — 21 DAG tests.
- `uv run airflow dags test factory --conf '{"issues":["demo/issue.md"]}' --mark-success-pattern 'job\.approve_.*'`
  — `dags test` never resolves HITL gates; always mark them.
- `uv run swfactory metrics --root .`, `uv run swfactory maintain --root .` — metrics / bands.
- `uv run swfactory doctor [--json] [--blueprint <name>]` — preflight: islo/gh/claude/srt CLIs, `islo login` + `--tool github/claude`, gateway profile, environment, snapshot, `gh auth` + repo, blueprint; exit 1 on a required red row, each with its `fix:`.
- `uv run swfactory webhook serve --port 8081 --airflow-url http://localhost:8080` — GitHub issue-label webhook receiver on the orchestrator sandbox -> `POST /api/v2/dags/<name>/dagRuns`.
- `deploy/islo/bootstrap.sh` — one-time islo org setup: gateway profiles + environments (phantom tokens), incoming webhook, knowledge items.
- `deploy/islo/deploy.sh` — orchestrator sandbox from `deploy/islo/orchestrator/{islo.yaml,start.sh}` (Airflow + receiver), prints the shared UI URL.
- `deploy/islo/knowledge.sh [owner/repo]` — publish CLAUDE.md, REVIEW.md, SKILL.md as islo knowledge items (rule/rule/skill, tag `swfactory`; get -> update|create, idempotent).

## Conventions
- Python 3.12, `from __future__ import annotations`, type hints, docstrings on public functions.
  Stdlib first (`subprocess`, `tomllib`, `statistics`, `xml.etree`). No Airflow import under `src/`;
  `dags/*.py` import swfactory only inside task callables (parity test asserts it).
- Protocols: `Sandbox` (local/srt/islo), `Agent` (claude/scripted), `Scm` (local/github). Stages are
  functions `Ctx -> StageResult` registered in `STAGES`; loops live inside stage functions, never in
  the DAG; task mapping fans out over jobs only (nested expansion is unsupported in Airflow 3.3.1).
- Blueprints may only ADD allowed tools or set a model per stage; gates only after `intent`/`plan`;
  `ttl_s > max gate timeout`. `SWF_*` env overrides blueprint values and CLI flags (env > init).
- Artifacts are committed under `docs/factory/<issue>/` in the target by `swfactory-bot` with
  `Factory-Run`/`Factory-Stage`/`Agent` trailers. `.factory/` is uncommitted scratch.
- Every stage is idempotent: a completed record in the orchestrator's stage log
  `.factory/<run_id>/stages.jsonl` -> `status="skipped"`. The sandbox is agent-writable and is
  never consulted for skips or the run budget (seeded from the same log). Every loop is bounded by
  `Config`; exhaustion is `StageError(kind="policy")` or a `factory:blocked` PR, never a retry.
- `deliver` never skips: it policy-checks the patch (`scm.validate_patch` — no `..`/absolute/
  `.git`/symlink, paths under the target dir + `docs/factory/`; `scan_secrets`) before any git or
  network call, force-updates only the bot-owned `factory/*` branch and edits an existing open PR
  (retry-safe). A rejected gate still delivers: `[REJECTED]` PR + `factory:rejected`, status blocked.
- srt `denyWrite` comes from the target's `factory.toml` (`config.protected_globs` before the
  sandbox exists, `SrtSandbox.set_protected(protected_for(contract, stage))` before every agent
  call): tests dir writable for `build`, denied for `fix`. `Edit(docs/factory/**)`/`Edit(.factory/**)`
  are denied to the agent in every stage.
- No `--env`/`--env-file` on islo argv, no tokens in the sandbox, `LocalSandbox` scrubs
  `ANTHROPIC_*`/`GH_TOKEN`/`GITHUB_TOKEN`/`AWS_*`/`ISLO_API*`; srt forwards only `ANTHROPIC_API_KEY`
  and only for `agent=claude`. `agent=claude` requires `sandbox=islo|srt` unless `--allow-local-agent`.
- Git identity travels as `git -c user.name=... -c user.email=...`, never `git config` (srt makes
  `.git/config` read-only). Never `claude --bare` or `--dangerously-skip-permissions`.
- Typed where machines consume it (`Plan`, `Review`, `BuildSummary`, `Diagnosis` via
  `--json-schema`); prose where humans do (intent.md, spec.md).

## Common mistakes
- Editing `tests/`, `factory.toml`, `REVIEW.md`, `.claude/`, `.github/` during a build/fix stage:
  the native `Edit(...)` deny rules in the sandbox's `.claude/settings.local.json` refuse it and
  `swf_guard.py` logs it. Fix the code, not the gate. Never point the hook at anything but
  `python3` — the islo image has python3 and nothing else (no pip, no uv).
- Running `git commit`, `git push`, `gh pr`, `curl`, `wget` from a Bash tool call in a stage: denied
  by substring, so even a heredoc containing those words is refused. Use Write/Edit for content.
- Running a scripted replay against the blueprint's `[sandbox] kind = "islo"`: the CLI already
  downgrades `agent=scripted` to `LocalSandbox` unless `--sandbox` is explicit; the DAG smoke path
  uses `SWF_AGENT=scripted SWF_SANDBOX=local`.
- Scripted fixtures are named `{stage}.{iteration}.{patch|json|md}`; iteration >= 2 of the build
  loop is stage `fix`, so the second patch is `fix.2.patch`, not `build.2.patch`. Review fixes
  continue the numbering at `fix.<max_build_iterations + k>` (`fix.4.patch` by default), never
  reusing a build-loop name. A fixture patch that changes a file to the same byte length as before
  can be masked by a stale `__pycache__` entry (mtime seconds + size): change the size too.
- `maintain` on a worker: never read metrics relative to cwd; `maintain.metrics_root` uses
  `$SWF_MAINTAIN_ROOT` or a shallow clone of the target's base branch and fails without `docs/factory`.
- crabbox: never `-artifact-glob` (use `-download`), default provider `local-container` (islo needs
  the `ISLO_API_KEY` that `scrub_env` strips), `.crabbox.yaml` jobs are maps. `tests=crabbox` only
  with `sandbox=local`. A target without `factory.toml` is refused: the factory never guesses.
- No Rust, no `CrabboxSandbox`, no blueprint->islo line.toml compiler: see README "Design decisions".
