# swfactory — institutional knowledge (keep under one page)

## What this is
An AI-native software factory. Airflow 3 runs one linear pipeline with two human gates (intent,
plan); Claude Code does stage work inside an islo MicroVM that holds no credentials; the
orchestrator alone talks to GitHub (`git am` a format-patch stream, push, `gh pr create`); a human
merges. `swfactory.stages.PIPELINE` drives both `swfactory run` and `dags/factory.py`.

## Commands
- `uv sync` — install. `uv run pytest` — 94 hermetic tests (fake subprocess, tmp git, no network).
- `uv run ruff check . && uv run ruff format --check .` — lint (line length 100; E,F,I,B,UP,SIM).
- `uv run swfactory demo` — scripted replay, local sandbox, local bare remote. No keys, ~5 s.
  Add `--approve prompt` to answer the gates yourself; `--record demo/scripted` on a real run
  rewrites the fixtures.
- `uv run swfactory demo --real` — claude agent in an islo sandbox, real PR on the repo (needs
  `islo login`, `GH_TOKEN`; see README bootstrap).
- `uv run swfactory run --issue <n|path> [--agent] [--sandbox] [--scm] [--approve] [--tests]`.
- `uv run swfactory metrics --root .` — aggregate `docs/factory/*/metrics.json`.
- `uv run swfactory approve <dag_run_id> intent|plan [--reject]` — answer an Airflow HITL gate.
- `uv sync --group airflow && uv run --group airflow pytest tests/test_dag_parity.py` — DAG parity.
- Inner loop off-host: `crabbox run --provider islo -- uv run pytest`.

## Conventions
- Python 3.12, `from __future__ import annotations`, type hints, docstrings on public functions.
  Stdlib first (`subprocess`, `tomllib`, `statistics`, `xml.etree`). No Airflow import under `src/`.
- Three protocols, two implementations each: `Sandbox` (local/islo), `Agent` (claude/scripted),
  `Scm` (local/github). Stages are functions `Ctx -> StageResult` in `stages.py`; loops live inside
  stage functions, never in the DAG.
- Artifacts are committed under `docs/factory/<issue>/` in the target by `swfactory-bot` with
  `Factory-Run`/`Factory-Stage`/`Agent` trailers. `.factory/` is uncommitted scratch.
- Every stage is idempotent: artifact exists -> `status="skipped"`. Every loop is bounded by
  `Config`; exhaustion is `StageError(kind="policy")` or a `factory:blocked` PR, never a retry.
- No `--env`/`--env-file` on islo argv, no tokens in the sandbox, `LocalSandbox` scrubs
  `ANTHROPIC_*`/`GH_TOKEN`/`GITHUB_TOKEN`/`AWS_*`/`ISLO_API*`. `agent=claude` requires
  `sandbox=islo` unless `--allow-local-agent` (dev only).
- Typed where machines consume it (`Plan`, `Review`, `BuildSummary`, `Diagnosis` via
  `--json-schema`); prose where humans do (intent.md, spec.md).
- Never `claude --bare` or `--dangerously-skip-permissions`; hooks are the gate.

## Common mistakes
- Editing `tests/`, `factory.toml`, `REVIEW.md`, `.claude/`, `.github/` during a build/fix stage:
  `swf_guard.py` denies it on purpose. Fix the code, not the gate.
- Running `git commit`, `git push`, `gh pr`, `curl`, `wget` from a Bash tool call in this repo:
  the same guard runs here via `.claude/settings.json` and matches on substrings, so even a
  heredoc containing those words is denied. Use the Write/Edit tools for file content.
- Scripted fixtures are named `{stage}.{iteration}.{patch|json|md}`; iteration >= 2 of the build
  loop is stage `fix`, so the second patch is `fix.2.patch`, not `build.2.patch`.
- `swfactory metrics`/`maintain` read committed `metrics.json`; scripted runs are excluded from
  bands. A target without `factory.toml` is refused: the factory never guesses test commands.
- `sandbox_ttl_s` must exceed `gate_timeout_h`; `tests=crabbox` only with `sandbox=local`.
