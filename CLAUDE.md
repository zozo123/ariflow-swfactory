# swfactory — institutional knowledge (keep under one page)

## What this is
An AI-native software factory (Anthropic SDLC playbook). Airflow 3 orchestrates; Claude Code agents do stage work in islo sandboxes; crabbox runs tests on leased boxes; humans approve intent, plan, and merge. Same stage code runs from `swfactory` CLI and from `dags/`.

## Commands
- `uv sync` — install. `uv run pytest` — unit + e2e demo tests (healthy: `... passed`).
- `uv run ruff check . && uv run ruff format --check .` — lint.
- `uv run swfactory demo` — scripted agent, local runner, local git remote. No keys. ~10s.
- `uv run swfactory demo --agent claude` — real Claude Code (needs `claude` login).
- `uv run swfactory run --issue demo/issue.md --repo . --scm github` — real PR on this repo.
- `uv run swfactory airflow` — start Airflow standalone with `dags/` loaded.

## Conventions
- One module per SDLC stage in `src/swfactory/stages/`; stages are functions, not classes.
- Depend on protocols (`Runner`, `Agent`, `Scm`), never on a concrete CLI. Add a backend by adding one file.
- Artifacts are committed markdown under `docs/factory/<issue-id>/` in the target repo. Never skip a link in the chain.
- Loops are bounded by config. No unbounded retries.
- The agent never merges. `deliver` opens a PR; branch protection does the rest.
- Secrets never enter prompts or sandboxes; islo gateway injects credentials.

## Common mistakes
- Using `${param}` in `job.toml`: islo substitutes `{param}`; bash expands `${}`.
- Editing `tests/` during a fix task: the hook blocks it on purpose. Fix the code.
- Forgetting `factory.toml` in a target repo: the factory refuses to guess test commands.
