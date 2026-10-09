# swfactory agent contract

This repository is an **Airflow-governed software atelier**:

`work order -> Cell -> Airflow -> bounded agent work -> evidence -> PR -> authorized merge`

## Authority

- **Airflow is the only lifecycle scheduler.** Do not create a competing agent loop.
- Outer harnesses submit, inspect, answer human gates, and verify evidence.
- Inner stage agents **never commit, push, open PRs, or merge**; the backend owns managed mutations.
- Autonomous gates follow `config/autonomous.toml`. Policy, budgets, protections, review skill,
  and authority implementation changes remain human maintenance PRs.
- Rust is operator-side only; **never run Rust inside a work cell**.
- Cell identity + epoch fence external mutations. Never bypass that boundary.
- Never place service/GitHub credentials in a stage sandbox.
- Control-plane/protected paths are human-maintained. Missing, failed, skipped, or cancelled required evidence means **fail closed**.
- Fix code, not the gate: do not weaken tests, policy, evidence, or protection to make CI green.

## Work

For an outer harness, keep one stable session identity:

```bash
swf submit --harness codex --factory-id codex-session-17 --issue 1204
```

Read `factory.toml`, the relevant blueprint, tests, and docs before changing behavior. Keep the patch minimal.

## Verify from repository root

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run pytest
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --locked
uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py tests/test_dag_stress.py
uv run swfactory demo
```

Do not call a change healthy until the required GitHub candidate-readiness fan-in is green for the exact head.

Details: `README.md`, `skills/swfactory/SKILL.md`, `docs/promotion-policy.md`, `docs/design.md`.
