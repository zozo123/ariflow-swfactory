## Summary

<!-- What changes, and why? -->

## Trust-boundary impact

<!-- Agent permissions, credentials, network, patch validation, or "None". -->

## Search / authority discipline

- [ ] This change does not demand elimination of exploration nondeterminism; any new entropy stays outside authority, identity, and promotion decisions.

## Verification

- [ ] `uv run ruff check .`
- [ ] `uv run ruff format --check .`
- [ ] `uv run pytest`
- [ ] `uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py`
- [ ] `uv run swfactory demo`

## Operational notes

<!-- Deployment, migration, rollback, or "None". -->
