# Contributing to swfactory

Thanks for helping make the factory safer and more useful. Changes should preserve the trust
boundary: an agent may produce code and patches, but only the orchestrator may hold GitHub
credentials or publish changes.

## Development setup

swfactory targets Python 3.12 and uses [uv](https://docs.astral.sh/uv/).

```sh
git clone https://github.com/zozo123/ariflow-swfactory.git
cd ariflow-swfactory
uv sync --all-groups
uv run ruff check .
uv run ruff format --check .
uv run pytest
uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py
uv run swfactory demo
```

The default suite and scripted demo are hermetic: they do not need API keys or network access.

## Pull requests

- Keep a PR focused on one behavior or operational concern.
- Add or update tests for every behavior change.
- Update `README.md` and `CLAUDE.md` when commands, invariants, or supported versions change.
- Never commit credentials, `.env` files, generated `.factory/` state, or local Airflow state.
- Describe the risk, trust-boundary impact, and exact verification commands in the PR body.

Before opening a PR, run the same lint, test, Airflow smoke, and demo commands shown above. See
`CLAUDE.md` for the architecture invariants and `REVIEW.md` for the review contract.

## Reporting security issues

Do not open a public issue for a suspected vulnerability. Follow [SECURITY.md](SECURITY.md).
