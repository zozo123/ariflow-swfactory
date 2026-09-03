# Contributing to swfactory

Thanks for helping make the factory safer and more useful. Changes should preserve the trust
boundary: an agent may produce code and patches, but only the orchestrator may hold GitHub
credentials or publish changes.

## Development setup

swfactory targets Python 3.12 and uses [uv](https://docs.astral.sh/uv/).

```sh
git clone https://github.com/zozo123/ariflow-swfactory.git
cd ariflow-swfactory
uv sync --all-groups                 # dev (pytest, ruff) + airflow (apache-airflow 3.3.1)
uv run ruff check .
uv run ruff format --check .
uv run pytest                        # the whole suite, DAG tests included with --all-groups
uv run swfactory demo
```

The suite and the scripted demo are hermetic: they do not need API keys or network access. On a
checkout synced without the airflow group (`uv sync`) the DAG tests skip, so run them explicitly:

```sh
uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py
```

## Pull requests

- Keep a PR focused on one behavior or operational concern.
- Add or update tests for every behavior change.
- Update `README.md` and `CLAUDE.md` when commands, invariants, or supported versions change.
- Add a bullet under `## [Unreleased]` in `CHANGELOG.md` for anything a user of the factory
  would notice: a blueprint key, a `SWF_*` knob, a CLI flag, a sandbox behaviour, a security
  boundary.
- Never commit credentials, `.env` files, generated `.factory/` state, or local Airflow state.
- Describe the risk, trust-boundary impact, and exact verification commands in the PR body.

Before opening a PR, run the same lint, test, Airflow smoke, and demo commands shown above. See
`CLAUDE.md` for the architecture invariants and `REVIEW.md` for the review contract.

## Release

Releasing is a tag push. `.github/workflows/release.yml` does the rest: lint, the hermetic suite,
the scripted demo, DAG parity and smoke, `uv build`, then a GitHub Release whose body is that
version's `CHANGELOG.md` section, with the wheel and the sdist attached.

1. Bump `version` in `pyproject.toml` and run `uv lock` so `uv.lock` records it. The scheme and
   what counts as a breaking change are in
   [docs/design.md](docs/design.md#versioning-and-release).
2. Move the `## [Unreleased]` bullets into a dated `## [X.Y.Z]` section in `CHANGELOG.md` and fix
   the link definitions at the bottom of the file.
3. Open a PR with those two changes and merge it once CI is green.
4. Tag the merge commit on `main` and push the tag:

```sh
git switch main && git pull
git tag -a v1.2.3 -m "swfactory 1.2.3"
git push origin v1.2.3
```

The workflow refuses — before building anything — a tag that does not match `pyproject.toml`, a
version with no `CHANGELOG.md` section, or a version that already has a release, so a mistyped or
undocumented tag fails loudly and publishes nothing. Nothing goes to PyPI: the wheel and sdist
attached to the GitHub Release are the artifacts.

## Reporting security issues

Do not open a public issue for a suspected vulnerability. Follow [SECURITY.md](SECURITY.md).
