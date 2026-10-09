# Contributing to swfactory

Thanks for helping make the factory safer and more useful. Changes should preserve the trust
boundary: an agent may produce code and patches, but only the orchestrator may hold GitHub
credentials or publish changes.

## Development setup

swfactory targets Python 3.12 and uses [uv](https://docs.astral.sh/uv/).

```sh
git clone https://github.com/zozo123/ariflow-swfactory.git
cd ariflow-swfactory
uv sync --locked                     # core development tools; no scheduler required
uv run swfactory demo                # one issue, one target, scripted agent, local delivery
uv run ruff check .
uv run ruff format --check .
uv run pytest tests/test_stages_scripted.py
```

The scripted demo and core tests need no model keys or scheduler. Dependency installation needs
network access. Start with tests for the behavior you change; use GitHub Actions for the heavier
Python/Rust/Airflow matrix while continuing development locally. The demo's report and local PR
description are under `.factory/<run_id>/`.

Push a PR to start CI, then inspect its current checks without blocking your editing session:

```sh
gh pr checks <number>
```

For asynchronous compute on a development branch, dispatch just the check you need:

```sh
gh workflow run dev-checks.yml --ref <branch> -f check=core
gh run list --workflow dev-checks.yml --branch <branch> --limit 5
```

Choose `lint`, `core`, `rust`, or `evals`. Push the branch first. Results and retained test reports
are available in Actions; continue editing while the run executes. A newer dispatch of the same
check on the same branch cancels the older run. These auxiliary checks do not replace candidate
readiness or release evidence.

Wait for required checks on the final commit before promotion. For the complete core suite run
`uv run pytest`. Add Airflow only when working on scheduler behavior; the pinned integration group
uses Airflow 3.3.2:

```sh
uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py tests/test_dag_stress.py
```

## Pull requests

- Keep a PR focused on one behavior or operational concern.
- Add or update tests for every behavior change.
- Update `README.md` and canonical `AGENTS.md` when commands, invariants, or supported versions change.
- Add a bullet under `## [Unreleased]` in `CHANGELOG.md` for anything a user of the factory
  would notice: a blueprint key, a `SWF_*` knob, a CLI flag, a sandbox behaviour, a security
  boundary.
- Never commit credentials, `.env` files, generated `.factory/` state, or local Airflow state.
- Describe the risk, trust-boundary impact, and exact verification commands in the PR body.

Before opening a PR, run lint and tests relevant to your change; CI runs the full required matrix. See
`AGENTS.md` for the agent contract (`CLAUDE.md` is its symlink) and `REVIEW.md` for the review contract.

## Release

Releasing is a tag push. `.github/workflows/release.yml` does the rest: lint, the hermetic suite,
the scripted demo, DAG parity and smoke, `uv build`, then a GitHub Release whose body is that
version's `CHANGELOG.md` section, with the wheel, the sdist, the four `swf` tarballs, the SBOMs,
the candidate-readiness evidence, `provenance.json` and `SHA256SUMS` attached.

1. Keep the versions in `pyproject.toml` and `Cargo.toml` (`workspace.package.version`)
   aligned. Run `uv lock` and `cargo check --workspace` to refresh
   the corresponding lockfiles. The scheme and
   what counts as a breaking change are in
   [docs/design.md](docs/design.md#versioning-and-release).
2. Move the `## [Unreleased]` bullets into a dated `## [X.Y.Z]` section in `CHANGELOG.md` and fix
   the link definitions at the bottom of the file.
3. Open a PR with the version and changelog changes and merge it once CI is green. Confirm
   candidate-readiness on the exact final main commit before tagging; the release workflow
   consumes its retained evidence.
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
