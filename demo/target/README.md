# calc (demo target)

A deliberately tiny library the software factory operates on. `demo/issue.md` (DEMO-1) asks for
`percent_change(old, new)`; `uv run swfactory demo` replays a recorded run against a copy of this
directory and produces intent -> spec -> plan -> code + tests -> reviewed PR under
`docs/factory/DEMO-1/`.

`factory.toml` is the contract: the test command (`uv run --group dev pytest --junitxml=.factory/junit.xml`),
the lint command, and the paths the agent may not edit (`factory.toml`, `tests/`). `CLAUDE.md`
holds the agent-facing notes for this package.
