# calc (demo target)

A deliberately tiny library the software factory operates on. `demo/issue.md` (DEMO-1) asks for
`percent_change(old, new)`; `uv run swfactory demo` replays a recorded run against a copy of this
directory and produces intent -> spec -> plan -> code + tests -> reviewed PR under
`docs/factory/DEMO-1/`.

`factory.toml` is the contract: the test and lint commands, and the paths the agent may not edit
(`factory.toml`, `tests/`). The test command rebuilds source bytecode with checked hashes before
running pytest and writing `.factory/junit.xml`. This prevents rapid same-size fixes from
executing stale timestamp-based bytecode. `CLAUDE.md` holds the agent-facing notes for this package.
