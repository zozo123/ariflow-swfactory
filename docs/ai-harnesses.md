# AI harnesses as the factory entrypoint

The factory can be entered from Codex CLI, Claude Code, Grok, or any shell-capable AI agent. The
outer AI harness owns the conversation and decides what work to request. It does not own lifecycle
scheduling: Apache Airflow remains the only lifecycle scheduler and durable Factory Cells remain the
single-writer authority for issue x target work.

## Session identity

Every outer harness session should have two stable values:

- `SWF_HARNESS`: short harness name, for example `codex`, `claude`, `grok`, or `custom`.
- `SWF_FACTORY_ID`: unique stable id for that outer session. Reuse it when the same session retries.

The pair becomes the backend admission actor `harness:<name>:<factory-id>`. That identity participates
in deterministic submission identity, per-actor admission, Cell activation evidence, and Airflow run
configuration. It is metadata and admission identity, not a replacement for the Cell epoch.

Direct-Airflow `swf submit` intentionally refuses harness metadata because that route cannot preserve
the governed origin identity. Configure `SWF_BACKEND_URL`/`SWF_BACKEND_TOKEN` for harness-native use.

## Codex CLI

```bash
export SWF_HARNESS=codex
export SWF_FACTORY_ID=codex-$(date +%Y%m%d)-a
swf submit --issue 1201 --issue 1202
```

Codex can also use the repository wrapper:

```bash
bash scripts/swf-harness.sh --harness codex --factory-id codex-a17 --issue 1201
```

`AGENTS.md` is the repository-level Codex/generic agent contract.

## Claude Code

```bash
export SWF_HARNESS=claude
export SWF_FACTORY_ID=claude-a17
swf submit --issue 1203
```

The `.claude/skills/swfactory` skill distinguishes the outer Claude session from a stage agent that
is already inside a factory sandbox. A stage agent never pushes, commits, opens PRs, or reads service
credentials; the factory performs those mutations after validation.

## Grok or another agent

No custom SDK is required. Any shell-capable harness can use the same operator surface:

```bash
export SWF_HARNESS=grok
export SWF_FACTORY_ID=grok-a17
swf submit --issue 1204
```

For another harness use a short validated name such as `custom`, `aider`, or `internal-agent`.

## Many factories on one repository

Different outer sessions may submit work to the same repository concurrently:

```bash
SWF_HARNESS=codex  SWF_FACTORY_ID=codex-1  swf submit --issue 101 &
SWF_HARNESS=claude SWF_FACTORY_ID=claude-1 swf submit --issue 102 &
SWF_HARNESS=grok   SWF_FACTORY_ID=grok-1   swf submit --issue 103 &
wait
```

The concurrency rules are intentionally asymmetric:

1. Different issue x target Cells may be active together, subject to admission/backpressure limits.
2. The same issue x target Cell has one active lifecycle owner. A competing session cannot create a
   second writer; Cell activation/replay/fencing decides the outcome.
3. Every job gets an isolated run/workdir/remote and a delivery branch derived from deterministic run
   identity. Harness sessions never share a mutable checkout.
4. External mutations are idempotent or fenced by Cell epoch and operation key.
5. Publication and promotion remain singular factory authorities even when execution is wide.

This means "many factories" is safe horizontal fan-out, not several schedulers fighting over one
piece of state.

## Operator loop

The harness may keep driving the conversation while the factory runs:

```bash
swf attention
swf runs list
swf jobs list --attention
swf gates list --ready
swf gates approve --all --yes
```

Those commands observe or answer the governed runtime. They do not transfer scheduling authority out
of Airflow.
