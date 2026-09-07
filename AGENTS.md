# AI harness entrypoint

This repository is designed to be entered from an outer AI coding harness such as Codex CLI,
Claude Code, Grok, or another shell-capable agent.

## Authority model

The outer harness owns the conversation and decides what work to request. It does **not** become a
second scheduler. Apache Airflow is the only lifecycle scheduler. Durable Factory Cells own
issue x target identity and epoch fencing. The factory owns sandbox lifecycle, commits, publication,
approvals, evidence, retries, and cleanup.

## Start work

Give every outer harness session a stable identity and reuse it for retries from that session:

```bash
scripts/swf-harness.sh --harness codex --factory-id codex-session-17 --issue 1201
```

Or set the identity once in the harness environment and submit repeatedly:

```bash
export SWF_HARNESS=codex
export SWF_FACTORY_ID=codex-session-17
swf submit --issue 1201
swf submit --issue 1202 --issue 1203
```

Claude Code uses `--harness claude`; Grok uses `--harness grok`; any other harness may use a short
ASCII name such as `custom`.

## Concurrency

Many harness sessions may target the same repository at once. Use a different `SWF_FACTORY_ID` per
outer session. Different issue x target Cells can run concurrently, bounded by factory admission.
Two sessions that race for the same issue x target Cell do not get two writers: the Cell's active
epoch is the authority and the duplicate is replayed, queued, or refused instead of clobbering it.
Delivery branches include the deterministic factory run identity, so independent sessions do not
share a publication branch.

## During a run

Use `swf attention`, `swf runs`, `swf jobs`, `swf cells`, and `swf gates` to observe and answer
human gates. Do not bypass a gate by editing Airflow state or by pushing a replacement branch.

Stage agents inside factory sandboxes are deliberately less trusted than the outer harness: they may
edit only their planned workspace and must not push, create PRs, commit, or access service
credentials. The factory performs those mutations after validation and records evidence.
