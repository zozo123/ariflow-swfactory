# Inspecting and recovering local runs

The control plane saves authoritative run evidence in `.factory/<run-id>/state/`. A run's
sandbox copy is an audit artifact; it never authorizes stage skips, approval decisions or spend.
The CLI and Airflow tasks use the same host state and ownership boundary.

```sh
uv run swfactory state list
uv run swfactory state list --attention --json
uv run swfactory state inspect <run-id> --events 50
```

Use `--root /path/to/.factory` when the worker's state lives elsewhere. These commands only read
local evidence: they do not connect to Airflow, reconnect to a sandbox, execute an agent or repair
files. `list` reports the most recently changed runs; `--attention` filters that selected set for
failed/interrupted operations, journal damage or torn tails. `inspect` emits one JSON document
and exits 1 when some evidence cannot be read or validated. Missing runs exit 3.

The result includes identity, the latest non-skipped result per stage, recorded cost, current
ownership, recent operation records, journal sizes, incomplete tail lengths and archived fragments.
Corrupt stage evidence produces an unknown cost and an explicit error rather than a zero total.
Snapshots describe local evidence at read time; Airflow remains the authority for scheduler state.

## One owner while a run changes

Preparation, setup, each production stage, recording an approval and teardown acquire the same
POSIX `flock` on `state/run.lock`. The lock covers the full operation, including the initial
completed-stage check, budget refresh, sandbox changes and final stage journal append. It is
released between operations and while a person considers an approval.

A second mutation fails immediately with a retryable sandbox error. It does not invoke the agent,
close the sandbox or append a failed stage result over the owner's evidence. Existing Airflow
retry settings still decide whether that task retries automatically; teardown now has two retries.
Use the normal Airflow task retry/clear workflow after the active owner finishes when needed.

This is a kernel-held lock, with no timeout that could hand a live operation to another worker.
The operating system releases it when its owning process exits. Never remove or replace
`run.lock` to unlock a run: another inode would let two processes believe they own it. A stale PID
in the operation history is evidence, not lock ownership. There is deliberately no CLI unlock.

`state/operations.jsonl` records each attempt with an attempt ID, operation, process/host,
start time and terminal event. Exceptions record their type without copying prompts, credentials
or raw error bodies into the operation journal. A process killed before its terminal record leaves
an unclosed `started` entry. Inspection reports it as interrupted only when the lock is free; the
next owner preserves an `interrupted` receipt before recording its own attempt.

An interrupted operation may already have produced side effects. This history does not roll back
a commit, stop a remote command, reconstruct unknown model spend, or prove that an agent did no
work. Review the stage evidence and work-cell state before retrying interrupted work. Existing
workspace-HEAD checks, gate evidence, patch validation and delivery restrictions still apply.

## Journal recovery

Readers take a shared file lock so a concurrent append cannot appear as a damaged record. Each
record is decoded separately; an interrupted UTF-8 character in the final append cannot prevent
reading earlier complete records.

Writers validate the existing journal under an exclusive lock before changing it:

| Existing ending | Append behavior |
| --- | --- |
| Complete newline-terminated JSON records | Append the next record normally |
| Complete final JSON without a newline | Insert a separator and preserve the record |
| Incomplete final record without a newline | Save its exact bytes under `state/recovery/`, truncate only that fragment, then append |
| Invalid newline-terminated record, or corruption before the last line | Refuse to append; preserve the journal for investigation |

An inspection never changes a file. Automatic tail recovery happens only when a writer already
owns the journal. Fragments are named with a SHA-256 digest of the journal name and pre-repair
bytes. The fragment file and its directory are synced before truncation; repeating a recovery
after a crash preserves the same evidence. New state directories are mode 0700 and new journal,
lock and atomic replacement files are mode 0600. Existing permissions are preserved on append.

Atomic control/artifact writes now sync both content and directory entries. Newly created state
directories and control-file removals are synced as well. Symlinks that escape the run's state or
artifact root are refused.

## The inputs one epoch accepted

Airflow rebuilds a whole `Ctx` for every task: the blueprint is reloaded from the worker's disk,
the issue is fetched again, and `Config` re-reads that worker's `SWF_*` environment. So the first
context of a run admits an immutable snapshot of what it is executing — issue content, resolved
blueprint, the prompt templates that blueprint references, effective policy and target identity —
into `state/accepted-inputs.json`, and every
later context recomputes it and compares. Admission happens inside `runtime.ctx_for`, before the
sandbox and agent are constructed, so a drifted task refuses before any agent or provider I/O
rather than after the model has written code.

Its digest (`inputs:<sha256>`) is stamped onto every recorded approval next to the artifact digest
and the Cell/epoch, and is re-checked at continuation, so an answer given for one set of inputs
cannot publish another. The execution report (`RunReport.inputs_digest`), the publication receipt
(`metrics.json`) and the PR body all quote the same value.

`state/accepted-inputs.jsonl` is the append-only history: one `accepted` record per admission and
one `superseded` record per deliberate re-accept.

**Policy vs. operational settings.** Only settings that describe what a run may do are fenced
(`accepted_inputs.POLICY_SETTINGS`: sandbox and agent kind, loop bounds and budgets, egress
allowlist, image, credential *mode*, gate timeouts). Per-worker paths and ownership
(`OPERATIONAL_SETTINGS`: `fixtures_dir`, `workdir`, `record_dir`, `sandbox_owner`, and the
`gate_replay` path) may differ between workers. `tests/test_accepted_inputs.py` asserts every
`Config` field is classified, so a new knob cannot land outside both sets.

**Prompt templates.** The blueprint names stages; `src/swfactory/prompts/<name>.md` is what those
stages *say* to the model, so a change to one changes what gets written and what gets accepted.
The snapshot pins `sha256` of each template the resolved blueprint actually **references**
(`accepted_inputs.STAGE_PROMPTS`), by content and not by path: two workers whose checkouts live in
different directories agree, while two workers on different swfactory builds do not. Only the
referenced set is pinned — a line with no `spec` stage is not fenced by `spec.md`, and `diagnose.md`
(rendered by `maintain`, never by a line) fences nothing. The refusal names the file:
`prompt template prompts/build.md changed (…)`.

The swfactory *version* was considered instead and deliberately rejected: it is stricter, but it
refuses every in-flight epoch on every upgrade — including ones that cannot change what the model
is told — which trains operators to re-accept reflexively, and a routine re-accept is no longer a
decision. `tests/test_accepted_inputs.py` derives the rendered templates from the stage source, so
a new `render_prompt` call site or a new file in `prompts/` fails until it is placed.

**Gate replay.** `SWF_GATE_REPLAY` is split across that line: the fixture path is operational, its
*content* is policy, because the file answers release gates. The snapshot pins `sha256(fixture)`.
Pointing a second worker at its own identical copy is fine; changing the answers, or introducing a
fixture where the epoch was admitted without one, refuses.

**When the inputs genuinely changed.** A changed input is a new epoch, not a stuck Cell:

```sh
uv run swfactory state reaccept <run-id> --actor alice --reason "issue #7 edited by author"
```

This records who re-opened the epoch and why, then retires the pin so the *next* task admits the
current inputs. It re-opens the inputs; it does not re-authorize them — every earlier approval
keeps the digest it was given for, so delivery refuses on it and every gate must be answered
again. A backend-managed Cell refuses this route: advance its epoch through the backend instead,
which is the same fence one level up.

Teardown is the one context built with `enforce_inputs=False`. Cleanup is not work, and a refusal
that leaked the sandbox it exists to close would be a worse outcome than closing it.

### Upgrading swfactory mid-run

The pin's schema is versioned. When a build pins more than the one that admitted a run -- schema 2
added the prompt templates, the packaged review policy and the agent tool policies -- the next task
of that run refuses:

    this Cell epoch was admitted by an earlier swfactory build (accepted-inputs schema 1); this
    build (schema 2) also pins prompt templates, the packaged review policy and the agent tool
    policies, which that admission never covered.

That is deliberate. The old admission cannot vouch for inputs it never looked at, so the route is the
same as for any changed input: `swfactory state reaccept <run-id> --actor NAME --reason TEXT` for a
local run, or advance the Cell epoch through the backend for a managed one, and answer every gate
again. Plan an upgrade for a moment with no runs mid-flight, or expect one re-accept per live run.

The digest a receipt quotes is the one written at admission (`accepted-inputs.digest`), never a
recomputation, so receipts published under schema 1 stay verifiable against what was recorded.


## Budget handling across attempts

After acquiring stage ownership, the runtime refreshes its recorded spend. This also covers a
long-lived CLI context that was idle while another worker advanced the run. Each agent call gets
the smaller of the configured per-call limit and the remaining run budget; an exhausted budget
prevents the call. Returned cost is charged before downloading the agent envelope, so a failed
download is included in the failed stage's spend. The post-call run ceiling remains enforced.

The accounting is based on returned model costs and persisted stage results. A hard process kill
before either is recorded can still leave unknown spend; `recorded_cost_usd` deliberately does
not claim otherwise. Model-reported costs can overshoot a requested cap, which remains a failure.

## Deployment boundary

The worker processes for a run must see the same trusted state directory on a filesystem with
working POSIX locks and durability semantics. Independent replica disks do not coordinate, and
this is not distributed fencing of remote sandbox processes. Platforms without POSIX locks refuse
mutations instead of silently running without exclusion. Keep local state persistent and outside
agent-writable work-cell mounts; `work/` and `state/` remain siblings.

## Whole-factory backup and restore

This page covers one run's local evidence. The factory's authoritative Cell, operation, admission
and repair stores plus the evidence tree are backed up and restored as one unit, with mutations
withheld until the restore is validated and reconciled: see
[backup, restore and upgrade](backup-restore.md).
