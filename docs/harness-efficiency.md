# Harness efficiency: preserve evidence, reduce replay

The software factory treats harness efficiency as motion, never authority. Airflow still owns the
lifecycle; candidate identity, approvals, publication, and promotion evidence are unchanged.

The useful pattern from NVIDIA SoL-Pi is not another scheduler. It is a set of worker-side
mechanisms for reducing repeated model context while retaining exact observations. swfactory adopts
only the mechanisms that fit its trust boundary.

## Implemented: ObservationPack for failed verification

Before this change, failed verification retained only the last 6000 characters of stdout and the
last 2000 characters of stderr for the next repair call. Earlier diagnostic output was gone.

When either stream exceeds that old bound, swfactory now:

1. archives the complete sandbox result in host-owned run state;
2. runs the repository's deterministic secret-shape scan plus a conservative secret-assignment check;
3. mirrors the exact source into ignored .factory/observations scratch only when that classifier is clean;
4. assigns a content-addressed obs:sha256:<digest> handle;
5. selects bounded failure windows plus small head/tail context;
6. verifies every selected line range against the archived source and drops any sensitive excerpt;
7. passes the repair call the smaller verified observation and, only for a clean source, the recall path.

Small failures keep the previous prompt contract exactly.

## Evidence-preserving reducer

Reduction is deterministic and local. A compact observation is only a navigation aid. The archived
source remains authoritative.

If reduction cannot verify its excerpts, it raises. If the reduced prompt is not smaller than the
legacy tail, the factory falls back to that legacy tail and adds the exact-recall handle.

Raw diagnostic text is not automatically committed. The delivery artifact carries only:

- the observation digest and stable handle;
- source byte and line counts;
- compact-versus-legacy prompt bytes;
- exact quote spans and quote digests;
- whether a remote reducer was used;
- whether raw logs were committed.

Today both final flags are false. There is intentionally no remote reducer integration. If a log
matches a known token shape or a conservative secret-assignment pattern, the full source remains
host-only and the agent receives no raw recall path. This deliberately prefers a false-positive
loss of debugging context over expanding the remote-model trust boundary.

## Implemented: packed review context

Large code diffs are another repeated-context boundary. Ordinary review sends one diff; Liquid
review sends the same diff to three independent specialist lanes in each round. For diffs above
12 KB, the factory now:

1. archives the exact patch in host-owned run state;
2. refuses the optimization when the diff matches the local secret-shape classifier, preserving
   the prior full-diff behavior without creating another raw scratch copy;
3. mirrors clean patches into ignored `.factory/observations/` scratch behind a
   `diff:sha256:<digest>` handle;
4. replaces the injected full diff with a deterministic file/hunk/add/delete index;
5. tells the read-only reviewer that the index is navigation only and requires it to page the exact
   patch or changed files before making findings;
6. records only digest, source/head identity, file statistics, prompt bytes, and fan-out savings in
   the committed receipt.

Small diffs keep the previous prompt exactly. Liquid computes the pack once per review round and
reuses the same exact handle across all three lanes, so the optimization reduces repeated context
without giving any lane a different candidate view.

## Constrained harness research

Harness auto-research is useful only if efficiency stays subordinate to the factory invariants. An
experiment with a harness mechanism keeps its candidate SHA (the exact bytes produced), authority
digest (canonical promotion-relevant facts) and evidence digest separate from its efficiency
vector (prompt bytes, cost, wall time, model turns). A mechanism that changes the candidate SHA or
the authority digest, leaves evidence incomplete, or turns the verifier red is refused:

> An accelerator may change time-to-answer, never the answer.

Such research stays research-only, with no promotion action and no callback into Airflow, GitHub,
approvals, or candidate readiness.

## Mapping the other SoL-Pi mechanisms

Action Fusion maps only to factory-owned validation. swfactory already keeps mutation and
verification inside one bounded Airflow build stage, but it does not grant authority to arbitrary
model-selected shell commands merely to reduce turns. A future fused editing tool should execute
only an approved validation recipe and should retain both mutation and validation evidence.

Online Context Compact maps to the factory-controlled context that crosses otherwise stateless
agent calls. Failed-test ObservationPack and packed review diffs now cover the two largest repeated
payloads under factory control; provider session state remains disabled.

ObservationPack is the cleanest fit because the factory already has host-owned evidence and an
ignored scratch namespace. Stable handles let compute remain disposable while diagnostic identity
survives.

## Target dialect card and per-stage turn caps

Build and fix calls used to spend Read turns rediscovering facts the host already held (the test
command, the source and tests directories, the target's conventions file), and the build and fix
templates told a shell-less agent to run the test command. `stages.target_dialect` now renders
`prompts/dialect.md` once per run from the cached target contract, a sorted `git ls-files` listing
of the declared source and tests directories (at most 60 paths plus the total), and which of
`AGENTS.md` / `CLAUDE.md` exist at the target root. The card says plainly that the agent has no
shell and that the factory runs the test command after the call. It is stored in host control state
`target-dialect.md`, so every plan, build, Plan.work node, repair, review-fix, Liquid and fusion
prompt of the run embeds the same bytes after build commits move HEAD. It is delivered as
`docs/factory/<id>/dialect.md`. The template is pinned in `accepted_inputs.STAGE_PROMPTS`, so editing
it fences in-flight epochs like any other prompt.

A blueprint may lower one stage's turn cap with `[policy.<stage>] max_turns`, for example a
draft-style build budget while review keeps `limits.max_turns`. The override may only tighten: a
value above `limits.max_turns` refuses to load, and each call runs under
`min(Config.max_turns, override)` in both the `claude -p --max-turns` argument and an external
wrapper's `budget.max_turns`. A cap that is too low ends the call as `error_max_turns`, which fails
the stage closed.

On the Plan.work path, `harness_efficiency.pack_planned_files` also gives each node call a snapshot
of its declared files, and each repair call a snapshot of the plan's files, framed by the pinned
`prompts/file_pack.md`. The snapshot is read from git at the HEAD the call starts from, so it holds
committed bytes only and a retry at the same HEAD sees the same text. Files are taken in sorted
order, whole, within a 24 KB content budget. New, binary, non-UTF-8, read-denied (`.env*`) and
over-budget paths are listed with a reason instead of their content. If the local secret classifier
matches any packed file, the whole pack is refused, and the prompt is then exactly the prompt the
call had before packing existed. The same happens when nothing fits. The receipt is
`docs/factory/<id>/harness-file-packs/<sha256>.json`. It records paths, sizes and digests, never
contents.

`metrics.json` records `agent_calls` (every reserved provider call in the run's call ledger,
including fusion workers' ledgers) and `model_turns` (the sum of settle receipts' `num_turns`; a call
whose outcome is unknown adds no turns), so a change that is meant to cut turns has a number to move.

## Benchmark contract

Do not turn byte savings into a public cost claim. A research run should record, per mechanism and
environment:

- exact source/candidate identity;
- authority digest and evidence digest;
- verifier result;
- prompt/input/output token counts when the provider exposes them;
- measured cost;
- wall time;
- model turns;
- observation bytes and bytes avoided;
- environment/model/toolchain identity.

Only measured runs with equal authority projection and complete evidence may enter the efficiency
frontier. Public README performance claims should cite those retained measurements, not estimates.
