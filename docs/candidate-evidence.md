# Candidate evidence

A candidate is one answer to one question: an exact input commit in, an exact output commit out.
This pipeline keeps the answer inspectable after its disposable workspace is gone. Each step
writes a digest-bound receipt, and each later step re-verifies the earlier ones instead of
trusting them.

```text
recorded commit --source-snapshot--> archive digest ----------------+
       |                                                             |
       +--candidate-worktree create--> edit, commit --freeze--> immutable ref
                                                                     |
                                       candidate-evidence build <----+
                                       diff + artifacts + digests
                                                  |
                     stored CampaignReport -------+--> experiment-tree    (lineage)
                                                  +--> campaign-decision  (fan-in)
                                                              |
                                                    review / promotion gate
```

The pipeline adapts three disciplines from
[alphaXiv/OpenResearch](https://github.com/alphaXiv/OpenResearch): a run executes an immutable
archive of its recorded commit, parallel directions get private Git worktrees, and a later
experiment descends from an earlier result rather than from the original base. It does not import
OpenResearch's scheduler, storage, UI or authority model.

## Snapshot

```sh
uv run swfactory source-snapshot . --revision HEAD --json > source.json
```

`git archive` of the resolved commit becomes `.factory/source-snapshots/<sha256>.tar`. The receipt
records the full commit SHA, the archive's SHA-256 and byte size, its path and whether it was a
cache hit. Dirty tracked edits and untracked files are not inputs. A cached entry is never trusted
by name: its digest and size are recomputed before reuse, and a mismatch fails. The cache is an
optimisation; the digest is the identity.

## Worktree

```sh
uv run swfactory candidate-worktree create cand-a --input-head <commit>
# edit and commit inside the printed worktree, then:
uv run swfactory candidate-worktree freeze .factory/candidate-worktrees/<hash>.json
uv run swfactory candidate-worktree remove .factory/candidate-worktrees/<hash>.json
```

Each candidate gets one detached worktree at its exact input commit, under a path derived from a
hash of the candidate id, never from raw user text. Freeze refuses uncommitted files, an output
that did not advance past the input or does not descend from it, and a receipt whose path became
a symlink. It records the answer under `refs/swfactory/candidates/<hash>` with a create-only ref
update, so an answered candidate cannot be rewritten, and writes `<hash>.frozen.json`. Removing the
worktree keeps the ref: the ref is evidence, the worktree is disposable. A remote provider need not
use worktrees; it can consume the same snapshot and return a committed output revision.

## Recipe

A verification is reproducible only if the command that ran is as fixed as the bytes it ran on.
`.swfactory/candidate-run.json` is read with `git show <commit>:<path>`, never from the working
tree, so a dirty orchestrator checkout cannot change it:

```json
{
  "schema_version": 1,
  "argv": ["uv", "run", "pytest", "-q"],
  "cwd": ".",
  "timeout_s": 1800,
  "resources": {"cpus": 4, "memory_mb": 8192},
  "environment": {"PYTHONHASHSEED": "0"}
}
```

`argv` is an array, not a shell command; `cwd` must stay inside the repository; timeout, CPU and
memory are bounded; unknown fields fail closed. Environment keys that look secret (`TOKEN`,
`SECRET`, `PASSWORD`, `PASSWD`, `CREDENTIAL`, `API_KEY`) are refused, and a non-empty `secret_env`
is refused too: a stage that needs a credential asks the trusted broker for a scoped lease (see
[SECURITY.md](../SECURITY.md#credential-authority)). The recipe's canonical digest changes whenever
the command, environment, timeout or resources change.

## Bundle

```sh
uv run swfactory candidate-evidence build \
  .factory/candidate-worktrees/<hash>.frozen.json source.json .factory/candidates/cand-a-evidence \
  --repo . \
  --artifact agent-log=.factory/runs/cand-a/agent.log \
  --artifact benchmark=.factory/runs/cand-a/benchmark.json
uv run swfactory candidate-evidence verify .factory/candidates/cand-a-evidence --repo .
```

```text
cand-a-evidence/
├── manifest.json       canonical identity and digests
├── candidate.diff      exact --binary --full-index input..output diff
├── RESULT.md           human-readable projection of the manifest
└── artifacts/000-<digest> ...
```

The manifest binds the candidate id, input and output SHAs, the frozen ref, the source snapshot's
digest and size (the archive itself stays in the snapshot store), the diff's digest and every named
artifact's digest, size and retained path, under one canonical manifest digest. Artifacts are
copied in; the manifest never points at the caller's mutable original. Through the library,
`build_candidate_evidence_bundle(..., inherited_recipe=...)` also records the recipe the candidate
inherited from its input commit, and verification reloads it from Git. Build refuses a snapshot
whose commit is not the candidate input.

Verification fails closed when `manifest.json` changed without a matching digest, a retained file
is missing, a symlink or different bytes, the frozen ref no longer names the recorded output, or
the bundle has unsafe paths or duplicate artifact names. `RESULT.md` is a projection, not
evidence. The diff is kept because two candidates can pass the same tests with very different
changes; a score without the patch makes selection opaque.

A verified bundle can be held for the promotion window in a content-addressed store:

```sh
uv run swfactory candidate-evidence retain .factory/candidates/cand-a-evidence --repo . --ttl-hours 168
uv run swfactory candidate-evidence pin sha256:<digest>      # unpin reverses it
uv run swfactory candidate-evidence gc --dry-run --json
```

Objects live under `.factory/candidate-retention/objects/<digest>/` with a lease in
`leases/<digest>.json`. Re-retaining never shortens a lease, and pinning means only "keep these
bytes", never approved, selected or released. GC removes only expired, unpinned objects and reports
malformed leases, missing objects and redirected paths instead of guessing. The capability stays
experimental until release itself binds and verifies the retention lease.

## Replay

```sh
uv run swfactory snapshot-replay . source.json .factory/replays/run-1
uv run swfactory snapshot-replay-verify . source.json .factory/replays/run-1 --json
```

Replay runs the recipe committed at the snapshot's own commit against that snapshot's bytes
(`--recipe-path` defaults to `.swfactory/candidate-run.json`) and retains `recipe.json`,
`stdout.bin`, `stderr.bin` and `receipt.json`. The receipt binds source and recipe digests, the
requested CPU and memory, the resolved executable and its SHA-256, exit or timeout state, duration
and stream digests. Verification re-hashes every stream and the executable and reloads the recipe
from Git.

Replay refuses a snapshot and recipe from different commits, changed archive bytes, archive
traversal, links, devices and other non-regular members, a `cwd` outside the snapshot, a missing
executable, tampered evidence, and a non-empty or symlinked destination. It is replay evidence, not
a sandbox: untrusted candidate code belongs in an isolated provider, and the receipt records
`resource_enforcement = declared-not-enforced-local` rather than pretending to enforce quotas.

## Lineage

One campaign is one decision round; its candidates are siblings answering the same question from
the same input. A later round starts from the exact recorded head of the previous round's selected
candidate, so wins accumulate (no flat fan from the base) and unrelated ideas are not chained just
to look deep.

```text
base
├── repair   -> sha-a
├── rethink  -> sha-b   * selected
└── scratch  -> sha-c

sha-b
├── repair   -> sha-d
└── scratch  -> sha-e   * selected
```

A candidate is **answered** when it completed and produced a distinct output head; the answer may
still be bad, but it is frozen evidence. It stays **provisional** after a runner crash,
cancellation, a missing output head or an output equal to its input; provisional nodes may be
rerun, answered nodes are never rewritten. Candidate identity hashes campaign, Cell, epoch,
strategy, input head, generation parent, candidate parent, the parent decision digest, any search
provenance digest and depth, so the same strategy asked at another tree position is a different
question. `plan_requests(..., parent_decision_digest=...)` binds the verified parent campaign
decision into every child's identity. `parent_generation` (factory lineage) and `parent_candidate`
(experiment lineage) stay separate.

A stored `CampaignReport` (schema 3) carries an `experiment_round` with its nodes, depth, parent
candidate and winner:

```sh
uv run swfactory experiment-tree .factory/campaigns/round-0.json .factory/campaigns/round-1.json
uv run swfactory experiment-tree round-0.json round-1.json --mermaid   # or --json
```

It fails closed when a later round skips a depth, names the wrong parent, starts anywhere but the
previous winner's recorded head, reuses a candidate id or selects a provisional node. Mermaid node
ids come from validated positions, so candidate-controlled strings cannot add edges.

## Campaign decision

A `CampaignReport` records evaluations, ranking, refusals and the proposed winner; a bundle records
one candidate's bytes. The decision manifest joins them, so a reviewer can prove which retained
evidence the fan-in used:

```sh
uv run swfactory campaign-decision build \
  .factory/campaigns/round-0.json .factory/campaigns/round-0.decision.json --repo . \
  --candidate-evidence cand_repair=.factory/candidates/cand_repair-evidence \
  --candidate-evidence cand_rethink=.factory/candidates/cand_rethink-evidence
uv run swfactory campaign-decision verify .factory/campaigns/round-0.decision.json --repo . \
  --candidate-evidence cand_repair=.factory/candidates/cand_repair-evidence \
  --candidate-evidence cand_rethink=.factory/candidates/cand_rethink-evidence --json
```

For each candidate it retains id, strategy, state, input and output heads, frozen ref, ordered
evaluations, cost, duration and bundle digest, plus the winner, reason, ranking, refusals,
independence findings and cancellation state, under one digest. Every answered candidate needs a
bundle, losers included: keeping only the winner keeps a result and loses the experiment. Build
fails closed when an answered sibling has no bundle, a bundle belongs to another candidate or has
different lineage or ref, evidence is supplied for an unanswered candidate, the ranking omits or
duplicates a candidate, or the winner is not backed by answered evidence. Verify re-hashes every
bundle, re-checks every ref and requires the evidence set to equal the answered set, so no winner
swap, ranking edit, deleted loser or substituted bundle keeps the original digest.

## Research loop

A report carries two selections: `exploration_selection`, the evidence-best answered candidate a
descendant round may start from, and `selection`, the promotion proposal that still needs the human
gate. A research loop may decide what to test next; it may not decide what reaches `main`.
Exploration can be wild; convergence is not: replaying the same candidate identities, evidence
digests, requirements and authority state must reproduce the same promotion decision, so
first-finisher wins, unevidenced votes and random tie-breaks are excluded.

```sh
uv run swfactory research-schedule --max-depth 3     # --json for the document
```

```text
depth 0: repair rethink scratch
depth 1: repair rethink
depth 2: repair
depth 3: repair
```

The cooling schedule narrows width by one per depth to a single strategy, never exceeds
`--max-candidates`, and refuses duplicate strategies. It is data, not a controller, and not a claim
that this cooling law is optimal.

## Authority boundary

Nothing in this pipeline schedules, approves, publishes or promotes. No in-tree runner produces
campaigns; these commands read and verify what a campaign recorded. Airflow is the only lifecycle
scheduler, the Factory Cell owns durable work identity and epoch fencing, and human and
branch-protection gates remain the promotion authority. A bundle, a decision digest or a lineage
winner is evidence for selection and review, never permission.
