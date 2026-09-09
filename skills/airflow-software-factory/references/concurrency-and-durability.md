# Concurrency and durability: what fences what

Read this before changing anything that admits work, publishes, transitions a Cell, spends an agent
budget, or restores state — and before assuming two things that look independent are.

The factory is concurrent at two scales, and they need different mechanisms. Inside one instance
there is shared durable state, so the fences are database writes. Across instances there is no
shared state at all, so the only fence is the repository both instances are obliged to obey.

## Inside one factory instance

| Concern | Fence | Failure it prevents |
|---|---|---|
| Admission and capacity | durable work orders, membership, dispatch outbox | queued work admitted and then never dispatched; one sibling Cell's terminal transition releasing a whole multi-job reservation |
| Cell authority | epoch, checked atomically with the event append | a cancelled Cell publishing; a receipt appended at an epoch that no longer owns the work |
| External effects | operation journal, one owner per attempt with a lease | one logical operation running twice and committing two receipts |
| Accepted inputs | a versioned snapshot pinned at admission | the issue body, blueprint or policy changing between two tasks of one epoch — a person approves one plan, the factory builds another |
| Human gates | mode declared in the blueprint, enforced at construction, recording and continuation | `SWF_APPROVE=auto` converting a declared human gate; a missing response recorded as approval |
| Agent spend | a reservation journalled before the provider is paid | a kill after a paid call losing the charge, so the next run rebuilds a budget with more money in it than the account has |

Two rules hold across all of them.

**A durable fact must be committed before the refusal that reports it.** `with self.db` rolls back
on any exception, so a branch that writes a terminal state and then raises loses the very state a
later process needs. Defer the raise until the write has landed.

**Replaying a committed receipt is observation, not a new effect.** Fences that refuse new effects
must sit *after* the replay check, or idempotent retry breaks and a redelivered Airflow task opens a
second pull request.

## Across factory instances

Several sessions can run this factory at once, each inside its own AI harness, each with its own
Airflow, backend and state root. They share nothing but the repository — deliberately. A
coordination service was proposed and refused: it handed a new lower-trust credential an unbounded
write path, and its safety rested on nobody consuming its signals.

**Where to spend energy.** `refs/swf/claims/<key>`, one ref per issue × target. Git's ref creation
is a compare-and-swap the server enforces: pushing a ref that does not exist succeeds for exactly
one session. The claim carries a lease, because a harness session dies in ways that leave no trace
here — a context limit, a killed container, a spend limit reached mid-loop — and a lock with no
expiry strands its issue forever.

**What lands on the remote.** The publish branch is keyed on the work, not the run:
`factory/<issue>-<sha256(repo, target, issue)[:12]>`, the same inputs `CellIdentity.stable_id`
uses. Every session working one issue × target converges on one ref and one pull request, adopted
through a marker in the PR body rather than duplicated.

**A claim authorizes nothing.** It is advice about where to spend fuel, never permission to publish.
The Cell epoch remains the mutation authority; the publication lease remains the arbiter of the ref.
A test pins that no module on the mutation path imports the claim module, so a stolen, expired or
forged claim can only waste one session's time — it cannot cause a double publication.

## Two traps that have actually bitten

**Leasing against what you observed, not what you published.** `--force-with-lease=<ref>:<observed>`
looks correct and is not: instance B observes A's commit, does not contain it, and force-pushes
straight over it holding a perfectly valid lease. The question is not "did the ref move since I
looked" but "is what is on the remote mine".

**Publishing that writes.** A publication must not create state as a side effect. Minting an
identity file into the state root failed every run under srt, whose kernel confinement denies that
write — and the identity did not need a file at all: it is derived from the state root the instance
owns.

## Restoring state

A restore is where every fence above quietly comes back wrong. The rules that matter:

- A backup that catches three of four stores mid-write is worse than none, because it looks
  restorable. Quiesce, and prove the quiesce.
- Cross-store joins are the failure that looks like success: each store internally valid, the
  relationships between them lost.
- **An allowlist built from a snapshot cannot name what the snapshot lacked.** Cell identity is
  deterministic, so a Cell first activated *after* the backup is rebuilt under the same id, finds no
  journal row for its publication, and opens a second pull request. While a restore window is open,
  every Cell observes the remote first — not only the ones the snapshot knew about.
- Epoch fences are monotonic inside a running factory and **not** across a restore. A process the
  live factory had fenced out has its writes accepted again. That is reported rather than prevented,
  because carrying epochs forward would break the receipt-to-epoch join the whole contract rests on.

## Phases, and what they are not

One backlog under many sessions moves through `free` (no session holds it), `condensed` (one session
holds it and is paying the lease) and `sublimating` (the holder stopped paying; it is returning to
free).

This is naming, not authority. The repository's physics families are research — "advisory and
observational only … must not appear in the product's cognitive path" — and nothing branches on a
phase. It is what an operator reads: an all-`condensed` backlog has every session busy, an all-`free`
one has them idle or blind, and a rising `sublimating` count means sessions are dying mid-loop and
abandoning work, which is invisible from any single session's own logs.
