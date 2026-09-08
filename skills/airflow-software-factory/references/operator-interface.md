# Operator interfaces over a factory

Read this reference before building, reviewing, or auditing anything an operator points at a running
factory: a CLI, a terminal UI, a dashboard, a chat responder, a monitoring probe. A factory that
schedules well and reports badly is not trustworthy, and every failure below is a way an interface
lies while every one of its own tests passes.

## One operations layer, many faces

An operator interface is two separable things, and building them as one is how the second face
starts disagreeing with the first.

| Layer | Owns | Must not own |
|---|---|---|
| operations | validation, readiness, re-reads, the write itself, sanitising untrusted text | rendering, key bindings, colour, exit codes |
| face (CLI, TUI, bot) | layout, input, output format, exit status | any rule about whether an action is safe |

The rule: **a face dispatches and renders; it never decides.** The moment a keystroke and a
subcommand each carry their own copy of "is it safe to answer this gate", the factory has two
policies, and only one of them gets fixed.

In this repository `rust/crates/swf-app/src/ops.rs` is that layer — one `Ops` object holding every
operation. `swf-cli` and `swf-tui` are both renderers over it, so `swf gates approve` and pressing
`a` take the same validation path. `swfactory`'s Python CLI and `herd` control room sit over
`src/swfactory/control.py` for the same reason.

Prove agreement with a diff, not a paragraph. `swf snapshot --json` and `swfactory herd --once
--json` emit the same document, and `scripts/snapshot_diff.py` holds them to it against one live
server. It compares a *normalised projection*, not bytes: two clients read the same server
milliseconds apart, so timestamps, the task state of a job still in flight, the metrics filesystem
scan and each client's transient `errors` are dropped. What must match is identity and shape —
which runs exist, what each fanned out into, which issue each job carries, which gates are
outstanding against which job, what was published. `rust/crates/swf-domain/tests/contract.rs` pins
the shared roll-ups as equivalence cases. Claiming byte-equality would be a stronger promise than
two live reads can keep, and the first spurious diff would teach everyone to ignore the check. Two
control rooms that were only *described* as equivalent will drift within a release.

The client holds no authority of its own. It persists no copy of factory state that a decision could
be made from, it reaches the control plane only through the public API — never the metadata database
— and every write re-reads its subject immediately beforehand.

## Readiness is a property, not a sleep

A human-in-the-loop gate usually becomes *visible* before it becomes *answerable*. Here the detail
row is created just before the task defers, so an answer inside that window makes the scheduler see
a stale executor event and **fail the gate**. A human clicking through a UI never hits it; a polling
script hits it roughly once per dozen gates, which is exactly often enough to be blamed on
flakiness. `scripts/stress_airflow.sh` paid for this in a hand-tuned settle.

Model it instead. `rust/crates/swf-app/src/gates.rs` gives every gate a `ready` flag, true only when
its own task instance is parked in `awaiting_input`, and the write is preceded by a read that was
not the one that selected the gate. Answering an unready gate is refused unless forced, and the
refusal names the failing condition. A run the client could not read leaves every gate of it
unready, which is the safe direction to be wrong in.

Readiness is necessary and not sufficient: the window closes when the *scheduler* has finished
reconciling the worker that parked the task, which is after the task instance already reads
`awaiting_input`. So a gate must also have **existed** for `CONFIRM_INTERVAL` (5 s;
`SWF_GATE_SETTLE_SECS` raises it for a slower deployment without a rebuild). Measure that age with
the server's own `created_at` stamp and a server-derived now, never with a stopwatch this process
started — a local clock measures when *you* happened to look, so it resets on every invocation,
ignores a gate that has been open for a minute, and degenerates into a `sleep` tuned to the fastest
machine anyone runs it on. That version passed nine local runs and failed on the first CI runner.
Keep the local stopwatch only as the fallback for a resource the server did not stamp, and let one
unreadable timestamp degrade one item rather than refuse the batch around it.

Generalise to any create-then-park lifecycle: a resource that exists before it accepts work needs a
readiness predicate plus a server-anchored age, not a tuned delay. Then make the write survive a
lost race — re-read the subject just before mutating it, and report "another operator got there
first" as a distinct conflict outcome rather than a crash or, worse, a silent overwrite.

## A batch is not a loop with better ergonomics

The moment an operator has a hundred gates, one-command-per-gate stops being an interface. But a
command that answers a whole selection has a blast radius, and five properties earn it that power.

**A dry run must be structurally unable to write.** Not "does not write" — *cannot*. Build the
report as a pure function of the selection so the writer is never reached, and test it with a fake
that panics on write, so the proof is in the shape of the code rather than in an observation that
happened to hold. Then check the dry run against the live service too: count what it planned, and
assert those same subjects are still unanswered afterwards. Compare identities, never counts — new
work arrives while you are printing, so an equal count is luck and a changed one is not evidence.

**Serialize per aggregate, parallelise across them.** Concurrent writes to *different* runs are
free; two writes racing inside *one* run made the scheduler answer the second with HTTP 500 and
fail that gate. Hold one write in flight per aggregate id and let the rest overlap — a busy factory
is wide, not deep, so nearly all the speed survives. Bound the total too: a filter that matched
everything must not open a socket per match against the service you are trying to help.

**One bad subject must not abandon the rest.** Collect a per-subject outcome, keep every one in the
report, and let the exit code reflect only genuine failure.

**Distinguish "someone else got there first" from "this went wrong".** A conflict is the system
working in a shared control room. A batch that exits non-zero for it teaches operators to stop
reading its exit code, which is the most expensive thing a tool can teach.

**Never report an outcome you cannot know.** A task that dies mid-write may or may not have landed
its mutation. Reporting that as "skipped" with a zero exit is the one answer that is certainly
wrong: mark it failed, exit non-zero, and tell the operator to go and re-read that subject.

## Ship it so it can be installed and verified

An operator client is only as good as the path to getting it. Publish a checksum file covering every
asset, and have the installer verify it with no flag to skip — an installer that downloads a binary
and runs it unchecked has only made the command shorter. Be honest in the docs about what that buys:
it catches a truncated or swapped download, and it is not provenance, because the sums travel beside
the file they describe.

Rehearse the release before performing it. A publish pipeline that has never run is discovered on
the day, in front of users, with a half-made release behind it — so give it a dry run that takes the
identical build path and stops one step short of publishing.

## Name the claim you are making

"It worked" is three different claims with three different forgers. Collapsing them into one green
tick is how a factory reports success it has not earned.

| Level | The claim | Who could forge it |
|---|---|---|
| reported | the workflow succeeded | the run itself, in files the run committed |
| published | a branch, PR, or artifact exists | the orchestrator credential |
| verified | we re-derived the result ourselves | nobody: fresh clone, recomputed hashes, re-run tests |

`rust/crates/swf-app/src/delivery.rs` keeps the three as separate fields and enforces that nothing
at `reported` can satisfy a `verified` check. Independent verification costs a clone and a test run,
so it is opt-in — and when it was not attempted the report says so rather than quietly settling for
the weaker claim. `scripts/swf_e2e.sh` clones each published branch from the remote and re-runs that
target's own test command; no verdict is taken from the worker's own workdir.

Give the failure verdicts the same care as the successes: a contradiction between levels is
`refuted`, a correctly recorded `[BLOCKED]` delivery is a *pass*, and "we could not tell" is
`inconclusive` and never `verified`.

## Page every collection

A client that pages and one that does not behave identically on small jobs and differ only on the
large ones — the runs actually worth watching.

Airflow clamps the `limit` query parameter to `[api] maximum_page_limit` (default 100) with a plain
`min()`: no header, no warning, no error. A request for 500 task instances returns `200 OK` with the
first hundred, and a control room that trusts the response draws a complete-looking table over a
third of a 300-task run.

`_paged` in `src/swfactory/control.py` and the loop in `rust/crates/swf-adapters/src/airflow.rs`
walk `offset` under three rules, each of which is a data-loss bug when broken:

1. advance `offset` by the rows actually **returned**, never by the limit requested — the server may
   have clamped it;
2. treat an empty page as the terminator, because `total_entries` may be `null` or absent;
3. never let a caller's "newest N" shrink the ask below one row — `min(0, 100) == 0` asks this API
   for nothing at all.

Bound the loop, and when the bound is hit record it (`truncated`, surfaced as an `airflow:truncated`
entry in the snapshot) instead of swallowing it. A short table is honest; a short table that claims
to be whole is not. Test the shapes that make an off-by-one look like success: three full pages, a
null `total_entries`, a short final page, an exactly-page-sized collection, an empty first page.

## Make a read's failure visible without making it the answer

Two conventions keep scripts and monitors honest:

- **Exit codes are a table, not an accident**, and they are public surface under the project's
  semver policy. A caller seeing `4` must know it needs a credential rather than a retry.
  `rust/crates/swf-cli/src/exit.rs` maps every operational error onto one code in one place.
- **Reads report a dead source inside the answer, not as the answer's failure.** `snapshot`,
  `attention` and `jobs list` always exit 0 and carry an `errors` array naming every source they
  lost, because a monitoring step must never confuse "nothing needs a human" with "we could not
  tell".
  Structural promises hold under degradation too: a listing stays an array, and truncation or a
  per-source failure never changes a document's type.

With `--json`, print exactly one document to stdout and every diagnostic to stderr, including on
failure — a pipeline that captured the document should never have to inspect `$?` to interpret it.

## Sanitise untrusted text in the layer, not the renderer

Log lines, gate bodies, PR titles and issue text are written by agents, by CI and by strangers, and
a terminal obeys `ESC [ 2 J` no matter who wrote it. Rendering such a string unfiltered escalates
*whoever produced a log line* into *whoever is watching the factory*. Drop C0 controls and DEL, keep
`\t`, swallow CSI/OSC and the other escape families whole, and bound every line.

Do it once, in the operations layer, on the way out of the adapter: one `sanitize` module
(`rust/crates/swf-domain/src/sanitize.rs`) applied before a gate body or log line ever reaches a
face. A face added next year cannot remember a rule it never had to know.

## Before shipping an operator interface, prove

1. Every action a face offers exists as one operation in the shared layer, with no second
   validation.
2. Two faces over the same layer are compared by an automated diff of one real document.
3. No credential is stored in the client's config; it stores the *name* of the variable, and the
   config loader rejects a literal secret rather than warning about it.
4. Every mutating command re-reads its subject immediately before the write.
5. Gate readiness is a modelled predicate; forcing past it is explicit and named.
6. Delivery reporting distinguishes reported, published and verified, and can reach verified.
7. Every collection read pages to exhaustion, with truncation reported.
8. Non-interactive use never blocks on a prompt: a destructive action without confirmation is a
   usage error that explains itself.
9. Untrusted text is sanitised before rendering, in shared code.
10. Destructive infrastructure commands re-check ownership against a fresh listing at the moment of
    removal, and refuse when no owner is configured.
11. A bulk action has a dry run that cannot write, serializes per aggregate, keeps a per-subject
    outcome, treats a lost race as a conflict rather than a failure, and never reports an outcome
    it cannot know.
12. The published artifact can be installed and checksum-verified by someone who has never seen the
    repository, and the release pipeline has been rehearsed rather than first run in public.
