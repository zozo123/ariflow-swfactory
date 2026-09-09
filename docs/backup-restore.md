# Backing up, restoring and upgrading factory state

The factory's authoritative state is these things in one directory — the state root, `.factory/` by
default:

| Store | Path under the state root | Holds |
| --- | --- | --- |
| Factory Cells | `cells.sqlite3` | Cell identity, epoch, lifecycle state and the event history that fences mutations |
| Operation journal | `control/operations.sqlite3` | every external effect's intent, in-doubt state, observation and committed receipt (#2042) |
| Admission and dispatch | `control/admission.sqlite3` | work orders, capacity membership and the dispatch outbox (#2058) |
| Repair leases | `control/repairs.sqlite3` | cleanup debt and who is currently repairing it |
| Evidence | `evidence/<cell_id>/events.jsonl` | the hash-chained record of what each Cell actually did |
| Run directories | `<run_id>/state/` | the accepted-inputs pin (#2065), recorded approvals (#2066), stage verdicts and cost accounting |

They are one unit. A backup that catches four of them is not a partial backup, it is a **wrong**
one: it looks restorable, and restoring it starts a factory whose journal disagrees with the world.
Run directories are in the manifest for the same reason: a restore replaces the whole state root, so
a backup that skipped them would verify clean and still delete the record of which inputs a human
approved.

## The supported deployment boundary

**One host, one shared local state root, one backend process.** That is the qualified shape, and
`swfactory.deployment_profile` enforces it rather than describing something else:

- `replicas` must be `1`. A second replica opens its own copy of these SQLite files and its own
  copy of the operation journal; both then re-drive the same in-doubt publication.
- `topology` must be `single-host-local-state`. `multi-host-postgres` is **unqualified** and
  refused. This project has not implemented a distributed datastore, and a profile that promises
  one is worse than a profile that has none.
- No Postgres DSN. Airflow keeps its own metadata database; the factory's five stores do not.
- The state root must be on a local filesystem. NFS, CIFS/SMB, 9p and sshfs are refused by name,
  because SQLite's locking cannot fence a second writer there. A host that will not report its
  filesystem type is allowed through — a false refusal only teaches operators to add a bypass flag.
- POSIX advisory locking must work on the state root; it is probed on every backend start.

Several receiver/worker processes on that one host may share the state root. That is the shared
local state the boundary is named for. Two hosts is not a supported configuration at any distance.

## Schema versions and the rollback refusal

Every store stamps `PRAGMA user_version` with the schema this binary writes, and the evidence tree
carries `evidence/.schema.json`. Three rules follow, all enforced when a store is opened:

1. A store stamped **higher** than this binary supports is refused. Rolling a binary back is safe;
   letting the older binary write state the newer one owns is not.
2. A store stamped at this binary's version must already hold its tables. The old behaviour —
   `CREATE TABLE IF NOT EXISTS` on open — would quietly rebuild a table that a partial restore
   dropped and hand back an empty, healthy-looking factory.
3. An unstamped legacy store is adopted only after its tables and Cell row versions are checked.

`uv run swfactory backup status` prints all five versions and exits 2 when any is incompatible.
**Run it with the target binary before a rollback**; that is the rollback gate.

## Taking a backup

```sh
uv run swfactory backup create /backups/factory-$(date +%s) --state-root .factory
uv run swfactory backup verify /backups/factory-1757000000
```

`create` holds a write transaction on every store simultaneously, in a fixed order, so all five are
captured at one logical instant; if any store cannot be quiesced the whole backup is abandoned
rather than written torn. Those locks are taken one at a time, so the instant is *proved* rather
than assumed: each store's `PRAGMA data_version` is read before the first lock and again once the
last is held, and a store that committed while the others were being fenced abandons the backup —
otherwise the Cell store is captured a moment older than the journal beside it, and the manifest
still says `quiesced: true`. Each database is copied through SQLite's online backup API, so
write-ahead log content is folded into the copy — `cp *.sqlite3` is not a backup and loses exactly
the commits that are newest. The manifest is written last and carries every file's SHA-256, each
store's schema version and each Cell's evidence tail digest. A directory with no complete manifest
is a copy, not a backup, and `verify` says so.

Run backups on an idle or lightly loaded factory. The quiesce window is short, but it does block
writers, and a 30-second wait is a refusal rather than a queue.

## Restoring, and reconciling before anything replays

```sh
uv run swfactory backup restore /backups/factory-1757000000 \
    --state-root .factory --actor alice --reason "host loss 2026-09-09"
uv run swfactory backup status --state-root .factory
uv run swfactory backup resume --state-root .factory --actor alice --reason "verified"
uv run swfactory backup reconciled --state-root .factory --cell-id cell_...
```

`restore` verifies the backup completely — digests, declared files, undeclared files, evidence
chains — and refuses any store whose schema is newer than this binary, **before it places a single
byte**. It also checks the joins *between* the stores, because each one can be byte-perfect while
the set of them comes from two different instants: an operation bound to a Cell or an epoch the Cell
store does not have (the journal is newer), or a Cell whose own history records an external
mutation the journal holds no receipt for (the Cells are newer, so the factory would re-drive an
effect it has already made). `create` refuses to write such a backup and `verify` refuses to accept
one. Existing state is moved aside (`\<state-root>.superseded-<epoch>`), never deleted: after a
bad restore that tree is the only record of what the factory really did while the backup aged.

A restored factory then holds three states, and this is the part that matters:

The gate is a marker file, `restore/pending.json`, and the same fact is stamped inside the admission
store: deleting the marker to unstick a stuck factory leaves it refusing with a diagnostic instead
of quietly resuming blind replay. `backup close` clears both.

- **`pending`** — every external effect is refused. The snapshot has not been validated, so nothing
  reaches GitHub, Airflow or a provider. `status` exits 1.
- **`reconciling`** — after `resume`, mutations are allowed *only if they observe first*. Each Cell
  the snapshot restored must look at remote state before its first attempt, because the snapshot
  cannot contain an effect that committed after it was taken. This is #2042's
  `observe_before_first_attempt`: without a reconciler the journal raises `OperationInDoubt`
  instead of publishing, and with one it adopts the pull request that already exists.
- **`clear`** — only after `backup close --window-reviewed`. The marker is archived under
  `.factory/restore/completed-*.json` as the drill's evidence.

While the window is open **every** Cell observes before its first attempt, not only the Cells the
snapshot listed. Cell ids are a hash of repo/target/issue, so a Cell first activated after the
backup was taken is rebuilt under the same id by the restored factory, finds no journal row for the
pull request it already opened, and would open a second one. No local record can name that Cell —
which is why emptying the worklist does not end the window and an operator does:

```sh
uv run swfactory backup close --state-root .factory --actor alice \
    --reason "checked the 40 minutes between snapshot and restore" --window-reviewed
```

`close` refuses while any restored Cell or dispatch intent is unreconciled, and refuses without
`--window-reviewed`, naming the interval the state cannot see. That interval is the residual risk of
any restore: effects committed inside it exist only at the remote, and after the window closes the
factory trusts its own journal again.

## What a restore cannot undo

These survive every refusal above, and an operator has to plan around them rather than discover them
during an incident:

- **Mutation fences move backwards.** A Cell's epoch (#2041) is monotonic inside a running factory
  and is not monotonic across a restore: the snapshot's epoch becomes the truth again, so a process
  the live factory had already fenced out at a higher epoch has its writes accepted. `restore`
  compares the state root it sets aside against the snapshot and records every rolled-back fence in
  the marker; `backup status` prints them. Stop every worker sharing the state root before
  restoring, and treat a printed rollback as a process that must be killed, not warned.
- **The window itself.** Effects committed between the backup and the restore exist only at the
  remote. That is what `observation_required` is for, and it ends when a human says it does.
- **Retry budgets roll back with everything else.** An operation whose attempts were spent after
  the snapshot comes back with its counter at the snapshot's value, so the factory may attempt it
  more times than the budget allowed. While the window is open each of those attempts observes
  first; after it closes they are ordinary attempts.
- **The evidence tree is copied, not fenced.** The four databases are held under a write
  transaction; an evidence append is a file append and takes none of those locks, so the captured
  evidence can be a moment newer than the journal beside it. The chain is verified, so this is skew
  and not a torn file, but "one instant" is exactly true only of the databases.
- **Run directories are copied without holding their run locks.** A run journal can be captured
  mid-append; `RunState` reads a torn tail as a torn tail, and the manifest digests whatever was
  copied, so `verify` will not notice.
- **A newer Cell store is only detectable where something else points at it.** The join check finds
  a Cell that records an external mutation with no receipt, and an operation whose epoch is ahead of
  its Cell. A Cell store whose only extra history is a local epoch takeover has nothing pointing at
  it from another store, and no check here will see that it is newer.

A restored operation that a recovery plan would call `retry` is reported as `observe` instead while
the window is open: a snapshot taken mid-attempt cannot prove the attempt never reached the
provider, and #2042's planner is reasoning about a factory that watched its own attempts.

`reconciled` refuses a claim the records do not back: a Cell with unresolved operations and no
recorded observation stays gated, and so does a dispatch intent still `inflight` in the outbox.
"We checked" is not evidence; the journal's observation is.

## The drill

Run this whole sequence against a scratch copy after any change to a store schema, and before any
binary rollback. `tests/test_restore_contract.py::test_the_operator_drill_runs_end_to_end_through_the_cli`
runs exactly it, hermetically, on every CI run.

1. Seed or take a factory holding queued work, a committed publication, an in-doubt effect and
   cleanup debt.
2. `backup create`, then `backup verify`.
3. Advance the live factory (publish something the backup cannot know about).
4. `backup restore --replace-existing`, then `backup status` — it must exit 1 and say `pending`.
5. `backup resume`, then attempt the effect the snapshot still thinks is owed. It must refuse
   without a reconciler and adopt the existing remote effect with one.
6. `backup reconciled --cell-id ...` per Cell, then `backup status` — exit 0, `mutations_allowed`,
   and `observation_required` still true.
7. Re-drive the effect the *live* factory made in step 3 from its rebuilt Cell. It must observe and
   adopt, not publish a second time.
8. `backup close --window-reviewed`, then `backup status` — `observation_required` false.

The webhook inbox (`.factory/webhooks/inbox.sqlite3`) is a separate durable store with its own
backup guidance in [docs/webhooks.md](webhooks.md); it is not part of this manifest. `restore` moves
the *whole* state root aside, so when the inbox is configured under it, it goes with the superseded
tree and must be carried back by hand — undelivered events are otherwise waiting in a directory
nothing reads any more. Point `SWF_WEBHOOK_INBOX` outside the state root, or restore it explicitly.
Local run evidence under `.factory/<run-id>/` is captured and restored with the stores; its
per-run recovery semantics are in [docs/run-recovery.md](run-recovery.md).
