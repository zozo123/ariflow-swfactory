# swf — the factory's native operator binary

`swf` is one native executable that operates the whole software factory from a terminal: connect an
environment, submit governed work, watch every mapped job, read a task's log, answer an approval
gate, verify what was delivered, and remove your own sandboxes. It is an **operator's client**, not
a second factory. It schedules nothing, runs no stage, holds no credential of its own and makes no
decision it did not first re-read from the service that owns it.

The normal connection is Rust console → Python factory backend → Airflow.
See [backend setup and API v1](factory-backend.md) before connecting. Airflow, GitHub and islo
credentials live on that backend; the terminal uses `SWF_BACKEND_TOKEN`.

The code is split across five crates:
`swf-domain` (contracts and the pure roll-ups, no I/O at all), `swf-adapters` (Airflow `/api/v2`,
`gh`, `islo`, the committed `metrics.json` files), `swf-app` (the operations), `swf-tui` and
`swf-cli`. Every command and every keystroke in `swf tui` goes through the same `Ops` object, so the
two faces of the product cannot drift: the TUI is a renderer over the operations layer, never a
second implementation. Airflow keeps scheduling, retries, task mapping and the HITL gates — see
[design.md](design.md) for what that control plane owns.

```sh
swf context add prod --backend-url https://factory.example.com --airflow-url https://airflow.example.com --repo acme/widgets --use
swf doctor                                     # readiness: one line per check, a fix: each failure
swf submit --issue 42 --blueprint factory         # governed work to Airflow
swf attention                                     # what needs a person right now
swf gates approve 'factory/manual__…#1:plan'      # answer one identified gate
swf tui                                           # the same operations, interactively
```

## Install

`swf` is a single binary with no runtime dependency: no Python, no `uv`, no virtualenv. Every
GitHub Release carries one tarball per platform, named `swf-<version>-<target>.tar.gz`, alongside a
`SHA256SUMS` file that covers **every** asset in that release — the four tarballs and the Python
wheel and sdist alike.

### The short way

```sh
curl -fsSL https://zozo123.github.io/ariflow-swfactory/install.sh | sh
```

That works out the platform, resolves the latest release, downloads the archive **and** its
`SHA256SUMS`, verifies the digest, and puts `swf` in `~/.local/bin`. It never uses `sudo` and never
writes outside the install directory. If the checksum does not match it prints both digests and
installs nothing — there is deliberately no flag to skip that check, because an installer that
fetches a binary and runs it unverified has only made the command shorter.

| Variable | What it does |
| --- | --- |
| `SWF_VERSION=v2.1.0` | pin an exact release instead of "latest" — do this in CI, where a moving target is a reproducibility bug |
| `SWF_INSTALL_DIR=/usr/local/bin` | install somewhere else (you supply the write permission) |
| `SWF_BASE_URL=https://mirror.internal/swf` | fetch the assets from a mirror, for an air-gapped host |

Piping a script into a shell is a decision to trust the source, and the right instinct is to read it
first. It is short and it is the same file either way:

```sh
curl -fsSL https://zozo123.github.io/ariflow-swfactory/install.sh -o install.sh
less install.sh && sh install.sh
```

The steps below are that script, by hand. Do them if you would rather not run someone else's shell
script, or if you are packaging `swf` for someone else.

| Target | The machine it is for |
| --- | --- |
| `aarch64-apple-darwin` | Apple silicon Mac |
| `x86_64-apple-darwin` | Intel Mac |
| `x86_64-unknown-linux-gnu` | most Linux servers, containers and CI runners |
| `aarch64-unknown-linux-gnu` | Graviton, Ampere, arm64 images |

### 1. Download the archive and its checksums

Take both from the same release, and check one against the other before unpacking anything:

```sh
VERSION=2.1.0
TARGET=aarch64-apple-darwin
BASE=https://github.com/zozo123/ariflow-swfactory/releases/download/v$VERSION

curl -fLO "$BASE/swf-$VERSION-$TARGET.tar.gz"
curl -fLO "$BASE/SHA256SUMS"

shasum -a 256 --ignore-missing -c SHA256SUMS     # sha256sum -c … on Linux
```

`SHA256SUMS` lists all six assets, so `--ignore-missing` is what lets it verify the one file you
actually downloaded; without it the five you skipped are reported as failures and the real answer
is lost in the noise. The line to read is `swf-<version>-<target>.tar.gz: OK`.

**Why bother, honestly.** A digest nobody compares is decoration — publishing it is only worth the
bytes if someone runs the check, which is why the command is here rather than left implied. What it
buys you is real but bounded: a truncated, corrupted or half-swapped download is caught before you
put the file on `$PATH`. What it does **not** buy you is provenance. `SHA256SUMS` is not a
signature, and it travels beside the file it describes, so anyone able to replace the tarball on a
release could replace the sums with it. The defence against *that* is that the tag, not the release
page, is the source of truth: every leg builds from the tagged tree with `--locked`, so you can
rebuild from source (below) and compare rather than trust.

### 2. Unpack it and put `swf` on `$PATH`

The archive unpacks into a directory of its own name, so two versions or two platforms can sit in
one download folder without colliding and `tar xzf` never scatters files into the current
directory. Inside are `swf`, `LICENSE`, `README.md` and a `completions/` directory.

```sh
tar xzf "swf-$VERSION-$TARGET.tar.gz"
install -m 0755 "swf-$VERSION-$TARGET/swf" ~/.local/bin/swf   # any directory on $PATH
swf version                                                   # must print $VERSION
swf doctor                                                    # then: is this machine ready
```

`swf version` is the check that matters after a copy: the release refuses to publish a tarball
whose binary disagrees with the version in its name, so a mismatch here means the wrong file
landed on `$PATH`, not a bad build.

The macOS binaries are checksummed but **not** codesigned or notarized, so Gatekeeper quarantines a
downloaded tarball and kills the first run: `xattr -d com.apple.quarantine ~/.local/bin/swf`.

### 3. Shell completions

Every archive already ships `completions/swf.bash`, `completions/swf.zsh` and
`completions/swf.fish`. They are generated by the binary itself during the release build and copied
into all four archives, so they are byte-identical across platforms and cannot describe a CLI that
no longer exists — which a checked-in copy eventually would.

`swf completions <shell>` prints the same text on demand. Prefer it whenever the binary did not
come from an archive, or has moved on from the one it did:

```sh
swf completions zsh  > ~/.zfunc/_swf     # zsh wants the file named _swf, on $fpath
swf completions bash > ~/.local/share/bash-completion/completions/swf
swf completions fish > ~/.config/fish/completions/swf.fish
```

### 4. Or build it from source

The release legs build exactly this, adding only `--target` and `--locked`:

```sh
cargo build --release --manifest-path rust/Cargo.toml            # the workspace
cargo build --release --manifest-path rust/Cargo.toml -p swf-cli # the binary alone
install -m 0755 rust/target/release/swf ~/.local/bin/swf
```

The MSRV is Rust `1.82`. Add `--locked` to resolve the dependency versions CI tested rather than
whatever is newest today. A source build needs no checksum step for the obvious reason: you already
have the tree it was built from.

Nothing is published to a package manager: no Homebrew tap, no `cargo install`, no distro
package. The release tarballs are the artifacts, as they are for the Python wheel.

## Connect a factory — `swf context`

Start `swfactory backend` on the control-plane host first and set `SWF_BACKEND_TOKEN` on the
operator machine. The default endpoint is `http://localhost:8082`. The `--token-env`, `--user`
and `--password-env` options below apply only to explicit `--direct` compatibility mode.
Without a backend, use `swf context add ... --direct --force` to retain a direct connection.

A context is one factory environment: where Airflow is, which repository deliveries land in, who
owns the sandboxes, which DAGs to read, and **the name of the variable** the credential lives in.

```sh
swf context add prod --airflow-url https://airflow.example.com/airflow/ \
                     --repo acme/widgets --owner me@example.com \
                     --backend-url https://factory.example.com --use
swf context list                                  # every environment, the active one marked
swf context show                                  # the active one, every credential redacted
swf --context staging jobs list                   # one command against another environment
```

`$SWF_CONFIG`, when set, is the **path to the config file itself**. Otherwise the file is
`$XDG_CONFIG_HOME/swf/config.toml`, or the platform config directory (for example
`~/Library/Application Support/swf/config.toml` on macOS). It is written atomically at mode
`0600`. Precedence for the active context is `--context NAME` > `$SWF_CONTEXT` > the file's
`default` key > the only context if there is exactly one > a built-in `local` pointing at
`http://localhost:8080`. That fallback is never written to disk. `swf context show` identifies it as the built-in
fallback; `swf doctor` reports the connectivity/authentication checks it can actually perform for
that context (and, in normal backend mode, the backend's own required checks).

`airflow_url` is a **full base URL, path prefix included** — `/api/v2` and `/auth/token` are
appended to it — because `[api] base_url` can put Airflow behind a prefix.

## Commands

| Command | Purpose |
| --- | --- |
| `swf context list \| use \| add \| show \| remove` | the environments this machine knows about |
| `swf doctor` | is this machine able to drive a factory: one line per check, a `fix:` per failure |
| `swf submit --issue <ref>… [--blueprint N] [--target R]… [--wait]` | send governed work to Airflow |
| `swf attention` | approvals waiting, failures, blocked deliveries, orphan sandboxes |
| `swf runs list \| inspect <run> \| stop <run> \| unpause <dag>` | DAG runs |
| `swf jobs list [--attention] \| inspect <job>` | mapped jobs — identity, progress, gates |
| `swf logs <job> [--task T] [--attempt N] [--follow]` | one task attempt's log |
| `swf gates list \| review <gate> \| approve <gate> \| reject <gate>` | one identified approval gate |
| `swf deliveries list \| verify <delivery> [--all] [--clone]` | what was published, and whether it is true |
| `swf sandboxes list \| inspect <name> \| rm <name>` | ownership, status, lifecycle |
| `swf metrics [--root P]` | the committed metrics history |
| `swf snapshot` | one pass over every source, in `swfactory herd --once --json`'s document |
| `swf stack up [--build] \| down [--volumes] \| status` | the local Docker development stack |
| `swf tui` | the interactive factory |
| `swf completions <shell>` · `swf version` | packaging |

Global flags apply everywhere: `--context <name>`, `--json`, `--no-color`, `--timeout <s>`,
`-v/--verbose` (repeatable), `-y/--yes`.

`--yes` is required for the six mutations that answer or destroy — `gates approve`, `gates reject`,
`runs stop`, `sandboxes rm`, `context remove`, `stack down` — whenever stdin is not a terminal or
`--json` is set. `submit` and `runs unpause` deliberately do **not** confirm: they create and enable
rather than answer or destroy, and a script that submits work should not need a flag to say it meant
it.

### The unit is the job

A complete identity is `(context, dag_id, run_id, map_index)`, rendered `dag/run#index`; a gate adds
the task, rendered `dag/run#index:gate`. Parsing splits on the **last** `#`, because an Airflow run
id contains `:` and `+`. A bare `dag/run` means `#-1` (the unmapped tasks of the run), and a bare
gate name expands to `job.approve_<name>`, so `factory/manual__2026-09-06T08:04:02+00:00#1:plan` and
`…#1:job.approve_plan` are the same gate. An issue number is never an identity: one issue across two
targets is two jobs.

## Exit codes

The exit-code table is public surface under the project's semver policy
([design.md](design.md#versioning-and-release)); so are the command names and the `--json` document
keys. A script that sees `4` must know it needs a credential and not a retry.

| Code | Meaning |
| --- | --- |
| 0 | success |
| 1 | operational failure — a check is red, a gate answer was refused by policy, verification failed |
| 2 | usage error — a bad flag, an unparseable id, a mutation without `--yes` on a non-TTY |
| 3 | not found — no such context, run, job, gate, delivery or sandbox |
| 4 | authentication or authorisation failure |
| 5 | service unreachable — DNS, connect, TLS, timeout, or a missing `gh`/`islo` |
| 6 | conflict — the gate was already answered, or the evidence moved under the operator |

Three commands exit on a *report* rather than an error. `swf doctor` prints the checks and exits 1
when a required row is red — the rows are the answer, so there is no error envelope.
`swf deliveries verify` exits 1 unless every report it produced is verified. `swf stack` exits 1
whenever the stack is not answering — `up` re-reads it afterwards rather than trusting
`docker compose`'s exit code. And three commands always exit 0 by design: `attention`,
`jobs list` and `snapshot` are reads, and a source being down is reported *inside* the answer rather
than as the answer's failure — a monitoring step must not confuse "nothing needs a human" with "we
could not tell".

## The JSON contract

`--json` prints **exactly one** document to stdout and every diagnostic to stderr, so
`swf … --json | jq` is safe in a pipeline even when the command failed. Listings are bare arrays
(`context list`, `runs list`, `jobs list`, `gates list`, `deliveries list`, `doctor`,
`deliveries verify --all`); truncation and per-source failures never change a document's type.

A non-zero exit still leaves a parseable answer:

```json
{"error": {"kind": "conflict", "message": "…", "exit_code": 6, "hint": "…"}}
```

`kind` is a 1:1 map onto the table above — `operational`, `usage`, `not_found`, `auth`,
`unreachable`, `conflict` — and `exit_code` is duplicated inside the object on purpose, so a
consumer reading a captured document never has to inspect `$?`. `message` is one sentence, already
sanitised.

A degraded read says which source it lost, in the document rather than beside it:

```console
$ swf attention --json
{
  "blocked": [], "failures": [], "gates": [], "orphans": [],
  "errors": [{"source": "airflow", "message": "http://localhost:8080 … is unreachable"}]
}
```

`swf snapshot --json` is the equivalence surface for `swfactory herd --once --json`: the same
`collected_at` / `runs` / `gates` / `prs` / `sandboxes` / `metrics` / `errors` keys, in the same
order, with Python's `isoformat()` timestamps (`+00:00`, never `Z`). It is printed as raw bytes
rather than re-serialised so key order and escaping survive a byte diff, and
`scripts/snapshot_diff.py` compares the two documents from one live server.

## What `runs stop` really does

It **marks the Airflow run failed**. That is all Airflow offers, and the name has to say it: tasks
already running keep running, the agent keeps working, and any sandbox the run created stays up.
Its `--json` document says the same thing in three fields rather than one, because a document that
only said `"stopped": true` would be read as "the agent has been halted and the sandbox is gone":

```json
{"action": "marked_failed", "run_marked_failed": true,
 "processes_stopped": false, "sandboxes_removed": false, "note": "…"}
```

Remove the sandbox separately with `swf sandboxes rm`. Closing the TUI stops nothing at all.

## Gates: readiness, and re-validation before every write

A HITL detail exists from the moment the operator creates it, which is just **before** the task
defers. Answering inside that window makes the scheduler see a stale executor event and *fail* the
gate — a race a human clicking `a` cannot hit and a polling script hits about once per dozen gates
(`scripts/stress_airflow.sh` paid for this knowledge). So a gate carries `ready`, true only once its
task instance has been parked in `awaiting_input` on two consecutive polls, and `swf gates approve`
refuses an unready gate unless `--force` is passed — and says which condition failed.

`swf gates review <gate>` prints the evidence and a revision. Handing that revision back as
`--expect <rev>` makes the approval conditional on the evidence not having moved: a mismatch is exit
6, a conflict, not a warning. The approval itself re-reads the gate immediately before the `PATCH`
and refuses one that is already answered. Another operator getting there first is a normal outcome,
not a crash.

Airflow records the responder from the API credential (`responded_by_user`), and the run commits
that identity into `approvals.json`. **The actor is the Airflow user the token belongs to** — never
a name typed into `swf`. Use a personal token; a shared one makes every approval look like the same
person.

## Operating at scale

Answering one gate is `swf gates review` then `swf gates approve`, and there is nothing to it. The
day that hurts is the one with four blueprints, sixty runs and a hundred gates on the board, where
the question stops being *how do I answer this gate* and becomes *which of these are mine, and can
I answer that whole set at once without answering something I never read*. Four things make that
answerable: filters that narrow a listing to the set you actually mean, a dry run that touches
nothing, a bulk answer that refuses everything not ready, and a listing that says out loud when it
has been shortened.

### Narrow the listing before you read it

The three listings that grow with the factory take filters, and the filters compose: each one
narrows, none of them widens, and an unmatched filter yields an empty listing rather than
everything.

| Listing | Narrows by |
| --- | --- |
| `swf gates list` | `--dag`, `--blueprint`, `--issue`, `--gate`, `--ready`, `--limit` |
| `swf jobs list` | `--dag`, `--state`, `--issue`, `--attention`, `--limit` |
| `swf runs list` | `--dag`, `--state`, `--limit` |

For example:

```sh
swf gates list --dag factory --issue 42 --ready --limit 50
swf jobs list --dag factory --state failed --issue 42 --attention --limit 50
swf runs list --dag factory --state running --limit 50
```

Prefer a filter to a `jq` select for the set you are about to *answer*: the filters are the same
selection `gates approve --all` applies, so a listing you narrowed with them is literally the batch
you are about to run, while a `jq` pipeline is a second implementation that can disagree with it.
`jq` is the right tool for shaping the output you keep.

### Answer a batch, dry run first

`--dry-run` applies to bulk `gates approve --all` and `gates reject --all`. It runs the same
selection logic as the real batch, reports which ready gates would be answered (and which are
skipped), and performs no mutation. For example, on a non-interactive shell:

```sh
swf gates approve --all --dag factory --issue 42 --ready --yes --dry-run
swf gates approve --all --dag factory --issue 42 --ready --yes
```


The rule to keep is mechanical rather than a matter of judgement: **run the line with `--dry-run`,
read what it selected, then re-run the identical line with `--dry-run` removed.** Editing a filter
between the two runs is precisely the mistake the dry run exists to catch, so change nothing else —
not the limit, not the DAG, not the shell history entry. A batch is also a mutation, so on a
non-TTY or with `--json` it needs `--yes` like every other answer.

### Bulk answering only touches ready gates

The readiness rule above is not relaxed for a batch; it matters more there. A person pressing `a`
cannot realistically land inside the window between a HITL detail being created and its task
deferring, but a loop over a hundred gates arrives faster than the scheduler does and hits that
window repeatedly — and a gate answered inside it is *failed*, not queued. So a bulk answer selects
only gates whose `ready` is true, reports the `⋯ arming` ones as skipped, and has no batch
equivalent of `--force`: forcing is a decision about one gate you have read, and it does not
generalise to a set you have not. A skipped gate is not lost — it arms within seconds and the next
pass takes it.

Nothing else about a single answer is dropped either. Each gate is re-read immediately before its
own `PATCH`, so a gate another operator answered between the dry run and the batch is a per-gate
conflict inside the report rather than a batch that aborts halfway through with no record of what
it already did. `--expect` stays a single-gate flag for the same reason readiness does not
generalise: a revision names one piece of evidence.

### Telling a shortened listing from a short one

Collection reads are paginated to exhaustion but bounded, and Airflow's cursor mode returns no
total, so the only honest terminator is an empty page — which means a listing that hit its page cap
cannot be recognised by counting rows. It is reported instead, and never by changing the shape of
the answer: a `--json` listing stays a bare array and the warning goes to stderr
(`the gate list was truncated; some gates are not shown`), so `swf gates list --json | jq` is
unaffected and a person watching the terminal is told. In the TUI the same fact is the `⋯ truncated`
badge in the header and, on Infrastructure, a `read stopped at its page bound — some rows are
hidden` detail on the row of the source that hit it.

Treat that warning as a stop sign in front of a batch. A bulk answer over a truncated listing has
answered *a* set, not *the* set, and the remainder is invisible rather than merely unanswered.
Narrow with a filter until the warning is gone, then dry run.

## Deliveries: three verdicts, never one tick

Three different claims get called "it worked", and they have different forgers, so
`swf deliveries verify` keeps them apart (non-negotiable 10):

| Verdict field | Level | Who could forge it |
| --- | --- | --- |
| `workflow_succeeded` | reported | the run itself, in files the run committed |
| `branch_published` | published | the orchestrator credential — a ref, a PR, labels, a title |
| `independently_verified` | verified | nobody: fresh clone, recomputed hashes, re-run tests |

Nothing at `reported` can ever satisfy a `verified` check. `--clone` is what makes the third column
reachable at all; without it `swf` reports honestly that it did not try. The overall `verdict` is
one of `verified`, `verified_blocked` (a `[BLOCKED]`/`[REJECTED]` delivery is a *correct* outcome
consistently recorded), `published_only`, `reported_only`, `refuted` (one contradiction is enough)
or `inconclusive`.

## The interactive factory — `swf tui`

`swf tui` is the same operations layer with a screen on it. It refuses `--json` and a non-TTY stdout
(exit 2). Rendering never awaits and never touches the network: every effect is an async task that
sends a message back over a bounded channel, so the screen cannot block on a dead service. The
terminal is restored on normal exit, on error, on panic and on SIGINT/SIGTERM.

Every frame on this page is pasted verbatim out of `swf-tui`'s snapshot tests
(`rust/crates/swf-tui/tests/snapshots/`), so a screen that changes shape fails a test before it
reaches the documentation. This is Jobs, rendered at 120 columns by 40 rows in the monochrome
theme; the fence is 120 columns wide and is meant to scroll sideways rather than be wrapped to fit.

```text
swf prod  ·  airflow https://airflow.example.com  ·  repo acme/widgets  ·  owner me@example.com
actor admin  ·  refreshed 09:30:00 (0s ago)
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 1 attention     5 │ jobs 4 rows                                                  │ factory/manual__2026-03-14T09#0
 2 jobs          4 │dag      run              job issue  stage    state           │issue        142
 3 job detail      │factory  manual__2026-03- 0   142    approve_ ◆ awaiting_input│state        ◆ awaiting_input
 4 review          │factory  manual__2026-03- 1   143    build_an ▸ running       │stage        approve_plan
 5 deliveries    2 │hotfix   manual__2026-03- 0   sre-9  build_an ✗ failed        │
 6 infrastructure 7│hotfix   manual__2026-03- 1   sre-10 deliver  ✓ success       │tasks
 7 history         │                                                              │job.spec            ✓ success
                   │                                                              │job.approve_plan    ◆ awaiting_input
                   │                                                              │
                   │                                                              │gates
                   │                                                              │approve_plan        ◆ ready
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
 activity ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
09:30:00 watching prod at https://airflow.example.com







enter detail  t trigger  s stop  o open  L logs  r refresh  / search  : cmd  ? help  q quit
```

The left column is the view list, each view carrying the number of rows behind it — `attention 5`
is legible from any other screen. The middle is the table that `/` filters; the right is the
selected job: its identity, the tasks it has finished and the gates it is waiting on. Under them is
this session's activity log, and the last line is the key map **for the current view**, not for the
whole application.

Two details on that screen are easy to read past.

**Freshness is per source.** `refreshed 09:30:00 (0s ago)` is the last refresh *pass*, not a
promise about the data behind it. Each source — `airflow`, `gates`, `github`, `islo`, `metrics` —
carries its own `fetched_at`, and the header grows a badge the moment one of them falls behind the
others: `◔ stale` as soon as **any single** source is older than three refresh intervals,
`⋯ truncated` when a collection read stopped at its page bound, and one reverse-video `✗ github`
per failing source, sorted so the header does not reshuffle between refreshes. So the quiet header
above is a claim about every source at once, and the per-source stamps themselves are rows on
Infrastructure (`source  github  ✓ fresh  -  0s`). It is worth learning because the failure it
reports is otherwise silent: a `gh` that cannot authenticate leaves the Deliveries pane looking
merely empty.

**`◆ ready` is not `⋯ arming`.** In the gates block on the right, `approve_plan  ◆ ready` means
that gate's task instance has been seen parked in `awaiting_input` on two consecutive polls, so an
answer will land. A gate that exists but has not parked yet renders `⋯ arming`; pressing `a` on it
notes `… is not answerable yet: its task is not parked` in the activity pane, and because the TUI
never sends `force` the write comes back `refused` rather than going through. The wait is seconds
and the race it avoids fails the gate outright — see
[Gates](#gates-readiness-and-re-validation-before-every-write) for why.

Review is the approval itself: the same gate with the evidence a person is being asked to sign for.

```text
swf prod  ·  airflow https://airflow.example.com  ·  repo acme/widgets  ·  owner me@example.com
actor admin  ·  refreshed 09:30:00 (0s ago)
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 1 attention     5 │ review                                                       │ factory/manual__2026-03-14T09#0:job.
 2 jobs          4 │gate         factory/manual__2026-03-14T09#0:job.approve_plan │approve_plan
 3 job detail      │job          factory/manual__2026-03-14T09#0                  │issue        142
 4 review          │stage        approve_plan                                     │state        ◆ awaiting_input
 5 deliveries    2 │job state    ◆ awaiting_input                                 │stage        approve_plan
 6 infrastructure 7│task state   ◆ awaiting_input  ◆ answerable                   │
 7 history         │revision     2916c986f955d863                                 │tasks
                   │actor        admin                                            │job.spec            ✓ success
                   │options      approve, reject                                  │job.approve_plan    ◆ awaiting_input
                   │url                                                           │
                   │https://airflow.example.com/dags/factory/runs/manual__2026-03-│gates
                   │14T09                                                         │approve_plan        ◆ ready
                   │                                                              │
                   │evidence                                                      │
                   │plan for issue 142: split the ingest worker                   │
                   │three files change; the migration is reversible.              │
                   │                                                              │
                   │a approve  ·  x reject  ·  both re-read the gate before       │
                   │writing                                                       │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
                   │                                                              │
 activity ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
09:30:00 watching prod at https://airflow.example.com







a approve  x reject  esc back  r refresh  / search  : cmd  ? help  q quit
```

`task state   ◆ awaiting_input  ◆ answerable` is that readiness fact again, spelled for someone
about to press a key — the left half is Airflow's state, the right half is whether `swf` will send
the answer at all. `revision 2916c986f955d863` is what goes back as `expect`, which makes the
approval conditional on *this* evidence rather than merely on this gate, and the footer narrows to
`a approve  x reject  esc back` because those are the only writes this screen has.

Seven views: **Attention** (what needs a human), **Jobs**, **Job detail**, **Review**,
**Deliveries**, **Infrastructure** (sandboxes and per-source health), **History** (the committed
metrics). The detail pane folds away below 100 columns and the navigation below 72. Every state
renders as a glyph **and** a word (`◆ awaiting_input`), so the screen is legible without colour;
a monochrome render is byte-identical to the coloured one apart from the styling.

| Key | Does |
| --- | --- |
| `r` / `F5` | refresh every source now |
| `Tab` / `S-Tab`, `1`–`7` | switch view |
| `j` `k` / arrows, `g` `G`, `ctrl-f` `ctrl-b` | move the cursor |
| `enter` / `esc` | open the selected job or gate / back, or clear the filter |
| `a` | approve the selected gate |
| `x` | reject the gate — remove the sandbox on Infrastructure |
| `t` | trigger a blueprint with issue refs |
| `s` | mark the selected Airflow run failed |
| `o` | open the run or delivery in a browser |
| `v` | verify the selected delivery |
| `L` / `l` | fetch the selected task's log / show or hide the activity pane |
| `/` · `:` · `?` | filter the table · command palette · this key map |
| `q` / `ctrl-c` | quit — remote work keeps running |

One deliberate departure from `herd`'s muscle memory: `r` is refresh on **every** view. In `herd`,
`r` rejects on the Gates tab and `F5` is the only refresh there, which means the same key destroys
an approval on one screen and is harmless on the others. Reject is `x`.

Selection is keyed by `JobId`/`GateId`, not by row index, so a refresh that reorders the table never
moves the cursor under the operator. Each source carries its own `fetched_at` and error: one dead
service marks its own pane stale (older than 3× the refresh interval) and leaves every other pane
working. Changing the selected run cancels the in-flight requests for the old one.

Approving from the TUI shows the exact job, the gate, the authenticated actor and the evidence
revision that is on screen, and sends that revision as `expect` — never `force`. A refusal renders
as `refused` and does not trigger a refresh; a failure renders as `failed`.

## How this relates to Airflow, and to `herd`

**Airflow schedules; `swf` operates.** Airflow owns the run history, mapped jobs, retries, timeouts,
the HITL gates and the responder identity. `swf` reads Airflow's public API and writes exactly four
things through it: a triggered DAG run, a gate response, a run marked failed, and a DAG unpaused. It
never touches the metadata database — from any crate, ever, and it persists nothing between
invocations: there is no local copy of the factory's state that a decision could be made from, and
every write re-reads its subject from the API immediately beforehand.

`herd` ([herd.md](herd.md)) remains the Python control room and the compatibility target: it is what
`swf snapshot --json` is diffed against in CI. For an operator, `swf` is canonical — it is the one
that installs without Python and the one the exit-code table is promised for. For the repository,
`herd` is still the reference implementation of the snapshot document, and `scripts/swf_e2e.sh` is
the acceptance test that the two agree against one live server.

## Security posture

- **No secrets in the config file.** A context stores the *name* of an environment variable
  (`token_env`, `password_env`) and never a value. The `Auth` type has no `Token(String)` arm, so no
  future edit can add one by accident. A literal `password`, `token` or `secret` key anywhere in the
  file is a **load error**, not a warning: `swf` refuses to start rather than teach the habit.
  `swf context show` redacts to the variable name and has no unredacted counterpart.
- **No metadata-database access, from any crate.** Airflow's public REST API only.
- **Untrusted text is sanitised before it reaches a terminal.** Log lines, gate bodies, PR titles
  and issue text are written by agents, by CI and by strangers. A terminal obeys `ESC [ 2 J` no
  matter who wrote it, so rendering such a string would escalate from *whoever produced a log
  line* to *whoever is watching the factory*. C0 controls and DEL are dropped (`\t` kept),
  CSI/OSC and the other escape families are swallowed whole, and every line is bounded.
- **The islo ownership guard.** Listing is `islo ls --output json`, **never** `--all` — visible is
  one refactor away from deletable. Removal refuses unless all three hold: an owner is configured;
  the name matches the factory pattern `swf-<slug>-<run8>`; and a **fresh** listing taken right
  before the `islo rm` still reports `created_by == owner` (case-insensitive). All three are exit 1,
  an operational refusal. Without an owner every removal is refused outright.
- **Argv is always a `Vec<String>`.** `gh`, `islo`, `git` and `docker compose` are never invoked
  through a shell string, so a branch name or a repository path cannot become two arguments.
  Subprocesses are bounded at 120 s; `--timeout` bounds HTTP only, because `gh` on a slow API is not
  a hang.
- **A branch name that fails git's rules is a finding, not a refusal.** It is reported on the
  delivery report as `branch_name_invalid` with the offending name, and everything else is still
  verified. Refusing to look at a branch the factory happily created would make the operator's tool
  less useful than the bug it is complaining about.

## Honest limits — what `swf` does not do

This is the operator interface, phases B through E of a migration. **The execution engine is still
Python on Airflow, and there is no plan to move it.** Concretely:

- `swf` runs no stage. `swfactory run`, `demo`, `evals`, `webhook` and `maintain` have no `swf`
  equivalent and are not going to get one — stage semantics live in `stages.py`
  ([design.md](design.md#design-decisions)).
- `swf` reads blueprints; it does not compile or rewrite them, and it needs a factory checkout (or
  `SWF_BLUEPRINTS_DIR`) to read them at all. `swf doctor` warns rather than fails when it has none.
- There is no `--approve-all`. Answering every pending gate unattended is a CI backstop, and it
  stays in `swfactory herd --approve-all`.
- `runs inspect` and `jobs inspect` find a run inside the newest 200 runs of its DAG and otherwise
  exit 3. Airflow's API has no "get one run by id" route this client uses yet.
- `logs --follow` polls; Airflow has no streaming log endpoint. `--task`, when it is not given,
  picks the failure, else what is running, else the last task.
- `swf stack` drives `deploy/docker/compose.yml` and nothing else. `deploy/islo/*` is a production
  deployment with its own script, its own credentials and its own blast radius
  ([islo.md](islo.md), [docker.md](docker.md)).
- Collection reads are paginated to exhaustion, but bounded: a read that hits its page cap says
  `truncated` rather than silently hiding jobs. `total_entries` is `null` in Airflow's cursor mode,
  so the real terminator is an empty page.
- The macOS binaries are unsigned: checksummed, never codesigned or notarized, so Gatekeeper
  quarantines a fresh download and the operator clears it by hand (see [Install](#install)).
