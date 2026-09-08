# Self-hosting: the factory develops itself

Every other line targets `demo/target`, a toy calculator committed inside this repo. The
`selfhost` line targets the repository root, so a work cell edits `src/swfactory/` itself.

This is deliberately narrow. `docs/liquid-methodology.md` already governs it: candidate factories
"cannot inherit parent production credentials or install themselves because a score improved", and
the primitives are "foundations for governed experiments, not a claim of an autonomous production
self-improvement loop". Self-hosting here means exactly one thing — **the factory authors pull
requests against itself, and a human merges them**. Nothing promotes itself.

## What made it possible

Three files, and one of them was the whole blocker:

| File | Why |
| --- | --- |
| `factory.toml` (repo root) | The factory refuses a target without one. `stages.seed_local_workdir` raises "a local root target must run from the target checkout containing factory.toml"; `config.load_target_contract` raises "the factory refuses to guess commands". |
| `blueprints/selfhost.toml` | `dir = ""` is the repo root. `--target-dir ""` cannot substitute: `runtime.job_config` re-applies target identity from the job dict after CLI overrides, so a blueprint file is the only route — and the flag fails *silently*, building `demo/target` instead. |
| `demo/selfhost-scripted/` | `demo/scripted/build.1.patch` diffs `src/calc/*`, so it cannot apply at the repo root. A root-shaped fixture set is what lets the whole loop be proven with no model credential. |

## Both backends, one line

`sandbox` is an operational knob, not job identity (`config.IDENTITY_SETTINGS`), and env settings
outrank blueprint values. So the same blueprint runs on either backend and the target never moves:

```sh
# islo MicroVM (production; the blueprint default)
uv run swfactory run --blueprint selfhost --issue 2026 --agent claude

# local Docker (dev box) - same line, same target, different cell
uv run swfactory run --blueprint selfhost --issue 2026 --agent claude --sandbox docker
```

Their support status is not equal, and `config/capability-inventory.json` now says so: both
`sandbox.islo` and `sandbox.docker` are **experimental**, and `selfhost.factory` is experimental
with no end-to-end evidence. Before this change islo had no claim at all, even though it is the
default sandbox and the documented production deployment.

### Local Docker

```sh
docker build -t swfactory-sandbox:local -f deploy/docker/sandbox.Dockerfile .

SWF_DOCKER_IMAGE=swfactory-sandbox:local \
SWF_FIXTURES_DIR=demo/selfhost-scripted \
uv run swfactory run --blueprint selfhost --issue demo/selfhost-issue.md \
  --agent scripted --sandbox docker --scm local --approve auto
```

`SWF_DOCKER_IMAGE` is required: the built-in default is a `ghcr.io` image that is not published,
so without it the run fails with `registry: denied`. `--scm local` pushes the branch into a bare
repo under `.factory/<run_id>/remote.git` and writes `.factory/<run_id>/pr.md`, so nothing reaches
GitHub. `--agent scripted` needs no model key.

The operator's working tree is safe: the cell is bind-mounted only `.factory/<run_id>/work`, a copy
of the checkout with a fresh `git init`. The live tree and its history are never mounted into a
cell, and the Docker socket is mounted into the `airflow` service only, never into a sandbox. Start
a docker run from a dedicated clone anyway — the local backends seed from the host working
directory, and under Compose the orchestrator container is root over `${PWD}`.

### islo

`deploy/islo/deploy.sh` already defaults `SWF_TARGET_REPO` to `SWF_CONTROL_REPO`, so the same-repo
case self-hosting needs is the coded default rather than a special case. One repository gets one
webhook, and `webhook.route` drops PR labels and comments, so the factory's own delivery labels
cannot feed back into a new run.

Two things to know before the first islo self-hosted run:

- **The root `factory.toml` must already be merged to `main`.** The cell clones
  `github://<repo>:<base_branch>` and reads the contract out of that clone. A working-tree copy
  satisfies `doctor` but not the cell, so `doctor` goes green and the run then dies at setup.
- **Leave `[sandbox] snapshot` unset.** The golden snapshot bakes a clone of this very repo, so
  pinning it hands the agent a stale copy of the tree it is patching.

The contract's test command is deliberately Python-only. `cargo test` would fail in an islo cell:
the deny-by-default `swfactory` gateway profile allows `pypi.org` and `files.pythonhosted.org` but
no crates.io host, and CLAUDE.md keeps Rust out of a work cell in any case. Rust stays the
operator's client, covered by CI's own `rust` job.

## How a run is stopped from widening its own cage

Self-hosting removes the separation every other line relies on: the guard hook, the blueprints and
the sandbox implementation are now inside the agent's workspace. Three layers, and it matters which
covers what:

| Layer | Covers | Gap |
| --- | --- | --- |
| `agent.GUARD_PATH_DENY` + the PreToolUse hook | `factory.toml`, `.claude/**`, `.github/**`, `REVIEW.md`, `bands.yaml`, `.git/**`, `docs/factory/**`, `.factory/**` — hard-coded, so a cell cannot propose an edit to the list that binds it | `install_guard` runs only when `policy.writes`, so read-only stages get neither deny rules nor the hook — including `diagnose`, the one stage handed a shell |
| `factory.toml` `[paths] protected` | Everything governing future runs: `blueprints/`, `dags/`, `scripts/`, `pyproject.toml`, `uv.lock`, `CLAUDE.md`, the capability inventory, and the seven confinement modules | Kernel-enforced on srt and docker only. `IsloSandbox` has no `set_protected` and `stages` applies it behind a `hasattr` check, so on the production backend it is agent-enforced (deny rules) but not kernel-enforced |
| `.github/workflows/control-plane-gate.yml` | The same list, re-checked against the delivered diff outside the cage | Advisory until it is a required context on the protected branch |

The gate reads the protected list from the **base** revision, never from the pull request. Reading
the PR's own copy would let one diff shrink the list and edit the newly-unprotected file in the
same commit and pass green — the gate would police only what the diff permitted it to police. A
missing base `factory.toml` is a hard failure for the same reason.

Never put a `src/**` glob in `protected`. `sandbox._literal_prefix` reduces each entry to its
longest wildcard-free prefix, so `src/**/*.py` becomes `src`: the whole package would be mounted
`:ro` and no build stage could edit any code. `tests/test_selfhost.py` asserts every entry survives
that reduction unchanged.

## The residual, stated plainly

A self-hosted run can still edit the ~180 modules under `src/swfactory/` that are not in the
confinement set, and the next run imports them. That is the point of the line, not a defect, but it
means the protected list is a floor and not a proof. What actually contains it is the human merge:
the agent holds no GitHub credential, `deliver` force-updates only the bot-owned `factory/*`
branch, and promotion is a person.

Two more limits worth naming:

- **The blueprint's `auto = false` binds the managed path, not the direct CLI.** `--approve auto`
  self-approves both gates on `swfactory run`, as the proof run below did. Treat unattended
  self-hosting as a deliberate operator choice, not something the blueprint prevents.
- **`sandbox.docker` cleanup is unresolved.** Teardown is `--rm` only, `close()` is a no-op, and
  argv sets no `--name`/`--label`, so an interrupted run can leave one unidentifiable container.
  That is why the claim stays experimental.

## Proof of the loop

One self-hosted run, docker cell, keyless scripted agent, no GitHub writes:

```
run                    bcb183a6
issue                  SELFHOST-1
agent / sandbox / scm  scripted / docker:work / local
stages                 intent:ok -> spec:ok -> plan:ok -> build_and_test:ok -> review:ok -> deliver:ok
build_and_test         iterations=1, first_pass_ci=1, tests_passed=1, tests_failed=0, tests_count=692
deliver                blockers=0, commits=2, rejected=0, denied_tool_calls=0
```

`tests_count=692` is this repository's own suite running inside the work cell, not the calculator's
seven. The build commit changed exactly the one file the plan declared. `pr.md` stamps itself
`SCRIPTED REPLAY - not real work: do not merge`, which is what a keyless proof should say.

What is **not** proven: no self-hosted run has used a real model (`ANTHROPIC_API_KEY`), none has run
on islo (`ISLO_API_KEY`), and no self-authored pull request has been merged. It is also unsettled
whether `islo use --init minimal` executes the repo-root `islo.yaml` setup script that installs
`uv`; if it does not, the contract's test command must self-bootstrap `uv` in the cell. One `islo
use` settles it. Until those exist, `selfhost.factory` carries no end-to-end evidence and says so.
