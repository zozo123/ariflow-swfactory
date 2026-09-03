# deploy/docker — fully local factory (Docker, for TESTING)

Everything on one machine, no islo account, no cloud: Airflow 3 and the webhook receiver run in
containers; every agent/test/git command of a run executes in a **sibling sandbox container**
(`SWF_SANDBOX=docker`, `DockerSandbox` in `src/swfactory/sandbox.py`) over the run's workdir.

```
host  ─┬─ docker compose (deploy/docker/compose.yml)
       │    airflow   : airflow standalone, UI+API :8080, DAGs from <repo>/dags     (start.sh)
       │    webhook   : swfactory webhook serve :8081 --airflow-url http://airflow:8080 (webhook.sh)
       │        both: repo bind-mounted at its HOST path (${PWD}:${PWD}), working_dir ${PWD}
       │        airflow also: /var/run/docker.sock  (root-equivalent on the host, see limits)
       └─ per command of a run: docker run --rm --init -v <workdir>:<workdir> ... swfactory-sandbox:local bash -lc <cmd>
              <workdir> = <repo>/.factory/<run_id>/work  — same absolute path on host, in airflow, in the sandbox
```

## Run it

```bash
# 0. from the REPO ROOT (compose mounts ${PWD} at ${PWD}; the paths must match the daemon's)
docker build -t swfactory-sandbox:local -f deploy/docker/sandbox.Dockerfile .
#    Linux: add --build-arg UID=$(id -u) --build-arg GID=$(id -g) so the agent's files are yours
#    (or: UID=$(id -u) GID=$(id -g) docker compose -f deploy/docker/compose.yml build sandbox-image)

# 1. Airflow + webhook (first start: uv sync --group airflow into a volume, then db migrate)
docker compose -f deploy/docker/compose.yml up            # add -d to detach
#    real agent instead of the scripted replay:
#    SWF_AGENT=claude ANTHROPIC_API_KEY=sk-ant-... docker compose -f deploy/docker/compose.yml up
#    deliver to GitHub instead of the local bare remote: SWF_SCM=github GH_TOKEN=ghp_... (gh is in the image)

# 2. UI: http://localhost:8080 — user admin, password from the container log
#    ("Simple auth manager | Password for user 'admin': ...") or
docker compose -f deploy/docker/compose.yml exec airflow \
  cat /opt/airflow_home/simple_auth_manager_passwords.json.generated

# 3. trigger the factory DAG (dag_id = blueprint name, e.g. `default` from blueprints/default.toml)
#    in the UI, or through the API / a GitHub issue event posted to http://localhost:8081/webhooks/github

# 4. approve the gates: Airflow UI (Required Actions on job.approve_intent / job.approve_plan) or
uv run swfactory approve <dag_run_id> intent --blueprint default --airflow-url http://localhost:8080 --token <JWT>
uv run swfactory approve <dag_run_id> plan   --blueprint default --airflow-url http://localhost:8080 --token <JWT>
#    (JWT: POST http://localhost:8080/auth/token with {"username":"admin","password":"<pw>"})

# One-shot, no Airflow: the CLI on the host with sandbox containers (docker CLI + daemon needed)
uv run swfactory demo --sandbox docker                                  # scripted replay
uv run swfactory run --issue demo/issue.md --agent claude --sandbox docker --scm local --approve prompt
SWF_DOCKER_IMAGE=swfactory-sandbox:local uv run swfactory demo --sandbox docker   # local image instead of ghcr
```

`.factory/<run_id>/` (workdir, `stages.jsonl`, `report.json`) lands in the repo checkout on the
host, exactly like `--sandbox local|srt`. Stop with `docker compose -f deploy/docker/compose.yml
down` (`-v` also drops the Airflow DB, the venv volume and the generated password).

## Knobs (`SWF_*` env or CLI flags; env wins over the blueprint)

| Config field | env | default | meaning |
|---|---|---|---|
| `sandbox` | `SWF_SANDBOX` / `--sandbox docker` | `local` | selects `DockerSandbox` |
| `docker_image` | `SWF_DOCKER_IMAGE` | `ghcr.io/zozo123/swfactory-sandbox:latest` | image of every sandbox container (compose sets `swfactory-sandbox:local`) |
| `docker_credentials` | `SWF_DOCKER_CREDENTIALS` | `env` | `env`: `ANTHROPIC_API_KEY` crosses (only with `--agent claude`); `host`: bind-mount `~/.claude` + `~/.claude.json` into the container `$HOME` (`/home/swf`) — **hands your Claude OAuth session to the agent container**; no key is passed. Linux only in practice: on macOS Claude Code keeps the OAuth token in the Keychain, not in `~/.claude`. |
| `docker_network` | `SWF_DOCKER_NETWORK` | `bridge` | `--network` of every sandbox container; `none` = no egress (fine for the scripted replay, breaks `uv sync`/the agent) |
| `docker_user` | `SWF_DOCKER_USER` | image user (`swf`, uid 1000) | `--user uid[:gid]` override, e.g. `root` when the bind-mounted workdir was created by root |

`--agent claude --sandbox docker` is accepted by `Config` without `--allow-local-agent`
(the container is the confinement). `--tests crabbox` stays local-only.

## What one command looks like

```
docker run --rm --init
  -v <workdir>:<workdir>                        # rw, same absolute path
  -v <workdir>/.github:<workdir>/.github:ro     # fixed read-only, like srt (.claude too when present)
  -v <workdir>/tests:<workdir>/tests:ro         # factory.toml `protected` literal prefixes that exist;
                                                #   per stage: tests/ is writable for build, ro for fix
  -v swfactory-sandbox-cache:/home/swf/.cache   # uv/npm caches survive the one-container-per-command model
  [-v ~/.claude:/home/swf/.claude -v ~/.claude.json:/home/swf/.claude.json]   # credentials=host only
  -w <cwd or workdir> --network bridge [--user X]
  -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0=*   # host-uid files
  [-e ANTHROPIC_API_KEY]                        # by NAME only, agent=claude + credentials=env
  <image> bash -lc '<cmd>'
```

The docker CLI itself runs with `scrub_env(os.environ)` (no `ANTHROPIC_*`, `GH_TOKEN`,
`GITHUB_TOKEN`, `AWS_*`, `ISLO_API*`) plus the explicit allowlist; `-e NAME` makes docker copy
the value from that environment, so a secret is never an argv token (`ps` on the host shows
none). `read`/`write`/`exists` are the orchestrator's own host file access; `git init` in
`ensure()` runs host-side, like `SrtSandbox`. Unit tests: `tests/test_sandbox_argv.py`
(`test_docker_*`).

## Honest limits — this is a testing deployment

- **The Docker socket is root-equivalent on the host.** The airflow container can start any
  container with any mount; whoever controls Airflow (UI password, webhook) controls your machine.
- **A container is not a MicroVM.** Sandbox containers share the host kernel; a kernel or runtime
  escape lands on your host. islo is the production trust boundary (`deploy/islo`).
- **No phantom tokens.** With `--agent claude` the real `ANTHROPIC_API_KEY` (or your OAuth
  session with `credentials=host`) is inside the container that runs model-written code. Egress is
  whatever `--network` allows; there is no domain allowlist (srt has one).
- **Not hardened**: no seccomp/AppArmor profile beyond Docker's defaults, no read-only rootfs, no
  resource limits, containers run as the image's uid 1000 (or `--user`).
- `.factory/` and the run workdirs are written by root (airflow container) and by uid 1000
  (sandbox containers): on Linux either build the sandbox image with your `UID`/`GID`, or set
  `SWF_DOCKER_USER=root`. Docker Desktop (macOS) maps bind-mount ownership, so it is usually a non-issue there.
- The workdir's `.venv` is created inside the container (Linux binaries) — do not `uv run` it on a
  macOS host; the host only needs git on it (`LocalGitScm` base repo).
- `credentials=host` mounts `~/.claude` read-write: the agent can read and alter your Claude
  settings, history and credentials. Prefer `env` with a scoped API key.

## Docker Sandboxes (microVM) — the alternative, and why it is not wired

Docker Desktop's *Docker Sandboxes* run an agent in a microVM with the workspace mounted:

```
docker sandbox run --workspace <dir> --credentials host claude -p "<prompt>"
```

That is a real VM boundary (better than a container) and it can forward your host Claude login
(`--credentials host`). It is not wired as a `Sandbox` because it runs **only the agent**: the
`Sandbox` protocol also has to run the target's tests, `uv sync`, `git add/commit/format-patch`
and the guard probe in the same confined place, and `docker sandbox run` has no "run this shell
command in the sandbox" mode with an exit code, no per-command `cwd`, and no read-only sub-mounts
for `protected` globs. Wiring it would mean the agent step in a microVM and every other command
unconfined on the host — a false sense of a boundary. If Docker Sandboxes grow an `exec`, a
`DockerSandbox` subclass could swap `argv()`; the rest of this module would not change. crabbox
already knows a `docker-sandbox` provider for the **test command** only (`--tests crabbox`,
local sandbox); see the README "crabbox" section.
