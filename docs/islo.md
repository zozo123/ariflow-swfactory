# Running the factory on islo (production)

Two tiers, both islo sandboxes, one trust boundary between them. Nothing that can push to GitHub
ever shares a VM with model-generated code, and nothing that can call Anthropic ever sees a GitHub
token.

| Tier | Runs | Credentials | Egress |
| --- | --- | --- | --- |
| **Orchestrator** — one sandbox, `swf-orchestrator` (trusted) | Airflow 3 (`airflow standalone`, UI `:8080`), `swfactory webhook serve --port 8081 --airflow-url http://localhost:8080`, and `deliver`: `git am` of the agent's format-patch stream, push `factory/*`, `gh pr create` | gateway-injected `GH_TOKEN`; environment-injected `ISLO_API_KEY` to spawn agent VMs; **no** Anthropic key | `swfactory-orchestrator` gateway: github.com, api.github.com, releases.islo.dev, the islo API |
| **Agents** — one MicroVM per (issue, target), `swf-<issue>-<run>` (untrusted) | clone of the target (`--source`), `claude -p` per stage, the target's tests, bot-authored commits | gateway-injected `ANTHROPIC_API_KEY`; never a GitHub token, never `--env` | `swfactory` gateway, deny-by-default: api.anthropic.com, github.com, api.github.com, pypi.org, files.pythonhosted.org, astral.sh |

The orchestrator spawns agent VMs with the same `IsloSandbox.argv` the CLI uses (`--gateway-profile
swfactory --environment swfactory --init minimal --delete-after --pause-after-idle --auto-resume
on_activity`), reads artifacts out with `islo cp`, and applies the patch on its own side — the
"agent never pushes" property is structural, not a prompt. `islo cp`
does not resume a paused VM, so file transfers retry once after `islo resume`.

## Bootstrap order

```sh
islo login && islo login --tool github && islo login --tool claude   # once per org (phantom tokens)
deploy/islo/bootstrap.sh    # one-time: agent gateway + environment, optional snapshot, doctor,
                            #   knowledge items. Idempotent: every step skips when its object exists
export GITHUB_WEBHOOK_SECRET=$(openssl rand -hex 32)  # generate once; store and reuse on redeploy
# First provision the swfactory-orchestrator gateway and ISLO_API_KEY environment described in
# deploy/islo/deploy.sh's prerequisite header.
deploy/islo/deploy.sh       # orchestrator sandbox from deploy/islo/orchestrator/{islo.yaml,start.sh}:
                            #   its own gateway + environment (ISLO_API_KEY), Airflow + the receiver,
                            #   the islo incoming webhook and the GitHub hook; prints the shared UI URL
uv run swfactory doctor     # preflight, --json for CI (evals-islo runs it first)
gh issue edit 42 --add-label factory        # GitHub -> islo incoming webhook -> :8081 -> DAG `factory`
uv run swfactory approve <dag_run_id> intent        # or answer both gates in the Airflow UI at the
uv run swfactory approve <dag_run_id> plan          #   shared URL (islo share swf-orchestrator 8080)
# -> PR on the target, labeled per blueprint (factory[:blocked|:rejected]); a human merges
```

`.github/CODEOWNERS` (`* @zozo123`) plus branch protection (`gh api -X PUT
repos/<owner/repo>/branches/main/protection`: 1 review, code owners, `test` status check) make the
human the required reviewer.

## Gateway, environment, snapshot

`bootstrap.sh` (flags verified against islo 0.48.1 and gh 2.83; `REPO` / `TARGET_DIR` / `PROFILE` /
`ENV` / `BRANCH` / `SNAPSHOT` override the defaults):

| Step | Command | Skipped when |
| --- | --- | --- |
| login | `islo login`; `islo login --tool github`; `islo login --tool claude` | `islo status` says authenticated / the integration is listed |
| gateway | `islo gateway create --name swfactory --default-action deny --internet-access true`, then `islo gateway swfactory add-rule --host <h> --action allow` for api.anthropic.com github.com api.github.com pypi.org files.pythonhosted.org astral.sh | the profile / rule host exists |
| environment | `islo environment create --name swfactory --gateway-secret 'ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY;host=api.anthropic.com;auth=bearer'` | the environment exists |
| snapshot (`SNAPSHOT=1`) | `islo use swf-golden --source github://<repo>:main ... -- bash -lc 'cd <target> && uv sync --group dev && claude --version'`; `islo snapshot save swf-golden --name swf-golden-<date>`; `islo rm swf-golden --force` | the snapshot exists |
| verify | `uv run swfactory doctor`, then `deploy/islo/knowledge.sh <repo>` | never |

`ANTHROPIC_API_KEY` is needed only for the first `islo environment create`. It is never echoed; the
only place it appears is that command's argv (the CLI has no stdin form for `--gateway-secret`), so
it is visible to `ps` on that host for an instant. Bootstrap needs no `GH_TOKEN` — `gh` uses its own
login; the bot PAT is an orchestrator runtime concern.

Warm start: bake a snapshot once and set it in the blueprint (`[sandbox] snapshot`) or
`SWF_ISLO_SNAPSHOT` (the repo variable of the same name feeds `evals-islo`). `islo.yaml`'s setup
script (uv only) does not re-run from a snapshot, which is the point.

```sh
islo use swf-golden --source github://zozo123/ariflow-swfactory:main --gateway-profile swfactory \
  --environment swfactory --init minimal --output plain -- \
  bash -lc 'cd /workspace/ariflow-swfactory/demo/target && uv sync --group dev && claude --version'
islo snapshot save swf-golden --name swf-golden-$(date +%Y%m%d) && islo rm swf-golden
export SWF_ISLO_SNAPSHOT=swf-golden-$(date +%Y%m%d)
```

## Webhook wiring

```
GitHub (issues, issue_comment) --HMAC--> islo incoming webhook (verifies X-Hub-Signature-256,
  idempotent on X-GitHub-Delivery) --> swf-orchestrator:8081 (swfactory webhook serve)
  --> POST /api/v2/dags/<blueprint>/dagRuns on the orchestrator's Airflow (:8080, islo share'd)
```

`deploy.sh` creates the incoming webhook by name (`islo webhook incoming create --deliver-to-port
8081 --path /webhooks/github --hmac-secret-value $GITHUB_WEBHOOK_SECRET`), reuses it on redeploy
(rotate with `islo webhook incoming update`), and points the repo hook at the receiver URL it
returns. `webhook.route` maps events to one DAG run:

| Event | Result |
| --- | --- |
| `issues.labeled` with `factory` | `POST /api/v2/dags/factory/dagRuns {"issues": ["<n>"]}` |
| `issues.labeled` with `factory:<name>` | the same against DAG `<name>` |
| `issue_comment.created` `@factory run [<name>]` on an issue | the same |
| `pull_request.*`, `factory:blocked` / `factory:rejected` (deliver's own PR labels), anything else | ignored |

The receiver takes Airflow credentials from `AIRFLOW_TOKEN` or `AIRFLOW_USER` + `AIRFLOW_PASSWORD`
and serves `GET /healthz` (what `deploy.sh` polls). `--secret-env SWF_WEBHOOK_SECRET` enables local
HMAC verification for the case where the receiver is exposed without islo in front; with the var
unset it trusts islo's upstream check. `swfactory webhook route <event> <payload.json>` is the dry
run. `dispatch.yml` (a GitHub Action posting to the Airflow API with the `AIRFLOW_URL` /
`AIRFLOW_TOKEN` secrets) stays as the alternative trigger when `:8080` is shared instead of `:8081`.

## Knowledge items

`deploy/islo/knowledge.sh [owner/repo]` (called by bootstrap, safe to rerun) publishes `CLAUDE.md`
and `REVIEW.md` as `rule` items and `.claude/skills/swfactory/SKILL.md` as a `skill`, tagged
`swfactory` and linked to the repo (`islo knowledge get` -> `update`, else `create`). The knowledge
layer supplies operating context. Separately, `install_guard` writes only the factory-owned hook
and restricted Claude settings below `.factory/`; it never overwrites the target's `.claude/` tree.

## From your shell and scheduled validation

`swfactory demo --real` (= `run --issue demo/issue.md --agent claude --sandbox islo --scm github
--approve prompt`) is the same path from a laptop; add `--record demo/scripted` to rewrite the demo
fixtures from real agent outputs. Agent VMs are created with `--auto-resume on_activity
--pause-after-idle 900 --delete-after <ttl>`.

The `evals-islo` job in `.github/workflows/evals.yml` is configured to exercise the production
pieces (`deploy/islo/*`, `swfactory doctor`, the receiver, and knowledge items). It reruns the demo
issue in an islo MicroVM weekly and whenever the institutional knowledge changes, with
`ISLO_API_KEY` and no Anthropic key on the runner, and inspects the same `report.json` the CLI
writes.

Sandbox safety: every `islo rm` is preceded by a plain `islo ls` (own scope, never `--all`), the
name must match the factory pattern `swf-<slug>-<run8>`, and `created_by` must equal
`SWF_SANDBOX_OWNER` when set. The nightly sweep refuses to run without an owner. Teammates'
sandboxes are never listed or touched.

Dev escape hatch, honestly: `--agent claude --sandbox local --allow-local-agent` runs the real agent
and its code unconfined on your machine in `.factory/<run>/work`; `LocalSandbox` scrubs
`ANTHROPIC_*`, so `claude` must be logged in on the host. Prefer `--sandbox srt`.
