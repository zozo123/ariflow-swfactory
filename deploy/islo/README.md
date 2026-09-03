# deploy/islo — the production deployment

Scripts only; the topology, the trust boundary and every step they take are documented in
[../../docs/islo.md](../../docs/islo.md).

```sh
export ANTHROPIC_API_KEY=<real key>     # only the first time (islo environment create)
deploy/islo/bootstrap.sh                # gateway + environment + doctor + knowledge; idempotent
                                        # REPO / TARGET_DIR / PROFILE / ENV / BRANCH override defaults
SNAPSHOT=1 deploy/islo/bootstrap.sh     # also bake swf-golden-<date> (warm start)

export GITHUB_WEBHOOK_SECRET=$(openssl rand -hex 32)   # generate once, reuse on every redeploy
deploy/islo/deploy.sh                   # orchestrator sandbox (Airflow + webhook receiver) from
                                        # orchestrator/{islo.yaml,start.sh}; prints the shared UI URL
deploy/islo/knowledge.sh [owner/repo]   # publish CLAUDE.md / REVIEW.md / SKILL.md as islo knowledge
uv run swfactory doctor [--json]        # read-only preflight; exit 1 with a `fix:` per failed row
```
