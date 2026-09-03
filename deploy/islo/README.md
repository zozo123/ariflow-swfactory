# deploy/islo — one-time islo bootstrap

`bootstrap.sh` sets up the islo side of the factory exactly as README "Real run (islo)" describes,
idempotently (every step is skipped when its object exists), then runs `swfactory doctor`.

```sh
export ANTHROPIC_API_KEY=<real key>     # only needed the first time (environment create)
deploy/islo/bootstrap.sh                # REPO / TARGET_DIR / PROFILE / ENV override the defaults
SNAPSHOT=1 deploy/islo/bootstrap.sh     # also bake swf-golden-<date> (README "Warm start")
```

| Step | Command (flags verified against islo 0.48.1) | Skipped when |
| --- | --- | --- |
| login | `islo login`; `islo login --tool github`; `islo login --tool claude` | `islo status` says authenticated / integration listed |
| gateway | `islo gateway create --name swfactory --default-action deny --internet-access true`, then `islo gateway swfactory add-rule --host <h> --action allow` for api.anthropic.com github.com api.github.com pypi.org files.pythonhosted.org astral.sh | profile / rule host exists |
| environment | `islo environment create --name swfactory --gateway-secret 'ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY;host=api.anthropic.com;auth=bearer'` | environment exists |
| snapshot (`SNAPSHOT=1`) | `islo use swf-golden --source github://<repo>:main --gateway-profile swfactory --environment swfactory --init minimal --output plain -- bash -lc 'cd /workspace/<repo>/<target_dir> && uv sync --group dev && claude --version'`; `islo snapshot save swf-golden --name swf-golden-<date>`; `islo rm swf-golden --force` | snapshot exists |
| verify | `uv run swfactory doctor [--blueprint <name>] [--json]` | never |

`swfactory doctor` (`src/swfactory/doctor.py`) is read-only: islo CLI + auth, github/claude
integrations, gateway profile, environment, snapshot (when the blueprint sets one), `gh auth
status`, `gh repo view <repo>`, `claude --version` (required only for `srt`/`local` sandboxes),
`srt`/`npx` (info), blueprint validity, `factory.toml` in the target dir. Exit 1 if a required check
fails; every failure prints the exact `fix:` command.

Secrets: the key is never echoed; it appears only in the argv of `islo environment create` (the CLI
offers no stdin form for `--gateway-secret`). No `GH_TOKEN` is needed for bootstrap — `gh` uses its
own login; the bot PAT is an orchestrator runtime concern (README).
