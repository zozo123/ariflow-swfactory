# Claude gateways, including Databricks

The Claude agent can use an operator-selected HTTPS gateway with the `srt`, `docker`
(`credentials=env`), and `toolset` sandboxes. The factory passes an exact list of model
settings only to agent execution. Target tests and lifecycle commands receive none of them.

For Databricks, use your workspace's Anthropic-compatible endpoint and a model that you
can query. See the official [Claude Code setup](https://docs.databricks.com/aws/en/ai-gateway/coding-agent-claude)
and [Messages API](https://docs.databricks.com/aws/en/machine-learning/model-serving/query-anthropic-messages)
guides. Unity Gateway uses `/ai-gateway/anthropic` and Unity Catalog model names; existing
serving endpoints use `/serving-endpoints/anthropic` and endpoint names. Select the matching pair.

For example, on the trusted worker:

```sh
export ANTHROPIC_BASE_URL=https://YOUR-WORKSPACE.cloud.databricks.com/serving-endpoints/anthropic
export ANTHROPIC_MODEL=databricks-claude-sonnet-4-6
export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
# Supply ANTHROPIC_AUTH_TOKEN through your secret manager or an unlogged shell prompt.
# Never put the token in a blueprint, command argument, source file, or issue.
```

`ANTHROPIC_AUTH_TOKEN` requires an explicit HTTPS base URL on port 443. URLs with embedded
credentials, query parameters or fragments are refused before provisioning. A bearer gateway
does not also receive an unrelated `ANTHROPIC_API_KEY`. The factory never forwards
`DATABRICKS_TOKEN`, `ISLO_API_KEY`, `GH_TOKEN` or `SWF_BACKEND_TOKEN` into this model process.
The gateway hostname is added to the srt/toolset egress allowlist. Docker retains its configured
network policy. Per-stage blueprint models override `ANTHROPIC_MODEL`.

## Run a real-model local rehearsal

With Docker running and the variables above set:

```sh
docker build -t swfactory-sandbox:local -f deploy/docker/sandbox.Dockerfile .
uv sync --locked
uv run swfactory demo --agent claude --sandbox docker --scm local
```

This uses real paid model calls with the default blueprint's turn, retry and spend limits.
The demo's declared replay fixture answers its rehearsal gates. It publishes only to a local
bare Git remote; it does not open or merge a GitHub PR. Inspect
`.factory/<run_id>/report.json`, `pr.md`, stage evidence, and `cleanup.json`.
A successful provider request alone does not establish a successful delivery.

For a managed deployment, keep these variables on the worker that launches the model process,
submit through the backend, and answer the declared human gates. The backend retains publication
credentials. See [the operator flow](factory-backend.md#from-work-order-to-finish-line).

Islo uses its own gateway-injected credentials and never receives these host variables.
Configure the provider's gateway and environment separately; see [hosted deployment](islo.md).
External-agent profiles have a separate explicit credential contract and are unchanged.

