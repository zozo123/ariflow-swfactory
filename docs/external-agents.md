# External stage agents

`agent=external` connects an independently installed coding harness to the existing factory
stages. Airflow schedules the lifecycle; the harness handles one bounded `spec`, `plan`, `build`,
`fix`, or `review` invocation inside the selected sandbox. The factory still owns
approvals, fresh verification evidence, commits, publication, and promotion.

This is an integration seam. A profile identifies an operator-installed wrapper; it does not
certify that its underlying model runtime enforces policy or USD limits. No vendor agent package
is added to swfactory's dependencies. Install the wrapper and its runtime independently, and pin
their versions. For Docker execution, use a digest-pinned image containing the executable at the
profile's absolute path. This initial boundary supports Docker and explicitly enabled local
development execution; other sandbox adapters are refused.

## Operator profile

Set `SWF_AGENT=external` and `SWF_AGENT_PROFILE=/absolute/path/to/profile.json`, or pass
`--agent external --agent-profile /absolute/path/to/profile.json` to the Python CLI. Profiles
are operator configuration, never a shell command supplied in an issue or work order. The factory
binds the profile manifest, executable identity, selected model, and execution configuration to
the accepted inputs. Changing that binding requires a new admission rather than silently resuming
an earlier run.

The no-charge subprocess used by the tests has this manifest:

```json
{
  "schema_version": 1,
  "id": "external-fixture",
  "version": "1",
  "argv": ["/absolute/factory/tests/fixtures/external_agent/fixture.py"],
  "model": "fixture",
  "credential_env": [],
  "budget_mode": "no_charge",
  "enforces_usd_limit": false
}
```

`argv` is an argument vector with an absolute executable path. The factory appends
`--request <sandbox-request-file>` and runs it from the target checkout. Unknown manifest fields
are refused. Both the operator profile and executable must live outside the candidate checkout.
Model selection is explicit; a wrapper must not substitute a model silently.
`credential_env` declares supported model-provider key names passed only during agent execution.
Backend, GitHub, and other service credentials must remain outside the stage sandbox.

`no_charge` is an operator assertion for a fixture or other runtime that cannot incur provider
charges. Do not use it to make an unmeasured paid runtime appear free. Paid profiles use
`budget_mode="hard_cap"` and must declare `enforces_usd_limit=true`; the installed wrapper/runtime
must actually enforce the granted USD limit before making paid calls. That declaration needs
operator qualification. A prompt asking the agent to stay within a budget is insufficient.

## Request and response

The request is schema version 1 and includes `call_id`, `accepted_inputs_digest`, profile identity,
`stage`, `iteration`, explicit `model`, `prompt`, optional `output_schema`, a `budget` with `usd`,
`max_turns`, and `timeout_s`, and a `policy` describing allowed tools, denied tools, write access,
protected paths, and writable paths. These are inputs to the wrapper's own tool enforcement.
The factory does not translate another harness's tool names into universal sandbox controls.
Native blueprint tool-policy additions and model overrides that differ from the admitted profile
are refused rather than silently translated.

Docker mounts the checkout read-only and grants writes only to the contract's existing source
root, plus its test root for build calls. Fix calls keep tests read-only. Protection is
conservative: a protected descendant may freeze its containing subtree so a harness cannot
replace that path by renaming an ancestor. Choose compatible target contracts; do not weaken
protected paths to make a wrapper run.

The wrapper emits exactly one JSON object on stdout. Send progress and diagnostics to stderr.
Stdout is capped at 1 MiB and stderr at 128 KiB; exceeding either cap aborts the attempt rather
than accepting truncated output.
For example, a successful plan response is:

```json
{
  "schema_version": 1,
  "call_id": "<request.call_id>",
  "profile_id": "<request.profile_id>",
  "status": "success",
  "data": {
    "files": ["src/example.py", "tests/test_example.py"],
    "steps": ["Implement the approved change and add tests"],
    "tests": ["Verify the required behavior"],
    "risks": []
  },
  "num_turns": 1,
  "session_id": "<optional-vendor-session>"
}
```

`status` is `success` or `error`. Optional fields are `text`, `data`, `num_turns`, and
`session_id`; typed stages validate `data` against the supplied schema. Spec output uses `text`.
The response must match the factory's call and profile identity. Additional fields, malformed
JSON, mismatched identity, missing typed output, nonzero exit, and timeout fail closed. A wrapper
must leave build/fix edits uncommitted, obey read-only stages, protect tests during fixes, and
never commit, push, create a PR, or merge. A successful response cannot replace the factory's
verification, approval, or workspace checks.

Host-owned call receipts and committed envelopes bind the output to the call, accepted inputs,
and profile. Sandbox-authored envelopes are not authoritative. Call identities also let an
operator correlate a vendor session with the factory's durable call ledger.

## Spend and recovery

Candidate-provided cost or usage is never trusted. The response schema forbids those fields.
Every invocation reserves its granted ceiling in the existing host call ledger before the
executable starts. The no-charge fixture settles to zero. For a paid profile, even a successful
response leaves usage unknown and the reservation charged until trusted provider evidence is
reconciled through the existing `CallLedger.reconcile` observation path. This can exhaust the
run budget while output exists; it is intentional. Timeouts, malformed responses, and dead
workers likewise cannot release potentially spent money.

Reports distinguish observed `total_cost_usd` from reserved `unreconciled_cost_usd`. An unknown
paid reservation is neither a zero-cost observation nor a final invoice. The factory refuses
another paid launch for the same stage and iteration while its prior call remains unresolved,
even if the run has unused budget.

Do not automatically launch a new paid session after an ambiguous result. Inspect the host call
identity and provider outcome, reconcile observed usage or definite non-submission, then resume
under the existing [recovery procedure](run-recovery.md). Model output and a wrapper's success
status are not billing observations.

## Prime Agent boundary

A Prime Agent wrapper can map the request to its configured model, stage tools, and internal
reasoning/tool loop, then translate its final output to the response contract. Keep that loop
inside one factory invocation. Airflow and the factory retain the spec/plan gates, build/fix
and review/fix bounds, test execution, and delivery authority. Do not launch a second lifecycle
scheduler from the wrapper.

Prime-specific support still requires a separately installed, version-pinned wrapper and evidence
that the selected runtime enforces the factory's tool restrictions, turn limits, cancellation,
and paid USD grant. This seam makes no claim that Prime supplies native USD-cap enforcement.

## Hermetic qualification

`tests/fixtures/external_agent/fixture.py` is a real executable using only the Python standard
library. It reads the existing demo spec/plan/review fixtures, applies the deliberately broken
build patch, then applies the source-only repair without committing. It needs no provider keys
and makes no model calls.

```sh
uv run pytest tests/test_stages_external.py
uv run --group airflow pytest tests/test_dag_smoke.py
```

The CLI test exercises real subprocess calls, red then fresh green JUnit, host receipts and call
identities, replay approvals, factory bot commits, and delivery to a local bare Git remote. The
Airflow smoke runs both scripted and external selection through the real DAG, including the
negative case where marking approval tasks successful without a replay decision authorizes
nothing. Replay decisions remain refused for backend-managed production work. Autonomous policy
gates also refuse external agents until that harness receives separate qualification; selecting
a generic wrapper does not grant autonomous approval authority.

Local external execution requires `allow_local_agent=True` or `--allow-local-agent`. It is an
explicit development escape hatch with no isolation boundary; use it only for a trusted fixture
or deliberate local rehearsal. The harness qualification does not establish production support
for a vendor runtime or sandbox.
