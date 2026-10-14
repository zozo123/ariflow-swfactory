# Evidence-aware candidate fusion

Two experiments may produce complementary partial repairs even when neither
repair satisfies the complete product contract. `swfactory campaign-fusion`
combines their disjoint regular-file changes into one new patch proposal while
retaining the exact contributing revisions and diagnostic observations.

The proposal commands are experimental controller-side search primitives. They do
not run agents or tests, create worktrees, commit, update refs, change campaign
selection, approve, publish, or promote. Airflow remains the lifecycle scheduler.
A proposal always records `authority = "search-only"` and `verification = "required"`.

## Execute the complete bounded workflow

```sh
uv run swfactory demo --fusion
```

This keyless scripted replay runs the normal intent and plan gates, four distinct
checkpoint checkouts, independent candidate checks, composition, verification,
review and local delivery. One repair adds the implementation and requirement
tests; another adds the export. Both fail separately, while their composition
passes all seven target tests. The demo is mechanism evidence, not a claim that
scripted fixtures establish model performance or production-provider readiness.

A trusted installed blueprint opts into the managed build runtime with:

```toml
[fusion]
max_rounds = 2
max_parallel = 4
max_cost_usd = 2.0
timeout_s = 600

[fusion.models]
repair-a = "claude-sonnet-4-5"
repair-b = "claude-sonnet-4-5"
```

Omit `[fusion]` to retain the ordinary build path. Unknown fields and roles are
refused. Model overrides are optional and do not imply statistically independent
errors. Fusion parameters, approved plan and prompt content are bound to accepted
inputs and a host-owned retry pin.

The runtime currently supports scripted local replay and native Claude on the
existing SRT/Docker boundaries. Local execution remains test-only. Remote islo,
toolset and boat checkpoint transport and external harness execution fail closed
before model spending; native memory forks are not claimed. Checkpoints carry
source history, while each worker prepares locked dependencies separately.

Each round runs two repair strategies, one diagnostic and one challenger. Workers
leave edits uncommitted; the controller validates approved file scope, freezes
source revisions and invokes the pinned contract's lint and test commands. Tests
must produce fresh valid nonempty JUnit evidence. Diagnostic observations stay
untrusted. Passing siblings are retained as fallbacks. Disjoint composition is
tried first; overlaps or failing composition invoke one bounded semantic synthesis
attempt using candidate diffs, observations and controller-observed failures.
Every resulting program is checked afresh. If all programs fail, one retained
continuation and its counterexamples guide the second and final round.

The controller reserves the whole campaign before launch, limits individual
model calls to shares of that reservation, and permits at most ten model calls
and nine verifier invocations. Child Docker attempt identities are distinct.
Credentials are projected through the existing sandbox boundary, never copied
from Git configuration or carried in source checkpoints. Workers are drained and
their disposable checkouts removed before adoption. A host-owned
`fusion-cancelled` control file stops new work and adoption; running calls drain
under their timeouts. Managed runs recheck the backend Cell epoch and policy.

Completed attempts replay from receipts. An interrupted attempt is in doubt and
requires reconciliation rather than a blind paid retry. On success, the
controller adopts the selected patch into the governed workspace, requires its
tree to equal the checked candidate, and runs final product verification there.
Existing review, repair, evidence publication and authorized merge follow normally.
No search result grants delivery authority.

The delivery evidence includes `replica-fusion-report.json`: pinned inputs,
round lineage, all attempt outcomes, exact source/recipe-bound check receipts,
counterexamples, winning artifact, final verification, cost and fusion-only status.
The Git pool and candidate bundles remain in host-owned run storage. A fusion-only
win means the composed/synthesized candidate passed while both individual repair
candidates had completed failing checks; missing checks never count as failures.

## Build from retained sibling evidence

First retain frozen candidate bundles and build the campaign decision as described
in [candidate evidence](candidate-evidence.md). All contributors must be answered
siblings from that same decision and base commit. Answered means an immutable
output exists; it does not mean the candidate passed its acceptance checks.

```sh
uv run swfactory campaign-fusion build \
  .factory/campaigns/decision.json .factory/fusion/round-0 \
  --repo . --include repair-a --include repair-b \
  --candidate-evidence repair-a=.factory/candidates/repair-a \
  --candidate-evidence repair-b=.factory/candidates/repair-b \
  --observations .factory/campaigns/observations.json
```

Supply `--candidate-evidence` for **every answered sibling**, including siblings
not selected for composition. The existing campaign verifier rechecks complete
membership, bundle bytes and frozen refs before composition. Select two to four
distinct contributors explicitly; completion order never determines composition.

The command loads `factory.toml` from the exact base commit, preserves its build
protections and fixed authority paths, and refuses symlinks, submodules, overlapping
file edits and file/directory collisions. An overlap requires a separate semantic
synthesis attempt; it is never silently resolved or dropped. Each contributor is
limited to 512 changed files and the generated patch to 16 MiB.

Composition uses a temporary private Git index. It may write unreachable tree
objects to the repository object store, but leaves the real index, HEAD, refs and
working files unchanged, including staged user edits. Existing output directories
are refused. No candidate code, Git hooks, external diff programs or text conversion
programs are executed.

The output contains:

```text
round-0/
├── candidate.diff      binary-capable, full-index patch against the common base
└── manifest.json       input, result tree, contributors, observations and digests
```

The manifest binds the campaign decision, Cell and epoch, common base, every
contributing bundle and output revision, the resulting Git tree, patch bytes and
observation ledger. `existing_selection` preserves the campaign's search selection
as context; it is not a claim that a passing fallback exists.

## Diagnostics do not need a patch

An optional observation document can include diagnostics from agents that never
produced a changed revision:

```json
{
  "schema_version": 1,
  "authority": "untrusted-observations",
  "campaign_decision_digest": "sha256:<64 lowercase hex digits>",
  "observations": [
    {
      "evidence_id": "reproducer-duplicate-dispatch",
      "producer_id": "diagnostic-c",
      "kind": "counterexample",
      "statement": "Retry after ownership changes can dispatch the message twice.",
      "subject_head": "<40-character base or answered-candidate SHA>",
      "artifact_digest": "sha256:<optional witness artifact digest>",
      "source_evidence_ids": ["original-retry-failure"]
    }
  ]
}
```

`kind` is `hypothesis`, `reproducer` or `counterexample`. Omit `artifact_digest`
when there is no retained witness reference. The input is limited to 1 MiB and
256 observations; unknown fields are refused. Subjects must belong to the bound
campaign, and the document must name its exact decision digest.

Repeated evidence IDs with identical content are collected once, retaining all
producer IDs. Reusing an ID for a different subject, statement, witness or lineage
is an error. An artifact digest is an untrusted reference: this command does not
fetch or authenticate that witness, reproduce a claim, or derive a confidence
score from multiple producers. Diagnostic observations remain separate from code
composition; they are retained for review and subsequent constrained synthesis.

## Integrity is separate from program acceptance

```sh
uv run swfactory campaign-fusion verify \
  .factory/campaigns/decision.json .factory/fusion/round-0 \
  --repo . \
  --candidate-evidence repair-a=.factory/candidates/repair-a \
  --candidate-evidence repair-b=.factory/candidates/repair-b \
  --observations .factory/campaigns/observations.json
```

Verification reconstructs the composition from the original inputs and compares
the exact manifest and patch. It refuses changed lineage, observations, refs or
artifact bytes. Supply the original observations if the build included them.
The command checks artifact integrity, not behavior; its success cannot satisfy
the product's test, review or delivery gates.

To use the proposal, the trusted controller must admit it into an isolated target
workspace at the recorded base, apply normal patch policy, freeze the resulting
candidate and run the required product verification and review on those exact
bytes. Contributor test results never transfer to the composed program. Review
repairs require fresh verification too. Missing required evidence remains a refusal.

The standalone proposal commands remain useful independently of the managed
runtime. They never activate a blueprint or inherit the runtime's acceptance
receipts. A proposal's integrity check still cannot replace product verification.
