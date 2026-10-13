# Evidence-aware candidate fusion

Two experiments may produce complementary partial repairs even when neither
repair satisfies the complete product contract. `swfactory campaign-fusion`
combines their disjoint regular-file changes into one new patch proposal while
retaining the exact contributing revisions and diagnostic observations.

This is an experimental controller-side search primitive. It does not run agents
or tests, create worktrees, commit, update refs, change campaign selection, approve,
publish, or promote. Airflow remains the lifecycle scheduler. A proposal always
records `authority = "search-only"` and `verification = "required"`.

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

This first increment deliberately does not wire fusion into the default managed
build stage, execute a model synthesizer or start a second repair round. Those
integrations need their own budget reservations, journalled attempts, cancellation
and epoch checks. The artifact contract provides a concrete starting point without
changing the existing scheduling or publication boundary.
