# Candidate experiment tree

The Liquid Software Factory already had bounded candidate campaigns, isolated execution
contracts, SHA-bound evidence, and a separate human promotion boundary. This slice makes the
**lineage between candidate rounds explicit**.

The design was inspired by the experiment-tree discipline in
[alphaXiv/OpenResearch](https://github.com/alphaXiv/OpenResearch): parallel directions are
independent, successful runs are tied to immutable revisions, evidence stays attached to the work
that produced it, and later experiments descend from an earlier result rather than repeatedly
branching from the original baseline.

This repository adapts those ideas to a software factory. It does not import OpenResearch's
scheduler, storage model, UI, or authority structure.

## Stacked bushes

One campaign is one decision round. Its candidates are siblings because they answer the same
question from the same input revision.

```text
base
├── repair   -> sha-a
├── rethink  -> sha-b   * selected
└── scratch  -> sha-c

sha-b
├── repair   -> sha-d
└── scratch  -> sha-e   * selected

sha-e
...
```

Width represents unresolved options in one round. Depth represents decisions already resolved and
carried forward. A second round therefore starts from the exact recorded head of the previous
round's selected candidate.

The tree rejects both common degeneracies:

- **flat fan** — every new idea branches from the original base, so wins never accumulate;
- **noodle** — unrelated co-equal ideas are chained only to manufacture depth.

## Frozen versus provisional

A candidate is **answered** when its execution completed successfully and produced a distinct
recorded output head. The answer can still be bad: tests may fail or the candidate may lose to a
sibling. It is nevertheless evidence and is frozen.

A candidate remains **provisional** when infrastructure did not answer the question:

- runner crash;
- cancellation;
- missing output head;
- output head identical to the input head.

Provisional nodes can be repaired and rerun. Answered nodes are not rewritten; later work branches
from a selected answered node.

This distinction prevents an OOM or dead runner from becoming fake experimental evidence while also
preventing a disappointing but valid result from being silently edited away.

## Exact-revision exploration versus promotion

The factory keeps two explicit decisions instead of overloading one word:

- `exploration_selection` chooses the evidence-best answered revision that later experiments may
  descend from;
- `selection` remains promotion-aware and still requires the existing human gate.

Both refuse candidates with no distinct output revision. A frozen candidate also needs its immutable
candidate evidence digest before autonomous exploration may descend from it.

Promotion requires all exploration conditions **plus human approval**.

This distinction lets an autonomous experiment loop ask the next question without granting itself
authority to merge, deploy, or promote the answer. The experiment tree records exploration lineage;
the promotion surface remains singular and human-gated.

## Two independent lineages

Candidate lineage and factory-generation lineage are deliberately separate:

- `parent_generation` links one factory generation to another;
- `parent_candidate` links one experiment round to the selected candidate of the previous round.

Overloading one identifier for both would make replay, audit, and promotion ambiguous.

Candidate identity now includes campaign, Cell, epoch, strategy, input head, generation parent,
candidate parent, and depth. The same strategy asked at a different tree position is therefore a
different question rather than an accidental replay alias.

## Verified evidence-backed descent

An exploration choice may become the parent of the next research round without gaining release
authority. Before it does, rebuild the evidence chain:

1. the exploration winner must be an answered, frozen candidate;
2. `swfactory campaign-decision build` re-verifies every answered sibling's retained evidence
   bundle against Git and rebuilds the deterministic fan-in;
3. the experiment-tree winner must equal the exploration winner, and its recorded head must equal
   the frozen candidate output;
4. the children are planned from that exact output SHA at `depth + 1`, and
   `plan_requests(..., parent_decision_digest=...)` binds the campaign-decision SHA-256 into every
   child's stable identity.

Two child questions with the same apparent source SHA are therefore different questions when they
descend from different verified parent decisions.

Planning requests schedules nothing. Airflow remains the lifecycle scheduler, and promotion still
requires the existing human/branch-protection gate.

## Authority boundary

The experiment tree is advisory state.

It does **not**:

- schedule Airflow tasks;
- create or merge pull requests;
- grant repository credentials;
- override Cell epoch fencing;
- decide human approval;
- promote a factory generation.

Apache Airflow remains the lifecycle scheduler. Factory Cells remain the durable mutation identity.
The existing promotion boundary remains singular.

## Stored report

`CampaignReport.to_dict()` now emits schema version 3, records both selection surfaces, and includes an `experiment_round` object:

```json
{
  "schema_version": 3,
  "campaign_id": "round-1",
  "input_head": "sha-parent",
  "experiment_round": {
    "schema_version": 1,
    "round_id": "round-1",
    "depth": 1,
    "parent_candidate": "cand_previous_winner",
    "winner_id": "cand_current_winner",
    "nodes": [
      {
        "id": "cand_current_winner",
        "state": "answered",
        "frozen": true,
        "recorded_head": "sha-current"
      }
    ]
  }
}
```

Evidence strings from each evaluation are retained on the node so the lineage points back to the
reason a revision was judged.

## Inspecting multiple rounds

Each campaign report can be validated and rendered with the module entry point:

```sh
uv run python -m swfactory.experiment_tree \
  .factory/campaigns/round-0.json \
  .factory/campaigns/round-1.json
```

The command fails closed if a later round:

- skips a depth;
- names the wrong parent candidate;
- starts from anything other than the previous winner's recorded head;
- reuses a candidate identity;
- selects a provisional node.

Use `--json` to emit the combined validated tree. Use `--mermaid` to emit a deterministic
GitHub-compatible flowchart that highlights selected nodes and preserves the same validated parent
edges:

```sh
uv run swfactory experiment-tree \
  .factory/campaigns/round-0.json \
  .factory/campaigns/round-1.json \
  --mermaid
```

Because node identifiers in the Mermaid document are generated from validated depth/index positions,
candidate-controlled strings cannot create graph edges or alter lineage semantics.

## Current support boundary

This feature strengthens the **experimental candidate-campaign/generation model**. It does not mean
the default managed build stage now launches recursive provider forks. Provider-native fork
execution remains governed by the existing capability inventory and lifecycle contracts.
