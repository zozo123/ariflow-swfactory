# Campaign decision evidence

OpenResearch-style parallel exploration is useful only if deterministic fan-in remains auditable after
the disposable workspaces are gone. The factory therefore retains a separate **campaign decision
manifest** that binds the stored ranking and winner to every answered sibling's candidate-evidence
bundle.

This sits above the existing evidence layers:

```text
exact input commit
       |
       v
source snapshot digest
       |
       v
isolated candidate worktree
       |
       v
frozen output ref
       |
       v
candidate evidence bundle
  source + output + diff + artifacts
       |
       +-----------------------+
       |                       |
       v                       v
 candidate A digest      candidate B digest   ...
       \                       /
        \                     /
         +-- deterministic fan-in --+
                     |
                     v
          campaign decision digest
                     |
                     v
          review / promotion gate
```

The manifest is evidence, not authority. Airflow remains the lifecycle scheduler and the existing
human/branch-protection boundary remains the promotion authority.

## Why a second manifest?

A `CampaignReport` records evaluations, ranking, refusals, and the proposed winner. A candidate
evidence bundle records the exact retained bytes for one answered candidate. Without a join between
them, a later reviewer can see both objects but cannot prove which retained candidate evidence the
fan-in decision actually used.

`CampaignDecisionManifest` closes that seam.

For every candidate it retains:

- logical candidate id and strategy;
- state, exact input/output heads, and frozen candidate ref;
- ordered structured evaluations;
- cost and duration used by deterministic ranking;
- the canonical digest of the retained candidate-evidence bundle.

It also retains the exact selection winner, reason, ranking, refusals, independence findings, and
campaign cancellation state under one canonical SHA-256 digest.

## Losing evidence is first-class

Every **answered** candidate requires a retained evidence bundle, including candidates that lost.

That is intentional. If only the winner survives, the system preserves a result but loses the
experimental information that justified preferring it. A failed or provisional candidate may lack
an answered bundle because infrastructure did not produce a distinct frozen revision.

The builder fails closed if:

- an answered sibling has no retained bundle;
- a bundle belongs to another candidate;
- its input/output Git lineage differs from the campaign outcome;
- its frozen ref differs from the campaign outcome;
- evidence is supplied for a provisional/failed candidate;
- selection ranking omits or duplicates candidates;
- a proposed winner is not backed by answered evidence.

## Build

After candidate bundles exist, join them to the stored campaign report:

```sh
uv run swfactory campaign-decision build \
  .factory/campaigns/round-0.json \
  .factory/campaigns/round-0.decision.json \
  --repo . \
  --candidate-evidence cand_repair=.factory/candidates/cand_repair-evidence \
  --candidate-evidence cand_rethink=.factory/candidates/cand_rethink-evidence
```

The command verifies each candidate bundle, including its immutable Git ref, before writing the
decision manifest.

## Verify

```sh
uv run swfactory campaign-decision verify \
  .factory/campaigns/round-0.decision.json \
  --repo . \
  --candidate-evidence cand_repair=.factory/candidates/cand_repair-evidence \
  --candidate-evidence cand_rethink=.factory/candidates/cand_rethink-evidence \
  --json
```

Verification recomputes the campaign manifest digest, re-hashes every answered candidate bundle,
re-checks its frozen candidate ref, and requires the evidence membership to match the answered
candidate set exactly.

This means neither a winner swap, ranking edit, deleted losing sibling, artifact mutation, bundle
substitution, nor frozen-ref drift can silently preserve the original campaign-decision identity.

## Relationship to the experiment tree

The [candidate experiment tree](experiment-tree.md) answers **where this round sits in lineage**.
The decision manifest answers **why this round collapsed to this winner, against these exact
siblings and evidence**.

A later round should therefore descend from the prior selected output SHA, while the retained
decision digest makes the fan-in that selected that SHA independently auditable.
