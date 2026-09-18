# Deterministic experiment continuation

The experiment tree records which candidate won a bounded sibling round. The next round must not
quietly choose a different parent, start from a moving branch, or reuse the previous campaign
identity.

`swfactory.evolution.plan_next_round` turns one validated campaign report into the only legal next
sibling bush:

```text
round N
  input = A
    |
    +-- repair  -> B
    +-- rethink -> C  * selected, frozen
    +-- scratch -> D
                    |
                    v
round N+1
  input = C
  parent_candidate = exact selected candidate id
    |
    +-- repair
    +-- rethink
    +-- scratch
```

## Trust boundary

Stored campaign JSON is treated as untrusted input. Continuation is refused unless all of these
agree:

- the top-level campaign id and the embedded experiment round id;
- the top-level input head and the experiment round input head;
- the top-level selected winner and the experiment round winner;
- the winner is an answered/frozen node;
- the winner has an exact recorded output revision;
- the new campaign id differs from the previous campaign id;
- the requested width/depth still fits the ordinary campaign budget.

The emitted requests inherit the existing Cell id and epoch, set `depth = previous_depth + 1`,
set `parent_candidate` to the exact winning node, and use the winner's recorded Git SHA as every
sibling's `input_head`.

## Operator use

Plan the default repair/rethink/scratch sibling bush:

```sh
uv run swfactory experiment-next .factory/campaigns/round-0.json \
  --campaign-id round-1 \
  --json
```

Choose an explicit bounded strategy set:

```sh
uv run swfactory experiment-next .factory/campaigns/round-0.json \
  --campaign-id round-1 \
  --strategy repair \
  --strategy scratch \
  --json
```

The output includes each deterministic `CandidateRequest.logical_id`, exact input SHA, parent
candidate, depth, budget share, and timeout.

## Authority boundary

Continuation is **planning**, not execution.

It does not:

- start an Airflow task;
- create a worktree or sandbox;
- publish a branch or PR;
- choose a new winner;
- approve promotion.

Airflow remains the lifecycle scheduler. Candidate execution still uses the existing isolated
worktree/provider boundary, and promotion still requires the existing human gate.

This separation lets an operator or a future Airflow stage inspect the exact next exploration shape
before spending compute, while preserving the Liquid factory invariant: exploration can branch
widely, but ancestry and convergence are deterministic.
