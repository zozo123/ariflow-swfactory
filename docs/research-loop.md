# Bounded autoresearch annealing

The candidate system can now close the experimental loop without closing the promotion loop.

This slice adapts the useful **Autoresearch** pattern from
[alphaXiv/OpenResearch](https://github.com/alphaXiv/OpenResearch): propose independent directions,
run them, inspect retained evidence, choose what to explore next, and repeat. The adaptation stays
inside the Liquid Software Factory's authority model.

```text
root SHA
   |
   +-- round 0: repair / rethink / scratch    (high exploration entropy)
   |          |
   |          +-- evidence-best answered SHA
   |                         |
   +-- round 1: repair / rethink              (cooling)
   |                         |
   |                         +-- evidence-best answered SHA
   |                                            |
   +-- round 2: repair                          (collapse)
                                                  |
                                                  v
                                         final explored SHA
                                                  |
                                             HUMAN GATE
                                                  |
                                                  v
                                             promotion
```

## Two decisions, not one

A research loop needs permission to decide **what to test next**. It does not need permission to
decide **what reaches main**.

`CampaignReport` therefore carries two explicit selections:

- `exploration_selection` — deterministic evidence-based choice for descendant experiments;
- `selection` — the existing promotion-aware choice, including `human_gate`.

Both require a successful candidate, a distinct output SHA, and all required evaluation dimensions.
Only promotion requires human approval.

This separation fixes an earlier modeling ambiguity where the experiment tree could only descend
after setting `human_approved=True`. Exploration can now be autonomous while release authority
remains singular.

## Cooling schedule

`annealed_strategy_schedule(...)` starts with the configured strategy set and reduces width by one
per depth until one strategy remains. With the default strategies and depth 3:

```text
depth 0   repair  rethink  scratch
depth 1   repair  rethink
depth 2   repair
depth 3   repair
```

The exact strategy schedule may be supplied explicitly. Every round still obeys
`CampaignBudget.max_candidates`, and duplicate strategies in a round are rejected.

The default is intentionally simple and deterministic. It is a control surface, not a claim that
this exact cooling law is universally optimal.

## Exact descent

After a round finishes:

1. all sibling outcomes are retained in request order;
2. `exploration_selection` ranks only stored candidate properties, never completion order;
3. the selected candidate must have an exact distinct `output_head`;
4. the next round's `input_head` is exactly that output SHA;
5. the next round's `parent_candidate` is exactly that candidate id;
6. `ExperimentTree.validate()` rechecks the complete stacked lineage.

A later round cannot silently restart from the original base, skip a depth, or descend from an
unselected sibling.

## Global bounds

`run_annealing_loop(...)` treats the existing `CampaignBudget` as a loop-wide envelope:

- `max_depth` bounds how many descendant decisions can accumulate;
- `max_candidates` bounds width in each round;
- `max_cost_usd` is decremented cumulatively across rounds;
- `max_wall_s` is decremented cumulatively across rounds.

The loop stops with an explicit reason:

- `max_depth`;
- `budget_exhausted`;
- `no_exploration_candidate`;
- `cancelled`;
- `schedule_exhausted`.

No winner is invented when required evidence is missing.

## What remains human

The loop can end with an `exploration_winner` while `promotion_winner` is still `null`.

That is the expected autonomous case. A model or agent may decide that a result is worth another
experiment. It may not convert that research decision into a merge decision. Branch protection,
the existing human gate, and the ordinary publication/promotion path remain authoritative.

## Scheduler boundary

`run_annealing_loop` is an in-stage bounded controller. It does not create a new lifecycle
scheduler. Airflow still decides when the governed stage runs, retries, times out, or is cancelled.

This preserves the Liquid rule: create entropy inside a bounded execution phase, then destroy that
entropy before promotion.


## Jev-weighted stochastic build lanes

The current Python campaign runtime can consume the replayable build-hypothesis receipt emitted by
the Rust exploration contract. This is a migration bridge, not a second Jev implementation:
Python never calls the provider and never interprets probability vectors.

A receipt whose `choices.strategy` is `repair`, `rethink`, or `scratch` moves that declared
strategy to the front of the campaign schedule. With a narrow candidate budget this changes which
real candidate build runs; with a wider budget the remaining declared strategies stay behind it so
diversity is preserved.

The bridge refuses receipts that are not `authority=exploration-only`, refuses unknown strategy
values, and refuses simultaneous ownership by an explicit strategy schedule. Candidate evaluation,
evidence requirements, `exploration_selection`, the human gate, and promotion selection are
unchanged.
