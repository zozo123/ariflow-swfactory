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

## Wild exploration, deterministic convergence

**Exploration is allowed to be wild. Convergence is not.** Exploration entropy may change which
hypotheses are tried. It may not change the meaning of the final gate: replaying the same candidate
identities, evidence digests, requirements and authority state must reproduce the same promotion
decision. First-finisher wins, majority votes without evidence and random tie-breaks at promotion
are therefore forbidden.

## Two decisions, not one

A research loop needs permission to decide **what to test next**. It does not need permission to
decide **what reaches main**.

A stored `CampaignReport` therefore carries two explicit selections:

- `exploration_selection` — the evidence-based choice for descendant experiments;
- `selection` — the promotion-aware choice, including `human_gate`.

Only promotion requires human approval. The readers enforce the rest: `experiment-tree` refuses a
selected node that is not answered, and `campaign-decision` refuses a winner without answered
candidate evidence.

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

`swfactory research-schedule --max-depth 3` prints the same schedule (`--json` for the document).
Width never exceeds `--max-candidates`, and duplicate strategies are refused.

The default is intentionally simple and deterministic. It is a control surface, not a claim that
this exact cooling law is universally optimal.

## Exact descent

`ExperimentTree.validate()` rechecks the complete stacked lineage:

1. the selected candidate must be answered, with an exact recorded output head;
2. the next round's `input_head` is exactly that output SHA;
3. the next round's `parent_candidate` is exactly that candidate id;
4. depths are contiguous and no candidate id is reused.

A later round cannot silently restart from the original base, skip a depth, or descend from an
unselected sibling. No winner is invented when required evidence is missing.

## What remains human

A campaign can end with an `exploration_selection` winner while the promotion `selection` winner is
still `null`.

That is the expected autonomous case. A model or agent may decide that a result is worth another
experiment. It may not convert that research decision into a merge decision. Branch protection,
the existing human gate, and the ordinary publication/promotion path remain authoritative.

## Scheduler boundary

The cooling schedule is data, not a controller. No in-tree driver runs annealing rounds; Airflow
still decides when a governed stage runs, retries, times out, or is cancelled.

This preserves the Liquid rule: create entropy inside a bounded execution phase, then destroy that
entropy before promotion.
