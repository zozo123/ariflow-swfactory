# Dark Factory Architecture

## Thesis

The software factory should become **dark** in the industrial sense: most routine work proceeds without a human standing inside the line.

That does **not** mean autonomous authority.

It means:

- humans define intent, policy, protected boundaries, budgets, and promotion rules;
- the factory continuously explores, implements, tests, reviews, repairs, replays, and compares candidates;
- Airflow owns durable distributed lifecycle scheduling;
- Rust owns factory semantics, identity, evidence, and authority-sensitive execution;
- disposable workers and sandboxes do the untrusted work;
- stochastic systems may propose and explore broadly;
- only deterministic, evidence-bound, policy-compliant results can approach promotion;
- protected publication remains explicit and attributable.

The core rule is:

> **Maximize entropy in search. Minimize entropy as authority increases.**

A dark factory is therefore not an unattended merge bot. It is an unattended **search-and-verification plant** with a narrow, explicit authority boundary.

---

## What exists today

The repository already contains most of the primitives needed for this model.

### Durable lifecycle

Apache Airflow is the only lifecycle scheduler for managed work. It owns:

- DAG scheduling;
- retries;
- task mapping;
- timeouts;
- durable waits;
- human approval points.

Airflow does not own software-factory semantics.

### Rust-first factory runtime

The long-term boundary is now explicit:

```text
operator / harness
      |
      v
     swf
  Rust factory
      |
      +-----------------------+
      |                       |
      v                       v
 Airflow binding        direct local harness
 scheduler only         same factory semantics
```

Rust increasingly owns:

- Factory Cell identity;
- typed stage invocation;
- manager protocol;
- authority-sensitive invariants;
- evidence contracts;
- deterministic convergence;
- operator surfaces.

Python remains where useful for Airflow DAG registration and transitional bindings, but stage ownership migrates one-way toward Rust.

### Factory Cells

A Factory Cell is the durable identity for one logical work item.

Workers are disposable.
Sandboxes are disposable.
Attempts are disposable.

The Cell is not.

Authority-sensitive effects are fenced by Cell identity and epoch so replacement compute cannot silently inherit stale authority.

### Liquid exploration

The factory intentionally permits nondeterminism during search.

Examples:

- multiple candidate lanes;
- repair vs rethink vs start-from-scratch;
- orthogonal hypothesis axes;
- stochastic schedule selection;
- independent review lenses;
- disposable environments;
- speculative worktrees.

The goal is not to make search deterministic. The goal is to make the **decision that survives search** deterministic and reproducible.

### Jev as stochastic oracle

Jev is now integrated as an exploration component, not an authority component.

The current design is:

```text
bounded factory-declared search space
            |
            v
  Jev probability weights
            |
            v
 local sampling with retained entropy
            |
            v
 replayable hypothesis receipt
            |
            v
  actual candidate execution
            |
            v
 ordinary tests / evidence / review
            |
            v
 deterministic fan-in
            |
            v
 existing promotion authority
```

Jev cannot create new authority-bearing options.
It can only weight options the factory already permits.

Provider failure degrades back to local exploration instead of becoming a control-plane failure.

### Deterministic convergence

Candidate generation may be stochastic.

Final convergence must not depend on:

- completion order;
- worker identity;
- wall-clock ordering;
- model confidence;
- retry timing.

The winning result is derived from retained, content-addressed evidence and an explicit objective.

### Protected control plane

The repository already enforces an important dark-factory rule:

> the factory may not silently rewrite the machinery that gives it authority.

Control-plane paths are protected. Even a green PR modifying them requires an explicit maintainer exemption.

That mechanism is more important in a dark factory than in a normal repository because self-improvement otherwise becomes self-authorization.

---

## How one work order should flow

```text
                         HUMAN / POLICY
                               |
                     intent + constraints
                               |
                               v
                    +----------------------+
                    |   Work Order / Cell  |
                    +----------------------+
                               |
                               v
                         AIRFLOW SCHEDULER
                retries / mapping / waits / budgets
                               |
                               v
                     RUST FACTORY MANAGER
                               |
             +-----------------+-----------------+
             |                                   |
             v                                   v
       SEARCH PLANE                        AUTHORITY PLANE
  stochastic / disposable                deterministic / fenced
             |                                   |
   +---------+---------+                         |
   |         |         |                         |
 repair    rethink   scratch                     |
   |         |         |                         |
   +---------+---------+                         |
             |                                   |
             v                                   |
       verify candidates                         |
             |                                   |
             v                                   |
      independent reviews                        |
             |                                   |
             +---------------+-------------------+
                             |
                             v
                   DETERMINISTIC FAN-IN
                             |
                             v
                  exact candidate evidence
                             |
                             v
                      promotion policy
                             |
                      +------+------+
                      |             |
                    refuse        approve
                                      |
                                      v
                              protected publish
                                      |
                                      v
                               HUMAN MERGE / RULE
```

---

## The dark-factory control loop

A mature factory should run five loops.

### 1. Intake loop

Continuously normalize incoming work:

- issues;
- failed CI;
- operator requests;
- incidents;
- dependency updates;
- performance regressions;
- security findings;
- self-improvement proposals.

The output is a bounded, immutable problem statement.

The intake loop may classify and deduplicate.
It may not grant execution authority by itself.

### 2. Search loop

Generate independent candidate strategies.

Possible sources of entropy:

- model choice;
- prompts;
- repair strategy;
- decomposition;
- implementation shape;
- review lens;
- tool profile;
- candidate branch;
- sandbox snapshot.

All entropy is retained in a replay receipt.

This allows the factory to be highly nondeterministic **without becoming irreproducible**.

### 3. Verification loop

Every candidate is challenged with evidence that is harder to forge than the candidate itself.

Examples:

- tests;
- lint;
- build;
- static analysis;
- independent recomputation;
- adversarial fixtures;
- policy checks;
- protected-path checks;
- replay in a fresh environment.

A candidate never becomes more trusted merely because the model that produced it reports success.

### 4. Convergence loop

The factory collapses the candidate cloud.

It should:

1. reject candidates that violate hard constraints;
2. compare survivors under one content-addressed objective;
3. resolve equivalent implementations;
4. retain the strongest evidence;
5. produce exactly one candidate or an explicit no-winner state.

The convergence result is a projection from high-entropy search into low-entropy authority.

### 5. Learning loop

The factory measures itself.

Useful signals include:

- candidate success rate;
- repair success by failure class;
- verifier catch rate;
- review disagreement;
- wasted compute;
- queue depth;
- human intervention rate;
- escaped defects;
- rollback rate;
- evidence completeness;
- time-to-convergence.

The learning loop may propose:

- new search strategies;
- different budgets;
- new review lenses;
- new tools;
- different annealing schedules.

It must not bind those changes into authority policy without the required gate.

---

## Search plane vs authority plane

This separation is the central safety and scaling mechanism.

| Search plane | Authority plane |
| --- | --- |
| stochastic | deterministic |
| many candidates | one accepted state |
| disposable sandboxes | durable identities |
| model suggestions | policy decisions |
| speculative branches | protected publication |
| local entropy | retained evidence |
| broad permissions inside sandbox | minimal external capabilities |
| replaceable workers | fenced Cell epochs |
| best-effort heuristics | fail-closed contracts |

The dark factory should become **more autonomous in the left column** and **more strict in the right column** at the same time.

---

## Human role

A dark factory should reduce human mechanical work, not erase human responsibility.

Humans should increasingly act at these boundaries:

### Intent

What outcome is actually wanted?

### Policy

What is allowed, protected, expensive, or irreversible?

### Exceptional ambiguity

When evidence does not distinguish the choices.

### Control-plane evolution

Changes to identity, policy, fencing, credentials, protected paths, promotion, and verifier semantics.

### Promotion

Where organizational policy requires explicit acceptance.

Everything between those boundaries should become progressively more automatic.

---

## Evolution from today's factory

### Generation 0 — scripted automation

```text
issue -> agent -> tests -> PR
```

Useful, but little durable state or authority discipline.

### Generation 1 — managed factory

```text
issue -> Airflow lifecycle -> governed stages -> evidence -> PR
```

This repository already passed this point.

### Generation 2 — liquid factory

```text
problem
  -> parallel candidate search
  -> evidence
  -> deterministic convergence
  -> promotion
```

This is largely present now.

### Generation 3 — dark factory

```text
continuous intake
  -> autonomous bounded search
  -> autonomous repair/retry/replay
  -> adversarial verification
  -> deterministic convergence
  -> exception-only human attention
  -> explicit protected promotion
  -> self-measurement
  -> proposed process improvements
  -> repeat
```

Humans move from operating stages to governing the plant.

### Generation 4 — self-optimizing dark factory

The factory can run controlled experiments on its own process:

- compare search strategies;
- compare verifier sets;
- change candidate budgets;
- measure marginal value of another lane;
- learn which repair strategy works by failure type;
- learn which reviewer catches which defect class;
- cache and replay known-good execution environments.

The optimization loop remains outside the authority loop:

```text
learn -> propose policy/process change -> verify -> human/policy bind
```

Never:

```text
learn -> silently rewrite authority
```

---

## Dark-factory invariants

These should remain true even when humans are no longer watching individual runs.

### Identity

Two different authority domains must never collapse to one Cell identity.

### Epoch fencing

A replaced worker or retry must not retain stale authority.

### Capability binding

External capabilities must bind to immutable execution identity and current policy.

### No ambient secrets

Workers receive only the minimum capability required for the current operation.

### Evidence monotonicity

Reported evidence may become verified evidence only through an independent verification step.

### Search cannot promote

No stochastic oracle, model score, candidate count, or confidence can directly authorize promotion.

### Exact-candidate binding

Promotion evidence must bind to the exact bytes being promoted.

### Independent verification

The component under test cannot be the sole source of proof that it passed.

### Protected control plane

Self-improvement may propose control-plane changes but cannot authorize them.

### Replayability

Every stochastic choice that matters to execution must be recoverable from retained state.

### Bounded autonomy

Every loop has explicit time, cost, concurrency, and attempt limits.

### Explicit failure

If the factory cannot prove a safe transition, the result is blocked or in-doubt, never silently assumed successful.

---

## What to build next

The highest-value work is now invariant closure rather than more architectural surface area.

### P0 — identity correctness

Close all Cell and policy identity ambiguities before expanding Rust authority.

In particular:

- make target identity structurally unambiguous;
- enforce one Cell-ID contract in Python and Rust;
- define canonical policy bytes across languages;
- add adversarial cross-language fixtures.

### P0 — capability leases

Bind credentials to:

```text
factory_run
dag_run
task_instance
stage
sandbox
attempt
cell
epoch
operation
policy
```

A retry is a new security event.

### P0 — dark-worker boundary

Airflow workers and coding sandboxes should have an executable NEVER-list:

- no ambient GitHub token;
- no raw publication authority;
- no control-plane mutation;
- no credential-broker bypass;
- no implicit secret injection.

### P1 — independent convergence verifier

A promotion-grade result should be independently recomputed from:

- exact source;
- exact recipe;
- exact dependency/tool identity;
- fresh execution environment.

### P1 — exception-only operator attention

The operator UI should focus on:

- blocked authority transitions;
- in-doubt effects;
- lease debt;
- fence debt;
- verifier disagreement;
- human gates.

Routine successful search should fade into the background.

### P1 — queue governor

A self-improving factory must not drown its own human gates.

Proposal entropy can remain high while enrollment rate is bounded.

### P2 — adaptive search budget

Spend more compute only when another candidate has positive expected information value.

Stop when:

- one candidate dominates under the objective;
- additional lanes are redundant;
- the budget is exhausted;
- uncertainty is below the configured threshold.

### P2 — failure memory

Build a durable map:

```text
failure signature
    -> strategies tried
    -> evidence
    -> successful repair class
    -> cost / latency
```

Future runs use this as prior information, not as authority.

---

## End state

The target is not:

> AI writes code while nobody watches.

The target is:

> A continuously operating software-production system explores aggressively, executes in disposable environments, retains exact evidence, challenges its own outputs, converges deterministically, and requests human attention only where authority or genuine ambiguity requires it.

That is a dark software factory.

The factory can be highly creative.

The factory can be highly autonomous.

The factory can even improve the process that creates software.

But the closer an action gets to durable external authority, the fewer degrees of freedom remain.

**Exploration is liquid. Evidence crystallizes. Authority is singular.**
