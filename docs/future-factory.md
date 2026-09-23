# Future Factory: recursive Liquid software engineering

This document is the executable architecture north star for the repository.

The compact form is:

> **Create entropy where exploration benefits from it. Destroy entropy before authority.**

And the hard boundary is:

> **The factory may recursively improve how it thinks, but never recursively expand what it is allowed to do.**

The machine-readable version of this contract lives in
[`config/future-factory.yaml`](../config/future-factory.yaml).

## End state

The target is not "more coding agents". It is a scientific engine for software:

```text
immutable problem / accepted inputs
            |
            v
     FACTORY CELL + EPOCH
            |
            v
 cheap heterogeneous worlds  <---------------------------+
 gas: explore / mutate                                    |
            |                                             |
            v                                             |
 content-addressed artifact blackboard                    |
            |                                             |
            v                                             |
 measure diversity / disagreement / evidence              |
            |                                             |
            v                                             |
 recursive search-law extraction                          |
            |                                             |
            v                                             |
 phase-aware population + compute allocation              |
            |                                             |
       +----+---------------------+                       |
       |                          |                       |
       v                          v                       |
 critical disagreement       useful local basin           |
 deep selective compute      narrow exploitation          |
       |                          |                       |
       +------------+-------------+                       |
                    v                                     |
             freeze exact candidate                       |
                    |                                     |
                    v                                     |
        independent verifier population                   |
                    |                                     |
                    v                                     |
             evidence crystal                             |
                    |                                     |
                    v                                     |
             AuthorityRequest                             |
                    |                                     |
                    v                                     |
          RUST REALITY KERNEL                             |
 Cell / epoch / policy / capability / evidence / effect   |
                    |                                     |
                    v                                     |
           explicit promotion gate                        |
                    |                                     |
                    v                                     |
              durable effect                              |
                    |                                     |
                    +--> memory/search laws ---------------+
```

Airflow owns time. Rust owns durable reality. Python is the experimental cortex.
Sandboxes are disposable executable worlds. Evidence is memory.

## 1. Durable matter, disposable motion

The factory deliberately separates things that may disappear from things whose identity must survive.

**Disposable motion**

- workers;
- agent sessions;
- prompts;
- model samples;
- sandboxes;
- VMs and containers;
- speculative worktrees;
- branches;
- intermediate plans;
- temporary contexts;
- unselected candidate implementations.

**Durable matter**

- Cell identity;
- Cell epoch;
- work order;
- accepted input digest;
- policy digest;
- approval digest;
- operation intent;
- mutation journal;
- frozen candidate digest;
- source digest;
- execution recipe digest;
- evidence digest;
- publication receipt;
- recursive search provenance.

Replacing compute never transfers authority.

## 2. One lifecycle scheduler

Apache Airflow remains the only managed lifecycle scheduler.

Airflow owns:

- task mapping;
- waits;
- retries;
- timeouts;
- human interrupts;
- bounded concurrency;
- fan-in;
- durable continuation.

It does not decide truth, promotion, policy identity or external mutation authority.

The target product statement remains:

> **Rust is the factory. Airflow is the scheduler. `swf` is the harness.**

## 3. Factory Cells and fencing

A Factory Cell is the durable issue x target identity.

The epoch is its mutation fence.

A stale process may continue to compute, but every protected effect must reject the old epoch.
Authority therefore survives worker replacement without making the worker authoritative.

The first task in an epoch admits one immutable snapshot of:

- issue/work-order content;
- blueprint;
- referenced prompts;
- effective policy;
- target identity.

Later tasks recompute and compare before agent/provider I/O. A drifted run refuses before writing code.

## 4. Search and authority are different universes

Search may be:

- stochastic;
- redundant;
- speculative;
- heterogeneous;
- recursively adaptive;
- massively parallel.

Authority may not.

Search cannot:

- approve a gate;
- mint credentials;
- publish;
- merge;
- promote;
- rewrite policy;
- weaken a verifier.

A search result can produce an **authority request**. It cannot produce an authority grant.

## 5. System 1 creates entropy

System 1 is a regime, not a particular model.

The factory explores through diversity across:

- model;
- prompt;
- role;
- runtime;
- context;
- strategy;
- mutation operator;
- verifier assumptions.

The basic strategy family remains:

- repair;
- rethink;
- scratch.

Additional worlds can be specialist, adversarial, security-focused or counterfactual.

The goal is not agent count. The goal is **effective independent search**.

## 6. Effective independent search

Ten models producing the same behavior are not ten independent experiments.

`swfactory.swarm_dynamics.effective_independent_search` discounts populations by pairwise
behavioral correlation.

The controller therefore measures:

- raw population;
- effective population;
- mean pairwise correlation;
- novelty;
- distinct executable outputs.

When the population collapses, the right response is not automatically "spawn more".
It is to mutate search coordinates.

## 7. Artifact-mediated swarm coordination

Agents should not need one giant hidden shared conversation.

The recursive search blackboard retains content-addressed artifacts:

- hypotheses;
- failures;
- evidence;
- counterexamples;
- search laws;
- verifier results.

Artifacts have immutable identities and optional parent links.

The blackboard contains no credentials and carries no promotion authority.

This gives the swarm a shared external medium while keeping individual trajectories independently
replayable.

## 8. Recursive search

A normal campaign searches implementations.

Recursive search also searches the **search process**.

Each completed campaign is reduced into order parameters:

- attempts;
- answered worlds;
- evidence completeness;
- required-dimension passes;
- unique outputs;
- disagreement;
- novelty;
- cost;
- strategy-specific conversion rates.

Those observations are compressed into bounded search laws such as:

- prefer a strategy that repeatedly yields evidence-complete answers;
- deprioritize a strategy producing correlated outputs;
- diversify after population collapse;
- measure when disagreement is high;
- verify when evidence is high and disagreement is low;
- perturb after a dead basin.

Search laws are lossy summaries. Raw evidence remains authoritative.

## 9. Search provenance is part of candidate identity

A candidate is not only:

```text
parent + strategy + input SHA
```

It is also a consequence of a search policy.

The request therefore carries a `search_provenance_digest`.

Changing the recursive plan that caused a candidate to exist changes the logical question and
therefore changes candidate identity.

The adaptive round document retains both the full swarm-plan receipt and the full search-provenance
receipt beside their digests. Candidate evidence retains the provenance digest. That creates a
replayable chain from frozen candidate evidence back to the exact search laws, blackboard identity,
hotspots, phase and population decision that caused the candidate to exist.

This makes meta-search replayable rather than hidden controller state.

## 10. Phase control is metacognition

The factory classifies observable regimes:

| Phase | Meaning | Default response |
| --- | --- | --- |
| gas | high entropy, weak coherence | diverge |
| liquid | mobile structured search | coordinate |
| critical | decision-sensitive disagreement | measure |
| crystal | low entropy, strong exact evidence | verify |
| glass | low mobility without enough evidence | perturb |
| jammed | queue/resource/debt dominates | drain |

The phase controller is search-only.

It can recommend population, context and verification posture.
It cannot approve or promote.

## 11. Population and compute are phase-aware

`swfactory.swarm_dynamics` turns phase + information state into a bounded heterogeneous population.

### Gas

Use many cheap independent explorers and mutators.

High correlation increases pressure to change diversity axes rather than duplicate agents.

### Liquid

Coordinate through compact evidence:

- explorers;
- synthesizers;
- critics;
- normal verifiers.

### Critical / anneal

Stop spreading expensive compute uniformly.

Rank disagreement hotspots by approximately:

```text
value = disagreement * evidence_gap * impact
```

Spend deep reasoning on the hotspots most capable of changing the decision.

### Crystal

Stop broad search.

Run:

- exact replay;
- fresh independent verifiers;
- red-team verification.

### Glass

Do not "think harder" in the same context.

Use:

- fresh context;
- new models/prompts;
- new mutations;
- changed strategy;
- bounded perturbation.

### Jammed

Stop feeding the queue.

Use compute for:

- cleanup;
- memory compaction;
- finishing already-started verification;
- recovery.

### Executable population manifests

The experimental `swfactory.population_manifest` layer turns a `SwarmPlan` into an immutable
provider-neutral task set. It is the bridge between "allocate three verifier trajectories" and a
runtime adapter that can actually launch those trajectories.

Each task binds:

- swarm-plan digest;
- search-provenance digest;
- role and compute tier;
- context policy;
- temperature;
- diversity axes;
- disagreement hotspots;
- a deterministic variant digest.

Replicas are different executable questions, not copies with different names. Independent
verification tasks must use fresh or frozen context; they may not inherit another trajectory's
conversation state.

Providers return `BehaviorReceipt` records containing gauge-dependent runtime metadata plus a
behavior signature and optional candidate/evidence digests. The reducer computes:

- effective independent search;
- mean behavioral correlation;
- unique candidate count;
- candidate disagreement;
- independent-verifier answers;
- total cost and duration.

Those measurements may change the next search allocation. They remain evidence about search, not
authority over durable reality. The manifest explicitly names Airflow as the scheduler and contains
no credential, Cell mutation, publication or promotion capability.

A generic trusted-backend HTTP population adapter now realizes this manifest contract, but that is
not a blanket claim of vendor support. A provider/model/runtime combination becomes operationally
supported only after its gateway, credential source, quotas, timeout semantics and retained receipts
are live-qualified.

### Deterministic provider binding

The experimental `swfactory.provider_binding` layer consumes those provider-neutral tasks plus a
human/operator-supplied allowlist of concrete provider, model, runtime, prompt, context, mutation,
verifier and attack-surface choices.

Each population task already carries deterministic per-axis coordinates. Binding maps each coordinate
into the corresponding allowlisted choice, producing a `BoundPopulationTask` without changing the
task id or variant digest. The same manifest and choice set therefore replay to the same concrete
provider configuration.

A declared required axis with no available choice fails closed. An undeclared axis is not silently
invented. Provider binding is still search-only data: it does not invoke a model, schedule a task,
mint a credential, mutate a Cell, publish, merge, approve or promote. Airflow remains the lifecycle
scheduler and authority remains outside the provider mapping.

### Managed population execution closes the in-stage runtime seam

The repository now has both an offline operator loop and an experimental managed in-stage executor:

```text
research-adapt --json
  -> population-bind
  -> Airflow schedules one lifecycle task
  -> PopulationExecutor fans out bound search trajectories inside that task
  -> BehaviorReceipt[] + PopulationTelemetry
  -> canonical retained PopulationExecutionReport
  -> next research-adapt --population-telemetry ...
```

`swfactory.population_execution` does not become another lifecycle scheduler. It behaves like the
existing bounded work executor: Airflow has already decided *when* the task exists; the executor only
runs bounded search trajectories inside that task and fans their evidence back in deterministically.

Before execution it revalidates the population manifest and provider binding. During fan-in it
rejects receipt identity drift in provider/model/runtime, refuses two verifier tasks that claim
independence while collapsing to the same concrete verifier binding, converts provider exceptions
into failed search receipts, and derives population telemetry automatically. Reports are written as
canonical JSON with a digest and can be reloaded and revalidated after process exit.

The managed runtime now reaches a generic trusted provider gateway through the factory backend. The
Airflow worker holds only `SWF_BACKEND_URL` and `SWF_BACKEND_TOKEN`. For each bound trajectory the
backend rechecks Cell epoch, policy and authoritative Airflow ownership, journals the paid model call
as the canonical `model_call` resource, binds the operation intent to the exact invocation and
adapter configuration, and—only when that adapter declares an explicit dotted capability—mints and
redeems an attempt-bound credential lease inside the backend process.

Provider adapter configuration carries **credential environment-variable names, never values**.
The raw credential is handed directly from lease redemption to the trusted adapter callback, then
the lease is revoked. It never enters XCom, the population manifest, the provider binding, the
execution report, the candidate artifact, the operation receipt, or the evidence chain.

Provider output is retained in a backend-owned artifact whose digest binds both the invocation
digest and the raw output hash. The journal stores only the artifact/receipt identities. An
ambiguous timeout is therefore **in doubt**, not replayable: a redelivered Airflow task reads the
journal instead of blindly spending on the provider again. Exact committed redelivery replays the
sanitized receipt without another model call.

The generic adapter is a contract and gateway, not a support claim for every vendor. Concrete
provider endpoints remain experimental until live-qualified. The runtime still cannot create
Airflow lifecycle work, publish, approve, merge, or promote.

### Measured marginal information now controls the next search envelope

The experimental `swfactory.adaptive_information` controller closes the next loop without adding a
scheduler. It reads the retained `PopulationExecutionReport` and, when available, the exact prior
`PopulationManifest`. Each measured lane is reduced to answer rate, candidate/evidence yield,
effective independent search, pairwise correlation, disagreement pressure and compute-tier cost.

The controller applies the same cognitive-harness proxy already declared elsewhere:

```text
marginal_information = (1 - correlation) * expected_information / compute_units
```

The resulting `InformationBudgetDecision` may only narrow the human-declared outer `SwarmBudget`.
It can cap redundant measured roles, reserve one independent verifier when disagreement remains
high, switch the next search posture to `measure` or `perturb`, or recommend `drain` when the
population is settled and no measured lane has positive marginal value. A cancelled population
drains before replacement search is created.

Managed build stages retain `population-information-budget.json` beside the execution report.
Retries reload that exact decision by digest rather than recomputing mutable defaults. Recursive
planning embeds the decision and its digest in the next round receipt; the swarm-plan reason also
binds the information-budget digest, so descendant candidate identity reflects *which retained
evidence justified the compute allocation*.

This remains search-only. It cannot widen agents, parallelism, deep compute, exact replay or compute
units beyond the declared envelope, and it cannot schedule Airflow, mint credentials, weaken
verification, mutate Cells, publish, approve, merge or promote.

## 12. Local crystals

A local crystal is a frozen exact candidate worth verification.

It binds:

- candidate digest;
- source digest;
- recipe digest;
- policy digest;
- evidence digest.

It also records:

- coherence;
- evidence completeness;
- verifier disagreement;
- independent verifier count;
- contradictions.

A crystal means **verify this exact thing**.

It does not mean:

- approved;
- publishable;
- merged;
- promoted.

## 13. System 2 destroys entropy by measurement

System 2 is an architecture of deliberation:

- freeze candidate generation;
- identify disagreements;
- run discriminating tests;
- replay in clean environments;
- challenge independently;
- red-team;
- compare invariant evidence;
- eliminate degrees of freedom;
- converge deterministically.

Computation becomes epistemology only when the observation is tied to the exact world that produced it.

## 14. Gauge invariance

The factory distinguishes representation from reality.

Gauge-dependent examples:

- model;
- prompt;
- agent identity;
- reasoning style;
- explanation;
- token count;
- trajectory name;
- branch name.

Gauge-invariant examples:

- Cell identity;
- epoch;
- candidate digest;
- source digest;
- recipe digest;
- policy digest;
- evidence digest;
- artifact digest;
- external-effect digest.

Promotion can depend only on the second class.

Equivalent worlds may be collapsed deterministically.
Inequivalent worlds still need evidence.

## 15. Candidate evidence and replay capsules

A successful candidate should leave enough retained information to reconstruct what happened.

Existing primitives already include:

- source snapshots;
- frozen candidate refs;
- candidate evidence bundles;
- execution recipes;
- test/review evidence;
- campaign fan-in manifests.

The target replay capsule binds:

```text
source
+ execution recipe
+ executable/environment identity
+ stdout/stderr
+ artifact digests
+ measurement receipts
+ candidate digest
```

The capsule is stronger than a prose claim that "the tests passed".

## 16. Evidence fan-in, not winner-pointer trust

A recursive round may use one evidence-selected candidate as the parent of the next experiment.

Before that decision becomes lineage, the factory rebuilds the campaign decision from immutable
sibling evidence.

The next question therefore descends from a verified parent decision, not merely an in-memory
winner pointer.

## 17. Exploration selection is not promotion selection

Autonomous search may choose:

> which candidate should we investigate next?

That is not the same question as:

> which candidate may alter durable reality?

The first can be automated under an exploration budget.

The second remains behind explicit human/parent authority.

## 18. Credentials are capabilities

Raw provider credentials never belong in worker/XCom/artifact state.

Credential authority is projected as short-lived leases bound to:

- capability;
- operation;
- Cell;
- epoch;
- attempt;
- task/process;
- expiry.

Possession of an old lease is not proof of current authority.

## 19. Mutation is an observed state machine

External effects are not treated as ordinary retriable function calls.

Once an effect may have happened, the local state is unknown until observation proves one of:

- committed;
- definitely absent;
- ambiguous;
- divergent;
- refused.

The factory observes before replay.

This rule applies especially to publication and restored backups.

## 20. Self-hosting does not allow self-authorization

A factory worker may improve ordinary factory code.

It may not modify, in the same run that benefits from the modification:

- its own governing policy;
- required CI;
- sandbox confinement;
- blueprints;
- DAG lifecycle;
- authority kernel;
- support claims;
- future-factory laws.

An agent may not widen its own cage in the PR that uses the wider cage.

## 21. Self-improvement is an ordinary experiment

The factory may propose new versions of itself.

Lifecycle:

```text
current controller
      |
      v
candidate controller worlds
      |
      v
shadow evaluation
      |
      v
independent verification
      |
      v
adoptable candidate
      |
      v
ordinary external promotion authority
```

"Adoptable" is intentionally not "adopted".

Recursive improvement may alter cognition.
It cannot alter its own authority boundary.

## 22. Memory crystallization

Observations become durable knowledge gradually:

```text
observation
 -> trace
 -> correlated pattern
 -> candidate belief
 -> memory crystal
```

Independent recurrence strengthens memory.

Contradiction prevents automatic crystallization.

This applies both to facts about product behavior and laws about the factory's own search process.

## 23. Context is a controlled resource

Context policies include:

- fresh;
- inherit;
- compact;
- frozen.

The repository already has evidence-preserving context mechanisms:

- ObservationPack;
- exact recall handles;
- local evidence-preserving reducer;
- packed review diffs;
- online context compaction.

The future fused-editing path may reduce turns only if it executes an approved validation recipe
and preserves mutation + validation evidence.

## 24. Dissipation is part of healthy cognition

An ordered factory exports waste.

Track:

- discarded worlds;
- reclaimed contexts;
- cleaned sandboxes;
- cancelled retries;
- retired stale memories.

Leaving every failed trajectory alive is not learning. It is cognitive debt.

## 25. Turbo, snapshots and caches are catalysts

Caches answer:

> have these inputs already produced these outputs?

Authority answers:

> are these exactly the bytes/evidence that may perform this effect?

A cache hit can avoid computation.
It can never satisfy an approval, fence, mutation or promotion gate.

The same rule applies to forkable VM snapshots.

## 26. Human attention is scarce compute

Routine success should fade into the background.

Humans should primarily see:

- genuine ambiguity;
- verifier disagreement that survives deep measurement;
- authority requests;
- glass;
- jammed conditions;
- novel security events;
- policy changes;
- self-improvement adoption.

## 27. Truthful support claims

Architecture aspiration and production support are intentionally different concepts.

New recursive-search and swarm mechanisms remain **experimental** until real managed-run evidence
supports them.

The capability inventory is the source of truth for support status.

A feature existing in Python is not evidence that the production path exercises it.

## 28. Graduation path

The shortest path from today's repository to the full end state is:

1. emit phase/swarm telemetry from real managed runs;
2. expose search laws, phase, provenance receipts and compute allocation in `swf`;
3. plug real heterogeneous model/runtime providers into population lanes;
4. add provider-backed forkable snapshots without transferring authority;
5. build complete replay capsules;
6. learn search policies offline from retained campaigns;
7. shadow-run candidate controllers;
8. graduate proven contracts from Python into Rust;
9. keep one explicit authority boundary throughout.

The goal is not an autonomous system that can do anything.

The goal is an autonomous system that can **search extremely broadly, learn how to search better,
prove exactly what it found, and still be unable to silently grant itself more power.**
