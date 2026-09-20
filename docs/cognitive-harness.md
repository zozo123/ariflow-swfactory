# Cognitive Harness: Executable Mind for the Dark Software Factory

## Thesis

The software factory is not merely a workflow that calls agents.

Assume the whole system runs **inside an AI harness**. Then the harness is the organism and the
factory is its cognitive dynamics:

```text
intent / environment
        |
        v
+-----------------------------+
|        AI HARNESS           |
|                             |
|  System 1: create entropy   |
|  System 2: measure/collapse |
|  memory: consolidate        |
|  phase: metacognition       |
+-------------+---------------+
              |
              v
      RUST REALITY BOUNDARY
 identity / epoch / policy /
 evidence / capability / effect
              |
              v
         durable reality
```

The central law is:

> **The mind may be stochastic. Reality may not.**

This architecture deliberately permits the cognitive interior to be exploratory, redundant,
contradictory and nondeterministic while making the external authority boundary narrower and more
deterministic.

## The organism

### AI harness

The harness supplies goals, tools, context, memory, models and the environment in which hypotheses
can become executable experiments.

### Airflow: temporal nervous system

Airflow owns **time and lifecycle**, not truth:

- durable continuation;
- mapping and fan-out;
- retries and timeouts;
- waiting/sleeping;
- human interrupts;
- admission and bounded concurrency;
- fan-in.

It answers *when cognition exists and may run*, not *which belief is true*.

### Rust: constitutional / reality kernel

Rust increasingly owns facts the mind must not hallucinate:

- durable identity;
- Cell epoch;
- capability and lease identity;
- exact candidate/source/recipe/policy/evidence digests;
- recovery and mutation contracts;
- deterministic convergence contracts;
- the request that an exact candidate cross into a durable effect.

### Python: experimental cortex

Python remains intentionally permissive for:

- new search strategies;
- phase signals;
- stochastic fields;
- model ensembles;
- evals;
- memory experiments;
- annealing schedules;
- offline learning.

A successful experimental mechanism can later graduate into a stable Rust domain contract.

```text
idea -> Python experiment -> retained evidence -> stable contract -> Rust invariant
```

## System 1: hot stochastic cognition

System 1 is not "a smaller model". It is a **generative regime**:

- many executable hypotheses;
- different models/prompts/roles;
- repair/rethink/scratch;
- independent worktrees/sandboxes;
- cheap measurements;
- deliberate decorrelation.

It corresponds mostly to gas/liquid dynamics.

```text
problem
  +-- world A: minimal repair
  +-- world B: invariant rethink
  +-- world C: scratch implementation
  +-- world D: security skeptic
  +-- world E: historical analogy
```

The purpose is coverage of useful state space, not agreement.

## System 2: measurement and entropy destruction

System 2 is an **architecture of deliberation**, not merely a "smart model":

- freeze candidate generation;
- identify disagreement;
- execute exact tests;
- challenge candidates independently;
- replay in fresh environments;
- compare gauge-invariant evidence;
- eliminate degrees of freedom;
- deterministically converge.

System 1 creates useful entropy. System 2 destroys it using measurement.

## Metacognition: the phase controller

The phase controller does not answer the task. It answers:

> **What kind of thinking should the system be doing now?**

| Observed phase | Default cognitive posture |
| --- | --- |
| gas | diverge through isolated worlds |
| liquid | coordinate mobile specialist worlds |
| critical | stop widening; measure |
| crystal | verify one exact candidate |
| glass | inject one bounded fresh perturbation |
| jammed | stop creating work and drain |
| anneal | transition mode: prune worlds while verifier independence rises |

This is the factory's metacognitive thermostat.

## Gauge semantics

Internal AI cognition has many descriptions that should not affect reality.

### Gauge-dependent quantities

Examples:

- model/vendor;
- prompt wording;
- agent identity;
- reasoning style;
- explanation;
- token count;
- branch/trajectory name;
- runtime name.

These may change while the externally meaningful computation remains identical.

### Gauge-invariant observables

Promotion-grade evidence cares about:

- exact candidate digest;
- exact source digest;
- exact execution recipe;
- exact policy digest;
- evidence digest;
- artifact digest;
- external-effect digest;
- Cell identity and epoch.

Therefore:

> **Promotion may depend only on gauge-invariant observables.**

### Gauge fixing

During search there may be multiple representation-equivalent worlds. `gauge_fix` groups worlds
that have the same invariant observable fingerprint and selects one deterministic canonical
representation. It **does not** decide between inequivalent candidates.

```text
many representations
       |
       v
equivalence classes
       |
       v
canonical representation
       |
       v
one exact evidence-bearing candidate
```

## Many-world executable cognition

A sandbox is not just infrastructure. It is an epistemic instrument.

Instead of asking a model to imagine whether an idea works:

```text
hypothesis
   |
   v
disposable computer/world
   |
   v
experiment
   |
   v
measurement receipt
   |
   v
updated belief
```

A `WorldCandidate` binds a trajectory to source/recipe/policy/candidate/evidence identities.
Worlds are disposable. Durable identity and evidence are not.

This makes computation part of reasoning itself.

## Measurement

A `MeasurementReceipt` records one exact observation over one world:

- tests;
- static analysis;
- replay;
- performance;
- security;
- formal verification;
- human observation.

A world cannot be treated as independently verified unless at least one independent measurement is
bound to that exact world.

This is the basis for turning model belief into evidence.

## Jev and stochastic fields

Jev is modeled as a **stochastic field**, not an authority.

The factory declares the allowed values:

```text
strategy in {repair, rethink, scratch}
```

An oracle may alter:

```text
P(repair), P(rethink), P(scratch)
```

It may not:

- introduce an undeclared authority-bearing option;
- merge;
- approve;
- publish;
- bypass policy.

So an oracle changes the probability distribution over worlds, not the authority of a world.

## Heterogeneous ensembles

"The model" is replaced by an ensemble:

- frontier models;
- cheap models;
- specialized verifiers;
- static analyzers;
- fuzzers;
- formal tools;
- tests;
- humans.

The key quantity is **independence**, not count.

The current experimental contract measures Jaccard correlation over retained behavior signatures:

```text
C_ij = overlap(signature_i, signature_j)
diversity = 1 - mean(C_ij)
```

A tenth highly correlated agent may be less useful than one independent critic.

A simple marginal-value proxy is:

```text
information_value = (1 - correlation) * expected_information / cost
```

## Cognitive timescales

| Layer | Typical timescale | Examples |
| --- | --- | --- |
| reflex | milliseconds-seconds | cache, lint, grep, admission, cleanup |
| System 1 | seconds-minutes | parallel hypotheses, cheap experiments |
| System 2 | minutes-hours | replay, verification, convergence |
| consolidation | hours-days | memory promotion, strategy/process learning |
| authority boundary | deliberate | exact effect request |

This allows the harness to think at multiple speeds without confusing a fast heuristic with a
durable decision.

## Memory crystallization

Memory undergoes its own phase progression:

```text
observation
   -> trace
   -> correlated pattern
   -> candidate belief
   -> memory crystal
```

Repeated independent evidence can crystallize durable knowledge. Contradiction keeps a belief in the
candidate stage.

Context itself can be:

- fresh: break inherited assumptions;
- retained: keep useful trajectory state;
- compacted: coarse-grain microscopic history while retaining durable evidence.

## Human attention as exception economics

Human attention is the most expensive resource in the dark factory.

Routine successful search should disappear into the plant. Humans should primarily see:

- genuine ambiguity;
- verifier disagreement;
- glass/jammed conditions;
- unusual security events;
- policy/control-plane changes;
- authority requests.

The cognitive contract therefore emits `routine`, `exception`, or `authority-boundary`
attention classes.

## Recursive self-improvement

The factory may propose changes to its own cognition:

- new prompts;
- new phase thresholds;
- new agent topologies;
- new evaluators;
- new annealing schedules;
- new controllers.

But a self-improvement proposal is just another bounded experiment:

```text
current controller (baseline)
   +-- candidate controller A
   +-- candidate controller B
   +-- candidate controller C
            |
            v
       shadow evidence
            |
            v
   independent verification
            |
            v
      adoptable candidate
            |
            v
 ordinary human/policy adoption
```

The candidate controller can become **adoptable**. It cannot adopt itself.

## Dissipative cognition

The factory remains ordered by continuously exporting cognitive waste:

- rejected candidate worlds;
- obsolete context;
- dead sandboxes;
- cancelled retries;
- stale memories.

`DissipationSnapshot` makes this visible rather than treating cleanup as incidental plumbing.

This is the practical software analogue of a dissipative structure: continuous compute/energy flows
through the system while failed microscopic states are discarded and useful macroscopic order is
maintained.

## Authority is conserved

The cognitive layer can produce an `AuthorityRequest`. It cannot produce an authority grant.

An authority request binds:

```text
Cell
epoch
candidate
source
recipe
policy
evidence
requested effect
```

and declares:

```text
authority = request-only
requires  = rust-authority-kernel
```

This is the architectural conservation law:

> **Compute may multiply. Authority may not.**

No number of agents, votes, model confidence scores or stochastic samples can manufacture a
capability that was not granted by policy.

## Full loop

```text
human intent
    |
    v
AI harness
    |
    +--> reflex
    |
    +--> SYSTEM 1
    |      many worlds
    |      stochastic fields
    |      heterogeneous ensemble
    |
    v
phase / metacognition
    |
    +--> glass -> perturb
    +--> jammed -> drain
    +--> critical -> measure
    |
    v
SYSTEM 2
measurement / replay / adversarial verification
    |
    v
gauge fixing + deterministic convergence
    |
    v
crystal
    |
    v
AuthorityRequest
    |
    v
RUST REALITY BOUNDARY
identity / epoch / policy / evidence / capability
    |
    v
durable effect
    |
    v
memory / consolidation / process learning
    |
    +---------------------------> next cognition
```

## Code surfaces

Python experimental cortex:

- `swfactory.phase_control`
- `swfactory.cognitive_harness`

Rust domain/reality-facing contract:

- `swf_domain::phase_control`
- `swf_domain::cognitive_harness`

Governance:

- `config/phase-control.yaml`
- `config/cognitive-harness.yaml`
- `factory.toml` protected control-plane paths

The cognitive modules are deliberately pure. They do not schedule Airflow, mutate GitHub, redeem a
credential, mark evidence verified, or promote a candidate.

That separation is the point.
