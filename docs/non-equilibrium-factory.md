# Non-equilibrium Software Factory Control Doctrine

The ambition is mission-critical, aerospace-class systems rigor. This repository is **not** claiming
NASA affiliation, NASA certification, or that software is literal thermodynamic matter. The rule is
stronger and more useful: borrow mathematics from statistical mechanics, non-equilibrium physics,
complex systems, nuclear/rare-event physics, and molecular regulatory networks **only where the
mapping produces measurable variables, falsifiable predictions, or safer control decisions**.

Apache Airflow remains the only lifecycle scheduler. Factory Cells remain the durable issue x target
identity and epoch-fencing authority. Physics-informed controllers may recommend; they never become
a second scheduler, a second source of truth, or a way around security/evidence gates.

## 1. The factory is a driven many-current system

Do not reduce factory health to one queue length, one throughput number, or one latency percentile.
Observe a vector of currents `J_i`, each with its own velocity and acceleration:

- **work current**: accepted Cells / time;
- **compute current**: sandbox CPU/GPU-seconds / time;
- **mutation current**: externally committed effects / time;
- **publication current**: verified repository publications / time;
- **evidence current**: sealed evidence events / time;
- **cleanup current**: reclaimed resources / time;
- **cost current**: attributed spend / time;
- **uncertainty current**: operations entering/leaving in-doubt states / time;
- **security current**: authorized/refused capability requests / time.

For every current, retain at least `J_i`, `dJ_i/dt`, and its conjugate control pressure/affinity `X_i`
when one is defined. A burst with zero acceleration is different from an accelerating burst. Two
systems with identical throughput but opposite cleanup/evidence currents are not in the same state.

The control layer uses the standard non-equilibrium bilinear diagnostic

```text
sigma_proxy = sum_i J_i X_i
```

as an **entropy-production proxy**, not literal joules per kelvin. Persistent negative values are a
model/sign-convention alarm: inspect the chosen affinities rather than pretending the second law was
violated by a CI system.

Implementation: `src/swfactory/non_equilibrium.py`.

## 2. Max entropy is a default for uncertainty, not an excuse for randomness

When several allocations satisfy known constraints and there is no evidence favoring one, use the
Jaynes maximum-entropy principle: encode what is known and avoid inventing information.

For discrete alternatives with effective cost `E_i`, use Gibbs/Boltzmann weights

```text
p_i = exp(-beta E_i) / Z
```

where `beta` is a **dimensionless control parameter**. Low beta keeps broad exploration; high beta
concentrates on lower-cost states. This is used for ensemble/model weighting and can also be used for
bounded candidate selection. It does not replace hard constraints, authorization, or Airflow.

Relevant lineage: Boltzmann and Gibbs for statistical ensembles; E. T. Jaynes for maximum entropy as
inference under constraints.

## 3. Many models run in parallel; the mixture is the product

A single worldview is too brittle. Evaluate the same durable factory snapshot with independent
models and mix their recommendations:

1. **flux model** - throughput, saturation, acceleration;
2. **entropy model** - exploration versus collapse;
3. **stability model** - defect pressure and crystallization readiness;
4. **recovery model** - stale writers, ambiguity, cleanup debt, metastability;
5. **evidence model** - irreversibility, provenance, missing proof;
6. **security model** - trust-boundary pressure and refusals;
7. **cost model** - resource/free-energy-like pressure;
8. **rare-event model** - fast accelerations, barrier crossing, tail risk.

No model receives unilateral authority. Each emits a **pitch**: action utilities plus an
 evidence-derived reliability. The mixer preserves non-zero participation from the ensemble, applies
maximum-entropy weighting, and then incorporates bounded pairwise couplings.

This is intentionally similar to a large protein/regulatory network:

- activation and inhibition are local pairwise couplings;
- multiple pathways can produce the same phenotype/action;
- cooperativity can amplify a weak signal only when several conditions agree;
- negative feedback damps runaway throughput;
- positive feedback can lock in stabilization after sufficient evidence;
- redundancy allows degraded operation when one model is weak;
- no protein/model is the organism/control plane by itself.

Couplings are advisory. They never mutate durable state directly.

## 4. Phase vocabulary

The factory can occupy qualitatively different dynamical regimes. Use phase labels to change control
biases, never lifecycle authority.

| Phase | Software-factory interpretation | Preferred response |
| --- | --- | --- |
| `gas` | very high dispersion, weak coupling, many independent exploratory states | increase structure; stabilize useful clusters |
| `liquid` | high mobility with adaptive structure | normal productive mode; preserve flow |
| `critical` | large accelerations near a transition boundary | observe carefully; verify and bound rates |
| `crystal` | low entropy, low defects, repeatable structure | verify, seal evidence, promote |
| `glass` | low mobility but still disordered; metastable local minimum | perturb/recover; avoid mistaking stasis for stability |
| `jammed` | blockage or cleanup/resource debt dominates | stop feeding pressure; reclaim and recover |

### Hysteresis

Do not flip control policy on every noisy boundary crossing. Phase decisions include hysteresis. A
liquid/crystal system near the boundary retains its previous phase until evidence moves far enough.
This is the same reason magnetic/material phase systems remember their path: state depends on
history, not only the instantaneous scalar metric.

### Order parameters

A phase label must be derived from observable order parameters. Current implementation uses a
bounded combination of configurational entropy, failures, blocked work, evidence gaps, security
refusals, mobility, acceleration, and cleanup debt. New order parameters require evidence and tests.

Relevant lineage: Landau-style order parameters and phase transitions; glassy dynamics and
metastability for disordered low-mobility states.

## 5. Nucleation: stabilization should have a barrier

A new architecture should not crystallize from one lucky result. Treat promotion/stabilization as a
nucleation problem: there is a driving force toward the new phase and a surface/interface penalty
for changing contracts, migrations, operators, docs, and rollback paths.

The control module exposes a dimensionless classical-nucleation barrier proxy

```text
barrier ~ surface_penalty^3 / driving_force^2
```

and a Kramers/Arrhenius-like crossing probability

```text
P ~ exp(-barrier / T_eff)
```

where `T_eff` is exploration/noise, not kelvin. This encourages sustained evidence before expensive
fan-in while still allowing a high-driving-force improvement to nucleate quickly.

Relevant lineage: classical nucleation theory and Kramers barrier crossing.

## 6. Jarzynski and Crooks: compare transition protocols, not snapshots

For repeated controlled transitions between comparable factory states, record a scalar effective
work/cost per trajectory. Then the Jarzynski estimator

```text
Delta F_eff = -(1/beta) log <exp(-beta W)>
```

can compare protocols using the full non-equilibrium work distribution instead of only the mean.
Crooks' relation provides the corresponding forward/reverse log-ratio diagnostic.

Guardrails:

- only compare repeated trajectories with the same declared protocol and observable definition;
- retain all samples/evidence, especially rare low-work trajectories that dominate the exponential
  average;
- report uncertainty; small-sample Jarzynski estimates are biased and tail-sensitive;
- call the quantity **effective** free energy/cost, never literal thermodynamic free energy.

Relevant lineage: Christopher Jarzynski and Gavin Crooks.

## 7. Onsager/Prigogine ideas: linear response first, nonlinear control when required

Near a stable operating point, test whether currents respond approximately linearly to small
control-pressure changes:

```text
J_i ~= sum_j L_ij X_j
```

The response matrix `L` is empirical. Measure it; do not invent coefficients. Reciprocity can be a
useful hypothesis when symmetry assumptions are defensible, not a default assertion for software.

Far from equilibrium, expect nonlinear response, bifurcations, hysteresis, and multiple attractors.
Entropy production and dissipative-structure ideas from non-equilibrium thermodynamics are useful
for asking whether the factory is maintaining useful ordered flow by continuously exporting
waste/debt (cleanup, invalid branches, failed sandboxes) rather than accumulating it.

Relevant lineage: Lars Onsager and Ilya Prigogine.

## 8. Large deviations and nuclear/rare-event physics

Average-case control is insufficient for destructive external mutations. Treat rare failures as
first-class trajectories.

Useful transfers from nuclear and rare-event physics include:

- **barrier penetration/crossing**: model the probability of escaping a metastable failure mode;
- **branching/criticality**: reproduction number of retries, child factories, or failure cascades must
  stay subcritical unless intentionally bounded;
- **chain-reaction containment**: one bad mutation must not trigger unbounded retries/publications;
- **rare-event sampling**: weight tail trajectories explicitly when estimating catastrophic risk;
- **critical slowing down**: rising correlation/recovery time near a transition is an early warning,
  not proof of safety.

A software factory should be designed so a failure cascade has a reproduction factor below one after
fencing, retry budgets, admission control, and cleanup are applied.

## 9. Protein-system principles

The protein-network analogy is useful because it naturally resists central-monolith design.

Apply these patterns where measurable:

- **allostery**: a signal in one subsystem changes the effective response of another without handing
  it direct mutation authority;
- **cooperativity**: require several weak signals before a strong promotion/recovery response;
- **negative feedback**: throughput pressure raises throttling/cleanup drive;
- **positive feedback with saturation**: repeated clean evidence can increase stabilization drive but
  must saturate;
- **degeneracy**: different mechanisms can provide the same resilience property;
- **modularity**: local failure should not dissolve all global structure;
- **homeostasis**: target operating ranges rather than a single maximum-throughput point.

Do **not** turn this into an opaque neural controller. Every coupling and pitch must be inspectable,
versioned, bounded, and evidence-linked.

## 10. Quantum effects: explicit threshold for use

Classical stochastic control is the default and is sufficient for the current factory. Do not add
"quantum" vocabulary for novelty.

Quantum methods are justified only when at least one is true:

- execution actually uses a quantum device and decoherence/error models materially affect results;
- a quantum algorithm is being benchmarked against a classical baseline for a defined optimization
  or sampling problem;
- a mathematically quantum-inspired representation is demonstrably better on retained evidence.

If none applies, use classical probability, Markov/non-Markov stochastic processes, control theory,
large-deviation methods, and statistical mechanics.

## 11. Non-negotiable software invariants

Physics-informed control sits **below** these invariants:

1. Apache Airflow is the only lifecycle scheduler.
2. Factory Cell identity is durable: issue x target.
3. Every external mutation carries `(cell_id, epoch, operation_key)`.
4. Epochs fence stale writers.
5. In-doubt effects are observed before replay; no blind retry.
6. Security capability and tenant boundaries fail closed.
7. Publication authority remains outside disposable sandboxes.
8. Evidence is retained, redacted, hash-verifiable, and tied to the same mutation identity.
9. Inner workgraphs are bounded; they are not second schedulers.
10. Promotion requires evidence and explicit authority; child factories cannot self-promote.

`src/swfactory/core_capabilities.py` is the cross-cutting execution seam for these invariants.
`src/swfactory/non_equilibrium.py` is the advisory physics/ensemble layer.

## 12. Acceptance standard

A new physics-inspired idea belongs in production only when it has all of:

- an explicit state variable or observable;
- units or declared dimensionless normalization;
- a deterministic implementation;
- a falsifiable expected behavior;
- retained evidence or a reproducible benchmark;
- bounded failure behavior;
- no new scheduling/persistence authority;
- tests for edge cases and limiting cases;
- an operator-visible explanation of why the recommendation was produced.

That is the standard: not physics-flavored naming, but a measurable non-equilibrium control system
whose many models, many currents, many accelerations, and many phases converge toward a safer and
more capable software factory.
