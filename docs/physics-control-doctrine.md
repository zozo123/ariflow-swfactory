# Physics control doctrine

This document defines the permanent modeling doctrine for swfactory's high-concurrency control plane.

**Rigor target:** mission-critical / flight-grade engineering. References to "NASA-grade" mean the desired level of explicit assumptions, evidence, fault containment, reproducibility, reviewability, and conservative release gating. They do **not** claim NASA affiliation, certification, endorsement, or use.

The physics is a modeling toolbox, not a source of authority. Apache Airflow remains the **only lifecycle scheduler**. Durable Factory Cell identity, positive epoch fencing, canonical policy, publication authority, and human/repository release policy remain the only mechanisms allowed to cause external state change.

## 1. Non-negotiable invariants

1. **One scheduler:** only Airflow owns lifecycle scheduling, retries, mapped execution, timeouts, and stage ordering.
2. **One durable work identity:** issue × target maps to one Factory Cell lineage; disposable compute never becomes the work identity.
3. **Positive epoch fencing:** every mutating operation is bound to `(cell_id, epoch, operation_key)` and stale epochs fail closed.
4. **One publication authority:** models cannot push branches, open/merge PRs, approve gates, or promote releases.
5. **Models are advisory:** a model may measure, rank, recommend, refuse, or request throttling; it may never create a second state machine.
6. **Evidence before promotion:** every recommendation that affects admission, throttling, fencing, or release must retain the measured signals and the deterministic decision.
7. **Entropy may grow before it shrinks:** liquid development may intentionally fan out implementations. Final crystallization is a separate explicit one-way gate.
8. **No metaphor outranks a contract:** if a physics analogy conflicts with a tested software invariant, the software invariant wins.

## 2. State is a many-field, many-current system

A busy factory is not described by one queue or one scalar "load". It carries many simultaneous currents with different characteristic speeds and response times:

- work arrival / completion current;
- evidence production / verification current;
- external mutation current;
- retry and cancellation current;
- artifact / cache current;
- cost and model-token current;
- approval / human-attention current;
- provider-capacity current;
- cleanup / reconciliation current;
- branch creation / annihilation current.

Each current may have its own flux `J`, velocity `v`, acceleration `dv/dt`, gradient or generalized force `X`, diffusion coefficient, latency, and saturation boundary. A control decision therefore uses a vector or field of observables rather than pretending one global queue depth is sufficient.

Operationally, the canonical sample lives in `src/swfactory/physics_mixture.py` and records currents plus phase, entropy, work, occupancy, branching, many-body, and barrier observables.

## 3. Parallel models, deterministic fan-in

No individual model owns truth. swfactory evaluates several simplified models **in parallel** and combines their signals deterministically.

### 3.1 Hydrodynamic / transport model

Use for queue flow, propagation, saturation, burst behavior, transport locality, diffusion, convection, shocks, and backpressure.

Useful concepts:

- continuity / conservation;
- pressure gradients;
- viscosity / hysteresis;
- Reynolds-like regime changes;
- compressible-style shock/load shedding;
- diffusion versus convection;
- boundary layers and trust boundaries;
- many characteristic velocities and accelerations.

Do **not** solve literal Navier–Stokes equations unless a measured control problem actually requires that complexity. The analogy is valuable only when it improves a bounded deterministic control law.

### 3.2 Boltzmann–Gibbs / maximum-entropy model

Use for uncertainty, ensembles of alternatives, population weighting, and the distinction between useful exploration entropy and accidental coordination entropy.

Core ideas:

- Boltzmann weights for comparing bounded alternatives;
- Gibbs free-energy-like tradeoffs among value, cost, risk, and entropy;
- partition functions as normalized rankings, never release authority;
- maximum entropy as the least-committal state consistent with known constraints;
- order parameters as measurable authority/convergence signals.

Maximum entropy is a **reasoning prior**, not permission to randomize production behavior.

### 3.3 Non-equilibrium statistical mechanics

Most real factory operation is driven and non-equilibrium: work arrives, resources are consumed, policies change, retries dissipate effort, and external mutations are irreversible unless explicitly compensated.

Retain at least these observables when applicable:

- entropy production proxy `σ = Σ J_i X_i`;
- dissipated work and hysteresis;
- relaxation time and response lag;
- fluctuation–dissipation response;
- linear-response / Onsager-like coupling near stable regimes;
- rare-event and barrier-crossing rates away from them.

For repeated non-equilibrium work samples, swfactory may use the Jarzynski relation as an **estimator analogy**:

`ΔF = -(1/β) log < exp(-β W) >`

and the Crooks forward/reverse log-ratio proxy:

`log(P_F/P_R) = β (W - ΔF)`

The implementation uses numerically stable log-mean-exp. These estimators never make release decisions on their own; they contribute evidence about dissipation, irreversibility, and whether a proposed transition is well characterized.

### 3.4 Phase transitions, criticality, nucleation, and crystallization

Development phases are explicit operational states:

- **GAS:** high exploration, weak coupling, low durable commitment;
- **LIQUID:** many interacting implementations with fluid interfaces;
- **MIXED:** competing regimes coexist; interfaces and authority overlap require careful control;
- **SUPERCRITICAL:** stress regime; high coupling/load makes ordinary local intuition unreliable;
- **CRYSTAL_CANDIDATE:** entropy is low enough and order/evidence high enough to propose one stable structure.

Useful concepts include Landau/order-parameter reasoning, hysteresis, metastability, spinodal-like instability, critical slowing down, nucleation barriers, Kramers-like escape over barriers, domain walls, defects, and surface tension between incompatible contracts.

**Crystallization is never automatic.** A model may emit only `crystallize_candidate`; the explicit release gate must still be open and all stronger safety signals must be clear.

### 3.5 Reaction kinetics and nuclear-chain criticality

Use only for **software branching cascades** such as retries spawning retries, event amplification, fan-out storms, recursive factories, or failure propagation.

A software multiplication factor `k` is interpreted qualitatively:

- `k < 1`: subcritical; cascades decay;
- `k ≈ 1`: critical; small perturbations persist;
- `k > 1`: supercritical; cascades grow and require hold/shedding/fencing.

This is an operational branching-process analogy, not nuclear-system design. The useful principles are criticality, delayed response, poisoning/damping analogies, finite inventory, containment, and conservative shutdown behavior.

### 3.6 Discrete / quantum-inspired effects

Use sparingly, only when a continuous model loses essential structure.

Appropriate cases:

- exclusion: exactly one Cell writer may occupy a mutation authority;
- discrete occupancy / finite capacity;
- rare barrier crossing where ordinary diffusion approximations are poor;
- interference-like conflicts between mutually incompatible action paths;
- measurement as evidence acquisition that changes what decisions are admissible.

A "tunneling" probability is only a rare-transition proxy. Quantum vocabulary must never be used to justify nondeterministic authority, hidden state, or bypassing a policy gate.

### 3.7 Many-body protein / biochemical network model

Large agent systems often resemble cooperative molecular networks more than independent workers. Use many-body biochemical ideas when local interactions create emergent global behavior.

Useful concepts:

- stoichiometric constraints and resource balance;
- cooperative binding / Hill-like gain;
- allostery: one local state changing distant interaction affinity;
- competing pathways and pathway saturation;
- chaperones / repair capacity;
- degradation / garbage collection;
- reaction–diffusion and Turing-like pattern formation;
- liquid–liquid phase separation / condensates;
- rugged energy landscapes and kinetic traps;
- multimeric complexes where a capability exists only when several compatible components assemble;
- homeostasis and feedback inhibition.

The current implementation exposes cooperative load, stoichiometric stress, repair/degradation capacity, and condensate fraction as deterministic advisory signals.

## 4. Mixture-of-physics fan-in

The final recommendation is a deterministic safety-oriented mixture, not an average that can wash out a severe signal.

1. Validate canonical Cell identity, epoch, and Airflow scheduler authority.
2. Evaluate all registered models from the **same immutable sample**.
3. Retain every model's inputs, phase, risk, recommendation, and evidence.
4. Select the strongest safety action first: `REFUSE > FENCE > SHED > HOLD > THROTTLE > REBALANCE > ADMIT`.
5. Use continuous risk only to break ties inside an action severity.
6. Treat crystallization votes separately; they cannot override a stronger safety action.
7. Even unanimous crystallization votes remain `HOLD` until the explicit release gate is open.
8. Publication/promotion still uses the normal swfactory authority and repository policy.

This structure is intentionally closer to fault-tolerant sensor fusion than to a single learned controller.

## 5. Maximum useful entropy, then deterministic dissipation

The desired trajectory is not "minimize entropy at all times."

During exploration, useful entropy is valuable: independent branches, diverse models, and competing hypotheses reduce correlated failure and local-minimum lock-in. The system should maximize **useful constrained entropy** subject to budget, safety, identity, and authority limits.

Before release, the objective reverses: remove duplicates, reconcile interfaces, select one authority per boundary, seal evidence, delete superseded paths, and reduce unexplained degrees of freedom. This is deterministic entropy dissipation, not premature cleanup.

A good final state therefore comes from:

`diverse bounded exploration -> measured interaction -> explicit phase transition -> evidence-backed fan-in -> one stable authority`

not from either permanent chaos or early crystallization.

## 6. Mission-critical engineering rules

For a model or control law to graduate from experiment to trusted use, it must have:

- explicit units and normalization;
- deterministic behavior for identical inputs;
- bounded runtime and memory;
- a fail-closed invalid-input path;
- evidence explaining every refusal/throttle/fence;
- replayable test vectors;
- sensitivity analysis around thresholds;
- fault injection for stale writers, cancellation, retries, restart, provider loss, and ambiguous external results;
- calibration showing that the model improves a real operational objective;
- a documented region of validity and known failure modes;
- no hidden scheduling, publication, policy, or promotion authority.

A mathematically elegant model that cannot satisfy those rules remains an experiment.

## 7. Model selection is itself observable

The mixture must explain not just *what* it recommends but *why a model mattered*.

For every decision retain:

- Cell / epoch / operation identity;
- model version;
- raw and normalized observables;
- phase and regime classification;
- risk score;
- action recommendation;
- dominant-model reason;
- disagreement among models;
- entropy-production and free-energy/work summaries when present;
- crystallization score and whether the release gate was open;
- final authority action and resulting evidence digest.

This prevents "the model said so" from becoming an unreviewable control path.

## 8. What this doctrine forbids

- Physics metaphors that create a second scheduler.
- Averaging away a fence/refusal because several low-risk models vote "admit."
- Treating probabilistic ranking as permission to mutate external state.
- Calling a branch "crystal" and thereby bypassing release review.
- Using quantum language as an excuse for nondeterministic ownership.
- Treating high branch count as progress without evidence of useful diversity.
- Treating low entropy as quality if it merely reflects premature convergence.
- Claiming mission-critical or NASA certification from these models alone.

## 9. Canonical implementation surfaces

- `src/swfactory/physics_mixture.py` — deterministic multi-model evaluator.
- `src/swfactory/ocean_*.py` — hydrodynamic/transport control primitives.
- `ops/physics-mixture-v1.json` — machine-readable model registry and validity rules.
- `ops/phase240-v1.json` — multiphase stress domains.
- `ops/statmech360-v1.json` — statistical-mechanics / many-body stress domains.

Future physics-inspired work should extend these surfaces rather than inventing parallel control authorities.
