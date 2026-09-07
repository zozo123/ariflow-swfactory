# Physics operating model for the software factory

This document is the permanent doctrine for using physics, information theory, control theory, and biological-network ideas in this repository.

The models are **parallel advisory lenses over the same Factory Cell trajectory**. They are not competing schedulers, and they do not create hidden lifecycle loops. Apache Airflow remains the only lifecycle scheduler. Factory Cell identity plus epoch remains the external-mutation authority. Evidence remains durable after compute disappears.

The point is not metaphor. The point is to borrow mathematical structures that expose failure, instability, irreversibility, congestion, criticality, and convergence earlier than ordinary workflow code does.

## 1. Non-negotiable invariants

Every model, now or later, must preserve these laws:

1. **Airflow schedules lifecycle work.** No physics model, agent, inner DAG, reaction network, Monte Carlo sampler, or controller may become a second scheduler.
2. **Cell identity persists; compute does not.** A Cell is the durable issue × target state/evidence boundary. Workers and sandboxes are disposable realizations.
3. **Epochs fence mutation.** External mutation is read-only, idempotent, or addressed by `(cell_id, epoch, operation_key)`.
4. **Authority is singular where correctness requires it.** Scheduling, Cell ownership, publication, policy, and promotion never become emergent consensus.
5. **Evidence outlives execution.** Observables, refusals, work, entropy production, costs, hazards, repairs, and promotion reasons are reconstructable.
6. **Models advise; invariants decide.** A beautiful statistical score can never override a failed safety, authority, security, or exact-head release gate.
7. **Crystallization is explicit.** High entropy is desirable during exploration; deterministic entropy collapse happens only at release.

## 2. One state, many simultaneous lenses

For a Cell trajectory `Gamma`, evaluate many models in parallel and retain the individual outputs. Do not hide disagreement behind one magic scalar.

```text
                              +-------------------------+
                              | durable Factory Cell    |
                              | identity + epoch        |
                              +-----------+-------------+
                                          |
                  same trajectory/evidence stream Gamma|
                                          v
       +----------------+-----------------+----------------+----------------+
       |                |                 |                |                |
  hydrodynamics   stochastic thermo   phase/crystal   stat mech/info   bio networks
       |                |                 |                |                |
       +----------------+-----------------+----------------+----------------+
                                          |
                                  constrained fusion
                                          |
                        +-----------------+------------------+
                        | safety kernel + Airflow authority  |
                        +-----------------+------------------+
                                          |
                               explore / hold / repair /
                               throttle / crystallize
```

Fusion means **intersection of hard constraints plus explicit comparison of soft observables**. It does not mean averaging away contradictions.

## 3. Hydrodynamics: many currents, speeds, and accelerations

A factory is not one queue and not one scalar pressure. Track multiple coupled currents, for example:

- issue arrival current;
- admitted-work current;
- worker-start current;
- mutation current;
- evidence current;
- review current;
- publication current;
- retry/recovery current;
- cleanup/reconciliation current;
- cost/compute current.

Each current can have a flux, velocity, acceleration, relaxation time, saturation, and boundary condition. Useful structures include:

- continuity/conservation laws for durable work identity;
- pressure/velocity coupling for queues and throughput;
- viscosity as damping/backpressure;
- shocks as sudden bursts or provider loss;
- turbulence as nonlinear retry/conflict amplification;
- boundary layers as repository/provider/tenant transitions;
- vorticity as work circulating without net progress;
- cavitation as apparently available capacity collapsing under real load;
- diffusion vs convection for evidence propagation vs active dispatch;
- hysteresis so admission does not chatter around thresholds.

The hydrodynamic model answers: **where is work flowing, accumulating, accelerating, recirculating, or becoming unstable?**

## 4. Equilibrium statistical mechanics: Boltzmann, Gibbs, and Jaynes

When comparing a population of alternative strategies, use ensemble reasoning rather than pretending one sample is truth.

- **Boltzmann weighting** ranks alternatives by effective energy/cost/risk/evidence.
- **Gibbs ensembles** make population and capacity assumptions explicit.
- **Partition functions** normalize alternatives and expose concentration of probability mass.
- **Shannon/Jaynes maximum entropy** chooses the least-committed distribution consistent with known constraints.
- **Free energy** balances expected utility/cost against retained exploratory entropy.

Use MaxEnt when the unknown is a distribution over states. Do not collapse uncertainty earlier than evidence requires.

## 5. Non-equilibrium statistical mechanics: trajectories, not snapshots

Software factories are driven systems: work arrives, resources change, failures occur, retries happen, and mutations are irreversible. Equilibrium alone is insufficient.

Use:

- **Maximum Caliber** as the path-space extension of MaxEnt: infer distributions over complete trajectories subject to observed path constraints;
- **Onsager-style coupled fluxes** when one gradient drives several currents;
- **entropy production** to measure irreversible coordination/retry/mutation cost;
- **detailed-balance tests** to detect hidden directional bias in retry/repair transitions;
- **Jarzynski equality** to reason from non-equilibrium work samples about a free-energy difference;
- **Crooks fluctuation relation** to compare forward and reverse path likelihoods;
- **fluctuation theorems** to treat rare reverse-looking events as measurable rather than impossible;
- **Kramers/barrier intuition** for escape from metastable architectural states.

The non-equilibrium model answers: **what path produced the state, how irreversible was it, and are retries/repairs secretly ratcheting the system in one direction?**

## 6. Phases of matter and crystallization

Development deliberately moves through phases:

- **GAS** — broad unconstrained exploration;
- **LIQUID** — recombinable work flowing through stable interfaces;
- **MIXED** — incompatible phases/authorities coexist explicitly;
- **SUPERCRITICAL** — deliberate maximum stress and concurrency;
- **SOLID** — one coherent exact-head release surface.

Add real phase-transition concepts:

- order parameters for authority singularity, conflict density, and CI health;
- metastability for branches that appear stable but lack enough evidence;
- hysteresis to avoid phase thrashing;
- phase coexistence for competing implementations;
- nucleation barriers before a local pattern becomes architecture;
- critical nuclei as the minimum evidence-backed implementation worth propagating;
- crystal defects as durable schema/lineage/policy discontinuities;
- grain boundaries as seams between independently evolved subsystems;
- annealing as controlled relaxation/reconciliation before release;
- quenching only when an emergency hotfix explicitly accepts the resulting defect risk.

The crystallization rule remains: **maximize useful entropy, then collapse it deterministically exactly once.**

## 7. Nuclear-physics lens: only where chain behavior matters

Use nuclear ideas selectively for cascading/reproductive processes, not as decoration.

- **effective multiplication factor `k_eff`** for retry/fan-out chains: below one dies out, near one is critical, above one runs away;
- **half-life/decay** for stale branches, leases, cache entries, and transient authority claims;
- **absorbers/poisons** as deliberate circuit breakers that terminate a runaway chain;
- **binding energy** as a cohesion-vs-interface-cost measure for a candidate module or release;
- **activation barriers** for dangerous promotions or irreversible external mutation;
- **critical mass** as a warning that individually harmless couplings can become system-wide cascades.

Never use nuclear language to justify more concurrency. Use it to recognize **runaway reproduction and containment requirements**.

## 8. Quantum-style model: discrete operator algebra, not mysticism

Quantum effects are useful only when the software structure is genuinely discrete or order-sensitive.

- creation/annihilation operators for explicit branch/work quanta;
- number operators for active populations;
- commutators for operations whose ordering changes observable state;
- exclusion-like constraints where two writers/owners cannot coexist;
- bosonic-like batching where compatible jobs may share locality without sharing identity;
- tunneling-like rare transitions only as a model for crossing a high barrier through an explicitly authorized exceptional path.

Do **not** claim physical quantum behavior. The value is the algebra of discrete populations and non-commuting operations.

## 9. Complex multi-protein / reaction-network lens

Large software factories resemble regulated reaction networks more than simple pipelines in one important sense: many components interact simultaneously through scarce partners, nonlinear cooperativity, compartments, and feedback.

Track:

- species/components with finite concentration/capacity;
- stoichiometric complexes that only function when all required members are present;
- competing binding for scarce providers, reviewers, locks, or publication authority;
- **Hill-style cooperativity** for nonlinear admission/activation;
- allosteric regulation where one signal changes another pathway's sensitivity;
- chaperone capacity for expensive repair/normalization work;
- **kinetic proofreading** for repeated independent checks before irreversible promotion;
- reaction-network flux rather than just task counts;
- compartment boundaries for tenant/repository/security isolation;
- liquid-liquid phase separation as formation of temporary specialized work condensates;
- negative feedback for homeostasis and positive feedback only when bounded;
- Hopfield/Ninio-style proofreading intuition: spend bounded extra work to suppress catastrophic acceptance errors.

The biological-network model answers: **which complexes can actually form, what scarce component is limiting them, and where nonlinear regulation is amplifying or suppressing flow?**

## 10. Information theory

Physics without information flow is incomplete for a software factory.

Use:

- Shannon entropy for uncertainty/diversity;
- KL divergence for drift between expected and observed behavior;
- mutual information for whether a signal actually predicts an outcome;
- channel capacity for provider/API/event throughput;
- rate-distortion tradeoffs when compressing evidence or coarse-graining worker detail;
- Landauer intuition as a reminder that deleting information has a cost: superseded state may be removed only after durable evidence makes the deletion reconstructable.

Distinguish **useful entropy** (independent hypotheses) from **coordination entropy** (duplicates, stale state, hidden authority, noise).

## 11. Critical phenomena and complex systems

Use the existing statistical-mechanics wave as a diagnostic toolbox:

- susceptibility: how strongly the system reacts to a small perturbation;
- correlation length: how far a failure or retry storm propagates;
- percolation: when local dependency connectivity becomes global coupling;
- renormalization/coarse-graining: summarize local workers into higher-level stable contracts while retaining safety evidence;
- universality: invariants that survive provider/language/repository changes;
- spin-glass/frustration: many locally plausible states with globally incompatible constraints;
- Monte Carlo: bounded exploration of implementation alternatives, always retaining replay metadata.

Near a critical point, small policy or load changes can create large effects. That is a reason to **increase observability and damping**, not to trust a mean-field average.

## 12. Control theory

The physics models produce observables; control theory turns them into bounded feedback.

Useful tools:

- negative feedback for queue/pressure stabilization;
- feed-forward control for predictable bursts;
- Lyapunov-style stability arguments for proving a recovery policy cannot diverge;
- Kalman/Bayesian filtering for noisy telemetry, without hiding raw evidence;
- model-predictive control for bounded admission decisions when future capacity is known;
- gain scheduling across GAS/LIQUID/MIXED/SUPERCRITICAL/SOLID regimes;
- anti-windup and hysteresis to stop controllers from accumulating impossible demand.

Controllers may recommend Airflow parameters or admission state. **They never become lifecycle schedulers.**

## 13. High-assurance / aerospace-style safety kernel

Treat the final factory as safety-critical infrastructure even when the workload is ordinary software.

Required design habits:

- explicit hazard inventory and fault-containment zones;
- FMEA/fault-tree/STPA-style reasoning for high-consequence transitions;
- graceful degradation before uncontrolled retry;
- bounded autonomy and fail-closed external mutation;
- deterministic replay of decisions and evidence;
- independent observability paths for critical authority/evidence signals;
- watchdogs and circuit breakers around runaway chains;
- no single telemetry estimate may silently redefine truth;
- exact-head release evidence, not branch-name trust;
- recovery drills and fault injection before calling a path hardened;
- safety case for crystallization: claim -> evidence -> invariant -> decision.

Redundancy is welcome for sensing and execution. **Authority redundancy is not.** Multiple observers may vote or disagree; one canonical authority decides each protected concern.

## 14. Fusion: how the parallel models produce one decision

For each candidate trajectory, retain a vector rather than only a score:

```text
X = (
  currents/pressure/acceleration,
  entropy + useful-information gain,
  entropy production + irreversible bias,
  phase/order parameters,
  criticality/susceptibility/correlation,
  reaction-network bottlenecks/cooperativity,
  chain-reaction k_eff,
  discrete operation conflicts,
  hazards/safety evidence,
  cost/latency/reliability
)
```

Then apply this order:

1. **Hard invariant rejection.** Any authority, security, Cell/epoch, evidence, or scheduler violation => refuse/hold.
2. **Hazard containment.** Runaway chain, unstable feedback, critical propagation, or missing proof => throttle/repair/isolate.
3. **Parallel model comparison.** Preserve disagreements between lenses and attach them to evidence.
4. **Bounded decision.** Airflow-owned orchestration receives advisory actions such as expand, mix, hold, throttle, repair, or crystallize.
5. **Crystallization gate.** Only exact-head, evidence-backed, singular-authority state may become SOLID and merge to `main`.

No weighted average can turn a failed hard invariant into a pass.

## 15. Practical selection guide

| Situation | Primary lenses |
| --- | --- |
| Queue surge / provider saturation | hydrodynamics + control + queueing |
| Retry storm / ambiguous mutation | non-equilibrium + detailed balance + nuclear chain containment |
| Many competing branches | MaxEnt/ensembles + spin-glass/frustration + phase coexistence |
| Stable-looking but weakly proven design | metastability + nucleation barrier + safety case |
| Complex shared dependencies | reaction networks + percolation + compartments |
| Order-sensitive mutations | discrete/commutator model + epoch fencing |
| Release convergence | crystal/order parameters + proofreading + exact-head CI |
| Cross-repo failure propagation | correlation length + percolation + fault-containment zones |
| Evidence compression | information theory + coarse-graining + provenance |
| Extreme stress test | supercritical phase + fluctuation response + hazard containment |

## 16. What success looks like

The factory should be able to be wildly parallel and high-entropy during development while becoming boring, deterministic, inspectable, and singular at release.

The strongest final state is not the state preferred by one theory. It is the state that survives **all relevant lenses simultaneously** while satisfying the hard invariants.

> **Explore like a gas, flow like a liquid, regulate like a living network, measure like a non-equilibrium physicist, contain cascades like a reactor engineer, verify like high-assurance flight software, and crystallize exactly once.**
