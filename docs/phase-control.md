# Phase-aware Liquid / Dark Factory

The factory now distinguishes **state** from **control posture**.

- A **phase** is an observed property of the factory.
- A **mode** is a reversible recommendation for how search and verification should behave.
- Neither is lifecycle authority.

Apache Airflow remains the only lifecycle scheduler. Factory Cells, epochs, policy, capability
boundaries, evidence, publication and promotion keep their existing authority.

The implementation is deliberately smaller than the full research program in
[non-equilibrium-factory.md](non-equilibrium-factory.md): it makes the phase vocabulary executable
without claiming that Jarzynski/Crooks/Onsager or a general thermodynamic controller is production
machinery.

## States

| Phase | Observable interpretation | Failure if misread |
| --- | --- | --- |
| gas | high candidate entropy, weak coherence | converge too early and lose useful hypotheses |
| liquid | mobile candidate set with useful specialist structure | over-coordinate and destroy exploration |
| critical | transition region: high disagreement/acceleration/branching | keep spawning when measurement is more valuable |
| crystal | low entropy + high coherence + high independent evidence | mistake a model's confidence for proof |
| glass | low mobility without sufficient evidence/order | mistake being stuck for being stable |
| jammed | queue/resource/cleanup debt dominates progress | add more work to a saturated system |

**Annealing is intentionally not a phase.** It is a mode that reduces degrees of freedom while
verification independence rises.

## Modes

| Mode | Search posture | Candidate action | Context action | Verification | New implementation lanes |
| --- | --- | --- | --- | --- | --- |
| diverge | increase isolated trajectories | expand | fresh | normal | yes |
| coordinate | hold specialist lanes | coordinate | retain | normal | yes |
| measure | stop widening | freeze | retain | increase | no |
| anneal | decrease/fork finalists | prune | retain | increase | no |
| verify | no new search | verify exact candidate | compact | maximum | no |
| perturb | one bounded fresh trajectory | reset | fresh | increase | yes, bounded |
| drain | stop spawning | hold | compact | maximum | no |

This borrows useful mechanics from multi-agent systems such as OpenClaw—isolated versus forked
trajectories, specialist lanes, bounded branching, context pressure/coarse-graining, queue
back-pressure and exception-only attention—without importing another scheduler or authority model.

## Order parameters

A phase decision is derived from a versioned vector of dimensionless observables:

```text
Phi = (
  candidate_entropy,
  coherence,
  mobility,
  queue_pressure,
  queue_acceleration,
  resource_pressure,
  branching_ratio,
  evidence_completeness,
  context_pressure,
  debt_pressure,
  verifier_disagreement,
)
```

The current pure controller derives three diagnostics:

```text
jam_pressure
  = clamp(0.40 Q + 0.25 R + 0.35 D + 0.15 max(dQ/dt, 0))

order_parameter
  = clamp(0.35 C + 0.35 E + 0.15 (1-H) + 0.15 (1-V))

transition_pressure
  = clamp(0.50 V + 0.20 |dQ/dt| + 0.20 branching_pressure + 0.10 context_pressure)
```

where H is candidate entropy, C coherence, E evidence completeness and V verifier disagreement.
These are engineering diagnostics, not physical units.

### Candidate entropy

For candidate weights p_i:

```text
H = -sum(p_i log p_i) / log(N)
```

H=1 is maximally dispersed; H=0 means one effective candidate.

### Branching criticality

The controller records:

```text
R_branch = (children + retries) / terminal_parents
```

R_branch >= 1 is treated as supercritical pressure. That does not automatically mean jammed: a
bounded experiment can be critical without being saturated. But sustained failure cascades should
be driven below one by retry budgets, admission control, fencing and cleanup.

## Hysteresis

The same noisy snapshot should not flip the factory back and forth between phases. The controller
therefore has explicit retention bands:

- a crystal remains crystal under a modest loss of order/evidence;
- a jam remains jammed until debt pressure materially falls;
- glass remains glass until mobility/evidence meaningfully improves;
- gas/liquid/critical boundaries require stronger evidence to reverse immediately.

Strong transitions into crystal or jammed are not delayed.

## Multi-agent trajectory semantics

The controller names three trajectory shapes:

- **isolated**: start without inheriting assumptions; use for divergent search and glass breaking;
- **forked**: descend from a selected candidate; use while annealing finalists;
- **specialist**: independent role/lens over shared immutable inputs; use in liquid/critical review;
- **none**: do not create another implementation trajectory.

These describe how an existing executor may spend a bounded search budget. They do not create tasks
or change Airflow's DAG.

## Context as coarse-graining

Context is treated as a finite search resource:

- **fresh**: deliberately drop inherited assumptions for decorrelated exploration;
- **retain**: keep relevant trajectory state while work is still mobile;
- **compact**: preserve durable evidence/summary while removing microscopic execution history.

Compaction is an accelerator and memory-management operation. It cannot alter the exact candidate,
evidence digest or promotion policy. High context pressure also contributes to transition pressure;
in coordinate/measure/anneal modes it can recommend compaction without changing the candidate.

## Authority boundary

Every phase assessment carries:

```json
{"authority": "search-only"}
```

A recommendation may shape:

- number/type of candidate lanes within existing budgets;
- isolated vs forked vs specialist trajectory choice;
- context retention/compaction/fresh-start policy;
- verification intensity;
- queue intake posture;
- operator attention priority.

It may **not**:

- approve a gate;
- mint or redeem a credential;
- bypass a protected path;
- publish or merge;
- change Cell epoch;
- mark evidence verified;
- promote a candidate.

In particular:

> **Crystal means "verification posture", not "permission to promote."**

## Cross-language contract

Python: `swfactory.phase_control`

Rust: `swf_domain::phase_control`

Read-only operator surfaces:

```sh
swfactory phase-assess phase.json --json
swf phase phase.json --json
```

Both consume the same fixture:

`tests/fixtures/contract/phase_control.json`

The fixture covers gas, liquid, critical, annealing posture, crystal, glass, jammed, supercritical
branching and crystal hysteresis. Any Python/Rust disagreement is a contract failure. Assessments
retain the exact observation plus previous phase, so the classification is self-contained for replay.

## Relationship to Liquid review

The existing Liquid review stage keeps its local defect/temperature/nucleation diagnostics, but its
phase names now come from the shared factory phase vocabulary and its retained `annealing.json`
records the corresponding control mode.

Review remains narrower than factory-wide control. For example, review-local `gas` means
"measurement is incomplete", not "spawn arbitrary new implementation work": the stage is already
inside a fixed Airflow lifecycle boundary.

## Rollout

1. **Contract (this change):** shared states, modes, order parameters, hysteresis and fixtures.
2. **Observe:** emit phase snapshots from real runs without changing behavior.
3. **Advisory:** show phase/mode in operator attention and retained receipts.
4. **Search-only closed loop:** allow bounded search knobs to follow recommendations.
5. **Adaptive control:** learn thresholds/budgets from retained trajectories.
6. **Dark factory:** routine search/repair/verification fades into the background; humans see
   authority boundaries, genuine ambiguity and exceptional debt.

Each step must preserve the same authority boundary.
