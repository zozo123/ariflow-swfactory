# Phase mechanics of software change

This repository treats development as a controlled phase transition.

The physics language is a model for coordination, not a claim that software obeys fluid dynamics. Its purpose is to make one rule precise: **maximize exploration before convergence; crystallize only once.**

## The state space

| Phase | Meaning | Allowed behavior | Forbidden behavior |
| --- | --- | --- | --- |
| **GAS** | unconstrained exploration | many hypotheses, branches, spikes, issue streams | pretending exploratory state is release state |
| **LIQUID** | useful ideas begin to flow together | grouped PRs, shared contracts, fast recombination | freezing architecture early |
| **MIXED** | incompatible implementations coexist | explicit comparison, conflict exposure, authority selection | silently stacking duplicate authorities |
| **SUPERCRITICAL** | maximum productive stress | extreme concurrency, fault injection, retries, pressure, replacement, cancellation | promoting unstable control paths to `main` |
| **SOLID** | one coherent release surface | exact-head CI, one authority per concern, deletion of superseded paths, promotion | further hidden fan-out inside the release |

## Conservation laws

Across every phase, five invariants do not melt:

1. **Airflow schedules.** Apache Airflow is the only lifecycle scheduling authority.
2. **The Cell persists.** A Factory Cell is the durable issue × target identity; compute is disposable.
3. **Epochs fence mutation.** Every external mutation is read-only, idempotent, or fenced by Cell epoch and operation identity.
4. **Evidence survives execution.** Claims, repairs, refusals, latency, cost, and promotion decisions must be reconstructable after workers disappear.
5. **Authority is singular at crystallization.** Scheduling, Cell ownership, publication, promotion, and policy each collapse to one surviving authority before release.

## Thermodynamics of the repo

We deliberately inject **entropy** through parallel branches, competing implementations, generated issue matrices, adversarial failures, and disposable workers. Entropy is useful while it increases information.

We apply **pressure** through bounded queues, concurrency, deadlines, cancellation, retries, stale-writer takeovers, provider loss, and operator load. Pressure exposes weak contracts.

We add **viscosity** with backpressure, hysteresis, budgets, admission control, and human gates so pressure does not become oscillation.

We seek **flow** when independent work can recombine through stable contracts. We seek **dissipation** when duplicate abstractions, stale branches, retry storms, and dead authority paths should disappear.

The goal is not minimum entropy. The goal is **maximum useful entropy followed by deterministic entropy collapse**.

## Phase transition rule

```text
GAS
  -> LIQUID        when experiments acquire stable interfaces
  -> MIXED         when incompatible implementations or authorities coexist
  -> SUPERCRITICAL when we intentionally maximize concurrency and failure pressure
  -> SOLID         only when crystallization is explicitly requested and every release invariant holds
```

There is no shortcut from GAS, LIQUID, MIXED, or SUPERCRITICAL directly to `main`.

## Crystallization gate

Promotion is allowed only when all of the following are true at the exact candidate head:

- no unresolved implementation conflicts remain;
- no duplicate lifecycle or mutation authorities remain;
- every generated issue assigned to the wave is resolved or explicitly rejected with evidence;
- required CI and conformance checks are green;
- stale branches and superseded abstractions are deleted or made inert;
- the surviving contracts still preserve Airflow scheduling, Cell identity, epoch fencing, and retained evidence.

Only then does the repository become **SOLID** and eligible to merge to `main`.

## One-line doctrine

> **Be gas while discovering, liquid while combining, mixed while choosing, supercritical while stress-testing, and solid exactly once—at release.**

This is the operating model for high-concurrency development in this repository.
