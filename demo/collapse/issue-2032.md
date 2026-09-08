---
id: COLLAPSE-2032
title: Quarantine the physics models out of the product's cognitive path
labels: [factory, selfhost]
---
## Problem

The intuitive core idea — **exploration entropy → stabilization → convergence** — is good and should stay. The statistical-mechanics vocabulary around it should not be in the product path: Jarzynski estimators, Crooks relations, nucleation barriers, Onsager response, protein networks, nuclear criticality.

Current surface: `src/swfactory/physics_bundle_01..18.py` (`WaveBundle(id, PhysicsWave.OCEAN120|PHASE240|STATMECH360, lo, hi)` plus a `run()` forwarding to `execute_wave`), `physics_wave_runtime.py`, and `non_equilibrium.py`. The 18 physics bundles are imported only by `tests/test_physics_bundle_coverage.py`.

The risk is not that the physics is wrong — `docs/non-equilibrium-factory.md` is disciplined and says the layer is advisory, must use measurable variables, and cannot gain lifecycle authority. The risk is that a reader meeting this before the kernel concludes the project is an AI-generated physics metaphor, which undersells a genuinely strong distributed-systems design.

## Acceptance

- Physics wave taxonomy survives as **coverage rows** in `config/liquid-spec.yaml`, not as 18 modules.
- `physics_bundle_*.py`, `physics_wave_runtime.py` and `tests/test_physics_bundle_coverage.py` are deleted.
- `non_equilibrium.py`: verify first whether anything in it influences a scheduling or mutation decision. If it does, that is a doctrine violation and must be reported before anything moves. If it is genuinely advisory and unreferenced from the product path, move the material to `docs/research/`.
- `docs/research/README.md` states what the material is, why it is not in the product path, and **the falsifiable prediction each borrowed equation would have to make to earn its way back**.
- The production control surface is stated in boring measurable terms, with a per-variable table of already-measured / derivable-from-existing-state / not-available: queue depth, failure rate, retry rate, in-doubt operations, cleanup debt, cost, time-to-evidence, time-to-merge, sandbox saturation, cancellation lag.

## Note

If any of this is to be replaced by real theory rather than deleted, the honest candidate is **queueing theory** (utilization vs latency, Little's law) for admission control and sandbox saturation — not thermodynamics. That is boring, well understood, and actually predictive for the variables above.
