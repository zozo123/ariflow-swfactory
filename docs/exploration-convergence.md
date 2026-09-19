# Exploration entropy and deterministic convergence

The factory should be deliberately asymmetric.

**Exploration is allowed to be wild. Convergence is not.**

During exploration, independent workers should attack the same problem from genuinely different
directions. Ordering may vary. Model/provider choices may vary. Decomposition may vary. Candidate
workspaces are disposable. The search phase should maximize useful diversity subject to explicit
budget, authority and isolation limits.

At fan-in, all of that freedom ends. Final reasoning must be reconstructable from immutable inputs
and retained evidence. Candidate arrival order, worker timing and scheduler interleaving must not
change the selected result. Canonical ordering, explicit required dimensions, deterministic
serialization and one promotion authority collapse the search space to one auditable decision.

The machine-readable policy is [config/exploration-convergence.yaml](../config/exploration-convergence.yaml).

## Non-negotiable boundary

Exploration entropy can influence **which hypotheses are generated**. It cannot influence the
meaning of the final gate.

Final promotion is therefore a pure function of:

1. immutable candidate identities;
2. retained evidence digests;
3. explicit correctness/evidence requirements;
4. current human/authority state.

If replaying those inputs can produce two different promotion decisions, convergence is broken.

## Failure modes this forbids

- first-finisher wins;
- majority vote without evidence;
- random tie-breaking at promotion;
- hidden model preference in the final gate;
- completion-order-sensitive ranking;
- candidate evidence generated only after the winner is chosen;
- exploration workers publishing directly;
- widening authority because many independent lanes agree.

The intended shape is:

```text
high-entropy exploration
  -> independent frozen candidates
  -> retained evidence
  -> canonical deterministic ranking
  -> explicit human/policy gate
  -> one promoted state
```


## Stochastic build oracle

Jev may be used as an **exploration oracle**, not as a builder-of-record or a promotion judge.

The factory first declares a finite search space such as strategy, review lens, implementation
shape, or tool profile. The pinned Jev model may return probability mass over those declared
values. It cannot add an axis or value. The factory normalizes the distribution and samples locally
from a recorded entropy token.

The retained receipt binds the model, rubric, normalized distribution digest, entropy token, and
sampled hypothesis. Replaying the same distribution and entropy reproduces the same hypothesis.
Changing the Jev distribution can therefore change **what gets tried**, while ordinary candidate
verification, retained evidence, deterministic fan-in, and the human/policy promotion gate remain
unchanged.

If the provider is unavailable or its response is malformed, exploration falls back to a local
uniform distribution over the same declared search space. Provider quality may affect search
efficiency; it may not affect build liveness or authority.
