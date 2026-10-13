# Speculative campaigns: evidence-checked shadow fan-in

This is the **first intentionally non-authoritative slice** of acceptance-aware
speculative software execution. Its goal is to measure whether an independently
run verifier would favor a different already-frozen candidate than the factory's
existing deterministic campaign selection. It does **not** spawn sandboxes,
change the work DAG, update branches, accept a PR, or promote a generation.

The starting point is the existing [candidate evidence](candidate-evidence.md)
pipeline, not a new source of release authority:

`pinned input -> private candidate -> frozen output -> retained evidence -> campaign decision`

A separately operated checker adds:

`pinned verifier -> one reported outcome per answered candidate -> shadow comparison`

The shadow report is data for later experiments. Its `suggested_candidate` is
**not an approval** and cannot be consumed as a release gate.

## Inputs

1. A `campaign-decision` manifest, including the immutable decision digest.
2. Retained evidence bundles for **every answered candidate**, reverified
   against the local Git repo (source/output, candidate ref, diffs, artifacts).
3. One digest-bound `ShadowObservations` document. Each observation includes
   `candidate_id`, `output_head`, unique `observation_id`, the pinned verifier
   recipe digest, a digest of the verifier's retained output, a reported result,
   and nonnegative cost/work measurements.

The accepted reported results are `passed`, `failed`, `timed_out`, and
`refused`. Failed or refused candidate-generation attempts remain in the
campaign manifest; they do not masquerade as answered outputs.

The observation's `verifier_digest` and `evidence_digest` **identify** the
external verifier and its result. They do **not**, by themselves, authenticate
that a trustworthy verifier actually ran. Do not build a promotion decision
from a self-reported verdict. The trusted verifier runner, signed/attested
receipts, freshness and anti-replay checks, and verifier independence remain
follow-up work.

## Generate a shadow observations document

The external checker must first execute the pinned verifier against each
frozen candidate, independently of candidate generation, and retain its full
test log. After obtaining that result, a trusted recorder can write the
following Python data structure, substituting the actual recipe and log hashes:

~~~python
from pathlib import Path

from swfactory.campaign_decision import load_campaign_decision
from swfactory.speculative_shadow import (
    ShadowObservations,
    ShadowVerifierObservation,
    write_shadow_observations,
)

decision = load_campaign_decision(Path("campaign-decision.json"))
repair = next(c for c in decision.candidates if c.candidate_id == "cand_repair")

observations = ShadowObservations(
    campaign_decision_digest=decision.digest(),
    receipts=(
        ShadowVerifierObservation(
            candidate_id=repair.candidate_id,
            output_head=repair.output_head,
            observation_id="trusted-verifier-run-2026-10-13-001",
            verifier_digest="sha256:" + "b" * 64,  # replace with actual pinned recipe digest
            evidence_digest="sha256:" + "c" * 64,  # replace with actual retained log digest
            result="failed",
            cost_usd=0.42,
            duration_s=14.0,
        ),
        # Add every other answered candidate, using the same verifier digest.
    ),
)
write_shadow_observations(Path("shadow-observations.json"), observations)
~~~

The placeholder hashes above are illustrative only. The recorder must use
real recorded digest values, with an external process responsible for verifying
the outputs. A partial set of observations is allowed **only for measuring
incomplete coverage**; it cannot produce a candidate suggestion.

## Run read-only fan-in

~~~sh
uv run swfactory campaign-shadow compare \
  campaign-decision.json shadow-observations.json shadow-report.json \
  --repo . \
  --candidate-evidence cand_repair=.factory/candidates/cand_repair-evidence \
  --candidate-evidence cand_rethink=.factory/candidates/cand_rethink-evidence \
  --json
~~~

Repeat `--candidate-evidence` for **every** answered candidate. If the evidence
manifest, frozen ref, binary diff, or any retained artifact has drifted,
the command refuses to write a shadow report.

The deterministic report records:

- Every campaign candidate, including failed, skipped, and cancelled attempts.
- All reported verifier passes, failures, refusals and timeouts, with missing
  observations explicitly marked.
- The existing selection and a distinct shadow suggestion, in the **existing
  ranking order**, not checker-completion order.
- Whether the two selections agree, or `null` when evidence is incomplete or
  the campaign was cancelled.
- Additive verifier cost and **sum of work seconds**, *not* wall-clock latency,
  speedup, statistical independence, or a calibrated acceptance probability.
- Digests for the existing decision, shadow observations, and resulting report.

**Abstain** if any answered candidate lacks a verifier observation, if a
campaign is cancelled, or if no candidate reports a pass. Also refuse inputs
with unrecognized candidates, output SHA disagreement, duplicate observations,
different verifier recipe hashes, negative or nonfinite cost, or a different
campaign epoch/decision digest.

The report file carries `authority: advisory-only`. It never modifies the
factory's original winner or enables the human promotion gate.

## Next steps: Cambridge speculative-sandbox thesis

This PR is an *instrumentation* prerequisite, not the speculative engine:

1. Wire a trusted external verifier with pinned toolchain, isolated execution,
   independent witnesses, retained logs, and authenticated results.
2. Implement real fork/checkpoint execution under a bounded campaign budget,
   distinguishing warm-cache reuse, disk restoration and demonstrated
   memory-preserving forks. Verify that siblings do not share writable state.
3. Record negative results as first-class training observations and measure
   conditional acceptance by strategy family, verifier and shared ancestor.
4. Train and calibrate a shadow-only conditional predictor; compare it with
   a **warm serial baseline**, fixed-width parallel search, and greedy adaptive
   search using held-out work orders. Measure success by deadline and total
   cost rather than treating “more agents” as progress.
5. Preserve the current Airflow Cell / epoch and controller-only promotion gate.
   A winner from any future speculative scheduler must be reverified by the
   authorized controller before publication.

This is intentionally conservative: the core contribution is making
counterfactual candidate outcomes inspectable **without mistaking a model vote
or correlated verifier report for independent evidence**.
