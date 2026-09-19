# Harness efficiency: preserve evidence, reduce replay

The software factory treats harness efficiency as motion, never authority. Airflow still owns the
lifecycle; candidate identity, approvals, publication, and promotion evidence are unchanged.

The useful pattern from NVIDIA SoL-Pi is not another scheduler. It is a set of worker-side
mechanisms for reducing repeated model context while retaining exact observations. swfactory adopts
only the mechanisms that fit its trust boundary.

## Implemented: ObservationPack for failed verification

Before this change, failed verification retained only the last 6000 characters of stdout and the
last 2000 characters of stderr for the next repair call. Earlier diagnostic output was gone.

When either stream exceeds that old bound, swfactory now:

1. archives the complete sandbox result in host-owned run state;
2. runs the repository's deterministic secret-shape scan plus a conservative secret-assignment check;
3. mirrors the exact source into ignored .factory/observations scratch only when that classifier is clean;
4. assigns a content-addressed obs:sha256:<digest> handle;
5. selects bounded failure windows plus small head/tail context;
6. verifies every selected line range against the archived source and drops any sensitive excerpt;
7. passes the repair call the smaller verified observation and, only for a clean source, the recall path.

Small failures keep the previous prompt contract exactly.

## Evidence-preserving reducer

Reduction is deterministic and local. A compact observation is only a navigation aid. The archived
source remains authoritative.

If reduction cannot verify its excerpts, it raises. If the reduced prompt is not smaller than the
legacy tail, the factory falls back to that legacy tail and adds the exact-recall handle.

Raw diagnostic text is not automatically committed. The delivery artifact carries only:

- the observation digest and stable handle;
- source byte and line counts;
- compact-versus-legacy prompt bytes;
- exact quote spans and quote digests;
- whether a remote reducer was used;
- whether raw logs were committed.

Today both final flags are false. There is intentionally no remote reducer integration. If a log
matches a known token shape or a conservative secret-assignment pattern, the full source remains
host-only and the agent receives no raw recall path. This deliberately prefers a false-positive
loss of debugging context over expanding the remote-model trust boundary.

## Constrained harness research

Harness auto-research is useful only if efficiency is subordinate to the factory invariants.
HarnessTrial therefore separates:

- candidate_sha: exact bytes the experiment produced;
- authority_digest: canonical promotion-relevant facts;
- evidence_digest: retained evidence for that experiment;
- efficiency vector: prompt bytes, micro-USD cost, wall time, and model turns.

A candidate mechanism is refused if its candidate SHA changes, its authority digest changes, its
evidence is incomplete, or its verifier is red.

Among admissible trials there is no invented weighted score. The factory keeps the Pareto frontier:
a mechanism is dominated only when another admissible trial is no worse on every measured
efficiency dimension and strictly better on at least one.

That encodes the governing rule directly:

> An accelerator may change time-to-answer, never the answer.

Selection is research-only. It returns no promotion action and has no callback into Airflow,
GitHub, approvals, or candidate readiness.

## Mapping the other SoL-Pi mechanisms

Action Fusion maps only to factory-owned validation. swfactory already keeps mutation and
verification inside one bounded Airflow build stage, but it does not grant authority to arbitrary
model-selected shell commands merely to reduce turns. A future fused editing tool should execute
only an approved validation recipe and should retain both mutation and validation evidence.

Online Context Compact is less directly applicable because Claude invocations currently use
no-session-persistence. The factory-controlled context is what crosses agent calls, so compacting
large verification observations is the useful boundary today.

ObservationPack is the cleanest fit because the factory already has host-owned evidence and an
ignored scratch namespace. Stable handles let compute remain disposable while diagnostic identity
survives.

## Benchmark contract

Do not turn byte savings into a public cost claim. A research run should record, per mechanism and
environment:

- exact source/candidate identity;
- authority digest and evidence digest;
- verifier result;
- prompt/input/output token counts when the provider exposes them;
- measured cost;
- wall time;
- model turns;
- observation bytes and bytes avoided;
- environment/model/toolchain identity.

Only measured runs with equal authority projection and complete evidence may enter the efficiency
frontier. Public README performance claims should cite those retained measurements, not estimates.
