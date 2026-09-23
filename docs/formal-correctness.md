# Formal correctness without pretending arbitrary software is decidable

## Thesis

The factory must not collapse “search converged” into “program is correct”.

For arbitrary software there is no general decision procedure for semantic correctness. The useful
formal object is therefore an indexed judgment about one exact artifact under explicit assumptions:

```text
Gamma |-_{M} artifact : phi
```

Read this as: under assumptions `Gamma`, using model `M`, the frozen artifact satisfies claim
`phi`.

The factory does not produce `Correct(artifact)`. It produces an artifact plus bounded claims and
the evidence that justifies exactly those claims.

## Two conservation laws

The dark factory already has one core law:

> **Computation cannot mint authority.**

Formal correctness adds its epistemic twin:

> **Computation cannot mint truth.**

An agent may generate code, hypotheses, tests, counterexamples, specifications, models, or a
candidate proof. None of those objects becomes a justified claim merely because the generating
model says so. Trusted gates grant authority; trusted verifiers admit evidence.

So the factory has two orthogonal integrity axes:

```text
epistemic integrity x authority integrity
```

## Correctness is a relation, not a scalar

For candidate `c`, freeze:

```text
Q = (c, Gamma, M, P, Phi)
```

where:

- `c` is the exact artifact digest;
- `Gamma` is the assumptions/environment contract;
- `M` is the abstraction or executable model;
- `P` is the evidence/promotion policy;
- `Phi` is the finite claim set.

The **Formal Quench** hashes that tuple. Changing artifact, assumptions, model, policy, or claim text
changes the quench identity and invalidates evidence issued for the previous question.

For evidence set `E`, define:

```text
J(Q, E) = { phi in Phi | E supports phi under the exact frozen Q }
```

The honesty invariant is:

```text
AdvertisedClaims(Q) subseteq J(Q, E)
```

The repository must never advertise a statement stronger than the evidence relation warrants.

## Evidence is property-relative

There is intentionally no universal ranking such as:

```text
test < fuzz < model-check < theorem
```

A theorem about the wrong abstraction can say less about the deployed binary than a runtime test
against the real environment. Each claim therefore declares the evidence method that gives that
claim meaning.

| Method | Honest interpretation |
| --- | --- |
| `test` | the declared examples passed |
| `fuzz` | no counterexample was observed in the declared fuzz campaign |
| `static-analysis` | the named analyzer established its scoped property |
| `replay` | the frozen computation reproduced under the declared replay contract |
| `bounded-exhaustive` | the declared finite state/input region was exhausted |
| `model-check` | the stated invariant holds in the explored formal model |
| `theorem` | a proof checker accepted the theorem under explicit definitions/axioms |
| `benchmark` | the declared performance experiment measured the stated quantity under its environment |
| `observation` | an external/open-world behavior was observed under the retained integration context |

The claim text must reflect the method’s scope.

## Three distinct uncertainties

1. **Search uncertainty** — which candidate should survive? Gas/liquid/critical/crystal address this.
2. **Epistemic uncertainty** — which claims about the frozen candidate remain unresolved?
3. **Specification uncertainty** — did we formalize the property the user actually needed?

A crystal addresses the first. It does not eliminate the second or third.

## Formal Quench is not another phase

The phase vocabulary stays thermodynamic:

```text
gas -> liquid -> critical -> crystal
                  ^           |
                  |           v
                melt     Formal Quench
```

The Formal Quench is an orthogonal certification operation:

1. freeze artifact bytes;
2. freeze assumptions;
3. freeze the model;
4. freeze policy and claim statements;
5. generate claim-specific evidence obligations;
6. admit only evidence bound to that exact quench;
7. derive a Claim Certificate.

A trusted counterexample refutes a claim and can **melt the crystal**, returning the system to
critical/perturbation search with the witness retained.

## Executable Claim Certificate

`swfactory.formal_claims` implements the first pure contract.

A `FormalQuench` binds artifact, assumptions, model, policy, and claims. Each claim fixes a statement,
an evidence method, required/optional status, and minimum independent verifier count.

An `EvidenceReceipt` binds to both the quench digest and claim digest.

Receipts cannot self-declare trust. Certificate derivation receives the trusted-verifier set from
outside the evidence object; untrusted/search receipts are retained but cannot justify or refute a
claim. A search-found counterexample must be checked by a trusted verifier before it changes the
justified claim set.

A trusted refutation dominates supporting receipts.

The resulting `ClaimCertificate.meets_claim_policy` is **not promotion authority**. It is an
evidence predicate that the existing authority plane may consume.


## The certificate is a projection of a justification hypergraph

A flat list of evidence is useful for display, but it is not the strongest mathematical object.
Compound conclusions depend jointly on premises, and one lemma may feed several conclusions.
The canonical provenance shape is therefore a typed, content-addressed **justification hypergraph**:

```text
J_Q = (V, E)
```

for one frozen Formal Quench `Q`.

Nodes are typed as artifact, assumption, model, evidence, counterexample, refinement, or claim.
A derivation is a hyperedge:

```text
{v1, v2, ..., vn} --rule/verifier/receipt--> conclusion
```

The ordinary dependency projection must be acyclic. Every node and edge is content-addressed, so the
graph digest commits to the exact artifact, assumptions, evidence, verifier receipts, refinement
claims, and inference structure.

For example:

```text
model + TLC receipt
        |
        v
[stale epochs cannot commit] ----+
                                  |
artifact + model                  |
        |                         |
        v                         v
[runtime refines model] + assumptions
                  |
                  v
      [publication preconditions]
```

`swfactory.justification_graph` checks the structural contract and projects trusted support and
refutation. It deliberately does **not** pretend to replay the semantics of TLC, Lean, a fuzzer, or
an external system. Those verifiers produce receipts; the graph ensures they cannot drift across
the frozen question or self-assert trust.

The useful end-state is **justification-carrying promotion**, not a vague "proof-carrying" label:

```text
artifact + justification_graph_root + authority_receipt
```

with four laws:

```text
No claim without a derivation.
No derivation without its assumptions.
No proof transport without an explicit refinement argument.
No justified claim implies authority.
```

A trusted counterexample creates a graph cut: the refuted claim and every downstream conclusion
that needs it lose justification. In the phase metaphor, this is the formal meaning of melting a
false crystal.

## Knowing when *not* to crystallize is part of correctness

Phase classification and freeze policy answer different questions.

A phase says what the search currently looks like. A freeze/quench decision asks whether stopping
semantic motion is epistemically justified. Therefore:

> **Observed crystal is descriptive; crystallization admission is selective.**

`swfactory.crystallization` keeps the decision advisory and orthogonal to authority. It separates
three outcomes:

- **keep-liquid** — do not freeze while the specification is moving, candidate entropy remains high,
  or the implementation is still changing materially;
- **measure** — stop widening and gather evidence while coverage is incomplete or independent
  verifiers disagree;
- **freeze** — exact bytes are stable enough to bind evidence, after which formalization may still
  be empirical, optional, or formal.

Even after freezing, formal proof may be the wrong instrument. Prefer empirical evidence when the
abstraction is too weak or the environment is too open/volatile. Formalization becomes a strong fit
only when the claim has a credible abstraction, the semantics are stable enough to freeze, and the
consequence of error justifies the cost.

This creates an explicit negative capability:

```text
unstable spec            -> keep liquid
high candidate entropy   -> keep liquid
high semantic velocity   -> keep liquid
evidence gaps            -> measure
verifier disagreement    -> measure
weak abstraction         -> freeze + empirical verification
open-world behavior      -> freeze + fuzz/observation
performance claim        -> freeze + benchmark
finite transition safety -> optional/model-check/formal, depending on risk
pure mathematical kernel -> optional/theorem, when the abstraction is the implementation
```

The thresholds are policy, not universal constants. The default policy is conservative and
replaceable; its output remains `search-only`.

This avoids two symmetric mistakes:

1. crystallizing too early and turning a local minimum into doctrine;
2. formalizing the wrong abstraction and mistaking proof strength for system truth.


## What should be formalized first

Do not attempt to prove arbitrary generated applications. Formalize the small control plane whose
mistakes invalidate every downstream evidence claim:

```text
State =
  (Cell,
   epoch,
   Airflow binding,
   policy,
   operations,
   evidence,
   candidate,
   approvals,
   publication)
```

High-value invariants:

### Epoch fencing

```text
e < CurrentEpoch(cell) => not Mutate(cell, e)
```

### Evidence non-transferability

```text
Evidence(e, candidate_a, policy_a)
and (candidate_a, policy_a) != (candidate_b, policy_b)
=> not Authorizes(e, candidate_b, policy_b)
```

### No blind replay

```text
ExternalEffectState = in_doubt => ObserveBeforeRetry
```

### Publication requires bound evidence

```text
Published(c)
=> CurrentEpoch
   and AirflowBound
   and Frozen(c)
   and EvidenceBound(c)
   and Approved(c)
```

### Search cannot grant truth or authority

```text
PhaseDecision / ModelOutput
  !=> VerifiedClaim
  !=> AuthorityGrant
```

## Why TLA+ first

The authority kernel’s hard failures are temporal and distributed: retries, duplicate delivery,
crashes between remote effect and local receipt, stale workers, restore windows, reordering, and
publication after cancellation.

`formal/authority/AuthorityKernel.tla` is the first model. It covers epoch replacement, Airflow
binding, candidate freeze/evidence/approval, ambiguous external effects, trusted grants, and
publication.

This proves only claims about the model under its assumptions. It does **not** prove that
`ControlKernel`, SQLite, Airflow, GitHub, Linux, or the Python/Rust implementation refines the model.

## The real finish line is refinement

A formal model without an implementation relation is executable design documentation, not a runtime
theorem.

The next layer should project durable runtime events into the formal action vocabulary:

```text
real execution
    |
    v
canonical authority trace
    |
    v
TLA+ action projection
    |
    v
trace admitted by model?
```

Conceptually:

```text
RuntimeTrace <= AuthorityModel
```

Only after that bridge exists can a certificate honestly say both:

1. the abstract model satisfied the declared invariant; and
2. the observed implementation trace conformed to modeled transitions.

## Where Lean belongs

Lean belongs later, after a small pure semantic kernel exists, for statements such as:

```text
transition : State x Request -> State + Refusal
```

and theorems like:

```text
reachable s
and request.epoch < s.epoch
=> transition(s, request) = Refusal
```

Do not use Lean as a decorative wrapper around Python/Airflow. Extract a small mathematical kernel
first; then prove properties whose refinement story can be defended.

## Factory framing

The factory transforms:

```text
unbounded stochastic hypotheses
```

into:

```text
frozen artifact
+ explicitly bounded claims
+ evidence/proofs supporting exactly those claims
```

subject to:

```text
AdvertisedClaims subseteq SupportedClaims
OutputAuthority subseteq ExplicitlyGrantedAuthority
```

> **Explore nondeterministically, preserve the option to stay liquid, freeze only stable questions,
> construct explicit derivations for bounded claims, and never allow confidence or computation to
> masquerade as proof or permission.**
