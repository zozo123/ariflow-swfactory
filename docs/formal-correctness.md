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

Receipts with `authority=search` are retained but cannot justify or refute a claim. A search-found
counterexample must be checked by a trusted verifier before it changes the justified claim set.

A trusted refutation dominates supporting receipts.

The resulting `ClaimCertificate.meets_claim_policy` is **not promotion authority**. It is an
evidence predicate that the existing authority plane may consume.

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

> **Explore nondeterministically, converge empirically, certify only bounded propositions, and never
> allow confidence or computation to masquerade as proof or permission.**
