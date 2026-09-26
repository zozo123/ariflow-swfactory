# The Software Atelier

## Purpose

The software factory is an **atelier**: a place where people and agents can explore, make, compare,
and refine software. The atelier protects room for invention while keeping a strict boundary around
what may be called verified, approved, or released.

This is a product and workflow framing for the existing factory. It does not introduce a second
scheduler, grant agents new authority, or claim that every step described here is already enforced
by the managed runtime. Apache Airflow remains the only lifecycle scheduler; Factory Cell identity
and epoch continue to fence managed effects; promotion remains an explicit authority decision.

> **Explore freely within the work order. Make every finished claim answerable to evidence.**

The process is inspired by rigorous agent workflows such as pstack: investigate before changing,
choose a method suited to the task, keep the change small, and verify against the real artifact.
The atelier extends that discipline across durable work identity, evidence, uncertainty, and release.

## The making process

```text
commission -> sketch -> critique -> freeze -> verify -> steward -> release
                  ^         |                    |
                  +---------+--------------------+
                 counterexample or new question
```

A counterexample or changed requirement returns the work to exploration. It does not silently
preserve an earlier verdict.

### 1. Commission: understand the work

A commission starts with the accepted work order and records:

- the intended outcome and who it is for;
- constraints, protected surfaces, and authority limits;
- observable acceptance criteria;
- assumptions and unresolved questions;
- which criteria require human judgment.

The brief is a contract for the work, not a claim that its requirements are complete or mutually
consistent. If the intent is ambiguous, preserve that uncertainty and ask for a decision at the
appropriate human gate rather than letting an agent turn a guess into a requirement.

### 2. Sketch: make alternatives safely

Agents may investigate, prototype, and explore alternative implementations in bounded, isolated
execution. Each result is a proposal. Search outputs, model confidence, phase assessments, and
agent consensus are not verification or approval.

When alternatives are explored, retain enough lineage to distinguish independent attempts from
copies that share a prompt, context, code, or evidence. Parallel count alone does not establish
independence. Exploration remains subject to the work order's budgets, sandbox policy, and existing
authority boundaries.

The default managed line currently uses one governed build/review path. Multi-candidate sketching
is an architectural direction and must not be described as a default runtime capability until its
execution and lineage are wired and retained.

### 3. Critique: test the work against its brief

Compare a candidate with the commission, not just with another agent's preferred answer. Critique
should identify:

- which acceptance criteria have evidence;
- which criteria are unmet, contradicted, or not yet checked;
- material tradeoffs and rejected alternatives;
- whether the evidence is independent and relevant to this candidate;
- changes to the brief or assumptions that would alter the decision.

A reviewer's verdict is itself a recorded judgment. It does not replace the underlying findings or
their evidence.

### 4. Freeze: name the exact object under review

Before final verification, identify the exact candidate bytes and the context that gives a claim its
meaning. For formal claims, that context includes assumptions, model, policy, and claim text; for
empirical claims it includes the relevant environment, inputs, and procedure.

Any change to the candidate or its governing assumptions invalidates evidence whose identity was
bound to the previous object. A frozen candidate is ready to measure; it is not thereby correct,
approved, or publishable.

### 5. Verify: make bounded claims

Correctness is a relation between a particular artifact, a claim, assumptions, and evidence. It is
not one scalar score for the whole project. Record claims separately across behavior, security,
performance, operations, and user acceptance where those dimensions matter.

For every important claim, retain:

| Field | Question answered |
| --- | --- |
| Claim | What exactly are we asserting? |
| Scope | Which artifact, inputs, environment, and conditions does it cover? |
| Method | Proof, model check, bounded check, test, fuzzing, replay, benchmark, or observation? |
| Assumptions | What must hold for the result to apply? |
| Result | What did the verifier actually establish or observe? |
| Uncertainty | What remains unknown about the specification, model, implementation, or environment? |
| Next step | What is the most useful action to reduce that uncertainty? |

Use language that matches the method. A test suite passing means its declared tests passed. Fuzzing
means no counterexample was observed in the declared campaign. A model checker establishes a
property of the explored model and bounds. A theorem checker accepts a proposition under its
definitions and axioms. None alone proves that an arbitrary deployed system meets an unstated user
need.

Formalizability is a method assessment, not a proof result. A formal proof of an abstraction does
not establish that the running implementation refines that abstraction unless a separate refinement
argument and evidence support that bridge. Open-world behavior, subjective quality, and changing
requirements may remain empirical or judgment-based by nature. Preserve those limits instead of
forcing them into a false proof label.

### 6. Steward: decide whether to release

The steward evaluates the frozen candidate against policy, claim evidence, unresolved uncertainty,
and the human decisions required by the commission. Evidence supports a decision; it does not grant
permission. Agents and search controllers cannot approve their own work, publish through a path
outside the factory, or convert a confidence score into authority.

Possible outcomes should remain explicit:

- **continue exploring** when the brief or candidate is still moving;
- **measure** when evidence is incomplete or verifiers disagree;
- **revise** when a counterexample or unmet criterion changes the work;
- **hold** when uncertainty or risk exceeds the release policy;
- **approve/release** only through the configured authority path.

A release record should identify the exact candidate, applicable policy, decision-maker, evidence
root, and any accepted residual uncertainty.

## The atelier's invariants

1. **One lifecycle authority.** Airflow schedules managed lifecycle work. Local search or execution
   helpers cannot become competing lifecycle schedulers.
2. **One authority plane.** Cell identity and epoch fence managed mutation. Search, model output,
   evidence, and phase classification cannot mint permission.
3. **One claim at a time.** Evidence justifies only the bounded claim it actually supports, for the
   candidate and assumptions to which it is bound.
4. **Uncertainty stays visible.** Unknown, empirical-only, bounded, and formally established are
   distinct states; missing assessment is not success.
5. **Judgment stays legible.** Human taste and acceptance may govern release, but must not be
   misrepresented as machine proof.
6. **The artifact outlives the worker.** Preserve intent, decision lineage, evidence, and release
   state in durable factory records; execution sessions remain disposable.
7. **The public description matches the runtime.** Proposed capabilities remain labeled as proposed
   until the managed path actually enforces and retains them.

## Current boundary and graduation path

The repository already has the core ingredients: Airflow-managed stages and human gates, durable
Factory Cells and epochs, candidate/evidence identity, phase control marked search-only, and pure
formal-claim and justification contracts. Some formal contracts are explicitly recorded as not yet
wired to managed readiness or promotion. The default managed blueprint remains a single build and
review cell, not a parallel candidate atelier.

The next engineering milestone is one end-to-end, managed release record that ties together the
accepted commission, exact candidate identity, claim-level verification and uncertainty, and the
existing authority decision. It must preserve the distinction between evidence and permission and
must survive process replacement. Future multi-sketch execution should add retained parentage and
independence evidence without creating a second scheduler.

Graduate each part only when the implementation and retained artifacts demonstrate it. Until then,
this document is the workflow north star, not a claim that the runtime has already closed those seams.

## Relation to existing architecture

- [Liquid methodology](liquid-methodology.md) governs entropy, search, and convergence.
- [Managed lifecycle](lifecycle.md) defines Airflow stages, gates, and execution boundaries.
- [Formal correctness](formal-correctness.md) defines scoped claims, evidence, formal limits, and
  refinement obligations.
- [Future Factory](future-factory.md) describes the longer-term search and execution architecture.
