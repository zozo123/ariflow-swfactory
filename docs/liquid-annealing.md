# Liquid relaxation and annealed review

The Liquid Software Factory deliberately creates implementation freedom early, then destroys
uncertainty before promotion. This document describes the **implemented review-convergence slice**
of that method.

The design borrows a useful idea from modern agentic software-factory workflows: generation is only
one part of delivery. Independent review, bounded repair, candidate-bound verification, retained
evidence, and a separate promotion authority matter at least as much. The reference that motivated
this refinement was the Pragmatic Engineer article on OpenAI's software-factory workflow:
https://newsletter.pragmaticengineer.com/p/openai-software-factory

The adaptation is native to this repository. It does not copy another orchestration model and it
does not create another scheduler.


## Factory-wide phase contract

The review-local annealer now uses the canonical phase names from `swfactory.phase_control` and
records a control mode in each annealing state. Review remains intentionally narrower than the
factory-wide phase controller: it cannot expand the Airflow graph or grant authority. Its posture is
`anneal` for material defects, `measure` for incomplete/critical review, `verify` for crystal,
`perturb` for glass and `drain` for jammed. See [phase-aware control](phase-control.md).

## One line, one authority

```text
issue / durable intent
        |
        v
 intent -> human gate -> spec -> plan -> human gate
                                      |
                                      v
                         bounded Plan.work exploration
                                      |
                                      v
                              candidate + tests
                                      |
                                      v
                 +---------------- review ----------------+
                 |                                         |
                 | correctness  verification  risk         |
                 |      \          |          /            |
                 |       deterministic fan-in              |
                 |               |                         |
                 |     blocker or major remains?           |
                 |        | yes              | no          |
                 |        v                  v             |
                 |   fix -> re-test ----> anneal           |
                 |        ^              / crystal?        |
                 |        +-------------/                  |
                 +----------------|------------------------+
                                  v
                           deliver evidence PR
                                  |
                                  v
                           HUMAN merge decision
```

Apache Airflow still owns the lifecycle stage graph. A Factory Cell still owns durable issue x target
identity and epoch fencing. The annealer owns neither. It runs *inside* Airflow's existing `review`
stage and can only return evidence plus `ok`/`blocked`.

## What was adapted

| General software-factory idea | Liquid Software Factory implementation |
| --- | --- |
| Start from outcome and context | issue -> `intent.md` -> `spec.md` -> `plan.md`; accepted inputs freeze the epoch |
| Let agents implement | bounded `Plan.work` execution in a disposable Cell |
| Review from several angles | correctness, verification, and risk specialist lanes |
| Feed defects back to implementation | blocker **and major** findings enter the bounded `fix` loop |
| Re-run CI after a repair | every relaxation repair executes the target verification command before re-review |
| Gate risk explicitly | risk-sensitive interface penalty is recorded, but hard software invariants decide readiness |
| Retain a delivery trail | `review.json`, `annealing.json`, test evidence, accepted-input digest, approvals |
| Separate deployment/promotion | `deliver` publishes a PR; the factory does not merge or deploy it |
| Feed production learning back | incidents, regressions, and follow-up work return through ordinary issue/intake paths |

The last two rows are deliberate non-features: an automated reviewer is not allowed to turn a good
score into self-promotion.

## Specialist fan-out

The managed Liquid line selects `REVIEW_LIQUID.md`. Its exact content is part of the Cell epoch's
accepted-input digest. The review task invokes three read-only lanes over the same candidate:

- `correctness` — invariants, state transitions, concurrency, recovery, API/data contracts;
- `verification` — test quality, failure-path coverage, plan fidelity, candidate-bound evidence;
- `risk` — security, capability widening, external effects, lifecycle authority, rollback and cost.

The lanes are **independent but currently executed sequentially** inside one Airflow task. This is
intentional. The current managed work path owns one governed workspace and one call ledger; adding a
thread pool merely to look parallel would make accounting and provider behavior harder to reason
about. Once a provider can prove isolated review execution and safe concurrent accounting, the
inner loop can become parallel without changing the Airflow DAG, Cell identity, fan-in algorithm, or
evidence schema.

Fan-in is deterministic. Findings are keyed by `(file, line, normalized title)`. If two lanes report
the same defect at different severities, the stronger severity survives. The blueprint's nit cap is
then applied.

## Relaxation loop

The base review contract reserves `request_changes` for blockers. The Liquid line adds a stronger
host-owned convergence rule:

```text
material_defects = blockers + majors
```

If `material_defects > 0` and `max_review_fixes` is not exhausted:

1. render the existing write-scoped `fix` prompt with only the material findings;
2. let the coding agent repair the candidate under the existing protected-path policy;
3. let the trusted factory commit the result;
4. run the target's declared verification command;
5. add a synthetic blocker if that verification is red;
6. run all three specialist lanes again.

Minor and nit findings stay visible but do not cause another paid repair round by themselves.

The loop is bounded by the existing blueprint budget and `max_review_fixes`; the annealer does not
create a new budget surface or mutate `ctx.cfg`.

## Annealing observables

`src/swfactory/liquid_annealing.py` records a small, dimensionless diagnostic model. It is meant to
make convergence explainable, not to pretend software is literal matter.

For one retained review round:

```text
defect_energy =
    8 * blockers
  + 3 * majors
  + 0.75 * minors
  + 0.10 * nits
  + 8 if tests are red

breadth = min(changed_files / 20, 1)
risk    = min(risky_files / 6, 1)
surface_penalty = min(1, 0.65 * breadth + 0.35 * risk)

temperature = min(1, defect_energy/(defect_energy+5) + 0.25*surface_penalty)
beta        = min(20, 1/max(temperature, 0.05))
order       = max(0, 1-temperature)

driving_force = bounded_mean(tests_green, lane_coverage, 1/(1+defect_energy))
barrier       = (surface_penalty + 0.05)^3 / driving_force^2
crossing      = exp(-barrier / max(temperature, 0.05))
```

These values explain *why* a candidate looks hot, broad, risky, stalled, or ordered. They never
replace the ordinary readiness predicates.

## Phases

| Phase | Retained evidence meaning |
| --- | --- |
| `gas` | no material defect, but ordinary crystallization prerequisites are incomplete |
| `liquid` | one or more major findings remain and relaxation can still progress |
| `critical` | blocker exists or verification is red |
| `glass` | the same material-defect signature survived another repair/review round |
| `jammed` | repair budget is exhausted while verification is still red |
| `crystal` | green verification, all lanes completed, zero blockers, zero majors |

A phase is a diagnostic label, not an authorization state.

## Nucleation barrier and risk

The existing non-equilibrium doctrine proposes the dimensionless nucleation proxy

```text
barrier ~ surface_penalty^3 / driving_force^2
P ~ exp(-barrier / T_eff)
```

The implemented annealer uses that proxy only as retained explanation. Trust-boundary-heavy or very
wide diffs increase `surface_penalty`; clean verification and complete review increase
`driving_force`. A high crossing probability still cannot override a major finding, a red test, a
human gate, an epoch fence, branch protection, or the human merge decision.

## Evidence

An annealed Liquid review adds `docs/factory/<issue>/annealing.json` beside the existing review
evidence. The artifact contains:

```json
{
  "schema_version": 1,
  "strategy": "liquid-relaxation-annealing",
  "authority": "advisory-review-only",
  "changed_files": ["..."],
  "risky_files": 1,
  "specialist_lanes": ["correctness", "verification", "risk"],
  "trace": [
    {
      "round": 0,
      "lanes": [],
      "findings": [],
      "state": {"phase": "liquid", "crystallized": false}
    }
  ],
  "final": {"phase": "crystal", "crystallized": true}
}
```

`review.json` retains the ordinary finding list and also carries the final annealing state and final
lane receipts. Downstream delivery continues to use the review stage's authoritative status. If the
candidate does not crystallize before the repair budget is exhausted, the stage is `blocked` and a
blocked evidence PR may be published; publication is not acceptance.

## Failure behavior

| Condition | Result |
| --- | --- |
| specialist call fails | review task fails; no invented missing lane |
| lane returns blocker | critical; repair while budget remains |
| lane returns major | liquid; repair while budget remains |
| repair makes tests red | synthetic blocker; next round cannot crystallize |
| same material defects survive | glass diagnostic; continue only within the bound |
| red tests at final round | jammed + blocked |
| major remains at final round | blocked even if `Review.verdict` itself is `approve` |
| all lanes clean, tests green | crystal + review `ok` |

## Verification

The hermetic tests cover the convergence seam without paid model calls:

```sh
uv run --group dev pytest tests/test_liquid_annealing.py
uv run --group dev ruff check src/swfactory/liquid_annealing.py \
  src/swfactory/stage_registry.py tests/test_liquid_annealing.py
uv run --group dev ruff format --check src/swfactory/liquid_annealing.py \
  src/swfactory/stage_registry.py tests/test_liquid_annealing.py
```

The full repository gates remain authoritative. In particular, the Airflow parity checks verify the
DAG still has one `review` lifecycle task, and the control-plane gate protects the files that define
annealing semantics.

## Boundary with the broader physics doctrine

`docs/non-equilibrium-factory.md` contains a much larger research program: multi-current control,
maximum-entropy model mixing, hysteresis, non-equilibrium work relations, rare-event control, and
other ideas. This implementation does **not** claim that whole controller exists. It implements only
one narrow, testable transfer: review relaxation plus phase/nucleation diagnostics around a real
software readiness predicate.

That boundary is intentional: physics vocabulary belongs in production only when it leaves behind a
measurable variable, a falsifiable rule, and a safer control decision.
