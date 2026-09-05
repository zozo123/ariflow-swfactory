# Evals — the factory's own regression suite

`CLAUDE.md`, the stage prompts, `REVIEW.md`, `bands.yaml`, the `.claude/` knowledge and hook
sources, and the blueprints steer the agent. The [AI-native SDLC
playbook](https://claude.com/blog/the-ai-native-sdlc-playbook) argues that this configuration
deserves the same regression discipline as code: collect 20–50 real tasks with accepted outcomes,
run them non-interactively when the configuration changes, and gate those changes on the result.

`demo/evals/` is that suite. Each eval is one real, tiny change to the `calc` demo target whose
front matter states the expected outcome in machine-checkable fields. `swfactory.evals` runs the
suite through the same pipeline as `swfactory run`, scores it, and compares the score with the
committed `demo/evals/baseline.json`. **A regression fails the build; a new pass never does.**

Honest status: **7 evals**, not 20-50. The gap is deliberate — every eval must be worth its
seconds — and the suite grows the playbook's way: each production incident earns one, written by
whoever owned the incident.

## Running it

```sh
uv run python -m swfactory.evals                      # scripted agent, local sandbox, ~30 s, no keys
uv run python -m swfactory.evals --only 06-build-loop-sign
uv run python -m swfactory.evals --update-baseline    # re-bless the score (review the diff!)
uv run python -m swfactory.evals --agent claude --sandbox srt   # the real agent, real cost
```

Exit code 1 means a regression (or an unusable baseline). Runs land in
`.factory/evals/<suite-run>/<slug>/{run,work}` — gitignored scratch, one fresh workdir per eval, so
no eval can resume another's stage log and score a pass it did not earn. `scm` is always `local`:
an eval never opens a pull request.

CI is configured to run the suite keylessly in `.github/workflows/evals.yml` (`eval-suite`), whose
`paths` trigger
lists every file that steers the agent — `CLAUDE.md`, `REVIEW.md`, `bands.yaml`, `.claude/**`,
`src/swfactory/prompts/**`, `blueprints/**`, `demo/evals/**`. The keyed `real-demo` and
`evals-islo` jobs (real agent, weekly) are unchanged.

## Layout

```
demo/evals/baseline.json          the committed score: {passed, total, evals: {<id>: {...}}}
demo/evals/<nn>-<slug>/issue.md   the issue the factory is given + the `expect:` block
demo/evals/<nn>-<slug>/spec.md    ScriptedAgent fixtures for this eval, one per stage call
                       plan.json  (spec.md, plan.json, build.1.patch, review.json, fix.N.patch)
```

The eval directory *is* the fixtures dir (`Config.fixtures_dir`), so `ScriptedAgent` replays it and
no eval can accidentally borrow another's patch. Fixture names are
`{stage}.{iteration}.{patch|json|md}` with a `{stage}.{ext}` fallback; iteration >= 2 of the build
loop is stage `fix` (`fix.2.patch`), and a review fix continues at
`fix.<max_build_iterations + k>` — `fix.4.patch` with the default limits. A second review round
reads `review.2.json`, falling back to `review.json`, which is how an eval says "the blocker
survives the fix round".

The fixtures are **authored**, not recorded: an eval's job is to encode the outcome a good run must
produce. Re-record them against the real agent when the target changes:

```sh
uv run swfactory run --issue demo/evals/01-average/issue.md --agent claude --sandbox srt \
  --scm local --approve auto --record demo/evals/01-average
```

## The `expect:` schema

Every key is optional; an unknown key is an error (a misspelt expectation that quietly checks
nothing is the one failure mode a suite must not have). Checks run against the run's `RunReport`
and the workdir it left behind — no model call, no network.

| Key | Type | Passes when |
| --- | --- | --- |
| `stages` | list of stage names | each named stage has `status == "ok"` in the report. Stages left out are unconstrained, which is how a blocked eval omits `review`/`deliver` |
| `tests_pass` | bool | `report.tests_passed` equals it (the last test run of the run) |
| `max_build_iterations` | int | `build_and_test.iterations <= n`, and the stage recorded it. An **upper bound**: converging in fewer iterations is an improvement, never a failure |
| `max_review_fixes` | int | `review.fixes <= n`, same bound semantics |
| `blockers` | int | exact blocker count left by the run (`deliver.blockers`, else `review.blockers`) |
| `label` | string | the delivered PR carries it — read back from the published `pr.md` when it is on disk, so a labelling bug fails the eval rather than a copy of deliver's rule. With a GitHub PR url (no token here) the `deliver` counters stand in |
| `exports` | list of dotted names | e.g. `calc.average`: the module binds the name at top level and, when it declares `__all__`, lists it there. Checked by parsing the source (AST) — agent-authored code is never imported on the orchestrator, only inside the sandbox |
| `artifacts_contain` | map artifact -> substrings | `docs/factory/<id>/<artifact>` exists and contains every substring. The cheap way to assert prose: an under-specified issue must produce a spec with open questions |

```yaml
---
id: EVAL-MEDIAN
title: Add median(values) to calc
labels: [factory]
expect:
  stages: [intent, spec, plan, build_and_test]   # review/deliver are expected to block
  tests_pass: true
  max_build_iterations: 1
  blockers: 1
  label: factory:blocked
  exports: [calc.median]
---
```

## The suite today

| Eval | Shape it pins |
| --- | --- |
| `01-average` | the clean path: a new pure function, first-pass CI, exported and in `__all__` |
| `02-finite-inputs` | an input-validation fix inside existing functions; nothing new exported |
| `03-api-contract` | a docstring/API-contract change, pinned by tests that read `__doc__` |
| `04-underspecified-rounding` | a deliberately vague issue: the correct outcome is a spec whose **open questions** are recorded (checked via `artifacts_contain`) and the narrowest implementation, not a silent guess |
| `05-blocked-missing-tests` | the reviewer must **block** a public function shipped without tests, and the fix round must fail to launder it: `tests/` is `denyWrite` for `fix`, so the PR ships `[BLOCKED]` + `factory:blocked` |
| `06-build-loop-sign` | the bounded build loop: iteration 1 gets the sign wrong, the tests catch it, `fix.2` converges (`max_build_iterations: 2`) |
| `07-review-fix-validation` | a review blocker the fix stage *can* resolve (missing `ValueError` in `src/`): one fix round, then a clean re-review |

## Scoring, baseline and the gate

- `score(results)` -> `{"passed": n, "total": m, "evals": {<id>: {slug, passed, failures}}}`. This
  dict *is* the baseline format, so `--update-baseline` writes exactly what a run produced.
- `baseline_diff(current, baseline)` returns **regressions only**: an eval the baseline records as
  passing that now fails, or one that has vanished from the suite (its expectation stopped being
  checked). A new pass, and a newly added eval that fails, are not regressions — the second is a
  gap you commit deliberately by updating the baseline in the same PR.
- `tests/test_evals.py` asserts the baseline covers exactly the committed suite, so an eval added
  without its baseline entry fails `pytest` rather than being silently ungated.

## Adding an eval

1. `mkdir demo/evals/08-<slug>` and write `issue.md`: a real, tiny, verifiable change to
   `demo/target`, plus the `expect:` block that says what a good run must produce.
2. Add the fixtures for each stage call the run makes (`spec.md`, `plan.json`, `build.1.patch`,
   `review.json`, plus `fix.N.patch` / `review.2.json` if the eval is about a loop). Plan fidelity
   is checked in code: `plan.json`'s `files` must be exactly the files the patches touch, or the
   reviewer raises a fidelity finding you did not intend.
3. `uv run python -m swfactory.evals --only 08-<slug>` until it passes for the right reason.
4. `uv run python -m swfactory.evals --update-baseline`, review the `baseline.json` diff, and
   commit both.

Known limits: the scripted agent replays fixtures, so the suite gates the *pipeline and its
policy* (loops, gates, review contract, protected paths, labels, exports) — not the model's
judgement. Only `--agent claude` measures that, and it costs money and a key; that is what the
weekly `real-demo` / `evals-islo` jobs are for.
