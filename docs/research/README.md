# Research

Material that is **not in the product path** and holds no lifecycle or mutation authority.

Nothing here is imported by `src/swfactory/`. The Liquid spec checker
(`python -m swfactory.liquid_spec`) resolves every `runtime_anchor` in
[`config/liquid-spec.yaml`](../../config/liquid-spec.yaml) to real code, and the research families
in that file are recorded with `state: declared` and `support: unsupported` precisely so that
declared exploration can never read as shipped capability.

## What moved here, and why

The non-equilibrium / statistical-mechanics layer
([`docs/non-equilibrium-factory.md`](../non-equilibrium-factory.md)) and the Ocean120 / Phase240 /
StatMech360 wave taxonomy. Their doctrine was already disciplined — advisory only, measurable
variables, no lifecycle authority — but the *code* was shaped like a decider even though nothing
called it:

- `non_equilibrium.evaluate_factory` returned a `ControlAction` from
  `{PROMOTE, THROTTLE, RECOVER, CLEANUP, ...}`, and `mix_pitches` biased PROMOTE/VERIFY utilities
  by a classified `Phase`.
- `physics_wave_runtime.execute_wave` overrode a concern's action by wave.

Neither was reachable from stages, control, backend, herd, metrics, dags or scripts — the only
importer of `non_equilibrium` was its own test. So this is a removal of vocabulary, not of
behaviour. The one idea worth keeping is the intuition, and it is one line:

> Create entropy where exploration benefits from it; destroy entropy before promotion.

That belongs in the methodology, and it is already there. It does not need Jarzynski.

## The bar to come back

A borrowed equation earns its way into the product path by making a **falsifiable prediction about
a factory control decision** that a boring model does not already make. Concretely, it must:

1. name the measured variables it consumes, all of which already exist in `metrics`;
2. predict a specific decision (admit / throttle / promote / recover) *before* the run, not after;
3. beat a documented boring baseline on recorded runs — not match it, beat it;
4. stay advisory: the prediction is an input to a human or to admission control, never an
   authority that can mutate a repository.

Absent (3), the honest description is "an interesting analogy", and analogies belong here.

## The boring baseline it has to beat

If any theory is going to replace intuition here, the candidate is **queueing theory**, not
thermodynamics. Utilization vs latency and Little's law are actually predictive for the two
variables that matter operationally — sandbox saturation and queue depth — and they are
well understood, cheap to compute, and falsifiable in a week.

The production control surface should be stated in these terms:

| variable | status |
| --- | --- |
| queue depth | derivable from Airflow's mapped-task state |
| failure rate | in `metrics` |
| retry rate | `operations.jsonl` records attempts separately from results |
| in-doubt operations | first-class in the operation journal |
| cleanup debt | `cleanup_receipt` |
| cost | per-stage and per-job budgets are already recorded |
| time-to-evidence | derivable from stage timestamps |
| sandbox saturation | derivable from `max_parallel_jobs` vs active cells |
| time-to-merge | **not available** — no merge timestamp is recorded anywhere |
| cancellation lag | **not available** — needs a schema change, not a query |

The last two are a schema gap, and naming them is more useful than modelling them.
