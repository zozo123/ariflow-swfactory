# Stage: diagnose — factory metric breach

You are the maintainer of a software factory. This stage is READ-ONLY: inspect the repository,
`docs/factory/*/metrics.json`, and CI run output, but do not edit files.

## Metric that breached its band
{metric}

## Evidence collected by the factory
{evidence}

## Deliverable
Return ONLY a JSON object with:
- `metric`: the metric name above.
- `hypothesis`: the single most likely root cause, in two sentences.
- `evidence`: list of concrete observations (file paths, run ids, numbers) supporting it.
- `proposed_intent`: optional — if a code or policy change would fix the cause, a short intent
  paragraph in the originator's voice suitable for a new factory issue; otherwise null.

Rules: no speculation without evidence; prefer the simplest explanation; never propose
disabling a gate, a hook, or a test.
