# Commit-bound candidate execution recipes

Immutable source snapshots answer **what bytes ran**. Candidate evidence bundles answer **what happened**. A reproducible verification also needs to answer **what exact command and execution envelope ran those bytes**.

Inspired by alphaXiv/OpenResearch's rule that remote jobs read their run manifest from the same recorded revision they execute, the factory can load `.swfactory/candidate-run.json` directly from an exact Git commit.

## Invariant

```text
candidate source SHA
      |
      +--> git archive -----------------> source snapshot digest
      |
      +--> git show SHA:.swfactory/candidate-run.json
                         |
                         v
                 canonical recipe digest
                         |
                         v
                 CandidateManifest digest
```

A dirty orchestrator checkout cannot change the recipe because the loader reads the Git object, not the working-tree file.

## Recipe shape

```json
{
  "schema_version": 1,
  "argv": ["uv", "run", "pytest", "-q"],
  "cwd": ".",
  "timeout_s": 1800,
  "resources": {"cpus": 4, "memory_mb": 8192},
  "environment": {"PYTHONHASHSEED": "0"},
  "secret_env": ["GH_TOKEN"]
}
```

`argv` is an array, not a shell command. `cwd` must stay inside the repository. CPU, memory, and timeout are bounded. Public deterministic environment values are digest-bound; secret **names** are digest-bound but secret values are never committed to the recipe.

Keys that look secret (`TOKEN`, `SECRET`, `PASSWORD`, `CREDENTIAL`, `API_KEY`) are refused from the public `environment` object and must appear only by name in `secret_env`.

Unknown fields fail closed instead of being silently ignored, so adding a new execution privilege or resource dimension requires an explicit schema change.

## Evidence binding

`bind_execution_recipe(manifest, recipe)` records recipe SHA-256, recipe path, and recipe commit in `CandidateManifest`. The recipe commit must equal the candidate `source_sha`.

Changing only the verification command, environment, timeout, CPU, or RAM changes the candidate manifest digest even when source files are otherwise unchanged. Release attestation therefore cannot accidentally treat two different verification envelopes as the same candidate evidence.

## Authority boundary

The recipe is evidence and execution intent, not authority. It does not schedule Airflow tasks, expose secret values, publish candidate refs, select a winner, merge a pull request, or approve promotion. Provider adapters still decide how to realize the declared bounded resources, and must report mismatches rather than silently substituting a different envelope.
