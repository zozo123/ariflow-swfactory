# Replay capsules from committed recipes

The factory can now reconstruct one candidate verification from **two immutable
objects at the same Git commit**:

1. the content-addressed source snapshot; and
2. `.swfactory/candidate-run.json`, loaded from that exact commit.

This extends the OpenResearch-style invariant from "remember the experiment" to
"make the recorded experiment directly replayable."

```text
recorded Git commit
   |             |
   |             +--> committed execution recipe
   v
source snapshot
   |             |
   +------ same commit ------+
                             v
                    local replay capsule
                    /       |        \
               stdout    stderr    receipt
                  |          |         |
                  +---- SHA-256 -------+
```

## Run

Create a snapshot receipt:

```sh
uv run swfactory source-snapshot . --revision <commit> --json > source.json
```

Replay the recipe stored in that commit:

```sh
uv run swfactory snapshot-replay . source.json .factory/replays/run-1
```

Verify the retained capsule against both the source archive and the recipe still
stored in the recorded Git object:

```sh
uv run swfactory snapshot-replay-verify \
  . source.json .factory/replays/run-1 --json
```

The recipe path defaults to `.swfactory/candidate-run.json`; replay never accepts
a free-floating mutable recipe file.

## Retained evidence

The replay directory contains:

- `recipe.json`: the commit-bound recipe and its canonical digest;
- `stdout.bin` and `stderr.bin`: exact retained process streams;
- `receipt.json`: source commit/digest/size, recipe commit/path/digest,
  requested CPU/RAM, resolved executable path and executable SHA-256,
  exit/timeout state, duration, and stream digests.

Verification re-hashes every retained stream and the executable. With `--repo`
through the CLI it also reloads the recipe from the recorded Git commit and
compares its digest.

## Fail-closed rules

Replay refuses:

- a source snapshot and recipe from different commits;
- changed or missing source archive bytes;
- archive traversal paths;
- archive symlinks, hardlinks, devices, fifos, and other non-regular members;
- cwd traversal outside the reconstructed snapshot;
- secret-bearing recipes in the local adapter;
- missing host executables;
- tampered recipe, receipt, stdout, stderr, or executable bytes;
- nonempty or symlinked evidence destinations.

## Authority and isolation boundary

This is **replay evidence**, not a scheduler, promotion gate, or security sandbox.

Airflow still owns lifecycle scheduling. Human/branch-protection policy still
owns promotion. Untrusted candidate code belongs in the factory's isolated
provider paths.

The committed recipe declares CPU and RAM, but this local adapter records
`resource_enforcement = declared-not-enforced-local` rather than pretending to
enforce provider-grade quotas. That distinction is part of the signed replay
receipt. A provider adapter can later consume the same source and recipe digests
while adding a runtime/image digest and real quota enforcement.

Likewise, `secret_env` records secret **names** only. Local replay refuses such a
recipe instead of asking operators to put secret values into a replay manifest
or shell history.
