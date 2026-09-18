# Immutable snapshot replay capsules

An experiment is easier to trust when another operator can reconstruct the same recorded source bytes, run the same explicit command recipe, and verify the retained outputs.

This layer builds on the OpenResearch idea that experiments should carry their executable context with them. In this factory, replay remains an **evidence primitive**: it does not schedule Airflow tasks, publish branches, select candidates, or approve promotion.

## Capsule

A replay capsule contains:

- `recipe.json` — exact argv vector, relative cwd, explicit non-secret environment, timeout, and recipe digest;
- `stdout.bin` / `stderr.bin` — retained command output;
- `receipt.json` — source commit, source snapshot digest/size, recipe digest, exit/timeout state, duration, stream digests, and receipt digest.

```text
SourceSnapshot(commit, sha256, bytes)
          |
          v
  safe tar extraction
          |
          v
SnapshotRunRecipe
 argv + cwd + env + timeout
          |
          v
 local process (no shell)
          |
          v
stdout/stderr + SnapshotRunReceipt
          |
          v
 re-hash + source-byte verification
```

## Recipe

Example `recipe.json`:

```json
{
  "schema_version": 1,
  "argv": ["/usr/bin/python3", "-m", "pytest", "tests/test_unit.py"],
  "cwd": ".",
  "env": [["PYTHONHASHSEED", "0"]],
  "timeout_s": 300.0
}
```

The command is executed directly as argv. There is no shell interpolation.

Environment is empty unless explicitly declared. Secret-looking keys such as `*_TOKEN`, `*_PASSWORD`, `*_SECRET`, `*_API_KEY`, and access/private-key names are refused so a replay manifest cannot silently become long-lived credential storage.

## Run

First create a source snapshot receipt:

```sh
uv run swfactory source-snapshot . --revision <commit> --json > source.json
```

Then execute the recipe:

```sh
uv run swfactory snapshot-replay run source.json recipe.json .factory/replays/run-1
```

Verify retained evidence and, optionally, re-bind it to the original snapshot bytes:

```sh
uv run swfactory snapshot-replay verify \
  .factory/replays/run-1 \
  --source-receipt source.json
```

## Fail-closed boundaries

Replay refuses:

- changed or missing source-snapshot bytes;
- archive members with absolute or `..` paths;
- symlinks, hardlinks, devices, fifos, and other non-regular archive members;
- cwd traversal outside the extracted snapshot;
- empty/NUL-bearing argv entries;
- duplicate or invalid environment keys;
- secret-looking environment keys;
- timeouts outside the supported range;
- nonempty or symlinked evidence destinations;
- changed recipe, receipt, stdout, or stderr bytes during verification.

## What this proves

A verified capsule proves that the retained receipt, recipe, source archive identity, and retained output bytes agree with each other. It makes source/command/output provenance inspectable and replayable.

## What this does not prove

This local adapter is **not a sandbox** and is not a complete hermetic toolchain capture.

- The host executable, kernel, shared libraries, CPU, clocks, and external services are not content-addressed by this receipt.
- A command may still have effects outside the extracted tree if the host grants them.
- Timeout bounds the parent process wait; it is not a universal remote-process lifecycle primitive.
- Untrusted candidate code should continue to execute in the factory's isolated sandbox/provider paths.

A later provider integration can consume the same recipe and snapshot digest while adding an image/runtime digest. That is the path from replayable source evidence to fully reproducible execution environments without inventing a second scheduler.
