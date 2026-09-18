# Candidate evidence bundles

OpenResearch makes evidence inspectable in the context of the experiment that produced it. The
factory now applies that idea at the candidate boundary: a frozen candidate can retain its exact
Git diff and named logs/results under one digest-bound manifest.

This complements, rather than replaces, the existing evidence layers:

- source snapshots bind execution input to an exact committed tree;
- candidate worktrees isolate mutable exploration and freeze the answered output SHA;
- **candidate evidence bundles** bind that input/output pair to the diff and retained run evidence;
- experiment trees compare candidate siblings and carry the selected answer forward;
- candidate readiness and branch protection remain the promotion boundary.

## Bundle shape

```text
candidate evidence/
├── manifest.json       machine-verifiable identity and digests
├── candidate.diff      exact --binary --full-index input..output diff
├── RESULT.md           human-readable projection of the manifest
└── artifacts/
    ├── 000-<digest>    copied log/result/benchmark bytes
    └── 001-<digest>
```

The manifest binds:

- candidate logical id;
- exact input and output Git SHAs;
- immutable `refs/swfactory/candidates/...` ref;
- source snapshot SHA-256 and byte size;
- retained binary diff digest and size;
- every named artifact's digest, size, and retained relative path;
- a canonical manifest digest.

The source archive itself is not duplicated into every evidence bundle. Its digest and size are
recorded, while the content-addressed source-snapshot store remains the source-byte retention
layer.

## Build

First freeze a candidate and capture the source-snapshot receipt. Then:

```sh
uv run swfactory candidate-evidence build \
  .factory/candidates/cand.frozen.json \
  .factory/candidates/source.json \
  .factory/candidates/cand-evidence \
  --repo . \
  --artifact agent-log=.factory/runs/cand/agent.log \
  --artifact benchmark=.factory/runs/cand/benchmark.json
```

Artifacts are copied into the bundle. The manifest never points at the caller's mutable original.

## Verify

```sh
uv run swfactory candidate-evidence verify \
  .factory/candidates/cand-evidence --repo .
```

Verification fails closed when:

- `manifest.json` changed without a matching canonical digest;
- a retained diff/log/result is absent, replaced by a symlink, or has different bytes;
- the immutable candidate ref no longer resolves to the recorded output SHA;
- the bundle structure contains unsafe relative paths or duplicate artifact names.

Build additionally refuses a source snapshot whose commit differs from the candidate input.

## Why keep the diff?

Two candidates can both pass the same test suite while making radically different changes. A
candidate score without the exact patch makes selection opaque. Retaining the binary full-index
diff lets a reviewer or later evaluator inspect *what changed* beside *what passed* without
reconstructing a deleted worktree.

## RESULT.md

`RESULT.md` is intentionally a projection, not an authority. It makes the important identity and
artifact digests easy to read in a terminal or UI. `manifest.json` plus the retained bytes are the
verifiable evidence.

## Authority boundary

A bundle cannot make a candidate promotable. It records evidence for selection and review. Airflow
still owns lifecycle scheduling; the Factory Cell owns durable work identity; candidate selection
remains deterministic; human/branch-protection gates remain the promotion authority.


## Promotion-window retention

A verified bundle can be imported into the factory-owned retention store:

```sh
uv run swfactory candidate-evidence retain \
  .factory/candidates/cand-evidence \
  --repo . \
  --ttl-hours 168
```

Retention is content-addressed by the bundle's canonical manifest digest. The store owns two
namespaces:

```text
.factory/candidate-retention/
├── objects/<manifest-sha256>/   copied candidate evidence bundle
└── leases/<manifest-sha256>.json
```

Re-retaining the same bundle never shortens its lease. A selected or released candidate can be
made immune to expiry collection without granting it any promotion authority:

```sh
uv run swfactory candidate-evidence pin sha256:<digest>
```

Pinning means only **retain these bytes**. It does not mean approved, selected, merged, or released.

Garbage collection is explicit and inspectable:

```sh
uv run swfactory candidate-evidence gc --dry-run --json
uv run swfactory candidate-evidence gc
```

The sweep removes only bundles whose leases are both expired and unpinned. Malformed leases,
missing objects, redirected/symlinked object paths, and other ambiguous states are reported and
left untouched rather than guessed through. The deletion target is reconstructed from the
validated digest under the factory-owned object root; no persisted arbitrary path is ever used as
a deletion authority.

This closes the storage side of the Liquid artifact invariant. The capability remains
**experimental** until the release/promotion path itself binds and checks these retention leases in
a release drill.
