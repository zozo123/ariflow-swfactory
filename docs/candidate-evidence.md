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

## Campaign invariant

The isolated campaign adapter now seals this bundle **before** it removes a successful candidate
worktree. That makes evidence retention part of the candidate lifecycle rather than an optional
post-processing step:

```text
isolated worktree -> commit -> freeze immutable ref -> snapshot input -> seal evidence -> delete worktree
                                                        |
                                                        +-> failure => candidate refused
```

A successful frozen candidate returned by `worktree_candidate_runner` carries both
`evidence_bundle_path` and `evidence_digest`. If source snapshotting, artifact collection, or
bundle verification fails, the frozen ref is retained for diagnosis but the outcome becomes
`refused` and cannot win selection. The experiment tree records the evidence digest beside the
candidate ref.

An optional artifact collector can name files inside the disposable workspace; the adapter copies
those bytes into the bundle before cleanup. With no collector, the bundle still retains the exact
source identity and Git diff.

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
