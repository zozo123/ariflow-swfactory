# Immutable source snapshots

Candidate lineage is only reproducible if execution receives the same bytes the lineage names.
`swfactory source-snapshot` turns one Git revision into an immutable, content-addressed tar archive
before compute starts.

This is inspired by the source-snapshot discipline in
[alphaXiv/OpenResearch](https://github.com/alphaXiv/OpenResearch): experiments run from an archive
of the recorded commit rather than from a mutable worktree. The implementation here is native to
the Liquid Software Factory and does not add a scheduler, provider, or promotion authority.

## Contract

```text
mutable checkout
     |
     | resolve REV^{commit}
     v
exact Git commit
     |
     | git archive --format=tar
     v
temporary archive
     |
     | SHA-256 + byte size
     v
.factory/source-snapshots/objects/<sha256>.tar
     |
     +--> verified commit-keyed index
     |
     +--> candidate/provider staging
```

The guarantees are deliberately narrow and testable:

1. **The revision resolves to a commit object before any cache directory is created.**
2. **Only committed files enter the archive.** Dirty tracked edits and untracked files are absent
   because `git archive` reads the Git object database, not the worktree.
3. **The object name is its SHA-256 digest.** The retained receipt also records its byte size and
   exact commit SHA.
4. **Cache reuse is verified, never assumed.** A commit-keyed index can avoid rebuilding an archive,
   but the referenced object is re-hashed before it is returned.
5. **Corruption is quarantined.** A cached object whose bytes no longer match its receipt is moved
   under `quarantine/` and rebuilt from the recorded commit.
6. **A branch moving later does not move an existing experiment.** Callers can pass the recorded
   commit SHA directly and receive that historical tree.

## Operator use

```sh
uv run swfactory source-snapshot . --revision HEAD
uv run swfactory source-snapshot . --revision <candidate-sha> --json
```

The JSON receipt is suitable for candidate/run evidence:

```json
{
  "cache_key": "source-snapshot:...",
  "commit_sha": "...",
  "format": "tar",
  "path": ".../objects/<sha256>.tar",
  "schema_version": 1,
  "sha256": "...",
  "size_bytes": 123456
}
```

The archive is execution input, not approval. Airflow still owns lifecycle scheduling, Factory Cell
epochs still fence mutation, candidate evaluation still produces evidence, and a human gate still
owns promotion.

## Why not archive the current directory?

Copying a workspace recursively makes the run depend on editor buffers, generated files, ignored
files, local caches, and whatever happened between planning and provider startup. Recording a Git
SHA while executing different bytes is worse than recording no SHA because the evidence looks
reproducible when it is not.

The snapshot path makes the identity physical: the exact commit becomes an exact byte stream with a
digest that can be checked before and after transport.

## Current boundary

This slice provides the trusted snapshot producer, verifier, cache, CLI, and tests. Provider
adapters can consume the archive as their staged source input; adding that staging to a particular
provider must keep the same receipt and verification semantics. A provider must not silently fall
back to copying the live workspace when a snapshot is unavailable.
