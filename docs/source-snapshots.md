# Immutable source snapshots

Candidate execution is only reproducible when the bytes given to a sandbox are tied to the same
recorded Git revision that later evidence names.

The factory therefore has a provider-neutral source snapshot primitive:

```text
recorded commit SHA
       |
       v
git archive <exact commit>
       |
       v
SHA-256 + byte size
       |
       v
content-addressed private tar
       |
       +--> provider / sandbox execution input
       |
       +--> candidate manifest evidence
```

## Invariant

A candidate may name a source revision, but execution evidence is stronger when it also names the
immutable archive bytes derived from that revision. The archive is created with `git archive`
from the resolved commit object. Dirty tracked edits and untracked files are not inputs.

The snapshot receipt records:

- the full resolved commit SHA;
- SHA-256 of the archive;
- exact archive size;
- content-addressed archive path;
- whether the archive was a verified cache hit.

An existing content-addressed entry is never trusted by name alone. Its digest and size are
recomputed before reuse. Mismatch is a hard failure.

## Authority boundary

Source snapshots do not schedule work, select candidates, merge branches, or promote releases.

- Airflow remains the only lifecycle scheduler.
- Factory Cells remain the durable issue x target authority.
- The experiment tree records candidate lineage.
- Source snapshots bind execution input bytes to one candidate revision.
- Candidate/release evidence binds verification to the exact source snapshot.
- Human/branch-protection gates remain the promotion authority.

## Operator inspection

```sh
uv run swfactory source-snapshot . --revision HEAD --json
```

The command creates or reuses `.factory/source-snapshots/<sha256>.tar`, verifies it, and prints
the receipt. The content-addressed cache is an optimization; the digest is the identity.

## Relationship to OpenResearch

This adapts a useful OpenResearch principle: each experiment run receives an immutable archive of
its recorded commit, so local and remote compute consume the same source. The implementation here
is native to the Liquid Software Factory and its existing Cell, evidence, provider, and promotion
boundaries.
