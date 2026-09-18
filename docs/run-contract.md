# Fixed experiment run contract

Candidate branches are only scientifically or operationally comparable when they are measured the same way. The factory therefore supports a canonical **run contract** for candidate campaigns.

This adapts one of OpenResearch's cardinal rules: once an experiment tree starts, its run command and execution environment are fixed. The Liquid factory keeps that rule as evidence and validation; it does not move scheduling authority out of Airflow.

## Contract

A `RunContract` records four things:

- `argv`: the exact command as argument tokens, not a shell string;
- `cwd`: a repository-relative working directory;
- `runtime`: a stable harness/runtime identity, preferably an immutable image or toolchain digest;
- `env_fingerprint`: a trusted fingerprint describing the execution environment without storing raw secret values.

The canonical JSON form is SHA-256 hashed. The digest is part of each candidate's logical identity.

```text
command argv -\
cwd          --+
runtime      --+-- canonical JSON -- SHA-256 -- contract digest
env identity -/                              |
                                              +-- candidate identity
                                              +-- campaign report
                                              +-- experiment round
```

## Why environment values are not stored

The contract is a comparison boundary, not a secret store. API keys, tokens, credentials, and other raw environment values must remain outside it. `env_fingerprint` is supplied by trusted orchestration and may represent a sanitized environment manifest, image digest, lockfile/toolchain identity, or another stable non-secret description.

A changed fingerprint means a changed measurement contract.

## Campaign rule

All siblings in one campaign must carry the same contract digest. Mixing contracts fails before candidate execution.

A contracted campaign report emits the canonical `run_contract`, its digest, and the same digest on the corresponding `experiment_round`.

The candidate logical ID includes the digest, so replay/deduplication cannot alias two otherwise identical requests that were measured differently.

## Tree rule

All rounds in one stacked experiment tree must have exactly the same contract digest.

This rejects both forms of silent drift:

```text
round 0: command A / env A
round 1: command B / env A    -> reject

round 0: contracted
round 1: no contract          -> reject
```

Legacy trees where every round predates run contracts still load. The invalid state is a mixed tree where the measurement rule changes midway.

## Authority boundary

The run contract does not execute the command, schedule candidates, choose a winner, publish a ref, or approve promotion. It only binds the measurement recipe to experiment identity and lineage.

Airflow remains lifecycle authority. Candidate worktrees remain disposable execution state. Frozen candidate refs and evidence bundles remain execution evidence. Human/branch protection remains promotion authority.
