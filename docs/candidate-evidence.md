# Candidate evidence in context

A candidate answer should be inspectable without reconstructing its history from unrelated stores.

The candidate worktree path already gives each exploration direction private mutable Git state, and
the frozen candidate ref preserves the exact committed answer after that worktree is destroyed.
This layer retains the **context needed to review that answer** beside it:

```text
exact input SHA
      |
      +--> isolated candidate worktree
                 |
                 +--> committed answer
                 |
                 +--> frozen refs/swfactory/candidates/<id-hash>
                              |
                              +--> changes.patch
                              +--> result.json
                              +--> retained artifacts/*
                              +--> manifest.json + manifest digest
```

This is inspired by the evidence-in-context discipline in
[alphaXiv/OpenResearch](https://github.com/alphaXiv/OpenResearch): logs, diffs, files, results,
and artifacts stay tied to the experiment that produced them.

## What is bound

Every bundle binds:

- the stable candidate id;
- the exact input commit;
- the exact frozen output commit;
- the deterministic immutable candidate ref;
- a binary-capable `git diff` from input to output;
- the candidate result/evaluations document;
- any explicitly retained regular-file artifacts;
- SHA-256 and byte length for every retained file;
- one digest over the canonical manifest.

The bundle directory is derived from a hash of the candidate id. Raw candidate text never becomes
a filesystem path.

## Write-once semantics

A successful candidate may be replayed, but its evidence cannot be rewritten.

If a bundle already exists, reuse succeeds only when:

1. the frozen candidate ref still names the recorded output;
2. the output still descends from the recorded input;
3. the manifest self-digest is valid;
4. every retained artifact still matches its digest and size;
5. the requested result document is identical; and
6. the requested artifact set and bytes are identical.

A replay with a different result or artifact set is evidence drift and is refused.

## Campaign integration

`worktree_candidate_runner(...)` now creates this evidence automatically for every successful
frozen candidate. By default it is retained next to the worktree root under
`candidate-evidence/`; callers can supply another `evidence_root`.

`CandidateOutcome` records:

- `candidate_ref`;
- `candidate_evidence_manifest`;
- `candidate_evidence_digest`.

The experiment-tree node also retains the evidence digest. This keeps tree lineage compact while
the outcome points to the operator-readable bundle.

A candidate that fails before freeze gets no frozen ref and no answered-candidate evidence bundle.
Infrastructure failure remains repairable execution state rather than a fake experiment answer.

## Authority boundary

Candidate evidence does not decide the winner and does not promote anything.

- Airflow remains the lifecycle scheduler.
- Candidate ranking remains deterministic and evidence-driven.
- The frozen candidate ref is the Git identity.
- Branch protection / human promotion remains the release authority.

The evidence bundle makes a decision inspectable; it does not make the decision.
