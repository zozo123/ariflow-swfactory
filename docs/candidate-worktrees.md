# Candidate worktrees

Parallel candidate exploration needs mutable state, but candidates must not share that state.

The local repository runtime therefore supports one detached Git worktree per candidate:

```text
immutable input commit / source snapshot
               |
               +--------------------+
               |                    |
               v                    v
       candidate worktree A  candidate worktree B
          (mutable)             (mutable)
               |                    |
          commit answer          fail/repair
               |
               v
refs/swfactory/candidates/<candidate-id hash>
        immutable answered revision
               |
        disposable worktree removed
```

## Invariants

A candidate workspace:

- starts detached at the exact recorded input commit;
- has a path derived from a hash of the candidate id, never raw user text;
- is physically distinct from sibling worktrees;
- cannot be frozen while it contains uncommitted changes;
- cannot be frozen without advancing beyond the input;
- cannot freeze an output that does not descend from the recorded input;
- freezes with an atomic create-only Git ref, so an answered candidate cannot be rewritten;
- refuses a worktree receipt whose path was replaced by a symlink;
- can be removed without deleting the frozen candidate ref.

The immutable ref is evidence. The worktree is disposable execution state.

## Relationship to source snapshots

Source snapshots and candidate worktrees solve different halves of reproducibility.

- A **source snapshot** seals the exact committed bytes handed to execution.
- A **candidate worktree** gives one local candidate a private mutable checkout rooted at that commit.
- A **frozen candidate ref** preserves the committed answer after the mutable workspace is destroyed.
- The **experiment tree** records which frozen answers were siblings and which winner became the next parent.

Remote providers do not need to use Git worktrees internally. They can consume the same source snapshot and
return a committed output revision. This module is a local repository-runtime primitive, not a provider support claim.

## Authority boundary

Candidate worktrees do not schedule, approve, publish, or promote anything.

Airflow remains the managed lifecycle scheduler. Factory Cells remain durable work identity. Candidate selection
and verification remain separate, and human / branch-protection gates remain the promotion authority.

## Inspiration

This adapts the private-worktree discipline from
[alphaXiv/OpenResearch](https://github.com/alphaXiv/OpenResearch): parallel directions get isolated Git state,
while successful answers are recorded before their disposable execution workspace disappears.
