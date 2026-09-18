# Isolated local candidate worktrees

The Liquid factory's candidate campaigns need sibling implementations to start from the same recorded revision without sharing mutable filesystem state. The local experimental adapter now uses detached Git worktrees for that boundary.

This is inspired by [alphaXiv/OpenResearch](https://github.com/alphaXiv/OpenResearch), where parallel research directions get independent worktrees. The adaptation here preserves the factory's existing authority model: a worktree is disposable compute state, not a branch-publishing or promotion authority.

## Shape

```text
                   exact input SHA
                         |
          +--------------+--------------+
          |              |              |
          v              v              v
   detached WT A   detached WT B   detached WT C
      repair          rethink          scratch
          |              |              |
      commit A         commit B         commit C
          |              |              |
          +--------- deterministic fan-in --------+
                         |
                  evidence + selection
```

Each candidate path is derived from a digest of the candidate's logical identity. Candidate text is never interpolated into a filesystem path.

## Recording a result

A candidate output is recordable only when its detached worktree still exists, `git status --porcelain --untracked-files=all` is empty, and `HEAD` resolves to an exact commit.

The campaign adapter treats the worktree's observed `HEAD` as authoritative. If a worker reports a different `output_head`, the candidate fails rather than allowing claimed metadata to disagree with Git state. An unchanged `HEAD` can be observed, but existing experiment-tree selection refuses it because `output_head == input_head`.

## Cleanup and failure

`worktree_candidate_runner(...)` removes the disposable worktree in a `finally` path, including runner errors and dirty-workspace failures. Infrastructure failures therefore do not leak a live candidate workspace into later attempts.

Creation uses an exclusive reservation keyed by candidate identity. Two concurrent replays of the same candidate cannot share one deterministic path or clean up each other's worktree.

Normal removal lets Git refuse dirty state. Recovery paths may use forced removal only after retaining whatever failure evidence they require.

## Boundary

This feature is a **local experimental candidate isolation adapter**. It does not mean every provider implements native forked filesystems, `sandbox.snapshot` is a live fork, Airflow has stopped being the lifecycle scheduler, a worktree can publish or merge a PR, or a candidate can approve itself.

Provider-native fork execution remains governed by its own capability evidence. The local adapter makes candidate-campaign semantics executable without pretending all remote backends expose the same primitive.
