# The promotion boundary

A green job name is not a gate. Until this document existed, `candidate-readiness` fanned in every
mandatory leg, sealed SHA-bound evidence, and went green on every pull request — while the live
branch protection API required only `test` and `airflow-parity`, administrators could merge past
both, `release.yml` shipped on the strength of its own smaller smoke suite without ever opening the
candidate manifest, and the protected-paths gate armed itself only for branches named `factory/*`.
Four green ticks, one enforced boundary, and not the same one. That is issue
[#2048](https://github.com/zozo123/ariflow-swfactory/issues/2048).

The boundary is now expressed as code in three files that must agree with each other:

| File | What it is |
| --- | --- |
| [`.github/promotion-policy.yml`](../.github/promotion-policy.yml) | The desired state: required contexts, bypass policy, which legs are mandatory and which are deliberately advisory. |
| [`scripts/promotion_policy.py`](../scripts/promotion_policy.py) | Applies it, diffs it against the live settings, and enforces it in CI and at release. |
| [`.github/live-protection-2026-09-08.json`](../.github/live-protection-2026-09-08.json) | A raw read-only export of what was actually live when the issue was reproduced. It is the *before*, kept so the drift check has something to be tested against. |

`uv run pytest tests/test_promotion_policy.py` is the executable half of this document.

## What must be green, and what must not be able to be skipped

Mandatory legs — `test`, `eval-suite`, `airflow-parity`, `airflow-main`, `rust`,
`contract-equivalence` — fan into `candidate-readiness`, which refuses to seal evidence unless each
one reported `success`.

**`skipped` is a refusal.** This is the failure mode the whole gate exists for. GitHub reports a
skipped required check as neutral, and a neutral check does not block a merge; a gate that tests
only for `failure` therefore promotes a candidate that nothing ran on. A leg can be skipped without
anyone intending it: an `if:` condition that stops matching, a `needs:` upstream that was cancelled,
a path filter added for speed. `cancelled`, `neutral`, `timed_out`, `action_required` and *absent*
are refusals for the same reason: none of them is evidence that the leg ran and passed.

Advisory legs — `live-gate-e2e`, `srt-smoke`, `docker-smoke`, `airflow-main-sandbox-toolset` —
stay advisory, on purpose. Their hosted-runner timing and provider sandboxes are not yet supported
release claims (see `config/capability-inventory.json`). They may fail without blocking anything,
and they never count as evidence. What they may **not** do is be mandatory in one place and advisory
in another: the policy refuses to load if a leg appears in both lists, and the audit fails if an
advisory leg is not actually `continue-on-error` in `ci.yml`, or a mandatory one is.

## The PR-merge-to-release identity rule

Evidence is bound to a commit. What ships is a **tree**.

Merging a pull request produces a commit on `main` with a new SHA even when its content is
byte-identical to the candidate that was tested, so a release gate that demanded commit equality
would refuse every legitimate release, and one that demanded nothing would accept any. The tree is
the honest invariant: it is what was compiled, tested, archived and checksummed.

> A tag is admissible only when retained candidate evidence exists whose `tested_sha` names a commit
> whose **tree** is identical to the tree of the commit the tag points at.

In practice, `ci.yml` runs on every push to `main`, so the commit a release tag names has a
readiness run of its own; `release.yml`'s `candidate-evidence` job selects CI runs by `head_sha`,
recovers the `candidate-readiness` artifact, and compares trees. Consequences worth stating plainly:

* A tag on a commit that was never pushed to `main` has no evidence and is refused.
* A squash that changed anything — one whitespace character — produces a different tree, so the
  evidence for the pre-squash head does not cover it.
* An expired artifact retention window is indistinguishable from evidence that never existed, and
  both refuse the tag. Tag within the retention window, or re-run CI on the commit first.
* `release.yml`'s own lint/pytest/demo/DAG-parity suite passing on the tagged tree proves nothing
  about the mandatory legs, so it cannot substitute for evidence.
* The manifest digest is **recomputed** by the verifier rather than trusted. A manifest downloaded
  from an artifact store with one red leg edited to `success` no longer matches its own digest.

A dry run reports the same verdict but does not fail on it: a rehearsal creates no tag and ships
nothing, and failing it would only stop maintainers from exercising the four-target build.

## Changing the control plane

`factory.toml` `[paths] protected` lists the paths that govern the factory — `.github/`,
`scripts/`, `dags/`, `blueprints/`, `pyproject.toml`, the sandbox and security modules. Inside a
work cell they are denied to the agent twice over (Claude Code `permissions.deny` on every backend,
plus a kernel-level read-only mount on srt and docker). `control-plane-gate.yml` is the layer that
runs *outside* the cage and re-checks the same list, read from the base revision, against the
delivered diff.

It now applies to **every pull request**. It used to arm itself only for `factory/*` heads, which
made a branch prefix a credential: a delivery pushed to `feat/…` was ungated, and nothing about a
name is an attestation. Failing closed is the only version of this check that is worth having.

**The human maintenance path.** A maintainer who intends a control-plane change applies the
`control-plane-maintenance` label to the pull request by hand. Applying a label needs triage or
write permission on the repository, so it is an act by a named account, recorded in the timeline —
and the gate reads *who* applied it from that timeline, not from the label list, because the run
that enforces the gate is usually a later `synchronize` whose actor is whoever pushed.

A label applied by one of the factory's own identities (`.github/promotion-policy.yml`
`control_plane.factory_identities`) does not exempt anything, and neither does a label whose applier
cannot be identified. An agent may not widen its own cage in the pull request that uses the wider
cage — that is the whole shape of the attack, and self-attestation is not a mitigation for it.

There is a cost, and it is deliberate: an ordinary human pull request that touches `scripts/` or
`.github/` now goes red on `protected-paths` until somebody labels it. That is a control-plane edit
being asked for a second signature, which is what the issue asked for.

## Applying this file

Nothing applies itself. `scripts/promotion_policy.py apply` prints the exact `PUT` body and stops;
`--confirm` sends it:

```bash
# what would change
uv run python scripts/promotion_policy.py diff            # needs an admin-scoped GH_TOKEN
uv run python scripts/promotion_policy.py apply           # prints the payload, applies nothing
uv run python scripts/promotion_policy.py apply --confirm # a maintainer, deliberately
```

The payload is the whole desired state, never a patch: `PUT /branches/{branch}/protection` clears
every field the body omits, so the file has to be complete or applying it would silently relax
something it does not mention.

**Applying this wedges in-flight pull requests.** Every open pull request that has not produced a
`candidate-readiness` and a `protected-paths` result will sit unmergeable until it is rebased and
re-run. Apply it when the queue is quiet, not in the middle of a fan-in.

The drift check (`.github/workflows/promotion-policy.yml`) runs `audit` everywhere — offline,
deterministic, and blocking — and runs `diff` against the live API only where a maintainer token
exists. Reading branch protection needs administrative scope, which `github.token` does not have;
when `SWF_POLICY_ADMIN_TOKEN` is absent the step says, in warnings, that the boundary was **not**
verified. An unverified run must never be mistaken for a verified one.
