#!/usr/bin/env python3
"""Enforce the promotion boundary declared in `.github/promotion-policy.yml`.

Issue #2048: a `candidate-readiness` job existed and went green, but live branch protection
required only `test` and `airflow-parity`, administrators could bypass it, and the release
workflow ran its own smaller suite without ever consuming the candidate manifest.  A green job
name that nothing requires is decoration.

This script is the enforcement the name implied:

``gate``              refuses to seal a candidate unless every mandatory leg reported ``success``.
                      Missing, failed, cancelled and -- the one that fails open in practice --
                      *skipped* legs are all refusals.  GitHub reports a skipped required check as
                      neutral, so a gate that only looks for ``failure`` lets it through.
``diff``              compares live branch protection against the checked-in desired state and
                      exits non-zero on drift, so the settings page cannot quietly disagree with
                      the repository.
``apply``             writes the desired state to the API.  A maintainer runs this deliberately;
                      nothing in CI does.
``control-plane``     decides whether the protected-paths gate is armed for a pull request.  The
                      branch name is not an input: it used to be, and renaming the branch disarmed
                      the gate.
``release-evidence``  refuses a tag whose shipped tree has no retained candidate evidence, even
                      when the release workflow's own smoke suite is green.
``audit``             checks this file against the workflows it claims to govern, offline.

The policy file is the authority.  Nothing below invents a setting the file does not declare.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = REPO_ROOT / ".github/promotion-policy.yml"
SCHEMA_VERSION = 1

#: The only conclusion that is evidence of a leg having run and passed.  Everything else --
#: including ``skipped`` and ``neutral``, which GitHub does not count as a failure -- is a refusal.
PASSING = "success"


class PolicyViolation(RuntimeError):
    """The promotion boundary was not satisfied, or the policy itself is inconsistent."""


@dataclass(frozen=True)
class ControlPlaneDecision:
    enforce: bool
    reason: str


@dataclass(frozen=True)
class Policy:
    document: Mapping[str, Any]
    repository: str
    branch: str
    required_contexts: tuple[str, ...]
    strict: bool
    enforce_admins: bool
    required_approving_review_count: int
    dismiss_stale_reviews: bool
    require_code_owner_reviews: bool
    require_last_push_approval: bool
    required_conversation_resolution: bool
    required_linear_history: bool
    allow_force_pushes: bool
    allow_deletions: bool
    mandatory_checks: tuple[str, ...]
    advisory_checks: tuple[str, ...]
    control_plane_exemption_label: str
    factory_identities: tuple[str, ...]
    evidence_artifact_prefix: str

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> Policy:
        if document.get("schema_version") != SCHEMA_VERSION:
            raise PolicyViolation(f"unsupported policy schema {document.get('schema_version')!r}")
        protection = document["branch_protection"]
        status = protection["required_status_checks"]
        reviews = protection["required_pull_request_reviews"]
        checks = document["checks"]
        mandatory = tuple(checks["mandatory"])
        advisory = tuple(checks.get("advisory", ()))
        # A leg cannot be both.  Listing one in each place is how an advisory leg silently becomes
        # load-bearing (or a mandatory leg silently stops being one) without anyone deciding it.
        overlap = sorted(set(mandatory) & set(advisory))
        if overlap:
            raise PolicyViolation(f"checks are declared both mandatory and advisory: {', '.join(overlap)}")
        if not mandatory:
            raise PolicyViolation("policy declares no mandatory checks")
        control_plane = document["control_plane"]
        release = document["release"]
        return cls(
            document=document,
            repository=str(document["repository"]),
            branch=str(document["branch"]),
            required_contexts=tuple(status["contexts"]),
            strict=bool(status["strict"]),
            enforce_admins=bool(protection["enforce_admins"]),
            required_approving_review_count=int(reviews["required_approving_review_count"]),
            dismiss_stale_reviews=bool(reviews["dismiss_stale_reviews"]),
            require_code_owner_reviews=bool(reviews["require_code_owner_reviews"]),
            require_last_push_approval=bool(reviews["require_last_push_approval"]),
            required_conversation_resolution=bool(protection["required_conversation_resolution"]),
            required_linear_history=bool(protection["required_linear_history"]),
            allow_force_pushes=bool(protection["allow_force_pushes"]),
            allow_deletions=bool(protection["allow_deletions"]),
            mandatory_checks=mandatory,
            advisory_checks=advisory,
            control_plane_exemption_label=str(control_plane["exemption_label"]),
            factory_identities=tuple(control_plane["factory_identities"]),
            evidence_artifact_prefix=str(release["evidence_artifact_prefix"]),
        )


def load_policy(path: Path | str = DEFAULT_POLICY) -> Policy:
    return Policy.from_document(yaml.safe_load(Path(path).read_text(encoding="utf-8")))


# ---------------------------------------------------------------------------------------------
# The gate: which reported results permit a candidate to be sealed.
# ---------------------------------------------------------------------------------------------


def evaluate_gate(policy: Policy, results: Mapping[str, str]) -> list[str]:
    """Return one line per reason the candidate may not be promoted; empty means promote."""
    blockers: list[str] = []
    for name in policy.mandatory_checks:
        conclusion = (results.get(name) or "").strip()
        if not conclusion:
            blockers.append(f"mandatory check {name!r} reported no result: a leg that never ran is not a pass")
        elif conclusion == "skipped":
            # The headline failure mode.  A skipped required check reports neutral to the merge
            # box, so a gate that tests only for `failure` promotes a candidate nothing ran on.
            blockers.append(f"mandatory check {name!r} was skipped: a skipped mandatory leg is not a pass")
        elif conclusion != PASSING:
            blockers.append(f"mandatory check {name!r} is {conclusion!r}, not {PASSING!r}")

    known = set(policy.mandatory_checks) | set(policy.advisory_checks)
    for name in sorted(set(results) - known):
        # A renamed job would otherwise keep the policy demanding a name nothing reports, while
        # the name that does report is governed by nothing.
        blockers.append(f"result reported for unknown check {name!r}: .github/promotion-policy.yml is stale")
    return blockers


def require_gate(policy: Policy, results: Mapping[str, str]) -> None:
    blockers = evaluate_gate(policy, results)
    if blockers:
        raise PolicyViolation("candidate is not promotable:\n  " + "\n  ".join(blockers))


SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def evaluate_identity(
    *,
    recorded_head: str,
    recorded_base: str,
    recorded_tested: str,
    live_head: str,
    live_base: str,
) -> list[str]:
    """Refuse evidence that belongs to a different candidate than the one being promoted.

    Concurrency cancels a superseded run, but a cancelled run's artifact still exists; identity is
    what stops evidence for an older head or an older base from being re-used for this merge.

    An unresolvable SHA is a refusal rather than a pass.  An expression that evaluates to nothing
    -- a step output from a step that was skipped, a context key that does not exist on this event
    -- reaches a workflow as the empty string, so "compare only what was supplied" would mean the
    check disappears exactly when the workflow around it is wrong.
    """
    blockers: list[str] = []
    for label, sha in (("head", recorded_head), ("base", recorded_base), ("tested", recorded_tested)):
        if not SHA_RE.fullmatch(sha or ""):
            blockers.append(f"recorded {label} SHA {sha!r} is not a resolved commit; evidence identity is unverifiable")
    for label, sha in (("head", live_head), ("base", live_base)):
        if not SHA_RE.fullmatch(sha or ""):
            blockers.append(f"live {label} SHA {sha!r} could not be resolved; refusing to seal unverified evidence")
    if blockers:
        return blockers
    if recorded_head != live_head:
        blockers.append(f"evidence head {recorded_head} is not the candidate head {live_head}: stale head")
    if recorded_base != live_base:
        blockers.append(f"evidence base {recorded_base} is not the candidate base {live_base}: stale base")
    return blockers


# ---------------------------------------------------------------------------------------------
# Desired state versus live settings.
# ---------------------------------------------------------------------------------------------


def protection_payload(policy: Policy) -> dict[str, Any]:
    """The exact body `PUT /branches/{branch}/protection` receives.

    Every field is required by that endpoint: omitting one clears it, so the payload is the whole
    desired state and never a patch.
    """
    return {
        "required_status_checks": {
            "strict": policy.strict,
            "contexts": list(policy.required_contexts),
        },
        "enforce_admins": policy.enforce_admins,
        "required_pull_request_reviews": {
            "required_approving_review_count": policy.required_approving_review_count,
            "dismiss_stale_reviews": policy.dismiss_stale_reviews,
            "require_code_owner_reviews": policy.require_code_owner_reviews,
            "require_last_push_approval": policy.require_last_push_approval,
        },
        # No user/team/app allowlist: an allowlist is a bypass by another name, and this repository
        # has none today.  `null` is how the API says "no restrictions", not "leave as is".
        "restrictions": None,
        "required_conversation_resolution": policy.required_conversation_resolution,
        "required_linear_history": policy.required_linear_history,
        "allow_force_pushes": policy.allow_force_pushes,
        "allow_deletions": policy.allow_deletions,
    }


def desired_live_shape(policy: Policy) -> dict[str, Any]:
    """The desired state in the shape `GET .../protection` returns it.

    The read and write shapes of this endpoint differ -- booleans come back as ``{"enabled": ...}``
    -- so the comparison is done in the read shape and this is what a compliant branch looks like.
    """
    return {
        "required_status_checks": {"strict": policy.strict, "contexts": list(policy.required_contexts)},
        "enforce_admins": {"enabled": policy.enforce_admins},
        "required_pull_request_reviews": {
            "required_approving_review_count": policy.required_approving_review_count,
            "dismiss_stale_reviews": policy.dismiss_stale_reviews,
            "require_code_owner_reviews": policy.require_code_owner_reviews,
            "require_last_push_approval": policy.require_last_push_approval,
        },
        "required_conversation_resolution": {"enabled": policy.required_conversation_resolution},
        "required_linear_history": {"enabled": policy.required_linear_history},
        "allow_force_pushes": {"enabled": policy.allow_force_pushes},
        "allow_deletions": {"enabled": policy.allow_deletions},
    }


def _enabled(live: Mapping[str, Any], key: str) -> bool | None:
    node = live.get(key)
    if isinstance(node, Mapping):
        return bool(node.get("enabled"))
    return None if node is None else bool(node)


def diff_live_protection(policy: Policy, live: Mapping[str, Any]) -> list[str]:
    """Report every way the live branch protection differs from the declared desired state."""
    drift: list[str] = []
    status = live.get("required_status_checks") or {}
    live_contexts = set(status.get("contexts") or [c.get("context") for c in status.get("checks") or []])
    for missing in sorted(set(policy.required_contexts) - live_contexts):
        drift.append(f"required_status_checks.contexts is missing {missing!r}: the boundary is not enforced live")
    for extra in sorted(live_contexts - set(policy.required_contexts)):
        # Extra required contexts are not laxer, but they are undeclared: the file must remain the
        # complete account of what a merge is gated on.
        drift.append(f"required_status_checks.contexts has undeclared {extra!r}")
    if bool(status.get("strict")) != policy.strict:
        drift.append(f"required_status_checks.strict is {status.get('strict')!r}, policy declares {policy.strict!r}")

    for key, want in (
        ("enforce_admins", policy.enforce_admins),
        ("required_conversation_resolution", policy.required_conversation_resolution),
        ("required_linear_history", policy.required_linear_history),
        ("allow_force_pushes", policy.allow_force_pushes),
        ("allow_deletions", policy.allow_deletions),
    ):
        got = _enabled(live, key)
        if got != want:
            drift.append(f"{key} is {got!r}, policy declares {want!r}")

    reviews = live.get("required_pull_request_reviews") or {}
    for key, want in (
        ("required_approving_review_count", policy.required_approving_review_count),
        ("dismiss_stale_reviews", policy.dismiss_stale_reviews),
        ("require_code_owner_reviews", policy.require_code_owner_reviews),
        ("require_last_push_approval", policy.require_last_push_approval),
    ):
        got = reviews.get(key)
        if got != want:
            drift.append(f"required_pull_request_reviews.{key} is {got!r}, policy declares {want!r}")
    return drift


# ---------------------------------------------------------------------------------------------
# Control-plane scope: who may change the confinement, and how.
# ---------------------------------------------------------------------------------------------


def control_plane_decision(
    policy: Policy,
    *,
    head_ref: str,
    labels: Iterable[str],
    label_actor: str | None,
) -> ControlPlaneDecision:
    """Decide whether the protected-paths gate is armed for this pull request.

    `head_ref` is accepted only so callers do not have to special-case it, and is deliberately not
    consulted.  The previous gate armed itself for `factory/*` branches alone, which turned the
    branch name into a privilege: a factory run that pushed to `feat/...` was ungated.  The gate
    now applies to every pull request, and the only way out is an explicit label applied by a
    human maintainer -- documented in docs/promotion-policy.md.
    """
    del head_ref  # never an input to this decision; see the docstring
    names = {str(label) for label in labels}
    if policy.control_plane_exemption_label not in names:
        return ControlPlaneDecision(True, "no exemption label: control-plane paths are protected on every branch")
    actor = (label_actor or "").strip()
    if not actor:
        # An exemption whose applier cannot be identified is an exemption nobody granted.
        return ControlPlaneDecision(True, "exemption label has no identifiable applier; refusing to honour it")
    if actor in policy.factory_identities:
        return ControlPlaneDecision(
            True,
            f"exemption label was applied by the factory identity {actor!r}; the agent may not widen its own cage",
        )
    return ControlPlaneDecision(
        False,
        f"{policy.control_plane_exemption_label!r} applied by {actor!r}: human maintenance of the control plane",
    )


# ---------------------------------------------------------------------------------------------
# Release: retained evidence for the shipped artifact source identity.
# ---------------------------------------------------------------------------------------------


def manifest_digest(document: Mapping[str, Any]) -> str:
    """Recompute the manifest digest from the manifest body.

    Deliberately an independent recomputation rather than a call into
    :mod:`swfactory.candidate_readiness`: the release verifier reads a manifest downloaded from an
    artifact store, and a producer that vouches for its own output proves nothing.  An edited
    manifest -- one red leg rewritten to ``success`` -- no longer matches its digest.
    """
    body = {key: value for key, value in document.items() if key != "manifest_digest"}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def verify_release_evidence(
    policy: Policy,
    manifest: Mapping[str, Any] | None,
    *,
    shipped_commit: str,
    shipped_tree: str,
    tested_tree: str | None,
) -> None:
    """Refuse a release whose shipped tree was never the tree the candidate legs ran against.

    The PR-merge-to-release identity rule (docs/promotion-policy.md): evidence is bound to a
    commit, but what ships is a *tree*.  A merge into main gives the shipped commit a new SHA even
    when its content is byte-identical to the tested candidate, so requiring commit equality would
    refuse every legitimate release; requiring nothing would accept any.  The tree is the honest
    invariant -- it is what was compiled, tested and archived.
    """
    if manifest is None:
        raise PolicyViolation(
            f"no retained candidate evidence for {shipped_commit}: the release workflow's own smoke suite "
            "does not establish that the mandatory candidate legs ever ran against this tree"
        )
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise PolicyViolation(f"candidate evidence has unsupported schema {manifest.get('schema_version')!r}")
    # The manifest arrives from an artifact store, so every field is untrusted until the digest is
    # recomputed below -- including `tested_sha`, which the workflow resolves through the API
    # before it gets here.  A value that is not a commit is refused rather than looked up.
    for field in ("head_sha", "base_sha", "tested_sha"):
        if not SHA_RE.fullmatch(str(manifest.get(field) or "")):
            raise PolicyViolation(f"candidate evidence {field} {manifest.get(field)!r} is not a resolved commit")
    recorded = str(manifest.get("manifest_digest") or "")
    if not recorded:
        raise PolicyViolation("candidate evidence carries no manifest digest")
    if recorded != manifest_digest(manifest):
        raise PolicyViolation(f"candidate evidence digest {recorded} does not cover its own body; evidence was edited")

    checks = {str(check["name"]): check for check in manifest.get("checks") or []}
    for name in policy.mandatory_checks:
        check = checks.get(name)
        if check is None:
            raise PolicyViolation(f"candidate evidence has no leg {name!r}; it was not produced under this policy")
        if not check.get("required"):
            raise PolicyViolation(f"candidate evidence records {name!r} as advisory, but the policy makes it mandatory")
        if str(check.get("status")) != PASSING:
            raise PolicyViolation(f"candidate evidence records {name!r} as {check.get('status')!r}, not {PASSING!r}")

    if not tested_tree:
        raise PolicyViolation(
            f"cannot resolve the tree of tested commit {manifest.get('tested_sha')}; refusing to ship unverified"
        )
    if tested_tree != shipped_tree:
        raise PolicyViolation(
            f"shipped tree {shipped_tree} is not the tested tree {tested_tree} "
            f"(evidence tested {manifest.get('tested_sha')}, tag ships {shipped_commit}); "
            "the artifacts would come from a tree no candidate leg ever ran against"
        )


# ---------------------------------------------------------------------------------------------
# Offline audit: the policy against the workflows it claims to govern.
# ---------------------------------------------------------------------------------------------


def _steps(job: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [step for step in (job.get("steps") or []) if isinstance(step, Mapping)]


def _run_text(job: Mapping[str, Any]) -> str:
    return "\n".join(str(step.get("run") or "") for step in _steps(job))


def _condition(job: Mapping[str, Any]) -> str | None:
    """The job-level ``if:``, whitespace-normalized, or ``None`` when the job carries none."""
    raw = job.get("if")
    return None if raw is None else " ".join(str(raw).split())


def _fails_open_on_error(value: Any) -> bool:
    """``continue-on-error`` is safe only when absent or the literal ``false``.

    ``continue-on-error: true`` reports the job as *successful* to `needs` and to the merge box no
    matter what it did, which is the whole point of the setting.  An expression -- ``${{ ... }}``,
    which YAML loads as a string -- is the same fail-open wearing a disguise: it is truthy at
    audit time only as a value nobody compared against ``True``.
    """
    return value is not None and value is not False


def leg_guard(policy: Policy) -> str:
    """The only job-level ``if:`` a mandatory leg may carry.

    Expensive legs skip for pull requests that do not target the protected branch, which is safe
    only because nothing is protected there.  Pinning the exact string is what stops a second
    condition -- ``&& github.event.pull_request.draft == false`` is the classic -- from being
    appended later: that would skip the leg on a pull request that *does* target the branch, and a
    skipped required check satisfies branch protection.
    """
    return f"github.event_name == 'push' || github.base_ref == '{policy.branch}'"


def aggregate_guard(policy: Policy) -> str:
    """The only job-level ``if:`` the aggregate context may carry.

    ``always()`` is mandatory, not optional.  Without it a failed mandatory leg does not fail the
    aggregate -- it *skips* it, and GitHub counts a skipped required check as satisfied.  The
    aggregate must therefore always run and refuse in its own steps.
    """
    return f"always() && ({leg_guard(policy)})"


#: Status functions that make a job run even though the jobs it `needs` did not succeed.  A fan-in
#: job may use them only when it goes on to inspect every result itself, which the aggregate does
#: and which no downstream release job does.
FAN_IN_OVERRIDES = ("always(", "cancelled(", "failure(")


def _reachable(jobs: Mapping[str, Any], start: str) -> set[str]:
    """Every job that transitively `needs` ``start``."""
    reached = {start}
    changed = True
    while changed:
        changed = False
        for name, job in jobs.items():
            if name in reached or not isinstance(job, Mapping):
                continue
            needs = job.get("needs") or []
            needs = [needs] if isinstance(needs, str) else list(needs)
            if reached.intersection(needs):
                reached.add(name)
                changed = True
    return reached


def audit_policy(policy: Policy, repo_root: Path) -> list[str]:
    """Catch the drift that no live API call can see: a policy that names jobs nothing defines.

    Every rule below exists because the corresponding shape is green in GitHub's merge box while
    testing nothing.  The audit is deliberately literal -- it compares strings rather than trying
    to interpret an expression language -- because a rule that guesses is a rule that can be
    talked round.
    """
    problems: list[str] = []
    workflows = repo_root / ".github/workflows"
    ci = yaml.safe_load((workflows / "ci.yml").read_text(encoding="utf-8"))["jobs"]
    gate = yaml.safe_load((workflows / "control-plane-gate.yml").read_text(encoding="utf-8"))["jobs"]
    release = yaml.safe_load((workflows / "release.yml").read_text(encoding="utf-8"))["jobs"]

    for name in policy.mandatory_checks:
        job = ci.get(name)
        if job is None:
            problems.append(f"mandatory check {name!r} is not a job in ci.yml")
            continue
        if _fails_open_on_error(job.get("continue-on-error")):
            problems.append(
                f"mandatory check {name!r} sets continue-on-error to {job['continue-on-error']!r}; "
                "the job then reports success to `needs` however it ends"
            )
        for step in _steps(job):
            if _fails_open_on_error(step.get("continue-on-error")):
                label = step.get("name") or step.get("run") or step.get("uses") or "<step>"
                problems.append(
                    f"mandatory check {name!r} has a continue-on-error step ({str(label).splitlines()[0][:60]!r}); "
                    "a failed step there still leaves the job green"
                )
        condition = _condition(job)
        if condition is not None and condition != leg_guard(policy):
            problems.append(
                f"mandatory check {name!r} carries the condition {condition!r}; only {leg_guard(policy)!r} "
                "is sanctioned, because any other condition can skip the leg on a protected pull request"
            )

    for name in policy.advisory_checks:
        job = ci.get(name)
        if job is None:
            problems.append(f"advisory check {name!r} is not a job in ci.yml")
        elif job.get("continue-on-error") is not True:
            problems.append(f"advisory check {name!r} is declared advisory but blocks the run")

    aggregate_name = "candidate-readiness"
    aggregate = ci.get(aggregate_name)
    if aggregate is None:
        problems.append(f"ci.yml has no {aggregate_name} job to require")
    else:
        needs = set(aggregate.get("needs") or [])
        for missing in sorted(set(policy.mandatory_checks) - needs):
            problems.append(f"{aggregate_name} does not depend on mandatory check {missing!r}")
        for extra in sorted(needs - set(policy.mandatory_checks)):
            problems.append(f"{aggregate_name} depends on {extra!r}, which the policy does not declare mandatory")
        if _fails_open_on_error(aggregate.get("continue-on-error")):
            problems.append(f"{aggregate_name} sets continue-on-error; the required context would pass regardless")
        condition = _condition(aggregate)
        if condition != aggregate_guard(policy):
            problems.append(
                f"{aggregate_name} carries the condition {condition!r}, not {aggregate_guard(policy)!r}: "
                "without always() a failed leg skips this job, and a skipped required check is satisfied"
            )
        # `if: always()` only relocates the decision into the job; the job has to make it.  A
        # fan-in that runs unconditionally and inspects nothing is green whatever its needs did.
        runs = _run_text(aggregate)
        if "promotion_policy.py gate" not in runs:
            problems.append(f"{aggregate_name} never runs `promotion_policy.py gate`; if: always() then decides nothing")
        else:
            for name in policy.mandatory_checks:
                if f'--result "{name}=' not in runs:
                    problems.append(
                        f"{aggregate_name} does not pass the result of mandatory check {name!r} to the gate"
                    )
        if "promotion_policy.py audit" not in runs:
            problems.append(
                f"{aggregate_name} does not run `promotion_policy.py audit`; this audit is only blocking "
                "where a required context runs it"
            )

    for context in policy.required_contexts:
        job = ci.get(context) or gate.get(context)
        if job is None:
            problems.append(f"required context {context!r} is not produced by any checked-in job")
            continue
        if context == aggregate_name:
            continue  # its sanctioned always()-guard is checked above
        if _condition(job) is not None:
            problems.append(
                f"required context {context!r} carries a job-level condition; a skipped required check "
                "satisfies branch protection, so the job must report on every pull request"
            )
        if _fails_open_on_error(job.get("continue-on-error")):
            problems.append(f"required context {context!r} sets continue-on-error and cannot refuse anything")

    evidence_job = "candidate-evidence"
    if evidence_job not in release:
        problems.append(f"release.yml has no {evidence_job} job; a tag would ship without consulting evidence")
    else:
        if _condition(release[evidence_job]) is not None:
            problems.append(f"release.yml {evidence_job} carries a condition and can be skipped away")
        if _fails_open_on_error(release[evidence_job].get("continue-on-error")):
            problems.append(f"release.yml {evidence_job} sets continue-on-error and cannot refuse a tag")
        if "promotion_policy.py release-evidence" not in _run_text(release[evidence_job]):
            problems.append(f"release.yml {evidence_job} never runs `promotion_policy.py release-evidence`")
        downstream = _reachable(release, evidence_job)
        for name in sorted(set(release) - downstream):
            problems.append(
                f"release.yml job {name!r} does not transitively need {evidence_job!r} and would run for a tag "
                "the evidence gate refused"
            )
        for name, job in sorted(release.items()):
            condition = _condition(job) or ""
            if any(token in condition for token in FAN_IN_OVERRIDES):
                problems.append(
                    f"release.yml job {name!r} has the condition {condition!r}, which runs it even when the jobs "
                    "it needs did not succeed"
                )
    return problems


# ---------------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------------


def _gh_json(*args: str) -> Any:
    proc = subprocess.run(["gh", "api", *args], check=False, text=True, capture_output=True)
    if proc.returncode != 0:
        raise PolicyViolation(f"gh api {' '.join(args)} failed: {proc.stderr.strip()}")
    return json.loads(proc.stdout)


def _parse_results(pairs: Sequence[str]) -> dict[str, str]:
    results: dict[str, str] = {}
    for pair in pairs:
        name, sep, conclusion = pair.partition("=")
        if not sep or not name.strip():
            raise PolicyViolation(f"--result expects NAME=CONCLUSION, got {pair!r}")
        results[name.strip()] = conclusion.strip()
    return results


def _cmd_gate(args: argparse.Namespace, policy: Policy) -> int:
    results = _parse_results(args.result)
    blockers = evaluate_gate(policy, results)
    # Never conditional.  The previous form ran this only when a caller passed `--live-*`, so the
    # one caller that mattered -- ci.yml, which passed neither -- skipped the identity check
    # entirely and the code read as coverage it did not provide.  On a push there is no second
    # source for the identity, so the recorded values stand in and only their shape is checked.
    blockers += evaluate_identity(
        recorded_head=args.head_sha,
        recorded_base=args.base_sha,
        recorded_tested=args.tested_sha,
        live_head=args.live_head or args.head_sha,
        live_base=args.live_base or args.base_sha,
    )
    for line in blockers:
        print(f"::error::{line}")
    if blockers:
        print("candidate-readiness refuses to seal evidence for this candidate", file=sys.stderr)
        return 1
    print(f"all {len(policy.mandatory_checks)} mandatory legs reported success; candidate is promotable")
    return 0


def _cmd_diff(args: argparse.Namespace, policy: Policy) -> int:
    if args.live_json:
        live = json.loads(Path(args.live_json).read_text(encoding="utf-8"))
    else:
        live = _gh_json(f"repos/{policy.repository}/branches/{policy.branch}/protection")
    drift = diff_live_protection(policy, live)
    for line in drift:
        print(f"::error::branch protection drift: {line}")
    if drift:
        print(
            "live branch protection does not match .github/promotion-policy.yml; "
            "a maintainer applies the file with `scripts/promotion_policy.py apply --confirm`",
            file=sys.stderr,
        )
        return 1
    print(f"live protection on {policy.repository}@{policy.branch} matches the declared policy")
    return 0


def _cmd_apply(args: argparse.Namespace, policy: Policy) -> int:
    payload = protection_payload(policy)
    target = f"repos/{policy.repository}/branches/{policy.branch}/protection"
    if not args.confirm:
        # Applying this wedges every in-flight pull request that has not produced the new contexts,
        # so it is never a side effect of running the script.
        print(json.dumps(payload, indent=2, sort_keys=True))
        print(f"\nwould PUT the above to {target}; re-run with --confirm to apply", file=sys.stderr)
        return 0
    proc = subprocess.run(
        ["gh", "api", "--method", "PUT", target, "--input", "-"],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        print(f"::error::applying branch protection failed: {proc.stderr.strip()}")
        return 1
    print(f"applied .github/promotion-policy.yml to {target}")
    return 0


def _labels(args: argparse.Namespace) -> list[str]:
    """The pull request's labels, exactly as GitHub spells them.

    Label names may contain spaces, so a space-joined string cannot be split back into labels: a
    label called ``needs control-plane-maintenance review`` tokenises into three "labels", one of
    which is the exemption. `--labels-json` (``toJSON(...labels.*.name)``) is unambiguous and is
    what the workflow passes; `--labels` survives only for a caller that has one label.
    """
    if args.labels_json.strip():
        parsed = json.loads(args.labels_json)
        if not isinstance(parsed, list):
            raise PolicyViolation(f"--labels-json must be a JSON array, got {type(parsed).__name__}")
        return [str(item) for item in parsed]
    return [args.labels] if args.labels.strip() else []


def _cmd_control_plane(args: argparse.Namespace, policy: Policy) -> int:
    decision = control_plane_decision(
        policy,
        head_ref=args.head_ref,
        labels=_labels(args),
        label_actor=args.label_actor,
    )
    print(f"enforce={'yes' if decision.enforce else 'no'} reason={decision.reason}")
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as handle:
            handle.write(f"enforce={'yes' if decision.enforce else 'no'}\n")
    return 0


def _cmd_release_evidence(args: argparse.Namespace, policy: Policy) -> int:
    manifest = None
    path = Path(args.manifest) if args.manifest else None
    if path is not None and path.is_file():
        manifest = json.loads(path.read_text(encoding="utf-8"))
    try:
        verify_release_evidence(
            policy,
            manifest,
            shipped_commit=args.shipped_commit,
            shipped_tree=args.shipped_tree,
            tested_tree=args.tested_tree,
        )
    except PolicyViolation as exc:
        print(f"::error::{exc}")
        return 1
    print(f"candidate evidence covers the shipped tree {args.shipped_tree}; release may proceed")
    return 0


def _cmd_audit(args: argparse.Namespace, policy: Policy) -> int:
    problems = audit_policy(policy, Path(args.repo_root))
    for line in problems:
        print(f"::error::{line}")
    if problems:
        return 1
    print("the policy and the checked-in workflows agree")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--policy", default=str(DEFAULT_POLICY), type=Path)
    sub = parser.add_subparsers(dest="command", required=True)

    gate = sub.add_parser("gate", help="refuse a candidate whose mandatory legs are not all success")
    gate.add_argument("--result", action="append", default=[], help="NAME=CONCLUSION, once per leg")
    gate.add_argument("--head-sha", default="")
    gate.add_argument("--base-sha", default="")
    gate.add_argument("--tested-sha", default="")
    gate.add_argument("--live-head", default="")
    gate.add_argument("--live-base", default="")
    gate.set_defaults(func=_cmd_gate)

    diff = sub.add_parser("diff", help="fail on drift between live protection and the policy")
    diff.add_argument("--live-json", default="", help="read the live settings from a file instead of the API")
    diff.set_defaults(func=_cmd_diff)

    apply_ = sub.add_parser("apply", help="write the policy to the live repository (maintainers only)")
    apply_.add_argument("--confirm", action="store_true")
    apply_.set_defaults(func=_cmd_apply)

    control_plane = sub.add_parser("control-plane", help="decide whether the protected-paths gate is armed")
    control_plane.add_argument("--head-ref", default="")
    control_plane.add_argument("--labels", default="", help="a single label name, verbatim")
    control_plane.add_argument("--labels-json", default="", help="a JSON array of label names")
    control_plane.add_argument("--label-actor", default="")
    control_plane.add_argument("--github-output", default="")
    control_plane.set_defaults(func=_cmd_control_plane)

    evidence = sub.add_parser("release-evidence", help="refuse a tag with no evidence for its shipped tree")
    evidence.add_argument("--manifest", default="")
    evidence.add_argument("--shipped-commit", required=True)
    evidence.add_argument("--shipped-tree", required=True)
    evidence.add_argument("--tested-tree", default="")
    evidence.set_defaults(func=_cmd_release_evidence)

    audit = sub.add_parser("audit", help="check the policy against the checked-in workflows, offline")
    audit.add_argument("--repo-root", default=str(REPO_ROOT))
    audit.set_defaults(func=_cmd_audit)

    args = parser.parse_args(argv)
    try:
        policy = load_policy(args.policy)
        return int(args.func(args, policy))
    except PolicyViolation as exc:
        print(f"::error::{exc}")
        return 2


if __name__ == "__main__":  # pragma: no cover - CLI seam
    raise SystemExit(main())
