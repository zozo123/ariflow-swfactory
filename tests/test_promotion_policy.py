"""The promotion boundary, tested from the outside.

Issue #2048: a candidate-readiness job existed and went green, but nothing required it.  These
tests are the negative gate the issue asks for -- they are written against the failure modes that
let a green job name decorate an unenforced boundary:

* a mandatory leg that is *skipped* reports neutral to GitHub and is the classic way a required
  check fails open, so it gets a test of its own alongside missing/failed/cancelled;
* branch naming must not confer privilege, so the control-plane decision must be identical for
  ``factory/x`` and ``feature/x``;
* a release must refuse a tag whose artifact source identity has no retained evidence, even when
  the release workflow's own smaller smoke suite is green.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("promotion_policy", REPO / "scripts/promotion_policy.py")
assert _spec and _spec.loader
promotion_policy = importlib.util.module_from_spec(_spec)
# The module must be registered before it executes: `dataclass` resolves annotations through
# `sys.modules[cls.__module__]`, which is absent for a spec-loaded file and raises there instead.
sys.modules["promotion_policy"] = promotion_policy
_spec.loader.exec_module(promotion_policy)

PolicyViolation = promotion_policy.PolicyViolation
POLICY = promotion_policy.load_policy(REPO / ".github/promotion-policy.yml")


def _sha(ch: str) -> str:
    return ch * 40


def _all_green() -> dict[str, str]:
    return {name: "success" for name in POLICY.mandatory_checks}


# --------------------------------------------------------------------------------------------
# Acceptance box 2: missing / failed / cancelled / skipped mandatory legs block promotion.
# --------------------------------------------------------------------------------------------


def test_every_mandatory_leg_green_promotes() -> None:
    assert promotion_policy.evaluate_gate(POLICY, _all_green()) == []


@pytest.mark.parametrize(
    "conclusion",
    ["skipped", "failure", "cancelled", "neutral", "timed_out", "action_required", "stale", ""],
)
def test_a_non_success_mandatory_leg_blocks_promotion(conclusion: str) -> None:
    """`skipped` is the load-bearing case: GitHub reports a skipped required check as neutral,
    which a naive gate reads as "not failing" and lets through."""
    results = _all_green()
    results["airflow-main"] = conclusion
    blockers = promotion_policy.evaluate_gate(POLICY, results)

    assert blockers, f"{conclusion!r} must block, not pass"
    assert any("airflow-main" in line for line in blockers)
    with pytest.raises(PolicyViolation, match="airflow-main"):
        promotion_policy.require_gate(POLICY, results)


def test_a_mandatory_leg_that_reported_nothing_blocks_promotion() -> None:
    """A leg whose job never ran reports no result at all; absence must not read as absence of
    failure."""
    results = _all_green()
    del results["rust"]

    blockers = promotion_policy.evaluate_gate(POLICY, results)
    assert any("rust" in line and "no result" in line for line in blockers)


def test_skipped_is_named_as_skipped_not_merely_as_not_success() -> None:
    results = _all_green()
    results["eval-suite"] = "skipped"
    assert any("skipped" in line for line in promotion_policy.evaluate_gate(POLICY, results))


def test_advisory_legs_never_block_and_never_count_as_evidence() -> None:
    """Deliberately experimental legs stay advisory -- that is intended, not a hole."""
    results = _all_green() | {name: "failure" for name in POLICY.advisory_checks}
    assert promotion_policy.evaluate_gate(POLICY, results) == []


def test_an_unknown_check_name_is_reported_as_policy_drift() -> None:
    """A renamed job would otherwise silently stop being mandatory: the policy would keep
    demanding a name nothing reports, and the reported name would be nobody's business."""
    results = _all_green() | {"invented-leg": "success"}
    assert any("invented-leg" in line for line in promotion_policy.evaluate_gate(POLICY, results))


def test_a_mandatory_leg_may_not_be_downgraded_to_advisory_in_the_same_policy() -> None:
    with pytest.raises(PolicyViolation, match="both mandatory and advisory"):
        promotion_policy.Policy.from_document(
            {
                **POLICY.document,
                "checks": {"mandatory": ["test"], "advisory": ["test"]},
            }
        )


# --------------------------------------------------------------------------------------------
# Acceptance box 2 (identity half): stale base/head and a wrong tested tree block promotion.
# --------------------------------------------------------------------------------------------


def test_evidence_from_a_superseded_head_blocks_promotion() -> None:
    blockers = promotion_policy.evaluate_identity(
        recorded_head=_sha("a"),
        recorded_base=_sha("b"),
        recorded_tested=_sha("c"),
        live_head=_sha("d"),
        live_base=_sha("b"),
    )
    assert any("head" in line for line in blockers)


def test_evidence_integrated_against_a_superseded_base_blocks_promotion() -> None:
    blockers = promotion_policy.evaluate_identity(
        recorded_head=_sha("a"),
        recorded_base=_sha("b"),
        recorded_tested=_sha("c"),
        live_head=_sha("a"),
        live_base=_sha("e"),
    )
    assert any("base" in line for line in blockers)


def test_matching_identity_does_not_block() -> None:
    assert (
        promotion_policy.evaluate_identity(
            recorded_head=_sha("a"),
            recorded_base=_sha("b"),
            recorded_tested=_sha("c"),
            live_head=_sha("a"),
            live_base=_sha("b"),
        )
        == []
    )


# --------------------------------------------------------------------------------------------
# Acceptance box 1: the exported configuration proves the aggregate context and bypass policy.
# --------------------------------------------------------------------------------------------


def test_the_desired_state_requires_the_aggregate_context_and_forbids_admin_bypass() -> None:
    assert "candidate-readiness" in POLICY.required_contexts
    assert "protected-paths" in POLICY.required_contexts
    assert POLICY.enforce_admins is True
    assert POLICY.strict is True, "a stale base must not be mergeable"


def test_the_live_configuration_recorded_on_this_repository_is_reported_as_drift() -> None:
    """The state observed read-only on 2026-09-08: only `test` and `airflow-parity` are required
    and administrators may bypass.  The drift check must say so rather than pass quietly."""
    live = json.loads((REPO / ".github/live-protection-2026-09-08.json").read_text())
    drift = promotion_policy.diff_live_protection(POLICY, live)

    assert any("candidate-readiness" in line for line in drift)
    assert any("enforce_admins" in line for line in drift)


def test_a_live_configuration_equal_to_the_desired_state_reports_no_drift() -> None:
    assert promotion_policy.diff_live_protection(POLICY, promotion_policy.desired_live_shape(POLICY)) == []


def test_dropping_one_required_context_live_is_drift() -> None:
    live = promotion_policy.desired_live_shape(POLICY)
    live["required_status_checks"]["contexts"] = [
        c for c in live["required_status_checks"]["contexts"] if c != "candidate-readiness"
    ]
    assert any("candidate-readiness" in line for line in promotion_policy.diff_live_protection(POLICY, live))


def test_the_apply_payload_is_exactly_the_declared_desired_state() -> None:
    """`apply` must not invent settings the checked-in file does not declare, or the file stops
    being the authority on what is live."""
    payload = promotion_policy.protection_payload(POLICY)

    assert payload["required_status_checks"]["contexts"] == list(POLICY.required_contexts)
    assert payload["enforce_admins"] is True
    assert payload["restrictions"] is None


# --------------------------------------------------------------------------------------------
# Acceptance box 3: release refuses a tag with no valid evidence for the shipped identity.
# --------------------------------------------------------------------------------------------


def _manifest(**overrides: Any) -> dict[str, Any]:
    from swfactory.candidate_readiness import build_manifest

    tmp = Path(overrides.pop("tmp_path"))
    artifact = tmp / "leg.txt"
    artifact.write_text("green\n", encoding="utf-8")
    manifest = build_manifest(
        head_sha=_sha("a"),
        base_sha=_sha("b"),
        tested_sha=_sha("a"),
        required=[(name, artifact) for name in POLICY.mandatory_checks],
    )
    document = manifest.canonical_dict()
    document["manifest_digest"] = manifest.digest()
    document.update(overrides)
    return document


def _resealed(document: dict[str, Any]) -> dict[str, Any]:
    """Re-digest a mutated manifest, so a test about a *missing leg* is not answered by the
    tamper check instead of by the leg check it is aiming at."""
    document["manifest_digest"] = promotion_policy.manifest_digest(document)
    return document


def test_release_accepts_evidence_whose_tested_tree_is_the_shipped_tree(tmp_path: Path) -> None:
    promotion_policy.verify_release_evidence(
        POLICY,
        _manifest(tmp_path=tmp_path),
        shipped_commit=_sha("a"),
        shipped_tree=_sha("7"),
        tested_tree=_sha("7"),
    )


def test_release_refuses_a_tag_whose_shipped_tree_was_never_the_tested_tree(tmp_path: Path) -> None:
    """The smaller smoke suite in release.yml passing on this tree is irrelevant: nothing proves
    the candidate legs ever ran against it."""
    with pytest.raises(PolicyViolation, match="tree"):
        promotion_policy.verify_release_evidence(
            POLICY,
            _manifest(tmp_path=tmp_path),
            shipped_commit=_sha("a"),
            shipped_tree=_sha("7"),
            tested_tree=_sha("8"),
        )


def test_release_refuses_evidence_missing_a_mandatory_leg(tmp_path: Path) -> None:
    document = _manifest(tmp_path=tmp_path)
    document["checks"] = [c for c in document["checks"] if c["name"] != "contract-equivalence"]
    _resealed(document)
    with pytest.raises(PolicyViolation, match="contract-equivalence"):
        promotion_policy.verify_release_evidence(
            POLICY, document, shipped_commit=_sha("a"), shipped_tree=_sha("7"), tested_tree=_sha("7")
        )


def test_release_refuses_evidence_whose_leg_was_not_successful(tmp_path: Path) -> None:
    document = _manifest(tmp_path=tmp_path)
    for check in document["checks"]:
        if check["name"] == "rust":
            check["status"] = "skipped"
    _resealed(document)
    with pytest.raises(PolicyViolation, match="rust"):
        promotion_policy.verify_release_evidence(
            POLICY, document, shipped_commit=_sha("a"), shipped_tree=_sha("7"), tested_tree=_sha("7")
        )


def test_release_refuses_a_manifest_whose_digest_does_not_cover_its_own_body(tmp_path: Path) -> None:
    """The verifier recomputes the digest instead of trusting the producer, so an edited manifest
    downloaded from an artifact store cannot launder a red leg into a green one."""
    document = _manifest(tmp_path=tmp_path)
    document["head_sha"] = _sha("f")
    with pytest.raises(PolicyViolation, match="digest"):
        promotion_policy.verify_release_evidence(
            POLICY, document, shipped_commit=_sha("f"), shipped_tree=_sha("7"), tested_tree=_sha("7")
        )


def test_release_refuses_absent_evidence() -> None:
    with pytest.raises(PolicyViolation, match="no retained candidate evidence"):
        promotion_policy.verify_release_evidence(
            POLICY, None, shipped_commit=_sha("a"), shipped_tree=_sha("7"), tested_tree=None
        )


def test_the_verifier_recomputes_the_same_digest_the_producer_writes(tmp_path: Path) -> None:
    """Pins the release verifier to `swfactory.candidate_readiness`: if the producer's canonical
    form ever changes, this fails here rather than in a release that refuses every valid tag."""
    document = _manifest(tmp_path=tmp_path)
    assert promotion_policy.manifest_digest(document) == document["manifest_digest"]


# --------------------------------------------------------------------------------------------
# Acceptance box 4: control-plane edits are refused, with no implicit privilege from a branch name.
# --------------------------------------------------------------------------------------------


def test_branch_naming_confers_no_privilege() -> None:
    """The old gate enforced only on `factory/*`, so renaming the branch disarmed it."""
    factory = promotion_policy.control_plane_decision(POLICY, head_ref="factory/issue-1", labels=[], label_actor=None)
    human_looking = promotion_policy.control_plane_decision(POLICY, head_ref="feat/tidy", labels=[], label_actor=None)

    assert factory.enforce is True
    assert human_looking.enforce is True
    assert factory.reason == human_looking.reason


def test_the_documented_human_path_is_a_maintainer_applied_label() -> None:
    decision = promotion_policy.control_plane_decision(
        POLICY,
        head_ref="chore/rotate-protected-paths",
        labels=[POLICY.control_plane_exemption_label],
        label_actor="a-human-maintainer",
    )
    assert decision.enforce is False
    assert POLICY.control_plane_exemption_label in decision.reason


def test_the_factory_identity_cannot_grant_itself_the_exemption() -> None:
    for actor in [*POLICY.factory_identities, None, ""]:
        decision = promotion_policy.control_plane_decision(
            POLICY,
            head_ref="factory/issue-1",
            labels=[POLICY.control_plane_exemption_label],
            label_actor=actor,
        )
        assert decision.enforce is True, f"{actor!r} must not be able to self-exempt"


# --------------------------------------------------------------------------------------------
# The policy is wired to the workflows it claims to govern -- otherwise it is more decoration.
# --------------------------------------------------------------------------------------------


def _workflow(name: str) -> dict[str, Any]:
    return yaml.safe_load((REPO / ".github/workflows" / name).read_text())


def test_every_mandatory_leg_is_a_real_blocking_job_in_ci() -> None:
    jobs = _workflow("ci.yml")["jobs"]
    for name in POLICY.mandatory_checks:
        assert name in jobs, f"policy demands {name}, ci.yml has no such job"
        assert jobs[name].get("continue-on-error") is not True, f"{name} is mandatory but cannot fail the run"


def test_every_advisory_leg_is_a_real_non_blocking_job_in_ci() -> None:
    jobs = _workflow("ci.yml")["jobs"]
    for name in POLICY.advisory_checks:
        assert name in jobs
        assert jobs[name].get("continue-on-error") is True, f"{name} is declared advisory but blocks the run"


def test_the_aggregate_job_fans_in_exactly_the_mandatory_legs() -> None:
    needs = set(_workflow("ci.yml")["jobs"]["candidate-readiness"]["needs"])
    assert needs == set(POLICY.mandatory_checks)


def test_the_aggregate_job_evaluates_the_policy_rather_than_open_coding_it() -> None:
    text = (REPO / ".github/workflows/ci.yml").read_text()
    assert "scripts/promotion_policy.py gate" in text


def test_the_control_plane_gate_reports_on_every_pull_request() -> None:
    """A job that only runs for some branches reports `skipped` for the rest, and a skipped
    required context is exactly the fail-open this issue is about."""
    workflow = _workflow("control-plane-gate.yml")
    job = workflow["jobs"]["protected-paths"]
    assert "if" not in job, "a job-level condition would let the required context be skipped"
    assert job.get("continue-on-error") is not True


def test_the_control_plane_gate_does_not_scope_itself_by_branch_prefix() -> None:
    """The workflow must delegate the decision, not re-derive it: the old `case factory/*` shell
    was the privilege, and a copy of it left in place would keep granting it."""
    text = (REPO / ".github/workflows/control-plane-gate.yml").read_text()
    assert "factory/*)" not in text
    # The script path is a variable, not a literal, and that is the point: it is resolved from the
    # BASE revision so a pull request cannot supply the rules that judge it. Asserting a hardcoded
    # `scripts/promotion_policy.py` here would be asserting that hardening away.
    assert 'control-plane' in text and '"$SCRIPT" --policy "$POLICY" control-plane' in text
    assert 'git show "$rev:scripts/promotion_policy.py"' in text, (
        "the gate must take its own script from the base revision, not from the diff under review"
    )


def test_release_verifies_candidate_evidence_before_it_publishes_anything() -> None:
    jobs = _workflow("release.yml")["jobs"]
    assert "candidate-evidence" in jobs
    assert "candidate-evidence" in jobs["release"]["needs"]
    assert "scripts/promotion_policy.py release-evidence" in (REPO / ".github/workflows/release.yml").read_text()


def test_the_drift_check_workflow_exists_and_audits_the_policy() -> None:
    text = (REPO / ".github/workflows/promotion-policy.yml").read_text()
    assert "scripts/promotion_policy.py audit" in text
    assert "scripts/promotion_policy.py diff" in text


def test_the_policy_audit_agrees_with_the_checked_in_workflows() -> None:
    assert promotion_policy.audit_policy(POLICY, REPO) == []
