from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

import swfactory.security_contract as contract
from swfactory.security_contract import (
    CanonicalPolicy,
    MutationEnvelope,
    policy_digest_family,
    policy_digest_for_mapping,
)

ROOT = Path(__file__).resolve().parents[1]


def test_canonical_policy_and_mapping_entry_points_are_byte_identical() -> None:
    policies = [
        CanonicalPolicy(repo="acme/repo", target=".@main"),
        CanonicalPolicy(
            repo=" acme/repo ",
            target="src@main",
            protected_paths=("factory.toml", ".github/", "factory.toml"),
            allowed_domains=("GitHub.COM", "api.github.com"),
            sandbox_provider="islo",
            metadata=(("line", "factory"),),
        ),
    ]
    for policy in policies:
        assert policy.digest() == policy_digest_for_mapping(policy.canonical_dict())


def test_factory_job_projection_excludes_issue_identity_but_binds_authority_shape() -> None:
    base = {
        "repo": "acme/repo",
        "issue": "101",
        "dir": "src",
        "base_branch": "main",
        "sandbox": "islo",
    }
    first = CanonicalPolicy.for_factory_job("factory", base)
    second = CanonicalPolicy.for_factory_job("factory", {**base, "issue": "999"})

    assert first.canonical_dict() == second.canonical_dict()
    assert first.target == "src@main"
    assert first.metadata == (("line", "factory"),)
    assert first.digest() == second.digest()
    assert first.digest() != CanonicalPolicy.for_factory_job("hotfix", base).digest()
    assert first.digest() != CanonicalPolicy.for_factory_job(
        "factory", {**base, "base_branch": "release"}
    ).digest()


def test_policy_digest_family_marks_legacy_authority_and_mutations_refuse_it() -> None:
    legacy = "policy:" + "a" * 64
    current = CanonicalPolicy(repo="acme/repo", target=".@main").digest()

    assert policy_digest_family(legacy) == 0
    assert policy_digest_family(current) == contract.POLICY_SCHEMA_VERSION
    with pytest.raises(ValueError, match="reactivate the Cell at a new epoch"):
        MutationEnvelope(
            cell_id="cell_" + "a" * 24,
            epoch=1,
            operation_key="publish:1",
            policy_digest=legacy,
            trace_id="trace",
            actor="backend",
        ).validate()


def test_policy_digest_has_one_hash_construction_site() -> None:
    source = inspect.getsource(contract)
    assert source.count("hashlib.sha256(") == 1
    assert '"policy:" + hashlib.sha256' not in source


def test_python_matches_the_shared_policy_digest_fixture() -> None:
    document = json.loads(
        (ROOT / "tests/fixtures/contract/policy_digest.json").read_text(encoding="utf-8")
    )
    assert document["function"] == "policy_digest"
    for case in document["cases"]:
        assert policy_digest_for_mapping(case["input"]) == case["expected"]
