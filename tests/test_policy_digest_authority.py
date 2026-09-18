from swfactory.security_contract import CanonicalPolicy, policy_digest_for_mapping


def test_canonical_policy_and_mapping_share_one_digest_authority() -> None:
    policy = CanonicalPolicy(
        repo=" acme/repo ",
        target=" main ",
        protected_paths=("rust/", "rust/", "src/"),
        allowed_domains=("EXAMPLE.COM", "example.com"),
        metadata=(("z", "2"), ("a", "1")),
    )

    assert policy.digest() == policy_digest_for_mapping(policy.canonical_dict())


def test_policy_digest_is_invariant_to_mapping_key_order() -> None:
    left = {"repo": "acme/repo", "target": "main", "limits": {"cpu": 2, "memory": 4096}}
    right = {"limits": {"memory": 4096, "cpu": 2}, "target": "main", "repo": "acme/repo"}

    assert policy_digest_for_mapping(left) == policy_digest_for_mapping(right)
