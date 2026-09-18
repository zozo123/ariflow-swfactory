from pathlib import Path

import yaml


def test_exploration_convergence_policy_keeps_entropy_out_of_promotion() -> None:
    root = Path(__file__).resolve().parents[1]
    policy = yaml.safe_load((root / "config" / "exploration-convergence.yaml").read_text(encoding="utf-8"))

    assert policy["schema_version"] == 1
    assert policy["exploration"]["authority"] == "exploration-only"
    assert policy["exploration"]["nondeterminism"] == "encouraged"
    assert policy["exploration"]["record_entropy_token"] is True

    convergence = policy["convergence"]
    assert convergence["authority"] == "deterministic-fan-in"
    assert convergence["completion_order_independent"] is True
    assert convergence["evidence_required"] is True
    assert convergence["single_promotion_authority"] is True
    assert convergence["human_promotion_boundary"] is True
    assert convergence["refuse_missing_evidence"] is True
