from itertools import permutations

import pytest

from swfactory.convergence_decision import build_convergence_decision


ROWS = (
    {"logical_id": "cand_b", "evidence_digest": "sha256:" + "b" * 64},
    {"logical_id": "cand_a", "evidence_digest": "sha256:" + "a" * 64},
    {"logical_id": "cand_c", "evidence_digest": "sha256:" + "c" * 64},
)


def test_convergence_digest_is_invariant_to_completion_order() -> None:
    digests = {
        build_convergence_decision(order, winner="cand_b", required_dimensions={"evidence", "correctness"}).digest
        for order in permutations(ROWS)
    }
    assert len(digests) == 1


def test_convergence_canonicalizes_candidates_and_dimensions() -> None:
    decision = build_convergence_decision(
        ROWS,
        winner="cand_b",
        required_dimensions=("evidence", "correctness", "evidence"),
    )
    assert decision.candidate_ids == ("cand_a", "cand_b", "cand_c")
    assert decision.required_dimensions == ("correctness", "evidence")
    assert decision.to_dict()["authority"] == "deterministic-fan-in"


@pytest.mark.parametrize(
    "rows,winner",
    [
        ((), "cand_a"),
        (({"logical_id": "cand_a", "evidence_digest": "bad"},), "cand_a"),
        (({"logical_id": "cand_a", "evidence_digest": "sha256:" + "a" * 64},), "missing"),
    ],
)
def test_convergence_refuses_unverifiable_decisions(rows, winner) -> None:
    with pytest.raises(ValueError):
        build_convergence_decision(rows, winner=winner, required_dimensions=("correctness",))
