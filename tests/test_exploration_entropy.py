from swfactory.exploration_entropy import deterministic_test_sampler, permute_exploration


def test_exploration_permutation_preserves_the_search_space() -> None:
    source = ("repair", "rethink", "scratch", "alternate-model")
    result = permute_exploration(source, sampler=deterministic_test_sampler(7), entropy_token="test-entropy")

    assert result.entropy_token == "test-entropy"
    assert set(result.values) == set(source)
    assert len(result.values) == len(source)
    assert result.values != source


def test_exploration_order_is_replayable_when_entropy_is_injected() -> None:
    source = ("a", "b", "c", "d", "e")
    first = permute_exploration(source, sampler=deterministic_test_sampler(91), entropy_token="same")
    second = permute_exploration(source, sampler=deterministic_test_sampler(91), entropy_token="same")

    assert first == second
    assert first.to_dict()["authority"] == "exploration-only"


def test_exploration_refuses_empty_or_duplicate_space() -> None:
    import pytest

    with pytest.raises(ValueError, match="at least one"):
        permute_exploration(())
    with pytest.raises(ValueError, match="distinct"):
        permute_exploration(("repair", "repair"))
