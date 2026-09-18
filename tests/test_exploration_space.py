from swfactory.exploration_space import build_exploration_space


def test_exploration_space_combines_orthogonal_hypotheses_without_authority() -> None:
    order, variants = build_exploration_space(
        {
            "strategy": ("repair", "rethink", "scratch"),
            "lens": ("minimal", "adversarial"),
            "tool_profile": ("code", "docs"),
        },
        max_variants=12,
    )

    assert len(variants) == 12
    assert len({item.logical_id for item in variants}) == 12
    assert set(order.values) == {item.logical_id for item in variants}
    assert all(item.to_dict()["authority"] == "exploration-only" for item in variants)


def test_exploration_space_respects_budget_without_collapsing_axes() -> None:
    _, variants = build_exploration_space(
        {
            "strategy": ("repair", "rethink", "scratch"),
            "review_lens": ("correctness", "security", "simplicity"),
        },
        max_variants=4,
    )

    assert len(variants) == 4
    assert all(dict(item.axes)["strategy"] in {"repair", "rethink", "scratch"} for item in variants)
    assert all(
        dict(item.axes)["review_lens"] in {"correctness", "security", "simplicity"} for item in variants
    )
