"""A descendant candidate's identity binds the parent decision it descends from."""

from __future__ import annotations

import pytest

from swfactory.evolution import CampaignError, Strategy, plan_requests


def test_child_identity_includes_parent_decision_digest() -> None:
    kwargs = {
        "campaign_id": "round-1",
        "cell_id": "cell",
        "epoch": 1,
        "input_head": "b" * 40,
        "strategies": (Strategy.REPAIR,),
        "parent_candidate": "cand-parent",
        "depth": 1,
    }
    first = plan_requests(parent_decision_digest="sha256:" + "1" * 64, **kwargs)[0]
    second = plan_requests(parent_decision_digest="sha256:" + "2" * 64, **kwargs)[0]

    assert first.logical_id != second.logical_id


def test_invalid_parent_decision_digest_is_refused() -> None:
    with pytest.raises(CampaignError, match="canonical sha256"):
        plan_requests(
            campaign_id="round-1",
            cell_id="cell",
            epoch=1,
            input_head="b" * 40,
            strategies=(Strategy.REPAIR,),
            parent_candidate="cand-parent",
            parent_decision_digest="not-a-digest",
            depth=1,
        )
