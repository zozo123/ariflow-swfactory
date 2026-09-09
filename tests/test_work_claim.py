"""Many harness sessions, one backlog: who works what, and who stands down.

`publication_identity` stops two sessions publishing twice. These tests cover the earlier and more
expensive question -- whether they both burn an agent loop at all.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from swfactory.work_claim import (
    DEFAULT_LEASE_S,
    Claim,
    claim_ref,
    may_take,
    parse_claim,
    refusal,
)

KEY = "3f94aa133b28"
T0 = datetime(2026, 9, 9, 12, 0, tzinfo=UTC)


def _claim(instance: str, *, at: datetime = T0, lease_s: float = DEFAULT_LEASE_S) -> Claim:
    return Claim(key=KEY, instance=instance, at=at, lease_s=lease_s)


def test_a_live_claim_sends_the_other_session_elsewhere() -> None:
    """The whole point: N sessions looping one backlog must not all pick the same issue."""
    held = _claim("swf-a")
    assert may_take(held, instance="swf-a", now=T0), "the holder may renew its own claim"
    assert not may_take(held, instance="swf-b", now=T0)
    assert may_take(None, instance="swf-b", now=T0), "unclaimed work is takeable"

    why = refusal(held, now=T0 + timedelta(minutes=10))
    assert "swf-a" in why and "3000s" in why
    assert "take other work" in why, "a refusal has to tell the session what to do instead"


def test_a_dead_session_does_not_strand_its_issue_forever() -> None:
    """A harness session dies in ways that leave no trace in the repo -- a context limit, a killed
    container, a spend limit reached mid-loop. A lock with no expiry would hold the issue for good;
    the lease bounds it to one period."""
    held = _claim("swf-a", lease_s=60)
    assert not may_take(held, instance="swf-b", now=T0 + timedelta(seconds=59))
    assert may_take(held, instance="swf-b", now=T0 + timedelta(seconds=61))
    assert held.expired(now=T0 + timedelta(seconds=61))


def test_a_claim_survives_the_round_trip_through_a_commit_message() -> None:
    """The claim lives in a commit message so an operator diagnosing a stuck backlog can read it
    with git alone, on a machine with none of this installed."""
    original = _claim("swf-a", lease_s=900)
    parsed = parse_claim(KEY, original.message())
    assert parsed == original


def test_a_commit_that_merely_mentions_the_key_is_not_a_claim() -> None:
    """Otherwise an unrelated ref in this namespace could hold the backlog hostage."""
    assert parse_claim(KEY, f"chore: mention {KEY} in passing\n\ninstance=swf-a\n") is None
    assert parse_claim(KEY, f"swf-claim {KEY}\n\nat={T0.isoformat()}\n") is None, "no instance"
    assert parse_claim(KEY, f"swf-claim {KEY}\n\ninstance=swf-a\nat=not-a-date\n") is None
    assert parse_claim(KEY, f"swf-claim other-key\n\ninstance=swf-a\nat={T0.isoformat()}\n") is None


def test_claims_live_under_their_own_ref_namespace() -> None:
    """Not under refs/heads: a claim is not a branch, must not appear in a branch listing, and must
    not be fetched by a default clone into somebody's working checkout."""
    ref = claim_ref(KEY)
    assert ref == f"refs/swf/claims/{KEY}"
    assert not ref.startswith("refs/heads/") and not ref.startswith("refs/tags/")


def test_a_claim_authorizes_nothing() -> None:
    """The publishing path must not consult claims. #2093 was refused because a coordination signal
    looked like authority; this one cannot, because nothing on the mutation path reads it."""
    from pathlib import Path

    # The MUTATION path, named explicitly. `cli.py` legitimately reads claims -- it is the operator
    # surface a harness session asks before spending energy -- so a blanket "nothing imports this"
    # would be the wrong invariant, and a first version asserted exactly that. What must hold is
    # that nothing which publishes, transitions a Cell or commits an operation consults a claim.
    root = Path(__file__).resolve().parents[1] / "src" / "swfactory"
    mutation_path = [
        root / "stages.py",
        root / "scm.py",
        root / "cells.py",
        root / "core_capabilities.py",
        root / "idempotency.py",
        root / "publication_identity.py",
        root / "backend" / "service.py",
        root / "backend" / "scm_service.py",
        root / "backend" / "core_service.py",
    ]
    consumers = sorted(
        path.name for path in mutation_path if path.exists() and "work_claim" in path.read_text(encoding="utf-8")
    )
    assert not consumers, f"a mutation path consults a claim: {consumers}"


def test_the_cli_reports_the_ref_a_run_actually_publishes() -> None:
    """`swf claim` is what a harness session reads before deciding to spend energy. If it named a
    different branch than `deliver` pushes, the operator would be told to look at a ref that does
    not exist -- and the first version did exactly that, deriving the id from the filename
    (`demo/issue.md` -> `issue`) rather than the front matter (`id: DEMO-1`).
    """
    import json
    import subprocess

    from swfactory.config import Config
    from swfactory.publication_identity import publication_key

    out = subprocess.run(
        ["uv", "run", "swfactory", "claim", "demo/issue.md", "--json"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    reported = json.loads(out)
    cfg = Config(issue="demo/issue.md")
    expected_key = publication_key(cfg.repo, cfg.target_dir, "DEMO-1")

    assert reported["issue_id"] == "DEMO-1", "the declared id, not the filename stem"
    assert reported["publication_key"] == expected_key
    assert reported["branch"] == f"factory/DEMO-1-{expected_key}"
    assert reported["claim_ref"] == claim_ref(expected_key)
