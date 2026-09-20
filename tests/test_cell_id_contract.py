"""One answer to "is this a Factory Cell id", checked identically in both languages.

`CellIdentity.stable_id` is the only minter of a Cell id and it produces `cell_` plus 24 lowercase
hex characters. That rule was being checked ten different ways:

    prefix only   airflow_binding.py, authority.py (x2), core_capabilities.py, idempotency.py,
                  liquid_security_runtime.py, and in Rust manager_protocol.rs and worker.rs
    prefix + len  runtime.py, security_contract.py
    full shape    swf-app/src/cells.rs (the operator surface, and only there)

So `cell_`, `cell_zzz` and `cell_` followed by two hundred characters were each valid to some
readers and invalid to others -- on the identity every epoch fence is keyed to. Nothing anywhere
checked the alphabet, though the minter can only ever produce lowercase hex.

These tests read `tests/fixtures/contract/is_cell_id.json` through the same contract-equivalence
harness that pins `policy_digest`, so the Rust crate answers the identical cases and a divergence
fails on both sides rather than in whichever language happens to be looked at.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from swfactory.cells import (
    CELL_ID_DIGEST_LEN,
    CELL_ID_LEN,
    CELL_ID_PREFIX,
    CellIdentity,
    is_cell_id,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "contract" / "is_cell_id.json"
CONTRACT = json.loads(FIXTURE.read_text(encoding="utf-8"))


ACCEPT = [c["input"]["value"] for c in CONTRACT["cases"] if c["expected"]]
REFUSE = [c for c in CONTRACT["cases"] if not c["expected"]]


def test_the_fixture_agrees_with_the_constants_the_code_uses() -> None:
    assert CONTRACT["prefix"] == CELL_ID_PREFIX
    assert CONTRACT["digest_len"] == CELL_ID_DIGEST_LEN
    assert CONTRACT["total_len"] == CELL_ID_LEN


@pytest.mark.parametrize("value", ACCEPT)
def test_every_accepted_shape_is_accepted(value: str) -> None:
    assert is_cell_id(value)


@pytest.mark.parametrize("case", REFUSE, ids=lambda c: c["name"])
def test_every_refused_shape_is_refused(case: dict) -> None:
    assert not is_cell_id(case["input"]["value"]), case.get("why", case["name"])


def test_what_the_minter_produces_is_what_the_readers_accept() -> None:
    """The only claim that matters: the contract must admit exactly the minter's output.

    A contract stricter than the minter refuses real Cells; one looser admits ids no Cell can have.
    """
    for repo, target, issue in [
        ("acme/repo", "main", "42"),
        ("a/b", "", "demo/issue.md"),
        ("zozo123/ariflow-swfactory", "release/1.2", "9999"),
        ("o/r", "main", "issue with spaces and Ünicode"),
    ]:
        minted = CellIdentity(repo=repo, target=target, issue=issue).stable_id()
        assert is_cell_id(minted), minted
        assert len(minted) == CELL_ID_LEN


def test_a_prefix_only_check_would_fail_this_suite() -> None:
    """Guards the guard. If someone restores `startswith("cell_")`, these are what break."""
    disagreements = [c["input"]["value"] for c in REFUSE if c["input"]["value"].startswith(CELL_ID_PREFIX)]
    assert disagreements, "the fixture must contain shapes a prefix-only check would admit"
    for value in disagreements:
        assert value.startswith(CELL_ID_PREFIX) and not is_cell_id(value)


def test_non_strings_are_refused_rather_than_raising() -> None:
    """Call sites pass values straight off JSON and Airflow conf; None and int must not explode."""
    for value in (None, 123, b"cell_0123456789abcdef01234567", ["cell_0123456789abcdef01234567"]):
        assert not is_cell_id(value)  # type: ignore[arg-type]


def test_no_reader_states_the_rule_for_itself() -> None:
    """The root cause was ten call sites each restating the rule. Pin that they now delegate.

    Greps the source rather than the behaviour: a second implementation that happens to agree today
    is exactly how the ten diverged in the first place.
    """
    src = Path(__file__).resolve().parents[1] / "src" / "swfactory"
    offenders = []
    for path in sorted(src.rglob("*.py")):
        if path.name == "cells.py":
            continue  # the one place the rule is allowed to exist
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if 'startswith("cell_")' in line or "startswith('cell_')" in line:
                offenders.append(f"{path.relative_to(src.parents[1])}:{number}")
    assert not offenders, "these restate the Cell id rule instead of calling is_cell_id: " + ", ".join(offenders)
