"""The capability inventory has to be the single public truth, not a second opinion.

The inventory was already structurally validated, so every claim carried a ``test`` and an
``evidence`` string.  Neither was ever resolved: a claim could cite a test file that had been
deleted, and README and ``site/`` described the same features in independent prose, so a
capability could read as shipped on the site while the inventory called it experimental.

These tests close both gaps.  Each one mutates a copy of the real documents or the real inventory
to prove the check bites rather than merely passing on a tree that happens to be clean.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from swfactory.capability_inventory import (
    REQUIRED_FIELDS,
    SUPPORT_RANK,
    CapabilityInventoryError,
    ci_identifiers,
    claims_table,
    load_inventory,
    overstatements,
    phrase_rank,
    public_surface_findings,
    readme_surface_rows,
    render_claims_table,
    site_surface_rows,
    support_matrix,
    unresolved_references,
    validate_public_surface,
    validate_references,
)

ROOT = Path(__file__).resolve().parents[1]
INVENTORY_PATH = ROOT / "config" / "capability-inventory.json"
README = ROOT / "README.md"
SITE = ROOT / "site" / "index.html"


@pytest.fixture
def document() -> dict[str, Any]:
    return load_inventory(INVENTORY_PATH)


def _claim(document: dict[str, Any], ident: str) -> dict[str, Any]:
    return next(claim for claim in document["claims"] if claim["id"] == ident)


# ---------------------------------------------------------------- resolvable references


def test_every_claim_cites_verification_that_actually_exists(document: dict[str, Any]) -> None:
    """The failure this repository actually had: four claims naming test files nobody wrote."""
    assert unresolved_references(document, root=ROOT) == []


def test_a_claim_that_cites_a_deleted_test_file_is_refused(document: dict[str, Any]) -> None:
    mutated = copy.deepcopy(document)
    _claim(mutated, "recovery.external-effects")["test"] = "tests/test_deleted_by_someone.py"

    problems = unresolved_references(mutated, root=ROOT)

    assert any("tests/test_deleted_by_someone.py" in problem for problem in problems)
    with pytest.raises(CapabilityInventoryError, match="unresolvable capability references"):
        validate_references(mutated, root=ROOT)


def test_a_claim_that_cites_a_job_no_workflow_defines_is_refused(document: dict[str, Any]) -> None:
    mutated = copy.deepcopy(document)
    _claim(mutated, "sandbox.srt")["test"] = "ci:srt-smoke-that-was-renamed"

    problems = unresolved_references(mutated, root=ROOT)

    assert any("srt-smoke-that-was-renamed" in problem for problem in problems)


def test_a_claim_whose_test_names_nothing_checkable_is_refused(document: dict[str, Any]) -> None:
    """Prose alone is what let ``generation contract tests`` mean nothing for as long as it did."""
    mutated = copy.deepcopy(document)
    _claim(mutated, "factory.generations")["test"] = "contract tests we intend to write"

    assert any("names no resolvable" in problem for problem in unresolved_references(mutated, root=ROOT))


def test_ci_identifiers_cover_job_ids_workflow_files_and_workflow_names() -> None:
    known = ci_identifiers(ROOT / ".github" / "workflows")

    assert {"test", "srt-smoke", "docker-smoke", "airflow-parity", "evals-islo"} <= known
    assert {"ci.yml", "ci", "evals.yml", "control-plane-gate"} <= known


# ---------------------------------------------------------------- generated public claims


def test_the_readme_claim_table_is_generated_from_the_inventory(document: dict[str, Any]) -> None:
    assert claims_table(README.read_text(encoding="utf-8")) == render_claims_table(document)


def test_every_claim_reaches_the_reader_with_its_support_level(document: dict[str, Any]) -> None:
    """A reader never opens the JSON, so each claim and its support word must be on the page."""
    table = claims_table(README.read_text(encoding="utf-8"))

    for claim in document["claims"]:
        assert f"`{claim['id']}`" in table, claim["id"]
        assert f"`{claim['support']}`" in table, claim["id"]


def test_an_edited_claim_table_no_longer_matches_the_inventory(document: dict[str, Any], tmp_path: Path) -> None:
    """The table is only honest while it is regenerated; a hand-edit has to be caught."""
    tree = _tree(tmp_path)
    text = (tree / "README.md").read_text(encoding="utf-8")
    (tree / "README.md").write_text(text.replace("`experimental`", "`supported`", 1), encoding="utf-8")

    findings = public_surface_findings(document, root=tree)

    assert any("generated capability table is stale" in finding for finding in findings)


# ---------------------------------------------------------------- overstatement


def test_the_public_documents_agree_with_the_inventory(document: dict[str, Any]) -> None:
    assert public_surface_findings(document, root=ROOT) == []
    validate_public_surface(document, root=ROOT)


def test_readme_prose_that_promotes_an_experimental_sandbox_fails(document: dict[str, Any]) -> None:
    """The exact mutation the issue asks for: say ``supported`` where the claim says otherwise."""
    overstated = "The `islo` sandbox is a supported production path.\n"

    findings = overstatements(document, {"README.md": overstated})

    assert findings and "sandbox.islo" in findings[0]


def test_site_status_stronger_than_the_claim_fails(document: dict[str, Any], tmp_path: Path) -> None:
    tree = _tree(tmp_path)
    page = tree / "site" / "index.html"
    text = page.read_text(encoding="utf-8")
    row = next(line for line in text.splitlines() if 'data-sandbox="docker"' in line)
    page.write_text(text.replace(row, row.replace(">experimental<", ">built in<")), encoding="utf-8")

    findings = public_surface_findings(document, root=tree)

    assert any("sandbox.docker" in finding for finding in findings), findings


def test_a_site_row_claiming_availability_without_a_claim_fails(document: dict[str, Any], tmp_path: Path) -> None:
    """``daytona`` is an integration seam; describing it as available needs a claim first."""
    tree = _tree(tmp_path)
    page = tree / "site" / "index.html"
    text = page.read_text(encoding="utf-8")
    row = next(line for line in text.splitlines() if 'data-sandbox="daytona"' in line)
    page.write_text(text.replace(row, row.replace(">custom backend required<", ">built in<")), encoding="utf-8")

    findings = public_surface_findings(document, root=tree)

    assert any("no claim" in finding for finding in findings), findings


def test_a_public_row_that_drops_its_claim_link_is_caught(document: dict[str, Any], tmp_path: Path) -> None:
    tree = _tree(tmp_path)
    readme = tree / "README.md"
    text = readme.read_text(encoding="utf-8")
    readme.write_text(text.replace("`sandbox.islo`", "the islo provider", 1), encoding="utf-8")

    findings = public_surface_findings(document, root=tree)

    assert any("no claim" in finding for finding in findings), findings


# ---------------------------------------------------------------- scoring


def test_a_refusal_is_never_scored_as_a_promise() -> None:
    """``supported`` is a substring of ``unsupported``; scoring it that way would invert honesty."""
    assert phrase_rank("this path is unsupported") == (0, "unsupported")
    assert phrase_rank("the policy is not supported here") == (0, "not supported")
    assert phrase_rank("supported") == (SUPPORT_RANK["supported"], "supported")
    assert phrase_rank("a durable Factory Cell owns the work") is None


def test_the_strongest_phrase_on_a_line_is_the_one_that_counts() -> None:
    """A hedge next to a promise is still a promise; the line is scored by its strongest word."""
    assert phrase_rank("experimental today, production-ready tomorrow") == (
        SUPPORT_RANK["supported"],
        "production-ready",
    )


def test_public_rows_are_read_from_both_documents(document: dict[str, Any]) -> None:
    matrix = support_matrix(document)
    rows = readme_surface_rows(README.read_text(encoding="utf-8"))
    rows += site_surface_rows(SITE.read_text(encoding="utf-8"), source="site/index.html")

    linked = {row.claim_id for row in rows if row.claim_id}
    assert {"sandbox.local-scripted", "sandbox.srt", "sandbox.docker", "sandbox.islo"} <= linked
    assert all(claim_id in matrix for claim_id in linked)
    assert [row for row in rows if row.source == "README.md"], "the README sandbox table must be marked"


def _tree(tmp_path: Path) -> Path:
    """A throwaway copy of the public documents, so a mutation test never edits the repository."""
    (tmp_path / "site").mkdir(parents=True, exist_ok=True)
    (tmp_path / "README.md").write_text(README.read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / "site" / "index.html").write_text(SITE.read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "capability-inventory.json").write_text(
        json.dumps(load_inventory(INVENTORY_PATH), indent=2) + "\n", encoding="utf-8"
    )
    return tmp_path


OVERSTATEMENTS = [
    "The islo sandbox is production-ready and generally available today.",
    "The srt sandbox is stable and generally available.",
    "Docker sandboxing ships today as a production-ready isolation boundary.",
    "We support the `islo` sandbox for production workloads.",
    "The `islo` sandbox is battle-tested, fully qualified and ready for production use.",
    "The `islo` sandbox is mature and hardened.",
    "Both `Plan.work` and the `islo` sandbox are production-ready.",
    "The islo sandbox works out of the box.",
    "The srt sandbox is GA.",
    "The islo sandbox is recommended for production deployments.",
    # Hard-wrapped: README wraps near 100 columns, so a claim token and the phrase that overstates
    # it routinely land on different physical lines.
    "The `islo` sandbox boundary is one we now consider fully\nproduction-ready for everyday use.",
]


@pytest.mark.parametrize("sentence", OVERSTATEMENTS)
def test_every_natural_way_to_overstate_an_experimental_sandbox_is_caught(sentence: str) -> None:
    """An honesty mechanism that catches one sentence shape is worse than none: it reads as
    coverage while a reader writes the same lie four other ways.

    The first version scored a single physical line against a claim's backticked tokens and took
    `max()` across every claim the line mentioned. Measured against these eleven, it caught one.
    Each entry here is a phrasing that sailed through: plain-prose naming instead of backticks,
    vocabulary it did not know, a supported claim in the same sentence laundering an experimental
    one, and a sentence split across two lines.
    """
    document = load_inventory(Path(INVENTORY_PATH))
    findings = overstatements(document, {"README.md": sentence})
    assert findings, f"not caught: {sentence!r}"


def test_a_shipped_product_with_a_colliding_name_is_not_an_overstatement() -> None:
    """`docs/design.md` describes an upstream Airflow provider called "Docker Sandboxes" that
    genuinely ships. Substring matching read that as our experimental docker sandbox being called
    shipped. Tokens match words, not prefixes — a check that cries wolf gets switched off."""
    document = load_inventory(Path(INVENTORY_PATH))
    line = "| `sbx` (Docker Sandboxes) | ships in the released `apache-airflow-providers-common-ai` |"
    assert overstatements(document, {"docs/design.md": line}) == []


def test_a_claim_cannot_opt_out_of_the_prose_rule() -> None:
    """`public_surface` is required. Without that, the cheapest way to describe an experimental
    capability as shipped is to delete the tokens that let the checker find the sentence."""
    assert "public_surface" in REQUIRED_FIELDS
