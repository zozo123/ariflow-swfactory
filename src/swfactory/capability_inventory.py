"""Versioned capability inventory for support and evidence claims.

The inventory is declarative.  It does not make a capability true by naming it.  A claim moves from
``declared`` to ``integrated`` to ``validated`` only when the listed runtime path and verification
surface exist.  Candidate-specific evidence is recorded separately by :mod:`candidate_readiness`.

It is also the single public truth for a claimed feature.  Two mechanisms keep it that way, and
both run in the required ``test`` job through ``python -m swfactory.capability_inventory``:

* every ``test``/``evidence`` reference must *resolve* — a cited file must exist and a cited
  ``ci:`` job must exist in ``.github/workflows`` — because a claim citing a deleted test is
  indistinguishable, to a reader, from a verified one;
* every feature statement in ``README.md`` and ``site/`` is generated from a claim or scored
  against it, so prose can never describe a capability at a stronger support level than the
  inventory carries.
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, NamedTuple

import yaml

SCHEMA_VERSION = 1
VALID_STATES = frozenset({"declared", "integrated", "validated", "unsupported", "experimental"})
VALID_SUPPORT = frozenset({"supported", "test_only", "experimental", "unsupported"})
REQUIRED_FIELDS = frozenset(
    {
        "id",
        "state",
        "support",
        "invariant",
        "owner",
        "runtime_entry",
        "environment",
        "test",
        "evidence",
        # Mandatory so a claim cannot opt out of the prose rule by declaring no surface. Without
        # it, the cheapest way to describe an experimental capability as shipped is to remove the
        # tokens that let the checker find the sentence.
        "public_surface",
    }
)


class CapabilityInventoryError(ValueError):
    """A capability claim is incomplete or internally inconsistent."""


def validate_claim(claim: dict[str, Any]) -> None:
    missing = REQUIRED_FIELDS - set(claim)
    if missing:
        raise CapabilityInventoryError(f"capability claim missing fields: {sorted(missing)}")
    ident = str(claim["id"]).strip()
    if not ident or len(ident) > 160:
        raise CapabilityInventoryError("capability id must be nonempty and bounded")
    state = str(claim["state"])
    support = str(claim["support"])
    if state not in VALID_STATES:
        raise CapabilityInventoryError(f"{ident}: invalid state {state!r}")
    if support not in VALID_SUPPORT:
        raise CapabilityInventoryError(f"{ident}: invalid support {support!r}")
    for field in ("invariant", "owner", "runtime_entry", "environment", "test"):
        if not str(claim[field]).strip():
            raise CapabilityInventoryError(f"{ident}: {field} must be nonempty")
    evidence = claim["evidence"]
    if not isinstance(evidence, list) or any(not isinstance(item, str) or not item.strip() for item in evidence):
        raise CapabilityInventoryError(f"{ident}: evidence must be a list of nonempty references")
    if state == "validated" and not evidence:
        raise CapabilityInventoryError(f"{ident}: validated claims require evidence references")
    if support == "supported" and state != "validated":
        raise CapabilityInventoryError(f"{ident}: supported claims must be validated")
    if state in {"experimental", "unsupported"} and support == "supported":
        raise CapabilityInventoryError(f"{ident}: experimental/unsupported claim cannot be supported")
    follow_up = claim.get("follow_up")
    if state == "experimental" and (not isinstance(follow_up, str) or not follow_up.strip()):
        raise CapabilityInventoryError(f"{ident}: experimental claims require an explicit follow_up")


def validate_inventory(document: dict[str, Any]) -> dict[str, Any]:
    if document.get("schema_version") != SCHEMA_VERSION:
        raise CapabilityInventoryError(f"unsupported capability inventory schema {document.get('schema_version')!r}")
    claims = document.get("claims")
    if not isinstance(claims, list):
        raise CapabilityInventoryError("claims must be a list")
    seen: set[str] = set()
    for claim in claims:
        if not isinstance(claim, dict):
            raise CapabilityInventoryError("every capability claim must be an object")
        validate_claim(claim)
        ident = str(claim["id"])
        if ident in seen:
            raise CapabilityInventoryError(f"duplicate capability id {ident!r}")
        seen.add(ident)
    return document


def load_inventory(path: Path) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise CapabilityInventoryError("capability inventory root must be an object")
    return validate_inventory(document)


def support_matrix(document: dict[str, Any]) -> dict[str, str]:
    validate_inventory(document)
    return {str(row["id"]): str(row["support"]) for row in document["claims"]}


# ---------------------------------------------------------------- resolvable references

# A claim may cite a file (any path with a slash and an extension) or a CI job written as
# ``ci:<name>``.  Prose around those references stays free-form; the references themselves are
# machine-checked, because a claim that cites a deleted test reads exactly like a verified one.
_FILE_REFERENCE = re.compile(r"(?<![\w./-])((?:[\w.-]+/)+[\w.-]+\.[A-Za-z0-9]+)")
_CI_REFERENCE = re.compile(r"(?<![\w-])ci:([A-Za-z0-9][\w.-]*)")


def references(text: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Split one ``test``/``evidence`` string into its file paths and its ``ci:`` job names."""
    return tuple(_FILE_REFERENCE.findall(text)), tuple(_CI_REFERENCE.findall(text))


def ci_identifiers(workflows: Path) -> set[str]:
    """Every CI name a claim may legitimately cite: workflow file, workflow name, job id, job name."""
    found: set[str] = set()
    for path in sorted(workflows.glob("*.yml")) + sorted(workflows.glob("*.yaml")):
        found.update({path.name, path.stem})
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as error:  # pragma: no cover - a broken workflow fails CI first
            raise CapabilityInventoryError(f"unreadable workflow {path}: {error}") from error
        if not isinstance(document, dict):
            continue
        if isinstance(name := document.get("name"), str):
            found.add(name)
        jobs = document.get("jobs")
        if not isinstance(jobs, dict):
            continue
        for job_id, job in jobs.items():
            found.add(str(job_id))
            if isinstance(job, dict) and isinstance(job.get("name"), str):
                found.add(job["name"])
    return found


def unresolved_references(document: dict[str, Any], *, root: Path) -> list[str]:
    """Claims whose verification surface cannot be reached from the repository as it stands."""
    validate_inventory(document)
    known = ci_identifiers(root / ".github" / "workflows")
    problems: list[str] = []
    for claim in document["claims"]:
        ident = str(claim["id"])
        fields = [("test", str(claim["test"]))]
        fields += [(f"evidence[{index}]", str(item)) for index, item in enumerate(claim["evidence"])]
        for field, value in fields:
            files, jobs = references(value)
            missing = [name for name in files if not (root / name).is_file()]
            problems += [f"{ident}: {field} cites missing file {name!r}" for name in missing]
            problems += [f"{ident}: {field} cites unknown CI job {name!r}" for name in jobs if name not in known]
        if not any(references(str(claim["test"]))):
            problems.append(f"{ident}: test names no resolvable file or ci: job")
    return problems


def validate_references(document: dict[str, Any], *, root: Path) -> dict[str, Any]:
    """Fail closed when a claim cites verification that no longer exists."""
    if problems := unresolved_references(document, root=root):
        raise CapabilityInventoryError("unresolvable capability references: " + "; ".join(problems))
    return document


# ---------------------------------------------------------------- public surface

SUPPORT_RANK: dict[str, int] = {"unsupported": 0, "test_only": 1, "experimental": 2, "supported": 3}

# The words a reader actually meets in README.md and site/, scored against the four support
# levels.  Nobody browsing the site opens this JSON, so prose is where a claim silently outruns
# its evidence; scoring the vocabulary is what lets a test catch the overstatement.
PUBLIC_SUPPORT_PHRASES: dict[str, int] = {
    "adapter": 0,
    "custom backend required": 0,
    "not supported": 0,
    "seam only": 0,
    "unsupported": 0,
    "scripted replay only": 1,
    "test only": 1,
    "test_only": 1,
    "advisory": 2,
    "experimental": 2,
    "preview": 2,
    "built in": 3,
    "built-in": 3,
    "generally available": 3,
    "guaranteed": 3,
    "production ready": 3,
    "production-ready": 3,
    "shipped": 3,
    "ships": 3,
    "stable": 3,
    "supported": 3,
    # Measured against real overstatements of an experimental claim: of eight natural phrasings,
    # only "production-ready" was caught. These are the other seven, plus the near neighbours that
    # showed up while probing.
    "battle-tested": 3,
    "enterprise-ready": 3,
    "for production": 3,
    "fully qualified": 3,
    "fully productionized": 3,
    "generally-available": 3,
    "hardened": 3,
    "is ga": 3,
    "mature": 3,
    "out of the box": 3,
    "production deployments": 3,
    "production path": 3,
    "production use": 3,
    "production workloads": 3,
    "ready for production": 3,
}

CLAIM_TABLE_START = "<!-- capability-inventory:start -->"
CLAIM_TABLE_END = "<!-- capability-inventory:end -->"
SANDBOX_TABLE_MARKER = "<!-- capability-surface:sandboxes -->"
INVENTORY_PATH = "config/capability-inventory.json"

_SITE_ROW = re.compile(r"<tr\s+data-sandbox=\"(?P<profile>[^\"]+)\"(?P<attrs>[^>]*)>(?P<cells>.*?)</tr>", re.S)
_SITE_CELL = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_SITE_CAPABILITY = re.compile(r"data-capability=\"([^\"]+)\"")
_TAG = re.compile(r"<[^>]+>")
_BACKTICKED = re.compile(r"`([^`]+)`")
# A sentence ends at .!? followed by space and something that starts a new sentence. Requiring the
# next character to be a capital or a backtick keeps dotted identifiers such as
# `swfactory.sandbox` intact, since those carry no space after the dot.
_SENTENCE = re.compile(r"(?<=[.!?])\s+(?=[A-Z`<])")


def phrase_rank(text: str) -> tuple[int, str] | None:
    """The strongest availability phrase in one line of public prose, if it makes one at all.

    Longest phrases are scored first and their span is blanked, so ``not supported`` can never be
    read a second time as the much stronger ``supported``.
    """
    remaining = text.casefold()
    best: tuple[int, str] | None = None
    for phrase in sorted(PUBLIC_SUPPORT_PHRASES, key=len, reverse=True):
        pattern = re.compile(rf"(?<![\w-]){re.escape(phrase)}(?![\w-])")
        while match := pattern.search(remaining):
            rank = PUBLIC_SUPPORT_PHRASES[phrase]
            if best is None or rank > best[0]:
                best = (rank, phrase)
            remaining = remaining[: match.start()] + " " * (match.end() - match.start()) + remaining[match.end() :]
    return best


def render_claims_table(document: dict[str, Any]) -> str:
    """Render the public claim table README ships, straight from the inventory rows."""
    validate_inventory(document)
    lines = [
        CLAIM_TABLE_START,
        f"<!-- Generated from {INVENTORY_PATH}; run `uv run python -m swfactory.capability_inventory --write`. -->",
        "",
        "| Claim | Support | Runtime entry | Verified by |",
        "| --- | --- | --- | --- |",
    ]
    for claim in document["claims"]:
        lines.append(f"| `{claim['id']}` | `{claim['support']}` | `{claim['runtime_entry']}` | {claim['test']} |")
    lines += ["", CLAIM_TABLE_END]
    return "\n".join(lines)


def claims_table(text: str) -> str:
    """The generated block as it currently stands in a document."""
    start = text.find(CLAIM_TABLE_START)
    end = text.find(CLAIM_TABLE_END)
    if start < 0 or end < start:
        raise CapabilityInventoryError("README is missing the generated capability-inventory block")
    return text[start : end + len(CLAIM_TABLE_END)]


def write_claims_table(document: dict[str, Any], readme: Path) -> bool:
    """Regenerate the README block in place; returns whether anything changed."""
    text = readme.read_text(encoding="utf-8")
    current = claims_table(text)
    rendered = render_claims_table(document)
    if current == rendered:
        return False
    readme.write_text(text.replace(current, rendered), encoding="utf-8")
    return True


class SurfaceRow(NamedTuple):
    """One public row that says how available a sandbox profile is."""

    source: str
    line: int
    profile: str
    claim_id: str | None
    status: str


def readme_surface_rows(text: str, *, source: str = "README.md") -> list[SurfaceRow]:
    """Rows of the README sandbox table that follows ``SANDBOX_TABLE_MARKER``."""
    rows: list[SurfaceRow] = []
    inside = False
    for number, line in enumerate(text.splitlines(), start=1):
        if SANDBOX_TABLE_MARKER in line:
            inside = True
            continue
        if not inside:
            continue
        if not line.startswith("|"):
            break
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 4 or set(cells[0]) <= set("- ") or cells[0].lower() == "sandbox":
            continue
        claim = _BACKTICKED.search(cells[1])
        rows.append(SurfaceRow(source, number, cells[0], claim.group(1) if claim else None, cells[2]))
    return rows


def site_surface_rows(text: str, *, source: str) -> list[SurfaceRow]:
    """Rows of the site sandbox table, which carry their claim id as ``data-capability``."""
    rows: list[SurfaceRow] = []
    for match in _SITE_ROW.finditer(text):
        cells = [_TAG.sub("", cell).strip() for cell in _SITE_CELL.findall(match.group("cells"))]
        if len(cells) < 2:
            continue
        capability = _SITE_CAPABILITY.search(match.group("attrs"))
        line = text.count("\n", 0, match.start()) + 1
        rows.append(
            SurfaceRow(source, line, match.group("profile"), capability.group(1) if capability else None, cells[1])
        )
    return rows


def surface_findings(document: dict[str, Any], rows: list[SurfaceRow]) -> list[str]:
    """Public rows that promise availability the inventory does not carry."""
    validate_inventory(document)
    matrix = support_matrix(document)
    findings: list[str] = []
    for row in rows:
        where = f"{row.source}:{row.line} ({row.profile})"
        scored = phrase_rank(row.status)
        if scored is None:
            findings.append(f"{where}: status {row.status!r} uses no known support word")
            continue
        rank, phrase = scored
        if row.claim_id is None:
            if rank >= SUPPORT_RANK["experimental"]:
                findings.append(f"{where}: {phrase!r} describes an available capability with no claim")
            continue
        if row.claim_id not in matrix:
            findings.append(f"{where}: cites unknown capability claim {row.claim_id!r}")
            continue
        expected = SUPPORT_RANK[matrix[row.claim_id]]
        if rank != expected:
            findings.append(f"{where}: {phrase!r} does not match {row.claim_id} support {matrix[row.claim_id]!r}")
    return findings


def overstatements(document: dict[str, Any], sources: Mapping[str, str]) -> list[str]:
    """Public sentences that promise more than the claim they are describing."""
    validate_inventory(document)
    claims = document["claims"]
    findings: list[str] = []
    for name, text in sources.items():
        for number, line in _prose_units(text):
            mentioned = [claim for claim in claims if _mentions(claim, line)]
            if not mentioned:
                continue
            scored = phrase_rank(line)
            if scored is None:
                continue
            rank, phrase = scored
            # Every mentioned claim is judged on its own. Taking `max()` across them let one
            # sentence launder another: "Both `Plan.work` and the `islo` sandbox are
            # production-ready" mentions a supported claim, so the experimental one rode along
            # unflagged. If a sentence promises more than a claim carries, that claim is
            # overstated regardless of what else the sentence also names.
            overstated = sorted(str(claim["id"]) for claim in mentioned if rank > SUPPORT_RANK[str(claim["support"])])
            if overstated:
                findings.append(f"{name}:{number}: {phrase!r} overstates {', '.join(overstated)}")
    return findings


def _prose_units(text: str) -> list[tuple[int, str]]:
    """Yield (line number, text) over sentences as a reader meets them, not as the file wraps them.

    README and the docs are hard-wrapped near 100 columns, so a claim token and the phrase that
    overstates it routinely land on different physical lines and a line-at-a-time scan sees
    neither sentence. Consecutive prose lines are joined; table rows, headings and HTML rows stay
    separate, because joining a table would put one row's `supported` next to another row's claim
    and invent an overstatement that nobody wrote.
    """
    units: list[tuple[int, str]] = []
    buffer: list[str] = []
    start = 0

    def flush() -> None:
        if buffer:
            # Unwrap first, then split into sentences. Only unwrapping would glue neighbouring
            # sentences together, and a paragraph whose first sentence says "built-in" and whose
            # third happens to mention `islo` would be reported as an overstatement nobody wrote.
            # A check that cries wolf gets switched off, which costs more than the misses.
            units.extend((start, part) for part in _SENTENCE.split(" ".join(buffer)) if part.strip())
            buffer.clear()

    for number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        standalone = not stripped or stripped.startswith(("|", "#", "<tr", "</tr", "<td", "<th", "```", "- ", "* "))
        if standalone:
            flush()
            if stripped:
                units.append((number, line))
            continue
        if not buffer:
            start = number
        buffer.append(stripped)
    flush()
    return units


def _mentions(claim: dict[str, Any], line: str) -> bool:
    # Case-insensitive: "Islo", "islo" and "ISLO" name one capability, and a sentence does not
    # stop overstating a claim by starting with it.
    haystack = line.lower()
    surface = claim.get("public_surface") or []
    tokens = (str(claim["id"]), *(str(item) for item in surface))
    # Word boundaries, not substrings. `docker sandbox` used to match inside "Docker Sandboxes" --
    # the name of an upstream Airflow provider in docs/design.md -- and reported our experimental
    # docker sandbox as overstated by a sentence about somebody else's shipped product.
    return any(re.search(rf"(?<!\w){re.escape(token.lower())}(?!\w)", haystack) for token in tokens)


def public_surface_findings(document: dict[str, Any], *, root: Path) -> list[str]:
    """Every way README and the site can disagree with the inventory, in one list."""
    readme = (root / "README.md").read_text(encoding="utf-8")
    # Recursive: a future site/docs/claims.html would otherwise be outside the rule entirely.
    pages = {
        str(path.relative_to(root / "site")): path.read_text(encoding="utf-8")
        for path in sorted((root / "site").rglob("*.html"))
    }
    # The prose rule follows the reader, not one file. Scanning README and the site alone left
    # "The built-in choices are `local`, `srt`, `docker`, `islo`" standing in OPERATIONS.md and
    # docs/swf.md -- the same sentence that was corrected on the site, one directory over.
    prose = {
        str(path.relative_to(root)): path.read_text(encoding="utf-8")
        for path in [root / "OPERATIONS.md", *sorted((root / "docs").glob("*.md"))]
        if path.exists()
    }
    findings: list[str] = []
    if claims_table(readme) != render_claims_table(document):
        findings.append("README.md: the generated capability table is stale; run --write")
    rows = readme_surface_rows(readme)
    for name, page in pages.items():
        rows += site_surface_rows(page, source=f"site/{name}")
    findings += surface_findings(document, rows)
    findings += overstatements(
        document,
        {"README.md": readme, **{f"site/{k}": v for k, v in pages.items()}, **prose},
    )
    return findings


def validate_public_surface(document: dict[str, Any], *, root: Path) -> dict[str, Any]:
    """Fail closed when the public documents claim more than the inventory backs."""
    if findings := public_surface_findings(document, root=root):
        raise CapabilityInventoryError("public capability claims disagree with the inventory: " + "; ".join(findings))
    return document


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the software-factory capability inventory")
    parser.add_argument("path", nargs="?", default=INVENTORY_PATH)
    parser.add_argument("--root", default=".", help="repository root the claims are checked against")
    parser.add_argument("--write", action="store_true", help="regenerate the README claim table in place")
    args = parser.parse_args(argv)
    root = Path(args.root)
    document = load_inventory(Path(args.path))
    if args.write:
        changed = write_claims_table(document, root / "README.md")
        print(json.dumps({"readme_claim_table": "rewritten" if changed else "unchanged"}, sort_keys=True))
    validate_references(document, root=root)
    validate_public_surface(document, root=root)
    counts: dict[str, int] = {}
    for row in document["claims"]:
        key = str(row["state"])
        counts[key] = counts.get(key, 0) + 1
    print(
        json.dumps(
            {"schema_version": SCHEMA_VERSION, "claims": len(document["claims"]), "states": counts}, sort_keys=True
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI seam
    raise SystemExit(main())
