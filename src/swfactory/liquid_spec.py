"""The Liquid matrix as validated data, not as Python modules.

The backlog generators produced a 90-domain x 10-concern matrix (900 cells) plus a legacy
snapshot and three research waves.  That metadata used to be expressed as 41 near-identical
modules (``liquid_bundle_*``, ``physics_bundle_*``, ``legacy_bundle_*``) that no runtime path
imported.  C10 of the methodology forbids that: more issue slices must not imply more permanent
abstractions.  So the matrix lives in ``config/liquid-spec.yaml`` and this module is the checker.

The checker exists to make the matrix *falsifiable*.  A declarative file drifts into fiction the
moment nothing tests it, so:

* every ``runtime_anchor`` must resolve to a real module or top-level attribute under
  ``src/swfactory/`` -- resolution is done by filesystem lookup plus AST inspection, never by
  importing, so the check stays hermetic, side-effect free and cheap enough for CI;
* every ``capability_claim`` must exist in ``config/capability-inventory.json``, and a row may
  only call itself ``supported`` when the claim it points at is itself validated there.

Naming a domain does not make it work.  ``state``/``support`` share the vocabulary of
:mod:`capability_inventory` and default to ``declared``/``unsupported`` -- aspirational, not
covered.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .capability_inventory import VALID_STATES, VALID_SUPPORT, load_inventory

SCHEMA_VERSION = 1

#: The seven canonical owning roles.  A domain row must be owned by exactly one of them.
CANONICAL_OWNERS = (
    "authority",
    "airflow",
    "workgraph",
    "recovery",
    "security",
    "evidence",
    "operator",
)

#: Families may additionally be marked ``research`` -- kept as history, outside the product path.
VALID_FAMILY_STATES = frozenset(VALID_STATES | {"research"})

REQUIRED_CONCERN_FIELDS = frozenset({"id", "name", "action", "metric", "question"})
REQUIRED_FAMILY_FIELDS = frozenset({"id", "kind", "state", "domains", "concerns_per_domain"})
REQUIRED_DOMAIN_FIELDS = frozenset(
    {
        "id",
        "slug",
        "family",
        "owner",
        "state",
        "support",
        "invariant",
        "runtime_anchor",
        "evidence_requirement",
    }
)

DEFAULT_SPEC_PATH = Path("config/liquid-spec.yaml")
DEFAULT_INVENTORY_PATH = Path("config/capability-inventory.json")

#: ``src/`` -- the import root that the dotted anchors are relative to.
SOURCE_ROOT = Path(__file__).resolve().parent.parent


class LiquidSpecError(ValueError):
    """The Liquid spec is incomplete, inconsistent, or claims something that does not exist."""


class AnchorError(LiquidSpecError):
    """A ``runtime_anchor`` does not resolve to real code under ``src/swfactory/``."""


@dataclass(frozen=True)
class Concern:
    """One column of the matrix: a recurring question with one boring production metric."""

    id: str
    name: str
    action: str
    metric: str
    question: str


@dataclass(frozen=True)
class FamilyArea:
    """One named area of a family that carries rows of its own instead of domain entries.

    The legacy snapshot reaches its ten areas by slug rather than by a ``domains:`` row, so their
    ``runtime_anchor`` values would otherwise never be resolved -- and an unresolved anchor is
    exactly what this file exists to make impossible.
    """

    slug: str
    owner: str
    runtime_anchor: str


@dataclass(frozen=True)
class Family:
    """Coverage metadata for a whole backlog family -- never one entry per numbered bundle."""

    id: str
    kind: str
    state: str
    domains: int
    concerns_per_domain: int
    issues: int | None = None
    superseded_python: str | None = None
    # The span this family claims, and how it was sliced before the collapse. These are the
    # invariants `liquid_release` used to assert; without them the ranges are decoration.
    span: tuple[int, int] | None = None
    span_field: str | None = None
    bundles: int | None = None
    tranches: int | None = None
    areas: tuple[FamilyArea, ...] = ()

    @property
    def cells(self) -> int:
        return self.domains * self.concerns_per_domain

    @property
    def span_width(self) -> int | None:
        return None if self.span is None else self.span[1] - self.span[0] + 1


@dataclass(frozen=True)
class Domain:
    """One row of the matrix: who owns it, what must hold, where it lives, what proves it."""

    id: str
    slug: str
    family: str
    owner: str
    state: str
    support: str
    invariant: str
    runtime_anchor: str
    evidence_requirement: str
    bundle: str | None = None
    capability_claim: str | None = None
    follow_up: str | None = None
    note: str | None = None

    @property
    def implemented(self) -> bool:
        """True only for rows whose support is an actual promise, not an aspiration."""
        return self.support in {"supported", "test_only"}


@dataclass(frozen=True)
class LiquidSpec:
    """The whole validated matrix, with the small typed API tests need."""

    schema_version: int
    owners: tuple[str, ...]
    concerns: tuple[Concern, ...]
    families: tuple[Family, ...]
    domains: tuple[Domain, ...]

    @property
    def matrix_cells(self) -> int:
        return len(self.domains) * len(self.concerns)

    @property
    def anchors(self) -> tuple[str, ...]:
        return tuple(sorted({domain.runtime_anchor for domain in self.domains}))

    @property
    def metrics(self) -> tuple[str, ...]:
        return tuple(concern.metric for concern in self.concerns)

    def domain(self, ident: str) -> Domain:
        for row in self.domains:
            if row.id == ident:
                return row
        raise KeyError(ident)

    def by_owner(self) -> dict[str, tuple[Domain, ...]]:
        return {owner: tuple(row for row in self.domains if row.owner == owner) for owner in self.owners}

    def counts(self, field: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self.domains:
            key = str(getattr(row, field))
            counts[key] = counts.get(key, 0) + 1
        return counts

    def duplicate_slugs(self) -> tuple[str, ...]:
        """Slugs claimed by more than one family -- generator noise that needs reconciling."""
        seen: dict[str, int] = {}
        for row in self.domains:
            seen[row.slug] = seen.get(row.slug, 0) + 1
        return tuple(sorted(slug for slug, count in seen.items() if count > 1))

    def summary(self) -> dict[str, Any]:
        return {
            "anchors": len(self.anchors),
            "concerns": len(self.concerns),
            "domains": len(self.domains),
            "duplicate_slugs": len(self.duplicate_slugs()),
            "families": len(self.families),
            "matrix_cells": self.matrix_cells,
            "schema_version": self.schema_version,
            "spans": {
                family.id: {"range": list(family.span), "issues": family.issues}
                for family in self.families
                if family.span is not None
            },
            "states": self.counts("state"),
            "support": self.counts("support"),
        }


# --------------------------------------------------------------------------------------------
# anchor resolution -- filesystem + AST, deliberately without importing anything
# --------------------------------------------------------------------------------------------


def _module_path(dotted: str, root: Path) -> Path | None:
    parts = dotted.split(".")
    if any(not part.isidentifier() for part in parts):
        return None
    base = root.joinpath(*parts)
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _top_level_names(path: Path) -> frozenset[str]:
    """Names bound at module top level, read from the AST so nothing is executed."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.ImportFrom | ast.Import):
            names.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
    return frozenset(names)


def resolve_anchor(anchor: str, *, root: Path | None = None) -> Path:
    """Resolve ``module`` or ``module:attribute`` to the file that defines it.

    Raises :class:`AnchorError` when it does not exist.  This is the mechanism that stops the
    spec drifting back into fiction: an anchor is a claim about code, so it is checked.
    """
    root = SOURCE_ROOT if root is None else root
    text = anchor.strip()
    if not text:
        raise AnchorError("runtime_anchor must be nonempty")
    if text.count(":") > 1:
        raise AnchorError(f"{anchor!r}: use at most one ':' to name an attribute")
    dotted, _, attribute = text.partition(":")
    if not dotted.startswith("swfactory.") and dotted != "swfactory":
        raise AnchorError(f"{anchor!r}: runtime anchors must be dotted paths under 'swfactory'")
    path = _module_path(dotted, root)
    if path is None:
        raise AnchorError(f"{anchor!r}: no module {dotted!r} under {root}")
    if attribute:
        if not attribute.isidentifier():
            raise AnchorError(f"{anchor!r}: {attribute!r} is not an identifier")
        if attribute not in _top_level_names(path):
            raise AnchorError(f"{anchor!r}: {dotted} defines no top-level {attribute!r}")
    return path


# --------------------------------------------------------------------------------------------
# validation
# --------------------------------------------------------------------------------------------


def _require(row: object, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise LiquidSpecError(f"every {label} must be a mapping, got {type(row).__name__}")
    missing = fields - set(row)
    if missing:
        raise LiquidSpecError(f"{label} missing fields: {sorted(missing)}")
    return row


def _text(row: dict[str, Any], field: str, label: str) -> str:
    value = str(row[field]).strip()
    if not value:
        raise LiquidSpecError(f"{label}: {field} must be nonempty")
    return value


def _concerns(document: dict[str, Any]) -> tuple[Concern, ...]:
    raw = document.get("concerns")
    if not isinstance(raw, list) or not raw:
        raise LiquidSpecError("concerns must be a nonempty list declared once for all domains")
    concerns: list[Concern] = []
    seen_ids: set[str] = set()
    seen_metrics: set[str] = set()
    for row in raw:
        row = _require(row, REQUIRED_CONCERN_FIELDS, "concern")
        ident = _text(row, "id", "concern")
        if ident in seen_ids:
            raise LiquidSpecError(f"duplicate concern id {ident!r}")
        seen_ids.add(ident)
        metric = _text(row, "metric", f"concern {ident}")
        if metric in seen_metrics:
            raise LiquidSpecError(f"concern {ident}: metric {metric!r} is already used")
        seen_metrics.add(metric)
        concerns.append(
            Concern(
                id=ident,
                name=_text(row, "name", f"concern {ident}"),
                action=_text(row, "action", f"concern {ident}"),
                metric=metric,
                question=_text(row, "question", f"concern {ident}"),
            )
        )
    return tuple(concerns)


def _families(document: dict[str, Any], *, owners: tuple[str, ...], root: Path) -> tuple[Family, ...]:
    raw = document.get("families")
    if not isinstance(raw, list) or not raw:
        raise LiquidSpecError("families must be a nonempty list of coverage metadata")
    families: list[Family] = []
    seen: set[str] = set()
    for row in raw:
        row = _require(row, REQUIRED_FAMILY_FIELDS, "family")
        ident = _text(row, "id", "family")
        if ident in seen:
            raise LiquidSpecError(f"duplicate family id {ident!r}")
        seen.add(ident)
        state = _text(row, "state", f"family {ident}")
        if state not in VALID_FAMILY_STATES:
            raise LiquidSpecError(f"family {ident}: invalid state {state!r}")
        for field in ("domains", "concerns_per_domain"):
            if not isinstance(row[field], int) or row[field] < 1:
                raise LiquidSpecError(f"family {ident}: {field} must be a positive integer")
        issues = row.get("issues")
        if issues is not None and (not isinstance(issues, int) or issues < 1):
            raise LiquidSpecError(f"family {ident}: issues must be a positive integer when present")

        # The span. `liquid_release` asserted contiguity, disjointness and the totals; the ranges
        # survived the collapse as YAML but nothing read them, so they were silently decorative.
        span: tuple[int, int] | None = None
        span_field: str | None = None
        for field in ("issue_range", "rank_range"):
            if field not in row:
                continue
            if span_field is not None:
                raise LiquidSpecError(f"family {ident}: declare at most one of issue_range/rank_range")
            bounds = row[field]
            if (
                not isinstance(bounds, list)
                or len(bounds) != 2
                or not all(isinstance(bound, int) for bound in bounds)
                or bounds[0] < 1
                or bounds[1] < bounds[0]
            ):
                raise LiquidSpecError(f"family {ident}: {field} must be [low, high] with 1 <= low <= high")
            span, span_field = (int(bounds[0]), int(bounds[1])), field
        if span is not None and issues is not None and span[1] - span[0] + 1 != issues:
            raise LiquidSpecError(
                f"family {ident}: {span_field} {list(span)} spans {span[1] - span[0] + 1} but issues says {issues}"
            )

        for field in ("bundles", "tranches"):
            if field in row and (not isinstance(row[field], int) or row[field] < 1):
                raise LiquidSpecError(f"family {ident}: {field} must be a positive integer")

        areas = _areas(row, ident=ident, owners=owners, root=root)
        if areas and len(areas) != int(row["domains"]):
            raise LiquidSpecError(f"family {ident}: declares {row['domains']} domains but carries {len(areas)} areas")

        families.append(
            Family(
                id=ident,
                kind=_text(row, "kind", f"family {ident}"),
                state=state,
                domains=int(row["domains"]),
                concerns_per_domain=int(row["concerns_per_domain"]),
                issues=issues,
                superseded_python=(str(row["superseded_python"]) if row.get("superseded_python") else None),
                span=span,
                span_field=span_field,
                bundles=(int(row["bundles"]) if "bundles" in row else None),
                tranches=(int(row["tranches"]) if "tranches" in row else None),
                areas=areas,
            )
        )
    _assert_spans_tile(families)
    return tuple(families)


def _areas(row: dict[str, Any], *, ident: str, owners: tuple[str, ...], root: Path) -> tuple[FamilyArea, ...]:
    """Parse and RESOLVE a family's areas, so their anchors are held to the same bar as a domain."""
    raw = row.get("areas")
    if raw is None:
        return ()
    if not isinstance(raw, list) or not raw:
        raise LiquidSpecError(f"family {ident}: areas must be a nonempty list when present")
    areas: list[FamilyArea] = []
    seen: set[str] = set()
    for entry in raw:
        entry = _require(entry, frozenset({"slug", "owner", "runtime_anchor"}), f"family {ident} area")
        slug = _text(entry, "slug", f"family {ident} area")
        if slug in seen:
            raise LiquidSpecError(f"family {ident}: duplicate area slug {slug!r}")
        seen.add(slug)
        owner = _text(entry, "owner", f"family {ident} area {slug}")
        if owner not in owners:
            raise LiquidSpecError(f"family {ident} area {slug}: unknown owner {owner!r}")
        anchor = _text(entry, "runtime_anchor", f"family {ident} area {slug}")
        resolve_anchor(anchor, root=root)
        areas.append(FamilyArea(slug=slug, owner=owner, runtime_anchor=anchor))
    return tuple(areas)


def _assert_spans_tile(families: list[Family]) -> None:
    """Spans of one kind must tile: sorted, contiguous, no gap and no overlap.

    This is the invariant the old manifest carried and the collapse nearly dropped. Without it a
    family can silently move its range, and the declared coverage stops meaning anything.
    """
    by_field: dict[tuple[str, str], list[Family]] = {}
    for family in families:
        if family.span is not None and family.span_field is not None:
            by_field.setdefault((family.kind, family.span_field), []).append(family)
    for (kind, field), group in sorted(by_field.items()):
        ordered = sorted(group, key=lambda f: f.span or (0, 0))
        for earlier, later in zip(ordered, ordered[1:], strict=False):
            assert earlier.span is not None and later.span is not None  # narrowed by the filter
            if later.span[0] <= earlier.span[1]:
                raise LiquidSpecError(
                    f"{kind} {field}: {earlier.id} {list(earlier.span)} overlaps {later.id} {list(later.span)}"
                )
            if later.span[0] != earlier.span[1] + 1:
                raise LiquidSpecError(
                    f"{kind} {field}: gap between {earlier.id} {list(earlier.span)} and {later.id} {list(later.span)}"
                )


def _domains(
    document: dict[str, Any],
    *,
    families: tuple[Family, ...],
    owners: tuple[str, ...],
    root: Path,
    claims: dict[str, dict[str, str]] | None,
) -> tuple[Domain, ...]:
    raw = document.get("domains")
    if not isinstance(raw, list) or not raw:
        raise LiquidSpecError("domains must be a nonempty list")
    family_ids = {family.id for family in families}
    domains: list[Domain] = []
    seen: set[str] = set()
    for row in raw:
        row = _require(row, REQUIRED_DOMAIN_FIELDS, "domain")
        ident = _text(row, "id", "domain")
        if len(ident) > 160:
            raise LiquidSpecError(f"domain id {ident!r} must be bounded")
        if ident in seen:
            raise LiquidSpecError(f"duplicate domain id {ident!r}")
        seen.add(ident)
        slug = _text(row, "slug", f"domain {ident}")
        family = _text(row, "family", f"domain {ident}")
        if family not in family_ids:
            raise LiquidSpecError(f"domain {ident}: unknown family {family!r}")
        if ident != f"{family.lower()}.{slug}":
            raise LiquidSpecError(f"domain {ident}: id must be '<family>.<slug>' for {family}/{slug}")
        owner = _text(row, "owner", f"domain {ident}")
        if owner not in owners:
            raise LiquidSpecError(f"domain {ident}: owner {owner!r} is not one of the canonical roles {list(owners)}")
        state = _text(row, "state", f"domain {ident}")
        support = _text(row, "support", f"domain {ident}")
        if state not in VALID_STATES:
            raise LiquidSpecError(f"domain {ident}: invalid state {state!r}")
        if support not in VALID_SUPPORT:
            raise LiquidSpecError(f"domain {ident}: invalid support {support!r}")
        if support == "supported" and state != "validated":
            raise LiquidSpecError(f"domain {ident}: supported rows must be validated")
        if state in {"experimental", "unsupported"} and support == "supported":
            raise LiquidSpecError(f"domain {ident}: experimental/unsupported row cannot be supported")
        invariant = _text(row, "invariant", f"domain {ident}")
        evidence = _text(row, "evidence_requirement", f"domain {ident}")
        anchor = _text(row, "runtime_anchor", f"domain {ident}")
        resolve_anchor(anchor, root=root)
        follow_up = str(row["follow_up"]).strip() if row.get("follow_up") else None
        if state == "experimental" and not follow_up:
            raise LiquidSpecError(f"domain {ident}: experimental rows require an explicit follow_up")
        claim = str(row["capability_claim"]).strip() if row.get("capability_claim") else None
        if state == "validated" and not claim:
            raise LiquidSpecError(
                f"domain {ident}: a validated row must cite a capability_claim from {DEFAULT_INVENTORY_PATH}"
            )
        if claim is not None and claims is not None:
            if claim not in claims:
                raise LiquidSpecError(f"domain {ident}: unknown capability_claim {claim!r}")
            if support == "supported" and claims[claim]["support"] != "supported":
                raise LiquidSpecError(
                    f"domain {ident}: capability_claim {claim!r} is "
                    f"{claims[claim]['support']!r}, so this row cannot be supported"
                )
        domains.append(
            Domain(
                id=ident,
                slug=slug,
                family=family,
                owner=owner,
                state=state,
                support=support,
                invariant=invariant,
                runtime_anchor=anchor,
                evidence_requirement=evidence,
                bundle=str(row["bundle"]).strip() if row.get("bundle") else None,
                capability_claim=claim,
                follow_up=follow_up,
                note=str(row["note"]).strip() if row.get("note") else None,
            )
        )
    return tuple(domains)


def validate_spec(
    document: Any,
    *,
    source_root: Path | None = None,
    claims: dict[str, dict[str, str]] | None = None,
) -> LiquidSpec:
    """Validate a parsed spec document and return the typed matrix.

    ``claims`` is the capability inventory's ``id -> {state, support}`` mapping.  Pass ``None``
    to skip the cross-reference (used by unit tests that do not carry an inventory).
    """
    if not isinstance(document, dict):
        raise LiquidSpecError("liquid spec root must be a mapping")
    if document.get("schema_version") != SCHEMA_VERSION:
        raise LiquidSpecError(f"unsupported liquid spec schema {document.get('schema_version')!r}")
    owners_raw = document.get("owners")
    if not isinstance(owners_raw, list) or [str(o) for o in owners_raw] != list(CANONICAL_OWNERS):
        raise LiquidSpecError(f"owners must be exactly {list(CANONICAL_OWNERS)} in that order")
    owners = tuple(str(owner) for owner in owners_raw)
    doctrine = document.get("doctrine")
    if not isinstance(doctrine, dict) or list(doctrine.get("phases") or []) != [
        "exploration",
        "stabilization",
        "convergence",
    ]:
        raise LiquidSpecError(
            "doctrine.phases must be [exploration, stabilization, convergence] -- the only "
            "narrative the product path carries"
        )
    concerns = _concerns(document)
    families = _families(document, owners=owners, root=SOURCE_ROOT if source_root is None else source_root)
    domains = _domains(
        document,
        families=families,
        owners=owners,
        root=SOURCE_ROOT if source_root is None else source_root,
        claims=claims,
    )
    for family in families:
        declared = sum(1 for row in domains if row.family == family.id)
        # `if declared and ...` let a family with zero rows claim any number at all, which covered
        # four of the six families. A liquid family must carry its rows; a family that reaches its
        # domains by area is checked against len(areas) in _families instead.
        if family.kind == "liquid" and not declared:
            raise LiquidSpecError(f"family {family.id}: kind 'liquid' must carry its domain rows")
        if declared and declared != family.domains:
            raise LiquidSpecError(f"family {family.id}: declares {family.domains} domains but {declared} rows exist")
        if not declared and not family.areas and family.kind != "research":
            raise LiquidSpecError(
                f"family {family.id}: carries neither domain rows nor areas, so its domain count is unfalsifiable"
            )
        if family.concerns_per_domain != len(concerns):
            raise LiquidSpecError(
                f"family {family.id}: concerns_per_domain {family.concerns_per_domain} does not "
                f"match the {len(concerns)} declared concerns"
            )
    return LiquidSpec(
        schema_version=SCHEMA_VERSION,
        owners=owners,
        concerns=concerns,
        families=families,
        domains=domains,
    )


def inventory_claims(path: Path = DEFAULT_INVENTORY_PATH) -> dict[str, dict[str, str]]:
    """Read the capability inventory as ``id -> {state, support}`` for cross-referencing."""
    document = load_inventory(path)
    return {str(row["id"]): {"state": str(row["state"]), "support": str(row["support"])} for row in document["claims"]}


def load_spec(
    path: Path = DEFAULT_SPEC_PATH,
    *,
    source_root: Path | None = None,
    inventory_path: Path | None = DEFAULT_INVENTORY_PATH,
) -> LiquidSpec:
    """Parse and fully validate a spec file, including anchors and capability cross-references."""
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    claims = inventory_claims(inventory_path) if inventory_path is not None else None
    return validate_spec(document, source_root=source_root, claims=claims)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the declarative Liquid matrix")
    parser.add_argument("path", nargs="?", default=str(DEFAULT_SPEC_PATH))
    parser.add_argument("--source-root", default=None, help="import root for runtime anchors")
    parser.add_argument(
        "--inventory",
        default=str(DEFAULT_INVENTORY_PATH),
        help="capability inventory to cross-reference capability_claim against",
    )
    args = parser.parse_args(argv)
    try:
        spec = load_spec(
            Path(args.path),
            source_root=Path(args.source_root) if args.source_root else None,
            inventory_path=Path(args.inventory),
        )
    except (LiquidSpecError, OSError, yaml.YAMLError) as exc:
        print(f"liquid-spec: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(spec.summary(), sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI seam
    raise SystemExit(main())
