"""Versioned capability inventory for support and evidence claims.

The inventory is declarative.  It does not make a capability true by naming it.  A claim moves from
``declared`` to ``integrated`` to ``validated`` only when the listed runtime path and verification
surface exist.  Candidate-specific evidence is recorded separately by :mod:`candidate_readiness`.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the software-factory capability inventory")
    parser.add_argument("path", nargs="?", default="config/capability-inventory.json")
    args = parser.parse_args(argv)
    document = load_inventory(Path(args.path))
    counts: dict[str, int] = {}
    for row in document["claims"]:
        key = str(row["state"])
        counts[key] = counts.get(key, 0) + 1
    print(json.dumps({"schema_version": SCHEMA_VERSION, "claims": len(document["claims"]), "states": counts}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI seam
    raise SystemExit(main())
