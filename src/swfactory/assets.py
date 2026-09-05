"""Locate immutable files shipped with swfactory.

Source checkouts keep blueprints and policy files at the repository root. Wheels carry the same
tree under ``swfactory/_assets``. Runtime code goes through this module so an installed CLI does
not accidentally depend on the directory it was launched from.
"""

from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
CHECKOUT_ROOT = PACKAGE_ROOT.parents[1]
PACKAGED_ROOT = PACKAGE_ROOT / "_assets"


def _asset_root() -> Path:
    if (CHECKOUT_ROOT / "blueprints").is_dir() and (CHECKOUT_ROOT / "REVIEW.md").is_file():
        return CHECKOUT_ROOT
    return PACKAGED_ROOT


ASSET_ROOT = _asset_root()


def asset_path(*parts: str) -> Path:
    """Return a path below the selected source-or-wheel asset tree."""

    return ASSET_ROOT.joinpath(*parts)
