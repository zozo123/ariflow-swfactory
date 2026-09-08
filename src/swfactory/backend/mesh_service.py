"""HTTP-facing operations for the repo-scoped Factory Mesh rendezvous.

This module intentionally sits beside, not inside, lifecycle submission.  A station may use a
shared backend as a rendezvous even when its own Airflow and Factory Cell stores are completely
independent.  Nothing here dispatches a DAG, advances a Cell epoch, publishes to GitHub, or touches
a sandbox.
"""

from __future__ import annotations

import atexit
import threading
from pathlib import Path
from typing import Any

from swfactory.station_mesh import StationMesh

_MESHES: dict[Path, StationMesh] = {}
_LOCK = threading.Lock()


def _mesh(factory: Any) -> StationMesh:
    root = Path(factory.state_root).resolve()
    with _LOCK:
        mesh = _MESHES.get(root)
        if mesh is None:
            mesh = StationMesh(root / "station-mesh.sqlite3")
            _MESHES[root] = mesh
        return mesh


def _close_all() -> None:
    with _LOCK:
        meshes = list(_MESHES.values())
        _MESHES.clear()
    for mesh in meshes:
        mesh.close()


atexit.register(_close_all)


def _positive(body: dict[str, Any], key: str) -> int:
    value = body.get(key)
    if type(value) is not int or value < 1:
        raise ValueError(f"{key} must be a positive integer")
    return value


def _nonnegative(body: dict[str, Any], key: str, default: int = 0) -> int:
    value = body.get(key, default)
    if type(value) is not int or value < 0:
        raise ValueError(f"{key} must be a nonnegative integer")
    return value


def _limit(body: dict[str, Any]) -> int:
    value = body.get("limit", 100)
    if type(value) is not int or not 1 <= value <= 1000:
        raise ValueError("limit must be an integer between 1 and 1000")
    return value


def operation(factory: Any, path: str, body: dict[str, Any]) -> Any:
    """Execute one authenticated mesh operation against this backend's rendezvous store."""
    mesh = _mesh(factory)
    if path == "/mesh/join":
        return mesh.join(
            station_id=body.get("station_id"),
            repo=body.get("repo"),
            incarnation_id=body.get("incarnation_id"),
            operator=body.get("operator"),
            generation=body.get("generation", "stable"),
            capabilities=body.get("capabilities"),
            metadata=body.get("metadata"),
            ttl_s=body.get("ttl_s"),
        )
    if path == "/mesh/heartbeat":
        return mesh.heartbeat(
            body.get("station_id"),
            _positive(body, "lease_epoch"),
            body.get("incarnation_id"),
            ttl_s=body.get("ttl_s"),
        )
    if path == "/mesh/leave":
        return mesh.leave(
            body.get("station_id"),
            _positive(body, "lease_epoch"),
            body.get("incarnation_id"),
        )
    if path == "/mesh/peers":
        return mesh.peers(
            body.get("repo"),
            include_expired=body.get("include_expired") is True,
            limit=_limit(body),
        )
    if path == "/mesh/say":
        return mesh.say(
            station_id=body.get("station_id"),
            station_lease_epoch=_positive(body, "station_lease_epoch"),
            repo=body.get("repo"),
            kind=body.get("kind"),
            topic=body.get("topic"),
            summary=body.get("summary"),
            cell_id=body.get("cell_id"),
            cell_epoch=body.get("cell_epoch"),
            to_station=body.get("to_station"),
            reply_to=body.get("reply_to"),
            artifacts=body.get("artifacts"),
            payload=body.get("payload"),
            ttl_s=body.get("ttl_s"),
            message_id=body.get("message_id"),
        )
    if path == "/mesh/inbox":
        return mesh.signals(
            body.get("repo"),
            after_seq=_nonnegative(body, "after_seq"),
            topic=body.get("topic"),
            cell_id=body.get("cell_id"),
            to_station=body.get("to_station"),
            include_expired=body.get("include_expired") is True,
            limit=_limit(body),
        )
    if path == "/mesh/claim":
        return mesh.claim(
            repo=body.get("repo"),
            cell_id=body.get("cell_id"),
            cell_epoch=_positive(body, "cell_epoch"),
            station_id=body.get("station_id"),
            station_lease_epoch=_positive(body, "station_lease_epoch"),
            purpose=body.get("purpose", "work"),
            ttl_s=body.get("ttl_s"),
        )
    if path == "/mesh/release":
        mesh.release(
            repo=body.get("repo"),
            cell_id=body.get("cell_id"),
            station_id=body.get("station_id"),
            station_lease_epoch=_positive(body, "station_lease_epoch"),
            claim_epoch=_positive(body, "claim_epoch"),
        )
        return {"released": True, "cell_id": body.get("cell_id")}
    if path == "/mesh/claims":
        return mesh.claims(
            body.get("repo"),
            include_expired=body.get("include_expired") is True,
            limit=_limit(body),
        )
    if path == "/mesh/conflicts":
        return mesh.conflicts(body.get("repo"))
    raise ValueError(f"unknown mesh operation: {path}")
