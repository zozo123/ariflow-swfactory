"""HTTP-facing operations for the repo-scoped Factory Mesh rendezvous.

This module intentionally sits beside, not inside, lifecycle submission. A station may use a
shared backend as a rendezvous even when its own Airflow and Factory Cell stores are completely
independent. Nothing here dispatches a DAG, advances a Cell epoch, publishes to GitHub, or touches
a sandbox.

The SQLite implementation is a *single rendezvous service* boundary: run one mesh backend process
for a given ``SWF_STATE_ROOT`` and let all remote stations talk to that HTTP endpoint. It is not a
multi-primary database protocol; a future Postgres implementation can preserve this API if the
rendezvous itself needs horizontal scaling.
"""

from __future__ import annotations

import atexit
import json
import threading
from pathlib import Path
from typing import Any

from swfactory.station_mesh import MeshError, StationMesh

_MESHES: dict[Path, StationMesh] = {}
_LOCK = threading.Lock()
MAX_METADATA_BYTES = 16 * 1024
MAX_SIGNAL_PAYLOAD_BYTES = 64 * 1024


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


def _repo(factory: Any, body: dict[str, Any]) -> Any:
    repo = body.get("repo")
    configured = getattr(factory, "repo", "") or ""
    if configured and repo != configured:
        raise MeshError(f"mesh rendezvous is scoped to repository {configured}")
    return repo


def _bounded_object(body: dict[str, Any], key: str, max_bytes: int) -> Any:
    value = body.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be a JSON object")
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > max_bytes:
        raise ValueError(f"{key} exceeds {max_bytes} encoded bytes")
    return value


def operation(factory: Any, path: str, body: dict[str, Any]) -> Any:
    """Execute one authenticated mesh operation against this backend's rendezvous store."""
    mesh = _mesh(factory)
    if path == "/mesh/join":
        return mesh.join(
            station_id=body.get("station_id"),
            repo=_repo(factory, body),
            incarnation_id=body.get("incarnation_id"),
            operator=body.get("operator"),
            generation=body.get("generation", "stable"),
            capabilities=body.get("capabilities"),
            metadata=_bounded_object(body, "metadata", MAX_METADATA_BYTES),
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
            _repo(factory, body),
            include_expired=body.get("include_expired") is True,
            limit=_limit(body),
        )
    if path == "/mesh/say":
        return mesh.say(
            station_id=body.get("station_id"),
            station_lease_epoch=_positive(body, "station_lease_epoch"),
            repo=_repo(factory, body),
            kind=body.get("kind"),
            topic=body.get("topic"),
            summary=body.get("summary"),
            cell_id=body.get("cell_id"),
            cell_epoch=body.get("cell_epoch"),
            to_station=body.get("to_station"),
            reply_to=body.get("reply_to"),
            artifacts=body.get("artifacts"),
            payload=_bounded_object(body, "payload", MAX_SIGNAL_PAYLOAD_BYTES),
            ttl_s=body.get("ttl_s"),
            message_id=body.get("message_id"),
        )
    if path == "/mesh/inbox":
        return mesh.signals(
            _repo(factory, body),
            after_seq=_nonnegative(body, "after_seq"),
            topic=body.get("topic"),
            cell_id=body.get("cell_id"),
            to_station=body.get("to_station"),
            include_expired=body.get("include_expired") is True,
            limit=_limit(body),
        )
    if path == "/mesh/claim":
        return mesh.claim(
            repo=_repo(factory, body),
            cell_id=body.get("cell_id"),
            cell_epoch=_positive(body, "cell_epoch"),
            station_id=body.get("station_id"),
            station_lease_epoch=_positive(body, "station_lease_epoch"),
            purpose=body.get("purpose", "work"),
            ttl_s=body.get("ttl_s"),
        )
    if path == "/mesh/release":
        try:
            mesh.release(
                repo=_repo(factory, body),
                cell_id=body.get("cell_id"),
                station_id=body.get("station_id"),
                station_lease_epoch=_positive(body, "station_lease_epoch"),
                claim_epoch=_positive(body, "claim_epoch"),
            )
        except KeyError as error:
            # A late duplicate release is a coordination conflict, never an internal backend
            # failure. The caller must re-read claims instead of guessing who owns the Cell now.
            raise MeshError("coordination claim no longer exists; re-read mesh claims") from error
        return {"released": True, "cell_id": body.get("cell_id")}
    if path == "/mesh/claims":
        return mesh.claims(
            _repo(factory, body),
            include_expired=body.get("include_expired") is True,
            limit=_limit(body),
        )
    if path == "/mesh/conflicts":
        return mesh.conflicts(_repo(factory, body))
    raise ValueError(f"unknown mesh operation: {path}")
