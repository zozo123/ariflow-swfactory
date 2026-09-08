"""Small operator client for the shared Factory Mesh rendezvous.

Run with ``uv run python -m swfactory.station_client``.  The client keeps only non-secret lease
identity in ``.factory/station.json``; bearer tokens stay in environment variables.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import socket
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from swfactory.station_mesh import SignalKind, new_incarnation_id
from swfactory.station_mesh import station_id as stable_station_id

DEFAULT_STATE = Path(".factory/station.json")


class ClientError(RuntimeError):
    pass


def _url(value: str | None) -> str:
    return (value or os.getenv("SWF_MESH_URL") or os.getenv("SWF_BACKEND_URL") or "http://localhost:8082").rstrip("/")


def _token(value: str | None) -> str:
    token = value or os.getenv("SWF_MESH_TOKEN") or os.getenv("SWF_BACKEND_TOKEN") or ""
    if not token:
        raise ClientError("set SWF_MESH_TOKEN or SWF_BACKEND_TOKEN")
    return token


def _call(base: str, token: str, path: str, body: dict[str, Any]) -> Any:
    request = urllib.request.Request(
        base + "/v1" + path,
        data=json.dumps(body, allow_nan=False).encode(),
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")[:1000]
        raise ClientError(f"mesh HTTP {error.code}: {detail}") from error
    except urllib.error.URLError as error:
        raise ClientError(f"cannot reach Factory Mesh at {base}: {error.reason}") from error
    try:
        return json.loads(raw) if raw else None
    except ValueError as error:
        raise ClientError("Factory Mesh returned invalid JSON") from error


def _load(path: Path) -> dict[str, Any]:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ClientError(f"no station lease at {path}; run 'join' first") from error
    if not isinstance(state, dict) or state.get("schema_version") != 1:
        raise ClientError(f"invalid station state at {path}")
    return state


def _save(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _json_object(text: str | None, field: str) -> dict[str, Any]:
    if not text:
        return {}
    try:
        value = json.loads(text)
    except ValueError as error:
        raise ClientError(f"{field} must be JSON") from error
    if not isinstance(value, dict):
        raise ClientError(f"{field} must be a JSON object")
    return value


def _identity(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "repo": state["repo"],
        "station_id": state["station_id"],
        "station_lease_epoch": state["lease_epoch"],
    }


def _print(value: Any) -> None:
    print(json.dumps(value, sort_keys=True, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Talk to other Airflow software-factory stations")
    parser.add_argument("--url", help="mesh backend; defaults SWF_MESH_URL/SWF_BACKEND_URL")
    parser.add_argument("--token", help="mesh bearer token; defaults SWF_MESH_TOKEN/SWF_BACKEND_TOKEN")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE, help="local non-secret station lease file")
    commands = parser.add_subparsers(dest="command", required=True)

    join = commands.add_parser("join", help="announce this station and acquire a fenced lease")
    join.add_argument("--repo", required=True)
    join.add_argument("--operator", default=os.getenv("USER") or getpass.getuser())
    join.add_argument("--station-id")
    join.add_argument("--generation", default=os.getenv("SWF_GENERATION") or "stable")
    join.add_argument("--capability", action="append", default=[])
    join.add_argument("--metadata", default="{}", help="small JSON object; never put secrets here")
    join.add_argument("--ttl", type=int, default=120)

    heartbeat = commands.add_parser("heartbeat", help="renew this process incarnation")
    heartbeat.add_argument("--ttl", type=int, default=120)

    commands.add_parser("leave", help="expire this station lease")

    peers = commands.add_parser("peers", help="list live stations on the same repository")
    peers.add_argument("--all", action="store_true", help="include expired station cards")
    peers.add_argument("--limit", type=int, default=100)

    say = commands.add_parser("say", help="publish a typed durable signal")
    say.add_argument("kind", choices=[kind.value for kind in SignalKind])
    say.add_argument("topic", help="repo, issue, cell, branch, artifact, or another stable topic")
    say.add_argument("summary")
    say.add_argument("--cell")
    say.add_argument("--cell-epoch", type=int)
    say.add_argument("--to")
    say.add_argument("--reply-to")
    say.add_argument("--artifact", action="append", default=[])
    say.add_argument("--payload", default="{}", help="JSON object with non-authoritative context")
    say.add_argument("--ttl", type=int, default=3600)
    say.add_argument("--message-id")

    inbox = commands.add_parser("inbox", help="read signals in monotonically increasing sequence order")
    inbox.add_argument("--after", type=int, default=0)
    inbox.add_argument("--topic")
    inbox.add_argument("--cell")
    inbox.add_argument("--all", action="store_true", help="include messages addressed to other stations")
    inbox.add_argument("--expired", action="store_true")
    inbox.add_argument("--limit", type=int, default=100)

    claim = commands.add_parser("claim", help="acquire/renew one repo-level coordination lease")
    claim.add_argument("cell_id")
    claim.add_argument("cell_epoch", type=int)
    claim.add_argument("--purpose", default="work")
    claim.add_argument("--ttl", type=int, default=900)

    release = commands.add_parser("release", help="release a coordination claim you still own")
    release.add_argument("cell_id")
    release.add_argument("claim_epoch", type=int)

    claims = commands.add_parser("claims", help="list current repo-level coordination leases")
    claims.add_argument("--all", action="store_true", help="include expired claims")
    claims.add_argument("--limit", type=int, default=100)

    commands.add_parser("conflicts", help="show orphaned claims and competing live intents")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    base = _url(args.url)
    token = _token(args.token)
    state_path: Path = args.state
    try:
        if args.command == "join":
            sid = args.station_id or stable_station_id(args.repo, args.operator, socket.gethostname())
            incarnation = new_incarnation_id()
            lease = _call(
                base,
                token,
                "/mesh/join",
                {
                    "station_id": sid,
                    "repo": args.repo,
                    "incarnation_id": incarnation,
                    "operator": args.operator,
                    "generation": args.generation,
                    "capabilities": args.capability,
                    "metadata": _json_object(args.metadata, "metadata"),
                    "ttl_s": args.ttl,
                },
            )
            _save(
                state_path,
                {
                    "schema_version": 1,
                    "mesh_url": base,
                    "repo": args.repo,
                    "station_id": sid,
                    "incarnation_id": incarnation,
                    "lease_epoch": lease["lease_epoch"],
                },
            )
            _print(lease)
            return 0

        state = _load(state_path)
        base = _url(args.url or state.get("mesh_url"))
        identity = _identity(state)
        if args.command == "heartbeat":
            value = _call(
                base,
                token,
                "/mesh/heartbeat",
                {
                    "station_id": state["station_id"],
                    "lease_epoch": state["lease_epoch"],
                    "incarnation_id": state["incarnation_id"],
                    "ttl_s": args.ttl,
                },
            )
        elif args.command == "leave":
            value = _call(
                base,
                token,
                "/mesh/leave",
                {
                    "station_id": state["station_id"],
                    "lease_epoch": state["lease_epoch"],
                    "incarnation_id": state["incarnation_id"],
                },
            )
        elif args.command == "peers":
            value = _call(
                base,
                token,
                "/mesh/peers",
                {"repo": state["repo"], "include_expired": args.all, "limit": args.limit},
            )
        elif args.command == "say":
            value = _call(
                base,
                token,
                "/mesh/say",
                {
                    **identity,
                    "kind": args.kind,
                    "topic": args.topic,
                    "summary": args.summary,
                    "cell_id": args.cell,
                    "cell_epoch": args.cell_epoch,
                    "to_station": args.to,
                    "reply_to": args.reply_to,
                    "artifacts": args.artifact,
                    "payload": _json_object(args.payload, "payload"),
                    "ttl_s": args.ttl,
                    "message_id": args.message_id,
                },
            )
        elif args.command == "inbox":
            value = _call(
                base,
                token,
                "/mesh/inbox",
                {
                    "repo": state["repo"],
                    "after_seq": args.after,
                    "topic": args.topic,
                    "cell_id": args.cell,
                    "to_station": None if args.all else state["station_id"],
                    "include_expired": args.expired,
                    "limit": args.limit,
                },
            )
        elif args.command == "claim":
            value = _call(
                base,
                token,
                "/mesh/claim",
                {
                    **identity,
                    "cell_id": args.cell_id,
                    "cell_epoch": args.cell_epoch,
                    "purpose": args.purpose,
                    "ttl_s": args.ttl,
                },
            )
        elif args.command == "release":
            value = _call(
                base,
                token,
                "/mesh/release",
                {**identity, "cell_id": args.cell_id, "claim_epoch": args.claim_epoch},
            )
        elif args.command == "claims":
            value = _call(
                base,
                token,
                "/mesh/claims",
                {"repo": state["repo"], "include_expired": args.all, "limit": args.limit},
            )
        elif args.command == "conflicts":
            value = _call(base, token, "/mesh/conflicts", {"repo": state["repo"]})
        else:  # pragma: no cover - argparse makes this unreachable
            raise ClientError(f"unknown command {args.command}")
        _print(value)
        return 0
    except (ClientError, KeyError, TypeError, ValueError) as error:
        print(f"station: {error}", file=__import__("sys").stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
