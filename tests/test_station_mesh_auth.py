from __future__ import annotations

import http.client
import json
import threading
from types import SimpleNamespace
from typing import Any

import pytest

from swfactory.backend.server import make_server

BACKEND_TOKEN = "b" * 40
MESH_TOKEN = "m" * 40


def call(server, path: str, token: str, body: dict[str, Any]) -> tuple[int, Any]:
    host, port = server.server_address[0], server.server_address[1]
    raw = json.dumps(body).encode()
    connection = http.client.HTTPConnection(host, port, timeout=10)
    try:
        connection.request(
            "POST",
            path,
            body=raw,
            headers={
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
                "Content-Length": str(len(raw)),
            },
        )
        response = connection.getresponse()
        payload = response.read()
        return response.status, json.loads(payload) if payload else None
    finally:
        connection.close()


def test_mesh_token_is_scoped_to_mesh_routes(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SWF_MESH_TOKEN", MESH_TOKEN)
    factory = SimpleNamespace(token=BACKEND_TOKEN, state_root=tmp_path)
    server = make_server(factory, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, lease = call(
            server,
            "/v1/mesh/join",
            MESH_TOKEN,
            {
                "station_id": "station_peer",
                "repo": "acme/widgets",
                "incarnation_id": "inc_peer",
                "operator": "peer",
            },
        )
        assert status == 200
        assert lease["station_id"] == "station_peer"

        status, peers = call(server, "/v1/mesh/peers", MESH_TOKEN, {"repo": "acme/widgets"})
        assert status == 200
        assert [peer["station_id"] for peer in peers] == ["station_peer"]

        # The same credential cannot cross into the privileged backend operation surface. If auth
        # were accidentally broad, this fake factory has no operation() and the request would hit a
        # 500 instead of being refused here.
        status, payload = call(server, "/v1/cells", MESH_TOKEN, {})
        assert status == 401
        assert payload == {"detail": "factory backend token required"}

        # A backend administrator remains allowed to inspect/use the mesh without a second token.
        status, peers = call(server, "/v1/mesh/peers", BACKEND_TOKEN, {"repo": "acme/widgets"})
        assert status == 200
        assert len(peers) == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_mesh_token_must_be_strong_and_distinct(tmp_path, monkeypatch) -> None:
    factory = SimpleNamespace(token=BACKEND_TOKEN, state_root=tmp_path)
    monkeypatch.setenv("SWF_MESH_TOKEN", "short")
    with pytest.raises(ValueError, match="at least 32"):
        make_server(factory, "127.0.0.1", 0)

    monkeypatch.setenv("SWF_MESH_TOKEN", BACKEND_TOKEN)
    with pytest.raises(ValueError, match="must differ"):
        make_server(factory, "127.0.0.1", 0)
