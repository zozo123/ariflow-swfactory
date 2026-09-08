from __future__ import annotations

from types import SimpleNamespace

import pytest

import swfactory.station_mesh as mesh_mod
from swfactory.backend.mesh_service import operation
from swfactory.station_mesh import ClaimConflict, MeshError, SignalKind, StaleStationLease, StationMesh


def join(mesh: StationMesh, station: str, incarnation: str, repo: str = "acme/widgets"):
    return mesh.join(
        station_id=station,
        repo=repo,
        incarnation_id=incarnation,
        operator=station,
        capabilities=["python", "islo"],
        ttl_s=120,
    )


def test_new_incarnation_fences_old_process(tmp_path) -> None:
    mesh = StationMesh(tmp_path / "mesh.sqlite3")
    try:
        first = join(mesh, "station_a", "inc_a1")
        second = join(mesh, "station_a", "inc_a2")
        assert first.lease_epoch == 1
        assert second.lease_epoch == 2
        with pytest.raises(StaleStationLease):
            mesh.heartbeat("station_a", first.lease_epoch, "inc_a1")
        assert mesh.heartbeat("station_a", second.lease_epoch, "inc_a2").lease_epoch == 2
    finally:
        mesh.close()


def test_claim_is_single_owner_and_expiry_takeover_is_fenced(tmp_path, monkeypatch) -> None:
    now = [1000.0]
    monkeypatch.setattr(mesh_mod.time, "time", lambda: now[0])
    mesh = StationMesh(tmp_path / "mesh.sqlite3")
    try:
        a = join(mesh, "station_a", "inc_a")
        b = join(mesh, "station_b", "inc_b")
        first = mesh.claim(
            repo=a.repo,
            cell_id="cell_deadbeef",
            cell_epoch=3,
            station_id=a.station_id,
            station_lease_epoch=a.lease_epoch,
            ttl_s=10,
        )
        renewed = mesh.claim(
            repo=a.repo,
            cell_id="cell_deadbeef",
            cell_epoch=3,
            station_id=a.station_id,
            station_lease_epoch=a.lease_epoch,
            ttl_s=10,
        )
        assert renewed.claim_epoch == first.claim_epoch == 1
        with pytest.raises(ClaimConflict):
            mesh.claim(
                repo=b.repo,
                cell_id="cell_deadbeef",
                cell_epoch=3,
                station_id=b.station_id,
                station_lease_epoch=b.lease_epoch,
            )

        now[0] += 11
        takeover = mesh.claim(
            repo=b.repo,
            cell_id="cell_deadbeef",
            cell_epoch=3,
            station_id=b.station_id,
            station_lease_epoch=b.lease_epoch,
        )
        assert takeover.claim_epoch == 2
        assert takeover.station_id == "station_b"
        with pytest.raises(ClaimConflict):
            mesh.release(
                repo=a.repo,
                cell_id="cell_deadbeef",
                station_id=a.station_id,
                station_lease_epoch=a.lease_epoch,
                claim_epoch=1,
            )
    finally:
        mesh.close()


def test_signals_are_idempotent_knocks_and_competing_intents_are_visible(tmp_path) -> None:
    mesh = StationMesh(tmp_path / "mesh.sqlite3")
    try:
        a = join(mesh, "station_a", "inc_a")
        b = join(mesh, "station_b", "inc_b")
        kwargs = {
            "station_id": a.station_id,
            "station_lease_epoch": a.lease_epoch,
            "repo": a.repo,
            "kind": SignalKind.INTENT.value,
            "topic": "issue:42",
            "summary": "I am preparing a plan; re-read the Cell before acting.",
            "cell_id": "cell_cafebabe",
            "cell_epoch": 2,
            "artifacts": ["docs/factory/42/intent.md"],
            "message_id": "msg_same",
        }
        first = mesh.say(**kwargs)
        replay = mesh.say(**kwargs)
        assert replay.seq == first.seq
        with pytest.raises(MeshError):
            mesh.say(**{**kwargs, "summary": "different content"})

        mesh.say(
            station_id=b.station_id,
            station_lease_epoch=b.lease_epoch,
            repo=b.repo,
            kind=SignalKind.INTENT.value,
            topic="issue:42",
            summary="I independently saw the same work.",
            cell_id="cell_cafebabe",
            cell_epoch=2,
        )
        conflicts = mesh.conflicts(a.repo)
        assert {
            "kind": "competing_intents",
            "cell_id": "cell_cafebabe",
            "cell_epoch": 2,
            "stations": ["station_a", "station_b"],
        } in conflicts
        inbox = mesh.signals(a.repo, after_seq=first.seq - 1)
        assert [signal.seq for signal in inbox] == sorted(signal.seq for signal in inbox)
    finally:
        mesh.close()


def test_orphaned_claim_surfaces_as_attention_not_implicit_takeover(tmp_path) -> None:
    mesh = StationMesh(tmp_path / "mesh.sqlite3")
    try:
        lease = join(mesh, "station_a", "inc_a")
        claim = mesh.claim(
            repo=lease.repo,
            cell_id="cell_orphan",
            cell_epoch=1,
            station_id=lease.station_id,
            station_lease_epoch=lease.lease_epoch,
        )
        mesh.leave(lease.station_id, lease.lease_epoch, lease.incarnation_id)
        assert {
            "kind": "orphaned_claim",
            "cell_id": claim.cell_id,
            "cell_epoch": 1,
            "station_id": lease.station_id,
            "claim_epoch": 1,
            "expires_at": claim.expires_at,
        } in mesh.conflicts(lease.repo)
    finally:
        mesh.close()


def test_backend_mesh_operation_exposes_shared_rendezvous(tmp_path) -> None:
    factory = SimpleNamespace(state_root=tmp_path)
    lease = operation(
        factory,
        "/mesh/join",
        {
            "station_id": "station_a",
            "repo": "acme/widgets",
            "incarnation_id": "inc_a",
            "operator": "alice",
            "capabilities": ["python"],
        },
    )
    peers = operation(factory, "/mesh/peers", {"repo": "acme/widgets"})
    assert lease.station_id == "station_a"
    assert [peer.station_id for peer in peers] == ["station_a"]
    signal = operation(
        factory,
        "/mesh/say",
        {
            "repo": "acme/widgets",
            "station_id": "station_a",
            "station_lease_epoch": lease.lease_epoch,
            "kind": "observation",
            "topic": "repo",
            "summary": "main moved; re-read GitHub before rebasing",
        },
    )
    assert signal.kind == "observation"
