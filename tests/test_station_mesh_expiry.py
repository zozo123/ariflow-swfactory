from __future__ import annotations

import pytest

import swfactory.station_mesh as mesh_mod
from swfactory.station_mesh import ClaimConflict, StaleStationLease, StationMesh


def test_expired_station_cannot_resurrect_with_late_heartbeat(tmp_path, monkeypatch) -> None:
    now = [1000.0]
    monkeypatch.setattr(mesh_mod.time, "time", lambda: now[0])
    mesh = StationMesh(tmp_path / "mesh.sqlite3")
    try:
        lease = mesh.join(
            station_id="station_a",
            repo="acme/widgets",
            incarnation_id="inc_same",
            operator="alice",
            ttl_s=10,
        )
        now[0] += 11
        with pytest.raises(StaleStationLease):
            mesh.heartbeat(lease.station_id, lease.lease_epoch, lease.incarnation_id, ttl_s=10)

        # Rejoining, even with a retried incarnation id, crosses an expiry boundary and therefore
        # receives a new epoch. Delayed packets from the old epoch stay fenced forever.
        renewed = mesh.join(
            station_id="station_a",
            repo="acme/widgets",
            incarnation_id="inc_same",
            operator="alice",
            ttl_s=10,
        )
        assert renewed.lease_epoch == lease.lease_epoch + 1
    finally:
        mesh.close()


def test_expired_claim_renewal_gets_new_epoch(tmp_path, monkeypatch) -> None:
    now = [2000.0]
    monkeypatch.setattr(mesh_mod.time, "time", lambda: now[0])
    mesh = StationMesh(tmp_path / "mesh.sqlite3")
    try:
        station = mesh.join(
            station_id="station_a",
            repo="acme/widgets",
            incarnation_id="inc_a",
            operator="alice",
            ttl_s=120,
        )
        first = mesh.claim(
            repo=station.repo,
            cell_id="cell_expiring_claim",
            cell_epoch=2,
            station_id=station.station_id,
            station_lease_epoch=station.lease_epoch,
            ttl_s=10,
        )
        now[0] += 11
        renewed = mesh.claim(
            repo=station.repo,
            cell_id="cell_expiring_claim",
            cell_epoch=2,
            station_id=station.station_id,
            station_lease_epoch=station.lease_epoch,
            ttl_s=10,
        )
        assert renewed.claim_epoch == first.claim_epoch + 1
        with pytest.raises(ClaimConflict):
            mesh.release(
                repo=station.repo,
                cell_id="cell_expiring_claim",
                station_id=station.station_id,
                station_lease_epoch=station.lease_epoch,
                claim_epoch=first.claim_epoch,
            )
    finally:
        mesh.close()


def test_dead_station_claim_is_takeoverable_before_claim_ttl(tmp_path, monkeypatch) -> None:
    now = [3000.0]
    monkeypatch.setattr(mesh_mod.time, "time", lambda: now[0])
    mesh = StationMesh(tmp_path / "mesh.sqlite3")
    try:
        dead = mesh.join(
            station_id="station_dead",
            repo="acme/widgets",
            incarnation_id="inc_dead",
            operator="alice",
            ttl_s=10,
        )
        live = mesh.join(
            station_id="station_live",
            repo="acme/widgets",
            incarnation_id="inc_live",
            operator="bob",
            ttl_s=120,
        )
        first = mesh.claim(
            repo=dead.repo,
            cell_id="cell_takeover",
            cell_epoch=5,
            station_id=dead.station_id,
            station_lease_epoch=dead.lease_epoch,
            ttl_s=900,
        )
        now[0] += 11
        takeover = mesh.claim(
            repo=live.repo,
            cell_id="cell_takeover",
            cell_epoch=5,
            station_id=live.station_id,
            station_lease_epoch=live.lease_epoch,
            ttl_s=900,
        )
        assert takeover.claim_epoch == first.claim_epoch + 1
        assert takeover.station_id == live.station_id
    finally:
        mesh.close()
