from __future__ import annotations

import pytest

from swfactory.station_mesh import SignalKind, StaleStationLease, StationMesh


def join(mesh: StationMesh, station: str, incarnation: str):
    return mesh.join(
        station_id=station,
        repo="acme/widgets",
        incarnation_id=incarnation,
        operator=station,
        ttl_s=120,
    )


def test_restart_orphans_old_epoch_and_can_reclaim_immediately(tmp_path) -> None:
    mesh = StationMesh(tmp_path / "mesh.sqlite3")
    try:
        first = join(mesh, "station_a", "inc_a1")
        other = join(mesh, "station_b", "inc_b1")
        claim = mesh.claim(
            repo=first.repo,
            cell_id="cell_restart",
            cell_epoch=4,
            station_id=first.station_id,
            station_lease_epoch=first.lease_epoch,
            ttl_s=900,
        )
        mesh.say(
            station_id=first.station_id,
            station_lease_epoch=first.lease_epoch,
            repo=first.repo,
            kind=SignalKind.INTENT.value,
            topic="issue:42",
            summary="old process intent",
            cell_id="cell_restart",
            cell_epoch=4,
        )
        mesh.say(
            station_id=other.station_id,
            station_lease_epoch=other.lease_epoch,
            repo=other.repo,
            kind=SignalKind.INTENT.value,
            topic="issue:42",
            summary="other station also saw the work",
            cell_id="cell_restart",
            cell_epoch=4,
        )
        assert any(row["kind"] == "competing_intents" for row in mesh.conflicts(first.repo))

        restarted = join(mesh, "station_a", "inc_a2")
        assert restarted.lease_epoch == first.lease_epoch + 1
        attention = mesh.conflicts(first.repo)
        assert any(
            row["kind"] == "orphaned_claim" and row["claim_epoch"] == claim.claim_epoch for row in attention
        )
        # The old process's intent is not attributed to the replacement incarnation.
        assert not any(row["kind"] == "competing_intents" for row in attention)

        # No 15-minute claim-TTL stall: a dead/replaced lease is takeoverable immediately, with a
        # new claim epoch so any late old release is fenced.
        resumed = mesh.claim(
            repo=restarted.repo,
            cell_id="cell_restart",
            cell_epoch=4,
            station_id=restarted.station_id,
            station_lease_epoch=restarted.lease_epoch,
            ttl_s=900,
        )
        assert resumed.claim_epoch == claim.claim_epoch + 1
        assert resumed.station_lease_epoch == restarted.lease_epoch
        assert not any(row["kind"] == "orphaned_claim" for row in mesh.conflicts(restarted.repo))

        with pytest.raises(StaleStationLease):
            mesh.release(
                repo=first.repo,
                cell_id="cell_restart",
                station_id=first.station_id,
                station_lease_epoch=first.lease_epoch,
                claim_epoch=claim.claim_epoch,
            )
    finally:
        mesh.close()
