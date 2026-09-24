from __future__ import annotations

import json
from pathlib import Path

import pytest

from swfactory.cells import CellIdentity, CellStore
from swfactory.credential_lease import (
    CredentialLeaseBroker,
    CredentialLeaseError,
    LeaseBinding,
)

CELL = "cell_0123456789abcdef01234567"
POLICY = "policy:v1:" + "a" * 64


def _binding(*, attempt: int = 1, epoch: int = 3, sandbox: str = "box-a") -> LeaseBinding:
    return LeaseBinding(
        factory_run_id="factory-run",
        dag_run_id="dag-run",
        task_instance_id="build[0]",
        stage_id="build",
        sandbox_id=sandbox,
        attempt_number=attempt,
        cell_id=CELL,
        epoch=epoch,
        operation_key="github_publish:abc",
        policy_digest=POLICY,
    )


@pytest.fixture
def broker(tmp_path: Path):
    value = CredentialLeaseBroker(
        tmp_path / "leases.sqlite3",
        providers={"github.publish": lambda _binding: "ghp_runtime_only"},
    )
    try:
        yield value
    finally:
        value.close()


def test_bearer_is_opaque_and_never_persisted(broker: CredentialLeaseBroker) -> None:
    handle = broker.mint(_binding(), capability="github.publish")
    row = broker.inspect(handle.lease_id)

    assert handle.bearer.startswith("swfl_")
    assert handle.bearer not in json.dumps(row)
    assert handle.bearer not in repr(handle)
    assert "secret_hash" not in row


def test_redeem_is_bound_to_attempt_sandbox_task_and_process(
    broker: CredentialLeaseBroker,
) -> None:
    handle = broker.mint(_binding(), capability="github.publish")

    assert (
        broker.redeem(
            handle,
            _binding(),
            process_nonce="process-nonce-0001",
        )
        == "ghp_runtime_only"
    )

    with pytest.raises(CredentialLeaseError, match="binding_mismatch"):
        broker.redeem(
            handle,
            _binding(attempt=2),
            process_nonce="process-nonce-0001",
        )

    with pytest.raises(CredentialLeaseError, match="binding_mismatch"):
        broker.redeem(
            handle,
            _binding(sandbox="box-b"),
            process_nonce="process-nonce-0001",
        )

    with pytest.raises(CredentialLeaseError, match="process_mismatch"):
        broker.redeem(
            handle,
            _binding(),
            process_nonce="different-process-02",
        )


def test_epoch_revocation_is_synchronous_and_denials_are_negative_provenance(
    broker: CredentialLeaseBroker,
) -> None:
    handle = broker.mint(_binding(), capability="github.publish")

    assert broker.revoke_epoch(CELL, 3) == 1
    with pytest.raises(CredentialLeaseError, match="revoked"):
        broker.redeem(
            handle,
            _binding(),
            process_nonce="process-nonce-0001",
        )

    denial = broker.denials()[0]
    assert denial["lease_id"] == handle.lease_id
    assert denial["reason"] == "revoked"
    assert handle.bearer not in json.dumps(denial)
    assert "ghp_runtime_only" not in json.dumps(denial)


def test_retry_must_mint_a_fresh_lease(broker: CredentialLeaseBroker) -> None:
    first = broker.mint(_binding(attempt=1), capability="github.publish")
    second = broker.mint(_binding(attempt=2), capability="github.publish")

    assert first.lease_id != second.lease_id
    assert first.bearer != second.bearer
    with pytest.raises(CredentialLeaseError, match="binding_mismatch"):
        broker.redeem(
            first,
            _binding(attempt=2),
            process_nonce="process-nonce-0002",
        )


def test_untrusted_bridge_cannot_smuggle_authority_material() -> None:
    raw = _binding().canonical()
    raw["bearer"] = "secret-from-xcom"

    with pytest.raises(CredentialLeaseError, match="unsupported fields"):
        LeaseBinding.from_untrusted(raw)


def test_broker_denies_unknown_capabilities(tmp_path: Path) -> None:
    broker = CredentialLeaseBroker(tmp_path / "leases.sqlite3")
    try:
        with pytest.raises(CredentialLeaseError, match="no trusted provider"):
            broker.mint(_binding(), capability="github.publish")
    finally:
        broker.close()


def test_epoch_advance_revokes_live_lease_before_takeover_returns(tmp_path: Path) -> None:
    store = CellStore(tmp_path / "cells.sqlite3")
    identity = CellIdentity("acme/widgets", ".", "42")
    cell = store.activate(identity, actor="test")
    broker = CredentialLeaseBroker(
        tmp_path / "leases.sqlite3",
        providers={"github.publish": lambda _binding: "raw"},
        epoch_reader=lambda cell_id: int(store.get(cell_id)["epoch"]),
    )
    store.on_authority_revoked = lambda cell_id, epoch, reason: broker.revoke_epoch(cell_id, epoch, reason=reason)
    binding = LeaseBinding(
        factory_run_id="factory-run",
        dag_run_id="dag-run",
        task_instance_id="build[0]",
        stage_id="build",
        sandbox_id="box-a",
        attempt_number=1,
        cell_id=str(cell["cell_id"]),
        epoch=1,
        operation_key="github_publish:abc",
        policy_digest=POLICY,
    )
    try:
        handle = broker.mint(binding, capability="github.publish")
        assert store.take_epoch(binding.cell_id, 1, actor="takeover") == 2
        with pytest.raises(CredentialLeaseError, match="revoked|stale_epoch"):
            broker.redeem(handle, binding, process_nonce="process-nonce-0001")
    finally:
        broker.close()
        store.close()


def test_wildcard_capability_is_refused_even_when_a_provider_exists(tmp_path: Path) -> None:
    broker = CredentialLeaseBroker(
        tmp_path / "leases.sqlite3",
        providers={"github": lambda _binding: "raw"},
    )
    try:
        with pytest.raises(CredentialLeaseError, match="explicit and deny-by-default"):
            broker.mint(_binding(), capability="github")
    finally:
        broker.close()

@pytest.mark.parametrize("field", ["epoch", "attempt_number"])
@pytest.mark.parametrize("value", [True, "3", 2**64])
def test_binding_rejects_values_outside_rust_u64_contract(field: str, value: object) -> None:
    raw = _binding().canonical()
    raw[field] = value
    with pytest.raises((CredentialLeaseError, ValueError), match=field):
        LeaseBinding.from_untrusted(raw)


def test_redeem_does_not_hold_broker_lock_while_reading_epoch(tmp_path: Path) -> None:
    seen = []
    broker_ref: dict[str, CredentialLeaseBroker] = {}

    def read_epoch(_cell_id: str) -> int:
        broker = broker_ref["broker"]
        acquired = broker.lock.acquire(blocking=False)
        seen.append(acquired)
        if acquired:
            broker.lock.release()
        return 3

    broker = CredentialLeaseBroker(
        tmp_path / "leases.sqlite3",
        providers={"github.publish": lambda _binding: "raw"},
        epoch_reader=read_epoch,
    )
    broker_ref["broker"] = broker
    try:
        handle = broker.mint(_binding(), capability="github.publish")
        assert broker.redeem(handle, _binding(), process_nonce="process-nonce-0001") == "raw"
        assert seen and all(seen)
    finally:
        broker.close()


def test_revoke_waits_until_provider_materialization_finishes(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    results: list[str] = []

    def provider(_binding: LeaseBinding) -> str:
        entered.set()
        assert release.wait(timeout=2)
        return "raw"

    broker = CredentialLeaseBroker(
        tmp_path / "leases.sqlite3",
        providers={"github.publish": provider},
    )
    try:
        handle = broker.mint(_binding(), capability="github.publish")

        redeem_thread = threading.Thread(
            target=lambda: results.append(
                broker.redeem(handle, _binding(), process_nonce="process-nonce-0001")
            )
        )
        redeem_thread.start()
        assert entered.wait(timeout=2)

        revoked: list[bool] = []
        revoke_thread = threading.Thread(target=lambda: revoked.append(broker.revoke(handle.lease_id)))
        revoke_thread.start()
        revoke_thread.join(timeout=0.05)
        assert revoke_thread.is_alive()

        release.set()
        redeem_thread.join(timeout=2)
        revoke_thread.join(timeout=2)

        assert results == ["raw"]
        assert revoked == [True]
    finally:
        broker.close()
