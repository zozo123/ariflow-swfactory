from __future__ import annotations

import pytest

from swfactory.maintenance_incidents import DurableIncidentLedger, IncidentIdentity, IncidentLedger, IncidentReceipt, IncidentState


def identity(evidence: str = "a" * 64) -> IncidentIdentity:
    return IncidentIdentity("zozo123/ariflow-swfactory", "run-1", evidence, "delivery_failure_rate", "v1")


def test_repeated_observation_creates_one_logical_incident() -> None:
    ledger = IncidentLedger()
    first = identity()
    assert ledger.propose(first, {"metric": "delivery_failure_rate"})[1] is True
    assert ledger.propose(first, {"metric": "delivery_failure_rate"})[1] is False
    ledger.begin_create(first)
    receipt = ledger.record_created(first, 42, "https://github.com/x/issues/42", {"metric": "delivery_failure_rate"})
    assert ledger.states[first.key] == IncidentState.OPEN
    assert ledger.receipts[first.key] == receipt


def test_lost_create_response_can_adopt_matching_observed_issue() -> None:
    ledger = IncidentLedger()
    item = identity()
    ledger.propose(item, {})
    ledger.begin_create(item)
    ledger.mark_unknown(item)
    observed = IncidentReceipt(item.key, 42, "https://github.com/x/issues/42", "b" * 64)
    assert ledger.adopt_observed(item, observed) == observed
    assert ledger.states[item.key] == IncidentState.OPEN


def test_observed_issue_for_another_incident_is_refused() -> None:
    ledger = IncidentLedger()
    item = identity()
    other = identity("c" * 64)
    ledger.propose(item, {})
    ledger.begin_create(item)
    ledger.mark_unknown(item)
    with pytest.raises(RuntimeError, match="another incident"):
        ledger.adopt_observed(item, IncidentReceipt(other.key, 43, "https://github.com/x/issues/43", "d" * 64))


def test_durable_ledger_survives_restart_and_dedupes(tmp_path) -> None:
    path = tmp_path / "incidents.json"
    item = identity()

    first = DurableIncidentLedger.load(path)
    assert first.propose(item, {"metric": "delivery_failure_rate"})[1] is True
    first.begin_create(item)
    first.record_created(item, 42, "https://github.com/x/issues/42", {"metric": "delivery_failure_rate"})

    second = DurableIncidentLedger.load(path)
    assert second.states[item.key] == IncidentState.OPEN
    assert second.receipts[item.key].issue_number == 42
    assert second.propose(item, {"metric": "delivery_failure_rate"})[1] is False


def test_in_doubt_creation_survives_restart_for_observation(tmp_path) -> None:
    path = tmp_path / "incidents.json"
    item = identity()

    first = DurableIncidentLedger.load(path)
    first.propose(item, {})
    first.begin_create(item)
    first.mark_unknown(item)

    second = DurableIncidentLedger.load(path)
    assert second.states[item.key] == IncidentState.IN_DOUBT
    receipt = IncidentReceipt(item.key, 42, "https://github.com/x/issues/42", "b" * 64)
    second.adopt_observed(item, receipt)

    third = DurableIncidentLedger.load(path)
    assert third.states[item.key] == IncidentState.OPEN
    assert third.receipts[item.key] == receipt
