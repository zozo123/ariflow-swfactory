from __future__ import annotations

from dataclasses import replace

import pytest

from swfactory.authority import ResourceKind
from swfactory.cells import CellStore, StaleEpoch, operation_key
from swfactory.core_capabilities import (
    AirflowBinding,
    CoreCapabilityError,
    CoreCapabilityRuntime,
    CoreMutationRequest,
)
from swfactory.idempotency import MutationOutcome, OperationInDoubt, OperationJournal
from swfactory.liquid_security_runtime import Capability, SecurityContext
from swfactory.liquid_workgraph_runtime import WorkNode
from swfactory.security_contract import CanonicalPolicy
from swfactory.trust_evidence import TrustedEvidence


@pytest.fixture
def runtime(tmp_path):
    cells = CellStore(tmp_path / "cells.db")
    journal = OperationJournal(tmp_path / "operations.db")
    value = CoreCapabilityRuntime(
        cells=cells,
        journal=journal,
        evidence=TrustedEvidence(tmp_path / "evidence"),
    )
    try:
        yield value
    finally:
        journal.close()
        cells.close()


def prepared(runtime: CoreCapabilityRuntime):
    policy = CanonicalPolicy(repo="acme/repo", target="main")
    cell = runtime.activate(
        repo="acme/repo",
        target="main",
        issue="42",
        actor="intake",
        policy=policy,
    )
    airflow = AirflowBinding(
        cell["cell_id"],
        int(cell["epoch"]),
        "software_factory",
        "run-42",
        0,
    )
    runtime.bind_airflow(airflow)
    security = SecurityContext(
        tenant="tenant-a",
        cell_id=cell["cell_id"],
        epoch=int(cell["epoch"]),
        role="publisher",
        capabilities=frozenset({Capability.PUBLISH_GIT}),
    )
    request = CoreMutationRequest(
        airflow=airflow,
        security=security,
        resource=ResourceKind.GITHUB_PUBLICATION,
        capability=Capability.PUBLISH_GIT,
        actor="python-backend",
        kind="github_publish",
        expected_policy_digest=policy.digest(),
        target_tenant="tenant-a",
        parts=("pull-request",),
    )
    return policy, cell, airflow, request


def test_activation_replay_converges_on_same_cell_and_policy_evidence(
    runtime: CoreCapabilityRuntime,
) -> None:
    policy = CanonicalPolicy(repo="acme/repo", target="main")
    first = runtime.activate(
        repo="acme/repo",
        target="main",
        issue="42",
        actor="intake-a",
        policy=policy,
    )
    second = runtime.activate(
        repo="acme/repo",
        target="main",
        issue="42",
        actor="intake-b",
        policy=policy,
    )

    assert first["cell_id"] == second["cell_id"]
    assert first["epoch"] == second["epoch"] == 1
    policy_events = [
        row
        for row in runtime.evidence.writer.read(first["cell_id"])
        if row.get("kind") == "policy_activation"
    ]
    assert len(policy_events) == 1


def test_core_external_effect_is_fenced_replayable_and_evidenced(
    runtime: CoreCapabilityRuntime,
) -> None:
    _, cell, _, request = prepared(runtime)
    calls = 0

    def publish():
        nonlocal calls
        calls += 1
        return {
            "url": "https://github.com/acme/repo/pull/1",
            "access_token": "must-not-persist",
        }

    first = runtime.execute_external(request, publish)
    second = runtime.execute_external(request, publish)

    assert calls == 1
    assert first.operation_key == second.operation_key
    assert first.replayed is False
    assert second.replayed is True
    assert first.result["access_token"] == "[REDACTED]"
    assert second.result == first.result

    events = [
        event
        for event in runtime.cells.history(cell["cell_id"])
        if event["operation_key"] == first.operation_key
    ]
    assert len(events) == 1
    assert events[0]["kind"] == "external_mutation"

    evidence = [
        row
        for row in runtime.evidence.writer.read(cell["cell_id"])
        if (row.get("payload") or {}).get("operation_key") == first.operation_key
    ]
    assert len(evidence) == 1
    assert evidence[0]["digest"] == first.evidence_digest == second.evidence_digest
    assert runtime.evidence.verify(cell["cell_id"])[0] is True


def test_stale_epoch_is_rejected_before_external_effect(
    runtime: CoreCapabilityRuntime,
) -> None:
    _, cell, _, request = prepared(runtime)
    runtime.cells.take_epoch(cell["cell_id"], int(cell["epoch"]), "operator")
    called = False

    def publish():
        nonlocal called
        called = True
        return {"ok": True}

    with pytest.raises(StaleEpoch):
        runtime.execute_external(request, publish)
    assert called is False


def test_policy_drift_is_rejected_before_external_effect(
    runtime: CoreCapabilityRuntime,
) -> None:
    _, cell, _, request = prepared(runtime)
    replacement = CanonicalPolicy(
        repo="acme/repo",
        target="main",
        metadata=(("revision", "2"),),
    )
    runtime.cells.patch(
        cell["cell_id"],
        int(cell["epoch"]),
        operation_key("policy-test-drift", cell["cell_id"], str(cell["epoch"])),
        policy_digest=replacement.digest(),
    )
    called = False

    def publish():
        nonlocal called
        called = True
        return {"ok": True}

    with pytest.raises(PermissionError, match="policy digest is stale"):
        runtime.execute_external(request, publish)
    assert called is False


def test_only_bound_airflow_run_may_drive_external_effect(
    runtime: CoreCapabilityRuntime,
) -> None:
    _, _, airflow, request = prepared(runtime)
    wrong = replace(airflow, run_id="other-run")
    request = replace(request, airflow=wrong)

    with pytest.raises(CoreCapabilityError, match="bound Airflow run"):
        runtime.execute_external(request, lambda: {"ok": True})


def test_capability_and_tenant_are_fail_closed(runtime: CoreCapabilityRuntime) -> None:
    _, _, _, request = prepared(runtime)
    request = replace(request, target_tenant="tenant-b")

    with pytest.raises(PermissionError, match="lacks"):
        runtime.execute_external(request, lambda: {"ok": True})


def test_in_doubt_operation_requires_observation_before_replay(
    runtime: CoreCapabilityRuntime,
) -> None:
    _, _, _, request = prepared(runtime)
    calls = 0

    def publish():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("connection dropped after request")
        return {"url": "https://github.com/acme/repo/pull/2"}

    with pytest.raises(RuntimeError, match="connection dropped"):
        runtime.execute_external(request, publish)

    with pytest.raises(OperationInDoubt):
        runtime.execute_external(request, publish)
    assert calls == 1

    safe_request = replace(request, replay_safe=True)
    result = runtime.execute_external(
        safe_request,
        publish,
        reconcile=lambda: MutationOutcome(
            "definitely_absent",
            evidence={"lookup": "not-found"},
        ),
    )
    assert calls == 2
    assert result.result["url"].endswith("/2")


def test_airflow_binding_is_idempotent_but_conflicts_fail(
    runtime: CoreCapabilityRuntime,
) -> None:
    _, cell, airflow, _ = prepared(runtime)
    before = [
        row
        for row in runtime.evidence.writer.read(cell["cell_id"])
        if row.get("kind") == "airflow_bound"
    ]
    assert runtime.bind_airflow(airflow)["airflow_run_id"] == airflow.run_id
    after = [
        row
        for row in runtime.evidence.writer.read(cell["cell_id"])
        if row.get("kind") == "airflow_bound"
    ]
    assert len(before) == len(after) == 1

    with pytest.raises(CoreCapabilityError, match="different Airflow run"):
        runtime.bind_airflow(replace(airflow, run_id="run-conflict"))


def test_issue_local_workgraph_stays_bounded(runtime: CoreCapabilityRuntime) -> None:
    plan = runtime.compile_work([WorkNode(f"n{i}") for i in range(8)], max_width=7)
    assert max(len(layer) for layer in plan.layers) <= 7
    assert set(node for layer in plan.layers for node in layer) == {f"n{i}" for i in range(8)}
