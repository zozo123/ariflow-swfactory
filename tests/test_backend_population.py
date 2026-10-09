from __future__ import annotations

import json
from pathlib import Path

import pytest
from support import bound_population, lane, swarm_plan

from swfactory.backend.service import Factory, Refused
from swfactory.cell_runtime import identity_for_job
from swfactory.idempotency import OperationInDoubt
from swfactory.population_adapter import PopulationAdapterResult
from swfactory.provider_binding import ProviderChoiceSet
from swfactory.swarm_dynamics import AgentRole, ComputeTier

TOKEN = "t" * 40
POLICY = "policy:v1:" + "a" * 64
SECRET = "provider-secret-never-persist"


class FakeAdapter:
    provider = "model-a"
    credential_capability = "model.invoke"
    credential_env = "TEST_MODEL_TOKEN"

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls = 0
        self.credentials: list[str | None] = []

    def execute(self, invocation, *, credential):
        self.calls += 1
        self.credentials.append(credential)
        if self.fail:
            raise TimeoutError("provider outcome unknown")
        assert invocation.task.provider == self.provider
        return PopulationAdapterResult(
            output=f"candidate:{invocation.task.task_id}",
            behavior_signature=("candidate", invocation.task.task_id),
            cost_usd=0.25,
            duration_s=1.5,
        )


def _population():
    plan = swarm_plan(
        lane(AgentRole.EXPLORER, ComputeTier.CHEAP, 1, temperature=1.0, axes=("provider", "model", "runtime")),
        units=1.0,
        reason="backend population test",
    )
    return bound_population(plan, ProviderChoiceSet(provider=("model-a",), model=("reasoner",), runtime=("backend",)))


def _factory(tmp_path: Path, monkeypatch, adapter: FakeAdapter) -> tuple[Factory, dict]:
    monkeypatch.setenv("TEST_MODEL_TOKEN", SECRET)
    factory = Factory(
        token=TOKEN,
        airflow_url="http://127.0.0.1:8080",
        repo="acme/widgets",
        state_root=tmp_path / ".factory",
        population_adapters={adapter.provider: adapter},
    )
    job = {
        "issue": "42",
        "repo": "acme/widgets",
        "dir": "",
        "base_branch": "main",
        "job_idx": 0,
    }
    cell = factory.cell_store.activate(identity_for_job(job), actor="test")
    cell = factory.cell_store.patch(
        cell["cell_id"],
        int(cell["epoch"]),
        "policy:test",
        policy_digest=POLICY,
    )
    cell = factory.cell_store.patch(
        cell["cell_id"],
        int(cell["epoch"]),
        "bind:test",
        state="running",
        airflow_dag_id="factory",
        airflow_run_id="run-1",
        map_index=0,
    )
    return factory, cell


def _body(cell: dict, *, operation_key: str = "population:test") -> dict:
    manifest, binding = _population()
    task = binding.tasks[0]
    return {
        "cell_id": cell["cell_id"],
        "epoch": int(cell["epoch"]),
        "policy_digest": POLICY,
        "operation_key": operation_key,
        "population_manifest_digest": manifest.digest(),
        "provider_binding_digest": binding.digest(),
        "task": task.canonical_dict() | {"binding_digest": task.binding_digest},
        "instruction": "Solve the bounded search question and return one candidate.",
        "context_artifact_digests": [],
    }


def test_backend_population_call_replays_without_duplicate_provider_spend(
    tmp_path: Path,
    monkeypatch,
) -> None:
    adapter = FakeAdapter()
    factory, cell = _factory(tmp_path, monkeypatch, adapter)
    body = _body(cell)
    try:
        first = factory.operation("/population/execute", body)
        second = factory.operation("/population/execute", body)

        assert adapter.calls == 1
        assert adapter.credentials == [SECRET]
        assert first["replayed"] is False
        assert second["replayed"] is True
        assert first["receipt"] == second["receipt"]
        assert first["candidate_artifact_digest"] == second["candidate_artifact_digest"]
        assert factory.population_artifacts.read(first["candidate_artifact_digest"])["output"].startswith("candidate:")

        serialized = json.dumps(
            {
                "first": first,
                "operation": factory.control.operations.get(body["operation_key"]),
                "evidence": factory.evidence.writer.read(cell["cell_id"]),
            }
        )
        assert SECRET not in serialized
        assert "TEST_MODEL_TOKEN" not in serialized
    finally:
        factory.close()


def test_backend_population_timeout_becomes_in_doubt_and_does_not_replay_blindly(
    tmp_path: Path,
    monkeypatch,
) -> None:
    adapter = FakeAdapter(fail=True)
    factory, cell = _factory(tmp_path, monkeypatch, adapter)
    body = _body(cell, operation_key="population:ambiguous")
    try:
        with pytest.raises(TimeoutError, match="outcome unknown"):
            factory.operation("/population/execute", body)

        with pytest.raises(OperationInDoubt):
            factory.operation("/population/execute", body)

        assert adapter.calls == 1
        row = factory.control.operations.get(body["operation_key"])
        assert row["state"] == "in_doubt"
        assert row["attempts"] == 1
    finally:
        factory.close()


def test_backend_population_refuses_stale_policy_before_adapter_or_lease(
    tmp_path: Path,
    monkeypatch,
) -> None:
    adapter = FakeAdapter()
    factory, cell = _factory(tmp_path, monkeypatch, adapter)
    body = _body(cell)
    body["policy_digest"] = "policy:v1:" + "f" * 64
    try:
        with pytest.raises(Refused, match="policy digest changed") as error:
            factory.operation("/population/execute", body)
        assert error.value.status == 409
        assert adapter.calls == 0
        assert factory.leases.denials() == []
    finally:
        factory.close()


def test_backend_population_refuses_unconfigured_bound_provider(
    tmp_path: Path,
    monkeypatch,
) -> None:
    adapter = FakeAdapter()
    factory, cell = _factory(tmp_path, monkeypatch, adapter)
    body = _body(cell)
    raw = dict(body["task"])
    raw["provider"] = "missing-provider"
    # Recompute is intentionally impossible here without the original binder: changing the provider
    # while retaining the binding digest must be rejected before adapter lookup.
    body["task"] = raw
    try:
        with pytest.raises(Exception, match="binding digest mismatch"):
            factory.operation("/population/execute", body)
        assert adapter.calls == 0
    finally:
        factory.close()
