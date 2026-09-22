from __future__ import annotations

import json
import threading
from pathlib import Path

from swfactory.backend import Factory, make_server
from swfactory.backend_population import BackendPopulationRunner, PopulationTaskInput
from swfactory.cell_runtime import identity_for_job
from swfactory.population_adapter import PopulationAdapterResult
from swfactory.population_execution import PopulationExecutor
from swfactory.population_manifest import build_population_manifest
from swfactory.provider_binding import ProviderChoiceSet, bind_population_manifest
from swfactory.swarm_dynamics import (
    AgentRole,
    ComputeTier,
    ContextPolicy,
    PopulationLane,
    SwarmPlan,
)

TOKEN = "b" * 40
SECRET = "model-token-held-only-by-backend"
POLICY = "policy:v1:" + "a" * 64


class Adapter:
    provider = "model-a"
    credential_capability = "model.invoke"
    credential_env = "TEST_MODEL_TOKEN"

    def __init__(self) -> None:
        self.calls = 0
        self.credentials: list[str | None] = []

    def execute(self, invocation, *, credential):
        self.calls += 1
        self.credentials.append(credential)
        return PopulationAdapterResult(
            output=f"answer:{invocation.task.task_id}:{invocation.instruction}",
            behavior_signature=("model-a", invocation.task.task_id),
            cost_usd=0.2,
            duration_s=0.5,
        )


def _population():
    plan = SwarmPlan(
        phase="liquid",
        mode="coordinate",
        lanes=(
            PopulationLane(
                role=AgentRole.EXPLORER,
                compute_tier=ComputeTier.CHEAP,
                count=2,
                context=ContextPolicy.FRESH,
                temperature=0.8,
                independent_verification=False,
                diversity_axes=("provider", "model", "runtime", "prompt"),
            ),
            PopulationLane(
                role=AgentRole.VERIFIER,
                compute_tier=ComputeTier.DEEP,
                count=1,
                context=ContextPolicy.FRESH,
                temperature=0.0,
                independent_verification=True,
                diversity_axes=("provider", "model", "runtime", "verifier"),
            ),
        ),
        selected_hotspots=(),
        crystals_to_verify=(),
        stop_new_work=False,
        estimated_compute_units=10.0,
        reason="managed e2e",
    )
    manifest = build_population_manifest(
        plan,
        search_provenance_digest="sha256:" + "1" * 64,
    )
    binding = bind_population_manifest(
        manifest,
        choices=ProviderChoiceSet(
            provider=("model-a",),
            model=("explorer", "verifier", "critic"),
            runtime=("backend-a", "backend-b", "backend-c"),
            prompt=("direct", "counterfactual"),
            verifier=("unit", "adversarial", "fresh"),
        ),
    )
    return manifest, binding


def _bound_cell(factory: Factory) -> dict:
    job = {
        "issue": "42",
        "repo": "acme/widgets",
        "dir": "",
        "base_branch": "main",
        "job_idx": 0,
    }
    cell = factory.cell_store.activate(identity_for_job(job), actor="e2e")
    cell = factory.cell_store.patch(
        cell["cell_id"],
        int(cell["epoch"]),
        "policy:e2e",
        policy_digest=POLICY,
    )
    return factory.cell_store.patch(
        cell["cell_id"],
        int(cell["epoch"]),
        "bind:e2e",
        state="running",
        airflow_dag_id="factory",
        airflow_run_id="run-e2e",
        map_index=0,
    )


def test_managed_population_runs_through_backend_without_provider_secret_on_worker(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TEST_MODEL_TOKEN", SECRET)
    adapter = Adapter()
    factory = Factory(
        token=TOKEN,
        airflow_url="http://127.0.0.1:8080",
        repo="acme/widgets",
        state_root=tmp_path / ".factory",
        population_adapters={adapter.provider: adapter},
    )
    cell = _bound_cell(factory)
    manifest, binding = _population()
    server = make_server(factory, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        host, port = server.server_address
        runner = BackendPopulationRunner(
            backend_url=f"http://{host}:{port}",
            backend_token=TOKEN,
            cell_id=cell["cell_id"],
            epoch=int(cell["epoch"]),
            policy_digest=POLICY,
            population_manifest_digest=manifest.digest(),
            provider_binding_digest=binding.digest(),
            inputs={
                task.task_id: PopulationTaskInput(
                    instruction=f"Investigate trajectory {task.task_id}",
                )
                for task in binding.tasks
            },
        )
        executor = PopulationExecutor(runner)

        first = executor.execute(manifest=manifest, binding=binding)
        second = executor.execute(manifest=manifest, binding=binding)

        assert first.telemetry.answered == len(manifest.tasks)
        assert second.telemetry.answered == len(manifest.tasks)
        assert adapter.calls == len(manifest.tasks), "second managed execution must replay backend receipts"
        assert adapter.credentials == [SECRET] * len(manifest.tasks)
        assert [receipt.digest() for receipt in first.receipts] == [
            receipt.digest() for receipt in second.receipts
        ]

        visible = json.dumps(
            {
                "report": first.canonical_dict(),
                "operations": factory.control.operations.unresolved(limit=100),
                "evidence": factory.evidence.writer.read(cell["cell_id"]),
            }
        )
        assert SECRET not in visible
        assert "TEST_MODEL_TOKEN" not in visible
        assert all(receipt.provider == "model-a" for receipt in first.receipts)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        factory.close()
