from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from swfactory.population_adapter import (
    HttpPopulationAdapter,
    HttpPopulationAdapterConfig,
    PopulationArtifactStore,
    PopulationInvocation,
    http_population_adapters_from_document,
)
from swfactory.population_manifest import PopulationManifestError, build_population_manifest
from swfactory.provider_binding import ProviderChoiceSet, bind_population_manifest
from swfactory.swarm_dynamics import (
    AgentRole,
    ComputeTier,
    ContextPolicy,
    PopulationLane,
    SwarmPlan,
)


class Response:
    def __init__(self, code: int, payload: dict) -> None:
        self.code = code
        self._raw = json.dumps(payload).encode()

    def read(self, amount: int | None = None) -> bytes:
        return self._raw if amount is None else self._raw[:amount]

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return None


def _invocation() -> PopulationInvocation:
    plan = SwarmPlan(
        phase="gas",
        mode="diverge",
        lanes=(
            PopulationLane(
                role=AgentRole.EXPLORER,
                compute_tier=ComputeTier.CHEAP,
                count=1,
                context=ContextPolicy.FRESH,
                temperature=1.0,
                independent_verification=False,
                diversity_axes=("provider", "model", "runtime"),
            ),
        ),
        selected_hotspots=(),
        crystals_to_verify=(),
        stop_new_work=False,
        estimated_compute_units=1.0,
        reason="adapter test",
    )
    manifest = build_population_manifest(
        plan,
        search_provenance_digest="sha256:" + "a" * 64,
    )
    binding = bind_population_manifest(
        manifest,
        choices=ProviderChoiceSet(
            provider=("model-a",),
            model=("reasoner",),
            runtime=("gateway",),
        ),
    )
    return PopulationInvocation(
        task=binding.tasks[0],
        population_manifest_digest=manifest.digest(),
        provider_binding_digest=binding.digest(),
        instruction="Produce one bounded candidate.",
        context_artifact_digests=("sha256:" + "c" * 64,),
        objective_digest="sha256:" + "d" * 64,
    )


def test_http_population_adapter_keeps_credential_out_of_request_body() -> None:
    seen = {}

    def opener(request, timeout=0):
        seen["url"] = request.full_url
        seen["headers"] = dict(request.header_items())
        seen["body"] = request.data
        seen["timeout"] = timeout
        return Response(
            200,
            {
                "output": "candidate bytes",
                "behavior_signature": ["novel", "bounded"],
                "cost_usd": 0.125,
                "duration_s": 2.0,
            },
        )

    adapter = HttpPopulationAdapter(
        HttpPopulationAdapterConfig(
            provider="model-a",
            endpoint="https://provider.example/v1/search",
            credential_capability="model.invoke",
            credential_env="MODEL_TOKEN",
            timeout_s=15.0,
        ),
        opener=opener,
    )
    result = adapter.execute(_invocation(), credential="runtime-secret")

    assert result.output == "candidate bytes"
    assert result.behavior_signature == ("novel", "bounded")
    assert seen["url"] == "https://provider.example/v1/search"
    assert seen["timeout"] == 15.0
    assert seen["headers"]["Authorization"] == "Bearer runtime-secret"
    assert b"runtime-secret" not in seen["body"]
    assert b"MODEL_TOKEN" not in seen["body"]


def test_plaintext_remote_population_adapter_is_refused() -> None:
    with pytest.raises(PopulationManifestError, match="loopback-only"):
        HttpPopulationAdapterConfig(
            provider="model-a",
            endpoint="http://provider.example/search",
        ).validate()


def test_population_adapter_config_contains_env_name_not_secret(monkeypatch) -> None:
    monkeypatch.setenv("MODEL_TOKEN", "actual-secret")
    adapters = http_population_adapters_from_document(
        {
            "model-a": {
                "endpoint": "https://provider.example/search",
                "credential_capability": "model.invoke",
                "credential_env": "MODEL_TOKEN",
            }
        }
    )

    adapter = adapters["model-a"]
    visible = json.dumps(adapter.config.canonical_dict())
    assert "MODEL_TOKEN" in visible
    assert "actual-secret" not in visible


def test_population_adapter_config_rejects_unknown_fields() -> None:
    with pytest.raises(PopulationManifestError, match="unknown fields"):
        http_population_adapters_from_document(
            {
                "model-a": {
                    "endpoint": "https://provider.example/search",
                    "api_key": "must-never-live-here",
                }
            }
        )


def test_population_artifact_identity_binds_invocation_and_output(tmp_path: Path) -> None:
    store = PopulationArtifactStore(tmp_path)
    same_output = "same candidate bytes"

    first, _ = store.retain(
        invocation_digest="sha256:" + "1" * 64,
        output=same_output,
    )
    second, _ = store.retain(
        invocation_digest="sha256:" + "2" * 64,
        output=same_output,
    )

    assert first != second
    assert store.read(first)["output"] == same_output
    assert store.read(second)["output"] == same_output
    assert store.read(first)["output_sha256"] == store.read(second)["output_sha256"]


def test_population_artifact_read_detects_byte_tampering(tmp_path: Path) -> None:
    store = PopulationArtifactStore(tmp_path)
    digest, path = store.retain(
        invocation_digest="sha256:" + "3" * 64,
        output="original",
    )
    document = json.loads(path.read_text())
    document["output"] = "tampered"
    path.write_text(json.dumps(document))

    with pytest.raises(PopulationManifestError, match="bytes do not match"):
        store.read(digest)
