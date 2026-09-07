"""The provenance gate must detect release fallbacks, including same-version wheels."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "verify_airflow_main", Path(__file__).resolve().parents[1] / "scripts/verify_airflow_main.py"
)
assert SPEC and SPEC.loader
verify_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_module)


@pytest.mark.parametrize("source", [{}, {"vcs_info": {"commit_id": "wrong"}}])
def test_provenance_rejects_release_or_wrong_commit(monkeypatch, source):
    class Distribution:
        version = "3.4.0"

        def read_text(self, name):
            return json.dumps(source)

    monkeypatch.setattr(verify_module.metadata, "distribution", lambda _: Distribution())
    with pytest.raises(RuntimeError, match="expected apache/airflow"):
        verify_module.verify("--upstream", "a" * 40)


def test_upstream_sbx_rejects_undeclared_network_policy():
    base = pytest.importorskip("airflow.providers.common.ai.sandbox.base")
    from swfactory.sandbox import ToolsetSandbox, load_toolset_backend

    sandbox = ToolsetSandbox(load_toolset_backend("sbx"), workdir="/tmp/factory")
    with pytest.raises(base.SandboxTerminalError, match="host policy"):
        sandbox.ensure()
    assert sandbox.sandbox_id is None


@pytest.mark.parametrize("bad_field", ["url", "commit_id"])
def test_islo_provenance_rejects_wrong_provider(monkeypatch, bad_field):
    upstream_commit, provider_commit = "a" * 40, "b" * 40
    fork = "https://github.com/zozo123/airflow.git"

    class Distribution:
        version = "0.8.0"

        def __init__(self, name):
            self.name = name

        def read_text(self, name):
            is_provider = self.name == verify_module.PACKAGES[-1]
            source = {
                "url": fork if is_provider else "https://github.com/apache/airflow.git",
                "vcs_info": {"commit_id": provider_commit if is_provider else upstream_commit},
            }
            if is_provider:
                if bad_field == "url":
                    source["url"] = "https://github.com/wrong/airflow.git"
                else:
                    source["vcs_info"]["commit_id"] = "c" * 40
            return json.dumps(source)

    monkeypatch.setattr(verify_module.metadata, "distribution", Distribution)
    with pytest.raises(RuntimeError, match="apache-airflow-providers-common-ai"):
        verify_module.verify("--islo", upstream_commit, provider_repo=fork, provider_commit=provider_commit)


def test_islo_provenance_requires_resolved_provider():
    with pytest.raises(ValueError, match="resolved provider"):
        verify_module.verify("--islo", "a" * 40)
