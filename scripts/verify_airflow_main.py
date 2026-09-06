"""Fail if a resolver replaced any requested upstream package with a release wheel."""

from __future__ import annotations

import importlib.metadata as metadata
import json
import os
import sys

PACKAGES = (
    "apache-airflow",
    "apache-airflow-core",
    "apache-airflow-task-sdk",
    "apache-airflow-providers-standard",
    "apache-airflow-providers-common-ai",
)


def verify(mode: str, commit: str) -> None:
    for name in PACKAGES:
        dist = metadata.distribution(name)
        source = json.loads(dist.read_text("direct_url.json") or "{}")
        actual = source.get("vcs_info", {}).get("commit_id")
        if (name != PACKAGES[-1] or mode == "--upstream") and (
            actual != commit or source.get("url") != "https://github.com/apache/airflow.git"
        ):
            raise RuntimeError(f"{name}: expected apache/airflow@{commit}, got {source}")
        print(f"{name} {dist.version}: {actual or 'release wheel'}")

    from airflow.providers.common.ai.sandbox.base import SandboxBackend, SandboxSpec
    from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset

    from swfactory.sandbox import load_toolset_backend

    backend = load_toolset_backend("islo" if mode == "--islo" else "sbx")
    if not isinstance(backend, SandboxBackend):
        raise TypeError(f"{type(backend).__name__} is not an upstream SandboxBackend")
    print(f"sandbox: {type(backend).__name__}, {SandboxToolset.__name__}, {SandboxSpec()}")


if __name__ == "__main__":
    verify(sys.argv[1], os.environ["AIRFLOW_COMMIT"])
