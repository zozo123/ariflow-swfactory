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


def verify(
    mode: str, commit: str, *, provider_repo: str | None = None, provider_commit: str | None = None
) -> None:
    if mode == "--islo" and (not provider_repo or not provider_commit):
        raise ValueError("--islo requires the resolved provider repository and commit")
    for name in PACKAGES:
        dist = metadata.distribution(name)
        source = json.loads(dist.read_text("direct_url.json") or "{}")
        actual = source.get("vcs_info", {}).get("commit_id")
        expected_repo, expected_commit = "https://github.com/apache/airflow.git", commit
        if name == PACKAGES[-1] and mode == "--islo":
            expected_repo, expected_commit = provider_repo, provider_commit
        if (name != PACKAGES[-1] or mode != "--pypi") and (
            actual != expected_commit or source.get("url") != expected_repo
        ):
            raise RuntimeError(
                f"{name}: expected apache/airflow or selected provider "
                f"{expected_repo}@{expected_commit}, got {source}"
            )
        print(f"{name} {dist.version}: {actual or 'release wheel'}")

    from airflow.providers.common.ai.sandbox.base import SandboxBackend, SandboxSpec
    from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset

    from swfactory.sandbox import load_toolset_backend

    backend = load_toolset_backend("islo" if mode == "--islo" else "sbx")
    if not isinstance(backend, SandboxBackend):
        raise TypeError(f"{type(backend).__name__} is not an upstream SandboxBackend")
    print(f"sandbox: {type(backend).__name__}, {SandboxToolset.__name__}, {SandboxSpec()}")


if __name__ == "__main__":
    verify(
        sys.argv[1],
        os.environ["AIRFLOW_COMMIT"],
        provider_repo=os.environ.get("AI_PROVIDER_REPO"),
        provider_commit=os.environ.get("AI_PROVIDER_COMMIT"),
    )
