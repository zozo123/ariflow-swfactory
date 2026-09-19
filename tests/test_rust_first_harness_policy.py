from pathlib import Path

import yaml


def test_rust_first_harness_policy_keeps_airflow_as_scheduler() -> None:
    root = Path(__file__).resolve().parents[1]
    policy = yaml.safe_load((root / "config" / "rust-first-harness.yaml").read_text(encoding="utf-8"))

    assert policy["harness_entrypoint"] == "rust-swf-cli"
    assert policy["factory_runtime"] == "rust"
    assert policy["lifecycle_scheduler"] == "apache-airflow"
    assert policy["binding"]["protocol"] == "swf-domain::manager_protocol"
    assert policy["binding"]["airflow_metadata_db_access"] == "forbidden"
    assert policy["invariants"]["single_scheduler"] is True
    assert policy["invariants"]["single_factory_authority"] is True
    assert policy["invariants"]["in_doubt_never_auto_retried_by_airflow_shim"] is True
