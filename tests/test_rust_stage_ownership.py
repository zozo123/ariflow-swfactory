from pathlib import Path

import yaml


def test_rust_stage_ownership_manifest_has_one_migration_direction() -> None:
    root = Path(__file__).resolve().parents[1]
    policy = yaml.safe_load((root / "config" / "rust-stage-ownership.yaml").read_text(encoding="utf-8"))

    assert policy["scheduler"] == "airflow"
    assert policy["harness_entrypoint"] == "rust-swf"
    assert policy["target_runtime"] == "rust"
    assert policy["rules"]["rust_is_terminal_owner"] is True
    assert policy["rules"]["no_python_fallback_after_rust"] is True
    assert policy["rules"]["python_business_logic_target"] == "zero"

    allowed = {"python-orchestration", "python-migrating", "rust"}
    assert set(policy["stages"].values()) <= allowed
