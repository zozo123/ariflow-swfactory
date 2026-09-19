from pathlib import Path


def test_rust_first_airflow_adr_keeps_one_scheduler_and_one_runtime_target() -> None:
    root = Path(__file__).resolve().parents[1]
    text = (root / "docs" / "adr-rust-first-airflow-runtime.md").read_text(encoding="utf-8")

    assert "Harness entrypoint | Rust" in text
    assert "Lifecycle scheduling | Airflow" in text
    assert "Airflow Rust coordinator" in text
    assert "delete Python product runtime" in text
    assert "never reads the metadata DB" in text
    assert "Airflow schedules. Rust reasons and executes." in text
