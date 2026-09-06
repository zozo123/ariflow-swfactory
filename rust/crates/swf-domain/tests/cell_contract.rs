use std::path::PathBuf;

use serde::Deserialize;
use swf_domain::cell::{CellEvent, CellRecord};

#[derive(Debug, Deserialize)]
struct Golden {
    cell: CellRecord,
    history: Vec<CellEvent>,
}

fn fixture() -> Golden {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/fixtures/cells/factory_cell_v1.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|err| panic!("invalid Factory Cell fixture {}: {err}", path.display()))
}

#[test]
fn rust_reads_the_python_backend_cell_v1_wire_shape() {
    let golden = fixture();
    assert_eq!(golden.cell.schema_version, 1);
    assert_eq!(golden.cell.cell_id, "cell_0123456789abcdef01234567");
    assert_eq!(golden.cell.epoch, 3);
    assert_eq!(
        golden.cell.airflow_identity().as_deref(),
        Some("factory/manual__2026-09-06T20:30:00+00:00#7")
    );
    assert_eq!(golden.history.len(), 2);
    assert_eq!(golden.history[0].operation_key, "activation:3");
}

#[test]
fn the_golden_document_round_trips_without_losing_fields() {
    let golden = fixture();
    let cell = serde_json::to_value(&golden.cell).expect("serialize cell");
    assert_eq!(cell["factory_generation"], "gen_abc123");
    assert_eq!(cell["compute"]["provider"], "islo");

    let history = serde_json::to_value(&golden.history).expect("serialize history");
    assert_eq!(history[1]["payload"]["state"], "queued");
    assert_eq!(history[1]["epoch"], 3);
}
