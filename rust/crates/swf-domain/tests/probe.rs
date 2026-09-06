use serde_json::json;
use swf_domain::model::Snapshot;
use swf_domain::snapshot;
use chrono::Utc;

#[test]
fn probe() {
    for (label, doc) in [
        ("runs: string",      json!({"runs": "oops"})),
        ("runs: object",      json!({"runs": {}})),
        ("runs: number",      json!({"runs": 3})),
        ("gates: string",     json!({"gates": "oops"})),
        ("conf via run",      json!({"runs": [{"dag_id":"d","run_id":"r","state":"s","conf":[]}]})),
        ("errors: array",     json!({"errors": []})),
        ("errors: string",    json!({"errors": "boom"})),
        ("errors: number",    json!({"errors": 1})),
        ("errors: map ok",    json!({"errors": {"gh":"boom"}})),
        ("metrics: number",   json!({"metrics": 42})),
        ("metrics: string",   json!({"metrics": "oops"})),
        ("metrics: array",    json!({"metrics": [1,2]})),
        ("prs: [null]",       json!({"prs": [null]})),
        ("collected_at: obj", json!({"collected_at": {"a":1}})),
        ("collected_at: 123", json!({"collected_at": 1756900800})),
        ("collected_at: bad", json!({"collected_at": "not-a-date"})),
    ] {
        match serde_json::from_value::<Snapshot>(doc) {
            Ok(s) => {
                let out = snapshot::snapshot_json(&s, Utc::now());
                println!("PROBE {label:22} => OK   metrics={} collected_at={}",
                    out.get("metrics").unwrap(), out.get("collected_at").unwrap());
            }
            Err(e) => println!("PROBE {label:22} => ERR  {e}"),
        }
    }
}
