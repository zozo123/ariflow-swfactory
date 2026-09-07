#!/usr/bin/env python3
from pathlib import Path

p = Path("rust/crates/swf-app/src/gates.rs")
text = p.read_text()

old = "pub const CONFIRM_INTERVAL: Duration = Duration::from_secs(3);"
new = "pub const CONFIRM_INTERVAL: Duration = Duration::from_secs(5);"
if old in text:
    text = text.replace(old, new, 1)
elif new not in text:
    raise SystemExit("CONFIRM_INTERVAL snippet not found")

old = """/// seconds is the interval `scripts/stress_airflow.sh` — the reference harness that does not
/// produce this failure — leaves between the poll that first sees a gate parked and the poll that
/// answers it. It costs a scripted approval one pause; it costs an interactive one nothing at all,
"""
new = """/// seconds was sufficient locally, but the live hosted-runner harness still reproduced a stale
/// executor event after three seconds. Five seconds is the conservative default now. It costs a
/// scripted approval one pause; it costs an interactive one nothing at all,
"""
if old in text:
    text = text.replace(old, new, 1)
elif new not in text:
    raise SystemExit("settle comment snippet not found")

old = """    let mut running: JoinSet<(usize, BatchItem)> = JoinSet::new();
"""
new = """    let mut running: JoinSet<(usize, BatchItem)> = JoinSet::new();
    // A 200 from one HITL PATCH means the API accepted the response; it does not mean the
    // scheduler has finished reconciling that task before the next mapped gate in the same run is
    // re-queued. Keep the per-run lock briefly after a successful write. Test callers that pass a
    // zero/short settle override inherit a zero/short cooldown, so deterministic unit tests stay
    // fast while the product default protects hosted schedulers.
    let run_write_cooldown = settle
        .unwrap_or(CONFIRM_INTERVAL)
        .min(Duration::from_secs(1));
"""
if old in text:
    text = text.replace(old, new, 1)
elif new not in text:
    raise SystemExit("running snippet not found")

old = """                let outcome = answer(
                    runs.as_ref(),
                    sightings.as_ref(),
                    &id,
                    decision,
                    &opts,
                    &cancel,
                )
                .await;
                (index, item_of(id, gate, issue, outcome))
"""
new = """                let outcome = answer(
                    runs.as_ref(),
                    sightings.as_ref(),
                    &id,
                    decision,
                    &opts,
                    &cancel,
                )
                .await;
                if outcome.is_ok() && !run_write_cooldown.is_zero() {
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => {}
                        () = tokio::time::sleep(run_write_cooldown) => {}
                    }
                }
                (index, item_of(id, gate, issue, outcome))
"""
if old in text:
    text = text.replace(old, new, 1)
elif new not in text:
    raise SystemExit("answer task snippet not found")

p.write_text(text)
print("live gate settle fix applied")
