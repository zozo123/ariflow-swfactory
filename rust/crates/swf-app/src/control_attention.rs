//! Durable control-plane debt projected into actionable operator attention.
//!
//! This projection contains no scheduling or mutation logic. It consumes the backend-owned queue,
//! operation journal and fleet summary so CLI/TUI renderers can show the same repair debt without
//! inventing another state machine.

use serde::Serialize;
use swf_domain::operator::{FleetSummary, OperationDebt, QueueSnapshot};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ControlAttentionItem {
    pub kind: String,
    pub id: String,
    pub state: String,
    pub reason: String,
    pub cell_id: Option<String>,
    pub epoch: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ControlAttention {
    pub items: Vec<ControlAttentionItem>,
}

impl ControlAttention {
    pub fn from_parts(
        queue: &QueueSnapshot,
        operations: &[OperationDebt],
        fleet: &FleetSummary,
    ) -> Self {
        let mut items = Vec::new();

        for row in &queue.queued {
            items.push(ControlAttentionItem {
                kind: "queued".into(),
                id: row.work_id.clone(),
                state: row.state.clone(),
                reason: row.reason.clone(),
                cell_id: row.cell_id.clone(),
                epoch: row.cell_epoch,
            });
        }

        for row in operations.iter().filter(|row| {
            !matches!(
                row.state.as_str(),
                "committed" | "definitely_absent" | "settled"
            )
        }) {
            items.push(ControlAttentionItem {
                kind: "operation_debt".into(),
                id: row.operation_key.clone(),
                state: row.state.clone(),
                reason: row
                    .last_error
                    .clone()
                    .unwrap_or_else(|| format!("{} requires reconciliation", row.kind)),
                cell_id: Some(row.cell_id.clone()),
                epoch: Some(row.epoch),
            });
        }

        if fleet.cleanup_debt > 0 {
            items.push(ControlAttentionItem {
                kind: "cleanup_debt".into(),
                id: "fleet/cleanup".into(),
                state: "attention".into(),
                reason: format!("{} cleanup receipts remain unsettled", fleet.cleanup_debt),
                cell_id: None,
                epoch: None,
            });
        }
        if fleet.cells_orphaned > 0 {
            items.push(ControlAttentionItem {
                kind: "orphaned_cells".into(),
                id: "fleet/orphans".into(),
                state: "attention".into(),
                reason: format!("{} Factory Cells are orphaned", fleet.cells_orphaned),
                cell_id: None,
                epoch: None,
            });
        }
        if fleet.cells_stale > 0 {
            items.push(ControlAttentionItem {
                kind: "stale_cells".into(),
                id: "fleet/stale".into(),
                state: "attention".into(),
                reason: format!("{} Factory Cells have stale authority", fleet.cells_stale),
                cell_id: None,
                epoch: None,
            });
        }

        items.sort_by(|left, right| (&left.kind, &left.id).cmp(&(&right.kind, &right.id)));
        Self { items }
    }

    pub fn is_clear(&self) -> bool {
        self.items.is_empty()
    }

    pub fn count(&self) -> usize {
        self.items.len()
    }
}
