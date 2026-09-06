//! Contracts and the pure roll-ups every interface renders.
//!
//! Nothing here performs I/O. That is the point: the same functions that decide what an operator
//! sees are the ones a fixture test can compare, value for value, against the Python
//! implementation they were ported from (`tests/fixtures/contract/`). A divergence in how a job's
//! state is rolled up is a divergence in what the factory *is*, so it has to be caught by a test
//! and not by an operator at 2 a.m.

pub mod blueprint;
pub mod cell;
pub mod control_plane;
pub mod doctor;
pub mod evidence;
pub mod ids;
pub mod metrics;
pub mod model;
pub mod operator;
pub mod rollup;
pub mod sanitize;
pub mod snapshot;
pub mod states;
pub mod worker;

pub use ids::{DeliveryId, GateId, IdError, JobId, RunRef};
pub use model::{
    Gate, IssueRef, JobRow, PullRequest, Run, SandboxRef, Snapshot, SourceHealth, TaskState,
};
pub use operator::{
    BackendCapabilities, ContractVersions, FleetSummary, LimitingDimension, OperationDebt,
    QueueEntry, QueuePressure, QueueSnapshot,
};
pub use worker::{WorkerBatch, WorkerReceipt, WorkerRole, MAX_WORKERS};
