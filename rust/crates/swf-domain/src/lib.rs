//! Contracts and the pure roll-ups every interface renders.
//!
//! Nothing here performs I/O. That is the point: the same functions that decide what an operator
//! sees are the ones a fixture test can compare, value for value, against the Python
//! implementation they were ported from (`tests/fixtures/contract/`). A divergence in how a job's
//! state is rolled up is a divergence in what the factory *is*, so it has to be caught by a test
//! and not by an operator at 2 a.m.

pub mod advisory;
pub mod blueprint;
pub mod build_exploration;
pub mod cell;
pub mod cognitive_harness;
pub mod control_plane;
pub mod doctor;
pub mod evidence;
pub mod factory;
pub mod ids;
pub mod manager_protocol;
pub mod metrics;
pub mod model;
pub mod operator;
pub mod phase_control;
pub mod policy;
pub mod rollup;
pub mod sanitize;
pub mod snapshot;
pub mod states;
pub mod worker;

pub use cognitive_harness::{
    classify_self_improvement, ensemble_diversity, gauge_fix, marginal_information_value,
    memory_phase, pairwise_correlation, plan_cognition, quantity_kind, validate_measurement_set,
    AuthorityRequest, CognitiveAttention, CognitiveError, CognitiveLayer, CognitivePlan,
    CognitiveReceipt, CognitiveTimescale, DissipationSnapshot, EnsembleMember,
    GaugeEquivalenceClass, ImprovementDisposition, MeasurementKind, MeasurementReceipt,
    MemoryAction, MemoryEvidence, MemoryPhase, PairwiseCorrelation, QuantityKind,
    SelfImprovementExperiment, StochasticField, WorldAction, WorldCandidate,
    COGNITIVE_HARNESS_AUTHORITY, COGNITIVE_HARNESS_SCHEMA_VERSION, REALITY_BOUNDARY,
};
pub use factory::{
    FactoryError, FactoryName, FactoryRunId, FactoryRunRequest, FactoryRunState, FactoryRunStatus,
    FactorySpec, SchedulerBinding,
};
pub use ids::{DeliveryId, GateId, IdError, JobId, RunRef};
pub use manager_protocol::{
    AirflowInvocation, ManagerEnvelope, ProtocolError, StageDisposition, StageInvocation,
    StageReceipt, MANAGER_API_VERSION,
};
pub use model::{
    Gate, IssueRef, JobRow, PullRequest, Run, SandboxRef, Snapshot, SourceHealth, TaskState,
};
pub use operator::{
    BackendCapabilities, ContractVersions, FleetSummary, LimitingDimension, OperationDebt,
    QueueEntry, QueuePressure, QueueSnapshot,
};
pub use phase_control::{
    AttentionClass, CandidateDirective, ContextDirective, ControlMode, FactoryPhase,
    PhaseAssessment, PhaseObservation, PhaseRecommendation, PhaseSignals, QueueDirective,
    SpawnDirective, TrajectoryMode, VerificationDirective, PHASE_CONTROL_AUTHORITY,
    PHASE_CONTROL_SCHEMA_VERSION,
};
pub use worker::{WorkerBatch, WorkerReceipt, WorkerRole, MAX_WORKERS};
