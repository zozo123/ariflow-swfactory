//! Service adapters: the only code in the workspace that talks to something outside the process.
//!
//! Every source sits behind a trait so the operations layer never sees a transport, and so a test
//! can drive the whole product without a network. Airflow is reached through its public REST API
//! (`/api/v2`) — never its metadata database — because the API is the contract Airflow keeps and
//! the database is not.

pub mod airflow;
pub mod error;
pub mod factory;
pub mod gh;
pub mod islo;
pub mod metrics_store;
pub mod traits;

pub use error::{AdapterError, Result};
pub use traits::{Deliveries, LogPage, MetricsStore, Runs, Sandboxes};
