//! The operations both interfaces expose.
//!
//! The CLI and the TUI are two renderings of this one layer: if a command can do it, a keystroke
//! can, and both take exactly the same validation path. Keeping the decisions here — not in the
//! argument parser and not in the widget — is what stops the two faces of the product from
//! drifting apart while the factory underneath keeps moving.

pub mod attention;
pub mod cells;
pub mod context;
pub mod control_plane;
pub mod delivery;
pub mod doctor;
pub mod gates;
pub mod logs;
pub mod operator;
pub mod ops;
pub mod snapshot;
pub mod stack;
pub mod submit;

pub use context::{Auth, Context, ContextStore};
pub use operator::OperatorOps;
pub use ops::{Ops, OpsError};
