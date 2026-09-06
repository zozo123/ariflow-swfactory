//! The interactive factory: one screen that answers what needs attention, what is moving, and
//! what can safely be done next.
//!
//! Model / update / view with every side effect pushed onto an async task that reports back as a
//! message. Rendering never awaits and never dials a service, so a dead Airflow slows nothing down
//! but the pane that depends on it — an unreliable connection is a normal operating condition
//! here, not an error path.

pub mod app;
pub mod effect;
pub mod event;
pub mod model;
pub mod theme;
pub mod view;

pub use app::run;
