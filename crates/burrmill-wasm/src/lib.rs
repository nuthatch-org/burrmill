//! Schema mode, aggregates mode, and pseudonymised mode for a browser tab.
//! Ingest, the engine, and the checks on a model's answer. The page does the HTTP.

mod aggregates;
mod chart;
mod format;
mod model;
mod narrate;
mod pseudonym;
mod redact;
mod session;

#[cfg(target_arch = "wasm32")]
mod wasm;

pub use chart::{Chart, ChartKind, check_chart};
pub use model::parse_model;
pub use narrate::narrate;
pub use redact::redact;
pub use session::{
    Column, PresentOut, QueryOut, RESULT_ROWS, Session, feedback_error, feedback_shape, present,
};
