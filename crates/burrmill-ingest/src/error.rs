//! Failures a caller can show. None of them carries a cell value.

use std::fmt;

/// A budget was crossed. `at` is how far the reader had got, in the same unit as `limit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetBreach {
    pub which: &'static str,
    pub limit: u64,
    pub at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestError {
    Budget(BudgetBreach),
    /// Sniff recognised a container this slice does not open. The bytes are not parsed.
    Unsupported {
        format: &'static str,
    },
    /// The byte source failed. The message is the source's, not a cell.
    Source(String),
}

pub type Result<T> = std::result::Result<T, IngestError>;

impl fmt::Display for IngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Budget(b) => {
                write!(
                    f,
                    "budget {} exceeded: limit {}, at {}",
                    b.which, b.limit, b.at
                )
            }
            Self::Unsupported { format } => {
                write!(f, "unsupported format: {format}")
            }
            Self::Source(m) => write!(f, "source: {m}"),
        }
    }
}

impl std::error::Error for IngestError {}
