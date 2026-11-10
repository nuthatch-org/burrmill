//! Counted limits. Wall time is optional and never consulted on wasm32, where `Instant` panics.

use crate::error::{BudgetBreach, IngestError, Result};

/// Defaults are [`Budget::untrusted`]: the limits for a file a stranger uploaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Budget {
    pub max_bytes: u64,
    pub max_rows: u64,
    pub max_columns: u64,
    pub max_cell_bytes: u64,
    pub max_memory_bytes: u64,
    pub sniff_bytes: u64,
    /// Off unless set. Ignored on wasm32; a set value there is a [`IngestError::Budget`] on `clock`.
    pub max_millis: Option<u64>,
}

impl Default for Budget {
    fn default() -> Self {
        Self::untrusted()
    }
}

impl Budget {
    pub fn untrusted() -> Self {
        Self {
            max_bytes: 200 << 20,
            max_rows: 5_000_000,
            max_columns: 1_024,
            max_cell_bytes: 1 << 20,
            max_memory_bytes: 512 << 20,
            sniff_bytes: 64 << 10,
            max_millis: None,
        }
    }

    pub fn max_bytes(mut self, n: u64) -> Self {
        self.max_bytes = n;
        self
    }

    pub fn max_rows(mut self, n: u64) -> Self {
        self.max_rows = n;
        self
    }

    pub fn max_columns(mut self, n: u64) -> Self {
        self.max_columns = n;
        self
    }

    pub fn max_cell_bytes(mut self, n: u64) -> Self {
        self.max_cell_bytes = n;
        self
    }

    pub fn max_memory_bytes(mut self, n: u64) -> Self {
        self.max_memory_bytes = n;
        self
    }

    pub fn sniff_bytes(mut self, n: u64) -> Self {
        self.sniff_bytes = n;
        self
    }

    pub fn max_millis(mut self, n: u64) -> Self {
        self.max_millis = Some(n);
        self
    }

    pub(crate) fn breach(&self, which: &'static str, limit: u64, at: u64) -> IngestError {
        IngestError::Budget(BudgetBreach { which, limit, at })
    }

    pub(crate) fn charge(&self, which: &'static str, limit: u64, at: u64) -> Result<()> {
        if at > limit {
            Err(self.breach(which, limit, at))
        } else {
            Ok(())
        }
    }
}

/// How far a successful ingest got. Counts, not values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BudgetUse {
    pub bytes: u64,
    pub rows: u64,
    pub columns: u64,
    pub memory_bytes: u64,
}
