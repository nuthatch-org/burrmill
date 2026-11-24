//! CSV and JSON files as tables (#389), read by `burrmill-ingest`, the browser tab's reader, so a
//! column is typed as the tab types it. The file's bytes are read once and hashed beside the parse;
//! CSV is parsed on the engine's threads into a batch every few MiB (#407). The table is held in
//! memory as Arrow, and that memory, with the file's bytes while it is read, comes out of the
//! engine's budget. A file that does not read cleanly is refused at its row and column:
//! a cell that does not fit its column's type is a refusal here, where the tab shows it as a null
//! on the report beside the table.

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use burrmill_ingest::{BudgetBreach, FormatHint, IngestError, Issue, Table};
use datafusion_catalog::{MemTable, TableProvider};
use sha2::{Digest, Sha256};

use crate::error::{BurrmillError, Result};

/// The most one JSON file may hold. A JSON text column is still one Arrow array with 32-bit
/// offsets, and at this size none can overflow it. CSV is built a batch at a time and has no cap.
pub(super) const MAX_JSON_BYTES: u64 = 1 << 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TextFormat {
    Csv,
    /// A top-level array of objects, or objects one after another (NDJSON).
    Json,
}

impl TextFormat {
    fn hint(self) -> FormatHint {
        match self {
            Self::Csv => FormatHint::Csv,
            Self::Json => FormatHint::Json,
        }
    }

    /// How a receipt names a file read this way.
    fn name(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
        }
    }
}

/// One file, read.
pub(super) struct TextTable {
    pub name: String,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    /// `<format>:<sha256>:<bytes>` of the file, as a receipt names a Parquet file by its content.
    pub input: String,
    /// Bytes of Arrow the table holds.
    pub held: u64,
}

/// The table `name` over the file at `path`, read in at most `memory` bytes where there is a bound,
/// on up to `threads` threads.
pub(super) fn read(
    path: &Path,
    name: &str,
    format: TextFormat,
    memory: Option<u64>,
    threads: usize,
) -> Result<TextTable> {
    let shown = path.display();
    let io = |e: std::io::Error| BurrmillError::Substrate(format!("{shown}: {e}"));
    let file = std::fs::File::open(path).map_err(io)?;
    let len = file.metadata().map_err(io)?.len();
    let memory = memory.unwrap_or(u64::MAX);
    let max_bytes = match format {
        TextFormat::Csv => memory,
        TextFormat::Json => memory.min(MAX_JSON_BYTES),
    };
    // The untrusted limits on a cell, a key, columns and nesting; the engine's memory for the rest,
    // which holds the file's bytes while it is read. Rows are bounded by the memory they take.
    let budget = burrmill_ingest::Budget::untrusted()
        .max_bytes(max_bytes)
        .max_rows(u64::MAX / 2)
        .max_memory_bytes(memory);
    let too_big = |at: u64| {
        refused(
            path,
            IngestError::Budget(BudgetBreach {
                which: "max_bytes",
                limit: max_bytes,
                at,
            }),
        )
    };
    if len > max_bytes {
        return Err(too_big(len));
    }
    let mut raw = Vec::with_capacity(len as usize);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut raw)
        .map_err(io)?;
    if raw.len() as u64 > max_bytes {
        return Err(too_big(raw.len() as u64));
    }
    // The hash on a thread of its own beside the parse, which then has one fewer.
    let threads = threads.max(1);
    let (sha, table) = std::thread::scope(|s| {
        let hasher = (threads > 1).then(|| s.spawn(|| Sha256::digest(&raw)));
        let parsers = if hasher.is_some() { threads - 1 } else { 1 };
        let table = burrmill_ingest::ingest_parallel(&raw, format.hint(), &budget, parsers);
        let sha = match hasher {
            Some(h) => h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)),
            None => Sha256::digest(&raw),
        };
        (sha, table)
    });
    let table = table.map_err(|e| refused(path, e))?;
    clean(path, &table)?;
    let batches = table.batches().to_vec();
    let Some(schema) = batches.first().map(|b| b.schema()) else {
        return Err(BurrmillError::Substrate(format!("{shown}: no columns")));
    };
    let held = batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum();
    let input = format!(
        "{}:{}:{}",
        format.name(),
        super::receipt::hex(sha),
        raw.len()
    );
    Ok(TextTable {
        name: name.to_string(),
        schema,
        batches,
        input,
        held,
    })
}

fn refused(path: &Path, e: IngestError) -> BurrmillError {
    let m = format!("{}: {e}", path.display());
    match e {
        IngestError::Budget(_) => BurrmillError::LimitExceeded(m),
        _ => BurrmillError::Substrate(m),
    }
}

/// Refused at the first row the reader flagged: a short row, a quote never closed, or a cell its
/// column's type does not read. Rows count from 1 after the header.
fn clean(path: &Path, table: &Table) -> Result<()> {
    let report = table.report();
    let Some(first) = report.rejected.iter().min_by_key(|i| i.row) else {
        return Ok(());
    };
    let column = first
        .column
        .as_ref()
        .and_then(|c| report.columns.iter().position(|r| &r.name == c));
    let at = match (column, &first.column) {
        (Some(i), Some(name)) => format!("row {}, column {} ({name})", first.row, i + 1),
        _ => format!("row {}", first.row),
    };
    let why = match (&first.reason, column) {
        (Issue::Ragged { expected, got }, _) => {
            format!("{got} fields where the widest row has {expected}")
        }
        (Issue::UnclosedQuote, _) => "a quote that is not closed".to_string(),
        (Issue::TypeMismatch, Some(i)) => format!(
            "not a {}, which the rest of the column is",
            report.columns[i].inferred.name()
        ),
        (Issue::TypeMismatch, None) => "not of its column's type".to_string(),
    };
    let more = report.rejected.len() as u64 + report.rejected_omitted - 1;
    let more = match more {
        0 => String::new(),
        n => format!(" ({n} more)"),
    };
    Err(BurrmillError::Substrate(format!(
        "{}: refused at {at}: {why}{more}",
        path.display()
    )))
}

impl super::Engine {
    /// `tables` beside the ones the engine opened with, each as one in-memory table.
    pub(super) fn register_text(&mut self, tables: Vec<TextTable>) -> Result<()> {
        if tables.is_empty() {
            return Ok(());
        }
        for t in tables {
            let provider: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(t.schema, vec![t.batches]).map_err(super::df_err)?);
            self.session.register_table(&t.name, Arc::clone(&provider));
            self.text_inputs.push((provider, t.input));
        }
        self.session
            .build_information_schema(|n| n.ends_with("__raw"))
            .map_err(super::df_err)
    }
}
