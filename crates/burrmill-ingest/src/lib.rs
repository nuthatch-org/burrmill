//! `burrmill-ingest`: untrusted CSV in, Arrow out, under counted budgets.
//!
//! RFC-0047 slice 1. The engine is not a dependency. Parquet, JSON and the wasm package are
//! later slices. The report describes the file and does not contain cell values; [`Table::preview`]
//! is how a local UI reads rows.

mod budget;
mod csv;
mod encoding;
mod error;
mod infer;
mod report;
mod sniff;
mod source;

// Arrow's ahash pulls getrandom 0.2 and chrono pulls 0.3. Both refuse to compile
// for wasm32-unknown-unknown until the JS backend is selected on the direct edge.
#[cfg(target_arch = "wasm32")]
use getrandom as _;
#[cfg(target_arch = "wasm32")]
use getrandom02 as _;

use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Int64Type, TimeUnit, TimestampMicrosecondType,
};

pub use budget::{Budget, BudgetUse};
pub use error::{BudgetBreach, IngestError, Result};
pub use report::{ColumnReport, InferredType, IngestReport, Issue, MAX_ISSUES, RowIssue};
pub use sniff::{Dialect, EncodingKind, Locale};
pub use source::{ByteSource, ReadSource, SliceSource};

/// What the caller claims the bytes are. [`FormatHint::Sniff`] refuses Parquet, zip and gzip
/// magic instead of parsing them as text. [`FormatHint::Csv`] parses anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatHint {
    Sniff,
    Csv,
}

/// Local table. `Debug` prints the report only, so logging a table cannot leak a cell.
pub struct Table {
    batches: Vec<RecordBatch>,
    report: IngestReport,
}

impl Table {
    pub fn report(&self) -> &IngestReport {
        &self.report
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Up to `n` data rows, for a local UI. This is the call that returns values.
    pub fn preview(&self, n: usize) -> Vec<Vec<Option<String>>> {
        let mut out = Vec::new();
        for batch in &self.batches {
            for row in 0..batch.num_rows() {
                if out.len() == n {
                    return out;
                }
                let mut cells = Vec::with_capacity(batch.num_columns());
                for col in batch.columns() {
                    cells.push(cell_string(col, row));
                }
                out.push(cells);
            }
        }
        out
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("rows", &self.report.rows)
            .field("columns", &self.report.columns.len())
            .field("report", &self.report)
            .finish()
    }
}

pub fn ingest_bytes(data: &[u8], hint: FormatHint, budget: &Budget) -> Result<Table> {
    ingest(&mut SliceSource::new(data), hint, budget)
}

pub fn ingest(source: &mut dyn ByteSource, hint: FormatHint, budget: &Budget) -> Result<Table> {
    if let Some(hint_len) = source.len_hint() {
        budget.charge("max_bytes", budget.max_bytes, hint_len)?;
    }
    // `Instant` panics on wasm32-unknown-unknown. A caller who set a clock budget
    // gets a budget error rather than a trap.
    #[cfg(target_arch = "wasm32")]
    if let Some(limit) = budget.max_millis {
        return Err(budget.breach("clock", limit, 0));
    }

    #[cfg(not(target_arch = "wasm32"))]
    let started = std::time::Instant::now();
    #[cfg(not(target_arch = "wasm32"))]
    let check_clock = |budget: &Budget| -> Result<()> {
        if let Some(limit) = budget.max_millis {
            let at = started.elapsed().as_millis() as u64;
            budget.charge("max_millis", limit, at)?;
        }
        Ok(())
    };
    #[cfg(target_arch = "wasm32")]
    let check_clock = |_budget: &Budget| -> Result<()> { Ok(()) };

    let mut raw = Vec::new();
    while let Some(chunk) = source.next_chunk()? {
        let at = raw.len() as u64 + chunk.len() as u64;
        budget.charge("max_bytes", budget.max_bytes, at)?;
        raw.extend_from_slice(&chunk);
        check_clock(budget)?;
    }
    budget.charge(
        "max_memory_bytes",
        budget.max_memory_bytes,
        raw.len() as u64,
    )?;

    if hint == FormatHint::Sniff
        && let Some(format) = sniff::refused_magic(&raw)
    {
        return Err(IngestError::Unsupported { format });
    }

    let sample_end = (budget.sniff_bytes as usize).min(raw.len());
    let encoding = sniff::sniff_encoding(&raw[..sample_end]);
    let text = sniff::decode(&raw, encoding);
    let bytes_read = raw.len() as u64;
    drop(raw);
    budget.charge(
        "max_memory_bytes",
        budget.max_memory_bytes,
        text.len() as u64,
    )?;

    let dialect = sniff::sniff_dialect(&text, encoding);
    let parsed = csv::parse_csv(&text, &dialect, budget, text.len() as u64, || {
        check_clock(budget)
    })?;
    drop(text);
    check_clock(budget)?;

    let data_rows_with_possible_header = parsed.rows.len() as u64;
    if data_rows_with_possible_header > budget.max_rows + 1 {
        return Err(budget.breach("max_rows", budget.max_rows, data_rows_with_possible_header));
    }

    let (batches, report) = infer::build(parsed, dialect, bytes_read);
    if report.rows > budget.max_rows {
        return Err(budget.breach("max_rows", budget.max_rows, report.rows));
    }
    Ok(Table { batches, report })
}

fn cell_string(col: &Arc<dyn Array>, row: usize) -> Option<String> {
    if col.is_null(row) {
        return None;
    }
    Some(match col.data_type() {
        DataType::Int64 => col.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::Boolean => col.as_boolean().value(row).to_string(),
        DataType::Utf8 => col.as_string::<i32>().value(row).to_string(),
        DataType::Date32 => format_date(col.as_primitive::<Date32Type>().value(row)),
        DataType::Decimal128(_, scale) => format_decimal(
            col.as_primitive::<Decimal128Type>().value(row),
            *scale as u8,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => col
            .as_primitive::<TimestampMicrosecondType>()
            .value(row)
            .to_string(),
        other => format!("{other:?}"),
    })
}

fn format_decimal(unscaled: i128, scale: u8) -> String {
    let neg = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let body = if scale == 0 {
        digits
    } else {
        let scale = scale as usize;
        if digits.len() <= scale {
            format!("0.{}{digits}", "0".repeat(scale - digits.len()))
        } else {
            let split = digits.len() - scale;
            format!("{}.{}", &digits[..split], &digits[split..])
        }
    };
    if neg { format!("-{body}") } else { body }
}

fn format_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

fn civil_from_days(z: i32) -> (i32, u32, u32) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
