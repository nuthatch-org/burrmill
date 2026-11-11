//! Parquet under the budgets CSV reads under. Its columns arrive typed, so nothing is inferred:
//! each becomes one of the types a CSV column can have, or text where none holds every value.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringArray, StringBuilder};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{
    DataType, Field, Schema, TimeUnit, TimestampMicrosecondType, TimestampNanosecondType,
    UInt64Type,
};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;

use crate::budget::{Budget, BudgetUse};
use crate::error::{IngestError, Result};
use crate::report::{ColumnReport, Format, InferredType, IngestReport};
use crate::sniff::{Dialect, EncodingKind, Locale};

const BATCH_ROWS: usize = 8192;

const STRICT: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: FormatOptions::new(),
};

pub(crate) fn read(
    raw: Vec<u8>,
    budget: &Budget,
    check_clock: &dyn Fn(&Budget) -> Result<()>,
) -> Result<(Vec<RecordBatch>, IngestReport)> {
    let bytes_read = raw.len() as u64;
    let file = Bytes::from(raw);
    let builder = ParquetRecordBatchReaderBuilder::try_new(file.clone()).map_err(invalid)?;
    let meta = builder.metadata().clone();

    let schema = Arc::clone(builder.schema());
    let width = schema.fields().len() as u64;
    budget.charge("max_columns", budget.max_columns, width)?;
    // A flat column decodes a value a row, and rows are counted. A repeated one decodes as many
    // as its levels say, which neither the footer nor a page header has to admit to.
    let schema_descr = meta.file_metadata().schema_descr();
    if schema_descr.columns().iter().any(|c| c.max_rep_level() > 0) {
        return Err(IngestError::Unsupported {
            format: "parquet with a list or map column",
        });
    }
    let mut declared_rows = meta.file_metadata().num_rows().max(0) as u64;
    for rg in meta.row_groups() {
        declared_rows = declared_rows.max(rg.num_rows().max(0) as u64);
    }
    let row_sum: u64 = meta
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows().max(0) as u64)
        .sum();
    budget.charge("max_rows", budget.max_rows, declared_rows.max(row_sum))?;
    // Each page header says how big the page decodes to, and the decoder allocates that before
    // it reads the page. The footer's sizes are counted too, but a page need not agree with them.
    let (mut declared_bytes, mut paged_bytes) = (0u64, 0u64);
    for c in meta.row_groups().iter().flat_map(|rg| rg.columns()) {
        if let Some(format) = unread_codec(c.compression()) {
            return Err(IngestError::Unsupported { format });
        }
        declared_bytes = declared_bytes.saturating_add(c.uncompressed_size().max(0) as u64);
        let start = c.dictionary_page_offset().unwrap_or(c.data_page_offset());
        let pages = crate::pages::decoded_size(&file, start, c.compressed_size())
            .ok_or(IngestError::Invalid { format: "parquet" })?;
        paged_bytes = paged_bytes.saturating_add(pages);
        budget.charge(
            "max_memory_bytes",
            budget.max_memory_bytes,
            bytes_read.saturating_add(declared_bytes.max(paged_bytes)),
        )?;
        check_clock(budget)?;
    }
    drop(file);
    let mut memory = bytes_read;
    let mut peak = memory;

    let reader = builder
        .with_batch_size(BATCH_ROWS)
        .build()
        .map_err(invalid)?;
    let mut decoded = Vec::new();
    let mut rows = 0u64;
    for batch in reader {
        let batch = batch.map_err(invalid)?;
        rows += batch.num_rows() as u64;
        budget.charge("max_rows", budget.max_rows, rows)?;
        memory += batch.get_array_memory_size() as u64;
        budget.charge("max_memory_bytes", budget.max_memory_bytes, memory)?;
        peak = peak.max(memory);
        check_clock(budget)?;
        decoded.push(batch);
    }
    // The file's own bytes are released with the reader.
    memory -= bytes_read;

    let names = crate::infer::unique_names(schema.fields().iter().map(|f| f.name().as_str()));
    let mut columns: Vec<Vec<ArrayRef>> = vec![Vec::with_capacity(decoded.len()); names.len()];
    for batch in decoded {
        for (i, c) in batch.columns().iter().enumerate() {
            columns[i].push(Arc::clone(c));
        }
    }

    let mut fields = Vec::with_capacity(names.len());
    let mut built: Vec<Vec<ArrayRef>> = Vec::with_capacity(names.len());
    let mut reports = Vec::with_capacity(names.len());
    for (name, parts) in names.into_iter().zip(columns) {
        let before: u64 = parts.iter().map(|a| a.get_array_memory_size() as u64).sum();
        let (parts, inferred, note) = convert(parts)?;
        let after: u64 = parts.iter().map(|a| a.get_array_memory_size() as u64).sum();
        // The decoded column is held until its replacement is whole.
        budget.charge("max_memory_bytes", budget.max_memory_bytes, memory + after)?;
        peak = peak.max(memory + after);
        memory = memory - before + after;
        if let Some(longest) = longest_text(&parts) {
            budget.charge("max_cell_bytes", budget.max_cell_bytes, longest)?;
        }
        check_clock(budget)?;
        let nulls: u64 = parts.iter().map(|a| a.null_count() as u64).sum();
        let non_empty = rows - nulls;
        fields.push(Field::new(
            &name,
            parts
                .first()
                .map_or(DataType::Utf8, |a| a.data_type().clone()),
            true,
        ));
        reports.push(ColumnReport {
            name,
            inferred,
            nulls,
            non_empty,
            matched: non_empty,
            note,
        });
        built.push(parts);
    }

    let schema = Arc::new(Schema::new(fields));
    let groups = built.first().map_or(0, |c| c.len());
    let mut batches = Vec::with_capacity(groups);
    for g in 0..groups {
        let cols: Vec<ArrayRef> = built.iter().map(|c| Arc::clone(&c[g])).collect();
        batches.push(RecordBatch::try_new(Arc::clone(&schema), cols).map_err(invalid)?);
    }

    let report = IngestReport {
        format: Format::Parquet,
        dialect: Dialect {
            delimiter: b',',
            quote: b'"',
            encoding: EncodingKind::Utf8,
            locale: Locale::Point,
        },
        header: true,
        rows,
        columns: reports,
        rejected: Vec::new(),
        rejected_omitted: 0,
        budget_used: BudgetUse {
            bytes: bytes_read,
            rows,
            columns: width,
            memory_bytes: peak,
        },
    };
    Ok((batches, report))
}

/// Codecs this build has no decoder for. Snappy is the one already in the graph.
fn unread_codec(c: Compression) -> Option<&'static str> {
    match c {
        Compression::UNCOMPRESSED | Compression::SNAPPY => None,
        Compression::GZIP(_) => Some("parquet compressed with gzip"),
        Compression::LZO => Some("parquet compressed with lzo"),
        Compression::BROTLI(_) => Some("parquet compressed with brotli"),
        Compression::LZ4 | Compression::LZ4_RAW => Some("parquet compressed with lz4"),
        Compression::ZSTD(_) => Some("parquet compressed with zstd"),
    }
}

/// The decoder's message is not shown: it can quote bytes of the file.
fn invalid<E>(_: E) -> IngestError {
    IngestError::Invalid { format: "parquet" }
}

type Converted = (Vec<ArrayRef>, InferredType, Option<&'static str>);

/// One column's arrays in a type a CSV column has, decided over all of them so every batch agrees.
fn convert(parts: Vec<ArrayRef>) -> Result<Converted> {
    let Some(dt) = parts.first().map(|a| a.data_type().clone()) else {
        return Ok((parts, InferredType::Text, None));
    };
    if let DataType::Dictionary(_, value) = &dt {
        let parts = cast_all(&parts, value)?;
        return convert(parts);
    }
    let to = |t: DataType, inferred: InferredType| Ok((cast_all(&parts, &t)?, inferred, None));
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => to(DataType::Int64, InferredType::Integer),
        DataType::UInt64 => {
            let fits = parts.iter().all(|a| {
                arrow::compute::max(a.as_primitive::<UInt64Type>())
                    .is_none_or(|m| m <= i64::MAX as u64)
            });
            if fits {
                to(DataType::Int64, InferredType::Integer)
            } else {
                to(
                    DataType::Decimal128(20, 0),
                    InferredType::Decimal {
                        precision: 20,
                        scale: 0,
                    },
                )
            }
        }
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s) => to(
            DataType::Decimal128(p, s),
            InferredType::Decimal {
                precision: p,
                scale: s,
            },
        ),
        DataType::Decimal256(p, s) if p <= 38 => to(
            DataType::Decimal128(p, s),
            InferredType::Decimal {
                precision: p,
                scale: s,
            },
        ),
        DataType::Boolean => to(DataType::Boolean, InferredType::Boolean),
        DataType::Date32 | DataType::Date64 => to(DataType::Date32, InferredType::Date),
        DataType::Timestamp(unit, tz) => match micros(&parts, unit, tz) {
            Some(parts) => Ok((parts, InferredType::Timestamp, None)),
            None => Ok((
                format_all(&parts)?,
                InferredType::Text,
                Some("timestamp finer than a microsecond or past its range, kept as written"),
            )),
        },
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            to(DataType::Utf8, InferredType::Text)
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Ok((
            hex_all(&parts)?,
            InferredType::Text,
            Some("binary, shown as hex"),
        )),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok((
            format_all(&parts)?,
            InferredType::Text,
            Some("floating point, kept as written"),
        )),
        _ => Ok((format_all(&parts)?, InferredType::Text, None)),
    }
}

fn cast_all(parts: &[ArrayRef], to: &DataType) -> Result<Vec<ArrayRef>> {
    parts
        .iter()
        .map(|a| cast_with_options(a, to, &STRICT).map_err(invalid))
        .collect()
}

/// UTC microseconds, as a CSV timestamp is, or `None` where that would change a value.
fn micros(parts: &[ArrayRef], unit: TimeUnit, tz: Option<Arc<str>>) -> Option<Vec<ArrayRef>> {
    if unit == TimeUnit::Nanosecond {
        let exact = parts.iter().all(|a| {
            a.as_primitive::<TimestampNanosecondType>()
                .iter()
                .flatten()
                .all(|v| v % 1000 == 0)
        });
        if !exact {
            return None;
        }
    }
    let to = DataType::Timestamp(TimeUnit::Microsecond, tz);
    parts
        .iter()
        .map(|a| {
            let a = cast_with_options(a, &to, &STRICT).ok()?;
            let utc = a
                .as_primitive::<TimestampMicrosecondType>()
                .clone()
                .with_timezone_opt(None::<Arc<str>>);
            Some(Arc::new(utc) as ArrayRef)
        })
        .collect()
}

fn format_all(parts: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    let options = FormatOptions::new().with_null("");
    parts
        .iter()
        .map(|a| {
            let f = ArrayFormatter::try_new(a.as_ref(), &options).map_err(invalid)?;
            let mut b = StringBuilder::with_capacity(a.len(), 0);
            for i in 0..a.len() {
                if a.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(f.value(i).to_string());
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        })
        .collect()
}

fn hex_all(parts: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let parts = cast_all(parts, &DataType::LargeBinary)?;
    Ok(parts
        .iter()
        .map(|a| {
            let a = a.as_binary::<i64>();
            let mut s = String::new();
            let out: StringArray = a
                .iter()
                .map(|v| {
                    v.map(|v| {
                        s.clear();
                        s.push_str("0x");
                        for b in v {
                            s.push(DIGITS[(b >> 4) as usize] as char);
                            s.push(DIGITS[(b & 15) as usize] as char);
                        }
                        s.clone()
                    })
                })
                .collect();
            Arc::new(out) as ArrayRef
        })
        .collect())
}

fn longest_text(parts: &[ArrayRef]) -> Option<u64> {
    parts
        .iter()
        .filter_map(|a| a.as_string_opt::<i32>())
        .flat_map(|a| a.offsets().windows(2).map(|w| (w[1] - w[0]) as u64))
        .max()
}
