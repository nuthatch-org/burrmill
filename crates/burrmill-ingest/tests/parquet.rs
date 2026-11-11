//! Parquet through the budgets CSV reads under (#147).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float64Array,
    Int32Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use burrmill_ingest::{
    Budget, BudgetBreach, Format, FormatHint, InferredType, IngestError, ingest_bytes,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

fn write(columns: Vec<(&str, ArrayRef)>, compression: Compression) -> Vec<u8> {
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    out
}

fn three_rows() -> Vec<u8> {
    let at = 1_700_000_000_000_000_000i64;
    write(
        vec![
            (
                "n",
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            ),
            ("big", Arc::new(UInt64Array::from(vec![1, u64::MAX, 2]))),
            ("small", Arc::new(UInt64Array::from(vec![1, 2, 3]))),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from(vec![150, -5, 0])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
            ),
            ("day", Arc::new(Date32Array::from(vec![19_754, 0, -1]))),
            (
                "at",
                Arc::new(
                    TimestampNanosecondArray::from(vec![at, at + 1_000, at + 2_000])
                        .with_timezone("+02:00"),
                ),
            ),
            (
                "fine",
                Arc::new(TimestampNanosecondArray::from(vec![at + 1, at, at])),
            ),
            ("ok", Arc::new(BooleanArray::from(vec![true, false, true]))),
            ("who", Arc::new(StringArray::from(vec!["a", "b", "c"]))),
            (
                "addr",
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(
                        [[0xab, 0x01], [0, 0], [0xff, 0x10]].iter(),
                    )
                    .unwrap(),
                ),
            ),
            ("ratio", Arc::new(Float64Array::from(vec![1.5, 0.1, -2.0]))),
        ],
        Compression::SNAPPY,
    )
}

#[test]
fn a_parquet_file_opens_with_its_types() {
    let table = ingest_bytes(&three_rows(), FormatHint::Sniff, &Budget::untrusted()).unwrap();
    let report = table.report();
    assert_eq!(report.format, Format::Parquet);
    assert_eq!(report.rows, 3);
    let types: Vec<_> = report
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.inferred.clone()))
        .collect();
    assert_eq!(
        types,
        vec![
            ("n", InferredType::Integer),
            (
                "big",
                InferredType::Decimal {
                    precision: 20,
                    scale: 0
                }
            ),
            ("small", InferredType::Integer),
            (
                "amount",
                InferredType::Decimal {
                    precision: 10,
                    scale: 2
                }
            ),
            ("day", InferredType::Date),
            ("at", InferredType::Timestamp),
            ("fine", InferredType::Text),
            ("ok", InferredType::Boolean),
            ("who", InferredType::Text),
            ("addr", InferredType::Text),
            ("ratio", InferredType::Text),
        ]
    );
    assert_eq!(report.columns[0].nulls, 1);
    let schema = table.batches()[0].schema();
    for f in schema.fields() {
        assert!(f.is_nullable(), "{}", f.name());
    }
    assert_eq!(
        schema.field(5).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    let row = |i: usize| table.preview(3)[i].clone();
    let cell = |s: &str| Some(s.to_string());
    assert_eq!(
        row(0),
        vec![
            cell("1"),
            cell("1"),
            cell("1"),
            cell("1.50"),
            cell("2024-02-01"),
            // UTC, whatever zone the file named.
            cell("1700000000000000"),
            cell("2023-11-14T22:13:20.000000001"),
            cell("true"),
            cell("a"),
            cell("0xab01"),
            cell("1.5"),
        ]
    );
    assert_eq!(row(1)[0], None);
    assert_eq!(row(1)[1], cell("18446744073709551615"));
    assert_eq!(row(1)[3], cell("-0.05"));
}

#[test]
fn a_parquet_file_is_held_to_each_budget() {
    let bytes = three_rows();
    let breach = |budget: Budget| match ingest_bytes(&bytes, FormatHint::Sniff, &budget) {
        Err(IngestError::Budget(b)) => b,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        breach(Budget::untrusted().max_rows(2)),
        BudgetBreach {
            which: "max_rows",
            limit: 2,
            at: 3
        }
    );
    assert_eq!(
        breach(Budget::untrusted().max_columns(10)),
        BudgetBreach {
            which: "max_columns",
            limit: 10,
            at: 11
        }
    );
    assert_eq!(breach(Budget::untrusted().max_bytes(10)).which, "max_bytes");
    let len = bytes.len() as u64;
    assert_eq!(
        breach(Budget::untrusted().max_memory_bytes(len)).which,
        "max_memory_bytes"
    );
    assert_eq!(
        breach(Budget::untrusted().max_cell_bytes(28)),
        BudgetBreach {
            which: "max_cell_bytes",
            limit: 28,
            at: 29
        }
    );
    let used = ingest_bytes(&bytes, FormatHint::Sniff, &Budget::untrusted())
        .unwrap()
        .report()
        .budget_used;
    assert!(used.memory_bytes > len, "{used:?}");
    let fits = Budget::untrusted().max_memory_bytes(used.memory_bytes);
    assert!(ingest_bytes(&bytes, FormatHint::Sniff, &fits).is_ok());
}

#[test]
fn many_rows_are_counted_before_they_are_decoded() {
    let n = 100_000;
    let bytes = write(
        vec![(
            "n",
            Arc::new(Int32Array::from_iter_values(0..n)) as ArrayRef,
        )],
        Compression::SNAPPY,
    );
    let err = ingest_bytes(
        &bytes,
        FormatHint::Sniff,
        &Budget::untrusted().max_rows(10).max_millis(u64::MAX),
    )
    .unwrap_err();
    assert_eq!(
        err,
        IngestError::Budget(BudgetBreach {
            which: "max_rows",
            limit: 10,
            at: n as u64
        })
    );
}

#[test]
fn a_codec_this_build_does_not_decode_is_refused_by_name() {
    let bytes = write(
        vec![("n", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef)],
        Compression::LZ4_RAW,
    );
    assert_eq!(
        ingest_bytes(&bytes, FormatHint::Sniff, &Budget::untrusted()).unwrap_err(),
        IngestError::Unsupported {
            format: "parquet compressed with lz4"
        }
    );
}

#[test]
fn a_broken_file_is_refused_without_its_bytes() {
    let mut bytes = three_rows();
    let footer = bytes.len() - 8;
    for b in &mut bytes[footer - 64..footer] {
        *b ^= 0x5a;
    }
    let err = ingest_bytes(&bytes, FormatHint::Sniff, &Budget::untrusted()).unwrap_err();
    assert_eq!(err, IngestError::Invalid { format: "parquet" });
    let err = ingest_bytes(
        b"PAR1CANARY_VALUE_991",
        FormatHint::Parquet,
        &Budget::untrusted(),
    )
    .unwrap_err();
    assert_eq!(err, IngestError::Invalid { format: "parquet" });
    assert!(!err.to_string().contains("CANARY"));
}

#[test]
fn a_csv_hint_still_reads_parquet_bytes_as_text() {
    let table = ingest_bytes(&three_rows(), FormatHint::Csv, &Budget::untrusted()).unwrap();
    assert_eq!(table.report().format, Format::Csv);
}

#[test]
fn an_empty_parquet_file_is_a_table_of_no_rows() {
    let bytes = write(
        vec![(
            "n",
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
        )],
        Compression::SNAPPY,
    );
    let table = ingest_bytes(&bytes, FormatHint::Parquet, &Budget::untrusted()).unwrap();
    assert_eq!(table.report().rows, 0);
}
