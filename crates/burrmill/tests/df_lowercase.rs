//! A column the host declares `address` is written lower-case, so `lower()` on it does not stop the
//! scan pruning segments by their statistics.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

const SEGMENTS: u64 = 20;

fn engine(declared_type: &str) -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
    ]));
    let files = (0..SEGMENTS)
        .map(|n| {
            let path = tmp.path().join(format!("transfer-{n:064x}.parquet"));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(vec![2 * n, 2 * n + 1])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        format!("0x{:040x}", 2 * n),
                        format!("0x{:040x}", 2 * n + 1),
                    ])),
                ],
            )
            .unwrap();
            let f = std::fs::File::create(&path).unwrap();
            let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
            let len = std::fs::metadata(&path).unwrap().len();
            (path, len)
        })
        .collect();
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 64 << 20,
        threads: 1,
        spill: None,
    })
    .unwrap();
    let declared = vec![
        ("block_number".into(), "u64".into()),
        ("who".into(), declared_type.into()),
    ];
    e.register_facts("t", &declared, files, &[], (None, None))
        .unwrap();
    (tmp, e)
}

/// Row groups the scan kept after pruning by statistics, from `EXPLAIN ANALYZE`.
fn kept_by_statistics(e: &Engine, sql: &str) -> usize {
    let mut plan = String::new();
    for b in e.sql(&format!("EXPLAIN ANALYZE {sql}")).unwrap() {
        let c = arrow::compute::cast(b.column(1), &DataType::Utf8).unwrap();
        let c = c.as_any().downcast_ref::<StringArray>().unwrap();
        (0..c.len()).for_each(|i| plan.push_str(c.value(i)));
    }
    let after = plan
        .split("row_groups_pruned_statistics=")
        .nth(1)
        .unwrap_or_else(|| panic!("no statistics pruning reported:\n{plan}"));
    let matched = after.split(" matched").next().unwrap();
    matched.rsplit(' ').next().unwrap().parse().unwrap()
}

const SQL: &str =
    "SELECT block_number FROM t WHERE lower(who) = '0x0000000000000000000000000000000000000007'";

#[test]
fn lower_on_an_address_column_still_prunes_by_statistics() {
    let (_t, e) = engine("address");
    let got = e.sql(SQL).unwrap();
    assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(kept_by_statistics(&e, SQL), 1);
}

#[test]
fn lower_on_any_other_text_column_is_left_alone() {
    let (_t, e) = engine("string");
    assert_eq!(
        e.sql(SQL)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(kept_by_statistics(&e, SQL), SEGMENTS as usize);
}
