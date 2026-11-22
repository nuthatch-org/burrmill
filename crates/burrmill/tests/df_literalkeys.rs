//! A literal group key leaves the aggregate (#381). Decorrelating TPC-H Q17's scalar subquery
//! groups by `l_partkey, true AS __always_true`; kept, the constant made the aggregate ordered, and
//! DataFusion 55's ordered aggregate emits its groups at a cost quadratic in their number: Q17 took
//! 26 s at SF10, 0.3 s at SF1.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

fn write(segs: &Path, name: &str, cols: Vec<(&str, Vec<i64>)>) {
    let schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, _)| Field::new(*n, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = cols
        .into_iter()
        .map(|(_, v)| Arc::new(Int64Array::from(v)) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let f = std::fs::File::create(segs.join(format!("{name}-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

const ROWS: i64 = 5_000;
const KEY: fn(i64) -> i64 = |i| i % 50;
const QTY: fn(i64) -> i64 = |i| (i * 7) % 37;

/// `li(pk, q, v)` over 50 keys and `p(pk)` naming ten of them, planned across threads.
fn fixture() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    write(
        &segs,
        "li",
        vec![
            ("pk", (0..ROWS).map(KEY).collect()),
            ("q", (0..ROWS).map(QTY).collect()),
            ("v", (0..ROWS).collect()),
        ],
    );
    write(&segs, "p", vec![("pk", (0..10).map(|k| k * 3).collect())]);
    let e = Engine::open_segments(&segs).unwrap();
    e.set_small_statement_bytes(0);
    (tmp, e)
}

fn physical(e: &Engine, sql: &str) -> String {
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        let kind = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let plans = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..plans.len() {
            if kind.value(i) == "physical_plan" {
                return plans.value(i).to_string();
            }
        }
    }
    panic!("no physical plan for {sql}");
}

/// Every cell of the answer as text, row by row.
fn rows(e: &Engine, sql: &str) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for b in e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err}")) {
        let cols: Vec<ArrayRef> = b
            .columns()
            .iter()
            .map(|c| arrow::compute::cast(c, &DataType::Utf8).unwrap())
            .collect();
        for r in 0..b.num_rows() {
            out.push(
                cols.iter()
                    .map(|c| {
                        let s = c.as_any().downcast_ref::<StringArray>().unwrap();
                        if s.is_null(r) {
                            "NULL".to_string()
                        } else {
                            s.value(r).to_string()
                        }
                    })
                    .collect(),
            );
        }
    }
    out
}

/// The aggregates of a plan: no literal key and no ordered mode.
fn assert_unordered(plan: &str) {
    for line in plan.lines().filter(|l| l.contains("AggregateExec")) {
        assert!(!line.contains("__always_true"), "a literal key:\n{plan}");
        assert!(
            !line.contains("ordering_mode"),
            "an ordered aggregate:\n{plan}"
        );
    }
}

#[test]
fn q17_shape_groups_by_the_key_alone() {
    let (_tmp, e) = fixture();
    let sql = "SELECT sum(v) AS s FROM li, p WHERE p.pk = li.pk \
               AND li.q < (SELECT 0.2 * avg(q) FROM li AS l2 WHERE l2.pk = p.pk)";
    assert_unordered(&physical(&e, sql));
    // q < avg / 5, exactly: 5 q n < sum.
    let parts: Vec<i64> = (0..10).map(|k| k * 3).collect();
    let mut want = 0i64;
    for &k in &parts {
        let of_k: Vec<i64> = (0..ROWS).filter(|&i| KEY(i) == k).collect();
        let (n, sum) = (of_k.len() as i64, of_k.iter().map(|&i| QTY(i)).sum::<i64>());
        // No row on the boundary, where a DOUBLE average could round either way.
        assert!(of_k.iter().all(|&i| 5 * QTY(i) * n != sum));
        want += of_k.iter().filter(|&&i| 5 * QTY(i) * n < sum).sum::<i64>();
    }
    assert_eq!(rows(&e, sql), vec![vec![want.to_string()]]);
}

#[test]
fn a_written_literal_key_is_answered_above_the_aggregate() {
    let (_tmp, e) = fixture();
    let sql = "SELECT pk, 'x' AS t, count(*) AS n FROM li GROUP BY pk, 'x' ORDER BY pk";
    assert_unordered(&physical(&e, sql));
    let got = rows(&e, sql);
    assert_eq!(got.len(), 50);
    assert_eq!(got[0], vec!["0", "x", "100"]);
    assert_eq!(got[49], vec!["49", "x", "100"]);
}

#[test]
fn literal_keys_alone_stay_grouped() {
    let (_tmp, e) = fixture();
    // Grouped by a constant only, no input answers no row, not one.
    assert!(
        rows(
            &e,
            "SELECT 'x' AS t, count(*) FROM li WHERE v < 0 GROUP BY 'x'"
        )
        .is_empty()
    );
    assert_eq!(
        rows(&e, "SELECT 'x' AS t, count(*) FROM li GROUP BY 'x'"),
        vec![vec!["x".to_string(), ROWS.to_string()]]
    );
}
