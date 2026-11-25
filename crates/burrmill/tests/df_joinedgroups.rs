//! An aggregate joined on its group key computes only the groups the join can keep (#433).
//! Decorrelating TPC-H Q17's scalar subquery averaged every part's quantity, 2M parts over 60M
//! rows at SF10, to join the averages to 2,040 of them.

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
const KEYS: i64 = 50;
const KEY: fn(i64) -> i64 = |i| i % KEYS;
const QTY: fn(i64) -> i64 = |i| (i * 7) % 37;
/// The parts `p.b = 0` keeps: one in five.
const KEPT: fn(i64) -> bool = |k| k % 5 == 0;

/// `li(pk, q, v)` over 50 keys and `p(pk, b)` naming each once, planned across threads.
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
    write(
        &segs,
        "p",
        vec![
            ("pk", (0..KEYS).collect()),
            ("b", (0..KEYS).map(|k| k % 5).collect()),
        ],
    );
    let e = Engine::open_segments(&segs).unwrap();
    e.set_small_statement_bytes(0);
    (tmp, e)
}

fn analyzed(e: &Engine, sql: &str) -> String {
    let mut out = String::new();
    for b in e.sql(&format!("EXPLAIN ANALYZE {sql}")).unwrap() {
        let plans = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..plans.len() {
            out += plans.value(i);
        }
    }
    out
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

/// The groups each aggregate computing `aggr` gave out whole, summed over its partitions.
fn groups_out(plan: &str, aggr: &str) -> Vec<u64> {
    plan.lines()
        .filter(|l| l.contains("AggregateExec") && l.contains(aggr) && !l.contains("Partial"))
        .map(|l| {
            let at = l.find("output_rows=").expect("metrics") + "output_rows=".len();
            let n: String = l[at..].chars().take_while(char::is_ascii_digit).collect();
            n.parse().unwrap()
        })
        .collect()
}

#[test]
fn q17_shape_averages_only_the_joined_parts() {
    let (_tmp, e) = fixture();
    let sql = "SELECT sum(v) AS s FROM li, p WHERE p.pk = li.pk AND p.b = 0 \
               AND li.q < (SELECT 0.2 * avg(q) FROM li AS l2 WHERE l2.pk = p.pk)";
    let plan = analyzed(&e, sql);
    // The 10 parts the outer query keeps, not all 50.
    assert_eq!(groups_out(&plan, "checked_avg"), vec![10], "{plan}");
    // q < avg / 5, exactly: 5 q n < sum.
    let mut want = 0i64;
    for k in (0..KEYS).filter(|&k| KEPT(k)) {
        let of_k: Vec<i64> = (0..ROWS).filter(|&i| KEY(i) == k).collect();
        let (n, sum) = (of_k.len() as i64, of_k.iter().map(|&i| QTY(i)).sum::<i64>());
        // No row on the boundary, where a DOUBLE average could round either way.
        assert!(of_k.iter().all(|&i| 5 * QTY(i) * n != sum));
        want += of_k.iter().filter(|&&i| 5 * QTY(i) * n < sum).sum::<i64>();
    }
    assert_eq!(rows(&e, sql), vec![vec![want.to_string()]]);
}

#[test]
fn an_inner_join_on_the_key_keeps_the_kept_groups() {
    let (_tmp, e) = fixture();
    let sql = "SELECT s.pk, s.n FROM (SELECT pk, count(*) AS n FROM li GROUP BY pk) AS s \
               JOIN p ON s.pk = p.pk WHERE p.b = 0 ORDER BY s.pk";
    assert_eq!(groups_out(&analyzed(&e, sql), "count"), vec![10]);
    let want: Vec<Vec<String>> = (0..KEYS)
        .filter(|&k| KEPT(k))
        .map(|k| vec![k.to_string(), (ROWS / KEYS).to_string()])
        .collect();
    assert_eq!(rows(&e, sql), want);
}

#[test]
fn an_outer_join_keeps_every_group_it_preserves() {
    let (_tmp, e) = fixture();
    // The aggregate is the preserved side: its groups that meet no part stay, with NULLs.
    let sql = "SELECT s.pk, s.n, p.b FROM (SELECT pk, count(*) AS n FROM li GROUP BY pk) AS s \
               LEFT JOIN (SELECT * FROM p WHERE b = 0) AS p ON s.pk = p.pk ORDER BY s.pk";
    assert_eq!(groups_out(&analyzed(&e, sql), "count"), vec![KEYS as u64]);
    let got = rows(&e, sql);
    assert_eq!(got.len(), KEYS as usize);
    for (k, r) in got.iter().enumerate() {
        let b = if KEPT(k as i64) { "0" } else { "NULL" };
        assert_eq!(
            r,
            &vec![k.to_string(), (ROWS / KEYS).to_string(), b.to_string()]
        );
    }
}

#[test]
fn a_volatile_filter_is_left_out_of_the_keys() {
    let (_tmp, e) = fixture();
    // A second `random()` would draw other rows than the join's; the keys are those of `b = 0`,
    // a superset of what the join meets.
    let sql = "SELECT s.pk, s.n FROM (SELECT pk, count(*) AS n FROM li GROUP BY pk) AS s \
               JOIN (SELECT * FROM p WHERE b = 0 AND random() < 2.0) AS p ON s.pk = p.pk \
               ORDER BY s.pk";
    assert_eq!(groups_out(&analyzed(&e, sql), "count"), vec![10]);
    assert_eq!(rows(&e, sql).len(), 10);
}
