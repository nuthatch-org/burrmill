//! A `HAVING` that compares each group with a total over the same rows sums the groups for that
//! total, and the groups are computed once (`df/grouptotal.rs`). TPC-H Q11's subquery joined and
//! scanned `partsupp` a second time, the half of its time at SF10 that this saves (#433).

use std::sync::Arc;

use arrow::array::{Decimal128Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

const ROWS: i64 = 4_000;

fn write(e: &mut Engine, dir: &std::path::Path, table: &str, batch: RecordBatch) {
    let path = dir.join(format!("{table}-{:064x}.parquet", 0));
    let mut w = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    e.register_facts(table, &[], vec![(path, len)], &[], (None, None))
        .unwrap();
}

/// Q11's shape: `items` is `partsupp` (`k` the part, `s` the supplier, `cost` a decimal, `qty` and
/// `w` a double), `sup` is `supplier` joined to its nation `n`.
fn engine(dir: &std::path::Path) -> Engine {
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 256 << 20,
        threads: 4,
        spill: None,
    })
    .unwrap();
    let n: Vec<i64> = (0..ROWS).collect();
    let items = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Int64, false),
            Field::new("cost", DataType::Decimal128(15, 2), false),
            Field::new("qty", DataType::Int64, false),
            Field::new("w", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(n.iter().map(|x| x % 400))),
            Arc::new(Int64Array::from_iter_values(n.iter().map(|x| x * 7 % 50))),
            Arc::new(
                Decimal128Array::from_iter_values(n.iter().map(|x| (x * 7919 % 100_000) as i128))
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ),
            Arc::new(Int64Array::from_iter_values(n.iter().map(|x| x * 31 % 97))),
            Arc::new(Float64Array::from_iter_values(
                n.iter().map(|x| (x * 13 % 101) as f64 / 7.0),
            )),
        ],
    )
    .unwrap();
    write(&mut e, dir, "items", items);
    let s: Vec<i64> = (0..50).collect();
    let sup = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("s", DataType::Int64, false),
            Field::new("n", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(s.clone())),
            Arc::new(Int64Array::from_iter_values(s.iter().map(|x| x % 5))),
        ],
    )
    .unwrap();
    write(&mut e, dir, "sup", sup);
    e
}

fn answer(e: &Engine, sql: &str) -> Vec<String> {
    e.sql(sql)
        .unwrap_or_else(|err| panic!("{sql}\n{err}"))
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .map(|r| r.to_string())
        .collect()
}

/// How many scans of `items` the physical plan holds.
fn item_scans(e: &Engine, sql: &str) -> usize {
    let mut plan = String::new();
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        for row in burrmill::df::encode::rows(&b).unwrap() {
            plan.push_str(&row.to_string());
        }
    }
    plan.split("\\n")
        .filter(|l| l.contains("DataSourceExec") && l.contains("items-"))
        .count()
}

/// Q11 over `items` and `sup`, its subquery's `FROM` and `WHERE` as `inner` writes them.
fn q11(value: &str, share: &str, nation: i64, inner: &str) -> String {
    format!(
        "SELECT k, {value} AS value FROM items, sup WHERE items.s = sup.s AND sup.n = {nation} \
         GROUP BY k HAVING {value} > (SELECT {value} * {share} {inner}) ORDER BY value DESC, k"
    )
}

const SAME: &str = "FROM items, sup WHERE items.s = sup.s AND sup.n = {n}";
/// The same rows, written so that the subquery's plan is not the aggregate's input.
const SWAPPED: &str = "FROM sup, items WHERE sup.n = {n} AND sup.s = items.s";

fn inner(written: &str, nation: i64) -> String {
    written.replace("{n}", &nation.to_string())
}

#[test]
fn a_q11_shaped_total_sums_the_groups_and_scans_once() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let value = "sum(cost * qty)";
    let sql = q11(value, "0.004", 2, &inner(SAME, 2));
    let reference = q11(value, "0.004", 2, &inner(SWAPPED, 2));
    let got = answer(&e, &sql);
    assert!(
        got.len() > 10 && got.len() < 300,
        "{} groups kept",
        got.len()
    );
    assert_eq!(got, answer(&e, &reference));
    assert_eq!(item_scans(&e, &sql), 1, "{sql} scanned items twice");
    assert_eq!(item_scans(&e, &reference), 2, "{reference}");
}

#[test]
fn no_rows_give_no_total_and_no_groups() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let value = "sum(cost * qty)";
    let sql = q11(value, "0.004", 9, &inner(SAME, 9));
    assert!(answer(&e, &sql).is_empty());
    assert_eq!(item_scans(&e, &sql), 1, "{sql}");
}

#[test]
fn a_total_of_doubles_is_still_summed_from_the_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let value = "sum(w)";
    let sql = q11(value, "0.004", 2, &inner(SAME, 2));
    let reference = q11(value, "0.004", 2, &inner(SWAPPED, 2));
    assert_eq!(answer(&e, &sql), answer(&e, &reference));
    assert_eq!(item_scans(&e, &sql), 2, "{sql}");
}

#[test]
fn a_total_over_other_rows_is_left_alone() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let value = "sum(cost * qty)";
    let sql = q11(value, "0.004", 2, &inner(SAME, 3));
    let reference = q11(value, "0.004", 2, &inner(SWAPPED, 3));
    assert_eq!(answer(&e, &sql), answer(&e, &reference));
    assert_eq!(item_scans(&e, &sql), 2, "{sql}");
}
