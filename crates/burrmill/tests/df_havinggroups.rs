//! A semi join on a `HAVING`'s groups, over a join to the rows they aggregate, joins the groups
//! instead (`df/havinggroups.rs`), and a `HAVING`'s groups are guessed few (`df/buildside.rs`).
//! TPC-H Q18 scanned and joined `lineitem` a second time to sum what its subquery had summed, and
//! its joins on the 624 orders the `HAVING` keeps at SF10 stayed partitioned (#433).

use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

const ORDERS: i64 = 2_000;

fn write(e: &mut Engine, dir: &std::path::Path, table: &str, batch: RecordBatch) -> u64 {
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
    len
}

fn decimals(v: impl Iterator<Item = i128>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from_iter_values(v)
            .with_precision_and_scale(15, 2)
            .unwrap(),
    )
}

/// Q18's shape: `cust` is `customer`, `ords` is `orders`, `lines` is `lineitem` with 1 to 7 lines
/// an order, 40 lines with a NULL order and 60 of orders that do not exist, both with groups the
/// `HAVING` keeps.
fn engine(dir: &std::path::Path) -> Engine {
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 256 << 20,
        threads: 4,
        spill: None,
    })
    .unwrap();
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = |m: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % m) as i64
    };
    let mut lines: Vec<(Option<i64>, i64, f64)> = Vec::new();
    for o in 0..ORDERS {
        for _ in 0..=next(7) {
            lines.push((Some(o), 1 + next(50), next(1000) as f64 / 7.0));
        }
    }
    for i in 0..100 {
        let o = (i >= 40).then_some(ORDERS + i % 20);
        lines.push((o, 50, 1.0));
    }
    let notes: Vec<String> = (0..lines.len())
        .map(|_| format!("{:08x}", next(1 << 32)))
        .collect();
    let line_bytes = write(
        &mut e,
        dir,
        "lines",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("l_k", DataType::Int64, true),
                Field::new("l_qty", DataType::Decimal128(15, 2), false),
                Field::new("l_w", DataType::Float64, false),
                Field::new("l_note", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter(lines.iter().map(|l| l.0))),
                decimals(lines.iter().map(|l| l.1 as i128 * 100)),
                Arc::new(Float64Array::from_iter_values(lines.iter().map(|l| l.2))),
                Arc::new(StringArray::from_iter_values(notes.iter())),
            ],
        )
        .unwrap(),
    );
    let o: Vec<i64> = (0..ORDERS).collect();
    let comments: Vec<String> = o
        .iter()
        .map(|_| format!("{:016x}", next(1 << 62)))
        .collect();
    let ord_bytes = write(
        &mut e,
        dir,
        "ords",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("o_k", DataType::Int64, false),
                Field::new("o_c", DataType::Int64, false),
                Field::new("o_price", DataType::Decimal128(15, 2), false),
                Field::new("o_comment", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(o.clone())),
                Arc::new(Int64Array::from_iter_values(o.iter().map(|k| k * 7 % 300))),
                decimals(o.iter().map(|k| (k * 7919 % 100_000) as i128)),
                Arc::new(StringArray::from_iter_values(comments.iter())),
            ],
        )
        .unwrap(),
    );
    let c: Vec<i64> = (0..300).collect();
    let cust_bytes = write(
        &mut e,
        dir,
        "cust",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("c_k", DataType::Int64, false),
                Field::new("c_name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(c.clone())),
                Arc::new(StringArray::from_iter_values(
                    c.iter().map(|k| format!("Customer#{k:09}")),
                )),
            ],
        )
        .unwrap(),
    );
    // `lines` reads more than `cust` and `ords` together, as `lineitem` does, but under the 3.75
    // times at which a tenth of a third of it is decisively the smaller side.
    let ratio = line_bytes as f64 / (ord_bytes + cust_bytes) as f64;
    assert!((1.0..3.75).contains(&ratio), "{ratio}");
    // Planned as a large statement is, not as one whose inputs are all small.
    e.set_small_statement_bytes(0);
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

fn plan(e: &Engine, sql: &str) -> String {
    let mut plan = String::new();
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        for row in burrmill::df::encode::rows(&b).unwrap() {
            plan.push_str(&row.to_string());
        }
    }
    plan
}

/// How many scans of `lines` the physical plan holds.
fn line_scans(e: &Engine, sql: &str) -> usize {
    plan(e, sql)
        .split("\\n")
        .filter(|l| l.contains("DataSourceExec") && l.contains("lines-"))
        .count()
}

/// Q18 over the fixture: `select` the outer aggregates, `sub` the subquery's `FROM` and `WHERE`,
/// `key` the outer column its `IN` keys.
fn q18(select: &str, sub: &str, key: &str) -> String {
    format!(
        "SELECT c_name, c_k, o_k, o_price, {select} FROM cust, ords, lines \
         WHERE {key} IN (SELECT l_k {sub} GROUP BY l_k HAVING sum(l_qty) > 200) \
         AND c_k = o_c AND o_k = l_k \
         GROUP BY c_name, c_k, o_k, o_price ORDER BY o_price DESC, o_k"
    )
}

const SAME: &str = "FROM lines";
/// The same groups over a plan that is not the outer `lines`, so nothing is rewritten.
const OTHER: &str = "FROM lines WHERE l_qty >= 0";

#[test]
fn a_q18_shaped_join_reads_the_havings_groups_and_scans_once() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let sql = q18("sum(l_qty) AS q", SAME, "o_k");
    let reference = q18("sum(l_qty) AS q", OTHER, "o_k");
    let got = answer(&e, &sql);
    assert!(
        got.len() > 10 && got.len() < 300,
        "{} orders kept",
        got.len()
    );
    assert_eq!(got, answer(&e, &reference));
    assert_eq!(line_scans(&e, &sql), 1, "{sql} scanned lines twice");
    assert_eq!(line_scans(&e, &reference), 2, "{reference}");
    // The join builds on the groups the `HAVING` keeps, collected, not on every order.
    let p = plan(&e, &sql);
    let join = p
        .split("HashJoinExec: ")
        .nth(1)
        .expect("a hash join")
        .split("HashJoinExec")
        .next()
        .unwrap();
    assert!(join.starts_with("mode=CollectLeft, join_type=Inner"), "{p}");
    let build = join.split("DataSourceExec").next().unwrap();
    assert!(build.contains("FilterExec: sum("), "{p}");
}

#[test]
fn a_semi_join_on_a_havings_groups_is_collected() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    // A count counts each order once per line, so the groups cannot stand in for the lines.
    let sql = q18("sum(l_qty) AS q, count(*) AS n", SAME, "o_k");
    assert_eq!(
        answer(&e, &sql),
        answer(&e, &q18("sum(l_qty) AS q, count(*) AS n", OTHER, "o_k"))
    );
    assert_eq!(line_scans(&e, &sql), 2, "{sql}");
    let p = plan(&e, &sql);
    assert!(
        p.contains("HashJoinExec: mode=CollectLeft, join_type=RightSemi"),
        "{p}"
    );
}

#[test]
fn sums_the_groups_do_not_hold_keep_the_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    for select in [
        // An order's price, once per line.
        "sum(o_price) AS p",
        // A sum of doubles, which the groups do not hold as exact sums.
        "sum(l_w) AS w",
        "max(l_qty) AS m",
    ] {
        let sql = q18(select, SAME, "o_k");
        assert_eq!(line_scans(&e, &sql), 2, "{sql}");
        if !select.contains("l_w") {
            assert_eq!(answer(&e, &sql), answer(&e, &q18(select, OTHER, "o_k")));
        }
    }
    // An `IN` on another column than the join's.
    let sql = q18("sum(l_qty) AS q", SAME, "o_c");
    assert_eq!(line_scans(&e, &sql), 2, "{sql}");
    assert_eq!(
        answer(&e, &sql),
        answer(&e, &q18("sum(l_qty) AS q", OTHER, "o_c"))
    );
}
