//! A Parquet scan evaluates its `WHERE` in the reader only where late materialization can spare
//! something (`df/staticfilters.rs`). TPC-H Q6's five range conjuncts, over unclustered rows and
//! with one fixed-width column besides, cost its `lineitem` scan 3.27 s of CPU at SF10 in the
//! reader, against 1.84 s read whole and filtered above (#433).

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

const ROWS: i64 = 20_000;

/// `rows`: `n` is 0..ROWS, `d` and `q` scatter over 0..100 and 0..50 as Q6's date and quantity do,
/// `price` is `n` and `note` is text.
fn engine(dir: &std::path::Path) -> Engine {
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 256 << 20,
        threads: 4,
        spill: None,
    })
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("d", DataType::Int64, false),
        Field::new("q", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let n: Vec<i64> = (0..ROWS).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(n.clone())),
            Arc::new(Int64Array::from_iter_values(
                n.iter().map(|x| x * 7919 % 100),
            )),
            Arc::new(Int64Array::from_iter_values(
                n.iter().map(|x| x / 100 * 37 % 50),
            )),
            Arc::new(Int64Array::from(n.clone())),
            Arc::new(StringArray::from_iter_values(
                n.iter().map(|x| format!("note {x}")),
            )),
        ],
    )
    .unwrap();
    let rows = dir.join(format!("rows-{:064x}.parquet", 0));
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(&rows).unwrap(), schema, None)
            .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&rows).unwrap().len();
    e.register_facts("rows", &[], vec![(rows, len)], &[], (None, None))
        .unwrap();

    let keys = dir.join(format!("keys-{:064x}.parquet", 0));
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(
            (0..ROWS).step_by(100),
        ))],
    )
    .unwrap();
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(&keys).unwrap(), schema, None)
            .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&keys).unwrap().len();
    e.register_facts("keys", &[], vec![(keys, len)], &[], (None, None))
        .unwrap();
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

/// The `rows` scan's line in `EXPLAIN ANALYZE`.
fn rows_scan(e: &Engine, sql: &str) -> String {
    let mut plan = String::new();
    for b in e.sql(&format!("EXPLAIN ANALYZE {sql}")).unwrap() {
        for row in burrmill::df::encode::rows(&b).unwrap() {
            plan.push_str(&row.to_string());
        }
    }
    plan.split("\\n")
        .find(|l| l.contains("DataSourceExec") && l.contains("rows-"))
        .unwrap_or_else(|| panic!("no rows scan in {plan}"))
        .to_string()
}

/// The rows the reader's row filter dropped in the `rows` scan, from `EXPLAIN ANALYZE`.
fn pruned_in_reader(e: &Engine, sql: &str) -> String {
    let line = rows_scan(e, sql);
    line.split("pushdown_rows_pruned=")
        .nth(1)
        .and_then(|m| m.split([',', ']']).next())
        .unwrap_or_else(|| panic!("no pushdown_rows_pruned in {line}"))
        .to_string()
}

/// The answer from `sql`'s filter applied by hand to every row.
fn expected(keep: impl Fn(i64, i64, i64) -> bool) -> (i64, i64) {
    let kept: Vec<i64> = (0..ROWS)
        .filter(|&n| keep(n, n * 7919 % 100, n / 100 * 37 % 50))
        .collect();
    (kept.len() as i64, kept.iter().sum())
}

#[test]
fn a_q6_shaped_scan_filters_above_the_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let sql = "SELECT count(*) AS c, CAST(sum(price) AS BIGINT) AS s FROM rows \
               WHERE d >= 10 AND d < 24 AND q >= 5 AND q <= 7 AND n < 15000";
    let (c, s) = expected(|n, d, q| (10..24).contains(&d) && (5..=7).contains(&q) && n < 15000);
    assert!(c > 0 && c < ROWS / 10, "{c}");
    assert_eq!(answer(&e, sql), [format!(r#"{{"c":{c},"s":{s}}}"#)]);
    assert_eq!(pruned_in_reader(&e, sql), "0", "the reader evaluated {sql}");
}

#[test]
fn an_equality_still_filters_in_the_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let sql =
        "SELECT count(*) AS c, CAST(sum(price) AS BIGINT) AS s FROM rows WHERE d = 17 AND q < 24";
    let (c, s) = expected(|_, d, q| d == 17 && q < 24);
    assert_eq!(answer(&e, sql), [format!(r#"{{"c":{c},"s":{s}}}"#)]);
    assert_ne!(pruned_in_reader(&e, sql), "0", "{sql}");
}

#[test]
fn a_text_column_read_besides_still_filters_in_the_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let sql = "SELECT count(*) AS c, max(note) AS m FROM rows WHERE d >= 10 AND d < 24 AND q < 24";
    let (c, _) = expected(|_, d, q| (10..24).contains(&d) && q < 24);
    let m = (0..ROWS)
        .filter(|n| (10..24).contains(&(n * 7919 % 100)) && n / 100 * 37 % 50 < 24)
        .map(|n| format!("note {n}"))
        .max()
        .unwrap();
    assert_eq!(answer(&e, sql), [format!(r#"{{"c":{c},"m":"{m}"}}"#)]);
    assert_ne!(pruned_in_reader(&e, sql), "0", "{sql}");
}

#[test]
fn a_join_filter_still_filters_in_the_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let sql = "SELECT count(*) AS c, CAST(sum(price) AS BIGINT) AS s FROM rows r JOIN keys k ON k.k = r.n \
               WHERE r.d >= 0 AND r.d < 100";
    let (c, s) = expected(|n, _, _| n % 100 == 0);
    assert_eq!(answer(&e, sql), [format!(r#"{{"c":{c},"s":{s}}}"#)]);
    assert_ne!(pruned_in_reader(&e, sql), "0", "{sql}");
}

/// TPC-H Q4's shape: a semi join's filter and a comparison of two columns, over a scan that reads
/// only the columns they read. In the reader, Q4's `lineitem` scan took 2.54 s of CPU at SF10 and
/// the statement 408–514 ms; with the join's filter left to the join, 0.87 s and 242–329 ms (#433).
#[test]
fn a_join_filter_over_only_its_columns_filters_above_the_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let sql = "SELECT count(*) AS c, CAST(sum(k) AS BIGINT) AS s FROM keys \
               WHERE EXISTS (SELECT 1 FROM rows WHERE n = k AND q > d)";
    let kept: Vec<i64> = (0..ROWS)
        .step_by(100)
        .filter(|n| n / 100 * 37 % 50 > n * 7919 % 100)
        .collect();
    let (c, s) = (kept.len(), kept.iter().sum::<i64>());
    assert!(c > 0 && c < (ROWS / 100) as usize, "{c}");
    assert_eq!(answer(&e, sql), [format!(r#"{{"c":{c},"s":{s}}}"#)]);
    let scan = rows_scan(&e, sql);
    assert!(scan.contains("DynamicFilter"), "no join filter in {scan}");
    assert_eq!(pruned_in_reader(&e, sql), "0", "the reader evaluated {sql}");
}
