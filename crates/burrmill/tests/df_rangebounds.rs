//! nuthatch #1800: a join's range condition bounds the probe scan by the build side's extremes, as
//! DuckDB's join filters do, so one day of a dated view reads that day's row groups.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

const DAYS: i64 = 30;
const PER_DAY: i64 = 1000;
const FIRST: i64 = 20000;

/// One segment per day, rows a minute apart, the epoch stored as decimal text as nuthatch's typed
/// document rows store it.
fn engine(dir: &std::path::Path, budget: bool) -> Engine {
    let schema = Arc::new(Schema::new(vec![
        Field::new("start_epoch", DataType::Utf8, true),
        Field::new("v", DataType::Int64, false),
    ]));
    let mut files = Vec::new();
    for d in 0..DAYS {
        let epochs: Vec<String> = (0..PER_DAY)
            .map(|j| ((FIRST + d) * 86400 + j * 60).to_string())
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(epochs)),
                Arc::new(Int64Array::from((0..PER_DAY).collect::<Vec<_>>())),
            ],
        )
        .unwrap();
        let path = dir.join(format!("rows-{d:064x}.parquet"));
        let f = std::fs::File::create(&path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let len = std::fs::metadata(&path).unwrap().len();
        files.push((path, len));
    }
    let mut e = match budget {
        true => Engine::open_empty_budgeted(burrmill::Budget {
            memory_bytes: 256 << 20,
            threads: 4,
            spill: None,
        }),
        false => Engine::open_empty(),
    }
    .unwrap();
    // The partitioned join is the point; small, it would be planned in one partition.
    e.set_small_statement_bytes(0);
    // An unsealed row as nuthatch holds one, so the scan is one arm of a union with the tip.
    let tip = serde_json::json!({ "start_epoch": ((FIRST + 40) * 86400).to_string(), "v": 7 });
    e.register_facts("rows", &[], files, &[tip], (None, None))
        .unwrap();
    e.register_view(
        "bounds",
        "SELECT DATE '1970-01-01' + CAST(d AS INTEGER) AS day, CAST(d * 86400 AS VARCHAR) AS first_epoch, \
         CAST(d * 86400 + 86399 AS VARCHAR) AS last_epoch FROM range(19000, 21000) AS t(d)",
    )
    .unwrap();
    e.register_view(
        "dated",
        "SELECT r.v, k.day FROM rows r JOIN bounds k \
         ON k.day = DATE '1970-01-01' + CAST(CAST(r.start_epoch AS BIGINT) // 86400 AS INTEGER) \
         AND r.start_epoch BETWEEN k.first_epoch AND k.last_epoch",
    )
    .unwrap();
    e.register_view(
        "dated_flipped",
        "SELECT r.v, k.day FROM rows r JOIN bounds k \
         ON k.day = DATE '1970-01-01' + CAST(CAST(r.start_epoch AS BIGINT) // 86400 AS INTEGER) \
         AND k.first_epoch <= r.start_epoch AND k.last_epoch >= r.start_epoch",
    )
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

/// The `rows` scan's line of `EXPLAIN ANALYZE`.
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

#[test]
fn one_day_reads_one_days_row_groups() {
    let tmp = tempfile::tempdir().unwrap();
    for budget in [false, true] {
        let e = engine(tmp.path(), budget);
        for view in ["dated", "dated_flipped"] {
            let sql = format!(
                "SELECT count(*) AS n, sum(CAST(v AS BIGINT)) AS s FROM {view} WHERE day = DATE '1970-01-01' + {}",
                FIRST + 7
            );
            assert_eq!(
                answer(&e, &sql),
                [r#"{"n":1000,"s":"499500"}"#],
                "{view} {budget}"
            );
            let line = rows_scan(&e, &sql);
            assert!(
                line.contains(&format!(
                    "row_groups_pruned_statistics={DAYS} total → 1 matched"
                )),
                "{view} {budget}: {line}"
            );
        }
    }
}

/// A join that keeps the probe's unmatched rows emits every row, so nothing may be skipped.
#[test]
fn a_join_keeping_unmatched_probe_rows_reads_them_all() {
    let tmp = tempfile::tempdir().unwrap();
    for budget in [false, true] {
        let e = engine(tmp.path(), budget);
        let sql = format!(
            "SELECT count(*) AS n, count(k.day) AS matched FROM rows r LEFT JOIN \
             (SELECT * FROM bounds WHERE day = DATE '1970-01-01' + {}) k \
             ON k.day = DATE '1970-01-01' + CAST(CAST(r.start_epoch AS BIGINT) // 86400 AS INTEGER) \
             AND r.start_epoch BETWEEN k.first_epoch AND k.last_epoch",
            FIRST + 7
        );
        assert_eq!(
            answer(&e, &sql),
            [format!(
                r#"{{"n":{},"matched":{PER_DAY}}}"#,
                DAYS * PER_DAY + 1
            )],
            "{budget}"
        );
    }
}

/// No build row: the join emits nothing, and the scan need read nothing.
#[test]
fn a_day_with_no_bound_reads_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    for budget in [false, true] {
        let e = engine(tmp.path(), budget);
        let sql = "SELECT count(*) AS n FROM dated WHERE day = DATE '1970-01-01' + 30000";
        assert_eq!(answer(&e, sql), [r#"{"n":0}"#], "{budget}");
    }
}

/// Several build rows: the scan reads from the least lower bound to the greatest upper one.
#[test]
fn a_run_of_days_reads_from_the_first_bound_to_the_last() {
    let tmp = tempfile::tempdir().unwrap();
    for budget in [false, true] {
        let e = engine(tmp.path(), budget);
        let sql = format!(
            "SELECT count(*) AS n, sum(CAST(v AS BIGINT)) AS s FROM dated \
             WHERE day BETWEEN DATE '1970-01-01' + {} AND DATE '1970-01-01' + {}",
            FIRST + 7,
            FIRST + 9
        );
        assert_eq!(
            answer(&e, &sql),
            [r#"{"n":3000,"s":"1498500"}"#],
            "{budget}"
        );
        let line = rows_scan(&e, &sql);
        assert!(
            line.contains(&format!(
                "row_groups_pruned_statistics={DAYS} total → 3 matched"
            )),
            "{budget}: {line}"
        );
    }
}

/// Bounds read from segments make a partitioned join, where each partition's build side holds only
/// some of the bounds: one partition's extremes would skip rows another partition matches.
#[test]
fn a_partitioned_join_reads_every_partitions_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("first_epoch", DataType::Utf8, true),
        Field::new("last_epoch", DataType::Utf8, true),
    ]));
    let mut files = Vec::new();
    for d in [3, 11, 19, 27] {
        let day = FIRST + d;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![(day * 86400).to_string()])),
                Arc::new(StringArray::from(vec![(day * 86400 + 86399).to_string()])),
            ],
        )
        .unwrap();
        let path = tmp.path().join(format!("picked-{d:064x}.parquet"));
        let f = std::fs::File::create(&path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let len = std::fs::metadata(&path).unwrap().len();
        files.push((path, len));
    }
    let rows = tempfile::tempdir().unwrap();
    let mut e = engine(rows.path(), false);
    e.register_facts("picked", &[], files, &[], (None, None))
        .unwrap();
    let sql = "SELECT count(*) AS n FROM rows r JOIN picked p \
               ON r.start_epoch BETWEEN p.first_epoch AND p.last_epoch \
               AND p.first_epoch = CAST(CAST(r.start_epoch AS BIGINT) // 86400 * 86400 AS VARCHAR)";
    assert_eq!(answer(&e, sql), [r#"{"n":4000}"#]);
}
