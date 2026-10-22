//! #52: a sort with LIMIT builds a top-k dynamic filter, and it reaches the Parquet scan, so row
//! groups the filter rules out are skipped rather than read.

use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use parquet::file::properties::WriterProperties;

const ROWS: u64 = 32_000;
const GROUP: usize = 500;

/// One segment, so one partition reads its row groups in order of ascending `block_number`.
fn engine(dir: &std::path::Path, budget: bool) -> Engine {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
    ]));
    let segs = {
        let span = 0..ROWS;
        let who: Vec<String> = span.clone().map(|i| format!("0x{i:040x}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(span.collect::<Vec<_>>())),
                Arc::new(StringArray::from(who)),
            ],
        )
        .unwrap();
        let path = dir.join(format!("t-{:064x}.parquet", 1));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(GROUP))
            .build();
        let f = std::fs::File::create(&path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let len = std::fs::metadata(&path).unwrap().len();
        vec![(path, len)]
    };
    let mut e = match budget {
        true => Engine::open_empty_budgeted(burrmill::Budget {
            memory_bytes: 256 << 20,
            threads: 1,
            spill: None,
        }),
        false => Engine::open_empty(),
    }
    .unwrap();
    let declared = vec![
        ("block_number".into(), "u64".into()),
        ("who".into(), "address".into()),
    ];
    e.register_facts("t", &declared, segs, &[], (None, None))
        .unwrap();
    e
}

fn analyze(e: &Engine, sql: &str) -> String {
    let mut out = String::new();
    for b in e.sql(&format!("EXPLAIN ANALYZE {sql}")).unwrap() {
        for row in burrmill::df::encode::rows(&b).unwrap() {
            out.push_str(&row.to_string());
        }
    }
    out
}

/// The scan's line of `EXPLAIN ANALYZE`, and the rows it produced.
fn scan(plan: &str) -> (&str, &str) {
    let line = plan
        .split("\\n")
        .find(|l| l.contains("DataSourceExec"))
        .unwrap_or_else(|| panic!("no scan in {plan}"));
    let rows = line
        .split("metrics=[output_rows=")
        .nth(1)
        .and_then(|m| m.split(',').next())
        .unwrap_or_else(|| panic!("no output_rows in {line}"));
    (line, rows)
}

/// Under a budget a sort over text runs over offsets, and its thresholds cannot be compared with the
/// scan's views, so that one top-k keeps its own filter and reads every row group. An address is
/// fixed binary inside the plan, so its thresholds reach the scan like a number's.
#[test]
fn top_k_skips_the_row_groups_its_filter_rules_out() {
    let tmp = tempfile::tempdir().unwrap();
    for budget in [false, true] {
        let e = engine(tmp.path(), budget);
        for (key, first) in [
            ("block_number", "0"),
            ("who", "\"0x0000000000000000000000000000000000000000\""),
        ] {
            let sql = format!("SELECT block_number, who FROM t ORDER BY {key} LIMIT 3");
            let rows: Vec<String> = e
                .sql(&sql)
                .unwrap()
                .iter()
                .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
                .map(|r| r[key].to_string())
                .collect();
            assert_eq!(rows.len(), 3, "{sql}");
            assert_eq!(rows[0], first, "{sql}");
            let plan = analyze(&e, &sql);
            let (line, read) = scan(&plan);
            match (budget, key) {
                // Unbudgeted, the segment is split over eight partitions that race the filter.
                (false, _) => assert!(line.contains("DynamicFilter"), "{key}: {line}"),
                // The first row group is read whole; every other is ruled out once it holds 3 rows.
                (true, _) => assert_eq!(read, GROUP.to_string(), "{key}: {line}"),
            }
        }
    }
}

/// A key that contains another, over columns a projection moved: DataFusion's filter, pushed to the
/// scan, read `p` at the sort's index for it, which at the scan is `block_number`, and skipped the
/// row groups that held the answer.
#[test]
fn a_key_inside_another_answers_as_without_the_filter() {
    let tmp = tempfile::tempdir().unwrap();
    for budget in [false, true] {
        let e = engine(tmp.path(), budget);
        for sql in [
            "WITH s AS (SELECT block_number, 31999 - block_number AS p, 31999 - block_number AS q FROM t) \
             SELECT p FROM s ORDER BY CASE WHEN block_number % 2 = 0 THEN p ELSE q END, block_number LIMIT 3",
            "WITH s AS (SELECT block_number, arrow_cast(who, 'Utf8') AS who, arrow_cast(who, 'Utf8') AS amount FROM t) \
             SELECT block_number AS p FROM s ORDER BY CASE WHEN block_number % 2 = 0 THEN who ELSE amount END DESC, \
             block_number LIMIT 3",
        ] {
            let got: Vec<String> = e
                .sql(sql)
                .unwrap_or_else(|err| panic!("budget {budget}: {sql}: {err}"))
                .iter()
                .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
                .map(|r| r["p"].to_string())
                .collect();
            let want = if sql.contains("DESC") {
                ["31999", "31998", "31997"]
            } else {
                ["0", "1", "2"]
            };
            assert_eq!(got, want, "budget {budget}: {sql}");
        }
    }
}

/// Only a top-k's filter is passed down. A `WHERE` on a column the query does not return, handed down
/// again, left a lambda above it reading its variable against the scan's columns, and refused.
#[test]
fn a_where_beside_a_lambda_is_not_pushed_again() {
    let tmp = tempfile::tempdir().unwrap();
    for budget in [false, true] {
        let e = engine(tmp.path(), budget);
        let sql = "SELECT [c FOR c IN string_split(COALESCE(who, 'ab'), ',') IF c <> ''][1] IS NULL AS n \
                   FROM t WHERE block_number % 16 = 0";
        let rows = e
            .sql(sql)
            .unwrap_or_else(|err| panic!("budget {budget}: {err}"))
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(rows, 2_000, "budget {budget}");
    }
}
