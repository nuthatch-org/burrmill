//! A `UNION ALL` answers its first arm's rows, then the next's, where nothing above it reorders, as
//! DuckDB's does (#430); under an aggregate, a join or a sort the union is left as it was.

use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use serde_json::Value;

fn segment(dir: &std::path::Path, n: u64) -> (std::path::PathBuf, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
    ]));
    let rows = 0..500u64;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                rows.clone().map(|i| n * 1000 + i),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.map(|i| format!("w{}", (i * 7 + n) % 97)),
            )),
        ],
    )
    .unwrap();
    let path = dir.join(format!("t-{n:064x}.parquet"));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

/// Eight segments, planned across the engine's threads however small, so a scan of `t` runs in
/// several partitions.
fn engine(dir: &std::path::Path) -> Engine {
    let declared =
        [("block_number", "u64"), ("who", "string")].map(|(c, t)| (c.to_string(), t.to_string()));
    let mut e = Engine::open_empty().unwrap();
    e.register_facts(
        "t",
        &declared,
        (1..=8).map(|n| segment(dir, n)).collect(),
        &[],
        (None, None),
    )
    .unwrap();
    e.set_small_statement_bytes(0);
    e
}

fn rows(e: &Engine, sql: &str) -> Vec<Value> {
    e.sql(sql)
        .unwrap()
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

fn column(e: &Engine, sql: &str, name: &str) -> Vec<Value> {
    rows(e, sql).into_iter().map(|r| r[name].clone()).collect()
}

fn plan(e: &Engine, sql: &str) -> String {
    let mut plan = String::new();
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        let kind = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let text = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        (0..text.len())
            .filter(|i| kind.value(*i) == "physical_plan")
            .for_each(|i| plan.push_str(text.value(i)));
    }
    plan
}

/// DuckDB's `setops/union_limit.test` and `setops/test_union_order.test`, run often enough that
/// arms coalesced as they arrive would have come back out of order.
#[test]
fn arms_answer_in_the_order_written() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    for _ in 0..20 {
        assert_eq!(
            column(
                &e,
                "SELECT 1 AS n UNION ALL SELECT * FROM range(2, 100) UNION ALL SELECT 999 LIMIT 5",
                "n"
            ),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(
            column(
                &e,
                "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT NULL UNION ALL SELECT 3",
                "n"
            ),
            [Value::from(1), Value::from(2), Value::Null, Value::from(3)]
        );
        assert_eq!(
            column(
                &e,
                "SELECT (SELECT NULL) AS b UNION ALL SELECT CAST(1 AS BOOLEAN)",
                "b"
            ),
            [Value::Null, Value::from(true)],
            "a scalar subquery above the union runs first and leaves its order"
        );
        assert_eq!(
            column(
                &e,
                "SELECT CAST(count(*) AS INT) AS n FROM t WHERE block_number < 2000 UNION ALL \
                 SELECT CAST(count(*) AS INT) FROM t UNION ALL \
                 SELECT CAST(min(block_number) AS INT) FROM t",
                "n"
            ),
            [500, 4000, 1000]
        );
        assert_eq!(
            column(
                &e,
                "WITH u AS (SELECT range AS i FROM range(3) UNION ALL SELECT range FROM range(3)) \
                 SELECT row_number() OVER () AS r, i FROM u",
                "i"
            ),
            [0, 1, 2, 0, 1, 2],
            "a window over no order reads the union's rows as they come"
        );
    }
}

/// An arm of several partitions still runs in all of them; its rows come between the arms around it.
#[test]
fn an_arm_of_several_partitions_comes_between_its_neighbours() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let sql = "SELECT 'first' AS w UNION ALL SELECT who FROM t UNION ALL SELECT 'last'";
    let p = plan(&e, sql);
    assert!(p.contains("OrderedUnionExec"), "{p}");
    assert!(
        p.contains("CoalescePartitionsExec"),
        "the arm is coalesced as before\n{p}"
    );
    for _ in 0..20 {
        let w = column(&e, sql, "w");
        assert_eq!(w.len(), 4002);
        assert_eq!(w[0], "first");
        assert_eq!(w[4001], "last");
        assert!(w[1..4001].iter().all(|v| v.as_str().unwrap().starts_with('w')));
    }
}

/// Where the union's order cannot reach the answer, its arms still run as partitions of one plan.
#[test]
fn a_union_under_an_aggregate_join_or_sort_is_left_alone() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    for sql in [
        "SELECT count(*) FROM (SELECT who FROM t UNION ALL SELECT who FROM t)",
        "SELECT who, count(*) FROM (SELECT who FROM t UNION ALL SELECT who FROM t) GROUP BY who",
        "SELECT u.who FROM (SELECT who FROM t UNION ALL SELECT who FROM t) u \
         JOIN (SELECT DISTINCT who FROM t) d ON u.who = d.who",
        "SELECT who FROM t UNION ALL SELECT who FROM t ORDER BY who",
    ] {
        let p = plan(&e, sql);
        assert!(!p.contains("OrderedUnionExec"), "{sql}\n{p}");
        assert!(p.contains("UnionExec"), "{sql}\n{p}");
    }
    let sorted = column(
        &e,
        "SELECT block_number FROM t WHERE block_number % 3 = 0 UNION ALL \
         SELECT block_number FROM t WHERE block_number % 3 = 1 ORDER BY block_number",
        "block_number",
    );
    assert!(sorted.windows(2).all(|w| w[0].as_u64() <= w[1].as_u64()));
}
