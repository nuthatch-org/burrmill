//! ShareRepeats over views that read views, as a nest's derivations are written.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

fn write(segs: &Path, table: &str, ids: &[&str], pos: &[u64]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("block_number", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids.to_vec())) as ArrayRef,
            Arc::new(UInt64Array::from(pos.to_vec())),
        ],
    )
    .unwrap();
    let f = std::fs::File::create(segs.join(format!("{table}-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// `placed` holds an aggregate and is read by `applied`, which holds one too and is read four
/// times by `bet`: `bs_bet` over `bs_applied` over `bs_placed`.
fn engine() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    write(&segs, "place", &["a", "b", "a", "c"], &[1, 2, 3, 4]);
    write(&segs, "roll", &["a", "b", "a", "c", "b"], &[2, 3, 5, 6, 7]);
    let mut e = Engine::open_segments(&segs).unwrap();
    e.register_view(
        "placed",
        "SELECT id, max(block_number) AS pos, count(*) AS n FROM place GROUP BY id",
    )
    .unwrap();
    e.register_view(
        "applied",
        "SELECT r.id, max(r.block_number) AS pos, min(p.pos) AS placed_at
         FROM roll r JOIN placed p ON p.id = r.id AND p.pos < r.block_number GROUP BY r.id",
    )
    .unwrap();
    e.register_view(
        "bet",
        "SELECT p.id, p.n, a1.pos AS last, a2.placed_at, a3.pos + a4.pos AS twice
         FROM placed p
         LEFT JOIN applied a1 ON a1.id = p.id
         LEFT JOIN applied a2 ON a2.id = p.id
         LEFT JOIN applied a3 ON a3.id = p.id
         LEFT JOIN applied a4 ON a4.id = p.id",
    )
    .unwrap();
    (tmp, e)
}

fn rows(e: &Engine, sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err}")) {
        let cols: Vec<ArrayRef> = b
            .columns()
            .iter()
            .map(|c| arrow::compute::cast(c, &DataType::Utf8).unwrap())
            .collect();
        for i in 0..b.num_rows() {
            let row: Vec<String> = cols
                .iter()
                .map(|c| {
                    let s = c.as_any().downcast_ref::<StringArray>().unwrap();
                    if s.is_null(i) {
                        "NULL".into()
                    } else {
                        s.value(i).into()
                    }
                })
                .collect();
            out.push(row.join("|"));
        }
    }
    out.sort();
    out
}

/// Scans that ran: `EXPLAIN ANALYZE` reports rows only for an operator that executed.
fn executed_scans(e: &Engine, table: &str, sql: &str) -> usize {
    let plan = rows(e, &format!("EXPLAIN ANALYZE {sql}")).join("\n");
    plan.lines()
        .filter(|l| l.contains("DataSourceExec") && l.contains(&format!("{table}-")))
        .filter(|l| l.contains("output_rows="))
        .count()
}

#[test]
fn a_view_repeated_inside_a_repeated_view_is_read_once() {
    let (_t, e) = engine();
    let sql = "SELECT * FROM bet";
    assert_eq!(
        rows(&e, sql),
        vec!["a|2|5|3|10", "b|1|7|2|14", "c|1|6|4|12"]
    );
    assert_eq!(executed_scans(&e, "place", sql), 1);
    assert_eq!(executed_scans(&e, "roll", sql), 1);
}

/// Scans in the physical plan, whether or not they run.
fn planned_scans(e: &Engine, table: &str, sql: &str) -> usize {
    let plan = rows(e, &format!("EXPLAIN {sql}")).join("\n");
    plan.lines()
        .filter(|l| l.contains("DataSourceExec") && l.contains(&format!("{table}-")))
        .count()
}

/// #92: each copy of a shared subquery was analysed, optimised and planned again, so planning grew
/// with how often a view was reached. Each is planned once now, however often it is read.
#[test]
fn a_shared_subquery_is_planned_once_however_often_it_is_read() {
    let (_t, e) = engine();
    assert_eq!(planned_scans(&e, "place", "SELECT * FROM bet"), 1);
    assert_eq!(planned_scans(&e, "roll", "SELECT * FROM bet"), 1);
    let twice = "SELECT x.id, y.n FROM bet x JOIN bet y ON x.id = y.id";
    assert_eq!(rows(&e, twice), vec!["a|2", "b|1", "c|1"]);
    assert_eq!(planned_scans(&e, "place", twice), 1);
    assert_eq!(planned_scans(&e, "roll", twice), 1);
}

/// A statement's ORDER BY holds over a shared subquery. With the definitions beside it under one
/// node, DataFusion never recorded the statement's ordering and dropped its sort.
#[test]
fn a_statement_over_a_shared_subquery_keeps_its_order() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let ids: Vec<String> = (0..50_000u64)
        .map(|i| format!("k{:05}", (i * 7919) % 20_000))
        .collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let pos: Vec<u64> = (0..50_000).collect();
    write(&segs, "place", &ids, &pos);
    let mut e = Engine::open_segments(&segs).unwrap();
    e.register_view(
        "placed",
        "SELECT id, max(block_number) AS pos, count(*) AS n FROM place GROUP BY id",
    )
    .unwrap();
    let sql = "SELECT x.id, x.n, y.pos FROM placed x JOIN placed y ON x.id = y.id \
               ORDER BY x.pos DESC";
    let pos: Vec<u64> = e
        .sql(sql)
        .unwrap()
        .iter()
        .flat_map(|b| {
            b.column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(pos.len(), 20_000);
    assert!(pos.windows(2).all(|w| w[0] >= w[1]), "not in order");
}

/// #97: a shared subquery was computed whole, every column of it, whatever its readers took.
#[test]
fn a_shared_subquery_computes_only_what_its_readers_take() {
    let (_t, e) = engine();
    let sql = "SELECT x.id, y.n FROM placed x JOIN placed y ON x.id = y.id ORDER BY x.id";
    assert_eq!(rows(&e, sql), vec!["a|2", "b|1", "c|1"]);
    assert_eq!(planned_scans(&e, "place", sql), 1);
    let plan = rows(&e, &format!("EXPLAIN {sql}")).join("\n");
    assert!(
        !plan.contains("max("),
        "the unread `pos` is still computed:\n{plan}"
    );
}

#[test]
fn a_shared_subquery_read_for_no_column_still_has_its_rows() {
    let (_t, e) = engine();
    let sql = "SELECT a.c + b.c FROM (SELECT count(*) AS c FROM placed) a, \
               (SELECT count(*) AS c FROM placed) b";
    assert_eq!(rows(&e, sql), vec!["6"]);
    assert_eq!(planned_scans(&e, "place", sql), 1);
}

/// Rows a table's scan produced, from `EXPLAIN ANALYZE`.
fn scanned_rows(e: &Engine, table: &str, sql: &str) -> usize {
    let plan = rows(e, &format!("EXPLAIN ANALYZE {sql}")).join("\n");
    plan.lines()
        .filter(|l| l.contains("DataSourceExec") && l.contains(&format!("{table}-")))
        .filter_map(|l| l.split("output_rows=").nth(1))
        .map(|r| {
            r.split(|c: char| !c.is_ascii_digit())
                .next()
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })
        .sum()
}

/// #97: a filter every reader of a shared subquery applies reaches inside it.
#[test]
fn a_filter_every_reader_applies_reaches_the_shared_scan() {
    let (_t, e) = engine();
    let sql = "SELECT x.id, y.n FROM placed x JOIN placed y ON x.id = y.id WHERE x.id = 'a'";
    assert_eq!(rows(&e, sql), vec!["a|2"]);
    assert_eq!(
        scanned_rows(&e, "place", sql),
        2,
        "the shared scan read every id"
    );
}

/// #99: a repeated subquery holding a window, and no aggregate, was computed at every reach.
#[test]
fn a_repeated_window_is_computed_once() {
    let (_t, mut e) = engine();
    e.register_view(
        "running",
        "SELECT id, block_number, sum(block_number) OVER (PARTITION BY id ORDER BY block_number) AS run
         FROM roll",
    )
    .unwrap();
    let sql = "SELECT a.id, a.block_number, a.run, b.run FROM running a
               JOIN running b ON a.id = b.id AND a.block_number = b.block_number
               ORDER BY a.id, a.block_number";
    assert_eq!(
        rows(&e, sql),
        vec!["a|2|2|2", "a|5|7|7", "b|3|3|3", "b|7|10|10", "c|6|6|6"]
    );
    assert_eq!(executed_scans(&e, "roll", sql), 1);
}
