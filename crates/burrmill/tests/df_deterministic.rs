//! Deterministic mode (#340): one statement over the same rows answers the same Arrow IPC bytes at
//! 1, 2, 8 and 32 threads over three splits of the rows into segments. Without the mode some of
//! these statements answer in a different order, which is what makes the comparison a test.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine};
use parquet::file::properties::WriterProperties;

const ROWS: i64 = 6_000;

/// The same rows cut into `segments` files of row groups of `group` rows.
fn layout(dir: &Path, segments: usize, group: usize) {
    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
        Field::new("x", DataType::Float64, true),
    ]));
    let per = (ROWS as usize).div_ceil(segments);
    for s in 0..segments {
        let lo = (s * per) as i64;
        let hi = ((s + 1) * per).min(ROWS as usize) as i64;
        let ids: Vec<i64> = (lo..hi).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from_iter(ids.iter().map(|i| match i % 11 {
                    0 => None,
                    n => Some(format!("key-with-a-long-name-{}", n % 5)),
                }))),
                Arc::new(Int64Array::from_iter(
                    ids.iter().map(|i| (i % 13 != 0).then_some(i % 7)),
                )),
                Arc::new(Float64Array::from_iter(ids.iter().map(|i| {
                    (i % 17 != 0).then_some((*i as f64) * 0.1 + 1e15 * ((i % 3) as f64))
                }))),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(group))
            .build();
        let f = std::fs::File::create(dir.join(format!("t-{s:064x}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
}

fn engines(root: &Path, deterministic: bool) -> Vec<(String, Engine)> {
    let mut out = Vec::new();
    for (i, (segments, group)) in [(1, 6_000), (4, 700), (13, 97)].iter().enumerate() {
        let dir = root.join(format!("layout{i}"));
        if !dir.exists() {
            layout(&dir, *segments, *group);
        }
        for threads in [1, 2, 8, 32] {
            let e = Engine::builder()
                .segments(&dir)
                .budget(Budget {
                    memory_bytes: 512 << 20,
                    threads,
                    spill: None,
                })
                .open()
                .unwrap();
            e.set_deterministic(deterministic);
            out.push((format!("{segments} segments at {threads} threads"), e));
        }
    }
    out
}

const STATEMENTS: &[&str] = &[
    "SELECT * FROM t",
    "SELECT k, COUNT(*), SUM(v), SUM(x), AVG(x), MIN(x), MAX(x) FROM t GROUP BY k",
    "SELECT v, k FROM t ORDER BY v LIMIT 50",
    "SELECT * FROM t LIMIT 30",
    "SELECT * FROM t ORDER BY v DESC LIMIT 25 OFFSET 7",
    "SELECT k, row_number() OVER (PARTITION BY k ORDER BY v) AS n, id FROM t",
    "SELECT id, lag(id) OVER (ORDER BY v) AS before FROM t",
    "SELECT k, array_agg(id) AS ids FROM t WHERE id < 400 GROUP BY k",
    "SELECT k, string_agg(CAST(id AS VARCHAR), ',') AS ids FROM t WHERE id < 300 GROUP BY k",
    "SELECT k, first_value(id) AS f FROM t GROUP BY k",
    "SELECT v, SUM(x) OVER (ORDER BY v ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM t",
    "SELECT a.k, COUNT(*) FROM t a JOIN t b ON a.v = b.v AND a.id < 200 AND b.id < 200 GROUP BY a.k",
    "SELECT k FROM t UNION ALL SELECT k FROM t WHERE v = 3",
    "SELECT DISTINCT k, v FROM t",
    "SELECT * FROM (SELECT id, v FROM t LIMIT 40) s WHERE v > 2",
    "SELECT k, quantile_cont(x, 0.5) AS m, median(v) AS mv FROM t GROUP BY k",
    // NULLs inside each list, whose child values the plan leaves as it likes.
    "SELECT k, array_agg(v) AS vs FROM t WHERE id < 500 GROUP BY k",
    // -0.0 and 0.0 tie in the sort, so the LIMIT may keep either.
    "SELECT round(sign(v - 3) / 7, 0) AS z FROM t ORDER BY 1 DESC LIMIT 900",
];

const REFUSED: &[(&str, &str)] = &[
    ("SELECT stddev(x) FROM t", "stddev"),
    ("SELECT k, var_pop(x) FROM t GROUP BY k", "var_pop"),
    ("SELECT corr(x, v) FROM t", "corr"),
    ("SELECT approx_median(x) FROM t", "approx_median"),
    ("SELECT random() FROM t", "random"),
    ("SELECT now()", "now"),
    (
        "SELECT first_value(id) OVER (PARTITION BY k ORDER BY v) FROM t",
        "ROWS frame",
    ),
];

#[test]
fn every_thread_count_and_layout_answers_the_same_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let engines = engines(tmp.path(), true);
    for sql in STATEMENTS {
        let (first, want) = (&engines[0].0, engines[0].1.sql_ipc(sql).unwrap());
        for (name, e) in &engines[1..] {
            let got = e.sql_ipc(sql).unwrap();
            assert!(
                got == want,
                "{sql}: {name} answered other bytes than {first}"
            );
        }
    }
}

#[test]
fn what_cannot_be_made_deterministic_is_named_and_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("one");
    layout(&dir, 1, 1_000);
    let e = Engine::open_segments(&dir).unwrap();
    for (sql, _) in REFUSED {
        assert!(e.sql(sql).is_ok(), "{sql} answers without the mode");
    }
    e.set_deterministic(true);
    for (sql, says) in REFUSED {
        let m = e.sql(sql).unwrap_err().to_string();
        assert!(
            m.contains("deterministic mode") && m.contains(says),
            "{sql}: {m}"
        );
    }
    // Exact over floats whatever the partitioning, so answered.
    assert!(e.sql("SELECT SUM(x), AVG(x) FROM t").is_ok());
}

#[test]
fn without_the_mode_the_same_statements_do_differ() {
    let tmp = tempfile::tempdir().unwrap();
    let engines = engines(tmp.path(), false);
    let differs = STATEMENTS.iter().any(|sql| {
        let want = engines[0].1.sql_ipc(sql).unwrap();
        engines[1..]
            .iter()
            .any(|(_, e)| e.sql_ipc(sql).unwrap() != want)
    });
    assert!(differs, "the comparison would pass without the mode");
}

#[test]
fn an_answer_is_cut_into_batches_of_a_fixed_size() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("one");
    layout(&dir, 13, 97);
    let e = Engine::open_segments(&dir).unwrap();
    e.set_deterministic(true);
    let sizes: Vec<usize> = e
        .sql("SELECT * FROM t")
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .collect();
    assert_eq!(sizes, [ROWS as usize]);
    let empty = e.sql("SELECT * FROM t WHERE id < 0").unwrap();
    assert_eq!(empty.len(), 1);
    assert_eq!(empty[0].num_rows(), 0);
}

/// The determinism oracle's seed 6120: the top rows of the left join's preserved side, which
/// `NarrowJoins` computes once, are sorted per partition by `abs(abs(..))` and merged, and the
/// sanity check found the merge's order unmet and refused the statement at two threads and up.
#[test]
fn a_page_ordered_by_a_nested_abs_answers_the_same_at_every_thread_count() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ev");
    std::fs::create_dir_all(&dir).unwrap();
    let docs = [
        Some(r#"{"a": 1}"#),
        Some(r#"{"a": "7"}"#),
        Some("not json"),
        None,
    ];
    let addrs = [Some("0xa"), Some("0xb"), Some("0xc"), None];
    let n = 120u64;
    let ev = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("from", DataType::Utf8, true),
            Field::new("doc", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(UInt64Array::from_iter_values((0..n).map(|i| 1 + i / 3))),
            Arc::new(StringArray::from_iter(
                (0..n).map(|i| addrs[(i * 7 % 4) as usize]),
            )),
            Arc::new(StringArray::from_iter(
                (0..n).map(|i| docs[(i * 5 % 4) as usize]),
            )),
        ],
    )
    .unwrap();
    let lbl = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("addr", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["0xa", "0xa", "0xb"]))],
    )
    .unwrap();
    for (name, b) in [("ev", ev), ("lbl", lbl)] {
        let f = std::fs::File::create(dir.join(format!("{name}-{:064x}.parquet", 1))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, b.schema(), None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
    }
    let sql = "SELECT abs(abs(e.block_number)) * -0.5 AS c0, \
               upper(rpad(TRY(e.doc ->> 'a'), 1, '*')) AS c1 \
               FROM ev e LEFT JOIN lbl l ON l.addr = e.\"from\" \
               ORDER BY 1 DESC NULLS FIRST, 2 DESC NULLS LAST LIMIT 20 OFFSET 1";
    let answers: Vec<Vec<u8>> = [1, 2, 8]
        .into_iter()
        .map(|threads| {
            let e = Engine::builder()
                .segments(&dir)
                .budget(Budget {
                    memory_bytes: 512 << 20,
                    threads,
                    spill: None,
                })
                .open()
                .unwrap();
            e.set_deterministic(true);
            e.sql_ipc(sql)
                .unwrap_or_else(|err| panic!("{threads} threads: {err}"))
        })
        .collect();
    assert!(answers.iter().all(|a| *a == answers[0]));
}
