//! What a statement's plan did, returned beside its answer (nuthatch RFC-0073 FR4). The sealed
//! bytes it read are the figure a host may promise to repeat, so most of these run a statement more
//! than once and across engines.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine, QueryMetrics};
use parquet::file::properties::WriterProperties;

/// Eight segments of 4,096 rows in row groups of 512, so a filter on `block_number` can skip most.
fn nest(root: &Path) {
    let segs = root.join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(512))
        .build();
    for s in 0..8u64 {
        let rows = 4_096u64;
        let first = s * rows;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(first..first + rows)),
                Arc::new(StringArray::from_iter_values(
                    (first..first + rows).map(|n| format!("0x{:040x}", n % 97)),
                )),
                Arc::new(StringArray::from_iter_values(
                    (first..first + rows).map(|n| (n * 7 % 1_000).to_string()),
                )),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(segs.join(format!("transfer-{s:064x}.parquet"))).unwrap();
        let mut w =
            parquet::arrow::ArrowWriter::try_new(f, schema.clone(), Some(props.clone())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
}

fn engine(root: &Path) -> Engine {
    Engine::open_nest_budgeted(
        root,
        Budget {
            memory_bytes: 256 << 20,
            threads: 4,
            spill: None,
        },
    )
    .unwrap()
}

fn metered(e: &Engine, sql: &str) -> QueryMetrics {
    let mut m = QueryMetrics::default();
    e.sql_for_each_metered(sql, &mut m, |_| Ok(())).unwrap();
    m
}

#[test]
fn a_statement_the_caller_stops_still_reports_what_it_read() {
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let e = engine(tmp.path());
    let mut m = QueryMetrics::default();
    let r = e.sql_for_each_metered("SELECT * FROM transfer", &mut m, |_| {
        Err(burrmill::BurrmillError::LimitExceeded("cap".into()))
    });
    assert!(r.is_err());
    assert!(m.sealed_bytes_scanned > 0);
    assert!(m.output_rows > 0);
}

const STATEMENTS: &[&str] = &[
    "SELECT * FROM transfer",
    "SELECT who, COUNT(*) FROM transfer GROUP BY who ORDER BY who",
    "SELECT amount FROM transfer WHERE block_number BETWEEN 1000 AND 1100",
    "SELECT block_number, who FROM transfer ORDER BY amount DESC, block_number LIMIT 5",
    "SELECT t.who FROM transfer t JOIN transfer u ON t.block_number = u.block_number WHERE u.block_number < 600",
    "SELECT who, SUM(CAST(amount AS DECIMAL(38,0))) AS s FROM transfer GROUP BY who",
];

#[test]
fn sealed_bytes_repeat_for_the_same_statement_in_one_engine_and_across_engines() {
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let first = engine(tmp.path());
    let second = engine(tmp.path());
    for sql in STATEMENTS {
        let want = metered(&first, sql);
        assert!(want.sealed_bytes_scanned > 0, "{sql}: nothing counted");
        for _ in 0..10 {
            assert_eq!(
                metered(&first, sql).sealed_bytes_scanned,
                want.sealed_bytes_scanned,
                "{sql}, warm"
            );
        }
        assert_eq!(
            metered(&second, sql).sealed_bytes_scanned,
            want.sealed_bytes_scanned,
            "{sql}, a fresh engine"
        );
    }
}

#[test]
fn pruning_and_projection_read_less() {
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let e = engine(tmp.path());
    let all = metered(&e, "SELECT * FROM transfer");
    let one_column = metered(&e, "SELECT block_number FROM transfer");
    let pruned = metered(
        &e,
        "SELECT * FROM transfer WHERE block_number BETWEEN 1000 AND 1100",
    );
    assert!(one_column.sealed_bytes_scanned < all.sealed_bytes_scanned);
    assert!(pruned.sealed_bytes_scanned * 10 < all.sealed_bytes_scanned);
    assert_eq!(all.output_rows, 8 * 4_096);
    assert_eq!(pruned.output_rows, 101);
}

#[test]
fn the_owned_fold_counts_what_it_reads() {
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let e = engine(tmp.path());
    let sql = r#"SELECT addr, SUM(d) AS net FROM (
                   SELECT who AS addr, CAST(amount AS DECIMAL(38,0)) AS d FROM transfer
                   UNION ALL
                   SELECT who AS addr, -CAST(amount AS DECIMAL(38,0)) AS d FROM transfer
                 ) GROUP BY addr"#;
    let plan: String = e
        .sql(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .map(|b| format!("{b:?}"))
        .collect();
    assert!(
        plan.contains("OwnedSignedFold"),
        "not the owned fold:\n{plan}"
    );
    let m = metered(&e, sql);
    assert!(m.sealed_bytes_scanned > 0);
    assert_eq!(
        metered(&e, sql).sealed_bytes_scanned,
        m.sealed_bytes_scanned
    );
}

#[test]
fn a_statement_reports_its_own_peak_and_time() {
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let e = engine(tmp.path());
    let m = metered(
        &e,
        "SELECT who, amount, COUNT(*) FROM transfer GROUP BY who, amount ORDER BY 3 DESC",
    );
    assert!(m.peak_memory_bytes > 0);
    assert!(m.exec_nanos > 0);
    // The engine's own peak is unchanged by the statement's recorder.
    assert!(e.take_memory_peak() >= m.peak_memory_bytes as usize);
}

#[test]
fn sql_for_each_answers_as_before() {
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let e = engine(tmp.path());
    let mut rows = 0;
    e.sql_for_each("SELECT * FROM transfer WHERE block_number < 10", |b| {
        rows += b.num_rows();
        Ok(())
    })
    .unwrap();
    assert_eq!(rows, 10);
}
