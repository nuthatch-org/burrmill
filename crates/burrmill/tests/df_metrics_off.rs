//! An unmetered statement pays nothing for its metrics: no selection pass over the footers. One test
//! in its own binary, since the pass count is process-wide.

use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Engine, QueryMetrics};

#[test]
fn only_a_metered_statement_runs_the_selection_pass() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_number",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from_iter_values(0..100))],
    )
    .unwrap();
    let f = std::fs::File::create(segs.join(format!("t-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let e = Engine::open_nest(tmp.path()).unwrap();
    let sql = "SELECT * FROM t WHERE block_number < 10";

    let before = burrmill::df::selection_passes();
    assert_eq!(
        e.sql(sql)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        10
    );
    e.sql_for_each(sql, |_| Ok(())).unwrap();
    assert_eq!(burrmill::df::selection_passes(), before, "unmetered");

    let mut m = QueryMetrics::default();
    e.sql_for_each_metered(sql, &mut m, |_| Ok(())).unwrap();
    assert_eq!(burrmill::df::selection_passes(), before + 1, "metered");
    assert!(m.sealed_bytes_scanned > 0);
}
