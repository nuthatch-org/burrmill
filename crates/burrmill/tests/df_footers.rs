//! A segment's footer is read once per process, not once per engine: a host opens a new engine
//! whenever its sealed watermark moves, and a catalogue read binds every table (nuthatch#2669).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

fn segment(dir: &std::path::Path, i: u8, cols: &[(&str, ArrayRef)]) -> (std::path::PathBuf, u64) {
    let schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        cols.iter().map(|(_, a)| a.clone()).collect(),
    )
    .unwrap();
    let path = dir.join(format!("t-{i:064x}.parquet"));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

fn bind(files: &[(std::path::PathBuf, u64)]) -> Vec<String> {
    let mut e = Engine::open_empty().unwrap();
    e.register_facts("t", &[], files.to_vec(), &[], (None, None))
        .unwrap();
    e.sql("SELECT block_number, extra FROM t ORDER BY block_number")
        .unwrap()
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .map(|r| r.to_string())
        .collect()
}

#[test]
fn a_second_engine_reads_no_footer_and_answers_the_same() {
    let dir = tempfile::tempdir().unwrap();
    let n = |v: i64| -> ArrayRef { Arc::new(Int64Array::from(vec![v])) };
    // Two segments of one schema, then one with a column the ABI gained.
    let files = vec![
        segment(dir.path(), 1, &[("block_number", n(1))]),
        segment(dir.path(), 2, &[("block_number", n(2))]),
        segment(
            dir.path(),
            3,
            &[
                ("block_number", n(3)),
                ("extra", Arc::new(StringArray::from(vec!["x"]))),
            ],
        ),
    ];
    let before = burrmill::df::footer_reads();
    let first = bind(&files);
    assert_eq!(burrmill::df::footer_reads() - before, 3);
    let second = bind(&files);
    assert_eq!(
        burrmill::df::footer_reads() - before,
        3,
        "the second engine read a footer"
    );
    assert_eq!(first, second);
    assert_eq!(first.len(), 3);
    assert!(first[2].contains("\"x\""), "{first:?}");
    assert!(first[0].contains("null"), "{first:?}");
}
