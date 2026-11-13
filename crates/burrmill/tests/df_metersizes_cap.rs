//! Past its limit the map of kept column sizes keeps what it holds rather than starting again, so a
//! nest larger than it still has those segments costed without their footers. The footer reads are
//! counted process-wide, so the file holds one test.

use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine, QueryMetrics};

#[test]
fn a_full_map_keeps_the_segments_it_holds() {
    const SEGMENTS: u64 = 400;
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
    for s in 0..SEGMENTS {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from_iter_values([s]))],
        )
        .unwrap();
        let f = std::fs::File::create(segs.join(format!("ev-{s:064x}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    // An eighth of 1 MiB for local footers, a quarter of that for the sizes: a few dozen segments.
    let e = Engine::open_nest_budgeted(
        tmp.path(),
        Budget {
            memory_bytes: 1 << 20,
            threads: 1,
            spill: None,
        },
    )
    .unwrap();
    let run = || {
        let before = burrmill::df::selection_footer_reads();
        let mut m = QueryMetrics::default();
        e.sql_for_each_metered("SELECT id FROM ev LIMIT 1", Some(&mut m), |_| Ok(()))
            .unwrap();
        (
            m.sealed_bytes_scanned,
            burrmill::df::selection_footer_reads() - before,
        )
    };
    let (first, _) = run();
    let (second, read) = run();
    let (third, again) = run();
    assert_eq!((first, second), (third, third));
    assert!(
        read > 0,
        "the map held all {SEGMENTS}: the fixture does not fill it"
    );
    assert!(read < SEGMENTS, "the map kept none of {SEGMENTS} segments");
    assert_eq!(read, again, "the map changed between runs");
}
