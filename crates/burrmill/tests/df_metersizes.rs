//! A metered statement's selected bytes without the footers it has already read once. The footer
//! reads are counted process-wide, so the file holds one test.

use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine, QueryMetrics};

fn metered(e: &Engine, sql: &str) -> QueryMetrics {
    let mut m = QueryMetrics::default();
    e.sql_for_each_metered(sql, Some(&mut m), |_| Ok(()))
        .unwrap();
    m
}

/// A metered statement with nothing to prune by is costed from each segment's column sizes, which
/// are kept apart from the footers, so a `LIMIT` over more segments than the footer cache holds does
/// not decode every footer again on each run.
#[test]
fn a_metered_limit_reads_no_footer_it_has_read_before() {
    const SEGMENTS: u64 = 400;
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
    ]));
    let mut selected = 0u64;
    for s in 0..SEGMENTS {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values([s])),
                Arc::new(StringArray::from_iter_values([format!("0x{s:040x}")])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(segs.join(format!("ev-{s:064x}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        let meta = w.close().unwrap();
        selected += meta
            .row_groups()
            .iter()
            .map(|g| g.column(0).compressed_size() as u64)
            .sum::<u64>();
    }
    // A footer cache of 64 KiB holds a few of the 400 footers.
    let e = Engine::open_nest_budgeted(
        tmp.path(),
        Budget {
            memory_bytes: 8 << 20,
            threads: 1,
            spill: None,
        },
    )
    .unwrap();
    let sql = "SELECT id FROM ev LIMIT 1";
    let first = metered(&e, sql);
    assert_eq!(first.sealed_bytes_scanned, selected);
    let before = burrmill::df::selection_footer_reads();
    let again = metered(&e, sql);
    assert_eq!(again.sealed_bytes_scanned, selected);
    assert_eq!(again.output_rows, 1);
    let read = burrmill::df::selection_footer_reads() - before;
    assert!(
        read < SEGMENTS / 10,
        "{read} footers read again for {SEGMENTS} segments"
    );

    // A host that binds the files has had every footer read already, so even the first run reads
    // none. Moved, so no footer above is known by its location.
    let moved = tmp.path().join("bound");
    std::fs::rename(&segs, &moved).unwrap();
    let files: Vec<_> = std::fs::read_dir(&moved)
        .unwrap()
        .map(|d| {
            let p = d.unwrap().path();
            let len = std::fs::metadata(&p).unwrap().len();
            (p, len)
        })
        .collect();
    let mut e = Engine::open_empty_budgeted(Budget {
        memory_bytes: 8 << 20,
        threads: 1,
        spill: None,
    })
    .unwrap();
    let declared = vec![("id".into(), "u64".into()), ("who".into(), "string".into())];
    e.register_facts("ev", &declared, files, &[], (None, None))
        .unwrap();
    let before = burrmill::df::selection_footer_reads();
    let bound = metered(&e, sql);
    assert_eq!(bound.sealed_bytes_scanned, selected);
    let read = burrmill::df::selection_footer_reads() - before;
    assert!(
        read < SEGMENTS / 10,
        "{read} footers read for {SEGMENTS} bound segments"
    );
}
