//! A segment rewritten at the same path and length is costed as it is now, not as it was: the
//! column sizes and footers a metered statement keeps are stamped with each file's modification
//! time.

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Engine, QueryMetrics};
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

fn write(path: &std::path::Path, a: usize, b: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(
                (0..1000).map(|_| "x".repeat(a)),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..1000).map(|_| "x".repeat(b)),
            )),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let f = std::fs::File::create(path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn metered(e: &Engine, sql: &str) -> u64 {
    let mut m = QueryMetrics::default();
    e.sql_for_each_metered(sql, Some(&mut m), |_| Ok(()))
        .unwrap();
    m.sealed_bytes_scanned
}

#[test]
fn a_same_length_rewrite_is_costed_afresh() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    let p = segs.join(format!("ev-{:064x}.parquet", 1));
    write(&p, 10, 50);
    let len1 = std::fs::metadata(&p).unwrap().len();
    let first = metered(
        &Engine::open_segments(&segs).unwrap(),
        "SELECT a FROM ev LIMIT 1",
    );
    std::thread::sleep(std::time::Duration::from_millis(1100));
    write(&p, 50, 10);
    let len2 = std::fs::metadata(&p).unwrap().len();
    assert_eq!(len1, len2, "lengths differ; fixture does not test the hole");
    // A fresh engine over the replaced file, as a second engine in the process would be.
    let second = metered(
        &Engine::open_segments(&segs).unwrap(),
        "SELECT a FROM ev LIMIT 1",
    );
    eprintln!("a selected {first} bytes, then {second} after a same-length rewrite");
    assert_ne!(second, first, "stale figure: {first} then {second}");
}

fn budget() -> burrmill::Budget {
    burrmill::Budget {
        memory_bytes: 512 << 10,
        threads: 1,
        spill: None,
    }
}

#[test]
fn a_rewrite_after_its_footer_left_the_cache_is_costed_afresh() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    let p = segs.join(format!("ev-{:064x}.parquet", 1));
    write(&p, 10, 50);
    // Eight tables, so no one statement holds more segments' file lists than the budget allows.
    const FILLS: [&str; 8] = [
        "filla", "fillb", "fillc", "filld", "fille", "fillf", "fillg", "fillh",
    ];
    for t in FILLS {
        for s in 0..75u64 {
            write(&segs.join(format!("{t}-{s:064x}.parquet")), 3, 3);
        }
    }
    let len1 = std::fs::metadata(&p).unwrap().len();
    let e1 = Engine::open_nest_budgeted(tmp.path(), budget()).unwrap();
    let first = metered(&e1, "SELECT a FROM ev");
    // Push ev's footer out of the process's footer cache.
    for t in FILLS {
        metered(&e1, &format!("SELECT a, b FROM {t}"));
    }
    drop(e1);
    std::thread::sleep(std::time::Duration::from_millis(1100));
    write(&p, 50, 10);
    assert_eq!(len1, std::fs::metadata(&p).unwrap().len());
    let e2 = Engine::open_nest_budgeted(tmp.path(), budget()).unwrap();
    let mut m = QueryMetrics::default();
    let r = e2.sql_for_each_metered("SELECT a FROM ev", Some(&mut m), |_| Ok(()));
    eprintln!(
        "first {first}, after rewrite {} ({:?})",
        m.sealed_bytes_scanned,
        r.err()
    );
    assert_ne!(m.sealed_bytes_scanned, first, "stale figure");
}
