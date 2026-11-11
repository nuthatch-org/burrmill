//! burrmill#152: the footers of local segments are cached once for the process, so the engine a host
//! opens at each seal parses none its predecessor read. The cache is the process's, so the file
//! holds one test.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine};

fn segment(dir: &Path, n: u64, rows: u64) -> (PathBuf, u64) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_number",
        DataType::UInt64,
        false,
    )]));
    let blocks: ArrayRef = Arc::new(UInt64Array::from_iter_values(0..rows));
    let path = dir.join(format!("t-{n:064x}.parquet"));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
    w.write(&RecordBatch::try_new(schema, vec![blocks]).unwrap())
        .unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

fn engine(memory_bytes: usize, files: &[(PathBuf, u64)]) -> Engine {
    let mut e = Engine::open_empty_budgeted(Budget {
        memory_bytes,
        threads: 2,
        spill: None,
    })
    .unwrap();
    e.register_facts("t", &[], files.to_vec(), &[], (None, None))
        .unwrap();
    e
}

fn count(e: &Engine) -> String {
    let rows = e.sql("SELECT count(*) AS n FROM t").unwrap();
    burrmill::df::encode::rows(&rows[0]).unwrap()[0].to_string()
}

#[test]
fn engines_share_the_footers_of_local_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut files: Vec<(PathBuf, u64)> = (0..120).map(|n| segment(dir.path(), n, 3)).collect();

    // burrmill#98: a statement keeps its footers for the next.
    let first = engine(256 << 20, &files);
    assert_eq!(first.footer_cache_bytes(), 0);
    assert_eq!(count(&first), r#"{"n":360}"#);
    let held = first.footer_cache_bytes();
    assert!(held > 0, "nothing cached after reading the segments");
    assert_eq!(count(&first), r#"{"n":360}"#);
    assert_eq!(first.footer_cache_bytes(), held);

    // The engine after a seal. The segments are no longer Parquet, so a footer it read again
    // would refuse the statement: a count needs nothing of a segment but its footer.
    let sealed: Vec<Vec<u8>> = files
        .iter()
        .map(|(p, _)| std::fs::read(p).unwrap())
        .collect();
    for (p, len) in &files {
        std::fs::write(p, vec![0u8; *len as usize]).unwrap();
    }
    let second = engine(256 << 20, &files);
    assert_eq!(second.footer_cache_bytes(), held);
    assert_eq!(count(&second), r#"{"n":360}"#);
    assert_eq!(second.footer_cache_bytes(), held);
    for ((p, _), bytes) in files.iter().zip(sealed) {
        std::fs::write(p, bytes).unwrap();
    }

    // A table that gained a segment reads that one footer and keeps the rest.
    files.push(segment(dir.path(), 120, 5));
    let grown = engine(256 << 20, &files);
    assert_eq!(count(&grown), r#"{"n":365}"#);
    let more = grown.footer_cache_bytes();
    assert!(more > held && more < held + held / 20, "{held} then {more}");

    // An entry answers for the size it was read at: the same path at another length is read anew.
    let replaced = segment(dir.path(), 0, 1000);
    assert_ne!(replaced.1, files[0].1);
    files[0] = replaced;
    assert_eq!(count(&engine(256 << 20, &files)), r#"{"n":1362}"#);

    // A smaller engine beside the others reads what they cached, and once they are gone the cache
    // holds what its own budget sets aside and no more.
    let small = engine(1 << 20, &files);
    assert!(small.footer_cache_bytes() > 128 << 10);
    assert_eq!(count(&small), r#"{"n":1362}"#);
    drop((first, second, grown));
    assert!(
        small.footer_cache_bytes() <= 128 << 10,
        "{} bytes of footers under a budget that allows 128 KiB",
        small.footer_cache_bytes()
    );
    assert_eq!(count(&small), r#"{"n":1362}"#);
}
