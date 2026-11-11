//! burrmill#153: a table bound again to the files it already has reuses the list its scans read,
//! on the same engine or another, and a changed file set builds a new one. The count of lists
//! built is the process's, so the file holds one test.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

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

fn bind(e: &mut Engine, files: &[(PathBuf, u64)]) -> String {
    e.register_facts("t", &[], files.to_vec(), &[], (None, None))
        .unwrap();
    let rows = e.sql("SELECT count(*) AS n FROM t").unwrap();
    burrmill::df::encode::rows(&rows[0]).unwrap()[0].to_string()
}

#[test]
fn a_table_bound_to_the_same_files_reuses_its_list() {
    let dir = tempfile::tempdir().unwrap();
    let mut files: Vec<(PathBuf, u64)> = (0..8).map(|n| segment(dir.path(), n, 3)).collect();
    let built = || burrmill::df::file_lists_built();
    let start = built();

    let mut first = Engine::open_empty().unwrap();
    assert_eq!(bind(&mut first, &files), r#"{"n":24}"#);
    assert_eq!(built() - start, 1);
    // The bind before each statement, and the engine opened beside this one at a seal.
    assert_eq!(bind(&mut first, &files), r#"{"n":24}"#);
    let mut second = Engine::open_empty().unwrap();
    assert_eq!(bind(&mut second, &files), r#"{"n":24}"#);
    assert_eq!(built() - start, 1, "the same files were listed again");

    // A sealed segment more is another file set, and so is the same set less its first file,
    // which is what the lists are found by.
    files.push(segment(dir.path(), 8, 5));
    assert_eq!(bind(&mut second, &files), r#"{"n":29}"#);
    assert_eq!(built() - start, 2);
    assert_eq!(bind(&mut second, &files[1..]), r#"{"n":26}"#);
    assert_eq!(built() - start, 3);
    // The first engine still reads the eight it was bound to, and binding them again finds them.
    assert_eq!(bind(&mut first, &files[..8]), r#"{"n":24}"#);
    assert_eq!(built() - start, 3);

    // The same path at another length is another file.
    let mut longer = files[..8].to_vec();
    longer[7] = segment(dir.path(), 7, 100);
    assert_ne!(longer[7].1, files[7].1);
    assert_eq!(bind(&mut first, &longer), r#"{"n":121}"#);
    assert_eq!(built() - start, 4);

    // A list lives as long as a table is bound to it, and no longer.
    drop((first, second));
    let mut third = Engine::open_empty().unwrap();
    assert_eq!(bind(&mut third, &longer), r#"{"n":121}"#);
    assert_eq!(built() - start, 5);
}
