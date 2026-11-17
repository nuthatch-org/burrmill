//! Any Parquet a host names, opened as one table: arbitrary file names, nested directories, hive
//! partitions, and a prefix in an object store, each pruned as a nest's segments are (#338).

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, BurrmillError, Engine, QueryMetrics};
use parquet::file::properties::WriterProperties;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("who", DataType::Utf8, false),
    ]))
}

/// `rows` rows from `first`, in row groups of 256.
fn bytes(first: i64, rows: i64) -> Vec<u8> {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(first..first + rows)),
            Arc::new(StringArray::from_iter_values(
                (first..first + rows).map(|n| format!("w{}", n % 7)),
            )),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(256))
        .build();
    let mut out = Vec::new();
    let mut w = parquet::arrow::ArrowWriter::try_new(&mut out, schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    out
}

fn write(root: &Path, rel: &str, first: i64, rows: i64) {
    let p = root.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, bytes(first, rows)).unwrap();
}

fn one(e: &Engine, sql: &str) -> String {
    let b = e.sql(sql).unwrap();
    arrow::util::pretty::pretty_format_batches(&b)
        .unwrap()
        .to_string()
}

fn count(e: &Engine, sql: &str) -> i64 {
    let b = e.sql(&format!("SELECT CAST(({sql}) AS BIGINT)")).unwrap();
    let c = b[0].column(0);
    c.as_any().downcast_ref::<Int64Array>().unwrap().value(0)
}

fn metered(e: &Engine, sql: &str) -> QueryMetrics {
    let mut m = QueryMetrics::default();
    e.sql_for_each_metered(sql, Some(&mut m), |_| Ok(()))
        .unwrap();
    m
}

fn budget() -> Budget {
    Budget {
        memory_bytes: 256 << 20,
        threads: 2,
        spill: None,
    }
}

#[test]
fn a_directory_of_any_names_is_one_table_and_markers_are_skipped() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "part-00000.snappy.parquet", 0, 1_000);
    write(tmp.path(), "nested/deeper/trades.parquet", 1_000, 500);
    std::fs::write(tmp.path().join("_SUCCESS"), b"").unwrap();
    std::fs::write(tmp.path().join("_metadata.parquet"), b"not parquet").unwrap();
    std::fs::write(tmp.path().join(".part-0.parquet.crc"), b"").unwrap();
    let e = Engine::builder()
        .parquet(tmp.path(), "trades")
        .budget(budget())
        .open()
        .unwrap();
    assert_eq!(count(&e, "SELECT COUNT(*) FROM trades"), 1_500);
    assert_eq!(
        count(&e, "SELECT SUM(id) FROM trades"),
        (0..1_500).sum::<i64>()
    );

    let file = Engine::builder()
        .parquet(&tmp.path().join("nested/deeper/trades.parquet"), "t")
        .open()
        .unwrap();
    assert_eq!(count(&file, "SELECT MIN(id) FROM t"), 1_000);
}

#[test]
fn hive_directories_are_columns_and_a_filter_on_one_reads_only_its_files() {
    let tmp = tempfile::tempdir().unwrap();
    for (i, (year, region)) in [(2023, "eu"), (2024, "eu"), (2024, "us"), (2025, "us")]
        .iter()
        .enumerate()
    {
        write(
            tmp.path(),
            &format!("year={year}/region={region}/data-{i}.parquet"),
            i as i64 * 1_000,
            1_000,
        );
    }
    let e = Engine::builder()
        .parquet(tmp.path(), "t")
        .budget(budget())
        .open()
        .unwrap();
    assert_eq!(
        e.describe("SELECT * FROM t").unwrap(),
        [
            ("id".to_string(), "BIGINT".to_string()),
            ("who".into(), "VARCHAR".into()),
            ("year".into(), "BIGINT".into()),
            ("region".into(), "VARCHAR".into()),
        ]
    );
    assert_eq!(count(&e, "SELECT COUNT(*) FROM t WHERE year = 2024"), 2_000);
    assert_eq!(
        count(
            &e,
            "SELECT MIN(id) FROM t WHERE year = 2024 AND region = 'us'"
        ),
        2_000
    );
    let grouped = one(
        &e,
        "SELECT region, COUNT(*) AS n FROM t GROUP BY region ORDER BY region",
    );
    assert!(
        grouped.contains("| eu     | 2000 |") && grouped.contains("| us     | 2000 |"),
        "{grouped}"
    );

    let all = metered(&e, "SELECT SUM(id) FROM t");
    let one_file = metered(&e, "SELECT SUM(id) FROM t WHERE year = 2025");
    assert!(
        one_file.sealed_bytes_read * 3 < all.sealed_bytes_read,
        "{} of {}",
        one_file.sealed_bytes_read,
        all.sealed_bytes_read
    );
    assert!(
        one_file.sealed_bytes_scanned * 3 < all.sealed_bytes_scanned,
        "{one_file:?} {all:?}"
    );
}

#[test]
fn row_groups_a_filter_excludes_are_not_read() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "a.parquet", 0, 8_192);
    let e = Engine::builder()
        .parquet(tmp.path(), "t")
        .budget(budget())
        .open()
        .unwrap();
    let all = metered(&e, "SELECT SUM(id) FROM t");
    let few = metered(&e, "SELECT SUM(id) FROM t WHERE id BETWEEN 100 AND 300");
    assert!(
        few.sealed_bytes_scanned * 8 < all.sealed_bytes_scanned,
        "{} of {}",
        few.sealed_bytes_scanned,
        all.sealed_bytes_scanned
    );
    assert!(few.sealed_bytes_read * 8 < all.sealed_bytes_read);
}

#[test]
fn a_prefix_in_a_store_is_a_table_and_two_stores_do_not_meet() {
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as Key};
    let put = |s: &object_store::memory::InMemory, k: &str, first, rows| {
        futures::executor::block_on(s.put(
            &Key::parse(k).unwrap(),
            PutPayload::from(bytes(first, rows)),
        ))
        .unwrap();
    };
    let a = object_store::memory::InMemory::new();
    put(&a, "lake/trades/day=2024-01-01/x.parquet", 0, 100);
    put(&a, "lake/trades/day=2024-01-02/y.parquet", 100, 100);
    put(&a, "lake/other/z.parquet", 0, 5);
    let b = object_store::memory::InMemory::new();
    put(&b, "lake/trades/day=2024-01-01/x.parquet", 0, 7);
    let a: Arc<dyn ObjectStore> = Arc::new(a);
    let b: Arc<dyn ObjectStore> = Arc::new(b);

    let r = Engine::builder()
        .parquet_store(a.clone(), "lake/trades", "t")
        .open();
    assert!(matches!(r, Err(BurrmillError::Config(_))), "{:?}", r.err());

    let e = futures::executor::block_on(
        Engine::builder()
            .parquet_store(a, "lake/trades", "t")
            .parquet_store(b, "lake/trades", "u")
            .budget(budget())
            .open_async(),
    )
    .unwrap();
    assert_eq!(count(&e, "SELECT COUNT(*) FROM t"), 200);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM u"), 7);
    assert_eq!(
        count(&e, "SELECT COUNT(*) FROM t WHERE day = DATE '2024-01-02'"),
        100
    );
    assert_eq!(
        e.describe("SELECT day FROM t").unwrap()[0].1,
        "DATE".to_string()
    );
}

#[test]
fn a_table_named_twice_or_a_partition_shadowing_a_column_refuses() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "a/x.parquet", 0, 10);
    write(tmp.path(), "b/id=3/x.parquet", 0, 10);
    let r = Engine::builder()
        .parquet(&tmp.path().join("a"), "t")
        .parquet(&tmp.path().join("a"), "T")
        .open();
    assert!(
        matches!(&r, Err(BurrmillError::Config(m)) if m.contains("two tables")),
        "{:?}",
        r.err()
    );
    let r = Engine::builder().parquet(&tmp.path().join("b"), "t").open();
    assert!(
        matches!(&r, Err(BurrmillError::Config(m)) if m.contains("also a column")),
        "{:?}",
        r.err()
    );
    let r = Engine::builder()
        .parquet(&tmp.path().join("missing"), "t")
        .open();
    assert!(
        matches!(r, Err(BurrmillError::NoSegments(_))),
        "{:?}",
        r.err()
    );
}

#[test]
fn sql_still_cannot_read_a_file() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "a.parquet", 0, 10);
    let e = Engine::builder().parquet(tmp.path(), "t").open().unwrap();
    let path = tmp.path().join("a.parquet");
    for sql in [
        format!("SELECT * FROM read_parquet('{}')", path.display()),
        format!("SELECT * FROM '{}'", path.display()),
        format!("SELECT * FROM parquet_scan('{}')", path.display()),
    ] {
        assert!(e.sql(&sql).is_err(), "{sql}");
    }
}
