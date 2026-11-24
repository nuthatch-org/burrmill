//! A join whose build side is larger than its budget answers with a spill directory and is refused
//! without one (#383). The statements are TPC-H Q21's shape: an `EXISTS` and a `NOT EXISTS` of a
//! table against itself on one key, with an inequality beside it, which DataFusion plans as a
//! partitioned hash join building on the whole table, and which cannot spill.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, BurrmillError, Engine};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

const KEYS: i64 = 800_000;

/// Each key three times, in three passes over the keys; an even key's three rows share a supplier,
/// an odd key's have three.
fn write(path: &Path) {
    write_passes(path, 3);
}

/// As [`write`], over `passes` passes.
fn write_passes(path: &Path, passes: i64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Int64, false),
    ]));
    // Uncompressed and without dictionaries, so the file holds about what its rows hold decoded.
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let mut w = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        schema.clone(),
        Some(props),
    )
    .unwrap();
    for pass in 0..passes {
        let k: Vec<i64> = (0..KEYS).map(|i| (i * 7919) % KEYS).collect();
        let s: Vec<i64> = k
            .iter()
            .map(|k| if k % 2 == 0 { 5 } else { pass })
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(k)), Arc::new(Int64Array::from(s))],
        )
        .unwrap();
        w.write(&batch).unwrap();
    }
    w.close().unwrap();
}

const EXISTS: &str = "SELECT count(*) AS n FROM t a WHERE EXISTS \
    (SELECT 1 FROM t b WHERE b.k = a.k AND b.s <> a.s)";
const NOT_EXISTS: &str = "SELECT count(*) AS n FROM t a WHERE NOT EXISTS \
    (SELECT 1 FROM t b WHERE b.k = a.k AND b.s <> a.s)";

fn engine(file: &Path, memory_bytes: usize, spill: Option<&Path>) -> Engine {
    Engine::builder()
        .parquet(file, "t")
        .budget(Budget {
            memory_bytes,
            threads: 2,
            spill: spill.map(|d| (d.to_path_buf(), 4 << 30)),
        })
        .open()
        .unwrap()
}

fn count(e: &Engine, sql: &str) -> Result<i64, BurrmillError> {
    let b = e.sql(sql)?;
    let c = b[0].column(0);
    let n = c.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(n.len(), 1);
    Ok(n.value(0))
}

fn plan(e: &Engine, sql: &str) -> String {
    let b = e.sql(&format!("EXPLAIN {sql}")).unwrap();
    arrow::util::pretty::pretty_format_batches(&b)
        .unwrap()
        .to_string()
}

#[test]
fn a_join_larger_than_its_budget_spills() {
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("t.parquet");
    write(&file);
    let spill = tmp.path().join("spill");
    std::fs::create_dir(&spill).unwrap();
    let odd = 3 * (KEYS / 2);
    let budget = 32 << 20;
    assert!(
        std::fs::metadata(&file).unwrap().len() > budget as u64,
        "the build side must not fit"
    );

    let refusing = engine(&file, budget, None);
    assert!(plan(&refusing, EXISTS).contains("HashJoinExec"));
    for sql in [EXISTS, NOT_EXISTS] {
        match count(&refusing, sql) {
            Err(BurrmillError::LimitExceeded(m)) => assert!(
                m.contains("a spill directory would let operators that can spill do so"),
                "{m}"
            ),
            r => panic!("{sql} answered {r:?} past its budget with nowhere to spill"),
        }
    }

    let spilling = engine(&file, budget, Some(&spill));
    let p = plan(&spilling, EXISTS);
    assert!(
        p.contains("SortMergeJoinExec") && !p.contains("HashJoinExec"),
        "{p}"
    );
    assert_eq!(count(&spilling, EXISTS).unwrap(), odd);
    assert_eq!(count(&spilling, NOT_EXISTS).unwrap(), 3 * KEYS - odd);
}

#[test]
fn a_join_that_fits_keeps_its_hash_join() {
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("t.parquet");
    write(&file);
    let spill = tmp.path().join("spill");
    std::fs::create_dir(&spill).unwrap();
    // The build side's file holds under half the statement pool, so it stays a hash join.
    let roomy = engine(&file, 256 << 20, Some(&spill));
    let p = plan(&roomy, EXISTS);
    assert!(
        p.contains("HashJoinExec") && !p.contains("SortMergeJoinExec"),
        "{p}"
    );
    assert_eq!(count(&roomy, EXISTS).unwrap(), 3 * (KEYS / 2));
}

#[test]
fn a_join_building_on_groups_keeps_its_hash_join() {
    let tmp = tempfile::tempdir().unwrap();
    let (file, outer) = (tmp.path().join("t.parquet"), tmp.path().join("u.parquet"));
    write(&file);
    write_passes(&outer, 6);
    let spill = tmp.path().join("spill");
    std::fs::create_dir(&spill).unwrap();
    // As TPC-H Q18's `IN`: the grouped side's files are larger than the pool, and the other side's
    // larger still, so the join builds on the keys the `HAVING` kept: the even ones, whose
    // suppliers sum to 15.
    let sql = "SELECT count(*) AS n FROM u WHERE k IN \
        (SELECT k FROM t GROUP BY k HAVING sum(s) > 10)";
    let e = Engine::builder()
        .parquet(&file, "t")
        .parquet(&outer, "u")
        .budget(Budget {
            memory_bytes: 32 << 20,
            threads: 2,
            spill: Some((spill, 4 << 30)),
        })
        .open()
        .unwrap();
    let p = plan(&e, sql);
    assert!(
        p.contains("HashJoinExec") && !p.contains("SortMergeJoinExec"),
        "{p}"
    );
    assert_eq!(count(&e, sql).unwrap(), 6 * (KEYS / 2));
}
