//! A scan's bookkeeping for the segments it opens is charged to the pool. A sort that can spill above
//! it must still answer: what the scan charges may not take the room the sort needs to go on.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine, SharedPool};

const SEGMENTS: u64 = 1_500;
const ROWS: u64 = 2;

fn segments() -> Vec<(PathBuf, u64)> {
    static WRITTEN: std::sync::OnceLock<Vec<(PathBuf, u64)>> = std::sync::OnceLock::new();
    WRITTEN.get_or_init(write).clone()
}

fn write() -> Vec<(PathBuf, u64)> {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("df_scansort/data2/0123456789abcdef0123456789abcdef/segments");
    std::fs::create_dir_all(&dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]));
    (0..SEGMENTS)
        .map(|n| {
            let path = dir.join(format!("qos_indexer_attempt_rows-{n:064x}.parquet"));
            if !path.exists() {
                let first = n * ROWS;
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from_iter_values(first..first + ROWS)),
                        Arc::new(StringArray::from_iter_values(
                            (first..first + ROWS).map(|i| format!("0x{:040x}", i * 7919 % 100_003)),
                        )),
                        Arc::new(StringArray::from_iter_values(
                            (first..first + ROWS).map(|i| format!("{:>60}", i % 997)),
                        )),
                    ],
                )
                .unwrap();
                let tmp = dir.join(format!("{n}.{}.tmp", std::process::id()));
                let f = std::fs::File::create(&tmp).unwrap();
                let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
                w.write(&batch).unwrap();
                w.close().unwrap();
                std::fs::rename(&tmp, &path).unwrap();
            }
            let len = std::fs::metadata(&path).unwrap().len();
            (path, len)
        })
        .collect()
}

fn engine(memory_bytes: usize, spill: &std::path::Path, pool: Option<&SharedPool>) -> Engine {
    let mut b = Engine::builder().budget(Budget {
        memory_bytes,
        threads: 4,
        spill: Some((spill.to_path_buf(), 1 << 30)),
    });
    if let Some(p) = pool {
        b = b.shared_pool(p);
    }
    let mut e = b.open().unwrap();
    let declared = vec![
        ("block_number".into(), "u64".into()),
        ("who".into(), "address".into()),
        ("amount".into(), "string".into()),
    ];
    e.register_facts("t", &declared, segments(), &[], (None, None))
        .unwrap();
    e
}

const SORT: &str = "SELECT amount, who FROM t ORDER BY amount, who";

fn answer(e: &Engine) -> Vec<RecordBatch> {
    e.sql(SORT).unwrap_or_else(|err| panic!("refused: {err}"))
}

fn same(got: &[RecordBatch], want: &[RecordBatch]) {
    let rows = |b: &[RecordBatch]| -> Vec<String> {
        b.iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .map(|r| r.to_string())
            .collect()
    };
    assert_eq!(rows(got), rows(want));
}

/// At this budget the scan's segments, charged whole, take the pool before the sort has reserved
/// anything, and the sort could not go on even after spilling.
#[test]
fn a_spilling_sort_over_a_scan_of_many_segments_answers() {
    let spill = tempfile::tempdir().unwrap();
    let want = answer(&engine(256 << 20, spill.path(), None));
    assert_eq!(want.iter().map(|b| b.num_rows()).sum::<usize>(), 3_000);
    same(&answer(&engine(5 << 19, spill.path(), None)), &want);
}

/// Two engines on one pool, as nuthatch's `/sql` runs them: the other engine's scan is paused part
/// way through, holding what it has charged, while this one sorts.
#[test]
fn a_sort_answers_beside_another_engines_scan_on_the_same_pool() {
    let spill = tempfile::tempdir().unwrap();
    let want = answer(&engine(256 << 20, spill.path(), None));
    let budget = 4 << 20;
    let pool = SharedPool::new(budget);
    let other = engine(budget, spill.path(), Some(&pool));
    let ours = engine(budget, spill.path(), Some(&pool));
    let (paused, wait) = std::sync::mpsc::channel();
    let (resume, resumed) = std::sync::mpsc::channel::<()>();
    std::thread::scope(|s| {
        let other = &other;
        let scan = s.spawn(move || {
            let mut first = true;
            other.sql_for_each("SELECT block_number, who FROM t", |_| {
                if std::mem::take(&mut first) {
                    paused.send(()).unwrap();
                    resumed.recv().unwrap();
                }
                Ok(())
            })
        });
        wait.recv().unwrap();
        assert!(pool.reserved() > 0, "the paused scan holds nothing");
        let got = ours.sql(SORT);
        resume.send(()).unwrap();
        scan.join().unwrap().unwrap();
        same(&got.unwrap_or_else(|err| panic!("refused: {err}")), &want);
    });
}
