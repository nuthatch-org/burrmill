//! A metered statement's scan CPU (nuthatch RFC-0073 FR4): the thread CPU its segment scans spend
//! inside their own polls. Its own binary, so the process CPU it is checked against is its own.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine, QueryMetrics};
use parquet::file::properties::WriterProperties;

/// Sixteen segments of 32,768 rows in row groups of 4,096: enough decoding to time.
fn nest(root: &Path) {
    let segs = root.join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4_096))
        .build();
    for s in 0..16u64 {
        let rows = 32_768u64;
        let first = s * rows;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(first..first + rows)),
                Arc::new(StringArray::from_iter_values(
                    (first..first + rows)
                        .map(|n| format!("0x{:040x}", n * 2_654_435_761 % 1_000_003)),
                )),
                Arc::new(StringArray::from_iter_values(
                    (first..first + rows).map(|n| (n * 7 % 100_000).to_string()),
                )),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(segs.join(format!("t-{s:064x}.parquet"))).unwrap();
        let mut w =
            parquet::arrow::ArrowWriter::try_new(f, schema.clone(), Some(props.clone())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
}

fn engine(root: &Path, threads: usize) -> Engine {
    Engine::open_nest_budgeted(
        root,
        Budget {
            memory_bytes: 512 << 20,
            threads,
            spill: None,
        },
    )
    .unwrap()
}

fn metered(e: &Engine, sql: &str) -> QueryMetrics {
    let mut m = QueryMetrics::default();
    e.sql_for_each_metered(sql, Some(&mut m), |_| Ok(()))
        .unwrap();
    m
}

fn process_cpu_nanos() -> u64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `u` is a valid, writable rusage.
    assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) }, 0);
    let ns = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1_000;
    ns(u.ru_utime) + ns(u.ru_stime)
}

const SCAN: &str = "SELECT count(*) FROM t WHERE who LIKE '%abc%' OR amount = '7'";

/// Held by each test, so the process CPU one reads is not another's.
static ALONE: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn scan_cpu_is_most_of_a_scan_heavy_statement_and_never_more_than_the_process() {
    let _alone = ALONE.lock().unwrap_or_else(|p| p.into_inner());
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let e = engine(tmp.path(), 1);
    metered(&e, SCAN);
    let (mut scan, mut process) = (0u64, 0u64);
    for _ in 0..5 {
        let before = process_cpu_nanos();
        scan += metered(&e, SCAN).scan_cpu_nanos;
        process += process_cpu_nanos() - before;
    }
    eprintln!(
        "scan {} ms of process {} ms",
        scan / 1_000_000,
        process / 1_000_000
    );
    assert!(scan > 0);
    // The thread clock ticks finer than rusage, so allow one tick a run.
    assert!(scan <= process + 5 * 10_000_000, "{scan} > {process}");
    assert!(
        scan * 3 > process,
        "the scans' polls missed the decoding: {scan} of {process}"
    );
}

#[test]
fn a_statement_with_no_scan_and_an_unmetered_one_count_nothing() {
    let _alone = ALONE.lock().unwrap_or_else(|p| p.into_inner());
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let e = engine(tmp.path(), 4);
    assert_eq!(metered(&e, "SELECT 1").scan_cpu_nanos, 0);
    let mut m = QueryMetrics::default();
    e.sql_for_each_metered(SCAN, None, |_| Ok(())).unwrap();
    assert_eq!(m, QueryMetrics::default());
    e.sql_for_each_metered(SCAN, Some(&mut m), |_| Ok(()))
        .unwrap();
    assert!(m.scan_cpu_nanos > 0);
}

/// The clock is a stream wrapper the scan already had a node for, so the plan is the one an unmetered
/// statement runs, and a TopK's dynamic filter still reaches the scan and prunes it.
#[test]
fn timing_the_scans_leaves_the_plan_and_its_dynamic_filters_alone() {
    let _alone = ALONE.lock().unwrap_or_else(|p| p.into_inner());
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let e = engine(tmp.path(), 4);
    let topk = "SELECT * FROM t ORDER BY block_number DESC LIMIT 5";
    let explain = |sql: &str| -> String {
        e.sql(&format!("EXPLAIN {sql}"))
            .unwrap()
            .iter()
            .map(|b| format!("{b:?}"))
            .collect()
    };
    let plan = explain(topk);
    assert!(plan.contains("DynamicFilter"), "{plan}");
    metered(&e, topk);
    assert_eq!(explain(topk), plan);
    let m = metered(&e, topk);
    assert!(m.scan_cpu_nanos > 0);
    assert!(
        m.sealed_bytes_read * 2 < m.sealed_bytes_scanned,
        "the dynamic filter pruned nothing: {m:?}"
    );
    let join = "SELECT t.who FROM t JOIN t u ON t.block_number = u.block_number \
                WHERE u.block_number < 100";
    let j = metered(&e, join);
    let whole = metered(
        &e,
        "SELECT t.who, u.block_number FROM t, t u WHERE t.block_number = u.block_number",
    );
    assert!(
        j.sealed_bytes_read < whole.sealed_bytes_read,
        "the join's filter pruned nothing: {j:?} vs {whole:?}"
    );
}
