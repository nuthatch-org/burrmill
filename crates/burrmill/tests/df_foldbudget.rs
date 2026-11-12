//! A substituted fold on a budgeted engine is held to the budget, as the aggregate it replaces is.
//! The live heap is counted by this binary's allocator, so the file holds one test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine, QueryMetrics};

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let now = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, new) };
        if !q.is_null() {
            if new >= l.size() {
                let now = LIVE.fetch_add(new - l.size(), Ordering::Relaxed) + new - l.size();
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(l.size() - new, Ordering::Relaxed);
            }
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const BUDGET: usize = 16 << 20;

const FOLD: &str = r#"SELECT addr, SUM(d) AS net FROM (
    SELECT "to" AS addr, CAST("value" AS DECIMAL(38,0)) AS d FROM transfer
    UNION ALL
    SELECT "from" AS addr, -CAST("value" AS DECIMAL(38,0)) AS d FROM transfer
) GROUP BY addr HAVING SUM(d) <> 0"#;

/// Two segments of `rows` transfers, every party distinct.
fn segments(segs: &Path, rows: usize) {
    std::fs::create_dir_all(segs).unwrap();
    let schema = Arc::new(Schema::new(
        ["from", "to", "value"]
            .map(|n| Field::new(n, DataType::Utf8, false))
            .to_vec(),
    ));
    for s in 0..2 {
        let party = |i: usize| format!("0x{:040x}", s * 2 * rows + i);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values((0..rows).map(party))),
                Arc::new(StringArray::from_iter_values((rows..2 * rows).map(party))),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| (i + 1).to_string()),
                )),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(segs.join(format!("transfer-{s:064x}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
}

fn budgeted(segs: &Path) -> Engine {
    Engine::builder()
        .segments(segs)
        .budget(Budget {
            memory_bytes: BUDGET,
            threads: 2,
            spill: None,
        })
        .open()
        .unwrap()
}

#[test]
fn a_substituted_fold_answers_within_the_budget_or_refuses() {
    let tmp = tempfile::tempdir().unwrap();

    // Small enough for the budget: still substituted, and the same answer as unbudgeted.
    let small = tmp.path().join("small");
    segments(&small, 1_000);
    let e = budgeted(&small);
    let plan = format!("{:?}", e.sql(&format!("EXPLAIN {FOLD}")).unwrap());
    assert!(plan.contains("OwnedSignedFold"), "not substituted:\n{plan}");
    let count = |e: &Engine| {
        e.sql(&format!("SELECT count(*) FROM ({FOLD})")).unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(count(&e), 4_000);
    assert_eq!(count(&Engine::open_segments(&small).unwrap()), 4_000);

    // A million parties, whose aggregation is several times the budget.
    let large = tmp.path().join("large");
    segments(&large, 250_000);
    let e = budgeted(&large);
    let mut m = QueryMetrics::default();
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let r = e.sql_for_each_metered(FOLD, Some(&mut m), |_| Ok(()));
    let grew = PEAK.load(Ordering::Relaxed) - before;
    match r {
        Err(err) => assert!(
            err.to_string().contains("OwnedSignedFoldExec"),
            "refused for something else: {err}"
        ),
        Ok(()) => panic!(
            "answered {} rows at a peak of {grew} heap bytes, {} reported, under a budget of {BUDGET}",
            m.output_rows, m.peak_memory_bytes
        ),
    }
    assert!(grew < 4 * BUDGET, "grew the heap by {grew} bytes");
}
