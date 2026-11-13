//! A metered statement with a predicate is costed from each segment's kept row-group statistics
//! once its footer has been read, and the figure is the one the footer gives, byte for byte. The
//! footer reads and the heap are counted process-wide, so the file holds one test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine, QueryMetrics};
use parquet::file::properties::WriterProperties;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Ordering::Relaxed);
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
            LIVE.fetch_add(new, Ordering::Relaxed);
            LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const ROWS: u64 = 40;

/// `segments` segments of `ROWS` rows in row groups of 10, with a number, an address, a nullable
/// text and a nullable number, so every predicate below prunes some row groups and keeps others.
fn nest(root: &Path, segments: u64) {
    let segs = root.join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(10))
        .build();
    for s in 0..segments {
        let ids = s * ROWS..(s + 1) * ROWS;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(ids.clone())),
                Arc::new(StringArray::from_iter_values(
                    ids.clone().map(|i| format!("0x{:040x}", i * 7 % 997)),
                )),
                Arc::new(StringArray::from_iter(
                    ids.clone()
                        .map(|i| (i % 13 != 0).then(|| format!("note {}", i % 50))),
                )),
                Arc::new(Int64Array::from_iter(
                    ids.map(|i| (i % 30 >= 10).then_some(i as i64 % 100)),
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

fn copy(from: &Path, to: &Path) {
    let segs = to.join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    for e in std::fs::read_dir(from.join("segments")).unwrap() {
        let p = e.unwrap().path();
        std::fs::copy(&p, segs.join(p.file_name().unwrap())).unwrap();
    }
}

fn engine(root: &Path, memory_bytes: usize) -> Engine {
    Engine::open_nest_budgeted(
        root,
        Budget {
            memory_bytes,
            threads: 2,
            spill: None,
        },
    )
    .unwrap()
}

/// The figure, and the footers the pass read for it.
fn metered(e: &Engine, sql: &str) -> (u64, u64) {
    let before = burrmill::df::selection_footer_reads();
    let mut m = QueryMetrics::default();
    e.sql_for_each_metered(sql, Some(&mut m), |_| Ok(()))
        .unwrap_or_else(|err| panic!("{sql}: {err}"));
    (
        m.sealed_bytes_scanned,
        burrmill::df::selection_footer_reads() - before,
    )
}

const PREDICATES: &[&str] = &[
    "id = 1234",
    "id BETWEEN 1000 AND 2000",
    "id > 2300",
    "id IN (3, 900, 2100)",
    "who = '0x0000000000000000000000000000000000000007'",
    "who >= '0x00000000000000000000000000000000000003' AND who < '0x00000000000000000000000000000000000004'",
    "who IN ('0x0000000000000000000000000000000000000010', '0x00000000000000000000000000000000000003e5')",
    "note IS NULL",
    "note IS NOT NULL AND note > 'note 4'",
    "n IS NULL",
    "n < 15",
    "n BETWEEN 40 AND 60 AND id < 1500",
];

#[test]
fn a_kept_summary_costs_a_statement_as_its_footer_does() {
    const SMALL: u64 = 60;
    let tmp = tempfile::tempdir().unwrap();
    let master = tmp.path().join("master");
    nest(&master, SMALL);

    // Each predicate over a fresh copy: the first run reads every footer, the second none, and the
    // two figures are the same.
    for (k, p) in PREDICATES.iter().enumerate() {
        let root = tmp.path().join(format!("copy{k}"));
        copy(&master, &root);
        let e = engine(&root, 64 << 20);
        let sql = format!("SELECT id FROM t WHERE {p} LIMIT 1");
        let (from_footers, read) = metered(&e, &sql);
        assert_eq!(read, SMALL, "{p}: the first run read {read} footers");
        let (kept, read) = metered(&e, &sql);
        assert_eq!(read, 0, "{p}: the second run read {read} footers");
        assert_eq!(kept, from_footers, "{p}");
        assert!(
            from_footers > 0,
            "{p} selects nothing: the fixture does not test it"
        );
    }

    // Past the limit, what is not kept is read from its footer, and the figure does not move.
    let root = tmp.path().join("capped");
    copy(&master, &root);
    let e = engine(&root, 1 << 20);
    let mut reads = Vec::new();
    for p in PREDICATES {
        let sql = format!("SELECT id FROM t WHERE {p} LIMIT 1");
        let (first, _) = metered(&e, &sql);
        let (second, read) = metered(&e, &sql);
        assert_eq!(first, second, "{p} past the limit");
        reads.push(read);
    }
    assert!(
        reads.iter().any(|r| *r < SMALL),
        "nothing was kept: {reads:?}"
    );
    assert!(
        reads.iter().any(|r| *r > 0),
        "the limit was not reached: {reads:?}"
    );
    drop(e);

    // A metered keyset page over 3,000 segments: the first page reads every footer, the next one
    // none, and the next page's figure is the footer's.
    const LARGE: u64 = 3_000;
    let big = tmp.path().join("big");
    nest(&big, LARGE);
    let e = engine(&big, 256 << 20);
    let page = |last: u64| format!("SELECT id FROM t WHERE id > {last} ORDER BY id LIMIT 100");
    let base = LIVE.load(Ordering::Relaxed);
    let footers = e.footer_cache_bytes();
    let (_, read) = metered(&e, &page(0));
    assert!(read >= LARGE, "the first page read {read} footers");
    let kept =
        (LIVE.load(Ordering::Relaxed) - base).saturating_sub(e.footer_cache_bytes() - footers);
    eprintln!(
        "{LARGE} segments: {kept} bytes kept beside the footer cache, {} a segment",
        kept / LARGE as usize
    );
    let (next, read) = metered(&e, &page(100));
    assert!(read < LARGE / 100, "the next page read {read} footers");
    let fresh = tmp.path().join("big-fresh");
    copy(&big, &fresh);
    let (from_footers, read) = metered(&engine(&fresh, 256 << 20), &page(100));
    assert!(read >= LARGE, "a fresh engine read {read} footers");
    assert_eq!(next, from_footers);
}
