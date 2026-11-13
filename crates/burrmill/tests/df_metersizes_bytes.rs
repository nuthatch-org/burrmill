//! What the map of kept column sizes counts is the heap it takes, near enough that its limit
//! means what it says. The live heap is counted by this binary's allocator, so the file holds one
//! test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine};

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

#[test]
fn the_count_is_the_heap() {
    const SEGMENTS: usize = 2_000;
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    // QoS's shape: one row group of 25 text columns, two of them named by a predicate.
    let schema = Arc::new(Schema::new(
        (0..25)
            .map(|c| Field::new(format!("column_{c}"), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let paths: Vec<_> = (0..SEGMENTS)
        .map(|s| {
            let columns: Vec<ArrayRef> = (0..25)
                .map(|c| Arc::new(StringArray::from_iter_values([format!("{s}-{c}")])) as ArrayRef)
                .collect();
            let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
            let p = segs.join(format!("qos_indexer_attempt_rows-{s:064x}.parquet"));
            let f = std::fs::File::create(&p).unwrap();
            let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
            p
        })
        .collect();
    // An engine whose allowance holds them all.
    let _e = Engine::open_empty_budgeted(Budget {
        memory_bytes: 4 << 30,
        threads: 1,
        spill: None,
    })
    .unwrap();
    let base = LIVE.load(Ordering::Relaxed);
    let counted = burrmill::df::keep_summaries(&paths, &["column_0", "column_1"]);
    let heap = LIVE.load(Ordering::Relaxed) - base;
    eprintln!(
        "{SEGMENTS} segments: {heap} bytes on the heap ({} a segment), {counted} counted ({} a segment)",
        heap / SEGMENTS,
        counted / SEGMENTS
    );
    assert!(
        counted.abs_diff(heap) < heap / 5,
        "counted {counted} for {heap} bytes on the heap"
    );
    // Kept again, an entry and its schema are counted once.
    assert_eq!(
        burrmill::df::keep_summaries(&paths, &["column_0", "column_1"]),
        counted
    );
}
