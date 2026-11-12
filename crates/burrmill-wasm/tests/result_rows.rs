//! A quadratic statement over an ordinary upload is cut at `RESULT_ROWS` as it streams, so what the
//! answer holds is the rows kept, not the whole answer. The live heap is counted by this binary's
//! allocator, so the file holds one test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use burrmill_ingest::{Budget, FormatHint};
use burrmill_wasm::{RESULT_ROWS, Session};

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

#[test]
fn a_cross_join_of_an_upload_holds_the_rows_kept_not_the_answer() {
    let mut csv = String::from("posted,merchant,amount\n");
    for i in 0..7_000 {
        csv.push_str(&format!("2024-01-02,MERCHANT_{i:05},-{i}.00\n"));
    }
    let table =
        burrmill_ingest::ingest_bytes(csv.as_bytes(), FormatHint::Sniff, &Budget::untrusted())
            .unwrap();
    let s = Session::open(table).unwrap();

    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let out = s
        .query("SELECT a.merchant, b.merchant AS m2 FROM t a, t b")
        .unwrap();
    let grew = PEAK.load(Ordering::Relaxed) - before;

    assert_eq!(out.rows.len(), RESULT_ROWS);
    assert!(out.truncated);
    assert_eq!(out.columns.len(), 2);
    // The whole answer is 49M rows, about 2.2 GB. The tab's engine budget is 256 MiB.
    assert!(grew < 64 << 20, "the query grew the heap by {grew} bytes");
}
