//! Peak live heap, with `--features alloc-stats`: what the process held at its worst, as against
//! peak RSS, which also counts what the allocator freed and did not return.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
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

/// Calls to `alloc` so far; a `realloc` is not counted.
pub fn allocs() -> u64 {
    ALLOCS.load(Ordering::Relaxed)
}

pub fn peak_mb() -> usize {
    PEAK.load(Ordering::Relaxed) / (1024 * 1024)
}

/// Samples `/proc/self/statm` every 2 ms and keeps the sample of greatest resident size: that
/// moment's file-backed and anonymous pages, and the live heap then.
#[cfg(target_os = "linux")]
pub fn sample_peak() -> std::sync::Arc<std::sync::Mutex<(usize, usize, usize)>> {
    let best = std::sync::Arc::new(std::sync::Mutex::new((0usize, 0usize, 0usize)));
    let b = std::sync::Arc::clone(&best);
    std::thread::spawn(move || {
        let page = 4096usize;
        loop {
            if let Ok(t) = std::fs::read_to_string("/proc/self/statm") {
                let f: Vec<usize> = t
                    .split_whitespace()
                    .filter_map(|x| x.parse().ok())
                    .collect();
                let (rss, shared) = (f[1] * page, f[2] * page);
                let mut g = b.lock().unwrap();
                if rss > g.0 {
                    *g = (rss, shared, LIVE.load(Ordering::Relaxed));
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    });
    best
}
