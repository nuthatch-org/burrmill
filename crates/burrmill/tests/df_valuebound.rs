//! A statement cannot build values past its pool: each of these grew the process gigabytes past a
//! 512 MB pool, through functions, `||`, literals repeated per row and table series, none of which
//! DataFusion charges. The live heap is counted by this binary's allocator, so the file holds one
//! test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use burrmill::{Budget, Engine};

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let live = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
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
            let live = LIVE.fetch_add(new, Ordering::Relaxed) + new;
            PEAK.fetch_max(live, Ordering::Relaxed);
            LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const BUDGET: usize = 64 << 20;

/// What a statement may hold past its pool: a refused call's inputs and the one value or batch a
/// function may build before its result is checked.
const SLACK: usize = 24 << 20;

/// The most the heap grew while `sql` ran, and what it answered.
fn peak(
    e: &Engine,
    sql: &str,
) -> (
    usize,
    burrmill::Result<Vec<arrow::record_batch::RecordBatch>>,
) {
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let got = e.sql(sql);
    (PEAK.load(Ordering::Relaxed).saturating_sub(base), got)
}

fn nested(depth: usize, f: impl Fn(String) -> String, seed: &str) -> String {
    (0..depth).fold(seed.to_string(), |s, _| f(s))
}

#[test]
fn no_statement_builds_values_past_its_pool() {
    let e = Engine::open_empty_budgeted(Budget {
        memory_bytes: BUDGET,
        threads: 2,
        spill: None,
    })
    .unwrap();
    let pool = BUDGET - BUDGET / 8;
    let replace = nested(40, |s| format!("replace({s}, 'a', 'aa')"), "'a'");
    let encode = nested(40, |s| format!("encode({s}, 'hex')"), "x");
    let doubles = vec!["'ab'"; 40].join(", ");
    let concat = vec!["s"; 200].join(", ");
    let columns = (0..20)
        .map(|i| format!("repeat(x, {}) AS c{i}", 8_000_000 + i))
        .collect::<Vec<_>>()
        .join(", ");
    let refused = [
        "SELECT repeat('a', 1500000000) AS s".to_string(),
        "SELECT lpad('a', 1000000000) AS s".into(),
        "SELECT rpad(x, 1000000000, 'é') AS s FROM (VALUES ('a')) t(x)".into(),
        "SELECT array_repeat(1, 100000000) AS l".into(),
        "SELECT range(100000000) AS l".into(),
        "SELECT generate_series(1, 100000000) AS l".into(),
        "SELECT range(DATE '1970-01-01', DATE '9999-01-01', INTERVAL 1 DAY) AS l".into(),
        "SELECT array_resize([1], 100000000) AS l".into(),
        "SELECT printf('%1000000000d', 1) AS s".into(),
        "WITH RECURSIVE r(s, n) AS (SELECT 'ab', 0 UNION ALL SELECT s || s, n + 1 FROM r WHERE n < 40) \
         SELECT max(length(s)) AS m FROM r"
            .into(),
        format!("SELECT length({replace}) AS m"),
        format!("SELECT length({replace}) AS m FROM (VALUES (1)) t(x)"),
        format!("SELECT length({}) AS m FROM (VALUES ('a')) t(x)", replace.replacen("'a', 'a', 'aa')", "x, 'a', 'aa')", 1)),
        format!("SELECT length({encode}) AS m FROM (VALUES ('a')) t(x)"),
        format!("SELECT length(list_reduce([{doubles}], lambda a, x: a || a)) AS m"),
        "SELECT repeat('a', 10000000) AS s FROM range(100000)".into(),
        format!("SELECT '{}' AS s FROM range(100000)", "a".repeat(64 << 10)),
        format!(
            "SELECT CASE WHEN r >= 0 THEN '{}' END AS s FROM range(100000) t(r)",
            "a".repeat(64 << 10)
        ),
        "SELECT CASE WHEN r >= 0 THEN repeat('a', 1000000) END AS s FROM range(100000) t(r)".into(),
        "SELECT repeat(x, 1000000) AS s FROM (SELECT 'a' AS x FROM range(1000))".into(),
        format!("SELECT {columns} FROM (VALUES ('a')) t(x)"),
        format!(
            "SELECT length(concat({concat})) AS m FROM \
             (SELECT repeat(x, 1000000) AS s FROM (VALUES ('a')) t(x))"
        ),
        "SELECT regexp_replace(repeat(x, 1000000), 'a', repeat(x, 100), 'g') AS s \
         FROM (VALUES ('a')) t(x)"
            .into(),
        "SELECT string_agg(CAST(r AS VARCHAR), repeat('-', 4096)) AS s FROM range(1000000) t(r)"
            .into(),
        "SELECT r % 3 AS k, string_agg(CAST(r AS VARCHAR), repeat('-', 4096)) AS s \
         FROM range(1000000) t(r) GROUP BY 1"
            .into(),
        "SELECT count(*) AS n FROM range(9000000)".into(),
    ];
    for sql in &refused {
        let (grew, got) = peak(&e, sql);
        let err = match got {
            Ok(_) => panic!("answered: {sql}"),
            Err(err) => err.to_string(),
        };
        eprintln!(
            "{:>6.1} MiB  {}",
            grew as f64 / (1 << 20) as f64,
            &sql[..sql.len().min(90)]
        );
        assert!(err.contains("Out of Memory Error"), "{sql}\n{err}");
        assert!(
            grew < pool + SLACK,
            "{sql}\ngrew {grew} bytes against a pool of {pool}"
        );
        assert_eq!(e.memory_reserved(), 0, "{sql} left a charge behind");
    }
}
