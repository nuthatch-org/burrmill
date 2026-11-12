//! burrmill#86: what a scan holds for each segment it opens is charged to the pool, so a statement's
//! heap less what the pool holds stays small however many segments it reads. burrmill#156: it is
//! charged as the segment opens, not for the whole table before the first row. The live heap is
//! counted by this binary's allocator, so the file holds one test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
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

/// As many as a QoS table holds is slow to write; the cost is per segment, so fewer show it.
const SEGMENTS: u64 = 1_500;

/// Segments as nuthatch seals them: a long path, two rows each, a bloom filter on the address.
fn segments() -> Vec<(PathBuf, u64)> {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("df_scanpool/data/0123456789abcdef0123456789abcdef/segments");
    std::fs::create_dir_all(&dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder()
        .set_column_bloom_filter_enabled("who".into(), true)
        .build();
    (0..SEGMENTS)
        .map(|n| {
            let path = dir.join(format!("transfer_events-{n:064x}.parquet"));
            if !path.exists() {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from(vec![2 * n, 2 * n + 1])),
                        Arc::new(StringArray::from(vec![
                            format!("0x{:040x}", 2 * n),
                            format!("0x{:040x}", 2 * n + 1),
                        ])),
                        Arc::new(StringArray::from(vec!["1", "2"])),
                    ],
                )
                .unwrap();
                let tmp = dir.join(format!("{n}.{}.tmp", std::process::id()));
                let f = std::fs::File::create(&tmp).unwrap();
                let mut w =
                    parquet::arrow::ArrowWriter::try_new(f, schema.clone(), Some(props.clone()))
                        .unwrap();
                w.write(&batch).unwrap();
                w.close().unwrap();
                std::fs::rename(&tmp, &path).unwrap();
            }
            let len = std::fs::metadata(&path).unwrap().len();
            (path, len)
        })
        .collect()
}

/// The heap a statement may hold beyond what the pool counts, over all its segments.
const OUTSIDE: usize = 2 << 20;

#[test]
fn a_scan_holds_its_segments_bookkeeping_inside_the_pool() {
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 256 << 20,
        threads: 1,
        spill: None,
    })
    .unwrap();
    let declared = vec![
        ("block_number".into(), "u64".into()),
        ("who".into(), "address".into()),
        ("amount".into(), "string".into()),
    ];
    e.register_facts("t", &declared, segments(), &[], (None, None))
        .unwrap();
    let base = LIVE.load(Ordering::Relaxed);
    // One row from each segment, so batches come out while the scan is still opening segments.
    let (mut outside, mut held_then, mut rows) = (0usize, 0usize, 0usize);
    e.sql_for_each(
        "SELECT block_number FROM t WHERE block_number % 2 = 0",
        |b| {
            // Footers stay cached outside the pool by design, in the eighth it was sized without.
            let live = LIVE
                .load(Ordering::Relaxed)
                .saturating_sub(base)
                .saturating_sub(e.footer_cache_bytes());
            let held = e.memory_reserved();
            if live.saturating_sub(held) > outside {
                (outside, held_then) = (live.saturating_sub(held), held);
            }
            rows += b.num_rows();
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(rows, SEGMENTS as usize);
    eprintln!(
        "{SEGMENTS} segments: at most {outside} outside the pool ({} per segment), pool {held_then}",
        outside / SEGMENTS as usize
    );
    assert!(
        outside < OUTSIDE,
        "{outside} bytes outside the pool during a scan of {SEGMENTS} segments (pool held {held_then})"
    );

    // burrmill#156: the charge follows the segments opened. A batch per segment here, so the pool
    // is read while the scan is part-way through the table. A text column puts `CompactViews`
    // between the scan and its wrapper, which once left such a scan charged nothing.
    let mut held = Vec::new();
    e.sql_for_each("SELECT block_number, who FROM t", |_| {
        let live = LIVE
            .load(Ordering::Relaxed)
            .saturating_sub(base)
            .saturating_sub(e.footer_cache_bytes());
        // The plan lists every segment before it opens one, and that is held from the start.
        if held.is_empty() {
            assert!(live.saturating_sub(e.memory_reserved()) < OUTSIDE / 2);
        }
        held.push(e.memory_reserved());
        Ok(())
    })
    .unwrap();
    let whole = *held.last().unwrap();
    assert!(whole > SEGMENTS as usize * 5_632, "{whole}");
    assert!(
        held[0] * 3 < whole,
        "the first batch came with {} of {whole} already held",
        held[0]
    );
    // Two scans of the table: the build side is read and dropped before the probe side has opened
    // much, so the statement never holds for both whole.
    e.take_memory_peak();
    e.sql("SELECT count(*) FROM t a JOIN t b ON a.block_number = b.block_number")
        .unwrap();
    let peak = e.take_memory_peak();
    assert!(peak < whole + whole / 2, "{peak} against {whole} a scan");

    // Three scans, one of them a join's build side that ends before the rest: what each holds
    // is charged until its memory goes, so the heap beside the pool stays small whether the
    // statement fits the budget or is refused for its scans' bookkeeping.
    let three = "SELECT a.block_number, b.who FROM t a JOIN t b ON a.block_number = b.block_number \
                 UNION ALL SELECT block_number, who FROM t";
    for memory_bytes in [8 << 20, 64 << 20] {
        let mut e = Engine::open_empty_budgeted(burrmill::Budget {
            memory_bytes,
            threads: 4,
            spill: None,
        })
        .unwrap();
        e.register_facts("t", &declared, segments(), &[], (None, None))
            .unwrap();
        let base = LIVE.load(Ordering::Relaxed);
        let mut outside = 0usize;
        let r = e.sql_for_each(three, |_| {
            let live = LIVE
                .load(Ordering::Relaxed)
                .saturating_sub(base)
                .saturating_sub(e.footer_cache_bytes());
            outside = outside.max(live.saturating_sub(e.memory_reserved()));
            Ok(())
        });
        if let Err(err) = r {
            assert!(err.to_string().contains("a scan's bookkeeping"), "{err}");
        }
        assert!(
            outside < OUTSIDE,
            "{outside} bytes outside the pool over three scans at {memory_bytes}"
        );
    }
}
