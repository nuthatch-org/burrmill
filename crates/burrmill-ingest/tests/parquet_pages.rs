//! A page header that claims more than the budget is refused before the decoder allocates for it.
//! Its own binary, so the largest allocation seen is this test's.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use burrmill_ingest::{Budget, BudgetBreach, FormatHint, IngestError, ingest_bytes};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};

static LARGEST: AtomicUsize = AtomicUsize::new(0);

struct Watch;

unsafe impl GlobalAlloc for Watch {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LARGEST.fetch_max(l.size(), Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        LARGEST.fetch_max(l.size(), Ordering::Relaxed);
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        LARGEST.fetch_max(new, Ordering::Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}

#[global_allocator]
static WATCH: Watch = Watch;

const CLAIM: u64 = 256 << 20;

fn varint(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
    out
}

fn read_varint(b: &[u8]) -> (u64, usize) {
    let (mut v, mut n) = (0u64, 0);
    loop {
        v |= u64::from(b[n] & 0x7f) << (7 * n);
        n += 1;
        if b[n - 1] & 0x80 == 0 {
            return (v, n);
        }
    }
}

/// One Snappy data page of a thousand integers, its header rewritten to claim [`CLAIM`] bytes.
/// The longer header takes its extra bytes from the end of the page, so every offset in the
/// footer still holds.
fn lying_file() -> Vec<u8> {
    let column = Arc::new(Int32Array::from_iter_values(0..1000)) as ArrayRef;
    let batch = RecordBatch::try_from_iter([("n", column)]).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false)
        .build();
    let mut file = Vec::new();
    let mut w = ArrowWriter::try_new(&mut file, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    let reader = SerializedFileReader::new(Bytes::from(file.clone())).unwrap();
    let (start, len) = reader.metadata().row_group(0).column(0).byte_range();
    let (start, end) = (start as usize, (start + len) as usize);

    // type, uncompressed_page_size, compressed_page_size: three i32 fields, the first a data page.
    assert_eq!(file[start..start + 3], [0x15, 0x00, 0x15]);
    let (_, claimed) = read_varint(&file[start + 3..]);
    assert_eq!(file[start + 3 + claimed], 0x15);
    let (stored, stored_len) = read_varint(&file[start + 4 + claimed..]);
    let old = 4 + claimed + stored_len;

    let lie = varint(CLAIM << 1);
    let grown = lie.len() - claimed;
    // Zigzag: an i32 of n is 2n.
    let less = varint(stored - 2 * grown as u64);
    assert_eq!(less.len(), stored_len);
    let mut head = vec![0x15, 0x00, 0x15];
    head.extend(lie);
    head.push(0x15);
    head.extend(less);

    file.copy_within(start + old..end - grown, start + old + grown);
    file[start..start + head.len()].copy_from_slice(&head);
    file
}

#[test]
fn a_page_claiming_more_than_the_budget_is_refused_before_it_is_allocated() {
    let file = lying_file();
    let budget = Budget::untrusted().max_memory_bytes(16 << 20);
    LARGEST.store(0, Ordering::Relaxed);
    let err = ingest_bytes(&file, FormatHint::Sniff, &budget).unwrap_err();
    let largest = LARGEST.load(Ordering::Relaxed);
    assert_eq!(
        err,
        IngestError::Budget(BudgetBreach {
            which: "max_memory_bytes",
            limit: 16 << 20,
            at: file.len() as u64 + CLAIM,
        })
    );
    assert!(largest < 1 << 20, "allocated {largest} bytes at once");
}
