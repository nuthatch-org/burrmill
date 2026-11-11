//! What a Parquet file would make the decoder allocate past the budget is refused before it does:
//! a page header's claim, and a list column's values. Its own binary, and one test at a time, so
//! the largest allocation seen is the test's own.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int32Array, ListArray, RecordBatch, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
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

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

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
    let _alone = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
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

fn write(columns: Vec<(&str, ArrayRef)>) -> Vec<u8> {
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut file = Vec::new();
    let mut w = ArrowWriter::try_new(&mut file, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    file
}

/// One row whose list holds `n` zeros: a few kilobytes of file, `4 * n` bytes decoded, and no
/// page that says so.
fn one_long_list(n: usize) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, false)),
        OffsetBuffer::from_lengths([n]),
        Arc::new(Int32Array::from(vec![0; n])),
        None,
    ))
}

#[test]
fn a_list_column_is_refused_before_its_values_are_decoded() {
    let _alone = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let id = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    let list = one_long_list(8 << 20);
    let inside: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("tags", list.data_type().clone(), false)),
        Arc::clone(&list),
    )]));
    for column in [list, inside] {
        let file = write(vec![("id", Arc::clone(&id)), ("c", column)]);
        assert!(file.len() < 64 << 10, "{}", file.len());
        LARGEST.store(0, Ordering::Relaxed);
        let err = ingest_bytes(&file, FormatHint::Sniff, &Budget::untrusted()).unwrap_err();
        let largest = LARGEST.load(Ordering::Relaxed);
        assert_eq!(
            err,
            IngestError::Unsupported {
                format: "parquet with a list or map column"
            }
        );
        assert!(largest < 1 << 20, "allocated {largest} bytes at once");
    }
}

#[test]
fn a_struct_of_plain_fields_still_opens_as_text() {
    let _alone = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let point: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("x", DataType::Int32, false)),
        Arc::new(Int32Array::from(vec![7, 8])) as ArrayRef,
    )]));
    let table = ingest_bytes(
        &write(vec![("point", point)]),
        FormatHint::Sniff,
        &Budget::untrusted(),
    )
    .unwrap();
    assert_eq!(
        table.preview(2),
        vec![
            vec![Some("{x: 7}".to_string())],
            vec![Some("{x: 8}".to_string())]
        ]
    );
}
