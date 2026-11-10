//! Ingest plus the owned fold, linked, so `wasm-size` has a `.wasm` to measure.
//! Not a JS API. Nothing here is run.

use std::hint::black_box;
use std::path::Path;

use burrmill_ingest::{ingest_bytes, Budget, FormatHint};

fn main() {
    let sample = b"date,amount\n2024-02-01,1.50\n";
    let table = ingest_bytes(sample, FormatHint::Sniff, &Budget::untrusted()).expect("sample");
    black_box(table.report().rows);

    // Opening a directory fails in a tab. `query` stays on the Ok path, which is what keeps the
    // fold in the module: a size link that only named `open_segments` would drop it.
    let opened = burrmill::Burrmill::open_segments("t", Path::new("."));
    let rows = opened.and_then(|db| {
        db.query("SELECT 1", burrmill::Limits::default())
            .map(|a| a.rows().len())
    });
    let _ = black_box(rows);
}
