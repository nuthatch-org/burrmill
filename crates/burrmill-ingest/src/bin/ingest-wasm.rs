//! The reader, linked, so `wasm-size` has a `.wasm` to measure. Not a JS API.

use burrmill_ingest::{Budget, FormatHint, ingest_bytes};

fn main() {
    let sample = b"date,amount\n2024-02-01,1.50\n";
    let table = ingest_bytes(sample, FormatHint::Sniff, &Budget::untrusted()).expect("sample");
    std::hint::black_box(table.report().rows);
}
