//! Ingest plus the full Engine, one module: the single download a schema-mode page would make.
//! Not a JS API.

use std::hint::black_box;
use std::path::Path;

use burrmill_ingest::{ingest_bytes, Budget, FormatHint};

fn main() {
    let sample = b"date,amount\n2024-02-01,1.50\n";
    let table = ingest_bytes(sample, FormatHint::Sniff, &Budget::untrusted()).expect("sample");
    black_box(table.report().rows);

    black_box(burrmill::Engine::open_segments(Path::new(".")).err());
    let Ok(mut engine) = burrmill::Engine::open_empty() else {
        return;
    };
    let rows: &[serde_json::Value] = &[];
    black_box(engine.register_rows("t", rows).err());
    let _ = black_box(engine.sql_async("SELECT 1"));
}
