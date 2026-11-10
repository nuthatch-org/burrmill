#![no_main]

use libfuzzer_sys::fuzz_target;

use burrmill_ingest::{ingest_bytes, Budget, FormatHint};

// Tighter than `Budget::untrusted` so a long input cannot ask for the 512 MiB ceiling.
// The hour-long run is the slice gate and is not what a `cargo test` does.
fuzz_target!(|data: &[u8]| {
    let budget = Budget::untrusted()
        .max_bytes(1 << 20)
        .max_rows(50_000)
        .max_columns(256)
        .max_memory_bytes(8 << 20);
    let _ = ingest_bytes(data, FormatHint::Sniff, &budget);
    let _ = ingest_bytes(data, FormatHint::Csv, &budget);
});
