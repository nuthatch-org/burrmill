#![no_main]

use libfuzzer_sys::fuzz_target;

use burrmill::{Budget, Engine};

thread_local! {
    static ENGINE: Engine = engine();
}

// A view to name, with the columns the dialect rewrites have types to work on.
fn engine() -> Engine {
    let mut e = Engine::open_empty_budgeted(Budget {
        memory_bytes: 64 << 20,
        threads: 1,
        spill: None,
    })
    .expect("engine");
    e.register_view(
        "t",
        "SELECT * FROM (VALUES (1, 'a', 1.5, CAST(2 AS HUGEINT)), (2, NULL, NULL, NULL)) \
         v(block_number, who, amount, total)",
    )
    .expect("view");
    e
}

// Parse, the dialect rewrites, the analyzer and optimiser rules and physical planning, but not
// execution, so a slow statement is not a finding. A refusal is an answer; a panic is not.
fuzz_target!(|data: &[u8]| {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };
    let _ = burrmill::inspect::reach(sql);
    ENGINE.with(|e| {
        let _ = e.parquet_scans(sql);
    });
});
