//! The full Engine, linked. Not a JS API. The future is built and not polled: there is no
//! JS runtime in a size link, and constructing it is what keeps `sql_async`.

use std::hint::black_box;
use std::path::Path;

fn main() {
    black_box(burrmill::Engine::open_segments(Path::new(".")).err());
    let Ok(mut engine) = burrmill::Engine::open_empty() else {
        return;
    };
    let rows: &[serde_json::Value] = &[];
    black_box(engine.register_rows("t", rows).err());
    let _ = black_box(engine.sql_async("SELECT 1"));
}
