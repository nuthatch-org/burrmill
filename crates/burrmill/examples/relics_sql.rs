//! Run SQL files against a sealed Legacy relic and print what Burrmill returns or refuses.
//!
//! `cargo run --release --example relics_sql --features relics -- <relic-dir> <file.sql>...`
//!
//! Each file is one statement. A refusal is printed as the error Burrmill raised, because the
//! refusal is part of the result (RFC-0004 §12.3).

use std::time::Instant;

use arrow::util::pretty::pretty_format_batches;
use burrmill::Engine;

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(root) = args.next() else {
        eprintln!("usage: relics_sql <relic-dir> <file.sql>...");
        std::process::exit(2);
    };
    let opened = Instant::now();
    let engine = match Engine::open_relics(std::path::Path::new(&root)) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("open failed: {e}");
            std::process::exit(1);
        }
    };
    println!("-- opened {root} in {:.2}s", opened.elapsed().as_secs_f64());
    for path in args {
        let sql = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        println!("\n-- {path}\n{}", sql.trim());
        let started = Instant::now();
        match engine.sql(&sql) {
            Ok(batches) => {
                println!("{}", pretty_format_batches(&batches).expect("format"));
                println!("-- ok in {:.2}s", started.elapsed().as_secs_f64());
            }
            Err(e) => {
                println!("-- refused in {:.2}s: {e}", started.elapsed().as_secs_f64());
            }
        }
    }
}
