//! Every `.parquet` file under a directory as table `t`, with exact decimals.
//!
//! cargo run --example parquet_dir --features datafusion -- data/ 'SELECT count(*) FROM t'

use std::path::Path;

use arrow::util::pretty::print_batches;
use burrmill::Engine;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let dir = args.next().ok_or("usage: parquet_dir <dir> <sql>")?;
    let sql = args.next().ok_or("usage: parquet_dir <dir> <sql>")?;

    let engine = Engine::builder().parquet(Path::new(&dir), "t").open()?;
    engine.set_strict_decimals(true);
    print_batches(&engine.sql(&sql)?)?;
    Ok(())
}
