//! `csv-ingest`: how fast Burrmill reads a large CSV into a table, against DuckDB's `read_csv`
//! (#389). The CSV is generated into a temporary directory, the same bytes on every machine, and
//! deleted when the run ends. Both engines answer one statement over every column, compared before
//! any time is printed; then each reads the file into memory, best of `RUNS`: Burrmill through
//! `EngineBuilder::csv` (the tab's reader, one thread), DuckDB through
//! `CREATE TABLE AS SELECT * FROM read_csv(...)` at `THREADS` and at one thread.
//!
//! `BYTES` of CSV (1,000,000,000), `THREADS` (8), `RUNS` (3), `BUDGET` in bytes (8 GiB).

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use arrow::util::display::array_value_to_string;

/// Rows until the file holds `bytes`, six columns: an id, a two-place amount, a date, a short
/// name, a small integer and a boolean.
fn generate(path: &Path, bytes: u64) -> anyhow::Result<u64> {
    let mut w = std::io::BufWriter::new(std::fs::File::create(path)?);
    let mut written = 0u64;
    let header = "id,amount,day,who,qty,ok\n";
    w.write_all(header.as_bytes())?;
    written += header.len() as u64;
    let mut rows = 0u64;
    let mut line = String::new();
    while written < bytes {
        rows += 1;
        let i = rows;
        line.clear();
        use std::fmt::Write as _;
        writeln!(
            line,
            "{i},{}.{:02},2024-{:02}-{:02},w{},{},{}",
            1 + (i * 7919) % 99_999,
            (i * 31) % 100,
            1 + i % 12,
            1 + (i / 12) % 28,
            (i * 104_729) % 5000,
            (i * 13) % 1000,
            i.is_multiple_of(3)
        )?;
        w.write_all(line.as_bytes())?;
        written += line.len() as u64;
    }
    w.flush()?;
    Ok(rows)
}

fn best(runs: usize, mut f: impl FnMut() -> anyhow::Result<()>) -> anyhow::Result<Duration> {
    let mut best = Duration::MAX;
    for _ in 0..runs.max(1) {
        let at = Instant::now();
        f()?;
        best = best.min(at.elapsed());
    }
    Ok(best)
}

const COLUMNS: usize = 7;

/// Every column read, each answer as text so the two engines' types need not agree.
fn statement(table: &str, amount: &str) -> String {
    format!(
        "SELECT CAST(COUNT(*) AS VARCHAR), CAST(SUM(id) AS VARCHAR), CAST(SUM({amount}) AS VARCHAR), \
         CAST(MIN(day) AS VARCHAR), MAX(who), CAST(SUM(qty) AS VARCHAR), \
         CAST(SUM(CASE WHEN ok THEN 1 ELSE 0 END) AS VARCHAR) FROM {table}"
    )
}

pub fn run() -> anyhow::Result<()> {
    let bytes = crate::env_usize("BYTES", 1_000_000_000) as u64;
    let threads = crate::env_usize("THREADS", 8);
    let runs = crate::env_usize("RUNS", 3);
    let memory = crate::env_usize("BUDGET", 8 << 30);
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("trades.csv");
    let rows = generate(&path, bytes)?;
    let size = std::fs::metadata(&path)?.len();
    let shown = path.display().to_string();
    let budget = || burrmill::Budget {
        memory_bytes: memory,
        threads,
        spill: None,
    };
    let open = || {
        burrmill::Engine::builder()
            .csv(&path, "trades")
            .budget(budget())
            .open()
    };

    // Parity first, untimed.
    let engine = open()?;
    let ours = engine.sql(&statement("trades", "amount"))?;
    let ours: Vec<String> = (0..COLUMNS)
        .map(|i| array_value_to_string(ours[0].column(i), 0))
        .collect::<Result<_, _>>()?;
    let all = engine.sql("SELECT * FROM trades")?;
    let types: Vec<String> = all[0]
        .schema()
        .fields()
        .iter()
        .map(|f| format!("{} {}", f.name(), f.data_type()))
        .collect();
    drop((all, engine));
    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch(&format!("SET threads = {threads};"))?;
    // DuckDB reads the amount as DOUBLE; summed as the two-place decimal it was written as.
    let theirs: Vec<String> = duck.query_row(
        &statement(
            &format!("read_csv('{shown}')"),
            "CAST(amount AS DECIMAL(18,2))",
        ),
        [],
        |r| (0..COLUMNS).map(|i| r.get::<_, String>(i)).collect(),
    )?;
    anyhow::ensure!(
        ours == theirs,
        "the answers differ: burrmill {ours:?}, duckdb {theirs:?}"
    );
    let answer = ours;

    let ours = best(runs, || {
        open()?;
        Ok(())
    })?;
    let duck_at = |n: usize| -> anyhow::Result<Duration> {
        duck.execute_batch(&format!("SET threads = {n};"))?;
        best(runs, || {
            duck.execute_batch(&format!(
                "CREATE OR REPLACE TABLE t AS SELECT * FROM read_csv('{shown}'); DROP TABLE t;"
            ))?;
            Ok(())
        })
    };
    let theirs = duck_at(threads)?;
    let theirs_one = duck_at(1)?;

    let mb = size as f64 / 1e6;
    let line = |who: &str, d: Duration| {
        println!(
            "{who:<36} {:>8.2} s {:>8.1} MB/s",
            d.as_secs_f64(),
            mb / d.as_secs_f64()
        )
    };
    println!(
        "CSV of {size} bytes, {rows} rows, best of {runs}; burrmill budget {} MiB",
        memory >> 20
    );
    println!("burrmill types: {}", types.join(", "));
    println!("answers agree: {answer:?}");
    line("burrmill EngineBuilder::csv (1 thread)", ours);
    line(&format!("duckdb read_csv ({threads} threads)"), theirs);
    line("duckdb read_csv (1 thread)", theirs_one);
    println!(
        "burrmill / duckdb: {:.2} at {threads} threads, {:.2} at 1",
        ours.as_secs_f64() / theirs.as_secs_f64(),
        ours.as_secs_f64() / theirs_one.as_secs_f64()
    );
    Ok(())
}
