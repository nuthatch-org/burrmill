//! `json-ingest`: how fast Burrmill reads a large NDJSON file, and the same records as one JSON
//! array, into a table, against DuckDB's `read_ndjson` and `read_json` (#413). The files are
//! generated into a temporary directory, the same bytes on every machine, and deleted when the run
//! ends. Both engines answer one statement over every column, compared before any time is printed;
//! then each reads each file into memory, best of `RUNS`, at `THREADS` and at one thread: Burrmill
//! through `EngineBuilder::json` (the tab's reader) under a budget of that many threads, DuckDB
//! through `CREATE TABLE AS SELECT * FROM read_ndjson(...)` or `read_json(..., format = 'array')`.
//!
//! `BYTES` of NDJSON (1,000,000,000), `THREADS` (8), `RUNS` (3), `BUDGET` in bytes (4 GiB, the
//! CLI's default `--memory`). `json-ingest PATH` only writes the NDJSON file to `PATH`.

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use arrow::util::display::array_value_to_string;

/// Records until the file holds `bytes`, the CSV bench's six columns: an id, a two-place amount,
/// a date, a short name, a small integer and a boolean. One a line, or one array of them.
fn generate(path: &Path, bytes: u64, array: bool) -> anyhow::Result<u64> {
    let mut w = std::io::BufWriter::new(std::fs::File::create(path)?);
    let mut written = 0u64;
    if array {
        w.write_all(b"[\n")?;
        written += 2;
    }
    let mut rows = 0u64;
    let mut line = String::new();
    while written < bytes {
        rows += 1;
        let i = rows;
        line.clear();
        use std::fmt::Write as _;
        if array && i > 1 {
            line.push_str(",\n");
        }
        write!(
            line,
            "{{\"id\":{i},\"amount\":{}.{:02},\"day\":\"2024-{:02}-{:02}\",\"who\":\"w{}\",\"qty\":{},\"ok\":{}}}",
            1 + (i * 7919) % 99_999,
            (i * 31) % 100,
            1 + i % 12,
            1 + (i / 12) % 28,
            (i * 104_729) % 5000,
            (i * 13) % 1000,
            i.is_multiple_of(3)
        )?;
        if !array {
            line.push('\n');
        }
        w.write_all(line.as_bytes())?;
        written += line.len() as u64;
    }
    if array {
        w.write_all(b"\n]\n")?;
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
    if let Some(out) = std::env::args().nth(2) {
        let rows = generate(Path::new(&out), bytes, false)?;
        println!("{out}: {rows} records");
        return Ok(());
    }
    let threads = crate::env_usize("THREADS", 8);
    let runs = crate::env_usize("RUNS", 3);
    let memory = crate::env_usize("BUDGET", 4 << 30);
    let dir = tempfile::tempdir()?;
    let duck = duckdb::Connection::open_in_memory()?;
    let mut lines = Vec::new();
    for (shape, array, reader) in [
        ("NDJSON", false, "read_ndjson('{}')"),
        ("JSON array", true, "read_json('{}', format = 'array')"),
    ] {
        let path = dir.path().join(if array {
            "trades.json"
        } else {
            "trades.ndjson"
        });
        let rows = generate(&path, bytes, array)?;
        let size = std::fs::metadata(&path)?.len();
        let reader = reader.replace("{}", &path.display().to_string());
        let open = |threads: usize| {
            burrmill::Engine::builder()
                .json(&path, "trades")
                .budget(burrmill::Budget {
                    memory_bytes: memory,
                    threads,
                    spill: None,
                })
                .open()
        };

        // Parity first, untimed.
        let engine = open(threads)?;
        let ours = engine.sql(&statement("trades", "amount"))?;
        let ours: Vec<String> = (0..COLUMNS)
            .map(|i| array_value_to_string(ours[0].column(i), 0))
            .collect::<Result<_, _>>()?;
        let all = engine.sql("SELECT * FROM trades LIMIT 1")?;
        let types: Vec<String> = all[0]
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{} {}", f.name(), f.data_type()))
            .collect();
        drop((all, engine));
        duck.execute_batch(&format!("SET threads = {threads};"))?;
        // DuckDB reads the amount as DOUBLE; summed as the two-place decimal it was written as.
        let theirs: Vec<String> = duck.query_row(
            &statement(&reader, "CAST(amount AS DECIMAL(18,2))"),
            [],
            |r| (0..COLUMNS).map(|i| r.get::<_, String>(i)).collect(),
        )?;
        anyhow::ensure!(
            ours == theirs,
            "{shape}: the answers differ: burrmill {ours:?}, duckdb {theirs:?}"
        );

        let ours_at = |n: usize| {
            best(runs, || {
                open(n)?;
                Ok(())
            })
        };
        let ours_many = ours_at(threads)?;
        let ours_one = ours_at(1)?;
        let duck_at = |n: usize| -> anyhow::Result<Duration> {
            duck.execute_batch(&format!("SET threads = {n};"))?;
            best(runs, || {
                duck.execute_batch(&format!(
                    "CREATE OR REPLACE TABLE t AS SELECT * FROM {reader}; DROP TABLE t;"
                ))?;
                Ok(())
            })
        };
        let theirs_many = duck_at(threads)?;
        let theirs_one = duck_at(1)?;
        std::fs::remove_file(&path)?;

        let mb = size as f64 / 1e6;
        let line = |who: &str, d: Duration| {
            format!(
                "{who:<44} {:>8.2} s {:>8.1} MB/s",
                d.as_secs_f64(),
                mb / d.as_secs_f64()
            )
        };
        let duck_name = reader.split('(').next().unwrap_or("read_json").to_string();
        lines.push(format!(
            "{shape} of {size} bytes, {rows} records, best of {runs}; burrmill budget {} MiB",
            memory >> 20
        ));
        lines.push(format!("burrmill types: {}", types.join(", ")));
        lines.push(format!("answers agree: {ours:?}"));
        lines.push(line(
            &format!("burrmill EngineBuilder::json ({threads} threads)"),
            ours_many,
        ));
        lines.push(line(
            &format!("duckdb {duck_name} ({threads} threads)"),
            theirs_many,
        ));
        lines.push(line("burrmill EngineBuilder::json (1 thread)", ours_one));
        lines.push(line(&format!("duckdb {duck_name} (1 thread)"), theirs_one));
        lines.push(format!(
            "burrmill / duckdb: {:.2} at {threads} threads, {:.2} at 1",
            ours_many.as_secs_f64() / theirs_many.as_secs_f64(),
            ours_one.as_secs_f64() / theirs_one.as_secs_f64()
        ));
    }
    for l in lines {
        println!("{l}");
    }
    Ok(())
}
