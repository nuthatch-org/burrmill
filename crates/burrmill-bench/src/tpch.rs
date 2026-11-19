//! `tpch`: the 22 TPC-H queries on Burrmill, DuckDB and, where `DFCLI` names it, datafusion-cli,
//! at one thread count, over Parquet that `tpchgen-cli` wrote (#346). Each Burrmill answer is
//! compared with DuckDB's before any time is printed; a query Burrmill refuses is listed with its
//! reason. Times are the best of `RUNS` warm runs.
//!
//! `DIR=<dir of <table>.parquet or <table>/>` (required), `THREADS` (8), `RUNS` (3), `BUDGET` in
//! bytes (2 GiB), `QUERIES` as `1,6,15` (all), `DFCLI=<path to datafusion-cli>`.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn query(n: usize) -> anyhow::Result<String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("tpch/q{n}.sql"));
    Ok(std::fs::read_to_string(path)?
        .trim()
        .trim_end_matches(';')
        .to_string())
}

/// Where `table` is under `dir`: a file or a directory of parts, whichever tpchgen-cli wrote.
fn location(dir: &Path, table: &str) -> anyhow::Result<PathBuf> {
    for p in [dir.join(format!("{table}.parquet")), dir.join(table)] {
        if p.exists() {
            return Ok(p);
        }
    }
    anyhow::bail!("no {table} under {}", dir.display())
}

fn best<T>(runs: usize, mut f: impl FnMut() -> Result<T, String>) -> Result<(Duration, T), String> {
    let mut best: Option<(Duration, T)> = None;
    for _ in 0..runs.max(1) {
        let at = Instant::now();
        let out = f()?;
        let took = at.elapsed();
        if best.as_ref().is_none_or(|(b, _)| took < *b) {
            best = Some((took, out));
        }
    }
    Ok(best.expect("one run"))
}

/// Seconds datafusion-cli reports for each statement of `sql`, run once per line in one session.
fn dfcli(
    bin: &str,
    dir: &Path,
    threads: usize,
    sql: &str,
    runs: usize,
) -> Result<Duration, String> {
    let mut script = format!("SET datafusion.execution.target_partitions = {threads};\n");
    for t in TABLES {
        let loc = location(dir, t).map_err(|e| e.to_string())?;
        script += &format!(
            "CREATE EXTERNAL TABLE {t} STORED AS PARQUET LOCATION '{}';\n",
            loc.display()
        );
    }
    for _ in 0..runs.max(1) {
        script += &format!("{};\n", sql.replace('\n', " "));
    }
    let f = std::env::temp_dir().join(format!("burrmill-tpch-{}.sql", std::process::id()));
    std::fs::write(&f, script).map_err(|e| e.to_string())?;
    let out = std::process::Command::new(bin)
        .args(["--maxrows", "1", "-f"])
        .arg(&f)
        .output()
        .map_err(|e| e.to_string())?;
    let _ = std::fs::remove_file(&f);
    let text = String::from_utf8_lossy(&out.stdout);
    let secs: Vec<f64> = text
        .lines()
        .filter_map(|l| l.split("Elapsed ").nth(1))
        .filter_map(|l| l.split_whitespace().next()?.parse().ok())
        .collect();
    if !out.status.success() || secs.len() < runs.max(1) {
        let err = String::from_utf8_lossy(&out.stderr);
        return Err(err
            .lines()
            .next()
            .unwrap_or("datafusion-cli failed")
            .to_string());
    }
    let best = secs[secs.len() - runs.max(1)..]
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    Ok(Duration::from_secs_f64(best))
}

pub fn run() -> anyhow::Result<()> {
    let dir = PathBuf::from(std::env::var("DIR").map_err(|_| anyhow::anyhow!("set DIR"))?);
    let threads = crate::env_usize("THREADS", 8);
    let runs = crate::env_usize("RUNS", 3);
    let budget = crate::env_usize("BUDGET", 2 << 30);
    let dfcli_bin = std::env::var("DFCLI").ok();
    let which: Vec<usize> = match std::env::var("QUERIES") {
        Ok(q) => q.split(',').filter_map(|n| n.trim().parse().ok()).collect(),
        Err(_) => (1..=22).collect(),
    };

    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch(&format!("SET threads = {threads};"))?;
    let mut b = burrmill::Engine::builder().budget(burrmill::Budget {
        memory_bytes: budget,
        threads,
        spill: None,
    });
    for t in TABLES {
        let loc = location(&dir, t)?;
        let glob = if loc.is_dir() {
            format!("{}/**/*.parquet", loc.display())
        } else {
            loc.display().to_string()
        };
        duck.execute_batch(&format!(
            "CREATE VIEW {t} AS SELECT * FROM read_parquet('{glob}');"
        ))?;
        b = b.parquet(&loc, t);
    }
    let engine = std::sync::Arc::new(std::thread::spawn(move || b.open()).join().expect("open")?);

    println!(
        "TPC-H over {} at {threads} threads, budget {} MiB, best of {runs}",
        dir.display(),
        budget >> 20
    );
    println!("| Q | DuckDB ms | Burrmill ms | ratio | datafusion-cli ms | answer |");
    println!("|---|---:|---:|---:|---:|---|");
    let (mut agreed, mut refused, mut differ) = (0, 0, 0);
    let ms = |d: Duration| format!("{:.0}", d.as_secs_f64() * 1e3);
    for n in which {
        let sql = query(n)?;
        let (dt, want) = best(runs, || crate::fuzz::duck_rows(&duck, &sql))
            .map_err(|e| anyhow::anyhow!("DuckDB on Q{n}: {e}"))?;
        let e2 = std::sync::Arc::clone(&engine);
        let q = sql.clone();
        let got = std::thread::spawn(move || best(runs, || crate::fuzz::burrmill_rows(&e2, &q)))
            .join()
            .expect("engine thread");
        let df = dfcli_bin
            .as_deref()
            .map(|bin| {
                dfcli(bin, &dir, threads, &sql, runs).map_or_else(|e| format!("refused: {e}"), ms)
            })
            .unwrap_or_else(|| "-".into());
        match got {
            Ok((bt, g)) => {
                let ordered = sql.to_lowercase().contains("order by");
                let same = crate::fuzz::float_close(&want, &g, ordered);
                if same {
                    agreed += 1;
                } else {
                    differ += 1;
                }
                println!(
                    "| {n} | {} | {} | {:.2}x | {df} | {} |",
                    ms(dt),
                    ms(bt),
                    bt.as_secs_f64() / dt.as_secs_f64(),
                    if same {
                        "same".to_string()
                    } else {
                        format!("DIFFERS ({} rows against {})", g.len(), want.len())
                    }
                );
            }
            Err(e) => {
                refused += 1;
                let why: String = e.chars().take(140).collect();
                println!("| {n} | {} | refused | | {df} | {why} |", ms(dt));
            }
        }
    }
    println!("\n{agreed} the same as DuckDB, {refused} refused, {differ} different");
    std::thread::spawn(move || drop(engine))
        .join()
        .expect("drop engine");
    anyhow::ensure!(differ == 0, "{differ} answers differ from DuckDB's");
    Ok(())
}
