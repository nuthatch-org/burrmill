//! `tpch`: the 22 TPC-H queries on Burrmill, DuckDB and, where `DFCLI` names it, datafusion-cli,
//! at one thread count, over Parquet that `tpchgen-cli` wrote (#346). Each Burrmill answer is
//! compared with DuckDB's before any time is printed; a query Burrmill refuses is listed with its
//! reason. Times are the best of `RUNS` warm runs.
//!
//! `DIR=<dir of <table>.parquet or <table>/>` (required), `THREADS` (8), `RUNS` (3), `BUDGET` in
//! bytes (2 GiB), `QUERIES` as `1,6,15` (all), `DFCLI=<path to datafusion-cli>`.
//!
//! `SPILL=<dir>` gives Burrmill's budget a spill directory there, capped at `SPILL_CAP` bytes (500
//! GiB), and the table then gives the most the directory held during a query. `ENGINE=burrmill` or
//! `duckdb` runs one engine alone, comparing nothing, so the peak RSS printed for it is its own:
//! the high-water mark is reset before each query (`/proc/self/clear_refs`), but what one engine's
//! allocator kept would count against the other. `ANALYZE=1` prints Burrmill's `EXPLAIN ANALYZE`
//! of each query after its runs, for the operators' memory and spill metrics; `ANALYZE=plan`, its
//! `EXPLAIN`.
//!
//! `DUCKDB=<path to the duckdb CLI>` runs DuckDB as that CLI rather than the bundled library, for
//! its answers and its times (`duckcli`); its peak RSS is then the timed CLI session's (#414).

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

/// Resets this process's peak RSS to what it holds now; Linux only, and a no-op elsewhere.
fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

/// This process's peak RSS in MiB since the last [`reset_peak_rss`], from `VmHWM`.
fn peak_rss_mib() -> String {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            let l = s.lines().find(|l| l.starts_with("VmHWM:"))?;
            let kb: u64 = l.split_whitespace().nth(1)?.parse().ok()?;
            Some(format!("{}", kb >> 10))
        })
        .unwrap_or_else(|| "-".into())
}

/// Bytes the files under `dir` hold now.
fn dir_bytes(dir: &Path) -> u64 {
    walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}

/// The `spilled_bytes` of every operator in an `EXPLAIN ANALYZE`, summed.
fn spilled_in_all(plan: &str) -> u64 {
    plan.split("spilled_bytes=")
        .skip(1)
        .filter_map(|m| {
            let mut w = m.split_whitespace();
            let n: f64 = w.next()?.parse().ok()?;
            let unit = match w.next()?.trim_end_matches([',', ']']) {
                "B" => 1u64,
                "KB" => 1 << 10,
                "MB" => 1 << 20,
                "GB" => 1 << 30,
                "TB" => 1 << 40,
                _ => return None,
            };
            Some((n * unit as f64) as u64)
        })
        .sum()
}

/// The most `dir` held while `f` ran, sampled every 20 ms.
fn with_spill_peak<T>(dir: Option<&Path>, f: impl FnOnce() -> T) -> (T, Option<u64>) {
    let Some(dir) = dir else {
        return (f(), None);
    };
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (d, s) = (dir.to_path_buf(), std::sync::Arc::clone(&stop));
    let watcher = std::thread::spawn(move || {
        let mut peak = 0;
        while !s.load(std::sync::atomic::Ordering::Relaxed) {
            peak = peak.max(dir_bytes(&d));
            std::thread::sleep(Duration::from_millis(20));
        }
        peak.max(dir_bytes(&d))
    });
    let out = f();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    (out, Some(watcher.join().expect("spill watcher")))
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
    let spill = std::env::var("SPILL").ok().map(PathBuf::from);
    let spill_cap = crate::env_usize("SPILL_CAP", 500 << 30) as u64;
    let engine_only = std::env::var("ENGINE").unwrap_or_else(|_| "both".into());
    let (run_duck, run_burr) = match engine_only.as_str() {
        "both" => (true, true),
        "burrmill" => (false, true),
        "duckdb" => (true, false),
        e => anyhow::bail!("ENGINE={e}: both, burrmill or duckdb"),
    };
    let analyze = crate::env_flag("ANALYZE");
    let explain = if std::env::var("ANALYZE").as_deref() == Ok("plan") {
        "EXPLAIN"
    } else {
        "EXPLAIN ANALYZE"
    };
    let which: Vec<usize> = match std::env::var("QUERIES") {
        Ok(q) => q.split(',').filter_map(|n| n.trim().parse().ok()).collect(),
        Err(_) => (1..=22).collect(),
    };

    let duck_cli = std::env::var("DUCKDB").ok();
    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch(&format!("SET threads = {threads};"))?;
    let mut duck_setup = format!("SET threads = {threads};\n");
    let mut b = burrmill::Engine::builder().budget(burrmill::Budget {
        memory_bytes: budget,
        threads,
        spill: spill.clone().map(|d| (d, spill_cap)),
    });
    for t in TABLES {
        let loc = location(&dir, t)?;
        let glob = if loc.is_dir() {
            format!("{}/**/*.parquet", loc.display())
        } else {
            loc.display().to_string()
        };
        let view = format!("CREATE VIEW {t} AS SELECT * FROM read_parquet('{glob}');");
        duck_setup += &view;
        duck_setup += "\n";
        if duck_cli.is_none() {
            duck.execute_batch(&view)?;
        }
        b = b.parquet(&loc, t);
    }
    let engine = std::sync::Arc::new(std::thread::spawn(move || b.open()).join().expect("open")?);
    let duck_version = match &duck_cli {
        Some(bin) => format!("CLI {bin}, {}", crate::duckcli::version(bin)?),
        None => duck.query_row("SELECT version()", [], |r| r.get::<_, String>(0))?,
    };
    println!("DuckDB: {duck_version}");

    println!(
        "TPC-H over {} at {threads} threads, budget {} MiB{}, best of {runs}, engines: {engine_only}",
        dir.display(),
        budget >> 20,
        spill
            .as_ref()
            .map_or(String::new(), |d| format!(", spilling to {}", d.display()))
    );
    println!(
        "| Q | DuckDB ms | DuckDB peak RSS MiB | Burrmill ms | Burrmill peak RSS MiB | most spilled MiB | ratio | datafusion-cli ms | answer |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---|");
    let (mut agreed, mut refused, mut differ) = (0, 0, 0);
    let ms = |d: Duration| format!("{:.0}", d.as_secs_f64() * 1e3);
    for n in which {
        let sql = query(n)?;
        reset_peak_rss();
        let mut cli_rss = None;
        let (dt, want) = match (&duck_cli, run_duck) {
            (_, false) => (Duration::ZERO, Vec::new()),
            (None, true) => best(runs, || crate::fuzz::duck_rows(&duck, &sql))
                .map_err(|e| anyhow::anyhow!("DuckDB on Q{n}: {e}"))?,
            (Some(bin), true) => {
                let want = crate::duckcli::rows(bin, &duck_setup, &sql)
                    .map_err(|e| anyhow::anyhow!("DuckDB on Q{n}: {e}"))?;
                let (dt, rss) = crate::duckcli::best(bin, &duck_setup, &[&sql], runs)
                    .map_err(|e| anyhow::anyhow!("DuckDB on Q{n}: {e}"))?;
                cli_rss = Some(rss);
                (dt, want)
            }
        };
        let duck_rss = match cli_rss {
            Some(rss) => rss.to_string(),
            None if run_duck => peak_rss_mib(),
            None => "-".into(),
        };
        if !run_burr {
            println!("| {n} | {} | {duck_rss} | - | - | - | | - | - |", ms(dt));
            continue;
        }
        let e2 = std::sync::Arc::clone(&engine);
        let q = sql.clone();
        reset_peak_rss();
        let base_rss = peak_rss_mib();
        engine.take_memory_peak();
        let (got, spilled) = with_spill_peak(spill.as_deref(), || {
            std::thread::spawn(move || best(runs, || crate::fuzz::burrmill_rows(&e2, &q)))
                .join()
                .expect("engine thread")
        });
        let burr_rss = peak_rss_mib();
        eprintln!(
            "Q{n}: Burrmill's pool held at most {} MiB; the process held {base_rss} MiB before",
            engine.take_memory_peak() >> 20
        );
        let spilled = spilled.map_or("-".into(), |b| format!("{}", b >> 20));
        if analyze {
            let e2 = std::sync::Arc::clone(&engine);
            let q = format!("{explain} {sql}");
            let plan = std::thread::spawn(move || e2.sql(&q))
                .join()
                .expect("engine thread");
            match plan {
                Ok(batches) => {
                    let text = arrow::util::pretty::pretty_format_batches(&batches)?;
                    eprintln!("Q{n} {explain}\n{text}");
                    eprintln!(
                        "Q{n}: operators spilled {} MiB in all",
                        spilled_in_all(&text.to_string()) >> 20
                    );
                }
                Err(e) => eprintln!("Q{n} {explain} refused: {e}"),
            }
        }
        let df = dfcli_bin
            .as_deref()
            .map(|bin| {
                dfcli(bin, &dir, threads, &sql, runs).map_or_else(|e| format!("refused: {e}"), ms)
            })
            .unwrap_or_else(|| "-".into());
        match got {
            Ok((bt, _)) if !run_duck => println!(
                "| {n} | - | - | {} | {burr_rss} | {spilled} | | {df} | not compared |",
                ms(bt)
            ),
            Ok((bt, g)) => {
                let ordered = sql.to_lowercase().contains("order by");
                let same = crate::fuzz::float_close(&want, &g, ordered);
                if same {
                    agreed += 1;
                } else {
                    differ += 1;
                }
                println!(
                    "| {n} | {} | {duck_rss} | {} | {burr_rss} | {spilled} | {:.2}x | {df} | {} |",
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
                eprintln!("Q{n} refused: {e}");
                let why: String = e.chars().take(140).collect();
                println!(
                    "| {n} | {} | {duck_rss} | refused | {burr_rss} | {spilled} | | {df} | {why} |",
                    ms(dt)
                );
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
