//! RFC-0044 slice 1: the go/no-go.
//!
//! Reproduce nuthatch's #987 result in Burrmill proper - the owned operator at or under DuckDB's
//! latency on `net_balances`, at exact parity, across a segment sweep. If that does not reproduce,
//! the thesis is unproven and the honest outcome is to stop here cheaply.
//!
//! The discipline is RFC-0004's and it is not decoration:
//!
//! - **Parity first, untimed, before a single figure is printed.** An earlier version of the nuthatch
//!   gate emitted its RESULT line and *then* bailed on a parity failure, so an invalid comparison
//!   could be copied into a record before anyone read the error. Timings from engines that disagree
//!   are not fast-versus-slow, they are meaningless.
//! - **Run order is a confound.** Whichever engine goes first pays to warm the page cache and the
//!   second gets it free; the first such measurement was 3.9x apart for exactly that reason.
//!   `ORDER=burrmill_first` runs it the other way. A ratio that survives both orderings is about the
//!   engines.
//! - **Repeats inside one process**, median reported. Writing a ten-thousand-file fixture costs far
//!   more than the query, so a fresh process per repeat would be measuring the fixture writer.
//! - **`BREAK_PARITY=1`** drops a row from the candidate on purpose. The run must then refuse with no
//!   RESULT line. A guard nobody has watched refuse is not a guard.

mod df_views;
mod dialect_parity;
mod encode_parity;
mod engine_views;
mod error_parity;
mod fixture;
mod fuzz;
mod generate;
mod oracles;
mod reach_parity;
mod scan_parity;
mod serve;
mod shapes;
mod views;

use std::path::{Path, PathBuf};
use std::time::Instant;

use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use fixture::FixtureSpec;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// `is_ok()` alone is true for an **empty** value, so `FLAG=` would read as set. That exact bug
/// silently forced one ordering for a whole sweep, and it was found by the ordering control - which
/// is the point of having one.
fn env_flag(key: &str) -> bool {
    std::env::var(key)
        .map(|v| !matches!(v.trim(), "" | "0" | "false"))
        .unwrap_or(false)
}

/// **Peak** resident set size, not current.
///
/// This used to read `ps -o rss=` on macOS, which reports the process's RSS *right now*. Peak memory
/// in a fold happens while the aggregate and the answer are both live, and by the time the harness
/// asks, that moment has passed and the allocator has handed pages back. The Linux branch read
/// `VmHWM`, a true high-water mark, so the same code reported two different kinds of number
/// depending on the machine and the friendlier one came from the development laptop. That is the
/// third measurement defect found in a day and it is the same shape as the other two: the harness
/// flattering the thing it was built to check.
///
/// `getrusage` gives a genuine high-water mark on both. The unit does not agree between them -
/// bytes on macOS, kilobytes on Linux - which is a trap worth naming rather than a curiosity.
fn rss_mb() -> u64 {
    // SAFETY: `getrusage` writes a plain POD struct through the pointer and returns 0 on success.
    // Zeroed is a valid `rusage`, and nothing here retains the pointer.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return 0;
        }
        let max = ru.ru_maxrss as u64;
        if cfg!(target_os = "macos") {
            max >> 20
        } else {
            max >> 10
        }
    }
}

#[cfg(feature = "alloc-stats")]
mod heap;
#[cfg(feature = "alloc-stats")]
#[global_allocator]
static ALLOC: heap::Counting = heap::Counting;
#[cfg(all(
    not(feature = "alloc-stats"),
    target_os = "linux",
    not(target_env = "musl")
))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    // DuckDB follows the host's zone, as it does in nuthatch, which sets none; hosted servers run in
    // UTC. Set here, before any thread exists. `SET TimeZone` would consult the zone on every query,
    // which switches on a DuckDB bug (docs/upstream/duckdb-cast-comparison-null-constant.md) that
    // nuthatch meets only in queries that consult it themselves.
    std::env::set_var("TZ", "UTC");
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(bench_main())
}

async fn bench_main() -> anyhow::Result<()> {
    #[cfg(all(feature = "alloc-stats", target_os = "linux"))]
    let sampled = heap::sample_peak();
    let r = run().await;
    #[cfg(feature = "alloc-stats")]
    eprintln!("HEAP\tpeak_live_mb={}", heap::peak_mb());
    #[cfg(all(feature = "alloc-stats", target_os = "linux"))]
    {
        let (rss, file, live) = *sampled.lock().unwrap();
        let mb = |b: usize| b / (1024 * 1024);
        eprintln!(
            "ATPEAK\trss_mb={}\tfile_mb={}\tanon_mb={}\tlive_mb={}",
            mb(rss),
            mb(file),
            mb(rss - file),
            mb(live)
        );
    }
    r
}

async fn run() -> anyhow::Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some("inspect") => inspect(),
        Some("explain") => explain(),
        Some("fold") => fold_only(),
        Some("df-fold") => df_fold().await,
        Some("encode-parity") => encode_parity::run(),
        Some("error-parity") => error_parity::run(),
        Some("dialect-parity") => dialect_parity::run(),
        Some("reach-parity") => reach_parity::run(),
        Some("scan-parity") => scan_parity::run(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: scan-parity <nest>"))?,
        ),
        Some("refs-parity") => reach_parity::run_refs(std::env::args().nth(2).as_deref()),
        Some("engine-analyze") => engine_views::analyze(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: engine-analyze <nest> <view>"))?,
            &std::env::args()
                .nth(3)
                .ok_or_else(|| anyhow::anyhow!("usage: engine-analyze <nest> <view>"))?,
        ),
        Some("engine-explain") => engine_views::explain(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: engine-explain <nest> <view>"))?,
            &std::env::args()
                .nth(3)
                .ok_or_else(|| anyhow::anyhow!("usage: engine-explain <nest> <view>"))?,
        ),
        Some("engine-sql") => engine_views::sql_files(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: engine-sql <nest> <file>..."))?,
            &std::env::args().skip(3).collect::<Vec<_>>(),
        ),
        Some("serve-views") => serve::run_views(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: serve-views <nest>"))?,
        ),
        Some("rewrite-parity") => engine_views::rewrite_parity(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: rewrite-parity <nest> <views-dir>"))?,
            &std::env::args()
                .nth(3)
                .ok_or_else(|| anyhow::anyhow!("usage: rewrite-parity <nest> <views-dir>"))?,
        ),
        Some("engine-views") => engine_views::run(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: engine-views <nest>"))?,
        ),
        Some("duck-keywords") => dialect_parity::duck_keywords(),
        Some("fuzz") => fuzz::run(),
        Some("duck-eval") => dialect_parity::duck_eval(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: duck-eval <sql>"))?,
        ),
        Some("duck-names") => dialect_parity::duck_names(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: duck-names <sql>"))?,
        ),
        Some("nest") => nest(),
        Some("gen") => generated(),
        Some("cast") => cast_table(),
        Some("serve") => serve::run(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: serve <fixture-dir>"))?,
        ),
        Some("views") => views::run(
            &std::env::args()
                .nth(2)
                .ok_or_else(|| anyhow::anyhow!("usage: views <segments> <views>"))?,
            &std::env::args()
                .nth(3)
                .ok_or_else(|| anyhow::anyhow!("usage: views <segments> <views>"))?,
        ),
        Some("df-views") => {
            df_views::run(
                &std::env::args()
                    .nth(2)
                    .ok_or_else(|| anyhow::anyhow!("usage: df-views <nest-dir>"))?,
            )
            .await
        }
        Some("shapes") => shapes::run(&std::env::args().skip(2).collect::<Vec<_>>()),
        Some("slt") => slt_against_duckdb(),
        Some("duckdb-gaps") => duckdb_gaps(),
        _ => bench().await,
    }
}

/// Print a segment's schema. Needed before pointing the fold at a real nest, because a nest table's
/// columns are whatever the contract's event declared and guessing them is how you get a parity
/// failure that looks like an engine bug.
fn inspect() -> anyhow::Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let path = std::env::args()
        .nth(2)
        .ok_or_else(|| anyhow::anyhow!("usage: inspect <file|dir>"))?;
    let p = PathBuf::from(&path);
    let file = if p.is_dir() {
        std::fs::read_dir(&p)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|f| f.extension().is_some_and(|x| x == "parquet"))
            .ok_or_else(|| anyhow::anyhow!("no parquet under {path}"))?
    } else {
        p
    };
    let b = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&file)?)?;
    let md = b.metadata();
    println!("file:       {}", file.display());
    println!("rows:       {}", md.file_metadata().num_rows());
    println!("row groups: {}", md.num_row_groups());
    println!("schema:");
    for f in b.schema().fields() {
        println!("  {:<28} {}", f.name(), f.data_type());
    }
    Ok(())
}

/// `EXPLAIN` against a directory of segments, without running anything.
fn explain() -> anyhow::Result<()> {
    let dir = std::env::args()
        .nth(2)
        .ok_or_else(|| anyhow::anyhow!("usage: explain <dir>"))?;
    let db = burrmill::Burrmill::open_segments("t", Path::new(&dir))?;
    let sql = std::env::args().nth(3).unwrap_or_else(|| {
        "SELECT addr, SUM(d) AS net FROM (\
           SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
           UNION ALL \
           SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
         ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"
            .into()
    });
    println!("{}", db.explain(&sql)?);
    Ok(())
}

async fn bench() -> anyhow::Result<()> {
    let spec = FixtureSpec {
        rows: env_usize("ROWS", 2_000_000),
        segments: env_usize("SEGMENTS", 1).max(1),
        addrs: env_usize("ADDRS", 512),
        // Nest-shaped by default. NARROW=1 writes the old four-column schema, which is only useful
        // for measuring what projection pushdown is worth - see fixture.rs.
        narrow: env_flag("NARROW"),
    };
    let repeats = env_usize("REPEATS", 1).max(1);
    let burrmill_first = matches!(std::env::var("ORDER").as_deref(), Ok("burrmill_first"));

    let tmp = tempfile::tempdir()?;
    let seg: PathBuf = match std::env::var("KEEP_FIXTURE") {
        Ok(d) => {
            std::fs::create_dir_all(&d)?;
            PathBuf::from(d)
        }
        Err(_) => tmp.path().to_path_buf(),
    };

    println!(
        "== RFC-0044 slice 1 gate: {} rows / {} segment(s) / {} distinct parties ==",
        spec.rows, spec.segments, spec.addrs
    );
    let t = Instant::now();
    let written = fixture::write(&seg, &spec)?;
    println!(
        "fixture: {written} files in {:?} (rss {} MB)",
        t.elapsed(),
        rss_mb()
    );

    // ---- parity, first, untimed ------------------------------------------------------------
    let (duck0, _) = oracles::duckdb(&seg)?;
    let (df0, _) = oracles::datafusion(&seg).await?;
    let (mut bm0, _, metrics) = oracles::burrmill(&seg)?;
    if env_flag("BREAK_PARITY") {
        bm0.pop();
    }
    for (label, got) in [("datafusion", &df0), ("burrmill", &bm0)] {
        if *got != duck0 {
            let first: Vec<String> = duck0
                .iter()
                .zip(got.iter())
                .filter(|(a, b)| a != b)
                .take(3)
                .map(|(a, b)| format!("duck {a:?} vs {label} {b:?}"))
                .collect();
            anyhow::bail!(
                "PARITY FAILED for {label}: {} rows against DuckDB's {}; first differences: \
                 {first:?}. No timing is reported - a comparison between engines that disagree is \
                 not a measurement.",
                got.len(),
                duck0.len()
            );
        }
    }
    println!(
        "parity:  verified on {} parties  (burrmill read {} rows across {} morsels, skipped {}; \
         plan {} ms, scan {} ms, merge {} ms)",
        duck0.len(),
        metrics.rows_read,
        metrics.morsels,
        metrics.rows_skipped,
        metrics.plan_ms,
        metrics.scan_ms,
        metrics.merge_ms
    );

    // ---- timings, interleaved --------------------------------------------------------------
    let mut d_all = Vec::new();
    let mut f_all = Vec::new();
    let mut b_all = Vec::new();
    for _ in 0..repeats {
        if burrmill_first {
            let (_, b) = (0, oracles::burrmill(&seg)?.1);
            let (_, f) = ((), oracles::datafusion(&seg).await?.1);
            let (_, d) = ((), oracles::duckdb(&seg)?.1);
            b_all.push(b);
            f_all.push(f);
            d_all.push(d);
        } else {
            d_all.push(oracles::duckdb(&seg)?.1);
            f_all.push(oracles::datafusion(&seg).await?.1);
            b_all.push(oracles::burrmill(&seg)?.1);
        }
    }
    d_all.sort_unstable();
    f_all.sort_unstable();
    b_all.sort_unstable();
    let dm = d_all[d_all.len() / 2];
    let fm = f_all[f_all.len() / 2];
    let bm = b_all[b_all.len() / 2];

    println!(
        "RESULT\trows={}\tsegments={}\taddrs={}\trepeats={repeats}\torder={}\tduck_ms={dm}\tdf_ms={fm}\tburrmill_ms={bm}\tdf_ratio={:.2}\tburrmill_ratio={:.2}\tparity=verified\tgroups={}\trss_mb={}",
        spec.rows,
        spec.segments,
        spec.addrs,
        if burrmill_first { "burrmill_first" } else { "duck_first" },
        fm as f64 / dm.max(1) as f64,
        bm as f64 / dm.max(1) as f64,
        duck0.len(),
        rss_mb()
    );
    println!("duck_all={d_all:?}");
    println!("df_all={f_all:?}");
    println!("burrmill_all={b_all:?}");
    Ok(())
}

/// The 6.3 memory gate: the signed fold on the DataFusion path, one mode per process so peak RSS
/// belongs to that mode. `MODE=cast` sums `CAST(text AS DECIMAL(38,0))` through the checked sum,
/// `try` the `TRY_CAST` form that becomes an exact text sum, and `stock` is DataFusion unhosted,
/// whose sum wraps: the baseline the checked state is paid against.
async fn df_fold() -> anyhow::Result<()> {
    let dir = std::env::args()
        .nth(2)
        .ok_or_else(|| anyhow::anyhow!("usage: df-fold <dir>"))?;
    let mode = std::env::var("MODE").unwrap_or_else(|_| "cast".into());
    // One fold by default: allocator retention builds across folds in a process, so the peak of
    // five is not the peak of one. Take latency medians with REPEATS=5 in a separate run.
    let repeats = env_usize("REPEATS", 1).max(1);
    let cast = if mode == "try" { "TRY_CAST" } else { "CAST" };
    let sql = format!(
        "SELECT addr, SUM(d) AS net FROM (\
           SELECT \"to\" AS addr, {cast}(\"value\" AS DECIMAL(38,0)) AS d FROM t \
           UNION ALL \
           SELECT \"from\" AS addr, -{cast}(\"value\" AS DECIMAL(38,0)) AS d FROM t\
         ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"
    );
    let mut all = Vec::new();
    let mut rows = 0usize;
    let mut digest = None;
    if mode == "duck" {
        // The incumbent, embedded as nuthatch embeds it, same thread budget, batches consumed and
        // dropped as they arrive: the like-for-like of `STREAM=1`.
        use std::hash::Hasher;
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!(
            "SET threads TO {};",
            burrmill::Limits::default().max_threads
        ))?;
        conn.execute_batch(&format!(
            "CREATE VIEW t AS SELECT * FROM read_parquet('{dir}/*.parquet');"
        ))?;
        // In DuckDB's idiom, as the views write it: its text-to-DECIMAL(38,0) cast is ~15x slower
        // than to HUGEINT on this fold, which would make the incumbent look worse than it is.
        let duck_sql = sql.replace("DECIMAL(38,0)", "HUGEINT");
        for _ in 0..repeats {
            let t = Instant::now();
            let mut stmt = conn.prepare(&duck_sql)?;
            let mut h = std::collections::hash_map::DefaultHasher::new();
            rows = 0;
            for b in stmt.query_arrow([])? {
                rows += b.num_rows();
                if env_flag("PARITY") {
                    use duckdb::arrow::array::{Array, StringArray};
                    use std::hash::Hash;
                    let utf8 = duckdb::arrow::datatypes::DataType::Utf8;
                    let cols: Vec<_> = b
                        .columns()
                        .iter()
                        .map(|c| duckdb::arrow::compute::cast(c, &utf8))
                        .collect::<Result<_, _>>()?;
                    let cols: Vec<&StringArray> = cols
                        .iter()
                        .map(|c| c.as_any().downcast_ref().unwrap())
                        .collect();
                    for i in 0..b.num_rows() {
                        cols.iter()
                            .for_each(|c| c.is_valid(i).then(|| c.value(i)).hash(&mut h));
                    }
                }
            }
            all.push(t.elapsed().as_millis());
            digest = env_flag("PARITY").then(|| h.finish());
        }
    } else if mode == "stock" {
        let threads = burrmill::Limits::default().max_threads;
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(threads));
        ctx.register_parquet("t", &dir, ParquetReadOptions::default())
            .await?;
        for _ in 0..repeats {
            let t = Instant::now();
            let b = ctx.sql(&sql).await?.collect().await?;
            all.push(t.elapsed().as_millis());
            rows = b.iter().map(|b| b.num_rows()).sum();
            digest = parity_digest(&b)?;
        }
    } else {
        // The engine owns a runtime, and this function is already inside one.
        (all, rows, digest) = std::thread::scope(|s| {
            s.spawn(|| -> anyhow::Result<_> {
                let engine = burrmill::Engine::open_segments(Path::new(&dir))?;
                let (mut all, mut rows, mut digest) = (Vec::new(), 0, None);
                for _ in 0..repeats {
                    let t = Instant::now();
                    if env_flag("STREAM") {
                        // Nothing held: the peak is the operator's, as `fold` measures it.
                        use std::hash::Hasher;
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        let parity = env_flag("PARITY");
                        rows = 0;
                        engine.sql_for_each(&sql, |b| {
                            rows += b.num_rows();
                            if parity {
                                digest_batch(&mut h, &b).map_err(|e| {
                                    burrmill::BurrmillError::Substrate(e.to_string())
                                })?;
                            }
                            Ok(())
                        })?;
                        all.push(t.elapsed().as_millis());
                        digest = parity.then(|| h.finish());
                        continue;
                    }
                    let b = engine.sql(&sql)?;
                    all.push(t.elapsed().as_millis());
                    rows = b.iter().map(|b| b.num_rows()).sum();
                    digest = parity_digest(&b)?;
                }
                Ok((all, rows, digest))
            })
            .join()
            .expect("df-fold thread")
        })?;
    }
    all.sort_unstable();
    println!(
        "DFFOLD\tmode={mode}\tgroups={rows}\tmedian_ms={}\tall={all:?}\tpeak_rss_mb={}\tdigest={}",
        all[all.len() / 2],
        rss_mb(),
        digest.map_or("-".into(), |d: u64| format!("{d:016x}"))
    );
    Ok(())
}

/// With `PARITY=1`, an order-sensitive hash of every cell as text; off by default because the
/// copy it makes would sit inside the RSS figure it is printed next to.
fn parity_digest(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> anyhow::Result<Option<u64>> {
    use std::hash::Hasher;
    if !env_flag("PARITY") {
        return Ok(None);
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for b in batches {
        digest_batch(&mut h, b)?;
    }
    Ok(Some(h.finish()))
}

/// Row by row, so a digest built one streamed batch at a time equals one over the collected answer.
fn digest_batch(
    h: &mut std::collections::hash_map::DefaultHasher,
    b: &datafusion::arrow::record_batch::RecordBatch,
) -> anyhow::Result<()> {
    use datafusion::arrow::array::{Array, StringArray};
    use std::hash::Hash;
    let cols: Vec<_> = b
        .columns()
        .iter()
        .map(|c| datafusion::arrow::compute::cast(c, &datafusion::arrow::datatypes::DataType::Utf8))
        .collect::<Result<_, _>>()?;
    let cols: Vec<&StringArray> = cols
        .iter()
        .map(|c| c.as_any().downcast_ref().unwrap())
        .collect();
    for i in 0..b.num_rows() {
        cols.iter()
            .for_each(|c| c.is_valid(i).then(|| c.value(i)).hash(h));
    }
    Ok(())
}

/// Burrmill alone against an existing fixture, so peak RSS is the *operator's* and not a process
/// that has also linked DuckDB and instantiated a DataFusion session.
///
/// The combined harness reports its own footprint, and that number is honest about what it measures
/// but useless for the 256 MB gate: most of it is the two oracles. Reporting it as Burrmill's would
/// be the kind of flattering-by-accident measurement RFC-0004 exists to prevent - here it flatters
/// in the wrong direction, but a number that does not measure what it claims is no better for being
/// pessimistic.
fn fold_only() -> anyhow::Result<()> {
    let dir = std::env::args()
        .nth(2)
        .ok_or_else(|| anyhow::anyhow!("usage: fold <dir>"))?;
    let repeats = env_usize("REPEATS", 3).max(1);
    // **THREADS, not RAYON_NUM_THREADS.** The fold runs in its own bounded pool as of roadmap
    // 1.2c, so the ambient rayon pool no longer decides anything and setting it would silently
    // measure nothing. Defaults to the budget in `Limits`.
    let threads = env_usize("THREADS", burrmill::Limits::default().max_threads);
    let mut catalog = burrmill::Catalog::new();
    catalog.register(burrmill::SealedSegments::discover("t", Path::new(&dir))?);
    let db = burrmill::Burrmill::with_threads(catalog, threads)?;
    let sql = "SELECT addr, SUM(d) AS net FROM (\
                 SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
                 UNION ALL \
                 SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
               ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";

    let mut all = Vec::new();
    let mut rows = 0usize;
    let mut metrics = burrmill::FoldMetrics::default();
    for _ in 0..repeats {
        let t = Instant::now();
        // **Nothing is materialised here, and that is the point of this subcommand.** The RSS gate
        // is a claim about the operator, so the harness must not add a copy of the answer to the
        // thing it is measuring. `oracles::burrmill` collects into `Vec<(String, i128)>` because the
        // parity comparison needs owned rows; doing that here put a million fresh `String`s inside
        // the number and made the operator look 80 MB worse than it is. Same family of defect as
        // roadmap 1.1: a harness measuring itself.
        let answer = db.query(sql, burrmill::Limits::default())?;
        all.push(t.elapsed().as_millis());
        rows = answer.rows().len();
        metrics = answer.metrics();
    }
    all.sort_unstable();
    println!(
        "FOLD\tgroups={rows}\tmorsels={}\trows_read={}\tmedian_ms={}\tplan_ms={}\tscan_ms={}\tmerge_ms={}\tagg_mb={}\tthreads={threads}\tall={all:?}\tpeak_rss_mb={}",
        metrics.morsels,
        metrics.rows_read,
        all[all.len() / 2],
        metrics.plan_ms,
        metrics.scan_ms,
        metrics.merge_ms,
        metrics.agg_bytes >> 20,
        rss_mb()
    );
    Ok(())
}

/// Run the fold against a **real nest's sealed segments, read-only**, head to head with DuckDB.
///
/// Nothing here writes to the nest, copies it, or opens anything but the Parquet files for reading.
/// A synthetic fixture can only prove the operator is fast on a table shaped the way its author
/// imagined; this proves it reads what nuthatch actually seals - the real bimodal size distribution,
/// the real twelve-column event schema, the real uint256-as-text values.
///
///     burrmill-bench nest <segments-dir> <table-prefix>
///     CREDIT=receiver DEBIT=payer VALUE=tokens burrmill-bench nest ... escrow__deposit
///
/// # Phase decomposition (roadmap 1.1)
///
/// The first version of this harness reported one number per engine and the real-nest ratios it
/// produced were not measuring the fold. DuckDB's time was flat at roughly 620 ms across tables
/// whose row counts differed fifty-twofold, which is not what a query engine's time does. The cause
/// is that a nest keeps every table's segments in **one** directory - 38,429 files in the nest this
/// was run against - and `read_parquet('<dir>/<prefix>-*.parquet')` enumerates and pattern-matches
/// all of them on every execution, while Burrmill's `open_nest_table` did its one `read_dir`
/// *outside* the timed region. The harness was charging DuckDB for catalog construction and giving
/// Burrmill the same work for free.
///
/// So both are now measured both ways, and the phases are printed rather than summed:
///
/// - `glob` - DuckDB's own `glob()` over the pattern, i.e. directory enumeration alone.
/// - `bind` / `exec` - `prepare()` against row iteration, splitting planning from execution.
/// - `list` - the same query with the file list materialised once and passed explicitly, which is
///   the catalog Burrmill is given.
/// - Burrmill's `plan` / `scan` / `merge` come from `FoldMetrics` and were always there.
///
/// A server answering a query holds a catalog; it does not re-stat the directory per request. That
/// makes `list` against Burrmill's existing figure the like-for-like comparison, and the glob
/// figure a measurement of how much a nest directory costs to enumerate.
fn nest() -> anyhow::Result<()> {
    let dir = std::env::args()
        .nth(2)
        .ok_or_else(|| anyhow::anyhow!("usage: nest <dir> <prefix>"))?;
    let prefix = std::env::args()
        .nth(3)
        .ok_or_else(|| anyhow::anyhow!("usage: nest <dir> <prefix>"))?;
    let credit = std::env::var("CREDIT").unwrap_or_else(|_| "receiver".into());
    let debit = std::env::var("DEBIT").unwrap_or_else(|_| "payer".into());
    let value = std::env::var("VALUE").unwrap_or_else(|_| "tokens".into());
    let repeats = env_usize("REPEATS", 5).max(1);

    let sql = format!(
        "SELECT addr, SUM(d) AS net FROM (\
           SELECT \"{credit}\" AS addr, TRY_CAST(\"{value}\" AS HUGEINT) AS d FROM t \
           UNION ALL \
           SELECT \"{debit}\" AS addr, -TRY_CAST(\"{value}\" AS HUGEINT) AS d FROM t\
         ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"
    );

    let db = burrmill::Burrmill::open_nest_table("t", Path::new(&dir), &format!("{prefix}-"))?;
    println!("{}", db.explain(&sql)?);

    // DuckDB over the same files, selected by the same prefix. The glob is DuckDB's own way of
    // naming many segments, and it is the only way to keep the two engines reading identical bytes.
    let glob = format!("{dir}/{prefix}-*.parquet");
    // DuckDB's client cannot hand back a HUGEINT directly, so its projection casts to text and the
    // harness parses. Burrmill returns the i128 as an i128, which is the point.
    let duck_sql = sql.replace("SUM(d) AS net", "SUM(d)::VARCHAR AS net");

    // How big is the haystack the glob searches, as against the needles it returns? This is the
    // whole of the 1.1 question and it costs one `read_dir` to answer.
    let (dir_entries, prefix_files) = {
        let mut total = 0usize;
        let mut matched: Vec<PathBuf> = Vec::new();
        for e in std::fs::read_dir(&dir)?.flatten() {
            total += 1;
            let p = e.path();
            if p.extension().is_some_and(|x| x == "parquet")
                && p.file_name()
                    .and_then(|f| f.to_str())
                    .is_some_and(|f| f.starts_with(&format!("{prefix}-")))
            {
                matched.push(p);
            }
        }
        matched.sort();
        (total, matched)
    };

    // DuckDB reading the identical files, named explicitly instead of by pattern. This is the same
    // bytes and the same plan with the directory scan removed - the catalog handed over rather than
    // rediscovered.
    let file_list = prefix_files
        .iter()
        .map(|p| format!("'{}'", p.display()))
        .collect::<Vec<_>>()
        .join(",");
    let list_sql = duck_sql.clone();

    let run_duck = || -> anyhow::Result<(oracles::Rows, u128)> {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!(
            "CREATE VIEW t AS SELECT * FROM read_parquet('{glob}');"
        ))?;
        let t = std::time::Instant::now();
        let mut stmt = conn.prepare(&duck_sql)?;
        let mut out = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let addr: String = r.get(0)?;
            let net: String = r.get(1)?;
            out.push((addr, net.parse::<i128>()?));
        }
        Ok((out, t.elapsed().as_millis()))
    };

    // The same run, split at `prepare()`. Binding a Parquet scan is where DuckDB expands the glob
    // and unifies the schema across every matched file; execution is where it reads rows.
    let run_duck_split = || -> anyhow::Result<(u128, u128)> {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!(
            "CREATE VIEW t AS SELECT * FROM read_parquet('{glob}');"
        ))?;
        let t = std::time::Instant::now();
        let mut stmt = conn.prepare(&duck_sql)?;
        let bind_ms = t.elapsed().as_millis();
        let t2 = std::time::Instant::now();
        let mut rows = stmt.query([])?;
        let mut n = 0u64;
        while let Some(r) = rows.next()? {
            let _: String = r.get(0)?;
            let _: String = r.get(1)?;
            n += 1;
        }
        std::hint::black_box(n);
        Ok((bind_ms, t2.elapsed().as_millis()))
    };

    // Directory enumeration alone, using DuckDB's own glob function so the number is DuckDB's cost
    // and not this harness's `read_dir`.
    let run_duck_glob = || -> anyhow::Result<u128> {
        let conn = duckdb::Connection::open_in_memory()?;
        let t = std::time::Instant::now();
        let mut stmt = conn.prepare(&format!("SELECT count(*) FROM glob('{glob}')"))?;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let n: i64 = r.get(0)?;
            std::hint::black_box(n);
        }
        Ok(t.elapsed().as_millis())
    };

    // The catalog handed over: an explicit file list, no pattern to expand.
    let duck_over = |files: &[PathBuf]| -> anyhow::Result<u128> {
        let list = files
            .iter()
            .map(|p| format!("'{}'", p.display()))
            .collect::<Vec<_>>()
            .join(",");
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!(
            "CREATE VIEW t AS SELECT * FROM read_parquet([{list}]);"
        ))?;
        let t = std::time::Instant::now();
        let mut stmt = conn.prepare(&duck_sql)?;
        let mut n = 0u64;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let _: String = r.get(0)?;
            let _: String = r.get(1)?;
            n += 1;
        }
        std::hint::black_box(n);
        Ok(t.elapsed().as_millis())
    };

    let burrmill_over = |files: &[PathBuf]| -> anyhow::Result<u128> {
        let segs = burrmill::SealedSegments::from_files("t", files.to_vec());
        let mut cat = burrmill::Catalog::new();
        cat.register(segs);
        let db = burrmill::Burrmill::new(cat);
        let t = std::time::Instant::now();
        let a = db.query(&sql, burrmill::Limits::default())?;
        std::hint::black_box(a.rows().len());
        Ok(t.elapsed().as_millis())
    };

    let run_duck_list = || -> anyhow::Result<(oracles::Rows, u128)> {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!(
            "CREATE VIEW t AS SELECT * FROM read_parquet([{file_list}]);"
        ))?;
        let t = std::time::Instant::now();
        let mut stmt = conn.prepare(&list_sql)?;
        let mut out = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let addr: String = r.get(0)?;
            let net: String = r.get(1)?;
            out.push((addr, net.parse::<i128>()?));
        }
        Ok((out, t.elapsed().as_millis()))
    };

    let run_burrmill = || -> anyhow::Result<(oracles::Rows, u128, burrmill::FoldMetrics)> {
        let t = std::time::Instant::now();
        let a = db.query(&sql, burrmill::Limits::default())?;
        let ms = t.elapsed().as_millis();
        Ok((
            a.rows().iter().map(|(k, v)| (k.to_string(), v)).collect(),
            ms,
            a.metrics(),
        ))
    };

    // Burrmill paying for its own catalog, which the previous harness did outside the timer.
    let run_burrmill_cold = || -> anyhow::Result<u128> {
        let t = std::time::Instant::now();
        let db = burrmill::Burrmill::open_nest_table("t", Path::new(&dir), &format!("{prefix}-"))?;
        let a = db.query(&sql, burrmill::Limits::default())?;
        std::hint::black_box(a.rows().len());
        Ok(t.elapsed().as_millis())
    };

    // Parity first, untimed, as everywhere else.
    let (d0, _) = run_duck()?;
    let (b0, _, metrics) = run_burrmill()?;
    if d0 != b0 {
        let first: Vec<String> = d0
            .iter()
            .zip(b0.iter())
            .filter(|(a, b)| a != b)
            .take(3)
            .map(|(a, b)| format!("duck {a:?} vs burrmill {b:?}"))
            .collect();
        anyhow::bail!(
            "PARITY FAILED on {prefix}: burrmill {} rows against DuckDB's {}; first differences: \
             {first:?}. No timing is reported.",
            b0.len(),
            d0.len()
        );
    }
    // The explicit-list variant reads the same files by another name, so it must agree too. If it
    // does not, the file list is not the set the glob matched and every figure below is void.
    let (l0, _) = run_duck_list()?;
    if l0 != d0 {
        anyhow::bail!(
            "PARITY FAILED between DuckDB's glob and its explicit file list on {prefix}: {} rows \
             against {}. The two are not reading the same segments; no timing is reported.",
            l0.len(),
            d0.len()
        );
    }
    println!(
        "parity:  verified on {} parties  ({} rows read across {} morsels, {} skipped)",
        d0.len(),
        metrics.rows_read,
        metrics.morsels,
        metrics.rows_skipped
    );
    println!(
        "catalog: {} entries in {dir}, {} match {prefix}-*.parquet  ({:.1}% of the directory)",
        dir_entries,
        prefix_files.len(),
        100.0 * prefix_files.len() as f64 / dir_entries.max(1) as f64
    );

    let mut ds = Vec::new();
    let mut bs = Vec::new();
    let mut binds = Vec::new();
    let mut execs = Vec::new();
    let mut globs = Vec::new();
    let mut lists = Vec::new();
    let mut colds = Vec::new();
    let mut plans = Vec::new();
    let mut scans = Vec::new();
    let mut merges = Vec::new();
    for _ in 0..repeats {
        ds.push(run_duck()?.1);
        let (b_ms, m) = {
            let (_, ms, m) = run_burrmill()?;
            (ms, m)
        };
        bs.push(b_ms);
        plans.push(m.plan_ms);
        scans.push(m.scan_ms);
        merges.push(m.merge_ms);
        let (bind, exec) = run_duck_split()?;
        binds.push(bind);
        execs.push(exec);
        globs.push(run_duck_glob()?);
        lists.push(run_duck_list()?.1);
        colds.push(run_burrmill_cold()?);
    }
    let median = |v: &mut Vec<u128>| {
        v.sort_unstable();
        v[v.len() / 2]
    };
    let (dm, bm) = (median(&mut ds), median(&mut bs));
    let (bind_m, exec_m) = (median(&mut binds), median(&mut execs));
    let (glob_m, list_m, cold_m) = (median(&mut globs), median(&mut lists), median(&mut colds));
    let (plan_m, scan_m, merge_m) = (median(&mut plans), median(&mut scans), median(&mut merges));
    // **The scale check (roadmap 1.1b).** Halve the input and see whether the time follows. An
    // engine whose time barely moves is not being measured on the query; it is being measured on
    // something fixed, and that is exactly how the real-nest ratios came to be a statement about a
    // 38,429-file directory scan. Fixed cost is estimated by linear extrapolation from two points -
    // crude, and enough to tell 5% fixed from 90%.
    let half = &prefix_files[..prefix_files.len() / 2];
    let (dh, bh) = (duck_over(half)?, burrmill_over(half)?);
    let (df, bf) = (duck_over(&prefix_files)?, burrmill_over(&prefix_files)?);
    let fixed = |t_half: u128, t_full: u128| -> u128 { (2 * t_half).saturating_sub(t_full) };
    let (duck_fixed, burr_fixed) = (fixed(dh, df), fixed(bh, bf));
    let frac = |f: u128, t: u128| -> f64 { f as f64 / t.max(1) as f64 };
    let (duck_frac, burr_frac) = (frac(duck_fixed, df), frac(burr_fixed, bf));
    println!(
        "SCALE\ttable={prefix}\tduck_half_ms={dh}\tduck_full_ms={df}\tduck_fixed_ms={duck_fixed}\tduck_fixed_pct={:.0}\tburr_half_ms={bh}\tburr_full_ms={bf}\tburr_fixed_ms={burr_fixed}\tburr_fixed_pct={:.0}",
        duck_frac * 100.0,
        burr_frac * 100.0
    );

    // **A ratio dominated by fixed cost is not printed as a number.** RFC-0004's discipline is to
    // make the wrong figure unavailable rather than merely discouraged: if more than half of either
    // engine's time is independent of how much data it read, there is no ratio here that means
    // "faster on this query", and a reader must not be able to copy one out.
    //
    // **The gate covers the number it publishes.** The first version of this check measured the
    // explicit-list path and then gated the glob-path ratio, which is a different quantity - the
    // same mistake in miniature as the one 1.1b exists to prevent, caught the first time it ran. The
    // headline `ratio` is therefore the like-for-like one, DuckDB handed the same catalog Burrmill
    // holds, and it is the one the scale check measured.
    let safe = |frac_d: f64, frac_b: f64, num: f64| -> String {
        if frac_d > 0.5 || frac_b > 0.5 {
            format!(
                "UNSAFE_fixed_duck={:.0}pct_burr={:.0}pct",
                frac_d * 100.0,
                frac_b * 100.0
            )
        } else {
            format!("{num:.2}")
        }
    };
    let ratio_field = safe(duck_frac, burr_frac, bm as f64 / list_m.max(1) as f64);

    // The glob path's fixed cost is not estimated, it is measured: `glob()` plus bind, before a row
    // is read. On a nest whose segments directory holds tens of thousands of files this is most of
    // the number, which is why the published real-nest ratios were wrong for a whole slice.
    let glob_fixed_frac = (glob_m + bind_m) as f64 / dm.max(1) as f64;
    let glob_ratio_field = safe(glob_fixed_frac, burr_frac, bm as f64 / dm.max(1) as f64);
    if ratio_field.starts_with("UNSAFE") || glob_ratio_field.starts_with("UNSAFE") {
        eprintln!(
            "WARNING: {prefix}: a ratio was withheld because most of an engine's time does not vary \
             with input size. glob path: {:.0}% fixed. list path: DuckDB {:.0}%, Burrmill {:.0}%. \
             Use the PHASE and SCALE lines and say what the fixed cost is.",
            glob_fixed_frac * 100.0,
            duck_frac * 100.0,
            burr_frac * 100.0
        );
    }
    println!(
        "NEST\ttable={prefix}\tsegments={}\trows={}\tgroups={}\tduck_list_ms={list_m}\tduck_glob_ms={dm}\tburrmill_ms={bm}\tratio={ratio_field}\tglob_ratio={glob_ratio_field}\tparity=verified\trss_mb={}",
        metrics.morsels,
        metrics.rows_read,
        d0.len(),
        rss_mb()
    );
    println!(
        "PHASE\ttable={prefix}\tdir_entries={dir_entries}\tmatched={}\tduck_glob_ms={glob_m}\tduck_bind_ms={bind_m}\tduck_exec_ms={exec_m}\tduck_list_ms={list_m}\tburr_plan_ms={plan_m}\tburr_scan_ms={scan_m}\tburr_merge_ms={merge_m}\tburr_cold_ms={cold_m}\tlist_ratio={:.2}\tcold_ratio={:.2}",
        prefix_files.len(),
        bm as f64 / list_m.max(1) as f64,
        cold_m as f64 / dm.max(1) as f64
    );
    println!(
        "duck_all={ds:?}\nburrmill_all={bs:?}\nduck_list_all={lists:?}\nduck_glob_all={globs:?}"
    );
    Ok(())
}

/// The generated corpus against DuckDB (roadmap 2.1).
///
///     CASES=2000 burrmill-bench gen
///
/// Every case is compared three ways: the fold, DuckDB over the identical segments, and the
/// non-optimising reference. Any disagreement stops the run and prints the seed. See
/// [`crate::generate`] for why DuckDB is here at all when the library already has a reference.
fn generated() -> anyhow::Result<()> {
    let cases = env_usize("CASES", 500);
    let start = env_usize("SEED", 0) as u64;
    let sql = "SELECT addr, SUM(d) AS net FROM (\
                 SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
                 UNION ALL \
                 SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
               ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";
    let duck_sql = sql.replace("SUM(d) AS net", "SUM(d)::VARCHAR AS net");

    let (mut agreed, mut both_refused, mut order_dependent) = (0usize, 0usize, 0usize);
    for case in 0..cases {
        let seed = start
            .wrapping_add(case as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut rng = generate::Rng(seed);
        let rows = generate::gen_rows(&mut rng);
        let splits = 1 + rng.below(6);
        let dir = tempfile::tempdir()?;
        generate::write_segments(dir.path(), &rows, splits)?;

        let ours = burrmill::Burrmill::open_segments("t", dir.path())?
            .query(sql, burrmill::Limits::default())
            .map(|a| {
                a.rows()
                    .iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect::<Vec<_>>()
            });

        let glob = format!("{}/seg-*.parquet", dir.path().display());
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!(
            "CREATE VIEW t AS SELECT * FROM read_parquet('{glob}');"
        ))?;
        let theirs: Result<Vec<(String, i128)>, String> = (|| {
            let mut stmt = conn.prepare(&duck_sql).map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            let mut r = stmt.query([]).map_err(|e| e.to_string())?;
            while let Some(row) = r.next().map_err(|e| e.to_string())? {
                let a: String = row.get(0).map_err(|e| e.to_string())?;
                let n: String = row.get(1).map_err(|e| e.to_string())?;
                out.push((a, n.parse::<i128>().map_err(|e| e.to_string())?));
            }
            Ok(out)
        })();

        match (&ours, &theirs) {
            (Ok(a), Ok(b)) if a == b => agreed += 1,
            (Err(burrmill::BurrmillError::Overflow(_)), Err(_)) => both_refused += 1,
            (Ok(a), Ok(b)) => anyhow::bail!(
                "SEED={seed}: Burrmill and DuckDB disagree on the answer. \
                 Burrmill {} rows, DuckDB {} rows. First difference: {:?}",
                a.len(),
                b.len(),
                a.iter().zip(b.iter()).find(|(x, y)| x != y)
            ),
            // **One engine refusing where the other answers is not a wrong answer, and it is not
            // a pass either.** Checked addition is order-dependent: a party whose values are
            // `i128::MAX, +1, -1` sums to exactly `i128::MAX`, but any order that meets the `+1`
            // first overflows on the way to an answer it could have represented. Both engines do
            // this, in both directions, and the corpus found each within two thousand cases.
            //
            // Neither ever returns a wrapped number, which is the guarantee that matters. What is
            // weaker than it reads is the refusal: it fires when an intermediate partial sum leaves
            // the range, not when the answer does. Counted and reported rather than failed, because
            // deciding what it should do instead has a memory cost - roadmap 2.1b.
            (Err(burrmill::BurrmillError::Overflow(_)), Ok(_)) | (Ok(_), Err(_)) => {
                order_dependent += 1
            }
            // Anything other than an overflow, on data DuckDB answered, is a real refusal to
            // explain. The compiler insisted on this arm and was right to: folding it into the
            // order-dependent bucket would have quietly excused a NotAllowed or a Substrate error.
            (Err(e), Ok(b)) => anyhow::bail!(
                "SEED={seed}: Burrmill refused with a non-overflow error ({e}) where DuckDB \
                 returned {} rows",
                b.len()
            ),
            (Err(a), Err(b)) => anyhow::bail!("SEED={seed}: both refused, differently: {a} / {b}"),
        }
    }
    println!(
        "GEN\tcases={cases}\tagreed={agreed}\tboth_refused={both_refused}\torder_dependent_refusal={order_dependent}"
    );
    Ok(())
}

/// Where Burrmill's `TRY_CAST(text AS HUGEINT)` and DuckDB's disagree, printed rather than assumed.
///
/// The fold implements `TRY_CAST` with `str::parse::<i128>()`, and the generated corpus found within
/// four hundred cases that this is **not** what DuckDB means by the same expression. The two are
/// close enough to look identical on every value a real nest holds - `uint256` rendered as digits -
/// and different on the edges, which is exactly the shape of bug that survives a benchmark.
fn cast_table() -> anyhow::Result<()> {
    let lits = [
        " 7",
        "7 ",
        "+7",
        "1e18",
        "0x10",
        "",
        "not a number",
        "  -5  ",
        "7.0",
        "7.9",
        "007",
        "1_000",
        "+-7",
        "9223372036854775808",
        "170141183460469231731687303715884105727",
        "-170141183460469231731687303715884105728",
        "170141183460469231731687303715884105728",
        "1,000",
        " ",
        "\t7",
    ];
    let conn = duckdb::Connection::open_in_memory()?;
    println!(
        "{:<45} {:<24} duckdb TRY_CAST",
        "literal", "rust parse::<i128>"
    );
    let mut diffs = 0;
    for l in lits {
        let ours = l
            .parse::<i128>()
            .map(|v| v.to_string())
            .unwrap_or_else(|_| "NULL".into());
        let mut stmt = conn.prepare("SELECT TRY_CAST(? AS HUGEINT)::VARCHAR")?;
        let theirs: Option<String> = stmt.query_row([l], |r| r.get(0))?;
        let theirs = theirs.unwrap_or_else(|| "NULL".into());
        let mark = if ours == theirs {
            ""
        } else {
            diffs += 1;
            "  <-- DIVERGES"
        };
        println!("{:<45} {ours:<24} {theirs:<24}{mark}", format!("{l:?}"));
    }
    println!("\n{diffs} of {} literals diverge", lits.len());
    Ok(())
}

/// Run the `.slt` corpus against **DuckDB** (roadmap 2.3).
///
/// The corpus lives in `crates/burrmill/tests/slt/` and runs against Burrmill on every `cargo test`.
/// This points the identical files at the other engine, and it is the whole reason for using a
/// standard format rather than inventing one: it turns "these are the answers Burrmill gives" into
/// "these are the answers the standard gives, and Burrmill gives them too". When DuckDB leaves the
/// graph in Q4 the files keep working; only this command goes.
fn slt_against_duckdb() -> anyhow::Result<()> {
    struct Duck(duckdb::Connection);

    impl sqllogictest::DB for Duck {
        // `anyhow::Error` does not implement `std::error::Error`, which the trait requires, so the
        // corpus sees DuckDB's own error type. That is the right one anyway: `statement error`
        // matches against the engine's message, and wrapping it would blur what was actually said.
        type Error = duckdb::Error;
        type ColumnType = sqllogictest::DefaultColumnType;

        fn run(
            &mut self,
            sql: &str,
        ) -> Result<sqllogictest::DBOutput<Self::ColumnType>, duckdb::Error> {
            // DuckDB's client cannot hand a HUGEINT back directly, so the projection is cast to text
            // and the harness reads the digits. A mechanical, documented rewrite of the projection
            // only - the fold, the grouping and the ordering are the corpus's own text.
            let sql = sql.replace("SUM(d) AS net", "SUM(d)::VARCHAR AS net");
            let mut stmt = self.0.prepare(&sql)?;
            let mut out = Vec::new();
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                out.push(vec![r.get::<_, String>(0)?, r.get::<_, String>(1)?]);
            }
            Ok(sqllogictest::DBOutput::Rows {
                types: vec![
                    sqllogictest::DefaultColumnType::Text,
                    sqllogictest::DefaultColumnType::Integer,
                ],
                rows: out,
            })
        }

        fn engine_name(&self) -> &str {
            "duckdb"
        }
    }

    // The same tables the Burrmill runner builds, written as Parquet and exposed as views.
    let max = "170141183460469231731687303715884105727";
    let tables: Vec<(&str, Vec<generate::Row>)> = vec![
        (
            "t",
            vec![
                ("0xaa", "0xbb", "100"),
                ("0xbb", "0xcc", "30"),
                ("0xaa", "0xcc", "5"),
            ],
        ),
        (
            "zeros",
            vec![("0xaa", "0xbb", "50"), ("0xbb", "0xaa", "50")],
        ),
        (
            "nulls",
            vec![
                ("0xaa", "0xbb", "not a number"),
                ("0xaa", "0xbb", ""),
                ("0xaa", "0xbb", " 7"),
            ],
        ),
        ("boundary", vec![("0xaa", "0xbb", max)]),
        (
            "overflow",
            vec![("0xaa", "0xbb", max), ("0xaa", "0xbb", "1")],
        ),
    ]
    .into_iter()
    .map(|(n, rows)| {
        (
            n,
            rows.into_iter()
                .map(|(f, t, v)| generate::Row {
                    from: f.into(),
                    to: t.into(),
                    value: v.into(),
                })
                .collect(),
        )
    })
    .collect();

    let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("../burrmill/tests/slt");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&corpus)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "slt"))
        .collect();
    files.sort();
    anyhow::ensure!(!files.is_empty(), "no .slt files in {}", corpus.display());

    let mut failures = 0usize;
    for splits in [1usize, 2, 7] {
        let root = tempfile::tempdir()?;
        let conn = duckdb::Connection::open_in_memory()?;
        for (name, rows) in &tables {
            let dir = root.path().join(name);
            std::fs::create_dir_all(&dir)?;
            generate::write_segments(&dir, rows, splits)?;
            conn.execute_batch(&format!(
                "CREATE VIEW {name} AS SELECT * FROM read_parquet('{}/seg-*.parquet');",
                dir.display()
            ))?;
        }
        for file in &files {
            let c = conn.try_clone()?;
            let mut runner =
                sqllogictest::Runner::new(|| std::future::ready(Ok(Duck(c.try_clone().unwrap()))));
            if let Err(e) = runner.run_file(file) {
                failures += 1;
                eprintln!(
                    "DuckDB disagrees with {} at {splits} segment(s):\n{e}",
                    file.display()
                );
            }
        }
    }
    println!(
        "SLT\tengine=duckdb\tfiles={}\tlayouts=3\tfailures={failures}",
        files.len()
    );
    anyhow::ensure!(
        failures == 0,
        "{failures} corpus expectation(s) are not the standard's answers"
    );
    Ok(())
}

/// **A reproduction of DuckDB silently wrapping a `HUGEINT` sum.** Kept as a command because a
/// third-party bug this project relies on being absent is a thing to re-check, not to remember.
///
/// Two rows credit one party with `i128::MAX` and then `1`. The true sum is `MAX + 1`, which no
/// 128-bit integer can hold, and DuckDB **returns `i128::MIN`** - a wrapped balance, silently - as
/// soon as the aggregation genuinely runs in parallel. At one thread, or over a single file, the
/// same query correctly refuses. The check is in the single-threaded path and missing from the
/// partial-aggregate combine.
///
/// Measured on libduckdb-sys 1.10501.0. This is the "not watertight everywhere" that the README
/// hedges about, made concrete, and it is precisely the failure mode Burrmill exists to remove: a
/// wrong number that looks exactly like a balance.
///
/// If this ever prints "refused" everywhere, DuckDB has fixed it and the `skipif duckdb` in
/// `tests/slt/cast_and_overflow.slt` should come out.
fn duckdb_gaps() -> anyhow::Result<()> {
    let max = "170141183460469231731687303715884105727";
    let rows = vec![
        generate::Row {
            from: "0xaa".into(),
            to: "0xbb".into(),
            value: max.into(),
        },
        generate::Row {
            from: "0xaa".into(),
            to: "0xbb".into(),
            value: "1".into(),
        },
    ];
    println!(
        "true sum for 0xbb is MAX+1 = 170141183460469231731687303715884105728, NOT representable"
    );
    println!("i128::MIN                  = {}\n", i128::MIN);
    for threads in [1, 2, 4] {
        for splits in [1usize, 2, 3] {
            let root = tempfile::tempdir()?;
            let dir = root.path().join("overflow");
            std::fs::create_dir_all(&dir)?;
            generate::write_segments(&dir, &rows, splits)?;
            let conn = duckdb::Connection::open_in_memory()?;
            conn.execute_batch(&format!("SET threads TO {threads};"))?;
            conn.execute_batch(&format!(
                "CREATE VIEW overflow AS SELECT * FROM read_parquet('{}/seg-*.parquet');",
                dir.display()
            ))?;
            let sql = "SELECT addr, SUM(d)::VARCHAR AS net FROM (\
                 SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM overflow \
                 UNION ALL \
                 SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM overflow\
               ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";
            print!("duck_threads={threads} files={splits} -> ");
            match conn.prepare(sql).and_then(|mut st| {
                let mut out = Vec::new();
                let mut r = st.query([])?;
                while let Some(row) = r.next()? {
                    out.push((row.get::<_, String>(0)?, row.get::<_, String>(1)?));
                }
                Ok(out)
            }) {
                Ok(v) => {
                    let bb = v.iter().find(|(a, _)| a == "0xbb").map(|(_, n)| n.clone());
                    match bb.as_deref() {
                        Some("-170141183460469231731687303715884105728") => {
                            println!("WRAPPED: 0xbb = i128::MIN, true value is MAX+1")
                        }
                        Some(other) => println!("answered 0xbb = {other}"),
                        None => println!("answered, 0xbb absent: {v:?}"),
                    }
                }
                Err(e) => println!("refused ({})", e.to_string().lines().next().unwrap_or("")),
            }
        }
    }
    Ok(())
}
