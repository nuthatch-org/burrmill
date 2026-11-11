//! `host-scan <nest> <queries.tsv>`: what the scan side costs on the path nuthatch drives, where
//! `engine-sql` opens the nest once. A host opens an empty engine at every seal, binds each table
//! with `register_facts` from the list it holds, and binds again before each statement.
//!
//! Three engines in one process, so the later ones find the page cache warm and whatever the
//! process shares, and nothing an engine keeps to itself. Each counts every table, which reads
//! nothing of a segment but its footer, `ROUNDS` (5) times; the first also runs the statements,
//! `STATEMENT_ROUNDS` (1) times, with `EACH=1` a line for each. `BUDGET_MB` (2048) and `THREADS`
//! (8) are the session a host opens. With `--features alloc-stats` each phase prints the
//! allocations it made.

use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::df_views::load_nest;

type Declared = Vec<(String, String)>;

fn declared(root: &Path) -> anyhow::Result<std::collections::HashMap<String, Declared>> {
    let doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("schema.json"))?)?;
    Ok(doc["tables"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|t| {
            let cols = t["columns"]
                .as_array()?
                .iter()
                .filter_map(|c| {
                    Some((
                        c["name"].as_str()?.to_string(),
                        c["storage"].as_str()?.to_string(),
                    ))
                })
                .collect();
            Some((t["table"].as_str()?.to_string(), cols))
        })
        .collect())
}

/// Process CPU, user and system, in milliseconds: the box is rarely quiet, and wall time alone
/// then says more about the neighbours.
fn cpu_ms() -> u128 {
    // SAFETY: `getrusage` fills a plain struct; zeroed is a valid one.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        let ms = |t: libc::timeval| t.tv_sec as u128 * 1000 + t.tv_usec as u128 / 1000;
        ms(ru.ru_utime) + ms(ru.ru_stime)
    }
}

fn allocs() -> u64 {
    #[cfg(feature = "alloc-stats")]
    return crate::heap::allocs();
    #[cfg(not(feature = "alloc-stats"))]
    0
}

struct Phase(&'static str, Instant, u128, u64);

impl Phase {
    fn start(name: &'static str) -> Self {
        Self(name, Instant::now(), cpu_ms(), allocs())
    }
    fn end(self, rest: &str) {
        println!(
            "{}\twall_ms={:.1}\tcpu_ms={}\tallocs={}\t{rest}",
            self.0,
            self.1.elapsed().as_secs_f64() * 1e3,
            cpu_ms() - self.2,
            allocs() - self.3,
        );
    }
}

struct Host {
    tables: Vec<(String, Declared, Vec<(PathBuf, u64)>)>,
    views: Vec<(String, String)>,
    budget: burrmill::Budget,
}

impl Host {
    fn facts(&self, e: &mut burrmill::Engine) -> anyhow::Result<()> {
        for (name, cols, files) in &self.tables {
            e.register_facts(name, cols, files.clone(), &[], (None, None))
                .map_err(|x| anyhow::anyhow!("{name}: {x}"))?;
        }
        Ok(())
    }

    fn open(&self, label: &'static str) -> anyhow::Result<burrmill::Engine> {
        let reads = burrmill::df::footer_reads();
        let p = Phase::start(label);
        let mut e = burrmill::Engine::open_empty_budgeted(self.budget.clone())?;
        self.facts(&mut e)?;
        p.end(&format!(
            "bind_footer_reads={}",
            burrmill::df::footer_reads() - reads
        ));
        for (n, b) in &self.views {
            if let Err(x) = e.register_view(n, b) {
                let x = x.to_string();
                println!("VIEW\t{n}\trefused: {}", x.lines().next().unwrap_or(""));
            }
        }
        Ok(e)
    }
}

/// Every statement once: the summed wall time, how many were refused, and the most one held in
/// the pool.
fn pass(
    label: &str,
    e: &burrmill::Engine,
    statements: &[(String, String)],
    metered: bool,
    each: bool,
) {
    let (mut pool_peak, mut refused, mut selected) = (0usize, 0usize, 0u64);
    let p = Phase::start("PASS");
    for (id, sql) in statements {
        e.take_memory_peak();
        let t = Instant::now();
        let mut m = burrmill::QueryMetrics::default();
        let r = e.sql_for_each_metered(sql, metered.then_some(&mut m), |_| Ok(()));
        let ms = t.elapsed().as_secs_f64() * 1e3;
        let pool = e.take_memory_peak();
        pool_peak = pool_peak.max(pool);
        refused += r.is_err() as usize;
        selected += m.sealed_bytes_scanned;
        if each {
            println!(
                "STMT\t{label}\t{id}\tms={ms:.1}\tpool_peak_mb={}\t{}",
                pool >> 20,
                r.err().map_or("ok".into(), |x| x
                    .to_string()
                    .lines()
                    .next()
                    .unwrap_or("")
                    .to_string())
            );
        }
    }
    p.end(&format!(
        "{label}\trefused={refused}\tpool_peak_mb={}\tfooter_cache_kb={}\tselected_bytes={selected}",
        pool_peak >> 20,
        e.footer_cache_bytes() >> 10,
    ));
}

pub fn run(root: &str, queries: &str) -> anyhow::Result<()> {
    let env = |k: &str, d: usize| crate::env_usize(k, d);
    let nest = load_nest(Path::new(root))?;
    let mut decl = declared(Path::new(root))?;
    let host = Host {
        tables: nest
            .tables
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    decl.remove(&t.name).unwrap_or_default(),
                    t.files.clone(),
                )
            })
            .collect(),
        views: nest
            .views
            .iter()
            .map(|v| (v.name.clone(), v.body.clone()))
            .collect(),
        budget: burrmill::Budget {
            memory_bytes: env("BUDGET_MB", 2048) << 20,
            threads: env("THREADS", 8),
            spill: None,
        },
    };
    let statements: Vec<(String, String)> = std::fs::read_to_string(queries)?
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .filter_map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            Some((
                f.first()?.to_string(),
                f.last()?.trim_end_matches(';').to_string(),
            ))
        })
        .collect();
    let segments: usize = host.tables.iter().map(|t| t.2.len()).sum();
    println!(
        "NEST\ttables={}\tsegments={segments}\tviews={}\tstatements={}\tbudget_mb={}\tthreads={}",
        host.tables.len(),
        host.views.len(),
        statements.len(),
        host.budget.memory_bytes >> 20,
        host.budget.threads,
    );
    let rounds = env("ROUNDS", 5);
    let each = crate::env_flag("EACH");
    // A count reads nothing of a segment but its footer.
    let counts: Vec<(String, String)> = host
        .tables
        .iter()
        .filter(|t| !t.2.is_empty())
        .map(|t| (t.0.clone(), format!("SELECT count(*) FROM \"{}\"", t.0)))
        .collect();
    // An engine owns a runtime, and this one is inside the harness's.
    std::thread::spawn(move || -> anyhow::Result<()> {
        let repeat = |label: &str, e: &burrmill::Engine, set: &[(String, String)], metered| {
            pass(&format!("{label}/cold"), e, set, metered, false);
            for _ in 1..rounds {
                pass(&format!("{label}/warm"), e, set, metered, false);
            }
        };
        let first = host.open("OPEN\tfirst")?;
        repeat("first/counts", &first, &counts, false);
        for _ in 0..env("STATEMENT_ROUNDS", 1) {
            pass("first/statements", &first, &statements, false, each);
        }
        // The seal: a new engine over the same segments, the earlier ones still alive as a host's
        // are while their statements finish.
        let second = host.open("OPEN\tsecond")?;
        repeat("second/counts", &second, &counts, false);
        let mut third = host.open("OPEN\tthird")?;
        repeat("third/metered-counts", &third, &counts, true);
        for _ in 0..rounds {
            let p = Phase::start("REBIND");
            host.facts(&mut third)?;
            p.end(&format!("segments={segments}"));
        }
        pass("third/rebound", &third, &counts, false, false);
        Ok(())
    })
    .join()
    .expect("probe")
}
