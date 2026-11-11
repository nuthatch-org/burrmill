//! The 6.6 memory gate in a binary shaped like nuthatch after the swap: Burrmill with `datafusion`
//! and nothing else linked. The bench binary carries DuckDB and the umbrella crate too, and the code
//! pages a query touches in it count in its RSS: 55 MB file-backed against the bare fold's 21, with
//! the anonymous memory level. `cargo run --release --example hosted_fold --features datafusion <segments>`
//!
//! With `registered` after the directory the table is bound as nuthatch binds it, on an engine
//! opened empty with `register_facts` and addresses declared, which is the path production takes.
//!
//! Linux only for the split, which samples `/proc/self/statm` and keeps the largest reading.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SQL: &str = "SELECT addr, SUM(d) AS net FROM (\
    SELECT \"to\" AS addr, CAST(\"value\" AS DECIMAL(38,0)) AS d FROM t \
    UNION ALL \
    SELECT \"from\" AS addr, -CAST(\"value\" AS DECIMAL(38,0)) AS d FROM t\
    ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(1)
        .ok_or("usage: hosted_fold <segments> [registered]")?;
    let registered = std::env::args().nth(2).as_deref() == Some("registered");
    let peak = Arc::new(Mutex::new((0usize, 0usize)));
    let p = Arc::clone(&peak);
    std::thread::spawn(move || {
        loop {
            if let Ok(t) = std::fs::read_to_string("/proc/self/statm") {
                let f: Vec<usize> = t
                    .split_whitespace()
                    .filter_map(|x| x.parse().ok())
                    .collect();
                let mut g = p.lock().unwrap();
                if f[1] * 4096 > g.0 {
                    *g = (f[1] * 4096, f[2] * 4096);
                }
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    });
    let engine = match registered {
        false => burrmill::Engine::open_segments(std::path::Path::new(&dir))?,
        true => {
            let mut files = Vec::new();
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                if entry.path().extension().is_some_and(|x| x == "parquet") {
                    files.push((entry.path(), entry.metadata()?.len()));
                }
            }
            files.sort();
            let declared = [
                ("block_number", "u64"),
                ("from", "address"),
                ("to", "address"),
                ("value", "word32"),
            ]
            .map(|(c, t)| (c.to_string(), t.to_string()));
            let mut engine = burrmill::Engine::open_empty()?;
            engine.register_facts("t", &declared, files, &[], (None, None))?;
            engine
        }
    };
    let plan = format!("{:?}", engine.sql(&format!("EXPLAIN {SQL}"))?);
    let folded = plan.contains("OwnedSignedFold");
    let t = Instant::now();
    let mut rows = 0;
    engine.sql_for_each(SQL, |b| {
        rows += b.num_rows();
        Ok(())
    })?;
    let ms = t.elapsed().as_millis();
    let hwm = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM"))
                .map(|l| l.to_string())
        })
        .unwrap_or_default();
    let (rss, file) = *peak.lock().unwrap();
    let mb = |b: usize| b / (1024 * 1024);
    println!(
        "HOSTED\tregistered={registered}\tfolded={folded}\tgroups={rows}\tms={ms}\t{}\tsampled_rss_mb={}\tfile_mb={}\tanon_mb={}",
        hwm.split_whitespace().collect::<Vec<_>>().join("="),
        mb(rss),
        mb(file),
        mb(rss - file)
    );
    Ok(())
}
