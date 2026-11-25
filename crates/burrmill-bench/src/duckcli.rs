//! DuckDB as a CLI process rather than the bundled library, for `tpch` and `csv-ingest` when
//! `DUCKDB=<path to the duckdb CLI>` is set (#414): the bundled crate is pinned to the 1.5 line,
//! and the comparison has to be against the current release. Answers come back through a Parquet
//! file the CLI writes, read with this workspace's Arrow and encoded as Burrmill's rows are; times
//! are what the CLI's `.timer` reports for each statement, as datafusion-cli's are.

use std::path::Path;
use std::process::Command;
use std::time::Duration;

/// The CLI's `--version` line, e.g. `v2.0.1 (Cyanoptera) 8d78114e90`.
pub(crate) fn version(bin: &str) -> anyhow::Result<String> {
    let out = Command::new(bin)
        .args(["-no-agent", "--version"])
        .output()?;
    anyhow::ensure!(out.status.success(), "{bin} --version failed");
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Runs `script` in one CLI session, refusing on any error, and gives its stdout and the
/// session's peak RSS in MiB, from `wait4`.
fn session(bin: &str, script: &str) -> Result<(String, u64), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let (f, out, err) = (
        dir.path().join("script.sql"),
        dir.path().join("stdout"),
        dir.path().join("stderr"),
    );
    std::fs::write(&f, script).map_err(|e| e.to_string())?;
    let file = |p: &Path| std::fs::File::create(p).map_err(|e| e.to_string());
    let child = Command::new(bin)
        .args(["-no-agent", "-bail", "-init", "/dev/null", "-f"])
        .arg(&f)
        .stdout(file(&out)?)
        .stderr(file(&err)?)
        .spawn()
        .map_err(|e| e.to_string())?;
    let mut status = 0;
    // SAFETY: an all-zero rusage is valid, and the pid is this process's own child, waited once.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let pid = unsafe { libc::wait4(child.id() as libc::pid_t, &mut status, 0, &mut usage) };
    if pid < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let read = |p: &Path| std::fs::read_to_string(p).map_err(|e| e.to_string());
    let (out, err) = (read(&out)?, read(&err)?);
    if !libc::WIFEXITED(status) || libc::WEXITSTATUS(status) != 0 || err.contains("Error") {
        return Err(err
            .lines()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("duckdb failed")
            .to_string());
    }
    // Linux gives ru_maxrss in KiB.
    Ok((out, usage.ru_maxrss as u64 >> 10))
}

/// The seconds `.timer` reported, in order, one for each statement after it was switched on.
fn timings(stdout: &str) -> Vec<f64> {
    stdout
        .lines()
        .filter_map(|l| l.split("Run Time (s): real ").nth(1))
        .filter_map(|l| l.split_whitespace().next()?.parse().ok())
        .collect()
}

/// The best of `runs` runs of `statements` after `setup`, in one session, their rows discarded.
/// A run is every statement once, and its time is theirs summed; the CLI times a line, so each
/// is written on one. Gives the session's peak RSS in MiB beside it.
pub(crate) fn best(
    bin: &str,
    setup: &str,
    statements: &[&str],
    runs: usize,
) -> Result<(Duration, u64), String> {
    let per_run = statements.len().max(1);
    let mut script = format!("{setup}\n.mode trash\n.timer on\n");
    for _ in 0..runs.max(1) {
        for s in statements {
            script += &format!("{};\n", s.replace('\n', " "));
        }
    }
    let (out, rss) = session(bin, &script)?;
    let secs = timings(&out);
    let want = runs.max(1) * per_run;
    if secs.len() != want {
        return Err(format!("{} timings, expected {want}", secs.len()));
    }
    let best = secs
        .chunks(per_run)
        .map(|c| c.iter().sum::<f64>())
        .fold(f64::INFINITY, f64::min);
    Ok((Duration::from_secs_f64(best), rss))
}

/// `sql`'s rows after `setup`, encoded as [`crate::fuzz::burrmill_rows`] encodes Burrmill's, in
/// the order DuckDB gave them. DuckDB writes a `HUGEINT` to Parquet as a `DOUBLE`, so each such
/// column is cast first to `DECIMAL(38,0)`, which is what Burrmill's `HUGEINT` is.
pub(crate) fn rows(bin: &str, setup: &str, sql: &str) -> Result<Vec<String>, String> {
    let sql = sql.replace('\n', " ");
    let (described, _) = session(
        bin,
        &format!("{setup}\n.mode list\n.separator \"\\t\"\n.headers off\nDESCRIBE ({sql});\n"),
    )?;
    let wide: Vec<String> = described
        .lines()
        .filter_map(|l| {
            let mut f = l.split('\t');
            let (name, ty) = (f.next()?, f.next()?);
            ty.ends_with("HUGEINT")
                .then(|| format!("CAST(\"{name}\" AS DECIMAL(38,0)) AS \"{name}\""))
        })
        .collect();
    let sql = if wide.is_empty() {
        sql
    } else {
        format!("SELECT * REPLACE ({}) FROM ({sql})", wide.join(", "))
    };
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let file = dir.path().join("answer.parquet");
    session(
        bin,
        &format!(
            "{setup}\nCOPY ({sql}) TO '{}' (FORMAT parquet, COMPRESSION snappy);\n",
            file.display()
        ),
    )?;
    read_rows(&file).map_err(|e| e.to_string())
}

fn read_rows(file: &Path) -> anyhow::Result<Vec<String>> {
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(file)?,
    )?
    .build()?;
    let mut out = Vec::new();
    for b in reader {
        for r in burrmill::df::encode::rows(&b?)? {
            out.push(r.to_string());
        }
    }
    Ok(out)
}
