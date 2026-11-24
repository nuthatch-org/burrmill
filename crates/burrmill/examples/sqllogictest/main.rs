//! DuckDB's own sqllogictest suite against Burrmill (#386).
//!
//! `docs/bench/sqllogictest.sh` fetches DuckDB's `test/sql` at a pinned tag and runs this. Every
//! record of every `.test` file is one of: **pass**; **refused**, an error naming a feature
//! Burrmill does not support; **wrong**, an answer that differs from DuckDB's; **error**, any other
//! error, a timeout or a crash; **skipped**, a record the harness cannot give a fair run (a
//! `require`d extension, a `load`ed database, a setting it cannot map, or a statement after one
//! of those). See `docs/bench/sqllogictest.md`.
//!
//! Each file runs in a child process of its own, so a crash or a hang costs that record and not
//! the run: the parent kills a child that goes quiet, records the record it was on as an error,
//! and starts the file again past it.
//!
//! ```text
//! sqllogictest run <duckdb-checkout> <out-dir> [--jobs N] [--timeout SECONDS] [--only PREFIX]
//! sqllogictest report <out-dir> <baseline> (--check | --update) [--doc PATH --tag TAG --command CMD]
//! ```

mod md5;
mod parse;
mod render;
mod run;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use run::Class;

const CLASSES: [Class; 5] = [
    Class::Pass,
    Class::Refused,
    Class::Wrong,
    Class::Error,
    Class::Skipped,
];

fn usage() -> ! {
    eprintln!(
        "usage:\n  sqllogictest run <duckdb-checkout> <out-dir> [--jobs N] [--timeout SECONDS] [--only PREFIX]\n  \
         sqllogictest report <out-dir> <baseline> (--check | --update) [--doc PATH --tag TAG --command CMD]"
    );
    std::process::exit(2)
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("child") => child(&args[1..]),
        Some("run") => parent(&args[1..]),
        Some("report") => std::process::exit(report(&args[1..])),
        _ => usage(),
    }
}

/// `child <duckdb-root> <file> <timeout> <seq,seq,...>`: one file, one line per record on stdout,
/// `seq \t line \t class \t detail`, then `END`. The listed seqs are not run: the parent already
/// has them, as the record a crash or hang was on.
fn child(args: &[String]) {
    let [root, file, timeout, skip] = args else {
        usage()
    };
    let root = Path::new(root);
    let skip: HashSet<usize> = skip.split(',').filter_map(|s| s.parse().ok()).collect();
    let timeout = Duration::from_secs_f64(timeout.parse().unwrap_or(10.0));
    let mut out = std::io::stdout().lock();
    let test_dir = std::env::temp_dir().join(format!("burrmill-slt-{}", std::process::id()));
    let mut env = HashMap::new();
    for (k, v) in [
        ("TEST_DIR", test_dir.display().to_string()),
        ("TEMP_DIR", test_dir.display().to_string()),
        ("DATA_DIR", root.join("data").display().to_string()),
        ("BUILD_DIRECTORY", root.join("build").display().to_string()),
        ("UUID", "00000000-0000-0000-0000-000000000000".to_string()),
        (
            "BASE_TEST_NAME",
            Path::new(file)
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
    ] {
        env.insert(k.to_string(), v);
    }
    let path = root.join(file);
    let script = match parse::parse(&path, &env) {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(out, "0\t0\terror\tunreadable: {e}");
            let _ = writeln!(out, "END");
            return;
        }
    };
    let say = |out: &mut std::io::StdoutLock, seq: usize, line: usize, c: Class, d: &str| {
        let d = d.replace(['\t', '\n', '\r'], " ");
        let _ = writeln!(out, "{seq}\t{line}\t{}\t{d}", c.name());
        let _ = out.flush();
    };
    if let Some(why) = &script.skip_file {
        for (i, &l) in run::script_lines(&script).iter().enumerate() {
            say(&mut out, i, l, Class::Skipped, why);
        }
        let _ = writeln!(out, "END");
        return;
    }
    let mut session = match run::Session::new(timeout) {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(out, "0\t0\terror\tengine did not open: {e}");
            let _ = writeln!(out, "END");
            return;
        }
    };
    let mut stopped: Option<String> = None;
    for (seq, item) in script.items.iter().enumerate() {
        let line = match item {
            parse::Item::Run(r) => Some(r.line),
            parse::Item::Skip { line, .. } => Some(*line),
            _ => None,
        };
        if let Some(why) = &stopped {
            if let Some(l) = line {
                say(&mut out, seq, l, Class::Skipped, &format!("after {why}"));
            }
            continue;
        }
        if skip.contains(&seq) {
            if let parse::Item::Run(r) = item
                && run::writes(r)
            {
                stopped = Some("a write that crashed or hung".into());
            }
            continue;
        }
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| session.item(item)));
        let o = match r {
            Ok(o) => o,
            Err(p) => {
                let msg = p
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                Some(run::Outcome {
                    class: Class::Error,
                    detail: format!("harness panicked: {msg}"),
                    stop: Some("a panic".into()),
                })
            }
        };
        let Some(o) = o else { continue };
        if let Some(l) = line {
            say(&mut out, seq, l, o.class, &o.detail);
        }
        if o.stop.is_some() {
            stopped = o.stop;
        }
    }
    let _ = writeln!(out, "END");
    let _ = std::fs::remove_dir_all(&test_dir);
}

/// A file and its records' outcomes, by line.
type FileRecs = (String, BTreeMap<usize, Rec>);

/// One record's worst outcome over every time it ran (a record in a loop runs many times).
#[derive(Clone)]
struct Rec {
    class: Class,
    detail: String,
}

fn worse(a: Class, b: Class) -> bool {
    let rank = |c: Class| match c {
        Class::Wrong => 4,
        Class::Error => 3,
        Class::Refused => 2,
        Class::Pass => 1,
        Class::Skipped => 0,
    };
    rank(a) > rank(b)
}

/// Runs one file to the end, past crashes and hangs.
fn run_file(exe: &Path, root: &Path, file: &str, timeout: f64) -> BTreeMap<usize, Rec> {
    let mut recs: BTreeMap<usize, Rec> = BTreeMap::new();
    let mut seen: HashSet<usize> = HashSet::new();
    let mut skip: Vec<usize> = Vec::new();
    let quiet = Duration::from_secs_f64(timeout * 4.0 + 30.0);
    let note =
        |recs: &mut BTreeMap<usize, Rec>, line: usize, class: Class, detail: String| match recs
            .get(&line)
        {
            Some(r) if !worse(class, r.class) => {}
            _ => {
                recs.insert(line, Rec { class, detail });
            }
        };
    for _attempt in 0..8 {
        let skip_arg = skip
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut child = match Command::new(exe)
            .args([
                "child",
                &root.display().to_string(),
                file,
                &timeout.to_string(),
                &skip_arg,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(c) => c,
            Err(e) => {
                note(&mut recs, 0, Class::Error, format!("could not start: {e}"));
                return recs;
            }
        };
        let stdout = child.stdout.take().expect("piped");
        let (tx, rx) = mpsc::channel::<String>();
        let reader = std::thread::spawn(move || {
            for l in BufReader::new(stdout).lines() {
                let Ok(l) = l else { break };
                if tx.send(l).is_err() {
                    break;
                }
            }
        });
        let mut last_seq: Option<usize> = None;
        let mut finished = false;
        let mut hung = false;
        loop {
            match rx.recv_timeout(quiet) {
                Ok(l) if l == "END" => {
                    finished = true;
                    break;
                }
                Ok(l) => {
                    let mut p = l.splitn(4, '\t');
                    let (Some(seq), Some(line), Some(class), detail) =
                        (p.next(), p.next(), p.next(), p.next())
                    else {
                        continue;
                    };
                    let (Ok(seq), Ok(line), Some(class)) = (
                        seq.parse::<usize>(),
                        line.parse::<usize>(),
                        Class::parse(class),
                    ) else {
                        continue;
                    };
                    last_seq = Some(seq);
                    // A restarted child repeats what ran before the crash; the first answer stands.
                    if !seen.insert(seq) {
                        continue;
                    }
                    note(&mut recs, line, class, detail.unwrap_or("").to_string());
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    hung = true;
                    break;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        if hung {
            let _ = child.kill();
        }
        let status = child.wait();
        let _ = reader.join();
        if finished {
            return recs;
        }
        // The record after the last one reported is the one that crashed or hung. Its line is
        // not known here; the restarted child reports it as skipped, and it is marked an error
        // below once its line is known.
        let culprit = last_seq.map_or(0, |s| s + 1);
        let why = if hung {
            "no answer: killed after the statement timeout".to_string()
        } else {
            match status {
                Ok(s) => format!("the process died: {s}"),
                Err(e) => format!("the process died: {e}"),
            }
        };
        let line = culprit_line(root, file, culprit);
        match line {
            Some(l) => {
                seen.insert(culprit);
                recs.insert(
                    l,
                    Rec {
                        class: Class::Error,
                        detail: why,
                    },
                );
            }
            None => {
                note(&mut recs, 0, Class::Error, why);
                return recs;
            }
        }
        skip.push(culprit);
    }
    note(
        &mut recs,
        0,
        Class::Error,
        "gave up after repeated crashes".into(),
    );
    recs
}

/// The line of the record at `seq`, by parsing the file again (cheap, and only after a crash).
fn culprit_line(root: &Path, file: &str, seq: usize) -> Option<usize> {
    let script = parse::parse(&root.join(file), &HashMap::new()).ok()?;
    script
        .items
        .iter()
        .enumerate()
        .skip(seq)
        .find_map(|(_, it)| match it {
            parse::Item::Run(r) => Some(r.line),
            _ => None,
        })
}

fn list(dir: &Path, root: &Path, out: &mut Vec<String>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<PathBuf> = rd.filter_map(|e| e.ok().map(|e| e.path())).collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            list(&p, root, out);
        } else if p.extension().is_some_and(|e| e == "test") {
            out.push(p.strip_prefix(root).unwrap().display().to_string());
        }
    }
}

fn parent(args: &[String]) {
    let [root, out_dir, ..] = args else { usage() };
    let root = PathBuf::from(root);
    let out_dir = PathBuf::from(out_dir);
    let jobs: usize = flag(args, "--jobs")
        .and_then(|j| j.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(2, |n| n.get()));
    let timeout: f64 = flag(args, "--timeout")
        .and_then(|t| t.parse().ok())
        .unwrap_or(10.0);
    let only = flag(args, "--only").unwrap_or_default();
    let mut files = Vec::new();
    list(&root.join("test/sql"), &root, &mut files);
    files.retain(|f| f.starts_with(&format!("test/sql/{only}")));
    if files.is_empty() {
        eprintln!("no .test files under {}/test/sql", root.display());
        std::process::exit(2);
    }
    std::fs::create_dir_all(&out_dir).expect("out dir");
    let exe = std::env::current_exe().expect("own path");
    let total = files.len();
    eprintln!("{total} files, {jobs} at a time, {timeout}s per statement");
    let queue = Arc::new(Mutex::new(files.into_iter().rev().collect::<Vec<_>>()));
    let results: Arc<Mutex<Vec<FileRecs>>> = Arc::default();
    let started = std::time::Instant::now();
    let workers: Vec<_> = (0..jobs)
        .map(|_| {
            let queue = queue.clone();
            let results = results.clone();
            let exe = exe.clone();
            let root = root.clone();
            std::thread::spawn(move || {
                loop {
                    let Some(f) = queue.lock().unwrap().pop() else {
                        break;
                    };
                    let recs = run_file(&exe, &root, &f, timeout);
                    let mut r = results.lock().unwrap();
                    r.push((f, recs));
                    if r.len().is_multiple_of(250) {
                        eprintln!("{}/{total} files, {:?}", r.len(), started.elapsed());
                    }
                }
            })
        })
        .collect();
    for w in workers {
        let _ = w.join();
    }
    let mut results = std::mem::take(&mut *results.lock().unwrap());
    results.sort_by(|a, b| a.0.cmp(&b.0));
    let mut tsv = String::from("file\tline\tclass\tdetail\n");
    for (f, recs) in &results {
        for (line, r) in recs {
            tsv.push_str(&format!("{f}\t{line}\t{}\t{}\n", r.class.name(), r.detail));
        }
    }
    std::fs::write(out_dir.join("records.tsv"), tsv).expect("write records.tsv");
    eprintln!(
        "{total} files in {:?}; records in {}",
        started.elapsed(),
        out_dir.join("records.tsv").display()
    );
}

type Counts = [u64; 5];

fn idx(c: Class) -> usize {
    CLASSES.iter().position(|x| *x == c).unwrap()
}

/// `test/sql/<dir>/...` -> `<dir>`.
fn top_dir(file: &str) -> String {
    let rest = file.strip_prefix("test/sql/").unwrap_or(file);
    match rest.split_once('/') {
        Some((d, _)) => d.to_string(),
        None => "(top)".to_string(),
    }
}

fn read_records(out_dir: &Path) -> Vec<(String, usize, Class, String)> {
    let text = std::fs::read_to_string(out_dir.join("records.tsv")).expect("records.tsv");
    text.lines()
        .skip(1)
        .filter_map(|l| {
            let mut p = l.splitn(4, '\t');
            let f = p.next()?.to_string();
            let line = p.next()?.parse().ok()?;
            let c = Class::parse(p.next()?)?;
            Some((f, line, c, p.next().unwrap_or("").to_string()))
        })
        .collect()
}

fn read_baseline(path: &Path) -> BTreeMap<String, Counts> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return BTreeMap::new();
    };
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.starts_with("directory\t"))
        .filter_map(|l| {
            let p: Vec<&str> = l.split('\t').collect();
            if p.len() != 6 {
                return None;
            }
            let mut c = [0u64; 5];
            for (i, v) in p[1..].iter().enumerate() {
                c[i] = v.parse().ok()?;
            }
            Some((p[0].to_string(), c))
        })
        .collect()
}

fn write_baseline(path: &Path, dirs: &BTreeMap<String, Counts>) {
    let mut s = String::from(
        "# Per-directory record counts from DuckDB's test/sql at the tag in docs/bench/sqllogictest.sh.\n\
         # CI fails if any directory's wrong count rises or its pass count falls; regenerate with\n\
         # docs/bench/sqllogictest.sh --update.\n\
         directory\tpass\trefused\twrong\terror\tskipped\n",
    );
    for (d, c) in dirs {
        s.push_str(&format!(
            "{d}\t{}\t{}\t{}\t{}\t{}\n",
            c[0], c[1], c[2], c[3], c[4]
        ));
    }
    std::fs::write(path, s).expect("write baseline");
}

fn report(args: &[String]) -> i32 {
    let [out_dir, baseline, ..] = args else {
        usage()
    };
    let out_dir = Path::new(out_dir);
    let baseline = Path::new(baseline);
    let records = read_records(out_dir);
    let mut dirs: BTreeMap<String, Counts> = BTreeMap::new();
    let mut files: BTreeMap<String, Counts> = BTreeMap::new();
    for (f, _, c, _) in &records {
        dirs.entry(top_dir(f)).or_default()[idx(*c)] += 1;
        files.entry(f.clone()).or_default()[idx(*c)] += 1;
    }
    let mut ftsv = String::from("file\tpass\trefused\twrong\terror\tskipped\n");
    for (f, c) in &files {
        ftsv.push_str(&format!(
            "{f}\t{}\t{}\t{}\t{}\t{}\n",
            c[0], c[1], c[2], c[3], c[4]
        ));
    }
    std::fs::write(out_dir.join("files.tsv"), ftsv).expect("write files.tsv");
    let old = read_baseline(baseline);
    let mut worse_lines = Vec::new();
    let mut better = false;
    let names: HashSet<&String> = dirs.keys().chain(old.keys()).collect();
    let mut names: Vec<&String> = names.into_iter().collect();
    names.sort();
    for d in names {
        let now = dirs.get(d).copied().unwrap_or_default();
        let was = old.get(d).copied().unwrap_or_default();
        if now[2] > was[2] {
            worse_lines.push(format!("{d}: wrong answers {} -> {}", was[2], now[2]));
        }
        if now[0] < was[0] {
            worse_lines.push(format!("{d}: passes {} -> {}", was[0], now[0]));
        }
        if now[0] > was[0] || now[2] < was[2] {
            better = true;
        }
    }
    if let Some(doc) = flag(args, "--doc") {
        let tag = flag(args, "--tag").unwrap_or_default();
        let command = flag(args, "--command").unwrap_or_default();
        write_doc(Path::new(&doc), &dirs, &records, &tag, &command);
    }
    let mut total = [0u64; 5];
    for c in dirs.values() {
        for i in 0..5 {
            total[i] += c[i];
        }
    }
    eprintln!(
        "{} records: {} pass, {} refused, {} wrong, {} error, {} skipped",
        total.iter().sum::<u64>(),
        total[0],
        total[1],
        total[2],
        total[3],
        total[4]
    );
    if args.iter().any(|a| a == "--update") {
        write_baseline(baseline, &dirs);
        eprintln!("wrote {}", baseline.display());
        return 0;
    }
    if !worse_lines.is_empty() {
        eprintln!("sqllogictest ratchet: worse than {}:", baseline.display());
        for l in &worse_lines {
            eprintln!("  {l}");
        }
        eprintln!(
            "records are in {}; each line is file, line, class, detail.",
            out_dir.join("records.tsv").display()
        );
        return 1;
    }
    if better {
        eprintln!(
            "sqllogictest ratchet: better than {}. Lock it in with `docs/bench/sqllogictest.sh --update` \
             and commit the baseline and docs/bench/sqllogictest.md.",
            baseline.display()
        );
    } else {
        eprintln!("sqllogictest ratchet: matches {}", baseline.display());
    }
    0
}

/// A skip's reason, without the record-specific part, for the doc's breakdown.
fn reason(detail: &str) -> String {
    let d = detail.strip_prefix("after ").unwrap_or(detail);
    let d = d.split(':').next().unwrap_or(d);
    let words: Vec<&str> = d.split_whitespace().take(4).collect();
    if detail.starts_with("after ") {
        format!("after {}", words.join(" "))
    } else {
        words.join(" ")
    }
}

fn write_doc(
    path: &Path,
    dirs: &BTreeMap<String, Counts>,
    records: &[(String, usize, Class, String)],
    tag: &str,
    command: &str,
) {
    let mut total = [0u64; 5];
    for c in dirs.values() {
        for i in 0..5 {
            total[i] += c[i];
        }
    }
    let all: u64 = total.iter().sum();
    let ran = all - total[4];
    let pct = |n: u64, of: u64| {
        if of == 0 {
            "0.0".to_string()
        } else {
            format!("{:.1}", n as f64 * 100.0 / of as f64)
        }
    };
    let date = std::env::var("SLT_DATE").unwrap_or_default();
    let files: HashSet<&String> = records.iter().map(|r| &r.0).collect();
    let mut s = String::new();
    s.push_str("# DuckDB's sqllogictest suite against Burrmill\n\n");
    s.push_str(
        "Generated by `docs/bench/sqllogictest.sh`; do not edit by hand. The counts are the ones in\n\
         `docs/bench/sqllogictest-baseline.tsv`, which CI holds Burrmill to (#386).\n\n",
    );
    s.push_str(&format!(
        "- DuckDB tag: `{tag}`, every `test/sql/**/*.test` ({} files)\n- Run: {date}\n- Reproduce: `{command}`\n\n",
        files.len()
    ));
    s.push_str("## Totals\n\n| | records | of all | of those run |\n|---|---:|---:|---:|\n");
    for (i, c) in CLASSES.iter().enumerate() {
        let of_run = if *c == Class::Skipped {
            String::new()
        } else {
            format!("{}%", pct(total[i], ran))
        };
        s.push_str(&format!(
            "| {} | {} | {}% | {} |\n",
            c.name(),
            total[i],
            pct(total[i], all),
            of_run
        ));
    }
    s.push_str(&format!("| all | {all} | | |\n\n"));
    s.push_str(
        "## What each class means\n\n\
         - **pass**: the statement did what DuckDB's file expects, or the query's answer matches.\n\
         - **refused**: Burrmill refused with an error naming what it does not support (`not in the\n  \
           admitted subset`, `not supported`, `not implemented`).\n\
         - **wrong**: Burrmill answered and the answer differs, or a statement DuckDB refuses succeeded.\n\
         - **error**: any other error, a statement timeout, or a crash.\n\
         - **skipped**: the harness could not give the record a fair run; the reasons are below.\n\n\
         A record is one `statement` or `query` in a file. One inside a `loop` runs once per iteration\n\
         and counts once, as its worst outcome.\n\n",
    );
    s.push_str(
        "## How a record is judged\n\n\
         As DuckDB's own runner judges it (`test/sqlite/result_helper.cpp` at the tag): values are\n\
         written as DuckDB's `CAST(v AS VARCHAR)` (NULL as `NULL`, an empty string as `(empty)`, a\n\
         boolean as `1`/`0`), `rowsort` and `valuesort` sort that text, a result written as `N values\n\
         hashing to H` is compared by MD5, and a value whose text differs is equal only in a numeric\n\
         column, where the expected text cast to the column's type must equal the answer: integers and\n\
         decimals exactly, floats within 1% of the answer plus 1e-8 (DuckDB's `ApproxEqual`). The\n\
         column count must match the type letters, and an expected row splits on tabs as DuckDB splits\n\
         it (empty pieces dropped). `statement error` passes on an error containing the expected\n\
         message. The harness is stricter than DuckDB's runner in two places: a result compared by\n\
         label or past the hash threshold is also compared value by value when the file lists the\n\
         values; and a `nosort` answer of several distinct rows is wrong if Burrmill's plan leaves\n\
         their order unfixed (a union or a merge of partitions with no sort above it) or a rerun\n\
         answers in another order, even when this run's order matched, since DuckDB's file fixes it.\n\n\
         Burrmill answers queries only, so the harness keeps the tables the files build: `CREATE\n\
         TABLE`, `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE VIEW` and transactions are mapped onto\n\
         `Engine::register_batch`, `register_view` and `drop_relation`, with every value computed by\n\
         Burrmill (the column types are its `CAST(NULL AS type)`, the rows its answer to the `VALUES`\n\
         or `SELECT` cast to the column types, an update its `CASE WHEN` over the table). NOT NULL,\n\
         PRIMARY KEY and UNIQUE are checked by the harness; a record the harness decides alone (a\n\
         `DROP`, a constraint violation) is skipped, not passed, and so is a write DuckDB refuses that\n\
         the mapping accepts (a default, a clause or a constraint it does not check). Settings that\n\
         change no answer (`threads`, `memory_limit`, profiling, verification, compression) are\n\
         skipped, and after a resource limit (`memory_limit`, `max_temp_directory_size`, ...) an error\n\
         DuckDB raises under it is skipped too, as the harness does not impose it. Any other setting,\n\
         `ATTACH`, `COPY`, `ALTER`, `CREATE MACRO` and the like skip the rest of the file, and so does a\n\
         failed setup write, a write the engine is handed inside a query (`INSERT ... RETURNING`, a DML\n\
         CTE), `EXPLAIN ANALYZE`, and a write on a named connection, so no answer is compared against\n\
         state DuckDB never had.\n\
         Each file runs in its own process on one thread, each statement under a 30-second deadline.\n\n",
    );
    s.push_str("## Per directory\n\n| directory | pass | refused | wrong | error | skipped | pass of run |\n|---|---:|---:|---:|---:|---:|---:|\n");
    for (d, c) in dirs {
        let run = c[0] + c[1] + c[2] + c[3];
        s.push_str(&format!(
            "| {d} | {} | {} | {} | {} | {} | {}% |\n",
            c[0],
            c[1],
            c[2],
            c[3],
            c[4],
            pct(c[0], run)
        ));
    }
    let mut reasons: HashMap<String, u64> = HashMap::new();
    for r in records.iter().filter(|r| r.2 == Class::Skipped) {
        *reasons.entry(reason(&r.3)).or_default() += 1;
    }
    let mut reasons: Vec<(String, u64)> = reasons.into_iter().collect();
    reasons.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    s.push_str("\n## Why records were skipped\n\nThe 25 commonest reasons.\n\n| reason | records |\n|---|---:|\n");
    for (r, n) in reasons.iter().take(25) {
        s.push_str(&format!("| {} | {n} |\n", r.replace('|', "\\|")));
    }
    s.push_str(
        "\n## Licence\n\n\
         DuckDB's test files are read as data from a checkout made at run time, and none is committed\n\
         here. They are DuckDB's, under its MIT licence:\n\n\
         > Copyright 2018-2026 Stichting DuckDB Foundation\n>\n\
         > Permission is hereby granted, free of charge, to any person obtaining a copy of this software\n\
         > and associated documentation files (the \"Software\"), to deal in the Software without\n\
         > restriction, including without limitation the rights to use, copy, modify, merge, publish,\n\
         > distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the\n\
         > Software is furnished to do so, subject to the following conditions:\n>\n\
         > The above copyright notice and this permission notice shall be included in all copies or\n\
         > substantial portions of the Software.\n>\n\
         > THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING\n\
         > BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND\n\
         > NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,\n\
         > DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,\n\
         > OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.\n",
    );
    std::fs::write(path, s).expect("write doc");
}
