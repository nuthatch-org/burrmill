//! The sandbox corpus (#344): every statement under `corpus/sandbox/` is refused, with the kind its
//! header names, by an engine opened as docs/sandbox.md tells a host to open one, inside a time and
//! a memory bound. A case marked `gap` is a known gap in docs/sandbox.md, and runs only in the
//! ignored test below until its fix switches it on.
#![cfg(feature = "datafusion")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, BurrmillError, Engine, Stop};
use serde_json::json;

/// docs/sandbox.md's configuration, as a host opens it.
const MEMORY_BYTES: usize = 256 << 20;
const THREADS: usize = 4;
const DEADLINE: Duration = Duration::from_secs(1);
/// What the host reads of one answer before it refuses it, as `Limits::serving` caps a result.
const RESULT_BYTES: usize = 32 << 20;

/// Past the deadline a refused case may take this long to return. A join's bound is 100 ms.
const LATE: Duration = Duration::from_secs(1);
/// What the whole corpus may add to the process's resident high-water mark: the pool, what a
/// statement holds outside it, and what the allocator keeps between statements.
const RESIDENT_BYTES: u64 = 1 << 30;

struct Case {
    name: String,
    /// Kinds, any one of which is right. An exhaustion case may meet the pool or the deadline
    /// first, depending on the machine.
    expect: Vec<String>,
    says: Option<String>,
    gap: Option<String>,
    sql: String,
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../corpus/sandbox")
}

fn cases() -> Vec<Case> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(root())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    paths.sort();
    paths.iter().map(|p| case(p)).collect()
}

/// A header of `-- key: value` lines, then the statement. `pad-to` pads the statement with a
/// trailing comment to that many bytes, so a statement over the limit need not be committed.
fn case(path: &Path) -> Case {
    let text = std::fs::read_to_string(path).unwrap();
    let name = path.file_stem().unwrap().to_string_lossy().into_owned();
    let (mut expect, mut says, mut gap, mut pad, mut tries) = (None, None, None, None, false);
    let mut body = Vec::new();
    for line in text.lines() {
        let header = body.is_empty().then(|| line.strip_prefix("-- ")).flatten();
        let value = |v: &str| Some(v.trim().to_string());
        match header.and_then(|h| h.split_once(": ")) {
            Some(("expect", v)) => expect = Some(v.split('|').map(|k| k.trim().into()).collect()),
            Some(("says", v)) => says = value(v),
            Some(("gap", v)) => gap = value(v),
            Some(("pad-to", v)) => pad = Some(v.trim().parse::<usize>().unwrap()),
            Some(("tries", _)) => tries = true,
            _ if header.is_some() => panic!("{name}: header line {line:?}"),
            _ => body.push(line),
        }
    }
    let mut sql = body.join("\n").trim().to_string();
    if let Some(n) = pad {
        sql.push_str("\n-- ");
        let fill = n.saturating_sub(sql.len());
        sql.push_str(&"x".repeat(fill));
    }
    assert!(tries, "{name}: no `-- tries:` line");
    Case {
        expect: expect.unwrap_or_else(|| panic!("{name}: no `-- expect:` line")),
        name,
        says,
        gap,
        sql,
    }
}

fn kind(e: &BurrmillError) -> &'static str {
    match e {
        BurrmillError::NotAllowed(_) => "NotAllowed",
        BurrmillError::Overflow(_) => "Overflow",
        BurrmillError::Timeout => "Timeout",
        BurrmillError::Cancelled => "Cancelled",
        BurrmillError::Seam(_) => "Seam",
        BurrmillError::LimitExceeded(_) => "LimitExceeded",
        BurrmillError::Plan(_) => "Plan",
        BurrmillError::Substrate(_) => "Substrate",
        BurrmillError::Parse(_) => "Parse",
        BurrmillError::NoSegments(_) => "NoSegments",
        BurrmillError::Config(_) => "Config",
        BurrmillError::Internal(_) => "Internal",
        BurrmillError::Unverified(_) => "Unverified",
        _ => "unknown",
    }
}

fn segment(dir: &Path, table: &str, columns: Vec<(&str, Arc<dyn arrow::array::Array>)>) {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), false))
            .collect::<Vec<_>>(),
    ));
    let batch =
        RecordBatch::try_new(schema.clone(), columns.into_iter().map(|c| c.1).collect()).unwrap();
    let f = std::fs::File::create(dir.join(format!("{table}-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// A nest with a wide column, so `token__transfer__raw` is registered beneath the public view, a
/// sealed table of a million rows for the joins, and a host table the corpus tries to write to.
fn sandboxed() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    segment(
        &segs,
        "token__transfer",
        vec![
            ("from", Arc::new(StringArray::from(vec!["0xa", "0xb"]))),
            ("to", Arc::new(StringArray::from(vec!["0xb", "0xc"]))),
            ("value", Arc::new(StringArray::from(vec!["10", "4"]))),
        ],
    );
    let n = 1_000_000u64;
    segment(
        &segs,
        "events",
        vec![
            (
                "block_number",
                Arc::new(UInt64Array::from_iter_values(0..n)),
            ),
            (
                "who",
                Arc::new(StringArray::from_iter_values(
                    (0..n).map(|i| format!("0x{:040x}", i % 100)),
                )),
            ),
        ],
    );
    std::fs::write(
        tmp.path().join("schema.json"),
        r#"{"tables":[{"table":"token__transfer","columns":[
            {"name":"from","storage":"text"},
            {"name":"to","storage":"text"},
            {"name":"value","storage":"word16"}
        ]}]}"#,
    )
    .unwrap();
    let mut engine = Engine::builder()
        .nest(tmp.path())
        .budget(Budget {
            memory_bytes: MEMORY_BYTES,
            threads: THREADS,
            spill: None,
        })
        .open()
        .unwrap();
    engine.set_explain(false);
    engine
        .register_rows("labels", &[json!({"a": "0x1", "l": "alice"})])
        .unwrap();
    assert!(engine.tables().iter().any(|t| t == "token__transfer__raw"));
    (tmp, engine)
}

/// The statement as a host runs it: under its own deadline, its answer read up to the host's cap.
fn run(engine: &Engine, sql: &str) -> (Result<usize, BurrmillError>, Duration) {
    let stop = Stop {
        deadline: Some(Instant::now() + DEADLINE),
        ..Default::default()
    };
    let started = Instant::now();
    let (mut rows, mut bytes) = (0, 0);
    let r = engine.sql_for_each_stoppable(sql, &[], &stop, None, |b| {
        rows += b.num_rows();
        bytes += b.get_array_memory_size();
        if bytes > RESULT_BYTES {
            return Err(BurrmillError::LimitExceeded(format!(
                "the answer is past the host's result cap of {RESULT_BYTES} bytes"
            )));
        }
        Ok(())
    });
    (r.map(|()| rows), started.elapsed())
}

fn resident_peak() -> u64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `u` is a valid, writable rusage.
    assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) }, 0);
    let max = u.ru_maxrss as u64;
    // Bytes on macOS, KiB on Linux.
    if cfg!(target_os = "macos") {
        max
    } else {
        max * 1024
    }
}

/// What is wrong with `case`'s outcome, if anything.
fn judge(case: &Case, r: &Result<usize, BurrmillError>, took: Duration) -> Option<String> {
    let expected = case.expect.join(" | ");
    let got = match r {
        Ok(0) if case.expect == ["NoRows"] => return None,
        Ok(n) => return Some(format!("answered with {n} rows; expected {expected}")),
        Err(e) => e,
    };
    let text = got.to_string();
    if !case.expect.iter().any(|k| k == kind(got)) {
        let head: String = text.chars().take(300).collect();
        return Some(format!("{} ({head}); expected {expected}", kind(got)));
    }
    if let Some(s) = &case.says
        && !text.contains(s.as_str())
    {
        return Some(format!("{text} does not say {s:?}"));
    }
    // An error quoting a whole input is cut at 4 KiB.
    if text.len() > 5 << 10 {
        return Some(format!("its text is {} bytes", text.len()));
    }
    if text.contains("bug report") {
        return Some(format!("{text} asks the caller to file a bug"));
    }
    if took > DEADLINE + LATE {
        return Some(format!("{} after {took:?}", kind(got)));
    }
    None
}

#[test]
fn every_attempt_in_the_corpus_is_refused() {
    let cases = cases();
    assert!(cases.len() >= 40, "{} cases", cases.len());
    let (_tmp, engine) = sandboxed();
    let tables = engine.tables();
    let before = resident_peak();
    let mut wrong = Vec::new();
    let mut grew = Vec::new();
    for case in cases.iter().filter(|c| c.gap.is_none()) {
        let at = resident_peak();
        let (r, took) = run(&engine, &case.sql);
        if let Some(why) = judge(case, &r, took) {
            wrong.push(format!("{}: {why}", case.name));
        }
        let peak = engine.take_memory_peak();
        if peak > MEMORY_BYTES {
            wrong.push(format!("{}: the pool held {peak} bytes", case.name));
        }
        grew.push((resident_peak() - at, case.name.as_str()));
        // A refused statement leaves nothing behind for the next.
        if let Err(e) = run(&engine, "SELECT count(*) FROM labels").0 {
            wrong.push(format!("{}: the next statement failed: {e}", case.name));
        }
    }
    let grown = resident_peak() - before;
    if grown > RESIDENT_BYTES {
        grew.sort_unstable_by(|a, b| b.cmp(a));
        wrong.push(format!(
            "resident memory grew {} MiB; most at {:?}",
            grown >> 20,
            &grew[..5]
        ));
    }
    assert!(
        wrong.is_empty(),
        "{} of {} not refused as expected:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
    assert_eq!(engine.tables(), tables);
    let labels = engine.sql("SELECT * FROM labels").unwrap();
    assert_eq!(labels.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

/// The third clause of #344: a join-heavy statement stops within 100 ms of its deadline (#350).
#[test]
fn a_join_stops_within_100ms_of_its_deadline() {
    let (_tmp, engine) = sandboxed();
    let joins: Vec<Case> = cases()
        .into_iter()
        .filter(|c| c.name.starts_with("join-"))
        .collect();
    assert!(!joins.is_empty());
    for case in &joins {
        let (r, took) = run(&engine, &case.sql);
        assert!(
            judge(case, &r, took).is_none(),
            "{}: {:?} after {took:?}",
            case.name,
            r.map_err(|e| e.to_string())
        );
        assert!(
            took < DEADLINE + Duration::from_millis(100),
            "{}: {took:?}",
            case.name
        );
    }
}

/// The gaps docs/sandbox.md lists, each refused once it is closed.
#[test]
#[ignore = "known gaps, listed in docs/sandbox.md"]
fn the_known_gaps_are_refused() {
    let (_tmp, engine) = sandboxed();
    let gaps: Vec<Case> = cases().into_iter().filter(|c| c.gap.is_some()).collect();
    let wrong: Vec<String> = gaps
        .iter()
        .filter_map(|case| {
            let (r, took) = run(&engine, &case.sql);
            judge(case, &r, took).map(|why| format!("{}: {why}", case.name))
        })
        .collect();
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
