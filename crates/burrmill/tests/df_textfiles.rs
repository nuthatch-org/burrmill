//! CSV and JSON files opened by `Engine::builder()` (#389): typed by the tab's reader, named in a
//! receipt by their bytes, held to the budget, and refused at the row and column that is wrong.

use std::path::Path;

use burrmill::{Budget, BurrmillError, Engine};
use sha2::{Digest, Sha256};

const CSV: &str =
    "id,price,day,who\n1,1.50,2024-01-01,a\n2,2.25,2024-01-02,b\n3,10.00,2024-01-03,\n";

fn file(dir: &Path, name: &str, text: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, text).unwrap();
    p
}

fn budget(memory_bytes: usize, threads: usize) -> Budget {
    Budget {
        memory_bytes,
        threads,
        spill: None,
    }
}

#[test]
fn a_csv_is_typed_as_the_tab_types_it() {
    let t = tempfile::tempdir().unwrap();
    let p = file(t.path(), "data.csv", CSV);
    let e = Engine::builder().csv(&p, "data").open().unwrap();
    let got = e.sql("SELECT * FROM data").unwrap();
    let tab = burrmill_ingest::ingest_bytes(
        CSV.as_bytes(),
        burrmill_ingest::FormatHint::Csv,
        &burrmill_ingest::Budget::untrusted(),
    )
    .unwrap();
    assert_eq!(got[0].schema().fields(), tab.batches()[0].schema().fields());
    let sum = e.sql("SELECT SUM(price) AS s FROM data").unwrap();
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&sum)
            .unwrap()
            .to_string(),
        "+-------+\n| s     |\n+-------+\n| 13.75 |\n+-------+"
    );
}

#[test]
fn every_corpus_file_is_typed_or_refused_as_the_tab_reads_it_at_any_thread_count() {
    use burrmill_ingest::FormatHint;
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../corpus/ingest");
    let (mut csv, mut json) = (0, 0);
    for entry in std::fs::read_dir(&dir).unwrap() {
        let p = entry.unwrap().path();
        let hint = match p.extension().and_then(|e| e.to_str()) {
            Some("csv") => {
                csv += 1;
                FormatHint::Csv
            }
            Some("json" | "ndjson") => {
                json += 1;
                FormatHint::Json
            }
            _ => continue,
        };
        let bytes = std::fs::read(&p).unwrap();
        let tab =
            burrmill_ingest::ingest_bytes(&bytes, hint, &burrmill_ingest::Budget::untrusted());
        let tab = tab
            .ok()
            .filter(|t| t.report().rejected.is_empty() && !t.batches().is_empty());
        for threads in [1, 8] {
            let b = Engine::builder();
            let b = match hint {
                FormatHint::Csv => b.csv(&p, "t"),
                _ => b.json(&p, "t"),
            };
            let e = b.budget(budget(1 << 30, threads)).open();
            let shown = format!("{} at {threads} threads", p.display());
            match (&tab, e) {
                (Some(tab), Ok(e)) => {
                    let schema = e.result_schema("SELECT * FROM t").unwrap();
                    assert_eq!(
                        schema.fields(),
                        tab.batches()[0].schema().fields(),
                        "{shown}"
                    );
                    let n = e.sql("SELECT COUNT(*) FROM t").unwrap();
                    let n = arrow::util::display::array_value_to_string(n[0].column(0), 0).unwrap();
                    assert_eq!(n, tab.report().rows.to_string(), "{shown}");
                }
                (None, Err(_)) => {}
                (tab, e) => panic!(
                    "{shown}: the tab read it {}, the engine {}",
                    if tab.is_some() { "cleanly" } else { "not" },
                    if e.is_ok() { "did" } else { "refused it" }
                ),
            }
        }
    }
    assert!(csv >= 10, "corpus shrank to {csv} CSV files");
    assert!(json >= 2, "corpus shrank to {json} JSON files");
}

#[test]
fn json_and_ndjson_are_records() {
    let t = tempfile::tempdir().unwrap();
    let a = file(
        t.path(),
        "a.json",
        r#"[{"k": "x", "n": 1}, {"k": "y", "n": 2}]"#,
    );
    let b = file(
        t.path(),
        "b.ndjson",
        "{\"k\": \"x\", \"m\": 5}\n{\"k\": \"z\"}\n",
    );
    let e = Engine::builder()
        .json(&a, "a")
        .json(&b, "b")
        .open()
        .unwrap();
    let got = e.sql("SELECT a.n, b.m FROM a JOIN b ON a.k = b.k").unwrap();
    let text = arrow::util::pretty::pretty_format_batches(&got)
        .unwrap()
        .to_string();
    assert_eq!(
        text,
        "+---+---+\n| n | m |\n+---+---+\n| 1 | 5 |\n+---+---+"
    );
}

#[test]
fn a_receipt_names_the_file_by_its_bytes_at_any_thread_count() {
    let t = tempfile::tempdir().unwrap();
    let p = file(t.path(), "data.csv", CSV);
    let want = format!("csv:{}:{}", hex(&Sha256::digest(CSV)), CSV.len());
    let mut receipts = Vec::new();
    for threads in [1, 4] {
        let e = Engine::builder()
            .csv(&p, "data")
            .budget(budget(1 << 30, threads))
            .open()
            .unwrap();
        e.set_deterministic(true);
        let (_, r) = e
            .sql_receipt("SELECT who, SUM(price) FROM data GROUP BY who")
            .unwrap();
        assert_eq!(r.inputs, vec![want.clone()]);
        e.verify_receipt(&r).unwrap();
        receipts.push(r);
    }
    assert_eq!(receipts[0], receipts[1]);

    std::fs::write(&p, CSV.replace("2.25", "2.26")).unwrap();
    let e = Engine::builder().csv(&p, "data").open().unwrap();
    e.set_deterministic(true);
    let err = e.verify_receipt(&receipts[0]).unwrap_err();
    assert!(matches!(err, BurrmillError::Unverified(_)), "{err}");
}

#[test]
fn a_file_over_the_budget_is_refused_and_what_it_holds_is_the_budgets() {
    let t = tempfile::tempdir().unwrap();
    let p = file(t.path(), "data.csv", CSV);
    let err = refused(
        Engine::builder()
            .csv(&p, "data")
            .budget(budget(100, 1))
            .open(),
    );
    assert!(
        matches!(&err, BurrmillError::LimitExceeded(m) if m.contains("max_memory_bytes")),
        "{err}"
    );
    // Two files that each fit, but not together.
    let held = {
        let e = Engine::builder().csv(&p, "data").open().unwrap();
        let b = e.sql("SELECT * FROM data").unwrap();
        b.iter().map(|b| b.get_array_memory_size()).sum::<usize>()
    };
    let q = file(t.path(), "more.csv", CSV);
    let one = |memory| {
        Engine::builder()
            .csv(&p, "data")
            .budget(budget(memory, 1))
            .open()
    };
    let two = |memory| {
        Engine::builder()
            .csv(&p, "data")
            .csv(&q, "more")
            .budget(budget(memory, 1))
            .open()
    };
    let memory = (held..held * 8)
        .step_by(16)
        .find(|&m| one(m).is_ok() && two(m).is_err())
        .expect("some budget holds one file and refuses the second");
    let err = refused(two(memory));
    assert!(matches!(err, BurrmillError::LimitExceeded(_)), "{err}");
}

/// About 10 MB of CSV over several of the reader's chunks, with quoted line ends, and `oops` in
/// place of row `bad_at`'s integer if there is one.
fn large_csv(bad_at: Option<usize>) -> String {
    let mut s = String::from("id,n,who\n");
    let mut row = 0;
    while s.len() < 10 << 20 {
        row += 1;
        let n = if Some(row) == bad_at {
            "oops".to_string()
        } else {
            (row % 1000).to_string()
        };
        let who = if row % 101 == 0 {
            format!("\"two\nlines {row}\"")
        } else {
            format!("w{}", row % 7)
        };
        s.push_str(&format!("{row},{n},{who}\n"));
    }
    s
}

#[test]
fn a_large_csv_is_one_table_and_one_receipt_at_any_thread_count() {
    let t = tempfile::tempdir().unwrap();
    let text = large_csv(None);
    let p = file(t.path(), "large.csv", &text);
    let want = format!("csv:{}:{}", hex(&Sha256::digest(&text)), text.len());
    let tab = burrmill_ingest::ingest_bytes(
        text.as_bytes(),
        burrmill_ingest::FormatHint::Csv,
        &burrmill_ingest::Budget::untrusted(),
    )
    .unwrap();
    let mut seen = Vec::new();
    for threads in [1, 3, 8] {
        let e = Engine::builder()
            .csv(&p, "data")
            .budget(budget(1 << 30, threads))
            .open()
            .unwrap();
        e.set_deterministic(true);
        let all = e.sql("SELECT * FROM data LIMIT 1").unwrap();
        assert_eq!(all[0].schema().fields(), tab.batches()[0].schema().fields());
        let (got, r) = e
            .sql_receipt("SELECT who, COUNT(*), SUM(n), MIN(id), MAX(id) FROM data GROUP BY who ORDER BY who")
            .unwrap();
        assert_eq!(r.inputs, vec![want.clone()]);
        e.verify_receipt(&r).unwrap();
        let shown = arrow::util::pretty::pretty_format_batches(&got)
            .unwrap()
            .to_string();
        seen.push((shown, r));
    }
    assert_eq!(seen[0], seen[1]);
    assert_eq!(seen[0], seen[2]);
}

#[test]
fn a_contradiction_far_into_a_large_csv_is_refused_at_its_row() {
    let t = tempfile::tempdir().unwrap();
    let p = file(t.path(), "late.csv", &large_csv(Some(300_000)));
    for threads in [1, 8] {
        let err = refused(
            Engine::builder()
                .csv(&p, "t")
                .budget(budget(1 << 30, threads))
                .open(),
        )
        .to_string();
        assert!(
            err.contains("refused at row 300000, column 2 (n): not a INTEGER, which the rest of the column is"),
            "{err}"
        );
    }
}

/// About 10 MB of JSON records over several of the reader's chunks, one a line (CRLF where
/// `array` is not set) or one array of them, with nested values, a key that comes and goes and
/// `"oops"` in place of record `bad_at`'s integer if there is one.
fn large_json(array: bool, bad_at: Option<usize>) -> String {
    let mut s = String::from(if array { "[\n" } else { "" });
    let mut row = 0;
    while s.len() < 10 << 20 {
        row += 1;
        let n = if Some(row) == bad_at {
            "\"oops\"".to_string()
        } else {
            (row % 1000).to_string()
        };
        let extra = if row % 97 == 0 {
            format!(", \"tags\": [{{\"t\": {row}}}, \"line\\nend\"], \"late\": {row}.5")
        } else {
            String::new()
        };
        if array && row > 1 {
            s.push_str(",\n");
        }
        s.push_str(&format!(
            "{{\"id\": {row}, \"n\": {n}, \"who\": \"w{}\"{extra}}}",
            row % 7
        ));
        if !array {
            s.push_str("\r\n");
        }
    }
    if array {
        s.push_str("\n]\n");
    }
    s
}

#[test]
fn a_large_json_file_is_one_table_and_one_receipt_at_any_thread_count() {
    let t = tempfile::tempdir().unwrap();
    for (name, array) in [("large.ndjson", false), ("large.json", true)] {
        let text = large_json(array, None);
        let p = file(t.path(), name, &text);
        let want = format!("json:{}:{}", hex(&Sha256::digest(&text)), text.len());
        let tab = burrmill_ingest::ingest_bytes(
            text.as_bytes(),
            burrmill_ingest::FormatHint::Json,
            &burrmill_ingest::Budget::untrusted(),
        )
        .unwrap();
        assert!(tab.batches().len() > 1, "{name}");
        let mut seen = Vec::new();
        for threads in [1, 3, 8] {
            let e = Engine::builder()
                .json(&p, "data")
                .budget(budget(1 << 30, threads))
                .open()
                .unwrap();
            e.set_deterministic(true);
            let all = e.sql("SELECT * FROM data LIMIT 1").unwrap();
            assert_eq!(
                all[0].schema().fields(),
                tab.batches()[0].schema().fields(),
                "{name}"
            );
            let (got, r) = e
                .sql_receipt(
                    "SELECT who, COUNT(*), SUM(n), MIN(id), MAX(id), COUNT(tags), SUM(late) \
                     FROM data GROUP BY who ORDER BY who",
                )
                .unwrap();
            assert_eq!(r.inputs, vec![want.clone()]);
            e.verify_receipt(&r).unwrap();
            let shown = arrow::util::pretty::pretty_format_batches(&got)
                .unwrap()
                .to_string();
            seen.push((shown, r));
        }
        assert_eq!(seen[0], seen[1], "{name}");
        assert_eq!(seen[0], seen[2], "{name}");
    }
}

#[test]
fn a_contradiction_far_into_a_large_json_file_is_refused_at_its_row_and_key() {
    let t = tempfile::tempdir().unwrap();
    for (name, array) in [("late.ndjson", false), ("late.json", true)] {
        let p = file(t.path(), name, &large_json(array, Some(200_000)));
        for threads in [1, 8] {
            let err = refused(
                Engine::builder()
                    .json(&p, "t")
                    .budget(budget(1 << 30, threads))
                    .open(),
            )
            .to_string();
            assert!(
                err.contains("refused at row 200000, column 2 (n): not a INTEGER, which the rest of the column is"),
                "{name} at {threads} threads: {err}"
            );
        }
    }
}

#[test]
fn a_malformed_file_is_refused_at_its_row_and_column() {
    let t = tempfile::tempdir().unwrap();
    for (name, text, want) in [
        (
            "short.csv",
            "a,b,c\n1,2,3\n4,5\n6,7,8\n",
            "refused at row 2, column 3 (c): 2 fields where the widest row has 3",
        ),
        (
            "typed.csv",
            "id,n\n1,10\n2,11\n3,12\n4,13\n5,14\n6,15\n7,16\n8,17\n9,18\n10,19\n11,oops\n",
            "refused at row 11, column 2 (n): not a INTEGER, which the rest of the column is",
        ),
        (
            "quote.csv",
            "a,b\n1,2\n3,\"never closed\n",
            "refused at row 2, column 2 (b): a quote that is not closed",
        ),
        (
            "bad.ndjson",
            "{\"a\": 1}\n{\"a\": 2}\n{\"a\" 3}\n",
            "not a readable json file: line 3, column 6",
        ),
    ] {
        let p = file(t.path(), name, text);
        let b = Engine::builder();
        let b = if name.ends_with(".csv") {
            b.csv(&p, "t")
        } else {
            b.json(&p, "t")
        };
        let err = refused(b.open()).to_string();
        assert!(err.contains(want), "{name}: {err}");
        assert!(err.contains(name), "{name}: {err}");
    }
}

#[test]
fn a_name_is_one_table_whatever_reads_it() {
    let t = tempfile::tempdir().unwrap();
    let p = file(t.path(), "data.csv", CSV);
    let q = file(t.path(), "data.json", r#"[{"a": 1}]"#);
    let err = refused(Engine::builder().csv(&p, "data").json(&q, "DATA").open());
    assert!(matches!(err, BurrmillError::Config(_)), "{err}");
}

fn refused(r: burrmill::Result<Engine>) -> BurrmillError {
    match r {
        Ok(_) => panic!("opened"),
        Err(e) => e,
    }
}

fn hex(d: &[u8]) -> String {
    d.iter().map(|b| format!("{b:02x}")).collect()
}
