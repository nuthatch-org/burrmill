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
