//! The receipt corpus (#341): committed segments, statements and the receipts they give. A change
//! that moves any receipt's statement, inputs or answer fails here, and the release that ships it
//! says so in CHANGELOG.md. `BURRMILL_REGENERATE=1` rewrites `receipts.jsonl` (and writes the
//! segments only where they are missing, since they are the inputs and not the answer).
#![cfg(feature = "datafusion")]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Engine, Receipt};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../corpus/receipts")
}

fn write_segments() {
    let segs = root().join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int64, false),
        Field::new("who", DataType::Utf8, true),
        Field::new("amount", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
    ]));
    for s in 0..3i64 {
        let path = segs.join(format!("transfer-{s:064x}.parquet"));
        if path.exists() {
            continue;
        }
        let ids: Vec<i64> = (s * 100..s * 100 + 100).collect();
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from_iter(
                    ids.iter()
                        .map(|i| (i % 13 != 0).then(|| format!("0x{:040x}", i % 7))),
                )),
                Arc::new(StringArray::from_iter(ids.iter().map(|i| {
                    Some(format!("{}{}", i * 1_000_003 % 99_991, "000000000000"))
                }))),
                Arc::new(Float64Array::from_iter(
                    ids.iter().map(|i| (i % 11 != 0).then(|| *i as f64 / 8.0)),
                )),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(&path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
    }
}

/// `statements.sql`, one per line, `strict:` before one run under strict decimals.
fn statements() -> Vec<(bool, String)> {
    std::fs::read_to_string(root().join("statements.sql"))
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with("--"))
        .map(|l| match l.strip_prefix("strict:") {
            Some(s) => (true, s.trim().to_string()),
            None => (false, l.trim().to_string()),
        })
        .collect()
}

#[test]
fn the_corpus_receipts_are_unchanged() {
    let regenerate = std::env::var("BURRMILL_REGENERATE").is_ok();
    if regenerate {
        write_segments();
    }
    let e = Engine::open_segments(&root().join("segments")).unwrap();
    e.set_deterministic(true);
    let mut lines = Vec::new();
    for (strict, sql) in statements() {
        e.set_strict_decimals(strict);
        let (_, r) = e.sql_receipt(&sql).unwrap_or_else(|x| panic!("{sql}: {x}"));
        lines.push(r.to_json());
    }
    let path = root().join("receipts.jsonl");
    if regenerate {
        std::fs::write(&path, lines.join("\n") + "\n").unwrap();
        return;
    }
    let committed: Vec<Receipt> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|l| Receipt::parse(l).unwrap())
        .collect();
    assert_eq!(
        committed.len(),
        lines.len(),
        "statements.sql and receipts.jsonl disagree"
    );
    for (want, got) in committed.iter().zip(&lines) {
        let got = Receipt::parse(got).unwrap();
        let same = want.statement == got.statement
            && want.strict_decimals == got.strict_decimals
            && want.inputs == got.inputs
            && want.answer == got.answer;
        assert!(
            same,
            "the receipt for {} moved: {want:?} became {got:?}. If that is meant, regenerate with \
             BURRMILL_REGENERATE=1 and say so in CHANGELOG.md",
            want.statement
        );
    }
}
