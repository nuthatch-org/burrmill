//! Relic corpora (RFC-0004 §11): admission, the five tables, exact uint256 and refusal on overflow.
//!
//! The fixture is a synthetic relic 2079 sealed by The Legacy's archive-RPC Shadow tests, with a
//! detached pact start: every transaction type, withdrawals from block 17,031,171, and log data
//! whose first word is far past 10^38.

use std::path::{Path, PathBuf};

use arrow::array::{Array, AsArray, BooleanArray, Decimal128Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use burrmill::Engine;

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/relics/002079")
}

fn one(batches: &[RecordBatch]) -> RecordBatch {
    let b: Vec<_> = batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .cloned()
        .collect();
    assert_eq!(b.len(), 1, "{}", pretty_format_batches(batches).unwrap());
    b[0].clone()
}

fn count(e: &Engine, sql: &str) -> i64 {
    one(&e.sql(sql).unwrap())
        .column(0)
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(0)
}

fn manifest_rows(table: &str) -> i64 {
    let m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture().join("manifest.json")).unwrap()).unwrap();
    m["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["table"] == table)
        .map_or(0, |f| f["row_count"].as_i64().unwrap())
}

#[test]
fn every_table_counts_what_the_manifest_says() {
    let e = Engine::open_relics(&fixture()).unwrap();
    for t in ["headers", "transactions", "receipts", "logs", "withdrawals"] {
        assert_eq!(
            count(&e, &format!("SELECT COUNT(*) FROM {t}")),
            manifest_rows(t),
            "{t}"
        );
    }
}

#[test]
fn the_relics_table_says_what_was_admitted_and_how_it_is_anchored() {
    let e = Engine::open_relics(&fixture()).unwrap();
    let b = one(&e
        .sql("SELECT index, admitted, bytes_checked, detached, anchor, failure FROM relics")
        .unwrap());
    assert_eq!(b.num_rows(), 1);
    assert_eq!(
        b.column(0)
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        2079
    );
    let flag = |i: usize| {
        b.column(i)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    };
    assert!(flag(1) && flag(2) && flag(3));
    let anchor = b
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert!(anchor.contains("not established"), "{anchor}");
    assert!(b.column(5).is_null(0));
}

#[test]
fn a_tampered_file_is_listed_and_exposes_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let relic = dir.path().join("relics/002079");
    std::fs::create_dir_all(&relic).unwrap();
    for f in std::fs::read_dir(fixture()).unwrap().flatten() {
        std::fs::copy(f.path(), relic.join(f.file_name())).unwrap();
    }
    let mut bytes = std::fs::read(relic.join("logs.parquet")).unwrap();
    bytes[100] ^= 1;
    std::fs::write(relic.join("logs.parquet"), bytes).unwrap();

    let e = Engine::open_relics(dir.path()).unwrap();
    let b = one(&e.sql("SELECT admitted, failure FROM relics").unwrap());
    assert!(
        !b.column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    let why = b
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert!(
        why.contains("logs.parquet") && why.contains("BLAKE3"),
        "{why}"
    );
    assert_eq!(count(&e, "SELECT COUNT(*) FROM headers"), 0);
}

fn dec(e: &Engine, literal: &str) -> Option<i128> {
    let b = one(&e.sql(&format!("SELECT u256_dec({literal}) AS v")).unwrap());
    let a = b
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    (!a.is_null(0)).then(|| a.value(0))
}

fn hexlit(v: i128) -> String {
    let h = format!("{v:x}");
    format!("X'{}{h}'", if h.len() % 2 == 1 { "0" } else { "" })
}

#[test]
fn u256_dec_is_exact_to_the_last_representable_value_and_null_past_it() {
    let e = Engine::open_relics(&fixture()).unwrap();
    let max = 10i128.pow(38) - 1;
    assert_eq!(dec(&e, "X'00'"), Some(0));
    assert_eq!(dec(&e, "X'01'"), Some(1));
    assert_eq!(dec(&e, &hexlit(max)), Some(max));
    assert_eq!(dec(&e, &hexlit(max + 1)), None);
    assert_eq!(dec(&e, &format!("X'{}'", "ff".repeat(32))), None);
    let wide = e.sql(&format!("SELECT u256_dec(X'{}')", "01".repeat(33)));
    assert!(wide.is_err(), "33 bytes must be refused, not NULL");
}

/// `u256_dec` is tainted exactly as a nest's `_dec` is (RFC-0044 §3.2): SUM skips a row the cast
/// reports as NULL, as DuckDB's does, and refuses only a total that does not fit; any other
/// aggregate refuses at the first row whose value was dropped.
#[test]
fn u256_dec_is_tainted_as_a_nest_dec_column_is() {
    let e = Engine::open_relics(&fixture()).unwrap();
    let skipped = one(&e
        .sql("SELECT SUM(u256_dec(word(data, 0))) AS s FROM logs")
        .unwrap());
    let plain = one(&e
        .sql("SELECT SUM(TRY_CAST(u256_text(word(data, 0)) AS DECIMAL(38,0))) AS s FROM logs")
        .unwrap());
    assert_eq!(skipped.column(0), plain.column(0));

    let refused = e.sql("SELECT MAX(u256_dec(word(data, 0))) FROM logs");
    let err = refused
        .expect_err("MAX over a dropped value must refuse")
        .to_string();
    assert!(err.contains("TRY_CAST value"), "{err}");

    let b = one(&e
        .sql(
            "SELECT SUM(CASE WHEN u256_dec(word(data, 0)) IS NULL THEN 1 ELSE 0 END) AS oversized, \
                    COUNT(*) AS logs FROM logs",
        )
        .unwrap());
    println!(
        "{}",
        pretty_format_batches(std::slice::from_ref(&b)).unwrap()
    );
    let oversized = b
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .map_or_else(
            || {
                b.column(0)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(0) as i128
            },
            |a| a.value(0),
        );
    assert!(oversized > 0, "the fixture has words past 10^38");
}

#[test]
fn two_representable_values_whose_total_does_not_fit_are_refused() {
    let e = Engine::open_relics(&fixture()).unwrap();
    // 10^38 - 1 twice, as two log-data words: each fits, their sum does not.
    let max = format!("{:064x}", 10u128.pow(38) - 1);
    let err = e
        .sql(&format!(
            "SELECT SUM(u256_dec(word(d, 0))) FROM (SELECT X'{max}' AS d UNION ALL SELECT X'{max}' AS d) t"
        ))
        .expect_err("an overflowing exact total must refuse")
        .to_string();
    assert!(err.contains("does not fit"), "{err}");
}

#[test]
fn dec_columns_sit_beside_every_uint256_column() {
    let e = Engine::open_relics(&fixture()).unwrap();
    let b = one(&e
        .sql(
            "SELECT COUNT(*) AS n, SUM(value_dec) AS wei, \
                    SUM(CASE WHEN value_overflow THEN 1 ELSE 0 END) AS over \
             FROM transactions",
        )
        .unwrap());
    println!("{}", pretty_format_batches(&[b]).unwrap());
    e.sql("SELECT base_fee_per_gas_dec, effective_gas_price_dec FROM headers, receipts LIMIT 1")
        .unwrap();
}

#[test]
fn the_withdrawals_showcase_answers_from_the_shapella_block() {
    let e = Engine::open_relics(&fixture()).unwrap();
    let b = one(&e
        .sql(
            "SELECT COUNT(*) AS withdrawals, SUM(amount) AS gwei_total, \
                    MIN(block_number) AS first_block, MIN(\"index\") AS first_index \
             FROM withdrawals WHERE block_number BETWEEN 17031168 AND 17039359",
        )
        .unwrap());
    println!(
        "{}",
        pretty_format_batches(std::slice::from_ref(&b)).unwrap()
    );
    assert_eq!(
        b.column(2)
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        17_031_171
    );
}
