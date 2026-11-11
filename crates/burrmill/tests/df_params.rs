//! `$n` placeholders: a host passes values beside the statement instead of writing them into it.
//! The statement meets every refusal a literal one does, and a value is never read as SQL.
#![cfg(feature = "datafusion")]

use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{BurrmillError, Engine, Param};
use serde_json::{Value, json};

fn segment(dir: &std::path::Path, rows: &[(u64, &str, &str)]) -> (std::path::PathBuf, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let path = dir.join(format!("t-{:064x}.parquet", 1));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

/// `t` over one segment, with `amount_dec` beside `amount`.
fn engine(dir: &std::path::Path) -> Engine {
    let declared = vec![
        ("block_number".to_string(), "u64".to_string()),
        ("who".to_string(), "string".to_string()),
        ("amount".to_string(), "word32".to_string()),
    ];
    let file = segment(dir, &[(10, "x", "5"), (11, "y", "7"), (12, "x", "9")]);
    let mut e = Engine::open_empty().unwrap();
    e.register_facts("t", &declared, vec![file], &[], (None, None))
        .unwrap();
    e
}

fn rows(e: &Engine, sql: &str, params: &[Param]) -> Vec<Value> {
    e.sql_with_params(sql, params)
        .unwrap_or_else(|err| panic!("{sql}\n{err:?}"))
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

#[test]
fn a_parameter_is_a_value_and_never_sql() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let count = "SELECT count(*) AS n FROM t WHERE who = $1";
    assert_eq!(rows(&e, count, &["x".into()]), vec![json!({"n": 2})]);
    for smuggled in [
        "x' OR '1'='1",
        "x' --",
        "x'; DROP TABLE t; --",
        "x' UNION SELECT who FROM t__raw --",
    ] {
        assert_eq!(
            rows(&e, count, &[smuggled.into()]),
            vec![json!({"n": 0})],
            "{smuggled}"
        );
    }
    // Text naming a hidden table is only text.
    assert_eq!(
        rows(&e, "SELECT $1 AS v", &["t__raw".into()]),
        vec![json!({"v": "t__raw"})]
    );
}

#[test]
fn parameters_answer_as_the_literals_they_stand_for() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let cases: [(&str, &str, Vec<Param>); 5] = [
        (
            "SELECT who, amount_dec FROM t WHERE amount_dec > $1 ORDER BY block_number",
            "SELECT who, amount_dec FROM t WHERE amount_dec > 5 ORDER BY block_number",
            vec![Param::Int(5)],
        ),
        (
            "SELECT sum(amount_dec) AS s FROM t WHERE block_number BETWEEN $1 AND $2",
            "SELECT sum(amount_dec) AS s FROM t WHERE block_number BETWEEN 11 AND 12",
            vec![Param::UInt(11), Param::UInt(12)],
        ),
        (
            "SELECT who FROM t WHERE who IN ($1, $2) ORDER BY block_number",
            "SELECT who FROM t WHERE who IN ('y', 'z') ORDER BY block_number",
            vec!["y".into(), "z".into()],
        ),
        (
            "SELECT coalesce($1, who) AS w FROM t WHERE $2 ORDER BY block_number",
            "SELECT coalesce(NULL, who) AS w FROM t WHERE true ORDER BY block_number",
            vec![Param::Null, Param::Bool(true)],
        ),
        (
            "SELECT who FROM t WHERE amount_dec = (SELECT max(amount_dec) FROM t WHERE who = $1)",
            "SELECT who FROM t WHERE amount_dec = (SELECT max(amount_dec) FROM t WHERE who = 'x')",
            vec!["x".into()],
        ),
    ];
    for (with, literal, params) in cases {
        assert_eq!(rows(&e, with, &params), rows(&e, literal, &[]), "{with}");
    }
}

#[test]
fn a_parameterised_statement_meets_every_refusal() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let refused = |sql: &str, params: &[Param]| match e.sql_with_params(sql, params) {
        Err(BurrmillError::NotAllowed(m)) => m,
        other => panic!("{sql} was not refused: {other:?}"),
    };
    let x = [Param::from("x")];
    refused("SELECT * FROM t__raw WHERE who = $1", &x);
    refused("DELETE FROM t WHERE who = $1", &x);
    refused("SELECT * FROM read_parquet($1)", &x);
    let deep = format!("SELECT $1{} AS v", " + 1".repeat(200));
    refused(&deep, &[Param::Int(1)]);

    // The value bound applies to what a parameter builds, as to what a literal builds.
    e.set_value_limit(1 << 10);
    let inline = e.sql("SELECT repeat('x', 1048576) AS v").unwrap_err();
    let bound = e
        .sql_with_params("SELECT repeat('x', $1) AS v", &[Param::Int(1 << 20)])
        .unwrap_err();
    assert_eq!(
        std::mem::discriminant(&inline),
        std::mem::discriminant(&bound),
        "{inline:?} against {bound:?}"
    );
}

#[test]
fn an_address_parameter_compares_as_an_address_literal_does() {
    let tmp = tempfile::tempdir().unwrap();
    let a = format!("0x{}", "ab".repeat(20));
    let b = format!("0x{}", "cd".repeat(20));
    let file = segment(tmp.path(), &[(10, &a, "5"), (11, &b, "7")]);
    let declared = vec![
        ("block_number".to_string(), "u64".to_string()),
        ("who".to_string(), "address".to_string()),
        ("amount".to_string(), "word32".to_string()),
    ];
    let mut e = Engine::open_empty().unwrap();
    e.register_facts("t", &declared, vec![file], &[], (None, None))
        .unwrap();
    let mixed = format!("0x{}", "AB".repeat(20));
    for value in [a.as_str(), mixed.as_str(), "0xab"] {
        let literal = format!("SELECT who, amount FROM t WHERE who = '{value}'");
        assert_eq!(
            rows(
                &e,
                "SELECT who, amount FROM t WHERE who = $1",
                &[value.into()]
            ),
            rows(&e, &literal, &[]),
            "{value}"
        );
    }
}

#[test]
fn a_placeholder_without_a_value_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    assert!(
        e.sql_with_params("SELECT who FROM t WHERE who = $2", &["x".into()])
            .is_err()
    );
}

#[test]
fn the_admission_bound_counts_a_parameterised_statement() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let sql = "SELECT count(*) FROM t WHERE who = $1 UNION ALL SELECT count(*) FROM t";
    assert_eq!(e.parquet_scans_with_params(sql, &["x".into()]).unwrap(), 2);
}
