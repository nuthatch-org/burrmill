//! The owned fold over tables a host registers (`register_facts`), as nuthatch binds every table.
//!
//! The reference for each answer is the same statement with `WHERE 1 = 1` in an arm, which the
//! rule does not substitute, so it runs on DataFusion's own checked aggregate.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use serde_json::{Value, json};

type Transfer<'a> = (u64, &'a str, &'a str, &'a str);

fn segment(dir: &Path, n: u64, rows: &[Transfer], memo: bool) -> (PathBuf, u64) {
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ];
    let text = |f: for<'a> fn(&'a Transfer<'a>) -> &'a str| {
        Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>())) as ArrayRef
    };
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        text(|r| r.1),
        text(|r| r.2),
        text(|r| r.3),
    ];
    if memo {
        fields.push(Field::new("memo", DataType::Utf8, false));
        columns.push(text(|_| "m"));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let path = dir.join(format!("transfer-{n:064x}.parquet"));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

fn declared() -> Vec<(String, String)> {
    // Text, not `address`: the hand-written rows name their parties `0xa`.
    declared_with("string")
}

fn declared_with(party: &str) -> Vec<(String, String)> {
    [
        ("block_number", "u64"),
        ("from", party),
        ("to", party),
        ("value", "word32"),
    ]
    .iter()
    .map(|(c, t)| (c.to_string(), t.to_string()))
    .collect()
}

fn answer(e: &Engine, sql: &str) -> Result<Vec<Value>, String> {
    e.sql(sql).map_err(|e| e.to_string()).map(|batches| {
        batches
            .iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .collect()
    })
}

fn folded(e: &Engine, sql: &str) -> bool {
    let plan = answer(e, &format!("EXPLAIN {sql}")).unwrap();
    serde_json::to_string(&plan)
        .unwrap()
        .contains("OwnedSignedFold")
}

const CAST: &str = r#"CAST("value" AS DECIMAL(38,0))"#;
const TRY_CAST: &str = r#"TRY_CAST("value" AS DECIMAL(38,0))"#;

/// The balance fold over `table`, summing `value`, with `arm` closing its second arm.
fn fold(table: &str, value: &str, arm: &str) -> String {
    format!(
        r#"SELECT addr, SUM(d) AS net FROM (
             SELECT "to" AS addr, {value} AS d FROM {table}
             UNION ALL
             SELECT "from" AS addr, -{value} AS d FROM {table} {arm}
           ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"#
    )
}

/// The statement's answer and whether the owned fold gave it, checked against DataFusion's.
fn checked(e: &Engine, table: &str, value: &str) -> (Result<Vec<Value>, String>, bool) {
    let (sql, reference) = (fold(table, value, ""), fold(table, value, "WHERE 1 = 1"));
    assert!(!folded(e, &reference), "reference was substituted");
    let (got, want) = (answer(e, &sql), answer(e, &reference));
    match (&got, &want) {
        (Ok(g), Ok(w)) => assert_eq!(g, w, "{sql}"),
        (Err(_), Err(_)) => {}
        _ => panic!("{sql}\nfold {got:?}\ndatafusion {want:?}"),
    }
    (got, folded(e, &sql))
}

const ROWS: [Transfer; 4] = [
    (10, "0xa", "0xb", "10"),
    (11, "0xb", "0xc", "4"),
    (12, "0xa", "0xa", "1"),
    (13, "0xc", "0xa", "2"),
];

#[test]
fn a_registered_table_is_folded_and_agrees() {
    let tmp = tempfile::tempdir().unwrap();
    let files = vec![
        segment(tmp.path(), 1, &ROWS[..2], false),
        segment(tmp.path(), 2, &ROWS[2..], false),
    ];
    let mut e = Engine::open_empty().unwrap();
    e.register_facts("transfer", &declared(), files, &[], (None, None))
        .unwrap();
    let want = vec![
        json!({"addr": "0xa", "net": "-8"}),
        json!({"addr": "0xb", "net": "6"}),
        json!({"addr": "0xc", "net": "2"}),
    ];
    for value in [CAST, TRY_CAST] {
        let (got, folded) = checked(&e, "transfer", value);
        assert!(folded, "{value} was not substituted");
        assert_eq!(got.unwrap(), want, "{value}");
    }
}

#[test]
fn unsealed_rows_a_window_and_differing_segments_are_not_folded() {
    let tmp = tempfile::tempdir().unwrap();
    let files = || {
        vec![
            segment(tmp.path(), 1, &ROWS[..2], false),
            segment(tmp.path(), 2, &ROWS[2..], false),
        ]
    };
    let mut e = Engine::open_empty().unwrap();
    let hot = [json!({"block_number": 20, "from": "0xc", "to": "0xd", "value": "7"})];
    e.register_facts("transfer", &declared(), files(), &hot, (None, None))
        .unwrap();
    let (got, folded) = checked(&e, "transfer", CAST);
    assert!(!folded, "unsealed rows were left out of a fold");
    assert!(got.unwrap().contains(&json!({"addr": "0xd", "net": "7"})));

    e.register_facts("transfer", &declared(), files(), &[], (None, Some(11)))
        .unwrap();
    let (got, folded) = checked(&e, "transfer", CAST);
    assert!(!folded, "a window was folded past");
    assert_eq!(
        got.unwrap(),
        vec![
            json!({"addr": "0xa", "net": "-10"}),
            json!({"addr": "0xb", "net": "6"}),
            json!({"addr": "0xc", "net": "4"}),
        ]
    );

    // The fold refuses a column a segment lacks; the scan reads it as NULL.
    let mixed = vec![
        segment(tmp.path(), 3, &ROWS[..2], false),
        segment(tmp.path(), 4, &ROWS[2..], true),
    ];
    e.register_facts("transfer", &declared(), mixed, &[], (None, None))
        .unwrap();
    let (got, folded) = checked(&e, "transfer", CAST);
    assert!(!folded, "segments of two schemas were folded");
    assert_eq!(got.unwrap().len(), 3);
}

#[test]
fn a_fold_reads_the_registration_its_plan_scans() {
    let tmp = tempfile::tempdir().unwrap();
    let mut e = Engine::open_empty().unwrap();
    let first = vec![segment(tmp.path(), 1, &ROWS[..2], false)];
    e.register_facts("transfer", &declared(), first, &[], (None, None))
        .unwrap();
    // A view keeps the plan it was defined with, the first registration's scan.
    e.register_view("kept", "SELECT * FROM transfer").unwrap();
    let before = checked(&e, "kept", CAST).0.unwrap();

    let second = vec![segment(tmp.path(), 2, &ROWS[2..], false)];
    e.register_facts("transfer", &declared(), second, &[], (None, None))
        .unwrap();
    let (got, folded) = checked(&e, "transfer", CAST);
    assert!(folded);
    assert_eq!(
        got.unwrap(),
        vec![
            json!({"addr": "0xa", "net": "2"}),
            json!({"addr": "0xc", "net": "-2"}),
        ]
    );
    let (kept, folded) = checked(&e, "kept", CAST);
    assert!(!folded, "a view over the first registration was folded");
    assert_eq!(kept.unwrap(), before);
}

/// The fold reads every value strictly, as it does over an opened nest (`df_fold`'s
/// `refusals_carry_over`): where DataFusion's `TRY_CAST` drops a row that will not cast, or its
/// `CAST` reads `1e3`, a folded statement refuses. A registration brings that to a host's tables.
#[test]
fn a_folded_registered_table_refuses_a_value_datafusion_reads_or_drops() {
    for (value, bad) in [
        (TRY_CAST, "12abc"),
        (TRY_CAST, "9999999999999999999999999999999999999999"),
        (CAST, "1e3"),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let rows: [Transfer; 2] = [(1, "0xa", "0xb", "10"), (2, "0xb", "0xc", bad)];
        let files = vec![segment(tmp.path(), 1, &rows, false)];
        let mut e = Engine::open_empty().unwrap();
        e.register_facts("transfer", &declared(), files, &[], (None, None))
            .unwrap();
        let sql = fold("transfer", value, "");
        assert!(folded(&e, &sql), "{value} over {bad}");
        let refused = answer(&e, &sql).unwrap_err();
        assert!(refused.contains("will not cast"), "{refused}");
        let unfolded = answer(&e, &fold("transfer", value, "WHERE 1 = 1")).unwrap();
        assert!(unfolded.contains(&json!({"addr": "0xa", "net": "-10"})));
    }
}

/// splitmix64, as `generated_folds` draws its cases.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

#[test]
fn generated_folds_over_registered_tables_agree_with_datafusion() {
    let d38 = "9".repeat(38);
    let (mut refused, mut answered) = (0, 0);
    for seed in 0..40u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let addrs: Vec<String> = (0..1 + rng.below(12))
            .map(|i| format!("0x{i:040x}"))
            .collect();
        // One case in four draws values at the 38-digit line, where sums refuse.
        let huge = seed % 4 == 3;
        let rows: Vec<(u64, String, String, String)> = (0..1 + rng.below(60))
            .map(|i| {
                let value = match rng.below(if huge { 3 } else { 1 }) {
                    0 => rng.below(1_000_000).to_string(),
                    1 => d38.clone(),
                    _ => format!("{}", u128::MAX / (1 + rng.below(9) as u128) / 4),
                };
                let (from, to) = (rng.below(addrs.len()), rng.below(addrs.len()));
                (i as u64, addrs[from].clone(), addrs[to].clone(), value)
            })
            .collect();
        let rows: Vec<Transfer> = rows
            .iter()
            .map(|r| (r.0, r.1.as_str(), r.2.as_str(), r.3.as_str()))
            .collect();
        let tmp = tempfile::tempdir().unwrap();
        let per = rows.len().div_ceil(1 + rng.below(9)).max(1);
        let files: Vec<_> = rows
            .chunks(per)
            .enumerate()
            .map(|(i, chunk)| segment(tmp.path(), i as u64, chunk, false))
            .collect();
        let mut e = Engine::open_empty().unwrap();
        let declared = declared_with(["address", "string"][seed as usize % 2]);
        e.register_facts("transfer", &declared, files, &[], (None, None))
            .unwrap();
        for value in [CAST, TRY_CAST] {
            let (got, folded) = checked(&e, "transfer", value);
            assert!(folded, "seed {seed}: {value} was not substituted");
            match got {
                Ok(_) => answered += 1,
                Err(_) => refused += 1,
            }
        }
    }
    assert!(refused > 0 && answered > refused, "{answered} {refused}");
}
