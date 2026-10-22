//! An address column is fixed binary inside a plan and the text the host wrote at every boundary
//! (docs/research/replacing-duckdb/addresses-as-fixed-binary.md). Each test is a context the
//! lodestar views put an address in.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use serde_json::{Value, json};

fn addr(n: u64) -> String {
    format!("0x{n:040x}")
}

fn write(
    dir: &std::path::Path,
    table: &str,
    n: u64,
    rows: &[(u64, u64, &str)],
) -> (std::path::PathBuf, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, true),
        Field::new("amount", DataType::Utf8, true),
    ]));
    let who: Vec<Option<String>> = rows
        .iter()
        .map(|(_, w, _)| (*w != 0).then(|| addr(*w)))
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(who)),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let path = dir.join(format!("{table}-{n:064x}.parquet"));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

/// `t`: block 1..6, `who` 1,2,1,3,NULL,2 with amounts; `u`: ids as text beside the same addresses;
/// an unsealed row of `t` at block 7 for address 3.
fn engine() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let t = vec![
        write(
            tmp.path(),
            "t",
            1,
            &[(1, 1, "10"), (2, 2, "20"), (3, 1, "30")],
        ),
        write(
            tmp.path(),
            "t",
            2,
            &[(4, 3, "40"), (5, 0, "50"), (6, 2, "60")],
        ),
    ];
    let u = vec![write(
        tmp.path(),
        "u",
        1,
        &[(1, 1, "one"), (2, 2, "two"), (3, 9, "nine")],
    )];
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 64 << 20,
        threads: 2,
        spill: None,
    })
    .unwrap();
    let declared = vec![
        ("block_number".to_string(), "u64".to_string()),
        ("who".to_string(), "address".to_string()),
        ("amount".to_string(), "string".to_string()),
        ("later".to_string(), "address".to_string()),
    ];
    let hot = [json!({"block_number": 7, "who": addr(3), "amount": "70"})];
    e.register_facts("t", &declared, t, &hot, (None, None))
        .unwrap();
    let declared_u = vec![
        ("block_number".to_string(), "u64".to_string()),
        ("who".to_string(), "address".to_string()),
        ("amount".to_string(), "string".to_string()),
    ];
    e.register_facts("u", &declared_u, u, &[], (None, None))
        .unwrap();
    (tmp, e)
}

fn rows(e: &Engine, sql: &str) -> Vec<Value> {
    e.sql(sql)
        .unwrap_or_else(|err| panic!("{sql}\n{err}"))
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

fn col(e: &Engine, sql: &str) -> Vec<Value> {
    rows(e, sql)
        .into_iter()
        .map(|r| r.as_object().unwrap().values().next().unwrap().clone())
        .collect()
}

#[test]
fn an_address_is_served_as_the_text_the_host_wrote() {
    let (_t, e) = engine();
    assert_eq!(
        col(&e, "SELECT who FROM t ORDER BY block_number"),
        vec![
            json!(addr(1)),
            json!(addr(2)),
            json!(addr(1)),
            json!(addr(3)),
            Value::Null,
            json!(addr(2)),
            json!(addr(3))
        ]
    );
    // The type a host sees is text, as before.
    let batches = e.sql("SELECT who, later FROM t LIMIT 1").unwrap();
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::Utf8View
    );
    assert_eq!(
        batches[0].schema().field(1).data_type(),
        &DataType::Utf8View
    );
    // Inside the plan it is fixed binary, and the scan decodes it.
    let plan = rows(&e, "EXPLAIN SELECT who FROM t")
        .iter()
        .map(|r| r.to_string())
        .collect::<String>();
    assert!(plan.contains("address_bytes"), "{plan}");
}

#[test]
fn a_literal_of_either_case_compares_as_an_address() {
    let (_t, e) = engine();
    let upper = addr(2).to_uppercase().replace("0X", "0x");
    for lit in [addr(2), upper] {
        assert_eq!(
            col(
                &e,
                &format!("SELECT block_number FROM t WHERE who = '{lit}' ORDER BY 1")
            ),
            vec![json!(2), json!(6)]
        );
    }
    assert_eq!(
        col(
            &e,
            &format!("SELECT count(*) FROM t WHERE lower(who) = '{}'", addr(1))
        ),
        vec![json!(2)]
    );
    assert_eq!(
        col(
            &e,
            &format!("SELECT count(*) FROM t WHERE who <> '{}'", addr(1))
        ),
        vec![json!(4)]
    );
    // Text that is not an address compares as text, as DuckDB compares it.
    assert_eq!(
        col(&e, "SELECT count(*) FROM t WHERE who = 'nobody'"),
        vec![json!(0)]
    );
    assert_eq!(
        col(&e, "SELECT count(*) FROM t WHERE who < '0x1'"),
        vec![json!(6)]
    );
    assert_eq!(
        col(&e, "SELECT count(*) FROM t WHERE who IS NULL"),
        vec![json!(1)]
    );
}

#[test]
fn a_literal_pushed_to_the_scan_prunes_segments() {
    let (_t, e) = engine();
    let plan = rows(
        &e,
        &format!(
            "EXPLAIN ANALYZE SELECT block_number FROM t WHERE who = '{}'",
            addr(3)
        ),
    )
    .iter()
    .map(|r| r.to_string())
    .collect::<String>();
    let after = plan
        .split("row_groups_pruned_statistics=")
        .nth(1)
        .unwrap_or_else(|| panic!("no statistics pruning reported:\n{plan}"));
    assert!(after.starts_with("2 total → 1 matched"), "{plan}");
}

#[test]
fn in_and_between_and_case_take_address_literals() {
    let (_t, e) = engine();
    assert_eq!(
        col(
            &e,
            &format!(
                "SELECT count(*) FROM t WHERE who IN ('{}', '{}')",
                addr(1),
                addr(3)
            )
        ),
        vec![json!(4)]
    );
    assert_eq!(
        col(
            &e,
            &format!("SELECT count(*) FROM t WHERE who IN ('{}', 'x')", addr(1))
        ),
        vec![json!(2)]
    );
    assert_eq!(
        col(
            &e,
            &format!(
                "SELECT count(*) FROM t WHERE who BETWEEN '{}' AND '{}'",
                addr(2),
                addr(3)
            )
        ),
        vec![json!(4)]
    );
    assert_eq!(
        col(
            &e,
            &format!(
                "SELECT CASE who WHEN '{}' THEN 'one' ELSE 'other' END FROM t WHERE block_number = 1",
                addr(1)
            )
        ),
        vec![json!("one")]
    );
    assert_eq!(
        col(
            &e,
            "SELECT CASE WHEN block_number = 1 THEN who ELSE 'none' END FROM t \
             WHERE block_number <= 2 ORDER BY block_number"
        ),
        vec![json!(addr(1)), json!("none")]
    );
}

#[test]
fn text_contexts_see_the_text() {
    let (_t, e) = engine();
    assert_eq!(
        col(
            &e,
            "SELECT who || '-' || amount FROM t WHERE block_number = 1"
        ),
        vec![json!(format!("{}-10", addr(1)))]
    );
    assert_eq!(
        col(
            &e,
            "SELECT CAST(who AS VARCHAR) FROM t WHERE block_number = 1"
        ),
        vec![json!(addr(1))]
    );
    assert_eq!(
        col(&e, "SELECT length(who) FROM t WHERE block_number = 1"),
        vec![json!(42)]
    );
    assert_eq!(
        col(&e, "SELECT substr(who, 1, 4) FROM t WHERE block_number = 1"),
        vec![json!("0x00")]
    );
    assert_eq!(
        col(&e, "SELECT upper(who) FROM t WHERE block_number = 1"),
        vec![json!(addr(1).to_uppercase())]
    );
    assert_eq!(
        col(&e, "SELECT count(*) FROM t WHERE who LIKE '0x%1'"),
        vec![json!(2)]
    );
    assert_eq!(
        col(
            &e,
            "SELECT coalesce(who, 'none') FROM t WHERE block_number = 5"
        ),
        vec![json!("none")]
    );
    assert_eq!(
        col(
            &e,
            "SELECT coalesce(who, later) FROM t WHERE block_number = 1"
        ),
        vec![json!(addr(1))]
    );
    assert_eq!(
        col(
            &e,
            "SELECT string_agg(who, ',') FROM t WHERE block_number IN (1, 2)"
        ),
        vec![json!(format!("{},{}", addr(1), addr(2)))]
    );
    let mut both = col(
        &e,
        "SELECT who FROM t WHERE block_number = 1 UNION ALL SELECT 'text'",
    );
    both.sort_by_key(|v| v.to_string());
    assert_eq!(both, vec![json!(addr(1)), json!("text")]);
}

#[test]
fn keys_joins_and_aggregates_stay_binary() {
    let (_t, e) = engine();
    assert_eq!(
        rows(&e, "SELECT who, count(*) AS n, sum(CAST(amount AS HUGEINT)) AS s FROM t WHERE who IS NOT NULL GROUP BY who ORDER BY who")
            .iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>(),
        vec![
            format!(r#"{{"who":"{}","n":2,"s":"40"}}"#, addr(1)),
            format!(r#"{{"who":"{}","n":2,"s":"80"}}"#, addr(2)),
            format!(r#"{{"who":"{}","n":2,"s":"110"}}"#, addr(3)),
        ]
    );
    assert_eq!(col(&e, "SELECT max(who) FROM t"), vec![json!(addr(3))]);
    assert_eq!(col(&e, "SELECT min(who) FROM t"), vec![json!(addr(1))]);
    assert_eq!(col(&e, "SELECT count(DISTINCT who) FROM t"), vec![json!(3)]);
    assert_eq!(
        col(
            &e,
            "SELECT u.amount FROM t JOIN u ON u.who = t.who WHERE t.block_number = 2"
        ),
        vec![json!("two")]
    );
    assert_eq!(
        col(
            &e,
            "SELECT count(*) FROM t WHERE who IN (SELECT who FROM u)"
        ),
        vec![json!(4)]
    );
    assert_eq!(
        col(
            &e,
            "SELECT who FROM t WHERE who IS NOT NULL ORDER BY who DESC, block_number LIMIT 1"
        ),
        vec![json!(addr(3))]
    );
    assert_eq!(
        col(
            &e,
            "SELECT sum(CAST(amount AS HUGEINT)) OVER (PARTITION BY who ORDER BY block_number) FROM t WHERE block_number = 3"
        ),
        vec![json!("30")]
    );
    let plan = rows(&e, "EXPLAIN SELECT who, count(*) FROM t GROUP BY who")
        .iter()
        .map(|r| r.to_string())
        .collect::<String>();
    // Decoded under the aggregate, grouped as it is, never rendered inside the plan.
    assert!(
        plan.contains("address_bytes(who@0) as who") && plan.contains("gby=[who@0 as who]"),
        "{plan}"
    );
    assert!(!plan.contains("address_text"), "{plan}");
}
