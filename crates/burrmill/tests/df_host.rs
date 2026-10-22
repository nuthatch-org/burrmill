//! A host registering tables itself: segment lists, hot rows, declared columns and a window, as
//! nuthatch's shadow will (RFC-0044 Amendment 2, phase 2b).

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use serde_json::{Value, json};

fn segment(dir: &std::path::Path, n: u64, rows: &[(u64, &str, &str)]) -> (std::path::PathBuf, u64) {
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
    let path = dir.join(format!("t-{n:064x}.parquet"));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

fn rows(engine: &Engine, sql: &str) -> Vec<Value> {
    engine
        .sql(sql)
        .unwrap()
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

fn declared() -> Vec<(String, String)> {
    vec![
        ("block_number".into(), "u64".into()),
        // Text, not `address`: these rows name their actors `x`, `y` and `z`.
        ("who".into(), "string".into()),
        ("amount".into(), "word32".into()),
        ("memo".into(), "string".into()),
    ]
}

#[test]
fn sealed_and_hot_union_with_declared_columns_and_derived_decimals() {
    let tmp = tempfile::tempdir().unwrap();
    let a = segment(tmp.path(), 1, &[(10, "x", "5"), (11, "y", "7")]);
    let b = segment(
        tmp.path(),
        2,
        &[(12, "x", "99999999999999999999999999999999999999999")],
    );
    let hot = vec![
        json!({"block_number": 20, "who": "z", "amount": "1", "memo": "late"}),
        json!({"block_number": 21, "who": "x", "amount": null}),
    ];
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![a, b], &hot, (None, None))
        .unwrap();
    assert!(engine.has_table("t"));
    assert!(engine.has_table("T"));
    assert!(!engine.has_table("t__raw"));

    // `sum` over `UBIGINT` is `HUGEINT`, which nuthatch's JSON carries as text.
    let got = rows(
        &engine,
        "SELECT count(*) AS n, sum(block_number) AS s FROM t",
    );
    assert_eq!(got, vec![json!({"n": 5, "s": "74"})]);

    // The declared column no segment carries is NULL there and read from hot where it has it.
    let got = rows(
        &engine,
        "SELECT block_number, memo FROM t WHERE memo IS NOT NULL ORDER BY block_number",
    );
    assert_eq!(got, vec![json!({"block_number": 20, "memo": "late"})]);

    // `_dec` and `_overflow` beside the wide column, across sealed and hot alike.
    let got = rows(
        &engine,
        "SELECT block_number, amount_dec, amount_overflow FROM t ORDER BY block_number",
    );
    assert_eq!(
        got,
        vec![
            json!({"block_number": 10, "amount_dec": "5", "amount_overflow": false}),
            json!({"block_number": 11, "amount_dec": "7", "amount_overflow": false}),
            json!({"block_number": 12, "amount_dec": null, "amount_overflow": true}),
            json!({"block_number": 20, "amount_dec": "1", "amount_overflow": false}),
            json!({"block_number": 21, "amount_dec": null, "amount_overflow": false}),
        ]
    );
}

#[test]
fn a_window_bounds_the_rows_and_registration_replaces() {
    let tmp = tempfile::tempdir().unwrap();
    let a = segment(
        tmp.path(),
        1,
        &[(10, "x", "5"), (11, "y", "7"), (12, "x", "9")],
    );
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![a.clone()], &[], (Some(10), Some(11)))
        .unwrap();
    let got = rows(&engine, "SELECT block_number FROM t ORDER BY 1");
    assert_eq!(got, vec![json!({"block_number": 11})]);

    engine
        .register_facts("t", &declared(), vec![a], &[], (None, None))
        .unwrap();
    let got = rows(&engine, "SELECT count(*) AS n FROM t");
    assert_eq!(got, vec![json!({"n": 3})]);

    // Declared and never sealed: empty, typed, and its derived columns exist.
    engine
        .register_facts("u", &declared(), vec![], &[], (None, None))
        .unwrap();
    let got = rows(&engine, "SELECT count(*) AS n, sum(amount_dec) AS s FROM u");
    assert_eq!(got, vec![json!({"n": 0, "s": null})]);
}

/// A cancel armed before the statement is that statement's answer. The host clears the token
/// before the next one.
#[test]
fn a_cancel_armed_before_the_statement_is_not_forgotten() {
    let mut engine = Engine::open_empty().unwrap();
    engine.register_rows("t", &[json!({"n": "1"})]).unwrap();
    engine.cancel_token().cancel();
    let r = engine.sql("SELECT count(*) AS n FROM t");
    assert!(
        matches!(r, Err(burrmill::BurrmillError::Cancelled)),
        "{:?}",
        r.map(|_| ())
    );
    // No batch at all: the refusal is the check before execution, not the one between batches.
    let r = engine.sql("SELECT * FROM t WHERE false");
    assert!(
        matches!(r, Err(burrmill::BurrmillError::Cancelled)),
        "{:?}",
        r.map(|_| ())
    );
    engine.cancel_token().reset();
    assert_eq!(
        rows(&engine, "SELECT count(*) AS n FROM t"),
        vec![json!({"n": 1})]
    );
}

/// A statement stopped from another thread ends as `Cancelled` at its next batch. The host
/// clears the token; the engine then answers the next statement.
#[test]
fn a_cancel_from_another_thread_stops_the_statement_and_not_the_engine() {
    // A cross join of a sealed table with itself, summed: nothing reaches the output until the
    // join has run to the end, so only a cancel seen by the scan can stop it in time.
    let tmp = tempfile::tempdir().unwrap();
    let many: Vec<(u64, &str, &str)> = (0..8_000u64).map(|i| (i, "x", "1")).collect();
    let seg = segment(tmp.path(), 1, &many);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let token = engine.cancel_token();
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(150));
        token.cancel();
    });
    let started = std::time::Instant::now();
    let r = engine.sql("SELECT sum(a.block_number * b.block_number) AS s FROM t a, t b");
    stopper.join().unwrap();
    assert!(
        matches!(r, Err(burrmill::BurrmillError::Cancelled)),
        "{:?}",
        r.map(|_| ())
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(20));
    engine.cancel_token().reset();
    assert_eq!(rows(&engine, "SELECT 1 AS one"), vec![json!({"one": 1})]);
}

/// A statement that reads no nest table stops too: nuthatch's `/sql` watchdog is the only timeout
/// once DuckDB is gone, and `range` and a recursive CTE have no scan of ours to see the token.
fn stops_on_cancel(runaway: &str) {
    let engine = Engine::open_empty().unwrap();
    let token = engine.cancel_token();
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(150));
        token.cancel();
    });
    let started = std::time::Instant::now();
    let r = engine.sql(runaway);
    let took = started.elapsed();
    stopper.join().unwrap();
    assert!(
        matches!(r, Err(burrmill::BurrmillError::Cancelled)),
        "{:?}",
        r.map(|_| ())
    );
    assert!(
        took < std::time::Duration::from_secs(2),
        "stopped after {took:?}"
    );
    engine.cancel_token().reset();
    assert_eq!(rows(&engine, "SELECT 1 AS one"), vec![json!({"one": 1})]);
}

#[test]
fn a_cancel_stops_a_recursive_cte() {
    stops_on_cancel(
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 1000000000) \
         SELECT count(*) AS n FROM t",
    );
}

#[test]
fn a_cancel_stops_a_join_of_ranges() {
    stops_on_cancel(
        "SELECT count(*) AS n FROM range(1000000) a, range(1000000) b WHERE a.range + b.range = -1",
    );
}

/// A statement cancelled while it holds memory gives all of it back: a grouped cross join, stopped
/// once its hash table holds 64 MB of a bounded pool, leaves the pool at zero when its tasks have
/// stopped. They stop after the caller has its error, each at its next yield, and on a loaded runner
/// that was more than a second later (#62).
#[test]
fn a_cancelled_join_returns_its_memory() {
    let tmp = tempfile::tempdir().unwrap();
    let many: Vec<(u64, &str, &str)> = (0..8_000u64).map(|i| (i, "x", "1")).collect();
    let seg = segment(tmp.path(), 1, &many);
    let mut engine = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 1 << 30,
        threads: 4,
        spill: None,
    })
    .unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let token = engine.cancel_token();
    let (r, peak) = std::thread::scope(|s| {
        let watcher = s.spawn(|| {
            let started = std::time::Instant::now();
            let mut peak = 0;
            while started.elapsed() < std::time::Duration::from_secs(30) {
                peak = peak.max(engine.memory_reserved());
                if peak >= 64 << 20 {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            token.cancel();
            peak
        });
        let r = engine.sql(
            "SELECT a.block_number AS x, b.block_number AS y, count(*) AS n FROM t a, t b GROUP BY 1, 2",
        );
        (r, watcher.join().unwrap())
    });
    assert!(
        peak >= 64 << 20,
        "the join never held 64 MB, so this proves nothing: {peak}"
    );
    assert!(
        matches!(r, Err(burrmill::BurrmillError::Cancelled)),
        "{:?}",
        r.map(|_| ())
    );
    let returned = std::time::Instant::now();
    while engine.tasks_alive() > 0 {
        assert!(
            returned.elapsed() < std::time::Duration::from_secs(30),
            "the cancelled statement's tasks never stopped"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(
        engine.memory_reserved(),
        0,
        "a cancelled statement kept its reservation"
    );
    engine.cancel_token().reset();
    assert_eq!(rows(&engine, "SELECT 1 AS one"), vec![json!({"one": 1})]);
}

#[test]
fn rows_become_a_text_table() {
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_rows(
            "labels",
            &[
                json!({"address": "0xab", "label": "exchange"}),
                json!({"address": "0xcd", "label": null}),
            ],
        )
        .unwrap();
    let got = rows(
        &engine,
        "SELECT address, label FROM labels ORDER BY address",
    );
    assert_eq!(
        got,
        vec![
            json!({"address": "0xab", "label": "exchange"}),
            json!({"address": "0xcd", "label": null}),
        ]
    );
}

#[test]
fn a_bounded_engine_refuses_what_an_unbounded_one_answers() {
    let tmp = tempfile::tempdir().unwrap();
    let who: Vec<String> = (0..200_000).map(|i| format!("0x{i:040x}")).collect();
    let data: Vec<(u64, &str, &str)> = who.iter().map(|w| (1, w.as_str(), "1")).collect();
    let seg = segment(tmp.path(), 1, &data);
    let sql = "SELECT who, count(*) AS n FROM t GROUP BY who ORDER BY who";

    let mut free = Engine::open_empty().unwrap();
    free.register_facts("t", &declared(), vec![seg.clone()], &[], (None, None))
        .unwrap();
    assert_eq!(rows(&free, sql).len(), 200_000);

    let mut bounded = Engine::open_empty_within(4 << 20).unwrap();
    bounded
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let err = bounded.sql(sql).unwrap_err().to_string();
    assert!(err.contains("Resources exhausted"), "{err}");
    assert_eq!(
        rows(&bounded, "SELECT count(*) AS n FROM t"),
        vec![json!({"n": 200000})]
    );
}

#[test]
fn ordered_list_per_group_across_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let a = segment(
        tmp.path(),
        1,
        &[(5, "x", "1"), (2, "y", "7"), (9, "x", "3")],
    );
    let b = segment(
        tmp.path(),
        2,
        &[(1, "x", "4"), (7, "y", "8"), (3, "z", "0")],
    );
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![a, b], &[], (None, None))
        .unwrap();
    let got = rows(
        &engine,
        "SELECT who, CAST(list(amount ORDER BY block_number DESC) AS VARCHAR) AS l, \
         CAST(list(amount ORDER BY block_number) FILTER (WHERE amount <> '0') AS VARCHAR) AS f \
         FROM t GROUP BY who ORDER BY who",
    );
    assert_eq!(
        got,
        vec![
            json!({"who": "x", "l": "[3, 1, 4]", "f": "[4, 1, 3]"}),
            json!({"who": "y", "l": "[8, 7]", "f": "[7, 8]"}),
            json!({"who": "z", "l": "[0]", "f": null}),
        ]
    );
}

#[test]
fn ordered_list_when_partial_aggregation_is_skipped() {
    let tmp = tempfile::tempdir().unwrap();
    // Pairs for the first 30,000 rows, singles after: 285,000 groups, enough to skip.
    let who: Vec<String> = (0..300_000u64)
        .map(|i| format!("0x{:040x}", if i < 30_000 { i / 2 } else { i }))
        .collect();
    let data: Vec<(u64, &str, &str)> = who
        .iter()
        .enumerate()
        .map(|(i, w)| {
            (
                300_000 - i as u64,
                w.as_str(),
                if i % 2 == 0 { "a" } else { "b" },
            )
        })
        .collect();
    let seg = segment(tmp.path(), 1, &data);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let got = rows(
        &engine,
        "SELECT CAST(l AS VARCHAR) AS l, count(*) AS n FROM \
         (SELECT who, list(amount ORDER BY block_number) AS l FROM t GROUP BY who) GROUP BY 1 \
         ORDER BY 1",
    );
    assert_eq!(
        got,
        vec![
            json!({"l": "[a]", "n": 135000}),
            json!({"l": "[b, a]", "n": 15000}),
            json!({"l": "[b]", "n": 135000}),
        ]
    );
}

#[test]
fn last_value_ignore_nulls_carries_the_last_non_null() {
    let tmp = tempfile::tempdir().unwrap();
    let seg = segment(
        tmp.path(),
        1,
        &[
            (1, "a", "0"),
            (2, "a", "5"),
            (3, "a", "0"),
            (4, "a", "7"),
            (5, "b", "0"),
            (6, "b", "0"),
        ],
    );
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let got = rows(
        &engine,
        "SELECT who, block_number, last_value(CASE WHEN amount <> '0' THEN amount END IGNORE NULLS) \
         OVER (PARTITION BY who ORDER BY block_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS l \
         FROM t ORDER BY who, block_number",
    );
    assert_eq!(
        got,
        vec![
            json!({"who": "a", "block_number": 1, "l": null}),
            json!({"who": "a", "block_number": 2, "l": "5"}),
            json!({"who": "a", "block_number": 3, "l": "5"}),
            json!({"who": "a", "block_number": 4, "l": "7"}),
            json!({"who": "b", "block_number": 5, "l": null}),
            json!({"who": "b", "block_number": 6, "l": null}),
        ]
    );
    // An exact type goes through the checked rule, which refuses aggregates it does not know.
    let got = rows(
        &engine,
        "SELECT last_value(CASE WHEN block_number % 2 = 0 THEN block_number END IGNORE NULLS) \
         OVER (ORDER BY block_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS l \
         FROM t ORDER BY block_number",
    );
    let got: Vec<_> = got.iter().map(|r| r["l"].clone()).collect();
    assert_eq!(
        got,
        vec![
            json!(null),
            json!(2),
            json!(2),
            json!(4),
            json!(4),
            json!(6)
        ]
    );
}

#[test]
fn a_sort_over_budget_spills_into_the_given_directory_only() {
    let tmp = tempfile::tempdir().unwrap();
    let who: Vec<String> = (0..1_000_000u64)
        .map(|i| format!("0x{:040x}", (i * 7919) % 1_000_000))
        .collect();
    let data: Vec<(u64, &str, &str)> = who.iter().map(|w| (1, w.as_str(), "1")).collect();
    let seg = segment(tmp.path(), 1, &data);
    let sql = "SELECT who FROM t ORDER BY who";
    let budget = |spill| burrmill::Budget {
        memory_bytes: 64 << 20,
        threads: 2,
        spill,
    };

    let mut refused = Engine::open_empty_budgeted(budget(None)).unwrap();
    refused
        .register_facts("t", &declared(), vec![seg.clone()], &[], (None, None))
        .unwrap();
    assert!(refused.sql(sql).is_err());

    let spill = tempfile::tempdir().unwrap();
    let mut spilling =
        Engine::open_empty_budgeted(budget(Some((spill.path().to_path_buf(), 1 << 30)))).unwrap();
    spilling
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let mut seen = 0usize;
    let mut last = String::new();
    let mut wrote_there = false;
    spilling
        .sql_for_each(sql, |b| {
            wrote_there |= std::fs::read_dir(spill.path()).unwrap().next().is_some();
            for r in burrmill::df::encode::rows(&b).unwrap() {
                let w = r["who"].as_str().unwrap().to_string();
                assert!(w >= last);
                last = w;
                seen += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, 1_000_000);
    assert!(wrote_there, "nothing was spilled to the directory");
}

#[test]
fn a_host_text_function_propagates_null_refuses_on_error_and_stays_behind_a_case_guard() {
    let tmp = tempfile::tempdir().unwrap();
    let seg = segment(tmp.path(), 1, &[(1, "0x", "7"), (2, "0x2a", "5")]);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let hex: burrmill::df::TextFunction = std::sync::Arc::new(|v: &[&str]| {
        let h = v[0]
            .strip_prefix("0x")
            .filter(|h| !h.is_empty())
            .ok_or("empty word")?;
        u64::from_str_radix(h, 16)
            .map(|n| n.to_string())
            .map_err(|e| e.to_string())
    });
    let cat: burrmill::df::TextFunction = std::sync::Arc::new(|v: &[&str]| Ok(v.join("|")));
    engine.register_text_function("t_hex", 1, hex);
    engine.register_text_function("t_cat", 3, cat);

    assert!(engine.sql("SELECT t_hex(who) FROM t").is_err());
    assert_eq!(
        rows(
            &engine,
            "SELECT CASE WHEN who = '0x' THEN '-' ELSE t_hex(who) END AS v, \
             t_cat(who, amount, 'k') AS c, t_hex(NULL) AS n FROM t ORDER BY block_number"
        ),
        vec![
            json!({"v": "-", "c": "0x|7|k", "n": null}),
            json!({"v": "42", "c": "0x2a|5|k", "n": null}),
        ]
    );
}

#[test]
fn parquet_scans_count_reads_of_segments_and_refuse_what_rescans() {
    let tmp = tempfile::tempdir().unwrap();
    let a = segment(tmp.path(), 1, &[(1, "x", "1"), (2, "y", "2")]);
    let b = segment(tmp.path(), 2, &[(3, "x", "3")]);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![a, b], &[], (None, None))
        .unwrap();
    engine
        .register_rows("labels", &[json!({"who": "x", "name": "ex"})])
        .unwrap();
    let n = |sql: &str| engine.parquet_scans(sql);
    assert_eq!(n("SELECT who FROM t").unwrap(), 1);
    assert_eq!(
        n("SELECT count(*) FROM t a JOIN t b ON a.who = b.who").unwrap(),
        2
    );
    assert_eq!(n("SELECT name FROM labels").unwrap(), 0);
    assert_eq!(
        n("SELECT who FROM t ORDER BY block_number LIMIT 1").unwrap(),
        1
    );
    assert_eq!(
        n("SELECT who, name FROM t JOIN labels USING (who)").unwrap(),
        1
    );
    assert!(n("SELECT count(*) FROM t a JOIN t b ON a.block_number < b.block_number").is_err());
    assert!(
        n("WITH RECURSIVE r AS (SELECT 1 AS k UNION ALL SELECT k + 1 FROM r WHERE k < 3) SELECT * FROM r")
            .is_err()
    );
}

#[test]
fn host_tables_hold_results_roll_back_and_round_trip_through_parquet() {
    let tmp = tempfile::tempdir().unwrap();
    let seg = segment(
        tmp.path(),
        1,
        &[(1, "x", "1"), (2, "y", "2"), (3, "x", "3")],
    );
    let mut e = Engine::open_empty().unwrap();
    e.register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let count = |e: &Engine, rel: &str| {
        rows(e, &format!("SELECT count(*) AS n FROM {rel}"))[0]["n"].clone()
    };

    assert_eq!(
        e.create_table_as("f", "SELECT who, count(*) AS n FROM t GROUP BY who")
            .unwrap(),
        2
    );
    e.begin().unwrap();
    e.create_table_as("f", "SELECT * FROM f WHERE false")
        .unwrap();
    e.create_table_as("g", "SELECT 1 AS k").unwrap();
    assert_eq!(count(&e, "f"), json!(0));
    e.rollback().unwrap();
    assert_eq!(count(&e, "f"), json!(2));
    assert!(e.sql("SELECT * FROM g").is_err());
    e.begin().unwrap();
    e.create_table_as("f", "SELECT who, n + 1 AS n FROM f")
        .unwrap();
    e.commit().unwrap();
    assert!(e.commit().is_err());
    assert_eq!(rows(&e, "SELECT sum(n) AS s FROM f")[0]["s"], json!("5"));

    let path = tmp.path().join("ck.parquet");
    assert_eq!(
        e.write_parquet("SELECT * FROM f ORDER BY ALL", &path)
            .unwrap(),
        2
    );
    assert_eq!(e.load_parquet("ck", &path).unwrap(), 2);
    assert_eq!(
        rows(&e, "SELECT * FROM ck ORDER BY who"),
        rows(&e, "SELECT * FROM f ORDER BY who")
    );
    assert!(e.drop_relation("ck"));
    assert!(!e.drop_relation("ck"));

    assert_eq!(
        e.describe(
            "SELECT CAST(NULL AS UBIGINT) AS a, CAST(NULL AS INT) AS b, CAST(NULL AS VARCHAR) AS c, \
             CAST(NULL AS DECIMAL(20,2)) AS d, CAST(NULL AS BOOLEAN) AS e, CAST(NULL AS BIGINT) AS f \
             WHERE false"
        )
        .unwrap()
        .into_iter()
        .map(|(_, t)| t)
        .collect::<Vec<_>>(),
        vec!["UBIGINT", "INTEGER", "VARCHAR", "DECIMAL(20,2)", "BOOLEAN", "BIGINT"]
    );

    let ipc = e.sql_ipc("SELECT * FROM f ORDER BY who").unwrap();
    let back: Vec<_> = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(back.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[test]
fn a_repeated_name_in_any_branch_of_a_union_is_answered_as_duckdb_does() {
    let tmp = tempfile::tempdir().unwrap();
    let seg = segment(tmp.path(), 1, &[(1, "x", "5"), (2, "y", "7")]);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let names = |sql: &str| -> Vec<String> {
        engine.sql(sql).unwrap()[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    };
    assert_eq!(
        names(
            "SELECT who, block_number, block_number FROM t UNION ALL SELECT who, block_number, block_number FROM t"
        ),
        vec!["who", "block_number", "block_number"]
    );
    assert_eq!(
        names("SELECT who AS a, amount AS b FROM t UNION ALL SELECT who, who FROM t"),
        vec!["a", "b"]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT count(*) AS n, count(DISTINCT b) AS d FROM \
             (SELECT who AS a, amount AS b FROM t UNION ALL SELECT who, who FROM t) s"
        ),
        vec![json!({"n": 4, "d": 4})]
    );
}

/// A host's engine API is synchronous and callable from anywhere, as DuckDB's is: from a thread
/// already driving the host's own runtime, the engine answers, counts scans and is dropped.
#[test]
fn the_engine_answers_and_drops_inside_a_hosts_runtime() {
    let host = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    host.block_on(async {
        let engine = Engine::open_empty().unwrap();
        assert_eq!(rows(&engine, "SELECT 1 AS one"), vec![json!({"one": 1})]);
        assert_eq!(engine.parquet_scans("SELECT 1 AS one").unwrap(), 0);
        drop(engine);
    });
}

/// A historical window cannot place a row with no block number, so, as on DuckDB, it refuses the
/// statement rather than leaving the row out of the answer. Such rows are sealed, from before
/// segments were stamped.
#[test]
fn a_historical_window_refuses_an_unstamped_row() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, true),
        Field::new("who", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]));
    let write = |name: &str, stamps: Vec<Option<u64>>| {
        let n = stamps.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(stamps)),
                Arc::new(StringArray::from(vec!["0xa"; n])),
                Arc::new(StringArray::from(vec!["5"; n])),
            ],
        )
        .unwrap();
        let path = tmp.path().join(name);
        let mut w = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema.clone(),
            None,
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let len = std::fs::metadata(&path).unwrap().len();
        (path, len)
    };
    let mut engine = Engine::open_empty().unwrap();
    let unstamped = write("unstamped.parquet", vec![None, Some(12)]);
    engine
        .register_facts("t", &declared(), vec![unstamped], &[], (Some(10), Some(20)))
        .unwrap();
    let err = engine
        .sql("SELECT count(*) AS n FROM t")
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(err.contains("unstamped archived row"), "{err}");

    let stamped = write("stamped.parquet", vec![Some(5), Some(12)]);
    engine
        .register_facts("t", &declared(), vec![stamped], &[], (Some(10), Some(20)))
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT count(*) AS n FROM t"),
        vec![json!({"n": 1})]
    );
}

/// Every segment's footer is read when the table is defined, as DuckDB's `read_parquet` binds them
/// all: a file that is not Parquet refuses the definition, naming itself, so a host can drop it
/// and define the table from what remains, not find out when a query reads it.
#[test]
fn a_segment_that_will_not_bind_refuses_the_definition() {
    let tmp = tempfile::tempdir().unwrap();
    let good = segment(tmp.path(), 1, &[(1, "0xa", "5")]);
    let bad = tmp.path().join("t-bad.parquet");
    std::fs::write(&bad, b"not parquet, not even close").unwrap();
    let bad_len = std::fs::metadata(&bad).unwrap().len();
    let mut engine = Engine::open_empty().unwrap();
    let err = engine
        .register_facts(
            "t",
            &declared(),
            vec![good.clone(), (bad.clone(), bad_len)],
            &[],
            (None, None),
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("t-bad.parquet"), "{err}");
    engine
        .register_facts("t", &declared(), vec![good], &[], (None, None))
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT count(*) AS n FROM t"),
        vec![json!({"n": 1})]
    );
}

/// A hot row without a counter has NULL there, as DuckDB's `read_json` reads it, not 0: a missing
/// block number is not block zero.
#[test]
fn a_hot_row_missing_a_counter_reads_null() {
    let mut engine = Engine::open_empty().unwrap();
    let hot = [
        json!({"who": "0xa", "amount": "5"}),
        json!({"block_number": 12, "who": "0xb", "amount": "7"}),
    ];
    engine
        .register_facts("t", &declared(), Vec::new(), &hot, (None, None))
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT who, block_number FROM t ORDER BY who"),
        vec![
            json!({"who": "0xa", "block_number": null}),
            json!({"who": "0xb", "block_number": 12})
        ]
    );
    assert_eq!(
        rows(&engine, "SELECT min(block_number) AS m FROM t"),
        vec![json!({"m": 12})]
    );
}

/// Segments that drifted, one carrying a column the other lacks, answer the same whichever comes
/// first: the table's columns are every segment's, not the first one's.
#[test]
fn a_column_only_a_later_segment_carries_is_read_in_either_order() {
    let tmp = tempfile::tempdir().unwrap();
    let old = segment(tmp.path(), 1, &[(1, "0xa", "5"), (2, "0xb", "7")]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("memo", DataType::Utf8, true),
        Field::new("amount", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![3u64, 4])),
            Arc::new(StringArray::from(vec!["0xc", "0xd"])),
            Arc::new(StringArray::from(vec![Some("paid"), None])),
            Arc::new(StringArray::from(vec!["1", "2"])),
        ],
    )
    .unwrap();
    let path = tmp.path().join("t-new.parquet");
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None)
            .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let new = (path.clone(), std::fs::metadata(&path).unwrap().len());
    for files in [
        vec![old.clone(), new.clone()],
        vec![new.clone(), old.clone()],
    ] {
        let mut engine = Engine::open_empty().unwrap();
        engine
            .register_facts("t", &declared(), files, &[], (None, None))
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT count(*) AS n, count(memo) AS m FROM t"),
            vec![json!({"n": 4, "m": 1})]
        );
    }
}

/// A view replaces a view but not a host's table, as DuckDB's catalogue has it: nuthatch degrades
/// the one view on that refusal, where a silent replacement would lose the table.
#[test]
fn a_view_does_not_replace_a_table() {
    let mut e = Engine::open_empty().unwrap();
    e.create_table_as("t", "SELECT 1 AS x").unwrap();
    let refused = e
        .register_view("t", "SELECT 2 AS y")
        .expect_err("a table holds the name");
    assert!(
        matches!(refused, burrmill::BurrmillError::Plan(_)),
        "{refused:?}"
    );
    assert!(
        refused
            .to_string()
            .contains("is of type Table, trying to replace with type View"),
        "{refused}"
    );
    assert!(
        e.register_facts(
            "t",
            &[("block_number".into(), "BIGINT".into())],
            vec![],
            &[],
            (None, None)
        )
        .is_err()
    );
    assert_eq!(rows(&e, "SELECT x FROM t"), vec![json!({"x": 1})]);

    e.register_view("v", "SELECT 1 AS a").unwrap();
    e.register_view("v", "SELECT 2 AS a").unwrap();
    assert_eq!(rows(&e, "SELECT a FROM v"), vec![json!({"a": 2})]);
    assert!(e.drop_relation("t"));
    e.register_view("t", "SELECT 3 AS z").unwrap();
    assert_eq!(rows(&e, "SELECT z FROM t"), vec![json!({"z": 3})]);
}

/// #10: a statement cannot read `__raw`, `__hot` or `__union`. The public view keeps the window.
#[test]
fn a_statement_cannot_name_the_hidden_parts_of_a_registration() {
    let mut engine = Engine::open_empty().unwrap();
    let hot = vec![
        json!({"block_number": 10, "who": "a", "amount": "1"}),
        json!({"block_number": 30, "who": "b", "amount": "2"}),
    ];
    engine
        .register_facts("t", &declared(), Vec::new(), &hot, (None, Some(10)))
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT max(block_number) AS m FROM t"),
        vec![json!({"m": 10})]
    );
    let leaked = engine.sql("SELECT max(block_number) AS m FROM t__union");
    assert!(
        leaked.is_err(),
        "t__union answered past the window: {leaked:?}"
    );
    assert!(engine.sql("SELECT * FROM t__hot").is_err());
    assert!(engine.sql("SELECT * FROM t__raw").is_err());
    assert!(engine.sql("SELECT * FROM \"T__UNION\"").is_err());
    assert!(burrmill::inspect::reach("SELECT * FROM t__union").is_err());

    // A later registration of another table does not rebind `t`, so the old tip must stay unread.
    engine
        .register_facts("u", &declared(), Vec::new(), &[], (None, None))
        .unwrap();
    assert!(
        engine
            .sql("SELECT max(block_number) AS m FROM t__hot")
            .is_err()
    );
    let hidden = |n: &str| {
        let l = n.to_ascii_lowercase();
        l.ends_with("__raw") || l.ends_with("__hot") || l.ends_with("__union")
    };
    assert!(
        !engine.tables().iter().any(|n| hidden(n)),
        "hidden parts stayed registered: {:?}",
        engine.tables()
    );

    // A hidden name registered on its own, not via `register_facts`, is still refused.
    engine
        .register_rows("e__raw", &[json!({"a": "1"})])
        .unwrap();
    assert!(engine.sql("SELECT a FROM e__raw").is_err());
}

/// #13, #24, #33: a statement refused bare is refused the same way under any EXPLAIN, before it plans,
/// and `EXPLAIN ANALYZE` cannot write to a host's in-memory tables.
#[test]
fn explain_does_not_admit_what_it_wraps() {
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_rows("labels", &[json!({"a": "0x1", "l": "alice"})])
        .unwrap();
    let count = |e: &Engine| rows(e, "SELECT count(*) AS n FROM labels");
    for (bare, kind) in [
        ("INSERT INTO labels VALUES ('0xz', 'mallory')", "INSERT"),
        ("DELETE FROM labels", "DELETE"),
        ("COPY (SELECT 1) TO '/tmp/out.csv'", "COPY"),
        (
            "CREATE EXTERNAL TABLE pw STORED AS CSV LOCATION '/etc/passwd'",
            "CREATE",
        ),
        ("SELECT 100000000000000000000 AS n", "wider than 64 bits"),
    ] {
        for wrap in [
            "",
            "EXPLAIN ANALYZE ",
            "EXPLAIN ANALYZE VERBOSE ",
            "EXPLAIN ",
            "EXPLAIN VERBOSE ",
        ] {
            let sql = format!("{wrap}{bare}");
            let got = engine.sql(&sql);
            assert_eq!(
                count(&engine),
                vec![json!({"n": 1})],
                "{sql} changed labels"
            );
            match got {
                Err(burrmill::BurrmillError::NotAllowed(m)) => {
                    assert!(m.contains(kind), "{sql}: {m}")
                }
                other => panic!("{sql}: expected a refusal naming {kind}, got {other:?}"),
            }
        }
    }
    assert!(engine.sql("EXPLAIN SELECT * FROM labels").is_ok());
}

/// A budgeted hash join keeps every build-side batch, and the repartition under it handed each
/// 128-row batch of string views a 1 MiB buffer: 34 MB of addresses refused at 512 MiB, charged
/// 1 MiB a batch. The same join over `Utf8`, or without a budget, answered.
#[test]
fn a_budgeted_hash_join_over_text_is_charged_for_its_rows_not_their_buffers() {
    let tmp = tempfile::tempdir().unwrap();
    let who: Vec<String> = (0..400_000u64).map(|i| format!("0x{i:040x}")).collect();
    let data: Vec<(u64, &str, &str)> = who
        .iter()
        .enumerate()
        .map(|(i, w)| (i as u64, w.as_str(), w.as_str()))
        .collect();
    let segs: Vec<_> = data
        .chunks(25_000)
        .enumerate()
        .map(|(n, c)| segment(tmp.path(), n as u64, c))
        .collect();
    let engine = |budget: Option<usize>| {
        let mut e = match budget {
            Some(bytes) => Engine::open_empty_budgeted(burrmill::Budget {
                memory_bytes: bytes,
                threads: 8,
                spill: None,
            }),
            None => Engine::open_empty(),
        }
        .unwrap();
        e.register_facts("t", &declared(), segs.clone(), &[], (None, None))
            .unwrap();
        e
    };
    let views = "SELECT count(*) AS n, max(b.amount) AS m FROM t a JOIN t b ON a.who = b.who";
    let text = "WITH s AS (SELECT arrow_cast(who, 'Utf8') AS who, arrow_cast(amount, 'Utf8') AS amount FROM t) \
                SELECT count(*) AS n, max(b.amount) AS m FROM s a JOIN s b ON a.who = b.who";
    let want = vec![json!({"n": 400_000, "m": who.last().unwrap()})];

    assert_eq!(rows(&engine(None), views), want);
    assert_eq!(rows(&engine(Some(512 << 20)), text), want);
    match engine(Some(512 << 20)).sql(views) {
        Ok(b) => assert_eq!(
            b.iter()
                .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
                .collect::<Vec<_>>(),
            want
        ),
        Err(e) => panic!("the budgeted join over views refused: {e}"),
    }
}

/// #21: more than one statement is refused by name; none of them runs, the first included.
#[test]
fn several_statements_are_refused_not_run_in_part() {
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_rows("labels", &[json!({"a": "0x1", "l": "alice"})])
        .unwrap();
    for sql in [
        "SELECT 1 AS a; SELECT 2 AS b",
        "SELECT a FROM labels; DELETE FROM labels",
        "EXPLAIN SELECT 1; SELECT 2",
    ] {
        match engine.sql(sql) {
            Err(burrmill::BurrmillError::NotAllowed(m)) => {
                assert!(m.contains("statements where one is allowed"), "{sql}: {m}")
            }
            other => panic!("{sql}: expected a refusal of several statements, got {other:?}"),
        }
    }
    assert_eq!(rows(&engine, "SELECT 1 AS a;"), vec![json!({"a": 1})]);
}

/// One column at two integer widths, as an offchain snapshot whose feed widened between pulls.
fn priced(dir: &std::path::Path, n: u64, block: u64, price: ArrayRef) -> (std::path::PathBuf, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("price", price.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![block; price.len()])), price],
    )
    .unwrap();
    let path = dir.join(format!("t-{n:064x}.parquet"));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

/// #36: segments carrying one column at two integer widths bind at the narrowest type that holds
/// both, as DuckDB's `union_by_name` does, and each file's rows are read at it.
#[test]
fn integer_widths_that_differ_between_segments_widen() {
    use arrow::array::{Int32Array, Int64Array, UInt32Array};
    let declared = vec![
        ("block_number".to_string(), "u64".to_string()),
        ("price".to_string(), "string".to_string()),
    ];
    let one = |a: ArrayRef, b: ArrayRef| {
        let tmp = tempfile::tempdir().unwrap();
        let files = vec![priced(tmp.path(), 1, 1, a), priced(tmp.path(), 2, 2, b)];
        let mut engine = Engine::open_empty().unwrap();
        engine
            .register_facts("t", &declared, files, &[], (None, None))
            .unwrap_or_else(|e| panic!("{e}"));
        rows(
            &engine,
            "SELECT block_number, price, typeof(price) AS ty FROM t ORDER BY block_number, price",
        )
    };
    assert_eq!(
        one(
            Arc::new(Int32Array::from(vec![-7, i32::MAX])),
            Arc::new(Int64Array::from(vec![i64::MIN]))
        ),
        vec![
            json!({"block_number": 1, "price": -7, "ty": "BIGINT"}),
            json!({"block_number": 1, "price": i32::MAX, "ty": "BIGINT"}),
            json!({"block_number": 2, "price": i64::MIN, "ty": "BIGINT"}),
        ]
    );
    assert_eq!(
        one(
            Arc::new(UInt32Array::from(vec![u32::MAX])),
            Arc::new(Int64Array::from(vec![-1]))
        ),
        vec![
            json!({"block_number": 1, "price": u32::MAX, "ty": "BIGINT"}),
            json!({"block_number": 2, "price": -1, "ty": "BIGINT"}),
        ]
    );
    assert_eq!(
        one(
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            Arc::new(Int64Array::from(vec![i64::MIN]))
        ),
        vec![
            json!({"block_number": 1, "price": u64::MAX.to_string(), "ty": "DECIMAL(38,0)"}),
            json!({"block_number": 2, "price": i64::MIN.to_string(), "ty": "DECIMAL(38,0)"}),
        ]
    );
}

/// A repartition hands every 128-row batch of string views a 1 MiB coalescing buffer, and the sort
/// above it holds each one: 34 MB of addresses refused at 1 GiB. The same sort over `Utf8`, or
/// without a budget, answered.
#[test]
fn a_budgeted_sort_over_repartitioned_text_is_charged_for_its_rows_not_their_buffers() {
    let tmp = tempfile::tempdir().unwrap();
    let who: Vec<String> = (0..400_000u64).map(|i| format!("0x{i:040x}")).collect();
    let data: Vec<(u64, &str, &str)> = who
        .iter()
        .enumerate()
        .map(|(i, w)| (i as u64, w.as_str(), w.as_str()))
        .collect();
    let segs: Vec<_> = data
        .chunks(25_000)
        .enumerate()
        .map(|(n, c)| segment(tmp.path(), n as u64, c))
        .collect();
    let engine = |budget: Option<usize>| {
        let mut e = match budget {
            Some(bytes) => Engine::open_empty_budgeted(burrmill::Budget {
                memory_bytes: bytes,
                threads: 8,
                spill: None,
            }),
            None => Engine::open_empty(),
        }
        .unwrap();
        e.register_facts("t", &declared(), segs.clone(), &[], (None, None))
            .unwrap();
        e
    };
    let over = |from: &str| {
        format!(
            "SELECT count(*) AS n, max(r) AS m FROM (SELECT row_number() OVER \
             (PARTITION BY who ORDER BY block_number) AS r FROM {from})"
        )
    };
    let views = over("t");
    let text = over("(SELECT arrow_cast(who, 'Utf8') AS who, block_number FROM t)");
    let want = vec![json!({"n": 400_000, "m": 1})];

    assert_eq!(rows(&engine(None), &views), want);
    assert_eq!(rows(&engine(Some(1 << 30)), &text), want);
    match engine(Some(1 << 30)).sql(&views) {
        Ok(b) => assert_eq!(
            b.iter()
                .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
                .collect::<Vec<_>>(),
            want
        ),
        Err(e) => panic!("the budgeted sort over views refused: {e}"),
    }
}
