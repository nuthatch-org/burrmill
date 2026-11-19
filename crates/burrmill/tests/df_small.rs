//! Small statements (#348): planned in one partition, and not planned again when run again.

use burrmill::Engine;

fn answer(e: &Engine, sql: &str) -> String {
    let rows: Vec<_> = e
        .sql(sql)
        .unwrap()
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    serde_json::to_string(&rows).unwrap()
}

fn plan(e: &Engine, sql: &str) -> String {
    let batches = e.sql(&format!("EXPLAIN {sql}")).unwrap();
    arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

#[test]
fn a_small_statement_is_planned_in_one_partition() {
    let e = Engine::builder().threads(8).open().unwrap();
    let sql = "SELECT k, count(*) AS n FROM (VALUES (1), (2), (1)) t(k) GROUP BY k ORDER BY k";
    let p = plan(&e, sql);
    assert!(!p.contains("RepartitionExec"), "{p}");
    assert!(!p.contains("SortPreservingMergeExec"), "{p}");
    assert_eq!(answer(&e, sql), r#"[{"k":1,"n":2},{"k":2,"n":1}]"#);
    e.set_small_statement_bytes(0);
    let p = plan(&e, sql);
    assert!(p.contains("RepartitionExec"), "{p}");
    assert_eq!(answer(&e, sql), r#"[{"k":1,"n":2},{"k":2,"n":1}]"#);
}

#[test]
fn a_statement_run_again_sees_a_view_registered_since() {
    let mut e = Engine::open_empty().unwrap();
    e.register_view("v", "SELECT 1 AS a").unwrap();
    assert_eq!(answer(&e, "SELECT a FROM v"), r#"[{"a":1}]"#);
    assert_eq!(answer(&e, "SELECT a FROM v"), r#"[{"a":1}]"#);
    e.register_view("v", "SELECT 2 AS a").unwrap();
    assert_eq!(answer(&e, "SELECT a FROM v"), r#"[{"a":2}]"#);
}

#[test]
fn a_statement_run_again_follows_the_settings() {
    let e = Engine::open_empty().unwrap();
    let sql = "SELECT 1 / 4 AS q";
    assert_eq!(answer(&e, sql), r#"[{"q":0.25}]"#);
    e.set_strict_decimals(true);
    assert!(e.sql(sql).is_err());
    e.set_strict_decimals(false);
    assert_eq!(answer(&e, sql), r#"[{"q":0.25}]"#);
    let random = "SELECT random() AS r";
    assert!(e.sql(random).is_ok());
    e.set_deterministic(true);
    assert!(e.sql(random).is_err());
}

#[test]
fn a_shared_subquery_runs_afresh_each_time() {
    let e = Engine::open_empty().unwrap();
    let sql = "WITH g AS (SELECT k, count(*) AS n FROM (VALUES (1), (2), (1)) t(k) GROUP BY k) \
               SELECT a.k, a.n, b.n AS m FROM g a JOIN g b ON a.k = b.k ORDER BY 1";
    let want = r#"[{"k":1,"n":2,"m":2},{"k":2,"n":1,"m":1}]"#;
    let p = plan(&e, sql);
    assert!(p.contains("SharedDefsExec"), "{p}");
    for _ in 0..3 {
        assert_eq!(answer(&e, sql), want);
    }
}
