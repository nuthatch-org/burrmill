//! Correlated subqueries grouped by an empty grouping set (#422): an outer row with no match still
//! gets a row per empty set, as in DuckDB.

use burrmill::Engine;
use serde_json::{Value, json};

fn rows(sql: &str) -> Vec<Value> {
    let engine = Engine::open_empty().unwrap();
    let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}\n{e}"));
    got.iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

fn pairs(sql: &str) -> Vec<(Value, Value)> {
    rows(sql)
        .into_iter()
        .map(|r| (r["k"].clone(), r["c"].clone()))
        .collect()
}

#[test]
fn a_lateral_over_repeated_empty_sets_has_a_row_per_set_for_every_outer_row() {
    let sql = "SELECT o.k, a.c FROM (VALUES (1), (7)) AS o(k), LATERAL (SELECT count(*) AS c \
               FROM (VALUES (1), (1), (2)) AS i(k) WHERE i.k = o.k GROUP BY GROUPING SETS ((), ())) AS a \
               ORDER BY ALL";
    assert_eq!(
        pairs(sql),
        [(1, 2), (1, 2), (7, 0), (7, 0)].map(|(k, c)| (json!(k), json!(c)))
    );
    // `GROUPING SETS (())` alone is one row per outer row, also as a scalar subquery.
    let sql = "SELECT o.k, (SELECT count(*) FROM (VALUES (1)) AS i(k) WHERE i.k = o.k \
               GROUP BY GROUPING SETS (())) AS c FROM (VALUES (1), (7)) AS o(k) ORDER BY ALL";
    assert_eq!(
        pairs(sql),
        [(1, 1), (7, 0)].map(|(k, c)| (json!(k), json!(c)))
    );
}

#[test]
fn a_non_empty_set_has_rows_only_for_an_outer_row_that_matched() {
    let sql = "SELECT o.k, a.c, a.gm, a.g FROM (VALUES (1), (7)) AS o(k), LATERAL (\
               SELECT count(*) AS c, GROUPING(g) AS gm, g, sum(v) AS s FROM (VALUES (1, 10, 5), (1, 20, 6)) AS i(k, g, v) \
               WHERE i.k = o.k GROUP BY ROLLUP (g)) AS a ORDER BY ALL";
    let got: Vec<_> = rows(sql)
        .into_iter()
        .map(|r| {
            (
                r["k"].clone(),
                r["c"].clone(),
                r["gm"].clone(),
                r["g"].clone(),
            )
        })
        .collect();
    let want = [
        (json!(1), json!(1), json!(0), json!(10)),
        (json!(1), json!(1), json!(0), json!(20)),
        (json!(1), json!(2), json!(1), Value::Null),
        (json!(7), json!(0), json!(1), Value::Null),
    ];
    assert_eq!(got, want);
}

#[test]
fn a_having_sees_the_empty_inputs_aggregates() {
    // Grouped by empty sets, and not grouped at all: count(*) is 0 for 7 in both.
    for group in ["GROUP BY GROUPING SETS ((), ())", ""] {
        let sql = format!(
            "SELECT o.k, a.c FROM (VALUES (1), (7)) AS o(k), LATERAL (SELECT count(*) AS c \
             FROM (VALUES (1)) AS i(k) WHERE i.k = o.k {group} HAVING count(*) = 0) AS a ORDER BY ALL"
        );
        let n = if group.is_empty() { 1 } else { 2 };
        assert_eq!(pairs(&sql), vec![(json!(7), json!(0)); n], "{sql}");
        let sql = sql.replace("count(*) = 0", "count(*) > 0");
        assert_eq!(pairs(&sql), vec![(json!(1), json!(1)); n], "{sql}");
    }
    let sql = "SELECT o.k, a.s AS c FROM (VALUES (7)) AS o(k) LEFT JOIN LATERAL (SELECT sum(v) AS s \
               FROM (VALUES (1, 42)) AS i(k, v) WHERE i.k = o.k GROUP BY GROUPING SETS ((), ())) AS a ON true \
               ORDER BY ALL";
    assert_eq!(pairs(sql), vec![(json!(7), Value::Null); 2]);
}

#[test]
fn a_scalar_subquery_over_repeated_empty_sets_is_refused_by_name() {
    let engine = Engine::open_empty().unwrap();
    let sql = "SELECT o.k, (SELECT sum(v) FROM (VALUES (1, 42)) AS i(k, v) WHERE i.k = o.k \
               GROUP BY GROUPING SETS ((), ())) AS s FROM (VALUES (7)) AS o(k)";
    let e = engine.sql(sql).unwrap_err().to_string();
    assert!(
        e.contains("grouped by an empty grouping set") && e.contains("not supported"),
        "{e}"
    );
}
