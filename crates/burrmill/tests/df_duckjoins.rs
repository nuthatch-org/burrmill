//! Answers from DuckDB's `test/sql/join` that Burrmill gave wrong (#423), each as DuckDB gives it.

use burrmill::Engine;
use serde_json::{Value, json};

fn rows(e: &Engine, sql: &str) -> Vec<Value> {
    e.sql(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

/// A NULL inside a struct or list orders after every value, as in DuckDB; Arrow's comparator puts
/// it first (test/sql/join/inner/test_join_is_distinct.test:271, test_nested_inequality.test:198).
#[test]
fn a_null_inside_a_struct_or_list_orders_last() {
    let e = Engine::open_empty().unwrap();
    let got = rows(
        &e,
        "SELECT {'x': NULL::INTEGER} > {'x': 0} AS a, {'x': 1} < {'x': NULL::INTEGER} AS b, \
         [1, NULL] > [1, 2] AS c, [1, NULL] <= [1, 2] AS d, \
         {'x': 5} BETWEEN {'x': 1} AND {'x': NULL::INTEGER} AS e, \
         CAST(NULL AS STRUCT(x INTEGER)) < {'x': 1} AS f",
    );
    assert_eq!(
        got,
        vec![json!({"a": true, "b": true, "c": true, "d": false, "e": true, "f": null})]
    );

    // The same over columns, in a join's condition: DuckDB's six rows.
    let got = rows(
        &e,
        "WITH s(s) AS (VALUES ({'a': 1}), ({'a': 2}), (NULL), (NULL), ({'a': NULL::INTEGER})) \
         SELECT l.s.a AS l, r.s.a AS r FROM s l JOIN s r ON l.s <= r.s ORDER BY l.s, r.s",
    );
    assert_eq!(
        got,
        vec![
            json!({"l": 1, "r": 1}),
            json!({"l": 1, "r": 2}),
            json!({"l": 1, "r": null}),
            json!({"l": 2, "r": 2}),
            json!({"l": 2, "r": null}),
            json!({"l": null, "r": null}),
        ]
    );
}

/// `POSITIONAL JOIN` is refused by name; sqlparser read `POSITIONAL` as an alias and the `JOIN` as
/// a cross join, and answered 9,006 rows where DuckDB pairs 3,002
/// (test/sql/join/positional/test_positional_join.test:59).
#[test]
fn a_positional_join_is_refused_by_name() {
    let e = Engine::open_empty().unwrap();
    for sql in [
        "WITH a(x) AS (VALUES (1), (2), (3)), b(y) AS (VALUES (1), (2)) \
         SELECT count(x), count(y) FROM a POSITIONAL JOIN b",
        "WITH a(x) AS (VALUES (1)) SELECT * FROM a t1 positional join a t2",
    ] {
        let err = e.sql(sql).unwrap_err().to_string();
        assert!(
            err.contains("POSITIONAL JOIN is not supported"),
            "{sql}: {err}"
        );
    }
    // A column or a quoted alias of that name is no join.
    let got = rows(
        &e,
        "WITH a(positional) AS (VALUES (1)) SELECT b.positional FROM a \"positional\" JOIN a b ON true",
    );
    assert_eq!(got, vec![json!({"positional": 1})]);
}
