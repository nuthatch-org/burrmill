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

/// A table of `Int32` columns, each a list of values, `None` for NULL.
fn table(e: &mut Engine, name: &str, cols: &[(&str, &[Option<i32>])]) {
    use std::sync::Arc;
    let schema = Arc::new(arrow::datatypes::Schema::new(
        cols.iter()
            .map(|(c, _)| arrow::datatypes::Field::new(*c, arrow::datatypes::DataType::Int32, true))
            .collect::<Vec<_>>(),
    ));
    let arrays = cols
        .iter()
        .map(|(_, v)| {
            Arc::new(arrow::array::Int32Array::from(v.to_vec())) as arrow::array::ArrayRef
        })
        .collect();
    let b = arrow::record_batch::RecordBatch::try_new(schema, arrays).unwrap();
    e.register_batch(name, vec![b]).unwrap();
}

/// `SELECT *` over `USING` and `NATURAL` joins is the left side's columns, then the right's but the
/// join columns, each join column from the side the join keeps; DataFusion kept the copy whose table
/// name sorts first (test/sql/join/natural/natural_join.test:46). A bare join column of a `RIGHT` or
/// `FULL` join is the right side's or both coalesced, where DataFusion read the left side's.
#[test]
fn using_and_natural_joins_answer_as_duckdb() {
    let mut e = Engine::open_empty().unwrap();
    table(
        &mut e,
        "t3",
        &[("a", &[Some(1)]), ("b", &[Some(2)]), ("c", &[Some(3)])],
    );
    table(
        &mut e,
        "t2",
        &[("a", &[Some(1), Some(2)]), ("c", &[Some(3), Some(4)])],
    );
    table(
        &mut e,
        "zz",
        &[("c", &[Some(3), Some(9)]), ("a", &[Some(1), Some(9)])],
    );
    // Columns in order: a JSON object's keys would not show it.
    let cols = |sql: &str| -> Vec<String> {
        e.sql(sql).unwrap()[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    };
    assert_eq!(cols("SELECT * FROM t3 NATURAL JOIN t2"), ["a", "b", "c"]);
    assert_eq!(
        cols("SELECT * FROM t3 JOIN t2 USING (a, c)"),
        ["a", "b", "c"]
    );
    assert_eq!(
        cols("SELECT * FROM t3 RIGHT JOIN t2 USING (a)"),
        ["a", "b", "c", "c"]
    );
    assert_eq!(
        rows(&e, "SELECT * FROM t3 NATURAL JOIN t2"),
        vec![json!({"a": 1, "b": 2, "c": 3})]
    );
    // The left side's copy, which the right side's is NULL beside.
    assert_eq!(
        rows(&e, "SELECT * FROM zz NATURAL LEFT JOIN t2 ORDER BY c"),
        vec![json!({"c": 3, "a": 1}), json!({"c": 9, "a": 9})]
    );
    assert_eq!(
        rows(&e, "SELECT a FROM t3 NATURAL RIGHT JOIN t2 ORDER BY a"),
        vec![json!({"a": 1}), json!({"a": 2})]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT a, count(*) AS n FROM t2 FULL JOIN zz USING (a) WHERE a > 1 GROUP BY a \
             ORDER BY a"
        ),
        vec![json!({"a": 2, "n": 1}), json!({"a": 9, "n": 1})]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT t2.c FROM t3 RIGHT JOIN t2 USING (a) ORDER BY a DESC"
        ),
        vec![json!({"c": 4}), json!({"c": 3})]
    );
    // Over CTEs that list their columns too.
    assert_eq!(
        rows(
            &e,
            "WITH l(a, b) AS (VALUES (1, 2)), r(b, a) AS (VALUES (2, 1)) \
             SELECT * FROM r NATURAL JOIN l"
        ),
        vec![json!({"b": 2, "a": 1})]
    );
}

/// A `NATURAL JOIN` of sides that share no column is DuckDB's binder error, where DataFusion made
/// a cross join (natural_join.test:88 and :94).
#[test]
fn a_natural_join_without_shared_columns_is_an_error() {
    let e = Engine::open_empty().unwrap();
    let err = e
        .sql("SELECT * FROM (VALUES (1, 2)) l(a, b) NATURAL JOIN (VALUES (3, 4)) r(c, d)")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Binder Error: No columns found to join on in NATURAL JOIN")
            && err.contains("Left candidates: l.a, l.b")
            && err.contains("Right candidates: r.c, r.d"),
        "{err}"
    );
}
