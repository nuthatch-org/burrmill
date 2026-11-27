//! Answers from DuckDB's `test/sql/aggregate` that Burrmill gave wrong (#421), each as DuckDB gives
//! it or refused by name.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use serde_json::{Value, json};

fn rows(e: &Engine, sql: &str) -> Vec<Value> {
    e.sql(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

fn refused(e: &Engine, sql: &str, says: &str) {
    match e.sql(sql) {
        Ok(b) => panic!("{sql}: answered {b:?}"),
        Err(err) => {
            let err = err.to_string();
            assert!(err.contains(says), "{sql}: {err}");
            assert!(err.contains("not supported"), "{sql}: {err}");
        }
    }
}

/// DuckDB's tables of test/sql/aggregate/aggregates/test_count_qualified_star.test:143.
fn tables() -> Engine {
    let mut e = Engine::open_empty().unwrap();
    let int = |v: &[Option<i32>]| Arc::new(Int32Array::from(v.to_vec())) as ArrayRef;
    let mut add = |name: &str, cols: Vec<(&str, ArrayRef)>| {
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|(c, a)| Field::new(*c, a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let arrays = cols.into_iter().map(|(_, a)| a).collect();
        let b = RecordBatch::try_new(schema, arrays).unwrap();
        e.register_batch(name, vec![b]).unwrap();
    };
    add("l", vec![("id", int(&[Some(1), Some(2), Some(3)]))]);
    add(
        "r",
        vec![
            ("id", int(&[Some(2), Some(3)])),
            ("y", Arc::new(StringArray::from(vec![None, Some("x")]))),
        ],
    );
    add(
        "s",
        vec![
            ("id", int(&[Some(1), Some(2)])),
            ("z", int(&[Some(10), None])),
        ],
    );
    add("n", vec![("v", int(&[None, Some(1)]))]);
    e
}

/// `COUNT(t.*)` counts the rows `t` is present in: one whose every column is NULL counts, one an
/// outer join NULL-extends `t` in does not. DataFusion read it as `COUNT(*)`
/// (test/sql/aggregate/aggregates/test_count_qualified_star.test:19 and 24 more).
#[test]
fn count_of_a_relation_star_skips_the_rows_an_outer_join_null_extends() {
    let e = tables();
    assert_eq!(
        rows(
            &e,
            "SELECT l.id, count(*) AS n, count(l.*) AS ls, count(r.*) AS rs, count(s.*) AS ss \
             FROM l LEFT JOIN r ON l.id = r.id LEFT JOIN s ON l.id = s.id GROUP BY l.id ORDER BY 1"
        ),
        vec![
            json!({"id": 1, "n": 1, "ls": 1, "rs": 0, "ss": 1}),
            json!({"id": 2, "n": 1, "ls": 1, "rs": 1, "ss": 1}),
            json!({"id": 3, "n": 1, "ls": 1, "rs": 1, "ss": 0}),
        ]
    );
    // A row whose every column is NULL is a row.
    assert_eq!(
        rows(&e, "SELECT count(n.*) AS c, count(n.v) AS v FROM n"),
        vec![json!({"c": 2, "v": 1})]
    );
    for (sql, want) in [
        // The left side of a RIGHT join, a USING join, an alias, a subquery, a self join.
        (
            "SELECT count(r.*) AS c FROM r RIGHT JOIN l ON l.id = r.id",
            2,
        ),
        ("SELECT count(r.*) AS c FROM l LEFT JOIN r USING (id)", 2),
        (
            "SELECT count(rr.*) AS c FROM l LEFT JOIN r rr ON l.id = rr.id",
            2,
        ),
        (
            "SELECT count(t.*) AS c FROM l LEFT JOIN (SELECT * FROM r) t ON l.id = t.id",
            2,
        ),
        (
            "SELECT count(r2.*) AS c FROM r r1 LEFT JOIN r r2 ON r1.y = r2.y",
            1,
        ),
        // FILTER, and the same over a window.
        (
            "SELECT count(r.*) FILTER (WHERE l.id > 2) AS c FROM l LEFT JOIN r ON l.id = r.id",
            1,
        ),
        (
            "SELECT max(c) AS c FROM (SELECT count(r.*) OVER () AS c \
             FROM l LEFT JOIN r ON l.id = r.id)",
            2,
        ),
        // A subquery that wraps the outer join is a relation of its own, not NULL-extended.
        (
            "SELECT count(t.*) AS c FROM (SELECT r.* FROM l LEFT JOIN r ON l.id = r.id) t",
            3,
        ),
        // An inner join extends nothing.
        ("SELECT count(r.*) AS c FROM l JOIN r ON true", 6),
    ] {
        assert_eq!(rows(&e, sql), vec![json!({"c": want})], "{sql}");
    }
    // In HAVING and ORDER BY.
    assert_eq!(
        rows(
            &e,
            "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id GROUP BY l.id \
             HAVING count(r.*) > 0 ORDER BY count(r.*) DESC, l.id DESC"
        ),
        vec![json!({"id": 3}), json!({"id": 2})]
    );
    // The column keeps DuckDB's name.
    let got = e
        .sql("SELECT count(r.*) FROM l LEFT JOIN r ON l.id = r.id")
        .unwrap();
    assert_eq!(got[0].schema().field(0).name(), "count(r.*)");
}

/// Where no condition says a NULL-extended relation's row is present, `COUNT(t.*)` is refused by
/// name: every side of a FULL join, whose rows may pass unmatched, and a join on `true` or on `IS
/// NOT DISTINCT FROM` (test_count_qualified_star.test:95, 113).
#[test]
fn count_of_a_relation_star_is_refused_where_presence_is_unknown() {
    let e = tables();
    for sql in [
        "SELECT count(r.*) FROM l FULL JOIN r ON l.id = r.id",
        "SELECT count(l.*) FROM l FULL OUTER JOIN r ON l.id = r.id",
        "SELECT count(r.*) FROM l LEFT JOIN r ON true",
        "SELECT count(n2.*) FROM n n1 LEFT JOIN n n2 ON n1.v IS NOT DISTINCT FROM n2.v",
        "SELECT count(DISTINCT r.*) FROM r",
    ] {
        refused(&e, sql, "COUNT(");
    }
}

/// `f(x) EXPORT_STATE` is DuckDB's aggregate state; sqlparser read the keyword as an alias and the
/// aggregate's value was answered (test/sql/aggregate/aggregate_state_export/
/// ordered_aggregate_export_state.test:48, aggregate_state_union.test:14).
#[test]
fn export_state_is_refused_by_name() {
    let e = Engine::open_empty().unwrap();
    for sql in [
        "SELECT list(i ORDER BY i DESC) EXPORT_STATE FROM (VALUES (1), (3), (2)) t(i)",
        "SELECT typeof(s) FROM (SELECT sum(1) AS s UNION ALL SELECT sum(1) export_state) t",
        "SELECT corr(i, i + 1) EXPORT_STATE FROM (VALUES (1), (2)) t(i)",
    ] {
        refused(&e, sql, "EXPORT_STATE");
    }
    // Over DISTINCT it is DuckDB's binder error (test_export_state_distinct.test:26).
    match e.sql("SELECT count(DISTINCT i) EXPORT_STATE FROM (VALUES (1)) t(i)") {
        Ok(b) => panic!("answered {b:?}"),
        Err(err) => assert!(
            err.to_string()
                .contains("EXPORT_STATE is not supported for DISTINCT aggregates"),
            "{err}"
        ),
    }
    // Where DuckDB's state is the value, the value is the answer (count_export_state.test:5).
    assert_eq!(
        rows(
            &e,
            "SELECT count(*) EXPORT_STATE, min(i) EXPORT_STATE, bool_or(i > 1) EXPORT_STATE \
             FROM (VALUES (1), (2)) t(i)"
        )
        .len(),
        1
    );
    // A quoted alias of the name is a name.
    assert_eq!(
        rows(&e, "SELECT count(*) AS \"export_state\""),
        vec![json!({"export_state": 1})]
    );
}

/// `regr_slope` and `regr_r2` as DuckDB answers them: a slope over no spread in `x` is NaN, an R²
/// over no spread in `y` is 1, and an R² whose `y` spread overflows is 0; DataFusion's were NULL,
/// NULL and NaN (test/sql/aggregate/aggregates/test_regression.test:69, 93, 98, 108).
#[test]
fn regression_slope_and_r2_answer_as_duckdb() {
    let e = Engine::open_empty().unwrap();
    let one = |sql: &str| -> Value {
        let got = rows(&e, &format!("SELECT {sql} AS v"));
        got[0]["v"].clone()
    };
    let float = |sql: &str| -> Option<f64> {
        let v = one(sql);
        match v {
            Value::Null => None,
            Value::Number(n) => n.as_f64(),
            Value::String(s) => Some(s.parse().unwrap_or_else(|_| panic!("{sql}: {s}"))),
            v => panic!("{sql}: {v}"),
        }
    };
    assert_eq!(one("isnan(regr_slope(1, 1))"), json!(true));
    assert_eq!(float("regr_slope(NULL, NULL)"), None);
    assert_eq!(float("regr_r2(NULL, NULL)"), None);
    assert_eq!(float("regr_r2(1, 1)"), None);
    let range = |agg: &str| float(&format!("(SELECT {agg} FROM range(5) t(i))"));
    assert_eq!(range("regr_r2(0, i)"), Some(1.0));
    assert_eq!(range("regr_r2(0, 1e230 * i)"), Some(1.0));
    assert_eq!(range("regr_r2(1e230 * i, i)"), Some(0.0));
    assert_eq!(range("regr_r2(1e230 * i, 0)"), None);
    // Where both have spread, the answers are as they were.
    assert_eq!(range("regr_slope(2 * i + 1, i)"), Some(2.0));
    assert!((range("regr_r2(2 * i + 1, i)").unwrap() - 1.0).abs() < 1e-12);
    // Over groups, a pair with a NULL is no pair.
    assert_eq!(
        rows(
            &e,
            "SELECT g, regr_slope(y, x) IS NULL AS null_slope, isnan(regr_slope(y, x)) AS nan_slope \
             FROM (VALUES (1, 1.0, 1.0), (1, 2.0, NULL), (2, NULL, 3.0)) t(g, y, x) \
             GROUP BY g ORDER BY g"
        ),
        vec![
            json!({"g": 1, "null_slope": false, "nan_slope": true}),
            json!({"g": 2, "null_slope": true, "nan_slope": null}),
        ]
    );
}

/// DuckDB's binder errors where DataFusion answered: `count` of several arguments
/// (test/sql/aggregate/aggregates/test_incorrect_aggregate.test:5, test_aggregate_types_scalar.test:15),
/// `COUNT(DISTINCT *)` (test_count.test:49), and a `DISTINCT ON` key that is a literal but no
/// position of the select list (test/sql/aggregate/distinct/test_distinct_on.test:127, 133).
#[test]
fn duckdb_binder_errors_are_errors() {
    let e = Engine::open_empty().unwrap();
    for (sql, says) in [
        ("SELECT COUNT(1, 2)", "No function matches"),
        ("SELECT COUNT(1, 2, 3)", "No function matches"),
        (
            "SELECT COUNT(DISTINCT *) FROM (VALUES (1)) t(i)",
            "STAR expression is only allowed as the root element",
        ),
        (
            "SELECT DISTINCT ON (2) i FROM (VALUES (1), (2)) t(i)",
            "ORDER term out of range - should be between 1 and 1",
        ),
        (
            "SELECT DISTINCT ON (i, 'literal') i FROM (VALUES (1), (2)) t(i)",
            "DISTINCT ON non-integer literal has no effect",
        ),
    ] {
        match e.sql(sql) {
            Ok(b) => panic!("{sql}: answered {b:?}"),
            Err(err) => assert!(err.to_string().contains(says), "{sql}: {err}"),
        }
    }
    // A position is the select item there.
    assert_eq!(
        rows(
            &e,
            "SELECT DISTINCT ON (2) i % 2 AS a, i % 3 AS b FROM range(6) t(i) ORDER BY 2, 1"
        ),
        vec![
            json!({"a": 0, "b": 0}),
            json!({"a": 0, "b": 1}),
            json!({"a": 0, "b": 2}),
        ]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT count(1) AS a, count(*) AS b, count(DISTINCT 1) AS c"
        ),
        vec![json!({"a": 1, "b": 1, "c": 1})]
    );
}

/// `string_agg(x, NULL)` is NULL in DuckDB; DataFusion joined the values with no separator
/// (test/sql/aggregate/aggregates/test_aggregate_types_scalar.test:132).
#[test]
fn string_agg_with_a_null_separator_is_null() {
    let e = Engine::open_empty().unwrap();
    assert_eq!(
        rows(
            &e,
            "SELECT string_agg('hello', ' ') AS a, string_agg('hello', NULL) AS b, \
             string_agg(NULL, ' ') AS c, string_agg('', '') AS d"
        ),
        vec![json!({"a": "hello", "b": null, "c": null, "d": ""})]
    );
    // Two such items, unnamed, keep names of their own.
    let got = e
        .sql("SELECT string_agg('hello', NULL), string_agg(NULL, NULL)")
        .unwrap();
    let names: Vec<String> = got[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names.len(), 2);
    assert_ne!(names[0], names[1]);
}

/// A window that takes a column itself, in a `GROUP BY ALL`, would be grouped on, which DuckDB
/// refuses; one over aggregates or over no column is not grouped on
/// (test/sql/aggregate/group/group_by_all_window.test:9, 31, 37).
#[test]
fn a_window_over_a_column_in_group_by_all_is_duckdb_s_binder_error() {
    let e = Engine::open_empty().unwrap();
    let t = "(SELECT i % 3 AS g, i AS v FROM range(10) r(i)) t";
    for (sql, says) in [
        (
            format!("SELECT g, sum(v) AS s, lag(sum(v)) OVER (ORDER BY g) FROM {t} GROUP BY ALL"),
            "Cannot mix aggregates with non-aggregated columns",
        ),
        (
            format!("SELECT g, rank() OVER (ORDER BY g) FROM {t} GROUP BY ALL"),
            "Cannot group on a window clause",
        ),
        (
            format!("SELECT g, sum(v) OVER () FROM {t} GROUP BY ALL"),
            "Cannot group on a window clause",
        ),
    ] {
        match e.sql(&sql) {
            Ok(b) => panic!("{sql}: answered {b:?}"),
            Err(err) => assert!(err.to_string().contains(says), "{sql}: {err}"),
        }
    }
    assert_eq!(
        rows(
            &e,
            &format!(
                "SELECT g, sum(v) AS s, rank() OVER (ORDER BY sum(v)) AS r FROM {t} \
                 GROUP BY ALL ORDER BY g"
            )
        ),
        vec![
            json!({"g": 0, "s": "18", "r": 3}),
            json!({"g": 1, "s": "12", "r": 1}),
            json!({"g": 2, "s": "15", "r": 2}),
        ]
    );
}
