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
    // A quoted alias of the name is a name.
    assert_eq!(
        rows(&e, "SELECT count(*) AS \"export_state\""),
        vec![json!({"export_state": 1})]
    );
}
