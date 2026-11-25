//! The functions the engine registers, audited (roadmap 6.2's allowlist question).
//!
//! DuckDB needed an allowlist because its own functions read files, environment and network
//! (`read_csv`, `getenv`, `glob`, the `duckdb_*` catalogue). This lists what DataFusion's
//! registry, as the engine builds it, puts in reach, and fails if anything of that kind appears.

use burrmill::{BurrmillError, Engine};

#[test]
fn nothing_registered_reaches_outside_the_query() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Utf8, true),
    ]));
    let b = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(arrow::array::StringArray::from(vec![
            "a",
        ]))],
    )
    .unwrap();
    let f = std::fs::File::create(segs.join(format!("t-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
    let e = Engine::open_segments(&segs).unwrap();
    let names = e.function_names();
    if std::env::var("PRINT").is_ok() {
        println!("{}", names.join("\n"));
    }
    let suspicious = [
        "read", "file", "glob", "env", "shell", "exec", "http", "url", "load", "import", "copy",
        "attach", "path", "dir", "sys", "setting", "catalog", "query", "sql",
    ];
    let hits: Vec<&String> = names
        .iter()
        .filter(|n| suspicious.iter().any(|s| n.contains(s)))
        .collect();
    assert!(hits.is_empty(), "functions to examine: {hits:?}");
    for sql in [
        "SELECT input_file_name() FROM t",
        "SELECT file_row_index() FROM t",
    ] {
        let err = e.sql(sql).unwrap_err().to_string();
        assert!(
            !err.contains(segs.to_str().unwrap()),
            "leaks the path: {err}"
        );
    }
}

/// `error(text)` as DuckDB has it: the statement fails with the text wherever a row reaches it, and
/// a `CASE` that does not take its branch never does.
#[test]
fn error_raises_only_where_it_is_reached() {
    let engine = Engine::open_empty().unwrap();
    let err = engine
        .sql("SELECT error('the tripwire') AS e")
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(err.contains("the tripwire"), "{err}");
    let err = engine
        .sql("SELECT CASE WHEN x > 1 THEN error('two is too many') ELSE x END AS y FROM (VALUES (1), (2)) t(x)")
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(err.contains("two is too many"), "{err}");
    let got = engine
        .sql("SELECT CASE WHEN x > 5 THEN error('never') ELSE x END AS y FROM (VALUES (1), (2)) t(x)")
        .unwrap();
    assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

/// DuckDB's `first` and `last` aggregates, as nuthatch's port emitter folds an overlay: the value in
/// the given order, a NULL included, with `FILTER` to pass one over.
#[test]
fn first_and_last_are_duckdbs_aggregates() {
    let engine = Engine::open_empty().unwrap();
    let got = engine
        .sql(
            "SELECT last(x ORDER BY y) AS a, first(x ORDER BY y) AS b, \
             last(x ORDER BY y) FILTER (WHERE x IS NOT NULL) AS c \
             FROM (VALUES (1, 5), (NULL, 9), (3, 2), (4, 6)) t(x, y)",
        )
        .unwrap();
    let rows: Vec<_> = got
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    assert_eq!(rows, vec![serde_json::json!({"a": null, "b": 3, "c": 4})]);
}

/// `list_sum` is DuckDB's `sum` over the elements, exact for integers and decimals and refused
/// past 38 digits; `list_min` exists, and both extremes take a NULL list.
#[test]
fn list_sum_min_and_max_are_duckdbs() {
    let engine = Engine::open_empty().unwrap();
    let got = engine
        .sql(
            "SELECT list_sum([1, 2, 3]) AS a, list_sum([1.5, 2.25]) AS b, \
             list_sum([9007199254740993, 1]) AS c, list_sum([NULL, 1]) AS d, list_sum([]) AS e, \
             list_min([3, 1, NULL, 2]) AS f, list_min(NULL) AS g, list_max(NULL) AS h",
        )
        .unwrap();
    let rows: Vec<_> = got
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![serde_json::json!({
            "a": "6", "b": "3.75", "c": "9007199254740994", "d": "1", "e": null,
            "f": 1, "g": null, "h": null,
        })]
    );
    let past = format!(
        "SELECT list_sum([CAST('{0}' AS HUGEINT), CAST('{0}' AS HUGEINT)]) AS a",
        "9".repeat(38)
    );
    let err = engine.sql(&past).unwrap_err().to_string();
    assert!(err.contains("list_sum does not fit"), "{err}");
}

/// DuckDB's `BIGNUM`, as the network nest's views write it: token amounts cast from text, added,
/// negated and summed, exactly, and read back as text. It is `DECIMAL(38,0)` here, the nest's
/// 38-digit line, so a value past it refuses rather than being guessed at.
#[test]
fn bignum_is_the_38_digit_decimal() {
    let engine = Engine::open_empty().unwrap();
    let one = |sql: &str| -> serde_json::Value {
        let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        got.iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .next()
            .unwrap()
    };
    assert_eq!(
        one(
            "SELECT CAST(CAST('9000000000000000000000000000' AS BIGNUM) + CAST('1000000000000000000' AS BIGNUM) AS VARCHAR) AS x"
        ),
        serde_json::json!({"x": "9000000001000000000000000000"})
    );
    assert_eq!(
        one("SELECT CAST(CAST(0 AS BIGNUM) - CAST('7' AS BIGNUM) AS VARCHAR) AS x"),
        serde_json::json!({"x": "-7"})
    );
    assert_eq!(
        one(
            "SELECT CAST(sum(CAST(v AS BIGNUM)) AS VARCHAR) AS x FROM (VALUES ('5000000000000000000000000000'), ('5000000000000000000000000000')) t(v)"
        ),
        serde_json::json!({"x": "10000000000000000000000000000"})
    );
    let refused = engine
        .sql("SELECT CAST('12345678901234567890123456789012345678901234567890' AS BIGNUM) AS x")
        .expect_err("a value past 38 digits refuses")
        .to_string();
    assert!(refused.contains("to DECIMAL(38,0)"), "{refused}");
}

/// `AS MATERIALIZED` is only a hint to DuckDB's planner; the answer is the CTE's either way.
#[test]
fn a_materialized_cte_answers_as_a_plain_one() {
    let engine = Engine::open_empty().unwrap();
    for sql in [
        "WITH t AS MATERIALIZED (SELECT 1 AS n) SELECT n FROM t",
        "WITH t AS NOT MATERIALIZED (SELECT 1 AS n) SELECT n FROM t",
        "WITH RECURSIVE t(n) AS MATERIALIZED (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT max(n) AS n FROM t",
    ] {
        let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 1, "{sql}");
    }
    let got = engine.sql("SELECT 'AS MATERIALIZED (' AS n").unwrap();
    let rows: Vec<_> = got
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![serde_json::json!({"n": "AS MATERIALIZED ("})],
        "a string is left as written"
    );
}

/// What the network nest's controller and escrow views call: a whole-string regex match, and `hex`
/// of a byte value as DuckDB prints it, upper case with no leading zeros.
#[test]
fn regexp_full_match_and_hex_as_duckdb_has_them() {
    let engine = Engine::open_empty().unwrap();
    let got = engine
        .sql(
            "SELECT regexp_full_match('0x' || repeat('0', 24) || repeat('ab', 20), '0x0{24}[0-9a-fA-F]{40}') AS whole, \
             regexp_full_match('x0x', '0x') AS part, hex(10) AS a, hex(255) AS b, hex(0) AS c, \
             lpad(hex(7 & 255), 2, '0') AS d",
        )
        .unwrap();
    let rows: Vec<_> = got
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"whole": true, "part": false, "a": "A", "b": "FF", "c": "0", "d": "07"})
        ]
    );
}

/// DuckDB allows an unaliased expression to repeat inside a CTE, whose column list names it; the
/// recursive fold the network nest's indexer view seeds this way.
#[test]
fn a_repeated_expression_inside_a_cte_is_not_a_clash() {
    let engine = Engine::open_empty().unwrap();
    let got = engine
        .sql(
            "WITH RECURSIVE f(a, b, c, d) AS (SELECT DISTINCT 1, CAST(0 AS BIGNUM), CAST(0 AS BIGNUM), 0 \
             UNION ALL SELECT a + 1, b, c, d FROM f WHERE a < 3) SELECT count(*) AS n FROM f",
        )
        .unwrap_or_else(|e| panic!("{e}"));
    let rows: Vec<_> = got
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    assert_eq!(rows, vec![serde_json::json!({"n": 3})]);
}

/// Shifts as DuckDB 1.5 does them: `>>` is 0 past the width, `<<` refuses anything shifted out,
/// and a literal takes the other side's type.
#[test]
fn shifts_are_duckdbs() {
    let engine = Engine::open_empty().unwrap();
    let one = |sql: &str| -> serde_json::Value {
        let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        got.iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .next()
            .unwrap()
    };
    assert_eq!(
        one(
            "SELECT CAST(1000 AS BIGINT) >> 64 AS a, CAST(-1000 AS BIGINT) >> 3 AS b, CAST(1000 AS UBIGINT) >> -1 AS c, \
             (CAST(66051 AS UBIGINT) >> 8) & 255 AS d, CAST(3 AS BIGINT) << 2 AS e"
        ),
        serde_json::json!({"a": 0, "b": -125, "c": 0, "d": 2, "e": 12})
    );
    for sql in [
        "SELECT CAST(1 AS INTEGER) << 31 AS a",
        "SELECT CAST(1 AS BIGINT) << 64 AS a",
        "SELECT CAST(-1 AS BIGINT) << 2 AS a",
    ] {
        assert!(engine.sql(sql).is_err(), "{sql} refuses");
    }
}

/// `information_schema` as nuthatch's `.tables` and `.schema` read it, current after a view is
/// defined past the first read.
#[test]
fn information_schema_follows_the_views() {
    let mut engine = Engine::open_empty().unwrap();
    let rows = |engine: &Engine, sql: &str| -> Vec<serde_json::Value> {
        let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        got.iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .collect()
    };
    let tables = "SELECT table_name FROM information_schema.tables ORDER BY 1";
    engine.register_view("a", "SELECT 1 AS x").unwrap();
    assert_eq!(
        rows(&engine, tables),
        vec![serde_json::json!({"table_name": "a"})]
    );
    engine
        .register_view("b", "SELECT 'y' AS y, 2 AS z")
        .unwrap();
    assert_eq!(
        rows(&engine, tables),
        vec![
            serde_json::json!({"table_name": "a"}),
            serde_json::json!({"table_name": "b"})
        ]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'b' ORDER BY ordinal_position"
        ),
        vec![
            serde_json::json!({"column_name": "y", "data_type": "VARCHAR"}),
            serde_json::json!({"column_name": "z", "data_type": "BIGINT"}),
        ]
    );
}

/// A statement that cannot plan is told apart from one that failed reading, as DuckDB's binder
/// errors are: nuthatch's integrity sweep reads damaged data into the second and only the second.
#[test]
fn a_statement_that_does_not_plan_is_a_plan_error() {
    let mut engine = Engine::open_empty().unwrap();
    engine.register_view("t", "SELECT 1 AS x").unwrap();
    for sql in [
        "SELECT no_such_column FROM t",
        "SELECT x + 'a' FROM t",
        "SELECT * FROM t WHERE x < 'a'",
    ] {
        let e = engine.sql(sql).expect_err(sql);
        assert!(matches!(e, BurrmillError::Plan(_)), "{sql}: {e:?}");
    }
    let e = engine
        .sql("SELECT error('at a row') FROM t")
        .expect_err("raised at a row");
    assert!(matches!(e, BurrmillError::Substrate(_)), "{e:?}");
}

fn one_row(engine: &Engine, sql: &str) -> Vec<serde_json::Value> {
    let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    got.iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

/// #17: LIKE has no escape character unless `ESCAPE` names one, and then any one character may be
/// it, as DuckDB 1.5 answers each of these.
#[test]
fn like_escapes_only_with_an_escape_clause() {
    let engine = Engine::open_empty().unwrap();
    assert_eq!(
        one_row(
            &engine,
            r#"SELECT 'ab' LIKE 'a\b' AS a, 'a\b' LIKE 'a\\b' AS b, 'a\b' LIKE 'a\b' AS c, 'a%' LIKE 'a$%' ESCAPE '$' AS d, 'ab' LIKE 'a$%' ESCAPE '$' AS e, 'a$' LIKE 'a$$' ESCAPE '$' AS f, 'a_c' LIKE 'a\_c' ESCAPE '\' AS g, 'abc' LIKE 'a\_c' ESCAPE '\' AS h"#
        ),
        vec![
            serde_json::json!({"a": false, "b": false, "c": true, "d": true, "e": false, "f": true, "g": true, "h": false})
        ]
    );
    assert_eq!(
        one_row(
            &engine,
            r#"SELECT 'A\B' ILIKE 'a\b' AS a, 'AB' ILIKE 'a\b' AS b, 'A%' ILIKE 'a#%' ESCAPE '#' AS c, 'a\b' NOT LIKE 'a\b' AS d, 'a' LIKE 'a' ESCAPE '' AS e, '%' LIKE '%%' ESCAPE '%' AS f, 'x' LIKE '%%' ESCAPE '%' AS g, 'ab' LIKE 'a$b' ESCAPE '$' AS h"#
        ),
        vec![
            serde_json::json!({"a": true, "b": false, "c": true, "d": false, "e": true, "f": true, "g": false, "h": true})
        ]
    );
    assert_eq!(
        one_row(
            &engine,
            r#"SELECT s LIKE p AS m FROM (VALUES ('a\b', 'a\b'), ('ab', 'a\b'), ('a\b', 'a\\b'), ('x', NULL)) t(s, p)"#
        ),
        [true, false, false]
            .map(|m| serde_json::json!({ "m": m }))
            .into_iter()
            .chain([serde_json::json!({ "m": null })])
            .collect::<Vec<_>>()
    );
    for (sql, why) in [
        (
            r#"SELECT 'a$' LIKE 'a$' ESCAPE '$' AS a"#,
            "must not end with escape character",
        ),
        (
            r#"SELECT 'a\' LIKE 'a\' ESCAPE '\' AS a"#,
            "must not end with escape character",
        ),
        (
            r#"SELECT s LIKE p ESCAPE '$' AS m FROM (VALUES ('a%', 'a$%')) t(s, p)"#,
            "is not supported here",
        ),
    ] {
        let e = engine.sql(sql).expect_err(sql).to_string();
        assert!(e.contains(why), "{sql}: {e}");
    }
}

/// #18: text to DECIMAL(p,0) and HUGEINT reads every spelling DuckDB 1.5 reads: an exponent,
/// underscores between digits, a fraction rounded half away from zero; NULL from `TRY_CAST` and a
/// refusal from `CAST` for what it does not.
#[test]
fn text_to_decimal_reads_duckdbs_spellings() {
    let engine = Engine::open_empty().unwrap();
    assert_eq!(
        one_row(
            &engine,
            "SELECT TRY_CAST('1e3' AS DECIMAL(38,0)) a, TRY_CAST('1.5e18' AS HUGEINT) b, TRY_CAST('1_000' AS HUGEINT) c, \
             TRY_CAST(' 12 ' AS HUGEINT) d, TRY_CAST('+7' AS DECIMAL(38,0)) e, TRY_CAST('1.5' AS DECIMAL(38,0)) f, \
             TRY_CAST('-2.5' AS HUGEINT) g, TRY_CAST('0x10' AS HUGEINT) h, TRY_CAST('abc' AS HUGEINT) i, \
             TRY_CAST('' AS DECIMAL(38,0)) j"
        ),
        vec![
            serde_json::json!({"a": "1000", "b": "1500000000000000000", "c": "1000", "d": "12", "e": "7",
            "f": "2", "g": "-3", "h": null, "i": null, "j": null})
        ]
    );
    assert_eq!(
        one_row(
            &engine,
            "SELECT TRY_CAST('1e38' AS DECIMAL(38,0)) a, TRY_CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) b, \
             TRY_CAST('99999999999999999999999999999999999999.5' AS DECIMAL(38,0)) c, TRY_CAST('123' AS DECIMAL(2,0)) d, \
             TRY_CAST('1e2' AS DECIMAL(3,0)) e, TRY_CAST('1_000' AS DECIMAL(38,0)) f, TRY_CAST('2.5' AS HUGEINT) g, \
             TRY_CAST('1e-1' AS DECIMAL(38,0)) h, TRY_CAST('5e-1' AS HUGEINT) i, TRY_CAST('1E3' AS DECIMAL(38,0)) j, \
             TRY_CAST('1__0' AS HUGEINT) k, TRY_CAST('inf' AS HUGEINT) l"
        ),
        vec![
            serde_json::json!({"a": null, "b": "99999999999999999999999999999999999999", "c": null, "d": null,
            "e": "100", "f": "1000", "g": "3", "h": "0", "i": "1", "j": "1000", "k": null, "l": null})
        ]
    );
    assert_eq!(
        one_row(
            &engine,
            "SELECT CAST('1e3' AS DECIMAL(38,0)) a, CAST('1_000' AS HUGEINT) b, CAST(' 7 ' AS HUGEINT) c, \
             CAST('1.5' AS DECIMAL(38,0)) d"
        ),
        vec![serde_json::json!({"a": "1000", "b": "1000", "c": "7", "d": "2"})]
    );
    assert_eq!(
        one_row(
            &engine,
            "SELECT TRY_CAST('1__0' AS BIGINT) a, TRY_CAST('1_0.5_5' AS HUGEINT) b, TRY_CAST('1_0e1_0' AS HUGEINT) c, \
             TRY_CAST('1.0_0' AS DECIMAL(38,0)) d, TRY_CAST('1e_1' AS HUGEINT) e"
        ),
        vec![serde_json::json!({"a": null, "b": "11", "c": "100000000000", "d": "1", "e": null})]
    );
    for sql in [
        "SELECT CAST('abc' AS HUGEINT) a",
        "SELECT CAST('1e39' AS DECIMAL(38,0)) a",
    ] {
        let e = engine.sql(sql).expect_err(sql).to_string();
        assert!(e.contains("Could not convert string"), "{sql}: {e}");
    }
}

/// #20: statements DuckDB 1.5 answers, answered as it does, each value as DuckDB printed it.
#[test]
fn duckdb_answers_that_were_refusals() {
    let engine = Engine::open_empty().unwrap();
    for (sql, want) in [
        (
            "SELECT to_json(1.5::DOUBLE) a, to_json(0.1::DOUBLE) b, to_json(1e20::DOUBLE) c, to_json(1e-7::DOUBLE) d, \
             to_json(2.0::DOUBLE) e, to_json(-0.0::DOUBLE) f, to_json(1e21::DOUBLE) g, to_json(1e-6::DOUBLE) h, \
             to_json(123456789012345678901234.0::DOUBLE) i, to_json('nan'::DOUBLE) j, to_json('-inf'::DOUBLE) k, \
             to_json(0.1::FLOAT) l",
            serde_json::json!({"a": "1.5", "b": "0.1", "c": "100000000000000000000.0", "d": "1e-7", "e": "2.0",
                "f": "-0.0", "g": "1e21", "h": "0.000001", "i": "1.2345678901234569e23", "j": "NaN",
                "k": "-Infinity", "l": "0.10000000149011612"}),
        ),
        (
            "SELECT to_json(1.50) a, to_json(100.00) b, to_json(-0.05) c, \
             to_json(CAST(12345678901234567890.123 AS DECIMAL(38,3))) d, to_json(0.0) e, \
             to_json(struct_pack(x := 1.5::DOUBLE, y := 2.25)) f, to_json([1.5::DOUBLE, NULL]) g, \
             to_json(CAST(-12.340 AS DECIMAL(10,3))) h",
            serde_json::json!({"a": "1.5", "b": "100.0", "c": "-0.05", "d": "12345678901234567890.123",
                "e": "0.0", "f": "{\"x\":1.5,\"y\":2.25}", "g": "[1.5,null]", "h": "-12.34"}),
        ),
        (
            "SELECT printf('%.2f', 1.5) a, printf('%f', 2.25) b, printf('%5.1f', 1.25) c, printf('%g', 1.5) d, \
             printf('%e', 1.5) e, format('{}', 1.5) f, format('{}', 100.00) g, format('{:.3f}', 2.5) h, \
             format('{}', CAST(12345678901234567890.123 AS DECIMAL(38,3))) i",
            serde_json::json!({"a": "1.50", "b": "2.250000", "c": "  1.2", "d": "1.5", "e": "1.500000e+00",
                "f": "1.5", "g": "100.0", "h": "2.500", "i": "1.2345678901234567e+19"}),
        ),
        (
            "SELECT printf('%,d', 1234567) a, printf('%,d', -1234) b, printf('%,d', 12) c, \
             printf('%,5d|%-,8d|%,x', 1234, 1234, 255) d, printf('%,f', 1234.5::DOUBLE) e, \
             printf('%,.0f', 999.5::DOUBLE) f, printf('%,.2f', 1234567.891) g",
            serde_json::json!({"a": "1,234,567", "b": "-1,234", "c": "12", "d": "1,234|1,234   |255",
                "e": "1,234.500000", "f": "1,000", "g": "1,234,567.89"}),
        ),
        (
            "SELECT hex(CAST(255 AS HUGEINT)) a, hex(CAST(-1 AS HUGEINT)) b, hex(CAST(0 AS HUGEINT)) c, \
             hex(CAST(-256 AS HUGEINT)) d",
            serde_json::json!({"a": "FF", "b": "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "c": "0",
                "d": "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00"}),
        ),
        (
            "SELECT if(1 < 2, 'a', 'b') a, if(NULL, 1, 2) b, if(false, 1, 2.5) c, \
             'abc' ^@ 'ab' d, 'abc' ^@ 'b' e, NULL ^@ 'a' f, 'ab' || 'c' ^@ 'abc' g",
            serde_json::json!({"a": "a", "b": 2, "c": "2.5", "d": true, "e": false, "f": null, "g": true}),
        ),
        (
            "SELECT list_reverse_sort([3, NULL, 1, 2])[1] a, list_reverse_sort([3, NULL, 1, 2])[4] b, \
             list_reverse_sort(['b', 'a', NULL])[2] c, list_reverse_sort(NULL) d",
            serde_json::json!({"a": 3, "b": null, "c": "a", "d": null}),
        ),
        (
            "SELECT split_part('a,b,c', ',', 0) a, split_part('a,b,c', ',', -1) b, split_part('a,b,c', ',', 5) c, \
             TIMESTAMP '2024-01-02 03:04' = TIMESTAMP '2024-01-02 03:04:00' AS d, \
             CAST(TIMESTAMP '2024-01-02T03:04' AS VARCHAR) AS e",
            serde_json::json!({"a": "", "b": "c", "c": "", "d": true, "e": "2024-01-02 03:04:00"}),
        ),
        (
            "SELECT count(*) AS n, count(*) FILTER (WHERE u < '2.5') AS m FROM \
             (SELECT CAST(x AS UBIGINT) AS u FROM range(5) t(x)) s WHERE u < '4' AND '1' <= u",
            serde_json::json!({"n": 3, "m": 2}),
        ),
        (
            "FROM (SELECT 1 AS a) t SELECT a",
            serde_json::json!({"a": 1}),
        ),
        ("FROM (SELECT 1 AS a) t", serde_json::json!({"a": 1})),
    ] {
        assert_eq!(one_row(&engine, sql), vec![want], "{sql}");
    }
    for (sql, n) in [
        ("SELECT 1 AS a FROM range(5) LIMIT 1.5", 2),
        ("SELECT 1 AS a FROM range(5) LIMIT 2.5", 3),
        ("SELECT 1 AS a FROM range(5) LIMIT 0.4", 0),
    ] {
        assert_eq!(one_row(&engine, sql).len(), n, "{sql}");
    }
}

/// #20: what DuckDB 1.5 answers and burrmill does not, refused by name.
#[test]
fn duckdb_answers_still_refused_say_so_by_name() {
    let engine = Engine::open_empty().unwrap();
    for (sql, name) in [
        (
            "SELECT approx_count_distinct(x) a FROM range(100) t(x)",
            "approx_count_distinct",
        ),
        (
            "SELECT list_aggregate([1, 2, 3], 'sum') a",
            "list_aggregate",
        ),
        (
            "SELECT strptime('2024-01-02 03:04:05', '%Y-%m-%d %H:%M:%S') a",
            "strptime",
        ),
        ("SELECT try_strptime('x', '%Y') a", "try_strptime"),
        (
            "SELECT COLUMNS('a') FROM (SELECT 1 AS a, 2 AS b) t",
            "COLUMNS",
        ),
        ("SELECT 'abc' GLOB 'a*' a", "GLOB"),
        ("PIVOT (SELECT 1 AS a, 'x' AS k) ON k USING sum(a)", "PIVOT"),
        ("SELECT * FROM unnest([1, 2]) t(x)", "unnest"),
        (
            "SELECT * FROM range(DATE '2024-01-01', DATE '2024-01-03', INTERVAL 1 DAY) t(d)",
            "range",
        ),
        ("SELECT * FROM range(NULL) t(x)", "range"),
        ("SELECT * FROM (SELECT 3 AS n) s, range(s.n) t(x)", "range"),
        (
            "SELECT DISTINCT a FROM (VALUES (1, 2), (2, 1)) t(a, b) ORDER BY b",
            "SELECT DISTINCT",
        ),
        ("WITH x(a, a) AS (SELECT 1, 2) SELECT * FROM x", "x(a, a)"),
        (
            "SELECT CAST(struct_pack(a := 1, b := 'x') AS VARCHAR) a",
            "CAST from Struct",
        ),
    ] {
        let e = engine.sql(sql).expect_err(sql).to_string();
        let first = e.lines().next().unwrap_or_default();
        assert!(
            first.contains(name)
                && (first.contains("not supported") || first.contains("Unsupported")),
            "{sql}: {e}"
        );
    }
}

/// #173: a refusal with a supported equivalent names it, and the equivalent answers.
#[test]
fn refusals_name_what_to_write_instead() {
    let engine = Engine::open_empty().unwrap();
    for (refused, hint, instead, want) in [
        (
            "SELECT approx_count_distinct(x) a FROM range(100) t(x)",
            "COUNT(DISTINCT ...)",
            "SELECT COUNT(DISTINCT x) a FROM range(100) t(x)",
            serde_json::json!({"a": 100}),
        ),
        (
            "SELECT list_aggregate([1, 2, 3], 'sum') a",
            "sum(v) FROM (SELECT unnest(...) AS v)",
            "SELECT (SELECT sum(v) FROM (SELECT unnest([1, 2, 3]) AS v)) a",
            serde_json::json!({"a": "6"}),
        ),
        (
            "SELECT array_aggregate([1, 2, 3], 'max') a",
            "(SELECT sum(v) FROM (SELECT unnest(...) AS v))",
            "SELECT (SELECT max(v) FROM (SELECT unnest([1, 2, 3]) AS v)) a",
            serde_json::json!({"a": 3}),
        ),
        (
            "SELECT COLUMNS('a') FROM (SELECT 1 AS a, 2 AS b) t",
            "* EXCLUDE (...)",
            "SELECT * EXCLUDE (b) FROM (SELECT 1 AS a, 2 AS b) t",
            serde_json::json!({"a": 1}),
        ),
        (
            "SELECT 'abc' GLOB 'a?c*' a",
            "LIKE is, with % for * and _ for ?",
            "SELECT 'abc' LIKE 'a_c%' a",
            serde_json::json!({"a": true}),
        ),
        (
            "PIVOT (SELECT 1 AS a, 'x' AS k) ON k USING sum(a)",
            "FILTER (WHERE ...) or CASE, under GROUP BY",
            "SELECT sum(a) FILTER (WHERE k = 'x') AS x FROM (SELECT 1 AS a, 'x' AS k) t",
            serde_json::json!({"x": "1"}),
        ),
        (
            "UNPIVOT (SELECT 1 AS a) ON a INTO NAME k VALUE v",
            "UNION ALL",
            "SELECT 'a' AS k, a AS v FROM (SELECT 1 AS a) t",
            serde_json::json!({"k": "a", "v": 1}),
        ),
        (
            "SELECT strptime('2024-01-02 03:04:05', '%Y-%m-%d %H:%M:%S') a",
            "CAST(... AS TIMESTAMP)",
            "SELECT CAST(CAST('2024-01-02 03:04:05' AS TIMESTAMP) AS VARCHAR) a",
            serde_json::json!({"a": "2024-01-02 03:04:05"}),
        ),
        (
            "SELECT try_strptime('x', '%Y') a",
            "TRY_CAST(... AS TIMESTAMP)",
            "SELECT TRY_CAST('x' AS TIMESTAMP) a",
            serde_json::json!({"a": null}),
        ),
        (
            "SELECT s LIKE p ESCAPE '$' AS m FROM (VALUES ('a%', 'a$%')) t(s, p)",
            r"ESCAPE '\' is",
            r"SELECT s LIKE p ESCAPE '\' AS m FROM (VALUES ('a%', 'a\%')) t(s, p)",
            serde_json::json!({"m": true}),
        ),
    ] {
        let e = engine.sql(refused).expect_err(refused).to_string();
        let first = e.lines().next().unwrap_or_default();
        assert!(
            first.contains("is not supported here") && first.contains(hint),
            "{refused}: {e}"
        );
        assert_eq!(one_row(&engine, instead), vec![want], "{instead}");
    }
}

/// DuckDB's TIMESTAMP, and DATETIME with it, is microseconds: a finer fraction is cut off, as
/// DuckDB cuts it, and the column is microseconds for nuthatch's encoder.
#[test]
fn a_cast_to_timestamp_is_microseconds() {
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
    let engine = Engine::open_empty().unwrap();
    let got = engine
        .sql(
            "SELECT CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP) a, \
             TRY_CAST('2024-01-02 03:04:05.1234567' AS TIMESTAMP) b, \
             '2024-01-02 03:04:05.123456'::TIMESTAMP c, CAST('2024-01-02' AS DATETIME) d",
        )
        .unwrap();
    let b = &got[0];
    for (i, want) in [
        1704164645123456,
        1704164645123456,
        1704164645123456,
        1704153600000000,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            b.column(i).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            b.column(i)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            want
        );
    }
}

/// What a statement builds is bounded per value (`df_valuebound` measures the heap), and nothing
/// under the bound answers differently for it: `||` keeps DataFusion's NULL and its column name, a
/// literal repeated per row keeps its name, a long constant left unfolded still answers, and the
/// pool holds nothing once the statement is done.
#[test]
fn built_values_answer_as_before_under_their_bound() {
    let e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 256 << 20,
        threads: 2,
        spill: None,
    })
    .unwrap();
    let one = |sql: &str| -> serde_json::Value {
        let got = e.sql(sql).unwrap_or_else(|err| panic!("{sql}: {err}"));
        got.iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .next()
            .unwrap()
    };
    assert_eq!(
        one("SELECT 'a' || x AS s, NULL || x AS n FROM (VALUES ('b')) t(x)"),
        serde_json::json!({"s": "ab", "n": null})
    );
    let names: Vec<String> = e
        .sql("SELECT x || 'b', repeat('é', 1000) FROM (VALUES ('a')) t(x)")
        .unwrap()[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names[0], "(x || 'b')");
    for (sql, want) in [
        ("SELECT length(repeat('ab', 5000000)) AS n", 10_000_000),
        (
            "SELECT length(repeat(x, 10000000)) AS n FROM (VALUES ('a')) t(x)",
            10_000_000,
        ),
        (
            "SELECT sum(length(s)) AS n FROM (SELECT repeat('a', 100000) AS s FROM range(1000))",
            100_000_000,
        ),
        (
            "SELECT sum(length(lpad(CAST(r AS VARCHAR), 64, '0'))) AS n FROM range(100000) t(r)",
            6_400_000,
        ),
        (
            "WITH RECURSIVE r(s, n) AS (SELECT 'ab', 0 UNION ALL SELECT s || s, n + 1 FROM r WHERE n < 20) \
             SELECT max(length(s)) AS n FROM r",
            2 << 20,
        ),
        (
            "SELECT length(string_agg(CAST(r AS VARCHAR), ',')) AS n FROM range(100000) t(r)",
            588_889,
        ),
        ("SELECT count(*) AS n FROM range(1000000)", 1_000_000),
    ] {
        let got = one(sql)["n"].to_string();
        assert_eq!(got.trim_matches('"'), want.to_string(), "{sql}");
        assert_eq!(e.memory_reserved(), 0, "{sql} left a charge behind");
    }
    e.set_value_limit(1 << 10);
    let err = e
        .sql("SELECT repeat(x, 2000) AS s FROM (VALUES ('a')) t(x)")
        .unwrap_err();
    assert!(matches!(err, BurrmillError::LimitExceeded(_)), "{err:?}");
    let err = err.to_string();
    assert!(
        err.contains("repeat would build a value of 2000 bytes, past the limit of 1024 bytes"),
        "{err}"
    );
    assert!(
        e.sql("SELECT repeat(x, 1000) AS s FROM (VALUES ('a')) t(x)")
            .is_ok()
    );
}

#[test]
fn pivot_after_from_is_refused_by_name_not_dumped() {
    let engine = Engine::open_empty().unwrap();
    for (sql, word) in [
        (
            "SELECT * FROM (SELECT 1 AS a, 'x' AS k) t PIVOT (sum(a) FOR k IN ('x'))",
            "PIVOT is not supported here; write one aggregate per value",
        ),
        (
            "SELECT * FROM (SELECT 1 AS a) t UNPIVOT (v FOR k IN (a))",
            "UNPIVOT is not supported here; write one SELECT per column",
        ),
    ] {
        let e = engine.sql(sql).expect_err(sql).to_string();
        assert!(e.contains(word), "{sql}: {e}");
        assert!(e.len() < 300 && !e.contains("Unsupported ast node"), "{e}");
    }
}

#[test]
fn a_parse_error_is_the_parsers_words_without_debug_output() {
    let engine = Engine::open_empty().unwrap();
    let e = engine
        .sql("SELECT x FROM (VALUES (1)) t(x)\n SELEC x")
        .unwrap_err();
    assert!(matches!(e, BurrmillError::Parse(_)), "{e:?}");
    let e = e.to_string();
    assert!(e.contains("syntax error at or near \"SELEC\""), "{e}");
    for debug in ["Diagnostic", "span:", "Location("] {
        assert!(!e.contains(debug), "{e}");
    }
}

#[test]
fn median_over_decimal_says_what_to_write() {
    let engine = Engine::open_empty().unwrap();
    let e = engine
        .sql("SELECT median(CAST(x AS DECIMAL(5,2))) m FROM (VALUES (1), (2)) t(x)")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("CAST(x AS DOUBLE)") && e.contains("quantile_cont"),
        "{e}"
    );
    for sql in [
        "SELECT median(CAST(x AS DOUBLE)) m FROM (VALUES (1), (2)) t(x)",
        "SELECT quantile_cont(CAST(x AS DECIMAL(5,2)), 0.5) m FROM (VALUES (1), (2)) t(x)",
        "SELECT approx_median(CAST(x AS DECIMAL(5,2))) m FROM (VALUES (1), (2)) t(x)",
    ] {
        assert_eq!(engine.sql(sql).unwrap()[0].num_rows(), 1, "{sql}");
    }
}

#[test]
fn an_alias_spelled_glob_is_an_alias_and_the_operator_still_refuses() {
    let engine = Engine::open_empty().unwrap();
    for sql in [
        r#"SELECT a "glob" FROM (SELECT 1 AS a) t"#,
        "SELECT a AS glob FROM (SELECT 1 AS a) t",
        r#"SELECT "glob" FROM (SELECT 1 AS "glob") t"#,
    ] {
        assert_eq!(engine.sql(sql).unwrap()[0].num_rows(), 1, "{sql}");
    }
    let e = engine.sql("SELECT 'a' GLOB 'a*'").unwrap_err().to_string();
    assert!(e.contains("GLOB is not supported here"), "{e}");
}

#[test]
fn asof_join_is_planned_and_the_word_stays_usable() {
    let engine = Engine::open_empty().unwrap();
    for (sql, rows) in [
        (
            "SELECT * FROM (SELECT 1 AS k) a ASOF JOIN (SELECT 2 AS k) b ON a.k >= b.k",
            0,
        ),
        (
            "SELECT a.k FROM (SELECT 3 AS k) a ASOF JOIN (SELECT 2 AS k) b USING (k)",
            1,
        ),
        (
            "SELECT * FROM (SELECT 1 AS k) a ASOF LEFT JOIN (SELECT 2 AS k) b ON a.k >= b.k",
            1,
        ),
    ] {
        let got: usize = engine.sql(sql).unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(got, rows, "{sql}");
    }
    for (sql, says) in [
        (
            "SELECT * FROM (SELECT 1 AS k) a ASOF JOIN (SELECT 2 AS k) b MATCH_CONDITION (a.k >= b.k) ON true",
            "Snowflake",
        ),
        (
            "SELECT * FROM (SELECT 1 AS k) a ASOF JOIN (SELECT 2 AS k) b USING (k)",
            "SELECT * over ASOF JOIN ... USING",
        ),
        (
            "SELECT * FROM (SELECT 1 AS k) a ASOF JOIN (SELECT 2 AS k) b ON a.k = b.k",
            "one inequality",
        ),
        (
            "SELECT * FROM (SELECT 1 AS k) a ASOF JOIN (SELECT 2 AS k) b ON a.k >= b.k AND a.k <= b.k",
            "exactly one inequality",
        ),
    ] {
        let e = engine.sql(sql).expect_err(sql).to_string();
        assert!(e.contains(says), "{sql}: {e}");
    }
    for sql in [
        "SELECT 1 AS asof",
        r#"SELECT 'x' AS "asof", 'asof join' AS s"#,
        "SELECT asof FROM (SELECT 1 AS asof) t",
        "SELECT * FROM (SELECT 1 AS k) a JOIN (SELECT 1 AS k, 1 AS asof) b ON a.k = b.asof JOIN (SELECT 1 AS c) c ON true",
        "SELECT * FROM (SELECT 1 AS k) a JOIN (SELECT 1 AS k, 1 AS asof) b ON a.k = b.asof LEFT JOIN (SELECT 1 AS c) c ON true",
        "WITH asof AS (SELECT 1 AS k) SELECT * FROM asof JOIN (SELECT 1 AS k) b USING (k)",
    ] {
        assert_eq!(engine.sql(sql).expect(sql)[0].num_rows(), 1, "{sql}");
    }
}

#[test]
fn parsing_grows_linearly_with_the_statement() {
    let statement = |kib: usize| {
        let mut sql = String::from("SELECT 1 AS c0");
        let mut i = 1;
        while sql.len() < kib * 1024 {
            sql.push_str(&format!(", {i} AS c{i}"));
            i += 1;
        }
        sql
    };
    // 4x the size: linear parsing costs about 4x, quadratic about 16x, so a
    // limit of 8x sits far from both. The two sizes are timed alternately, so a
    // slow patch on a shared runner hits both, and each keeps its best of 9.
    let (small_sql, large_sql) = (statement(16), statement(64));
    let time = |sql: &str| {
        let t = std::time::Instant::now();
        burrmill::inspect::parse(sql).unwrap();
        t.elapsed()
    };
    let (mut small, mut large) = (std::time::Duration::MAX, std::time::Duration::MAX);
    for _ in 0..9 {
        small = small.min(time(&small_sql));
        large = large.min(time(&large_sql));
    }
    assert!(
        large.as_secs_f64() < small.as_secs_f64() * 8.0,
        "16 KiB took {small:?}, 64 KiB took {large:?}, ratio {:.2}",
        large.as_secs_f64() / small.as_secs_f64()
    );
}

#[test]
fn asof_after_a_relation_named_by_a_keyword_is_the_join() {
    let engine = Engine::open_empty().unwrap();
    let sql = "WITH events(ts) AS (VALUES (5)), prices(ts, px) AS (VALUES (1, 10), (4, 11)) \
               SELECT events.ts, prices.px FROM events ASOF JOIN prices ON events.ts >= prices.ts";
    let b = engine.sql(sql).unwrap();
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&b)
            .unwrap()
            .to_string()
            .matches("11")
            .count(),
        1
    );
    assert_eq!(b.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

/// #430: DuckDB 2.0.1 raises `Division by zero` for `//` and `%` over integers, HUGEINT and
/// DECIMAL, a constant divisor or a column's (`test_division_by_zero.test`). A float is left as it
/// was: `//` and `%` by zero beside a DOUBLE are NULL here.
#[test]
fn exact_division_by_zero_raises_as_duckdb_does() {
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_view(
            "numbers",
            "SELECT * FROM (VALUES (7), (0), (NULL), (2)) t(i)",
        )
        .unwrap();
    for ty in [
        "TINYINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "UBIGINT",
        "DECIMAL(4,1)",
        "DECIMAL(18,3)",
        "DECIMAL(38,4)",
    ] {
        for op in ["//", "%"] {
            for sql in [
                format!("SELECT 7::{ty} {op} 0::{ty}"),
                format!("SELECT 7::{ty} {op} i::{ty} FROM numbers"),
            ] {
                let e = engine.sql(&sql).expect_err(&sql).to_string();
                assert!(e.contains("Division by zero"), "{sql}: {e}");
            }
        }
    }
    for sql in ["SELECT 7 // 0", "SELECT 7 % 0", "SELECT 7.5 % 0"] {
        let e = engine.sql(sql).expect_err(sql).to_string();
        assert!(e.contains("Division by zero"), "{sql}: {e}");
    }
    assert_eq!(
        one_row(
            &engine,
            "SELECT 7.5::DOUBLE % 0 AS a, 7.5::DOUBLE // 0 AS b, 7 // 2 AS c, 7 % 2 AS d"
        ),
        vec![serde_json::json!({"a": null, "b": null, "c": 3, "d": 1})]
    );
    // Where DuckDB may never reach the division, it is not raised (`test_try_in_clause.test`):
    // DataFusion would evaluate it for every row.
    assert_eq!(
        one_row(
            &engine,
            "SELECT i, CASE WHEN i = 0 THEN false ELSE (100 // i) IN (1, 3, 7) END AS a, \
             CASE WHEN i <> 0 AND (100 % i) IN (1, 2) THEN 1 ELSE 0 END AS b, \
             TRY(100 // i) AS c FROM numbers WHERE i IS NOT NULL ORDER BY i"
        ),
        vec![
            serde_json::json!({"i": 0, "a": false, "b": 0, "c": null}),
            serde_json::json!({"i": 2, "a": false, "b": 0, "c": 50}),
            serde_json::json!({"i": 7, "a": false, "b": 1, "c": 14}),
        ]
    );
    assert!(
        one_row(
            &engine,
            "SELECT nth_value(i, 1 // 0) OVER () AS n FROM numbers WHERE i > 10"
        )
        .is_empty()
    );
}

/// #430: a bare DECIMAL is DECIMAL(18,3), as DuckDB has it, within a list or a struct too
/// (`struct_of_lists_where.test:33`).
#[test]
fn a_bare_decimal_is_eighteen_three() {
    let engine = Engine::open_empty().unwrap();
    assert_eq!(
        one_row(
            &engine,
            "SELECT typeof(CAST(1 AS DECIMAL)) AS a, typeof(CAST(1 AS NUMERIC)) AS b, \
             typeof(CAST([1.5] AS DECIMAL[])) AS c, CAST('1.23456' AS DECIMAL) AS d, \
             typeof(CAST(NULL AS STRUCT(v DECIMAL[]))) AS e, typeof(CAST(1 AS DECIMAL(10))) AS f"
        ),
        vec![serde_json::json!({
            "a": "DECIMAL(18,3)", "b": "DECIMAL(18,3)", "c": "DECIMAL(18,3)[]", "d": "1.235",
            "e": "STRUCT(v DECIMAL(18,3)[])", "f": "DECIMAL(10,0)"
        })]
    );
}

/// #430: `typeof` names a type as DuckDB does where the engine's type is DuckDB's
/// (`type_literal.test:45`).
#[test]
fn typeof_names_types_as_duckdb_does() {
    let engine = Engine::open_empty().unwrap();
    assert_eq!(
        one_row(
            &engine,
            "SELECT typeof(TIMESTAMP(3) '2024-01-01 00:00:00') AS a, \
             typeof(CAST('2024-01-01' AS TIMESTAMP)) AS b, typeof(INTERVAL '1 day') AS c, \
             typeof({'a': 1::INTEGER, 'B c': 'x'}) AS d, typeof(MAP {'k': 1::INTEGER}) AS e"
        ),
        vec![serde_json::json!({
            "a": "TIMESTAMP_MS", "b": "TIMESTAMP", "c": "INTERVAL",
            "d": "STRUCT(a INTEGER, \"B c\" VARCHAR)", "e": "MAP(VARCHAR, INTEGER)"
        })]
    );
}

/// #430: `list_distinct` drops NULL (`list_distinct.test:62`), `[1, 2] || NULL` and a list `||`
/// a NULL list are NULL (`list_concat.test:256`), and `array_to_string` with a NULL separator joins
/// with `,` (`array_to_string.test:20`), as DuckDB 2.0.1 answers.
#[test]
fn list_functions_meet_null_as_duckdb_does() {
    let engine = Engine::open_empty().unwrap();
    assert_eq!(
        one_row(
            &engine,
            "SELECT to_json(list_sort(list_distinct([1, 1, NULL, 2, 2, 2, 3, NULL, NULL]))) AS a, \
             len(list_distinct([NULL, NULL])) AS b, list_distinct(NULL::INT[]) IS NULL AS c, \
             ([1, 2] || NULL) IS NULL AS d, ([1, 2] || NULL::INT[]) IS NULL AS e, \
             (NULL::INT[] || [1, 2]) IS NULL AS f, to_json([1, 2] || [3]) AS g, \
             array_to_string([1, 2, 3], NULL) AS h, array_to_string([1, 2, 3], '-') AS i, \
             array_to_string(NULL::INT[], '-') IS NULL AS j"
        ),
        vec![serde_json::json!({
            "a": "[1,2,3]", "b": 0, "c": true, "d": true, "e": true, "f": true, "g": "[1,2,3]",
            "h": "1,2,3", "i": "1-2-3", "j": true
        })]
    );
    assert_eq!(
        one_row(
            &engine,
            "SELECT to_json(list_sort(list_distinct(l))) AS a, to_json(l || m) AS b, \
             array_to_string(l, s) AS c FROM (VALUES ([2, NULL, 2, 1], NULL::INT[], NULL), \
             ([NULL], [4], '+')) t(l, m, s) ORDER BY c DESC"
        ),
        vec![
            serde_json::json!({"a": "[1,2]", "b": null, "c": "2,2,1"}),
            serde_json::json!({"a": "[]", "b": "[null,4]", "c": ""}),
        ]
    );
}

/// #430: `corr` over pairs with no spread is NaN in DuckDB, a single pair included
/// (`test_corr.test:15`), and NULL only over no pair at all.
#[test]
fn corr_without_spread_is_nan_as_duckdb_has_it() {
    let engine = Engine::open_empty().unwrap();
    let row = one_row(
        &engine,
        "SELECT isnan(corr(1, 1)) AS a, corr(NULL, NULL) IS NULL AS b, \
         isnan((SELECT corr(a, b) FROM (VALUES (1e301, 0), (-1e301, 0)) t(a, b))) AS c",
    );
    assert_eq!(
        row,
        vec![serde_json::json!({"a": true, "b": true, "c": true})]
    );
    let rows = one_row(
        &engine,
        "SELECT k, CASE WHEN isnan(corr(v, w)) THEN 'nan' ELSE CAST(round(corr(v, w), 8) AS VARCHAR) \
         END AS r, isnan(corr(v, v * 0)) AS z FROM \
         (VALUES (1, 10, NULL), (2, 10, 11), (2, 20, 22), (2, 25, NULL), (2, 30, 35), (3, 5, 5)) \
         t(k, v, w) GROUP BY k ORDER BY k",
    );
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"k": 1, "r": null, "z": true}),
            serde_json::json!({"k": 2, "r": "0.9988446", "z": true}),
            serde_json::json!({"k": 3, "r": "nan", "z": true}),
        ]
    );
}

/// #430: DuckDB joins a NATURAL JOIN on columns named alike but for case
/// (`case_insensitive_using.test:123`); DataFusion matched names exactly and kept both, so it is
/// refused by name. Names alike in case still join.
#[test]
fn a_natural_join_on_names_alike_but_for_case_is_refused() {
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_view("a", "SELECT 1 AS hello, 2 AS x")
        .unwrap();
    engine
        .register_view("b", "SELECT 1 AS \"HELLO\", 3 AS y")
        .unwrap();
    engine
        .register_view("c", "SELECT 1 AS hello, 4 AS z")
        .unwrap();
    let e = engine
        .sql("SELECT * FROM a NATURAL JOIN b")
        .expect_err("refused")
        .to_string();
    assert!(
        e.contains("NATURAL JOIN") && e.contains("not supported"),
        "{e}"
    );
    assert!(matches!(
        engine.sql("SELECT * FROM c NATURAL JOIN b"),
        Err(BurrmillError::NotAllowed(_))
    ));
    assert_eq!(
        one_row(&engine, "SELECT * FROM a NATURAL JOIN c"),
        vec![serde_json::json!({"hello": 1, "x": 2, "z": 4})]
    );
}
