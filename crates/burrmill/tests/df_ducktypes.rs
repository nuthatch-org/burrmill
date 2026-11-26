//! Answers from DuckDB's `test/sql/types` that Burrmill gave wrong (#419), each as DuckDB gives it.

use burrmill::Engine;
use serde_json::{Value, json};

fn rows(e: &Engine, sql: &str) -> Vec<Value> {
    e.sql(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

fn error(e: &Engine, sql: &str) -> String {
    match e.sql(sql) {
        Ok(_) => panic!("{sql}: answered where DuckDB raises an error"),
        Err(err) => err.to_string(),
    }
}

/// Text read as an INTERVAL the way DuckDB reads it, and an INTERVAL written as DuckDB writes it
/// (test/sql/types/interval/interval_constants.test:204, :252, :171, :391, test_interval.test:11,
/// interval_try_cast.test:70). Arrow read `'47.210'` as 3 years 11 months 6 days, kept the half of
/// `1.5 WEEK` as 84 hours, took `INTERVAL 3 decades` for 3 seconds named `decades`, and wrote
/// `24 mons`.
#[test]
fn intervals_parse_and_print_as_duckdb() {
    let e = Engine::open_empty().unwrap();
    let got = rows(
        &e,
        "SELECT '47.210'::INTERVAL::VARCHAR AS a, '-47.210'::INTERVAL::VARCHAR AS b, \
         '1.5 WEEK'::INTERVAL::VARCHAR AS c, '0.05 YEAR'::INTERVAL::VARCHAR AS d, \
         CAST(INTERVAL '1.5' HOUR AS VARCHAR) AS f, CAST(INTERVAL '-1.5' YEAR AS VARCHAR) AS g, \
         CAST(INTERVAL '1.5' SECOND AS VARCHAR) AS h, CAST(INTERVAL 3 decades AS VARCHAR) AS i, \
         CAST(INTERVAL 3 millennia AS VARCHAR) AS j, INTERVAL '2 years'::VARCHAR AS k, \
         INTERVAL '-2Y 4 days 5 Hours 1 MinUteS 3S 20mS 16uS'::VARCHAR AS l, \
         TRY_CAST('42 ' AS INTERVAL)::VARCHAR AS m, TRY_CAST('47.210 5' AS INTERVAL) AS n",
    );
    assert_eq!(
        got,
        vec![json!({
            "a": "00:00:47.21",
            "b": "-00:00:47.21",
            "c": "10 days 12:00:00",
            "d": "1 month",
            "f": "01:00:00",
            "g": "-1 year",
            "h": "00:00:01.5",
            "i": "30 years",
            "j": "3000 years",
            "k": "2 years",
            "l": "-2 years 4 days 05:01:03.020016",
            "m": "00:00:42",
            "n": null,
        })]
    );
    assert!(
        error(&e, "SELECT '47.210 5'::INTERVAL").contains("Could not convert string"),
        "a malformed interval is DuckDB's conversion error"
    );
}

/// Intervals compare normalized, a month as 30 days and a day as 24 hours, as in DuckDB; Arrow
/// compared the fields one by one (test/sql/types/interval/test_interval_comparison.test:11, :31,
/// :53, :114).
#[test]
fn intervals_compare_normalized() {
    let e = Engine::open_empty().unwrap();
    let got = rows(
        &e,
        "SELECT INTERVAL '30' DAY = INTERVAL '1' MONTH AS a, \
         INTERVAL '31' DAY > INTERVAL '1' MONTH AS b, \
         INTERVAL '30' HOUR <= INTERVAL '1' DAY AS c, \
         interval '1 month' - interval '4 days' = interval '26 days' AS d, \
         interval '1 day' - interval '1 hour' = interval '23 hours' AS f, \
         INTERVAL '40' DAY BETWEEN INTERVAL '1' MONTH AND INTERVAL '2' MONTH AS g",
    );
    assert_eq!(
        got,
        vec![json!({"a": true, "b": true, "c": false, "d": true, "f": true, "g": true})]
    );

    // In a join's condition: every row is below 1000 days, 2 years 3 months too (810 days), so
    // 4 x 4 rows; compared field by field, the 27 months were above and 12 were left.
    let got = rows(
        &e,
        "WITH t(i) AS (VALUES (INTERVAL '2 years 3 months'), (INTERVAL '-1734799452 DAYS'), \
         (INTERVAL '13 days'), (INTERVAL '1 month')), \
         s(c) AS (SELECT INTERVAL 1000 DAY FROM t) \
         SELECT count(*) AS n FROM t JOIN s ON t.i < s.c",
    );
    assert_eq!(got, vec![json!({"n": 16})]);
}

/// A TIMESTAMP minus a TIMESTAMP is an INTERVAL of days and time, as in DuckDB; DataFusion's was
/// a DURATION (test/sql/types/interval/test_interval_addition.test:301).
#[test]
fn a_timestamp_difference_is_an_interval() {
    let e = Engine::open_empty().unwrap();
    let got = rows(
        &e,
        "SELECT CAST(timestamp '1993-01-01 00:00:00' - timestamp '1991-01-01 01:00:30' AS VARCHAR) \
         AS a, CAST(timestamp '1991-01-01 01:00:30' - timestamp '1993-01-01 00:00:00' AS VARCHAR) \
         AS b",
    );
    assert_eq!(
        got,
        vec![json!({"a": "730 days 22:59:30", "b": "-730 days -22:59:30"})]
    );
}

/// DuckDB holds an interval's time in microseconds, to 2562047788 hours; Arrow's nanoseconds stop
/// at 2562047. Past that it is refused by name rather than cut short
/// (test/sql/types/interval/interval_colon_time_overflow.test:44).
#[test]
fn an_interval_past_arrows_nanoseconds_is_refused() {
    let e = Engine::open_empty().unwrap();
    let err = error(&e, "SELECT '999999999:00:00'::INTERVAL");
    assert!(err.contains("not supported"), "{err}");
}

/// What DuckDB's binder refuses, refused (test/sql/types/numeric/float_precision.test:47, :52,
/// decimal/test_decimal.test:153, :163, struct/struct_case_insensitivity.test:28,
/// type_constructor_error_location.test:30, list/recursive_unnest.test:103,
/// list/unnest_group_by.test:107, :122, nested/map/map_error.test:46). Each was answered.
#[test]
fn what_duckdbs_binder_refuses_is_an_error() {
    let e = Engine::open_empty().unwrap();
    for (sql, says) in [
        ("SELECT CAST(NULL AS FLOAT(0))", "at least 1 bit"),
        ("SELECT CAST(NULL AS FLOAT(54))", "less than 54 bits"),
        ("SELECT '0.1'::DECIMAL(40)", "between 1 and 38"),
        ("SELECT '1'::INTEGER(1000)", "syntax error"),
        (
            "SELECT {'hello': 3, 'HELLO': 4}",
            "Duplicate named argument",
        ),
        (
            "SELECT NULL::STRUCT(a INT, a INT)",
            "Duplicate struct entry name",
        ),
        (
            "SELECT UNNEST(UNNEST([[1, 2, 3]]))",
            "Nested UNNEST calls are not supported",
        ),
        (
            "SELECT unnest(['a', 'b']) AS tag, count(*) FROM range(1) GROUP BY ALL",
            "Cannot group on an UNNEST",
        ),
        (
            "SELECT count(*) FROM range(1) GROUP BY unnest({'a': 1, 'b': 2})",
            "UNNEST of struct cannot be used in GROUP BY",
        ),
        ("SELECT MAP(10, 12)", "No function matches"),
    ] {
        let err = error(&e, sql);
        assert!(err.contains(says), "{sql}: {err}");
    }
}

/// `FLOAT(p)` is DOUBLE from 25 bits, as in DuckDB; DataFusion made every `FLOAT(p)` a FLOAT
/// (test/sql/types/numeric/float_precision.test:21).
#[test]
fn a_float_of_25_bits_or_more_is_a_double() {
    let e = Engine::open_empty().unwrap();
    let got = rows(
        &e,
        "SELECT typeof(CAST(NULL AS FLOAT(24))) AS a, typeof(CAST(NULL AS FLOAT(25))) AS b, \
         typeof(CAST(NULL AS FLOAT(53))) AS c",
    );
    assert_eq!(
        got,
        vec![json!({"a": "FLOAT", "b": "DOUBLE", "c": "DOUBLE"})]
    );
}

/// Text cast to BLOB reads `\xAA` as one byte and a BLOB cast to text writes it back, as in DuckDB;
/// Arrow kept the four characters. A number has no cast to BLOB
/// (test/sql/types/blob/test_blob.test:12, test_blob_cast.test:18, :47, test_blob_function.test:44).
#[test]
fn blobs_cast_as_duckdb() {
    let e = Engine::open_empty().unwrap();
    let got = rows(
        &e,
        concat!(
            r"SELECT '\x41\x5a'::BLOB::VARCHAR AS a, '\x5A\x5A'::BLOB = 'ZZ'::BLOB AS b, ",
            r"'\x20\x00\xFF'::BYTEA::VARCHAR AS c, ('\xFF'::BLOB || 'ZZ'::BYTEA)::VARCHAR AS d, ",
            r"'a''b\x5Cc'::BLOB::VARCHAR AS f, TRY_CAST('\xA' AS BLOB) AS g",
        ),
    );
    assert_eq!(
        got,
        vec![json!({
            "a": "AZ",
            "b": true,
            "c": r" \x00\xFF",
            "d": r"\xFFZZ",
            "f": r"a\x27b\x5Cc",
            "g": null,
        })]
    );
    for (sql, says) in [
        ("SELECT 1::BYTEA", "Unimplemented type for cast"),
        ("SELECT 1::bigint::BYTEA", "Unimplemented type for cast"),
        (r"SELECT '\xA'::BLOB", "Invalid hex escape code"),
    ] {
        let err = error(&e, sql);
        assert!(err.contains(says), "{sql}: {err}");
    }
}

/// `x op ANY(list)` and `x op ALL(list)` as DuckDB answers them: over the list's elements as a
/// subquery, so a NULL list has none (test/sql/types/nested/list/any_list.test:26, :41, :74, :103).
/// DataFusion answered `4 = ANY([1, 2, 3, NULL])` false and `1 = ANY(NULL)` NULL.
#[test]
fn quantified_comparisons_over_lists_as_duckdb() {
    let e = Engine::open_empty().unwrap();
    let got = rows(
        &e,
        "SELECT 4 = ANY([1, 2, 3, NULL]) AS a, 1 = ANY(NULL) AS b, 4 > ALL([1, 2, 3, NULL]) AS c, \
         1 = ALL([1, 2, 3, NULL]) AS d, 1 = ANY([1, NULL]) AS f, NULL = ANY([1]) AS g, \
         3 > ANY([5, NULL, 1]) AS h, 1 = ANY([]::INT[]) AS i, 1 = ALL([]::INT[]) AS j",
    );
    assert_eq!(
        got,
        vec![json!({
            "a": null, "b": false, "c": null, "d": false, "f": true, "g": null, "h": true,
            "i": false, "j": true,
        })]
    );
    // Over a column of lists, one with a NULL element and one without.
    let got = rows(
        &e,
        "SELECT 6 = ANY(l) AS a FROM (VALUES ([1, NULL, 3]), ([1, 2])) t(l) ORDER BY 1 NULLS LAST",
    );
    assert_eq!(got, vec![json!({"a": false}), json!({"a": null})]);
}

/// `list_contains` as DuckDB answers it: NULL for a NULL list or needle, false for an empty list,
/// and NULL elements do not make a miss NULL (test/sql/function/list/list_contains.test:211, :221).
#[test]
fn list_contains_as_duckdb() {
    let e = Engine::open_empty().unwrap();
    let got = rows(
        &e,
        "SELECT list_contains([], NULL) AS a, list_contains([1], NULL) AS b, \
         list_contains(NULL::INT[], 1) AS c, list_contains([NULL, 0], 1) AS d, \
         list_contains([NULL, 0], 0) AS f",
    );
    assert_eq!(
        got,
        vec![json!({"a": false, "b": null, "c": null, "d": false, "f": true})]
    );
}

/// A list of numbers has no cast to a list of BLOBs, as a number has none to a BLOB
/// (test/sql/types/nested/array/array_cast.test:72).
#[test]
fn a_list_of_numbers_does_not_cast_to_blobs() {
    let e = Engine::open_empty().unwrap();
    let err = error(&e, "SELECT [1, 2, 3]::BLOB[3]");
    assert!(err.contains("Unimplemented type for cast"), "{err}");
}

/// Text read as a DATE, TIME or TIMESTAMP as DuckDB reads it, and a DATE or TIME written as DuckDB
/// writes it (test/sql/types/date/date_try_cast.test:56, :73, test_bc_dates.test:31,
/// time/time_try_cast.test:16, :21, :72, time_parsing.test:11, :23,
/// timestamp/timestamp_try_cast.test:56, :92). Arrow read neither `(BC)` nor `/`, read `11` as 11
/// nanoseconds and `23:59:60` as midnight, and wrote `14:42:04.350`.
#[test]
fn dates_and_times_parse_and_print_as_duckdb() {
    let e = Engine::open_empty().unwrap();
    let got = rows(
        &e,
        "SELECT try_cast('1111-11-11 (bc)' AS DATE)::VARCHAR AS a, \
         try_cast('2004/02/29' AS DATE)::VARCHAR AS b, '-1000-01-01'::DATE::VARCHAR AS c, \
         try_cast('5881580-07-10' AS DATE)::VARCHAR AS d, try_cast('11' AS TIME) IS NULL AS f, \
         try_cast('11:' AS TIME)::VARCHAR AS g, try_cast('23:59:60' AS TIME) IS NULL AS h, \
         '14:42:04.35'::TIME::VARCHAR AS i, '14:42:04.999999999'::TIME::VARCHAR AS j, \
         try_cast('1111-11-11 11:11' AS TIMESTAMP)::VARCHAR AS k, \
         try_cast('290309-12-22 (BC) 00:00:00' AS TIMESTAMP)::VARCHAR AS l, \
         TRY_CAST('2020-01-01 garbage' AS DATE) AS m",
    );
    assert_eq!(
        got,
        vec![json!({
            "a": "1111-11-11 (BC)",
            "b": "2004-02-29",
            "c": "1001-01-01 (BC)",
            "d": "5881580-07-10",
            "f": true,
            "g": "11:00:00",
            "h": true,
            "i": "14:42:04.35",
            "j": "14:42:04.999999",
            "k": "1111-11-11 11:11:00",
            "l": "290309-12-22 (BC) 00:00:00",
            "m": null,
        })]
    );
    for (sql, says) in [
        ("SELECT '1'::TIME", "time field value out of range"),
        (
            "SELECT CAST('2020-01-01 garbage' AS DATE)",
            "invalid date field format",
        ),
        ("SELECT '5881580-07-10'::date + 1", "Date out of range"),
    ] {
        let err = error(&e, sql);
        assert!(err.contains(says), "{sql}: {err}");
    }
}

/// DuckDB's infinite DATE and TIMESTAMP have no value in Arrow's; they are refused by name, a
/// `TRY_CAST` too, rather than answered NULL (test/sql/types/date/date_try_cast.test:94, :130,
/// timestamp/timestamp_try_cast.test:102).
#[test]
fn infinite_dates_are_refused_by_name() {
    let e = Engine::open_empty().unwrap();
    for sql in [
        "SELECT try_cast('infinity' AS DATE)",
        "SELECT try_cast('-infinity' AS DATE)",
        "SELECT try_cast('infinity' AS TIMESTAMP)",
    ] {
        let err = error(&e, sql);
        assert!(err.contains("not supported"), "{sql}: {err}");
    }
}

/// `date_diff` over a date past what microseconds hold is refused, where it was NULL
/// (test/sql/function/date/date_diff_extreme_dates.test:7).
#[test]
fn date_diff_past_microseconds_is_refused() {
    let e = Engine::open_empty().unwrap();
    let err = error(
        &e,
        "SELECT datediff('week', DATE '-5877641-06-25', DATE '5881580-07-10')",
    );
    assert!(err.contains("not supported"), "{err}");
}
