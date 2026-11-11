//! CheckedArithmetic on the DataFusion path (roadmap 6.3): the overflow corpus from investigation
//! 03, each case answering exactly or refusing, never wrapping and never dropping a row.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int64Array, StringArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, i256};
use arrow::record_batch::RecordBatch;
use burrmill::{BurrmillError, Engine};

const U256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";
const D38MAX: &str = "99999999999999999999999999999999999999";
const D76MAX: &str = "9999999999999999999999999999999999999999999999999999999999999999999999999999";

/// Each batch becomes its own segment file, so a table of several batches is summed in several
/// partitions and the partial/final split is exercised.
fn write_segments(segs: &Path, table: &str, batches: &[RecordBatch]) {
    for (i, b) in batches.iter().enumerate() {
        let f = std::fs::File::create(segs.join(format!("{table}-{i:064x}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, b.schema(), None).unwrap();
        w.write(b).unwrap();
        w.close().unwrap();
    }
}

fn col(name: &str, a: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(name, a)]).unwrap()
}

fn i64s(v: &[i64]) -> RecordBatch {
    col("v", Arc::new(Int64Array::from(v.to_vec())))
}

fn u64s(v: &[u64]) -> RecordBatch {
    col("v", Arc::new(UInt64Array::from(v.to_vec())))
}

fn dec128(v: &[&str]) -> RecordBatch {
    let a = Decimal128Array::from_iter_values(v.iter().map(|s| s.parse::<i128>().unwrap()))
        .with_precision_and_scale(38, 0)
        .unwrap();
    col("v", Arc::new(a))
}

fn dec256(v: &[&str]) -> RecordBatch {
    let a = Decimal256Array::from_iter_values(v.iter().map(|s| i256::from_string(s).unwrap()))
        .with_precision_and_scale(76, 0)
        .unwrap();
    col("v", Arc::new(a))
}

fn engine(tables: &[(&str, Vec<RecordBatch>)]) -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    for (t, b) in tables {
        write_segments(&segs, t, b);
    }
    let e = Engine::open_segments(&segs).unwrap();
    (tmp, e)
}

/// A nest whose `transfer.value` is uint256 text with a `_dec` companion, as nuthatch seals it.
fn transfers(rows: &[&[(&str, &str)]]) -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let batches: Vec<RecordBatch> = rows
        .iter()
        .map(|r| {
            RecordBatch::try_from_iter(vec![
                (
                    "party",
                    Arc::new(StringArray::from(r.iter().map(|x| x.0).collect::<Vec<_>>()))
                        as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(StringArray::from(r.iter().map(|x| x.1).collect::<Vec<_>>()))
                        as ArrayRef,
                ),
            ])
            .unwrap()
        })
        .collect();
    write_segments(&segs, "transfer", &batches);
    write_segments(
        &segs,
        "label",
        &[RecordBatch::try_from_iter(vec![
            (
                "party",
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
            ),
        ])
        .unwrap()],
    );
    std::fs::write(
        tmp.path().join("schema.json"),
        r#"{"tables":[{"table":"transfer","columns":[
            {"name":"party","storage":"text"},{"name":"value","storage":"word32"}]}]}"#,
    )
    .unwrap();
    let e = Engine::open_nest(tmp.path()).unwrap();
    (tmp, e)
}

/// Every cell as text, rows in answer order.
fn rows(e: &Engine, sql: &str) -> Vec<Vec<String>> {
    let batches = e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err:?}"));
    let mut out = Vec::new();
    for b in &batches {
        let cols: Vec<ArrayRef> = b
            .columns()
            .iter()
            .map(|c| arrow::compute::cast(c, &DataType::Utf8).unwrap())
            .collect();
        for i in 0..b.num_rows() {
            out.push(
                cols.iter()
                    .map(|c| {
                        let s = c.as_any().downcast_ref::<StringArray>().unwrap();
                        if s.is_null(i) {
                            "NULL".into()
                        } else {
                            s.value(i).to_string()
                        }
                    })
                    .collect(),
            );
        }
    }
    out
}

fn one(e: &Engine, sql: &str) -> String {
    let r = rows(e, sql);
    assert_eq!(r.len(), 1, "{sql}: {r:?}");
    r[0][0].clone()
}

fn refused(e: &Engine, sql: &str) -> String {
    match e.sql(sql) {
        Ok(b) => panic!(
            "{sql} answered {:?}",
            arrow::util::pretty::pretty_format_batches(&b)
                .unwrap()
                .to_string()
        ),
        Err(
            BurrmillError::NotAllowed(m) | BurrmillError::Plan(m) | BurrmillError::Substrate(m),
        ) => m,
        Err(other) => panic!("{sql}: {other:?}"),
    }
}

/// Refused as overflow, and not as a broken file or a plan it could not make.
fn overflowed(e: &Engine, sql: &str) -> String {
    match e.sql(sql) {
        Err(BurrmillError::Overflow(m)) => m,
        other => panic!("{sql}: {other:?}"),
    }
}

fn result_type(e: &Engine, sql: &str) -> DataType {
    e.sql(sql).unwrap()[0].schema().field(0).data_type().clone()
}

// 1a-1d: DataFusion wraps SUM(Int64); DuckDB widens to HUGEINT. So do we.
#[test]
fn integer_sums_widen_and_stay_exact() {
    let (_t, e) = engine(&[("t", vec![i64s(&[i64::MAX]), i64s(&[1])])]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), "9223372036854775808");
    assert_eq!(
        result_type(&e, "SELECT SUM(v) FROM t"),
        DataType::Decimal128(38, 0)
    );
    assert_eq!(
        one(&e, "SELECT SUM(v) FROM t GROUP BY v % 1"),
        "9223372036854775808"
    );

    let (_t, e) = engine(&[("t", vec![u64s(&[u64::MAX]), u64s(&[u64::MAX])])]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), "36893488147419103230");
}

#[test]
fn order_independent_refusal() {
    let (_t, e) = engine(&[(
        "t",
        vec![dec128(&[D38MAX]), dec128(&["1"]), dec128(&["-1"])],
    )]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), D38MAX);
    let (_t, e) = engine(&[("t", vec![dec128(&[D38MAX]), dec128(&["1"])])]);
    let m = overflowed(&e, "SELECT SUM(v) FROM t");
    assert!(m.contains("100000000000000000000000000000000000000"), "{m}");
}

// 2a-2c: a Decimal128(38,0) sum that leaves 38 digits, or i128, refuses; a partial that does
// while the total does not is harmless.
#[test]
fn decimal128_sums() {
    let (_t, e) = engine(&[("t", vec![dec128(&[D38MAX]), dec128(&[D38MAX])])]);
    overflowed(&e, "SELECT SUM(v) FROM t");
    let (_t, e) = engine(&[(
        "t",
        vec![dec128(&[D38MAX, D38MAX]), dec128(&[&format!("-{D38MAX}")])],
    )]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), D38MAX);
}

// 2d-2f.
#[test]
fn decimal256_sums() {
    let (_t, e) = engine(&[("t", vec![dec256(&[D76MAX]), dec256(&["1"])])]);
    overflowed(&e, "SELECT SUM(v) FROM t");
    let (_t, e) = engine(&[(
        "t",
        vec![dec256(&[D76MAX, D76MAX]), dec256(&[&format!("-{D76MAX}")])],
    )]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), D76MAX);
}

// 2g: DataFusion's AVG refuses when a partial overflows; ours only when the answer does. The
// answer is a DOUBLE, as DuckDB's is for every exact input.
#[test]
fn decimal_avg() {
    let (_t, e) = engine(&[("t", vec![dec128(&["1"]), dec128(&["2"])])]);
    assert_eq!(one(&e, "SELECT AVG(v) FROM t"), "1.5");
    let (_t, e) = engine(&[(
        "t",
        vec![
            dec128(&["99999999999999999999999999999999"]),
            dec128(&["99999999999999999999999999999999"]),
        ],
    )]);
    assert_eq!(one(&e, "SELECT AVG(v) FROM t"), "1e32");
}

// 1e-1h, 2i: scalar arithmetic, folded or on a column.
#[test]
fn scalars_refuse() {
    let (_t, e) = engine(&[("t", vec![i64s(&[i64::MAX, i64::MIN])])]);
    overflowed(&e, "SELECT 9223372036854775807 + 1");
    overflowed(&e, "SELECT 10000000000 * 10000000000");
    overflowed(&e, "SELECT v + 1 FROM t");
    overflowed(&e, "SELECT v * 2 FROM t");
    overflowed(&e, "SELECT v - (-1) FROM t");
    overflowed(&e, "SELECT -v FROM t");
    assert_eq!(
        one(&e, "SELECT v - 1 FROM t WHERE v > 0"),
        "9223372036854775806"
    );
    let (_t, e) = engine(&[("t", vec![dec128(&[D38MAX])])]);
    overflowed(&e, "SELECT v + 1 FROM t");
    assert_eq!(
        one(&e, "SELECT v - 1 FROM t"),
        "99999999999999999999999999999999999998"
    );
}

// #41: the least signed integer modulo -1 overflows in DuckDB, where Arrow's kernel wraps it to 0.
#[test]
fn least_integer_modulo_minus_one_refuses() {
    let (_t, e) = engine(&[("t", vec![i64s(&[i64::MIN, 7])])]);
    for sql in [
        "SELECT CAST(-9223372036854775808 AS BIGINT) % -1",
        "SELECT v % -1 FROM t",
        "SELECT v % CAST(-1 AS INTEGER) FROM t",
        "SELECT CAST(-2147483648 AS INTEGER) % -1",
        "SELECT CAST(-128 AS TINYINT) % CAST(-1 AS TINYINT)",
    ] {
        let m = overflowed(&e, sql);
        assert!(m.contains("Overflow in division"), "{sql}: {m}");
    }
    assert_eq!(one(&e, "SELECT v % -1 FROM t WHERE v > 0"), "0");
    assert_eq!(one(&e, "SELECT v % -2 FROM t WHERE v > 0"), "1");
    assert_eq!(one(&e, "SELECT v % 4 FROM t WHERE v < 0"), "0");
    assert_eq!(one(&e, "SELECT CAST(-2147483647 AS INTEGER) % -1"), "0");
    assert_eq!(one(&e, "SELECT v % 0 FROM t WHERE v > 0"), "NULL");
    // #50: HUGEINT is DECIMAL(38,0), so its least value refuses at the cast (#42), not at the `%`.
    let m = refused(
        &e,
        "SELECT CAST('-170141183460469231731687303715884105728' AS HUGEINT) % -1",
    );
    assert!(m.contains("DECIMAL(38,0)"), "{m}");
    for v in ["-9223372036854775808", &format!("-{D38MAX}")] {
        let sql = format!("SELECT CAST('{v}' AS HUGEINT) % CAST(-1 AS HUGEINT)");
        assert_eq!(one(&e, &sql), "0", "{sql}");
    }
}

// 2k: a literal past u64 would be a float before any rule saw it.
#[test]
fn wide_literals_refused_at_the_surface() {
    let (_t, e) = engine(&[("t", vec![i64s(&[1])])]);
    match e.sql("SELECT 99999999999999999999999999999999999999 + 1") {
        Err(BurrmillError::NotAllowed(m)) => assert!(m.contains("CAST"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        one(&e, "SELECT 18446744073709551615 FROM t"),
        "18446744073709551615"
    );
    assert_eq!(one(&e, "SELECT 1.5 FROM t"), "1.5");
}

#[test]
fn floats_pass_through() {
    let (_t, e) = engine(&[(
        "t",
        vec![col("v", Arc::new(Float64Array::from(vec![1.5, 2.0])))],
    )]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), "3.5");
    assert_eq!(one(&e, "SELECT v * 2 FROM t ORDER BY v LIMIT 1"), "3.0");
}

#[test]
fn unknown_exact_functions_are_refused() {
    let (_t, e) = engine(&[("t", vec![i64s(&[3])])]);
    let m = refused(&e, "SELECT v << 70 FROM t");
    assert!(m.contains("Left-shift value 70 is out of range"), "{m}");
    assert_eq!(one(&e, "SELECT v << 2 FROM t"), "12");
    let m = refused(&e, "SELECT factorial(v) FROM t");
    assert!(m.contains("factorial"), "{m}");
    assert_eq!(one(&e, "SELECT abs(v) FROM t"), "3");
    assert_eq!(one(&e, "SELECT count(*) FROM t"), "1");
}

#[test]
fn window_sums_are_checked() {
    let (_t, e) = engine(&[("t", vec![i64s(&[i64::MAX, 1])])]);
    let r = rows(&e, "SELECT SUM(v) OVER () FROM t");
    assert_eq!(r, vec![vec!["9223372036854775808".to_string()]; 2]);
    let r = rows(
        &e,
        "SELECT SUM(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
    );
    assert_eq!(
        r,
        vec![
            vec!["1".to_string()],
            vec!["9223372036854775808".to_string()]
        ]
    );
}

// 3a-3f: nuthatch's `_dec`. A row the cast reports as NULL is not a term of its sum.
#[test]
fn dec_companion_sums_are_exact() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", "7")], &[("a", "10")]]);
    assert_eq!(one(&e, "SELECT SUM(value_dec) FROM transfer"), "22");
    assert_eq!(
        rows(
            &e,
            "SELECT party, SUM(value_dec) FROM transfer GROUP BY party ORDER BY party"
        ),
        vec![
            vec!["a".to_string(), "15".into()],
            vec!["b".to_string(), "7".into()]
        ]
    );
    assert_eq!(
        result_type(&e, "SELECT SUM(value_dec) FROM transfer"),
        DataType::Decimal128(38, 0)
    );

    // One party's value does not fit DECIMAL(38,0). The column is NULL there, so the sum is 5.
    // The raw text still sums exactly, and two values that each fit can still overflow the total.
    let (_t, e) = transfers(&[&[("a", "5"), ("a", U256_MAX)]]);
    let total = "115792089237316195423570985008687907853269984665640564039457584007913129639940";
    assert_eq!(one(&e, "SELECT SUM(value_dec) FROM transfer"), "5");
    assert_eq!(
        one(&e, "SELECT checked_sum_text(value) FROM transfer"),
        total
    );
    assert_eq!(
        one(
            &e,
            "SELECT SUM(TRY_CAST(value AS DECIMAL(38,0))) FROM transfer"
        ),
        "5"
    );
    let (_t, e) = transfers(&[&[("a", D38MAX), ("a", D38MAX)]]);
    let m = overflowed(&e, "SELECT SUM(value_dec) FROM transfer");
    assert!(m.contains("does not fit"), "{m}");
}

/// 10^38 does not fit `DECIMAL(38,0)` and -10^38+5 does. Their exact sum is 5, so summing the
/// source text answers 5 where the column holds one value.
#[test]
fn sum_of_a_dec_column_skips_rows_the_column_reports_as_null() {
    const TEN38: &str = "100000000000000000000000000000000000000";
    const FITS: &str = "-99999999999999999999999999999999999995";
    let (_t, e) = transfers(&[&[("a", TEN38), ("a", FITS), ("b", U256_MAX), ("b", "7")]]);
    assert_eq!(
        one(&e, "SELECT SUM(value_dec) FROM transfer WHERE party = 'a'"),
        FITS
    );
    assert_eq!(
        one(
            &e,
            "SELECT SUM(value_dec) FILTER (WHERE NOT value_overflow) FROM transfer \
             WHERE party = 'a'"
        ),
        FITS
    );
    assert_eq!(
        one(&e, "SELECT AVG(value_dec) FROM transfer WHERE party = 'a'"),
        "-1e38"
    );
    assert_eq!(
        one(&e, "SELECT SUM(value_dec) FROM transfer WHERE party = 'b'"),
        "7"
    );
    assert_eq!(
        one(
            &e,
            "SELECT SUM(TRY_CAST(value AS BIGINT)) FROM transfer WHERE party = 'a'"
        ),
        "NULL"
    );
    assert_eq!(
        rows(
            &e,
            "SELECT party, SUM(value_dec) FROM transfer GROUP BY party ORDER BY party"
        ),
        vec![
            vec!["a".to_string(), FITS.into()],
            vec!["b".to_string(), "7".into()],
        ]
    );

    let (_t, e) = transfers(&[&[("a", "1e3"), ("a", "4")]]);
    assert_eq!(
        one(&e, "SELECT value_dec FROM transfer WHERE value = '1e3'"),
        "1000"
    );
    assert_eq!(one(&e, "SELECT SUM(value_dec) FROM transfer"), "1004");
}

#[test]
fn dec_companion_through_subqueries_and_joins() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", "7")], &[("a", "10")]]);
    assert_eq!(
        one(
            &e,
            "SELECT SUM(x) FROM (SELECT value_dec AS x FROM transfer WHERE party = 'a') s"
        ),
        "15"
    );
    assert_eq!(
        rows(
            &e,
            "SELECT l.name, SUM(t.value_dec) FROM transfer t JOIN label l ON t.party = l.party \
             GROUP BY l.name ORDER BY l.name"
        ),
        vec![
            vec!["alice".to_string(), "15".into()],
            vec!["bob".to_string(), "7".into()]
        ]
    );
    assert_eq!(
        one(
            &e,
            "WITH c AS (SELECT party, value_dec FROM transfer) SELECT SUM(value_dec) FROM c"
        ),
        "22"
    );
}

/// Any other aggregate over a `_dec` value answers while every value fits, and refuses once one did
/// not, as DuckDB would have left that row out (Chief, 2026-09-29). Window aggregates still refuse.
#[test]
fn other_reads_of_dec_companions_in_aggregates_refuse_only_what_did_not_fit() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", "7")]]);
    assert_eq!(one(&e, "SELECT MAX(value_dec) FROM transfer"), "7");
    assert_eq!(
        one(
            &e,
            "SELECT MIN(TRY_CAST(value AS DECIMAL(38,0))) FROM transfer"
        ),
        "5"
    );
    assert_eq!(one(&e, "SELECT count(value_dec) FROM transfer"), "2");
    assert_eq!(one(&e, "SELECT SUM(value_dec * 2) FROM transfer"), "24");
    assert_eq!(
        rows(
            &e,
            "SELECT l.name, MAX(t.value_dec * 3) FROM transfer t JOIN label l ON t.party = l.party GROUP BY l.name ORDER BY l.name"
        ),
        vec![
            vec!["alice".to_string(), "15".into()],
            vec!["bob".to_string(), "21".into()]
        ]
    );
    refused(&e, "SELECT SUM(value_dec) OVER () FROM transfer");

    // Reading the column itself is not an aggregate dropping rows: NULL is visible.
    assert_eq!(
        rows(
            &e,
            "SELECT lag(value_dec) OVER (ORDER BY party) FROM transfer"
        )
        .len(),
        2
    );
    assert_eq!(
        rows(&e, "SELECT value_dec FROM transfer ORDER BY party").len(),
        2
    );
    assert_eq!(
        one(&e, "SELECT count(*) FROM transfer WHERE value_dec > 6"),
        "1"
    );

    let (_t, e) = transfers(&[&[("a", "5"), ("b", U256_MAX)]]);
    for sql in [
        "SELECT MAX(value_dec) FROM transfer",
        "SELECT MIN(TRY_CAST(value AS DECIMAL(38,0))) FROM transfer",
        "SELECT count(value_dec) FROM transfer",
        "SELECT SUM(value_dec * 2) FROM transfer",
    ] {
        let m = refused(&e, sql);
        assert!(m.contains("did not fit"), "{sql}: {m}");
    }
    // A party whose own values all fit is still answered when grouped away from the one that did not.
    assert_eq!(
        one(&e, "SELECT MAX(value_dec) FROM transfer WHERE party = 'a'"),
        "5"
    );
}

// The signed fold nuthatch writes: credits and negated debits of a TRY_CAST, through UNION ALL.
#[test]
fn signed_fold_over_try_cast() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", "7")], &[("a", "10")]]);
    let fold = |cast: &str| {
        format!(
            "SELECT addr, SUM(d) FROM ( \
               SELECT party AS addr, {cast}(value AS DECIMAL(38,0)) AS d FROM transfer \
               UNION ALL \
               SELECT 'sink' AS addr, -{cast}(value AS DECIMAL(38,0)) AS d FROM transfer \
             ) GROUP BY addr ORDER BY addr"
        )
    };
    let want = vec![
        vec!["a".to_string(), "15".into()],
        vec!["b".to_string(), "7".into()],
        vec!["sink".to_string(), "-22".into()],
    ];
    assert_eq!(rows(&e, &fold("TRY_CAST")), want);
    assert_eq!(rows(&e, &fold("CAST")), want);
    assert_eq!(
        one(
            &e,
            "SELECT SUM(x) FROM (SELECT value_dec AS x FROM transfer UNION ALL SELECT -value_dec FROM transfer) u"
        ),
        "0"
    );
    assert_eq!(
        one(
            &e,
            "SELECT SUM(x) FROM (SELECT value_dec AS x FROM transfer UNION ALL SELECT CAST(1 AS DECIMAL(38,0))) u"
        ),
        "23"
    );

    // A uint256 credit and its debit: DuckDB drops both rows; the exact fold refuses both parties.
    let (_t, e) = transfers(&[&[("a", "5"), ("a", U256_MAX)]]);
    // Refused either way: by the checked sum, or, where the owned fold is substituted, on reading.
    let m = refused(&e, &fold("TRY_CAST"));
    assert!(
        m.contains("does not fit Decimal128(38, 0)")
            || m.contains("refuses it rather than dropping"),
        "{m}"
    );
}

/// A recursive fold with no `TRY_CAST` in it is not lossy: its aggregates answer. One whose seed
/// carries a `TRY_CAST` still refuses where a value did not fit, whichever column it ends in.
#[test]
fn a_recursive_fold_is_tainted_only_by_what_it_casts() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", "7")]]);
    assert_eq!(
        one(
            &e,
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT max(n) FROM t"
        ),
        "3"
    );
    let (_t, e) = transfers(&[&[("a", "5"), ("b", U256_MAX)]]);
    let m = refused(
        &e,
        "WITH RECURSIVE t(x, y, k) AS (SELECT CAST(0 AS DECIMAL(38,0)) AS x, value_dec AS y, 0 AS k FROM transfer \
         UNION ALL SELECT y, x, k + 1 FROM t WHERE k < 1) SELECT max(x) FROM t",
    );
    assert!(m.contains("TRY_CAST") || m.contains("did not fit"), "{m}");
}

/// A fold that carries a table's `_dec` companions along, as `SELECT *` does, sums its own clean
/// column: the companions taint only themselves, round after round.
#[test]
fn a_recursive_fold_carrying_dec_columns_sums_a_clean_one() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", U256_MAX)]]);
    assert_eq!(
        one(
            &e,
            "WITH RECURSIVE f AS (SELECT party, value, value_dec, value_overflow, CAST(1 AS DECIMAL(38,0)) AS amt, 1 AS k \
             FROM transfer UNION ALL SELECT party, value, value_dec, value_overflow, amt, k + 1 FROM f WHERE k < 2) \
             SELECT sum(amt) FROM f"
        ),
        "4"
    );
}

/// A lateral derived table's columns carry only their own taint: a clean `CAST` summed through one
/// answers, where the whole derived table had counted as lossy.
#[test]
fn a_correlated_subquery_is_tainted_only_by_what_it_casts() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", U256_MAX)]]);
    assert_eq!(
        one(
            &e,
            "SELECT sum(d.x) FROM transfer t CROSS JOIN LATERAL \
             (SELECT CAST(1 AS DECIMAL(38,0)) AS x FROM label l WHERE l.party = t.party) d"
        ),
        "2"
    );
}

/// An unnested list carries the taint of what was put in it and no more: a count over the elements
/// of a clean list answers (the QoS postings view counts buckets out of unnested JSON), and a sum
/// over a list that held a `TRY_CAST` is still refused.
#[test]
fn an_unnested_list_is_tainted_only_by_what_went_into_it() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", U256_MAX)]]);
    assert_eq!(
        one(
            &e,
            "SELECT count(DISTINCT x.v) FROM (SELECT unnest([1, 2, 2]) AS v FROM transfer) x"
        ),
        "2"
    );
    let why = refused(
        &e,
        "SELECT max(x.v) FROM (SELECT unnest([TRY_CAST(value AS HUGEINT)]) AS v FROM transfer) x",
    );
    assert!(why.contains("TRY_CAST"), "{why}");
}

/// #44: HUGEINT is `DECIMAL(38,0)` here, so 10^38 to 2^127 - 1 cannot be held, while DuckDB's
/// `TRY_CAST(... AS HUGEINT)` answers them. Where the value itself is read it refuses by name; a
/// NULL test and a sum answer what DuckDB answers. `AS DECIMAL(38,0)` is NULL there, as in DuckDB.
#[test]
fn hugeint_try_cast_past_38_digits_refuses_or_answers_as_duckdb() {
    const WINDOW: &str = "150000000000000000000000000000000000000";
    const LEAST: &str = "-170141183460469231731687303715884105728";
    const BACK: &str = "-149999999999999999999999999999999999999";
    let (_t, e) = transfers(&[&[("w", WINDOW), ("u", U256_MAX), ("f", "5"), ("l", LEAST)]]);
    for p in ["w", "l"] {
        let m = refused(
            &e,
            &format!("SELECT TRY_CAST(value AS HUGEINT) FROM transfer WHERE party = '{p}'"),
        );
        assert!(m.contains("HUGEINT"), "{p}: {m}");
    }
    assert_eq!(
        one(
            &e,
            "SELECT TRY_CAST(value AS DECIMAL(38,0)) FROM transfer WHERE party = 'w'"
        ),
        "NULL"
    );
    assert_eq!(
        rows(
            &e,
            "SELECT party, TRY_CAST(value AS HUGEINT) IS NULL, TRY_CAST(value AS HUGEINT) IS NOT NULL \
             FROM transfer ORDER BY party"
        ),
        vec![
            vec!["f".to_string(), "false".into(), "true".into()],
            vec!["l".to_string(), "false".into(), "true".into()],
            vec!["u".to_string(), "true".into(), "false".into()],
            vec!["w".to_string(), "false".into(), "true".into()],
        ]
    );
    assert_eq!(
        one(
            &e,
            "SELECT count(*) FROM transfer WHERE value IS NOT NULL AND TRY_CAST(value AS HUGEINT) IS NULL"
        ),
        "1"
    );

    let (_t, e) = transfers(&[&[("a", WINDOW), ("a", BACK), ("a", U256_MAX), ("a", "x")]]);
    assert_eq!(
        one(&e, "SELECT SUM(TRY_CAST(value AS HUGEINT)) FROM transfer"),
        "1"
    );
    assert_eq!(
        one(
            &e,
            "SELECT SUM(d) FROM (SELECT TRY_CAST(value AS HUGEINT) AS d FROM transfer) s"
        ),
        "1"
    );
    assert_eq!(
        one(
            &e,
            "SELECT SUM(d) FROM (SELECT TRY_CAST(value AS HUGEINT) AS d FROM transfer UNION ALL \
             SELECT -TRY_CAST(value AS HUGEINT) FROM transfer) u"
        ),
        "0"
    );
    // The signed fold refuses uint256 text where substituted, so this one holds only what fits i128.
    let (_t, f) = transfers(&[&[("a", WINDOW), ("a", BACK)]]);
    assert_eq!(
        rows(
            &f,
            "SELECT addr, SUM(d) FROM ( \
               SELECT party AS addr, TRY_CAST(value AS HUGEINT) AS d FROM transfer \
               UNION ALL \
               SELECT 'sink' AS addr, -TRY_CAST(value AS HUGEINT) AS d FROM transfer \
             ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"
        ),
        vec![
            vec!["a".to_string(), "1".into()],
            vec!["sink".to_string(), "-1".into()]
        ]
    );
    let fold = "SELECT addr, SUM(d) FROM ( \
                SELECT party AS addr, TRY_CAST(value AS HUGEINT) AS d FROM transfer \
                UNION ALL \
                SELECT 'sink' AS addr, -TRY_CAST(value AS HUGEINT) AS d FROM transfer \
                ) GROUP BY addr";
    let plan: String = f
        .sql(&format!("EXPLAIN {fold}"))
        .unwrap()
        .iter()
        .map(|b| {
            arrow::util::pretty::pretty_format_batches(std::slice::from_ref(b))
                .unwrap()
                .to_string()
        })
        .collect();
    assert!(plan.contains("OwnedSignedFold"), "{plan}");
    let m = refused(&e, "SELECT MAX(TRY_CAST(value AS HUGEINT)) FROM transfer");
    assert!(m.contains("HUGEINT") || m.contains("did not fit"), "{m}");

    // Each fits HUGEINT and their sum does not fit DECIMAL(38,0): refused, as DuckDB refuses past 2^127.
    let (_t, e) = transfers(&[&[("a", WINDOW), ("a", WINDOW)]]);
    let m = overflowed(&e, "SELECT SUM(TRY_CAST(value AS HUGEINT)) FROM transfer");
    assert!(m.contains("does not fit"), "{m}");
}

/// #63: #44's band from a float. DuckDB's `TRY_CAST(x AS HUGEINT)` answers a DOUBLE or FLOAT that
/// rounds to under 2^127 in magnitude, 10^38 and past included; here the value refuses by name, and
/// its NULL test and its sum answer as DuckDB's do. Expected values measured against DuckDB 1.5.
#[test]
fn hugeint_try_cast_of_a_float_past_38_digits_refuses_or_answers_as_duckdb() {
    let two127 = 2f64.powi(127);
    let below = f64::from_bits(two127.to_bits() - 1);
    let doubles = col(
        "v",
        Arc::new(Float64Array::from(vec![
            1.5e38,
            -1.5e38,
            1e38,
            2.5,
            f64::NAN,
            two127,
            -two127,
            below,
        ])),
    );
    let floats = col(
        "v",
        Arc::new(arrow::array::Float32Array::from(vec![1.5e38f32, -1.6e38])),
    );
    let (_t, e) = engine(&[("d", vec![doubles]), ("f", vec![floats])]);
    for w in ["1.5e38", "-1.5e38", "1.7014118346046921e38"] {
        let m = refused(
            &e,
            &format!("SELECT TRY_CAST(v AS HUGEINT) FROM d WHERE v = {w}"),
        );
        assert!(m.contains("HUGEINT"), "{w}: {m}");
    }
    let m = refused(
        &e,
        "SELECT TRY_CAST(v AS HUGEINT) + 1 FROM d WHERE v = 1.5e38",
    );
    assert!(m.contains("HUGEINT"), "{m}");
    let m = refused(&e, "SELECT TRY_CAST(v AS HUGEINT) FROM f WHERE v > 0");
    assert!(m.contains("HUGEINT"), "{m}");
    assert_eq!(
        rows(
            &e,
            "SELECT TRY_CAST(v AS HUGEINT) FROM d WHERE v = 1e38 OR v = 2.5 OR isnan(v) OR abs(v) >= 1.7014118346046923e38 ORDER BY v"
        ),
        vec![
            vec!["NULL".to_string()],
            vec!["2".to_string()],
            vec!["99999999999999997748809823456034029568".to_string()],
            vec!["NULL".to_string()],
            vec!["NULL".to_string()],
        ]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT TRY_CAST(v AS HUGEINT) IS NULL, TRY_CAST(v AS HUGEINT) IS NOT NULL FROM d ORDER BY v"
        )
        .into_iter()
        .map(|r| r.join(" "))
        .collect::<Vec<_>>(),
        vec![
            "true false",
            "false true",
            "false true",
            "false true",
            "false true",
            "false true",
            "true false",
            "true false",
        ]
    );
    assert_eq!(
        one(
            &e,
            "SELECT count(*) FROM f WHERE TRY_CAST(v AS HUGEINT) IS NOT NULL"
        ),
        "2"
    );

    // 1.5e38 and -1.5e38 cancel; NaN and 2^127 are NULL in DuckDB's cast and left out of its sum.
    let sum = "SELECT SUM(TRY_CAST(v AS HUGEINT)) FROM d WHERE abs(v) <> 1e38 AND abs(v) <> 1.7014118346046921e38";
    assert_eq!(one(&e, sum), "2");
    assert_eq!(
        one(
            &e,
            "SELECT SUM(h) FROM (SELECT TRY_CAST(v AS HUGEINT) AS h FROM d \
             WHERE abs(v) <> 1e38 AND abs(v) <> 1.7014118346046921e38) s"
        ),
        "2"
    );
    assert_eq!(
        one(
            &e,
            "SELECT SUM(h) FROM (SELECT TRY_CAST(v AS HUGEINT) AS h FROM d UNION ALL \
             SELECT -TRY_CAST(v AS HUGEINT) FROM d) u"
        ),
        "0"
    );
    assert_eq!(
        one(&e, "SELECT SUM(TRY_CAST(v AS HUGEINT)) FROM f"),
        "-9999998666165212282482104879554756608"
    );
    let m = overflowed(
        &e,
        "SELECT SUM(TRY_CAST(v AS HUGEINT)) FROM d WHERE v > 1e38",
    );
    assert!(m.contains("does not fit"), "{m}");
}
