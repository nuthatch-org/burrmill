//! DECIMAL past 38 digits (#349): up to 76 digits a column round-trips through Parquet, adds,
//! subtracts and multiplies exactly or is refused, and past 76 it is refused. HUGEINT stays the
//! 38-digit `DECIMAL(38,0)` nuthatch's plans are written against.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Decimal256Type, i256};
use burrmill::{BurrmillError, Engine};

fn engine() -> Engine {
    Engine::builder().open().unwrap()
}

fn one(e: &Engine, sql: &str) -> Result<String, String> {
    let b = e.sql(sql).map_err(|e| e.to_string())?;
    Ok(arrow::util::display::array_value_to_string(b[0].column(0), 0).unwrap())
}

fn nines(n: usize) -> String {
    "9".repeat(n)
}

#[test]
fn a_76_digit_column_round_trips_through_parquet() {
    let e = engine();
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("wide.parquet");
    let sql = format!(
        "SELECT CAST('{}' AS DECIMAL(76,0)) AS a, CAST('-{}.25' AS DECIMAL(60,2)) AS b \
         UNION ALL SELECT CAST(0 AS DECIMAL(76,0)), NULL",
        nines(76),
        nines(58)
    );
    assert_eq!(e.write_parquet(&sql, &path).unwrap(), 2);
    let back = Engine::builder().parquet(&path, "t").open().unwrap();
    assert_eq!(
        back.describe("SELECT a, b FROM t").unwrap(),
        [
            ("a".to_string(), "DECIMAL(76,0)".to_string()),
            ("b".into(), "DECIMAL(60,2)".into())
        ]
    );
    let b = back.sql("SELECT a, b FROM t ORDER BY a DESC").unwrap();
    let a = b[0].column(0).as_primitive::<Decimal256Type>();
    assert_eq!(a.value(0).to_string(), nines(76));
    assert_eq!(
        arrow::util::display::array_value_to_string(b[0].column(1), 0).unwrap(),
        format!("-{}.25", nines(58))
    );
    assert!(b[0].column(1).is_null(1));
}

#[test]
fn arithmetic_at_76_digits_is_exact_or_refused() {
    let e = engine();
    let max = format!("CAST('{}' AS DECIMAL(76,0))", nines(76));
    let half = format!("CAST('{}' AS DECIMAL(75,0))", nines(75));
    assert_eq!(
        one(&e, &format!("SELECT {max} - 1")).unwrap(),
        format!("{}8", nines(75))
    );
    assert_eq!(
        one(&e, &format!("SELECT {half} + {half}")).unwrap(),
        format!("1{}8", nines(74))
    );
    assert_eq!(
        one(&e, &format!("SELECT {half} * 2")).unwrap(),
        format!("1{}8", nines(74))
    );
    for sql in [
        format!("SELECT {max} + 1"),
        format!("SELECT -{max} - 1"),
        format!("SELECT {max} * 2"),
        format!("SELECT {half} * {half}"),
        format!("SELECT SUM(x) FROM (VALUES ({max}), ({max})) t(x)"),
    ] {
        let err = e.sql(&sql).unwrap_err();
        assert!(matches!(err, BurrmillError::Overflow(_)), "{sql}: {err}");
    }
    let err = e
        .sql("SELECT CAST('1' AS DECIMAL(77,0))")
        .unwrap_err()
        .to_string();
    assert!(err.contains("precision <= 76"), "{err}");
    let err = e
        .sql(&format!("SELECT CAST({max} AS DECIMAL(38,0))"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("Overflowing"), "{err}");
}

#[test]
fn hugeint_is_still_38_digits() {
    let e = engine();
    assert_eq!(
        e.describe("SELECT CAST(5 AS HUGEINT) * 3 AS h").unwrap()[0].1,
        "DECIMAL(38,0)"
    );
    let big = format!("CAST('{}' AS HUGEINT)", nines(38));
    assert!(matches!(
        e.sql(&format!("SELECT {big} + 1")).unwrap_err(),
        BurrmillError::Overflow(_)
    ));
    let b = e.sql("SELECT CAST(7 AS HUGEINT) AS h").unwrap();
    assert_eq!(b[0].column(0).data_type(), &DataType::Decimal128(38, 0));
}

#[test]
fn strict_decimals_hold_at_76_digits() {
    let e = engine();
    e.set_strict_decimals(true);
    let wide = |v: &str, s: u32| format!("CAST('{v}' AS DECIMAL(70,{s}))");
    let err = one(
        &e,
        &format!("SELECT CAST({} AS DECIMAL(70,2))", wide("1.005", 3)),
    )
    .unwrap_err();
    assert!(err.contains("digits past"), "{err}");
    assert_eq!(
        one(
            &e,
            &format!("SELECT CAST({} AS DECIMAL(70,2))", wide("1.000", 3))
        )
        .unwrap(),
        "1.00"
    );
    let err = one(&e, "SELECT CAST('1.005' AS DECIMAL(70,2))").unwrap_err();
    assert!(err.contains("digits past"), "{err}");
    let err = one(&e, &format!("SELECT div({}, 3, 2, 'down')", wide("1.5", 1))).unwrap_err();
    assert!(err.contains("up to 38 digits"), "{err}");
}

/// Sums, differences and comparisons of random values up to 76 digits against `i256`.
#[test]
fn generated_wide_arithmetic_matches_an_exact_oracle() {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }
    let e = engine();
    let mut r = Rng(0x0d0c_7676_beef_u64);
    let ten = i256::from_i128(10);
    let limit = (0..76).fold(i256::ONE, |x, _| x * ten);
    let (mut exact, mut refused) = (0, 0);
    for case in 0..400 {
        let digits = 39 + r.below(38) as usize;
        let value = |r: &mut Rng| {
            let mut v = i256::ZERO;
            for _ in 0..digits {
                v = v * ten + i256::from_i128(r.below(10) as i128);
            }
            if r.below(2) == 0 { -v } else { v }
        };
        let (a, b) = (value(&mut r), value(&mut r));
        let lit = |v: i256| format!("CAST('{v}' AS DECIMAL(76,0))");
        let (op, want) = match r.below(3) {
            0 => ("+", a + b),
            1 => ("-", a - b),
            _ => ("<", if a < b { i256::ONE } else { i256::ZERO }),
        };
        let sql = format!("SELECT {} {op} {}", lit(a), lit(b));
        match one(&e, &sql) {
            Ok(got) if op == "<" => {
                assert_eq!(got == "true", want == i256::ONE, "case {case}: {sql}");
                exact += 1;
            }
            Ok(got) => {
                assert!(
                    want.wrapping_abs() < limit,
                    "case {case}: {sql} answered {got}"
                );
                assert_eq!(got, want.to_string(), "case {case}: {sql}");
                exact += 1;
            }
            Err(m) => {
                assert!(
                    op != "<" && want.wrapping_abs() >= limit,
                    "case {case}: {sql} refused ({m}) but {want} fits"
                );
                refused += 1;
            }
        }
    }
    assert!(exact > 300, "{exact} exact, {refused} refused");
}
