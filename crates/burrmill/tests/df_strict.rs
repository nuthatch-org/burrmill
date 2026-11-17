//! Strict decimals (#339): every DECIMAL answer is exact at its declared scale or refused, checked
//! case by case and then over a generated corpus against an `i256` oracle. DuckDB parity stays the
//! default, so the same statements are also run without the mode.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Decimal128Type, i256};
use burrmill::Engine;

fn strict() -> Engine {
    let e = Engine::builder().open().unwrap();
    e.set_strict_decimals(true);
    e
}

/// The single value `sql` answers, as text, or the refusal.
fn one(e: &Engine, sql: &str) -> Result<String, String> {
    let b = e.sql(sql).map_err(|e| e.to_string())?;
    let c = b[0].column(0);
    Ok(arrow::util::display::array_value_to_string(c, 0).unwrap())
}

fn refused(e: &Engine, sql: &str, says: &str) {
    match one(e, sql) {
        Ok(v) => panic!("{sql} answered {v}"),
        Err(m) => assert!(m.contains(says), "{sql}: {m}"),
    }
}

#[test]
fn parity_is_the_default() {
    let e = Engine::builder().open().unwrap();
    let sql = "SELECT CAST(CAST(1.005 AS DECIMAL(10,3)) AS DECIMAL(10,2))";
    assert_eq!(one(&e, sql).unwrap(), "1.01");
    assert!(one(&e, "SELECT CAST(1.5 AS DECIMAL(10,1)) / 3").is_ok());
}

#[test]
fn a_rescale_that_drops_digits_refuses_and_round_is_how_to_ask() {
    let e = strict();
    let x = "CAST(1.005 AS DECIMAL(10,3))";
    refused(
        &e,
        &format!("SELECT CAST({x} AS DECIMAL(10,2))"),
        "digits past",
    );
    assert_eq!(
        one(&e, &format!("SELECT CAST(round({x}, 2) AS DECIMAL(10,2))")).unwrap(),
        "1.01"
    );
    assert_eq!(
        one(
            &e,
            "SELECT CAST(CAST(1.500 AS DECIMAL(10,3)) AS DECIMAL(10,1))"
        )
        .unwrap(),
        "1.5"
    );
    assert_eq!(
        one(&e, &format!("SELECT TRY_CAST({x} AS DECIMAL(10,2))")).unwrap(),
        ""
    );
    refused(&e, "SELECT CAST('1.005' AS DECIMAL(10,2))", "digits past");
    assert_eq!(
        one(&e, "SELECT CAST(' 1.50' AS DECIMAL(10,1))").unwrap(),
        "1.5"
    );
    refused(
        &e,
        "SELECT CAST(CAST(2.5 AS DECIMAL(10,1)) AS INTEGER)",
        "digits past",
    );
    assert_eq!(
        one(&e, "SELECT CAST(CAST(2.0 AS DECIMAL(10,1)) AS INTEGER)").unwrap(),
        "2"
    );
    refused(
        &e,
        "SELECT CAST(CAST(12.5 AS DECIMAL(10,1)) AS DECIMAL(3,2))",
        "too large",
    );
}

#[test]
fn division_needs_a_scale_and_a_rounding() {
    let e = strict();
    refused(&e, "SELECT CAST(1.5 AS DECIMAL(10,1)) / 3", "div(");
    refused(&e, "SELECT 1 / 2", "div(");
    assert_eq!(one(&e, "SELECT 7 // 2").unwrap(), "3");
    assert_eq!(
        one(&e, "SELECT div(10, 3, 4, 'half_even')").unwrap(),
        "3.3333"
    );
    assert_eq!(
        one(&e, "SELECT div(1.005, 1, 2, 'half_even')").unwrap(),
        "1.00"
    );
    assert_eq!(
        one(&e, "SELECT div(1.005, 1, 2, 'half_up')").unwrap(),
        "1.01"
    );
    assert_eq!(
        one(&e, "SELECT div(-1.005, 1, 2, 'floor')").unwrap(),
        "-1.01"
    );
    refused(&e, "SELECT div(1, 0, 2, 'down')", "division by zero");
    refused(&e, "SELECT div(1, 3, 2, 'nearest')", "rounding is one of");
    refused(
        &e,
        "SELECT div(1, 3, x, 'down') FROM (VALUES (2)) t(x)",
        "constant",
    );
    assert_eq!(
        e.describe("SELECT div(1, 3, 6, 'down')").unwrap()[0].1,
        "DECIMAL(38,6)"
    );
}

#[test]
fn nothing_becomes_a_double_unless_written() {
    let e = strict();
    let t = "FROM (VALUES (CAST(1.5 AS DECIMAL(10,1))), (CAST(2.5 AS DECIMAL(10,1)))) t(x)";
    refused(&e, &format!("SELECT AVG(x) {t}"), "div(SUM(x)");
    refused(&e, &format!("SELECT stddev(x) {t}"), "DOUBLE");
    refused(&e, &format!("SELECT sqrt(x) {t}"), "DOUBLE");
    refused(&e, &format!("SELECT x + CAST(1 AS DOUBLE) {t}"), "unasked");
    assert_eq!(one(&e, &format!("SELECT SUM(x) {t}")).unwrap(), "4.0");
    assert_eq!(
        one(
            &e,
            &format!("SELECT div(SUM(x), COUNT(x), 2, 'half_even') {t}")
        )
        .unwrap(),
        "2.00"
    );
    assert_eq!(
        one(&e, &format!("SELECT sqrt(CAST(MAX(x) AS DOUBLE)) {t}")).unwrap(),
        "1.5811388300841898"
    );
    assert_eq!(one(&e, "SELECT CAST(1.5 AS DOUBLE) / 2").unwrap(), "0.75");
}

// The generated corpus. Each case is one operation over literals of random precision and scale,
// and the answer is compared with the exact one as canonical text.

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

#[derive(Clone, Copy)]
struct Dec {
    v: i128,
    p: u32,
    s: u32,
}

impl Dec {
    fn sql(&self) -> String {
        format!(
            "CAST('{}' AS DECIMAL({},{}))",
            text(i256::from_i128(self.v), self.s),
            self.p,
            self.s
        )
    }
}

fn gen_dec(r: &mut Rng, max_p: u32) -> Dec {
    let p = 1 + r.below(max_p as u64) as u32;
    let s = r.below(p as u64 + 1) as u32;
    let digits = 1 + r.below(p as u64) as u32;
    let mut v: i128 = 0;
    for _ in 0..digits {
        v = v * 10 + r.below(10) as i128;
    }
    // Trailing zeros, often, so a narrowing cast sometimes keeps every digit.
    if r.below(3) == 0 && s > 0 {
        let z = 1 + r.below(s as u64) as u32;
        let cap = 10i128.pow(p - z);
        v = (v % cap) * 10i128.pow(z);
    }
    if r.below(2) == 0 {
        v = -v;
    }
    Dec { v, p, s }
}

/// `v` at scale `s` as text with no trailing fractional zeros.
fn text(v: i256, s: u32) -> String {
    let neg = v < i256::ZERO;
    let digits = v.wrapping_abs().to_string();
    let s = s as usize;
    let padded = format!("{digits:0>width$}", width = s + 1);
    let (w, f) = padded.split_at(padded.len() - s);
    let f = f.trim_end_matches('0');
    let body = if f.is_empty() {
        w.to_string()
    } else {
        format!("{w}.{f}")
    };
    if neg && body != "0" {
        format!("-{body}")
    } else {
        body
    }
}

/// `(p, s)` of `DECIMAL(p,s)`.
fn decimal_type(t: &str) -> Option<(u32, u32)> {
    let (p, s) = t
        .strip_prefix("DECIMAL(")?
        .strip_suffix(')')?
        .split_once(',')?;
    Some((p.parse().ok()?, s.parse().ok()?))
}

/// The digits and scale canonical text needs: `-0.0025` is 2 digits at scale 4.
fn canonical(t: &str) -> (u32, u32) {
    let t = t.trim_start_matches('-');
    let (w, f) = t.split_once('.').unwrap_or((t, ""));
    let w = w.trim_start_matches('0');
    let digits = if w.is_empty() {
        f.trim_start_matches('0').len() as u32
    } else {
        (w.len() + f.len()) as u32
    };
    (digits.max(f.len() as u32), f.len() as u32)
}

fn pow10(n: u32) -> i256 {
    let mut x = i256::ONE;
    for _ in 0..n {
        x *= i256::from_i128(10);
    }
    x
}

fn fits(v: i256, p: u32) -> bool {
    v.wrapping_abs() < pow10(p)
}

/// What the engine answered: the value as canonical text and its declared scale.
fn answer(e: &Engine, sql: &str) -> Result<(String, DataType), String> {
    let b = e.sql(sql).map_err(|e| e.to_string())?;
    let c = b[0].column(0);
    let t = c.data_type().clone();
    let shown = match &t {
        DataType::Decimal128(_, s) => {
            let a = c.as_primitive::<Decimal128Type>();
            if a.is_null(0) {
                "NULL".into()
            } else {
                text(i256::from_i128(a.value(0)), *s as u32)
            }
        }
        DataType::Int64 | DataType::Int32 => {
            arrow::util::display::array_value_to_string(c, 0).unwrap()
        }
        other => return Err(format!("answered a {other}")),
    };
    Ok((shown, t))
}

/// The exact answer, `None` where the operation has no exact answer in 38 digits at the scale
/// the engine declared, so a refusal is the only right answer.
enum Want {
    Exact(String),
    /// Exact as an unbounded decimal; the engine must answer it or refuse if its type cannot. The
    /// flag is set where the operands' types alone leave no result type (a product past scale 38).
    Value(i256, u32, bool),
    Refused,
}

#[test]
fn a_generated_corpus_is_exact_or_refused() {
    let cases: usize = std::env::var("BURRMILL_STRICT_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000);
    let e = strict();
    let mut r = Rng(0x05ee_ddec_13a1_u64);
    let (mut answered, mut refusals, mut checked) = (0, 0, 0);
    for case in 0..cases {
        let (sql, want) = match r.below(7) {
            op @ 0..=2 => {
                let (a, b) = (gen_dec(&mut r, 38), gen_dec(&mut r, 38));
                let (ai, bi) = (i256::from_i128(a.v), i256::from_i128(b.v));
                let untyped = op == 2 && a.s + b.s > 38;
                let (sym, v, s) = match op {
                    0 => {
                        let s = a.s.max(b.s);
                        ("+", ai * pow10(s - a.s) + bi * pow10(s - b.s), s)
                    }
                    1 => {
                        let s = a.s.max(b.s);
                        ("-", ai * pow10(s - a.s) - bi * pow10(s - b.s), s)
                    }
                    _ => ("*", ai * bi, a.s + b.s),
                };
                (
                    format!("SELECT {} {sym} {}", a.sql(), b.sql()),
                    Want::Value(v, s, untyped),
                )
            }
            3 => {
                // Cast to another precision and scale: the same value, or refused.
                let a = gen_dec(&mut r, 38);
                let p2 = 1 + r.below(38) as u32;
                let s2 = r.below(p2 as u64 + 1) as u32;
                let ai = i256::from_i128(a.v);
                let keeps = s2 >= a.s || ai % pow10(a.s - s2) == i256::ZERO;
                let at = if s2 >= a.s {
                    ai * pow10(s2 - a.s)
                } else {
                    ai / pow10(a.s - s2)
                };
                let want = if keeps && fits(at, p2) {
                    Want::Exact(text(ai, a.s))
                } else {
                    Want::Refused
                };
                (
                    format!("SELECT CAST({} AS DECIMAL({p2},{s2}))", a.sql()),
                    want,
                )
            }
            4 => {
                // div at 18 digits, where the oracle's products stay inside i256.
                let (a, mut b) = (gen_dec(&mut r, 18), gen_dec(&mut r, 18));
                let mut s = r.below(19) as u32;
                // Half the time a divisor of 2, 4 or 8 at the dividend's scale, so the last digit
                // kept is often an exact tie and each rounding is told apart.
                if r.below(2) == 0 {
                    let v = [2, 4, 8][r.below(3) as usize] * if r.below(2) == 0 { 1 } else { -1 };
                    b = Dec { v, p: 1, s: 0 };
                    s = a.s;
                }
                let modes = ["half_even", "half_up", "down", "up", "floor", "ceiling"];
                let m = modes[r.below(6) as usize];
                let sql = format!("SELECT div({}, {}, {s}, '{m}')", a.sql(), b.sql());
                if b.v == 0 {
                    (sql, Want::Refused)
                } else {
                    // a/b * 10^s = (A * 10^(s+sb)) / (B * 10^sa), as numerator over denominator.
                    let num = i256::from_i128(a.v) * pow10(s + b.s);
                    let den = i256::from_i128(b.v) * pow10(a.s);
                    let (num, den) = if den < i256::ZERO {
                        (-num, -den)
                    } else {
                        (num, den)
                    };
                    let fl = {
                        let q = num / den;
                        if num % den != i256::ZERO && num < i256::ZERO {
                            q - i256::ONE
                        } else {
                            q
                        }
                    };
                    let rem = num - fl * den; // 0 <= rem < den
                    let twice = rem * i256::from_i128(2);
                    let up = match m {
                        "floor" => false,
                        "ceiling" => rem != i256::ZERO,
                        "down" => rem != i256::ZERO && fl < i256::ZERO,
                        "up" => rem != i256::ZERO && fl >= i256::ZERO,
                        "half_up" => twice > den || (twice == den && fl >= i256::ZERO),
                        _ => twice > den || (twice == den && fl % i256::from_i128(2) != i256::ZERO),
                    };
                    let q = if up { fl + i256::ONE } else { fl };
                    let want = if fits(q, 38) {
                        Want::Exact(text(q, s))
                    } else {
                        Want::Refused
                    };
                    (sql, want)
                }
            }
            5 => {
                let xs: Vec<Dec> = {
                    let first = gen_dec(&mut r, 38);
                    (0..3)
                        .map(|_| {
                            let mut d = gen_dec(&mut r, first.p);
                            d.p = first.p;
                            d.s = first.s.min(d.p);
                            d.v %= 10i128.pow(first.p);
                            d
                        })
                        .collect()
                };
                let s = xs[0].s;
                let rows: Vec<String> = xs.iter().map(|d| format!("({})", d.sql())).collect();
                let total = xs.iter().fold(i256::ZERO, |t, d| t + i256::from_i128(d.v));
                (
                    format!("SELECT SUM(x) FROM (VALUES {}) t(x)", rows.join(", ")),
                    Want::Value(total, s, false),
                )
            }
            _ => {
                let a = gen_dec(&mut r, 38);
                let ai = i256::from_i128(a.v);
                let want = if a.s == 0 || ai % pow10(a.s) == i256::ZERO {
                    let i = ai / pow10(a.s);
                    if i >= i256::from_i128(i64::MIN as i128)
                        && i <= i256::from_i128(i64::MAX as i128)
                    {
                        Want::Exact(text(i, 0))
                    } else {
                        Want::Refused
                    }
                } else {
                    Want::Refused
                };
                (format!("SELECT CAST({} AS BIGINT)", a.sql()), want)
            }
        };
        let got = answer(&e, &sql);
        match (&want, &got) {
            (Want::Refused, Err(_)) => refusals += 1,
            (Want::Refused, Ok((v, _))) => {
                panic!("case {case}: {sql} answered {v}, wants a refusal")
            }
            (Want::Exact(w), Ok((v, _))) => {
                assert_eq!(v, w, "case {case}: {sql}");
                answered += 1;
            }
            (Want::Exact(w), Err(m)) => panic!("case {case}: {sql} refused ({m}), wants {w}"),
            (Want::Value(v, s, untyped), got) => {
                let exact = text(*v, *s);
                // Exact or refused; refused only when the declared type cannot hold it.
                let declared = e.describe(&sql).ok().map(|d| d[0].1.clone());
                match got {
                    Ok((g, _)) => {
                        assert_eq!(*g, exact, "case {case}: {sql} declared {declared:?}");
                        answered += 1;
                    }
                    Err(m) => {
                        // Refused at planning, no type was declared: right only if no 38-digit
                        // DECIMAL holds the value exactly.
                        let holds = match declared.as_deref().and_then(decimal_type) {
                            Some((p, ds)) => ds >= *s && fits(*v * pow10(ds - s), p),
                            None if *untyped => false,
                            None => {
                                let (digits, scale) = canonical(&exact);
                                scale <= 38 && digits <= 38
                            }
                        };
                        assert!(
                            !holds,
                            "case {case}: {sql} refused ({m}) but {exact} fits {declared:?}"
                        );
                        refusals += 1;
                    }
                }
            }
        }
        checked += 1;
        if let Ok((_, t)) = &got {
            assert!(!t.is_floating(), "case {case}: {sql} answered a {t}");
        }
    }
    eprintln!("{checked} cases: {answered} exact, {refusals} refused");
    assert!(answered * 2 > checked, "{answered} of {checked} answered");
}
