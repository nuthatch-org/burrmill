//! DuckDB's INTERVAL where Arrow's differs (#419).
//!
//! - **Text to INTERVAL** is DuckDB's `Interval::FromCString`: a bare number is seconds (`'47.2'`),
//!   a fraction of a unit spills into the next one down as DuckDB spills it (`'1.5 WEEK'` is 10
//!   days 12 hours, `'0.05 YEAR'` a month), and DuckDB's unit spellings (`2Y 1M 3S`) are read.
//!   Arrow reads `'47.2'` as months and keeps a week's fraction as hours.
//! - **INTERVAL to text** is DuckDB's `IntervalToStringCast::Format` (`2 years`, not `24 mons`).
//! - **Comparisons** normalize, as DuckDB's do: 30 days are a month and 24 hours a day, so
//!   `INTERVAL 30 DAY = INTERVAL 1 MONTH`. Arrow compares the three fields one by one.
//! - **TIMESTAMP - TIMESTAMP** is an INTERVAL of days and the time left over, as DuckDB's
//!   `Interval::GetDifference`; DataFusion's is a DURATION.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, IntervalMonthDayNanoArray, StringBuilder,
};
use arrow::datatypes::{
    DataType, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, IntervalMonthDayNano, IntervalMonthDayNanoType, IntervalUnit, TimeUnit,
};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::expr::{Between, ScalarFunction};
use datafusion_expr::{
    BinaryExpr, Cast, ColumnarValue, Expr, ExprSchemable, Operator, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TryCast, Volatility,
};

const MICROS_PER_SEC: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SEC;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
const NANOS_PER_DAY: i128 = MICROS_PER_DAY as i128 * 1000;

/// `target += input * mult`, then the fraction's share of `mult`, rounded: DuckDB's
/// `IntervalTryAddition`. `None` where a step leaves the field's type.
fn add32(target: &mut i32, input: i64, mult: i64, fraction: f64) -> Option<()> {
    let add: i32 = input.checked_mul(mult)?.try_into().ok()?;
    *target = target.checked_add(add)?;
    if fraction.abs() > 1e-10 {
        let add: i32 = ((fraction * mult as f64).round() as i64).try_into().ok()?;
        *target = target.checked_add(add)?;
    }
    Some(())
}

fn add64(target: &mut i64, input: i64, mult: i64, fraction: f64) -> Option<()> {
    *target = target.checked_add(input.checked_mul(mult)?)?;
    if fraction.abs() > 1e-10 {
        *target = target.checked_add((fraction * mult as f64).round() as i64)?;
    }
    Some(())
}

enum Unit {
    Months(i64),
    Quarter,
    Month,
    Week,
    Day,
    Micros(i64),
    Microsecond,
}

/// DuckDB's `TryGetDatePartSpecifier`, as far as an interval takes the part; `Err` for a part it
/// names but an interval does not take, which fails the same way.
fn unit(spec: &str) -> Option<Unit> {
    Some(match spec {
        "year" | "yr" | "y" | "years" | "yrs" => Unit::Months(12),
        "month" | "mon" | "months" | "mons" => Unit::Month,
        "day" | "days" | "d" | "dayofmonth" => Unit::Day,
        "decade" | "dec" | "decades" | "decs" => Unit::Months(120),
        "century" | "cent" | "centuries" | "c" => Unit::Months(1200),
        "millennium" | "mil" | "millenniums" | "millennia" | "mils" | "millenium" => {
            Unit::Months(12000)
        }
        "microseconds" | "microsecond" | "us" | "usec" | "usecs" | "usecond" | "useconds" => {
            Unit::Microsecond
        }
        "milliseconds" | "millisecond" | "ms" | "msec" | "msecs" | "msecond" | "mseconds" => {
            Unit::Micros(1000)
        }
        "second" | "sec" | "seconds" | "secs" | "s" => Unit::Micros(MICROS_PER_SEC),
        "minute" | "min" | "minutes" | "mins" | "m" => Unit::Micros(MICROS_PER_MINUTE),
        "hour" | "hr" | "hours" | "hrs" | "h" => Unit::Micros(MICROS_PER_HOUR),
        "week" | "weeks" | "w" | "weekofyear" => Unit::Week,
        "quarter" | "quarters" => Unit::Quarter,
        _ => return None,
    })
}

fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n')
}

fn two_digits(b: &[u8], pos: &mut usize) -> Option<i64> {
    let d = |c: u8| c.is_ascii_digit().then(|| i64::from(c - b'0'));
    let mut r = d(*b.get(*pos)?)?;
    *pos += 1;
    if let Some(n) = b.get(*pos).and_then(|c| d(*c)) {
        r = r * 10 + n;
        *pos += 1;
    }
    Some(r)
}

/// `H:MM[:SS[.ffffff]]` in microseconds, as DuckDB's `Time::TryConvertInterval` reads it, not
/// strictly: up to ten digits of hours, a missing field after its colon is zero, digits past the
/// sixth of the fraction are dropped, and whatever follows the time is ignored.
fn time_part(b: &[u8], negative: bool) -> Option<i64> {
    let mut pos = 0;
    let mut hour: i64 = 0;
    let mut digits = 0;
    while pos < b.len() && b[pos].is_ascii_digit() {
        digits += 1;
        if digits > 10 {
            return None;
        }
        hour = hour * 10 + i64::from(b[pos] - b'0');
        pos += 1;
    }
    if digits == 0 || b.get(pos) != Some(&b':') {
        return None;
    }
    pos += 1;
    let min = if pos == b.len() {
        0
    } else {
        two_digits(b, &mut pos)?
    };
    if min >= 60 {
        return None;
    }
    let sec = if pos == b.len() {
        0
    } else {
        if b[pos] != b':' {
            return None;
        }
        pos += 1;
        if pos == b.len() {
            0
        } else {
            two_digits(b, &mut pos)?
        }
    };
    if sec >= 60 {
        return None;
    }
    let mut micros = 0;
    if b.get(pos) == Some(&b'.') {
        pos += 1;
        let mut mult = 100_000;
        while pos < b.len() && b[pos].is_ascii_digit() {
            micros += i64::from(b[pos] - b'0') * mult;
            mult /= 10;
            pos += 1;
        }
    }
    let sub_hour = min * MICROS_PER_MINUTE + sec * MICROS_PER_SEC + micros;
    if negative {
        hour.checked_mul(-MICROS_PER_HOUR)?.checked_sub(sub_hour)
    } else {
        hour.checked_mul(MICROS_PER_HOUR)?.checked_add(sub_hour)
    }
}

/// DuckDB's `Interval::FromCString`: `(months, days, micros)`, or `None` where DuckDB's cast fails.
pub fn parse(s: &str) -> Option<(i32, i32, i64)> {
    let b = s.as_bytes();
    let len = b.len();
    let (mut months, mut days, mut micros) = (0i32, 0i32, 0i64);
    let mut pos = match b.first()? {
        b'@' => 1,
        b'P' | b'p' => return None,
        _ => 0,
    };
    let mut found_any = false;
    loop {
        while pos < len && is_space(b[pos]) {
            pos += 1;
        }
        if pos >= len {
            break;
        }
        let negative = match b[pos] {
            c if c.is_ascii_digit() => false,
            b'-' => {
                pos += 1;
                true
            }
            b'a' | b'A' => {
                if len - pos < 3 || !b[pos + 1].eq_ignore_ascii_case(&b'g') {
                    return None;
                }
                if !b[pos + 2].eq_ignore_ascii_case(&b'o')
                    || !b[pos + 3..].iter().all(|c| is_space(*c))
                {
                    return None;
                }
                if months == i32::MIN || days == i32::MIN || micros == i64::MIN {
                    return None;
                }
                (months, days, micros) = (-months, -days, -micros);
                break;
            }
            _ => return None,
        };
        let start = pos;
        while pos < len && b[pos].is_ascii_digit() {
            pos += 1;
        }
        if b.get(pos) == Some(&b':') {
            micros = micros.checked_add(time_part(&b[start..], negative)?)?;
            found_any = true;
            break;
        }
        if pos == start {
            return None;
        }
        let mut number: i64 = s[start..pos].parse().ok()?;
        let mut fraction = 0.0;
        if b.get(pos) == Some(&b'.') {
            pos += 1;
            let from = pos;
            while pos < len && b[pos].is_ascii_digit() {
                pos += 1;
            }
            if pos > from {
                fraction = format!("0.{}", &s[from..pos]).parse::<f64>().ok()?;
            }
        }
        if negative {
            number = -number;
            fraction = -fraction;
        }
        while pos < len && is_space(b[pos]) {
            pos += 1;
        }
        let from = pos;
        while pos < len && b[pos].is_ascii_alphabetic() {
            pos += 1;
        }
        let spec = s[from..pos].to_ascii_lowercase();
        if spec.is_empty() && !found_any {
            // A bare number is seconds, and only whitespace may follow it.
            add64(&mut micros, number, MICROS_PER_SEC, 0.0)?;
            add64(&mut micros, 0, MICROS_PER_SEC, fraction)?;
            if !b[pos..].iter().all(|c| is_space(*c)) {
                return None;
            }
            found_any = true;
            break;
        }
        match unit(&spec)? {
            Unit::Months(m) => add32(&mut months, number, m, fraction)?,
            Unit::Quarter => {
                add32(&mut months, number, 3, 0.0)?;
                let month = (fraction * 3.0) as i32;
                add32(&mut months, i64::from(month), 1, 0.0)?;
                add32(&mut days, 0, 30, fraction * 3.0 - f64::from(month))?;
            }
            Unit::Month => {
                add32(&mut months, number, 1, 0.0)?;
                let day = (fraction * 30.0) as i32;
                add32(&mut days, i64::from(day), 1, 0.0)?;
                add64(
                    &mut micros,
                    0,
                    MICROS_PER_DAY,
                    fraction * 30.0 - f64::from(day),
                )?;
            }
            Unit::Day => {
                add32(&mut days, number, 1, 0.0)?;
                add64(&mut micros, 0, MICROS_PER_DAY, fraction)?;
            }
            Unit::Week => {
                add32(&mut days, number, 7, 0.0)?;
                let day = (fraction * 7.0) as i32;
                add32(&mut days, i64::from(day), 1, 0.0)?;
                add64(
                    &mut micros,
                    0,
                    MICROS_PER_DAY,
                    fraction * 7.0 - f64::from(day),
                )?;
            }
            Unit::Microsecond => add64(&mut micros, number, 1, 0.0)?,
            Unit::Micros(m) => add64(&mut micros, number, m, fraction)?,
        }
        found_any = true;
    }
    found_any.then_some((months, days, micros))
}

/// DuckDB's `IntervalToStringCast::Format`.
pub fn text(months: i32, days: i32, micros: i64) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut unit = |v: i64, name: &str| {
        if v != 0 {
            let s = if v == 1 || v == -1 { "" } else { "s" };
            parts.push(format!("{v} {name}{s}"));
        }
    };
    if months != 0 {
        let years = months / 12;
        unit(i64::from(years), "year");
        unit(i64::from(months - years * 12), "month");
    }
    unit(i64::from(days), "day");
    if micros != 0 {
        let m = micros.unsigned_abs();
        let h = m / MICROS_PER_HOUR as u64;
        let rest = m % MICROS_PER_HOUR as u64;
        let mut t = format!(
            "{}{h:02}:{:02}:{:02}",
            if micros < 0 { "-" } else { "" },
            rest / MICROS_PER_MINUTE as u64,
            rest / MICROS_PER_SEC as u64 % 60
        );
        let f = rest % MICROS_PER_SEC as u64;
        if f != 0 {
            t.push('.');
            t.push_str(format!("{f:06}").trim_end_matches('0'));
        }
        parts.push(t);
    }
    if parts.is_empty() {
        "00:00:00".into()
    } else {
        parts.join(" ")
    }
}

/// An interval's length with a month as 30 days and a day as 24 hours, in nanoseconds: what DuckDB
/// compares, as its normalized `(months, days, micros)` with the remainders borrowed non-negative
/// order exactly as this total does.
fn length(v: IntervalMonthDayNano) -> i128 {
    i128::from(v.months) * 30 * NANOS_PER_DAY
        + i128::from(v.days) * NANOS_PER_DAY
        + i128::from(v.nanoseconds)
}

fn scalar_out(scalar: bool, out: ArrayRef) -> Result<ColumnarValue> {
    Ok(if scalar {
        ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
    } else {
        ColumnarValue::Array(out)
    })
}

fn one_arg(args: ScalarFunctionArgs, name: &str) -> Result<(bool, ArrayRef)> {
    let scalar = args
        .args
        .iter()
        .all(|a| matches!(a, ColumnarValue::Scalar(_)));
    let mut a = ColumnarValue::values_to_arrays(&args.args)?;
    if a.len() != 1 {
        return exec_err!("{name} takes one argument");
    }
    Ok((scalar, a.remove(0)))
}

const MDN: DataType = DataType::Interval(IntervalUnit::MonthDayNano);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
enum Kind {
    /// Text to INTERVAL, failing as DuckDB's cast fails.
    FromText,
    /// Text to INTERVAL, NULL where that would fail.
    TryFromText,
    /// INTERVAL to text.
    ToText,
    /// INTERVAL to its normalized length, DECIMAL(38,0) nanoseconds.
    Length,
    /// DURATION to INTERVAL, as days and the rest.
    FromDuration,
}

/// `burrmill_interval` and its kin: see [`Kind`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IntervalFn {
    kind: Kind,
    sig: Signature,
}

impl IntervalFn {
    fn udf(kind: Kind) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            kind,
            sig: Signature::any(1, Volatility::Immutable),
        }))
    }
    fn call(kind: Kind, arg: Expr) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(Self::udf(kind), vec![arg]))
    }
}

impl ScalarUDFImpl for IntervalFn {
    fn name(&self) -> &str {
        match self.kind {
            Kind::FromText => "burrmill_interval",
            Kind::TryFromText => "burrmill_try_interval",
            Kind::ToText => "burrmill_interval_text",
            Kind::Length => "burrmill_interval_length",
            Kind::FromDuration => "burrmill_duration_interval",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(match self.kind {
            Kind::FromText | Kind::TryFromText | Kind::FromDuration => MDN,
            Kind::ToText => DataType::Utf8,
            Kind::Length => DataType::Decimal128(38, 0),
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (scalar, a) = one_arg(args, self.name())?;
        let out: ArrayRef = match self.kind {
            Kind::FromText | Kind::TryFromText => {
                let a = arrow::compute::cast(&a, &DataType::Utf8)?;
                let mut out = Vec::with_capacity(a.len());
                for v in a.as_string::<i32>().iter() {
                    out.push(match v {
                        None => None,
                        Some(s) => match parse(s) {
                            Some((m, d, us)) => match us.checked_mul(1000) {
                                Some(ns) => Some(IntervalMonthDayNano::new(m, d, ns)),
                                None => return Err(too_long(s)),
                            },
                            None if self.kind == Kind::TryFromText => None,
                            None => {
                                return Err(DataFusionError::Execution(format!(
                                    "Conversion Error: Could not convert string '{s}' to INTERVAL"
                                )));
                            }
                        },
                    });
                }
                Arc::new(IntervalMonthDayNanoArray::from(out))
            }
            Kind::ToText => {
                let a = arrow::compute::cast(&a, &MDN)?;
                let mut b = StringBuilder::new();
                for v in a.as_primitive::<IntervalMonthDayNanoType>().iter() {
                    match v {
                        Some(v) => b.append_value(text(v.months, v.days, v.nanoseconds / 1000)),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            Kind::Length => {
                let a = arrow::compute::cast(&a, &MDN)?;
                let out: Decimal128Array = a
                    .as_primitive::<IntervalMonthDayNanoType>()
                    .iter()
                    .map(|v| v.map(length))
                    .collect();
                Arc::new(out.with_precision_and_scale(38, 0)?)
            }
            Kind::FromDuration => {
                let nanos: Vec<Option<i128>> = match a.data_type() {
                    DataType::Duration(TimeUnit::Second) => a
                        .as_primitive::<DurationSecondType>()
                        .iter()
                        .map(|v| v.map(|v| i128::from(v) * 1_000_000_000))
                        .collect(),
                    DataType::Duration(TimeUnit::Millisecond) => a
                        .as_primitive::<DurationMillisecondType>()
                        .iter()
                        .map(|v| v.map(|v| i128::from(v) * 1_000_000))
                        .collect(),
                    DataType::Duration(TimeUnit::Microsecond) => a
                        .as_primitive::<DurationMicrosecondType>()
                        .iter()
                        .map(|v| v.map(|v| i128::from(v) * 1000))
                        .collect(),
                    DataType::Duration(TimeUnit::Nanosecond) => a
                        .as_primitive::<DurationNanosecondType>()
                        .iter()
                        .map(|v| v.map(i128::from))
                        .collect(),
                    t => return exec_err!("burrmill_duration_interval: not a duration, {t}"),
                };
                let mut out = Vec::with_capacity(nanos.len());
                for v in nanos {
                    out.push(match v {
                        None => None,
                        Some(v) => {
                            let days = i32::try_from(v / NANOS_PER_DAY).map_err(|_| {
                                DataFusionError::Execution(
                                    "Conversion Error: Timestamp difference is out of bounds"
                                        .into(),
                                )
                            })?;
                            Some(IntervalMonthDayNano::new(
                                0,
                                days,
                                (v % NANOS_PER_DAY) as i64,
                            ))
                        }
                    });
                }
                Arc::new(IntervalMonthDayNanoArray::from(out))
            }
        };
        scalar_out(scalar, out)
    }
}

/// DuckDB holds an interval's time in microseconds, Arrow in nanoseconds: past 2562047 hours it
/// has no home here, and is refused rather than cut short.
fn too_long(s: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the INTERVAL '{s}' is not supported here: its time part passes 2562047 hours, which \
         Arrow's nanoseconds cannot hold"
    ))
}

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

fn is_interval(t: &DataType) -> bool {
    matches!(t, DataType::Interval(_))
}

fn length_of(e: Expr) -> Expr {
    IntervalFn::call(Kind::Length, e)
}

/// One expression, once types are coerced: see the module.
pub fn intervals_as_duckdb(e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    match e {
        Expr::Cast(Cast {
            ref expr,
            ref field,
        })
        | Expr::TryCast(TryCast {
            ref expr,
            ref field,
        }) => {
            let to = field.data_type();
            let from = expr.get_type(schema)?;
            let try_ = matches!(e, Expr::TryCast(_));
            if *to == MDN && is_text(&from) {
                let kind = if try_ {
                    Kind::TryFromText
                } else {
                    Kind::FromText
                };
                return Ok(Transformed::yes(IntervalFn::call(
                    kind,
                    expr.as_ref().clone(),
                )));
            }
            if is_text(to) && is_interval(&from) {
                let t = IntervalFn::call(Kind::ToText, expr.as_ref().clone());
                return Ok(Transformed::yes(if *to == DataType::Utf8 {
                    t
                } else {
                    Expr::Cast(Cast::new(Box::new(t), to.clone()))
                }));
            }
            Ok(Transformed::no(e))
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (lt, rt) = (left.get_type(schema)?, right.get_type(schema)?);
            let compares = matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
            );
            if compares && is_interval(&lt) && is_interval(&rt) {
                return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(length_of(*left)),
                    op,
                    Box::new(length_of(*right)),
                ))));
            }
            let e = Expr::BinaryExpr(BinaryExpr { left, op, right });
            if op == Operator::Minus
                && matches!(lt, DataType::Timestamp(..))
                && matches!(rt, DataType::Timestamp(..))
                && matches!(e.get_type(schema)?, DataType::Duration(_))
            {
                return Ok(Transformed::yes(IntervalFn::call(Kind::FromDuration, e)));
            }
            Ok(Transformed::no(e))
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) if is_interval(&expr.get_type(schema)?)
            && is_interval(&low.get_type(schema)?)
            && is_interval(&high.get_type(schema)?) =>
        {
            Ok(Transformed::yes(Expr::Between(Between::new(
                Box::new(length_of(*expr)),
                negated,
                Box::new(length_of(*low)),
                Box::new(length_of(*high)),
            ))))
        }
        e => Ok(Transformed::no(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_as_duckdb() {
        let day = MICROS_PER_DAY;
        assert_eq!(parse("47.210"), Some((0, 0, 47_210_000)));
        assert_eq!(parse("-47.210"), Some((0, 0, -47_210_000)));
        assert_eq!(parse("31.123456789"), Some((0, 0, 31_123_457)));
        assert_eq!(parse("42 "), Some((0, 0, 42_000_000)));
        assert_eq!(parse("47.210 5"), None);
        assert_eq!(parse("1.5 WEEK"), Some((0, 10, day / 2)));
        assert_eq!(parse("-1.5 WEEK"), Some((0, -10, -day / 2)));
        assert_eq!(parse("0.05 YEAR"), Some((1, 0, 0)));
        assert_eq!(parse("0.05 MONTH"), Some((0, 1, day / 2)));
        assert_eq!(parse("0.0000000004 DAY"), Some((0, 0, 35)));
        assert_eq!(
            parse("2Y 1 month 1M 3S 20mS 16uS"),
            Some((25, 0, 63_020_016))
        );
        assert_eq!(
            parse("1 day 2 hours ago"),
            Some((0, -1, -2 * MICROS_PER_HOUR))
        );
        assert_eq!(parse("00:00:"), Some((0, 0, 0)));
        assert_eq!(parse("-1:30"), Some((0, 0, -90 * MICROS_PER_MINUTE)));
        assert_eq!(parse("1 dow"), None);
        assert_eq!(parse(""), None);
        assert_eq!(parse("P1D"), None);
    }

    #[test]
    fn writes_as_duckdb() {
        assert_eq!(text(24, 0, 0), "2 years");
        assert_eq!(
            text(-24, 4, 18_063_020_016),
            "-2 years 4 days 05:01:03.020016"
        );
        assert_eq!(text(0, 0, 0), "00:00:00");
        assert_eq!(text(0, -1, -1_500_000), "-1 day -00:00:01.5");
    }
}
