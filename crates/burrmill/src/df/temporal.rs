//! DuckDB's casts between text and DATE, TIME and TIMESTAMP (#419), where Arrow's differ.
//!
//! - **Text to DATE** is DuckDB's `Date::TryConvertDate`: `-`, `/`, `\` or a space between the
//!   fields, a trailing ` (BC)`, `epoch`, and years from 5877642 BC to 5881580. Arrow read neither
//!   `(BC)` nor `/`, and stopped at year 262143.
//! - **Text to TIME** is DuckDB's `Time::TryConvertTime`: `11:` is 11:00, `24:00:00` is a time,
//!   `23:59:60` and a bare `11` are not, and text that is not a time is read as a timestamp's time.
//!   Arrow read `11` as 11 nanoseconds and `23:59:60` as midnight. Digits past the microsecond are
//!   dropped, as DuckDB's TIME holds microseconds.
//! - **Text to TIMESTAMP**, without a zone, is DuckDB's date then an optional time, so
//!   `1111-11-11 11:11` and `290309-12-22 (BC) 00:00:00` are read. Text with an offset or a zone
//!   after the time is left to Arrow's cast, as before.
//! - **DATE and TIME to text** are DuckDB's: `1994-08-14 (BC)`, and a TIME's fraction without its
//!   trailing zeros.
//!
//! DuckDB's `infinity` and `-infinity` have no value in Arrow's DATE or TIMESTAMP; they are refused
//! by name, a `TRY_CAST` too, since DuckDB's answer is not NULL.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, StringBuilder, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TryCast, Volatility,
};

const MICROS_PER_SEC: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SEC;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// Days since 1970-01-01 of a proleptic Gregorian date, year 0 being 1 BC.
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`].
pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn month_days(y: i64, m: i64) -> i64 {
    match m {
        2 if leap(y) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// DuckDB's `Date::TryFromDate`: its range is 5877642-06-25 (BC) to 5881580-07-10.
fn from_date(y: i64, m: i64, d: i64) -> Option<i32> {
    if !(1..=12).contains(&m) || d < 1 || d > month_days(y, m) {
        return None;
    }
    if (y, m, d) < (-5_877_641, 6, 25) || (y, m, d) > (5_881_580, 7, 10) {
        return None;
    }
    i32::try_from(days_from_civil(y, m, d)).ok()
}

#[derive(Debug, PartialEq)]
pub enum Fail {
    /// Not the format.
    Format,
    /// The format, but out of range.
    Range,
    /// DuckDB's `infinity` or `-infinity`.
    Infinite,
    /// A form this module leaves to Arrow (a timestamp's zone).
    Unhandled,
}

fn is_space(c: u8) -> bool {
    c.is_ascii_whitespace()
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

/// DuckDB's `Date::TryConvertDate`, not strict: the days, and where the date ended.
pub fn parse_date(s: &str) -> std::result::Result<(i32, usize), Fail> {
    let b = s.as_bytes();
    let len = b.len();
    let mut pos = 0;
    while pos < len && is_space(b[pos]) {
        pos += 1;
    }
    if pos >= len {
        return Err(Fail::Format);
    }
    let neg = b[pos] == b'-';
    if neg {
        pos += 1;
        if pos >= len {
            return Err(Fail::Format);
        }
    }
    if !b[pos].is_ascii_digit() {
        let rest = s[pos..].trim_end().to_ascii_lowercase();
        if rest == "infinity" || rest == "inf" {
            return Err(Fail::Infinite);
        }
        if rest == "epoch" && !neg {
            return Ok((0, len));
        }
        return Err(Fail::Format);
    }
    let mut year: i64 = 0;
    while pos < len && b[pos].is_ascii_digit() {
        if year >= 100_000_000 {
            return Err(Fail::Range);
        }
        year = year * 10 + i64::from(b[pos] - b'0');
        pos += 1;
    }
    if neg {
        year = -year;
    }
    if pos >= len {
        return Err(Fail::Format);
    }
    let sep = b[pos];
    pos += 1;
    if !matches!(sep, b' ' | b'-' | b'/' | b'\\') {
        return Err(Fail::Format);
    }
    let month = two_digits(b, &mut pos).ok_or(Fail::Format)?;
    if pos >= len || b[pos] != sep {
        return Err(Fail::Format);
    }
    pos += 1;
    if pos >= len {
        return Err(Fail::Format);
    }
    let day = two_digits(b, &mut pos).ok_or(Fail::Format)?;
    if len - pos >= 5
        && is_space(b[pos])
        && b[pos + 1] == b'('
        && b[pos + 2].eq_ignore_ascii_case(&b'b')
        && b[pos + 3].eq_ignore_ascii_case(&b'c')
        && b[pos + 4] == b')'
    {
        if neg || year == 0 {
            return Err(Fail::Format);
        }
        year = 1 - year;
        pos += 5;
    }
    if b.get(pos).is_some_and(|c| c.is_ascii_digit()) {
        return Err(Fail::Format);
    }
    from_date(year, month, day)
        .map(|d| (d, pos))
        .ok_or(Fail::Range)
}

/// A whole text as a DATE: what follows the date may only be spaces or a time DuckDB would read.
fn date_value(s: &str) -> std::result::Result<i32, Fail> {
    // What follows the date may be spaces, or the rest of a timestamp, whose date it then is.
    let (d, pos) = parse_date(s)?;
    if only_space(s.as_bytes(), pos) {
        return Ok(d);
    }
    // A time past a TIMESTAMP's last day still leaves its date (`294247-01-11 00:00:00`).
    match parse_timestamp(s) {
        Ok(_) | Err(Fail::Unhandled | Fail::Range) => Ok(d),
        Err(_) => Err(Fail::Format),
    }
}

/// DuckDB's `Time::TryConvertInternal`, not strict: microseconds, and where the time ended.
/// `None` where it is not a time; up to ten digits of hours.
fn time_internal(b: &[u8]) -> Option<(i64, usize)> {
    let len = b.len();
    let mut pos = 0;
    while pos < len && is_space(b[pos]) {
        pos += 1;
    }
    if pos >= len || !b[pos].is_ascii_digit() {
        return None;
    }
    let mut hour: i64 = 0;
    let mut digits = 0;
    while pos < len && b[pos].is_ascii_digit() {
        digits += 1;
        if digits > 10 {
            return None;
        }
        hour = hour * 10 + i64::from(b[pos] - b'0');
        pos += 1;
    }
    if pos >= len || b[pos] != b':' {
        return None;
    }
    pos += 1;
    let min = if pos == len {
        0
    } else {
        two_digits(b, &mut pos)?
    };
    if min >= 60 {
        return None;
    }
    let sec = if pos == len {
        0
    } else {
        if b[pos] != b':' {
            return None;
        }
        pos += 1;
        if pos == len {
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
        while pos < len && b[pos].is_ascii_digit() {
            micros += i64::from(b[pos] - b'0') * mult;
            mult /= 10;
            pos += 1;
        }
    }
    let t = hour
        .checked_mul(MICROS_PER_HOUR)?
        .checked_add(min * MICROS_PER_MINUTE + sec * MICROS_PER_SEC + micros)?;
    Some((t, pos))
}

/// `Z`, or DuckDB's `±H[H][[:]MM]` UTC offset, at `pos`; where it ended.
fn utc_offset(b: &[u8], pos: usize) -> Option<usize> {
    let mut p = pos;
    match b.get(p)? {
        b'Z' => return Some(p + 1),
        b'+' | b'-' => p += 1,
        _ => return None,
    }
    let digits = |p: &mut usize, max: usize| {
        let from = *p;
        while *p < b.len() && *p - from < max && b[*p].is_ascii_digit() {
            *p += 1;
        }
        *p > from
    };
    if !digits(&mut p, 2) {
        return None;
    }
    if b.get(p) == Some(&b':') {
        p += 1;
        if !digits(&mut p, 2) {
            return None;
        }
    } else {
        digits(&mut p, 2);
    }
    Some(p)
}

fn only_space(b: &[u8], pos: usize) -> bool {
    b[pos..].iter().all(|c| is_space(*c))
}

/// DuckDB's text to TIME, in microseconds.
pub fn parse_time(s: &str) -> std::result::Result<i64, Fail> {
    let b = s.as_bytes();
    let (t, pos) = match time_internal(b) {
        Some(x) => x,
        // Not a time: the time of a timestamp, if it is one.
        None => {
            return match parse_timestamp(s) {
                Ok(us) => Ok(us.rem_euclid(MICROS_PER_DAY)),
                Err(_) => Err(Fail::Format),
            };
        }
    };
    let mut pos = pos;
    if !only_space(b, pos) {
        // A UTC offset after a TIME is dropped, as DuckDB drops it.
        match utc_offset(b, pos) {
            Some(p) => pos = p,
            None => return Err(Fail::Format),
        }
        if !only_space(b, pos) {
            return Err(Fail::Format);
        }
    }
    if t > MICROS_PER_DAY {
        return Err(Fail::Format);
    }
    Ok(t)
}

/// DuckDB's text to TIMESTAMP, without a zone, in microseconds since the epoch.
pub fn parse_timestamp(s: &str) -> std::result::Result<i64, Fail> {
    let b = s.as_bytes();
    let (date, mut pos) = parse_date(s)?;
    let day = i64::from(date)
        .checked_mul(MICROS_PER_DAY)
        .ok_or(Fail::Range)?;
    if only_space(b, pos) {
        return Ok(day);
    }
    if matches!(b[pos], b' ' | b'T') {
        pos += 1;
    }
    let rest = &b[pos..];
    // The time runs to the first character that can start a zone or an offset.
    let mut end = 0;
    while end < rest.len() && is_space(rest[end]) {
        end += 1;
    }
    while end < rest.len() && !matches!(rest[end], b'Z' | b'+' | b'-' | b' ') {
        end += 1;
    }
    let (t, used) = time_internal(&rest[..end]).ok_or(Fail::Format)?;
    if used != end {
        return Err(Fail::Format);
    }
    if t > MICROS_PER_DAY {
        return Err(Fail::Range);
    }
    if !only_space(rest, end) {
        return Err(Fail::Unhandled);
    }
    let us = day.checked_add(t).ok_or(Fail::Range)?;
    // DuckDB's TIMESTAMP ends at 294247-01-10 04:00:54.775806, a microsecond short of the
    // infinity it keeps in `i64::MAX`.
    if us == i64::MAX || us <= -i64::MAX {
        return Err(Fail::Range);
    }
    Ok(us)
}

/// The first and last day of DuckDB's DATE, 5877642-06-25 (BC) and 5881580-07-10.
pub fn date_bounds() -> (i64, i64) {
    (
        days_from_civil(-5_877_641, 6, 25),
        days_from_civil(5_881_580, 7, 10),
    )
}

/// DuckDB's DATE to text.
pub fn date_text(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    if y <= 0 {
        format!("{:04}-{m:02}-{d:02} (BC)", 1 - y)
    } else {
        format!("{y:04}-{m:02}-{d:02}")
    }
}

/// DuckDB's TIME to text: the fraction without its trailing zeros.
pub fn time_text(us: i64) -> String {
    let mut s = format!(
        "{:02}:{:02}:{:02}",
        us / MICROS_PER_HOUR,
        us / MICROS_PER_MINUTE % 60,
        us / MICROS_PER_SEC % 60
    );
    let f = us % MICROS_PER_SEC;
    if f != 0 {
        s.push_str(format!(".{f:06}").trim_end_matches('0'));
    }
    s
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum Kind {
    Date,
    Time(TimeUnit),
    Timestamp(TimeUnit),
    DateText,
    TimeText,
    /// A count of days since the epoch, an Int64, as a DATE inside DuckDB's range.
    Days,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TemporalFn {
    kind: Kind,
    try_: bool,
    sig: Signature,
}

fn call(kind: Kind, try_: bool, arg: Expr) -> Expr {
    let f = Arc::new(ScalarUDF::from(TemporalFn {
        kind,
        try_,
        sig: Signature::any(1, Volatility::Immutable),
    }));
    Expr::ScalarFunction(ScalarFunction::new_udf(f, vec![arg]))
}

fn refusal(what: &str, s: &str, f: &Fail) -> DataFusionError {
    DataFusionError::Execution(match (what, f) {
        (_, Fail::Infinite) => format!(
            "the infinite {what} '{s}' is not supported here: Arrow's {what} has no infinity"
        ),
        ("DATE", Fail::Range) => {
            format!("Conversion Error: date field value out of range: \"{s}\"")
        }
        ("DATE", _) => format!(
            "Conversion Error: invalid date field format: \"{s}\", expected format is (YYYY-MM-DD)"
        ),
        ("TIME", _) => format!(
            "Conversion Error: time field value out of range: \"{s}\", expected format is \
             ([YYYY-MM-DD ]HH:MM:SS[.MS])"
        ),
        (_, Fail::Range) => {
            format!("Conversion Error: timestamp field value out of range: \"{s}\"")
        }
        _ => format!(
            "Conversion Error: invalid timestamp field format: \"{s}\", expected format is \
             (YYYY-MM-DD HH:MM:SS[.US][±HH:MM| ZONE])"
        ),
    })
}

impl ScalarUDFImpl for TemporalFn {
    fn name(&self) -> &str {
        match (&self.kind, self.try_) {
            (Kind::Date, false) => "burrmill_date",
            (Kind::Date, true) => "burrmill_try_date",
            (Kind::Time(_), false) => "burrmill_time",
            (Kind::Time(_), true) => "burrmill_try_time",
            (Kind::Timestamp(_), false) => "burrmill_timestamp",
            (Kind::Timestamp(_), true) => "burrmill_try_timestamp",
            (Kind::DateText, _) => "burrmill_date_text",
            (Kind::TimeText, _) => "burrmill_time_text",
            (Kind::Days, _) => "burrmill_days_date",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(match &self.kind {
            Kind::Date | Kind::Days => DataType::Date32,
            Kind::Time(u) => DataType::Time64(*u),
            Kind::Timestamp(u) => DataType::Timestamp(*u, None),
            Kind::DateText | Kind::TimeText => DataType::Utf8,
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args.as_slice(), [ColumnarValue::Scalar(_)]);
        let mut a = ColumnarValue::values_to_arrays(&args.args)?;
        if a.len() != 1 {
            return exec_err!("{} takes one argument", self.name());
        }
        let a = a.remove(0);
        let out: ArrayRef = match &self.kind {
            Kind::DateText => {
                let a = arrow::compute::cast(&a, &DataType::Date32)?;
                let mut b = StringBuilder::new();
                for v in a.as_primitive::<Date32Type>().iter() {
                    match v {
                        Some(d) => b.append_value(date_text(i64::from(d))),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            Kind::Days => {
                let (lo, hi) = date_bounds();
                let a = arrow::compute::cast(&a, &DataType::Int64)?;
                let mut out = Vec::with_capacity(a.len());
                for v in a.as_primitive::<arrow::datatypes::Int64Type>().iter() {
                    out.push(match v {
                        None => None,
                        Some(d) if (lo..=hi).contains(&d) => Some(d as i32),
                        Some(_) => {
                            return Err(DataFusionError::Execution(
                                "Out of Range Error: Date out of range".into(),
                            ));
                        }
                    });
                }
                Arc::new(Date32Array::from(out))
            }
            Kind::TimeText => {
                let us: Vec<Option<i64>> = match a.data_type() {
                    DataType::Time64(TimeUnit::Nanosecond) => a
                        .as_primitive::<Time64NanosecondType>()
                        .iter()
                        .map(|v| v.map(|v| v / 1000))
                        .collect(),
                    _ => arrow::compute::cast(&a, &DataType::Time64(TimeUnit::Microsecond))?
                        .as_primitive::<Time64MicrosecondType>()
                        .iter()
                        .collect(),
                };
                let mut b = StringBuilder::new();
                for v in us {
                    match v {
                        Some(t) => b.append_value(time_text(t)),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            kind => {
                let text = arrow::compute::cast(&a, &DataType::Utf8)?;
                let text = text.as_string::<i32>();
                let (what, parse): (&str, fn(&str) -> std::result::Result<i64, Fail>) = match kind {
                    Kind::Date => ("DATE", |s| date_value(s).map(i64::from)),
                    Kind::Time(_) => ("TIME", parse_time),
                    _ => ("TIMESTAMP", parse_timestamp),
                };
                let mut vals: Vec<Option<i64>> = Vec::with_capacity(text.len());
                for v in text.iter() {
                    let Some(s) = v else {
                        vals.push(None);
                        continue;
                    };
                    match parse(s) {
                        Ok(x) => vals.push(Some(x)),
                        Err(Fail::Unhandled) => {
                            // Left to Arrow's cast, as before this module.
                            let one = ScalarValue::Utf8(Some(s.to_string())).to_array()?;
                            let to = DataType::Timestamp(TimeUnit::Microsecond, None);
                            let got = if self.try_ {
                                arrow::compute::cast_with_options(
                                    &one,
                                    &to,
                                    &arrow::compute::CastOptions::default(),
                                )?
                            } else {
                                arrow::compute::cast_with_options(
                                    &one,
                                    &to,
                                    &arrow::compute::CastOptions {
                                        safe: false,
                                        ..Default::default()
                                    },
                                )?
                            };
                            vals.push(
                                got.as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                                    .iter()
                                    .next()
                                    .flatten(),
                            );
                        }
                        Err(f @ Fail::Infinite) => return Err(refusal(what, s, &f)),
                        Err(_) if self.try_ => vals.push(None),
                        Err(f) => return Err(refusal(what, s, &f)),
                    }
                }
                match kind {
                    Kind::Date => Arc::new(
                        vals.into_iter()
                            .map(|v| v.map(|d| d as i32))
                            .collect::<Date32Array>(),
                    ),
                    Kind::Time(TimeUnit::Nanosecond) => Arc::new(
                        vals.into_iter()
                            .map(|v| v.map(|t| t * 1000))
                            .collect::<Time64NanosecondArray>(),
                    ),
                    Kind::Time(_) => Arc::new(vals.into_iter().collect::<Time64MicrosecondArray>()),
                    Kind::Timestamp(u) => {
                        let mut out = Vec::with_capacity(vals.len());
                        for (v, s) in vals.into_iter().zip(text.iter()) {
                            out.push(match (v, u) {
                                (None, _) => None,
                                (Some(us), TimeUnit::Nanosecond) => match us.checked_mul(1000) {
                                    Some(ns) => Some(ns),
                                    None if self.try_ => None,
                                    None => {
                                        return Err(refusal(
                                            "TIMESTAMP",
                                            s.unwrap_or_default(),
                                            &Fail::Range,
                                        ));
                                    }
                                },
                                (Some(us), TimeUnit::Millisecond) => Some(us.div_euclid(1000)),
                                (Some(us), TimeUnit::Second) => Some(us.div_euclid(MICROS_PER_SEC)),
                                (Some(us), TimeUnit::Microsecond) => Some(us),
                            });
                        }
                        match u {
                            TimeUnit::Second => Arc::new(TimestampSecondArray::from(out)),
                            TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(out)),
                            TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(out)),
                            TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(out)),
                        }
                    }
                    _ => unreachable!(),
                }
            }
        };
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
        } else {
            ColumnarValue::Array(out)
        })
    }
}

/// `days` (an integer) as a DATE, an error past DuckDB's range rather than a day Arrow holds and
/// DuckDB does not.
pub fn days_to_date(days: Expr) -> Expr {
    call(Kind::Days, false, days)
}

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

/// The casts in the module's list, once types are coerced.
pub fn temporal_as_duckdb(e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let (inner, to, try_) = match &e {
        Expr::Cast(Cast { expr, field }) => (expr, field.data_type().clone(), false),
        Expr::TryCast(TryCast { expr, field }) => (expr, field.data_type().clone(), true),
        _ => return Ok(Transformed::no(e)),
    };
    let from = inner.get_type(schema)?;
    let arg = inner.as_ref().clone();
    let made = match (&from, &to) {
        (f, DataType::Date32) if is_text(f) => call(Kind::Date, try_, arg),
        (f, DataType::Time64(u)) if is_text(f) => call(Kind::Time(*u), try_, arg),
        (f, DataType::Timestamp(u, None)) if is_text(f) => call(Kind::Timestamp(*u), try_, arg),
        (DataType::Date32, t) if is_text(t) => call(Kind::DateText, false, arg),
        (DataType::Time64(_), t) if is_text(t) => call(Kind::TimeText, false, arg),
        _ => return Ok(Transformed::no(e)),
    };
    let made_type = match &to {
        t if is_text(t) => DataType::Utf8,
        t => t.clone(),
    };
    Ok(Transformed::yes(if made_type == to {
        made
    } else {
        Expr::Cast(Cast::new(Box::new(made), to))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_days_round_trip() {
        for d in [-2_147_483_000i64, -719_468, -1, 0, 1, 19_000, 2_147_483_000] {
            let (y, m, dd) = civil_from_days(d);
            assert_eq!(days_from_civil(y, m, dd), d);
        }
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
    }

    #[test]
    fn dates_as_duckdb() {
        let day = |s: &str| parse_date(s).map(|(d, _)| date_text(i64::from(d)));
        assert_eq!(day("1111-11-11 (bc)"), Ok("1111-11-11 (BC)".into()));
        assert_eq!(day("2004/02/29"), Ok("2004-02-29".into()));
        assert_eq!(day("5877642-06-25 (BC)"), Ok("5877642-06-25 (BC)".into()));
        assert_eq!(day("5881580-07-10"), Ok("5881580-07-10".into()));
        assert_eq!(day("5881580-07-11"), Err(Fail::Range));
        assert_eq!(day("-1000-01-01"), Ok("1001-01-01 (BC)".into()));
        assert_eq!(day("infinity"), Err(Fail::Infinite));
        assert_eq!(day("2021-02-30"), Err(Fail::Range));
        assert_eq!(day("2021-0230"), Err(Fail::Format));
    }

    #[test]
    fn times_as_duckdb() {
        let t = |s: &str| parse_time(s).map(time_text);
        assert_eq!(t("11:"), Ok("11:00:00".into()));
        assert_eq!(t("11:11:"), Ok("11:11:00".into()));
        assert_eq!(t("  11:11:11  "), Ok("11:11:11".into()));
        assert_eq!(t("24:00:00"), Ok("24:00:00".into()));
        assert_eq!(t("14:42:04.999999999"), Ok("14:42:04.999999".into()));
        assert_eq!(t("14:42:04.350"), Ok("14:42:04.35".into()));
        assert_eq!(t("11/11/11"), Ok("00:00:00".into()));
        assert!(t("23:59:60").is_err());
        assert!(t("11").is_err());
        assert!(t("1").is_err());
    }

    #[test]
    fn timestamps_as_duckdb() {
        assert_eq!(
            parse_timestamp("1111-11-11 11:11"),
            Ok(
                i64::from(parse_date("1111-11-11").unwrap().0) * MICROS_PER_DAY
                    + 11 * MICROS_PER_HOUR
                    + 11 * MICROS_PER_MINUTE
            )
        );
        assert!(parse_timestamp("290309-12-22 (BC) 00:00:00").is_ok());
        assert!(parse_timestamp("294247-01-10 04:00:54.775806").is_ok());
        assert_eq!(parse_timestamp("infinity"), Err(Fail::Infinite));
        assert_eq!(
            parse_timestamp("2021-01-01 10:00:00+01"),
            Err(Fail::Unhandled)
        );
    }
}
