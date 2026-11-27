//! Values as DuckDB's runner writes them, and compared as it compares them
//! (`test/sqlite/result_helper.cpp`, `SQLLogicTestConvertValue` and `CompareValues`).
//!
//! Every value is DuckDB's `CAST(v AS VARCHAR)`: NULL is `NULL`, an empty string `(empty)`, a
//! top-level boolean `1` or `0`. A value whose text differs is still equal if the column is
//! numeric and the expected text, cast to the column's type, equals it: floats within DuckDB's
//! `ApproxEqual` (1% of the answer plus 1e-8), integers and decimals exactly after the cast.

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::*;

/// What a column is, for the comparison.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Class {
    Float32,
    Float64,
    /// Integers and decimals, at this scale.
    Exact(i8),
    Boolean,
    Other,
}

pub fn class(t: &DataType) -> Class {
    match t {
        DataType::Float16 | DataType::Float32 => Class::Float32,
        DataType::Float64 => Class::Float64,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Class::Exact(0),
        DataType::Decimal32(_, s)
        | DataType::Decimal64(_, s)
        | DataType::Decimal128(_, s)
        | DataType::Decimal256(_, s) => Class::Exact(*s),
        DataType::Boolean => Class::Boolean,
        DataType::Dictionary(_, v) => class(v),
        _ => Class::Other,
    }
}

/// Whether the value is NULL, by the logical nulls: a `NullArray` has no null buffer, so
/// `is_null` says false for every one of its values.
pub fn null_at(col: &ArrayRef, i: usize) -> bool {
    col.logical_nulls().is_some_and(|n| n.is_null(i))
}

/// DuckDB's text for a top-level value.
pub fn top(col: &ArrayRef, i: usize) -> String {
    if null_at(col, i) {
        return "NULL".into();
    }
    if let DataType::Boolean = col.data_type() {
        return if col.as_boolean().value(i) { "1" } else { "0" }.into();
    }
    let s = value(col, i, false);
    if s.is_empty() {
        "(empty)".into()
    } else {
        s.replace('\0', "\\0")
    }
}

/// `{}` of DuckDB's fmt for a double: the shortest digits that round-trip, positional from
/// 1e-4 up to 1e16 (`0.0001`, `5e-05`), else `d.ddde+XX`, and a `.0` on a whole number.
pub fn float(v: f64, shortest: String) -> String {
    if v.is_nan() {
        return "nan".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "inf" } else { "-inf" }.into();
    }
    // `shortest` is Rust's `{:e}`: d[.ddd]e[-]X, the shortest round-trip digits.
    let (mant, exp) = shortest.split_once('e').unwrap_or((&shortest, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let neg = mant.starts_with('-');
    let digits: String = mant.chars().filter(|c| c.is_ascii_digit()).collect();
    let sign = if neg { "-" } else { "" };
    if !(-4..16).contains(&exp) {
        let (first, rest) = digits.split_at(1);
        let frac = if rest.is_empty() {
            String::new()
        } else {
            format!(".{rest}")
        };
        let esign = if exp < 0 { '-' } else { '+' };
        return format!("{sign}{first}{frac}e{esign}{:02}", exp.abs());
    }
    let point = exp + 1;
    let s = if point <= 0 {
        format!("0.{}{}", "0".repeat((-point) as usize), digits)
    } else if point as usize >= digits.len() {
        format!("{digits}{}.0", "0".repeat(point as usize - digits.len()))
    } else {
        let (a, b) = digits.split_at(point as usize);
        format!("{a}.{b}")
    };
    format!("{sign}{s}")
}

fn two(v: i64) -> String {
    format!("{v:02}")
}

/// `HH:MM:SS[.ffffff]` with trailing zeros of the fraction dropped, as DuckDB's `FormatMicros`.
fn time_of_day(micros_of_day: i64, nanos: Option<i64>) -> String {
    let secs = micros_of_day / 1_000_000;
    let mut frac_digits = 6;
    let mut frac = micros_of_day % 1_000_000;
    if let Some(n) = nanos {
        frac = frac * 1000 + n;
        frac_digits = 9;
    }
    let mut s = format!(
        "{}:{}:{}",
        two(secs / 3600),
        two(secs / 60 % 60),
        two(secs % 60)
    );
    if frac != 0 {
        let f = format!("{frac:0frac_digits$}");
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
    s
}

/// DuckDB's `Date::ToString`, over the proleptic Gregorian calendar to DuckDB's limits (chrono
/// stops at year 262143).
fn date(days: i64) -> String {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    if y <= 0 {
        format!("{:04}-{m:02}-{d:02} (BC)", 1 - y)
    } else {
        format!("{y:04}-{m:02}-{d:02}")
    }
}

fn timestamp(v: i64, unit: &TimeUnit) -> String {
    let (micros, nanos) = match unit {
        TimeUnit::Second => (v as i128 * 1_000_000, None),
        TimeUnit::Millisecond => (v as i128 * 1000, None),
        TimeUnit::Microsecond => (v as i128, None),
        TimeUnit::Nanosecond => (v as i128 / 1000, Some((v as i128).rem_euclid(1000) as i64)),
    };
    let micros = if nanos.is_some() {
        (v as i128).div_euclid(1000)
    } else {
        micros
    };
    let day = micros.div_euclid(86_400_000_000) as i64;
    let rem = micros.rem_euclid(86_400_000_000) as i64;
    format!(
        "{} {}",
        date(day),
        time_of_day(rem, nanos.filter(|n| *n != 0))
    )
}

/// DuckDB's `IntervalToStringCast::Format`.
fn interval(months: i32, days: i32, micros: i64) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut unit = |v: i64, name: &str| {
        if v != 0 {
            let s = if v == 1 || v == -1 { "" } else { "s" };
            parts.push(format!("{v} {name}{s}"));
        }
    };
    if months != 0 {
        let years = months / 12;
        unit(years as i64, "year");
        unit((months - years * 12) as i64, "month");
    }
    unit(days as i64, "day");
    if micros != 0 {
        let neg = micros < 0;
        let m = micros.unsigned_abs() as i64;
        let h = m / 3_600_000_000;
        let rest = m % 3_600_000_000;
        let mut t = format!(
            "{}{}:{}:{}",
            if neg { "-" } else { "" },
            two(h),
            two(rest / 60_000_000),
            two(rest / 1_000_000 % 60)
        );
        let f = rest % 1_000_000;
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

/// A string inside a LIST, STRUCT or MAP, quoted as DuckDB's `CalculateEscapedStringLength`.
fn nested_string(s: &str, key: bool) -> String {
    const SPECIAL: &[char] = &['"', '\'', '(', ')', ',', ':', '=', '[', '\\', ']', '{', '}'];
    let quote = key
        || s.is_empty()
        || s.starts_with(|c: char| c.is_ascii_whitespace())
        || (s.len() >= 2 && s.ends_with(|c: char| c.is_ascii_whitespace()))
        || s.eq_ignore_ascii_case("null")
        || s.contains(SPECIAL);
    if !quote {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

fn is_string(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

fn child(col: &ArrayRef, i: usize) -> String {
    if null_at(col, i) {
        return "NULL".into();
    }
    // A BLOB inside a LIST, STRUCT or MAP is cast to text and then quoted as a string is.
    if is_string(col.data_type())
        || matches!(
            col.data_type(),
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        )
    {
        return nested_string(&value(col, i, true), false);
    }
    value(col, i, true)
}

/// The text of a non-NULL value; `nested` for one inside a LIST, STRUCT or MAP.
pub fn value(col: &ArrayRef, i: usize, nested: bool) -> String {
    use DataType as D;
    macro_rules! prim {
        ($t:ty) => {
            col.as_primitive::<$t>().value(i).to_string()
        };
    }
    match col.data_type() {
        D::Boolean => {
            let b = col.as_boolean().value(i);
            if nested {
                b.to_string()
            } else {
                (b as u8).to_string()
            }
        }
        D::Int8 => prim!(Int8Type),
        D::Int16 => prim!(Int16Type),
        D::Int32 => prim!(Int32Type),
        D::Int64 => prim!(Int64Type),
        D::UInt8 => prim!(UInt8Type),
        D::UInt16 => prim!(UInt16Type),
        D::UInt32 => prim!(UInt32Type),
        D::UInt64 => prim!(UInt64Type),
        D::Float16 => {
            let v = col.as_primitive::<Float16Type>().value(i).to_f32();
            float(v as f64, format!("{v:e}"))
        }
        D::Float32 => {
            let v = col.as_primitive::<Float32Type>().value(i);
            float(v as f64, format!("{v:e}"))
        }
        D::Float64 => {
            let v = col.as_primitive::<Float64Type>().value(i);
            float(v, format!("{v:e}"))
        }
        D::Decimal32(..) => col.as_primitive::<Decimal32Type>().value_as_string(i),
        D::Decimal64(..) => col.as_primitive::<Decimal64Type>().value_as_string(i),
        D::Decimal128(..) => col.as_primitive::<Decimal128Type>().value_as_string(i),
        D::Decimal256(..) => col.as_primitive::<Decimal256Type>().value_as_string(i),
        D::Utf8 => col.as_string::<i32>().value(i).to_string(),
        D::LargeUtf8 => col.as_string::<i64>().value(i).to_string(),
        D::Utf8View => col.as_string_view().value(i).to_string(),
        D::Binary | D::LargeBinary | D::BinaryView | D::FixedSizeBinary(_) => {
            let bytes: &[u8] = match col.data_type() {
                D::Binary => col.as_binary::<i32>().value(i),
                D::LargeBinary => col.as_binary::<i64>().value(i),
                D::BinaryView => col.as_binary_view().value(i),
                _ => col.as_fixed_size_binary().value(i),
            };
            let mut s = String::new();
            for &b in bytes {
                if (32..=126).contains(&b) && b != b'\\' && b != b'\'' && b != b'"' {
                    s.push(b as char);
                } else {
                    s.push_str(&format!("\\x{b:02X}"));
                }
            }
            s
        }
        D::Date32 => date(col.as_primitive::<Date32Type>().value(i) as i64),
        D::Date64 => date(
            col.as_primitive::<Date64Type>()
                .value(i)
                .div_euclid(86_400_000),
        ),
        D::Timestamp(unit, tz) => {
            let v = match unit {
                TimeUnit::Second => col.as_primitive::<TimestampSecondType>().value(i),
                TimeUnit::Millisecond => col.as_primitive::<TimestampMillisecondType>().value(i),
                TimeUnit::Microsecond => col.as_primitive::<TimestampMicrosecondType>().value(i),
                TimeUnit::Nanosecond => col.as_primitive::<TimestampNanosecondType>().value(i),
            };
            let s = timestamp(v, unit);
            if tz.is_some() { format!("{s}+00") } else { s }
        }
        D::Time32(TimeUnit::Second) => time_of_day(
            col.as_primitive::<Time32SecondType>().value(i) as i64 * 1_000_000,
            None,
        ),
        D::Time32(_) => time_of_day(
            col.as_primitive::<Time32MillisecondType>().value(i) as i64 * 1000,
            None,
        ),
        D::Time64(TimeUnit::Nanosecond) => {
            let v = col.as_primitive::<Time64NanosecondType>().value(i);
            let n = v % 1000;
            time_of_day(v / 1000, (n != 0).then_some(n))
        }
        D::Time64(_) => time_of_day(col.as_primitive::<Time64MicrosecondType>().value(i), None),
        D::Interval(IntervalUnit::MonthDayNano) => {
            let v = col.as_primitive::<IntervalMonthDayNanoType>().value(i);
            interval(v.months, v.days, v.nanoseconds / 1000)
        }
        D::Interval(IntervalUnit::DayTime) => {
            let v = col.as_primitive::<IntervalDayTimeType>().value(i);
            interval(0, v.days, v.milliseconds as i64 * 1000)
        }
        D::Interval(IntervalUnit::YearMonth) => {
            interval(col.as_primitive::<IntervalYearMonthType>().value(i), 0, 0)
        }
        D::List(_) | D::LargeList(_) | D::FixedSizeList(..) | D::ListView(_) => {
            let items: ArrayRef = match col.data_type() {
                D::List(_) => col.as_list::<i32>().value(i),
                D::LargeList(_) => col.as_list::<i64>().value(i),
                D::FixedSizeList(..) => col.as_fixed_size_list().value(i),
                _ => col.as_list_view::<i32>().value(i),
            };
            let parts: Vec<String> = (0..items.len()).map(|j| child(&items, j)).collect();
            format!("[{}]", parts.join(", "))
        }
        D::Struct(fields) => {
            let s = col.as_struct();
            let parts: Vec<String> = fields
                .iter()
                .zip(s.columns())
                .map(|(f, c)| format!("{}: {}", nested_string(f.name(), true), child(c, i)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        D::Map(..) => {
            let m = col.as_map();
            let entries = m.value(i);
            let keys = entries.column(0);
            let vals = entries.column(1);
            let parts: Vec<String> = (0..entries.len())
                .map(|j| format!("{}={}", child(keys, j), child(vals, j)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        D::Dictionary(..) => {
            let plain = arrow::compute::cast(&col.slice(i, 1), &dictionary_value(col.data_type()))
                .expect("dictionary unpacks");
            value(&plain, 0, nested)
        }
        _ => arrow::util::display::ArrayFormatter::try_new(
            col.as_ref(),
            &arrow::util::display::FormatOptions::default(),
        )
        .map(|f| f.value(i).to_string())
        .unwrap_or_else(|_| format!("<{}>", col.data_type())),
    }
}

fn dictionary_value(t: &DataType) -> DataType {
    match t {
        DataType::Dictionary(_, v) => (**v).clone(),
        other => other.clone(),
    }
}

/// A decimal text's digits at `scale`, rounded half away from zero as DuckDB's string-to-decimal
/// and string-to-integer casts round: `(negative, digits)` without leading zeros, or `None` if the
/// text is not a number.
pub fn scaled(text: &str, scale: i8) -> Option<(bool, String)> {
    let t = text.trim();
    let (neg, t) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (mant, exp) = match t.find(['e', 'E']) {
        Some(p) => (&t[..p], t[p + 1..].parse::<i32>().ok()?),
        None => (t, 0),
    };
    let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    if !int.chars().chain(frac.chars()).all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut digits: Vec<u8> = int.bytes().chain(frac.bytes()).map(|b| b - b'0').collect();
    // The value is digits * 10^(exp - frac.len()); we want it * 10^scale, rounded.
    let shift = exp - frac.len() as i32 + scale as i32;
    if shift >= 0 {
        digits.extend(std::iter::repeat_n(0, shift as usize));
    } else {
        let drop = (-shift) as usize;
        if drop > digits.len() + 1 {
            digits = vec![0];
        } else {
            let keep = digits.len().saturating_sub(drop);
            let round_up = drop <= digits.len() && digits[keep] >= 5;
            digits.truncate(keep);
            if round_up {
                let mut k = digits.len();
                loop {
                    if k == 0 {
                        digits.insert(0, 1);
                        break;
                    }
                    k -= 1;
                    if digits[k] == 9 {
                        digits[k] = 0;
                    } else {
                        digits[k] += 1;
                        break;
                    }
                }
            }
        }
    }
    let s: String = digits.iter().map(|d| (b'0' + d) as char).collect();
    let s = s.trim_start_matches('0');
    let s = if s.is_empty() { "0" } else { s };
    Some((neg && s != "0", s.to_string()))
}

fn parse_float(text: &str) -> Option<f64> {
    let t = text.trim();
    match t.to_ascii_lowercase().as_str() {
        "nan" | "-nan" => Some(f64::NAN),
        "inf" | "infinity" => Some(f64::INFINITY),
        "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
        _ => t.parse::<f64>().ok(),
    }
}

fn approx(l: f64, r: f64) -> bool {
    if l.is_nan() && r.is_nan() {
        return true;
    }
    if !l.is_finite() || !r.is_finite() {
        return l == r;
    }
    (l - r).abs() <= r.abs() * 0.01 + 0.000_000_01
}

fn approx32(l: f32, r: f32) -> bool {
    if l.is_nan() && r.is_nan() {
        return true;
    }
    if !l.is_finite() || !r.is_finite() {
        return l == r;
    }
    (l - r).abs() <= (r.abs() as f64 * 0.01 + 0.000_000_01) as f32
}

fn boolean(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// Whether the answer's text `actual`, in a column of `class`, matches the file's `expected`.
pub fn equal(actual: &str, expected: &str, class: Class) -> bool {
    if actual == expected {
        return true;
    }
    if let Some(re) = expected
        .strip_prefix("<REGEX>:")
        .map(|r| (r, true))
        .or_else(|| expected.strip_prefix("<!REGEX>:").map(|r| (r, false)))
    {
        let (pattern, want) = re;
        if let Ok(re) = regex::RegexBuilder::new(&format!("^(?:{pattern})$"))
            .dot_matches_new_line(true)
            .build()
            && re.is_match(actual) == want
        {
            return true;
        }
    }
    // One side NULL and the other not is a mismatch; both NULL was equal text above.
    if actual == "NULL" || expected == "NULL" {
        return false;
    }
    match class {
        Class::Float64 => match (parse_float(actual), parse_float(expected)) {
            // ValuesAreEqual(result, expected) compares ApproxEqual(expected, result): the epsilon
            // is 1% of the answer.
            (Some(a), Some(e)) => approx(e, a),
            _ => false,
        },
        Class::Float32 => match (parse_float(actual), parse_float(expected)) {
            (Some(a), Some(e)) => approx32(e as f32, a as f32),
            _ => false,
        },
        Class::Exact(scale) => match (scaled(actual, scale), scaled(expected, scale)) {
            (Some(a), Some(e)) => a == e,
            _ => false,
        },
        Class::Boolean => {
            matches!((boolean(actual), boolean(expected)), (Some(a), Some(e)) if a == e)
        }
        Class::Other => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(v: f64) -> String {
        float(v, format!("{v:e}"))
    }

    #[test]
    fn floats_as_duckdb_prints_them() {
        assert_eq!(f(1.0), "1.0");
        assert_eq!(f(0.1), "0.1");
        assert_eq!(f(-2.5), "-2.5");
        assert_eq!(f(1e15), "1000000000000000.0");
        assert_eq!(f(1e16), "1e+16");
        assert_eq!(f(1.5e20), "1.5e+20");
        assert_eq!(f(0.0001), "0.0001");
        assert_eq!(f(0.00001), "1e-05");
        assert_eq!(f(123.456), "123.456");
        assert_eq!(f(0.0), "0.0");
    }

    #[test]
    fn decimal_text_rounds_to_the_scale() {
        assert_eq!(scaled("1.5", 0), Some((false, "2".into())));
        assert_eq!(scaled("-1.5", 0), Some((true, "2".into())));
        assert_eq!(scaled("3.14159", 2), Some((false, "314".into())));
        assert_eq!(scaled("3.1", 3), Some((false, "3100".into())));
        assert_eq!(scaled("1e2", 0), Some((false, "100".into())));
        assert_eq!(scaled("0.4", 0), Some((false, "0".into())));
        assert_eq!(scaled("9.99", 1), Some((false, "100".into())));
        assert_eq!(scaled("abc", 0), None);
    }

    #[test]
    fn comparison_follows_the_runner() {
        assert!(equal("0.30000000000000004", "0.3", Class::Float64));
        assert!(!equal("0.31", "0.3", Class::Float64));
        assert!(equal("42", "42.0", Class::Exact(0)));
        assert!(!equal("42", "43", Class::Exact(0)));
        assert!(equal("1.50", "1.5", Class::Exact(2)));
        assert!(equal("1", "true", Class::Boolean));
        assert!(!equal("NULL", "0", Class::Exact(0)));
        assert!(!equal("abc", "abd", Class::Other));
        assert!(equal("abc", "<REGEX>:a.c", Class::Other));
    }

    #[test]
    fn nested_strings_quote_as_duckdb() {
        assert_eq!(nested_string("hello", false), "hello");
        assert_eq!(nested_string("a,b", false), "'a,b'");
        assert_eq!(nested_string("", false), "''");
        assert_eq!(nested_string("NULL", false), "'NULL'");
        assert_eq!(nested_string("it's", false), "'it\\'s'");
        assert_eq!(nested_string("k", true), "'k'");
    }

    /// A BLOB inside a STRUCT is written as text and quoted as a string, as DuckDB's cast does
    /// (test/sql/function/list/aggregates/minmax_nested.test:63).
    #[test]
    fn nested_blobs_quote_as_strings() {
        let col: ArrayRef = std::sync::Arc::new(arrow::array::BinaryArray::from(vec![
            b"long_prefix_\x100".as_slice(),
            b"plain".as_slice(),
        ]));
        assert_eq!(child(&col, 0), r"'long_prefix_\\x100'");
        assert_eq!(child(&col, 1), "plain");
    }

    #[test]
    fn dates_as_duckdb_prints_them() {
        assert_eq!(date(0), "1970-01-01");
        assert_eq!(date(-719_162), "0001-01-01");
        assert_eq!(date(-719_163), "0001-12-31 (BC)");
        assert_eq!(date(2_147_483_646), "5881580-07-10");
        assert_eq!(date(-2_147_483_646), "5877642-06-25 (BC)");
    }

    #[test]
    fn intervals_as_duckdb_prints_them() {
        assert_eq!(interval(14, 3, 0), "1 year 2 months 3 days");
        assert_eq!(interval(0, 0, 5_400_000_000), "01:30:00");
        assert_eq!(interval(0, 0, 0), "00:00:00");
        assert_eq!(interval(0, -1, -1_500_000), "-1 day -00:00:01.5");
    }
}
