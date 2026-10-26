//! Cell text at the value's own scale. A decimal keeps its trailing zeros.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Int64Type, TimeUnit, TimestampMicrosecondType,
};

pub fn type_name(dt: &DataType) -> String {
    match dt {
        DataType::Int64 => "INTEGER".to_string(),
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        DataType::Date32 => "DATE".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => "TIMESTAMP".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "TEXT".to_string(),
        DataType::Float64 => "FLOAT".to_string(),
        other => other.to_string(),
    }
}

pub fn is_numeric(dt: &DataType) -> bool {
    matches!(dt, DataType::Int64 | DataType::Decimal128(_, _))
}

pub fn cell_string(col: &dyn Array, row: usize) -> Option<String> {
    if col.is_null(row) {
        return None;
    }
    Some(match col.data_type() {
        DataType::Int64 => col.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::Boolean => col.as_boolean().value(row).to_string(),
        DataType::Utf8 => col.as_string::<i32>().value(row).to_string(),
        DataType::LargeUtf8 => col.as_string::<i64>().value(row).to_string(),
        DataType::Utf8View => col.as_string_view().value(row).to_string(),
        DataType::Date32 => format_date(col.as_primitive::<Date32Type>().value(row)),
        DataType::Decimal128(_, scale) => format_decimal(
            col.as_primitive::<Decimal128Type>().value(row),
            *scale as u8,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            format_timestamp(col.as_primitive::<TimestampMicrosecondType>().value(row))
        }
        DataType::Float64 => {
            let v = col
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row);
            if v.is_finite() {
                v.to_string()
            } else {
                "null".to_string()
            }
        }
        _ => "?".to_string(),
    })
}

pub fn format_decimal(unscaled: i128, scale: u8) -> String {
    let neg = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let body = if scale == 0 {
        digits
    } else {
        let scale = scale as usize;
        if digits.len() <= scale {
            format!("0.{}{digits}", "0".repeat(scale - digits.len()))
        } else {
            let split = digits.len() - scale;
            format!("{}.{}", &digits[..split], &digits[split..])
        }
    };
    if neg { format!("-{body}") } else { body }
}

pub fn format_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

pub fn format_timestamp(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    let days = i32::try_from(secs.div_euclid(86_400)).unwrap_or(i32::MAX);
    let tod = secs.rem_euclid(86_400) as u32;
    let (y, m, d) = civil_from_days(days);
    let hh = tod / 3600;
    let mm = (tod % 3600) / 60;
    let ss = tod % 60;
    if frac == 0 {
        format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
    } else {
        format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{frac:06}Z")
    }
}

/// Howard Hinnant's `civil_from_days`. `days` is a proleptic Gregorian day count
/// with day 0 at the Unix epoch.
fn civil_from_days(z: i32) -> (i32, u32, u32) {
    let z = z.wrapping_add(719_468);
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

pub fn is_canonical_number(s: &str) -> bool {
    let s = s.strip_prefix('-').unwrap_or(s);
    let (int, frac) = match s.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (s, None),
    };
    if int.is_empty() || !int.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    match frac {
        None => true,
        Some(f) => !f.is_empty() && f.bytes().all(|b| b.is_ascii_digit()),
    }
}

/// Group the integer part. `canonical` is an exact decimal, never a float.
/// The fractional digits are copied, so the scale does not change.
pub fn format_number(canonical: &str, group: &str, decimal: &str) -> String {
    if !is_canonical_number(canonical) {
        return canonical.to_string();
    }
    let neg = canonical.starts_with('-');
    let body = if neg { &canonical[1..] } else { canonical };
    let (int, frac) = match body.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (body, None),
    };
    let mut rev = String::new();
    for (i, c) in int.chars().rev().enumerate() {
        if i > 0 && i.is_multiple_of(3) && !group.is_empty() {
            for g in group.chars().rev() {
                rev.push(g);
            }
        }
        rev.push(c);
    }
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    out.extend(rev.chars().rev());
    if let Some(frac) = frac {
        out.push_str(decimal);
        out.push_str(frac);
    }
    out
}

pub fn is_ident(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub fn reserved_suffix(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.ends_with("__raw") || lower.ends_with("__hot") || lower.ends_with("__union")
}
