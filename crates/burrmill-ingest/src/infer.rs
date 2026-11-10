//! Type inference over a full read, then Arrow. A column that mostly fits keeps the type;
//! the cells that do not are nulls listed on the report, not silent coercions.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::csv::Parsed;
use crate::report::{ColumnReport, InferredType, IngestReport, Issue, RowIssue, TYPE_CONFIDENCE};
use crate::sniff::{Dialect, Locale};

#[derive(Clone, Copy)]
enum Kind {
    Empty,
    Bool(bool),
    Int(i64),
    Decimal { unscaled: i128, scale: u8 },
    Date(i32),
    Timestamp(i64),
    Text,
}

pub fn build(
    parsed: Parsed,
    dialect: Dialect,
    budget_bytes: u64,
) -> (Vec<RecordBatch>, IngestReport) {
    let width = parsed.rows.iter().map(|r| r.len()).max().unwrap_or(0);
    let header = is_header(&parsed.rows, dialect.locale);
    let data: &[Vec<String>] = if header {
        &parsed.rows[1..]
    } else {
        &parsed.rows
    };
    let names = column_names(
        header
            .then(|| parsed.rows.first().map(|r| r.as_slice()))
            .flatten(),
        width,
    );

    let mut report = IngestReport {
        dialect,
        header,
        rows: data.len() as u64,
        columns: Vec::with_capacity(width),
        rejected: Vec::new(),
        rejected_omitted: 0,
        budget_used: crate::budget::BudgetUse {
            bytes: budget_bytes,
            rows: data.len() as u64,
            columns: width as u64,
            memory_bytes: parsed.memory_bytes,
        },
    };

    for (physical, _) in parsed.unclosed_rows.iter().enumerate() {
        let row = parsed.unclosed_rows[physical];
        let data_row = if header { row.saturating_sub(1) } else { row };
        report.push_issue(RowIssue {
            row: data_row,
            column: None,
            reason: Issue::UnclosedQuote,
        });
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(width);
    for (col_idx, name) in names.iter().enumerate() {
        let mut kinds = Vec::with_capacity(data.len());
        let mut nulls = 0u64;
        for (row_idx, row) in data.iter().enumerate() {
            if col_idx >= row.len() {
                kinds.push(Kind::Empty);
                nulls += 1;
                if col_idx == row.len() {
                    report.push_issue(RowIssue {
                        row: (row_idx as u64) + 1,
                        column: Some(name.clone()),
                        reason: Issue::Ragged {
                            expected: width,
                            got: row.len(),
                        },
                    });
                }
                continue;
            }
            let kind = classify(&row[col_idx], report.dialect.locale);
            if matches!(kind, Kind::Empty) {
                nulls += 1;
            }
            kinds.push(kind);
        }
        let (inferred, matched) = choose(&kinds);
        let non_empty = data.len() as u64 - nulls;
        let array = materialise(data, col_idx, &kinds, &inferred, &mut report, name);
        report.columns.push(ColumnReport {
            name: name.clone(),
            inferred,
            nulls,
            non_empty,
            matched,
        });
        columns.push(array);
        let _ = col_idx;
    }

    let batches = if width == 0 {
        Vec::new()
    } else {
        let fields: Vec<Field> = report
            .columns
            .iter()
            .map(|c| Field::new(&c.name, arrow_type(&c.inferred), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        // 65_536 matches the engine's morsel. One batch is enough while rows are already resident.
        let batch =
            RecordBatch::try_new(schema, columns).expect("schema matches the columns just built");
        vec![batch]
    };

    (batches, report)
}

fn is_header(rows: &[Vec<String>], locale: Locale) -> bool {
    // One row is data. A first row of names above anything else is a header, including when
    // the rows under it are names too (`city,person` / `Sofia,Ivan`).
    if rows.len() < 2 {
        return false;
    }
    let first = &rows[0];
    first.iter().any(|c| !c.is_empty()) && first.iter().all(|c| c.is_empty() || is_name(c, locale))
}

fn is_name(cell: &str, locale: Locale) -> bool {
    !matches!(
        classify(cell, locale),
        Kind::Int(_) | Kind::Decimal { .. } | Kind::Date(_) | Kind::Timestamp(_) | Kind::Bool(_)
    )
}

fn column_names(header: Option<&[String]>, width: usize) -> Vec<String> {
    let mut names = Vec::with_capacity(width);
    for i in 0..width {
        let raw = header
            .and_then(|h| h.get(i))
            .map(|s| s.as_str())
            .unwrap_or("");
        let mut name = if raw.is_empty() {
            format!("column_{}", i + 1)
        } else {
            raw.to_string()
        };
        if names.iter().any(|n| n == &name) {
            let base = name.clone();
            let mut n = 2u32;
            while names.iter().any(|e| e == &name) {
                name = format!("{base}_{n}");
                n += 1;
            }
        }
        names.push(name);
    }
    names
}

fn choose(kinds: &[Kind]) -> (InferredType, u64) {
    let non_empty = kinds.iter().filter(|k| !matches!(k, Kind::Empty)).count();
    if non_empty == 0 {
        return (InferredType::Text, 0);
    }
    let n = non_empty as f64;
    let bools = count(kinds, |k| matches!(k, Kind::Bool(_)));
    let ints = count(kinds, |k| matches!(k, Kind::Int(_)));
    let decs = count(kinds, |k| matches!(k, Kind::Decimal { .. }));
    let dates = count(kinds, |k| matches!(k, Kind::Date(_)));
    let stamps = count(kinds, |k| matches!(k, Kind::Timestamp(_)));

    let ok = |matched: usize| matched as f64 / n >= TYPE_CONFIDENCE;

    if ok(bools) && ints == 0 && decs == 0 {
        return (InferredType::Boolean, bools as u64);
    }
    if decs > 0 && ok(ints + decs) {
        let (precision, scale) = decimal_params(kinds);
        return (
            InferredType::Decimal { precision, scale },
            (ints + decs) as u64,
        );
    }
    if ok(ints) {
        return (InferredType::Integer, ints as u64);
    }
    if ok(decs) {
        let (precision, scale) = decimal_params(kinds);
        return (InferredType::Decimal { precision, scale }, decs as u64);
    }
    if stamps > 0 && ok(dates + stamps) {
        return (InferredType::Timestamp, (dates + stamps) as u64);
    }
    if ok(dates) {
        return (InferredType::Date, dates as u64);
    }
    (InferredType::Text, non_empty as u64)
}

fn count(kinds: &[Kind], pred: impl Fn(&Kind) -> bool) -> usize {
    kinds.iter().filter(|k| pred(k)).count()
}

fn decimal_params(kinds: &[Kind]) -> (u8, i8) {
    let mut scale: u8 = 0;
    for k in kinds {
        if let Kind::Decimal { scale: s, .. } = k {
            scale = scale.max(*s);
        }
    }
    let mut precision: u8 = scale.max(1);
    for k in kinds {
        // At the column scale. Scale 0 under-counts, and the rescaled integer then exceeds precision.
        let unscaled = match k {
            Kind::Int(v) => rescale(*v as i128, 0, scale),
            Kind::Decimal { unscaled, scale: s } => rescale(*unscaled, *s, scale),
            _ => None,
        };
        if let Some(v) = unscaled {
            let digits = digit_count(v).max(scale);
            if digits <= 38 {
                precision = precision.max(digits);
            }
        }
    }
    (precision, scale as i8)
}

fn digit_count(v: i128) -> u8 {
    let n = v.unsigned_abs();
    if n == 0 { 1 } else { n.ilog10() as u8 + 1 }
}

fn rescale(unscaled: i128, from: u8, to: u8) -> Option<i128> {
    if to < from {
        return None;
    }
    let mut factor = 1i128;
    for _ in 0..(to - from) {
        factor = factor.checked_mul(10)?;
    }
    unscaled.checked_mul(factor)
}

fn materialise(
    data: &[Vec<String>],
    col_idx: usize,
    kinds: &[Kind],
    inferred: &InferredType,
    report: &mut IngestReport,
    name: &str,
) -> ArrayRef {
    match inferred {
        InferredType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(kinds.len());
            for (i, k) in kinds.iter().enumerate() {
                match k {
                    Kind::Bool(v) => b.append_value(*v),
                    Kind::Empty => b.append_null(),
                    _ => {
                        b.append_null();
                        mismatch(report, i, name);
                    }
                }
            }
            Arc::new(b.finish())
        }
        InferredType::Integer => {
            let mut b = Int64Builder::with_capacity(kinds.len());
            for (i, k) in kinds.iter().enumerate() {
                match k {
                    Kind::Int(v) => b.append_value(*v),
                    Kind::Empty => b.append_null(),
                    _ => {
                        b.append_null();
                        mismatch(report, i, name);
                    }
                }
            }
            Arc::new(b.finish())
        }
        InferredType::Decimal { precision, scale } => {
            let mut b = Decimal128Builder::with_capacity(kinds.len());
            let scale_u = *scale as u8;
            for (i, k) in kinds.iter().enumerate() {
                let unscaled = match k {
                    Kind::Int(v) => rescale(*v as i128, 0, scale_u),
                    Kind::Decimal { unscaled, scale: s } => rescale(*unscaled, *s, scale_u),
                    Kind::Empty => None,
                    _ => {
                        b.append_null();
                        mismatch(report, i, name);
                        continue;
                    }
                };
                match (k, unscaled) {
                    (Kind::Empty, _) => b.append_null(),
                    (_, Some(v)) if digit_count(v) <= *precision => b.append_value(v),
                    _ => {
                        b.append_null();
                        mismatch(report, i, name);
                    }
                }
            }
            let array = b
                .finish()
                .with_precision_and_scale(*precision, *scale)
                .expect("precision holds the values");
            Arc::new(array)
        }
        InferredType::Date => {
            let mut b = Date32Builder::with_capacity(kinds.len());
            for (i, k) in kinds.iter().enumerate() {
                match k {
                    Kind::Date(v) => b.append_value(*v),
                    Kind::Empty => b.append_null(),
                    _ => {
                        b.append_null();
                        mismatch(report, i, name);
                    }
                }
            }
            Arc::new(b.finish())
        }
        InferredType::Timestamp => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(kinds.len());
            for (i, k) in kinds.iter().enumerate() {
                match k {
                    Kind::Timestamp(v) => b.append_value(*v),
                    Kind::Date(days) => b.append_value(*days as i64 * 86_400 * 1_000_000),
                    Kind::Empty => b.append_null(),
                    _ => {
                        b.append_null();
                        mismatch(report, i, name);
                    }
                }
            }
            Arc::new(b.finish())
        }
        InferredType::Text => {
            let mut b = StringBuilder::with_capacity(kinds.len(), kinds.len() * 8);
            for row in data {
                match row.get(col_idx).map(|s| s.as_str()).unwrap_or("") {
                    "" => b.append_null(),
                    cell => b.append_value(cell),
                }
            }
            Arc::new(b.finish())
        }
    }
}

fn mismatch(report: &mut IngestReport, row_idx: usize, name: &str) {
    report.push_issue(RowIssue {
        row: (row_idx as u64) + 1,
        column: Some(name.to_string()),
        reason: Issue::TypeMismatch,
    });
}

fn arrow_type(t: &InferredType) -> DataType {
    match t {
        InferredType::Integer => DataType::Int64,
        InferredType::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
        InferredType::Date => DataType::Date32,
        InferredType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        InferredType::Boolean => DataType::Boolean,
        InferredType::Text => DataType::Utf8,
    }
}

fn classify(cell: &str, locale: Locale) -> Kind {
    let cell = cell.trim();
    if cell.is_empty() {
        return Kind::Empty;
    }
    if let Some(v) = parse_bool(cell) {
        return Kind::Bool(v);
    }
    if let Some(v) = parse_timestamp(cell) {
        return Kind::Timestamp(v);
    }
    if let Some(v) = parse_date(cell) {
        return Kind::Date(v);
    }
    if let Some(num) = parse_number(cell, locale) {
        return num;
    }
    Kind::Text
}

fn parse_bool(cell: &str) -> Option<bool> {
    if cell.eq_ignore_ascii_case("true") || cell.eq_ignore_ascii_case("yes") {
        Some(true)
    } else if cell.eq_ignore_ascii_case("false") || cell.eq_ignore_ascii_case("no") {
        Some(false)
    } else {
        None
    }
}

fn parse_date(cell: &str) -> Option<i32> {
    if let Some(days) = parse_iso_date(cell) {
        return Some(days);
    }
    // DD.MM.YYYY, the form Bulgarian exports use.
    let mut parts = cell.split('.');
    let d: i32 = parts.next()?.parse().ok()?;
    let m: i32 = parts.next()?.parse().ok()?;
    let y: i32 = parts.next()?.parse().ok()?;
    if parts.next().is_some()
        || !(1..=31).contains(&d)
        || !(1..=12).contains(&m)
        || !(1..=9999).contains(&y)
    {
        return None;
    }
    // Four-digit year. `1.2.3` is not a date.
    if cell.len() < 8 {
        return None;
    }
    Some(days_from_civil(y, m, d))
}

fn parse_iso_date(cell: &str) -> Option<i32> {
    if cell.len() != 10
        || cell.as_bytes().get(4) != Some(&b'-')
        || cell.as_bytes().get(7) != Some(&b'-')
    {
        return None;
    }
    let y: i32 = cell[0..4].parse().ok()?;
    let m: i32 = cell[5..7].parse().ok()?;
    let d: i32 = cell[8..10].parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(days_from_civil(y, m, d))
}

fn parse_timestamp(cell: &str) -> Option<i64> {
    let (date, time) = cell.split_once(['T', ' '])?;
    if time.len() < 8 {
        return None;
    }
    let days = parse_iso_date(date)?;
    let h: i64 = time[0..2].parse().ok()?;
    let min: i64 = time[3..5].parse().ok()?;
    let s: i64 = time[6..8].parse().ok()?;
    if time.as_bytes().get(2) != Some(&b':') || time.as_bytes().get(5) != Some(&b':') {
        return None;
    }
    if h > 23 || min > 59 || s > 60 {
        return None;
    }
    let mut micros = (days as i64) * 86_400 * 1_000_000 + (h * 3600 + min * 60 + s) * 1_000_000;
    if time.as_bytes().get(8) == Some(&b'.') {
        let frac: String = time[9..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .take(6)
            .collect();
        if !frac.is_empty() {
            let padded = format!("{frac:0<6}");
            if let Ok(extra) = padded.parse::<i64>() {
                micros += extra;
            }
        }
    }
    Some(micros)
}

fn parse_number(cell: &str, locale: Locale) -> Option<Kind> {
    let (decimal, group) = match locale {
        Locale::Point => ('.', ','),
        Locale::Comma => (',', '.'),
    };
    let mut s = cell;
    let neg = if let Some(rest) = s.strip_prefix('-') {
        s = rest;
        true
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
        false
    } else {
        false
    };
    if s.is_empty() || !s.chars().next()?.is_ascii_digit() {
        return None;
    }
    if s.chars()
        .any(|c| !(c.is_ascii_digit() || c == decimal || c == group))
    {
        return None;
    }
    let (whole, frac) = match s.split_once(decimal) {
        Some((w, f)) => {
            if f.is_empty() || !f.chars().all(|c| c.is_ascii_digit()) || f.contains(group) {
                return None;
            }
            (w, f)
        }
        None => (s, ""),
    };
    if whole.is_empty() {
        return None;
    }
    let whole_digits: String = whole.chars().filter(|c| *c != group).collect();
    if whole_digits.is_empty() || !whole_digits.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    // A grouping mark with no digits around it, or a group char in the wrong place, is text.
    if whole.contains(group) && !grouped_ok(whole, group) {
        return None;
    }
    let scale = frac.len() as u8;
    let digits = format!("{whole_digits}{frac}");
    if digits.len() > 38 {
        return None;
    }
    let mut unscaled: i128 = digits.parse().ok()?;
    if neg {
        unscaled = -unscaled;
    }
    if scale == 0
        && let Ok(v) = i64::try_from(unscaled)
    {
        return Some(Kind::Int(v));
    }
    Some(Kind::Decimal { unscaled, scale })
}

fn grouped_ok(whole: &str, group: char) -> bool {
    let parts: Vec<&str> = whole.split(group).collect();
    if parts.len() < 2 || parts.iter().any(|p| p.is_empty()) {
        return false;
    }
    parts[0].len() <= 3 && parts[1..].iter().all(|p| p.len() == 3)
}

/// Howard Hinnant's `days_from_civil`. 1970-01-01 is 0.
fn days_from_civil(mut y: i32, m: i32, d: i32) -> i32 {
    y -= i32::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = (y - era * 400) as u32;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u32;
    era * 146097 + doe as i32 - 719468
}
