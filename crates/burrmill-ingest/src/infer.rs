//! Type inference over a full read, then Arrow. A column that mostly fits keeps the type;
//! the cells that do not are nulls listed on the report, not silent coercions.
//!
//! The one path for CSV and JSON, in the tab and the CLI. A column is read into a [`Tally`] of
//! counts, which add across chunks of a file read in parallel; the counts choose its type, and a
//! [`BatchBuilder`] then turns rows into Arrow under that choice.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, BooleanBuilder, Date32Builder, Decimal128Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

#[cfg(feature = "json")]
use crate::json::{Parsed, Row};
use crate::report::{
    ColumnReport, InferredType, IngestReport, Issue, MAX_ISSUES, RowIssue, TYPE_CONFIDENCE,
};
#[cfg(feature = "json")]
use crate::sniff::Dialect;
use crate::sniff::Locale;

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

/// A row's fields, whoever parsed them.
pub(crate) trait Cells {
    fn len(&self) -> usize;
    fn cell(&self, i: usize) -> &str;
}

#[cfg(feature = "json")]
impl Cells for Row {
    fn len(&self) -> usize {
        Row::len(self)
    }

    fn cell(&self, i: usize) -> &str {
        &self[i]
    }
}

impl Cells for Vec<String> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn cell(&self, i: usize) -> &str {
        &self[i]
    }
}

/// How a column's non-empty cells read under one locale. Everything type choice needs, as counts
/// that add: a decimal's precision is kept as which digit counts occur at each scale.
#[derive(Clone)]
struct Tally {
    non_empty: u64,
    bools: u64,
    ints: u64,
    decs: u64,
    dates: u64,
    stamps: u64,
    scale: u8,
    /// Bit `d` of `digits[s]`: a non-zero number of `d` digits was written at scale `s`.
    digits: [u64; 39],
    zero_led: bool,
}

impl Default for Tally {
    fn default() -> Self {
        Self {
            non_empty: 0,
            bools: 0,
            ints: 0,
            decs: 0,
            dates: 0,
            stamps: 0,
            scale: 0,
            digits: [0; 39],
            zero_led: false,
        }
    }
}

impl Tally {
    fn add(&mut self, kind: Kind, cell: &str) {
        match kind {
            Kind::Empty => return,
            Kind::Bool(_) => self.bools += 1,
            Kind::Int(v) => {
                self.ints += 1;
                self.digit(0, v as i128);
                self.zero_led |= leading_zero(cell);
            }
            Kind::Decimal { unscaled, scale } => {
                self.decs += 1;
                self.scale = self.scale.max(scale);
                self.digit(scale, unscaled);
                self.zero_led |= leading_zero(cell);
            }
            Kind::Date(_) => self.dates += 1,
            Kind::Timestamp(_) => self.stamps += 1,
            Kind::Text => {}
        }
        self.non_empty += 1;
    }

    fn digit(&mut self, scale: u8, v: i128) {
        if v != 0 {
            self.digits[scale as usize] |= 1 << digit_count(v);
        }
    }

    fn merge(&mut self, o: &Tally) {
        self.non_empty += o.non_empty;
        self.bools += o.bools;
        self.ints += o.ints;
        self.decs += o.decs;
        self.dates += o.dates;
        self.stamps += o.stamps;
        self.scale = self.scale.max(o.scale);
        for (a, b) in self.digits.iter_mut().zip(o.digits) {
            *a |= b;
        }
        self.zero_led |= o.zero_led;
    }

    fn choose(&self) -> (InferredType, u64) {
        if self.non_empty == 0 {
            return (InferredType::Text, 0);
        }
        let n = self.non_empty as f64;
        let (bools, ints, decs, dates, stamps) =
            (self.bools, self.ints, self.decs, self.dates, self.stamps);
        let ok = |matched: u64| matched as f64 / n >= TYPE_CONFIDENCE;

        if ok(bools) && ints == 0 && decs == 0 {
            return (InferredType::Boolean, bools);
        }
        if decs > 0 && ok(ints + decs) {
            let (precision, scale) = self.decimal_params();
            return (InferredType::Decimal { precision, scale }, ints + decs);
        }
        if ok(ints) {
            return (InferredType::Integer, ints);
        }
        if ok(decs) {
            let (precision, scale) = self.decimal_params();
            return (InferredType::Decimal { precision, scale }, decs);
        }
        if stamps > 0 && ok(dates + stamps) {
            return (InferredType::Timestamp, dates + stamps);
        }
        if ok(dates) {
            return (InferredType::Date, dates);
        }
        (InferredType::Text, self.non_empty)
    }

    /// The column scale is the widest written; precision the most digits any number takes at that
    /// scale, leaving out one that needs more than 38. A zero takes one digit.
    fn decimal_params(&self) -> (u8, i8) {
        let scale = self.scale;
        let mut precision: u8 = scale.max(1);
        for (s, mask) in self.digits.iter().enumerate().take(scale as usize + 1) {
            for d in 1..64u8 {
                if mask & (1 << d) == 0 {
                    continue;
                }
                let at_scale = d + (scale - s as u8);
                if at_scale <= 38 {
                    precision = precision.max(at_scale);
                }
            }
        }
        (precision, scale as i8)
    }
}

/// One column's cells read under both locales, its votes on the decimal mark, and its bytes. A cell
/// with no `.` or `,` reads the same under either, and is counted once in `shared`.
#[derive(Clone, Default)]
pub(crate) struct ColumnTally {
    shared: Tally,
    point: Tally,
    comma: Tally,
    votes: Votes,
    /// Bytes of its cells, which a text column holds.
    pub bytes: u64,
}

impl ColumnTally {
    /// Counts `raw`, and says what it reads as under the point and the comma locale.
    fn observe(&mut self, raw: &str) -> (Kind, Kind) {
        self.bytes += raw.len() as u64;
        let cell = trim(raw);
        if cell.is_empty() {
            return (Kind::Empty, Kind::Empty);
        }
        if !cell.bytes().any(|b| b == b'.' || b == b',') {
            // No mark: the locale cannot change what it reads as.
            let kind = classify_trimmed(cell, Locale::Point);
            self.shared.add(kind, cell);
            return (kind, kind);
        }
        // A cell votes only for a mark it can be read under alone: `1,5` and `1.234,56` say comma,
        // `1.234` and `1,234` say nothing, and `items 1,2,3` is not a number at all.
        let fixed = classify_fixed(cell);
        let point = parse_number(cell, Locale::Point);
        let comma = parse_number(cell, Locale::Comma);
        match (point.is_some(), comma.is_some()) {
            (true, true) => self.votes.ambiguous += 1,
            (true, false) => self.votes.point += 1,
            (false, true) => self.votes.comma += 1,
            (false, false) => {}
        }
        let (point, comma) = (
            fixed.or(point).unwrap_or(Kind::Text),
            fixed.or(comma).unwrap_or(Kind::Text),
        );
        self.point.add(point, cell);
        self.comma.add(comma, cell);
        (point, comma)
    }

    pub fn merge(&mut self, o: &ColumnTally) {
        self.shared.merge(&o.shared);
        self.point.merge(&o.point);
        self.comma.merge(&o.comma);
        self.votes.point += o.votes.point;
        self.votes.comma += o.votes.comma;
        self.votes.ambiguous += o.votes.ambiguous;
        self.bytes += o.bytes;
    }

    fn under(&self, locale: Locale) -> Tally {
        let mut t = self.shared.clone();
        t.merge(match locale {
            Locale::Point => &self.point,
            Locale::Comma => &self.comma,
        });
        t
    }
}

/// Adds `row`'s cells to `tallies`, widening it to the row.
pub(crate) fn tally_row(tallies: &mut Vec<ColumnTally>, row: &impl Cells) {
    if tallies.len() < row.len() {
        tallies.resize_with(row.len(), ColumnTally::default);
    }
    for (i, t) in tallies.iter_mut().enumerate().take(row.len()) {
        t.observe(row.cell(i));
    }
}

/// Names over a column that is not text. Text over text (`Ivan,Petrov` / `Maria,Ivanova`) cannot
/// be told from data, and a value taken as a name would leave in the schema. `rest` is every row
/// after the first, of which there are `rows_after`.
pub(crate) fn is_header(
    first: &[String],
    rest: &[ColumnTally],
    rows_after: u64,
    locale: Locale,
) -> bool {
    if rows_after < 1 {
        return false;
    }
    if !first.iter().all(|c| c.is_empty() || is_name(c, locale)) {
        return false;
    }
    first.iter().enumerate().any(|(i, cell)| {
        !cell.is_empty()
            && rest
                .get(i)
                .is_some_and(|t| !matches!(t.under(locale).choose().0, InferredType::Text))
    })
}

fn is_name(cell: &str, locale: Locale) -> bool {
    !matches!(
        classify(cell, locale),
        Kind::Int(_) | Kind::Decimal { .. } | Kind::Date(_) | Kind::Timestamp(_) | Kind::Bool(_)
    )
}

pub(crate) fn column_names(header: Option<&[String]>, width: usize) -> Vec<String> {
    unique_names((0..width).map(|i| {
        header
            .and_then(|h| h.get(i))
            .map(String::as_str)
            .unwrap_or("")
    }))
}

/// `column_N` for a blank name, and `name_2`, `name_3` for a repeat.
pub(crate) fn unique_names<'a>(raw: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for (i, raw) in raw.enumerate() {
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

/// What each column is read as.
pub(crate) struct Plan {
    pub names: Vec<String>,
    pub columns: Vec<ColumnReport>,
    locales: Vec<Locale>,
    pub schema: SchemaRef,
    /// The file's decimal mark, as the report gives it.
    pub locale: Locale,
}

impl Plan {
    /// `tallies` over the `rows` data rows. `nested` flags the JSON columns that held objects.
    pub fn new(
        names: Vec<String>,
        tallies: &[ColumnTally],
        rows: u64,
        sniffed: Locale,
        nested: Option<&[bool]>,
    ) -> Self {
        let empty = ColumnTally::default();
        let tally = |i: usize| tallies.get(i).unwrap_or(&empty);
        let point: usize = (0..names.len()).map(|i| tally(i).votes.point).sum();
        let comma: usize = (0..names.len()).map(|i| tally(i).votes.comma).sum();
        let file_locale = match point.cmp(&comma) {
            std::cmp::Ordering::Greater => Some(Locale::Point),
            std::cmp::Ordering::Less => Some(Locale::Comma),
            std::cmp::Ordering::Equal => None,
        };
        let locale = file_locale.unwrap_or(sniffed);
        let mut columns = Vec::with_capacity(names.len());
        let mut locales = Vec::with_capacity(names.len());
        for (i, name) in names.iter().enumerate() {
            let column_locale = tally(i).votes.locale(file_locale, locale);
            let t = &tally(i).under(column_locale.unwrap_or(locale));
            let (mut inferred, mut matched) = t.choose();
            let mut note = None;
            let numeric = matches!(
                inferred,
                InferredType::Integer | InferredType::Decimal { .. }
            );
            if numeric && t.zero_led {
                note = Some(NOTE_LEADING_ZERO);
            } else if numeric && column_locale.is_none() {
                note = Some(NOTE_AMBIGUOUS_MARK);
            }
            if nested.is_some_and(|nested| nested[i]) {
                note = Some(NOTE_NESTED);
            }
            if note.is_some() {
                (inferred, matched) = (InferredType::Text, t.non_empty);
            }
            columns.push(ColumnReport {
                name: name.clone(),
                inferred,
                nulls: rows - t.non_empty,
                non_empty: t.non_empty,
                matched,
                note,
            });
            locales.push(column_locale.unwrap_or(locale));
        }
        Self {
            names,
            schema: schema(&columns),
            columns,
            locales,
            locale,
        }
    }

    /// As this, with every decimal at precision 38: a batch built under it holds the same values
    /// as under any plan that [`Plan::reads_as`] this one, and [`Plan::retag`] makes it that one's.
    pub fn widened(mut self) -> Self {
        for c in &mut self.columns {
            if let InferredType::Decimal { precision, .. } = &mut c.inferred {
                *precision = 38;
            }
        }
        self.schema = schema(&self.columns);
        self
    }

    /// Whether `other` reads every cell as this does: the same names, decimal marks and types,
    /// a decimal's precision aside.
    pub fn reads_as(&self, other: &Plan) -> bool {
        self.names == other.names
            && self.locales == other.locales
            && self.columns.len() == other.columns.len()
            && self.columns.iter().zip(&other.columns).all(|(a, b)| {
                match (&a.inferred, &b.inferred) {
                    (
                        InferredType::Decimal { scale: s, .. },
                        InferredType::Decimal { scale: t, .. },
                    ) => s == t,
                    (a, b) => a == b,
                }
            })
    }

    /// A batch built under a plan that reads as this one, typed as this one.
    pub fn retag(&self, batch: RecordBatch) -> RecordBatch {
        let arrays = batch
            .columns()
            .iter()
            .zip(&self.columns)
            .map(|(a, c)| match c.inferred {
                InferredType::Decimal { precision, scale } => {
                    let a = a
                        .as_primitive::<arrow::datatypes::Decimal128Type>()
                        .clone()
                        .with_precision_and_scale(precision, scale)
                        .expect("precision holds the values");
                    Arc::new(a) as ArrayRef
                }
                _ => Arc::clone(a),
            })
            .collect();
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .expect("a plan that reads as this one builds these columns")
    }

    /// Bytes of Arrow `rows` rows in `batches` batches take, counted before they are built, with
    /// a text column's cells' bytes from `tallies`.
    pub fn estimate(&self, rows: u64, batches: u64, tallies: &[ColumnTally]) -> u64 {
        let mut total = 0u64;
        for (i, c) in self.columns.iter().enumerate() {
            let fixed = match c.inferred {
                InferredType::Integer | InferredType::Timestamp => rows * 8,
                InferredType::Decimal { .. } => rows * 16,
                InferredType::Date => rows * 4,
                InferredType::Boolean => rows.div_ceil(8),
                InferredType::Text => (rows + batches) * 4 + tallies.get(i).map_or(0, |t| t.bytes),
            };
            total += fixed + rows.div_ceil(8);
        }
        total
    }
}

fn schema(columns: &[ColumnReport]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, arrow_type(&c.inferred), true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Issues found while building, by column, each list capped as the report caps it.
#[derive(Default)]
pub(crate) struct Issues {
    by_column: Vec<(Vec<RowIssue>, u64)>,
}

impl Issues {
    fn push(&mut self, column: usize, issue: impl FnOnce() -> RowIssue) {
        let (kept, total) = &mut self.by_column[column];
        if kept.len() < MAX_ISSUES {
            kept.push(issue());
        }
        *total += 1;
    }

    /// Rows numbered from 1 in a chunk, numbered from 1 in the file: `base` rows came before.
    pub fn renumber(&mut self, base: u64) {
        for (kept, _) in &mut self.by_column {
            for issue in kept {
                issue.row += base;
            }
        }
    }

    /// Onto `report` in its order: column by column, each in row order across `parts`.
    pub fn drain_into(parts: Vec<Issues>, width: usize, report: &mut IngestReport) {
        let mut parts = parts;
        for column in 0..width {
            for part in &mut parts {
                let (kept, total) = std::mem::take(&mut part.by_column[column]);
                let omitted = total - kept.len() as u64;
                for issue in kept {
                    report.push_issue(issue);
                }
                report.rejected_omitted += omitted;
            }
        }
    }
}

enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Integer(Int64Builder),
    Decimal(Decimal128Builder, u8, i8),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Text(StringBuilder),
}

impl ColumnBuilder {
    fn new(t: &InferredType, rows: usize, bytes: usize) -> Self {
        match t {
            InferredType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(rows)),
            InferredType::Integer => Self::Integer(Int64Builder::with_capacity(rows)),
            InferredType::Decimal { precision, scale } => {
                Self::Decimal(Decimal128Builder::with_capacity(rows), *precision, *scale)
            }
            InferredType::Date => Self::Date(Date32Builder::with_capacity(rows)),
            InferredType::Timestamp => {
                Self::Timestamp(TimestampMicrosecondBuilder::with_capacity(rows))
            }
            InferredType::Text => Self::Text(StringBuilder::with_capacity(rows, bytes)),
        }
    }

    /// Appends `cell`, or a null where the row has no such field. False where the cell does not
    /// read as the column's type, and a null went in for it.
    fn push(&mut self, cell: Option<&str>, locale: Locale) -> bool {
        let Some(cell) = cell else {
            self.push_null();
            return true;
        };
        if let Self::Text(b) = self {
            match cell {
                "" => b.append_null(),
                cell => b.append_value(cell),
            }
            return true;
        }
        let cell = trim(cell);
        if cell.is_empty() {
            self.push_null();
            return true;
        }
        // What `classify` would say, asking only the question the column's type asks. Under the
        // point locale nothing that reads as a boolean, date or timestamp reads as a number.
        match self {
            Self::Boolean(b) => {
                return match parse_bool(cell) {
                    Some(v) => {
                        b.append_value(v);
                        true
                    }
                    None => {
                        b.append_null();
                        false
                    }
                };
            }
            Self::Integer(b) if locale == Locale::Point => {
                return match parse_number(cell, locale) {
                    Some(Kind::Int(v)) => {
                        b.append_value(v);
                        true
                    }
                    _ => {
                        b.append_null();
                        false
                    }
                };
            }
            Self::Date(b) => {
                return match (cell.len() >= 8).then(|| parse_date(cell)).flatten() {
                    Some(v) => {
                        b.append_value(v);
                        true
                    }
                    None => {
                        b.append_null();
                        false
                    }
                };
            }
            _ => {}
        }
        let kind = if locale == Locale::Point && matches!(self, Self::Decimal(..)) {
            parse_number(cell, locale).unwrap_or(Kind::Text)
        } else {
            classify_trimmed(cell, locale)
        };
        self.take(kind)
    }

    /// `cell`, already read as `kind` under the column's locale.
    fn push_kind(&mut self, cell: &str, kind: Kind) -> bool {
        if let Self::Text(b) = self {
            match cell {
                "" => b.append_null(),
                cell => b.append_value(cell),
            }
            return true;
        }
        self.take(kind)
    }

    /// A value of `kind` into a column that is not text.
    fn take(&mut self, kind: Kind) -> bool {
        match (self, kind) {
            (Self::Boolean(b), Kind::Bool(v)) => b.append_value(v),
            (Self::Integer(b), Kind::Int(v)) => b.append_value(v),
            (Self::Decimal(b, precision, scale), Kind::Int(_) | Kind::Decimal { .. }) => {
                let unscaled = match kind {
                    Kind::Int(v) => rescale(v as i128, 0, *scale as u8),
                    Kind::Decimal { unscaled, scale: s } => rescale(unscaled, s, *scale as u8),
                    _ => None,
                };
                match unscaled {
                    Some(v) if digit_count(v) <= *precision => b.append_value(v),
                    _ => {
                        b.append_null();
                        return false;
                    }
                }
            }
            (Self::Date(b), Kind::Date(v)) => b.append_value(v),
            (Self::Timestamp(b), Kind::Timestamp(v)) => b.append_value(v),
            (Self::Timestamp(b), Kind::Date(days)) => {
                b.append_value(days as i64 * 86_400 * 1_000_000)
            }
            (this, Kind::Empty) => this.push_null(),
            (this, _) => {
                this.push_null();
                return false;
            }
        }
        true
    }

    fn push_null(&mut self) {
        match self {
            Self::Boolean(b) => b.append_null(),
            Self::Integer(b) => b.append_null(),
            Self::Decimal(b, ..) => b.append_null(),
            Self::Date(b) => b.append_null(),
            Self::Timestamp(b) => b.append_null(),
            Self::Text(b) => b.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::Integer(mut b) => Arc::new(b.finish()),
            Self::Decimal(mut b, precision, scale) => Arc::new(
                b.finish()
                    .with_precision_and_scale(precision, scale)
                    .expect("precision holds the values"),
            ),
            Self::Date(mut b) => Arc::new(b.finish()),
            Self::Timestamp(mut b) => Arc::new(b.finish()),
            Self::Text(mut b) => Arc::new(b.finish()),
        }
    }
}

/// Rows into one batch under a [`Plan`], noting what does not fit.
pub(crate) struct BatchBuilder<'a> {
    plan: &'a Plan,
    columns: Vec<ColumnBuilder>,
    issues: Issues,
    /// CSV: a short row is ragged. JSON: a record is missing its last keys.
    ragged: bool,
}

impl<'a> BatchBuilder<'a> {
    /// `bytes` per column is what a text column will hold, a capacity and not a bound.
    pub fn new(plan: &'a Plan, rows: usize, bytes: &[u64], ragged: bool) -> Self {
        let columns = plan
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let b = bytes.get(i).copied().unwrap_or(0) as usize;
                ColumnBuilder::new(&c.inferred, rows, b)
            })
            .collect();
        Self {
            plan,
            columns,
            issues: Issues {
                by_column: vec![(Vec::new(), 0); plan.columns.len()],
            },
            ragged,
        }
    }

    /// `row` as 1-based data row `at`.
    pub fn push(&mut self, row: &impl Cells, at: u64) {
        self.push_counting(row, at, None);
    }

    /// As [`BatchBuilder::push`], counting the row into `tallies` too, each cell read once.
    pub fn push_counting(
        &mut self,
        row: &impl Cells,
        at: u64,
        mut tallies: Option<&mut Vec<ColumnTally>>,
    ) {
        let len = row.len();
        if let Some(t) = tallies.as_deref_mut()
            && t.len() < len
        {
            t.resize_with(len, ColumnTally::default);
        }
        if let Some(t) = tallies.as_deref_mut() {
            // Cells past the guessed width are counted and not built.
            for (i, tally) in t.iter_mut().enumerate().take(len).skip(self.columns.len()) {
                tally.observe(row.cell(i));
            }
        }
        for (i, b) in self.columns.iter_mut().enumerate() {
            let cell = (i < len).then(|| row.cell(i));
            let locale = self.plan.locales[i];
            let fits = match (cell, tallies.as_deref_mut()) {
                (Some(c), Some(t)) => {
                    let (point, comma) = t[i].observe(c);
                    let kind = match locale {
                        Locale::Point => point,
                        Locale::Comma => comma,
                    };
                    b.push_kind(c, kind)
                }
                _ => b.push(cell, locale),
            };
            let reason = if !fits {
                Issue::TypeMismatch
            } else if i == len && self.ragged {
                Issue::Ragged {
                    expected: self.plan.columns.len(),
                    got: len,
                }
            } else {
                continue;
            };
            let names = &self.plan.names;
            self.issues.push(i, || RowIssue {
                row: at,
                column: Some(names[i].clone()),
                reason,
            });
        }
    }

    pub fn finish(self) -> (RecordBatch, Issues) {
        let arrays = self
            .columns
            .into_iter()
            .map(ColumnBuilder::finish)
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&self.plan.schema), arrays)
            .expect("schema matches the columns just built");
        (batch, self.issues)
    }
}

/// JSON's records: the first row is the names whatever it looks like, a short row is missing its
/// last keys, and a column flagged in `nested` held nested values.
#[cfg(feature = "json")]
pub fn build_keyed(
    parsed: Parsed,
    dialect: Dialect,
    budget_bytes: u64,
    nested: &[bool],
) -> (Vec<RecordBatch>, IngestReport) {
    let width = parsed.rows.iter().map(|r| r.len()).max().unwrap_or(0);
    let data: &[Row] = parsed.rows.get(1..).unwrap_or(&[]);
    let header: Option<Vec<String>> = parsed
        .rows
        .first()
        .map(|h| h.iter().map(str::to_string).collect());
    let names = column_names(header.as_deref(), width);
    let mut tallies = Vec::new();
    for row in data {
        tally_row(&mut tallies, row);
    }
    let rows = data.len() as u64;
    let plan = Plan::new(names, &tallies, rows, dialect.locale, Some(nested));
    let mut report = IngestReport {
        format: crate::report::Format::Json,
        dialect,
        header: true,
        rows,
        columns: Vec::new(),
        rejected: Vec::new(),
        rejected_omitted: 0,
        budget_used: crate::budget::BudgetUse {
            bytes: budget_bytes,
            rows,
            columns: width as u64,
            memory_bytes: parsed.memory_bytes,
        },
    };
    report.dialect.locale = plan.locale;
    let batches = if width == 0 {
        Vec::new()
    } else {
        let bytes: Vec<u64> = tallies.iter().map(|t| t.bytes).collect();
        let mut b = BatchBuilder::new(&plan, data.len(), &bytes, false);
        for (i, row) in data.iter().enumerate() {
            b.push(row, i as u64 + 1);
        }
        let (batch, issues) = b.finish();
        Issues::drain_into(vec![issues], width, &mut report);
        vec![batch]
    };
    report.columns = plan.columns;
    (batches, report)
}

fn digit_count(v: i128) -> u8 {
    let n = v.unsigned_abs();
    if let Ok(n) = u64::try_from(n) {
        return n.checked_ilog10().map_or(1, |d| d as u8 + 1);
    }
    n.ilog10() as u8 + 1
}

/// `str::trim`, at once where both ends are visible ASCII.
pub(crate) fn trim(s: &str) -> &str {
    match (s.as_bytes().first(), s.as_bytes().last()) {
        (Some(f), Some(l)) if f.is_ascii_graphic() && l.is_ascii_graphic() => s,
        _ => s.trim(),
    }
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
    let cell = trim(cell);
    if cell.is_empty() {
        return Kind::Empty;
    }
    classify_trimmed(cell, locale)
}

/// `cell` is trimmed and not empty.
fn classify_trimmed(cell: &str, locale: Locale) -> Kind {
    // Digits and a sign alone are never a boolean, date or timestamp, which need a letter, a `-`
    // after the year or a `.`; so a cell with no mark that reads as a number is one.
    if cell.as_bytes()[0].is_ascii_digit()
        && !cell.bytes().any(|b| b == b'.' || b == b',')
        && let Some(kind) = parse_number(cell, locale)
    {
        return kind;
    }
    if let Some(kind) = classify_fixed(cell) {
        return kind;
    }
    parse_number(cell, locale).unwrap_or(Kind::Text)
}

/// A boolean, a timestamp or a date: what a trimmed cell reads as whatever the locale. Each test
/// first looks at the cell's length and a byte or two, which every value of its kind has.
fn classify_fixed(cell: &str) -> Option<Kind> {
    let b = cell.as_bytes();
    if (2..=5).contains(&b.len())
        && matches!(b[0], b't' | b'T' | b'y' | b'Y' | b'f' | b'F' | b'n' | b'N')
        && let Some(v) = parse_bool(cell)
    {
        return Some(Kind::Bool(v));
    }
    // The date before the first `T` or space is ten bytes, and the time at least eight.
    if b.len() >= 19
        && matches!(b[10], b'T' | b' ')
        && let Some(v) = parse_timestamp(cell)
    {
        return Some(Kind::Timestamp(v));
    }
    // An ISO date is ten bytes, and `D.M.YYYY` at least eight.
    if b.len() >= 8
        && let Some(v) = parse_date(cell)
    {
        return Some(Kind::Date(v));
    }
    None
}

/// The number `cell` reads as under `locale`, without allocating: `parse_number_reference` is the
/// same rule written plainly.
fn parse_number(cell: &str, locale: Locale) -> Option<Kind> {
    let (decimal, group) = match locale {
        Locale::Point => (b'.', b','),
        Locale::Comma => (b',', b'.'),
    };
    let b = cell.as_bytes();
    let (neg, s) = match b.first() {
        Some(b'-') => (true, &b[1..]),
        Some(b'+') => (false, &b[1..]),
        _ => (false, b),
    };
    if !s.first()?.is_ascii_digit() {
        return None;
    }
    let mut unscaled: i128 = 0;
    let mut digits = 0usize;
    let mut i = 0;
    // Digits since the last grouping mark, and whether the groups so far are of three.
    let mut run = 0usize;
    let mut grouped = false;
    while i < s.len() && s[i] != decimal {
        let c = s[i];
        if c.is_ascii_digit() {
            digits += 1;
            if digits > 38 {
                return None;
            }
            unscaled = unscaled * 10 + i128::from(c - b'0');
            run += 1;
        } else if c == group {
            let fits = if grouped {
                run == 3
            } else {
                (1..=3).contains(&run)
            };
            if !fits {
                return None;
            }
            grouped = true;
            run = 0;
        } else {
            return None;
        }
        i += 1;
    }
    if grouped && run != 3 {
        return None;
    }
    let mut scale = 0u8;
    if i < s.len() {
        i += 1;
        if i == s.len() {
            return None;
        }
        for &c in &s[i..] {
            if !c.is_ascii_digit() {
                return None;
            }
            digits += 1;
            if digits > 38 {
                return None;
            }
            unscaled = unscaled * 10 + i128::from(c - b'0');
            scale += 1;
        }
    }
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
        || !(1..=12).contains(&m)
        || !(1..=9999).contains(&y)
        || !(1..=days_in_month(y, m)).contains(&d)
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
    if !(1..=12).contains(&m) || !(1..=days_in_month(y, m)).contains(&d) {
        return None;
    }
    Some(days_from_civil(y, m, d))
}

fn days_in_month(y: i32, m: i32) -> i32 {
    match m {
        2 if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn parse_timestamp(cell: &str) -> Option<i64> {
    let (date, time) = cell.split_once(['T', ' '])?;
    if time.len() < 8 || !time.is_ascii() {
        return None;
    }
    let days = parse_iso_date(date)?;
    if time.as_bytes()[2] != b':' || time.as_bytes()[5] != b':' {
        return None;
    }
    let h: i64 = digits(&time[0..2])?;
    let min: i64 = digits(&time[3..5])?;
    let s: i64 = digits(&time[6..8])?;
    if h > 23 || min > 59 || s > 60 {
        return None;
    }
    let mut micros = (days as i64) * 86_400 * 1_000_000 + (h * 3600 + min * 60 + s) * 1_000_000;
    let mut rest = &time[8..];
    if let Some(after) = rest.strip_prefix('.') {
        let len = after.bytes().take_while(u8::is_ascii_digit).count();
        if len == 0 {
            return None;
        }
        let kept = &after[..len.min(6)];
        micros += digits::<i64>(&format!("{kept:0<6}"))?;
        rest = &after[len..];
    }
    // An offset is applied, so the value is UTC. Anything else after the seconds is not a time.
    let offset = match rest.as_bytes() {
        [] | [b'Z'] => 0,
        [sign @ (b'+' | b'-'), ..] => {
            let body = &rest[1..];
            let (oh, om) = match body.len() {
                2 => (body, "00"),
                4 => (&body[0..2], &body[2..4]),
                5 if body.as_bytes()[2] == b':' => (&body[0..2], &body[3..5]),
                _ => return None,
            };
            let (oh, om): (i64, i64) = (digits(oh)?, digits(om)?);
            if oh > 23 || om > 59 {
                return None;
            }
            let secs = oh * 3600 + om * 60;
            if *sign == b'+' { secs } else { -secs }
        }
        _ => return None,
    };
    Some(micros - offset * 1_000_000)
}

fn digits<T: std::str::FromStr>(text: &str) -> Option<T> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

const NOTE_NESTED: &str = "kept as text: a value is a JSON object or array";
const NOTE_LEADING_ZERO: &str = "kept as text: a value has a leading zero";
const NOTE_AMBIGUOUS_MARK: &str = "kept as text: every number reads both as 1.234 and as 1,234, and nothing in the file says which";

#[derive(Default, Clone)]
struct Votes {
    point: usize,
    comma: usize,
    ambiguous: usize,
}

impl Votes {
    /// `None` when the column's numbers could be read either way and the file cannot say which.
    fn locale(&self, file: Option<Locale>, sniffed: Locale) -> Option<Locale> {
        match self.point.cmp(&self.comma) {
            std::cmp::Ordering::Greater => Some(Locale::Point),
            std::cmp::Ordering::Less => Some(Locale::Comma),
            std::cmp::Ordering::Equal if self.point > 0 => Some(file.unwrap_or(sniffed)),
            std::cmp::Ordering::Equal if self.ambiguous > 0 => file,
            std::cmp::Ordering::Equal => Some(file.unwrap_or(sniffed)),
        }
    }
}

/// `007` and `-0123`, not `0` or `0.5`. `cell` is trimmed.
fn leading_zero(cell: &str) -> bool {
    let unsigned = cell.strip_prefix(['-', '+']).unwrap_or(cell).as_bytes();
    unsigned.len() > 1 && unsigned[0] == b'0' && unsigned[1].is_ascii_digit()
}

/// The plain statement of [`parse_number`]'s rule, which a test holds it to.
#[cfg(test)]
fn parse_number_reference(cell: &str, locale: Locale) -> Option<Kind> {
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

#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn same(a: Option<Kind>, b: Option<Kind>) -> bool {
        match (a, b) {
            (None, None) => true,
            (Some(Kind::Int(x)), Some(Kind::Int(y))) => x == y,
            (
                Some(Kind::Decimal {
                    unscaled: u,
                    scale: s,
                }),
                Some(Kind::Decimal {
                    unscaled: v,
                    scale: t,
                }),
            ) => u == v && s == t,
            _ => false,
        }
    }

    #[test]
    fn the_fast_number_reader_is_the_plain_one() {
        // Every string of up to five of these, and a few long ones.
        let alphabet = ["1", "0", "9", ".", ",", "-", "+", "x", "123", "é"];
        let mut cells = vec![String::new()];
        let mut frontier = vec![String::new()];
        for _ in 0..5 {
            let mut next = Vec::new();
            for s in &frontier {
                for a in alphabet {
                    next.push(format!("{s}{a}"));
                }
            }
            cells.extend(next.iter().cloned());
            frontier = next;
        }
        cells.push("9".repeat(38));
        cells.push("9".repeat(39));
        cells.push(format!("{}.{}", "1".repeat(19), "2".repeat(19)));
        cells.push(format!("-{}", "9".repeat(19)));
        cells.push("1,234,567.891".to_string());
        cells.push("1.234.567,891".to_string());
        cells.push("12,34".to_string());
        for cell in &cells {
            for locale in [Locale::Point, Locale::Comma] {
                assert!(
                    same(
                        parse_number(cell, locale),
                        parse_number_reference(cell, locale)
                    ),
                    "{cell:?} under {locale:?}"
                );
            }
        }
    }
}
