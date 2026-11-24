//! JSON under the budgets CSV reads under: a top-level array of objects, or objects one after
//! another, newline-delimited or not.
//!
//! Each record's keys become columns and its values the text CSV inference then types, so a number
//! keeps the digits it was written with. A value that is itself an object or an array stays as its
//! JSON text. serde_json walks the syntax; no `Value` tree is built, and its messages, which can
//! quote the input, are not kept.
//!
//! It reads as CSV does (#407): the text is cut into chunks of about [`CHUNK_BYTES`] at record
//! starts and read in two passes, each over every chunk at once where there are threads; the first
//! counts what each key's values read as and builds a batch under a guess from the first chunk, and
//! the second builds again any chunk the guess did not type as the whole file does. Records are
//! never held as rows. A chunk's start is a guess, the first `{` after its offset that follows a
//! `}` (objects one after another) or a `,` (an array); the chunk before it, read from a known
//! record start, says where its records really end, and a chunk whose guess was wrong (a nested
//! object, or a string) is read again from there. A raw line end cannot be inside a JSON string,
//! which serde_json refuses as a control character, but a record may span lines, so line ends are
//! not where it cuts. Where any chunk does not read, the file is walked again from its start, as one
//! reader would, to say which error comes first and where.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use serde::de::{DeserializeSeed, Deserializer, Error, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;

use crate::budget::{Budget, BudgetUse};
use crate::csv::{CHUNK_BYTES, parallel};
use crate::error::{IngestError, Result};
use crate::infer::{self, BatchBuilder, Cells, ColumnTally, Issues, Plan};
use crate::report::{Format, IngestReport};
use crate::sniff::{Dialect, EncodingKind, Locale};

pub(crate) fn looks_like(text: &str) -> bool {
    text.trim_start().starts_with(['{', '['])
}

const WHITESPACE: [char; 4] = [' ', '\t', '\n', '\r'];

const INVALID: IngestError = IngestError::Invalid { format: "json" };

type Clock<'a> = &'a (dyn Fn() -> Result<()> + Sync);

/// Where in `text` the parser over `text[start..]` went wrong, as `IngestError::Syntax`.
fn at(text: &str, start: usize, e: &serde_json::Error) -> IngestError {
    let before = &text[..start];
    let line = before.matches('\n').count() as u64 + 1;
    let column = (before.len() - before.rfind('\n').map_or(0, |i| i + 1)) as u64;
    let (line, column) = match e.line() as u64 {
        0 => return INVALID,
        1 => (line, column + e.column() as u64),
        n => (line + n - 1, e.column() as u64),
    };
    IngestError::Syntax {
        format: "json",
        line,
        column,
    }
}

/// The keys seen so far as columns, and which of a record's cells each takes.
#[derive(Default)]
struct Keys {
    names: Vec<String>,
    index: HashMap<String, usize>,
    nested: Vec<bool>,
    /// Per column, the record that last wrote it and which of that record's cells it takes. A
    /// stamp and not a reset, so a short record does not pay for every column the file has.
    slots: Vec<(u64, usize)>,
    record: u64,
    /// Columns up to the last one this record wrote.
    width: usize,
}

impl Keys {
    /// These keys as the first columns, in order.
    fn seeded(names: &[String]) -> Self {
        let mut keys = Self::default();
        for name in names {
            keys.add(name);
        }
        keys
    }

    fn add(&mut self, key: &str) -> usize {
        let i = self.names.len();
        self.names.push(key.to_string());
        self.index.insert(key.to_string(), i);
        self.nested.push(false);
        self.slots.push((0, 0));
        i
    }

    fn column(&mut self, key: &str, b: &Budget) -> Result<usize> {
        b.charge("max_key_bytes", b.max_key_bytes, key.len() as u64)?;
        if let Some(&i) = self.index.get(key) {
            return Ok(i);
        }
        b.charge("max_columns", b.max_columns, self.names.len() as u64 + 1)?;
        Ok(self.add(key))
    }

    /// `cell` as the record's value for `column`. A key written twice keeps its last value, as
    /// every JSON reader does, and costs one cell however often it repeats.
    fn place<'de>(&mut self, cells: &mut Vec<Cow<'de, str>>, column: usize, cell: Cow<'de, str>) {
        match self.slots[column] {
            (record, at) if record == self.record => cells[at] = cell,
            _ => {
                self.slots[column] = (self.record, cells.len());
                self.width = self.width.max(column + 1);
                cells.push(cell);
            }
        }
    }
}

/// One record's cells by column, a missing key as an empty cell.
pub(crate) struct RecordCells<'r> {
    cells: &'r [Cow<'r, str>],
    slots: &'r [(u64, usize)],
    record: u64,
    width: usize,
}

impl Cells for RecordCells<'_> {
    fn len(&self) -> usize {
        self.width
    }

    fn cell(&self, i: usize) -> &str {
        match self.slots[i] {
            (record, at) if record == self.record => &self.cells[at],
            _ => "",
        }
    }
}

type OnRow<'f> = &'f mut dyn FnMut(&RecordCells<'_>) -> Result<()>;

/// What a record visitor reads into.
struct Sink<'a, 'f> {
    budget: &'a Budget,
    keys: &'a mut Keys,
    on_row: OnRow<'f>,
    /// Why the walk stopped, where it was a budget and not the syntax.
    stop: Option<IngestError>,
}

impl Sink<'_, '_> {
    /// One value as a cell: a string's content, `null` as nothing, anything else as written.
    fn cell<'de>(&mut self, raw: &'de str, column: usize) -> Result<Cow<'de, str>> {
        let b = self.budget;
        let cell = match raw.as_bytes().first() {
            Some(b'"') if !raw.contains('\\') => Cow::Borrowed(&raw[1..raw.len() - 1]),
            Some(b'"') => Cow::Owned(serde_json::from_str::<String>(raw).map_err(|_| INVALID)?),
            Some(b'{' | b'[') => {
                b.charge("max_depth", b.max_depth, depth(raw, b.max_depth))?;
                self.keys.nested[column] = true;
                Cow::Borrowed(raw)
            }
            Some(b'n') => Cow::Borrowed(""),
            _ => Cow::Borrowed(raw),
        };
        b.charge("max_cell_bytes", b.max_cell_bytes, cell.len() as u64)?;
        Ok(cell)
    }

    fn stopped<E: Error>(&mut self, why: IngestError) -> E {
        self.stop = Some(why);
        E::custom("stopped")
    }
}

/// How deep the arrays and objects in `raw` nest, counted no further than one past `limit`.
fn depth(raw: &str, limit: u64) -> u64 {
    let (mut now, mut deepest) = (0u64, 0u64);
    let (mut in_string, mut escaped) = (false, false);
    for b in raw.bytes() {
        if in_string {
            match b {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                now += 1;
                deepest = deepest.max(now);
                if deepest > limit {
                    break;
                }
            }
            b'}' | b']' => now = now.saturating_sub(1),
            _ => {}
        }
    }
    deepest
}

struct Array<'s, 'a, 'f>(&'s mut Sink<'a, 'f>);

impl<'de> DeserializeSeed<'de> for Array<'_, '_, '_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Array<'_, '_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an array of objects")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        while seq.next_element_seed(Record(&mut *self.0))?.is_some() {}
        Ok(())
    }
}

struct Record<'s, 'a, 'f>(&'s mut Sink<'a, 'f>);

impl<'de> DeserializeSeed<'de> for Record<'_, '_, '_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for Record<'_, '_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        let sink = self.0;
        sink.keys.record += 1;
        sink.keys.width = 0;
        let mut cells: Vec<Cow<'de, str>> = Vec::new();
        while let Some(key) = map.next_key_seed(Key)? {
            let column = match sink.keys.column(&key, sink.budget) {
                Ok(c) => c,
                Err(why) => return Err(sink.stopped(why)),
            };
            let raw: &'de RawValue = map.next_value()?;
            match sink.cell(raw.get(), column) {
                Ok(cell) => sink.keys.place(&mut cells, column, cell),
                Err(why) => return Err(sink.stopped(why)),
            }
        }
        let keys = &*sink.keys;
        let row = RecordCells {
            cells: &cells,
            slots: &keys.slots,
            record: keys.record,
            width: keys.width,
        };
        (sink.on_row)(&row).map_err(|why| sink.stopped(why))
    }
}

/// A key borrowed from the text where it has no escapes.
struct Key;

impl<'de> DeserializeSeed<'de> for Key {
    type Value = Cow<'de, str>;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> std::result::Result<Self::Value, D::Error> {
        d.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for Key {
    type Value = Cow<'de, str>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a key")
    }

    fn visit_borrowed_str<E: Error>(self, s: &'de str) -> std::result::Result<Self::Value, E> {
        Ok(Cow::Borrowed(s))
    }

    fn visit_str<E: Error>(self, s: &str) -> std::result::Result<Self::Value, E> {
        Ok(Cow::Owned(s.to_string()))
    }
}

/// The whole text walked as one reader walks it, holding nothing, for the first thing wrong with
/// it: the syntax at its line and column, or the budget it crosses first. `None` if nothing is.
fn first_error(text: &str, budget: &Budget, clock: Clock) -> Option<IngestError> {
    let mut keys = Keys::default();
    let mut rows = 0u64;
    let mut on_row = |_: &RecordCells<'_>| -> Result<()> {
        if rows >= budget.max_rows {
            return Err(budget.breach("max_rows", budget.max_rows, rows + 1));
        }
        rows += 1;
        if rows.is_multiple_of(4096) {
            clock()?;
        }
        Ok(())
    };
    let mut sink = Sink {
        budget,
        keys: &mut keys,
        on_row: &mut on_row,
        stop: None,
    };
    let body = text.trim_start_matches(WHITESPACE);
    // The error and the slice its parser was over, which starts this far into `text`.
    let read = if body.starts_with('[') {
        let mut de = serde_json::Deserializer::from_str(body);
        Array(&mut sink)
            .deserialize(&mut de)
            .and_then(|()| de.end())
            .map_err(|e| (e, text.len() - body.len()))
    } else {
        let mut rest = body;
        loop {
            if rest.is_empty() {
                break Ok(());
            }
            let mut de = serde_json::Deserializer::from_str(rest);
            if let Err(e) = Record(&mut sink).deserialize(&mut de) {
                break Err((e, text.len() - rest.len()));
            }
            let used = de.into_iter::<IgnoredAny>().byte_offset();
            rest = rest[used..].trim_start_matches(WHITESPACE);
        }
    };
    let (e, start) = read.err()?;
    Some(sink.stop.take().unwrap_or_else(|| at(text, start, &e)))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// `[` record `,` record ... `]`.
    Array,
    /// Records one after another.
    Objects,
}

fn skip_whitespace(b: &[u8], mut p: usize) -> usize {
    while p < b.len() && matches!(b[p], b' ' | b'\t' | b'\n' | b'\r') {
        p += 1;
    }
    p
}

/// The first `{` at or after `from` whose last byte before it but whitespace is `after`, or the
/// end: where a chunk is guessed to start.
fn guess_start(b: &[u8], from: usize, after: u8) -> usize {
    let mut i = from;
    while let Some(off) = b[i.min(b.len())..].iter().position(|&c| c == b'{') {
        let j = i + off;
        let mut k = j;
        while k > 0 && matches!(b[k - 1], b' ' | b'\t' | b'\n' | b'\r') {
            k -= 1;
        }
        if k > 0 && b[k - 1] == after {
            return j;
        }
        i = j + 1;
    }
    b.len()
}

/// Where a chunk's records were.
#[derive(Clone, Copy)]
struct Walked {
    end: usize,
    rows: u64,
}

/// The records from `start`, a record start (the array's first, which may be its `]`, where
/// `opens`), until the first whose next starts at or past `stop`, each passed to `on_row`.
#[allow(clippy::too_many_arguments)]
fn walk(
    text: &str,
    shape: Shape,
    opens: bool,
    start: usize,
    stop: usize,
    budget: &Budget,
    keys: &mut Keys,
    clock: Clock,
    on_row: OnRow,
) -> Result<Walked> {
    let b = text.as_bytes();
    let n = b.len();
    let mut walked = Walked {
        end: start,
        rows: 0,
    };
    if start >= stop {
        return Ok(walked);
    }
    let mut pos = start;
    if opens && b.get(pos) == Some(&b']') {
        if skip_whitespace(b, pos + 1) != n {
            return Err(INVALID);
        }
        walked.end = n;
        return Ok(walked);
    }
    let mut closed = false;
    let mut sink = Sink {
        budget,
        keys,
        on_row,
        stop: None,
    };
    while pos < n {
        let mut de = serde_json::Deserializer::from_str(&text[pos..]);
        if Record(&mut sink).deserialize(&mut de).is_err() {
            return Err(sink.stop.take().unwrap_or(INVALID));
        }
        let used = de.into_iter::<IgnoredAny>().byte_offset();
        walked.rows += 1;
        if walked.rows > budget.max_rows {
            return Err(budget.breach("max_rows", budget.max_rows, walked.rows));
        }
        if walked.rows.is_multiple_of(4096) {
            clock()?;
        }
        let mut p = skip_whitespace(b, pos + used);
        if shape == Shape::Array {
            match b.get(p) {
                Some(b',') => {
                    p = skip_whitespace(b, p + 1);
                    if p == n {
                        return Err(INVALID);
                    }
                }
                Some(b']') => {
                    if skip_whitespace(b, p + 1) != n {
                        return Err(INVALID);
                    }
                    closed = true;
                    p = n;
                }
                _ => return Err(INVALID),
            }
        }
        pos = p;
        if pos >= stop {
            break;
        }
    }
    if shape == Shape::Array && pos >= n && !closed {
        return Err(INVALID);
    }
    walked.end = pos;
    Ok(walked)
}

/// The first pass over one chunk: its keys in the order it met them, which begin with the guess's
/// where there was one, and what each one's values read as.
struct Counted {
    start: usize,
    walked: Walked,
    keys: Vec<String>,
    nested: Vec<bool>,
    tallies: Vec<ColumnTally>,
    /// Its batch under the guessed plan, rows numbered from 1 in the chunk, where one was built.
    built: Option<(RecordBatch, Issues)>,
}

/// A guess at the whole file's plan from its first chunk, built under while the rest is counted.
struct Guess<'a> {
    plan: Plan,
    /// The first chunk's keys, as written.
    keys: Vec<String>,
    rows_per_byte: f64,
    bytes_per_byte: Vec<f64>,
    /// Bytes of batches built under the guess, which stop being built past the memory budget.
    held: &'a AtomicU64,
    room: u64,
}

struct Text<'a> {
    text: &'a str,
    shape: Shape,
    /// Where the first record starts.
    first: usize,
    budget: &'a Budget,
    clock: Clock<'a>,
}

impl Text<'_> {
    fn count(&self, start: usize, stop: usize, guess: Option<&Guess>) -> (usize, Result<Counted>) {
        let guess = guess.filter(|g| g.held.load(Ordering::Relaxed) <= g.room);
        let mut keys = guess.map_or_else(Keys::default, |g| Keys::seeded(&g.keys));
        let mut tallies = Vec::new();
        let mut builder = guess.map(|g| {
            let len = stop.saturating_sub(start) as f64;
            let bytes: Vec<u64> = g.bytes_per_byte.iter().map(|b| (b * len) as u64).collect();
            BatchBuilder::new(&g.plan, (g.rows_per_byte * len) as usize, &bytes, false)
        });
        let mut at = 0u64;
        let walked = walk(
            self.text,
            self.shape,
            self.shape == Shape::Array && start == self.first,
            start,
            stop,
            self.budget,
            &mut keys,
            self.clock,
            &mut |row| {
                at += 1;
                match &mut builder {
                    Some(b) => b.push_counting(row, at, Some(&mut tallies)),
                    None => infer::tally_row(&mut tallies, row),
                }
                Ok(())
            },
        );
        let built = match (builder, guess) {
            // A chunk that met a key the guess has no column for keeps none of its batch.
            (Some(b), Some(g)) if walked.is_ok() && keys.names.len() == g.keys.len() => {
                let (batch, issues) = b.finish();
                let size = batch.get_array_memory_size() as u64;
                let held = g.held.fetch_add(size, Ordering::Relaxed) + size;
                (held <= g.room).then_some((batch, issues))
            }
            _ => None,
        };
        let counted = walked.map(|walked| Counted {
            start,
            walked,
            keys: keys.names,
            nested: keys.nested,
            tallies,
            built,
        });
        (start, counted)
    }
}

/// The text, which opens with `{` or `[` past any whitespace, as one table of its records. `memory`
/// is what is held already, the text.
pub(crate) fn read(
    text: &str,
    encoding: EncodingKind,
    budget: &Budget,
    memory: u64,
    bytes_read: u64,
    threads: usize,
    clock: Clock,
) -> Result<(Vec<RecordBatch>, IngestReport)> {
    read_in(
        text,
        encoding,
        budget,
        memory,
        bytes_read,
        threads,
        CHUNK_BYTES,
        clock,
    )
}

#[allow(clippy::too_many_arguments)]
fn read_in(
    text: &str,
    encoding: EncodingKind,
    budget: &Budget,
    memory: u64,
    bytes_read: u64,
    threads: usize,
    chunk_bytes: usize,
    clock: Clock,
) -> Result<(Vec<RecordBatch>, IngestReport)> {
    // Any chunk that does not read, read from a record start: the whole text says what is wrong.
    let refuse = |e: IngestError| first_error(text, budget, clock).unwrap_or(e);
    let b = text.as_bytes();
    let n = b.len();
    let body = skip_whitespace(b, 0);
    let (shape, first, after) = if b.get(body) == Some(&b'[') {
        (Shape::Array, skip_whitespace(b, body + 1), b',')
    } else {
        (Shape::Objects, body, b'}')
    };
    if shape == Shape::Array && first == n {
        return Err(refuse(INVALID));
    }
    let t = Text {
        text,
        shape,
        first,
        budget,
        clock,
    };
    let jobs = n.div_ceil(chunk_bytes.max(1)).max(1);
    let stop = |k: usize| {
        if k + 1 >= jobs {
            n
        } else {
            (k + 1) * chunk_bytes
        }
    };

    // The first chunk alone, and from it a guess at the plan.
    let first_chunk = t.count(first, stop(0), None).1.map_err(refuse)?;
    let held = AtomicU64::new(0);
    let guess = (!first_chunk.keys.is_empty() && jobs > 1).then(|| {
        let c = &first_chunk;
        let names = infer::column_names(Some(&c.keys), c.keys.len());
        let rows = c.walked.rows;
        let plan = Plan::new(names, &c.tallies, rows, Locale::Point, Some(&c.nested)).widened();
        let len = (c.walked.end - c.start).max(1) as f64;
        Guess {
            plan,
            keys: c.keys.clone(),
            rows_per_byte: rows as f64 / len,
            bytes_per_byte: c.tallies.iter().map(|t| t.bytes as f64 / len).collect(),
            held: &held,
            room: budget.max_memory_bytes.saturating_sub(memory),
        }
    });

    // The rest at once, each from its guessed start; then in order, each read again from where
    // the one before really ended if that is not where it was guessed to start.
    let guessed = parallel(jobs - 1, threads, |j| {
        let k = j + 1;
        t.count(
            guess_start(b, k * chunk_bytes, after),
            stop(k),
            guess.as_ref(),
        )
    });
    let mut chunks: Vec<Counted> = Vec::with_capacity(jobs);
    let mut at = first_chunk.walked.end;
    let mut rows = first_chunk.walked.rows;
    chunks.push(first_chunk);
    for (j, (start, counted)) in guessed.into_iter().enumerate() {
        let counted = if start == at {
            counted
        } else {
            t.count(at, stop(j + 1), None).1
        };
        let c = counted.map_err(refuse)?;
        rows += c.walked.rows;
        if rows > budget.max_rows {
            return Err(refuse(budget.breach("max_rows", budget.max_rows, rows)));
        }
        at = c.walked.end;
        chunks.push(c);
    }
    clock()?;

    // The file's keys in the order it met them, and every chunk's counts under them.
    let mut keys = Keys::default();
    let mut nested: Vec<bool> = Vec::new();
    let mut tallies: Vec<ColumnTally> = Vec::new();
    let mut columns_of: Vec<Vec<usize>> = Vec::with_capacity(chunks.len());
    for c in &chunks {
        let mut map = Vec::with_capacity(c.keys.len());
        for (i, key) in c.keys.iter().enumerate() {
            let g = match keys.index.get(key) {
                Some(&g) => g,
                None => {
                    if keys.names.len() as u64 >= budget.max_columns {
                        let at = keys.names.len() as u64 + 1;
                        return Err(refuse(budget.breach("max_columns", budget.max_columns, at)));
                    }
                    nested.push(false);
                    tallies.push(ColumnTally::default());
                    keys.add(key)
                }
            };
            nested[g] |= c.nested[i];
            if let Some(tally) = c.tallies.get(i) {
                tallies[g].merge(tally);
            }
            map.push(g);
        }
        columns_of.push(map);
    }
    let width = keys.names.len();
    let names = infer::column_names(Some(&keys.names), width);
    let plan = Plan::new(names, &tallies, rows, Locale::Point, Some(&nested));
    // What was built under the guess stands where the guess reads the file's first columns as the
    // file's plan does; a key it never met is a column of nulls in those batches.
    if !guess.as_ref().is_some_and(|g| g.plan.reads_as(&plan)) {
        for c in &mut chunks {
            c.built = None;
        }
    }
    let batches_made = chunks.iter().filter(|c| c.walked.rows > 0).count() as u64;
    let estimate = memory + plan.estimate(rows, batches_made, &tallies);
    budget.charge("max_memory_bytes", budget.max_memory_bytes, estimate)?;

    let dialect = Dialect {
        delimiter: b',',
        quote: b'"',
        encoding,
        locale: plan.locale,
    };
    let mut report = IngestReport {
        format: Format::Json,
        dialect,
        header: true,
        rows,
        columns: Vec::new(),
        rejected: Vec::new(),
        rejected_omitted: 0,
        budget_used: BudgetUse {
            bytes: bytes_read,
            rows,
            columns: width as u64,
            memory_bytes: 0,
        },
    };

    // The rows before each chunk.
    let mut before = Vec::with_capacity(chunks.len());
    let mut sum = 0u64;
    for c in &chunks {
        before.push(sum);
        sum += c.walked.rows;
    }

    // Second pass: a batch for each chunk with records that has none yet.
    let mut built: Vec<Option<(RecordBatch, Issues)>> =
        chunks.iter_mut().map(|c| c.built.take()).collect();
    let from_guess: Vec<bool> = built.iter().map(Option::is_some).collect();
    let todo: Vec<usize> = (0..chunks.len())
        .filter(|&k| built[k].is_none() && chunks[k].walked.rows > 0 && width > 0)
        .collect();
    let rebuilt = parallel(todo.len(), threads, |j| -> Result<_> {
        let k = todo[j];
        let c = &chunks[k];
        let mut bytes = vec![0u64; width];
        for (i, tally) in c.tallies.iter().enumerate() {
            bytes[columns_of[k][i]] = tally.bytes;
        }
        let mut b = BatchBuilder::new(&plan, c.walked.rows as usize, &bytes, false);
        let mut seeded = Keys::seeded(&keys.names);
        let mut data = before[k];
        walk(
            text,
            shape,
            shape == Shape::Array && c.start == first,
            c.start,
            c.walked.end,
            budget,
            &mut seeded,
            clock,
            &mut |row| {
                data += 1;
                b.push(row, data);
                Ok(())
            },
        )?;
        Ok(b.finish())
    });
    for (k, b) in todo.into_iter().zip(rebuilt) {
        built[k] = Some(b?);
    }
    let mut batches = Vec::with_capacity(built.len());
    let mut issues = Vec::with_capacity(built.len());
    for (k, b) in built.into_iter().enumerate() {
        let Some((batch, mut found)) = b else {
            continue;
        };
        let batch = if from_guess[k] {
            // Built under the guess: its rows are numbered within the chunk, its decimals at 38,
            // and it has no columns for the keys the first chunk did not meet.
            found.renumber(before[k]);
            found.widen(width);
            plan.retag(batch)
        } else {
            batch
        };
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
        issues.push(found);
    }
    Issues::drain_into(issues, width, &mut report);
    let arrays: u64 = batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum();
    report.budget_used.memory_bytes = memory + arrays;
    budget.charge(
        "max_memory_bytes",
        budget.max_memory_bytes,
        report.budget_used.memory_bytes,
    )?;
    report.columns = plan.columns;
    Ok((batches, report))
}

#[cfg(test)]
mod tests {
    use super::*;

    type Read = std::result::Result<(RecordBatch, IngestReport), IngestError>;

    fn whole(text: &str, chunk_bytes: usize, threads: usize) -> Read {
        let budget = Budget::untrusted();
        let n = text.len() as u64;
        let (batches, mut report) = read_in(
            text,
            EncodingKind::Utf8,
            &budget,
            n,
            n,
            threads,
            chunk_bytes,
            &|| Ok(()),
        )?;
        // The offsets of more batches take more bytes; nothing else may differ.
        report.budget_used.memory_bytes = 0;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
        let one = arrow::compute::concat_batches(&schema, &batches).unwrap();
        Ok((one, report))
    }

    fn same(text: &str, chunks: &[usize], threads: &[usize]) -> Read {
        let want = whole(text, usize::MAX / 2, 1);
        for &chunk_bytes in chunks {
            for &t in threads {
                let got = whole(text, chunk_bytes, t);
                assert_eq!(
                    got.as_ref().map(|g| &g.1),
                    want.as_ref().map(|w| &w.1),
                    "{text:?} at {chunk_bytes} bytes, {t} threads"
                );
                assert_eq!(
                    got.as_ref().map(|g| &g.0),
                    want.as_ref().map(|w| &w.0),
                    "{text:?} at {chunk_bytes} bytes, {t} threads"
                );
            }
        }
        want
    }

    const TEXTS: &[&str] = &[
        "{\"a\": 1, \"b\": \"x\"}\n{\"a\": 2, \"b\": \"y\"}\n{\"a\": 3}\n",
        "{\"a\": 1, \"b\": \"x\"}\r\n\r\n{\"a\": 2, \"b\": \"y\"}\r\n",
        "{\"a\": 1}{\"a\": 2}{\"b\": 3.5}{\"a\": 4}",
        "{\n  \"a\": 1,\n  \"b\": {\"c\": [1, {\"d\": 2}]}\n}\n{\n  \"b\": \"y\",\n  \"a\": 2\n}\n",
        "[{\"a\": 1}, {\"a\": 2, \"b\": [{\"x\": 1}, {\"x\": 2}]}, {\"b\": null}]",
        "[\n  {\"a\": 1.5, \"t\": \"2024-01-01\"},\n  {\"a\": 2, \"t\": \"2024-01-02\"}\n]\n",
        "[\n  {\n    \"a\": 1\n  },\n  {\n    \"a\": 2\n  }\n]",
        "[{\"s\": \"}{\\\"a\\\": 1}, {\"}, {\"s\": \",{\\n\"}]",
        "{\"s\": \"} {\"}\n{\"s\": \"\\\"}{\"}\n",
        "{\"k\u{e9}y\": \"\u{e9}\u{e9}\", \"\": 1}\n{\"k\u{e9}y\": \"\u{1F600}\"}\n",
        "{\"n\": 1}\n{\"n\": 2}\n{\"n\": 3}\n{\"n\": 4}\n{\"n\": \"ten\"}\n",
        "{\"a\": 1}\n{\"z\": 1}\n{\"a\": 2, \"z\": 2}\n{\"late\": true}\n",
        "[]",
        " [ ] \n",
        "",
        "\n\n",
        "{}",
        "[{}, {}]",
        "{\"a\": 1}",
        // Refused, at the same place however it is cut.
        "{\"a\": 1}\n{\"a\": 2}\n{\"a\": nope}\n",
        "[{\"a\": 1},\n {\"a\": 2},\n]",
        "[{\"a\": 1}, {\"a\": 2}",
        "[{\"a\": 1}] [{\"a\": 2}]",
        "[{\"a\": 1}, 5]",
        "{\"a\": 1} ]",
        "]",
        "[",
        "{\"a\": \"raw\nline end\"}",
    ];

    #[test]
    fn a_file_read_in_chunks_is_the_file_read_whole() {
        for text in TEXTS {
            let _ = same(text, &[1, 2, 3, 5, 7, 11, 16, 64], &[1, 3, 8]);
        }
    }

    #[test]
    fn a_raw_line_end_in_a_string_is_refused() {
        // So a line end is never inside a string, which is what lets NDJSON be cut at one.
        for text in ["{\"a\": \"x\ny\"}", "[{\"a\": \"x\r\"}]"] {
            assert!(matches!(
                whole(text, usize::MAX / 2, 1),
                Err(IngestError::Syntax { line: 1, .. })
            ));
        }
    }

    #[test]
    fn a_guess_from_the_first_chunk_that_the_file_overturns_is_rebuilt() {
        // Integers for the first chunks, then a wider decimal, a column gone to text and a key
        // the first chunk never met.
        let mut text = String::new();
        for i in 0..400 {
            text.push_str(&format!("{{\"a\": {i}, \"b\": {i}, \"c\": {i}}}\n"));
        }
        text.push_str("{\"a\": 1, \"b\": 123456.5, \"c\": \"x\"}\n{\"b\": 7.25, \"new\": [1]}\n");
        for i in 0..40 {
            text.push_str(&format!("{{\"c\": \"t{i}\", \"a\": {i}}}\n"));
        }
        let (_, report) = same(&text, &[64, 500, 2000], &[1, 4]).unwrap();
        let names: Vec<_> = report.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["a", "b", "c", "new"]);
        assert_eq!(report.rows, 442);

        // A key past the first chunk alone: the guessed batches are kept, with a column of nulls.
        let mut text = String::new();
        for i in 0..400 {
            text.push_str(&format!("{{\"a\": {i}, \"b\": {i}.5}}\n"));
        }
        text.push_str("{\"b\": 123456789.5, \"late\": \"x\"}\n");
        let (_, report) = same(&text, &[64, 500], &[1, 4]).unwrap();
        assert!(matches!(
            report.columns[1].inferred,
            crate::InferredType::Decimal {
                precision: 10,
                scale: 1
            }
        ));
        assert_eq!(report.columns[2].nulls, 400);
    }

    #[test]
    fn a_contradiction_far_past_the_first_chunk_is_reported_at_its_row_and_key() {
        let mut text = String::new();
        for i in 0..5000 {
            text.push_str(&format!("{{\"id\": {i}, \"day\": \"2024-01-01\"}}\n"));
        }
        text.push_str("{\"id\": 5000, \"day\": \"soon\"}\n");
        for i in 5001..6000 {
            text.push_str(&format!("{{\"id\": {i}, \"day\": \"2024-01-02\"}}\n"));
        }
        let (_, report) = same(&text, &[1000, 4096], &[1, 8]).unwrap();
        assert_eq!(report.columns[1].inferred, crate::InferredType::Date);
        assert_eq!(
            report.rejected,
            vec![crate::RowIssue {
                row: 5001,
                column: Some("day".into()),
                reason: crate::Issue::TypeMismatch,
            }]
        );
    }
}
