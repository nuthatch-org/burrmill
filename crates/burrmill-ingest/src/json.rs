//! JSON under the budgets CSV reads under: a top-level array of objects, or objects one after
//! another, newline-delimited or not.
//!
//! Each record's keys become columns and its values the text CSV inference then types, so a number
//! keeps the digits it was written with. A value that is itself an object or an array stays as its
//! JSON text. serde_json walks the syntax; no `Value` tree is built, and its messages, which can
//! quote the input, are not kept.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

use serde::de::{DeserializeSeed, Deserializer, Error, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;

use crate::budget::Budget;
use crate::csv::{Parsed, Row};
use crate::error::{IngestError, Result};

pub(crate) struct Records {
    /// The keys as the first row, as a CSV header is, then a row per record. A row ends at the
    /// record's last key, so a short row is missing keys, not a ragged line.
    pub parsed: Parsed,
    /// Per column: a value was an object or an array.
    pub nested: Vec<bool>,
}

pub(crate) fn looks_like(text: &str) -> bool {
    text.trim_start().starts_with(['{', '['])
}

pub(crate) fn parse(
    text: &str,
    budget: &Budget,
    memory_base: u64,
    clock: &mut dyn FnMut() -> Result<()>,
) -> Result<Records> {
    let mut sink = Sink {
        budget,
        clock,
        names: Vec::new(),
        index: HashMap::new(),
        nested: Vec::new(),
        slots: Vec::new(),
        record: 0,
        width: 0,
        rows: vec![Row::default()],
        memory: memory_base,
        stop: None,
    };
    let body = text.trim_start_matches(WHITESPACE);
    let read = if body.starts_with('[') {
        let mut de = serde_json::Deserializer::from_str(body);
        Array(&mut sink)
            .deserialize(&mut de)
            .and_then(|()| de.end())
    } else {
        let mut rest = body;
        loop {
            if rest.is_empty() {
                break Ok(());
            }
            let mut de = serde_json::Deserializer::from_str(rest);
            if let Err(e) = Record(&mut sink).deserialize(&mut de) {
                break Err(e);
            }
            let used = de.into_iter::<IgnoredAny>().byte_offset();
            rest = rest[used..].trim_start_matches(WHITESPACE);
        }
    };
    if read.is_err() {
        return Err(sink.stop.take().unwrap_or(INVALID));
    }
    let mut header = Row::default();
    for name in &sink.names {
        header.push(name);
    }
    sink.rows[0] = header;
    Ok(Records {
        parsed: Parsed {
            rows: sink.rows,
            unclosed_rows: Vec::new(),
            memory_bytes: sink.memory,
        },
        nested: sink.nested,
    })
}

const WHITESPACE: [char; 4] = [' ', '\t', '\n', '\r'];

const INVALID: IngestError = IngestError::Invalid { format: "json" };

struct Sink<'a> {
    budget: &'a Budget,
    clock: &'a mut dyn FnMut() -> Result<()>,
    names: Vec<String>,
    index: HashMap<String, usize>,
    nested: Vec<bool>,
    /// Per column, the record that last wrote it and which of that record's cells it takes. A
    /// stamp and not a reset, so a short record does not pay for every column the file has.
    slots: Vec<(u64, usize)>,
    record: u64,
    /// Columns up to the last one this record wrote.
    width: usize,
    rows: Vec<Row>,
    memory: u64,
    /// Why the walk stopped, where it was a budget and not the syntax.
    stop: Option<IngestError>,
}

impl Sink<'_> {
    fn column(&mut self, key: &str) -> Result<usize> {
        let b = self.budget;
        b.charge("max_key_bytes", b.max_key_bytes, key.len() as u64)?;
        if let Some(&i) = self.index.get(key) {
            return Ok(i);
        }
        let i = self.names.len();
        b.charge("max_columns", b.max_columns, i as u64 + 1)?;
        // The name twice, in the row of names and in the index.
        self.memory += 2 * (key.len() + std::mem::size_of::<String>()) as u64;
        b.charge("max_memory_bytes", b.max_memory_bytes, self.memory)?;
        self.names.push(key.to_string());
        self.index.insert(key.to_string(), i);
        self.nested.push(false);
        self.slots.push((0, 0));
        Ok(i)
    }

    /// One value as a cell: a string's content, `null` as nothing, anything else as written.
    fn cell<'de>(&mut self, raw: &'de str, column: usize) -> Result<Cow<'de, str>> {
        let b = self.budget;
        let cell = match raw.as_bytes().first() {
            Some(b'"') if !raw.contains('\\') => Cow::Borrowed(&raw[1..raw.len() - 1]),
            Some(b'"') => Cow::Owned(serde_json::from_str::<String>(raw).map_err(|_| INVALID)?),
            Some(b'{' | b'[') => {
                b.charge("max_depth", b.max_depth, depth(raw, b.max_depth))?;
                self.nested[column] = true;
                Cow::Borrowed(raw)
            }
            Some(b'n') => Cow::Borrowed(""),
            _ => Cow::Borrowed(raw),
        };
        b.charge("max_cell_bytes", b.max_cell_bytes, cell.len() as u64)?;
        Ok(cell)
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

    fn row(&mut self, cells: &[Cow<'_, str>]) -> Result<()> {
        let b = self.budget;
        if self.rows.len() as u64 > b.max_rows {
            return Err(b.breach("max_rows", b.max_rows, self.rows.len() as u64));
        }
        let mut row = Row::default();
        self.memory += std::mem::size_of::<Row>() as u64;
        for &(record, at) in &self.slots[..self.width] {
            let cell = if record == self.record {
                cells[at].as_ref()
            } else {
                ""
            };
            self.memory += (cell.len() + std::mem::size_of::<u32>()) as u64;
            row.push(cell);
        }
        b.charge("max_memory_bytes", b.max_memory_bytes, self.memory)?;
        self.rows.push(row);
        if self.rows.len().is_multiple_of(4096) {
            (self.clock)()?;
        }
        Ok(())
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

struct Array<'s, 'a>(&'s mut Sink<'a>);

impl<'de> DeserializeSeed<'de> for Array<'_, '_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Array<'_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an array of objects")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        while seq.next_element_seed(Record(&mut *self.0))?.is_some() {}
        Ok(())
    }
}

struct Record<'s, 'a>(&'s mut Sink<'a>);

impl<'de> DeserializeSeed<'de> for Record<'_, '_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for Record<'_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        let sink = self.0;
        sink.record += 1;
        sink.width = 0;
        let mut cells: Vec<Cow<'de, str>> = Vec::new();
        while let Some(key) = map.next_key_seed(Key)? {
            let column = match sink.column(&key) {
                Ok(c) => c,
                Err(why) => return Err(sink.stopped(why)),
            };
            let raw: &'de RawValue = map.next_value()?;
            match sink.cell(raw.get(), column) {
                Ok(cell) => sink.place(&mut cells, column, cell),
                Err(why) => return Err(sink.stopped(why)),
            }
        }
        sink.row(&cells).map_err(|why| sink.stopped(why))
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
