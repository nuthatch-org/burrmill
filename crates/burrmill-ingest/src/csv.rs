//! An owned CSV reader. Quoting and the error on a hostile field are the product, so this
//! does not rent `arrow-csv`.
//!
//! The text is cut into chunks of about [`CHUNK_BYTES`] at row ends, and read in two passes, each
//! over every chunk at once where there are threads: the first counts what each column's cells
//! read as, which chooses the types over the whole file, and the second builds one Arrow batch a
//! chunk under them. Rows are never held as rows. A chunk's start is a guess, the first line end
//! after a fixed offset; the chunk before it, read from a known row start, says where its rows
//! really end, and a chunk whose guess was wrong (a quoted field ran across the offset) is read
//! again from there. The chunks, the batches and every count are the same at any thread count.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use arrow::array::RecordBatch;

use crate::budget::{Budget, BudgetUse};
use crate::error::Result;
use crate::infer::{self, BatchBuilder, Cells, ColumnTally, Issues, Plan};
use crate::report::{Format, IngestReport, Issue, RowIssue};
use crate::sniff::Dialect;

/// Bytes of text a chunk starts from. A batch holds a chunk's rows.
pub(crate) const CHUNK_BYTES: usize = 4 << 20;

/// `f(0..jobs)` on up to `threads` threads, in job order. One thread runs on the caller's, which
/// is all wasm32 has.
pub(crate) fn parallel<T: Send>(
    jobs: usize,
    threads: usize,
    f: impl Fn(usize) -> T + Sync,
) -> Vec<T> {
    if threads <= 1 || jobs <= 1 {
        return (0..jobs).map(f).collect();
    }
    let next = AtomicUsize::new(0);
    let mut done: Vec<(usize, T)> = std::thread::scope(|s| {
        let workers: Vec<_> = (0..threads.min(jobs))
            .map(|_| {
                s.spawn(|| {
                    let mut mine = Vec::new();
                    loop {
                        let k = next.fetch_add(1, Ordering::Relaxed);
                        if k >= jobs {
                            return mine;
                        }
                        mine.push((k, f(k)));
                    }
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|w| w.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
            .collect()
    });
    done.sort_by_key(|(k, _)| *k);
    done.into_iter().map(|(_, t)| t).collect()
}

/// A field: a span of the text, or of the row's scratch where quotes had to be undone.
#[derive(Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
    scratch: bool,
}

#[derive(Default)]
struct Fields {
    spans: Vec<Span>,
    scratch: String,
    /// Fields in the row, which may be more than are kept once past `max_columns`.
    count: usize,
}

struct RowView<'a> {
    text: &'a str,
    fields: &'a Fields,
}

impl Cells for RowView<'_> {
    fn len(&self) -> usize {
        self.fields.spans.len()
    }

    fn cell(&self, i: usize) -> &str {
        let s = self.fields.spans[i];
        if s.scratch {
            &self.fields.scratch[s.start..s.end]
        } else {
            &self.text[s.start..s.end]
        }
    }
}

impl RowView<'_> {
    /// One field, or none whose text is not blank.
    fn is_blank(&self) -> bool {
        self.len() == 0 || (self.len() == 1 && self.cell(0).is_empty())
    }
}

struct Parser<'a> {
    text: &'a str,
    bytes: &'a [u8],
    delim: u8,
    quote: u8,
    /// Bytes that end an unquoted field.
    ends: [bool; 256],
    budget: &'a Budget,
}

/// A failure in a chunk, after `rows` of its non-blank rows.
struct Failed {
    rows: u64,
    error: crate::IngestError,
}

impl<'a> Parser<'a> {
    fn new(text: &'a str, dialect: &Dialect, budget: &'a Budget) -> Self {
        let mut ends = [false; 256];
        for b in [dialect.delimiter, b'\n', b'\r'] {
            ends[b as usize] = true;
        }
        Self {
            text,
            bytes: text.as_bytes(),
            delim: dialect.delimiter,
            quote: dialect.quote,
            ends,
            budget,
        }
    }

    fn unquoted_end(&self, from: usize) -> usize {
        let b = self.bytes;
        let mut i = from;
        while i < b.len() && !self.ends[b[i] as usize] {
            i += 1;
        }
        i
    }

    fn next_quote(&self, from: usize) -> Option<usize> {
        self.bytes[from..]
            .iter()
            .position(|&b| b == self.quote)
            .map(|i| from + i)
    }

    /// The row at `pos`, which is a row start before the end. Returns where the next row starts,
    /// and the field whose quote ran to the end, if one did.
    fn row(&self, mut pos: usize, f: &mut Fields) -> Result<(usize, Option<usize>)> {
        let (b, n) = (self.bytes, self.bytes.len());
        f.spans.clear();
        f.scratch.clear();
        f.count = 0;
        let keep = usize::try_from(self.budget.max_columns)
            .unwrap_or(usize::MAX)
            .saturating_add(1);
        loop {
            let mut unclosed = false;
            let span = if pos < n && b[pos] == self.quote {
                let mut seg = pos + 1;
                let from = f.scratch.len();
                let mut escaped = false;
                loop {
                    match self.next_quote(seg) {
                        Some(q) if q + 1 < n && b[q + 1] == self.quote => {
                            f.scratch.push_str(&self.text[seg..=q]);
                            escaped = true;
                            self.cell_fits(&[&f.scratch[from..]])?;
                            seg = q + 2;
                        }
                        Some(q) => {
                            let t = self.unquoted_end(q + 1);
                            let span = if !escaped && t == q + 1 {
                                Span {
                                    start: seg,
                                    end: q,
                                    scratch: false,
                                }
                            } else {
                                f.scratch.push_str(&self.text[seg..q]);
                                f.scratch.push_str(&self.text[q + 1..t]);
                                Span {
                                    start: from,
                                    end: f.scratch.len(),
                                    scratch: true,
                                }
                            };
                            pos = t;
                            break span;
                        }
                        None => {
                            unclosed = true;
                            let span = if escaped {
                                f.scratch.push_str(&self.text[seg..]);
                                Span {
                                    start: from,
                                    end: f.scratch.len(),
                                    scratch: true,
                                }
                            } else {
                                Span {
                                    start: seg,
                                    end: n,
                                    scratch: false,
                                }
                            };
                            pos = n;
                            break span;
                        }
                    }
                }
            } else {
                let t = self.unquoted_end(pos);
                let span = Span {
                    start: pos,
                    end: t,
                    scratch: false,
                };
                pos = t;
                span
            };
            let src = if span.scratch {
                f.scratch.as_str()
            } else {
                self.text
            };
            let raw = &src[span.start..span.end];
            self.cell_fits(&[raw])?;
            let trimmed = infer::trim(raw);
            let start = span.start + (trimmed.as_ptr() as usize - raw.as_ptr() as usize);
            if f.spans.len() < keep {
                f.spans.push(Span {
                    start,
                    end: start + trimmed.len(),
                    scratch: span.scratch,
                });
            }
            f.count += 1;
            if unclosed {
                self.columns_fit(f)?;
                return Ok((n, Some(f.count - 1)));
            }
            if pos >= n {
                self.columns_fit(f)?;
                return Ok((n, None));
            }
            match b[pos] {
                c if c == self.delim => pos += 1,
                b'\n' => {
                    self.columns_fit(f)?;
                    return Ok((pos + 1, None));
                }
                _ => {
                    // `\r`, alone or before `\n`.
                    self.columns_fit(f)?;
                    let next = if b.get(pos + 1) == Some(&b'\n') {
                        pos + 2
                    } else {
                        pos + 1
                    };
                    return Ok((next, None));
                }
            }
        }
    }

    /// A field's text, in pieces, against `max_cell_bytes`; past it, the breach is at the end of
    /// the character that crossed, as a reader adding one character at a time would see it.
    fn cell_fits(&self, pieces: &[&str]) -> Result<()> {
        let limit = self.budget.max_cell_bytes;
        let total: u64 = pieces.iter().map(|p| p.len() as u64).sum();
        if total <= limit {
            return Ok(());
        }
        let mut base = 0u64;
        for p in pieces {
            let len = p.len() as u64;
            if base + len > limit {
                let mut j = (limit - base) as usize;
                while !p.is_char_boundary(j) {
                    j -= 1;
                }
                let c = p[j..].chars().next().map_or(1, char::len_utf8);
                let at = base + (j + c) as u64;
                return Err(self.budget.breach("max_cell_bytes", limit, at));
            }
            base += len;
        }
        unreachable!("the pieces are over the limit")
    }

    fn columns_fit(&self, f: &Fields) -> Result<()> {
        let n = f.count as u64;
        if n > self.budget.max_columns {
            return Err(self
                .budget
                .breach("max_columns", self.budget.max_columns, n));
        }
        Ok(())
    }

    /// The first position at or after `from` just past a line end, quotes not considered, or the
    /// end: where a chunk is guessed to start.
    fn guess_start(&self, from: usize) -> usize {
        let b = self.bytes;
        let mut p = from.max(1);
        while p < b.len() {
            match b[p - 1] {
                b'\n' => return p,
                b'\r' if b[p] != b'\n' => return p,
                _ => p += 1,
            }
        }
        b.len()
    }

    /// Rows from `start`, a row start, until the first that ends at or past `stop`, each passed to
    /// `row` with its 0-based non-blank index in the chunk (`None` for a blank line).
    fn chunk(
        &self,
        start: usize,
        stop: usize,
        clock: &(dyn Fn() -> Result<()> + Sync),
        mut row: impl FnMut(&RowView, Option<u64>),
    ) -> std::result::Result<Walked, Failed> {
        let mut fields = Fields::default();
        let mut pos = start;
        let mut walked = Walked {
            end: start,
            rows: 0,
            physical: 0,
            unclosed: None,
        };
        let fail = |rows: u64| move |error| Failed { rows, error };
        if start < stop {
            while pos < self.bytes.len() {
                let (next, unclosed) = self.row(pos, &mut fields).map_err(fail(walked.rows))?;
                walked.physical += 1;
                if let Some(column) = unclosed {
                    walked.unclosed = Some((walked.physical, column));
                }
                let view = RowView {
                    text: self.text,
                    fields: &fields,
                };
                if view.is_blank() {
                    row(&view, None);
                } else {
                    row(&view, Some(walked.rows));
                    walked.rows += 1;
                }
                if walked.physical.is_multiple_of(4096) {
                    clock().map_err(fail(walked.rows))?;
                }
                pos = next;
                if pos >= stop {
                    break;
                }
            }
        }
        walked.end = pos;
        Ok(walked)
    }
}

/// Where a chunk's rows were.
#[derive(Clone, Copy)]
struct Walked {
    end: usize,
    /// Non-blank rows.
    rows: u64,
    /// Rows including blank lines, as a line count would number them.
    physical: u64,
    /// The 1-based physical row in the chunk whose quote ran to the end, and its field.
    unclosed: Option<(u64, usize)>,
}

/// The first pass over one chunk.
struct Counted {
    start: usize,
    walked: Walked,
    width: usize,
    /// The chunk's first non-blank row, which is not in `tallies`: the file's may be its header.
    first: Option<Vec<String>>,
    tallies: Vec<ColumnTally>,
    /// Its batch under a guessed plan, rows numbered from 1 in the chunk, where one was built.
    built: Option<(RecordBatch, Issues)>,
}

/// A guess at the whole file's plan from its first chunk, built under while the rest is counted.
/// Decimals are built at precision 38, which holds whatever precision the file turns out to need.
struct Guess<'a> {
    plan: Plan,
    header: bool,
    width: usize,
    /// Rows and each column's bytes per byte of text, for a builder's capacity.
    rows_per_byte: f64,
    bytes_per_byte: Vec<f64>,
    /// Bytes of batches built under the guess, which stop being built past the memory budget.
    held: &'a AtomicU64,
    room: u64,
}

fn count(
    p: &Parser,
    start: usize,
    stop: usize,
    clock: &(dyn Fn() -> Result<()> + Sync),
    guess: Option<&Guess>,
) -> (usize, std::result::Result<Counted, Failed>) {
    let mut width = 0;
    let mut first = None;
    let mut tallies = Vec::new();
    let guess = guess.filter(|g| g.held.load(Ordering::Relaxed) <= g.room);
    let mut builder = guess.map(|g| {
        let len = stop.saturating_sub(start) as f64;
        let bytes: Vec<u64> = g.bytes_per_byte.iter().map(|b| (b * len) as u64).collect();
        BatchBuilder::new(&g.plan, (g.rows_per_byte * len) as usize, &bytes, true)
    });
    let walked = p.chunk(start, stop, clock, |row, at| {
        let Some(at) = at else { return };
        width = width.max(row.len());
        if at == 0 {
            first = Some((0..row.len()).map(|i| row.cell(i).to_string()).collect());
            if let Some(b) = &mut builder {
                b.push(row, 1);
            }
        } else if let Some(b) = &mut builder {
            b.push_counting(row, at + 1, Some(&mut tallies));
        } else {
            infer::tally_row(&mut tallies, row);
        }
    });
    let built = match (builder, guess) {
        (Some(b), Some(g)) if walked.is_ok() => {
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
        width,
        first,
        tallies,
        built,
    });
    (start, counted)
}

/// The text as one CSV table under `dialect`. `memory` is what is held already, the text.
pub(crate) fn read(
    text: &str,
    dialect: Dialect,
    budget: &Budget,
    memory: u64,
    bytes_read: u64,
    threads: usize,
    clock: &(dyn Fn() -> Result<()> + Sync),
) -> Result<(Vec<RecordBatch>, IngestReport)> {
    read_in(
        text,
        dialect,
        budget,
        memory,
        bytes_read,
        threads,
        CHUNK_BYTES,
        clock,
    )
}

/// The header, the names and the counts of the data rows, from every row's counts.
fn settle(
    first: Option<&Vec<String>>,
    mut rest: Vec<ColumnTally>,
    rows: u64,
    width: usize,
    locale: crate::sniff::Locale,
) -> (bool, Vec<String>, Vec<ColumnTally>) {
    rest.resize_with(width, ColumnTally::default);
    let header = first.is_some_and(|f| infer::is_header(f, &rest, rows - 1, locale));
    if !header && let Some(row) = first {
        infer::tally_row(&mut rest, row);
    }
    let names = infer::column_names(header.then_some(first.map(Vec::as_slice)).flatten(), width);
    (header, names, rest)
}

#[allow(clippy::too_many_arguments)]
fn read_in(
    text: &str,
    dialect: Dialect,
    budget: &Budget,
    memory: u64,
    bytes_read: u64,
    threads: usize,
    chunk_bytes: usize,
    clock: &(dyn Fn() -> Result<()> + Sync),
) -> Result<(Vec<RecordBatch>, IngestReport)> {
    let p = Parser::new(text, &dialect, budget);
    let n = text.len();
    let jobs = n.div_ceil(chunk_bytes.max(1)).max(1);
    let stop = |k: usize| {
        if k + 1 >= jobs {
            n
        } else {
            (k + 1) * chunk_bytes
        }
    };
    let rows_limit = budget.max_rows.saturating_add(2);
    let failed = |rows_before: u64, f: Failed| {
        // A row cannot be counted past the one that crossed `max_rows`.
        if rows_before + f.rows < rows_limit {
            f.error
        } else {
            budget.breach("max_rows", budget.max_rows, rows_limit)
        }
    };

    // The first chunk alone, and from it a guess at the plan.
    let first_chunk = count(&p, 0, stop(0), clock, None).1;
    let first_chunk = first_chunk.map_err(|f| failed(0, f))?;
    let held = AtomicU64::new(0);
    let guess = (first_chunk.walked.rows > 0 && jobs > 1).then(|| {
        let c = &first_chunk;
        let (header, names, tallies) = settle(
            c.first.as_ref(),
            c.tallies.clone(),
            c.walked.rows,
            c.width,
            dialect.locale,
        );
        let rows = c.walked.rows - u64::from(header);
        let plan = Plan::new(names, &tallies, rows, dialect.locale, None).widened();
        let len = c.walked.end.max(1) as f64;
        Guess {
            plan,
            header,
            width: c.width,
            rows_per_byte: rows as f64 / len,
            bytes_per_byte: tallies.iter().map(|t| t.bytes as f64 / len).collect(),
            held: &held,
            room: budget.max_memory_bytes.saturating_sub(memory),
        }
    });

    // The rest at once, each from its guessed start, counted and built under the guess; then in
    // order, each read again from where the one before really ended if that is not where it was
    // guessed to start.
    let guessed = parallel(jobs - 1, threads, |j| {
        let k = j + 1;
        count(
            &p,
            p.guess_start(k * chunk_bytes),
            stop(k),
            clock,
            guess.as_ref(),
        )
    });
    let mut chunks: Vec<Counted> = Vec::with_capacity(jobs);
    let mut at = first_chunk.walked.end;
    let mut rows = first_chunk.walked.rows;
    if rows >= rows_limit {
        return Err(budget.breach("max_rows", budget.max_rows, rows_limit));
    }
    chunks.push(first_chunk);
    for (j, (start, counted)) in guessed.into_iter().enumerate() {
        let counted = if start == at {
            counted
        } else {
            count(&p, at, stop(j + 1), clock, None).1
        };
        let c = counted.map_err(|f| failed(rows, f))?;
        rows += c.walked.rows;
        if rows >= rows_limit {
            return Err(budget.breach("max_rows", budget.max_rows, rows_limit));
        }
        at = c.walked.end;
        chunks.push(c);
    }
    clock()?;

    // The whole file's counts, with every chunk's first row back in but the file's own.
    let width = chunks.iter().map(|c| c.width).max().unwrap_or(0);
    let mut rest: Vec<ColumnTally> = vec![ColumnTally::default(); width];
    let mut first: Option<Vec<String>> = None;
    for c in &mut chunks {
        for (t, o) in rest.iter_mut().zip(&c.tallies) {
            t.merge(o);
        }
        if let Some(row) = c.first.take() {
            if first.is_none() {
                first = Some(row);
            } else {
                infer::tally_row(&mut rest, &row);
            }
        }
    }
    let (header, names, tallies) = settle(first.as_ref(), rest, rows, width, dialect.locale);
    let data_rows = rows - u64::from(header);
    if data_rows > budget.max_rows {
        return Err(budget.breach("max_rows", budget.max_rows, data_rows));
    }
    let plan = Plan::new(names, &tallies, data_rows, dialect.locale, None);
    // What was built under the guess stands where the guess was the file's plan.
    let guessed_right = guess
        .as_ref()
        .is_some_and(|g| g.header == header && g.width == width && g.plan.reads_as(&plan));
    if !guessed_right {
        for c in &mut chunks {
            c.built = None;
        }
    }
    let batches_made = chunks.iter().filter(|c| c.walked.rows > 0).count() as u64;
    let estimate = memory + plan.estimate(data_rows, batches_made, &tallies);
    budget.charge("max_memory_bytes", budget.max_memory_bytes, estimate)?;

    let mut report = IngestReport {
        format: Format::Csv,
        dialect,
        header,
        rows: data_rows,
        columns: Vec::new(),
        rejected: Vec::new(),
        rejected_omitted: 0,
        budget_used: BudgetUse {
            bytes: bytes_read,
            rows: data_rows,
            columns: width as u64,
            memory_bytes: 0,
        },
    };
    report.dialect.locale = plan.locale;

    // Where each chunk's rows fall in the file, and the header's chunk.
    let mut placed = Vec::with_capacity(chunks.len());
    let (mut rows_before, mut physical_before) = (0u64, 0u64);
    let mut header_chunk = None;
    for (k, c) in chunks.iter().enumerate() {
        if let Some((row, column)) = c.walked.unclosed {
            let physical = physical_before + row;
            report.push_issue(RowIssue {
                row: if header {
                    physical.saturating_sub(1)
                } else {
                    physical
                },
                column: plan.names.get(column).cloned(),
                reason: Issue::UnclosedQuote,
            });
        }
        if header_chunk.is_none() && c.walked.rows > 0 {
            header_chunk = Some(k);
        }
        let skip = header && header_chunk == Some(k);
        let base = if header && header_chunk.is_some_and(|h| h < k) {
            rows_before - 1
        } else {
            rows_before
        };
        placed.push((skip, base));
        rows_before += c.walked.rows;
        physical_before += c.walked.physical;
    }

    // Second pass: a batch for each chunk with rows that has none yet.
    let mut built: Vec<Option<(RecordBatch, Issues)>> =
        chunks.iter_mut().map(|c| c.built.take()).collect();
    let from_guess: Vec<bool> = built.iter().map(Option::is_some).collect();
    let todo: Vec<usize> = (0..chunks.len())
        .filter(|&k| built[k].is_none() && chunks[k].walked.rows > 0 && width > 0)
        .collect();
    let rebuilt = parallel(todo.len(), threads, |j| -> Result<_> {
        let k = todo[j];
        let c = &chunks[k];
        let (skip, base) = placed[k];
        let bytes: Vec<u64> = c.tallies.iter().map(|t| t.bytes).collect();
        let mut b = BatchBuilder::new(&plan, c.walked.rows as usize, &bytes, true);
        let mut data = base;
        p.chunk(c.start, c.walked.end, clock, |row, at| {
            if at.is_none() || (skip && at == Some(0)) {
                return;
            }
            data += 1;
            b.push(row, data);
        })
        .map_err(|f| f.error)?;
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
            // Built under the guess: its rows are numbered within the chunk, its decimals at 38.
            found.renumber(placed[k].1);
            plan.retag(batch)
        } else {
            batch
        };
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
        issues.push(found);
    }
    if batches.is_empty() && width > 0 {
        batches.push(RecordBatch::new_empty(std::sync::Arc::clone(&plan.schema)));
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
    use crate::sniff::{EncodingKind, sniff_dialect};

    fn whole(text: &str, chunk_bytes: usize, threads: usize) -> (RecordBatch, IngestReport) {
        let dialect = sniff_dialect(text, EncodingKind::Utf8);
        let budget = Budget::untrusted();
        let (batches, mut report) = read_in(
            text,
            dialect,
            &budget,
            text.len() as u64,
            text.len() as u64,
            threads,
            chunk_bytes,
            &|| Ok(()),
        )
        .unwrap();
        // The offsets of more batches take more bytes; nothing else may differ.
        report.budget_used.memory_bytes = 0;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
        let one = arrow::compute::concat_batches(&schema, &batches).unwrap();
        (one, report)
    }

    const TEXTS: &[&str] = &[
        "id,note,amount\n1,\"a\nb\",1.5\n2,\"c,\"\"d\"\"\n\ne\",2.25\n3,plain,3\n",
        "a,b\r\n1,2\r\n3,4\r\n\r\n5,6\r\n",
        "a,b\r1,2\r3,4\r",
        "x;y\n1,5;2\n2,75;3\n",
        "n\n1\n2\n3\n4\n5\n6\n7\n8\n9\nten\n",
        "n,m\n1,1\n2,1.5\n3,123456.789\n4,\n",
        "k,v\n1,2024-01-01\n2,2024-01-02T10:00:00\n3,yes\n",
        "a,b,c\n1,2,3\n4,5\n6\n\n\n7,8,9\n",
        "a,b\n1,\"open\n2,3\n4,5",
        "a,b\n1,\"x\"y\n2, \"z\" \n",
        "just,names\nalso,text\n",
        "1,2\n3,4\n",
        "h\n",
        "",
        "\n\n\n",
    ];

    #[test]
    fn a_file_read_in_chunks_is_the_file_read_whole() {
        for text in TEXTS {
            let want = whole(text, usize::MAX / 2, 1);
            for chunk_bytes in [1, 2, 3, 5, 7, 11, 16, 64] {
                for threads in [1, 3, 8] {
                    let got = whole(text, chunk_bytes, threads);
                    assert_eq!(
                        got.1, want.1,
                        "{text:?} at {chunk_bytes} bytes, {threads} threads"
                    );
                    assert_eq!(
                        got.0, want.0,
                        "{text:?} at {chunk_bytes} bytes, {threads} threads"
                    );
                }
            }
        }
    }

    #[test]
    fn quoted_newlines_across_every_chunk_boundary() {
        let mut text = String::from("id,body,n\n");
        for i in 0..200 {
            text.push_str(&format!(
                "{i},\"line {i}\nstill {i}\r\n\"\"q\"\"\",{}\n",
                i * 3
            ));
        }
        let want = whole(&text, usize::MAX / 2, 1);
        assert_eq!(want.1.rows, 200);
        assert!(want.1.rejected.is_empty(), "{:?}", want.1.rejected);
        for chunk_bytes in [1, 9, 13, 31, 100, 257] {
            for threads in [1, 4] {
                let got = whole(&text, chunk_bytes, threads);
                assert_eq!(got.1, want.1, "at {chunk_bytes} bytes");
                assert_eq!(got.0, want.0, "at {chunk_bytes} bytes");
            }
        }
    }

    #[test]
    fn a_guess_from_the_first_chunk_that_the_file_overturns_is_rebuilt() {
        // Integers for the first chunks, then a wider decimal and finally a column gone to text.
        let mut text = String::from("a,b,c\n");
        for i in 0..400 {
            text.push_str(&format!("{i},{i},{i}\n"));
        }
        text.push_str("1,123456.5,x\n2,7.25,y\n");
        for i in 0..40 {
            text.push_str(&format!("{i},{i},t{i}\n"));
        }
        let want = whole(&text, usize::MAX / 2, 1);
        for chunk_bytes in [64, 500, 2000] {
            for threads in [1, 4] {
                let got = whole(&text, chunk_bytes, threads);
                assert_eq!(got.1, want.1, "at {chunk_bytes} bytes");
                assert_eq!(got.0, want.0, "at {chunk_bytes} bytes");
            }
        }
        // Only the decimal's precision moves: the guessed batches are kept and retagged.
        let mut text = String::from("a,b\n");
        for i in 0..400 {
            text.push_str(&format!("{i},{i}.5\n"));
        }
        text.push_str("1,123456789.5\n");
        let want = whole(&text, usize::MAX / 2, 1);
        assert!(matches!(
            want.1.columns[1].inferred,
            crate::InferredType::Decimal {
                precision: 10,
                scale: 1
            }
        ));
        assert_eq!(whole(&text, 64, 4), want);
    }
}
