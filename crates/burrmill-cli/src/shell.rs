//! `burrmill` with no command: a prompt over the tables the paths name (#390).
//!
//! A statement runs once a line ends it with `;`, and may span lines. A line starting with `.`
//! between statements is a dot-command. On a terminal the prompt keeps history and Ctrl-C cancels
//! the statement in flight through the engine's token; otherwise the lines come from stdin and no
//! prompt is printed.

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use burrmill::{Budget, BurrmillError, CancelToken, Engine};
use unicode_width::UnicodeWidthStr;

use crate::Format;

/// What a session's SQL may reach. Every session is a sandbox until #387 decides a trusted one; the
/// banner and the prompt read it from here and nowhere else.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Trust {
    Sandbox,
}

impl Trust {
    pub fn label(self) -> &'static str {
        match self {
            Trust::Sandbox => "sandbox",
        }
    }
    fn means(self) -> &'static str {
        match self {
            Trust::Sandbox => {
                "SELECT only, over the tables opened here; SQL reads no other file and writes nothing"
            }
        }
    }
}

/// The trust of every session today.
pub const TRUST: Trust = Trust::Sandbox;

const HELP: &str = ".tables          the tables, from information_schema
.schema [TABLE]  each table's columns, or TABLE's, from information_schema
.timer on|off    print each statement's time to stderr
.mode table|csv|json
                 how answers print; table unless set
.read FILE       run the statements in FILE
.help            this
.quit            leave; Ctrl-D does too
A statement runs once a line ends it with ;. Ctrl-C cancels the statement running, or clears the
line at the prompt.";

/// `.read` inside `.read` stops this deep, so a file that reads itself is refused.
const READ_DEPTH: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Mode {
    Table,
    Csv,
    Json,
}

/// Whether the session goes on after a line.
#[derive(Debug, PartialEq)]
pub enum Flow {
    Go,
    Quit,
}

/// Cancels the statement in flight, from any thread: what Ctrl-C calls.
#[derive(Clone)]
pub struct Interrupt {
    running: Arc<Mutex<bool>>,
    token: CancelToken,
}

impl Interrupt {
    /// Cancels the statement running, through the engine's token. False if none is.
    pub fn cancel(&self) -> bool {
        let running = self.running.lock().unwrap_or_else(|e| e.into_inner());
        if *running {
            self.token.cancel();
        }
        *running
    }
}

pub struct Shell<'e> {
    engine: &'e Engine,
    interrupt: Interrupt,
    mode: Mode,
    timer: bool,
    /// Lines of a statement not yet ended by `;`.
    pending: String,
    depth: usize,
    /// Whether any statement or command failed, for a script's exit status.
    pub failed: bool,
    out: Box<dyn Write + Send + 'e>,
    err: Box<dyn Write + Send + 'e>,
}

impl<'e> Shell<'e> {
    pub fn new(
        engine: &'e Engine,
        out: Box<dyn Write + Send + 'e>,
        err: Box<dyn Write + Send + 'e>,
    ) -> Self {
        Self {
            engine,
            interrupt: Interrupt {
                running: Arc::default(),
                token: engine.cancel_token(),
            },
            mode: Mode::Table,
            timer: false,
            pending: String::new(),
            depth: 0,
            failed: false,
            out,
            err,
        }
    }

    pub fn interrupt(&self) -> Interrupt {
        self.interrupt.clone()
    }

    /// Whether a statement has begun and not yet ended.
    pub fn pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Drops the statement begun, as Ctrl-C at the prompt does.
    pub fn clear(&mut self) {
        self.pending.clear();
    }

    /// One line of input: a dot-command, or part of a statement, running each one it ends.
    pub fn line(&mut self, line: &str) -> io::Result<Flow> {
        let flow = if self.pending.is_empty() && line.trim_start().starts_with('.') {
            self.dot(line.trim())?
        } else {
            self.pending.push_str(line);
            self.pending.push('\n');
            loop {
                let (end, content) = scan(&self.pending);
                if !content {
                    // Only whitespace and comments before the `;` or the end: nothing to run.
                    match end {
                        Some(end) => drop(self.pending.drain(..=end)),
                        None => {
                            self.pending.clear();
                            break;
                        }
                    }
                    continue;
                }
                let Some(end) = end else { break };
                let sql = self.pending[..end].trim().to_string();
                self.pending.drain(..=end);
                self.statement(&sql)?;
            }
            Flow::Go
        };
        self.out.flush()?;
        self.err.flush()?;
        Ok(flow)
    }

    /// The end of input: a last statement with no `;` runs.
    pub fn finish(&mut self) -> io::Result<()> {
        let sql = std::mem::take(&mut self.pending);
        if scan(&sql).1 {
            self.statement(sql.trim())?;
        }
        self.out.flush()?;
        self.err.flush()
    }

    /// Every line `input` holds, then its end.
    pub fn script(&mut self, input: impl BufRead) -> io::Result<Flow> {
        for line in input.lines() {
            if self.line(&line?)? == Flow::Quit {
                return Ok(Flow::Quit);
            }
        }
        self.finish()?;
        Ok(Flow::Go)
    }

    fn fail(&mut self, message: &str) -> io::Result<()> {
        self.failed = true;
        writeln!(self.err, "error: {message}")
    }

    /// `sql`'s answer, or `None` once its error is printed.
    fn query(&mut self, sql: &str) -> io::Result<Option<Vec<RecordBatch>>> {
        let started = Instant::now();
        *self
            .interrupt
            .running
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = true;
        let r = self.engine.sql(sql);
        {
            // Under the lock, so a Ctrl-C that lands as the statement ends cannot cancel the next.
            let mut running = self
                .interrupt
                .running
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *running = false;
            self.interrupt.token.reset();
        }
        if self.timer {
            writeln!(self.err, "time: {:.3}s", started.elapsed().as_secs_f64())?;
        }
        match r {
            Ok(batches) => Ok(Some(batches)),
            Err(BurrmillError::Cancelled) => {
                self.failed = true;
                writeln!(self.err, "cancelled")?;
                Ok(None)
            }
            Err(e) => {
                self.fail(&e.to_string())?;
                Ok(None)
            }
        }
    }

    fn statement(&mut self, sql: &str) -> io::Result<()> {
        if let Some(batches) = self.query(sql)? {
            self.print(&batches)?;
        }
        Ok(())
    }

    fn print(&mut self, batches: &[RecordBatch]) -> io::Result<()> {
        let r = match self.mode {
            Mode::Table => table(batches, &mut *self.out),
            Mode::Csv => crate::write(Format::Csv, batches, &mut *self.out),
            Mode::Json if batches.is_empty() => writeln!(self.out, "[]").map_err(|e| e.to_string()),
            Mode::Json => crate::write(Format::Json, batches, &mut *self.out)
                .and_then(|()| writeln!(self.out).map_err(|e| e.to_string())),
        };
        match r {
            Ok(()) => Ok(()),
            Err(e) => self.fail(&e),
        }
    }

    fn dot(&mut self, line: &str) -> io::Result<Flow> {
        let (command, arg) = match line.split_once(char::is_whitespace) {
            Some((c, a)) => (c, a.trim()),
            None => (line, ""),
        };
        match (command, arg) {
            (".quit" | ".exit", "") => return Ok(Flow::Quit),
            (".help", "") => writeln!(self.out, "{HELP}")?,
            (".tables", "") => self.statement(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema <> 'information_schema' ORDER BY table_name",
            )?,
            (".schema", "") => self.statement(
                "SELECT table_name, column_name, data_type, is_nullable \
                 FROM information_schema.columns WHERE table_schema <> 'information_schema' \
                 ORDER BY table_name, ordinal_position",
            )?,
            (".schema", t) => {
                let sql = format!(
                    "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
                     WHERE table_schema <> 'information_schema' AND table_name = '{}' \
                     ORDER BY ordinal_position",
                    t.replace('\'', "''")
                );
                match self.query(&sql)? {
                    Some(b) if b.iter().all(|b| b.num_rows() == 0) => {
                        self.fail(&format!("no table {t}"))?
                    }
                    Some(b) => self.print(&b)?,
                    None => {}
                }
            }
            (".timer", "on") => self.timer = true,
            (".timer", "off") => self.timer = false,
            (".mode", "table") => self.mode = Mode::Table,
            (".mode", "csv") => self.mode = Mode::Csv,
            (".mode", "json") => self.mode = Mode::Json,
            (".read", file) if !file.is_empty() => return self.read(PathBuf::from(file)),
            (".timer", _) => self.fail(".timer on or .timer off")?,
            (".mode", _) => self.fail(".mode table, .mode csv or .mode json")?,
            (".read", _) => self.fail(".read FILE")?,
            (".quit" | ".exit" | ".help" | ".tables", _) => {
                self.fail(&format!("{command} takes no argument"))?
            }
            _ => self.fail(&format!("no command {command}; .help lists them"))?,
        }
        Ok(Flow::Go)
    }

    fn read(&mut self, path: PathBuf) -> io::Result<Flow> {
        if self.depth >= READ_DEPTH {
            self.fail(&format!(".read nested past {READ_DEPTH} files"))?;
            return Ok(Flow::Go);
        }
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                self.fail(&format!("{}: {e}", path.display()))?;
                return Ok(Flow::Go);
            }
        };
        self.depth += 1;
        let r = self.script(io::BufReader::new(file));
        self.depth -= 1;
        r
    }
}

/// Where the first statement in `s` ends (the byte offset of its `;`, outside quotes and comments),
/// and whether anything but whitespace and comments comes before it, or before the end if none.
fn scan(s: &str) -> (Option<usize>, bool) {
    let b = s.as_bytes();
    let mut content = false;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"') => {
                content = true;
                i += 1;
                while i < b.len() && b[i] != q {
                    i += 1;
                }
                // A doubled quote closes the string and opens the next: the same as one string.
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < b.len() && !(b[i] == b'*' && b.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                i += 1;
            }
            b';' => return (Some(i), content),
            c if c.is_ascii_whitespace() => {}
            _ => content = true,
        }
        i += 1;
    }
    (None, content)
}

/// Each batch as one boxed table, then how many rows.
fn table(batches: &[RecordBatch], out: &mut dyn Write) -> Result<(), String> {
    let io = |e: io::Error| e.to_string();
    let Some(schema) = batches.first().map(|b| b.schema()) else {
        return writeln!(out, "(0 rows)").map_err(io);
    };
    let options = FormatOptions::default().with_null("NULL");
    let header: Vec<String> = schema.fields().iter().map(|f| cell(f.name())).collect();
    let mut rows: Vec<Vec<String>> = Vec::new();
    for b in batches {
        let columns = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        for r in 0..b.num_rows() {
            rows.push(
                columns
                    .iter()
                    .map(|c| cell(&c.value(r).to_string()))
                    .collect(),
            );
        }
    }
    let mut widths: Vec<usize> = header.iter().map(|h| h.width()).collect();
    for row in &rows {
        for (w, c) in widths.iter_mut().zip(row) {
            *w = (*w).max(c.width());
        }
    }
    let rule: String = widths.iter().fold("+".to_string(), |mut s, w| {
        s.push_str(&"-".repeat(w + 2));
        s.push('+');
        s
    });
    let line = |out: &mut dyn Write, row: &[String]| -> io::Result<()> {
        write!(out, "|")?;
        for (c, w) in row.iter().zip(&widths) {
            write!(out, " {c}{} |", " ".repeat(w - c.width()))?;
        }
        writeln!(out)
    };
    writeln!(out, "{rule}").map_err(io)?;
    line(out, &header).map_err(io)?;
    writeln!(out, "{rule}").map_err(io)?;
    for row in &rows {
        line(out, row).map_err(io)?;
    }
    writeln!(out, "{rule}").map_err(io)?;
    let n = rows.len();
    writeln!(out, "({n} row{})", if n == 1 { "" } else { "s" }).map_err(io)
}

/// A value on one line of a table: control characters escaped.
fn cell(s: &str) -> String {
    s.chars()
        .flat_map(|c| match c {
            '\n' => vec!['\\', 'n'],
            '\r' => vec!['\\', 'r'],
            '\t' => vec!['\\', 't'],
            c if c.is_control() => vec!['?'],
            c => vec![c],
        })
        .collect()
}

/// Bytes as the largest binary unit that divides them.
fn bytes(n: usize) -> String {
    for (shift, unit) in [(30, "GiB"), (20, "MiB"), (10, "KiB")] {
        if n >= 1 << shift && n.is_multiple_of(1 << shift) {
            return format!("{} {unit}", n >> shift);
        }
    }
    format!("{n} bytes")
}

/// What the banner says of the session: its trust and its budget.
pub fn banner(budget: &Budget) -> String {
    let threads = budget.threads;
    format!(
        "burrmill: a {} session: {}.\nbudget: {} of memory, {threads} thread{}{}.\n\
         .help for help, .quit or Ctrl-D to leave.",
        TRUST.label(),
        TRUST.means(),
        bytes(budget.memory_bytes),
        if threads == 1 { "" } else { "s" },
        if budget.spill.is_none() {
            ", no spill"
        } else {
            ""
        },
    )
}

/// Where history is kept: `$BURRMILL_HISTORY`, or `~/.burrmill_history`.
fn history() -> Option<PathBuf> {
    std::env::var_os("BURRMILL_HISTORY")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".burrmill_history")))
}

/// The prompt on a terminal, with history, until `.quit` or Ctrl-D.
pub fn interactive(shell: &mut Shell, budget: &Budget) -> io::Result<()> {
    use rustyline::error::ReadlineError;
    let mut editor = rustyline::DefaultEditor::new().map_err(io::Error::other)?;
    let history = history();
    if let Some(h) = &history {
        // None yet is the first session.
        let _ = editor.load_history(h);
    }
    println!("{}", banner(budget));
    let prompt = format!("{}> ", TRUST.label());
    let more = format!("{}> ", " ".repeat(prompt.len() - 5) + "...");
    loop {
        let line = editor.readline(if shell.pending() { &more } else { &prompt });
        match line {
            Ok(line) => {
                if !line.trim().is_empty() {
                    let _ = editor.add_history_entry(line.as_str());
                }
                if shell.line(&line)? == Flow::Quit {
                    break;
                }
            }
            Err(ReadlineError::Interrupted) => shell.clear(),
            Err(ReadlineError::Eof) => break,
            Err(e) => return Err(io::Error::other(e)),
        }
    }
    if let Some(h) = &history
        && let Err(e) = editor.save_history(h)
    {
        eprintln!("burrmill: history not kept: {}: {e}", h.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_statement_ends_at_a_semicolon_outside_quotes_and_comments() {
        assert_eq!(scan("SELECT 1"), (None, true));
        assert_eq!(scan("SELECT 1;"), (Some(8), true));
        assert_eq!(scan("SELECT ';' ;"), (Some(11), true));
        assert_eq!(scan("SELECT 'it''s;' ;"), (Some(16), true));
        assert_eq!(scan("-- a; b\nSELECT 1;"), (Some(16), true));
        assert_eq!(scan("/* ; */ SELECT \"a;\";"), (Some(19), true));
        assert_eq!(scan("  -- only\n"), (None, false));
        assert_eq!(scan(";"), (Some(0), false));
        assert_eq!(scan("/* never closed"), (None, false));
    }

    #[test]
    fn budgets_print_in_their_largest_unit() {
        assert_eq!(bytes(4 << 30), "4 GiB");
        assert_eq!(bytes(256 << 20), "256 MiB");
        assert_eq!(bytes(3 << 10), "3 KiB");
        assert_eq!(bytes(1536), "1536 bytes");
        assert_eq!(bytes(40), "40 bytes");
    }

    /// Writes into a buffer the test reads once the shell is done.
    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl Write for Buf {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Buf {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    /// Ctrl-C's path without the signal: a join that runs for seconds, cancelled from another
    /// thread, is back at the prompt within the engine's bound, and the next statement runs.
    #[test]
    fn a_cancel_from_another_thread_ends_the_statement_and_not_the_session() {
        // The engine's join bound is 100 ms in release (#344); an unoptimised build on a loaded
        // runner misses it by tens, as crates/burrmill/tests/df_host.rs allows. The rest is margin.
        let bound = Duration::from_millis(if cfg!(debug_assertions) { 1000 } else { 250 });
        let tmp = tempfile::tempdir().unwrap();
        let csv = tmp.path().join("t.csv");
        let body: String = std::iter::once("id\n".to_string())
            .chain((0..60_000).map(|i| format!("{i}\n")))
            .collect();
        std::fs::write(&csv, body).unwrap();
        let engine = Engine::builder().csv(&csv, "t").open().unwrap();
        let (out, err) = (Buf::default(), Buf::default());
        let mut shell = Shell::new(&engine, Box::new(out.clone()), Box::new(err.clone()));
        // A shared runner can stall one attempt; a cancel that is not seen is late on every one.
        let mut seen = Vec::new();
        for _ in 0..3 {
            let interrupt = shell.interrupt();
            let cancelled = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(300));
                while !interrupt.cancel() {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Instant::now()
            });
            let flow = shell
                .line("SELECT sum(a.id * b.id) AS s FROM t a,")
                .unwrap();
            assert_eq!(flow, Flow::Go);
            assert!(shell.pending());
            shell.line("t b;").unwrap();
            let back = Instant::now();
            let cancelled = cancelled.join().unwrap();
            let late = back.saturating_duration_since(cancelled);
            eprintln!("back {late:?} after the cancel");
            seen.push(late);
            if late < bound {
                break;
            }
        }
        assert!(seen.iter().any(|l| *l < bound), "late by {seen:?}");
        assert!(err.text().starts_with("cancelled\n"), "{}", err.text());
        shell.line("SELECT 1 AS one;").unwrap();
        assert_eq!(
            out.text(),
            "+-----+\n| one |\n+-----+\n| 1   |\n+-----+\n(1 row)\n"
        );
    }
}
