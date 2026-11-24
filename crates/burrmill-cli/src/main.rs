//! `burrmill`: SQL over Parquet, CSV and JSON files, with the three things datafusion-cli and DuckDB
//! do not offer (#343): strict decimals, answers independent of threads and file layout, and
//! receipts.
//!
//! ```text
//! burrmill q SQL [NAME=]PATH... [--strict] [--deterministic] [--receipt FILE]
//!                [--format csv|json|parquet|arrow] [--out FILE] [--threads N] [--memory BYTES]
//! burrmill verify RECEIPT [NAME=]PATH... [--threads N] [--memory BYTES]
//! burrmill [[NAME=]PATH...] [--strict] [--deterministic] [--threads N] [--memory BYTES]
//! ```
//!
//! With no command, `burrmill` is a shell over the tables the paths name (`shell.rs`).
//!
//! A PATH holding `segments/` is a nest. A directory's `.parquet` files are each a table named by
//! the file, and its subdirectories each one table of everything beneath; a directory whose
//! subdirectories are all `key=value` is itself one table. A file ending `.csv` is read as CSV, and
//! one ending `.json`, `.ndjson` or `.jsonl` as JSON, by `burrmill-ingest` (#389); any other file as
//! Parquet. `NAME=PATH` names one table outright.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use arrow::record_batch::RecordBatch;
use burrmill::{Budget, Engine, EngineBuilder, Receipt};

mod shell;

const USAGE: &str = "usage:
  burrmill q SQL [NAME=]PATH... [--strict] [--deterministic] [--receipt FILE]
                 [--format csv|json|parquet|arrow] [--out FILE] [--threads N] [--memory BYTES]
  burrmill verify RECEIPT [NAME=]PATH... [--threads N] [--memory BYTES]
  burrmill [[NAME=]PATH...] [--strict] [--deterministic] [--threads N] [--memory BYTES]
                 a shell over the tables; .help in it lists its commands

A PATH is a Parquet file or directory, a nest, or a .csv, .json, .ndjson or .jsonl file.
CSV and JSON are read into memory, out of --memory.";

#[derive(Debug, Default)]
struct Args {
    command: String,
    first: String,
    paths: Vec<String>,
    strict: bool,
    deterministic: bool,
    receipt: Option<PathBuf>,
    format: Format,
    out: Option<PathBuf>,
    threads: Option<usize>,
    memory: Option<usize>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
enum Format {
    #[default]
    Csv,
    Json,
    Parquet,
    Arrow,
}

fn parse(argv: &[String]) -> Result<Args, String> {
    let mut a = Args::default();
    let mut rest = Vec::new();
    let mut it = argv.iter();
    let value = |it: &mut std::slice::Iter<String>, flag: &str| {
        it.next()
            .cloned()
            .ok_or_else(|| format!("{flag} takes a value"))
    };
    while let Some(x) = it.next() {
        match x.as_str() {
            "--strict" => a.strict = true,
            "--deterministic" => a.deterministic = true,
            "--receipt" => a.receipt = Some(value(&mut it, x)?.into()),
            "--out" | "-o" => a.out = Some(value(&mut it, x)?.into()),
            "--format" => {
                a.format = match value(&mut it, x)?.as_str() {
                    "csv" => Format::Csv,
                    "json" => Format::Json,
                    "parquet" => Format::Parquet,
                    "arrow" => Format::Arrow,
                    f => return Err(format!("no format {f}: csv, json, parquet or arrow")),
                }
            }
            "--threads" => a.threads = Some(value(&mut it, x)?.parse().map_err(|_| "--threads N")?),
            "--memory" => {
                a.memory = Some(value(&mut it, x)?.parse().map_err(|_| "--memory BYTES")?)
            }
            "-h" | "--help" => return Err(String::new()),
            f if f.starts_with("--") => return Err(format!("no option {f}")),
            _ => rest.push(x.clone()),
        }
    }
    let mut rest = rest.into_iter();
    a.command = rest.next().ok_or("")?;
    a.first = rest.next().ok_or("")?;
    a.paths = rest.collect();
    if !matches!(a.command.as_str(), "q" | "verify") {
        return Err(format!("no command {}", a.command));
    }
    if a.paths.is_empty() {
        return Err("name the files to read".into());
    }
    // A receipt is made only of a deterministic answer.
    a.deterministic |= a.receipt.is_some() || a.command == "verify";
    Ok(a)
}

/// The tables `paths` name, each added to `b`.
fn tables(mut b: EngineBuilder, paths: &[String]) -> Result<EngineBuilder, String> {
    let mut nest = false;
    for p in paths {
        if let Some((name, path)) = p.split_once('=').filter(|(n, _)| !n.contains('/')) {
            b = file(b, Path::new(path), name);
            continue;
        }
        let path = Path::new(p);
        if path.join("segments").is_dir() {
            if nest {
                return Err("one nest at a time; name other tables NAME=PATH".into());
            }
            nest = true;
            b = b.nest(path);
        } else if path.is_file() {
            b = file(b, path, &stem(path)?);
        } else if path.is_dir() {
            let mut entries: Vec<PathBuf> = std::fs::read_dir(path)
                .map_err(|e| format!("{p}: {e}"))?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|e| {
                    e.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| !n.starts_with('.') && !n.starts_with('_'))
                })
                .collect();
            entries.sort();
            let dirs: Vec<&PathBuf> = entries.iter().filter(|e| e.is_dir()).collect();
            let partitioned = !dirs.is_empty()
                && dirs.iter().all(|d| {
                    d.file_name()
                        .is_some_and(|n| n.to_string_lossy().contains('='))
                });
            if partitioned {
                b = b.parquet(path, &stem(path)?);
                continue;
            }
            let mut any = false;
            for e in &entries {
                if e.is_dir() || e.extension().is_some_and(|x| x == "parquet") {
                    b = b.parquet(e, &stem(e)?);
                    any = true;
                }
            }
            if !any {
                return Err(format!("no .parquet files or directories under {p}"));
            }
        } else {
            return Err(format!("nothing at {p}"));
        }
    }
    Ok(b)
}

/// How a file is read, by its extension, ignoring case.
#[derive(Debug, PartialEq)]
enum Kind {
    Parquet,
    Csv,
    Json,
}

fn kind(p: &Path) -> Kind {
    let ext = p
        .extension()
        .and_then(|x| x.to_str())
        .map(str::to_ascii_lowercase);
    match ext.as_deref() {
        Some("csv") => Kind::Csv,
        Some("json" | "ndjson" | "jsonl") => Kind::Json,
        _ => Kind::Parquet,
    }
}

/// The table `name` over the file or directory at `path`, read as its extension says.
fn file(b: EngineBuilder, path: &Path, name: &str) -> EngineBuilder {
    match kind(path) {
        Kind::Csv => b.csv(path, name),
        Kind::Json => b.json(path, name),
        Kind::Parquet => b.parquet(path, name),
    }
}

fn stem(p: &Path) -> Result<String, String> {
    let name = p
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("{} has no name to give a table", p.display()))?;
    let stem = match kind(p) {
        Kind::Parquet => name.strip_suffix(".parquet"),
        Kind::Csv | Kind::Json => p.file_stem().and_then(|s| s.to_str()),
    };
    Ok(stem.unwrap_or(name).to_string())
}

fn budget(a: &Args) -> Budget {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    Budget {
        memory_bytes: a.memory.unwrap_or(4 << 30),
        threads: a.threads.unwrap_or(cores.min(8)),
        spill: None,
    }
}

fn engine(a: &Args) -> Result<Engine, String> {
    let e = tables(Engine::builder().budget(budget(a)), &a.paths)?
        .open()
        .map_err(|e| e.to_string())?;
    e.set_strict_decimals(a.strict);
    e.set_deterministic(a.deterministic);
    Ok(e)
}

fn write(
    format: Format,
    batches: &[RecordBatch],
    out: &mut (dyn Write + Send),
) -> Result<(), String> {
    let io = |e: &dyn std::fmt::Display| e.to_string();
    let Some(schema) = batches.first().map(|b| b.schema()) else {
        return Ok(());
    };
    match format {
        Format::Csv => {
            let mut w = arrow::csv::WriterBuilder::new()
                .with_header(true)
                .build(out);
            for b in batches {
                w.write(b).map_err(|e| io(&e))?;
            }
        }
        Format::Json => {
            let mut w = arrow::json::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, arrow::json::writer::JsonArray>(out);
            for b in batches {
                w.write(b).map_err(|e| io(&e))?;
            }
            w.finish().map_err(|e| io(&e))?;
        }
        Format::Arrow => {
            let mut w =
                arrow::ipc::writer::StreamWriter::try_new(out, &schema).map_err(|e| io(&e))?;
            for b in batches {
                w.write(b).map_err(|e| io(&e))?;
            }
            w.finish().map_err(|e| io(&e))?;
        }
        Format::Parquet => {
            let mut w =
                parquet::arrow::ArrowWriter::try_new(out, schema, None).map_err(|e| io(&e))?;
            for b in batches {
                w.write(b).map_err(|e| io(&e))?;
            }
            w.close().map_err(|e| io(&e))?;
        }
    }
    Ok(())
}

fn run(a: &Args) -> Result<(), String> {
    let e = engine(a)?;
    if a.command == "verify" {
        let text = std::fs::read_to_string(&a.first).map_err(|x| format!("{}: {x}", a.first))?;
        let r = Receipt::parse(text.trim()).map_err(|x| x.to_string())?;
        e.set_strict_decimals(r.strict_decimals);
        e.verify_receipt(&r).map_err(|x| x.to_string())?;
        eprintln!("verified: {}", r.statement);
        return Ok(());
    }
    let batches = match &a.receipt {
        Some(path) => {
            let (batches, r) = e.sql_receipt(&a.first).map_err(|x| x.to_string())?;
            std::fs::write(path, r.to_json() + "\n")
                .map_err(|x| format!("{}: {x}", path.display()))?;
            batches
        }
        None => e.sql(&a.first).map_err(|x| x.to_string())?,
    };
    match &a.out {
        Some(path) => {
            let f = std::fs::File::create(path).map_err(|x| format!("{}: {x}", path.display()))?;
            write(a.format, &batches, &mut std::io::BufWriter::new(f))
        }
        None => {
            let mut out = std::io::BufWriter::new(std::io::stdout());
            write(a.format, &batches, &mut out)?;
            out.flush().map_err(|x| x.to_string())
        }
    }
}

/// The first argument that is not an option or an option's value: the command, if one is named.
fn command(argv: &[String]) -> Option<&str> {
    let mut it = argv.iter();
    while let Some(x) = it.next() {
        match x.as_str() {
            "--receipt" | "--out" | "-o" | "--format" | "--threads" | "--memory" => {
                it.next();
            }
            f if f.starts_with('-') => {}
            c => return Some(c),
        }
    }
    None
}

/// `burrmill [PATH...]`: the shell's options are the engine's, and every other argument is a path.
fn parse_shell(argv: &[String]) -> Result<Args, String> {
    let mut a = Args {
        command: "shell".into(),
        ..Args::default()
    };
    let mut it = argv.iter();
    let value = |it: &mut std::slice::Iter<String>, flag: &str| {
        it.next()
            .cloned()
            .ok_or_else(|| format!("{flag} takes a value"))
    };
    while let Some(x) = it.next() {
        match x.as_str() {
            "--strict" => a.strict = true,
            "--deterministic" => a.deterministic = true,
            "--threads" => a.threads = Some(value(&mut it, x)?.parse().map_err(|_| "--threads N")?),
            "--memory" => {
                a.memory = Some(value(&mut it, x)?.parse().map_err(|_| "--memory BYTES")?)
            }
            f if f.starts_with('-') => return Err(format!("no option {f} without a command")),
            _ => a.paths.push(x.clone()),
        }
    }
    Ok(a)
}

/// The shell: a prompt on a terminal, otherwise stdin's statements with no prompt.
fn run_shell(a: &Args) -> ExitCode {
    use std::io::IsTerminal;
    let e = match engine(a) {
        Ok(e) => e,
        Err(m) => {
            eprintln!("burrmill: {m}");
            return ExitCode::FAILURE;
        }
    };
    let interactive = std::io::stdin().is_terminal();
    let mut shell = shell::Shell::new(
        &e,
        Box::new(std::io::BufWriter::new(std::io::stdout())),
        Box::new(std::io::stderr()),
    );
    let interrupt = shell.interrupt();
    // Ctrl-C cancels the statement running. With none, a script stops; at a prompt the line editor
    // has the key, and between statements it does nothing.
    if let Err(m) = ctrlc::set_handler(move || {
        if !interrupt.cancel() && !interactive {
            std::process::exit(130);
        }
    }) {
        eprintln!("burrmill: Ctrl-C will not cancel a statement: {m}");
    }
    let r = if interactive {
        shell::interactive(&mut shell, &budget(a))
    } else {
        shell.script(std::io::stdin().lock()).map(drop)
    };
    match r {
        // A reader that has gone, as `| head` does, is not a failure.
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => ExitCode::SUCCESS,
        Err(m) => {
            eprintln!("burrmill: {m}");
            ExitCode::FAILURE
        }
        Ok(()) if shell.failed && !interactive => ExitCode::FAILURE,
        Ok(()) => ExitCode::SUCCESS,
    }
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let help = argv.iter().any(|x| x == "-h" || x == "--help");
    if !help && !matches!(command(&argv), Some("q" | "verify")) {
        return match parse_shell(&argv) {
            Ok(a) => run_shell(&a),
            Err(m) => {
                eprintln!("burrmill: {m}\n{USAGE}");
                ExitCode::from(2)
            }
        };
    }
    let a = match parse(&argv) {
        Ok(a) => a,
        Err(m) => {
            if !m.is_empty() {
                eprintln!("burrmill: {m}");
            }
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(&a) {
        Ok(()) => ExitCode::SUCCESS,
        Err(m) => {
            eprintln!("burrmill: {m}");
            ExitCode::FAILURE
        }
    }
}
