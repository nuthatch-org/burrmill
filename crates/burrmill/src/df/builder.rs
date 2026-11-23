//! One way to open an engine: what it reads, what it may use, and whose pool it draws on.
//!
//! The `open_*` constructors are each one combination of these, kept for the hosts that call them.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::catalog::{self, NestTable, apply_schema_json, discover_tables};
use super::textfiles::{self, TextFormat, TextTable};
use super::{Budget, Engine, SharedPool};
use crate::error::{BurrmillError, Result};
use crate::limits::Limits;

enum Parquet {
    Local(PathBuf, String),
    Store(Arc<dyn object_store::ObjectStore>, String, String),
}

enum Source {
    Empty,
    Segments(PathBuf),
    Nest(PathBuf),
    Store(Arc<dyn object_store::ObjectStore>, String),
}

/// What [`Engine::builder`] returns. With no source the engine opens with no tables, for a host to
/// register its own; with no budget it is unbounded, at [`Limits`]'s threads unless set.
pub struct EngineBuilder {
    source: Source,
    parquet: Vec<Parquet>,
    text: Vec<(PathBuf, String, TextFormat)>,
    threads: usize,
    budget: Option<Budget>,
    pool: Option<SharedPool>,
    small_bytes: Option<u64>,
}

impl Engine {
    /// An engine opened from what the builder is given.
    pub fn builder() -> EngineBuilder {
        EngineBuilder {
            source: Source::Empty,
            parquet: Vec::new(),
            text: Vec::new(),
            threads: Limits::default().max_threads,
            budget: None,
            pool: None,
            small_bytes: None,
        }
    }

    /// Open every table under a `segments/` directory. Files are read as `<table>-<hash>.parquet`:
    /// the text before the last hyphen names the table (quote it in SQL if it holds one), and a
    /// `.parquet` file with no hyphen is skipped.
    pub fn open_segments(segments: &Path) -> Result<Self> {
        Self::builder().segments(segments).open()
    }

    /// Open a nest root (`segments/` plus optional `schema.json` for unsealed and `_dec`).
    pub fn open_nest(root: &Path) -> Result<Self> {
        Self::builder().nest(root).open()
    }

    /// As `open_nest`, held to `budget`: the session a host opens, batch size included.
    pub fn open_nest_budgeted(root: &Path, budget: Budget) -> Result<Self> {
        Self::builder().nest(root).budget(budget).open()
    }

    /// Open a nest held in an object store, `<root>/segments/` plus an optional `<root>/schema.json`:
    /// a bucket, a mirror over HTTP, or bytes a tab already holds. Footers are read by range.
    pub async fn open_store(
        store: Arc<dyn object_store::ObjectStore>,
        root: &str,
        threads: usize,
    ) -> Result<Self> {
        Self::builder()
            .store(store, root)
            .threads(threads)
            .open_async()
            .await
    }
}

impl EngineBuilder {
    /// Every table under a `segments/` directory, read as `<table>-<hash>.parquet`: the text before
    /// the last hyphen names the table, and a `.parquet` file with no hyphen is skipped.
    pub fn segments(mut self, dir: &Path) -> Self {
        self.source = Source::Segments(dir.to_path_buf());
        self
    }

    /// A nest root: `segments/` plus an optional `schema.json` for unsealed columns and `_dec`.
    pub fn nest(mut self, root: &Path) -> Self {
        self.source = Source::Nest(root.to_path_buf());
        self
    }

    /// A nest at `root` in an object store, which also becomes the engine's attached store. Opens
    /// with [`EngineBuilder::open_async`] only, since its listing and footers are reads.
    pub fn store(mut self, store: Arc<dyn object_store::ObjectStore>, root: &str) -> Self {
        self.source = Source::Store(store, root.to_string());
        self
    }

    /// The table `table` over a Parquet file, or every `.parquet` file beneath a directory, whatever
    /// their names. `key=value` directories become columns after the files' own, typed BIGINT, DATE
    /// or VARCHAR from the values they take; the files' columns are the first file's in path order.
    /// Beside any other source, and once per table.
    pub fn parquet(mut self, path: &Path, table: &str) -> Self {
        self.parquet
            .push(Parquet::Local(path.to_path_buf(), table.to_string()));
        self
    }

    /// As [`EngineBuilder::parquet`], over every `.parquet` key under `prefix` in `store`: a bucket
    /// on S3 or R2, or any other `object_store`. Opens with [`EngineBuilder::open_async`] only.
    pub fn parquet_store(
        mut self,
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        table: &str,
    ) -> Self {
        self.parquet
            .push(Parquet::Store(store, prefix.to_string(), table.to_string()));
        self
    }

    /// The table `table` over a CSV file, read whole into memory by `burrmill-ingest`, the browser
    /// tab's reader, which finds the delimiter, the header and each column's type as the tab does.
    /// The memory it holds comes out of the budget's, and a file that would need more than is left,
    /// or over 1 GiB, is refused. So is a file with a short row, a quote never closed, or a cell its
    /// column's type does not read, at that row (from 1, after the header) and column. A receipt
    /// names the file by its content, `csv:<sha256>:<bytes>`. Beside any other source.
    pub fn csv(mut self, path: &Path, table: &str) -> Self {
        self.text
            .push((path.to_path_buf(), table.to_string(), TextFormat::Csv));
        self
    }

    /// As [`EngineBuilder::csv`], over JSON: an array of objects, or objects one after another, as
    /// NDJSON is. Each key is a column, and a value that is an object or an array is its JSON text.
    /// Text that does not parse is refused at its line and column. A receipt names the file
    /// `json:<sha256>:<bytes>`.
    pub fn json(mut self, path: &Path, table: &str) -> Self {
        self.text
            .push((path.to_path_buf(), table.to_string(), TextFormat::Json));
        self
    }

    /// Planning partitions, fold workers and runtime threads, without a budget. A budget's own
    /// `threads` takes precedence.
    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    /// Every statement's working memory and the footer cache held under `budget`, at its threads,
    /// with DuckDB's 2,048-row vector. Over the bound a statement is refused unless it may spill.
    pub fn budget(mut self, budget: Budget) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Statements draw on `pool`, which other engines share, so one bound holds them together.
    /// Needs a budget, for the batch size and the footer cache's share.
    pub fn shared_pool(mut self, pool: &SharedPool) -> Self {
        self.pool = Some(pool.clone());
        self
    }

    /// See [`Engine::set_small_statement_bytes`].
    pub fn small_statement_bytes(mut self, bytes: u64) -> Self {
        self.small_bytes = Some(bytes);
        self
    }

    /// Open a local or empty source. A store source refuses: it opens with
    /// [`EngineBuilder::open_async`].
    pub fn open(mut self) -> Result<Engine> {
        if self.parquet.iter().any(|p| matches!(p, Parquet::Store(..))) {
            return Err(BurrmillError::Config(
                "an engine over a store opens with open_async".into(),
            ));
        }
        let mut tables = match &self.source {
            Source::Empty => Vec::new(),
            Source::Segments(dir) => segment_tables(dir)?,
            Source::Nest(root) => nest_tables(root)?,
            Source::Store(..) => {
                return Err(BurrmillError::Config(
                    "an engine over a store opens with open_async".into(),
                ));
            }
        };
        for p in &self.parquet {
            if let Parquet::Local(path, name) = p {
                tables.push(super::files::local_table(path, name)?);
            }
        }
        let text = self.read_text()?;
        self.open_over(tables, text, Vec::new())
    }

    /// Open any source, listing a store's segments and reading its `schema.json` first.
    pub async fn open_async(mut self) -> Result<Engine> {
        let nest = match std::mem::replace(&mut self.source, Source::Empty) {
            Source::Store(store, root) => Some((store_tables(&store, &root).await?, store)),
            other => {
                self.source = other;
                None
            }
        };
        let mut stores = Vec::new();
        let mut tables = Vec::new();
        for p in std::mem::take(&mut self.parquet) {
            match p {
                Parquet::Local(path, name) => tables.push(super::files::local_table(&path, &name)?),
                Parquet::Store(store, prefix, name) => {
                    // A store of its own for each, so two buckets' keys cannot meet.
                    let url = format!("parquet{}://store", stores.len());
                    tables.push(super::files::store_table(&store, &prefix, &name, &url).await?);
                    stores.push((url, store));
                }
            }
        }
        let (mut all, nest_store) = match nest {
            Some((t, store)) => (t, Some(store)),
            None => (
                match &self.source {
                    Source::Empty => Vec::new(),
                    Source::Segments(dir) => segment_tables(dir)?,
                    Source::Nest(root) => nest_tables(root)?,
                    Source::Store(..) => unreachable!("taken above"),
                },
                None,
            ),
        };
        all.extend(tables);
        let text = self.read_text()?;
        let engine = self.open_over(all, text, stores)?;
        if let Some(store) = nest_store {
            engine.attach_store(store)?;
        }
        Ok(engine)
    }

    /// Each CSV and JSON file, read in what the budget leaves after the ones before it, and the
    /// budget then left for statements.
    fn read_text(&mut self) -> Result<Vec<TextTable>> {
        let mut left = self.budget.as_ref().map(|b| b.memory_bytes as u64);
        let mut out = Vec::with_capacity(self.text.len());
        for (path, name, format) in &self.text {
            let t = textfiles::read(path, name, *format, left)?;
            left = left.map(|m| m.saturating_sub(t.held));
            out.push(t);
        }
        if let (Some(b), Some(left)) = (self.budget.as_mut(), left) {
            b.memory_bytes = left as usize;
        }
        Ok(out)
    }

    fn open_over(
        self,
        tables: Vec<NestTable>,
        text: Vec<TextTable>,
        stores: Vec<(String, Arc<dyn object_store::ObjectStore>)>,
    ) -> Result<Engine> {
        let mut names = std::collections::HashSet::new();
        let named = tables
            .iter()
            .map(|t| &t.name)
            .chain(text.iter().map(|t| &t.name));
        for name in named {
            if !names.insert(name.to_lowercase()) {
                return Err(BurrmillError::Config(format!(
                    "two tables are named {name}"
                )));
            }
        }
        // Silently unbounded is worse than refused: the host asked for its statements to be held.
        if self.pool.is_some() && self.budget.is_none() {
            return Err(BurrmillError::Config(
                "a shared pool needs a budget beside it".into(),
            ));
        }
        // A first-come pool lets one operator that could spill take what another then cannot get.
        if let (Some(b), Some(p)) = (&self.budget, &self.pool)
            && b.spill.is_some()
            && !p.spills
        {
            return Err(BurrmillError::Config(
                "a budget with a spill directory shares a pool made by SharedPool::spilling, not \
                 SharedPool::new"
                    .into(),
            ));
        }
        let mut engine = Engine::from_tables(
            tables,
            self.threads,
            self.budget,
            self.pool.as_ref(),
            stores,
        )?;
        engine.register_text(text)?;
        if let Some(bytes) = self.small_bytes {
            engine.set_small_statement_bytes(bytes);
        }
        Ok(engine)
    }
}

fn segment_tables(segments: &Path) -> Result<Vec<NestTable>> {
    let tables = discover_tables(segments)?;
    if tables.is_empty() {
        return Err(BurrmillError::NoSegments(none_found(
            "parquet tables",
            segments,
            segments,
        )));
    }
    Ok(tables)
}

fn nest_tables(root: &Path) -> Result<Vec<NestTable>> {
    let mut tables = discover_tables(&root.join("segments"))?;
    let schema = root.join("schema.json");
    if schema.is_file() {
        apply_schema_json(&mut tables, &schema)?;
    }
    if tables.is_empty() {
        return Err(BurrmillError::NoSegments(none_found(
            "tables",
            root,
            &root.join("segments"),
        )));
    }
    Ok(tables)
}

/// `no <what> under <shown>`, and when the scan skipped `.parquet` files for lacking a hyphen, which
/// they were and what name a table file takes.
fn none_found(what: &str, shown: &Path, scanned: &Path) -> String {
    let mut skipped: Vec<String> = std::fs::read_dir(scanned)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|f| f.strip_suffix(".parquet").is_some_and(|s| !s.contains('-')))
        .collect();
    skipped.sort();
    if skipped.is_empty() {
        return format!("no {what} under {}", shown.display());
    }
    let more = skipped.len().saturating_sub(5);
    skipped.truncate(5);
    format!(
        "no {what} under {}: files are read as <table>-<hash>.parquet, and {}{} have no hyphen",
        shown.display(),
        skipped.join(", "),
        if more > 0 {
            format!(" and {more} more")
        } else {
            String::new()
        }
    )
}

async fn store_tables(
    store: &Arc<dyn object_store::ObjectStore>,
    root: &str,
) -> Result<Vec<NestTable>> {
    let sub = |e: object_store::Error| BurrmillError::Substrate(e.to_string());
    let root = object_store::path::Path::parse(root)
        .map_err(|e| BurrmillError::Substrate(e.to_string()))?;
    let mut tables = catalog::discover_store_tables(store, &root).await?;
    match object_store::ObjectStoreExt::get(store, &root.clone().join("schema.json")).await {
        Ok(got) => {
            let raw = got.bytes().await.map_err(sub)?;
            let raw = std::str::from_utf8(&raw)
                .map_err(|e| BurrmillError::Substrate(format!("schema.json: {e}")))?;
            catalog::apply_schema_text(&mut tables, raw)?;
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(sub(e)),
    }
    if tables.is_empty() {
        return Err(BurrmillError::NoSegments(format!("no tables under {root}")));
    }
    Ok(tables)
}
