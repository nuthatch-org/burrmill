//! One way to open an engine: what it reads, what it may use, and whose pool it draws on.
//!
//! The `open_*` constructors are each one combination of these, kept for the hosts that call them.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::catalog::{self, NestTable, apply_schema_json, discover_tables};
use super::{Budget, Engine, SharedPool};
use crate::error::{BurrmillError, Result};
use crate::limits::Limits;

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
    threads: usize,
    budget: Option<Budget>,
    pool: Option<SharedPool>,
}

impl Engine {
    /// An engine opened from what the builder is given.
    pub fn builder() -> EngineBuilder {
        EngineBuilder {
            source: Source::Empty,
            threads: Limits::default().max_threads,
            budget: None,
            pool: None,
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

    /// Open a local or empty source. A store source refuses: it opens with
    /// [`EngineBuilder::open_async`].
    pub fn open(self) -> Result<Engine> {
        let tables = match &self.source {
            Source::Empty => Vec::new(),
            Source::Segments(dir) => segment_tables(dir)?,
            Source::Nest(root) => nest_tables(root)?,
            Source::Store(..) => {
                return Err(BurrmillError::Config(
                    "an engine over a store opens with open_async".into(),
                ));
            }
        };
        self.open_over(tables)
    }

    /// Open any source, listing a store's segments and reading its `schema.json` first.
    pub async fn open_async(mut self) -> Result<Engine> {
        let Source::Store(store, root) = std::mem::replace(&mut self.source, Source::Empty) else {
            return self.open();
        };
        let tables = store_tables(&store, &root).await?;
        let engine = self.open_over(tables)?;
        engine.attach_store(store)?;
        Ok(engine)
    }

    fn open_over(self, tables: Vec<NestTable>) -> Result<Engine> {
        // Silently unbounded is worse than refused: the host asked for its statements to be held.
        if self.pool.is_some() && self.budget.is_none() {
            return Err(BurrmillError::Config(
                "a shared pool needs a budget beside it".into(),
            ));
        }
        Engine::from_tables(tables, self.threads, self.budget, self.pool.as_ref())
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
