//! DataFusion-hosted SQL over a nest, component crates plus an owned planner (roadmap 6.0 C).
//!
//! Off unless the `datafusion` feature is enabled. The default `burrmill` graph stays free of it.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion_catalog::view::ViewTable;
use datafusion_catalog::{MemTable, Session, TableProvider};
use datafusion_sql::parser::Statement as DfStatement;
use datafusion_sql::planner::SqlToRel;
use sqlparser::ast::Statement as SqlStatement;

use crate::error::{BurrmillError, Result};
use crate::limits::Limits;

mod addresses;
mod cancel;
mod catalog;
mod checked;
mod compactviews;
mod heldinput;
pub use textfn::TextFunction;
mod constants;
mod correlate;
mod depth;
mod dialect;
mod latest;
mod narrow;
mod nullsub;
mod onerow;
mod outerorder;
pub use depth::check_expr_bounds;
mod buildside;
mod distinct;
mod distinctrows;
mod doubles;
mod duckfns;
pub mod encode;
mod errors;
mod extreme;
mod fastcast;
mod floatsum;
mod fold;
mod host;
mod joinfilters;
mod lastnonnull;
mod lists;
mod names;
mod ordered_agg;
mod printf;
mod rangebounds;
mod rangejoin;
mod rule;
mod sequential;
mod session;
mod sharing;
mod smallinputs;
mod tables;
pub use tables::duckdb_type;
mod subqueries;
mod textfn;
mod tojson;
mod topkfilter;
mod topn;
mod wide;

#[path = "generated/physical_planner.rs"]
mod physical_planner;
mod running;
#[path = "generated/schema_equivalence.rs"]
mod schema_equivalence;

use catalog::{NestTable, SegmentTable, apply_schema_json, discover_tables};
use session::MiniSession;

/// What one engine may use, as a host gives DuckDB `max_memory`, `threads` and `temp_directory`.
#[derive(Debug, Clone)]
pub struct Budget {
    /// Working memory of every statement and the footer cache together.
    pub memory_bytes: usize,
    /// Planning partitions, fold workers and the runtime's worker threads.
    pub threads: usize,
    /// Where operators that can spill may write, and how much. `None` refuses over the bound instead.
    pub spill: Option<(std::path::PathBuf, u64)>,
}

/// A memory pool several engines draw on, so one budget bounds their statements together rather
/// than each of them (nuthatch #1792). Every engine still keeps its own footer cache outside it.
#[derive(Debug, Clone)]
pub struct SharedPool(Arc<dyn datafusion_execution::memory_pool::MemoryPool>);

impl SharedPool {
    /// The pool a budget of `memory_bytes` gives its statements, as one budgeted engine builds.
    pub fn new(memory_bytes: usize) -> Self {
        Self(session::budget_pool(memory_bytes))
    }

    /// Bytes the statements of every engine on it hold.
    pub fn reserved(&self) -> usize {
        self.0.reserved()
    }
}

/// Concrete engine: SQL in, RecordBatches out. Generics stay inside this crate.
pub struct Engine {
    /// Taken only by `Drop`. A tab has no runtime; its event loop drives `sql_async`.
    #[cfg(not(target_arch = "wasm32"))]
    rt: Option<tokio::runtime::Runtime>,
    session: MiniSession,
    /// Partitions for a table the host registers later (`host.rs`); the ones opened here use it too.
    threads: usize,
    /// First-come-first-served admission, as on the owned path (roadmap 5.3). Without it, 32
    /// clients sharing one runtime starved one of them outright (roadmap 6.8).
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    gate: crate::gate::Gate,
    /// Set from another thread to stop the statement in flight at its next batch; armed again when
    /// the next statement starts, as DuckDB's interrupt handle behaves.
    cancel: crate::CancelToken,
    /// Inside a host transaction: what each name it has touched was bound to before it.
    txn: Option<HashMap<String, Option<Arc<dyn datafusion_expr::TableSource>>>>,
    /// Each segment's columns, as `register_facts` read them from its footer. Sealed segments never
    /// change, so once.
    bound_segments: HashMap<(std::path::PathBuf, u64), arrow::datatypes::SchemaRef>,
}

fn nest_tables(root: &Path) -> Result<Vec<NestTable>> {
    let mut tables = discover_tables(&root.join("segments"))?;
    let schema = root.join("schema.json");
    if schema.is_file() {
        apply_schema_json(&mut tables, &schema)?;
    }
    if tables.is_empty() {
        return Err(BurrmillError::NoSegments(format!(
            "no tables under {}",
            root.display()
        )));
    }
    Ok(tables)
}

impl Engine {
    /// Open every table under a `segments/` directory.
    pub fn open_segments(segments: &Path) -> Result<Self> {
        let tables = discover_tables(segments)?;
        if tables.is_empty() {
            return Err(BurrmillError::NoSegments(format!(
                "no parquet tables under {}",
                segments.display()
            )));
        }
        Self::from_tables(tables, Limits::default().max_threads, None, None)
    }

    /// Open a nest root (`segments/` plus optional `schema.json` for unsealed and `_dec`).
    pub fn open_nest(root: &Path) -> Result<Self> {
        Self::from_tables(
            nest_tables(root)?,
            Limits::default().max_threads,
            None,
            None,
        )
    }

    /// As `open_nest`, held to `budget`: the session a host opens, batch size included.
    pub fn open_nest_budgeted(root: &Path, budget: Budget) -> Result<Self> {
        Self::from_tables(nest_tables(root)?, budget.threads, Some(budget), None)
    }

    /// Open a nest held in an object store, `<root>/segments/` plus an optional `<root>/schema.json`:
    /// a bucket, a mirror over HTTP, or bytes a tab already holds. Footers are read by range.
    pub async fn open_store(
        store: Arc<dyn object_store::ObjectStore>,
        root: &str,
        threads: usize,
    ) -> Result<Self> {
        let sub = |e: object_store::Error| BurrmillError::Substrate(e.to_string());
        let root = object_store::path::Path::parse(root)
            .map_err(|e| BurrmillError::Substrate(e.to_string()))?;
        let mut tables = catalog::discover_store_tables(&store, &root).await?;
        match object_store::ObjectStoreExt::get(&store, &root.clone().join("schema.json")).await {
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
        let engine = Self::from_tables(tables, threads, None, None)?;
        engine.attach_store(store)?;
        Ok(engine)
    }

    /// The store that [`Engine::register_store_facts`] reads segments from, and any table opened
    /// by [`Engine::open_store`]. Replaces an earlier one.
    pub fn attach_store(&self, store: Arc<dyn object_store::ObjectStore>) -> Result<()> {
        let url = datafusion_execution::object_store::ObjectStoreUrl::parse(catalog::NEST_STORE)
            .map_err(df_err)?;
        self.session
            .runtime_env()
            .register_object_store(url.as_ref(), store);
        Ok(())
    }

    fn store(&self) -> Result<Arc<dyn object_store::ObjectStore>> {
        let url = datafusion_execution::object_store::ObjectStoreUrl::parse(catalog::NEST_STORE)
            .map_err(df_err)?;
        self.session
            .runtime_env()
            .object_store(&url)
            .map_err(|_| BurrmillError::Substrate("no store attached".into()))
    }

    fn from_tables(
        tables: Vec<NestTable>,
        threads: usize,
        budget: Option<Budget>,
        shared: Option<&SharedPool>,
    ) -> Result<Self> {
        let threads = budget.as_ref().map_or(threads, |b| b.threads.max(1));
        // One partition, so no operator repartitions and nothing spawns a task.
        #[cfg(target_arch = "wasm32")]
        let threads = {
            let _ = threads;
            1
        };
        #[cfg(not(target_arch = "wasm32"))]
        let rt = {
            let mut rt = tokio::runtime::Builder::new_multi_thread();
            if budget.is_some() {
                rt.worker_threads(threads);
            }
            rt.enable_all()
                .build()
                .map_err(|e| BurrmillError::Substrate(e.to_string()))?
        };
        let mut fold_tables = std::collections::HashMap::new();
        // The owned fold opens segment files itself, so a table in a store has none; the
        // substitution then declines and DataFusion's aggregate runs instead.
        for t in tables.iter().filter(|t| !t.files.is_empty() && !t.keyed) {
            let name = if t.wide.is_empty() {
                t.name.clone()
            } else {
                format!("{}__raw", t.name)
            };
            let files = t.files.iter().map(|(p, _)| p.clone());
            fold_tables.insert(
                name.clone(),
                crate::segment::SealedSegments::from_files(name, files),
            );
        }
        #[cfg(not(target_arch = "wasm32"))]
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads.max(1))
                .thread_name(|i| format!("burrmill-fold-{i}"))
                .build()
                .map_err(|e| BurrmillError::Substrate(e.to_string()))?,
        );
        #[cfg(target_arch = "wasm32")]
        let pool = tab_pool()?;
        let fold = fold::FoldTables {
            tables: Arc::new(fold_tables),
            pool,
        };
        let cancel = crate::CancelToken::new();
        let mut session = MiniSession::new(threads, fold, budget.as_ref(), shared, cancel.clone())
            .map_err(df_err)?;
        let groups = threads.max(1);
        for t in &tables {
            let provider: Arc<dyn TableProvider> = if t.files.is_empty() {
                Arc::new(MemTable::try_new(t.schema.clone(), vec![vec![]]).map_err(df_err)?)
            } else {
                Arc::new(SegmentTable::new(t, groups, cancel.clone())?)
            };
            if t.wide.is_empty() {
                session.note_addresses(&t.name, t.lowercase.iter().cloned());
                session.register_table(&t.name, provider);
            } else {
                let raw = format!("{}__raw", t.name);
                session.note_addresses(&raw, t.lowercase.iter().cloned());
                session.register_table(&raw, provider);
                let dec: String = t
                    .wide
                    .iter()
                    .map(|c| {
                        format!(
                            ", TRY_CAST(\"{c}\" AS DECIMAL(38,0)) AS \"{c}_dec\", \
                             (\"{c}\" IS NOT NULL AND TRY_CAST(\"{c}\" AS DECIMAL(38,0)) IS NULL) \
                             AS \"{c}_overflow\""
                        )
                    })
                    .collect();
                let sql = format!("SELECT *{dec} FROM \"{raw}\"");
                let logical = plan_query(&session, &sql)?;
                session.register_table(&t.name, Arc::new(ViewTable::new(logical, Some(sql))));
            }
        }
        session
            .build_information_schema(|n| n.ends_with("__raw"))
            .map_err(df_err)?;
        let gate = crate::gate::Gate::new(crate::default_width(threads.max(1)));
        Ok(Self {
            #[cfg(not(target_arch = "wasm32"))]
            rt: Some(rt),
            session,
            threads: threads.max(1),
            gate,
            cancel,
            txn: None,
            bound_segments: Default::default(),
        })
    }

    /// Define a view over the nest's tables and earlier views, as nuthatch defines its authored
    /// ones: `body` is the query after `AS`.
    pub fn register_view(&mut self, name: &str, body: &str) -> Result<()> {
        self.refuse_replacing_a_table(name)?;
        let logical = plan_query(&self.session, body)?;
        self.session.register_table(
            name,
            Arc::new(ViewTable::new(logical, Some(body.to_string()))),
        );
        self.session
            .build_information_schema(|n| n.ends_with("__raw"))
            .map_err(df_err)
    }

    /// A view replaces a view, as `CREATE OR REPLACE VIEW` does, but not a table: DuckDB refuses that,
    /// and a host that degrades the one view rather than lose the table depends on the refusal.
    fn refuse_replacing_a_table(&self, name: &str) -> Result<()> {
        use datafusion_catalog::default_table_source::source_as_provider;
        let table = self
            .session
            .table_source(name)
            .and_then(|s| source_as_provider(&s).ok())
            .is_some_and(|p| p.is::<datafusion_catalog::MemTable>());
        if table {
            return Err(BurrmillError::Plan(format!(
                "Catalog Error: Existing object {name} is of type Table, trying to replace with type View"
            )));
        }
        Ok(())
    }

    /// A host's own scalar function, as nuthatch registers `nuthatch_abi_tuple` and its kind into
    /// DuckDB. It is the host's to audit (`df_functions` covers only what Burrmill registers), and a
    /// function returning an integer or decimal type is refused by the checked rule unless it can
    /// be shown not to overflow, so hosts return text or check their own arithmetic.
    pub fn register_scalar_udf(&mut self, f: Arc<datafusion_expr::ScalarUDF>) {
        self.session.register_udf(f);
    }

    /// A host's text function under `name`: `arity` text arguments, one text result, NULL in any
    /// argument gives NULL, and `f`'s error refuses the statement.
    pub fn register_text_function(&mut self, name: &str, arity: usize, f: TextFunction) {
        self.session
            .register_udf(textfn::TextFn::udf(name, arity, f));
    }

    /// Every scalar, aggregate and window function a statement can call, sorted.
    pub fn function_names(&self) -> Vec<String> {
        self.session.function_names()
    }

    pub fn tables(&self) -> Vec<String> {
        self.session.table_names()
    }

    /// Run a query. DDL, DML, COPY, and table functions are refused before they plan.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        self.sql_for_each(sql, |b| {
            out.push(b);
            Ok(())
        })?;
        Ok(out)
    }

    /// A handle that stops the statement in flight, from any thread, at its next batch boundary.
    /// It stays armed until the host calls [`crate::CancelToken::reset`].
    /// A DataFusion join does not yield inside itself (apache/datafusion#19358), so the delay is
    /// bounded by one operator's work, not by one batch; the caller's deadline still stands.
    pub fn cancel_token(&self) -> crate::CancelToken {
        self.cancel.clone()
    }

    /// Bytes the statements in flight hold against the engine's memory pool.
    pub fn memory_reserved(&self) -> usize {
        self.session.runtime_env().memory_pool.reserved()
    }

    /// Bytes of Parquet footers the engine keeps between statements, outside the pool: at most an
    /// eighth of a budget, which the pool was sized without.
    pub fn footer_cache_bytes(&self) -> usize {
        self.session.footer_bytes()
    }

    /// Tasks alive on the engine's runtime. A cancelled statement returns before its partition tasks
    /// stop, each at its next yield, and its memory goes back as they do: zero here is when it has.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn tasks_alive(&self) -> usize {
        self.runtime().metrics().num_alive_tasks()
    }

    /// The most the statements have held against a budgeted engine's pool since the last call, and
    /// zero without a budget.
    pub fn take_memory_peak(&self) -> usize {
        let pool = &self.session.runtime_env().memory_pool;
        match datafusion_execution::memory_pool::PeakRecordingPool::from_pool(pool.as_ref()) {
            Some(p) => {
                let peak = p.peak_reserved();
                p.reset_peak();
                peak
            }
            None => 0,
        }
    }
}

impl Engine {
    /// Run a query and hand each batch to `f` as it is produced, holding none of them.
    ///
    /// `sql` collects; a million-row answer is then a million rows live at once, which is the
    /// caller's memory and not the engine's. An encoder writing to a socket wants this instead.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn sql_for_each(
        &self,
        sql: &str,
        mut f: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        // Parsed first, so malformed SQL is a syntax error as DuckDB reports it, not a refusal.
        refuse_hidden_names(sql)?;
        let logical = plan_query(&self.session, sql)?;
        refuse_non_query(sql)?;
        // The host arms the token and clears it. Clearing it here would run a statement whose
        // cancel arrived before it, or while it was still being planned.
        if self.cancel.is_cancelled() {
            return Err(BurrmillError::Cancelled);
        }
        let _pass = self.gate.enter();
        if tokio::runtime::Handle::try_current().is_err() {
            return self.runtime().block_on(self.stream(logical, &mut f));
        }
        // A host's runtime drives this thread and tokio will not block it, so the statement runs on
        // a thread of its own; its batches come back here, and `f` stays on the caller's thread.
        std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let run = s.spawn(move || {
                let mut send = |b| tx.send(b).map_err(|_| BurrmillError::Cancelled);
                self.runtime().block_on(self.stream(logical, &mut send))
            });
            // Refused by `f`: `rx` goes with the loop, the next send fails and the statement ends.
            let taken = rx.into_iter().try_for_each(&mut f);
            let run = run.join().unwrap_or_else(|p| std::panic::resume_unwind(p));
            taken.and(run)
        })
    }

    /// Run a query on the caller's event loop, for a host with one thread and no runtime: a
    /// browser tab. The plan runs in one partition, so nothing in it spawns a task.
    #[cfg(target_arch = "wasm32")]
    pub async fn sql_async(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        self.sql_async_for_each(sql, |b| {
            out.push(b);
            Ok(())
        })
        .await?;
        Ok(out)
    }

    /// As [`Engine::sql_for_each`], on the caller's event loop: each batch to `f` as it is made, so
    /// a tab holding a million-row answer holds it once, in whatever `f` makes of it.
    #[cfg(target_arch = "wasm32")]
    pub async fn sql_async_for_each(
        &self,
        sql: &str,
        mut f: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        refuse_hidden_names(sql)?;
        let logical = plan_query(&self.session, sql)?;
        refuse_non_query(sql)?;
        if self.cancel.is_cancelled() {
            return Err(BurrmillError::Cancelled);
        }
        self.stream(logical, &mut f).await
    }

    async fn stream(
        &self,
        logical: datafusion_expr::LogicalPlan,
        f: &mut impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        use futures::StreamExt;
        let physical = self
            .session
            .create_physical_plan(&logical)
            .await
            .map_err(plan_err)?;
        // A host sees an address as the text it wrote.
        let physical = addresses::rendered(physical, &self.session).map_err(df_err)?;
        let mut stream =
            sequential::execute_stream(physical, self.session.task_ctx()).map_err(df_err)?;
        // A plan with no nest scan (`range`, a recursive CTE) never meets `CancelExec`, so the
        // token is also raced here; dropping the stream aborts the partition tasks under it.
        // A tab has no timer to poll it on; there the token is seen at each batch instead.
        #[cfg(not(target_arch = "wasm32"))]
        let mut stopped = std::pin::pin!(async {
            while !self.cancel.is_cancelled() {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        });
        #[cfg(target_arch = "wasm32")]
        let mut stopped = std::pin::pin!(futures::future::pending::<()>());
        loop {
            let b = match futures::future::select(stream.next(), stopped.as_mut()).await {
                futures::future::Either::Left((Some(b), _)) => b,
                futures::future::Either::Left((None, _)) => break,
                futures::future::Either::Right(_) => return Err(BurrmillError::Cancelled),
            };
            if self.cancel.is_cancelled() {
                return Err(BurrmillError::Cancelled);
            }
            let b = match b {
                Ok(b) => b,
                // A scan stopped by the token surfaces here as its error; say what it was.
                Err(_) if self.cancel.is_cancelled() => return Err(BurrmillError::Cancelled),
                Err(e) => return Err(df_err(e)),
            };
            f(dialect::strip_dup_suffix(b))?;
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn runtime(&self) -> &tokio::runtime::Runtime {
        self.rt.as_ref().expect("taken only by drop")
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for Engine {
    fn drop(&mut self) {
        // From a thread a host's runtime drives, tokio refuses to wait for our workers to stop.
        if let Some(rt) = self.rt.take()
            && tokio::runtime::Handle::try_current().is_ok()
        {
            rt.shutdown_background();
        }
    }
}

impl Engine {
    /// Parquet scans in the statement's physical plan, for a host's admission bound (nuthatch
    /// RFC-0048). An operator not listed refuses, and so does any that can run its input more than
    /// once: a nested-loop join, and a recursive query, which has no static scan count at all.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn parquet_scans(&self, sql: &str) -> Result<u64> {
        refuse_hidden_names(sql)?;
        let logical = plan_query(&self.session, sql)?;
        refuse_non_query(sql)?;
        let plan = || {
            self.runtime()
                .block_on(self.session.create_physical_plan(&logical))
        };
        let physical = if tokio::runtime::Handle::try_current().is_err() {
            plan()
        } else {
            std::thread::scope(|s| {
                s.spawn(plan)
                    .join()
                    .unwrap_or_else(|p| std::panic::resume_unwind(p))
            })
        }
        .map_err(plan_err)?;
        scans(&physical)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn scans(p: &Arc<dyn datafusion_physical_plan::ExecutionPlan>) -> Result<u64> {
    use datafusion_datasource::file_scan_config::FileScanConfig;
    use datafusion_datasource::source::DataSourceExec;
    // `SortExec(TopK)` is a sort with a fetch: the name carries a mode after the operator.
    let own = match p.name().split('(').next().unwrap_or_default() {
        "DataSourceExec" => u64::from(
            p.downcast_ref::<DataSourceExec>()
                .is_some_and(|d| d.data_source().downcast_ref::<FileScanConfig>().is_some()),
        ),
        "OwnedSignedFoldExec" => 1,
        "ProjectionExec"
        | "FilterExec"
        | "HashJoinExec"
        | "SortMergeJoinExec"
        | "CrossJoinExec"
        | "AggregateExec"
        | "SortExec"
        | "SortPreservingMergeExec"
        | "GlobalLimitExec"
        | "LocalLimitExec"
        | "UnionExec"
        | "InterleaveExec"
        | "BoundedWindowAggExec"
        | "WindowAggExec"
        | "CoalesceBatchesExec"
        | "CoalescePartitionsExec"
        | "RepartitionExec"
        | "UnnestExec"
        | "EmptyExec"
        | "PlaceholderRowExec"
        | "ScalarSubqueryExec"
        | "CancelExec"
        | "CompactViewsExec"
        | "CompactHeldExec"
        | "CastViewsExec"
        | "ChargedWindowExec"
        | "CoalesceExec"
        | "GatherExec"
        | "RangeJoinExec"
        | "SharedExec"
        | "SharedDefsExec" => 0,
        other => {
            return Err(BurrmillError::NotAllowed(format!(
                "cannot bound physical plan operator {other:?}"
            )));
        }
    };
    p.children()
        .into_iter()
        .try_fold(own, |n, c| Ok(n.saturating_add(scans(c)?)))
}

/// The planner recurses. The caller's stack is whatever the host gave its worker, and a stack
/// overflow aborts the process. What the depth bound let through is planned on a stack of a known size.
#[cfg(not(target_arch = "wasm32"))]
const PLAN_STACK_BYTES: usize = 8 * 1024 * 1024;

/// A tab has no second thread; its one stack is sized when the module links, to at least this.
#[cfg(target_arch = "wasm32")]
fn plan_query(session: &MiniSession, sql: &str) -> Result<datafusion_expr::LogicalPlan> {
    plan_query_on_stack(session, sql)
}

#[cfg(not(target_arch = "wasm32"))]
fn plan_query(session: &MiniSession, sql: &str) -> Result<datafusion_expr::LogicalPlan> {
    let owned = sql.to_string();
    std::thread::scope(|scope| {
        let handle = std::thread::Builder::new()
            .name("burrmill-plan".into())
            .stack_size(PLAN_STACK_BYTES)
            .spawn_scoped(scope, || plan_query_on_stack(session, &owned))
            .map_err(|e| BurrmillError::Substrate(format!("planner thread: {e}")))?;
        handle
            .join()
            .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
    })
}

fn plan_query_on_stack(session: &MiniSession, sql: &str) -> Result<datafusion_expr::LogicalPlan> {
    #[cfg(not(target_arch = "wasm32"))]
    debug_assert_eq!(std::thread::current().name(), Some("burrmill-plan"));
    let (stmt, names) = dialect::parse(sql, &session.known_names())?;
    refuse_df_statement(&stmt)?;
    refuse_wide_literals(&stmt)?;
    let planner = SqlToRel::new(session);
    let plan = planner.statement_to_plan(stmt).map_err(plan_err)?;
    rename(plan, &names)
}

/// DuckDB's default names on the unaliased columns, by position, and the private suffixes of
/// `dialect` taken off wherever the plain name is unique. A name that stays repeated keeps its
/// suffix until the result batches, which may repeat names where a plan may not.
fn rename(
    plan: datafusion_expr::LogicalPlan,
    names: &[Option<String>],
) -> Result<datafusion_expr::LogicalPlan> {
    use datafusion_expr::{Expr, LogicalPlanBuilder};
    let schema = plan.schema().clone();
    let n = schema.fields().len();
    let by_position = names.len() == n;
    let suffixed = schema
        .fields()
        .iter()
        .any(|f| f.name().contains(dialect::DUP) || f.name().ends_with(']'));
    if !suffixed && (!by_position || names.iter().all(Option::is_none)) {
        return Ok(plan);
    }
    // DataFusion names a struct field `s[a]` (`s[n][k]` nested); DuckDB names it `a` (`k`).
    let field_access = |name: &str| {
        name.strip_suffix(']')
            .and_then(|n| n.rfind('[').map(|i| n[i + 1..].to_string()))
            .filter(|k| !k.is_empty())
    };
    let wanted: Vec<String> = (0..n)
        .map(|i| match names.get(i).filter(|_| by_position) {
            Some(Some(name)) => name.clone(),
            _ => {
                let own = schema.field(i).name();
                field_access(own).unwrap_or_else(|| own.clone())
            }
        })
        .collect();
    let base = |s: &str| s.split_once(dialect::DUP).map_or(s, |(b, _)| b).to_string();
    let finals: Vec<String> = wanted
        .iter()
        .map(|w| {
            let b = base(w);
            let clash = wanted.iter().filter(|o| base(o) == b).count() > 1;
            if clash { w.clone() } else { b }
        })
        .collect();
    let mut seen = std::collections::HashSet::new();
    if !finals.iter().all(|f| seen.insert(f.as_str())) {
        return Ok(plan);
    }
    if (0..n).all(|i| finals[i] == *schema.field(i).name()) {
        return Ok(plan);
    }
    let exprs: Vec<Expr> = (0..n)
        .map(|i| {
            let col = Expr::Column(datafusion_common::Column::from(schema.qualified_field(i)));
            if finals[i] == *schema.field(i).name() {
                col
            } else {
                col.alias(finals[i].as_str())
            }
        })
        .collect();
    LogicalPlanBuilder::from(plan)
        .project(exprs)
        .and_then(|b| b.build())
        .map_err(df_err)
}

/// The first keyword, past whitespace, opening parentheses and comments: `(SELECT ...) UNION ...`
/// and a statement opening with `-- note` are queries too.
fn leading_keyword(sql: &str) -> &str {
    let mut s = sql;
    loop {
        let t = s.trim_start_matches(|c: char| c.is_whitespace() || c == '(');
        if let Some(rest) = t.strip_prefix("--") {
            s = rest.split_once('\n').map_or("", |(_, r)| r);
        } else if let Some(rest) = t.strip_prefix("/*") {
            s = rest.split_once("*/").map_or("", |(_, r)| r);
        } else {
            return t;
        }
    }
}

/// `__raw`, `__hot` and `__union` are refused when the walk can see them. A statement it cannot
/// parse is left to the engine parser, which accepts more of DuckDB than the walk does (#10).
fn refuse_hidden_names(sql: &str) -> Result<()> {
    match crate::inspect::reach(sql) {
        Err(BurrmillError::NotAllowed(why))
            if why.contains("is not a table this surface serves") =>
        {
            Err(BurrmillError::NotAllowed(why))
        }
        _ => Ok(()),
    }
}

fn refuse_non_query(sql: &str) -> Result<()> {
    let trimmed = leading_keyword(sql);
    let head = trimmed
        .chars()
        .take_while(|c| c.is_ascii_alphabetic() || *c == '_')
        .collect::<String>()
        .to_ascii_uppercase();
    match head.as_str() {
        "SELECT" | "WITH" | "VALUES" | "EXPLAIN" | "FROM" => Ok(()),
        other => Err(BurrmillError::NotAllowed(format!(
            "only SELECT/WITH is admitted, not `{other}`"
        ))),
    }
}

fn refuse_df_statement(stmt: &DfStatement) -> Result<()> {
    match stmt {
        DfStatement::Statement(s) => refuse_sql_statement(s),
        DfStatement::CreateExternalTable(_) => Err(BurrmillError::NotAllowed(
            "CREATE EXTERNAL TABLE is not in the grammar we expose".into(),
        )),
        DfStatement::CopyTo(_) => Err(BurrmillError::NotAllowed(
            "COPY is not in the grammar we expose".into(),
        )),
        DfStatement::Explain(e) => refuse_df_statement(&e.statement),
        DfStatement::Reset(_) => Err(BurrmillError::NotAllowed(
            "RESET is not in the grammar we expose".into(),
        )),
    }
}

fn refuse_sql_statement(stmt: &SqlStatement) -> Result<()> {
    match stmt {
        SqlStatement::Query(_) => Ok(()),
        SqlStatement::Explain { statement, .. } => refuse_sql_statement(statement),
        other => Err(BurrmillError::NotAllowed(format!(
            "statement not admitted: {}",
            stmt_kind(other)
        ))),
    }
}

/// An integer literal past u64 parses as Float64 before any plan rule can see it, and a float
/// cannot hold it exactly.
fn refuse_wide_literals(stmt: &DfStatement) -> Result<()> {
    use sqlparser::ast::{Expr as SqlExpr, Value};
    use std::ops::ControlFlow;
    let s = match stmt {
        DfStatement::Statement(s) => s,
        DfStatement::Explain(e) => return refuse_wide_literals(&e.statement),
        _ => return Ok(()),
    };
    let mut found = None;
    let _ = crate::walk::exprs(s.as_ref(), |e| {
        if let SqlExpr::Value(v) = e
            && let Value::Number(n, _) = &v.value
            && n.bytes().all(|b| b.is_ascii_digit())
            && n.parse::<u64>().is_err()
        {
            found = Some(n.clone());
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    match found {
        Some(n) => Err(BurrmillError::NotAllowed(format!(
            "integer literal {n} is wider than 64 bits and would be read as a float; \
             write CAST('{n}' AS DECIMAL(38,0))"
        ))),
        None => Ok(()),
    }
}

fn stmt_kind(s: &SqlStatement) -> &'static str {
    match s {
        SqlStatement::Insert { .. } => "INSERT",
        SqlStatement::CreateTable { .. } => "CREATE TABLE",
        SqlStatement::CreateView { .. } => "CREATE VIEW",
        SqlStatement::Copy { .. } => "COPY",
        SqlStatement::Delete { .. } => "DELETE",
        SqlStatement::Update { .. } => "UPDATE",
        SqlStatement::Drop { .. } => "DROP",
        _ => "non-query",
    }
}

/// `df_err` for a failure before execution: what would be a substrate error is the plan's.
fn plan_err(e: datafusion_common::DataFusionError) -> BurrmillError {
    match df_err(e) {
        BurrmillError::Substrate(s) => BurrmillError::Plan(s),
        e => e,
    }
}

fn df_err(e: datafusion_common::DataFusionError) -> BurrmillError {
    let s = errors::restate(e.to_string());
    if s.contains("not yet implemented")
        || (s.contains("table function ") && s.contains("is not supported here"))
    {
        BurrmillError::NotAllowed(s)
    } else if s.contains("no table") {
        BurrmillError::NoSegments(s)
    } else {
        BurrmillError::Substrate(s)
    }
}

/// A tab cannot spawn a thread, so the one it has is the pool, and rayon lets a thread join only
/// one: every engine in the tab shares it.
#[cfg(target_arch = "wasm32")]
fn tab_pool() -> Result<Arc<rayon::ThreadPool>> {
    thread_local! {
        static POOL: std::cell::OnceCell<Arc<rayon::ThreadPool>> = const { std::cell::OnceCell::new() };
    }
    POOL.with(|p| {
        if let Some(pool) = p.get() {
            return Ok(Arc::clone(pool));
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .use_current_thread()
            .build()
            .map_err(|e| BurrmillError::Substrate(e.to_string()))?;
        Ok(Arc::clone(p.get_or_init(|| Arc::new(pool))))
    })
}
