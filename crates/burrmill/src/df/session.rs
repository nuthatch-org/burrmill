//! Component-crate session: no umbrella `datafusion` crate, no DDL, no table functions.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion_catalog::default_table_source::provider_as_source;
use datafusion_catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList,
    MemorySchemaProvider, Session, TableProvider,
};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::{ConfigOptions, TableOptions};
use datafusion_common::display::{PlanType, ToStringifiedPlan};
use datafusion_common::{DFSchema, Result as DFResult, TableReference, plan_datafusion_err};
use datafusion_execution::TaskContext;
use datafusion_execution::cache::cache_manager::{
    CacheManagerConfig, CachedFileMetadataEntry, FileMetadataCache,
};
use datafusion_execution::cache::default_cache::DefaultCache;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::memory_pool::{
    GreedyMemoryPool, MemoryPool, PeakRecordingPool, TrackConsumersPool,
};
use datafusion_execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::registry::{
    ExtensionTypeRegistryRef, FunctionRegistry, MemoryExtensionTypeRegistry,
};
use datafusion_expr::{
    AggregateUDF, Explain, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_functions::core::planner::CoreFunctionPlanner;
use datafusion_optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction;
use datafusion_optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion_optimizer::analyzer::{Analyzer, AnalyzerRule};
use datafusion_optimizer::optimizer::{Optimizer, OptimizerConfig};
use datafusion_physical_expr::create_physical_expr;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_session::{PhysicalOptimizerRule, PhysicalPlanner, QueryPlanner};

use super::checked::{CheckedAgg, Mode};
use super::fold::{FoldSubstitution, FoldTables, OwnedFoldPlanner};
use super::physical_planner::DefaultPhysicalPlanner;
use super::rule::CheckedArithmetic;

/// Parquet footers by segment path. Segments never change once sealed, so an entry stays right.
pub(super) type Footers = DefaultCache<object_store::path::Path, CachedFileMetadataEntry>;

pub struct MiniSession {
    session_id: String,
    config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
    footers: Arc<Footers>,
    catalog_list: Arc<MemoryCatalogProviderList>,
    scalar: HashMap<String, Arc<ScalarUDF>>,
    higher: HashMap<String, Arc<HigherOrderUDF>>,
    aggregate: HashMap<String, Arc<AggregateUDF>>,
    window: HashMap<String, Arc<WindowUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
    tables: HashMap<String, Arc<dyn TableSource>>,
    known: std::sync::Mutex<Option<super::dialect::Known>>,
    /// Built on first read: rebuilding it at every registration was quadratic in the views.
    information_schema: std::sync::Mutex<Option<HashMap<String, Arc<dyn TableSource>>>>,
    hidden: fn(&str) -> bool,
    analyzer: Analyzer,
    optimizer: Optimizer,
    physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    execution_props: ExecutionProps,
    table_options: TableOptions,
    extension_types: ExtensionTypeRegistryRef,
    alias_generator: Arc<AliasGenerator>,
    query_start: DateTime<Utc>,
    query_planner: Arc<dyn QueryPlanner + Send + Sync>,
}

impl std::fmt::Debug for MiniSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MiniSession")
    }
}

/// A budget's statement pool: the bound less the eighth the footer cache keeps outside it.
pub(super) fn budget_pool(memory_bytes: usize) -> Arc<dyn MemoryPool> {
    Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(memory_bytes - memory_bytes / 8),
        NonZeroUsize::new(5).expect("five"),
    ))
}

impl MiniSession {
    pub fn new(
        threads: usize,
        fold: FoldTables,
        budget: Option<&super::Budget>,
        shared: Option<&super::SharedPool>,
        cancel: crate::CancelToken,
    ) -> DFResult<Self> {
        // The token rides in the config too, for an owned operator that loops inside one poll.
        let mut config = SessionConfig::new()
            .with_target_partitions(threads.max(1))
            .with_collect_statistics(false)
            .with_extension(Arc::new(cancel.clone()));
        config.options_mut().sql_parser.enable_ident_normalization = false;
        // DuckDB types `1.5` as DECIMAL(2,1), and nuthatch prints a DECIMAL as a string.
        config.options_mut().sql_parser.parse_float_as_decimal = true;
        // It drops the ORDER BY of every view and derived table; `OutermostOrder` keeps the one that
        // orders the answer.
        config
            .options_mut()
            .sql_parser
            .enable_subquery_sort_elimination = false;
        // Each sorting partition reserves this up front to merge its spilled runs; DataFusion's
        // 10 MB each is more than a small budget holds, and a sort that cannot reserve it refuses.
        if let Some(b) = budget {
            let share = b.memory_bytes / (8 * threads.max(1));
            config.options_mut().execution.sort_spill_reservation_bytes =
                share.clamp(256 << 10, 10 << 20);
            // 8,192 rows of a wide cell exist before the caller caps the answer (#1650).
            config = config.with_batch_size(128);
        }
        // The footer cache sits outside the pool, so under a budget it takes an eighth of the bound
        // rather than adding to it. Only `SegmentTable`'s reader factory consults it.
        let footer_limit = budget.map_or(1 << 30, |b| b.memory_bytes / 8);
        let footers = Arc::new(Footers::new(footer_limit).with_name("burrmill footers"));
        let cache = CacheManagerConfig::default()
            .with_metadata_cache_limit(footer_limit)
            .with_file_metadata_cache(Some(Arc::clone(&footers) as Arc<FileMetadataCache>));
        let runtime = match budget {
            None => RuntimeEnvBuilder::new().with_cache_manager(cache),
            // Without a spill directory, over the bound is a refusal.
            Some(b) => RuntimeEnvBuilder::new()
                .with_cache_manager(cache)
                // The peak is this engine's own; `reserved` is the shared pool's.
                .with_memory_pool(Arc::new(PeakRecordingPool::new(
                    shared.map_or_else(|| budget_pool(b.memory_bytes), |p| Arc::clone(&p.0)),
                )))
                .with_disk_manager_builder(match &b.spill {
                    None => DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
                    Some((dir, cap)) => DiskManagerBuilder::default()
                        .with_mode(DiskManagerMode::Directories(vec![dir.clone()]))
                        .with_max_temp_directory_size(*cap),
                }),
        }
        .build_arc()?;
        // Under every name DataFusion gives a function: `length` is `character_length`'s, and
        // registering primary names alone refused it.
        let mut scalar = HashMap::new();
        for f in datafusion_functions::all_default_functions() {
            for a in f.aliases() {
                scalar.insert(a.clone(), Arc::clone(&f));
            }
            scalar.insert(f.name().to_string(), f);
        }
        // The only defaults that reach outside the query (roadmap 6.2 audit, `df_functions`):
        // `input_file_name()` would hand a caller the server's segment paths.
        scalar.retain(|_, f| !matches!(f.name(), "input_file_name" | "file_row_index"));
        if let Some(inner) = scalar.remove("decode") {
            let d = super::dialect::Decode::udf(inner);
            scalar.insert(d.name().to_string(), d);
        }
        let mark = super::dialect::HugeintMark::udf();
        scalar.insert(mark.name().to_string(), mark);
        if let Some(inner) = scalar.get("to_timestamp").cloned() {
            let t = super::duckfns::ToTimestamp::udf(inner);
            scalar.retain(|_, f| f.name() != "to_timestamp");
            for a in t.aliases() {
                scalar.insert(a.clone(), Arc::clone(&t));
            }
            scalar.insert("to_timestamp".into(), t);
        }
        for f in super::duckfns::all() {
            for a in f.aliases() {
                scalar.insert(a.clone(), Arc::clone(&f));
            }
            scalar.insert(f.name().to_string(), f);
        }
        let from_hex = super::dialect::FromHex::udf();
        scalar.insert("unhex".to_string(), Arc::clone(&from_hex));
        scalar.insert(from_hex.name().to_string(), from_hex);
        let round = super::dialect::RoundInt::udf();
        scalar.insert(round.name().to_string(), round);
        let intdiv = super::dialect::IntDiv::udf();
        scalar.insert(intdiv.name().to_string(), intdiv);
        let xor = super::dialect::Xor::udf();
        scalar.insert(xor.name().to_string(), xor);
        for f in super::printf::Printf::udfs() {
            scalar.insert(f.name().to_string(), f);
        }
        let to_json = super::tojson::ToJson::udf();
        scalar.insert(to_json.name().to_string(), to_json);
        for f in datafusion_functions_nested::all_default_nested_functions() {
            let f = match f.name() {
                "array_prepend" => super::lists::ElementAndList::udf(f, true),
                "array_append" => super::lists::ElementAndList::udf(f, false),
                _ => f,
            };
            for a in f.aliases() {
                scalar.insert(a.clone(), Arc::clone(&f));
            }
            scalar.insert(f.name().to_string(), f);
        }
        let mut higher = HashMap::new();
        for f in datafusion_functions_nested::all_default_higher_order_functions()
            .into_iter()
            .chain([super::lists::ListReduce::udf()])
        {
            for a in f.aliases() {
                higher.insert(a.clone(), Arc::clone(&f));
            }
            higher.insert(f.name().to_string(), f);
        }
        let mut aggregate = HashMap::new();
        for f in datafusion_functions_aggregate::all_default_aggregate_functions() {
            let f = match f.name() {
                "sum" | "avg" => super::floatsum::ExactDoubles::udaf(f),
                _ => f,
            };
            for a in f.aliases() {
                aggregate.insert(a.clone(), Arc::clone(&f));
            }
            aggregate.insert(f.name().to_string(), f);
        }
        // DuckDB's `list(x ORDER BY k)`.
        if let Some(f) = aggregate.get("array_agg").cloned() {
            let f = super::ordered_agg::OrderedArrayAgg::udaf(f);
            for a in f.aliases() {
                aggregate.insert(a.clone(), Arc::clone(&f));
            }
            aggregate.insert("array_agg".into(), Arc::clone(&f));
            aggregate.insert("list".into(), f);
        }
        if let Some(f) = aggregate.get("string_agg").cloned() {
            let f = super::ordered_agg::SelfSortingStringAgg::udaf(f);
            for a in f.aliases() {
                aggregate.insert(a.clone(), Arc::clone(&f));
            }
            aggregate.insert("string_agg".into(), f);
        }
        let exact_text = CheckedAgg::udaf(Mode::SumText, None);
        aggregate.insert(exact_text.name().to_string(), exact_text);
        let mut window = HashMap::new();
        for f in datafusion_functions_window::all_default_window_functions() {
            for a in f.aliases() {
                window.insert(a.clone(), Arc::clone(&f));
            }
            window.insert(f.name().to_string(), f);
        }
        let expr_planners: Vec<Arc<dyn ExprPlanner>> = vec![
            Arc::new(super::dialect::DuckPlanner),
            Arc::new(CoreFunctionPlanner::default()),
            Arc::new(datafusion_functions::datetime::planner::DatetimeFunctionPlanner),
            Arc::new(datafusion_functions::unicode::planner::UnicodeFunctionPlanner),
            Arc::new(datafusion_functions_nested::planner::NestedFunctionPlanner),
            Arc::new(datafusion_functions_nested::planner::FieldAccessPlanner),
            Arc::new(datafusion_functions_aggregate::planner::AggregateFunctionPlanner),
            Arc::new(datafusion_functions_window::planner::WindowFunctionPlanner),
        ];
        let catalog_list = Arc::new(MemoryCatalogProviderList::new());
        let catalog = MemoryCatalogProvider::new();
        catalog
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .expect("schema");
        catalog_list.register_catalog("datafusion".into(), Arc::new(catalog));

        Ok(Self {
            session_id: "s".into(),
            config,
            runtime,
            footers,
            catalog_list,
            scalar,
            higher,
            aggregate,
            window,
            expr_planners,
            tables: HashMap::new(),
            information_schema: std::sync::Mutex::new(None),
            hidden: |_| false,
            known: std::sync::Mutex::new(None),
            analyzer: Analyzer::with_rules(
                [
                    // First, so no later rule meets a sort that cannot order the answer.
                    Arc::new(super::outerorder::OutermostOrder)
                        as Arc<dyn AnalyzerRule + Send + Sync>,
                    Arc::new(ResolveGroupingFunction::new()),
                    Arc::new(super::nullsub::NullableSubqueries::default()),
                    // Before coercion: DuckDB's text comparisons depend on what was written.
                    Arc::new(super::dialect::DuckComparisons),
                    Arc::new(super::rule::CheckedShifts::default()),
                    Arc::new(TypeCoercion::new()),
                    Arc::new(super::dialect::DuckSemantics::default()),
                    // Before the checked rewrite, which would hide the shape it matches.
                    Arc::new(FoldSubstitution(fold)),
                    Arc::new(super::constants::MoveConstants),
                    Arc::new(CheckedArithmetic::default()),
                    Arc::new(super::topn::TopPerGroup),
                    Arc::new(super::extreme::PartitionExtreme),
                    // Before the split, which then sees a distinct count over bytes like any other.
                    Arc::new(super::distinctrows::DistinctRows::default()),
                    Arc::new(super::distinct::DistinctSplit),
                    Arc::new(super::fastcast::FastTextCasts),
                    // After every rule that rewrites a subquery from outside it, as the checked
                    // sums trace a value to its cast; the rest then see each shared one once.
                    Arc::new(super::sharing::ShareRepeats),
                ]
                .into_iter()
                .chain(
                    [
                        // Checked sums change their output type, so coercion runs again.
                        Arc::new(TypeCoercion::new()) as Arc<dyn AnalyzerRule + Send + Sync>,
                        Arc::new(super::doubles::DuckDoubles::default()),
                        Arc::new(super::onerow::SingleRowSubqueries::default()),
                        Arc::new(super::correlate::KeyedCorrelation),
                        Arc::new(super::correlate::SubqueriesBelowAggregates),
                        Arc::new(super::correlate::NonEquiCorrelation),
                        Arc::new(super::latest::LatestCorrelation),
                    ]
                    .into_iter()
                    .map(|r| {
                        Arc::new(super::sharing::OverShared(r))
                            as Arc<dyn AnalyzerRule + Send + Sync>
                    }),
                )
                .collect(),
            ),
            // Without `eliminate_group_by_constant`: it recomputes a key that is a function of
            // another key in a projection, where a rewritten `TRY_CAST(k AS BIGINT)`, which DataFusion
            // names `k`, collides with `k` itself and the query was refused.
            optimizer: Optimizer::with_rules(
                Optimizer::new()
                    .rules
                    .into_iter()
                    .filter(|r| r.name() != "eliminate_group_by_constant")
                    .collect(),
            ),
            // Last, so it sees the join filter after projection pushdown has made its operands columns.
            physical_optimizers: PhysicalOptimizer::new()
                .rules
                .into_iter()
                // Straight after DataFusion's own choice of sides, which without statistics is none.
                .flat_map(|r| {
                    let after_selection = r.name() == "join_selection";
                    std::iter::once(r).chain(after_selection.then(|| {
                        Arc::new(super::buildside::BuildOnSmaller)
                            as Arc<dyn PhysicalOptimizerRule + Send + Sync>
                    }))
                })
                .chain([
                    Arc::new(super::topkfilter::TopKFilters)
                        as Arc<dyn PhysicalOptimizerRule + Send + Sync>,
                    Arc::new(super::rangejoin::RangeJoin),
                    Arc::new(super::rangebounds::RangeBounds),
                    Arc::new(super::joinfilters::ComputedKeyFilters),
                    Arc::new(super::smallinputs::SmallInputs),
                    Arc::new(super::heldinput::HeldInput),
                ])
                // Only a bounded pool charges the views; unbounded, the copy buys nothing.
                .chain(budget.is_some().then(|| {
                    Arc::new(super::compactviews::CompactViews)
                        as Arc<dyn PhysicalOptimizerRule + Send + Sync>
                }))
                .chain([
                    Arc::new(super::cancel::Cancellable(cancel))
                        as Arc<dyn PhysicalOptimizerRule + Send + Sync>,
                    Arc::new(super::sharing::PublishShared),
                ])
                .collect(),
            execution_props: ExecutionProps::new(),
            table_options: TableOptions::new(),
            extension_types: Arc::new(MemoryExtensionTypeRegistry::default()),
            alias_generator: Arc::new(AliasGenerator::new()),
            query_start: DateTime::<Utc>::from(SystemTime::now()),
            query_planner: Arc::new(MiniQueryPlanner),
        })
    }

    /// `information_schema.tables` and `.columns` as DuckDB shows them to nuthatch's `.tables` and
    /// `.schema`: the visible tables, all views, with DuckDB's type names. A subset of DuckDB's
    /// columns, the ones those commands and a person reading them use.
    pub fn footer_bytes(&self) -> usize {
        self.footers.memory_used()
    }

    pub fn build_information_schema(&mut self, hidden: fn(&str) -> bool) -> DFResult<()> {
        self.hidden = hidden;
        *self
            .information_schema
            .get_mut()
            .unwrap_or_else(|e| e.into_inner()) = None;
        Ok(())
    }

    fn information_schema(&self) -> DFResult<HashMap<String, Arc<dyn TableSource>>> {
        use arrow::array::{ArrayRef, Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion_catalog::MemTable;

        let hidden = self.hidden;
        let mut names: Vec<&String> = self.tables.keys().filter(|n| !hidden(n)).collect();
        names.sort();
        let text = |v: Vec<String>| Arc::new(StringArray::from(v)) as ArrayRef;
        let n = names.len();
        let tschema = Arc::new(Schema::new(
            ["table_catalog", "table_schema", "table_name", "table_type"]
                .map(|c| Field::new(c, DataType::Utf8, false))
                .to_vec(),
        ));
        let tables = RecordBatch::try_new(
            Arc::clone(&tschema),
            vec![
                text(vec!["memory".into(); n]),
                text(vec!["main".into(); n]),
                text(names.iter().map(|s| s.to_string()).collect()),
                text(vec!["VIEW".into(); n]),
            ],
        )?;
        let (mut tn, mut cn, mut pos, mut nul, mut ty) = (vec![], vec![], vec![], vec![], vec![]);
        for name in &names {
            for (i, f) in self.tables[*name].schema().fields().iter().enumerate() {
                tn.push(name.to_string());
                cn.push(f.name().clone());
                pos.push(i as i32 + 1);
                nul.push(if f.is_nullable() { "YES" } else { "NO" }.to_string());
                ty.push(super::errors::duck_type(&f.data_type().to_string()));
            }
        }
        let m = tn.len();
        let cschema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]));
        let columns = RecordBatch::try_new(
            Arc::clone(&cschema),
            vec![
                text(vec!["memory".into(); m]),
                text(vec!["main".into(); m]),
                text(tn),
                text(cn),
                Arc::new(Int32Array::from(pos)),
                text(nul),
                text(ty),
            ],
        )?;
        let mut out = HashMap::new();
        for (name, schema, batch) in [("tables", tschema, tables), ("columns", cschema, columns)] {
            let t = MemTable::try_new(schema, vec![vec![batch]])?;
            out.insert(name.into(), provider_as_source(Arc::new(t)));
        }
        Ok(out)
    }

    /// A scalar function under its name and aliases, replacing any of the same name.
    pub fn register_udf(&mut self, f: Arc<ScalarUDF>) {
        for a in f.aliases() {
            self.scalar.insert(a.clone(), Arc::clone(&f));
        }
        self.scalar.insert(f.name().to_string(), f);
    }

    /// What `name` is bound to now, so a transaction can put it back.
    pub fn table_source(&self, name: &str) -> Option<Arc<dyn TableSource>> {
        self.tables.get(name).cloned()
    }

    /// Bind `name` to `source`, or unbind it.
    pub fn set_table_source(&mut self, name: &str, source: Option<Arc<dyn TableSource>>) {
        let old = match &source {
            Some(s) => self.tables.insert(name.to_string(), Arc::clone(s)),
            None => self.tables.remove(name),
        };
        self.renamed(name, old.as_ref(), source.as_ref());
    }

    pub fn register_table(&mut self, name: &str, table: Arc<dyn TableProvider>) {
        let new = provider_as_source(table);
        let old = self.tables.insert(name.to_string(), Arc::clone(&new));
        self.renamed(name, old.as_ref(), Some(&new));
    }

    /// The names of `name`'s table as it was taken out and as it is put in. Rebuilding them all at
    /// each of a nest's hundred-odd registrations was about a third of what defining its views cost.
    fn renamed(
        &mut self,
        name: &str,
        old: Option<&Arc<dyn TableSource>>,
        new: Option<&Arc<dyn TableSource>>,
    ) {
        let Some(known) = self.known.get_mut().expect("known names") else {
            return;
        };
        let columns = |t: &Arc<dyn TableSource>| -> Vec<String> {
            t.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        };
        if let Some(t) = old {
            known.remove_table(name, &columns(t));
        }
        if let Some(t) = new {
            known.add_table(name, columns(t));
        }
    }

    /// Every table and column name, for resolving identifiers as DuckDB does: built once, again
    /// only after a table or view is registered, and cheap to clone.
    pub fn known_names(&self) -> super::dialect::Known {
        let mut cached = self.known.lock().expect("known names");
        cached
            .get_or_insert_with(|| {
                super::dialect::Known::of_tables(self.tables.iter().map(|(name, t)| {
                    (
                        name.as_str(),
                        t.schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect(),
                    )
                }))
            })
            .clone()
    }

    pub fn function_names(&self) -> Vec<String> {
        let mut n: Vec<String> = self
            .scalar
            .keys()
            .chain(self.aggregate.keys())
            .chain(self.window.keys())
            .chain(self.higher.keys())
            .cloned()
            .collect();
        n.sort();
        n.dedup();
        n
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut n: Vec<String> = self.tables.keys().cloned().collect();
        n.sort();
        n
    }
}

#[derive(Debug)]
struct MiniQueryPlanner;

#[async_trait]
impl QueryPlanner for MiniQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(OwnedFoldPlanner),
            Arc::new(super::sharing::SharedPlanner),
            Arc::new(super::narrow::NarrowedPlanner),
        ])
        .create_physical_plan(logical_plan, session)
        .await
    }
}

impl OptimizerConfig for MiniSession {
    fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
        Some(self.query_start)
    }
    fn alias_generator(&self) -> &Arc<AliasGenerator> {
        &self.alias_generator
    }
    fn options(&self) -> Arc<ConfigOptions> {
        Arc::clone(self.config.options())
    }
    fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
        Some(self)
    }
}

impl FunctionRegistry for MiniSession {
    fn udfs(&self) -> HashSet<String> {
        self.scalar.keys().cloned().collect()
    }
    fn higher_order_function_names(&self) -> HashSet<String> {
        self.higher.keys().cloned().collect()
    }
    fn udafs(&self) -> HashSet<String> {
        self.aggregate.keys().cloned().collect()
    }
    fn udwfs(&self) -> HashSet<String> {
        self.window.keys().cloned().collect()
    }
    fn udf(&self, name: &str) -> DFResult<Arc<ScalarUDF>> {
        self.scalar
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("no udf {name}"))
    }
    fn higher_order_function(&self, name: &str) -> DFResult<Arc<HigherOrderUDF>> {
        self.higher
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("no hof {name}"))
    }
    fn udaf(&self, name: &str) -> DFResult<Arc<AggregateUDF>> {
        self.aggregate
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("no udaf {name}"))
    }
    fn udwf(&self, name: &str) -> DFResult<Arc<WindowUDF>> {
        self.window
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("no udwf {name}"))
    }
    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        self.expr_planners.clone()
    }
}

/// The number a literal argument to `range` or `generate_series` holds.
fn integer_argument(e: &Expr) -> Option<i64> {
    use datafusion_common::ScalarValue as S;
    match e {
        Expr::Literal(v, _) => match v {
            S::Int8(Some(n)) => Some(*n as i64),
            S::Int16(Some(n)) => Some(*n as i64),
            S::Int32(Some(n)) => Some(*n as i64),
            S::Int64(Some(n)) => Some(*n),
            S::UInt8(Some(n)) => Some(*n as i64),
            S::UInt16(Some(n)) => Some(*n as i64),
            S::UInt32(Some(n)) => Some(*n as i64),
            S::UInt64(Some(n)) => i64::try_from(*n).ok(),
            _ => None,
        },
        Expr::Negative(x) => integer_argument(x)?.checked_neg(),
        Expr::Cast(c) => integer_argument(&c.expr),
        _ => None,
    }
}

/// DuckDB's `range(stop)`, `range(start, stop[, step])` and `generate_series` (the same, the stop
/// included): one BIGINT column named after the function. The only table functions nuthatch
/// admits besides `unnest`, which DataFusion plans itself. Computed while planning, from literal
/// arguments, and refused past ten million rows rather than held in memory.
fn series(name: &str, args: &[Expr]) -> DFResult<Arc<dyn TableSource>> {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    let n: Vec<i64> = args
        .iter()
        .map(|a| {
            integer_argument(a).ok_or_else(|| {
                plan_datafusion_err!(
                    "{name} over anything but integer literals is not supported here"
                )
            })
        })
        .collect::<DFResult<_>>()?;
    let (start, stop, step) = match n.as_slice() {
        [stop] => (0, *stop, 1),
        [start, stop] => (*start, *stop, 1),
        [start, stop, step] => (*start, *stop, *step),
        _ => return Err(plan_datafusion_err!("{name} takes one to three arguments")),
    };
    if step == 0 {
        return Err(plan_datafusion_err!(
            "Binder Error: {name} step cannot be 0"
        ));
    }
    let inclusive = name == "generate_series";
    let span = (stop as i128 - start as i128) / step as i128 + 1;
    if span > 10_000_000 {
        return Err(plan_datafusion_err!(
            "{name} of more than ten million rows is refused here"
        ));
    }
    let mut values = Vec::with_capacity(span.max(0) as usize);
    let mut v = start as i128;
    let within = |v: i128| match (step > 0, inclusive) {
        (true, true) => v <= stop as i128,
        (true, false) => v < stop as i128,
        (false, true) => v >= stop as i128,
        (false, false) => v > stop as i128,
    };
    while within(v) {
        values.push(v as i64);
        v += step as i128;
    }
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values))],
    )?;
    let table = datafusion_catalog::MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(provider_as_source(Arc::new(table)))
}

impl ContextProvider for MiniSession {
    fn get_table_function_source(
        &self,
        name: &str,
        args: Vec<Expr>,
    ) -> DFResult<Arc<dyn TableSource>> {
        match name.to_ascii_lowercase().as_str() {
            n @ ("range" | "generate_series") => series(n, &args),
            other => {
                datafusion_common::not_impl_err!("table function {other} is not supported here")
            }
        }
    }
    fn get_table_source(&self, name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        let found = match name.schema() {
            Some(s) if s.eq_ignore_ascii_case("information_schema") => {
                let mut built = self
                    .information_schema
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if built.is_none() {
                    *built = Some(self.information_schema()?);
                }
                built.as_ref().and_then(|b| b.get(name.table()).cloned())
            }
            _ => self.tables.get(name.table()).cloned(),
        };
        found.ok_or_else(|| plan_datafusion_err!("no table {name}"))
    }
    fn create_cte_work_table(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
    ) -> DFResult<Arc<dyn TableSource>> {
        let table = datafusion_catalog::cte_worktable::CteWorkTable::new(name, schema);
        Ok(provider_as_source(Arc::new(table)))
    }
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.scalar.get(name).cloned()
    }
    fn get_higher_order_meta(&self, name: &str) -> Option<Arc<HigherOrderUDF>> {
        self.higher.get(name).cloned()
    }
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggregate.get(name).cloned()
    }
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.window.get(name).cloned()
    }
    fn get_variable_type(&self, _variable_names: &[String]) -> Option<arrow::datatypes::DataType> {
        None
    }
    fn options(&self) -> &ConfigOptions {
        self.config.options().as_ref()
    }
    fn udf_names(&self) -> Vec<String> {
        self.scalar.keys().cloned().collect()
    }
    fn higher_order_function_names(&self) -> Vec<String> {
        self.higher.keys().cloned().collect()
    }
    fn udaf_names(&self) -> Vec<String> {
        self.aggregate.keys().cloned().collect()
    }
    fn udwf_names(&self) -> Vec<String> {
        self.window.keys().cloned().collect()
    }
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
}

#[async_trait]
impl Session for MiniSession {
    fn session_id(&self) -> &str {
        &self.session_id
    }
    fn config(&self) -> &SessionConfig {
        &self.config
    }
    fn catalog_list(&self) -> Arc<dyn CatalogProviderList> {
        Arc::clone(&self.catalog_list) as Arc<dyn CatalogProviderList>
    }
    fn query_planner(&self) -> Arc<dyn QueryPlanner + Send + Sync> {
        Arc::clone(&self.query_planner)
    }
    fn optimize(&self, plan: &LogicalPlan) -> DFResult<LogicalPlan> {
        let LogicalPlan::Explain(e) = plan else {
            let analyzed = self.analyzer.execute_and_check(
                plan.clone(),
                self.config.options().as_ref(),
                |_, _| {},
            )?;
            let optimized = self.optimizer.optimize(analyzed, self, |_, _| {})?;
            return super::narrow::narrow(optimized);
        };
        // As the umbrella crate's `SessionState::optimize`: without this, EXPLAIN shows only the
        // plan as parsed, never what the analyzer and optimizer made of it.
        let mut stringified_plans = e.stringified_plans.clone();
        let analyzed = self.analyzer.execute_and_check(
            e.plan.as_ref().clone(),
            self.config.options().as_ref(),
            |p, rule| {
                let plan_type = PlanType::AnalyzedLogicalPlan {
                    analyzer_name: rule.name().into(),
                };
                stringified_plans.push(p.to_stringified(plan_type));
            },
        )?;
        stringified_plans.push(analyzed.to_stringified(PlanType::FinalAnalyzedLogicalPlan));
        let optimized = self.optimizer.optimize(analyzed, self, |p, rule| {
            let plan_type = PlanType::OptimizedLogicalPlan {
                optimizer_name: rule.name().into(),
            };
            stringified_plans.push(p.to_stringified(plan_type));
        })?;
        let optimized = super::narrow::narrow(optimized)?;
        stringified_plans.push(optimized.to_stringified(PlanType::OptimizedLogicalPlan {
            optimizer_name: "narrow_joins".into(),
        }));
        Ok(LogicalPlan::Explain(Explain {
            verbose: e.verbose,
            explain_format: e.explain_format.clone(),
            plan: Arc::new(optimized),
            stringified_plans,
            schema: Arc::clone(&e.schema),
            logical_optimization_succeeded: true,
            show_statistics: e.show_statistics,
        }))
    }
    fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        &self.physical_optimizers
    }
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.optimize(logical_plan)?;
        self.query_planner
            .create_physical_plan(&logical_plan, self)
            .await
    }
    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        create_physical_expr(
            &expr,
            df_schema,
            &self.execution_props,
            &PhysicalPlanningContext::default(),
        )
    }
    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        &self.scalar
    }
    fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        &self.higher
    }
    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        &self.aggregate
    }
    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        &self.window
    }
    fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
        &self.extension_types
    }
    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }
    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_options(&self) -> &TableOptions {
        &self.table_options
    }
    fn table_options_mut(&mut self) -> &mut TableOptions {
        &mut self.table_options
    }
    fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::new(
            None,
            self.session_id.clone(),
            self.config.clone(),
            self.scalar.clone(),
            self.higher.clone(),
            self.aggregate.clone(),
            self.window.clone(),
            Arc::clone(&self.runtime),
        ))
    }
}
