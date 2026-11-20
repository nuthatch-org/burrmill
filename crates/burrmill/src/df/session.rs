//! Component-crate session: no umbrella `datafusion` crate, no DDL, no table functions.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock, Mutex};

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
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{
    DFSchema, Result as DFResult, TableReference, plan_datafusion_err, resources_datafusion_err,
};
use datafusion_execution::TaskContext;
use datafusion_execution::cache::Cache;
use datafusion_execution::cache::cache_manager::{
    CacheManagerConfig, CachedFileMetadataEntry, FileMetadataCache,
};
use datafusion_execution::cache::default_cache::DefaultCache;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
    PeakRecordingPool, TrackConsumersPool, human_readable_size,
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

/// The footers of segments on this machine, one cache for every engine in the process: a host
/// opens an engine at each seal, and each read every footer again (#152). A path is absolute and an
/// entry answers only for the size it was read at. Store keys mean what the engine's store says,
/// so those stay in the engine's own cache.
static LOCAL_FOOTERS: LazyLock<Arc<Footers>> =
    LazyLock::new(|| Arc::new(Footers::new(0).with_name("burrmill local footers")));

/// What each live engine allows the local footers.
static LOCAL_SHARES: Mutex<Vec<usize>> = Mutex::new(Vec::new());

pub(super) fn local_footers() -> Arc<FileMetadataCache> {
    Arc::clone(&*LOCAL_FOOTERS) as Arc<FileMetadataCache>
}

/// One engine's allowance in the local footers' limit, given back when the engine goes.
struct LocalShare(usize);

impl LocalShare {
    fn new(bytes: usize) -> Self {
        let mut live = LOCAL_SHARES.lock().unwrap_or_else(|p| p.into_inner());
        live.push(bytes);
        LOCAL_FOOTERS.update_cache_limit(local_limit(&live));
        Self(bytes)
    }
}

impl Drop for LocalShare {
    fn drop(&mut self) {
        let mut live = LOCAL_SHARES.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(i) = live.iter().position(|b| *b == self.0) {
            live.swap_remove(i);
        }
        // The last engine out leaves the limit as it stood: a host that closes one engine before
        // it opens the next keeps the footers between them.
        if !live.is_empty() {
            LOCAL_FOOTERS.update_cache_limit(local_limit(&live));
        }
    }
}

/// The most any live engine allows: the largest budget in the process set that much aside, outside
/// its pool, and the engines beside it now add nothing to it.
fn local_limit(live: &[usize]) -> usize {
    live.iter().copied().max().unwrap_or(0)
}

/// The bytes the column sizes a metered statement keeps for each segment (`catalog::Summary`) may
/// hold: a fifth more beside the local footers' allowance, which they leave whole, since a scan
/// larger than that cache misses on every footer. A segment's sizes are a thirtieth to a fortieth
/// of its footer, so a fifth keeps several times the segments the footers can.
pub(super) fn local_sizes_limit() -> usize {
    local_limit(&LOCAL_SHARES.lock().unwrap_or_else(|p| p.into_inner())) / 5
}

/// Strict decimals, deterministic mode, addresses as text, and the value limit.
pub type Stamp = (bool, bool, bool, usize);

/// Plans kept for statements run again; past it they are all let go.
const PLANS_KEPT: usize = 128;

/// A statement longer than this is not kept: a long one is seldom run twice.
const PLANNED_SQL_BYTES: usize = 64 << 10;

pub struct MiniSession {
    session_id: String,
    config: SessionConfig,
    /// `config` at one target partition, for a statement over small inputs.
    one_partition: SessionConfig,
    /// `config`'s options with one optimizer pass, for the same statements.
    one_pass: Arc<ConfigOptions>,
    /// A statement scanning fewer bytes than this is planned in `one_partition`; zero, none is.
    small_bytes: std::sync::atomic::AtomicU64,
    runtime: Arc<RuntimeEnv>,
    footers: Arc<Footers>,
    _local_share: LocalShare,
    catalog_list: Arc<MemoryCatalogProviderList>,
    scalar: HashMap<String, Arc<ScalarUDF>>,
    higher: HashMap<String, Arc<HigherOrderUDF>>,
    aggregate: HashMap<String, Arc<AggregateUDF>>,
    window: HashMap<String, Arc<WindowUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
    tables: HashMap<String, Arc<dyn TableSource>>,
    /// By table, the columns the host writes as addresses; the `Addresses` rule reads it.
    addresses: super::addresses::Noted,
    known: std::sync::Mutex<Option<Arc<super::dialect::Known>>>,
    /// Each statement's optimized plan by its text, under the settings it was planned with. Every
    /// registration clears it.
    plans: Mutex<HashMap<String, (Stamp, LogicalPlan)>>,
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
    limits: Arc<super::bound::Limits>,
    strict: super::strict::Flag,
    deterministic: super::deterministic::Flag,
}

impl std::fmt::Debug for MiniSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MiniSession")
    }
}

/// A budget's statement pool: the bound less the eighth the footer cache keeps outside it.
///
/// With somewhere to spill, operators that can spill share what the others leave, so one that
/// spills is never starved by another that could have; first come, first served, a partition's
/// aggregate took 47 of 56 MB and its neighbour could not get 364 KB. Without, the first to ask
/// may have the lot, since none of them can give it back.
pub(super) fn budget_pool(memory_bytes: usize, spills: bool) -> Arc<dyn MemoryPool> {
    let size = memory_bytes - memory_bytes / 8;
    let five = NonZeroUsize::new(5).expect("five");
    if spills {
        Arc::new(TrackConsumersPool::new(LendingSpillPool::new(size), five))
    } else {
        Arc::new(TrackConsumersPool::new(GreedyMemoryPool::new(size), five))
    }
}

/// A pool where a consumer that can spill takes what is free up to its fair share, and past it
/// borrows, leaving one share free, no more than an even split with the others holding.
///
/// `FairSpillPool` caps each at a share of every spiller registered, finished or idle, which was 49
/// on nuthatch's QoS nest: a final aggregate replaying its spills, which cannot spill again, was
/// refused at 12.4 MB of a pool with 560 MB free.
#[derive(Debug)]
pub(super) struct LendingSpillPool {
    pool_size: usize,
    state: Mutex<Loans>,
}

#[derive(Debug, Default)]
struct Loans {
    spillers: usize,
    /// Bytes by spilling consumer, across its reservations; one holding nothing is absent.
    held: HashMap<usize, usize>,
    spillable: usize,
    unspillable: usize,
}

impl LendingSpillPool {
    pub(super) fn new(pool_size: usize) -> Self {
        Self {
            pool_size,
            state: Mutex::new(Loans::default()),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, Loans> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl Loans {
    /// What the spilling consumer `id` may take of `additional`. A loan leaves a share free because
    /// nothing makes a borrower give it back; the split counts small holders at what they hold, as a
    /// partial aggregate that could emit early once borrowed 53 MB and left a replay none.
    fn spill_room(&self, id: usize, additional: usize, pool_size: usize) -> usize {
        let free = pool_size.saturating_sub(self.spillable + self.unspillable);
        let spill_available = pool_size.saturating_sub(self.unspillable);
        let fair = spill_available / self.spillers.max(1);
        let held = self.held.get(&id).copied().unwrap_or(0);
        if held + additional <= fair {
            return free;
        }
        let kept = fair;
        let mut lendable = spill_available - kept;
        let mut others: Vec<usize> = self
            .held
            .iter()
            .filter(|(k, _)| **k != id)
            .map(|(_, b)| *b)
            .collect();
        others.sort_unstable();
        let mut sharing = others.len() + 1;
        for b in others {
            if b * sharing > lendable {
                break;
            }
            lendable -= b;
            sharing -= 1;
        }
        let level = (lendable / sharing).max(fair);
        free.saturating_sub(kept).min(level.saturating_sub(held))
    }

    fn add(&mut self, c: &MemoryConsumer, bytes: usize) {
        if c.can_spill() {
            *self.held.entry(c.id()).or_default() += bytes;
            self.spillable += bytes;
        } else {
            self.unspillable += bytes;
        }
    }
}

impl std::fmt::Display for LendingSpillPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "fair(pool_size: {})",
            human_readable_size(self.pool_size)
        )
    }
}

impl MemoryPool for LendingSpillPool {
    fn name(&self) -> &str {
        "fair"
    }

    fn register(&self, c: &MemoryConsumer) {
        if c.can_spill() {
            self.state().spillers += 1;
        }
    }

    fn unregister(&self, c: &MemoryConsumer) {
        if c.can_spill() {
            let mut s = self.state();
            s.spillers = s.spillers.saturating_sub(1);
            s.held.remove(&c.id());
        }
    }

    fn grow(&self, r: &MemoryReservation, additional: usize) {
        self.state().add(r.consumer(), additional);
    }

    fn shrink(&self, r: &MemoryReservation, shrink: usize) {
        let mut s = self.state();
        let c = r.consumer();
        if c.can_spill() {
            s.spillable = s.spillable.saturating_sub(shrink);
            if let Some(b) = s.held.get_mut(&c.id()) {
                *b = b.saturating_sub(shrink);
                if *b == 0 {
                    s.held.remove(&c.id());
                }
            }
        } else {
            s.unspillable = s.unspillable.saturating_sub(shrink);
        }
    }

    fn try_grow(&self, r: &MemoryReservation, additional: usize) -> DFResult<()> {
        let mut s = self.state();
        let c = r.consumer();
        let available = if c.can_spill() {
            s.spill_room(c.id(), additional, self.pool_size)
        } else {
            self.pool_size.saturating_sub(s.spillable + s.unspillable)
        };
        if additional > available {
            return Err(resources_datafusion_err!(
                "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {}",
                human_readable_size(additional),
                c.name(),
                human_readable_size(r.size()),
                human_readable_size(available),
                self
            ));
        }
        s.add(c, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        let s = self.state();
        s.spillable + s.unspillable
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size)
    }
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
        // At one target partition an InterleaveExec keeps nothing apart, and DataFusion's sort
        // enforcement can then drop a one-partition repartition under one it already built, which
        // fails the rebuild (#110). A plain union plans the same work.
        config.options_mut().optimizer.prefer_existing_union = threads <= 1;
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
            // A repartition's shared spill pool reads its files in turn and waits on an unfinished one
            // for a batch that went to another; with its writers held back by that same reader the
            // statement never ends. A file a batch, finished as it is written, is never waited on.
            // Set under every budget; only a repartition that spills reads it.
            config.options_mut().execution.max_spill_file_size_bytes =
                datafusion_common::config::ConfigNonZeroUsize::try_new(1).expect("one");
            // A projection is not a pool consumer, so a batch of wide cells exists whole before
            // the caller caps the answer (#1650). 2,048 is DuckDB's vector, which that issue
            // costs as fine; 128 was 1.7-2.1x on every heavy view (where-the-time-goes.md).
            config = config.with_batch_size(2048);
        }
        // The footer caches sit outside the pool, so under a budget they take an eighth of the bound
        // rather than adding to it. Only `SegmentTable`'s reader factory consults them: this one
        // for a store's segments, the process's for local ones.
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
                .with_memory_pool(Arc::new(PeakRecordingPool::new(shared.map_or_else(
                    || budget_pool(b.memory_bytes, b.spill.is_some()),
                    |p| Arc::clone(&p.pool),
                ))))
                .with_disk_manager_builder(match &b.spill {
                    None => DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
                    Some((dir, cap)) => DiskManagerBuilder::default()
                        .with_mode(DiskManagerMode::Directories(vec![dir.clone()]))
                        .with_max_temp_directory_size(*cap),
                }),
        }
        .build_arc()?;
        let limits = super::bound::Limits::new(budget.map(|_| &runtime.memory_pool));
        let strict = super::strict::Flag::default();
        let deterministic = super::deterministic::Flag::default();
        // Under every name DataFusion gives a function: `length` is `character_length`'s, and
        // registering primary names alone refused it.
        let mut scalar = HashMap::new();
        for f in datafusion_functions::all_default_functions() {
            let f = super::ordering::ordering_safe(f);
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
        scalar.insert("div".into(), super::strict::Div::udf());
        scalar.insert(super::asof::MARKER.into(), super::asof::Marker::udf());
        for f in super::duckfns::all() {
            for a in f.aliases() {
                scalar.insert(a.clone(), Arc::clone(&f));
            }
            scalar.insert(f.name().to_string(), f);
        }
        // `address_bytes` is placed by the rewrite only. Callable, it let a caller raise the
        // refusal that turns addresses off for everyone.
        let text = super::addresses::AddressText::udf();
        scalar.insert(text.name().to_string(), text);
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
                "array_sum" => super::lists::ListSum::udf(f),
                "array_min" | "array_max" => super::lists::ListExtreme::udf(f),
                _ => f,
            };
            // DataFusion names `array_max` `list_max` too, and `array_min` only so.
            if f.name() == "array_min" {
                scalar.insert("list_min".into(), Arc::clone(&f));
            }
            for a in f.aliases() {
                scalar.insert(a.clone(), Arc::clone(&f));
            }
            scalar.insert(f.name().to_string(), f);
        }
        let mut bounded: HashMap<*const ScalarUDF, Arc<ScalarUDF>> = HashMap::new();
        for f in scalar.values_mut() {
            let b = bounded
                .entry(Arc::as_ptr(f))
                .or_insert_with(|| super::bound::wrap(Arc::clone(f), &limits));
            *f = Arc::clone(b);
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
            let f = super::ordered_agg::SelfSortingStringAgg::udaf(f, &limits);
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
        let addresses = super::addresses::Noted::default();
        let catalog_list = Arc::new(MemoryCatalogProviderList::new());
        let catalog = MemoryCatalogProvider::new();
        catalog
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .expect("schema");
        catalog_list.register_catalog("datafusion".into(), Arc::new(catalog));
        let mut one_partition = config.clone().with_target_partitions(1);
        one_partition.options_mut().optimizer.prefer_existing_union = true;
        let mut one_pass = config.options().as_ref().clone();
        one_pass.optimizer.max_passes = 1;

        Ok(Self {
            session_id: "s".into(),
            config,
            one_partition,
            one_pass: Arc::new(one_pass),
            small_bytes: std::sync::atomic::AtomicU64::new(super::smallinputs::SMALL_BYTES),
            runtime,
            footers,
            _local_share: LocalShare::new(footer_limit),
            catalog_list,
            scalar,
            higher,
            aggregate,
            window,
            expr_planners,
            tables: HashMap::new(),
            addresses: addresses.clone(),
            information_schema: std::sync::Mutex::new(None),
            hidden: |_| false,
            known: std::sync::Mutex::new(None),
            plans: Mutex::new(HashMap::new()),
            analyzer: Analyzer::with_rules(
                [
                    // First, so no later rule meets a sort that cannot order the answer.
                    Arc::new(super::outerorder::OutermostOrder)
                        as Arc<dyn AnalyzerRule + Send + Sync>,
                    Arc::new(super::coercion::Grouping(ResolveGroupingFunction::new())),
                    // Before every rule that reads a join: a marked one is not one yet.
                    Arc::new(super::asof::AsofJoins),
                    // Before every rule that compares text, an address is fixed binary or text.
                    Arc::new(super::addresses::Addresses(addresses.clone())),
                    Arc::new(super::nullsub::NullableSubqueries::default()),
                    // Before DuckDB's `/`, which casts to DOUBLE what strict decimals refuses.
                    Arc::new(super::strict::Written(Arc::clone(&strict))),
                    // Before coercion: DuckDB's text comparisons depend on what was written.
                    Arc::new(super::dialect::DuckComparisons),
                    Arc::new(super::rule::CheckedShifts::default()),
                    Arc::new(super::coercion::First(Arc::new(super::coercion::Coercion))),
                    // Before DuckDB's `/` and rounding casts, which it refuses or replaces.
                    Arc::new(super::strict::StrictDecimals {
                        flag: Arc::clone(&strict),
                        refuse_only: false,
                    }),
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
                    // Checked sums change their output type, so coercion runs again.
                    Arc::new(super::coercion::Again(Arc::new(
                        super::sharing::OverShared(Arc::new(super::coercion::Coercion)),
                    ))),
                ]
                .into_iter()
                .chain(
                    [
                        Arc::new(super::doubles::DuckDoubles::default())
                            as Arc<dyn AnalyzerRule + Send + Sync>,
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
                .chain([
                    Arc::new(super::strict::StrictDecimals {
                        flag: Arc::clone(&strict),
                        refuse_only: true,
                    }) as Arc<dyn AnalyzerRule + Send + Sync>,
                    // Last of the rewrites, so the orders it completes are the plan's own.
                    Arc::new(super::deterministic::Deterministic(Arc::clone(
                        &deterministic,
                    ))) as Arc<dyn AnalyzerRule + Send + Sync>,
                    Arc::new(super::bound::BoundValues(Arc::clone(&limits))),
                ])
                .map(|r| {
                    Arc::new(super::cancel::Stoppable(r)) as Arc<dyn AnalyzerRule + Send + Sync>
                })
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
                    // Last in each pass, so the next pass optimizes what it cut.
                    .chain([
                        Arc::new(super::bound::BoundValues(Arc::clone(&limits)))
                            as Arc<dyn datafusion_optimizer::OptimizerRule + Send + Sync>,
                        Arc::new(super::sharing::PruneShared),
                    ])
                    .map(|r| {
                        Arc::new(super::cancel::Stoppable(r))
                            as Arc<dyn datafusion_optimizer::OptimizerRule + Send + Sync>
                    })
                    .collect(),
            ),
            // Last, so it sees the join filter after projection pushdown has made its operands columns.
            physical_optimizers: std::iter::once(
                // Before the sort and distribution passes, which read what a filter holds constant.
                Arc::new(super::addresses::TextAddressFilters)
                    as Arc<dyn PhysicalOptimizerRule + Send + Sync>,
            )
            .chain(
                PhysicalOptimizer::new()
                    .rules
                    .into_iter()
                    .map(|r| match r.name() {
                        "EnsureRequirements" => Arc::new(super::requirements::Requirements),
                        _ => r,
                    }),
            )
            // Straight after DataFusion's own choice of sides, which without statistics is none.
            .flat_map(|r| {
                let after_selection = r.name() == "join_selection";
                // Before the top-k filters are pushed, so the scans get the corrected one.
                let before_post = r.name() == "FilterPushdown(Post)";
                before_post
                    .then(|| {
                        Arc::new(super::topkfilter::TopKMergeOrder)
                            as Arc<dyn PhysicalOptimizerRule + Send + Sync>
                    })
                    .into_iter()
                    .chain(std::iter::once(r))
                    .chain(after_selection.then(|| {
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
                Arc::new(super::cancel::Cancellable)
                    as Arc<dyn PhysicalOptimizerRule + Send + Sync>,
                Arc::new(super::sharing::PublishShared),
            ])
            // A tab cannot spawn the task each partition would be coalesced on.
            .chain(cfg!(target_arch = "wasm32").then(|| {
                Arc::new(super::sequential::Sequential)
                    as Arc<dyn PhysicalOptimizerRule + Send + Sync>
            }))
            .map(|r| {
                Arc::new(super::cancel::Stoppable(r))
                    as Arc<dyn PhysicalOptimizerRule + Send + Sync>
            })
            .collect(),
            execution_props: ExecutionProps::new(),
            table_options: TableOptions::new(),
            extension_types: Arc::new(MemoryExtensionTypeRegistry::default()),
            alias_generator: Arc::new(AliasGenerator::new()),
            query_start: Utc::now(),
            query_planner: Arc::new(MiniQueryPlanner),
            limits,
            strict,
            deterministic,
        })
    }

    /// The context a statement's physical plan runs in. Operators never look a function up by
    /// name, so the registries are left out rather than copied for each statement.
    pub fn statement_ctx(&self) -> TaskContext {
        TaskContext::new(
            None,
            self.session_id.clone(),
            self.config.clone(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::clone(&self.runtime),
        )
    }

    /// The optimizer's view of the session for `analyzed`. A small statement gets one pass, since
    /// the second seldom changes a few nodes and costs as much as the first; not through a join,
    /// where a limit reaches each side of a chain one pass at a time.
    fn passes(&self, analyzed: &LogicalPlan) -> Passes<'_> {
        let below = self.small_bytes.load(std::sync::atomic::Ordering::Relaxed);
        let one =
            super::smallinputs::small_statement(analyzed, below) && !joins_or_subqueries(analyzed);
        Passes {
            session: self,
            one,
            start: Utc::now(),
        }
    }

    /// What the analyzer reads besides the catalogue, as it stands now.
    pub fn stamp(&self) -> Stamp {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.strict.load(Relaxed),
            self.deterministic.load(Relaxed),
            self.addresses.is_off(),
            self.limits.value_limit(),
        )
    }

    /// `sql`'s optimized plan from an earlier run under the same settings.
    pub fn cached_plan(&self, sql: &str) -> Option<LogicalPlan> {
        let plans = self.plans.lock().unwrap_or_else(|e| e.into_inner());
        let (stamp, plan) = plans.get(sql)?;
        (*stamp == self.stamp()).then(|| plan.clone())
    }

    /// Keeps `plan`, optimized under `stamp` from `written`, for `sql` run again. A shared
    /// subquery's nodes hold what its run computed, and a `range` holds rows charged to the pool,
    /// so a plan with either is planned afresh each time; so is one calling `now()` or anything
    /// else not immutable, which the optimizer folds to what it answered this time.
    pub fn keep_plan(&self, sql: &str, stamp: Stamp, written: &LogicalPlan, plan: &LogicalPlan) {
        if sql.len() > PLANNED_SQL_BYTES
            || matches!(plan, LogicalPlan::Explain(_) | LogicalPlan::Analyze(_))
            || calls_mutable(written)
            || super::sharing::shares(plan)
            || !self.reads_only_the_catalogue(plan)
        {
            return;
        }
        let mut plans = self.plans.lock().unwrap_or_else(|e| e.into_inner());
        if plans.len() >= PLANS_KEPT {
            plans.clear();
        }
        plans.insert(sql.to_string(), (stamp, plan.clone()));
    }

    /// Whether every table `plan` scans is one the catalogue holds now.
    fn reads_only_the_catalogue(&self, plan: &LogicalPlan) -> bool {
        let schema = self
            .information_schema
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let held = |s: &Arc<dyn TableSource>| {
            self.tables
                .values()
                .chain(schema.iter().flat_map(|m| m.values()))
                .any(|t| Arc::ptr_eq(t, s))
        };
        let mut all = true;
        let _ = plan.apply_with_subqueries(|p| {
            if let LogicalPlan::TableScan(s) = p {
                all = held(&s.source);
            }
            Ok(if all {
                TreeNodeRecursion::Continue
            } else {
                TreeNodeRecursion::Stop
            })
        });
        all
    }

    fn forget_plans(&mut self) {
        self.plans
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// The physical plan of an optimized `plan`.
    pub async fn physical_plan(&self, plan: &LogicalPlan) -> DFResult<Arc<dyn ExecutionPlan>> {
        // A partition a thread over a few kilobytes is a task each, the repartitions and merges
        // between them, and the passes that place them: most of what a small statement cost.
        let below = self.small_bytes.load(std::sync::atomic::Ordering::Relaxed);
        if self.config.target_partitions() > 1 && super::smallinputs::small_statement(plan, below) {
            let one = OnePartition {
                inner: self,
                config: &self.one_partition,
                table_options: self.table_options.clone(),
            };
            return self.query_planner.create_physical_plan(plan, &one).await;
        }
        self.query_planner.create_physical_plan(plan, self).await
    }

    pub fn set_small_bytes(&self, bytes: u64) {
        self.small_bytes
            .store(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn limits(&self) -> &super::bound::Limits {
        &self.limits
    }

    pub fn strict(&self) -> &super::strict::Flag {
        &self.strict
    }

    pub fn deterministic(&self) -> &super::deterministic::Flag {
        &self.deterministic
    }

    /// `information_schema.tables` and `.columns` as DuckDB shows them to nuthatch's `.tables` and
    /// `.schema`: the visible tables, all views, with DuckDB's type names. A subset of DuckDB's
    /// columns, the ones those commands and a person reading them use.
    pub fn footer_bytes(&self) -> usize {
        self.footers.memory_used() + LOCAL_FOOTERS.memory_used()
    }

    pub fn build_information_schema(&mut self, hidden: fn(&str) -> bool) -> DFResult<()> {
        self.forget_plans();
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
        self.forget_plans();
        let f = super::bound::wrap(f, &self.limits);
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
        self.forget_plans();
        let old = match &source {
            Some(s) => self.tables.insert(name.to_string(), Arc::clone(s)),
            None => self.tables.remove(name),
        };
        self.renamed(name, old.as_ref(), source.as_ref());
    }

    /// `columns` of `table` are addresses: fixed binary inside a plan.
    pub fn note_addresses(&self, table: &str, columns: impl IntoIterator<Item = String>) {
        self.addresses.note(table, columns);
        self.plans.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    /// Addresses are text from now on; whether they were fixed binary until now.
    pub fn addresses_off(&self) -> bool {
        self.addresses.turn_off()
    }

    pub fn address_refused(&self) -> bool {
        self.addresses.refused()
    }

    pub fn register_table(&mut self, name: &str, table: Arc<dyn TableProvider>) {
        self.forget_plans();
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
        let known = Arc::make_mut(known);
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
    pub fn known_names(&self) -> Arc<super::dialect::Known> {
        let mut cached = self.known.lock().expect("known names");
        let known = cached.get_or_insert_with(|| {
            Arc::new(super::dialect::Known::of_tables(self.tables.iter().map(
                |(name, t)| {
                    (
                        name.as_str(),
                        t.schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect(),
                    )
                },
            )))
        });
        Arc::clone(known)
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

/// Whether `plan` calls a function that is not immutable, subqueries included.
fn calls_mutable(plan: &LogicalPlan) -> bool {
    use datafusion_expr::Volatility;
    let mut found = false;
    let _ = plan.apply_with_subqueries(|p| {
        let _ = p.apply_expressions(|e| {
            found = TreeNode::exists(e, |x| {
                Ok(match x {
                    Expr::ScalarFunction(f) => {
                        f.func.signature().volatility != Volatility::Immutable
                    }
                    _ => false,
                })
            })?;
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        });
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
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

/// Whether `plan` holds a join, or a subquery the optimizer may make one.
fn joins_or_subqueries(plan: &LogicalPlan) -> bool {
    datafusion_common::tree_node::TreeNode::exists(plan, |p| {
        Ok(matches!(p, LogicalPlan::Join(_) | LogicalPlan::Subquery(_))
            || super::dag::holds(p, |e| {
                matches!(
                    e,
                    Expr::ScalarSubquery(_)
                        | Expr::Exists(_)
                        | Expr::InSubquery(_)
                        | Expr::SetComparison(_)
                )
            }))
    })
    .unwrap_or(true)
}

/// The session as the optimizer sees it, with one pass or the configured number.
struct Passes<'a> {
    session: &'a MiniSession,
    one: bool,
    /// What `now()` answers: when the statement was planned, not when the session was opened.
    start: DateTime<Utc>,
}

impl OptimizerConfig for Passes<'_> {
    fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
        Some(self.start)
    }
    fn alias_generator(&self) -> &Arc<AliasGenerator> {
        self.session.alias_generator()
    }
    fn options(&self) -> Arc<ConfigOptions> {
        match self.one {
            true => Arc::clone(&self.session.one_pass),
            false => OptimizerConfig::options(self.session),
        }
    }
    fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
        Some(self.session)
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
/// arguments, refused past ten million rows, and charged to the pool while the plan holds it.
fn series(
    name: &str,
    args: &[Expr],
    limits: &super::bound::Limits,
) -> DFResult<Arc<dyn TableSource>> {
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
    let rows = span.max(0) as usize;
    let column = super::bound::charge_column(limits, rows * 8, || {
        let mut values = Vec::with_capacity(rows);
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
        Arc::new(Int64Array::from(values))
    })?;
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), vec![column])?;
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
            n @ ("range" | "generate_series") => series(n, &args, &self.limits),
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
        let _planning = super::bound::Planning::enter();
        let LogicalPlan::Explain(e) = plan else {
            let analyzed = self.analyzer.execute_and_check(
                plan.clone(),
                self.config.options().as_ref(),
                |_, _| {},
            )?;
            let passes = self.passes(&analyzed);
            let optimized = self.optimizer.optimize(analyzed, &passes, |_, _| {})?;
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
        let passes = self.passes(&analyzed);
        let optimized = self.optimizer.optimize(analyzed, &passes, |p, rule| {
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
        self.physical_plan(&logical_plan).await
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

/// The session a small statement is physically planned in: its own, at one target partition.
struct OnePartition<'a> {
    inner: &'a MiniSession,
    config: &'a SessionConfig,
    table_options: TableOptions,
}

#[async_trait]
impl Session for OnePartition<'_> {
    fn session_id(&self) -> &str {
        self.inner.session_id()
    }
    fn config(&self) -> &SessionConfig {
        self.config
    }
    fn catalog_list(&self) -> Arc<dyn CatalogProviderList> {
        self.inner.catalog_list()
    }
    fn query_planner(&self) -> Arc<dyn QueryPlanner + Send + Sync> {
        self.inner.query_planner()
    }
    fn optimize(&self, plan: &LogicalPlan) -> DFResult<LogicalPlan> {
        self.inner.optimize(plan)
    }
    fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        self.inner.physical_optimizers()
    }
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.inner.optimize(logical_plan)?;
        self.inner
            .query_planner
            .create_physical_plan(&logical_plan, self)
            .await
    }
    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        Session::create_physical_expr(self.inner, expr, df_schema)
    }
    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        self.inner.scalar_functions()
    }
    fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        Session::higher_order_functions(self.inner)
    }
    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        self.inner.aggregate_functions()
    }
    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        self.inner.window_functions()
    }
    fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
        self.inner.extension_type_registry()
    }
    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        self.inner.runtime_env()
    }
    fn execution_props(&self) -> &ExecutionProps {
        Session::execution_props(self.inner)
    }
    fn as_any(&self) -> &dyn Any {
        Session::as_any(self.inner)
    }
    fn table_options(&self) -> &TableOptions {
        &self.table_options
    }
    fn table_options_mut(&mut self) -> &mut TableOptions {
        &mut self.table_options
    }
    fn task_ctx(&self) -> Arc<TaskContext> {
        self.inner.task_ctx()
    }
}

#[cfg(test)]
mod lending_pool {
    use super::*;

    fn pool(size: usize) -> Arc<dyn MemoryPool> {
        Arc::new(LendingSpillPool::new(size))
    }

    fn spiller(p: &Arc<dyn MemoryPool>, n: &str) -> MemoryReservation {
        MemoryConsumer::new(n).with_can_spill(true).register(p)
    }

    #[test]
    fn a_spiller_holding_nothing_cannot_take_the_pool_from_its_peers() {
        let p = pool(1000);
        let a = spiller(&p, "a");
        let b = spiller(&p, "b");
        let _c = spiller(&p, "c");
        let d = spiller(&p, "d");
        a.try_grow(10).unwrap();
        assert!(d.try_grow(990).is_err());
        b.try_grow(125).unwrap();
    }

    #[test]
    fn spillers_registered_after_a_loan_still_reach_their_shares() {
        let p = pool(1000);
        let a = spiller(&p, "a");
        let _b = spiller(&p, "b");
        while a.try_grow(10).is_ok() {}
        let late: Vec<_> = (0..8).map(|i| spiller(&p, &format!("late{i}"))).collect();
        for r in &late {
            r.try_grow(50).unwrap();
        }
    }

    #[test]
    fn a_loan_leaves_a_later_statement_room_to_finish() {
        let p = pool(1000);
        let a = spiller(&p, "a");
        let _b = spiller(&p, "b");
        while a.try_grow(10).is_ok() {}
        MemoryConsumer::new("final")
            .register(&p)
            .try_grow(400)
            .unwrap();
    }

    #[test]
    fn a_spiller_borrows_past_its_share_from_idle_ones() {
        let p = pool(1000);
        let a = spiller(&p, "a");
        let _idle: Vec<_> = (0..39).map(|i| spiller(&p, &format!("idle{i}"))).collect();
        // A fair share is 25: the replay of a spilled aggregate asks past it.
        a.try_grow(100).unwrap();
        a.new_empty().try_grow(100).unwrap();
    }

    #[test]
    fn concurrent_accounting_stays_within_the_pool() {
        let p = pool(1 << 20);
        let threads: Vec<_> = (0..16u64)
            .map(|t| {
                let p = Arc::clone(&p);
                std::thread::spawn(move || {
                    let mut x = t + 1;
                    for _ in 0..50 {
                        let mut r = MemoryConsumer::new(format!("c{t}"))
                            .with_can_spill(t % 4 != 0)
                            .register(&p);
                        let s = r.split(0);
                        for _ in 0..2000 {
                            x ^= x << 13;
                            x ^= x >> 7;
                            x ^= x << 17;
                            let n = (x % 20_000) as usize;
                            match x % 5 {
                                0 => drop(r.try_grow(n)),
                                1 => drop(s.try_grow(n)),
                                2 => r.shrink(n.min(r.size())),
                                3 => s.shrink(n.min(s.size())),
                                _ => drop(r.take()),
                            }
                            assert!(p.reserved() <= 1 << 20, "reserved {}", p.reserved());
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(p.reserved(), 0);
    }
}
