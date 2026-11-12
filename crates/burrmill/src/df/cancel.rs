//! A scan that stops when its statement's token is cancelled, and `Cancellable`, which puts the
//! same check where a join does its work. The token is the one in the task context the plan runs
//! under, so each statement stops at its own.
//!
//! DataFusion's operators do not yield inside themselves (apache/datafusion#19358): an aggregate
//! over a join produces nothing until it has consumed everything, so a check between output
//! batches sees the cancel only at the end. Every nest table is read through this wrapper, so a
//! cancelled statement dries up at its source within one scan batch, and whatever is above it fails
//! with the input.
//!
//! A join multiplies rows without reading its source again: a nested-loop join whose filter
//! rejects everything spends hours inside one poll and emits nothing. So every join's output is
//! checked, and so is every evaluation of its filter.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use arrow::datatypes::{FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::RecordBatchStream;
use datafusion_physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion_physical_plan::joins::utils::JoinFilter;
use datafusion_physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, NestedLoopJoinExecBuilder,
};
use datafusion_physical_plan::metrics::Count;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_session::PhysicalOptimizerRule;
use futures::StreamExt;

use crate::CancelToken;

#[derive(Debug)]
pub(super) struct CancelExec {
    inner: Arc<dyn ExecutionPlan>,
    /// Where a wrapped join's [`CancelExpr`] finds the token, which only `execute` knows.
    filter: Option<Slot>,
    /// A segment scan's thread-CPU meter, off until a metered statement arms it.
    cpu: Option<ScanCpu>,
    /// A segment scan's reservations, which its reader factory charges as each segment opens.
    opened: Option<Arc<super::catalog::Opened>>,
}

/// A plan is made for one statement, so the first token set is the only one.
type Slot = Arc<OnceLock<CancelToken>>;

/// The token of the statement `ctx` runs.
pub(super) fn statement_token(ctx: &TaskContext) -> CancelToken {
    ctx.session_config()
        .get_extension::<CancelToken>()
        .map(|t| t.as_ref().clone())
        .unwrap_or_default()
}

#[derive(Debug, Clone, Default)]
pub(super) struct ScanCpu {
    armed: Arc<AtomicBool>,
    nanos: Count,
}

impl CancelExec {
    fn wrap(inner: Arc<dyn ExecutionPlan>, filter: Option<Slot>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            inner,
            filter,
            cpu: None,
            opened: None,
        })
    }

    /// A segment scan, whose decoding [`arm_scan_cpu`] can time.
    pub(super) fn wrap_scan(
        inner: Arc<dyn ExecutionPlan>,
        opened: Arc<super::catalog::Opened>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            inner,
            filter: None,
            cpu: Some(ScanCpu::default()),
            opened: Some(opened),
        })
    }
}

/// Arm every segment scan in `plan` to time its polls, and hand back their counters, before it runs.
/// A scan decodes inside its own `poll_next`, so the thread CPU spent there is the scan's.
#[recursive::recursive]
pub(super) fn arm_scan_cpu(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Count>) {
    if let Some(cpu) = plan
        .downcast_ref::<CancelExec>()
        .and_then(|c| c.cpu.as_ref())
    {
        cpu.armed.store(true, Ordering::Relaxed);
        out.push(cpu.nanos.clone());
    }
    for c in plan.children() {
        arm_scan_cpu(c, out);
    }
}

impl DisplayAs for CancelExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CancelExec")
    }
}

impl ExecutionPlan for CancelExec {
    fn name(&self) -> &str {
        "CancelExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            inner: children.pop().expect("one child"),
            filter: self.filter.clone(),
            cpu: self.cpu.clone(),
            opened: self.opened.clone(),
        }))
    }
    // Passes a top-k's dynamic filter through, which would otherwise stop here short of the scan.
    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        match phase {
            FilterPushdownPhase::Post => {
                FilterDescription::from_children(parent_filters, &self.children())
            }
            // A `WHERE` reached the scan by the table's own pushdown. Pushed again, it left a lambda
            // above reading its variable against the scan's columns, and refused.
            FilterPushdownPhase::Pre => Ok(FilterDescription::all_unsupported(
                &parent_filters,
                &self.children(),
            )),
        }
    }
    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let token = statement_token(&ctx);
        if let Some(slot) = &self.filter {
            let _ = slot.set(token.clone());
        }
        let held = self.opened.as_ref().map(|opened| {
            let r = Arc::new(super::catalog::Bookkeeping::new(
                MemoryConsumer::new(format!("SegmentScan[{partition}]"))
                    .register(ctx.memory_pool()),
                Arc::clone(ctx.memory_pool()),
                super::catalog::headroom(&ctx),
            ));
            r.charge(super::catalog::listed(&self.inner, partition));
            opened.running(partition, &r);
            r
        });
        let mut stream = self.inner.execute(partition, ctx)?;
        if let Some(cpu) = self
            .cpu
            .as_ref()
            .filter(|c| c.armed.load(Ordering::Relaxed))
        {
            stream = Box::pin(Timed {
                inner: stream,
                nanos: cpu.nanos.clone(),
            });
        }
        let schema = stream.schema();
        let checked = stream.map(move |item| {
            let _held = &held;
            if token.is_cancelled() {
                return Err(DataFusionError::Execution("cancelled".into()));
            }
            item
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, checked)))
    }
}

/// A scan's stream, adding the thread CPU each poll takes to `nanos`. A poll runs on one thread from
/// start to finish, so the two readings are the same thread's clock.
struct Timed {
    inner: SendableRecordBatchStream,
    nanos: Count,
}

impl futures::Stream for Timed {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let started = thread_cpu_nanos();
        let out = self.inner.poll_next_unpin(cx);
        self.nanos
            .add(thread_cpu_nanos().saturating_sub(started) as usize);
        out
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl RecordBatchStream for Timed {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

#[cfg(unix)]
fn thread_cpu_nanos() -> u64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `t` is a valid, writable timespec for the duration of the call.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut t) } != 0 {
        return 0;
    }
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}

#[cfg(not(unix))]
fn thread_cpu_nanos() -> u64 {
    0
}

/// Wraps every join's output in [`CancelExec`] and its filter in [`CancelExpr`]. Last among the
/// physical rules, so it sees the joins `RangeJoin` made.
#[derive(Debug)]
pub(super) struct Cancellable;

impl PhysicalOptimizerRule for Cancellable {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            if !p.name().ends_with("JoinExec") {
                return Ok(Transformed::no(p));
            }
            let slot = Slot::default();
            let (p, filtered) = if let Some(j) = p.downcast_ref::<NestedLoopJoinExec>() {
                let p: Arc<dyn ExecutionPlan> = Arc::new(
                    NestedLoopJoinExecBuilder::from(j)
                        .with_filter(j.filter().map(|f| checked(f, &slot)))
                        .build()?,
                );
                (p, true)
            } else if let Some(j) = p.downcast_ref::<HashJoinExec>() {
                let p: Arc<dyn ExecutionPlan> = Arc::new(
                    j.builder()
                        .with_filter(j.filter().map(|f| checked(f, &slot)))
                        .build()?,
                );
                (p, true)
            } else {
                (p, false)
            };
            Ok(Transformed::yes(CancelExec::wrap(
                p,
                filtered.then_some(slot),
            )))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "cancellable"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn checked(f: &JoinFilter, slot: &Slot) -> JoinFilter {
    JoinFilter::new(
        Arc::new(CancelExpr {
            inner: Arc::clone(f.expression()),
            token: Arc::clone(slot),
        }),
        f.column_indices().to_vec(),
        Arc::clone(f.schema()),
    )
}

/// An expression that fails once the statement's token is cancelled, and is otherwise the one it
/// wraps.
#[derive(Debug)]
struct CancelExpr {
    inner: Arc<dyn PhysicalExpr>,
    token: Slot,
}

impl PartialEq for CancelExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for CancelExpr {}

impl std::hash::Hash for CancelExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl fmt::Display for CancelExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl PhysicalExpr for CancelExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.token.get().is_some_and(CancelToken::is_cancelled) {
            return Err(DataFusionError::Execution("cancelled".into()));
        }
        self.inner.evaluate(batch)
    }
    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.inner.return_field(input_schema)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            inner: children.pop().expect("one child"),
            token: Arc::clone(&self.token),
        }))
    }
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(f)
    }
}

thread_local! {
    /// The token of the statement this thread is planning. The rules are the engine's and cannot
    /// be handed it, and a statement is planned on one thread from start to finish.
    static PLANNING: std::cell::RefCell<Option<CancelToken>> = const { std::cell::RefCell::new(None) };
}

/// `f`, with every planning rule it runs stopping once `token` is cancelled. Not for a tab: its
/// one thread plans in a single stretch nothing else can cancel, and its statement is a future.
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn planning<T>(token: &CancelToken, f: impl FnOnce() -> T) -> T {
    struct Restore(Option<CancelToken>);
    impl Drop for Restore {
        fn drop(&mut self) {
            PLANNING.with(|p| *p.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(PLANNING.with(|p| p.replace(Some(token.clone()))));
    f()
}

/// Refuses once the statement being planned on this thread is cancelled.
pub(super) fn planning_goes_on() -> Result<()> {
    if PLANNING.with(|p| p.borrow().as_ref().is_some_and(CancelToken::is_cancelled)) {
        return Err(DataFusionError::Execution("cancelled".into()));
    }
    Ok(())
}

type Analyzing = dyn datafusion_optimizer::AnalyzerRule + Send + Sync;
type Optimizing = dyn datafusion_optimizer::OptimizerRule + Send + Sync;
type Physical = dyn PhysicalOptimizerRule + Send + Sync;

/// A planning rule that checks the statement's token first: before each rule, and before each node
/// for a rule the optimizer applies node by node.
#[derive(Debug)]
pub(super) struct Stoppable<R: ?Sized>(pub(super) Arc<R>);

impl datafusion_optimizer::AnalyzerRule for Stoppable<Analyzing> {
    fn analyze(
        &self,
        plan: datafusion_expr::LogicalPlan,
        config: &ConfigOptions,
    ) -> Result<datafusion_expr::LogicalPlan> {
        planning_goes_on()?;
        self.0.analyze(plan, config)
    }
    fn name(&self) -> &str {
        self.0.name()
    }
}

impl datafusion_optimizer::OptimizerRule for Stoppable<Optimizing> {
    fn name(&self) -> &str {
        self.0.name()
    }
    fn apply_order(&self) -> Option<datafusion_optimizer::optimizer::ApplyOrder> {
        self.0.apply_order()
    }
    fn rewrite(
        &self,
        plan: datafusion_expr::LogicalPlan,
        config: &dyn datafusion_optimizer::OptimizerConfig,
    ) -> Result<Transformed<datafusion_expr::LogicalPlan>> {
        planning_goes_on()?;
        self.0.rewrite(plan, config)
    }
}

impl PhysicalOptimizerRule for Stoppable<Physical> {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        planning_goes_on()?;
        self.0.optimize(plan, config)
    }
    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn datafusion_session::PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        planning_goes_on()?;
        self.0.optimize_with_context(plan, context)
    }
    fn name(&self) -> &str {
        self.0.name()
    }
    fn schema_check(&self) -> bool {
        self.0.schema_check()
    }
}
