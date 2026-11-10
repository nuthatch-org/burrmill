//! A scan that stops when the engine's token is cancelled, and `Cancellable`, which puts the same
//! check where a join does its work.
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
    token: CancelToken,
    /// A segment scan's thread-CPU meter, off until a metered statement arms it.
    cpu: Option<ScanCpu>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ScanCpu {
    armed: Arc<AtomicBool>,
    nanos: Count,
}

impl CancelExec {
    pub(super) fn wrap(
        inner: Arc<dyn ExecutionPlan>,
        token: CancelToken,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            inner,
            token,
            cpu: None,
        })
    }

    /// [`CancelExec::wrap`] for a segment scan, whose decoding [`arm_scan_cpu`] can time.
    pub(super) fn wrap_scan(
        inner: Arc<dyn ExecutionPlan>,
        token: CancelToken,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            inner,
            token,
            cpu: Some(ScanCpu::default()),
        })
    }
}

/// Arm every segment scan in `plan` to time its polls, and hand back their counters, before it runs.
/// A scan decodes inside its own `poll_next`, so the thread CPU spent there is the scan's.
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
            token: self.token.clone(),
            cpu: self.cpu.clone(),
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
        // Charged whether or not it fits, so the operators that can spill make room for it.
        let bytes = super::catalog::bookkeeping(&self.inner, partition);
        let held = (bytes > 0).then(|| {
            let r = MemoryConsumer::new(format!("SegmentScan[{partition}]"))
                .register(ctx.memory_pool());
            r.grow(bytes);
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
        let token = self.token.clone();
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
pub(super) struct Cancellable(pub(super) CancelToken);

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
            let p = if let Some(j) = p.downcast_ref::<NestedLoopJoinExec>() {
                Arc::new(
                    NestedLoopJoinExecBuilder::from(j)
                        .with_filter(j.filter().map(|f| self.checked(f)))
                        .build()?,
                )
            } else if let Some(j) = p.downcast_ref::<HashJoinExec>() {
                Arc::new(
                    j.builder()
                        .with_filter(j.filter().map(|f| self.checked(f)))
                        .build()?,
                )
            } else {
                p
            };
            Ok(Transformed::yes(CancelExec::wrap(p, self.0.clone())))
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

impl Cancellable {
    fn checked(&self, f: &JoinFilter) -> JoinFilter {
        JoinFilter::new(
            Arc::new(CancelExpr {
                inner: Arc::clone(f.expression()),
                token: self.0.clone(),
            }),
            f.column_indices().to_vec(),
            Arc::clone(f.schema()),
        )
    }
}

/// An expression that fails once the token is cancelled, and is otherwise the one it wraps.
#[derive(Debug)]
struct CancelExpr {
    inner: Arc<dyn PhysicalExpr>,
    token: CancelToken,
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
        if self.token.is_cancelled() {
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
            token: self.token.clone(),
        }))
    }
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(f)
    }
}
