//! `UNION ALL` arms answered in the order the statement wrote them, where that order is the answer's
//! (#430).
//!
//! DataFusion's `UnionExec` hands on each arm's partitions as partitions of its own, and the
//! `CoalescePartitionsExec` above it, or `execute_stream` at the root, drives each on a task and
//! passes batches on as they arrive: `SELECT 1 UNION ALL SELECT * FROM range(2, 100) UNION ALL
//! SELECT 999 LIMIT 5` answered `1, 999, 2, 3, 4`. DuckDB answers the first arm's rows, then the
//! second's, and statements with a `LIMIT` or no `ORDER BY` rely on it.
//!
//! [`UnionOrder`] walks down from the root through the operators that hand on their one input's
//! order (a projection, a filter, a limit, a window over no order), and there makes a union an
//! [`OrderedUnionExec`]: every arm still starts at once, on a task of its own, and the answer reads
//! the first to its end, then the next. An arm of several partitions is coalesced as before, so its
//! own rows keep whatever order they had. Under an aggregate, a join or a sort the order of the
//! union's rows cannot reach the answer, and nothing there changes.

use std::fmt;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, replace_children_if_necessary,
};
use datafusion_session::PhysicalOptimizerRule;
use futures::StreamExt;

/// Batches an arm may run ahead of the one being read.
#[cfg(not(target_arch = "wasm32"))]
const AHEAD: usize = 2;

/// A `UNION ALL` of one partition: each input's rows in turn, the inputs run at once.
#[derive(Debug)]
pub struct OrderedUnionExec {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
}

impl OrderedUnionExec {
    /// `union`'s arms read in order; each of `inputs` has one partition.
    fn new(inputs: Vec<Arc<dyn ExecutionPlan>>, union: &dyn ExecutionPlan) -> Self {
        let p = union.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(union.schema()),
            Partitioning::UnknownPartitioning(1),
            p.emission_type,
            p.boundedness,
        ));
        Self { inputs, properties }
    }
}

impl DisplayAs for OrderedUnionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OrderedUnionExec")
    }
}

impl ExecutionPlan for OrderedUnionExec {
    fn name(&self) -> &str {
        "OrderedUnionExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.inputs.len()]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.inputs.len()]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            inputs: children,
            properties: Arc::clone(&self.properties),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug_assert_eq!(partition, 0);
        let schema = self.schema();
        let mut arms = Vec::with_capacity(self.inputs.len());
        for (i, input) in self.inputs.iter().enumerate() {
            // The first is read as it runs; a tab, with no runtime to spawn on, reads each in turn.
            if i == 0 || cfg!(target_arch = "wasm32") {
                let (input, ctx) = (Arc::clone(input), Arc::clone(&ctx));
                arms.push(
                    futures::stream::once(async move { input.execute(0, ctx) })
                        .map(|s| match s {
                            Ok(s) => s.boxed(),
                            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
                        })
                        .flatten()
                        .boxed(),
                );
                continue;
            }
            #[cfg(not(target_arch = "wasm32"))]
            arms.push(spawned(Arc::clone(input), Arc::clone(&ctx)));
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(arms).flatten(),
        )))
    }
}

/// `input` run on a task now, at most [`AHEAD`] batches ahead of its reader; dropping the stream
/// stops the task.
#[cfg(not(target_arch = "wasm32"))]
fn spawned(
    input: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
) -> futures::stream::BoxStream<'static, Result<arrow::record_batch::RecordBatch>> {
    let mut builder = datafusion_physical_plan::stream::RecordBatchReceiverStreamBuilder::new(
        input.schema(),
        AHEAD,
    );
    let tx = builder.tx();
    builder.spawn(async move {
        let mut stream = match input.execute(0, ctx) {
            Ok(s) => s,
            Err(e) => {
                tx.send(Err(e)).await.ok();
                return Ok(());
            }
        };
        drop(input);
        while let Some(item) = stream.next().await {
            let stop = item.is_err();
            if tx.send(item).await.is_err() || stop {
                break;
            }
        }
        Ok(())
    });
    builder.build().boxed()
}

/// Every union whose rows' order is the answer's made an [`OrderedUnionExec`].
#[derive(Debug)]
pub(super) struct UnionOrder;

impl PhysicalOptimizerRule for UnionOrder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        ordered(plan)
    }

    fn name(&self) -> &str {
        "union_order"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `plan`, whose rows reach the answer in the order it hands them on, with each union met before
/// anything that reorders read in order.
#[recursive::recursive]
fn ordered(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(u) = plan.downcast_ref::<UnionExec>() {
        // Arms sorted alike make a union sorted per partition, which something above may rely on.
        if u.properties().output_ordering().is_some() {
            return Ok(plan);
        }
        let arms = u
            .inputs()
            .iter()
            .map(|a| {
                let a = ordered(Arc::clone(a))?;
                Ok(if a.properties().partitioning.partition_count() > 1 {
                    Arc::new(CoalescePartitionsExec::new(a)) as Arc<dyn ExecutionPlan>
                } else {
                    a
                })
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Arc::new(OrderedUnionExec::new(arms, u)));
    }
    if let Some(c) = plan.downcast_ref::<CoalescePartitionsExec>() {
        let input = ordered(Arc::clone(c.input()))?;
        if Arc::ptr_eq(&input, c.input()) {
            return Ok(plan);
        }
        if input.properties().partitioning.partition_count() == 1 {
            return Ok(match c.fetch() {
                Some(n) => Arc::new(GlobalLimitExec::new(input, 0, Some(n))),
                None => input,
            });
        }
        return replace_children_if_necessary(plan, vec![input]);
    }
    let children = plan.children();
    let maintains = plan.maintains_input_order();
    let required = plan.required_input_ordering();
    // A statement's shared subqueries run before its body, whose rows are the answer.
    let body = match children.len() {
        1 => 0,
        n if plan.name() == "SharedDefsExec" => n - 1,
        _ => return Ok(plan),
    };
    if !maintains.get(body).copied().unwrap_or(false)
        || required.get(body).is_some_and(Option::is_some)
    {
        return Ok(plan);
    }
    let mut children: Vec<Arc<dyn ExecutionPlan>> = children.into_iter().map(Arc::clone).collect();
    children[body] = ordered(Arc::clone(&children[body]))?;
    replace_children_if_necessary(plan, children)
}
