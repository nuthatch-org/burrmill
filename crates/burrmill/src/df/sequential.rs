//! Partitions merged by reading them one after another, for a host that cannot spawn a task.
//!
//! `CoalescePartitionsExec` drives each input partition on a task of its own, and so does
//! `execute_stream` above any plan of more than one. A browser tab has one thread and no runtime to
//! spawn on, while a `UNION ALL` still has a partition per arm however few the target asks for.
//! Coalescing promises no order, so reading partition 0 to its end, then 1, is the same answer.
//! It would deadlock under a `RepartitionExec` of several outputs, which share one producer, but a
//! single target partition plans none of those; one into a single partition is read in turn too.

use std::fmt;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_session::PhysicalOptimizerRule;
use futures::StreamExt;

/// `datafusion_physical_plan::execute_stream` for every caller in this crate. On wasm a plan the
/// session did not optimize as part of the query, such as a shared subquery's, gets
/// [`Sequential`]'s rewrite here, and one of several partitions is read in turn rather than
/// coalesced on spawned tasks, which a tab has no runtime for.
pub(crate) fn execute_stream(
    plan: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    #[cfg(target_arch = "wasm32")]
    let plan = {
        let plan = Sequential.optimize(plan, &ConfigOptions::default())?;
        if plan.properties().partitioning.partition_count() > 1 {
            Arc::new(SequentialCoalesceExec::new(plan)) as Arc<dyn ExecutionPlan>
        } else {
            plan
        }
    };
    datafusion_physical_plan::execute_stream(plan, ctx)
}

#[derive(Debug)]
pub struct SequentialCoalesceExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl SequentialCoalesceExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let p = input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            p.emission_type,
            p.boundedness,
        ));
        Self { input, properties }
    }
}

impl DisplayAs for SequentialCoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SequentialCoalesceExec")
    }
}

impl ExecutionPlan for SequentialCoalesceExec {
    fn name(&self) -> &str {
        "SequentialCoalesceExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
        Ok(Arc::new(Self::new(children.pop().expect("one child"))))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug_assert_eq!(partition, 0);
        let input = Arc::clone(&self.input);
        let n = input.properties().partitioning.partition_count();
        // Each partition is opened only when the one before it has ended.
        let batches = futures::stream::iter(0..n)
            .map(move |i| match input.execute(i, Arc::clone(&ctx)) {
                Ok(s) => s.boxed(),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            })
            .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            batches,
        )))
    }
}

/// Every `CoalescePartitionsExec` made a [`SequentialCoalesceExec`], its fetch kept as a limit.
#[derive(Debug)]
pub(super) struct Sequential;

impl PhysicalOptimizerRule for Sequential {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            // A repartition into one partition spawns a task per input partition all the same; with
            // one output it is its input read in turn.
            if let Some(r) = p.downcast_ref::<RepartitionExec>()
                && r.properties().partitioning.partition_count() == 1
            {
                let input = Arc::clone(r.input());
                return Ok(Transformed::yes(
                    if input.properties().partitioning.partition_count() > 1 {
                        Arc::new(SequentialCoalesceExec::new(input))
                    } else {
                        input
                    },
                ));
            }
            let Some(c) = p.downcast_ref::<CoalescePartitionsExec>() else {
                return Ok(Transformed::no(p));
            };
            let seq: Arc<dyn ExecutionPlan> =
                Arc::new(SequentialCoalesceExec::new(Arc::clone(c.input())));
            Ok(Transformed::yes(match c.fetch() {
                Some(n) => Arc::new(GlobalLimitExec::new(seq, 0, Some(n))),
                None => seq,
            }))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "sequential_coalesce"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_physical_plan::Partitioning;

    use super::*;

    /// A repartition into one partition is gone after the rewrite, read in turn when its input has
    /// several, so a tab never reaches its spawning `execute`.
    #[test]
    fn a_repartition_into_one_partition_is_read_in_turn() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()], vec![batch]], schema, None)
                .unwrap();
        let one: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(input.clone(), Partitioning::RoundRobinBatch(1)).unwrap(),
        );
        let out = Sequential.optimize(one, &ConfigOptions::default()).unwrap();
        assert!(out.is::<SequentialCoalesceExec>(), "{}", out.name());
        let mut found = false;
        out.apply(|p| {
            found |= p.is::<RepartitionExec>();
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert!(!found);

        let two: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(2)).unwrap());
        let kept = Sequential.optimize(two, &ConfigOptions::default()).unwrap();
        assert!(
            kept.is::<RepartitionExec>(),
            "several outputs are left alone"
        );
    }
}
