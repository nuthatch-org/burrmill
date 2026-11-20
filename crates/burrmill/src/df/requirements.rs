//! DataFusion's `EnsureRequirements`, `datafusion-physical-optimizer` 55.2.0, phase for phase
//! through its public pieces, less the distribution pass where it cannot change the plan.
//!
//! A small statement is planned at one target partition (`OnePartition`). There, over a plan
//! whose every node has one partition, with nothing that moves partitions and no window to
//! re-fit, and no scan it would split into byte ranges, that pass rebuilds every node and returns
//! what it was given: on the dialect corpus, 873 of 990 plans, and most of the rule's time on them.
//! Debug builds run it anyway and assert as much.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::ensure_requirements::enforce_distribution::{
    DistributionContext, PlanWithKeyRequirements, adjust_input_keys_ordering, ensure_distribution,
    reorder_join_keys_to_inputs,
};
use datafusion_physical_optimizer::ensure_requirements::enforce_sorting::replace_with_order_preserving_variants::{
    OrderPreservationContext, replace_with_order_preserving_variants,
};
use datafusion_physical_optimizer::ensure_requirements::enforce_sorting::sort_pushdown::{
    SortPushDown, assign_initial_requirements, pushdown_sorts,
};
use datafusion_physical_optimizer::ensure_requirements::enforce_sorting::{
    PlanWithCorrespondingCoalescePartitions, PlanWithCorrespondingSort, ensure_sorting,
    parallelize_sorts, replace_with_partial_sort,
};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

#[derive(Debug, Default)]
pub(super) struct Requirements;

impl PhysicalOptimizerRule for Requirements {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = if config.optimizer.top_down_join_key_reordering {
            PlanWithKeyRequirements::new_default(plan)
                .transform_down(adjust_input_keys_ordering)
                .data()?
                .plan
        } else {
            plan.transform_up(|p| Ok(Transformed::yes(reorder_join_keys_to_inputs(p)?)))
                .data()?
        };
        let plan = if one_partition_throughout(&plan, config) {
            #[cfg(debug_assertions)]
            assert_unchanged_by_distribution(&plan, config)?;
            plan
        } else {
            DistributionContext::new_default(plan)
                .transform_up(|c| ensure_distribution(c, config))
                .data()?
                .plan
        };
        let plan = PlanWithCorrespondingSort::new_default(plan)
            .transform_up(ensure_sorting)?
            .data
            .plan;
        let plan = if config.optimizer.repartition_sorts {
            PlanWithCorrespondingCoalescePartitions::new_default(plan)
                .transform_up(parallelize_sorts)
                .data()?
                .plan
        } else {
            plan
        };
        let plan = OrderPreservationContext::new_default(plan)
            .transform_up(|c| replace_with_order_preserving_variants(c, false, true, config))
            .data()?
            .plan;
        let mut pushdown = SortPushDown::new_default(plan);
        assign_initial_requirements(&mut pushdown);
        pushdown_sorts(pushdown)?
            .plan
            .transform_up(|p| Ok(Transformed::yes(replace_with_partial_sort(p)?)))
            .data()
    }

    fn name(&self) -> &str {
        "EnsureRequirements"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// One target partition, every node one partition, none of the nodes the distribution pass
/// removes, re-fits or interleaves, and no scan it would give byte ranges.
fn one_partition_throughout(plan: &Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> bool {
    if config.execution.target_partitions != 1 {
        return false;
    }
    let mut one = true;
    let _ = plan.apply(|p| {
        one = p.output_partitioning().partition_count() == 1
            && p.repartitioned(1, config).is_ok_and(|r| r.is_none())
            && !matches!(
                p.name(),
                "RepartitionExec"
                    | "CoalescePartitionsExec"
                    | "SortPreservingMergeExec"
                    | "WindowAggExec"
                    | "BoundedWindowAggExec"
                    | "UnionExec"
                    | "InterleaveExec"
            );
        Ok(if one {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    one
}

#[cfg(debug_assertions)]
fn assert_unchanged_by_distribution(
    plan: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<()> {
    use datafusion_physical_plan::displayable;
    let after = DistributionContext::new_default(Arc::clone(plan))
        .transform_up(|c| ensure_distribution(c, config))
        .data()?
        .plan;
    let (a, b) = (
        displayable(plan.as_ref()).indent(true).to_string(),
        displayable(after.as_ref()).indent(true).to_string(),
    );
    assert_eq!(a, b, "the distribution pass changed a one-partition plan");
    Ok(())
}
