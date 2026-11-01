//! `TopKFilters`: a top-k whose dynamic filter would reach the scan wrong is given a fresh one.
//!
//! The scan's copy of a top-k filter finds the sort keys in each new predicate by equality and swaps
//! in what they read at the scan, bottom-up (DataFusion 55, `DynamicFilterPhysicalExpr`). A key that
//! contains another, `CASE WHEN b % 2 = 0 THEN p ... END, b`, has `b` swapped inside it first, is
//! then no longer equal to itself, and keeps `p` at the sort's column index: at the scan that index
//! was `b`, and row groups holding the answer were skipped. Such a sort gets a filter nothing else
//! holds, and the scan's copy stays `true`.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_session::PhysicalOptimizerRule;

#[derive(Debug, Default)]
pub struct TopKFilters;

fn contains(e: &Arc<dyn PhysicalExpr>, part: &Arc<dyn PhysicalExpr>) -> bool {
    let mut found = false;
    let _ = e.apply(|n| {
        found = !Arc::ptr_eq(n, e) && n.as_ref() == part.as_ref();
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

fn remaps_wrongly(s: &SortExec) -> bool {
    if s.fetch().is_none() {
        return false;
    }
    let keys: Vec<&Arc<dyn PhysicalExpr>> = s.expr().iter().map(|k| &k.expr).collect();
    keys.iter().any(|a| keys.iter().any(|b| contains(a, b)))
}

impl PhysicalOptimizerRule for TopKFilters {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let Some(s) = p.downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(p));
            };
            if !remaps_wrongly(s) {
                return Ok(Transformed::no(p));
            }
            let sort = SortExec::new(s.expr().clone(), Arc::clone(s.input()))
                .with_preserve_partitioning(s.preserve_partitioning())
                .with_fetch(s.fetch());
            Ok(Transformed::yes(Arc::new(sort) as Arc<dyn ExecutionPlan>))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "topk_filters"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `TopKMergeOrder`: a top-k below a merge sorts by the merge's whole order.
///
/// Pushed below the merge, the sort drops each key that is constant within every partition, such as
/// a `UNION ALL` arm's literal. Its partitions share one dynamic filter, so the first to fill its
/// heap ruled out the others' rows by the keys left, rows the dropped key put first (nuthatch #2583).
/// Which partition fills first is the scheduler's choice, so the answer was too.
#[derive(Debug, Default)]
pub struct TopKMergeOrder;

impl PhysicalOptimizerRule for TopKMergeOrder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let Some(m) = p.downcast_ref::<SortPreservingMergeExec>() else {
                return Ok(Transformed::no(p));
            };
            let Some(s) = m.input().downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(p));
            };
            if s.fetch().is_none() || s.expr().starts_with(m.expr()) {
                return Ok(Transformed::no(p));
            }
            let mut order = m.expr().clone();
            order.extend(s.expr().iter().cloned());
            let sort = SortExec::new(order, Arc::clone(s.input()))
                .with_preserve_partitioning(s.preserve_partitioning())
                .with_fetch(s.fetch());
            let merge = Arc::clone(&p).with_new_children(vec![Arc::new(sort)])?;
            Ok(Transformed::yes(merge))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "topk_merge_order"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
