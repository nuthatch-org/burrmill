//! `SpillingJoins`: under a budget that may spill, a hash join whose build side may not fit is a
//! sort-merge join.
//!
//! DataFusion's hash join holds its whole build side in memory and cannot spill it; its sort-merge
//! join reads two inputs sorted on the keys, which an external sort spills, and spills the one key
//! group it buffers. TPC-H Q21 at SF10 builds two partitioned hash joins on `lineitem`'s 60 million
//! rows, its `EXISTS` and its `NOT EXISTS`, and was refused at the 1,792 MB a 2 GiB budget's
//! statements may hold, every byte of it in `HashJoinInput`, with a spill directory set or not
//! (#383).
//!
//! Statistics are off, so the rows a build side holds are unknown; the bytes its scans read from
//! disk, every column, are known ([`super::smallinputs::bytes_read`]), and are a rough measure.
//! At SF10 Q21's `orders` side, 647 MB of files filtered to one column, is built in 192 MB
//! (`build_mem_used`), and Q18's, the same files at four columns unfiltered, in 193 MB in each of
//! eight partitions. So a build side whose files hold more than half the statement pool, or that
//! cannot be measured, gets the join that can spill, and one under that keeps its hash join, faster
//! wherever it fits. Q18's is under, and fits as it did without a spill directory.
//!
//! A build side grouped by an aggregate keeps its hash join too. Its rows are groups, which the
//! files do not measure: Q18's `IN` builds on 624 of the 15 million orders it groups `lineitem`'s
//! 2.4 GB into, those whose quantity passes its `HAVING`. And the aggregate spills by itself. As a
//! sort-merge join, Q18 under 2 GiB was refused: its sorts could get no batch beside what its other
//! join had built.
//!
//! The rule is placed only for a budget with a spill directory; without one a sort-merge join could
//! only refuse later, and plans stay as they were. It runs straight after the choice of build
//! sides, so the sorts and repartitions the sort-merge join needs are placed by the passes after
//! it. A null-aware anti join, `NOT IN` over a nullable column, stays a hash join, since the
//! sort-merge join does not keep its semantics; so does one whose build side is the few rows a
//! limit kept ([`super::sharing::few_rows`]).

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_session::PhysicalOptimizerRule;

use super::sharing::few_rows;
use super::smallinputs::bytes_read;

#[derive(Debug)]
pub(super) struct SpillingJoins {
    /// Bytes a build side's files may hold and keep its hash join: half the statement pool.
    pub(super) build_bytes: u64,
}

impl SpillingJoins {
    /// For a statement pool of `pool_bytes`.
    pub(super) fn new(pool_bytes: usize) -> Self {
        Self {
            build_bytes: pool_bytes as u64 / 2,
        }
    }

    fn fits(&self, build: &Arc<dyn ExecutionPlan>) -> bool {
        few_rows(build)
            || grouped(build)
            || bytes_read(build).is_some_and(|b| b <= self.build_bytes)
    }
}

/// Whether an aggregate's groups are what `p` answers rows from.
fn grouped(p: &Arc<dyn ExecutionPlan>) -> bool {
    p.is::<AggregateExec>() || p.children().into_iter().any(grouped)
}

impl PhysicalOptimizerRule for SpillingJoins {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let Some(j) = p.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(p));
            };
            if j.null_aware || j.on().is_empty() || self.fits(j.left()) {
                return Ok(Transformed::no(p));
            }
            let smj: Arc<dyn ExecutionPlan> = Arc::new(SortMergeJoinExec::try_new(
                Arc::clone(j.left()),
                Arc::clone(j.right()),
                j.on().to_vec(),
                j.filter().cloned(),
                *j.join_type(),
                vec![Default::default(); j.on().len()],
                j.null_equality(),
            )?);
            // Both answer the join's columns in the same order; the hash join's own projection,
            // pushed into it, is put back above.
            if smj.schema().fields() != j.join_schema().fields() {
                return Ok(Transformed::no(p));
            }
            let out = match &j.projection {
                None => smj,
                Some(cols) => {
                    let schema = smj.schema();
                    let exprs: Vec<_> = cols
                        .iter()
                        .map(|&i| {
                            let name = schema.field(i).name().clone();
                            (
                                Arc::new(Column::new(&name, i))
                                    as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
                                name,
                            )
                        })
                        .collect();
                    Arc::new(ProjectionExec::try_new(exprs, smj)?)
                }
            };
            Ok(Transformed::yes(out))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "spilling_joins"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
