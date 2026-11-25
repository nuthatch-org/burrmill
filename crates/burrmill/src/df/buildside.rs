//! `BuildOnSmaller`: a hash join builds its table on the input that reads less from disk.
//!
//! DataFusion builds on the left input, the first table written, and swaps only when statistics say
//! the left is larger; statistics are off here. The QoS nest writes its 2.5 GB of rows first and
//! joins three small tables to them, so each of three nested joins held every row of the large one:
//! 60 GB and killed where DuckDB answers in 512 MB. The catalogue knows what each scan reads, as
//! [`super::smallinputs`] already uses, and that is enough to choose. Swapping is DataFusion's own
//! `swap_inputs`, which `JoinSelection` uses, so no answer changes; it runs straight after that
//! rule, before repartitions and dynamic filters are placed on the join's children. Semi and anti
//! joins hold their build side the same way, so they swap too; a null-aware anti join cannot.

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{JoinType, Result};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_session::PhysicalOptimizerRule;

use super::sharing::few_rows;
use super::smallinputs::bytes_read;

#[derive(Debug, Default)]
pub struct BuildOnSmaller;

/// The semi join [`super::joinedgroups`] adds, collected on its keys where they read at most a
/// sixteenth of what it filters. Partitioned, the dynamic filter it gives its probe's scan is a
/// `CASE` over the repartition hash of every row: in TPC-H Q17 at SF10, 3.19 s of the scan's CPU
/// over 60M rows, against 0.32 s for the collected filter.
fn collect_keys(j: &HashJoinExec) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if *j.partition_mode() != PartitionMode::Partitioned
        || *j.join_type() != JoinType::RightSemi
        || j.left().schema().fields().len() != 1
        || j.left().schema().field(0).name() != super::joinedgroups::KEY
    {
        return Ok(None);
    }
    match (bytes_read(j.left()), bytes_read(j.right())) {
        (Some(keys), Some(probe)) if keys.saturating_mul(16) <= probe => Ok(Some(Arc::new(
            j.builder()
                .with_partition_mode(PartitionMode::CollectLeft)
                .build()?,
        ))),
        _ => Ok(None),
    }
}

impl PhysicalOptimizerRule for BuildOnSmaller {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let Some(j) = p.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(p));
            };
            let swappable = j.join_type().supports_swap() && !j.null_aware;
            // The rows a limit kept, or their keys, are the side to build on, whatever was read to
            // find them. A semi join's keys are also collected whole, so the side it filters is read
            // where it is rather than repartitioned. A left join over the kept rows is not: collected
            // over a few thousand rows with repeated keys it ran a hundred times slower (#1951).
            let collect = matches!(j.join_type(), JoinType::RightSemi | JoinType::LeftSemi);
            match (few_rows(j.left()), few_rows(j.right())) {
                (true, _) if !collect => return Ok(Transformed::no(p)),
                (false, true) if swappable && !collect => {
                    return Ok(Transformed::yes(j.swap_inputs(*j.partition_mode())?));
                }
                (true, _) if *j.partition_mode() == PartitionMode::Partitioned => {
                    return Ok(Transformed::yes(Arc::new(
                        j.builder()
                            .with_partition_mode(PartitionMode::CollectLeft)
                            .build()?,
                    )));
                }
                (true, _) => return Ok(Transformed::no(p)),
                (false, true) if swappable => {
                    return Ok(Transformed::yes(j.swap_inputs(PartitionMode::CollectLeft)?));
                }
                _ => {}
            }
            if let Some(collected) = collect_keys(j)? {
                return Ok(Transformed::yes(collected));
            }
            if !swappable {
                return Ok(Transformed::no(p));
            }
            match (bytes_read(j.left()), bytes_read(j.right())) {
                (Some(build), Some(probe)) if build > probe => {
                    Ok(Transformed::yes(j.swap_inputs(*j.partition_mode())?))
                }
                _ => Ok(Transformed::no(p)),
            }
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "build_on_smaller"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
