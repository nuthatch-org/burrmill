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
//!
//! A filter keeps part of what its scan reads, and the build side holds only what it keeps: Q12
//! filters `lineitem` to 310.8K of its 60M rows at SF10 and joins `orders` to them, and on bytes
//! read alone the join built on all 15M orders, a 930 MB table, where DuckDB builds on the lineitem
//! rows (#433). Without statistics a filter's share is guessed from its predicate as System R did
//! ([`kept`]): a tenth for an equality, a third for a range on a column or a comparison of two, the
//! two bounds of one column's range counted once. A predicate it cannot read keeps everything, so a
//! side is never taken to be smaller than it reads on a guess it did not make. The guess overrides
//! the bytes read only where it is decisive ([`builds_larger`]); Q12's lineitem is guessed at a
//! 36th of its orders at SF10. Taking every filter to keep DataFusion's default 20% was tried and
//! not kept: it built Q13's join on the 15M orders of a `NOT LIKE` rather than on 1.5M customers,
//! and Q7's on two years of `lineitem` rather than on the orders.
//!
//! Where the guess overrode the bytes, an inner join also collects its build side, being guessed
//! small. Partitioned, its dynamic filter on the probe's scan is a `CASE` over each row's
//! repartition hash: in Q12 at SF10 2.37 s of the `orders` scan's CPU against 0.30 s collected,
//! one `EXPLAIN ANALYZE` each at a 1-minute load of 50.
//!
//! A semi join is collected too where its build side is guessed to keep at most an eighth of what
//! its probe side reads ([`filters_probe`]). Q4 builds on the 573.7K orders of one quarter and
//! probes all 60M `lineitem` rows; partitioned, the `CASE` its dynamic filter put in that scan cost
//! 1.77 s of row-filter CPU at SF10, against 0.39 s collected (#433). A semi join holds only its
//! build side's rows or keys, so collecting it moves no more into memory than partitioning did.
//!
//! A `HAVING` is guessed to keep a tenth of its aggregate's input as groups, before its own
//! predicate's share ([`HAVING_GROUPS`]). Q18's `IN (SELECT l_orderkey FROM lineitem GROUP BY
//! l_orderkey HAVING sum(l_quantity) > 300)` keeps 624 of 15M orders at SF10, and was guessed at a
//! third of `lineitem`'s 2.55 GB, so its semi join stayed partitioned. Once
//! [`super::havinggroups`] joins those groups to the orders in its place, that guess built the join
//! on all 15M orders and their customers, and the statement was refused under 2 GiB (#433).

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{JoinType, Result};
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, LikeExpr, Literal,
    NotExpr, TryCastExpr,
};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_physical_plan::filter::FilterExec;
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

/// Whether a semi join building on `build` is collected: its build side is guessed to keep at most
/// an eighth of what `probe` reads.
fn filters_probe(build: &Arc<dyn ExecutionPlan>, probe: &Arc<dyn ExecutionPlan>) -> bool {
    matches!(
        (bytes_kept(build), bytes_read(probe)),
        (Some(b), Some(p)) if b.saturating_mul(DECISIVE as u64) <= p
    )
}

/// How many times smaller a side's guessed share must leave it for the guess to decide.
const DECISIVE: f64 = 8.0;

/// Whether `build` is the larger side, and whether a guess said so against the bytes read: by the
/// bytes each reads, unless [`bytes_kept`] guesses one under an eighth of the other. One constant
/// can be five times off, `o_orderstatus = 'F'` keeping half the orders and not a tenth; with the
/// guess deciding at any margin, Q10 built on the 15M `lineitem` rows of `l_returnflag = 'R'`, a
/// quarter of them, rather than on 570K joined orders.
fn builds_larger(build: &Arc<dyn ExecutionPlan>, probe: &Arc<dyn ExecutionPlan>) -> (bool, bool) {
    let (Some(b), Some(p)) = (bytes_read(build), bytes_read(probe)) else {
        return (false, false);
    };
    let by_bytes = b > p;
    let (Some(kb), Some(kp)) = (bytes_kept(build), bytes_kept(probe)) else {
        return (by_bytes, false);
    };
    let (kb, kp) = (kb as f64, kp as f64);
    let larger = if kp >= DECISIVE * kb {
        false
    } else if kb >= DECISIVE * kp {
        true
    } else {
        by_bytes
    };
    (larger, larger != by_bytes)
}

/// The bytes `p` reads, [`bytes_read`], cut by each filter between it and its scans to the share
/// [`kept`] guesses that filter keeps; `None` if a scan cannot be measured.
fn bytes_kept(p: &Arc<dyn ExecutionPlan>) -> Option<u64> {
    if let Some(f) = p.downcast_ref::<FilterExec>() {
        let groups = if having(f.input()) {
            HAVING_GROUPS
        } else {
            1.0
        };
        return bytes_kept(f.input()).map(|b| (b as f64 * groups * kept(f.predicate())) as u64);
    }
    if let Some(definition) = super::sharing::definition(p) {
        return bytes_kept(&definition);
    }
    let children = p.children();
    if children.is_empty() {
        return bytes_read(p);
    }
    children.into_iter().map(bytes_kept).sum()
}

/// The share of its input a `HAVING`'s aggregate is guessed to keep as groups. Without statistics
/// a grouping's distinct count is a guess too; a tenth is the equality's. Taking every grouped
/// aggregate to keep a tenth was tried and not kept: it changed the plans of Q2 and Q17 too, and
/// those were not measured with it. With only the `HAVING`'s, of TPC-H's 22 plans only Q18's
/// changes, at SF1 and at SF10.
const HAVING_GROUPS: f64 = EQUAL;

/// Whether `p`, the input of a filter, is an aggregate with groups that the filter is the `HAVING`
/// of: its final step, whose rows are the groups.
fn having(p: &Arc<dyn ExecutionPlan>) -> bool {
    p.downcast_ref::<AggregateExec>()
        .is_some_and(|a| !a.group_expr().is_empty() && !matches!(a.mode(), AggregateMode::Partial))
}

/// The share of rows a predicate is guessed to keep, 0 to 1: the product of its conjuncts', where
/// the range comparisons of one column with constants count as one range and a conjunct it cannot
/// read keeps everything.
pub(super) fn kept(predicate: &Arc<dyn PhysicalExpr>) -> f64 {
    let mut conjuncts = Vec::new();
    split_and(predicate, &mut conjuncts);
    let mut ranged: Vec<&Column> = Vec::new();
    let mut share = 1.0;
    for c in conjuncts {
        if let Some(col) = ranged_column(c) {
            if ranged.contains(&col) {
                continue;
            }
            ranged.push(col);
        }
        share *= kept_one(c).unwrap_or(1.0);
    }
    share
}

fn split_and<'a>(e: &'a Arc<dyn PhysicalExpr>, out: &mut Vec<&'a Arc<dyn PhysicalExpr>>) {
    match e.as_ref().downcast_ref::<BinaryExpr>() {
        Some(b) if *b.op() == Operator::And => {
            split_and(b.left(), out);
            split_and(b.right(), out);
        }
        _ => out.push(e),
    }
}

/// The column a range comparison bounds by a constant: `col < 3`, `CAST(col AS t) >= '1994-01-01'`.
fn ranged_column(e: &Arc<dyn PhysicalExpr>) -> Option<&Column> {
    let b = e.as_ref().downcast_ref::<BinaryExpr>()?;
    if !matches!(
        b.op(),
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        return None;
    }
    match (column(b.left()), column(b.right())) {
        (Some(c), None) if constant(b.right()) => Some(c),
        (None, Some(c)) if constant(b.left()) => Some(c),
        _ => None,
    }
}

fn column(e: &Arc<dyn PhysicalExpr>) -> Option<&Column> {
    if let Some(c) = e.as_ref().downcast_ref::<CastExpr>() {
        return column(c.expr());
    }
    if let Some(c) = e.as_ref().downcast_ref::<TryCastExpr>() {
        return column(c.expr());
    }
    e.as_ref().downcast_ref::<Column>()
}

fn constant(e: &Arc<dyn PhysicalExpr>) -> bool {
    if let Some(c) = e.as_ref().downcast_ref::<CastExpr>() {
        return constant(c.expr());
    }
    e.as_ref().is::<Literal>()
}

const EQUAL: f64 = 0.1;
const RANGE: f64 = 1.0 / 3.0;

/// The share one conjunct is guessed to keep, or `None` where it cannot say.
fn kept_one(e: &Arc<dyn PhysicalExpr>) -> Option<f64> {
    if let Some(b) = e.as_ref().downcast_ref::<BinaryExpr>() {
        return match b.op() {
            Operator::And => Some(kept(e)),
            Operator::Or => Some((kept(b.left()) + kept(b.right())).min(1.0)),
            Operator::Eq => Some(EQUAL),
            Operator::NotEq => Some(1.0 - EQUAL),
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => Some(RANGE),
            _ => None,
        };
    }
    if let Some(l) = e.as_ref().downcast_ref::<LikeExpr>() {
        return Some(if l.negated() { 1.0 - EQUAL } else { EQUAL });
    }
    if let Some(i) = e.as_ref().downcast_ref::<InListExpr>() {
        let any = (EQUAL * i.list().len() as f64).min(1.0);
        return Some(if i.negated() { 1.0 - any } else { any });
    }
    if e.as_ref().is::<IsNullExpr>() {
        return Some(EQUAL);
    }
    if e.as_ref().is::<IsNotNullExpr>() {
        return Some(1.0 - EQUAL);
    }
    // Only a single comparison is negated: the share of `NOT (a AND b)` is not one less a guess
    // that may be too large.
    let n = e.as_ref().downcast_ref::<NotExpr>()?;
    match n.arg().as_ref().downcast_ref::<BinaryExpr>() {
        Some(b) if matches!(b.op(), Operator::And | Operator::Or) => None,
        _ => kept_one(n.arg()).map(|s| 1.0 - s),
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
            let (swap, guessed) = builds_larger(j.left(), j.right());
            let (build, probe) = if swap {
                (j.right(), j.left())
            } else {
                (j.left(), j.right())
            };
            let mode = match j.partition_mode() {
                PartitionMode::Partitioned if guessed && *j.join_type() == JoinType::Inner => {
                    PartitionMode::CollectLeft
                }
                PartitionMode::Partitioned if collect && filters_probe(build, probe) => {
                    PartitionMode::CollectLeft
                }
                m => *m,
            };
            if swap {
                Ok(Transformed::yes(j.swap_inputs(mode)?))
            } else if mode != *j.partition_mode() {
                Ok(Transformed::yes(Arc::new(
                    j.builder().with_partition_mode(mode).build()?,
                )))
            } else {
                Ok(Transformed::no(p))
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
