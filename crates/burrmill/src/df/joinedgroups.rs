//! `JoinedGroups`: an aggregate joined on its group key computes only the groups the join can keep
//! (#433).
//!
//! Decorrelating TPC-H Q17's scalar subquery averages `l_quantity` for every part, 2M of them over
//! all 60M `lineitem` rows at SF10, and then joins the averages to the 2,040 parts of one brand and
//! container. The other 99.9% of the groups are computed and dropped: at SF10 the aggregates took
//! 10.9 of the plan's CPU seconds and the join that dropped them built its table on the 2M. DuckDB
//! computes the subquery for the outer query's parts alone.
//!
//! The rule finds a join on `a = b` where `b` is, through aliases and projections, a column the
//! aggregate on one side groups by, `g`, and every row of the other side has an `a` that some row
//! of a filtered table scan holds as `k`: an inner or semi join to that scan on `a = k`, a filter,
//! an alias. A group whose `g` is not among the scan's `k` meets no row of the other side, so the
//! join drops it, and because the groups are split by `g`, keeping only the rows whose `g` is
//! among the `k` leaves every other group as it was. The aggregate's input becomes a semi join on
//! the scan's `k`, built on the scan, whose dynamic filter then reaches the input's own scan.
//!
//! Only where the join drops the aggregate's unmatched rows: an inner or semi join, an outer join
//! on its other side, or an anti join that keeps the other side. NULL equal to NULL keeps the plan,
//! as does a volatile filter, which a second scan would not repeat, or a key whose type differs.
//! The scan is read twice, so it must read at most a quarter of what the aggregate's input does,
//! by the catalogue's file sizes; unmeasured, the plan is kept. A semi join added once is marked by
//! its alias, so the optimizer's later passes do not add another.

use std::sync::Arc;

use datafusion_catalog::default_table_source::source_as_provider;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, NullEquality, Result};
use datafusion_expr::logical_plan::{Aggregate, Join, JoinConstraint, Projection, SubqueryAlias};
use datafusion_expr::{Expr, JoinType, LogicalPlan};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

/// The alias of an added semi join's keys, and its mark.
const KEYS: &str = "__joined_groups";
const KEY: &str = "__joined_group_key";

#[derive(Debug, Default)]
pub struct JoinedGroups;

impl OptimizerRule for JoinedGroups {
    fn name(&self) -> &str {
        "joined_groups"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(j) = &plan else {
            return Ok(Transformed::no(plan));
        };
        if j.null_equality != NullEquality::NullEqualsNothing || j.null_aware {
            return Ok(Transformed::no(plan));
        }
        use JoinType::*;
        // Which sides lose the rows that match nothing on the other.
        let (left_drops, right_drops) = match j.join_type {
            Inner | LeftSemi | RightSemi => (true, true),
            Left | LeftAnti | LeftMark => (false, true),
            Right | RightAnti | RightMark => (true, false),
            Full => (false, false),
        };
        for (l, r) in &j.on {
            let (Expr::Column(l), Expr::Column(r)) = (l, r) else {
                continue;
            };
            if right_drops && let Some(new) = reduce(&j.right, r, &j.left, l, config)? {
                return Ok(Transformed::yes(LogicalPlan::Join(Join {
                    right: Arc::new(new),
                    ..j.clone()
                })));
            }
            if left_drops && let Some(new) = reduce(&j.left, l, &j.right, r, config)? {
                return Ok(Transformed::yes(LogicalPlan::Join(Join {
                    left: Arc::new(new),
                    ..j.clone()
                })));
            }
        }
        Ok(Transformed::no(plan))
    }
}

/// `side` with the aggregate whose group key `b` is computed only for the keys `other`'s `a` can
/// take, or `None` where it is not such an aggregate or no scan bounds `a`.
fn reduce(
    side: &LogicalPlan,
    b: &Column,
    other: &LogicalPlan,
    a: &Column,
    config: &dyn OptimizerConfig,
) -> Result<Option<LogicalPlan>> {
    let Some((agg, g)) = grouped_by(side, b) else {
        return Ok(None);
    };
    if marked(&agg.input) {
        return Ok(None);
    }
    let Some((scan, k)) = keys_of(other, a) else {
        return Ok(None);
    };
    let (Ok(kf), Ok(gf)) = (
        scan.schema().qualified_field_from_column(&k),
        agg.input.schema().qualified_field_from_column(&g),
    ) else {
        return Ok(None);
    };
    if kf.1.data_type() != gf.1.data_type() {
        return Ok(None);
    }
    match (bytes(&scan), bytes(&agg.input)) {
        (Some(s), Some(i)) if s.saturating_mul(4) <= i => {}
        _ => return Ok(None),
    }
    let keys = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(LogicalPlan::Projection(Projection::try_new(
            vec![Expr::Column(k).alias(KEY)],
            Arc::new(scan),
        )?)),
        config.alias_generator().next(KEYS),
    )?);
    let key = Column::from(keys.schema().qualified_field(0));
    // Built on the keys; its output is the aggregate's input, columns and all.
    let semi = LogicalPlan::Join(Join::try_new(
        Arc::new(keys),
        Arc::clone(&agg.input),
        vec![(Expr::Column(key), Expr::Column(g))],
        None,
        JoinType::RightSemi,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let reduced = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(semi),
        agg.group_expr.clone(),
        agg.aggr_expr.clone(),
    )?);
    rebuild(side, b, reduced).map(Some)
}

/// The aggregate `b` is a group key of, beneath aliases, projections and filters, and the input
/// column it groups by.
fn grouped_by<'a>(plan: &'a LogicalPlan, b: &Column) -> Option<(&'a Aggregate, Column)> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            let i = agg.schema.index_of_column(b).ok()?;
            if i >= agg.group_expr.len() || agg.group_expr.iter().any(is_grouping_set) {
                return None;
            }
            match unaliased(&agg.group_expr[i]) {
                Expr::Column(g) => Some((agg, g.clone())),
                _ => None,
            }
        }
        LogicalPlan::Filter(f) => grouped_by(&f.input, b),
        _ => {
            let (input, c) = passed_through(plan, b)?;
            grouped_by(input, &c)
        }
    }
}

/// `plan` with the aggregate [`grouped_by`] finds replaced by `reduced`, whose columns are its own.
fn rebuild(plan: &LogicalPlan, b: &Column, reduced: LogicalPlan) -> Result<LogicalPlan> {
    let (input, c) = match plan {
        LogicalPlan::Aggregate(_) => return Ok(reduced),
        LogicalPlan::Filter(f) => (f.input.as_ref(), b.clone()),
        _ => passed_through(plan, b).expect("found by grouped_by"),
    };
    let new = rebuild(input, &c, reduced)?;
    plan.with_new_exprs(plan.expressions(), vec![new])
}

/// The input of a projection or alias and the column of it that `c` is.
fn passed_through<'a>(plan: &'a LogicalPlan, c: &Column) -> Option<(&'a LogicalPlan, Column)> {
    match plan {
        LogicalPlan::Projection(p) => {
            let i = p.schema.index_of_column(c).ok()?;
            match unaliased(&p.expr[i]) {
                Expr::Column(c) => Some((&p.input, c.clone())),
                _ => None,
            }
        }
        LogicalPlan::SubqueryAlias(s) => {
            let i = s.schema.index_of_column(c).ok()?;
            Some((&s.input, Column::from(s.input.schema().qualified_field(i))))
        }
        _ => None,
    }
}

/// A filtered scan, and its column, holding every value `a` takes in `plan`'s rows but NULL.
fn keys_of(plan: &LogicalPlan, a: &Column) -> Option<(LogicalPlan, Column)> {
    match plan {
        LogicalPlan::TableScan(t) => (plan.schema().has_column(a)
            && !t.filters.is_empty()
            && !t.filters.iter().any(Expr::is_volatile))
        .then(|| (plan.clone(), a.clone())),
        LogicalPlan::Filter(f) => match f.input.as_ref() {
            LogicalPlan::TableScan(t)
                if plan.schema().has_column(a)
                    && !f.predicate.is_volatile()
                    && !t.filters.iter().any(Expr::is_volatile) =>
            {
                Some((plan.clone(), a.clone()))
            }
            _ => keys_of(&f.input, a),
        },
        LogicalPlan::Join(j) => {
            let in_left = j.left.schema().has_column(a);
            let side = if in_left { &j.left } else { &j.right };
            if let Some(found) = keys_of(side, a) {
                return Some(found);
            }
            // Through the equality only where every row out has met a row of the other side.
            if !matches!(
                j.join_type,
                JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
            ) {
                return None;
            }
            j.on.iter().find_map(|(l, r)| match (l, r) {
                (Expr::Column(l), Expr::Column(r)) if in_left && l == a => keys_of(&j.right, r),
                (Expr::Column(l), Expr::Column(r)) if !in_left && r == a => keys_of(&j.left, l),
                _ => None,
            })
        }
        _ => {
            let (input, c) = passed_through(plan, a)?;
            keys_of(input, &c)
        }
    }
}

/// Whether `plan` already holds a semi join this rule added.
fn marked(plan: &LogicalPlan) -> bool {
    plan.exists(|p| {
        Ok(matches!(p, LogicalPlan::SubqueryAlias(s) if s.alias.table().starts_with(KEYS)))
    })
    .unwrap_or(true)
}

/// What every scan in `plan` reads, by the catalogue's file sizes; `None` if one is unmeasured.
fn bytes(plan: &LogicalPlan) -> Option<u64> {
    let mut total = 0u64;
    let mut measured = true;
    let _ = plan.apply(|p| {
        if let LogicalPlan::TableScan(s) = p {
            match source_as_provider(&s.source)
                .ok()
                .and_then(|t| super::smallinputs::table_bytes(&t))
            {
                Some(b) => total = total.saturating_add(b),
                None => measured = false,
            }
        }
        Ok(if measured {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    measured.then_some(total)
}

fn unaliased(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => unaliased(&a.expr),
        _ => e,
    }
}

fn is_grouping_set(e: &Expr) -> bool {
    matches!(e, Expr::GroupingSet(_))
}
