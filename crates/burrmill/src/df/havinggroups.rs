//! `HavingGroups`: a semi join on an aggregate's groups, over a join to the rows those groups
//! aggregate, joins the groups instead and sums their sums (#433).
//!
//! TPC-H Q18 keeps the orders whose lines hold over 300 in all, `o_orderkey IN (SELECT l_orderkey
//! FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)`, and then joins those orders
//! to `lineitem` again and groups them to sum `l_quantity` once more. The subquery has already
//! summed every order's lines: at SF10 the statement scanned all 60M `lineitem` rows twice, the
//! second scan 3.04 s of CPU, 2.59 s of it the row filter its partitioned join gave it.
//!
//! An inner join of `P` to `L` on `a = b`, where `b` is the column the aggregate `A` over the very
//! same `L` groups by, gives each row of `P` the rows of `b`'s group. An aggregate above that sums
//! a column `x` of `L` for its groups of `P`'s rows sums, for each of them, the whole of that
//! group's `x`: the group's own sum. So `L` is replaced by `A`'s groups, joined on `a = b`, and each
//! such sum sums `A`'s sum of `x`. The semi join kept the rows of `P` whose `a` is among the groups
//! the `HAVING` keeps; the `HAVING` now filters the groups the join meets, which keeps the same
//! rows, and the semi join goes. A row of `P` that met no row of `L` met no group either.
//!
//! Only `checked_sum` is taken apart: exact and wide enough that no partial sum overflows
//! ([`super::checked`]), the sum of the groups' sums is the sum of the rows, of the same type; a
//! sum that would come out of another type keeps the plan. Every aggregate above must be such a
//! sum of one of `A`'s, and `L`'s columns may reach it only as that column or as `b`: a count or a
//! sum of `P`'s columns counts each of `P`'s rows once per row of `L` it met, which the groups do
//! not repeat. The joins must be on that one equality, NULL equal to nothing, with no other
//! condition, and nothing in `L`, `A` or the `HAVING` may be volatile or correlated.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, NullEquality, Result};
use datafusion_expr::logical_plan::{Aggregate, Filter, Join, Projection};
use datafusion_expr::{Expr, JoinType, LogicalPlan};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

#[derive(Debug, Default)]
pub struct HavingGroups;

impl OptimizerRule for HavingGroups {
    fn name(&self) -> &str {
        "having_groups"
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
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(outer) = &plan else {
            return Ok(Transformed::no(plan));
        };
        // A plan this cannot rebuild is left as it was.
        match reuse(outer) {
            Ok(Some(p)) => Ok(Transformed::yes(p)),
            Ok(None) | Err(_) => Ok(Transformed::no(plan)),
        }
    }
}

/// The groups the semi join's `HAVING` keeps, and what of them the rewrite reads.
struct Groups<'a> {
    /// `A`, the aggregate.
    aggregate: &'a Aggregate,
    /// `A`'s group key, a column of `L`.
    key: Column,
    /// The `HAVING` over `A`, if there is one, the plan the join meets.
    kept: LogicalPlan,
}

/// The outer aggregate rewritten over the groups, or `None` where its shape is not the one above.
fn reuse(outer: &Aggregate) -> Result<Option<LogicalPlan>> {
    if outer
        .group_expr
        .iter()
        .any(|e| matches!(e, Expr::GroupingSet(_)))
    {
        return Ok(None);
    }
    // Down from the outer aggregate: projections of columns, one semi join, then the inner join.
    // `above` holds every projection on the way, top down.
    let mut above: Vec<&Projection> = Vec::new();
    let mut semi = None;
    let mut at = outer.input.as_ref();
    let join = loop {
        match at {
            LogicalPlan::Projection(p) if p.expr.iter().all(|e| matches!(e, Expr::Column(_))) => {
                above.push(p);
                at = &p.input;
            }
            LogicalPlan::Join(j) if semi.is_none() && j.join_type == JoinType::LeftSemi => {
                semi = Some(j);
                at = &j.left;
            }
            LogicalPlan::Join(j) if j.join_type == JoinType::Inner => break j,
            _ => return Ok(None),
        }
    };
    let Some(semi) = semi else {
        return Ok(None);
    };
    if !plain(semi) || !plain(join) {
        return Ok(None);
    }
    let [(Expr::Column(probe), Expr::Column(_))] = semi.on.as_slice() else {
        return Ok(None);
    };
    let Some(groups) = groups_of(&semi.right) else {
        return Ok(None);
    };
    let [(Expr::Column(l), Expr::Column(r))] = join.on.as_slice() else {
        return Ok(None);
    };
    let input = groups.aggregate.input.as_ref();
    let (rows_left, a, b) = if join.right.as_ref() == input {
        (false, l, r)
    } else if join.left.as_ref() == input {
        (true, r, l)
    } else {
        return Ok(None);
    };
    if *b != groups.key || !once(input) {
        return Ok(None);
    }
    // The semi join keys the same equality: on `a`, or on `b`, which equals it in every row.
    if probe != a && probe != b {
        return Ok(None);
    }
    let rows: Vec<Column> = input.schema().columns();
    let of_rows = |e: &Expr| e.column_refs().into_iter().any(|c| rows.contains(c));
    // `L`'s columns reach the groups above only as the key.
    if outer.group_expr.iter().any(|e| {
        e.column_refs()
            .into_iter()
            .any(|c| rows.contains(c) && c != b)
    }) {
        return Ok(None);
    }
    // Each aggregate above is a sum of `L`'s column `x` that `A` sums too; `x` becomes that sum.
    let mut sums: HashMap<Column, Column> = HashMap::new();
    for e in &outer.aggr_expr {
        let Expr::AggregateFunction(f) = unaliased(e) else {
            return Ok(None);
        };
        let [Expr::Column(x)] = f.params.args.as_slice() else {
            return Ok(None);
        };
        if !exact_sum(e) || !rows.contains(x) || x == b {
            return Ok(None);
        }
        let Some(at) = groups
            .aggregate
            .aggr_expr
            .iter()
            .position(|g| exact_sum(g) && unaliased(g) == unaliased(e))
        else {
            return Ok(None);
        };
        let field = groups.aggregate.group_expr.len() + at;
        sums.insert(
            x.clone(),
            Column::from(groups.aggregate.schema.qualified_field(field)),
        );
    }
    if sums.is_empty() {
        return Ok(None);
    }
    // Between the join and the aggregate, `L`'s columns are passed whole, `x` to be renamed.
    if above
        .iter()
        .flat_map(|p| &p.expr)
        .any(|e| of_rows(e) && !matches!(e, Expr::Column(c) if c == b || sums.contains_key(c)))
    {
        return Ok(None);
    }
    let (left, right) = if rows_left {
        (groups.kept, join.right.as_ref().clone())
    } else {
        (join.left.as_ref().clone(), groups.kept)
    };
    let mut plan = LogicalPlan::Join(Join::try_new(
        Arc::new(left),
        Arc::new(right),
        join.on.clone(),
        None,
        JoinType::Inner,
        join.join_constraint,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    // Every projection met on the way down, rebuilt bottom up. The semi join's rows had its left
    // side's columns, so the projections above it read the same columns without it.
    for p in above.iter().rev() {
        let exprs = p
            .expr
            .iter()
            .map(|e| rename(e, &sums))
            .collect::<Result<Vec<_>>>()?;
        plan = LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(plan))?);
    }
    let aggr = outer
        .aggr_expr
        .iter()
        .zip(outer.schema.fields().iter().skip(outer.group_expr.len()))
        .map(|(e, field)| Ok(rename(unaliased(e), &sums)?.alias(field.name())))
        .collect::<Result<Vec<_>>>()?;
    let rewritten = Aggregate::try_new(Arc::new(plan), outer.group_expr.clone(), aggr)?;
    let same = rewritten.schema.fields().len() == outer.schema.fields().len()
        && rewritten
            .schema
            .iter()
            .zip(outer.schema.iter())
            .all(|((qa, a), (qb, b))| {
                qa == qb && a.name() == b.name() && a.data_type() == b.data_type()
            });
    Ok(same.then_some(LogicalPlan::Aggregate(rewritten)))
}

/// An equi join with no other condition, NULL equal to nothing.
fn plain(j: &Join) -> bool {
    j.filter.is_none() && j.null_equality == NullEquality::NullEqualsNothing && !j.null_aware
}

/// The semi join's build side as groups: an alias of a projection of an aggregate's one group key,
/// through at most one filter, its `HAVING`.
fn groups_of(plan: &LogicalPlan) -> Option<Groups<'_>> {
    let plan = match plan {
        LogicalPlan::SubqueryAlias(s) => s.input.as_ref(),
        p => p,
    };
    let LogicalPlan::Projection(p) = plan else {
        return None;
    };
    let [Expr::Column(c)] = p.expr.as_slice() else {
        return None;
    };
    let (aggregate, having) = match p.input.as_ref() {
        LogicalPlan::Filter(f) => match f.input.as_ref() {
            LogicalPlan::Aggregate(a) => (a, Some(f)),
            _ => return None,
        },
        LogicalPlan::Aggregate(a) => (a, None),
        _ => return None,
    };
    let [Expr::Column(key)] = aggregate.group_expr.as_slice() else {
        return None;
    };
    // The projection passes the group key.
    if Column::from(aggregate.schema.qualified_field(0)) != *c {
        return None;
    }
    if aggregate.aggr_expr.iter().any(Expr::is_volatile)
        || having.is_some_and(|f| f.predicate.is_volatile() || f.predicate.contains_outer())
    {
        return None;
    }
    let kept = match having {
        Some(f) => LogicalPlan::Filter(
            Filter::try_new(
                f.predicate.clone(),
                Arc::new(LogicalPlan::Aggregate(aggregate.clone())),
            )
            .ok()?,
        ),
        None => LogicalPlan::Aggregate(aggregate.clone()),
    };
    Some(Groups {
        aggregate,
        key: key.clone(),
        kept,
    })
}

/// Whether one run of `plan` is every run: no outer reference, nothing volatile, not recursive.
fn once(plan: &LogicalPlan) -> bool {
    let mut once = true;
    let _ = plan.apply_with_subqueries(|n| {
        once = !n.contains_outer_reference()
            && n.expressions().iter().all(|e| !e.is_volatile())
            && !matches!(n, LogicalPlan::RecursiveQuery(_));
        Ok(if once {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    once
}

/// Whether `e` is a plain `checked_sum`: one argument, not distinct, no filter, no order.
fn exact_sum(e: &Expr) -> bool {
    match unaliased(e) {
        Expr::AggregateFunction(a) => {
            a.func.name() == "checked_sum"
                && a.params.args.len() == 1
                && !a.params.distinct
                && a.params.filter.is_none()
                && a.params.order_by.is_empty()
                && a.params.null_treatment.is_none()
        }
        _ => false,
    }
}

/// `e` with each column of `sums` read as the sum it maps to.
fn rename(e: &Expr, sums: &HashMap<Column, Column>) -> Result<Expr> {
    e.clone()
        .transform(|e| match &e {
            Expr::Column(c) => match sums.get(c) {
                Some(s) => Ok(Transformed::yes(Expr::Column(s.clone()))),
                None => Ok(Transformed::no(e)),
            },
            _ => Ok(Transformed::no(e)),
        })
        .map(|t| t.data)
}

fn unaliased(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => unaliased(&a.expr),
        _ => e,
    }
}
