//! `TotalOfGroups`: a `HAVING` that compares each group with the total over the same rows sums the
//! groups for that total, and the groups are computed once.
//!
//! TPC-H Q11 keeps the parts whose value is over a share of the value of all of them:
//! `GROUP BY ps_partkey HAVING sum(v) > (SELECT sum(v) * 0.0001 FROM <the same join>)`. The
//! subquery joined `partsupp`, `supplier` and `nation` again, so the statement scanned `partsupp`
//! twice, and each scan spent 430 ms of its 650 ms of CPU at SF10 in the row filter its partitioned
//! join gave it (#433). The total of the rows is the total of the groups' sums, so the subquery
//! now sums the aggregate's own groups, which are defined once ([`super::sharing`]) and read by
//! both: one join, one scan of `partsupp`.
//!
//! Only `checked_sum` is taken apart this way: its sums are exact and wide enough that no partial
//! sum overflows ([`super::checked`]), so the total is the same in any order and of the same type.
//! A float's sum would change with the order of its terms, and is left alone. The subquery must be
//! an aggregate with no groups over a plan equal to the one the `HAVING`'s aggregate groups, with
//! no outer reference and nothing volatile, every one of its aggregates one of the `HAVING`'s.
//! Grouping sets are left alone: a rollup's groups hold its total too, so they sum to more.

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::logical_plan::{Aggregate, Filter, Projection, Subquery};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;

use super::sharing::{Definitions, with_definitions};

#[derive(Debug, Default)]
pub struct TotalOfGroups;

impl AnalyzerRule for TotalOfGroups {
    fn name(&self) -> &str {
        "total_of_groups"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // A recursive term runs once per round over that round's rows: nothing in it is computed
        // once for the statement.
        let mut recursive = false;
        plan.apply_with_subqueries(|n| {
            recursive = matches!(n, LogicalPlan::RecursiveQuery(_));
            Ok(if recursive {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        if recursive {
            return Ok(plan);
        }
        with_definitions(plan, &mut |p, defs| {
            p.transform_up_with_subqueries(|n| rewrite(n, defs))
                .map(|t| t.data)
        })
    }
}

fn rewrite(n: LogicalPlan, defs: &mut Definitions) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Filter(f) = &n else {
        return Ok(Transformed::no(n));
    };
    let LogicalPlan::Aggregate(groups) = f.input.as_ref() else {
        return Ok(Transformed::no(n));
    };
    // Grouping sets hold rows of several groupings, a rollup's total among them, so their sum is
    // not the total of the rows.
    if groups.group_expr.is_empty()
        || groups
            .group_expr
            .iter()
            .any(|e| matches!(e, Expr::GroupingSet(_)))
        || !computed_once(&f.input)
    {
        return Ok(Transformed::no(n));
    }
    // Each subquery of the predicate that totals the same rows, rewritten over `reference`, which
    // is defined only once one is found.
    let mut reference = None;
    let predicate = f.predicate.clone().transform_down(|e| {
        let Expr::ScalarSubquery(s) = &e else {
            return Ok(Transformed::no(e));
        };
        let Some(total) = totals(s, groups) else {
            return Ok(Transformed::no(e));
        };
        // Planned over the aggregate itself first, so nothing is defined for a total that is not
        // rewritten; the reference has the same columns.
        if over_groups(&total, groups, LogicalPlan::Aggregate(groups.clone()))?.is_none() {
            return Ok(Transformed::no(e));
        }
        let r = reference
            .get_or_insert_with(|| defs.define(LogicalPlan::Aggregate(groups.clone())))
            .clone();
        match over_groups(&total, groups, r)? {
            Some(plan) => Ok(Transformed::new(
                Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(plan),
                    outer_ref_columns: vec![],
                    spans: s.spans.clone(),
                }),
                true,
                TreeNodeRecursion::Jump,
            )),
            None => Ok(Transformed::no(e)),
        }
    })?;
    let Some(r) = reference else {
        return Ok(Transformed::no(n));
    };
    let filter = Filter::try_new(predicate.data, Arc::new(r))?;
    Ok(Transformed::yes(LogicalPlan::Filter(filter)))
}

/// What a subquery holds once it is known to total `groups`' input: its projection's
/// expressions, and its aggregate with no groups.
struct Total<'a> {
    projection: &'a Projection,
    aggregate: &'a Aggregate,
}

/// The subquery's projection and aggregate if it is `SELECT <exprs> FROM (SELECT <sums> FROM
/// <input>)`, uncorrelated, with no groups, over the very input `groups` groups, each of its sums
/// one that `groups` computes.
fn totals<'a>(s: &'a Subquery, groups: &Aggregate) -> Option<Total<'a>> {
    if !s.outer_ref_columns.is_empty() {
        return None;
    }
    let LogicalPlan::Projection(projection) = s.subquery.as_ref() else {
        return None;
    };
    let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() else {
        return None;
    };
    if !aggregate.group_expr.is_empty() || aggregate.input != groups.input {
        return None;
    }
    if projection
        .expr
        .iter()
        .any(|e| e.is_volatile() || matches!(e, Expr::ScalarSubquery(_)))
    {
        return None;
    }
    aggregate
        .aggr_expr
        .iter()
        .all(|a| exact_sum(a) && position(groups, a).is_some())
        .then_some(Total {
            projection,
            aggregate,
        })
}

/// Whether `input` has no outer reference and nothing volatile, so that one run of it is every run.
fn computed_once(input: &LogicalPlan) -> bool {
    let mut once = true;
    let _ = input.apply_with_subqueries(|n| {
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
    match unalias(e) {
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

fn unalias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => unalias(&a.expr),
        e => e,
    }
}

/// The index of `groups`' output column that holds `aggregate`.
fn position(groups: &Aggregate, aggregate: &Expr) -> Option<usize> {
    let want = unalias(aggregate);
    groups
        .aggr_expr
        .iter()
        .position(|a| unalias(a) == want)
        .map(|i| groups.schema.fields().len() - groups.aggr_expr.len() + i)
}

/// The total's plan with each of its sums taken over `groups`' column for that sum, read from
/// `reference`; `None` if a sum would come out of another type than it did.
fn over_groups(
    total: &Total<'_>,
    groups: &Aggregate,
    reference: LogicalPlan,
) -> Result<Option<LogicalPlan>> {
    let mut sums = Vec::with_capacity(total.aggregate.aggr_expr.len());
    for (i, a) in total.aggregate.aggr_expr.iter().enumerate() {
        let (Some(at), Expr::AggregateFunction(f)) = (position(groups, a), unalias(a)) else {
            return Ok(None);
        };
        let column = Expr::Column(Column::from(groups.schema.qualified_field(at)));
        let name = total.aggregate.schema.field(i).name().clone();
        sums.push(
            Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::clone(&f.func),
                vec![column],
                false,
                None,
                vec![],
                None,
            ))
            .alias(name),
        );
    }
    let aggregate = Aggregate::try_new(Arc::new(reference), vec![], sums)?;
    let same_types = aggregate
        .schema
        .fields()
        .iter()
        .zip(total.aggregate.schema.fields())
        .all(|(a, b)| a.data_type() == b.data_type());
    if !same_types {
        return Ok(None);
    }
    let projection = Projection::try_new(
        total.projection.expr.clone(),
        Arc::new(LogicalPlan::Aggregate(aggregate)),
    )?;
    Ok(Some(LogicalPlan::Projection(projection)))
}
