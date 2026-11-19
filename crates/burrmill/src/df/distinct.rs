//! `DistinctSplit`: `COUNT(DISTINCT x)` beside other aggregates, planned in two levels.
//!
//! DataFusion keeps a set per group, in both aggregation phases, for a distinct count its own
//! rewrite cannot take apart when other aggregates share the query. On graph-allocations that made
//! `deployment_signal` 32 ms against DuckDB's 8; grouped first by `(k, x)` and then by `k`, 13.
//!
//! The inner aggregate groups by the keys and `x`, with each other aggregate taken per `(k, x)`;
//! the outer groups by the keys, counts `x` (NULL not counted, as DISTINCT does not) and folds the
//! partials: counts summed, sums summed, minimum and maximum of each. A checked sum split this way
//! may refuse on a partial past 38 digits where the whole would fit. That is a refusal, never a
//! different answer. Outputs keep their names and types.

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Column, Result};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::logical_plan::{Aggregate, Projection};
use datafusion_expr::{AggregateUDF, Cast, Expr, ExprSchemable, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct DistinctSplit;

impl AnalyzerRule for DistinctSplit {
    fn name(&self) -> &str {
        "distinct_split"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        super::dag::transform_up(plan, |p, _| match &p {
            LogicalPlan::Aggregate(a) => Ok(match split(a)? {
                Some(n) => Transformed::yes(n),
                None => Transformed::no(p),
            }),
            _ => Ok(Transformed::no(p)),
        })
        .map(|t| t.data)
    }
}

fn unalias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => unalias(&a.expr),
        e => e,
    }
}

fn call(func: Arc<AggregateUDF>, args: Vec<Expr>) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        func,
        args,
        false,
        None,
        vec![],
        None,
    ))
}

fn split(a: &Aggregate) -> Result<Option<LogicalPlan>> {
    if a.group_expr
        .iter()
        .any(|g| matches!(g, Expr::GroupingSet(_)))
    {
        return Ok(None);
    }
    let mut distinct_arg: Option<&Expr> = None;
    for e in &a.aggr_expr {
        let Expr::AggregateFunction(f) = unalias(e) else {
            return Ok(None);
        };
        let p = &f.params;
        if p.filter.is_some() || !p.order_by.is_empty() {
            return Ok(None);
        }
        let name = f.func.name();
        if p.distinct {
            if name != "count" || p.args.len() != 1 {
                return Ok(None);
            }
            match distinct_arg {
                Some(x) if *x != p.args[0] => return Ok(None),
                _ => distinct_arg = Some(&p.args[0]),
            }
        } else if !matches!(name, "count" | "sum" | "checked_sum" | "min" | "max") {
            return Ok(None);
        }
    }
    let Some(x) = distinct_arg.cloned() else {
        return Ok(None);
    };
    // Only worth it, and only exercised, with something beside the distinct count.
    if a.aggr_expr
        .iter()
        .all(|e| matches!(unalias(e), Expr::AggregateFunction(f) if f.params.distinct))
    {
        return Ok(None);
    }

    let keys = a.group_expr.len();
    let mut inner_group = a.group_expr.clone();
    inner_group.push(x.alias("__distinct"));
    let mut inner_aggr = Vec::new();
    for (i, e) in a.aggr_expr.iter().enumerate() {
        let Expr::AggregateFunction(f) = unalias(e) else {
            unreachable!()
        };
        if !f.params.distinct {
            inner_aggr.push(
                call(Arc::clone(&f.func), f.params.args.clone()).alias(format!("__part_{i}")),
            );
        }
    }
    let inner = Aggregate::try_new(Arc::clone(&a.input), inner_group, inner_aggr)?;
    let inner_schema = Arc::clone(&inner.schema);
    let inner = Arc::new(LogicalPlan::Aggregate(inner));

    let col = |name: &str| Expr::Column(Column::new_unqualified(name));
    let outer_group: Vec<Expr> = (0..keys)
        .map(|j| Expr::Column(Column::from(inner_schema.qualified_field(j))))
        .collect();
    let sum = datafusion_functions_aggregate::sum::sum_udaf();
    let count = datafusion_functions_aggregate::count::count_udaf();
    let mut outer_aggr = Vec::new();
    let mut counted = Vec::new();
    for (i, e) in a.aggr_expr.iter().enumerate() {
        let Expr::AggregateFunction(f) = unalias(e) else {
            unreachable!()
        };
        let part = format!("__part_{i}");
        let (expr, recount) = if f.params.distinct {
            (call(Arc::clone(&count), vec![col("__distinct")]), false)
        } else {
            match f.func.name() {
                "count" => (call(Arc::clone(&sum), vec![col(&part)]), true),
                _ => (call(Arc::clone(&f.func), vec![col(&part)]), false),
            }
        };
        outer_aggr.push(expr.alias(format!("__out_{i}")));
        counted.push(recount);
    }
    let outer = Aggregate::try_new(inner, outer_group, outer_aggr)?;
    let outer = Arc::new(LogicalPlan::Aggregate(outer));

    // The original aggregate's schema, name for name and type for type.
    let mut exprs = Vec::with_capacity(a.schema.fields().len());
    for j in 0..a.schema.fields().len() {
        let (q, field) = a.schema.qualified_field(j);
        let e = if j < keys {
            Expr::Column(Column::from(outer.schema().qualified_field(j)))
        } else {
            let i = j - keys;
            let c = col(&format!("__out_{i}"));
            let t = c.get_type(outer.schema())?;
            if counted[i] || t != *field.data_type() {
                Expr::Cast(Cast::new(Box::new(c), field.data_type().clone()))
            } else {
                c
            }
        };
        exprs.push(e.alias_qualified(q.cloned(), field.name()));
    }
    Ok(Some(LogicalPlan::Projection(Projection::try_new(
        exprs, outer,
    )?)))
}
