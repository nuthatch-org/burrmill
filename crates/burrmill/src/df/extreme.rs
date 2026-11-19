//! `PartitionExtreme`: the rows at their partition's minimum or maximum, without a sort.
//!
//! The QoS nest keeps the first document per bucket as
//! `QUALIFY k = min(k) OVER (PARTITION BY day, bucket)`. DataFusion plans a window by sorting its
//! whole input on the partition keys: 74 million wide rows there, refused for memory in the sort or
//! its merge at any bound a nest is given, and 600 s where it was not. A partition-wide `min` or
//! `max` is the same value as a `GROUP BY` on those keys joined back to the rows, NULL keys equal as
//! a window groups them; the aggregate holds one row per partition (57,000 buckets) and the rows
//! stream past it. The filter above stays as written. Only that filter-over-one-window shape is
//! taken: the rewrite reads its input twice, which a window over a small input does not want.

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Column, JoinType, NullEquality, Result};
use datafusion_expr::expr::{AggregateFunction, WindowFunctionDefinition};
use datafusion_expr::logical_plan::Filter;
use datafusion_expr::{
    BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator, WindowFrameBound,
};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct PartitionExtreme;

impl AnalyzerRule for PartitionExtreme {
    fn name(&self) -> &str {
        "partition_extreme"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        super::dag::transform_up(plan, |p, _| match &p {
            LogicalPlan::Filter(f) => Ok(match rewrite(f)? {
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

fn rewrite(f: &Filter) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Window(w) = f.input.as_ref() else {
        return Ok(None);
    };
    let [only] = w.window_expr.as_slice() else {
        return Ok(None);
    };
    let Expr::WindowFunction(wf) = unalias(only) else {
        return Ok(None);
    };
    let WindowFunctionDefinition::AggregateUDF(func) = &wf.fun else {
        return Ok(None);
    };
    let p = &wf.params;
    let whole = matches!(&p.window_frame.start_bound, WindowFrameBound::Preceding(v) if v.is_null())
        && matches!(&p.window_frame.end_bound, WindowFrameBound::Following(v) if v.is_null());
    if !matches!(func.name(), "min" | "max")
        || !p.order_by.is_empty()
        || p.distinct
        || p.filter.is_some()
        || !whole
        || p.partition_by.is_empty()
    {
        return Ok(None);
    }
    let Some(keys) = p
        .partition_by
        .iter()
        .map(|k| match k {
            Expr::Column(c) => Some(c.clone()),
            _ => None,
        })
        .collect::<Option<Vec<Column>>>()
    else {
        return Ok(None);
    };
    let base = w.input.as_ref();
    let width = base.schema().fields().len();
    let out = w.schema.field(width).name().clone();
    // The filter is an equality against the window's value, either way round.
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = &f.predicate
    else {
        return Ok(None);
    };
    let is_out = |e: &Expr| matches!(e, Expr::Column(c) if c.relation.is_none() && c.name == out);
    if is_out(left) == is_out(right) {
        return Ok(None);
    }

    const SIDE: &str = "__extreme_of";
    let extreme = LogicalPlanBuilder::from(base.clone())
        .aggregate(
            keys.iter()
                .enumerate()
                .map(|(i, k)| Expr::Column(k.clone()).alias(format!("__extreme_k{i}"))),
            [Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::clone(func),
                p.args.clone(),
                false,
                None,
                vec![],
                p.null_treatment,
            ))
            .alias("__extreme")],
        )?
        .alias(SIDE)?
        .build()?;
    let side_keys: Vec<Column> = (0..keys.len())
        .map(|i| Column::new(Some(SIDE), format!("__extreme_k{i}")))
        .collect();
    // The aggregate first: it is the side a hash join builds on.
    let rows = base
        .schema()
        .iter()
        .map(|(q, field)| Expr::Column(Column::new(q.cloned(), field.name())))
        .chain([Expr::Column(Column::new(Some(SIDE), "__extreme")).alias(out)]);
    let plan = LogicalPlanBuilder::from(extreme)
        .join_detailed(
            base.clone(),
            JoinType::Inner,
            (side_keys, keys),
            None,
            NullEquality::NullEqualsNull,
        )?
        .project(rows)?
        .filter(f.predicate.clone())?
        .build()?;
    Ok(Some(plan))
}
