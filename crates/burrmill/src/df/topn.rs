//! `TopPerGroup`: the latest row per key, planned as an aggregate rather than a window.
//!
//! nuthatch's views pick the latest row per key as `ROW_NUMBER() OVER (PARTITION BY k ORDER BY o)`
//! filtered to `rn = 1`, often beside `SUM(x) OVER (PARTITION BY k)`. DataFusion's window
//! operators are slow on many small partitions, and on the graph-allocations nest that one pattern
//! was most of the time in three of the four slowest views. Measured on its `rewards` step, the
//! same 758,271 rows cost 628 ms as windows and 178 ms as a `GROUP BY` (DuckDB: 191 ms).
//!
//! `Filter(rn = 1)` over a projection of the window chain becomes a `GROUP BY k` with
//! `first_value(c ORDER BY o)` for each other column, the partition aggregates as aggregates, and
//! `rn` as the constant 1. `ROW_NUMBER` and `first_value` break ties among equal `o` equally
//! arbitrarily. Anything else in the window chain, or anything but columns in the projection,
//! and the plan is left as it is.
//!
//! An ordered `first_value` costs per column where a window does not, so a wide row keeps its window
//! (#90): over BetSwirl's 160,802 placements, twelve text columns took 0.50 s as aggregates and
//! 0.20 s as the window, the two even at one; beside two partition sums the aggregate still won to
//! eight columns, 0.28 s against 0.34 s at six.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Column, Result, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, WindowFunction, WindowFunctionDefinition};
use datafusion_expr::logical_plan::{Aggregate, Filter, Projection, SubqueryAlias};
use datafusion_expr::{BinaryExpr, Expr, LogicalPlan, Operator, SortExpr, WindowFrameBound};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct TopPerGroup;

impl AnalyzerRule for TopPerGroup {
    fn name(&self) -> &str {
        "top_per_group"
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

/// `rn = 1`, either way round: the column tested.
fn tests_first(pred: &Expr) -> Option<&Column> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = pred
    else {
        return None;
    };
    let one = |e: &Expr| match e {
        Expr::Literal(v, _) => matches!(
            v,
            ScalarValue::UInt64(Some(1))
                | ScalarValue::Int64(Some(1))
                | ScalarValue::Int32(Some(1))
        ),
        _ => false,
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), r) if one(r) => Some(c),
        (l, Expr::Column(c)) if one(l) => Some(c),
        _ => None,
    }
}

fn whole_partition(w: &WindowFunction) -> bool {
    let f = &w.params.window_frame;
    matches!(&f.start_bound, WindowFrameBound::Preceding(v) if v.is_null())
        && matches!(&f.end_bound, WindowFrameBound::Following(v) if v.is_null())
}

/// The most columns the rewrite keeps by `first_value` before the window is cheaper, given how many
/// partition aggregates it also replaces.
fn worth_keeping(aggregates: usize) -> usize {
    match aggregates {
        0 => 2,
        _ => 8,
    }
}

enum Source {
    Key(Column),
    Latest(Column),
    Aggregate(WindowFunction),
    RowNumber,
}

fn rewrite(f: &Filter) -> Result<Option<LogicalPlan>> {
    let Some(rn) = tests_first(&f.predicate) else {
        return Ok(None);
    };
    let mut aliases = Vec::new();
    let mut node = f.input.as_ref();
    while let LogicalPlan::SubqueryAlias(s) = node {
        aliases.push(s.alias.clone());
        node = &s.input;
    }
    let LogicalPlan::Projection(p) = node else {
        return Ok(None);
    };

    // Every window expression in the chain, by the name it outputs.
    let mut windows: HashMap<String, WindowFunction> = HashMap::new();
    let mut base = p.input.as_ref();
    while let LogicalPlan::Window(w) = base {
        let offset = w.input.schema().fields().len();
        for (i, e) in w.window_expr.iter().enumerate() {
            let Expr::WindowFunction(wf) = unalias(e) else {
                return Ok(None);
            };
            windows.insert(
                w.schema.field(offset + i).name().clone(),
                wf.as_ref().clone(),
            );
        }
        base = &w.input;
    }
    if windows.is_empty() {
        return Ok(None);
    }

    // Exactly one ROW_NUMBER, ordered; the rest partition-wide aggregates on the same keys.
    let mut row_number: Option<&WindowFunction> = None;
    for w in windows.values() {
        let p = &w.params;
        match &w.fun {
            WindowFunctionDefinition::WindowUDF(u) if u.name() == "row_number" => {
                if row_number.is_some() || p.order_by.is_empty() || !p.args.is_empty() {
                    return Ok(None);
                }
                row_number = Some(w);
            }
            WindowFunctionDefinition::AggregateUDF(_)
                if p.order_by.is_empty()
                    && !p.distinct
                    && p.filter.is_none()
                    && whole_partition(w) => {}
            _ => return Ok(None),
        }
    }
    let Some(rnw) = row_number else {
        return Ok(None);
    };
    let keys = &rnw.params.partition_by;
    if keys.is_empty() || windows.values().any(|w| w.params.partition_by != *keys) {
        return Ok(None);
    }
    let key_cols: Vec<Column> = keys
        .iter()
        .map(|k| match k {
            Expr::Column(c) => Some(c.clone()),
            _ => None,
        })
        .collect::<Option<_>>()
        .unwrap_or_default();
    if key_cols.len() != keys.len() {
        return Ok(None);
    }
    let order: Vec<SortExpr> = rnw.params.order_by.clone();

    // What each projected column comes from.
    // Aliases keep positions, so the filter's own input finds `rn` whatever qualifies it.
    let rn_index = f.input.schema().index_of_column(rn).ok();
    // By position in the window's input: over a join, `p.pos` beside a key `r.pos` is not the key.
    let key_at: Vec<Option<usize>> = key_cols
        .iter()
        .map(|k| base.schema().index_of_column(k).ok())
        .collect();
    let mut sources = Vec::with_capacity(p.expr.len());
    for (i, e) in p.expr.iter().enumerate() {
        let Expr::Column(c) = unalias(e) else {
            return Ok(None);
        };
        let at = base.schema().index_of_column(c).ok();
        let source = match windows.get(&c.name) {
            Some(w) if matches!(&w.fun, WindowFunctionDefinition::WindowUDF(_)) => {
                if Some(i) != rn_index {
                    return Ok(None);
                }
                Source::RowNumber
            }
            Some(w) => Source::Aggregate(w.clone()),
            None if at.is_some() && key_at.contains(&at) => Source::Key(c.clone()),
            None if at.is_some() => Source::Latest(c.clone()),
            None => return Ok(None),
        };
        sources.push(source);
    }
    if rn_index.is_none_or(|i| !matches!(sources[i], Source::RowNumber)) {
        return Ok(None);
    }
    let latest = sources
        .iter()
        .filter(|s| matches!(s, Source::Latest(_)))
        .count();
    let aggregates = sources
        .iter()
        .filter(|s| matches!(s, Source::Aggregate(_)))
        .count();
    if latest > worth_keeping(aggregates) {
        return Ok(None);
    }

    let first_value = datafusion_functions_aggregate::first_last::first_value_udaf();
    let mut aggr = Vec::new();
    let mut outs: Vec<Expr> = Vec::with_capacity(sources.len());
    for (i, s) in sources.iter().enumerate() {
        let (qualifier, field) = p.schema.qualified_field(i);
        let out = match s {
            Source::Key(c) => Expr::Column(c.clone()),
            Source::RowNumber => Expr::Literal(ScalarValue::UInt64(Some(1)), None),
            Source::Latest(c) => {
                let name = format!("__top_{i}");
                aggr.push(
                    Expr::AggregateFunction(AggregateFunction::new_udf(
                        Arc::clone(&first_value),
                        vec![Expr::Column(c.clone())],
                        false,
                        None,
                        order.clone(),
                        None,
                    ))
                    .alias(&name),
                );
                Expr::Column(Column::new_unqualified(name))
            }
            Source::Aggregate(w) => {
                let WindowFunctionDefinition::AggregateUDF(func) = &w.fun else {
                    return Ok(None);
                };
                let name = format!("__top_{i}");
                aggr.push(
                    Expr::AggregateFunction(AggregateFunction::new_udf(
                        Arc::clone(func),
                        w.params.args.clone(),
                        false,
                        None,
                        vec![],
                        w.params.null_treatment,
                    ))
                    .alias(&name),
                );
                Expr::Column(Column::new_unqualified(name))
            }
        };
        outs.push(out.alias_qualified(qualifier.cloned(), field.name()));
    }
    let agg = Aggregate::try_new(Arc::new(base.clone()), keys.clone(), aggr)?;
    let mut plan = LogicalPlan::Projection(Projection::try_new(
        outs,
        Arc::new(LogicalPlan::Aggregate(agg)),
    )?);
    for a in aliases.into_iter().rev() {
        plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(plan), a)?);
    }
    Ok(Some(plan))
}
