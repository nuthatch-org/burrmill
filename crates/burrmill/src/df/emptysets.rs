//! Correlated subqueries grouped by an empty grouping set, answered as DuckDB answers them (#422).
//!
//! An empty grouping set, `GROUPING SETS ((), ...)`, `ROLLUP` or `CUBE`, makes a row per outer row
//! even where the subquery's filter keeps none of its rows: `count(*)` is 0 there, `sum` NULL.
//! DataFusion 55 decorrelates such a subquery as if it were grouped by the correlated column and
//! joins it, so an outer row without a match lost its rows, and a LATERAL answered fewer rows than
//! DuckDB without a word.
//!
//! `GROUPING SETS (())` alone is an aggregate with no GROUP BY, which DataFusion decorrelates
//! itself, and is planned as one. A LATERAL over any other such aggregate, or over an ungrouped one
//! with a HAVING, which DataFusion's decorrelation filters before the empty input's row is made, is
//! rewritten here: the outer rows are numbered, the subquery's rows LEFT JOINed on its correlated
//! conjuncts, and the aggregate grouped by the number and each set, every aggregate filtered to the
//! joined rows so that an outer row with no match aggregates no rows. A non-empty set's row for an
//! outer row with no match, which only the NULL-extended row made, is dropped. Any other correlated
//! subquery over an empty grouping set is refused by name.

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result, plan_err};
use datafusion_expr::expr::{AggregateFunction, WindowFunction, WindowFunctionDefinition};
use datafusion_expr::logical_plan::{Aggregate, Filter, Join, JoinType, Projection, SubqueryAlias};
use datafusion_expr::utils::{conjunction, enumerate_grouping_sets, split_conjunction_owned};
use datafusion_expr::{Expr, GroupingSet, LogicalPlan, LogicalPlanBuilder, lit};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct EmptyGroupingSets;

impl AnalyzerRule for EmptyGroupingSets {
    fn name(&self) -> &str {
        "empty_grouping_sets"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        if !any(&plan, |p| matches!(p, LogicalPlan::Aggregate(_)))? {
            return Ok(plan);
        }
        let mut n = 0;
        let plan = plan
            .transform_up_with_subqueries(|p| match p {
                LogicalPlan::Aggregate(a) if only_empty_set(&a.group_expr) => {
                    let input = Arc::clone(&a.input);
                    let a = Aggregate::try_new(input, vec![], a.aggr_expr)?;
                    Ok(Transformed::yes(LogicalPlan::Aggregate(a)))
                }
                LogicalPlan::Join(j) => match lateral(&j, n) {
                    Ok(Some(p)) => {
                        n += 1;
                        Ok(Transformed::yes(p))
                    }
                    // A shape this cannot rewrite is left to the check below.
                    Ok(None) | Err(_) => Ok(Transformed::no(LogicalPlan::Join(j))),
                },
                p => Ok(Transformed::no(p)),
            })?
            .data;
        let refused = any(&plan, |p| {
            matches!(p, LogicalPlan::Aggregate(a)
                if sets(&a.group_expr).is_some_and(|s| s.iter().any(Vec::is_empty))
                    && a.input.exists(|x| Ok(x.contains_outer_reference())).unwrap_or(true))
        })?;
        if refused {
            return plan_err!(
                "a correlated subquery grouped by an empty grouping set (GROUPING SETS with (), \
                 ROLLUP or CUBE) is not supported here"
            );
        }
        Ok(plan)
    }
}

/// Whether any node of `plan`, its subqueries' included, is one `f` picks.
fn any(plan: &LogicalPlan, f: impl Fn(&LogicalPlan) -> bool) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|p| {
        found = f(p);
        Ok(match found {
            true => TreeNodeRecursion::Stop,
            false => TreeNodeRecursion::Continue,
        })
    })?;
    Ok(found)
}

/// `GROUP BY GROUPING SETS (())`.
fn only_empty_set(group: &[Expr]) -> bool {
    matches!(group, [Expr::GroupingSet(GroupingSet::GroupingSets(s))] if s.len() == 1 && s[0].is_empty())
}

/// An aggregate's grouping sets, each a list of expressions; `None` if it has none.
fn sets(group: &[Expr]) -> Option<Vec<Vec<Expr>>> {
    let group = enumerate_grouping_sets(group.to_vec()).ok()?;
    let [Expr::GroupingSet(g)] = group.as_slice() else {
        return None;
    };
    Some(match g {
        GroupingSet::GroupingSets(s) => s.clone(),
        GroupingSet::Rollup(v) => (0..=v.len()).rev().map(|i| v[..i].to_vec()).collect(),
        GroupingSet::Cube(v) => (0..1usize << v.len())
            .rev()
            .map(|mask| {
                (0..v.len())
                    .filter(|i| mask & (1 << (v.len() - 1 - i)) != 0)
                    .map(|i| v[i].clone())
                    .collect()
            })
            .collect(),
    })
}

/// The parts of `LATERAL (SELECT ... FROM r WHERE ... GROUP BY ... [HAVING ...])` this rewrites.
struct Parts<'a> {
    alias: &'a SubqueryAlias,
    select: &'a Projection,
    having: Option<&'a Filter>,
    aggregate: &'a Aggregate,
    sets: Vec<Vec<Expr>>,
    correlated: Vec<Expr>,
    plain: Vec<Expr>,
    relation: &'a LogicalPlan,
}

fn parts<'a>(j: &'a Join) -> Option<Parts<'a>> {
    if !matches!(j.join_type, JoinType::Inner | JoinType::Left) || !j.on.is_empty() {
        return None;
    }
    let LogicalPlan::SubqueryAlias(alias) = j.right.as_ref() else {
        return None;
    };
    let LogicalPlan::Subquery(sub) = alias.input.as_ref() else {
        return None;
    };
    let LogicalPlan::Projection(select) = sub.subquery.as_ref() else {
        return None;
    };
    let (having, aggregate) = match select.input.as_ref() {
        LogicalPlan::Filter(f) => match f.input.as_ref() {
            LogicalPlan::Aggregate(a) => (Some(f), a),
            _ => return None,
        },
        LogicalPlan::Aggregate(a) => (None, a),
        _ => return None,
    };
    let sets = match sets(&aggregate.group_expr) {
        Some(s) if s.iter().any(Vec::is_empty) => s,
        None if aggregate.group_expr.is_empty() && having.is_some() => vec![vec![]],
        _ => return None,
    };
    let LogicalPlan::Filter(f) = aggregate.input.as_ref() else {
        return None;
    };
    let above = select.expr.iter().chain(having.map(|h| &h.predicate));
    let outer_above = above
        .chain(&aggregate.aggr_expr)
        .chain(sets.iter().flatten())
        .any(|e| e.contains_outer() || has_subquery(e));
    let outer_below = f
        .input
        .exists(|p| Ok(p.contains_outer_reference()))
        .unwrap_or(true);
    if outer_above || outer_below || has_subquery(&f.predicate) {
        return None;
    }
    let (correlated, plain): (Vec<Expr>, Vec<Expr>) = split_conjunction_owned(f.predicate.clone())
        .into_iter()
        .partition(|e| e.contains_outer());
    // Every outer column the subquery reads is the join's left input's, not a query's further out.
    let left = j.left.schema();
    let mut outer_here = true;
    for e in &correlated {
        e.apply(|x| {
            if let Expr::OuterReferenceColumn(_, c) = x {
                outer_here &= left.has_column(c);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .ok()?;
    }
    if correlated.is_empty() || !outer_here {
        return None;
    }
    Some(Parts {
        alias,
        select,
        having,
        aggregate,
        sets,
        correlated,
        plain,
        relation: f.input.as_ref(),
    })
}

fn has_subquery(e: &Expr) -> bool {
    e.exists(|x| {
        Ok(matches!(
            x,
            Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_)
        ))
    })
    .unwrap_or(true)
}

fn qualifiers(p: &LogicalPlan) -> std::collections::HashSet<String> {
    p.schema()
        .iter()
        .filter_map(|(q, _)| q.map(|q| q.to_string()))
        .collect()
}

fn lateral(j: &Join, n: usize) -> Result<Option<LogicalPlan>> {
    let Some(parts) = parts(j) else {
        return Ok(None);
    };
    if !qualifiers(&j.left).is_disjoint(&qualifiers(parts.relation)) {
        return Ok(None);
    }
    let (rid, marker, matched, grouped, key) = (
        format!("__burrmill_esr{n}"),
        format!("__burrmill_esm{n}"),
        format!("__burrmill_esn{n}"),
        format!("__burrmill_esg{n}"),
        format!("__burrmill_esk{n}"),
    );
    let row_number = Expr::from(WindowFunction::new(
        WindowFunctionDefinition::WindowUDF(
            datafusion_functions_window::row_number::row_number_udwf(),
        ),
        vec![],
    ))
    .alias(&rid);
    let numbered = LogicalPlanBuilder::from((*j.left).clone())
        .window(vec![row_number])?
        .build()?;

    // The subquery's rows, each marked, LEFT JOINed to the outer row they belong to.
    let mut relation = LogicalPlanBuilder::from(parts.relation.clone());
    if let Some(w) = conjunction(parts.plain) {
        relation = relation.filter(w)?;
    }
    let columns = relation.schema().columns().into_iter().map(Expr::Column);
    let relation = relation
        .project(columns.chain([lit(true).alias(&marker)]))?
        .build()?;
    let on = parts
        .correlated
        .into_iter()
        .map(|e| {
            e.transform(|x| match x {
                Expr::OuterReferenceColumn(_, c) => Ok(Transformed::yes(Expr::Column(c))),
                x => Ok(Transformed::no(x)),
            })
            .map(|t| t.data)
        })
        .collect::<Result<Vec<_>>>()?;
    let joined = LogicalPlanBuilder::from(numbered.clone())
        .join_on(relation, JoinType::Left, on)?
        .build()?;

    // Each set grouped by the outer row too; every aggregate over the joined rows only.
    let rid_col = Expr::Column(Column::new_unqualified(&rid));
    let marker_col = Expr::Column(Column::new_unqualified(&marker));
    let group = Expr::GroupingSet(GroupingSet::GroupingSets(
        parts
            .sets
            .iter()
            .map(|s| {
                std::iter::once(rid_col.clone())
                    .chain(s.iter().cloned())
                    .collect()
            })
            .collect(),
    ));
    let mut aggr = parts
        .aggregate
        .aggr_expr
        .iter()
        .map(|e| {
            let name = e.schema_name().to_string();
            let filtered = e
                .clone()
                .transform(|x| match x {
                    Expr::AggregateFunction(mut f) if f.func.name() != "grouping" => {
                        f.params.filter = Some(Box::new(match f.params.filter.take() {
                            Some(w) => w.and(marker_col.clone()),
                            None => marker_col.clone(),
                        }));
                        Ok(Transformed::yes(Expr::AggregateFunction(f)))
                    }
                    x => Ok(Transformed::no(x)),
                })?
                .data;
            Ok(match filtered {
                Expr::Alias(a) => Expr::Alias(a),
                e => e.alias(name),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    aggr.push(
        Expr::AggregateFunction(AggregateFunction::new_udf(
            datafusion_functions_aggregate::count::count_udaf(),
            vec![marker_col],
            false,
            None,
            vec![],
            None,
        ))
        .alias(&matched),
    );
    let mut columns: Vec<Expr> = Vec::new();
    for e in parts.sets.iter().flatten() {
        if !columns.contains(e) {
            columns.push(e.clone());
        }
    }
    let keep_all = columns.is_empty();
    if !keep_all {
        if columns.len() > 30 {
            return Ok(None);
        }
        aggr.push(
            Expr::AggregateFunction(AggregateFunction::new_udf(
                datafusion_functions_aggregate::grouping::grouping_udaf(),
                columns.clone(),
                false,
                None,
                vec![],
                None,
            ))
            .alias(&grouped),
        );
    }
    let mut plan = LogicalPlanBuilder::from(LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(joined),
        vec![group],
        aggr,
    )?));
    if !keep_all {
        // A row of an empty set (every column aggregated across) is kept for every outer row;
        // another set's only where the outer row matched.
        let every = lit((1i32 << columns.len()) - 1);
        let matched = Expr::Column(Column::new_unqualified(&matched)).gt(lit(0i64));
        let empty = Expr::Column(Column::new_unqualified(&grouped)).eq(every);
        plan = plan.filter(matched.or(empty))?;
    }
    if let Some(h) = parts.having {
        plan = plan.filter(h.predicate.clone())?;
    }
    let sub = plan
        .project(
            parts
                .select
                .expr
                .iter()
                .cloned()
                .chain([rid_col.clone().alias(&key)]),
        )?
        .alias(parts.alias.alias.clone())?
        .build()?;
    let key_col = Expr::Column(Column::new(Some(parts.alias.alias.clone()), &key));
    let on = std::iter::once(rid_col.eq(key_col)).chain(j.filter.clone());
    let outputs: Vec<Expr> = j
        .left
        .schema()
        .columns()
        .into_iter()
        .chain(parts.alias.schema.columns())
        .map(Expr::Column)
        .collect();
    let plan = LogicalPlanBuilder::from(numbered)
        .join_on(sub, j.join_type, on)?
        .project(outputs)?
        .build()?;
    if plan.schema().fields() != j.schema.fields() {
        return Ok(None);
    }
    Ok(Some(plan))
}
