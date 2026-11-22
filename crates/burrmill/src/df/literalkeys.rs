//! `LiteralGroupKeys`: DataFusion's `eliminate_group_by_constant`, for literal keys only (#381).
//!
//! The session drops DataFusion's rule, which also recomputes a key that is a function of another
//! key and so let a rewritten `TRY_CAST(k AS BIGINT)` collide with `k`. Without it, a literal key
//! stayed in the aggregate: the `true AS __always_true` that decorrelating a scalar subquery adds
//! (TPC-H Q17), or a `GROUP BY k, 'x'`. A constant counts as sorted, so DataFusion 55 planned the
//! aggregate in its ordered mode, whose last output is taken a batch at a time with
//! `EmitTo::First`, and each of those renumbers every group still held. That is quadratic in the
//! groups: at SF10, 179 s of the 190 CPU seconds Q17's partial aggregate took, emitting 2M parts'
//! averages in 2,048-row batches (`clamp_emit_to` in DataFusion's
//! `aggregates/aggregate_hash_table/common_ordered.rs`).
//!
//! Here only a key that is a literal, under any aliases, leaves the aggregate, and a projection
//! above puts it back under its own name, every other column passed through as it was. An
//! aggregate keyed by nothing but literals keeps them: without a key it would answer one row over
//! no input. Grouping sets are left alone.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::logical_plan::{Aggregate, Projection};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

#[derive(Debug, Default)]
pub struct LiteralGroupKeys;

/// The literal beneath any aliases, if that is what `e` is.
fn literal(e: &Expr) -> Option<&Expr> {
    match e {
        Expr::Alias(a) => literal(&a.expr),
        Expr::Literal(..) => Some(e),
        _ => None,
    }
}

impl OptimizerRule for LiteralGroupKeys {
    fn name(&self) -> &str {
        "literal_group_keys"
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
        let LogicalPlan::Aggregate(agg) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let keys: Vec<Expr> = agg
            .group_expr
            .iter()
            .filter(|e| literal(e).is_none())
            .cloned()
            .collect();
        if keys.len() == agg.group_expr.len()
            || keys.is_empty()
            || keys.iter().any(|e| matches!(e, Expr::GroupingSet(_)))
        {
            return Ok(Transformed::no(plan));
        }
        let inner = LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::clone(&agg.input),
            keys,
            agg.aggr_expr.clone(),
        )?);
        // The inner aggregate's columns, in order, are the outer one's without the literals.
        let mut passed = inner.schema().columns().into_iter();
        let mut exprs = Vec::with_capacity(agg.schema.fields().len());
        for (i, e) in agg.group_expr.iter().enumerate() {
            let (q, f) = agg.schema.qualified_field(i);
            exprs.push(match literal(e) {
                Some(l) => l.clone().alias_qualified(q.cloned(), f.name()),
                None => Expr::Column(passed.next().expect("a kept key")),
            });
        }
        exprs.extend(passed.map(Expr::Column));
        let out = Projection::try_new(exprs, Arc::new(inner))?;
        debug_assert!(
            out.schema
                .iter()
                .zip(agg.schema.iter())
                .all(|((q1, f1), (q2, f2))| q1 == q2 && f1.name() == f2.name()),
            "literal_group_keys changed the aggregate's columns"
        );
        Ok(Transformed::yes(LogicalPlan::Projection(out)))
    }
}
