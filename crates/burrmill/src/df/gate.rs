//! DataFusion optimizer rules that look first, and leave a plan alone that holds nothing they act on.
//!
//! `eliminate_join`, `extract_leaf_expressions` and `rewrite_set_comparison` changed no plan in the
//! corpus probe of #348 and cost some 16 µs a statement between them, in walks that rebuild
//! nothing. Each acts only on one kind of node or expression, so a read for that kind decides
//! whether the walk is worth making.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_expr::{Expr, ExpressionPlacement, LogicalPlan};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

type Rule = Arc<dyn OptimizerRule + Send + Sync>;

/// `rule`, gated, where it is one of the three; any other rule as it was.
pub(super) fn gated(rule: Rule) -> Rule {
    let trigger: fn(&LogicalPlan) -> bool = match rule.name() {
        // Rewrites a join, and only a join.
        "eliminate_join" => |p| matches!(p, LogicalPlan::Join(_)),
        // An expression the rule moves towards the leaves.
        "extract_leaf_expressions" => |p| {
            super::dag::holds(p, |e| {
                e.placement() == ExpressionPlacement::MoveTowardsLeafNodes
            })
        },
        "rewrite_set_comparison" => {
            |p| super::dag::holds(p, |e| matches!(e, Expr::SetComparison(_)))
        }
        _ => return rule,
    };
    Arc::new(Gated { rule, trigger })
}

#[derive(Debug)]
struct Gated {
    rule: Rule,
    trigger: fn(&LogicalPlan) -> bool,
}

impl OptimizerRule for Gated {
    fn name(&self) -> &str {
        self.rule.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        self.rule.apply_order()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // The plan, and the plans inside its expressions, which each of the three also walks.
        let mut found = false;
        plan.apply_with_subqueries(|p| {
            found = (self.trigger)(p);
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        if found {
            self.rule.rewrite(plan, config)
        } else {
            Ok(Transformed::no(plan))
        }
    }
}
