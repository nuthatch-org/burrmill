//! `OutermostOrder`: a view's or derived table's `ORDER BY` orders the answer when nothing above it
//! reorders, as DuckDB's does in practice (nuthatch#1984).
//!
//! DataFusion's SQL planner drops every sort without a fetch from a relation in `FROM`, and it
//! inlines a view there, so `SELECT * FROM v` came back unordered whatever `v` said. That pass is
//! switched off in the session, and this rule drops only the sorts whose order cannot reach the
//! answer or decide which rows a limit keeps: those under an aggregate, join, union, window,
//! distinct or another sort. Each subquery expression is its own answer.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct OutermostOrder;

impl AnalyzerRule for OutermostOrder {
    fn name(&self) -> &str {
        "outermost_order"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        keep(plan, true, &mut HashMap::new()).map(|t| t.data)
    }
}

/// Each shared input already walked, by its address and whether a sort there decides, beside
/// the original, so the address is not reused while the walk runs (`dag.rs`).
type Kept = HashMap<(*const LogicalPlan, bool), (Arc<LogicalPlan>, Arc<LogicalPlan>)>;

/// `decides`: a sort here would order the answer, or choose the rows of a limit above.
#[recursive::recursive]
fn keep(plan: LogicalPlan, decides: bool, kept: &mut Kept) -> Result<Transformed<LogicalPlan>> {
    let t = plan.map_subqueries(|q| keep(q, true, kept))?;
    let (plan, mut transformed) = (t.data, t.transformed);
    let mut plan = match plan {
        LogicalPlan::Sort(s) if s.fetch.is_none() && !decides => {
            return Ok(Transformed::yes(
                keep(Arc::unwrap_or_clone(s.input), false, kept)?.data,
            ));
        }
        p => p,
    };
    let below = match &plan {
        LogicalPlan::Limit(_) => true,
        LogicalPlan::Projection(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_) => decides,
        _ => false,
    };
    let Some(inputs) = super::dag::inputs_mut(&mut plan) else {
        return plan.map_children(|c| keep(c, below, kept)).map(|mut t| {
            t.transformed |= transformed;
            t
        });
    };
    for input in inputs {
        let key = (Arc::as_ptr(input), below);
        if let Some((_, done)) = kept.get(&key) {
            transformed |= !Arc::ptr_eq(done, input);
            *input = Arc::clone(done);
            continue;
        }
        let held = std::mem::replace(input, Arc::clone(&super::dag::PLACEHOLDER));
        let held = match Arc::try_unwrap(held) {
            Ok(p) => {
                let t = keep(p, below, kept)?;
                transformed |= t.transformed;
                *input = Arc::new(t.data);
                continue;
            }
            Err(held) => held,
        };
        let t = keep(held.as_ref().clone(), below, kept)?;
        let done = match t.transformed {
            true => Arc::new(t.data),
            false => Arc::clone(&held),
        };
        transformed |= t.transformed;
        kept.insert(key, (held, Arc::clone(&done)));
        *input = done;
    }
    Ok(Transformed::new_transformed(plan, transformed))
}
