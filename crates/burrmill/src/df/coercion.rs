//! DataFusion's `TypeCoercion`, over each shared subtree once (#347).
//!
//! Its walk rebuilds every node of every copy of every view, and derives each one's schema again
//! whether anything in it changed or not. The node-by-node work here is DataFusion's own,
//! `analyze_internal` in `datafusion-optimizer` 55.2.0, given to [`super::dag::transform_up`], less
//! the `MERGE` target, which no statement here reaches.

use std::sync::{Arc, LazyLock};

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction;
use datafusion_optimizer::analyzer::type_coercion::{TypeCoercion, TypeCoercionRewriter};

#[derive(Debug, Default)]
pub struct Coercion;

impl AnalyzerRule for Coercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        // The output's views are cast back only under this option, which no engine sets.
        if config.optimizer.expand_views_at_output {
            return TypeCoercion::new().analyze(plan, config);
        }
        static EMPTY: LazyLock<DFSchema> = LazyLock::new(DFSchema::empty);
        super::dag::transform_up(plan, |p, below| coerce(&EMPTY, p, below)).map(|t| t.data)
    }
}

thread_local! {
    /// The plan as the first coercion left it, for the second on the same thread.
    static COERCED: std::cell::RefCell<Option<LogicalPlan>> = const { std::cell::RefCell::new(None) };
}

/// The first coercion, remembering what it made.
#[derive(Debug)]
pub struct First(pub Arc<dyn AnalyzerRule + Send + Sync>);

impl AnalyzerRule for First {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let plan = self.0.analyze(plan, config)?;
        COERCED.with(|c| *c.borrow_mut() = Some(plan.clone()));
        Ok(plan)
    }
}

/// The second, left out where no rule since the first changed the plan: coercion is idempotent,
/// and on most statements nothing between the two does.
#[derive(Debug)]
pub struct Again(pub Arc<dyn AnalyzerRule + Send + Sync>);

impl AnalyzerRule for Again {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if COERCED
            .with(|c| c.borrow_mut().take())
            .is_some_and(|c| c == plan)
        {
            return Ok(plan);
        }
        self.0.analyze(plan, config)
    }
}

/// One node, its inputs done; `below` says one of them came back with another schema. A node is
/// reported changed wherever DataFusion's would differ from it, a schema left behind by an earlier
/// rule included, since the walk keeps the node it was given otherwise.
fn coerce(external: &DFSchema, plan: LogicalPlan, below: bool) -> Result<Transformed<LogicalPlan>> {
    let before = std::sync::Arc::clone(plan.schema());
    let rebuilt = below
        || matches!(
            plan,
            LogicalPlan::Union(_) | LogicalPlan::Join(_) | LogicalPlan::Limit(_)
        );
    let mut schema = merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(ts) = &plan {
        let source =
            DFSchema::try_from_qualified_schema(ts.table_name.clone(), &ts.source.schema())?;
        schema.merge(&source);
    }
    schema.merge(external);
    let plan = if let LogicalPlan::Filter(mut filter) = plan {
        filter.predicate = filter
            .predicate
            .cast_to(&arrow::datatypes::DataType::Boolean, &schema)?;
        LogicalPlan::Filter(filter)
    } else {
        plan
    };
    let mut rewrite = TypeCoercionRewriter::new(&schema);
    let names = NamePreserver::new(&plan);
    plan.map_expressions(|e| {
        let name = names.save(&e);
        e.rewrite(&mut rewrite)
            .map(|t| t.update_data(|e| name.restore(e)))
    })?
    .map_data(|p| rewrite.coerce_plan(p))?
    .map_data(|p| p.recompute_schema())
    .map(|t| {
        let changed = t.transformed || rebuilt || *t.data.schema() != before;
        Transformed::new_transformed(t.data, changed)
    })
}

/// DataFusion's `ResolveGroupingFunction`, which rebuilds every node it passes, run only over a
/// plan with a `grouping()` call for it to resolve.
#[derive(Debug)]
pub struct Grouping(pub ResolveGroupingFunction);

impl AnalyzerRule for Grouping {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut grouping = false;
        plan.apply_with_subqueries(|p| {
            p.apply_expressions(|e| {
                grouping = e.exists(|e| {
                    Ok(matches!(e, Expr::AggregateFunction(f) if f.func.name() == "grouping"))
                })?;
                Ok(stop(grouping))
            })?;
            Ok(stop(grouping))
        })?;
        match grouping {
            true => self.0.analyze(plan, config),
            false => Ok(plan),
        }
    }
}

fn stop(found: bool) -> TreeNodeRecursion {
    match found {
        true => TreeNodeRecursion::Stop,
        false => TreeNodeRecursion::Continue,
    }
}
