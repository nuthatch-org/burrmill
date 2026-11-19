//! `NullableSubqueries`: a scalar subquery is NULL when it finds no row.
//!
//! DataFusion 55 takes a scalar subquery's nullability from its column, so over a non-nullable one
//! `coalesce((SELECT true FROM t WHERE …), false)` is typed non-nullable and simplified to the bare
//! subquery, which then returns NULL for an empty `t`. Each such subquery is wrapped in
//! `maybe_null`, an identity typed nullable.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    ColumnarValue, Expr, LogicalPlan, LogicalPlanBuilder, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug)]
pub struct NullableSubqueries(Arc<ScalarUDF>);

impl Default for NullableSubqueries {
    fn default() -> Self {
        Self(Arc::new(ScalarUDF::from(MaybeNull(Signature::any(
            1,
            Volatility::Immutable,
        )))))
    }
}

impl AnalyzerRule for NullableSubqueries {
    fn name(&self) -> &str {
        "nullable_subqueries"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        super::dag::transform_up(plan, |p, _| {
            let before = Arc::clone(p.schema());
            let projection = matches!(p, LogicalPlan::Projection(_));
            let t = p.map_expressions(|e| {
                if !e.exists(|x| Ok(matches!(x, Expr::ScalarSubquery(_))))? {
                    return Ok(Transformed::no(e));
                }
                let name = e.schema_name().to_string();
                let t = e.transform_up(|x| match x {
                    Expr::ScalarSubquery(ref q) if !q.subquery.schema().field(0).is_nullable() => {
                        Ok(Transformed::yes(Expr::ScalarFunction(
                            ScalarFunction::new_udf(Arc::clone(&self.0), vec![x]),
                        )))
                    }
                    x => Ok(Transformed::no(x)),
                })?;
                Ok(if projection && t.transformed {
                    t.map_data(|e| e.alias_if_changed(name))?
                } else {
                    t
                })
            })?;
            if !t.transformed {
                return Ok(t);
            }
            t.map_data(|p| renamed(p.recompute_schema()?, &before))
        })
        .map(|t| t.data)
    }
}

/// `p` under the column names it had before, which an aggregate or window over a wrapped subquery
/// changes, and its parent reads.
pub(super) fn renamed(p: LogicalPlan, before: &DFSchema) -> Result<LogicalPlan> {
    if p.schema()
        .iter()
        .map(|(_, f)| f.name())
        .eq(before.iter().map(|(_, f)| f.name()))
    {
        return Ok(p);
    }
    let names = p
        .schema()
        .columns()
        .into_iter()
        .zip(before.iter())
        .map(|(c, (q, f))| Expr::Column(c).alias_qualified(q.cloned(), f.name()));
    LogicalPlanBuilder::from(p)
        .project(names.collect::<Vec<_>>())?
        .build()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MaybeNull(Signature);

impl ScalarUDFImpl for MaybeNull {
    fn name(&self) -> &str {
        "maybe_null"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args {
            [t] => Ok(t.clone()),
            _ => plan_err!("maybe_null takes one argument"),
        }
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            args.arg_fields[0].data_type().clone(),
            true,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(args.args.into_iter().next().expect("one argument"))
    }
}
