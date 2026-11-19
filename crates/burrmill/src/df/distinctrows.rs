//! `DistinctRows`: `COUNT(DISTINCT (a, b, c))` over a key of bytes, not a set of structs.
//!
//! DataFusion counts distinct values of a type it has no fast set for by keeping a `ScalarValue`
//! per value: for a row of three strings, a one-row struct array each, hashed by rebuilding it. The
//! QoS nest's `qos_indexer_daily` counts `DISTINCT (deployment, chain, gateway)` beside another
//! distinct count, where [`super::distinct`] does not apply, and one day of it ran past 500 s on
//! eight cores; DuckDB takes 0.7 s. Arrow's row format encodes a row, NULLs included, as bytes that
//! are equal exactly when the rows are, and DataFusion's distinct count over bytes is fast. So the
//! constructed row becomes its key and the count is unchanged. Only a row written out in the
//! statement is taken: it is never NULL itself, as a struct column can be.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, SortField};
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::expr::{AggregateFunction, ScalarFunction};
use datafusion_expr::logical_plan::Aggregate;
use datafusion_expr::{
    ColumnarValue, Expr, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, PartialEq, Eq, Hash)]
struct RowKey {
    sig: Signature,
}

impl ScalarUDFImpl for RowKey {
    fn name(&self) -> &str {
        "burrmill_row_key"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let columns: Vec<ArrayRef> = ColumnarValue::values_to_arrays(&args.args)?;
        let fields = columns
            .iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect();
        let converter = RowConverter::new(fields)?;
        let rows = converter.convert_columns(&columns)?;
        Ok(ColumnarValue::Array(Arc::new(rows.try_into_binary()?)))
    }
}

#[derive(Debug)]
pub struct DistinctRows {
    key: Arc<ScalarUDF>,
}

impl Default for DistinctRows {
    fn default() -> Self {
        Self {
            key: Arc::new(ScalarUDF::from(RowKey {
                sig: Signature::variadic_any(Volatility::Immutable),
            })),
        }
    }
}

impl AnalyzerRule for DistinctRows {
    fn name(&self) -> &str {
        "distinct_rows"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        super::dag::transform_up(plan, |p, _| {
            let LogicalPlan::Aggregate(a) = &p else {
                return Ok(Transformed::no(p));
            };
            let mut changed = false;
            let aggr: Vec<Expr> = a
                .aggr_expr
                .iter()
                .map(|e| match self.keyed(e) {
                    Some(k) => {
                        changed = true;
                        k
                    }
                    None => e.clone(),
                })
                .collect();
            if !changed {
                return Ok(Transformed::no(p));
            }
            let rebuilt = Aggregate::try_new(Arc::clone(&a.input), a.group_expr.clone(), aggr)?;
            Ok(Transformed::yes(LogicalPlan::Aggregate(rebuilt)))
        })
        .map(|t| t.data)
    }
}

impl DistinctRows {
    /// `e` with its distinct row counted by key, under the name it had.
    fn keyed(&self, e: &Expr) -> Option<Expr> {
        let mut inner = e;
        while let Expr::Alias(a) = inner {
            inner = &a.expr;
        }
        let Expr::AggregateFunction(f) = inner else {
            return None;
        };
        if f.func.name() != "count" || !f.params.distinct || f.params.args.len() != 1 {
            return None;
        }
        let mut row = &f.params.args[0];
        while let Expr::Alias(a) = row {
            row = &a.expr;
        }
        let Expr::ScalarFunction(s) = row else {
            return None;
        };
        // `named_struct` alternates names and values; `struct` and `row` are values alone.
        let values: Vec<Expr> = match s.func.name() {
            "named_struct" if s.args.len() % 2 == 0 => {
                s.args.iter().skip(1).step_by(2).cloned().collect()
            }
            "struct" | "row" => s.args.clone(),
            _ => return None,
        };
        if values.is_empty() {
            return None;
        }
        let mut params = f.params.clone();
        params.args = vec![Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(&self.key),
            values,
        ))];
        let counted = Expr::AggregateFunction(AggregateFunction {
            func: Arc::clone(&f.func),
            params,
        });
        Some(counted.alias(e.schema_name().to_string()))
    }
}
