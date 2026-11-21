//! `DuckDoubles`: DECIMAL to DOUBLE the way DuckDB rounds it, which is not always correctly.
//!
//! DuckDB casts a DECIMAL to DOUBLE directly while the unscaled integer is within 2^53, and
//! otherwise as the integer part plus the fraction over 10^scale. Above DECIMAL(18) the unscaled
//! value is a hugeint, which DuckDB converts as `lower + upper * 2^64`, rounding twice. arrow rounds
//! once, correctly, and so disagreed with DuckDB in the last place on 24 of 3,011 HUGEINTs sampled,
//! enough to move a DOUBLE fold over a delegation ledger by 16,384 wei. Modelled on DuckDB 1.5 and
//! checked against it on 3,011 HUGEINTs and 1,458 DECIMAL(38,9)s, all equal.
//!
//! The rule runs last, after the passes that add casts of their own.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Float64Array};
use arrow::datatypes::{DataType, Decimal128Type};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TryCast, Volatility,
};
use datafusion_optimizer::analyzer::AnalyzerRule;

/// DuckDB's `Hugeint::Cast<double>`.
fn hugeint(v: i128) -> f64 {
    let (upper, lower) = ((v >> 64) as i64, v as u64);
    if upper == -1 {
        -((u64::MAX - lower) as f64) - 1.0
    } else {
        lower as f64 + upper as f64 * 18446744073709551616.0
    }
}

/// DuckDB's `TryCastDecimalToFloatingPoint`, for a DECIMAL of `precision` and `scale`.
pub fn decimal_to_double(v: i128, precision: u8, scale: i8) -> f64 {
    let int = |x: i128| if precision > 18 { hugeint(x) } else { x as f64 };
    let unit = 10f64.powi(scale as i32);
    if scale <= 0 || v.unsigned_abs() <= 1u128 << 53 {
        return int(v) / unit;
    }
    let p = 10i128.pow(scale as u32);
    int(v / p) + int(v % p) / unit
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecimalToDouble {
    sig: Signature,
}

impl DecimalToDouble {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for DecimalToDouble {
    fn name(&self) -> &str {
        "burrmill_decimal_to_double"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [t @ DataType::Decimal128(..)] => Ok(vec![t.clone()]),
            _ => plan_err!("burrmill_decimal_to_double takes a DECIMAL"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    // NULL exactly where the DECIMAL is, as the cast it replaces: a union's schema derived before
    // this rule ran would otherwise disagree with the physical plan.
    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<arrow::datatypes::FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(arrow::datatypes::Field::new(
            self.name(),
            DataType::Float64,
            nullable,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [a] = args.args.as_slice() else {
            return plan_err!("burrmill_decimal_to_double takes one argument");
        };
        let DataType::Decimal128(p, s) = a.data_type() else {
            return plan_err!("burrmill_decimal_to_double takes a DECIMAL");
        };
        let convert = |d: &arrow::array::Decimal128Array| -> Float64Array {
            d.iter()
                .map(|v| v.map(|v| decimal_to_double(v, p, s)))
                .collect()
        };
        Ok(match a {
            ColumnarValue::Array(x) => ColumnarValue::Array(Arc::new(convert(
                x.as_primitive::<Decimal128Type>(),
            )) as ArrayRef),
            ColumnarValue::Scalar(x) => {
                let d = x.to_array()?;
                ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &convert(d.as_primitive::<Decimal128Type>()),
                    0,
                )?)
            }
        })
    }
}

#[derive(Debug)]
pub struct DuckDoubles {
    f: Arc<ScalarUDF>,
}

impl Default for DuckDoubles {
    fn default() -> Self {
        Self {
            f: DecimalToDouble::udf(),
        }
    }
}

impl AnalyzerRule for DuckDoubles {
    fn name(&self) -> &str {
        "duck_doubles"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|p| {
            let schema = super::dag::input_schema(&p);
            let names_matter = matches!(
                p,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            );
            p.map_expressions(|e| {
                super::dag::keeping_name(e, names_matter, |e| {
                    e.transform_up(|e| {
                        let inner = match &e {
                            Expr::Cast(Cast { expr, field })
                            | Expr::TryCast(TryCast { expr, field })
                                if field.data_type() == &DataType::Float64 =>
                            {
                                expr
                            }
                            _ => return Ok(Transformed::no(e)),
                        };
                        if !matches!(inner.get_type(&schema)?, DataType::Decimal128(..)) {
                            return Ok(Transformed::no(e));
                        }
                        Ok(Transformed::yes(Expr::ScalarFunction(
                            ScalarFunction::new_udf(
                                Arc::clone(&self.f),
                                vec![inner.as_ref().clone()],
                            ),
                        )))
                    })
                })
            })
        })
        .map(|t| t.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_as_duckdb_does() {
        // Correctly rounded, this is 4.758202831081925e19; DuckDB's two roundings give ...926e19.
        assert_eq!(
            decimal_to_double(47582028310819253533, 38, 0),
            4.758202831081926e19
        );
        assert_eq!(decimal_to_double(-1, 38, 0), -1.0);
        assert_eq!(decimal_to_double(1743532650, 38, 9), 1.74353265);
        // Past 2^53, integer part plus fraction: DuckDB's 9791626625542364.0, not ...366.0.
        assert_eq!(
            decimal_to_double(9791626625542365709860864, 38, 9),
            9791626625542364.0
        );
    }
}
