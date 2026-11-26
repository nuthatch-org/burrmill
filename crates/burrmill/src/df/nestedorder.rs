//! `<`, `>`, `<=`, `>=` and `BETWEEN` between structs or lists, as DuckDB orders them: a NULL
//! inside one (a field, an element) sorts after every value, so `{'x': NULL} > {'x': 0}` and
//! `[1, NULL] > [1, 2]`. DataFusion compares nested values with Arrow's comparator at its default
//! options, which put such a NULL first and answer the opposite. A NULL struct or list itself still
//! makes the comparison NULL. Equality is the same either way and is left alone.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, make_comparator};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion_expr::expr::{Between, ScalarFunction};
use datafusion_expr::{
    BinaryExpr, ColumnarValue, Expr, ExprSchemable, Operator, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

/// Whether DuckDB's order of `t` differs from Arrow's default: a type with values nested in it.
fn nested(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(..)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
    )
}

/// `burrmill_nested_lt(a, b)` and its kin: one ordering comparison of two nested values of one
/// type, nested NULLs last.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NestedOrder {
    op: Operator,
    name: &'static str,
    sig: Signature,
}

impl NestedOrder {
    fn udf(op: Operator) -> Option<Arc<ScalarUDF>> {
        let name = match op {
            Operator::Lt => "burrmill_nested_lt",
            Operator::LtEq => "burrmill_nested_lteq",
            Operator::Gt => "burrmill_nested_gt",
            Operator::GtEq => "burrmill_nested_gteq",
            _ => return None,
        };
        Some(Arc::new(ScalarUDF::from(Self {
            op,
            name,
            sig: Signature::any(2, Volatility::Immutable),
        })))
    }
}

impl ScalarUDFImpl for NestedOrder {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [l, r] = arrays.as_slice() else {
            return exec_err!("{} takes two arguments", self.name);
        };
        if l.len() != r.len() {
            return exec_err!("{}: arguments of different lengths", self.name);
        }
        let cmp = make_comparator(
            l.as_ref(),
            r.as_ref(),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )?;
        let op = self.op;
        let values = BooleanBuffer::collect_bool(l.len(), |i| {
            let o = cmp(i, i);
            match op {
                Operator::Lt => o.is_lt(),
                Operator::LtEq => o.is_le(),
                Operator::Gt => o.is_gt(),
                _ => o.is_ge(),
            }
        });
        let out = BooleanArray::new(values, NullBuffer::union(l.nulls(), r.nulls()));
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(out)))
        }
    }
}

fn call(op: Operator, l: Expr, r: Expr) -> Option<Expr> {
    NestedOrder::udf(op).map(|f| Expr::ScalarFunction(ScalarFunction::new_udf(f, vec![l, r])))
}

/// An ordering comparison or `BETWEEN` of nested values, rewritten to [`NestedOrder`]. Runs once
/// types are coerced, so both sides have one type.
pub fn nested_order(e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(
                op,
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            ) && nested(&left.get_type(schema)?)
                && nested(&right.get_type(schema)?) =>
        {
            Ok(Transformed::yes(
                call(op, *left, *right).expect("an ordering operator"),
            ))
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) if nested(&expr.get_type(schema)?)
            && nested(&low.get_type(schema)?)
            && nested(&high.get_type(schema)?) =>
        {
            let both = Expr::and(
                call(Operator::GtEq, (*expr).clone(), *low).expect("an ordering operator"),
                call(Operator::LtEq, *expr, *high).expect("an ordering operator"),
            );
            Ok(Transformed::yes(if negated {
                Expr::Not(Box::new(both))
            } else {
                both
            }))
        }
        e => Ok(Transformed::no(e)),
    }
}
