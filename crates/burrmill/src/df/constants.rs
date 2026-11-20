//! `MoveConstants`: DuckDB's `MoveConstantsRule`, so the overflows that surface are DuckDB's.
//!
//! DuckDB rewrites `x - 95 > 0` to `x > 95` before evaluating anything, for `+`, `-` and `*` over
//! integers against an integer constant, wherever the comparison is. Its UBIGINT underflow on
//! `x < 95` therefore never happens, and Burrmill, evaluating what was written, refused a query
//! DuckDB answers. This follows DuckDB's rule case for case (`optimizer/rule/move_constants.cpp`):
//! `c - x` and a negative multiplier flip the comparison; a product not cleanly divisible decides
//! `=` and `<>` outright and is otherwise left alone; a moved constant that does not fit the
//! comparison's type decides `=` (false, or NULL) and otherwise leaves it; a NULL constant makes
//! the comparison NULL, except for `IS [NOT] DISTINCT FROM`. DuckDB's binder has already made
//! `x BETWEEN a AND b` two comparisons, so the constants move through it too.

use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, lit, when};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct MoveConstants;

impl AnalyzerRule for MoveConstants {
    fn name(&self) -> &str {
        "move_constants"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        super::dag::transform_up(plan, |p, _| {
            if !super::dag::holds(&p, could_move) {
                return Ok(Transformed::no(p));
            }
            let schema = super::dag::input_schema(&p);
            let names_matter = matches!(
                p,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            );
            let t = p.map_expressions(|e| {
                if !e.exists(|x| Ok(could_move(x)))? {
                    return Ok(Transformed::no(e));
                }
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| {
                    if let Some(split) = between(&e, &schema)? {
                        return Ok(Transformed::yes(split));
                    }
                    Ok(match settled(&e, &schema)? {
                        Some(n) => Transformed::yes(n),
                        None => Transformed::no(e),
                    })
                })?;
                if t.transformed && names_matter && t.data.schema_name().to_string() != name {
                    Ok(Transformed::yes(t.data.alias(name)))
                } else {
                    Ok(t)
                }
            })?;
            if t.transformed {
                Ok(Transformed::yes(t.data.recompute_schema()?))
            } else {
                Ok(t)
            }
        })
        .map(|t| t.data)
    }
}

/// `e` with its constants moved as far as they go, if they moved at all.
fn settled(e: &Expr, schema: &DFSchema) -> Result<Option<Expr>> {
    let Some(mut e) = moved(e, schema)? else {
        return Ok(None);
    };
    while let Some(n) = moved(&e, schema)? {
        e = n;
    }
    Ok(Some(e))
}

/// A shape `between` or `moved` may rewrite: a BETWEEN, or a comparison with arithmetic on a side.
fn could_move(e: &Expr) -> bool {
    match e {
        Expr::Between(_) => true,
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            op.supports_propagation()
                && (matches!(left.as_ref(), Expr::BinaryExpr(_))
                    || matches!(right.as_ref(), Expr::BinaryExpr(_)))
        }
        _ => false,
    }
}

/// `x [NOT] BETWEEN a AND b` over integer arithmetic with a constant, as the two comparisons
/// DuckDB binds it to, each with its constants moved.
fn between(e: &Expr, schema: &DFSchema) -> Result<Option<Expr>> {
    let Expr::Between(b) = e else {
        return Ok(None);
    };
    let arithmetic = matches!(b.expr.as_ref(), Expr::BinaryExpr(BinaryExpr { left, op: Operator::Plus | Operator::Minus | Operator::Multiply, right })
        if constant(left).is_some() || constant(right).is_some());
    if !arithmetic
        || b.expr.is_volatile()
        || constant(&b.low).is_none()
        || constant(&b.high).is_none()
    {
        return Ok(None);
    }
    let side = |op, bound: &Expr| -> Result<Expr> {
        let c = Expr::BinaryExpr(BinaryExpr::new(b.expr.clone(), op, Box::new(bound.clone())));
        Ok(settled(&c, schema)?.unwrap_or(c))
    };
    let both = side(Operator::GtEq, &b.low)?.and(side(Operator::LtEq, &b.high)?);
    Ok(Some(if b.negated { !both } else { both }))
}

/// An integer constant, as written or cast from one: `None` if `e` is not one, `Some(None)` NULL.
fn constant(e: &Expr) -> Option<(Option<i128>, DataType)> {
    let (v, t) = match e {
        Expr::Literal(v, _) => (v.clone(), v.data_type()),
        Expr::Cast(c) => match c.expr.as_ref() {
            Expr::Literal(v, _) => (
                v.cast_to(c.field.data_type()).ok()?,
                c.field.data_type().clone(),
            ),
            _ => return None,
        },
        _ => return None,
    };
    if !integral(&t) {
        return None;
    }
    if v.is_null() {
        return Some((None, t));
    }
    let n = match v {
        ScalarValue::Int8(Some(x)) => x as i128,
        ScalarValue::Int16(Some(x)) => x as i128,
        ScalarValue::Int32(Some(x)) => x as i128,
        ScalarValue::Int64(Some(x)) => x as i128,
        ScalarValue::UInt8(Some(x)) => x as i128,
        ScalarValue::UInt16(Some(x)) => x as i128,
        ScalarValue::UInt32(Some(x)) => x as i128,
        ScalarValue::UInt64(Some(x)) => x as i128,
        ScalarValue::Decimal128(Some(x), _, 0) => x,
        _ => return None,
    };
    Some((Some(n), t))
}

/// DuckDB's integral types; HUGEINT is Burrmill's DECIMAL(38,0).
fn integral(t: &DataType) -> bool {
    t.is_integer() || matches!(t, DataType::Decimal128(_, 0))
}

/// `v` as a constant of type `t`, if it fits.
fn fit(v: i128, t: &DataType) -> Option<Expr> {
    let s = match t {
        DataType::Int8 => ScalarValue::Int8(Some(i8::try_from(v).ok()?)),
        DataType::Int16 => ScalarValue::Int16(Some(i16::try_from(v).ok()?)),
        DataType::Int32 => ScalarValue::Int32(Some(i32::try_from(v).ok()?)),
        DataType::Int64 => ScalarValue::Int64(Some(i64::try_from(v).ok()?)),
        DataType::UInt8 => ScalarValue::UInt8(Some(u8::try_from(v).ok()?)),
        DataType::UInt16 => ScalarValue::UInt16(Some(u16::try_from(v).ok()?)),
        DataType::UInt32 => ScalarValue::UInt32(Some(u32::try_from(v).ok()?)),
        DataType::UInt64 => ScalarValue::UInt64(Some(u64::try_from(v).ok()?)),
        DataType::Decimal128(p, 0) if v.unsigned_abs() < 10u128.pow(*p as u32) => {
            ScalarValue::Decimal128(Some(v), *p, 0)
        }
        _ => return None,
    };
    Some(lit(s))
}

fn flip(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::Gt => Operator::Lt,
        Operator::LtEq => Operator::GtEq,
        Operator::GtEq => Operator::LtEq,
        op => op,
    }
}

/// `b` when `x` is not NULL, else NULL: DuckDB's `ConstantOrNull`.
fn or_null(x: Expr, b: bool) -> Result<Expr> {
    when(x.is_null(), lit(ScalarValue::Boolean(None))).otherwise(lit(b))
}

fn moved(e: &Expr, schema: &DFSchema) -> Result<Option<Expr>> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = e else {
        return Ok(None);
    };
    let op = *op;
    if !matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    ) {
        return Ok(None);
    }
    let ((outer, outer_type), arith, op) = match (constant(right), constant(left)) {
        (Some(c), _) => (c, left.as_ref(), op),
        (None, Some(c)) => (c, right.as_ref(), flip(op)),
        (None, None) => return Ok(None),
    };
    let Expr::BinaryExpr(BinaryExpr {
        left: a,
        op: aop,
        right: b,
    }) = arith
    else {
        return Ok(None);
    };
    if !matches!(aop, Operator::Plus | Operator::Minus | Operator::Multiply)
        || !integral(&arith.get_type(schema)?)
    {
        return Ok(None);
    }
    // The constant among the arithmetic's operands, and the other, which must be integral too.
    let (inner, x, constant_first) = match (constant(b), constant(a)) {
        (Some((v, _)), _) => (v, a.as_ref(), false),
        (None, Some((v, _))) => (v, b.as_ref(), true),
        (None, None) => return Ok(None),
    };
    if !integral(&x.get_type(schema)?) {
        return Ok(None);
    }
    let (Some(inner), Some(outer)) = (inner, outer) else {
        if matches!(op, Operator::IsDistinctFrom | Operator::IsNotDistinctFrom) {
            return Ok(None);
        }
        return Ok(Some(lit(ScalarValue::Boolean(None))));
    };
    let x = x.clone();
    let (value, op) = match aop {
        Operator::Plus => match outer.checked_sub(inner) {
            Some(v) => (v, op),
            None => return Ok(None),
        },
        Operator::Minus if !constant_first => match outer.checked_add(inner) {
            Some(v) => (v, op),
            None => return Ok(None),
        },
        Operator::Minus => match inner.checked_sub(outer) {
            Some(v) => (v, flip(op)),
            None => return Ok(None),
        },
        _ => {
            if inner == 0 {
                return Ok(None);
            }
            if (outer == i128::MIN && inner == -1) || outer % inner != 0 {
                return Ok(match op {
                    Operator::Eq => Some(or_null(x, false)?),
                    Operator::NotEq => Some(or_null(x, true)?),
                    _ => None,
                });
            }
            let op = if inner < 0 { flip(op) } else { op };
            return Ok(Some(match fit(outer / inner, &outer_type) {
                Some(c) => Expr::BinaryExpr(BinaryExpr::new(Box::new(x), op, Box::new(c))),
                None => or_null(x, false)?,
            }));
        }
    };
    Ok(match fit(value, &outer_type) {
        Some(c) => Some(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(x),
            op,
            Box::new(c),
        ))),
        None if op == Operator::Eq => Some(or_null(x, false)?),
        None => None,
    })
}
