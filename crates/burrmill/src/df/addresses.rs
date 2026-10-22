//! An address is 20 bytes; the host writes it as `0x` and forty lower-case hex digits. Between the
//! scan and the answer it is `FixedSizeBinary(20)`, so a sort, a join and a group key move and
//! compare 20 bytes rather than 42 (addresses-as-fixed-binary.md).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, FixedSizeBinaryBuilder, StringViewBuilder};
use arrow::datatypes::DataType;
use datafusion_catalog::Session;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, Result as DFResult, ScalarValue, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility, lit,
};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::projection::ProjectionExec;

pub const WIDTH: i32 = 20;

/// The type an address column has inside a plan.
pub fn fixed() -> DataType {
    DataType::FixedSizeBinary(WIDTH)
}

pub fn is_address(t: &DataType) -> bool {
    *t == fixed()
}

/// The 20 bytes `s` spells as `0x` and forty hex digits of either case, or none.
pub fn decode(s: &str) -> Option<[u8; 20]> {
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))?
        .as_bytes();
    if hex.len() != 40 {
        return None;
    }
    let digit = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    let mut out = [0u8; 20];
    for (i, pair) in hex.chunks(2).enumerate() {
        out[i] = (digit(pair[0])? << 4) | digit(pair[1])?;
    }
    Some(out)
}

/// `0x` and forty lower-case hex digits, as the host writes an address.
pub fn encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(42);
    s.push_str("0x");
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 15) as usize] as char);
    }
    s
}

/// `address_bytes(text)`: the 20 bytes, or NULL where the text is not an address.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AddressBytes {
    sig: Signature,
}

impl AddressBytes {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for AddressBytes {
    fn name(&self) -> &str {
        "address_bytes"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null] => {
                Ok(vec![DataType::Utf8View])
            }
            [t] if is_address(t) => Ok(vec![fixed()]),
            _ => plan_err!("address_bytes takes one text argument"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(fixed())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        if is_address(a.data_type()) {
            return Ok(args.args[0].clone());
        }
        let a = a.as_string_view();
        let mut out = FixedSizeBinaryBuilder::with_capacity(a.len(), WIDTH);
        for i in 0..a.len() {
            if a.is_null(i) {
                out.append_null();
                continue;
            }
            // A host writes every address as `0x` and forty hex digits; anything else in an
            // address column is refused rather than read as nothing.
            match decode(a.value(i)) {
                Some(b) => out.append_value(b)?,
                None => {
                    return Err(datafusion_common::DataFusionError::Execution(format!(
                        "not an address: {:?}",
                        a.value(i)
                    )));
                }
            }
        }
        finish(Arc::new(out.finish()), scalar)
    }
}

/// `address_text(bytes)`: `0x` and forty lower-case hex digits.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AddressText {
    sig: Signature,
}

impl AddressText {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for AddressText {
    fn name(&self) -> &str {
        "address_text"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [t] if is_address(t) => Ok(vec![fixed()]),
            [DataType::Null] => Ok(vec![fixed()]),
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8] => {
                Ok(vec![DataType::Utf8View])
            }
            _ => plan_err!("address_text takes one address argument"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8View)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        if !is_address(a.data_type()) {
            return Ok(args.args[0].clone());
        }
        let a = a.as_fixed_size_binary();
        let mut out = StringViewBuilder::with_capacity(a.len());
        for i in 0..a.len() {
            if a.is_null(i) {
                out.append_null();
            } else {
                out.append_value(encode(a.value(i)));
            }
        }
        finish(Arc::new(out.finish()), scalar)
    }
}

/// A filter pushed to a scan, for the segments' own text: a decoded column is the column, a
/// fixed-binary literal is its text, and a filter that still holds an address function after that
/// is not pushed (`None`); pushdown is inexact and the filter stays above the scan.
pub(super) fn as_text_filter(filter: Expr) -> DFResult<Option<Expr>> {
    let mut keep = true;
    let out = filter.transform_up(|e| match e {
        Expr::ScalarFunction(f) if f.name() == "address_bytes" && f.args.len() == 1 => {
            Ok(Transformed::yes(f.args.into_iter().next().unwrap()))
        }
        Expr::Literal(ScalarValue::FixedSizeBinary(WIDTH, Some(ref b)), _) => Ok(Transformed::yes(
            lit(ScalarValue::Utf8View(Some(encode(b)))),
        )),
        Expr::Literal(ScalarValue::FixedSizeBinary(WIDTH, None), _) => {
            Ok(Transformed::yes(lit(ScalarValue::Utf8View(None))))
        }
        Expr::ScalarFunction(ref f) if f.name() == "address_text" => {
            keep = false;
            Ok(Transformed::no(e))
        }
        Expr::Cast(datafusion_expr::Cast { ref field, .. })
        | Expr::TryCast(datafusion_expr::TryCast { ref field, .. })
            if is_address(field.data_type()) =>
        {
            keep = false;
            Ok(Transformed::no(e))
        }
        _ => Ok(Transformed::no(e)),
    })?;
    Ok(keep.then_some(out.data))
}

/// `plan` with every fixed-binary address rendered as text, for a host.
pub(super) fn rendered(
    plan: Arc<dyn ExecutionPlan>,
    state: &dyn Session,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    if !schema.fields().iter().any(|f| is_address(f.data_type())) {
        return Ok(plan);
    }
    let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
    let exprs = schema
        .fields()
        .iter()
        .map(|f| {
            let column = Expr::Column(Column::from_name(f.name()));
            let e = match is_address(f.data_type()) {
                true => text_of(column),
                false => column,
            };
            Ok((state.create_physical_expr(e, &df_schema)?, f.name().clone()))
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
}

/// By table name, the columns a host writes as addresses.
#[derive(Debug, Default, Clone)]
pub struct Noted(Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<String>>>>);

impl Noted {
    pub fn note(&self, table: &str, columns: impl IntoIterator<Item = String>) {
        let columns: Vec<String> = columns.into_iter().collect();
        let mut m = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match columns.is_empty() {
            true => m.remove(table),
            false => m.insert(table.to_string(), columns),
        };
    }
    fn of(&self, table: &str) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(table)
            .cloned()
            .unwrap_or_default()
    }
}

/// A scan of a table with address columns, decoded: the plan above it sees fixed binary.
fn decoded_scan(
    scan: datafusion_expr::TableScan,
    noted: &Noted,
) -> DFResult<Transformed<LogicalPlan>> {
    let addresses = noted.of(scan.table_name.table());
    let wanted: Vec<bool> = scan
        .projected_schema
        .fields()
        .iter()
        .map(|f| {
            addresses.contains(f.name())
                && matches!(
                    f.data_type(),
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                )
        })
        .collect();
    if !wanted.iter().any(|w| *w) {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    }
    let exprs: Vec<Expr> = scan
        .projected_schema
        .iter()
        .zip(wanted)
        .map(|((q, f), w)| {
            let c = Expr::Column(Column::from((q, f)));
            match w {
                true => Expr::ScalarFunction(ScalarFunction::new_udf(AddressBytes::udf(), vec![c]))
                    .alias(f.name()),
                false => c.alias(f.name()),
            }
        })
        .collect();
    // Plain names, so a filter pushed down by name finds the decoded column, under the table's
    // own name again, so a reference the planner qualified finds it too.
    let name = scan.table_name.clone();
    let plan = LogicalPlanBuilder::from(LogicalPlan::TableScan(scan))
        .project(exprs)?
        .alias(name)?
        .build()?;
    Ok(Transformed::yes(plan))
}

fn finish(out: ArrayRef, scalar: bool) -> DFResult<ColumnarValue> {
    Ok(if scalar {
        ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
    } else {
        ColumnarValue::Array(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_either_case_and_refuses_the_rest() {
        let a = "0xB2BB92D0de618878e438b55d5846cfecd9301105";
        let b = decode(a).unwrap();
        assert_eq!(encode(&b), a.to_ascii_lowercase());
        assert_eq!(decode("0x00"), None);
        assert_eq!(decode("b2bb92d0de618878e438b55d5846cfecd9301105"), None);
        assert_eq!(decode("0xg2bb92d0de618878e438b55d5846cfecd9301105"), None);
        assert_eq!(decode(""), None);
        assert_eq!(decode(&encode(&[0; 20])), Some([0; 20]));
    }
}

/// Functions that keep an address an address when every argument is one.
const KEEPS_TYPE: &[&str] = &[
    "address_bytes",
    "address_text",
    "coalesce",
    "nullif",
    "ifnull",
    "nvl",
    "nvl2",
    "greatest",
    "least",
];

/// Aggregates and window functions that take an address as it is.
const KEEPS_AGGREGATE: &[&str] = &[
    "count",
    "min",
    "max",
    "array_agg",
    "first_value",
    "last_value",
    "any_value",
    "nth_value",
    "lag",
    "lead",
    "approx_distinct",
];

fn text_of(e: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(AddressText::udf(), vec![e]))
}

fn fixed_literal(bytes: [u8; 20]) -> Expr {
    lit(ScalarValue::FixedSizeBinary(WIDTH, Some(bytes.to_vec())))
}

/// The address a text literal spells, if it is one.
fn literal_address(e: &Expr) -> Option<[u8; 20]> {
    match e {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => decode(s),
        _ => None,
    }
}

/// Where an address meets anything that is not one, the address becomes its text; where it meets
/// text that spells an address, the text becomes the address. `lower` on an address is the
/// address. Before coercion, which would otherwise refuse fixed binary beside text.
#[derive(Debug)]
pub struct Addresses(pub Noted);

impl AnalyzerRule for Addresses {
    fn name(&self) -> &str {
        "addresses"
    }
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform_up_with_subqueries(|p| {
            let p = match p {
                LogicalPlan::TableScan(scan) => decoded_scan(scan, &self.0)?,
                LogicalPlan::Union(u) => textual_union(u)?,
                LogicalPlan::Join(j) => textual_join_keys(j)?,
                p => Transformed::no(p),
            };
            let mut schema = DFSchema::empty();
            for i in p.data.inputs() {
                schema.merge(i.schema());
            }
            let names_matter = matches!(
                p.data,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            );
            let was = p.transformed;
            let mut t = p.data.map_expressions(|e| {
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| rewrite(e, &schema))?;
                if t.transformed && names_matter && t.data.schema_name().to_string() != name {
                    Ok(Transformed::yes(t.data.alias(name)))
                } else {
                    Ok(t)
                }
            })?;
            t.transformed |= was;
            // A node keeps the schema it was built with when a child changes under it; with its
            // expressions rewritten for the child it has, it can be told what it holds.
            t.data = t.data.recompute_schema()?;
            Ok(t)
        })
        .map(|t| t.data)
    }
}

fn is_address_expr(e: &Expr, schema: &DFSchema) -> bool {
    e.get_type(schema).is_ok_and(|t| is_address(&t))
}

fn is_textual(e: &Expr, schema: &DFSchema) -> bool {
    e.get_type(schema)
        .is_ok_and(|t| matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8))
}

/// `e` beside something that is not an address: its text if it is one.
fn textual(e: Expr, schema: &DFSchema) -> Expr {
    match is_address_expr(&e, schema) {
        true => text_of(e),
        false => e,
    }
}

fn rewrite(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    use datafusion_expr::expr::InList;
    use datafusion_expr::{Between, BinaryExpr, Case, Cast, Like, Operator, TryCast};
    Ok(match e {
        Expr::ScalarFunction(f) => {
            let addresses: Vec<bool> = f.args.iter().map(|a| is_address_expr(a, schema)).collect();
            if !addresses.iter().any(|a| *a) {
                return Ok(Transformed::no(Expr::ScalarFunction(f)));
            }
            match f.name() {
                "lower" if f.args.len() == 1 => {
                    Transformed::yes(f.args.into_iter().next().unwrap())
                }
                name if KEEPS_TYPE.contains(&name) && addresses.iter().all(|a| *a) => {
                    Transformed::no(Expr::ScalarFunction(f))
                }
                _ => {
                    let args = f
                        .args
                        .into_iter()
                        .zip(addresses)
                        .map(|(a, is)| if is { text_of(a) } else { a })
                        .collect();
                    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(f.func, args)))
                }
            }
        }
        Expr::AggregateFunction(mut f) => {
            if KEEPS_AGGREGATE.contains(&f.func.name()) {
                return Ok(Transformed::no(Expr::AggregateFunction(f)));
            }
            let mut changed = false;
            f.params.args = f
                .params
                .args
                .into_iter()
                .map(|a| {
                    let t = textual(a, schema);
                    changed |=
                        matches!(t, Expr::ScalarFunction(ref s) if s.name() == "address_text");
                    t
                })
                .collect();
            match changed {
                true => Transformed::yes(Expr::AggregateFunction(f)),
                false => Transformed::no(Expr::AggregateFunction(f)),
            }
        }
        Expr::WindowFunction(mut w) => {
            // A frame over an address orders by its text: DataFusion runs a RANGE frame over
            // what it can compare as a scalar, and byte order and lower-case hex order agree.
            let mut changed = false;
            w.params.order_by = w
                .params
                .order_by
                .into_iter()
                .map(|mut s| {
                    if is_address_expr(&s.expr, schema) {
                        changed = true;
                        s.expr = text_of(s.expr);
                    }
                    s
                })
                .collect();
            let name = w.fun.name();
            if KEEPS_AGGREGATE.contains(&name) {
                return Ok(match changed {
                    true => Transformed::yes(Expr::WindowFunction(w)),
                    false => Transformed::no(Expr::WindowFunction(w)),
                });
            }
            let before = w.params.args.len();
            w.params.args = w
                .params
                .args
                .into_iter()
                .map(|a| {
                    let is = is_address_expr(&a, schema);
                    changed |= is;
                    if is { text_of(a) } else { a }
                })
                .collect();
            debug_assert_eq!(before, w.params.args.len());
            match changed {
                true => Transformed::yes(Expr::WindowFunction(w)),
                false => Transformed::no(Expr::WindowFunction(w)),
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (l, r) = (
                is_address_expr(&left, schema),
                is_address_expr(&right, schema),
            );
            if !l && !r {
                return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                    left,
                    op,
                    right,
                })));
            }
            let comparison = matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
            );
            if l && r {
                return Ok(match comparison {
                    true => Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })),
                    false => Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(text_of(*left)),
                        op,
                        right: Box::new(text_of(*right)),
                    })),
                });
            }
            let (addr, other) = if l { (left, right) } else { (right, left) };
            let null = matches!(*other, Expr::Literal(ScalarValue::Null, _));
            let (addr, other) = match (comparison, literal_address(&other), null) {
                (true, Some(a), _) => (addr, Box::new(fixed_literal(a))),
                (true, None, true) => (addr, other),
                _ => (Box::new(text_of(*addr)), other),
            };
            let (left, right) = if l { (addr, other) } else { (other, addr) };
            Transformed::yes(Expr::BinaryExpr(BinaryExpr { left, op, right }))
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            let (e, p) = (
                is_address_expr(&expr, schema),
                is_address_expr(&pattern, schema),
            );
            if !e && !p {
                return Ok(Transformed::no(Expr::Like(Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive,
                })));
            }
            Transformed::yes(Expr::Like(Like {
                negated,
                expr: Box::new(textual(*expr, schema)),
                pattern: Box::new(textual(*pattern, schema)),
                escape_char,
                case_insensitive,
            }))
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            if !is_address_expr(&expr, schema) {
                return Ok(Transformed::no(Expr::SimilarTo(Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive,
                })));
            }
            Transformed::yes(Expr::SimilarTo(Like {
                negated,
                expr: Box::new(text_of(*expr)),
                pattern,
                escape_char,
                case_insensitive,
            }))
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            if !is_address_expr(&expr, schema) {
                return Ok(Transformed::no(Expr::InList(InList {
                    expr,
                    list,
                    negated,
                })));
            }
            let decoded: Option<Vec<Expr>> = list
                .iter()
                .map(|l| {
                    if is_address_expr(l, schema) {
                        Some(l.clone())
                    } else {
                        literal_address(l).map(fixed_literal)
                    }
                })
                .collect();
            Transformed::yes(match decoded {
                Some(list) => Expr::InList(InList {
                    expr,
                    list,
                    negated,
                }),
                None => Expr::InList(InList {
                    expr: Box::new(text_of(*expr)),
                    list,
                    negated,
                }),
            })
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            if !is_address_expr(&expr, schema) {
                return Ok(Transformed::no(Expr::Between(Between {
                    expr,
                    negated,
                    low,
                    high,
                })));
            }
            let bound = |b: Box<Expr>| -> Option<Box<Expr>> {
                if is_address_expr(&b, schema) {
                    Some(b)
                } else {
                    literal_address(&b).map(|a| Box::new(fixed_literal(a)))
                }
            };
            Transformed::yes(match (bound(low.clone()), bound(high.clone())) {
                (Some(low), Some(high)) => Expr::Between(Between {
                    expr,
                    negated,
                    low,
                    high,
                }),
                _ => Expr::Between(Between {
                    expr: Box::new(text_of(*expr)),
                    negated,
                    low,
                    high,
                }),
            })
        }
        Expr::Cast(Cast { expr, field }) if is_address_expr(&expr, schema) => {
            Transformed::yes(match is_address(field.data_type()) {
                true => *expr,
                false => match field.data_type() {
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => text_of(*expr),
                    _ => Expr::Cast(Cast {
                        expr: Box::new(text_of(*expr)),
                        field,
                    }),
                },
            })
        }
        Expr::TryCast(TryCast { expr, field }) if is_address_expr(&expr, schema) => {
            Transformed::yes(match is_address(field.data_type()) {
                true => *expr,
                false => match field.data_type() {
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => text_of(*expr),
                    _ => Expr::TryCast(TryCast {
                        expr: Box::new(text_of(*expr)),
                        field,
                    }),
                },
            })
        }
        Expr::Case(Case {
            expr,
            when_then_expr,
            else_expr,
        }) => {
            // `CASE a WHEN 'lit'`: the operand against each `when`, as a comparison.
            let (expr, when_then_expr, mut changed) = match expr {
                Some(operand) if is_address_expr(&operand, schema) => {
                    let all = when_then_expr
                        .iter()
                        .all(|(w, _)| is_address_expr(w, schema) || literal_address(w).is_some());
                    if all {
                        let wt = when_then_expr
                            .into_iter()
                            .map(|(w, t)| match literal_address(&w) {
                                Some(a) => (Box::new(fixed_literal(a)), t),
                                None => (w, t),
                            })
                            .collect();
                        (Some(operand), wt, true)
                    } else {
                        (Some(Box::new(text_of(*operand))), when_then_expr, true)
                    }
                }
                e => (e, when_then_expr, false),
            };
            // Branches: an address beside a branch that is not one becomes text.
            let mut results: Vec<&Expr> = when_then_expr.iter().map(|(_, t)| t.as_ref()).collect();
            if let Some(e) = &else_expr {
                results.push(e);
            }
            let any_address = results.iter().any(|r| is_address_expr(r, schema));
            let any_other = results.iter().any(|r| {
                !is_address_expr(r, schema) && !matches!(r, Expr::Literal(ScalarValue::Null, _))
            });
            let (when_then_expr, else_expr) = if any_address && any_other {
                changed = true;
                (
                    when_then_expr
                        .into_iter()
                        .map(|(w, t)| (w, Box::new(textual(*t, schema))))
                        .collect(),
                    else_expr.map(|e| Box::new(textual(*e, schema))),
                )
            } else {
                (when_then_expr, else_expr)
            };
            let e = Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            });
            match changed {
                true => Transformed::yes(e),
                false => Transformed::no(e),
            }
        }
        Expr::InSubquery(mut s) => {
            let inner = s.subquery.subquery.schema().field(0).data_type().clone();
            let outer = is_address_expr(&s.expr, schema);
            match (outer, is_address(&inner)) {
                (true, false) => {
                    s.expr = Box::new(text_of(*s.expr));
                    Transformed::yes(Expr::InSubquery(s))
                }
                (false, true) if is_textual(&s.expr, schema) => {
                    // The subquery's column becomes text: its plan is projected below.
                    let sub = Arc::clone(&s.subquery.subquery);
                    let projected = textual_plan(sub)?;
                    s.subquery.subquery = Arc::new(projected);
                    Transformed::yes(Expr::InSubquery(s))
                }
                _ => Transformed::no(Expr::InSubquery(s)),
            }
        }
        e => Transformed::no(e),
    })
}

/// A join keyed on an address against anything that is not one compares their text; the keys
/// are not comparisons the expression walk meets.
fn textual_join_keys(mut j: datafusion_expr::Join) -> DFResult<Transformed<LogicalPlan>> {
    let (ls, rs) = (j.left.schema(), j.right.schema());
    let mut changed = false;
    j.on =
        j.on.into_iter()
            .map(|(l, r)| {
                let (la, ra) = (is_address_expr(&l, ls), is_address_expr(&r, rs));
                match (la, ra) {
                    (true, false) => {
                        changed = true;
                        (text_of(l), r)
                    }
                    (false, true) => {
                        changed = true;
                        (l, text_of(r))
                    }
                    _ => (l, r),
                }
            })
            .collect();
    Ok(match changed {
        true => Transformed::yes(LogicalPlan::Join(j)),
        false => Transformed::no(LogicalPlan::Join(j)),
    })
}

/// `plan` with every address column rendered as text.
fn textual_plan(plan: Arc<LogicalPlan>) -> DFResult<LogicalPlan> {
    let schema = Arc::clone(plan.schema());
    let exprs: Vec<Expr> = schema
        .iter()
        .map(|(q, f)| {
            let c = Expr::Column(Column::from((q, f)));
            match is_address(f.data_type()) {
                true => text_of(c).alias_qualified(q.cloned(), f.name()),
                false => c,
            }
        })
        .collect();
    LogicalPlanBuilder::from(Arc::unwrap_or_clone(plan))
        .project(exprs)?
        .build()
}

/// A union whose inputs disagree at a column where one is an address renders that column as text
/// on the inputs that have it as one.
fn textual_union(u: datafusion_expr::Union) -> DFResult<Transformed<LogicalPlan>> {
    let width = u.schema.fields().len();
    let mixed: Vec<bool> = (0..width)
        .map(|i| {
            let types: Vec<bool> = u
                .inputs
                .iter()
                .map(|p| is_address(p.schema().field(i).data_type()))
                .collect();
            types.iter().any(|t| *t) && !types.iter().all(|t| *t)
        })
        .collect();
    // A union keeps the schema it was built with when an input changes under it; one whose
    // inputs moved to fixed binary is rebuilt so its schema says so.
    let stale = u.inputs.iter().any(|p| {
        p.schema()
            .fields()
            .iter()
            .zip(u.schema.fields())
            .any(|(a, b)| a.data_type() != b.data_type())
    });
    if !mixed.iter().any(|m| *m) && !stale {
        return Ok(Transformed::no(LogicalPlan::Union(u)));
    }
    let mut inputs = Vec::with_capacity(u.inputs.len());
    for p in u.inputs {
        let needs = p
            .schema()
            .fields()
            .iter()
            .enumerate()
            .any(|(i, f)| mixed[i] && is_address(f.data_type()));
        if !needs {
            inputs.push(p);
            continue;
        }
        let exprs: Vec<Expr> = p
            .schema()
            .iter()
            .enumerate()
            .map(|(i, (q, f))| {
                let c = Expr::Column(Column::from((q, f)));
                match mixed[i] && is_address(f.data_type()) {
                    true => text_of(c).alias_qualified(q.cloned(), f.name()),
                    false => c,
                }
            })
            .collect();
        let projected = LogicalPlanBuilder::from(Arc::unwrap_or_clone(p))
            .project(exprs)?
            .build()?;
        inputs.push(Arc::new(projected));
    }
    // The union's schema is recomputed from its inputs.
    let rebuilt = datafusion_expr::Union::try_new_with_loose_types(inputs)?;
    Ok(Transformed::yes(LogicalPlan::Union(rebuilt)))
}
