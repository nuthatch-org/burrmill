//! Strict decimals (#339), off by default: a DECIMAL answer is exact at its declared scale or the
//! statement is refused. `/` and every function or aggregate that would make a DECIMAL a DOUBLE is
//! refused, `div(a, b, scale, mode)` divides at a named scale and rounding, and a cast that would
//! drop nonzero digits fails where DuckDB rounds. A cast to DOUBLE the statement writes stands.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Builder, Decimal256Builder, PrimitiveBuilder,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Decimal256Type, Field, FieldRef, Int8Type,
    Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type, i256,
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::expr::{AggregateFunction, ScalarFunction};
use datafusion_expr::{
    BinaryExpr, Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TryCast, Volatility,
};
use datafusion_optimizer::AnalyzerRule;

/// Whether an engine's statements are held to strict decimals, read as each is planned.
pub type Flag = Arc<AtomicBool>;

const MAX_DIGITS: u32 = 38;
const MAX_WIDE_DIGITS: u32 = 76;

fn is_decimal(t: &DataType) -> bool {
    matches!(t, DataType::Decimal128(..) | DataType::Decimal256(..))
}

fn input_schema(p: &LogicalPlan) -> DFSchema {
    let mut schema = DFSchema::empty();
    for i in p.inputs() {
        schema.merge(i.schema());
    }
    schema
}

/// Before DuckDB's division and coercion, where `/` and a cast are still what the statement wrote:
/// `/` between exact numbers is refused, and a DECIMAL cast to a float becomes `strict_double`, so
/// the checks after coercion take it as asked for.
#[derive(Debug)]
pub struct Written(pub Flag);

impl AnalyzerRule for Written {
    fn name(&self) -> &str {
        "strict_written"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        if !self.0.load(Ordering::Relaxed) {
            return Ok(plan);
        }
        let double = Arc::new(ScalarUDF::from(StrictDouble::default()));
        let t = plan.transform_up_with_subqueries(|p| {
            let schema = input_schema(&p);
            let t = p.map_expressions(|e| {
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| written(e, &schema, &double))?;
                if t.transformed && t.data.schema_name().to_string() != name {
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
        })?;
        Ok(t.data)
    }
}

fn written(e: Expr, schema: &DFSchema, double: &Arc<ScalarUDF>) -> Result<Transformed<Expr>> {
    let ty = |e: &Expr| e.get_type(schema).ok();
    if let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Divide,
        right,
    }) = &e
    {
        let exact = |t: Option<DataType>| t.is_some_and(|t| is_decimal(&t) || t.is_integer());
        if exact(ty(left)) && exact(ty(right)) {
            return plan_err!(
                "strict decimals: / is not exact here; write div(a, b, scale, 'half_even'), or // \
                 for integer division"
            );
        }
        return Ok(Transformed::no(e));
    }
    let (inner, to) = match &e {
        Expr::Cast(Cast { expr, field }) => (expr, field.data_type()),
        Expr::TryCast(TryCast { expr, field }) => (expr, field.data_type()),
        _ => return Ok(Transformed::no(e)),
    };
    if !to.is_floating() || !ty(inner).is_some_and(|t| is_decimal(&t)) {
        return Ok(Transformed::no(e));
    }
    let d = Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(double),
        vec![inner.as_ref().clone()],
    ));
    Ok(Transformed::yes(if *to == DataType::Float64 {
        d
    } else {
        Expr::Cast(Cast::new(Box::new(d), to.clone()))
    }))
}

/// After coercion: refuses what would answer a DECIMAL inexactly, and puts every cast that could
/// drop digits behind [`Rescale`]. `refuse_only` is the second pass, after the rules that follow
/// coercion, which may cast again.
#[derive(Debug)]
pub struct StrictDecimals {
    pub flag: Flag,
    pub refuse_only: bool,
}

impl AnalyzerRule for StrictDecimals {
    fn name(&self) -> &str {
        if self.refuse_only {
            "strict_decimals_late"
        } else {
            "strict_decimals"
        }
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        if !self.flag.load(Ordering::Relaxed) {
            return Ok(plan);
        }
        let t = plan.transform_up_with_subqueries(|p| {
            let schema = input_schema(&p);
            let t = p.map_expressions(|e| {
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| self.expr(e, &schema))?;
                if t.transformed && t.data.schema_name().to_string() != name {
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
        })?;
        Ok(t.data)
    }
}

impl StrictDecimals {
    fn expr(&self, e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        let ty = |e: &Expr| e.get_type(schema).ok();
        match &e {
            Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) => {
                let to = field.data_type();
                let Some(from) = ty(expr) else {
                    return Ok(Transformed::no(e));
                };
                if is_decimal(&from) && to.is_floating() {
                    return plan_err!(
                        "strict decimals: this turns a DECIMAL into DOUBLE unasked; write the \
                         cast if that is meant"
                    );
                }
                let text = matches!(
                    from,
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                );
                let scale = |t: &DataType| match t {
                    DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => Some(*s),
                    _ => None,
                };
                let narrows = match (scale(&from), scale(to)) {
                    (Some(s), Some(s2)) => s2 < s,
                    (Some(s), None) => s > 0 && to.is_integer(),
                    _ => false,
                };
                let to_exact = is_decimal(to) || to.is_integer();
                if self.refuse_only || !(narrows || (text && to_exact && !to.is_integer())) {
                    return Ok(Transformed::no(e));
                }
                let r = Rescale::udf(to.clone(), matches!(e, Expr::TryCast(_)));
                Ok(Transformed::yes(Expr::ScalarFunction(
                    ScalarFunction::new_udf(r, vec![expr.as_ref().clone()]),
                )))
            }
            Expr::AggregateFunction(AggregateFunction { func, params }) => {
                let decimal_in = params
                    .args
                    .iter()
                    .any(|a| ty(a).is_some_and(|t| is_decimal(&t)));
                if !decimal_in {
                    return Ok(Transformed::no(e));
                }
                let name = func.name();
                if matches!(name, "avg" | "mean") {
                    return plan_err!(
                        "strict decimals: AVG over a DECIMAL is a division; write \
                         div(SUM(x), COUNT(x), scale, 'half_even')"
                    );
                }
                if ty(&e).is_some_and(|t| t.is_floating()) {
                    return plan_err!("strict decimals: {name} over a DECIMAL answers a DOUBLE");
                }
                Ok(Transformed::no(e))
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                if func.name() == STRICT_DOUBLE {
                    return Ok(Transformed::no(e));
                }
                let decimal_in = args.iter().any(|a| ty(a).is_some_and(|t| is_decimal(&t)));
                if decimal_in && ty(&e).is_some_and(|t| t.is_floating()) {
                    return plan_err!(
                        "strict decimals: {} over a DECIMAL answers a DOUBLE",
                        func.name()
                    );
                }
                Ok(Transformed::no(e))
            }
            _ => Ok(Transformed::no(e)),
        }
    }
}

const STRICT_DOUBLE: &str = "strict_double";

/// A cast to DOUBLE the statement wrote, under strict decimals.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StrictDouble(Signature);

impl Default for StrictDouble {
    fn default() -> Self {
        Self(Signature::any(1, Volatility::Immutable))
    }
}

impl ScalarUDFImpl for StrictDouble {
    fn name(&self) -> &str {
        STRICT_DOUBLE
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args[0].cast_to(&DataType::Float64, None)
    }
}

/// The value at a row as an integer at its scale, `None` for NULL.
type Reader = dyn Fn(usize) -> Result<Option<(i256, u32)>>;

/// A cast to `to` that refuses a value it cannot hold exactly, or under `TRY_CAST` answers NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Rescale {
    sig: Signature,
    to: DataType,
    try_: bool,
}

impl Rescale {
    fn udf(to: DataType, try_: bool) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::any(1, Volatility::Immutable),
            to,
            try_,
        }))
    }
}

impl ScalarUDFImpl for Rescale {
    fn name(&self) -> &str {
        "strict_cast"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.to.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let n = a.len();
        // Each value as an integer at a scale, read exactly.
        let read: Box<Reader> = match a.data_type() {
            DataType::Decimal128(_, s) => {
                let s = (*s).max(0) as u32;
                let d = a.as_primitive::<Decimal128Type>().clone();
                Box::new(move |i| Ok((!d.is_null(i)).then(|| (i256::from_i128(d.value(i)), s))))
            }
            DataType::Decimal256(_, s) => {
                let s = (*s).max(0) as u32;
                let d = a.as_primitive::<Decimal256Type>().clone();
                Box::new(move |i| Ok((!d.is_null(i)).then(|| (d.value(i), s))))
            }
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                let t = arrow::compute::cast(&a, &DataType::Utf8)?;
                let t = t.as_string::<i32>().clone();
                let try_ = self.try_;
                Box::new(move |i| {
                    if t.is_null(i) {
                        return Ok(None);
                    }
                    match parse_exact(t.value(i)) {
                        Some(v) => Ok(Some(v)),
                        None if try_ => Ok(None),
                        None => exec_err!("strict decimals: '{}' is not a decimal", t.value(i)),
                    }
                })
            }
            other => return exec_err!("strict_cast does not read {other}"),
        };
        let shown = |v: i256, s: u32| show(v, s);
        let out: ArrayRef = match &self.to {
            DataType::Decimal128(p, s2) => {
                let mut b = Decimal128Builder::with_capacity(n);
                for i in 0..n {
                    match read(i)? {
                        None => b.append_null(),
                        Some((v, s)) => match rescale(v, s, (*s2).max(0) as u32, *p as u32) {
                            Ok(v) => b.append_value(v.as_i128()),
                            Err(_) if self.try_ => b.append_null(),
                            Err(why) => {
                                return exec_err!(
                                    "strict decimals: {} {why} DECIMAL({p},{s2}); round it first",
                                    shown(v, s)
                                );
                            }
                        },
                    }
                }
                Arc::new(b.finish().with_precision_and_scale(*p, *s2)?)
            }
            DataType::Decimal256(p, s2) => {
                let mut b = Decimal256Builder::with_capacity(n);
                for i in 0..n {
                    match read(i)? {
                        None => b.append_null(),
                        Some((v, s)) => match rescale(v, s, (*s2).max(0) as u32, *p as u32) {
                            Ok(v) => b.append_value(v),
                            Err(_) if self.try_ => b.append_null(),
                            Err(why) => {
                                return exec_err!(
                                    "strict decimals: {} {why} DECIMAL({p},{s2}); round it first",
                                    shown(v, s)
                                );
                            }
                        },
                    }
                }
                Arc::new(b.finish().with_precision_and_scale(*p, *s2)?)
            }
            t if t.is_integer() => {
                macro_rules! ints {
                    ($ty:ty) => {{
                        let mut b = PrimitiveBuilder::<$ty>::with_capacity(n);
                        for i in 0..n {
                            match read(i)? {
                                None => b.append_null(),
                                Some((v, s)) => {
                                    let r = rescale(v, s, 0, MAX_WIDE_DIGITS).and_then(|v| {
                                        v.to_i128()
                                            .and_then(|v| {
                                                <<$ty as ArrowPrimitiveType>::Native>::try_from(v)
                                                    .ok()
                                            })
                                            .ok_or("does not fit")
                                    });
                                    match r {
                                        Ok(v) => b.append_value(v),
                                        Err(_) if self.try_ => b.append_null(),
                                        Err(why) => {
                                            return exec_err!(
                                                "strict decimals: {} {why} {t}; round it first",
                                                shown(v, s)
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Arc::new(b.finish()) as ArrayRef
                    }};
                }
                match t {
                    DataType::Int8 => ints!(Int8Type),
                    DataType::Int16 => ints!(Int16Type),
                    DataType::Int32 => ints!(Int32Type),
                    DataType::Int64 => ints!(Int64Type),
                    DataType::UInt8 => ints!(UInt8Type),
                    DataType::UInt16 => ints!(UInt16Type),
                    DataType::UInt32 => ints!(UInt32Type),
                    _ => ints!(UInt64Type),
                }
            }
            other => return exec_err!("strict_cast does not write {other}"),
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

fn pow10(n: u32) -> i256 {
    let ten = i256::from_i128(10);
    (0..n).fold(i256::ONE, |x, _| x.wrapping_mul(ten))
}

/// `v` at scale `s` as an integer at scale `s2` under `p` digits, or why it cannot be one.
fn rescale(v: i256, s: u32, s2: u32, p: u32) -> std::result::Result<i256, &'static str> {
    let v = if s2 < s {
        let d = pow10(s - s2);
        if v.wrapping_rem(d) != i256::ZERO {
            return Err("has digits past");
        }
        v.wrapping_div(d)
    } else {
        v.checked_mul(pow10(s2 - s)).ok_or("does not fit")?
    };
    if v.wrapping_abs() >= pow10(p) {
        return Err("does not fit");
    }
    Ok(v)
}

/// `[+-]digits[.digits]`, spaces around it allowed, as an integer and its scale, to 76 digits.
fn parse_exact(t: &str) -> Option<(i256, u32)> {
    let t = t.trim();
    let (neg, t) = match t.as_bytes().first()? {
        b'-' => (true, &t[1..]),
        b'+' => (false, &t[1..]),
        _ => (false, t),
    };
    let (whole, frac) = t.split_once('.').unwrap_or((t, ""));
    if whole.is_empty() && frac.is_empty() {
        return None;
    }
    let digits = || whole.bytes().chain(frac.bytes());
    if !digits().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let significant = digits().skip_while(|b| *b == b'0').count();
    if significant > MAX_WIDE_DIGITS as usize {
        return None;
    }
    let ten = i256::from_i128(10);
    let mut v = i256::ZERO;
    for b in digits() {
        v = v
            .checked_mul(ten)?
            .checked_add(i256::from_i128((b - b'0') as i128))?;
    }
    let scale = u32::try_from(frac.len())
        .ok()
        .filter(|s| *s <= MAX_WIDE_DIGITS)?;
    Some((if neg { v.wrapping_neg() } else { v }, scale))
}

fn show(v: i256, s: u32) -> String {
    let digits = v.wrapping_abs().to_string();
    if s == 0 {
        return v.to_string();
    }
    let s = s as usize;
    let padded = format!("{digits:0>width$}", width = s + 1);
    let (w, f) = padded.split_at(padded.len() - s);
    format!("{}{w}.{f}", if v < i256::ZERO { "-" } else { "" })
}

/// How [`Div`] rounds the last digit it keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rounding {
    HalfEven,
    HalfUp,
    Down,
    Up,
    Floor,
    Ceiling,
}

impl Rounding {
    fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "half_even" => Self::HalfEven,
            "half_up" => Self::HalfUp,
            "down" => Self::Down,
            "up" => Self::Up,
            "floor" => Self::Floor,
            "ceiling" => Self::Ceiling,
            _ => return None,
        })
    }
}

/// `div(a, b, scale, mode)`: `a / b` exactly, rounded once to `scale` digits by `mode` (`half_even`,
/// `half_up`, `down`, `up`, `floor` or `ceiling`), as DECIMAL(38, scale). Refuses a zero divisor and
/// a quotient past 38 digits.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Div(Signature);

impl Div {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self(Signature::user_defined(
            Volatility::Immutable,
        ))))
    }
}

fn literal_arg<'a>(args: &'a ReturnFieldArgs, i: usize) -> Option<&'a ScalarValue> {
    args.scalar_arguments.get(i).copied().flatten()
}

impl ScalarUDFImpl for Div {
    fn name(&self) -> &str {
        "div"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        let [a, b, s, m] = args else {
            return plan_err!("div takes (a, b, scale, rounding)");
        };
        let num = |t: &DataType| match t {
            DataType::Decimal128(..) => Ok(t.clone()),
            t if t.is_integer() || t.is_null() => Ok(DataType::Decimal128(38, 0)),
            DataType::Decimal256(..) => plan_err!(
                "div takes DECIMALs of up to 38 digits; cast a wider one to DECIMAL(38, s) first"
            ),
            other => plan_err!("div divides integers and DECIMALs, not {other}"),
        };
        if !(s.is_integer()
            && matches!(m, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8))
        {
            return plan_err!("div takes a whole-number scale and a rounding mode in quotes");
        }
        Ok(vec![num(a)?, num(b)?, DataType::Int64, DataType::Utf8])
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        plan_err!("div's type is its scale's")
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let scale = match literal_arg(&args, 2) {
            Some(ScalarValue::Int64(Some(s))) if (0..=MAX_DIGITS as i64).contains(s) => *s as i8,
            _ => return plan_err!("div's scale is a constant from 0 to 38"),
        };
        match literal_arg(&args, 3) {
            Some(ScalarValue::Utf8(Some(m))) if Rounding::parse(m).is_some() => {}
            _ => {
                return plan_err!(
                    "div's rounding is one of 'half_even', 'half_up', 'down', 'up', 'floor', \
                     'ceiling'"
                );
            }
        }
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Decimal128(MAX_DIGITS as u8, scale),
            true,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let DataType::Decimal128(_, scale) = args.return_field.data_type().clone() else {
            return exec_err!("div answers a DECIMAL");
        };
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(mode))) = &args.args[3] else {
            return exec_err!("div's rounding is a constant");
        };
        let mode = Rounding::parse(mode).expect("checked when typed");
        let scalar = args.args[..2]
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args[..2])?;
        let scale_of = |a: &ArrayRef| match a.data_type() {
            DataType::Decimal128(_, s) => (*s).max(0) as u32,
            _ => 0,
        };
        let (sa, sb) = (scale_of(&arrays[0]), scale_of(&arrays[1]));
        let a = arrays[0].as_primitive::<Decimal128Type>();
        let b = arrays[1].as_primitive::<Decimal128Type>();
        let mut out = Decimal128Builder::with_capacity(a.len());
        for i in 0..a.len() {
            if a.is_null(i) || b.is_null(i) {
                out.append_null();
                continue;
            }
            match divide(a.value(i), sa, b.value(i), sb, scale as u32, mode) {
                Ok(q) => out.append_value(q),
                Err(why) => return exec_err!("div: {why}"),
            }
        }
        let out: ArrayRef = Arc::new(out.finish().with_precision_and_scale(38, scale)?);
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// `(a / 10^sa) / (b / 10^sb)` at scale `s`, rounded once by `mode`, as an integer at that scale.
fn divide(
    a: i128,
    sa: u32,
    b: i128,
    sb: u32,
    s: u32,
    mode: Rounding,
) -> std::result::Result<i128, &'static str> {
    if b == 0 {
        return Err("division by zero");
    }
    let ten = i256::from_i128(10);
    let limit = i256::from_i128(10i128.pow(MAX_DIGITS));
    let neg = (a < 0) != (b < 0);
    let mag = |v: i128| i256::from_i128(v).wrapping_abs();
    let (num, mut den) = (mag(a), mag(b));
    // The quotient is num * 10^k / den, k digits past the division's own.
    let k = s as i64 + sb as i64 - sa as i64;
    let (mut q, mut r);
    if k >= 0 {
        q = num / den;
        r = num % den;
        for _ in 0..k {
            r *= ten;
            q = q * ten + r / den;
            r %= den;
            if q >= limit {
                return Err("the quotient has more than 38 digits");
            }
        }
    } else {
        for _ in 0..(-k) {
            den *= ten;
        }
        q = num / den;
        r = num % den;
    }
    let zero = i256::ZERO;
    let half = r.cmp(&(den - r));
    let bump = match mode {
        Rounding::Down => false,
        Rounding::Up => r > zero,
        Rounding::Floor => r > zero && neg,
        Rounding::Ceiling => r > zero && !neg,
        Rounding::HalfUp => half != std::cmp::Ordering::Less,
        Rounding::HalfEven => {
            half == std::cmp::Ordering::Greater
                || (half == std::cmp::Ordering::Equal && q % i256::from_i128(2) != zero)
        }
    };
    if bump {
        q += i256::ONE;
    }
    if q >= limit {
        return Err("the quotient has more than 38 digits");
    }
    let q = q.as_i128();
    Ok(if neg { -q } else { q })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn division_rounds_once_by_its_mode() {
        use Rounding::*;
        // 1.005 / 1 at scale 2: the tie DuckDB's cast rounds up.
        let tie = |m| divide(1005, 3, 1, 0, 2, m).unwrap();
        assert_eq!(
            [tie(HalfEven), tie(HalfUp), tie(Down), tie(Up)],
            [100, 101, 100, 101]
        );
        let neg = |m| divide(-1005, 3, 1, 0, 2, m).unwrap();
        assert_eq!(
            [neg(HalfEven), neg(HalfUp), neg(Floor), neg(Ceiling)],
            [-100, -101, -101, -100]
        );
        assert_eq!(divide(10, 0, 3, 0, 4, HalfEven).unwrap(), 33_333);
        assert_eq!(divide(2, 0, 3, 0, 0, HalfEven).unwrap(), 1);
        // A divisor with more scale than the answer: 1 / 0.003 at scale 0.
        assert_eq!(divide(1, 0, 3, 3, 0, Down).unwrap(), 333);
        assert_eq!(divide(1, 0, 0, 0, 2, Down), Err("division by zero"));
        assert!(divide(i128::MAX / 10, 0, 1, 38, 0, Down).is_err());
    }

    #[test]
    fn a_rescale_keeps_every_digit_or_says_why() {
        let w = i256::from_i128;
        assert_eq!(rescale(w(1000), 3, 2, 10), Ok(w(100)));
        assert_eq!(rescale(w(1005), 3, 2, 10), Err("has digits past"));
        assert_eq!(rescale(w(15), 1, 3, 3), Err("does not fit"));
        assert_eq!(parse_exact(" -1.50 "), Some((w(-150), 2)));
        assert_eq!(parse_exact("1e3"), None);
        assert_eq!(parse_exact("."), None);
        assert_eq!(show(w(-5), 3), "-0.005");
        let wide = "9".repeat(76);
        assert_eq!(
            parse_exact(&wide).map(|(v, _)| show(v, 0)),
            Some(wide.clone())
        );
        assert_eq!(parse_exact(&format!("{wide}9")), None);
        assert_eq!(rescale(pow10(75), 0, 1, 76), Err("does not fit"));
    }
}
