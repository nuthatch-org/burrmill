//! `FastTextCasts`: `[TRY_]CAST(text AS DECIMAL(p,0))` without Arrow's general-purpose string parser.
//!
//! nuthatch's views cast text to HUGEINT at 305 sites, and on graph-allocations the cast was most
//! of a scan's time. Text that is an optional `-` and at most `p` significant digits is parsed
//! straight to i128. Anything else is read as DuckDB reads it ([`super::wide::duck_number`]), which
//! Arrow's cast does not: `1e3`, `1_000`. This runs last, after the checked arithmetic rule, which
//! finds lossy values by the `TRY_CAST` this replaces.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Builder};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result, ScalarValue, exec_datafusion_err, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct FastTextCasts;

impl AnalyzerRule for FastTextCasts {
    fn name(&self) -> &str {
        "fast_text_casts"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        super::dag::transform_up(plan, |p, _| {
            if !super::dag::holds(&p, is_cast) {
                return Ok(Transformed::no(p));
            }
            let schema = super::dag::input_schema(&p);
            let names_matter = matches!(
                p,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            );
            let t = p.map_expressions(|e| {
                if !e.exists(|x| Ok(is_cast(x)))? {
                    return Ok(Transformed::no(e));
                }
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| fast(e, &schema))?;
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

fn is_cast(e: &Expr) -> bool {
    matches!(e, Expr::Cast(_) | Expr::TryCast(_))
}

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)
}

fn fast(e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    // `TRY_CAST` to an integer reads hex as `CAST` does, with NULL for what it cannot; this runs
    // after the checked rule that looks for `TRY_CAST`, so replacing it here hides nothing.
    if let Expr::TryCast(datafusion_expr::TryCast { expr, field }) = &e
        && field.data_type().is_integer()
        && is_text(&expr.get_type(schema)?)
    {
        let udf = Arc::new(ScalarUDF::from(TextToInt {
            sig: Signature::any(1, Volatility::Immutable),
            to: field.data_type().clone(),
            safe: true,
        }));
        return Ok(Transformed::yes(Expr::ScalarFunction(
            ScalarFunction::new_udf(udf, vec![expr.as_ref().clone()]),
        )));
    }
    if let Expr::TryCast(datafusion_expr::TryCast { expr, field }) = &e
        && let Expr::ScalarFunction(f) = expr.as_ref()
        && f.func.name() == HUGEINT_SOURCE
        && is_text(&f.args[0].get_type(schema)?)
        && let DataType::Decimal128(p, 0) = field.data_type()
    {
        let udf = Arc::new(ScalarUDF::from(TextToDecimal {
            sig: Signature::any(1, Volatility::Immutable),
            precision: *p,
            safe: true,
            hugeint: true,
        }));
        return Ok(Transformed::yes(Expr::ScalarFunction(
            ScalarFunction::new_udf(udf, f.args.clone()),
        )));
    }
    if let Expr::TryCast(datafusion_expr::TryCast { expr, field }) = &e
        && let DataType::Decimal128(p, 0) = field.data_type()
        && is_text(&expr.get_type(schema)?)
    {
        let udf = Arc::new(ScalarUDF::from(TextToDecimal {
            sig: Signature::any(1, Volatility::Immutable),
            precision: *p,
            safe: true,
            hugeint: false,
        }));
        return Ok(Transformed::yes(Expr::ScalarFunction(
            ScalarFunction::new_udf(udf, vec![expr.as_ref().clone()]),
        )));
    }
    let Expr::Cast(Cast { expr, field }) = &e else {
        return Ok(Transformed::no(e));
    };
    if !is_text(&expr.get_type(schema)?) {
        return Ok(Transformed::no(e));
    }
    if field.data_type().is_integer() {
        let udf = Arc::new(ScalarUDF::from(TextToInt {
            sig: Signature::any(1, Volatility::Immutable),
            to: field.data_type().clone(),
            safe: false,
        }));
        return Ok(Transformed::yes(Expr::ScalarFunction(
            ScalarFunction::new_udf(udf, vec![expr.as_ref().clone()]),
        )));
    }
    let DataType::Decimal128(p, 0) = field.data_type() else {
        return Ok(Transformed::no(e));
    };
    let udf = Arc::new(ScalarUDF::from(TextToDecimal {
        sig: Signature::any(1, Volatility::Immutable),
        precision: *p,
        safe: false,
        hugeint: false,
    }));
    Ok(Transformed::yes(Expr::ScalarFunction(
        ScalarFunction::new_udf(udf, vec![expr.as_ref().clone()]),
    )))
}

/// `burrmill_text_to_decimal(x)`: `CAST(x AS DECIMAL(p,0))`, with the common case parsed directly.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TextToDecimal {
    sig: Signature,
    precision: u8,
    /// `TRY_CAST`: NULL where `CAST` would refuse.
    safe: bool,
    /// Written as HUGEINT: what DuckDB's HUGEINT holds past 38 digits refuses rather than reading NULL.
    hugeint: bool,
}

/// Text as DuckDB's DECIMAL(p,0) cast reads it, if the value has at most `p` digits.
fn duck_decimal(s: &str, precision: u8) -> Option<i128> {
    duck_int(s).filter(|v| v.unsigned_abs() < 10u128.pow(precision as u32))
}

/// An optional `-` and 1..=p significant digits, as i128; `None` sends the value to [`duck_decimal`].
fn plain(s: &str, precision: u8) -> Option<i128> {
    let b = s.as_bytes();
    let (neg, digits) = match b.first()? {
        b'-' => (true, &b[1..]),
        _ => (false, b),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let significant = digits.iter().skip_while(|&&d| d == b'0').count();
    if significant > precision as usize {
        return None;
    }
    let mut v: i128 = 0;
    for &d in digits {
        v = v * 10 + (d - b'0') as i128;
    }
    Some(if neg { -v } else { v })
}

impl ScalarUDFImpl for TextToDecimal {
    fn name(&self) -> &str {
        "burrmill_text_to_decimal"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !is_text(&args[0]) {
            return plan_err!("burrmill_text_to_decimal takes text, not {}", args[0]);
        }
        Ok(DataType::Decimal128(self.precision, 0))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let mut out = Decimal128Builder::with_capacity(a.len());
        let mut each = |_: usize, s: Option<&str>| -> Result<()> {
            let Some(s) = s else {
                out.append_null();
                return Ok(());
            };
            match plain(s, self.precision).or_else(|| duck_decimal(s, self.precision)) {
                Some(v) => out.append_value(v),
                None if self.hugeint && duck_hugeint(s).is_some() => return Err(past_decimal(s)),
                None if self.safe => out.append_null(),
                None => {
                    return Err(exec_datafusion_err!(
                        "Could not convert string '{s}' to DECIMAL({},0)",
                        self.precision
                    ));
                }
            }
            Ok(())
        };
        match a.data_type() {
            DataType::Utf8View => {
                let s = a.as_string_view();
                for i in 0..s.len() {
                    each(i, s.is_valid(i).then(|| s.value(i)))?
                }
            }
            DataType::Utf8 => {
                let s = a.as_string::<i32>();
                for i in 0..s.len() {
                    each(i, s.is_valid(i).then(|| s.value(i)))?
                }
            }
            _ => {
                let s = a.as_string::<i64>();
                for i in 0..s.len() {
                    each(i, s.is_valid(i).then(|| s.value(i)))?
                }
            }
        }
        let arr: ArrayRef = Arc::new(out.finish().with_precision_and_scale(self.precision, 0)?);
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?))
        } else {
            Ok(ColumnarValue::Array(arr))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::plain;

    #[test]
    fn the_fast_path_takes_only_what_is_plainly_an_integer() {
        assert_eq!(plain("0", 38), Some(0));
        assert_eq!(plain("-17", 38), Some(-17));
        assert_eq!(plain("00042", 38), Some(42));
        assert_eq!(plain(&"9".repeat(38), 38), Some(10i128.pow(38) - 1));
        for s in ["", "-", "+1", " 1", "1 ", "1.0", "1e3", &"9".repeat(39)] {
            assert_eq!(plain(s, 38), None, "{s:?}");
        }
        assert_eq!(plain("123", 2), None);
    }
}

/// `burrmill_text_to_int(x)`: `CAST(x AS <integer>)` as DuckDB reads it. DuckDB takes `0x`/`0X` hex
/// and `0b`/`0B` binary for integers of 64 bits or fewer (not HUGEINT, not DECIMAL), with
/// underscores between digits and surrounding spaces, no sign, and refuses what does not fit, as
/// measured with `burrmill-bench duck-eval`. A column with no such prefix goes to Arrow's cast.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TextToInt {
    sig: Signature,
    to: DataType,
    /// `TRY_CAST`: NULL where `CAST` would refuse.
    safe: bool,
}

fn duck_int_name(t: &DataType) -> &'static str {
    match t {
        DataType::Int8 => "INT8",
        DataType::Int16 => "INT16",
        DataType::Int32 => "INT32",
        DataType::Int64 => "INT64",
        DataType::UInt8 => "UINT8",
        DataType::UInt16 => "UINT16",
        DataType::UInt32 => "UINT32",
        _ => "UINT64",
    }
}

/// A `0x`/`0b` literal's value; `Some(None)` when prefixed but malformed, `None` when unprefixed.
pub(crate) fn prefixed(s: &str) -> Option<Option<u128>> {
    let t = s.trim_matches(|c: char| c.is_ascii_whitespace());
    let (radix, digits) = match t.get(..2) {
        Some("0x" | "0X") => (16, &t[2..]),
        Some("0b" | "0B") => (2, &t[2..]),
        _ => return None,
    };
    if digits.is_empty() || digits.starts_with('_') || digits.ends_with('_') {
        return Some(None);
    }
    let mut v: u128 = 0;
    for c in digits.chars().filter(|&c| c != '_') {
        let Some(d) = c.to_digit(radix) else {
            return Some(None);
        };
        match v
            .checked_mul(radix as u128)
            .and_then(|v| v.checked_add(d as u128))
        {
            Some(n) => v = n,
            None => return Some(None),
        }
    }
    Some(Some(v))
}

/// Unprefixed text as DuckDB's integer cast reads it ([`super::wide::duck_number`]); `None` for
/// anything else, or a value past `i128`.
fn duck_int(s: &str) -> Option<i128> {
    let n = super::wide::duck_number(s)?;
    let mut v: i128 = 0;
    for &d in &n.digits {
        v = v.checked_mul(10)?.checked_add(d as i128)?;
    }
    if v != 0 {
        for _ in 0..n.zeros {
            v = v.checked_mul(10)?;
        }
    }
    if n.round_up {
        v = v.checked_add(1)?;
    }
    Some(if n.neg { -v } else { v })
}

impl ScalarUDFImpl for TextToInt {
    fn name(&self) -> &str {
        "burrmill_text_to_int"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(self.to.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use arrow::array::{Int64Array, UInt64Array};
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let strict = CastOptions {
            safe: self.safe,
            ..Default::default()
        };
        let wrap = |out: ArrayRef| -> Result<ColumnarValue> {
            Ok(if scalar {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
            } else {
                ColumnarValue::Array(out)
            })
        };
        let text = cast_with_options(&a, &DataType::Utf8, &CastOptions::default())?;
        let text = text.as_string::<i32>();
        let signed = matches!(
            self.to,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        );
        let min: i128 = match self.to {
            DataType::Int8 => i8::MIN as i128,
            DataType::Int16 => i16::MIN as i128,
            DataType::Int32 => i32::MIN as i128,
            DataType::Int64 => i64::MIN as i128,
            _ => 0,
        };
        let max: u128 = match self.to {
            DataType::Int8 => i8::MAX as u128,
            DataType::Int16 => i16::MAX as u128,
            DataType::Int32 => i32::MAX as u128,
            DataType::Int64 => i64::MAX as u128,
            DataType::UInt8 => u8::MAX as u128,
            DataType::UInt16 => u16::MAX as u128,
            DataType::UInt32 => u32::MAX as u128,
            _ => u64::MAX as u128,
        };
        let refuse = |s: &str| {
            datafusion_common::DataFusionError::Execution(format!(
                "Conversion Error: Could not convert string '{s}' to {}",
                duck_int_name(&self.to)
            ))
        };
        let mut vals: Vec<Option<i128>> = Vec::with_capacity(text.len());
        for i in 0..text.len() {
            if text.is_null(i) {
                vals.push(None);
                continue;
            }
            let s = text.value(i);
            let v = match prefixed(s) {
                Some(Some(v)) if v <= max => Some(v as i128),
                Some(_) if self.safe => None,
                Some(_) => return Err(refuse(s)),
                None => match duck_int(s) {
                    Some(v) if v >= min && v <= max as i128 => Some(v),
                    _ if self.safe => None,
                    _ => return Err(refuse(s)),
                },
            };
            vals.push(v);
        }
        let wide: ArrayRef = if signed {
            Arc::new(Int64Array::from_iter(
                vals.iter().map(|v| v.map(|v| v as i64)),
            ))
        } else {
            Arc::new(UInt64Array::from_iter(
                vals.iter().map(|v| v.map(|v| v as u64)),
            ))
        };
        wrap(cast_with_options(&wide, &self.to, &strict)?)
    }
}

#[cfg(test)]
mod hex_tests {
    use super::{duck_int, prefixed};

    // Each as DuckDB 1.5 read it (`TRY_CAST(... AS BIGINT)`, `duck-eval`).
    #[test]
    fn plain_text_reads_as_duckdb_reads_it() {
        for (s, v) in [
            ("1.", Some(1)),
            (".5", Some(1)),
            ("1_000", Some(1000)),
            ("2.4999", Some(2)),
            ("-0.5", Some(-1)),
            ("1e-1", Some(0)),
            ("5e-1", Some(1)),
            ("00012", Some(12)),
            ("1 2", None),
            ("inf", None),
            ("-", None),
            ("1.5e1", Some(15)),
            ("+-1", None),
            ("\t7\n", Some(7)),
            (" 12 ", Some(12)),
            ("1.5", Some(2)),
            ("-2.5", Some(-3)),
            ("1e2", Some(100)),
            ("+7", Some(7)),
            ("", None),
            ("abc", None),
            ("9223372036854775807.5", Some(9223372036854775808)),
        ] {
            assert_eq!(duck_int(s), v, "{s:?}");
        }
    }

    // Each as DuckDB 1.5 read it (`duck-eval`).
    #[test]
    fn prefixes_read_as_duckdb_reads_them() {
        assert_eq!(prefixed("0x1F"), Some(Some(31)));
        assert_eq!(prefixed("0X1f"), Some(Some(31)));
        assert_eq!(prefixed(" 0x1f"), Some(Some(31)));
        assert_eq!(prefixed("0b101"), Some(Some(5)));
        assert_eq!(prefixed("0x1_f"), Some(Some(31)));
        assert_eq!(prefixed("0x"), Some(None));
        assert_eq!(prefixed("0xg1"), Some(None));
        assert_eq!(prefixed("-0x1f"), None);
        assert_eq!(prefixed("42"), None);
    }
}

/// A text literal as DuckDB's binder casts it to the number it is compared with; `None` where
/// DuckDB's cast would refuse.
pub(crate) fn literal_as(s: &str, to: &DataType) -> Option<ScalarValue> {
    let int = |v: i128| ScalarValue::Decimal128(Some(v), 38, 0).cast_to(to).ok();
    match to {
        t if t.is_integer() => match prefixed(s) {
            Some(v) => int(i128::try_from(v?).ok()?),
            None => int(duck_int(s)?),
        },
        DataType::Decimal128(p, 0) => {
            Some(ScalarValue::Decimal128(Some(duck_decimal(s, *p)?), *p, 0))
        }
        t => ScalarValue::Utf8(Some(s.to_string())).cast_to(t).ok(),
    }
}

/// Text as DuckDB's HUGEINT cast reads it: all of i128, where DECIMAL(38,0) stops at 38 digits.
pub(crate) fn duck_hugeint(s: &str) -> Option<i128> {
    super::wide::Wide::<3>::parse_integer(s)
        .ok()
        .flatten()?
        .to_i128()
}

fn past_decimal(shown: &str) -> datafusion_common::DataFusionError {
    exec_datafusion_err!(
        "TRY_CAST({shown} AS HUGEINT): DuckDB's HUGEINT holds it and HUGEINT here is DECIMAL(38,0), \
         which does not, so it is refused rather than read as NULL"
    )
}

/// A whole float as DuckDB's HUGEINT cast reads it: NULL from 2^127 in magnitude, -2^127 included.
pub(crate) fn float_hugeint(v: f64) -> Option<i128> {
    (v.abs() < 2f64.powi(127)).then_some(v as i128)
}

pub(crate) fn each_text(a: &ArrayRef, mut f: impl FnMut(Option<&str>) -> Result<()>) -> Result<()> {
    match a.data_type() {
        DataType::Utf8View => {
            let s = a.as_string_view();
            (0..s.len()).try_for_each(|i| f(s.is_valid(i).then(|| s.value(i))))
        }
        DataType::Utf8 => {
            let s = a.as_string::<i32>();
            (0..s.len()).try_for_each(|i| f(s.is_valid(i).then(|| s.value(i))))
        }
        DataType::LargeUtf8 => {
            let s = a.as_string::<i64>();
            (0..s.len()).try_for_each(|i| f(s.is_valid(i).then(|| s.value(i))))
        }
        t => plan_err!("expected text, got {t}"),
    }
}

pub(crate) const HUGEINT_SOURCE: &str = "burrmill_hugeint_source";

/// The text, or the rounded DOUBLE, under a `TRY_CAST(x AS HUGEINT)`, so the rules after
/// `DuckSemantics` can still tell it from `AS DECIMAL(38,0)`. Passes `x` through, refusing what
/// HUGEINT holds past 38 digits; with `fits`, whether `x` reads as a HUGEINT at all, which is that
/// cast's `IS NOT NULL`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct HugeintSource {
    sig: Signature,
    fits: bool,
}

impl HugeintSource {
    pub(crate) fn udf(fits: bool) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::any(1, Volatility::Immutable),
            fits,
        }))
    }
}

impl ScalarUDFImpl for HugeintSource {
    fn name(&self) -> &str {
        if self.fits {
            "burrmill_fits_hugeint"
        } else {
            HUGEINT_SOURCE
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !is_text(&args[0]) && args[0] != DataType::Float64 {
            return plan_err!("{} takes text or a DOUBLE, not {}", self.name(), args[0]);
        }
        Ok(if self.fits {
            DataType::Boolean
        } else {
            args[0].clone()
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let out: ArrayRef = if let Some(d) = a.as_primitive_opt::<arrow::datatypes::Float64Type>() {
            if self.fits {
                let fits = d.iter().map(|v| Some(v.and_then(float_hugeint).is_some()));
                Arc::new(fits.collect::<arrow::array::BooleanArray>())
            } else {
                for v in d.iter().flatten() {
                    if float_hugeint(v).is_some_and(|i| i.unsigned_abs() >= 10u128.pow(38)) {
                        return Err(past_decimal(&format!("{v:e}")));
                    }
                }
                a
            }
        } else if self.fits {
            let mut b = arrow::array::BooleanBuilder::with_capacity(a.len());
            each_text(&a, |s| {
                b.append_value(s.is_some_and(|s| duck_hugeint(s).is_some()));
                Ok(())
            })?;
            Arc::new(b.finish())
        } else {
            each_text(&a, |s| match s {
                Some(s) if plain(s, 38).is_none() && duck_decimal(s, 38).is_none() => {
                    match duck_hugeint(s) {
                        Some(_) => Err(past_decimal(&format!("'{s}'"))),
                        None => Ok(()),
                    }
                }
                _ => Ok(()),
            })?;
            a
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}
