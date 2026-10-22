//! DuckDB functions DataFusion lacks or spells differently, each as measured on DuckDB 1.5 with
//! `burrmill-bench duck-eval`. Burrmill runs in UTC, so every timestamp here is read in UTC.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int8Builder, Int64Builder, ListBuilder, StringBuilder,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDateTime, Timelike};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

fn udf(f: impl ScalarUDFImpl + 'static) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(f))
}

pub fn all() -> Vec<Arc<ScalarUDF>> {
    vec![
        udf(Strftime(Signature::user_defined(Volatility::Immutable))),
        udf(DateDiff(
            Signature::user_defined(Volatility::Immutable),
            vec!["datediff".into()],
        )),
        udf(RegexpExtract(Signature::user_defined(
            Volatility::Immutable,
        ))),
        udf(Sign(Signature::user_defined(Volatility::Immutable))),
        udf(RegexpReplace(Signature::user_defined(
            Volatility::Immutable,
        ))),
        udf(Json {
            sig: Signature::user_defined(Volatility::Immutable),
            kind: JsonKind::Extract,
        }),
        udf(Json {
            sig: Signature::user_defined(Volatility::Immutable),
            kind: JsonKind::ExtractString,
        }),
        udf(Json {
            sig: Signature::user_defined(Volatility::Immutable),
            kind: JsonKind::Type,
        }),
        udf(TryMark(Signature::user_defined(Volatility::Immutable))),
        udf(FromJson(Signature::user_defined(Volatility::Immutable))),
        udf(Len(Signature::user_defined(Volatility::Immutable))),
        udf(Substr(Signature::user_defined(Volatility::Immutable))),
        udf(StringSplit(
            Signature::user_defined(Volatility::Immutable),
            vec!["str_split".into(), "string_to_array".into()],
        )),
        // Volatile, or the simplifier folds `error('x')` at planning and fails every statement
        // that merely contains it.
        error_udf(),
        udf(Hex(Signature::user_defined(Volatility::Immutable))),
        udf(Typeof(Signature::any(1, Volatility::Immutable))),
    ]
}

/// `typeof(x)`: the type of `x` as DuckDB names it. A `HUGEINT` is carried as `DECIMAL(38,0)` here
/// and is named so.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Typeof(Signature);

impl ScalarUDFImpl for Typeof {
    fn name(&self) -> &str {
        "typeof"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let name = match args.args[0].data_type() {
            DataType::Null => "\"NULL\"".to_string(),
            t => super::tables::duckdb_type(&t),
        };
        Ok(ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(
            Some(name),
        )))
    }
}

/// `hex(x)` as DuckDB prints it, upper case without leading zeros: an integer as its 64-bit two's
/// complement (a HUGEINT as its 128-bit one), text or bytes as their bytes.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Hex(Signature);

impl ScalarUDFImpl for Hex {
    fn name(&self) -> &str {
        "hex"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [t] if t.is_integer() || t.is_null() => Ok(vec![DataType::Int64]),
            [t @ DataType::Decimal128(_, 0)] => Ok(vec![t.clone()]),
            [DataType::Binary | DataType::LargeBinary | DataType::BinaryView] => {
                Ok(vec![DataType::Binary])
            }
            [DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View] => Ok(vec![DataType::Utf8]),
            _ => plan_err!("hex takes an integer, text or bytes"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let mut b = StringBuilder::new();
        let bytes = |v: &[u8]| v.iter().map(|x| format!("{x:02X}")).collect::<String>();
        match a.data_type() {
            DataType::Int64 => {
                let a = a.as_primitive::<arrow::datatypes::Int64Type>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        b.append_null()
                    } else {
                        b.append_value(format!("{:X}", a.value(i)))
                    }
                }
            }
            // HUGEINT, as its 128-bit two's complement.
            DataType::Decimal128(..) => {
                let a = a.as_primitive::<arrow::datatypes::Decimal128Type>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        b.append_null()
                    } else {
                        b.append_value(format!("{:X}", a.value(i)))
                    }
                }
            }
            DataType::Binary => {
                let a = a.as_binary::<i32>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        b.append_null()
                    } else {
                        b.append_value(bytes(a.value(i)))
                    }
                }
            }
            _ => {
                let a = a.as_string::<i32>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        b.append_null()
                    } else {
                        b.append_value(bytes(a.value(i).as_bytes()))
                    }
                }
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

pub(super) fn error_udf() -> Arc<ScalarUDF> {
    udf(Error(Signature::user_defined(Volatility::Volatile)))
}

/// `error(text)`: fails the statement with `text` wherever a row reaches it. Typed NULL, as DuckDB
/// types it, so it sits in any `CASE` branch.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Error(Signature);

impl ScalarUDFImpl for Error {
    fn name(&self) -> &str {
        "error"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [_] => Ok(vec![DataType::Utf8]),
            _ => plan_err!("error takes one argument"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.number_rows == 0 {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        let text = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let text = text.as_string::<i32>();
        exec_err!("{}", if text.is_null(0) { "" } else { text.value(0) })
    }
}

fn scalar_out(scalar: bool, out: ArrayRef) -> Result<ColumnarValue> {
    Ok(if scalar {
        ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
    } else {
        ColumnarValue::Array(out)
    })
}

fn is_time(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
    )
}

/// Any timestamp or date as microseconds since the epoch, UTC.
fn micros(a: &ArrayRef) -> Result<ArrayRef> {
    Ok(cast(a, &DataType::Timestamp(TimeUnit::Microsecond, None))?)
}

fn naive(us: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_micros(us).map(|d| d.naive_utc())
}

/// `CAST(timestamp AS VARCHAR)` as DuckDB writes it: a space, the fraction's trailing zeros
/// dropped, and `+00` on one with a zone.
pub fn timestamp_text(us: i64, zoned: bool) -> Option<String> {
    let t = naive(us)?;
    let mut s = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        t.year(),
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    let frac = us.rem_euclid(1_000_000);
    if frac != 0 {
        s.push_str(format!(".{frac:06}").trim_end_matches('0'));
    }
    if zoned {
        s.push_str("+00");
    }
    Some(s)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimestampText(pub Signature);

impl ScalarUDFImpl for TimestampText {
    fn name(&self) -> &str {
        "burrmill_timestamp_text"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        Ok(args.to_vec())
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let zoned = matches!(a.data_type(), DataType::Timestamp(_, Some(_)));
        let us = micros(&a)?;
        let us = us.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
        let mut b = StringBuilder::new();
        for i in 0..us.len() {
            match us
                .is_valid(i)
                .then(|| timestamp_text(us.value(i), zoned))
                .flatten()
            {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

/// Returns its argument, declared nullable. See `dialect::case_nullability`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Nullable(pub Signature);

impl ScalarUDFImpl for Nullable {
    fn name(&self) -> &str {
        "burrmill_nullable"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        Ok(args.to_vec())
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(args.args[0].clone())
    }
}

/// One step of a DuckDB JSON path.
#[derive(Debug, PartialEq)]
enum Step {
    Key(String),
    Index(i64),
}

/// A DuckDB JSON path: `$` then `.key`, `."quoted key"`, `[n]` or `[#-n]` (from the end); a string
/// without `$` is one key. `None` for a path it does not read, which is refused.
fn json_path(p: &str) -> Option<Vec<Step>> {
    let Some(mut rest) = p.strip_prefix('$') else {
        return Some(vec![Step::Key(p.to_string())]);
    };
    let mut steps = Vec::new();
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix(".\"") {
            let end = r.find('"')?;
            steps.push(Step::Key(r[..end].to_string()));
            rest = &r[end + 1..];
        } else if let Some(r) = rest.strip_prefix('.') {
            let end = r.find(['.', '[']).unwrap_or(r.len());
            steps.push(Step::Key(r[..end].to_string()));
            rest = &r[end..];
        } else {
            let r = rest.strip_prefix('[')?;
            let end = r.find(']')?;
            let inner = &r[..end];
            let i = match inner.strip_prefix('#') {
                Some("") => return None,
                Some(from_end) => from_end.parse::<i64>().ok().filter(|n| *n < 0)?,
                None => inner.parse::<i64>().ok().filter(|n| *n >= 0)?,
            };
            steps.push(Step::Index(i));
            rest = &r[end + 1..];
        }
    }
    Some(steps)
}

fn json_walk<'a>(mut v: &'a serde_json::Value, steps: &[Step]) -> Option<&'a serde_json::Value> {
    for s in steps {
        v = match (s, v) {
            (Step::Key(k), serde_json::Value::Object(m)) => m.get(k)?,
            (Step::Index(i), serde_json::Value::Array(a)) => {
                let i = if *i < 0 { a.len() as i64 + i } else { *i };
                a.get(usize::try_from(i).ok()?)?
            }
            _ => return None,
        };
    }
    Some(v)
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "NULL",
        serde_json::Value::Bool(_) => "BOOLEAN",
        // DuckDB reads a non-negative integer as UBIGINT, a negative one as BIGINT.
        serde_json::Value::Number(n) if n.is_u64() => "UBIGINT",
        serde_json::Value::Number(n) if n.is_i64() => "BIGINT",
        serde_json::Value::Number(_) => "DOUBLE",
        serde_json::Value::String(_) => "VARCHAR",
        serde_json::Value::Array(_) => "ARRAY",
        serde_json::Value::Object(_) => "OBJECT",
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
enum JsonKind {
    Extract,
    ExtractString,
    Type,
}

/// `json_extract` (the value as JSON text, as DuckDB's JSON type prints), `json_extract_string`
/// (a string's own text, NULL for JSON null) and `json_type`, over DuckDB's paths. Malformed JSON
/// is refused, as DuckDB refuses it; a path that finds nothing is NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Json {
    sig: Signature,
    kind: JsonKind,
}

impl ScalarUDFImpl for Json {
    fn name(&self) -> &str {
        match self.kind {
            JsonKind::Extract => "json_extract",
            JsonKind::ExtractString => "json_extract_string",
            JsonKind::Type => "json_type",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [j] if self.kind == JsonKind::Type => Ok(vec![coerce_text(j)]),
            [j, p] if p.is_integer() => Ok(vec![coerce_text(j), DataType::Int64]),
            [j, p] => Ok(vec![coerce_text(j), coerce_text(p)]),
            _ => plan_err!("{} takes JSON text and a path", self.name()),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let doc = cast(&arrays[0], &DataType::Utf8)?;
        let doc = doc.as_string::<i32>();
        let path = arrays.get(1);
        let mut b = StringBuilder::new();
        for i in 0..doc.len() {
            let steps = match path {
                None => Some(vec![]),
                Some(p) if p.is_null(i) => None,
                Some(p) if p.data_type().is_integer() => Some(vec![Step::Index(
                    p.as_primitive::<arrow::datatypes::Int64Type>().value(i),
                )]),
                Some(p) => {
                    let text = cast(p, &DataType::Utf8)?;
                    let text = text.as_string::<i32>().value(i).to_string();
                    match json_path(&text) {
                        Some(s) => Some(s),
                        None => return exec_err!("Binder Error: JSON path error near '{text}'"),
                    }
                }
            };
            if doc.is_null(i) || steps.is_none() {
                b.append_null();
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(doc.value(i)).map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Invalid Input Error: Malformed JSON: {e}. Input: \"{}\"",
                    doc.value(i)
                ))
            })?;
            let found = json_walk(&v, steps.as_deref().unwrap_or(&[]));
            match (self.kind, found) {
                (_, None) => b.append_null(),
                (JsonKind::Type, Some(v)) => b.append_value(json_type_name(v)),
                (JsonKind::ExtractString, Some(serde_json::Value::Null)) => b.append_null(),
                (JsonKind::ExtractString, Some(serde_json::Value::String(s))) => b.append_value(s),
                (_, Some(v)) => {
                    b.append_value(serde_json::to_string(v).map_err(|e| {
                        datafusion_common::DataFusionError::Execution(e.to_string())
                    })?)
                }
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

/// The type a DuckDB JSON structure names: a type name, `[T]` for a list, `{"k": T}` for a struct.
fn structure_type(v: &serde_json::Value) -> Result<DataType> {
    use arrow::datatypes::Field;
    Ok(match v {
        serde_json::Value::String(t) => match t.to_ascii_uppercase().as_str() {
            "VARCHAR" | "TEXT" | "STRING" | "JSON" => DataType::Utf8,
            "BOOLEAN" | "BOOL" => DataType::Boolean,
            "TINYINT" => DataType::Int8,
            "SMALLINT" => DataType::Int16,
            "INTEGER" | "INT" => DataType::Int32,
            "BIGINT" => DataType::Int64,
            "UTINYINT" => DataType::UInt8,
            "USMALLINT" => DataType::UInt16,
            "UINTEGER" => DataType::UInt32,
            "UBIGINT" => DataType::UInt64,
            "HUGEINT" => DataType::Decimal128(38, 0),
            "DOUBLE" => DataType::Float64,
            "FLOAT" | "REAL" => DataType::Float32,
            other => return plan_err!("from_json: type {other} is not supported here"),
        },
        serde_json::Value::Array(a) if a.len() == 1 => {
            DataType::List(Arc::new(Field::new("l", structure_type(&a[0])?, true)))
        }
        serde_json::Value::Object(m) => DataType::Struct(
            m.iter()
                .map(|(k, t)| Ok(Field::new(k, structure_type(t)?, true)))
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        other => return plan_err!("from_json: structure {other} is not one DuckDB reads"),
    })
}

/// A JSON value as `t`, as DuckDB's `from_json` converts it: text to a number where it parses, a
/// scalar to text, NULL where it does not convert.
fn json_array(t: &DataType, values: &[Option<&serde_json::Value>]) -> Result<ArrayRef> {
    use arrow::array::*;
    use serde_json::Value as V;
    let text = |v: &V| match v {
        V::String(s) => Some(s.clone()),
        V::Null => None,
        other => serde_json::to_string(other).ok(),
    };
    let int = |v: &V| -> Option<i128> {
        match v {
            V::Number(n) => n
                .as_i64()
                .map(i128::from)
                .or_else(|| n.as_u64().map(i128::from)),
            V::String(s) => s.trim().parse::<i128>().ok(),
            V::Bool(b) => Some(*b as i128),
            _ => None,
        }
    };
    let float = |v: &V| -> Option<f64> {
        match v {
            V::Number(n) => n.as_f64(),
            V::String(s) => s.trim().parse::<f64>().ok(),
            _ => None,
        }
    };
    macro_rules! ints {
        ($b:ty, $t:ty) => {{
            let mut b = <$b>::with_capacity(values.len());
            for v in values {
                b.append_option(v.and_then(|v| int(v)).and_then(|n| <$t>::try_from(n).ok()));
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    Ok(match t {
        DataType::Utf8 => Arc::new(
            values
                .iter()
                .map(|v| v.and_then(text))
                .collect::<StringArray>(),
        ),
        DataType::Boolean => Arc::new(
            values
                .iter()
                .map(|v| match v {
                    Some(V::Bool(b)) => Some(*b),
                    Some(V::String(s)) if s.eq_ignore_ascii_case("true") => Some(true),
                    Some(V::String(s)) if s.eq_ignore_ascii_case("false") => Some(false),
                    Some(V::Number(n)) => n.as_f64().map(|f| f != 0.0),
                    _ => None,
                })
                .collect::<BooleanArray>(),
        ),
        DataType::Int8 => ints!(Int8Builder, i8),
        DataType::Int16 => ints!(Int16Builder, i16),
        DataType::Int32 => ints!(Int32Builder, i32),
        DataType::Int64 => ints!(Int64Builder, i64),
        DataType::UInt8 => ints!(UInt8Builder, u8),
        DataType::UInt16 => ints!(UInt16Builder, u16),
        DataType::UInt32 => ints!(UInt32Builder, u32),
        DataType::UInt64 => ints!(UInt64Builder, u64),
        DataType::Decimal128(p, sc) => {
            let mut b = Decimal128Builder::with_capacity(values.len());
            for v in values {
                b.append_option(v.and_then(&int));
            }
            Arc::new(b.finish().with_precision_and_scale(*p, *sc)?)
        }
        DataType::Float64 => Arc::new(
            values
                .iter()
                .map(|v| v.and_then(float))
                .collect::<Float64Array>(),
        ),
        DataType::Float32 => Arc::new(
            values
                .iter()
                .map(|v| v.and_then(float).map(|f| f as f32))
                .collect::<Float32Array>(),
        ),
        DataType::List(f) => {
            let mut offsets = vec![0i32];
            let mut items: Vec<Option<&V>> = Vec::new();
            let mut valid = Vec::with_capacity(values.len());
            for v in values {
                match v {
                    Some(V::Array(a)) => {
                        items.extend(a.iter().map(Some));
                        valid.push(true);
                    }
                    _ => valid.push(false),
                }
                offsets.push(items.len() as i32);
            }
            let child = json_array(f.data_type(), &items)?;
            Arc::new(ListArray::try_new(
                Arc::clone(f),
                arrow::buffer::OffsetBuffer::new(offsets.into()),
                child,
                Some(arrow::buffer::NullBuffer::from(valid)),
            )?)
        }
        DataType::Struct(fields) => {
            let valid: Vec<bool> = values
                .iter()
                .map(|v| matches!(v, Some(V::Object(_))))
                .collect();
            let children = fields
                .iter()
                .map(|f| {
                    let vs: Vec<Option<&V>> = values
                        .iter()
                        .map(|v| match v {
                            Some(V::Object(m)) => m.get(f.name()),
                            _ => None,
                        })
                        .collect();
                    json_array(f.data_type(), &vs)
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new(
                fields.clone(),
                children,
                Some(arrow::buffer::NullBuffer::from(valid)),
            )?)
        }
        other => return exec_err!("from_json: {other} is not supported here"),
    })
}

/// `from_json(json, structure)`: the document as the typed value its (literal) structure names.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FromJson(Signature);

impl ScalarUDFImpl for FromJson {
    fn name(&self) -> &str {
        "from_json"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [j, s] => Ok(vec![coerce_text(j), coerce_text(s)]),
            _ => plan_err!("from_json takes JSON text and a structure"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        plan_err!("from_json's type comes from its structure")
    }
    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<arrow::datatypes::FieldRef> {
        let Some(Some(structure)) = args.scalar_arguments.get(1) else {
            return plan_err!("from_json needs its structure as a literal");
        };
        let Some(text) = structure.try_as_str().flatten() else {
            return plan_err!("from_json needs its structure as text");
        };
        let v: serde_json::Value = serde_json::from_str(text).map_err(|e| {
            datafusion_common::DataFusionError::Plan(format!("from_json structure: {e}"))
        })?;
        Ok(Arc::new(arrow::datatypes::Field::new(
            self.name(),
            structure_type(&v)?,
            true,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let doc = cast(&arrays[0], &DataType::Utf8)?;
        let doc = doc.as_string::<i32>();
        let parsed = (0..doc.len())
            .map(|i| {
                if doc.is_null(i) {
                    return Ok(None);
                }
                serde_json::from_str::<serde_json::Value>(doc.value(i))
                    .map(Some)
                    .map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Invalid Input Error: Malformed JSON: {e}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let refs: Vec<Option<&serde_json::Value>> = parsed.iter().map(|v| v.as_ref()).collect();
        let out = json_array(args.return_field.data_type(), &refs)?;
        scalar_out(scalar, out)
    }
}

/// DuckDB's `len`: a list's element count or a string's character count, as BIGINT.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Len(Signature);

impl ScalarUDFImpl for Len {
    fn name(&self) -> &str {
        "len"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [t @ (DataType::List(_) | DataType::LargeList(_))] => Ok(vec![t.clone()]),
            [t] if t.is_null() => Ok(vec![DataType::Utf8]),
            [t] => Ok(vec![coerce_text(t)]),
            _ => plan_err!("len takes a list or a string"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let mut b = Int64Builder::with_capacity(a.len());
        match a.data_type() {
            DataType::List(_) => {
                let l = a.as_list::<i32>();
                for i in 0..l.len() {
                    b.append_option(l.is_valid(i).then(|| l.value_length(i) as i64));
                }
            }
            DataType::LargeList(_) => {
                let l = a.as_list::<i64>();
                for i in 0..l.len() {
                    b.append_option(l.is_valid(i).then(|| l.value_length(i)));
                }
            }
            _ => {
                let t = cast(&a, &DataType::Utf8)?;
                let t = t.as_string::<i32>();
                for i in 0..t.len() {
                    b.append_option(t.is_valid(i).then(|| t.value(i).chars().count() as i64));
                }
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

/// `TRY(x)` as written; `dialect::try_as_duckdb` replaces it once types are known.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TryMark(Signature);

impl ScalarUDFImpl for TryMark {
    fn name(&self) -> &str {
        "try"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [t] => Ok(vec![t.clone()]),
            _ => plan_err!("TRY takes one expression"),
        }
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("TRY was not planned")
    }
}

/// A function call under `TRY`: evaluated for the batch, and where that fails, row by row with
/// NULL for each row that fails, as DuckDB's `TRY` gives. The whole batch first, so the ordinary
/// case costs nothing extra.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TryCall {
    pub inner: Arc<ScalarUDF>,
}

impl ScalarUDFImpl for TryCall {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(args)
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.inner.return_type(args)
    }
    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<arrow::datatypes::FieldRef> {
        let f = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(f.as_ref().clone().with_nullable(true)))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let Ok(v) = self.inner.invoke_with_args(args.clone()) {
            return Ok(v);
        }
        let n = args.number_rows;
        let mut rows: Vec<ArrayRef> = Vec::with_capacity(n);
        for i in 0..n {
            let one = ScalarFunctionArgs {
                args: args
                    .args
                    .iter()
                    .map(|a| match a {
                        ColumnarValue::Array(x) => ColumnarValue::Array(x.slice(i, 1)),
                        s => s.clone(),
                    })
                    .collect(),
                number_rows: 1,
                ..args.clone()
            };
            let row = match self
                .inner
                .invoke_with_args(one)
                .and_then(|v| v.into_array(1))
            {
                Ok(v) => v,
                Err(_) => arrow::array::new_null_array(args.return_field.data_type(), 1),
            };
            rows.push(row);
        }
        let refs: Vec<&dyn Array> = rows.iter().map(|r| r.as_ref()).collect();
        if refs.is_empty() {
            return Ok(ColumnarValue::Array(arrow::array::new_empty_array(
                args.return_field.data_type(),
            )));
        }
        Ok(ColumnarValue::Array(arrow::compute::concat(&refs)?))
    }
}

/// `to_timestamp(seconds)`: TIMESTAMP WITH TIME ZONE in microseconds, UTC, as DuckDB has it, across
/// DuckDB's whole range. DataFusion's goes through nanoseconds and overflows past 2262. Anything
/// but a number goes to DataFusion's own.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToTimestamp {
    sig: Signature,
    inner: Arc<ScalarUDF>,
}

impl ToTimestamp {
    pub fn udf(inner: Arc<ScalarUDF>) -> Arc<ScalarUDF> {
        udf(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            inner,
        })
    }
}

impl ScalarUDFImpl for ToTimestamp {
    fn name(&self) -> &str {
        "to_timestamp"
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [t] if t.is_integer() || t.is_null() => Ok(vec![DataType::Int64]),
            [t] if t.is_numeric() => Ok(vec![DataType::Float64]),
            _ => self.inner.coerce_types(args),
        }
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args {
            [DataType::Int64 | DataType::Float64] => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
            _ => self.inner.return_type(args),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use arrow::array::TimestampMicrosecondBuilder;
        if args.args.len() != 1
            || !matches!(
                args.args[0].data_type(),
                DataType::Int64 | DataType::Float64
            )
        {
            return self.inner.invoke_with_args(args);
        }
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let mut b = TimestampMicrosecondBuilder::with_capacity(a.len());
        let out_of_range = |v: String| {
            exec_err!(
                "Conversion Error: Could not convert epoch seconds {v} to TIMESTAMP WITH TIME ZONE"
            )
        };
        for i in 0..a.len() {
            if a.is_null(i) {
                b.append_null();
                continue;
            }
            let us = match a.data_type() {
                DataType::Int64 => {
                    let v = a.as_primitive::<arrow::datatypes::Int64Type>().value(i);
                    match v.checked_mul(1_000_000) {
                        Some(us) => us,
                        None => return out_of_range(v.to_string()),
                    }
                }
                _ => {
                    let v = a.as_primitive::<arrow::datatypes::Float64Type>().value(i);
                    let us = (v * 1e6).round();
                    if !us.is_finite() || us.abs() >= 9.2e18 {
                        return out_of_range(v.to_string());
                    }
                    us as i64
                }
            };
            b.append_value(us);
        }
        let out: ArrayRef = Arc::new(b.finish().with_timezone("UTC"));
        scalar_out(scalar, out)
    }
}

/// DuckDB's `strftime` specifiers, which are not chrono's: `%f` is microseconds and `%g`
/// milliseconds. One it does not know is refused, as DuckDB refuses it.
fn strftime(t: NaiveDateTime, fmt: &str) -> Result<String> {
    const DAYS: [&str; 7] = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ];
    const MONTHS: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    let mut out = String::new();
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let mut spec = chars.next();
        let bare = spec == Some('-');
        if bare {
            spec = chars.next();
        }
        let two = |n: u32| {
            if bare {
                n.to_string()
            } else {
                format!("{n:02}")
            }
        };
        let hour12 = if t.hour().is_multiple_of(12) {
            12
        } else {
            t.hour() % 12
        };
        let wd = t.weekday().num_days_from_monday() as usize;
        match spec {
            Some('Y') => out.push_str(&format!("{:04}", t.year())),
            Some('y') => out.push_str(&two((t.year().rem_euclid(100)) as u32)),
            Some('m') => out.push_str(&two(t.month())),
            Some('d') => out.push_str(&two(t.day())),
            Some('H') => out.push_str(&two(t.hour())),
            Some('I') => out.push_str(&two(hour12)),
            Some('M') => out.push_str(&two(t.minute())),
            Some('S') => out.push_str(&two(t.second())),
            Some('p') => out.push_str(if t.hour() < 12 { "AM" } else { "PM" }),
            Some('j') => out.push_str(&if bare {
                t.ordinal().to_string()
            } else {
                format!("{:03}", t.ordinal())
            }),
            Some('b') => out.push_str(&MONTHS[t.month0() as usize][..3]),
            Some('B') => out.push_str(MONTHS[t.month0() as usize]),
            Some('a') => out.push_str(&DAYS[wd][..3]),
            Some('A') => out.push_str(DAYS[wd]),
            Some('w') => out.push_str(&t.weekday().num_days_from_sunday().to_string()),
            Some('u') => out.push_str(&t.weekday().number_from_monday().to_string()),
            Some('f') => out.push_str(&format!("{:06}", t.nanosecond() / 1000)),
            Some('g') => out.push_str(&format!("{:03}", t.nanosecond() / 1_000_000)),
            Some('%') => out.push('%'),
            _ => {
                return exec_err!(
                    "Invalid Input Error: Failed to parse format specifier {fmt}: Unrecognized format for strftime/strptime: %{}",
                    spec.map(String::from).unwrap_or_default()
                );
            }
        }
    }
    Ok(out)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Strftime(Signature);

impl ScalarUDFImpl for Strftime {
    fn name(&self) -> &str {
        "strftime"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [t, f] if is_time(t) || t.is_null() => Ok(vec![t.clone(), coerce_text(f)]),
            _ => plan_err!("strftime takes a timestamp or date and a format"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let us = micros(&arrays[0])?;
        let us = us.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
        let fmt = cast(&arrays[1], &DataType::Utf8)?;
        let fmt = fmt.as_string::<i32>();
        let mut b = StringBuilder::new();
        for i in 0..us.len() {
            if us.is_null(i) || fmt.is_null(i) {
                b.append_null();
                continue;
            }
            match naive(us.value(i)) {
                Some(t) => b.append_value(strftime(t, fmt.value(i))?),
                None => b.append_null(),
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

fn coerce_text(t: &DataType) -> DataType {
    if matches!(t, DataType::Utf8View | DataType::LargeUtf8) {
        t.clone()
    } else {
        DataType::Utf8
    }
}

/// Boundaries crossed between `a` and `b`, as DuckDB counts them: calendar months for month,
/// quarter and year, truncated units below a day, and whole weeks of days for week.
fn date_diff(part: &str, a: NaiveDateTime, b: NaiveDateTime, ua: i64, ub: i64) -> Result<i64> {
    let months = |t: NaiveDateTime| t.year() as i64 * 12 + t.month0() as i64;
    let floor = |u: i64, unit: i64| u.div_euclid(unit);
    Ok(match part.to_ascii_lowercase().trim_end_matches('s') {
        "year" | "yr" | "y" | "years" => b.year() as i64 - a.year() as i64,
        "quarter" => (months(b) / 3) - (months(a) / 3),
        "month" | "mon" => months(b) - months(a),
        "week" | "w" => (floor(ub, 86_400_000_000) - floor(ua, 86_400_000_000)) / 7,
        "day" | "d" | "dayofmonth" => floor(ub, 86_400_000_000) - floor(ua, 86_400_000_000),
        "hour" | "h" | "hr" => floor(ub, 3_600_000_000) - floor(ua, 3_600_000_000),
        "minute" | "min" | "m" => floor(ub, 60_000_000) - floor(ua, 60_000_000),
        "second" | "sec" | "s" => floor(ub, 1_000_000) - floor(ua, 1_000_000),
        "millisecond" | "ms" | "msec" => floor(ub, 1_000) - floor(ua, 1_000),
        "microsecond" | "us" | "usec" => match ub.checked_sub(ua) {
            Some(v) => v,
            None => return exec_err!("Overflow in date_diff of {ub} - {ua} microseconds"),
        },
        other => return exec_err!("date_diff: unit {other} is not supported here"),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DateDiff(Signature, Vec<String>);

impl ScalarUDFImpl for DateDiff {
    fn name(&self) -> &str {
        "date_diff"
    }
    fn aliases(&self) -> &[String] {
        &self.1
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [p, a, b] if (is_time(a) || a.is_null()) && (is_time(b) || b.is_null()) => {
                Ok(vec![coerce_text(p), a.clone(), b.clone()])
            }
            _ => plan_err!("date_diff takes a part and two timestamps or dates"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let part = cast(&arrays[0], &DataType::Utf8)?;
        let part = part.as_string::<i32>();
        let (a, b) = (micros(&arrays[1])?, micros(&arrays[2])?);
        let (a, b) = (
            a.as_primitive::<arrow::datatypes::TimestampMicrosecondType>(),
            b.as_primitive::<arrow::datatypes::TimestampMicrosecondType>(),
        );
        let mut out = Int64Builder::new();
        for i in 0..a.len() {
            if part.is_null(i) || a.is_null(i) || b.is_null(i) {
                out.append_null();
                continue;
            }
            let (ua, ub) = (a.value(i), b.value(i));
            match (naive(ua), naive(ub)) {
                (Some(ta), Some(tb)) => out.append_value(date_diff(part.value(i), ta, tb, ua, ub)?),
                _ => out.append_null(),
            }
        }
        scalar_out(scalar, Arc::new(out.finish()))
    }
}

/// `regexp_extract(s, pattern[, group])`: the first match's group (0, the whole match, unless
/// given), `''` where nothing matches, NULL for a NULL string.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpExtract(Signature);

impl ScalarUDFImpl for RegexpExtract {
    fn name(&self) -> &str {
        "regexp_extract"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [s, p] => Ok(vec![coerce_text(s), coerce_text(p)]),
            [s, p, g] if g.is_integer() || g.is_null() => {
                Ok(vec![coerce_text(s), coerce_text(p), DataType::Int64])
            }
            _ => plan_err!("regexp_extract takes a string, a pattern and an optional group"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let s = cast(&arrays[0], &DataType::Utf8)?;
        let s = s.as_string::<i32>();
        let p = cast(&arrays[1], &DataType::Utf8)?;
        let p = p.as_string::<i32>();
        let g = arrays
            .get(2)
            .map(|g| cast(g, &DataType::Int64))
            .transpose()?;
        let mut cache: Option<(String, regex::Regex)> = None;
        let mut b = StringBuilder::new();
        for i in 0..s.len() {
            if s.is_null(i) || p.is_null(i) {
                b.append_null();
                continue;
            }
            let group = g
                .as_ref()
                .map(|g| g.as_primitive::<arrow::datatypes::Int64Type>())
                .map_or(0, |g| if g.is_null(i) { 0 } else { g.value(i) });
            let pat = p.value(i);
            if cache.as_ref().is_none_or(|(c, _)| c != pat) {
                let re = regex::Regex::new(pat).map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!("regexp_extract: {e}"))
                })?;
                cache = Some((pat.to_string(), re));
            }
            let re = &cache.as_ref().expect("compiled").1;
            let found = re
                .captures(s.value(i))
                .and_then(|c| c.get(group.max(0) as usize).map(|m| m.as_str().to_string()));
            b.append_value(found.unwrap_or_default());
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

/// DuckDB's replacement text, RE2's: `\0`-`\9` for groups, `\\` for a backslash. `None` when it
/// names a group the pattern lacks, where RE2 refuses the rewrite and DuckDB keeps the string.
fn rewrite(caps: &regex::Captures, repl: &str, groups: usize) -> Option<String> {
    let mut out = String::new();
    let mut chars = repl.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some(d @ '0'..='9') => {
                let n = d as usize - '0' as usize;
                if n > groups {
                    return None;
                }
                out.push_str(caps.get(n).map_or("", |m| m.as_str()));
            }
            _ => return None,
        }
    }
    Some(out)
}

/// `regexp_replace(s, pattern, replacement[, options])` as DuckDB does it: the first match only
/// unless `g`, `i` for case, and a rewrite naming a missing group leaving the string as it was.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpReplace(Signature);

impl ScalarUDFImpl for RegexpReplace {
    fn name(&self) -> &str {
        "regexp_replace"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args.len() {
            3 | 4 => Ok(args.iter().map(coerce_text).collect()),
            _ => plan_err!("regexp_replace takes a string, a pattern, a replacement and options"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let text: Vec<ArrayRef> = arrays
            .iter()
            .map(|a| cast(a, &DataType::Utf8))
            .collect::<std::result::Result<_, _>>()?;
        let col = |i: usize| text.get(i).map(|a| a.as_string::<i32>());
        let (s, p, r, o) = (
            col(0).expect("s"),
            col(1).expect("p"),
            col(2).expect("r"),
            col(3),
        );
        let mut cache: Option<(String, String, regex::Regex)> = None;
        let mut b = StringBuilder::new();
        for i in 0..s.len() {
            if s.is_null(i) || p.is_null(i) || r.is_null(i) || o.is_some_and(|o| o.is_null(i)) {
                b.append_null();
                continue;
            }
            let opts = o.map_or("", |o| o.value(i));
            let (pat, global) = (p.value(i), opts.contains('g'));
            if cache
                .as_ref()
                .is_none_or(|(cp, co, _)| cp != pat || co != opts)
            {
                let re = regex::RegexBuilder::new(pat)
                    .case_insensitive(opts.contains('i') && !opts.contains('c'))
                    .dot_matches_new_line(opts.contains('s'))
                    .multi_line(opts.contains('m'))
                    .build()
                    .map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "regexp_replace: {e}"
                        ))
                    })?;
                cache = Some((pat.to_string(), opts.to_string(), re));
            }
            let re = &cache.as_ref().expect("compiled").2;
            let groups = re.captures_len() - 1;
            let (src, repl) = (s.value(i), r.value(i));
            let mut out = String::with_capacity(src.len());
            let mut last = 0;
            let mut kept = true;
            for caps in re.captures_iter(src) {
                let m = caps.get(0).expect("whole match");
                let Some(with) = rewrite(&caps, repl, groups) else {
                    kept = false;
                    break;
                };
                out.push_str(&src[last..m.start()]);
                out.push_str(&with);
                last = m.end();
                if !global {
                    break;
                }
            }
            if kept {
                out.push_str(&src[last..]);
                b.append_value(out);
            } else {
                b.append_value(src);
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

/// `sign(x)`: -1, 0 or 1 as a TINYINT, whatever the numeric type.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Sign(Signature);

impl ScalarUDFImpl for Sign {
    fn name(&self) -> &str {
        "sign"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [t] if t.is_numeric() || t.is_null() => Ok(vec![DataType::Float64]),
            _ => plan_err!("sign takes a number"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let a = a.as_primitive::<arrow::datatypes::Float64Type>();
        let mut b = Int8Builder::new();
        for i in 0..a.len() {
            if a.is_null(i) {
                b.append_null();
            } else {
                let v = a.value(i);
                b.append_value(if v > 0.0 {
                    1
                } else if v < 0.0 {
                    -1
                } else {
                    0
                });
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

/// `substr` / `substring` as DuckDB counts it. A negative start counts back from the end, a start
/// short of the first character shortens a written length, and a negative length runs backwards.
/// Arrow's substr does none of those: `substr('10', -3, 2)` came back empty.
fn duck_substr(s: &str, mut start: i64, len: Option<i64>) -> String {
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len() as i64;
    if start < 0 {
        start = n + start + 1;
    }
    let Some(mut len) = len else {
        if start < 1 {
            start = 1;
        }
        if start > n {
            return String::new();
        }
        return chars[(start as usize) - 1..].iter().collect();
    };
    if start < 1 {
        len = len.saturating_sub(1i64.saturating_sub(start));
        start = 1;
    }
    if len < 0 {
        let end = start;
        let begin = (start.saturating_add(len)).max(1);
        if end <= begin || begin > n {
            return String::new();
        }
        let end = end.min(n + 1);
        return chars[(begin as usize) - 1..(end as usize) - 1]
            .iter()
            .collect();
    }
    if len == 0 || start > n {
        return String::new();
    }
    let from = (start as usize) - 1;
    let take = (len as usize).min(chars.len() - from);
    chars[from..from + take].iter().collect()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Substr(Signature);

impl ScalarUDFImpl for Substr {
    fn name(&self) -> &str {
        "burrmill_substr"
    }
    fn aliases(&self) -> &[String] {
        &[]
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        let int = |t: &DataType| {
            if t.is_integer() || matches!(t, DataType::Decimal128(_, 0) | DataType::Null) {
                Ok(DataType::Int64)
            } else {
                plan_err!("substr start and length are integers, not {t}")
            }
        };
        match args {
            [s, start] => Ok(vec![coerce_text(s), int(start)?]),
            [s, start, len] => Ok(vec![coerce_text(s), int(start)?, int(len)?]),
            _ => plan_err!("substr takes a string, a start, and an optional length"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let s = cast(&arrays[0], &DataType::Utf8)?;
        let s = s.as_string::<i32>();
        let start = arrays[1].as_primitive::<arrow::datatypes::Int64Type>();
        let len = arrays
            .get(2)
            .map(|a| a.as_primitive::<arrow::datatypes::Int64Type>());
        let mut b = StringBuilder::new();
        for i in 0..s.len() {
            let Some(len) = len else {
                if s.is_null(i) || start.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(duck_substr(s.value(i), start.value(i), None));
                }
                continue;
            };
            if s.is_null(i) || start.is_null(i) || len.is_null(i) {
                b.append_null();
            } else {
                b.append_value(duck_substr(s.value(i), start.value(i), Some(len.value(i))));
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

/// `string_split(s, delim)`: an empty delimiter splits into characters (an empty string stays one
/// empty piece), and a NULL delimiter is the string unchanged, as one element.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StringSplit(Signature, Vec<String>);

impl ScalarUDFImpl for StringSplit {
    fn name(&self) -> &str {
        "string_split"
    }
    fn aliases(&self) -> &[String] {
        &self.1
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [s, d] if s.is_null() || is_text(s) => Ok(vec![coerce_text(s), coerce_text(d)]),
            _ => plan_err!("string_split takes a string and a delimiter"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let s = cast(&arrays[0], &DataType::Utf8)?;
        let d = cast(&arrays[1], &DataType::Utf8)?;
        let s = s.as_string::<i32>();
        let d = d.as_string::<i32>();
        let mut lists = ListBuilder::new(StringBuilder::new());
        for i in 0..s.len() {
            if s.is_null(i) {
                lists.append(false);
                continue;
            }
            let s = s.value(i);
            if d.is_null(i) {
                lists.values().append_value(s);
            } else {
                let delim = d.value(i);
                if delim.is_empty() {
                    if s.is_empty() {
                        lists.values().append_value("");
                    } else {
                        for c in s.chars() {
                            let mut buf = [0u8; 4];
                            lists.values().append_value(c.encode_utf8(&mut buf));
                        }
                    }
                } else {
                    for part in s.split(delim) {
                        lists.values().append_value(part);
                    }
                }
            }
            lists.append(true);
        }
        scalar_out(scalar, Arc::new(lists.finish()))
    }
}

fn is_text(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Each as DuckDB 1.5 printed it (`duck-eval`).
    #[test]
    fn formats_and_differences_as_duckdb() {
        let t = NaiveDateTime::parse_from_str("2024-03-05 07:08:09.123", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap();
        assert_eq!(
            strftime(
                t,
                "%Y-%m-%d %H:%M:%S|%b %d|%a|%j|%y|%I %p|%-d|%A %B|%f|%g|%%"
            )
            .unwrap(),
            "2024-03-05 07:08:09|Mar 05|Tue|065|24|07 AM|5|Tuesday March|123000|123|%"
        );
        assert!(strftime(t, "%e").is_err());
        let us = |s: &str| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .unwrap()
                .and_utc()
                .timestamp_micros()
        };
        let d = |p: &str, a: &str, b: &str| {
            date_diff(
                p,
                naive(us(a)).unwrap(),
                naive(us(b)).unwrap(),
                us(a),
                us(b),
            )
            .unwrap()
        };
        assert_eq!(d("day", "2024-01-01 23:00:00", "2024-01-02 01:00:00"), 1);
        assert_eq!(d("hour", "2024-01-01 23:59:00", "2024-01-02 00:01:00"), 1);
        assert_eq!(d("month", "2024-01-31 00:00:00", "2024-02-01 00:00:00"), 1);
        assert_eq!(d("year", "2023-12-31 00:00:00", "2024-01-01 00:00:00"), 1);
        assert_eq!(d("day", "2024-01-05 00:00:00", "2024-01-01 00:00:00"), -4);
        assert_eq!(d("minute", "2024-01-01 00:00:59", "2024-01-01 00:01:00"), 1);
        assert_eq!(
            d("quarter", "2024-03-31 00:00:00", "2024-04-01 00:00:00"),
            1
        );
        assert_eq!(d("week", "2024-01-06 00:00:00", "2024-01-08 00:00:00"), 0);
        assert_eq!(
            d("second", "2024-01-01 00:00:00.9", "2024-01-01 00:00:01.1"),
            1
        );
        assert_eq!(
            timestamp_text(1_704_067_200 * 1_000_000, true).unwrap(),
            "2024-01-01 00:00:00+00"
        );
        let re = regex::Regex::new("^s").unwrap();
        let caps = re.captures("swap").unwrap();
        assert_eq!(rewrite(&caps, "\\2-\\1", 0), None);
        let re = regex::Regex::new("(b)").unwrap();
        let caps = re.captures("abc").unwrap();
        assert_eq!(
            rewrite(&caps, "\\1\\1[\\0]\\\\", 1).as_deref(),
            Some("bb[b]\\")
        );
        assert_eq!(
            timestamp_text(1_704_067_200 * 1_000_000 + 500_000, true).unwrap(),
            "2024-01-01 00:00:00.5+00"
        );
        assert_eq!(
            timestamp_text(1_704_071_523 * 1_000_000 + 120, false).unwrap(),
            "2024-01-01 01:12:03.00012"
        );
        // Negative start counts from the end; a start before the first character shortens the
        // length; a negative length runs backwards. Measured with duck-eval.
        assert_eq!(duck_substr("abcdef", -1, Some(1)), "f");
        assert_eq!(duck_substr("abcdef", -3, Some(2)), "de");
        assert_eq!(duck_substr("abcdef", -10, Some(4)), "");
        assert_eq!(duck_substr("abcdef", -2, None), "ef");
        assert_eq!(duck_substr("ab", -3, Some(2)), "a");
        assert_eq!(duck_substr("10", -3, Some(2)), "1");
        assert_eq!(duck_substr("010", -3, Some(2)), "01");
        assert_eq!(duck_substr("abc", 0, Some(2)), "a");
        assert_eq!(duck_substr("abc", 0, None), "abc");
        assert_eq!(duck_substr("abcdef", -10, None), "abcdef");
        assert_eq!(duck_substr("abcdef", 5, Some(-3)), "bcd");
        assert_eq!(duck_substr("abcdef", 1, Some(-1)), "");
        assert_eq!(duck_substr("abcdef", 6, Some(-10)), "abcde");
        assert_eq!(duck_substr("abcdef", -2, Some(-2)), "cd");
        assert_eq!(duck_substr("abc", -1, Some(-1)), "b");
        assert_eq!(duck_substr("abc", 0, Some(-1)), "");
        assert_eq!(duck_substr("naïve", -2, Some(2)), "ve");
        assert_eq!(duck_substr("ééé", 0, Some(2)), "é");
        assert_eq!(duck_substr("ab", 5, Some(2)), "");
    }
}
