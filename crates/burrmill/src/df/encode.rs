//! nuthatch's result JSON, from Arrow (roadmap 6.4).
//!
//! nuthatch encodes each cell by way of duckdb-rs's `ValueRef`, which duckdb-rs builds from the
//! Arrow type alone (`row.rs`, `value_ref_internal`), and then `value_to_json` (`analytics.rs`). So
//! the encoding is a function of the Arrow type, and this ports that function. Two nuthatch details
//! ride along: scaled `Decimal128` columns are cast to `VARCHAR` by DuckDB before encoding (#1433),
//! and everything without an arm of its own is the `Debug` of the `ValueRef`.
//!
//! Types duckdb-rs has no arm for (it panics) and nested types (whose `Debug` prints the whole
//! Arrow column, not the value) are refused rather than imitated.

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, IntervalUnit, Time64MicrosecondType, TimeUnit, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use serde_json::{Map, Value};

use crate::error::{BurrmillError, Result};

/// One JSON object per row, keys inserted in column order as nuthatch does.
pub fn rows(batch: &RecordBatch) -> Result<Vec<Value>> {
    let schema = batch.schema();
    for (f, c) in schema.fields().iter().zip(batch.columns()) {
        if !encodable(c.data_type()) {
            return Err(BurrmillError::NotAllowed(format!(
                "result column `{}` is {}, which has no nuthatch JSON encoding",
                f.name(),
                c.data_type()
            )));
        }
    }
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let mut obj = Map::new();
        for (f, c) in schema.fields().iter().zip(batch.columns()) {
            obj.insert(f.name().clone(), value(c, i));
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

fn encodable(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        Null | Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float32
            | Float64
            | Decimal128(..)
            | Decimal256(..)
            | Utf8
            | LargeUtf8
            | Utf8View
            | Binary
            | LargeBinary
            | FixedSizeBinary(_)
            | BinaryView
            | Timestamp(..)
            | Date32
            | Time64(TimeUnit::Microsecond)
            | Interval(IntervalUnit::MonthDayNano)
    )
}

/// `digits` as a decimal of `scale` places, as DuckDB casts a DECIMAL to VARCHAR. With no integer
/// digits in the type (precision equal to scale), DuckDB writes no integer part: `.5`, `-.05`.
fn scaled(digits: String, precision: u8, scale: i8) -> String {
    if scale < 0 {
        return if digits == "0" {
            digits
        } else {
            digits + &"0".repeat(scale.unsigned_abs() as usize)
        };
    }
    let (neg, mag) = match digits.strip_prefix('-') {
        Some(m) => (true, m),
        None => (false, digits.as_str()),
    };
    let scale = scale as usize;
    let mag = format!("{mag:0>width$}", width = scale + 1);
    let (int, frac) = mag.split_at(mag.len() - scale);
    let int = if precision as usize == scale { "" } else { int };
    format!("{}{int}.{frac}", if neg { "-" } else { "" })
}

fn value(c: &ArrayRef, i: usize) -> Value {
    if c.is_null(i) {
        return Value::Null;
    }
    match c.data_type() {
        DataType::Boolean => Value::Bool(c.as_boolean().value(i)),
        DataType::Int8 => Value::from(c.as_primitive::<Int8Type>().value(i)),
        DataType::Int16 => Value::from(c.as_primitive::<Int16Type>().value(i)),
        DataType::Int32 => Value::from(c.as_primitive::<Int32Type>().value(i)),
        DataType::Int64 => Value::from(c.as_primitive::<Int64Type>().value(i)),
        DataType::UInt8 => Value::from(c.as_primitive::<UInt8Type>().value(i)),
        DataType::UInt16 => Value::from(c.as_primitive::<UInt16Type>().value(i)),
        DataType::UInt32 => Value::from(c.as_primitive::<UInt32Type>().value(i)),
        DataType::UInt64 => Value::from(c.as_primitive::<UInt64Type>().value(i)),
        DataType::Float32 => Value::from(c.as_primitive::<Float32Type>().value(i)),
        DataType::Float64 => Value::from(c.as_primitive::<Float64Type>().value(i)),
        DataType::Decimal128(p, s) => {
            let v = c.as_primitive::<Decimal128Type>().value(i).to_string();
            Value::String(if *s == 0 { v } else { scaled(v, *p, *s) })
        }
        DataType::Decimal256(p, s) => {
            let v = c.as_primitive::<Decimal256Type>().value(i).to_string();
            Value::String(if *s == 0 { v } else { scaled(v, *p, *s) })
        }
        DataType::Utf8 => Value::String(c.as_string::<i32>().value(i).to_owned()),
        DataType::LargeUtf8 => Value::String(c.as_string::<i64>().value(i).to_owned()),
        DataType::Utf8View => Value::String(c.as_string_view().value(i).to_owned()),
        DataType::Binary => blob(c.as_binary::<i32>().value(i)),
        DataType::LargeBinary => blob(c.as_binary::<i64>().value(i)),
        DataType::BinaryView => blob(c.as_binary_view().value(i)),
        DataType::FixedSizeBinary(_) => blob(c.as_fixed_size_binary().value(i)),
        DataType::Timestamp(unit, _) => {
            let v = match unit {
                TimeUnit::Second => c
                    .as_primitive::<arrow::datatypes::TimestampSecondType>()
                    .value(i),
                TimeUnit::Millisecond => c
                    .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                    .value(i),
                TimeUnit::Microsecond => c
                    .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                    .value(i),
                TimeUnit::Nanosecond => c
                    .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
                    .value(i),
            };
            Value::String(format!("Timestamp({unit:?}, {v})"))
        }
        DataType::Date32 => Value::String(format!(
            "Date32({})",
            c.as_primitive::<Date32Type>().value(i)
        )),
        DataType::Time64(TimeUnit::Microsecond) => Value::String(format!(
            "Time64(Microsecond, {})",
            c.as_primitive::<Time64MicrosecondType>().value(i)
        )),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let v = c
                .as_primitive::<arrow::datatypes::IntervalMonthDayNanoType>()
                .value(i);
            Value::String(format!(
                "Interval {{ months: {}, days: {}, nanos: {} }}",
                v.months, v.days, v.nanoseconds
            ))
        }
        DataType::Null => Value::Null,
        t => unreachable!("{t} is refused before encoding"),
    }
}

fn blob(b: &[u8]) -> Value {
    Value::String(format!("Blob({b:?})"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};

    use super::{rows, scaled};

    #[test]
    fn a_row_keeps_its_columns_in_select_order() {
        let batch = RecordBatch::try_from_iter([
            ("z", Arc::new(Int64Array::from(vec![1])) as _),
            ("a", Arc::new(Int64Array::from(vec![2])) as _),
        ])
        .unwrap();
        assert_eq!(rows(&batch).unwrap()[0].to_string(), r#"{"z":1,"a":2}"#);
    }

    #[test]
    fn scaled_decimals_keep_their_places() {
        assert_eq!(scaled("15000".into(), 10, 4), "1.5000");
        assert_eq!(scaled("-5".into(), 3, 2), "-0.05");
        assert_eq!(scaled("0".into(), 5, 3), "0.000");
        assert_eq!(scaled("-123456".into(), 18, 2), "-1234.56");
        assert_eq!(scaled("5".into(), 1, 1), ".5");
        assert_eq!(scaled("-5".into(), 2, 2), "-.05");
    }
}
