//! DuckDB's casts between text and BLOB (#419). Text becomes bytes with `\xAA` read as the byte it
//! names, as DuckDB's `Blob::ToBlob` reads it; a byte past ASCII must be written that way. A BLOB
//! becomes text with every byte outside printable ASCII, and `\`, `'` and `"`, written as `\xAA`,
//! as DuckDB's `Blob::ToString`. Arrow's casts take text's UTF-8 bytes as they are and refuse a
//! BLOB that is not UTF-8. A number has no cast to BLOB in DuckDB; Arrow wrote its digits.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, BinaryBuilder, StringBuilder};
use arrow::datatypes::DataType;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TryCast, Volatility,
};

fn hex(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|d| d as u8)
}

/// DuckDB's text to BLOB, or its error.
pub fn parse(s: &str) -> std::result::Result<Vec<u8>, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' {
            if i + 3 >= b.len() {
                return Err(format!(
                    "Conversion Error: Invalid hex escape code encountered in string -> blob \
                     conversion of string \"{s}\": unterminated escape code at end of blob"
                ));
            }
            match (b[i + 1], hex(b[i + 2]), hex(b[i + 3])) {
                (b'x', Some(h), Some(l)) => out.push(h << 4 | l),
                _ => {
                    return Err(format!(
                        "Conversion Error: Invalid hex escape code encountered in string -> blob \
                         conversion of string \"{s}\": {}",
                        String::from_utf8_lossy(&b[i..i + 4])
                    ));
                }
            }
            i += 4;
        } else if b[i] <= 127 {
            out.push(b[i]);
            i += 1;
        } else {
            return Err(format!(
                "Conversion Error: Invalid byte encountered in STRING -> BLOB conversion of string \
                 \"{s}\". All non-ascii characters must be escaped with hex codes (e.g. \\xAA)"
            ));
        }
    }
    Ok(out)
}

/// DuckDB's BLOB to text.
pub fn text(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut s = String::with_capacity(b.len());
    for &c in b {
        if (32..=126).contains(&c) && !matches!(c, b'\\' | b'\'' | b'"') {
            s.push(c as char);
        } else {
            s.push_str("\\x");
            s.push(HEX[usize::from(c >> 4)] as char);
            s.push(HEX[usize::from(c & 0xF)] as char);
        }
    }
    s
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BlobFn {
    /// Text to BLOB when set, BLOB to text when not.
    to_blob: bool,
    /// A failed parse is NULL rather than an error.
    try_: bool,
    sig: Signature,
}

impl BlobFn {
    fn call(to_blob: bool, try_: bool, arg: Expr) -> Expr {
        let f = Arc::new(ScalarUDF::from(Self {
            to_blob,
            try_,
            sig: Signature::any(1, Volatility::Immutable),
        }));
        Expr::ScalarFunction(ScalarFunction::new_udf(f, vec![arg]))
    }
}

impl ScalarUDFImpl for BlobFn {
    fn name(&self) -> &str {
        match (self.to_blob, self.try_) {
            (true, false) => "burrmill_blob",
            (true, true) => "burrmill_try_blob",
            (false, _) => "burrmill_blob_text",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(if self.to_blob {
            DataType::Binary
        } else {
            DataType::Utf8
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args.as_slice(), [ColumnarValue::Scalar(_)]);
        let mut a = ColumnarValue::values_to_arrays(&args.args)?;
        if a.len() != 1 {
            return exec_err!("{} takes one argument", self.name());
        }
        let a = a.remove(0);
        let out: ArrayRef = if self.to_blob {
            let a = arrow::compute::cast(&a, &DataType::Utf8)?;
            let mut b = BinaryBuilder::new();
            for v in a.as_string::<i32>().iter() {
                match v.map(parse) {
                    None => b.append_null(),
                    Some(Ok(v)) => b.append_value(v),
                    Some(Err(_)) if self.try_ => b.append_null(),
                    Some(Err(e)) => return Err(DataFusionError::Execution(e)),
                }
            }
            Arc::new(b.finish())
        } else {
            let a = arrow::compute::cast(&a, &DataType::Binary)?;
            let mut b = StringBuilder::new();
            for v in a.as_binary::<i32>().iter() {
                match v {
                    None => b.append_null(),
                    Some(v) => b.append_value(text(v)),
                }
            }
            Arc::new(b.finish())
        };
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
        } else {
            ColumnarValue::Array(out)
        })
    }
}

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

/// The element type under any lists.
fn leaf(t: &DataType) -> &DataType {
    match t {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            leaf(f.data_type())
        }
        t => t,
    }
}

fn is_blob(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

/// A cast between text and BLOB as DuckDB's, and a number's cast to BLOB refused as DuckDB refuses
/// it. Once types are coerced.
pub fn blobs_as_duckdb(e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let (inner, to, try_) = match &e {
        Expr::Cast(Cast { expr, field }) => (expr, field.data_type(), false),
        Expr::TryCast(TryCast { expr, field }) => (expr, field.data_type(), true),
        _ => return Ok(Transformed::no(e)),
    };
    let from = inner.get_type(schema)?;
    let retype = |x: Expr, made: DataType| {
        if *to == made {
            x
        } else {
            Expr::Cast(Cast::new(Box::new(x), to.clone()))
        }
    };
    if is_blob(to) && is_text(&from) {
        let x = BlobFn::call(true, try_, inner.as_ref().clone());
        return Ok(Transformed::yes(retype(x, DataType::Binary)));
    }
    if is_text(to) && is_blob(&from) {
        let x = BlobFn::call(false, false, inner.as_ref().clone());
        return Ok(Transformed::yes(retype(x, DataType::Utf8)));
    }
    let numeric = |t: &DataType| t.is_integer() || t.is_floating() || t.is_decimal();
    if is_blob(leaf(to)) && numeric(leaf(&from)) && !is_blob(to) {
        return plan_err!(
            "Conversion Error: Unimplemented type for cast ({} -> BLOB)",
            super::errors::duck_type(&leaf(&from).to_string())
        );
    }
    if is_blob(to) && numeric(&from) {
        if try_ {
            return Ok(Transformed::yes(Expr::Literal(
                ScalarValue::try_from(to)?,
                None,
            )));
        }
        return plan_err!(
            "Conversion Error: Unimplemented type for cast ({} -> BLOB)",
            super::errors::duck_type(&from.to_string())
        );
    }
    Ok(Transformed::no(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_and_writes_as_duckdb() {
        assert_eq!(parse(r"\xAA\xFF\xAA").unwrap(), vec![0xAA, 0xFF, 0xAA]);
        assert_eq!(parse(r"\x20\x00\xFF").unwrap(), vec![0x20, 0, 0xFF]);
        assert_eq!(
            parse(r"\xaa1199").unwrap(),
            vec![0xAA, b'1', b'1', b'9', b'9']
        );
        assert!(parse(r"\xA").is_err());
        assert!(parse(r"\yAA").is_err());
        assert!(parse("é").is_err());
        assert_eq!(text(&[0xAA, b'Z', b'\\', 0]), r"\xAAZ\x5C\x00");
    }
}
