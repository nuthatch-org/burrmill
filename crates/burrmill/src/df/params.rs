//! `$1`, `$2` and on in a statement, bound to values the host passes beside it, so a host need not
//! build SQL from what its caller sent.
//!
//! The statement is refused, parsed, rewritten and planned exactly as one without placeholders;
//! only then does each placeholder become a literal. A value is never parsed as SQL.

use datafusion_common::{ParamValues, ScalarValue};
use datafusion_expr::LogicalPlan;

use super::{Engine, plan_err};
use crate::error::Result;

/// A value for a `$n` placeholder, `$1` being the first.
#[derive(Debug, Clone, PartialEq)]
pub enum Param {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Text(String),
}

impl From<bool> for Param {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<i64> for Param {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<u64> for Param {
    fn from(v: u64) -> Self {
        Self::UInt(v)
    }
}

impl From<f64> for Param {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<&str> for Param {
    fn from(v: &str) -> Self {
        Self::Text(v.to_string())
    }
}

impl From<String> for Param {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}

impl<T: Into<Param>> From<Option<T>> for Param {
    fn from(v: Option<T>) -> Self {
        v.map_or(Self::Null, Into::into)
    }
}

impl Param {
    fn scalar(&self) -> ScalarValue {
        match self {
            // Untyped, as a bare `NULL` is: the plan's coercion gives it the type its place wants.
            Self::Null => ScalarValue::Null,
            Self::Bool(v) => ScalarValue::Boolean(Some(*v)),
            Self::Int(v) => ScalarValue::Int64(Some(*v)),
            Self::UInt(v) => ScalarValue::UInt64(Some(*v)),
            Self::Float(v) => ScalarValue::Float64(Some(*v)),
            // `Utf8`, the type a quoted literal in the statement has.
            Self::Text(v) => ScalarValue::Utf8(Some(v.clone())),
        }
    }
}

/// `logical` with each placeholder replaced by its value. With no values the plan is returned
/// as it is, so a statement without placeholders plans exactly as before.
pub(super) fn bind(logical: LogicalPlan, params: &[Param]) -> Result<LogicalPlan> {
    if params.is_empty() {
        return Ok(logical);
    }
    let values = ParamValues::from(params.iter().map(Param::scalar).collect::<Vec<_>>());
    logical.with_param_values(values).map_err(plan_err)
}

impl Engine {
    /// As [`Engine::sql`], with `params` bound to `$1`, `$2` and on.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn sql_with_params(
        &self,
        sql: &str,
        params: &[Param],
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let mut out = Vec::new();
        self.sql_for_each_with_params(sql, params, None, |b| {
            out.push(b);
            Ok(())
        })?;
        Ok(out)
    }
}
