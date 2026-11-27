//! `regr_slope(y, x)` and `regr_r2(y, x)` as DuckDB answers them where DataFusion's, after
//! PostgreSQL, does not (#421, test/sql/aggregate/aggregates/test_regression.test:69, 93, 98, 108).
//! Over pairs with no spread in `x`, DuckDB's slope is NaN (`regr_slope(1, 1)`), and its R² is 1
//! over pairs with no spread in `y` where `x` has some; DataFusion's are NULL. DuckDB's R² is the
//! square of `corr`, `cov / (σx·σy)`, which is 0 where `σy` overflows to infinity, where
//! DataFusion's `cov² / (σx²·σy²)` is NaN. DataFusion's accumulator keeps the moments DuckDB's
//! does (the count, the means, the sums of squared deviations and the co-moment); only the answer
//! is DuckDB's, read from them as DuckDB's `Finalize` reads its own.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Slope,
    R2,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DuckRegr {
    inner: Arc<AggregateUDF>,
    kind: Kind,
}

impl DuckRegr {
    /// DataFusion's `regr_slope` or `regr_r2`, answering as DuckDB; any other aggregate as it is.
    pub fn udaf(inner: Arc<AggregateUDF>) -> Arc<AggregateUDF> {
        let kind = match inner.name() {
            "regr_slope" => Kind::Slope,
            "regr_r2" => Kind::R2,
            _ => return inner,
        };
        Arc::new(AggregateUDF::from(Self { inner, kind }))
    }
}

impl AggregateUDFImpl for DuckRegr {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }
    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(Regr {
            inner: self.inner.accumulator(args)?,
            kind: self.kind,
        }))
    }
    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

#[derive(Debug)]
struct Regr {
    inner: Box<dyn Accumulator>,
    kind: Kind,
}

/// DuckDB's answer from DataFusion's state: `[count, mean_x, mean_y, m2_x, m2_y, co_moment]`.
fn answer(kind: Kind, state: &[ScalarValue]) -> Option<ScalarValue> {
    let [
        ScalarValue::UInt64(Some(n)),
        _,
        _,
        ScalarValue::Float64(Some(m2_x)),
        ScalarValue::Float64(Some(m2_y)),
        ScalarValue::Float64(Some(co)),
    ] = state
    else {
        return None;
    };
    let n = *n;
    let float = |v: f64| Some(ScalarValue::Float64(Some(v)));
    if n == 0 {
        return Some(ScalarValue::Float64(None));
    }
    let count = n as f64;
    // DuckDB's population variance, 0 below two values.
    let var = |m2: f64| if n > 1 { m2 / count } else { 0.0 };
    let (var_x, var_y) = (var(*m2_x), var(*m2_y));
    let cov = co / count;
    match kind {
        Kind::Slope => float(if var_x != 0.0 { cov / var_x } else { f64::NAN }),
        Kind::R2 => {
            if var_x == 0.0 {
                return Some(ScalarValue::Float64(None));
            }
            if var_y == 0.0 {
                return float(1.0);
            }
            let sd = var_x.sqrt() * var_y.sqrt();
            let corr = if sd != 0.0 { cov / sd } else { f64::NAN };
            float(corr.powi(2))
        }
    }
}

impl Accumulator for Regr {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(values)
    }
    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let state = self.inner.state()?;
        match answer(self.kind, &state) {
            Some(v) => Ok(v),
            None => self.inner.evaluate(),
        }
    }
    fn size(&self) -> usize {
        self.inner.size()
    }
}
