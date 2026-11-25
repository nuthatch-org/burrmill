//! `corr(y, x)` as DuckDB answers it where DataFusion's does not: over pairs with no spread in
//! one of them, a single pair included, DuckDB's is NaN (`corr(1, 1)`), where DataFusion's, after
//! PostgreSQL, is NULL. With no pair at all both are NULL. DataFusion's accumulators do the
//! arithmetic; this counts the pairs beside them and turns a NULL over at least one into NaN.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Float64Array};
use arrow::datatypes::{DataType, FieldRef, UInt64Type};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
    Signature,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Corr {
    inner: Arc<AggregateUDF>,
}

impl Corr {
    pub fn udaf(inner: Arc<AggregateUDF>) -> Arc<AggregateUDF> {
        Arc::new(AggregateUDF::from(Self { inner }))
    }
}

impl AggregateUDFImpl for Corr {
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
        Ok(Box::new(Single {
            inner: self.inner.accumulator(args)?,
            pairs: 0,
        }))
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(Groups {
            inner: self.inner.create_groups_accumulator(args)?,
            pairs: vec![],
        }))
    }
    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// Row `i` is a pair the aggregate takes: both values present and the filter, if any, passing.
fn pair(values: &[ArrayRef], filter: Option<&BooleanArray>, i: usize) -> bool {
    values.iter().all(|v| v.is_valid(i)) && filter.is_none_or(|f| f.is_valid(i) && f.value(i))
}

/// The pair counts in a merged state, whose first column is DataFusion's count.
fn counts(states: &[ArrayRef]) -> Option<&arrow::array::PrimitiveArray<UInt64Type>> {
    states.first()?.as_primitive_opt::<UInt64Type>()
}

/// NULL over at least one pair is NaN.
fn nan_over_pairs(v: ScalarValue, pairs: u64) -> ScalarValue {
    match v {
        ScalarValue::Float64(None) if pairs > 0 => ScalarValue::Float64(Some(f64::NAN)),
        v => v,
    }
}

#[derive(Debug)]
struct Single {
    inner: Box<dyn Accumulator>,
    pairs: u64,
}

impl Accumulator for Single {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let n = values.first().map_or(0, |v| v.len());
        self.pairs += (0..n).filter(|&i| pair(values, None, i)).count() as u64;
        self.inner.update_batch(values)
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let n = values.first().map_or(0, |v| v.len());
        let gone = (0..n).filter(|&i| pair(values, None, i)).count() as u64;
        self.pairs = self.pairs.saturating_sub(gone);
        self.inner.retract_batch(values)
    }
    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if let Some(c) = counts(states) {
            self.pairs += c.iter().flatten().sum::<u64>();
        }
        self.inner.merge_batch(states)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(nan_over_pairs(self.inner.evaluate()?, self.pairs))
    }
    fn size(&self) -> usize {
        self.inner.size() + std::mem::size_of::<u64>()
    }
}

struct Groups {
    inner: Box<dyn GroupsAccumulator>,
    pairs: Vec<u64>,
}

impl Groups {
    fn grow(&mut self, total: usize) {
        if self.pairs.len() < total {
            self.pairs.resize(total, 0);
        }
    }
}

impl GroupsAccumulator for Groups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        for (i, &g) in group_indices.iter().enumerate() {
            if pair(values, opt_filter, i) {
                self.pairs[g] += 1;
            }
        }
        self.inner
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        if let Some(c) = counts(values) {
            for (i, &g) in group_indices.iter().enumerate() {
                if c.is_valid(i) {
                    self.pairs[g] += c.value(i);
                }
            }
        }
        self.inner
            .merge_batch(values, group_indices, total_num_groups)
    }
    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let pairs = emit_to.take_needed(&mut self.pairs);
        let out = self.inner.evaluate(emit_to)?;
        let Some(v) = out.as_primitive_opt::<arrow::datatypes::Float64Type>() else {
            return Ok(out);
        };
        if !(0..v.len()).any(|i| v.is_null(i) && pairs.get(i).is_some_and(|&p| p > 0)) {
            return Ok(out);
        }
        let fixed: Float64Array = (0..v.len())
            .map(
                |i| match (v.is_valid(i), pairs.get(i).copied().unwrap_or(0)) {
                    (true, _) => Some(v.value(i)),
                    (false, 0) => None,
                    (false, _) => Some(f64::NAN),
                },
            )
            .collect();
        Ok(Arc::new(fixed))
    }
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        emit_to.take_needed(&mut self.pairs);
        self.inner.state(emit_to)
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, opt_filter)
    }
    fn size(&self) -> usize {
        self.inner.size() + self.pairs.capacity() * std::mem::size_of::<u64>()
    }
}
