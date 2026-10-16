//! `max` and `min` over a frame from the partition's start to the current row, as one pass over the
//! partition.
//!
//! DataFusion's streaming window evaluates such a frame row by row and builds a scalar for every
//! row: a running max over 224k rows cost 56 ms where DuckDB took 33, and the allocations ledger
//! carries the newest key forward this way three times per statement (#99).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray};
use arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Int32Type, Int64Type, UInt32Type, UInt64Type,
};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, Result, exec_err};
use datafusion_expr::expr::{WindowFunction, WindowFunctionDefinition};
use datafusion_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion_expr::{
    Expr, ExprSchemable, PartitionEvaluator, Signature, Volatility, WindowFrameBound,
    WindowFrameUnits, WindowUDF, WindowUDFImpl,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Running {
    max: bool,
    sig: Signature,
}

impl Running {
    fn udwf(max: bool) -> Arc<WindowUDF> {
        Arc::new(WindowUDF::from(Self {
            max,
            sig: Signature::any(1, Volatility::Immutable),
        }))
    }
}

impl WindowUDFImpl for Running {
    fn name(&self) -> &str {
        match self.max {
            true => "burrmill_running_max",
            false => "burrmill_running_min",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn partition_evaluator(
        &self,
        _: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(Scan { max: self.max }))
    }
    fn field(&self, args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let t = args.input_fields()[0].data_type().clone();
        Ok(Arc::new(Field::new(args.name(), t, true)))
    }
}

/// Not bounded, so the planner hands it the whole sorted partition once.
#[derive(Debug)]
struct Scan {
    max: bool,
}

impl PartitionEvaluator for Scan {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        let v = &values[0];
        Ok(match v.data_type() {
            DataType::Int32 => scan::<Int32Type>(v, self.max),
            DataType::Int64 => scan::<Int64Type>(v, self.max),
            DataType::UInt32 => scan::<UInt32Type>(v, self.max),
            DataType::UInt64 => scan::<UInt64Type>(v, self.max),
            DataType::Decimal128(..) => scan::<Decimal128Type>(v, self.max),
            t => return exec_err!("running max/min has no fast path for {t}"),
        })
    }
}

/// The extreme so far at each row, NULL until the first non-null value, as `max`/`min` give.
fn scan<T: ArrowPrimitiveType>(v: &ArrayRef, max: bool) -> ArrayRef
where
    T::Native: PartialOrd,
{
    let v = v.as_primitive::<T>();
    let mut held: Option<T::Native> = None;
    let out: PrimitiveArray<T> = v
        .iter()
        .map(|x| {
            if let Some(x) = x {
                held = match held {
                    Some(h) if (max && h >= x) || (!max && h <= x) => Some(h),
                    _ => Some(x),
                };
            }
            held
        })
        .collect();
    Arc::new(out.with_data_type(v.data_type().clone()))
}

/// `max(x)` or `min(x)` over `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, of a type the scan
/// takes, as the one-pass function. Anything else is left as it is.
pub fn rewrite(mut wf: WindowFunction, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let f = &wf.params.window_frame;
    let max = match &wf.fun {
        WindowFunctionDefinition::AggregateUDF(u) if u.name() == "max" => true,
        WindowFunctionDefinition::AggregateUDF(u) if u.name() == "min" => false,
        _ => return Ok(Transformed::no(Expr::WindowFunction(Box::new(wf)))),
    };
    let fits = wf.params.args.len() == 1
        && !wf.params.distinct
        && wf.params.filter.is_none()
        && wf.params.null_treatment.is_none()
        && f.units == WindowFrameUnits::Rows
        && matches!(&f.start_bound, WindowFrameBound::Preceding(v) if v.is_null())
        && matches!(f.end_bound, WindowFrameBound::CurrentRow)
        && matches!(
            wf.params.args[0].get_type(schema)?,
            DataType::Int32
                | DataType::Int64
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Decimal128(..)
        );
    if !fits {
        return Ok(Transformed::no(Expr::WindowFunction(Box::new(wf))));
    }
    wf.fun = WindowFunctionDefinition::WindowUDF(Running::udwf(max));
    Ok(Transformed::yes(Expr::WindowFunction(Box::new(wf))))
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;

    use super::*;

    #[test]
    fn the_extreme_so_far_skips_nulls_and_starts_null() {
        let v: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(3),
            None,
            Some(1),
            Some(5),
        ]));
        let max = Scan { max: true }
            .evaluate_all(&[Arc::clone(&v)], 5)
            .unwrap();
        let min = Scan { max: false }.evaluate_all(&[v], 5).unwrap();
        assert_eq!(
            max.as_primitive::<Int64Type>().iter().collect::<Vec<_>>(),
            vec![None, Some(3), Some(3), Some(3), Some(5)]
        );
        assert_eq!(
            min.as_primitive::<Int64Type>().iter().collect::<Vec<_>>(),
            vec![None, Some(3), Some(3), Some(1), Some(1)]
        );
    }
}
