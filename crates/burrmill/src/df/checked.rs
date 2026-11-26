//! Overflow-refusing SUM / AVG aggregates and `+ - *` / unary `-` scalars (roadmap 6.3).
//!
//! The aggregates accumulate in a [`Wide`] integer sized to the input, so a partial sum cannot
//! overflow and refusal is order-independent: `MAX, +1, -1` answers and `MAX, +1` refuses. Only the
//! final narrowing to the output type can fail, and that is an error naming the exact value, never
//! NULL and never a wrapped number.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Decimal128Builder, Decimal256Builder,
    FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringBuilder, UInt64Array,
};
use arrow::compute::kernels::numeric;
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{
    ArrowNativeTypeOp, ArrowPrimitiveType, DataType, Decimal128Type, Decimal256Type, DecimalType,
    Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type, UInt64Type, i256,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::binary::BinaryTypeCoercer;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, EmitTo, GroupsAccumulator,
    Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::wide::Wide;

/// `exact_wide`'s output: a 320-bit value, little-endian.
const WIDE_BYTES: i32 = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    Sum,
    /// The exact sum as canonical decimal text, which no uint256 total can overflow.
    SumText,
    /// The exact sum over the count, truncated to the output scale.
    Avg,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedAgg {
    sig: Signature,
    mode: Mode,
    /// A fixed output type: the built-in's own for `avg`, the `TRY_CAST` target's sum type for
    /// text that stood behind one. `None` derives it from the input.
    out: Option<DataType>,
}

impl CheckedAgg {
    pub fn udaf(mode: Mode, out: Option<DataType>) -> Arc<AggregateUDF> {
        Arc::new(AggregateUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            mode,
            out,
        }))
    }
}

pub fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

pub fn is_exact(t: &DataType) -> bool {
    t.is_integer()
        || matches!(
            t,
            DataType::Decimal32(..)
                | DataType::Decimal64(..)
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
        )
}

/// DuckDB's widening: any integer sums to HUGEINT, which here is `Decimal128(38, 0)`.
pub fn sum_type(input: &DataType) -> Option<DataType> {
    use DataType::*;
    Some(match input {
        t if t.is_integer() => Decimal128(38, 0),
        Decimal32(_, s) | Decimal64(_, s) | Decimal128(_, s) => Decimal128(38, *s),
        Decimal256(_, s) => Decimal256(76, *s),
        _ => return None,
    })
}

fn scale_of(t: &DataType) -> i8 {
    match t {
        DataType::Decimal32(_, s)
        | DataType::Decimal64(_, s)
        | DataType::Decimal128(_, s)
        | DataType::Decimal256(_, s) => *s,
        _ => 0,
    }
}

impl AggregateUDFImpl for CheckedAgg {
    fn name(&self) -> &str {
        match self.mode {
            Mode::Sum => "checked_sum",
            Mode::SumText => "checked_sum_text",
            Mode::Avg => "checked_avg",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        use DataType::*;
        let [t] = arg_types else {
            return plan_err!("{} takes one argument", self.name());
        };
        Ok(vec![match t {
            Int8 | Int16 | Int32 | Int64 => Int64,
            UInt8 | UInt16 | UInt32 | UInt64 => UInt64,
            Decimal32(p, s) | Decimal64(p, s) => Decimal128(*p, *s),
            Decimal128(..) | Decimal256(..) | Utf8 | LargeUtf8 | Utf8View => t.clone(),
            FixedSizeBinary(WIDE_BYTES) => t.clone(),
            t => return plan_err!("{} has no checked form for {t}", self.name()),
        }])
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(t) = &self.out {
            return Ok(t.clone());
        }
        match (self.mode, &arg_types[0]) {
            (Mode::SumText, _) => Ok(DataType::Utf8),
            (Mode::Sum, t) if is_text(t) => Ok(DataType::Decimal256(76, 0)),
            (Mode::Sum, t) => match sum_type(t) {
                Some(o) => Ok(o),
                None => plan_err!("checked_sum has no checked form for {t}"),
            },
            (Mode::Avg, t) => plan_err!("checked_avg needs its output type fixed, input {t}"),
        }
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let width = width_for(args.input_fields[0].data_type());
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[sum]", args.name),
                DataType::FixedSizeBinary((width * 8) as i32),
                true,
            )),
            Arc::new(Field::new(
                format!("{}[count]", args.name),
                DataType::UInt64,
                true,
            )),
        ])
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let spec = self.spec(&args)?;
        Ok(match width_for(&spec.in_ty) {
            2 => Box::new(Single(Groups::<2>::new(spec))),
            3 => Box::new(Single(Groups::<3>::new(spec))),
            _ => Box::new(Single(Groups::<5>::new(spec))),
        })
    }
    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let spec = self.spec(&args)?;
        Ok(match width_for(&spec.in_ty) {
            2 => Box::new(Groups::<2>::new(spec)),
            3 => Box::new(Groups::<3>::new(spec)),
            _ => Box::new(Groups::<5>::new(spec)),
        })
    }
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.accumulator(args)
    }
}

/// Limbs enough that 2^63 inputs of this type cannot leave the accumulator.
fn width_for(t: &DataType) -> usize {
    match t {
        t if t.is_integer() => 2,
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) => 3,
        _ => 5,
    }
}

#[derive(Clone)]
struct Spec {
    mode: Mode,
    name: &'static str,
    in_ty: DataType,
    out_ty: DataType,
    /// Text as DuckDB's `TRY_CAST` reads it, where that is still exactly an integer. Otherwise
    /// canonical only.
    lenient: bool,
    /// With `lenient`, `0x`/`0b` text too, as DuckDB's casts to 64-bit integers read it.
    hex: bool,
}

impl CheckedAgg {
    fn spec(&self, args: &AccumulatorArgs) -> Result<Spec> {
        let name = match self.mode {
            Mode::Sum => "checked_sum",
            Mode::SumText => "checked_sum_text",
            Mode::Avg => "checked_avg",
        };
        if args.is_distinct {
            return plan_err!("{name}(DISTINCT ...) has no checked form");
        }
        Ok(Spec {
            mode: self.mode,
            name,
            in_ty: args.exprs[0].data_type(args.schema)?,
            out_ty: args.return_field.data_type().clone(),
            lenient: false,
            hex: false,
        })
    }
}

struct Groups<const N: usize> {
    spec: Spec,
    sums: Vec<Wide<N>>,
    /// Doubles as the null tracker: a group that saw no value answers NULL.
    counts: Vec<u64>,
}

fn exec(msg: String) -> DataFusionError {
    DataFusionError::Execution(msg)
}

impl<const N: usize> Groups<N> {
    fn new(spec: Spec) -> Self {
        Self {
            spec,
            sums: vec![],
            counts: vec![],
        }
    }

    fn grow(&mut self, n: usize) {
        if self.sums.len() < n {
            self.sums.resize(n, Wide::ZERO);
            self.counts.resize(n, 0);
        }
    }

    /// Calls `f(row, value)` for every non-null, unfiltered input row.
    fn each(
        &self,
        a: &ArrayRef,
        filter: Option<&BooleanArray>,
        mut f: impl FnMut(usize, Wide<N>) -> Result<()>,
    ) -> Result<()> {
        let keep = |i: usize| a.is_valid(i) && filter.is_none_or(|f| f.is_valid(i) && f.value(i));
        let name = self.spec.name;
        let lenient = self.spec.lenient;
        // Lenient is `TRY_CAST`'s reading, where text that is not a number is NULL.
        let hex = self.spec.hex;
        let parse = |t: &str| -> std::result::Result<Option<Wide<N>>, String> {
            if !lenient {
                return Wide::parse_canonical(t).map(Some);
            }
            match super::fastcast::prefixed(t) {
                Some(Some(v)) if hex => Wide::parse_canonical(&v.to_string()).map(Some),
                Some(_) if hex => Ok(None),
                _ => Wide::parse_integer(t),
            }
        };
        match a.data_type() {
            DataType::Int64 => {
                let a = a.as_primitive::<Int64Type>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(i, Wide::from_i128(a.value(i) as i128))?;
                }
            }
            DataType::UInt64 => {
                let a = a.as_primitive::<UInt64Type>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(i, Wide::from_i128(a.value(i) as i128))?;
                }
            }
            DataType::Decimal128(..) => {
                let a = a.as_primitive::<Decimal128Type>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(i, Wide::from_i128(a.value(i)))?;
                }
            }
            DataType::Decimal256(..) => {
                let a = a.as_primitive::<Decimal256Type>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(i, Wide::from_i256(a.value(i)))?;
                }
            }
            DataType::Utf8View => {
                let a = a.as_string_view();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    if let Some(v) = parse(a.value(i)).map_err(|e| exec(format!("{name}: {e}")))? {
                        f(i, v)?;
                    }
                }
            }
            DataType::Utf8 => {
                let a = a.as_string::<i32>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    if let Some(v) = parse(a.value(i)).map_err(|e| exec(format!("{name}: {e}")))? {
                        f(i, v)?;
                    }
                }
            }
            DataType::FixedSizeBinary(WIDE_BYTES) if N == 5 => {
                let a = a.as_fixed_size_binary();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(i, Wide::from_le(a.value(i)))?;
                }
            }
            DataType::LargeUtf8 => {
                let a = a.as_string::<i64>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    if let Some(v) = parse(a.value(i)).map_err(|e| exec(format!("{name}: {e}")))? {
                        f(i, v)?;
                    }
                }
            }
            t => return exec_err!("{name}: unsupported input {t}"),
        }
        Ok(())
    }

    fn fold(&mut self, g: usize, v: Wide<N>, count: u64) -> Result<()> {
        match self.sums[g].checked_add(v) {
            Some(s) => {
                self.sums[g] = s;
                self.counts[g] += count;
                Ok(())
            }
            None => exec_err!("{}: running total left {} bits", self.spec.name, N * 64),
        }
    }

    /// `update_batch` over integers and `Decimal128`, read from their value buffers rather than
    /// row by row through `dyn Array`: Q1's seven checked sums and averages took 2.49 s of CPU over
    /// 59.14M rows into 4 groups at SF10 that way (#433). `None` for any other input.
    fn update_narrow(
        &mut self,
        a: &ArrayRef,
        group_indices: &[usize],
        filter: Option<&BooleanArray>,
    ) -> Option<Result<()>> {
        // The rows to add: valid, and kept by the filter where there is one.
        let keep = match filter {
            None => a.nulls().map(|n| n.inner().clone()),
            Some(f) => {
                let mut k = f.values().clone();
                if let Some(n) = f.nulls() {
                    k = &k & n.inner();
                }
                if let Some(n) = a.nulls() {
                    k = &k & n.inner();
                }
                Some(k)
            }
        };
        Some(match a.data_type() {
            DataType::Int64 => {
                self.fold_narrow(a.as_primitive::<Int64Type>().values(), group_indices, keep)
            }
            DataType::UInt64 => {
                self.fold_narrow(a.as_primitive::<UInt64Type>().values(), group_indices, keep)
            }
            DataType::Decimal128(..) => self.fold_narrow(
                a.as_primitive::<Decimal128Type>().values(),
                group_indices,
                keep,
            ),
            _ => return None,
        })
    }

    fn fold_narrow<T: Copy + Into<i128>>(
        &mut self,
        values: &[T],
        group_indices: &[usize],
        keep: Option<arrow::buffer::BooleanBuffer>,
    ) -> Result<()> {
        let (sums, counts) = (&mut self.sums, &mut self.counts);
        let mut add = |g: usize, v: T| match sums[g].checked_add(Wide::from_i128(v.into())) {
            Some(s) => {
                sums[g] = s;
                counts[g] += 1;
                true
            }
            None => false,
        };
        let added = match keep {
            None => group_indices.iter().zip(values).all(|(&g, &v)| add(g, v)),
            Some(k) if k.count_set_bits() == k.len() => {
                group_indices.iter().zip(values).all(|(&g, &v)| add(g, v))
            }
            Some(k) => k.set_indices().all(|i| add(group_indices[i], values[i])),
        };
        match added {
            true => Ok(()),
            false => exec_err!("{}: running total left {} bits", self.spec.name, N * 64),
        }
    }

    fn finish_one(&self, sum: Wide<N>, count: u64) -> Result<Wide<N>> {
        match self.spec.mode {
            Mode::Sum | Mode::SumText => Ok(sum),
            Mode::Avg => {
                let up = scale_of(&self.spec.out_ty) - scale_of(&self.spec.in_ty);
                let scaled = sum
                    .checked_mul_small(10u64.pow(up.max(0) as u32))
                    .ok_or_else(|| {
                        exec(format!(
                            "{}: scaled sum left {} bits",
                            self.spec.name,
                            N * 64
                        ))
                    })?;
                Ok(scaled.div_small(count))
            }
        }
    }

    fn build(&self, sums: &[Wide<N>], counts: &[u64]) -> Result<ArrayRef> {
        let name = self.spec.name;
        let out_ty = &self.spec.out_ty;
        let overflow = |v: Wide<N>| -> Result<ArrayRef> {
            Err(super::errors::overflow(format!(
                "{name} overflow: exact result {v} does not fit {out_ty}"
            )))
        };
        let n = sums.len();
        Ok(match out_ty {
            DataType::Utf8 => {
                let mut b = StringBuilder::new();
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                    } else {
                        b.append_value(self.finish_one(sums[i], counts[i])?.to_string());
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Decimal128(p, s) => {
                let mut b = Decimal128Builder::with_capacity(n);
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                        continue;
                    }
                    let v = self.finish_one(sums[i], counts[i])?;
                    match v.to_i128() {
                        Some(x) if Decimal128Type::is_valid_decimal_precision(x, *p) => {
                            b.append_value(x)
                        }
                        _ => return overflow(v),
                    }
                }
                Arc::new(b.finish().with_precision_and_scale(*p, *s)?)
            }
            DataType::Decimal256(p, s) => {
                let mut b = Decimal256Builder::with_capacity(n);
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                        continue;
                    }
                    let v = self.finish_one(sums[i], counts[i])?;
                    match v.to_i256() {
                        Some(x) if Decimal256Type::is_valid_decimal_precision(x, *p) => {
                            b.append_value(x)
                        }
                        _ => return overflow(v),
                    }
                }
                Arc::new(b.finish().with_precision_and_scale(*p, *s)?)
            }
            // DuckDB's `avg` is DOUBLE for every exact input: the exact sum as a DOUBLE, rounded as
            // DuckDB rounds a hugeint, over the count times 10^scale.
            DataType::Float64 if self.spec.mode == Mode::Avg => {
                let unit = 10f64.powi(scale_of(&self.spec.in_ty) as i32);
                let mut b = arrow::array::Float64Builder::with_capacity(n);
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                        continue;
                    }
                    let sum = match sums[i].to_i128() {
                        Some(x) => super::doubles::decimal_to_double(x, 38, 0),
                        None => sums[i].to_string().parse::<f64>().unwrap_or(f64::NAN),
                    };
                    b.append_value(sum / (counts[i] as f64 * unit));
                }
                Arc::new(b.finish())
            }
            t => return exec_err!("{name}: unsupported output type {t}"),
        })
    }

    fn encode(sums: &[Wide<N>], counts: Vec<u64>) -> Result<Vec<ArrayRef>> {
        let mut bytes = vec![0u8; sums.len() * Wide::<N>::BYTES];
        for (s, out) in sums.iter().zip(bytes.chunks_exact_mut(Wide::<N>::BYTES)) {
            s.write_le(out);
        }
        Self::state_arrays(bytes, counts)
    }

    /// The state columns from sums already little-endian, `BYTES` each, none NULL.
    fn state_arrays(bytes: Vec<u8>, counts: Vec<u64>) -> Result<Vec<ArrayRef>> {
        let sums = FixedSizeBinaryArray::try_new(Wide::<N>::BYTES as i32, bytes.into(), None)?;
        Ok(vec![Arc::new(sums), Arc::new(UInt64Array::from(counts))])
    }
}

impl<const N: usize> GroupsAccumulator for Groups<N> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        if let Some(r) = self.update_narrow(&values[0], group_indices, opt_filter) {
            return r;
        }
        let mut sums = std::mem::take(&mut self.sums);
        let mut counts = std::mem::take(&mut self.counts);
        let name = self.spec.name;
        let r = self.each(&values[0], opt_filter, |i, v| {
            let g = group_indices[i];
            sums[g] = sums[g]
                .checked_add(v)
                .ok_or_else(|| exec(format!("{name}: running total left {} bits", N * 64)))?;
            counts[g] += 1;
            Ok(())
        });
        self.sums = sums;
        self.counts = counts;
        r
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        self.build(&sums, &counts)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        Self::encode(&sums, counts)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let sums = values[0].as_fixed_size_binary();
        let counts = values[1].as_primitive::<UInt64Type>();
        let rows = group_indices
            .iter()
            .zip(sums.value_data().chunks_exact(Wide::<N>::BYTES))
            .zip(counts.values().iter());
        match sums.nulls() {
            None => {
                for ((&g, s), &c) in rows {
                    self.fold(g, Wide::from_le(s), c)?;
                }
            }
            Some(valid) => {
                for (i, ((&g, s), &c)) in rows.enumerate() {
                    if valid.is_valid(i) {
                        self.fold(g, Wide::from_le(s), c)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let n = values[0].len();
        let w = Wide::<N>::BYTES;
        // A row left out is a zero sum over no rows, as before.
        let mut bytes = vec![0u8; n * w];
        let mut counts = vec![0u64; n];
        self.each(&values[0], opt_filter, |i, v| {
            v.write_le(&mut bytes[i * w..(i + 1) * w]);
            counts[i] = 1;
            Ok(())
        })?;
        Self::state_arrays(bytes, counts)
    }

    fn size(&self) -> usize {
        self.sums.capacity() * std::mem::size_of::<Wide<N>>() + self.counts.capacity() * 8
    }
}

/// The one-group form of the same state, for ungrouped aggregates and windows.
struct Single<const N: usize>(Groups<N>);

impl<const N: usize> std::fmt::Debug for Single<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.spec.name)
    }
}

impl<const N: usize> Accumulator for Single<N> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let idx = vec![0usize; values[0].len()];
        self.0.update_batch(values, &idx, None, 1)
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.0.grow(1);
        let (mut sum, mut count) = (self.0.sums[0], self.0.counts[0]);
        let name = self.0.spec.name;
        self.0.each(&values[0], None, |_, v| {
            sum = sum
                .checked_sub(v)
                .ok_or_else(|| exec(format!("{name}: running total left {} bits", N * 64)))?;
            count -= 1;
            Ok(())
        })?;
        self.0.sums[0] = sum;
        self.0.counts[0] = count;
        Ok(())
    }
    fn supports_retract_batch(&self) -> bool {
        true
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.0.grow(1);
        let a = self.0.build(&self.0.sums, &self.0.counts)?;
        ScalarValue::try_from_array(&a, 0)
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.0.size()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.grow(1);
        Groups::<N>::encode(&self.0.sums[..1], vec![self.0.counts[0]])?
            .iter()
            .map(|a| ScalarValue::try_from_array(a, 0))
            .collect()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let idx = vec![0usize; states[0].len()];
        self.0.merge_batch(states, &idx, 1)
    }
}

/// `checked_add`, `checked_sub`, `checked_mul`, `checked_neg`: arrow's checked kernels, where
/// DataFusion uses the wrapping ones, plus the decimal precision check neither does.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedBinary {
    sig: Signature,
    /// `None` is unary negation.
    op: Option<Operator>,
}

impl CheckedBinary {
    pub fn udf(op: Option<Operator>) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            op,
        }))
    }
}

fn validate_precision(a: &ArrayRef) -> Result<()> {
    match a.data_type() {
        DataType::Decimal128(p, _) => a
            .as_primitive::<Decimal128Type>()
            .validate_decimal_precision(*p),
        DataType::Decimal256(p, _) => a
            .as_primitive::<Decimal256Type>()
            .validate_decimal_precision(*p),
        _ => Ok(()),
    }
    .map_err(|e| super::errors::overflow(e.to_string()))
}

/// One operand of [`decimal128`]: a column's values, or a literal's on every row.
trait Operand: Copy {
    fn at(self, i: usize) -> i128;
}

#[derive(Clone, Copy)]
struct Each<'a>(&'a [i128]);

#[derive(Clone, Copy)]
struct Same(i128);

impl Operand for Each<'_> {
    #[inline(always)]
    fn at(self, i: usize) -> i128 {
        self.0[i]
    }
}

impl Operand for Same {
    #[inline(always)]
    fn at(self, _: usize) -> i128 {
        self.0
    }
}

fn apply<L: Operand, R: Operand>(
    l: L,
    r: R,
    rows: usize,
    f: impl Fn(i128, i128) -> i128,
) -> Vec<i128> {
    (0..rows).map(|i| f(l.at(i), r.at(i))).collect()
}

/// The bits of the largest magnitude in `v`, so that every value is under 2 to that power.
fn magnitude_bits(v: &[i128]) -> u32 {
    128 - v
        .iter()
        .fold(0u128, |m, x| m | x.unsigned_abs())
        .leading_zeros()
}

/// `l + r`, `l - r` or `l * r` over `Decimal128` operands in plain `i128` arithmetic, where the
/// magnitudes in this batch show no row can overflow it, with arrow's result precision and scale.
/// Arrow's checked kernel makes a checked 128-bit multiply per row, and one more per operand for
/// the rescale of an addition: Q1's `l_extendedprice * (1 - l_discount)` took 1.02 s of CPU over
/// 59.14M rows at SF10 that way (#433). `None` leaves the batch to arrow: other types, a NULL or
/// lone literal, magnitudes that could overflow, or a result outside its precision, which arrow then
/// refuses as before.
fn decimal128(op: Operator, l: &ColumnarValue, r: &ColumnarValue) -> Option<ArrayRef> {
    enum D<'a> {
        Col(&'a Decimal128Array),
        Lit(i128),
    }
    fn dec(v: &ColumnarValue) -> Option<(D<'_>, u8, i8)> {
        match v {
            ColumnarValue::Array(a) => match a.data_type() {
                DataType::Decimal128(p, s) => {
                    Some((D::Col(a.as_primitive::<Decimal128Type>()), *p, *s))
                }
                _ => None,
            },
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(x), p, s)) => {
                Some((D::Lit(*x), *p, *s))
            }
            _ => None,
        }
    }
    let ((l, p1, s1), (r, p2, s2)) = (dec(l)?, dec(r)?);
    let rows = match (&l, &r) {
        (D::Col(a), _) | (_, D::Col(a)) => a.len(),
        (D::Lit(_), D::Lit(_)) => return None,
    };
    let bits = |d: &D| match d {
        D::Col(a) => magnitude_bits(a.values()),
        D::Lit(x) => magnitude_bits(&[*x]),
    };
    let (bl, br) = (bits(&l), bits(&r));
    // Arrow's rules, `decimal_op` in arrow-arith.
    let (precision, scale, bound, out) = match op {
        Operator::Multiply => {
            let scale = s1
                .checked_add(s2)
                .filter(|s| *s <= Decimal128Type::MAX_SCALE)?;
            let precision = p1.saturating_add(p2 + 1).min(Decimal128Type::MAX_PRECISION);
            // Under 2^63 each, the product is one 64-bit multiply; under 2^127 together, it fits.
            let out = if bl <= 63 && br <= 63 {
                let f = |a: i128, b: i128| (a as i64 as i128) * (b as i64 as i128);
                match (&l, &r) {
                    (D::Col(a), D::Col(b)) => apply(Each(a.values()), Each(b.values()), rows, f),
                    (D::Col(a), D::Lit(b)) => apply(Each(a.values()), Same(*b), rows, f),
                    (D::Lit(a), D::Col(b)) => apply(Same(*a), Each(b.values()), rows, f),
                    (D::Lit(_), D::Lit(_)) => return None,
                }
            } else if bl + br <= 127 {
                let f = |a: i128, b: i128| a.wrapping_mul(b);
                match (&l, &r) {
                    (D::Col(a), D::Col(b)) => apply(Each(a.values()), Each(b.values()), rows, f),
                    (D::Col(a), D::Lit(b)) => apply(Each(a.values()), Same(*b), rows, f),
                    (D::Lit(a), D::Col(b)) => apply(Same(*a), Each(b.values()), rows, f),
                    (D::Lit(_), D::Lit(_)) => return None,
                }
            } else {
                return None;
            };
            (precision, scale, bl + br, out)
        }
        Operator::Plus | Operator::Minus => {
            let scale = s1.max(s2);
            let precision = (scale.saturating_add((p1 as i8 - s1).max(p2 as i8 - s2)) as u8)
                .saturating_add(1)
                .min(Decimal128Type::MAX_PRECISION);
            let ml = 10i128.checked_pow(u32::try_from(scale - s1).ok()?)?;
            let mr = 10i128.checked_pow(u32::try_from(scale - s2).ok()?)?;
            // Each rescaled operand under 2^126, so their sum or difference fits.
            let unit_bits = |m: i128| magnitude_bits(&[m]);
            if bl + unit_bits(ml) > 126 || br + unit_bits(mr) > 126 {
                return None;
            }
            let sign = if op == Operator::Minus { -1 } else { 1 };
            let out = match (&l, &r) {
                (D::Col(a), D::Col(b)) if ml == 1 && mr == 1 => {
                    let f = |a: i128, b: i128| a.wrapping_add(b * sign);
                    apply(Each(a.values()), Each(b.values()), rows, f)
                }
                (D::Col(a), D::Col(b)) => {
                    let f = |a: i128, b: i128| (a * ml).wrapping_add(b * mr * sign);
                    apply(Each(a.values()), Each(b.values()), rows, f)
                }
                (D::Col(a), D::Lit(b)) if ml == 1 => {
                    apply(Each(a.values()), Same(*b * mr * sign), rows, |a, b| a + b)
                }
                (D::Col(a), D::Lit(b)) => {
                    let b = *b * mr * sign;
                    apply(Each(a.values()), Same(b), rows, |a, b| a * ml + b)
                }
                (D::Lit(a), D::Col(b)) if mr == 1 => {
                    let f = |a: i128, b: i128| a + b * sign;
                    apply(Same(*a * ml), Each(b.values()), rows, f)
                }
                (D::Lit(a), D::Col(b)) => {
                    let f = |a: i128, b: i128| a + b * mr * sign;
                    apply(Same(*a * ml), Each(b.values()), rows, f)
                }
                (D::Lit(_), D::Lit(_)) => return None,
            };
            let bound = (bl + unit_bits(ml)).max(br + unit_bits(mr)) + 1;
            (precision, scale, bound, out)
        }
        _ => return None,
    };
    // Arrow's precision check compares the scale as a `u8`, so it refuses every negative scale.
    if precision < scale as u8 {
        return None;
    }
    let nulls = match (&l, &r) {
        (D::Col(a), D::Col(b)) => arrow::buffer::NullBuffer::union(a.nulls(), b.nulls()),
        (D::Col(a), _) | (_, D::Col(a)) => a.nulls().cloned(),
        (D::Lit(_), D::Lit(_)) => return None,
    };
    let mut out = out;
    // Arrow leaves zero under a NULL, which no precision refuses.
    if let Some(n) = &nulls {
        for (v, valid) in out.iter_mut().zip(n.iter()) {
            if !valid {
                *v = 0;
            }
        }
    }
    // Every result is under 2^bound; where that is at most 10^precision, all of them fit it.
    let most = 10i128.pow(precision as u32) - 1;
    if bound > 126 || (1i128 << bound) - 1 > most {
        let (lo, hi) = out
            .iter()
            .fold((0i128, 0i128), |(lo, hi), &v| (lo.min(v), hi.max(v)));
        if lo < -most || hi > most {
            return None;
        }
    }
    Some(Arc::new(
        Decimal128Array::new(out.into(), nulls)
            .with_precision_and_scale(precision, scale)
            .ok()?,
    ))
}

impl ScalarUDFImpl for CheckedBinary {
    fn name(&self) -> &str {
        match self.op {
            Some(Operator::Plus) => "checked_add",
            Some(Operator::Minus) => "checked_sub",
            Some(Operator::Multiply) => "checked_mul",
            Some(Operator::Modulo) => "checked_rem",
            _ => "checked_neg",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match (self.op, arg_types) {
            (None, [t]) => Ok(vec![t.clone()]),
            (Some(op), [l, r]) => {
                let (l, r) = BinaryTypeCoercer::new(l, &op, r).get_input_types()?;
                Ok(vec![l, r])
            }
            _ => plan_err!("{}: wrong number of arguments", self.name()),
        }
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match self.op {
            None => Ok(arg_types[0].clone()),
            Some(op) => BinaryTypeCoercer::new(&arg_types[0], &op, &arg_types[1]).get_result_type(),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let fast = match (self.op, &args.args[..]) {
            (Some(op @ (Operator::Plus | Operator::Minus | Operator::Multiply)), [l, r]) => {
                decimal128(op, l, r)
            }
            _ => None,
        };
        let out = match fast {
            Some(out) => out,
            None => {
                let arrays = ColumnarValue::values_to_arrays(&args.args)?;
                let out: ArrayRef = match self.op {
                    Some(Operator::Plus) => numeric::add(&arrays[0], &arrays[1]),
                    Some(Operator::Minus) => numeric::sub(&arrays[0], &arrays[1]),
                    Some(Operator::Multiply) => numeric::mul(&arrays[0], &arrays[1]),
                    Some(Operator::Modulo) => {
                        least_modulo_minus_one(&arrays[0], &arrays[1])?;
                        numeric::rem(&arrays[0], &arrays[1])
                    }
                    _ => numeric::neg(&arrays[0]),
                }
                .map_err(super::errors::arithmetic)?;
                validate_precision(&out)?;
                out
            }
        };
        let want = args.return_field.data_type();
        let out = if out.data_type() == want {
            out
        } else {
            let c = cast_with_options(
                &out,
                want,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .map_err(|e| super::errors::overflow(e.to_string()))?;
            validate_precision(&c)?;
            c
        };
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// Arrow wraps the least signed integer `% -1` to 0; DuckDB refuses it, as it does that `//`.
fn least_modulo_minus_one(l: &ArrayRef, r: &ArrayRef) -> Result<()> {
    fn scan<T: ArrowPrimitiveType>(l: &ArrayRef, r: &ArrayRef, least: T::Native) -> Result<()>
    where
        T::Native: std::fmt::Display,
    {
        let r = cast_with_options(r, l.data_type(), &CastOptions::default())?;
        let minus_one = T::Native::ONE.neg_wrapping();
        for (x, y) in l.as_primitive::<T>().iter().zip(r.as_primitive::<T>()) {
            if x == Some(least) && y == Some(minus_one) {
                return Err(super::errors::overflow(format!(
                    "Overflow in division of {least} % -1"
                )));
            }
        }
        Ok(())
    }
    match l.data_type() {
        DataType::Int8 => scan::<Int8Type>(l, r, i8::MIN),
        DataType::Int16 => scan::<Int16Type>(l, r, i16::MIN),
        DataType::Int32 => scan::<Int32Type>(l, r, i32::MIN),
        DataType::Int64 => scan::<Int64Type>(l, r, i64::MIN),
        DataType::Decimal128(_, 0) => scan::<Decimal128Type>(l, r, i128::MIN),
        DataType::Decimal256(_, 0) => scan::<Decimal256Type>(l, r, i256::MIN),
        _ => Ok(()),
    }
}

/// `checked_shl` / `checked_shr`: an integer shift as DuckDB 1.5 does it. Arrow takes the count
/// modulo the width, so `1000 >> 64` would be 1000; DuckDB's `>>` is 0 for any count outside
/// `[0, bits)`, and its `<<` refuses such a count, a negative signed value, and any bit shifted out.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedShift {
    sig: Signature,
    left: bool,
}

impl CheckedShift {
    pub fn udf(left: bool) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            left,
        }))
    }
}

impl ScalarUDFImpl for CheckedShift {
    fn name(&self) -> &str {
        if self.left {
            "checked_shl"
        } else {
            "checked_shr"
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            // Mixed signs have no common integer type here (DuckDB's is HUGEINT); a count needs none.
            [l, r]
                if l.is_integer()
                    && r.is_integer()
                    && l.is_signed_integer() != r.is_signed_integer() =>
            {
                Ok(vec![l.clone(), r.clone()])
            }
            [l, r] if l.is_integer() && r.is_integer() => {
                let t =
                    BinaryTypeCoercer::new(l, &Operator::BitwiseShiftLeft, r).get_result_type()?;
                Ok(vec![t.clone(), t])
            }
            _ => plan_err!("{} takes two integers", self.name()),
        }
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let t = arrays[0].data_type().clone();
        let bits = (t.primitive_width().unwrap_or(8) * 8) as i128;
        let signed = t.is_signed_integer();
        let (min, max) = if signed {
            (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
        } else {
            (0, (1i128 << bits) - 1)
        };
        let wide = |a: &ArrayRef| -> Result<Vec<Option<i128>>> {
            let d = cast_with_options(a, &DataType::Decimal128(38, 0), &CastOptions::default())?;
            Ok(d.as_primitive::<Decimal128Type>().iter().collect())
        };
        let (xs, ns) = (wide(&arrays[0])?, wide(&arrays[1])?);
        let mut out = Vec::with_capacity(xs.len());
        for (x, n) in xs.into_iter().zip(ns) {
            let (Some(x), Some(n)) = (x, n) else {
                out.push(None);
                continue;
            };
            let in_range = (0..bits).contains(&n);
            out.push(Some(if !self.left {
                if in_range { x >> n } else { 0 }
            } else {
                if !in_range {
                    return exec_err!("Left-shift value {n} is out of range");
                }
                if signed && x < 0 {
                    return exec_err!("Cannot left-shift negative number {x}");
                }
                let v = x << n;
                if v > max || v < min {
                    return Err(super::errors::overflow(format!(
                        "Overflow in left shift ({x} << {n})"
                    )));
                }
                v
            }));
        }
        let d: ArrayRef = Arc::new(Decimal128Array::from(out).with_precision_and_scale(38, 0)?);
        let out = cast_with_options(
            &d,
            &t,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )?;
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// `exact_wide(x)` / `exact_wide_neg(x)`: an exact integer, or canonical decimal text, as a 320-bit
/// value. Internal: the rule uses it to carry a `TRY_CAST`'s source up to the sum that reads it, so
/// a negated uint256 never has to exist as text.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ExactWide {
    sig: Signature,
    neg: bool,
    /// The `TRY_CAST` read was to a 64-bit integer, which takes `0x`/`0b` text.
    hex: bool,
    /// The `TRY_CAST` was written as HUGEINT: text past i128, or a rounded float from 2^127, is
    /// NULL, as DuckDB reads it.
    hugeint: bool,
}

impl ExactWide {
    pub fn udf(neg: bool, hex: bool) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            neg,
            hex,
            hugeint: false,
        }))
    }

    pub fn hugeint_udf(neg: bool) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            neg,
            hex: false,
            hugeint: true,
        }))
    }
}

impl ScalarUDFImpl for ExactWide {
    fn name(&self) -> &str {
        match (self.neg, self.hex) {
            _ if self.hugeint && self.neg => "exact_wide_neg_hugeint",
            _ if self.hugeint => "exact_wide_hugeint",
            (false, false) => "exact_wide",
            (true, false) => "exact_wide_neg",
            (false, true) => "exact_wide_hex",
            (true, true) => "exact_wide_neg_hex",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        use DataType::*;
        Ok(vec![match arg_types {
            [Int8 | Int16 | Int32 | Int64] => Int64,
            [UInt8 | UInt16 | UInt32 | UInt64] => UInt64,
            [
                t @ (Decimal128(_, 0)
                | Decimal256(_, 0)
                | Utf8
                | LargeUtf8
                | Utf8View
                | FixedSizeBinary(WIDE_BYTES)),
            ] => t.clone(),
            [Float64] if self.hugeint => Float64,
            t => return plan_err!("{} has no exact form for {t:?}", self.name()),
        }])
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::FixedSizeBinary(WIDE_BYTES))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        if self.hugeint {
            let mut b = FixedSizeBinaryBuilder::with_capacity(a.len(), WIDE_BYTES);
            let mut buf = [0u8; WIDE_BYTES as usize];
            let mut push = |v: Option<i128>| -> Result<()> {
                match v {
                    Some(v) => {
                        let w = Wide::<5>::from_i128(v);
                        let w = if self.neg {
                            w.checked_neg().expect("i128 negates in 320 bits")
                        } else {
                            w
                        };
                        w.write_le(&mut buf);
                        b.append_value(buf)?;
                    }
                    None => b.append_null(),
                }
                Ok(())
            };
            match a.as_primitive_opt::<arrow::datatypes::Float64Type>() {
                Some(d) => d
                    .iter()
                    .try_for_each(|v| push(v.and_then(super::fastcast::float_hugeint)))?,
                None => super::fastcast::each_text(&a, |s| {
                    push(s.and_then(super::fastcast::duck_hugeint))
                })?,
            }
            let out: ArrayRef = Arc::new(b.finish());
            return Ok(if all_scalar {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
            } else {
                ColumnarValue::Array(out)
            });
        }
        let spec = Spec {
            mode: Mode::Sum,
            name: if self.neg {
                "exact_wide_neg"
            } else {
                "exact_wide"
            },
            in_ty: a.data_type().clone(),
            out_ty: DataType::FixedSizeBinary(WIDE_BYTES),
            lenient: true,
            hex: self.hex,
        };
        let g = Groups::<5>::new(spec);
        let mut b = FixedSizeBinaryBuilder::with_capacity(a.len(), WIDE_BYTES);
        let mut buf = [0u8; WIDE_BYTES as usize];
        let mut next = 0;
        g.each(&a, None, |i, v| {
            for _ in next..i {
                b.append_null();
            }
            let v = if self.neg {
                v.checked_neg()
                    .ok_or_else(|| exec(format!("{}: {v} has no negation", self.name())))?
            } else {
                v
            };
            v.write_le(&mut buf);
            b.append_value(buf)?;
            next = i + 1;
            Ok(())
        })?;
        for _ in next..a.len() {
            b.append_null();
        }
        let out: ArrayRef = Arc::new(b.finish());
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Decimal256Array, Int8Array, Int16Array, Int32Array, Int64Array, PrimitiveArray,
    };

    fn pair<T: ArrowPrimitiveType>(a: PrimitiveArray<T>, b: PrimitiveArray<T>) -> [ArrayRef; 2] {
        [Arc::new(a), Arc::new(b)]
    }

    fn dec(v: Vec<Option<i128>>, p: u8, s: i8) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(
            Decimal128Array::from(v)
                .with_precision_and_scale(p, s)
                .unwrap(),
        ))
    }

    fn lit(v: i128, p: u8, s: i8) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Decimal128(Some(v), p, s))
    }

    /// Arrow's checked kernel and the precision check, as `CheckedBinary` ran every batch before.
    fn arrow_kernel(op: Operator, l: &ColumnarValue, r: &ColumnarValue) -> Result<ArrayRef> {
        let a = ColumnarValue::values_to_arrays(&[l.clone(), r.clone()])?;
        let out = match op {
            Operator::Plus => numeric::add(&a[0], &a[1]),
            Operator::Minus => numeric::sub(&a[0], &a[1]),
            _ => numeric::mul(&a[0], &a[1]),
        }
        .map_err(super::super::errors::arithmetic)?;
        validate_precision(&out)?;
        Ok(out)
    }

    // #433: Q1's decimal arithmetic, `l_extendedprice * (1 - l_discount) * (1 + l_tax)`, each a
    // checked 128-bit multiply per row in arrow's kernel. These shapes must take the plain one.
    #[test]
    fn q1s_decimal_arithmetic_takes_the_plain_kernel() {
        let price = dec(vec![Some(9_999_999_999_999), Some(90_100), None], 15, 2);
        let disc = dec(vec![Some(10), Some(0), Some(4)], 15, 2);
        let one = lit(1, 20, 0);
        let less = decimal128(Operator::Minus, &one, &disc).expect("1 - l_discount");
        assert_eq!(less.data_type(), &DataType::Decimal128(23, 2));
        let less = ColumnarValue::Array(less);
        let disc_price = decimal128(Operator::Multiply, &price, &less).expect("price * (1 - disc)");
        assert_eq!(disc_price.data_type(), &DataType::Decimal128(38, 4));
        let more =
            ColumnarValue::Array(decimal128(Operator::Plus, &one, &disc).expect("1 + l_tax"));
        let charge = decimal128(Operator::Multiply, &ColumnarValue::Array(disc_price), &more)
            .expect("disc_price * (1 + l_tax)");
        let want = arrow_kernel(
            Operator::Multiply,
            &ColumnarValue::Array(
                arrow_kernel(
                    Operator::Multiply,
                    &price,
                    &ColumnarValue::Array(arrow_kernel(Operator::Minus, &one, &disc).unwrap()),
                )
                .unwrap(),
            ),
            &ColumnarValue::Array(arrow_kernel(Operator::Plus, &one, &disc).unwrap()),
        )
        .unwrap();
        assert_eq!(charge.as_ref(), want.as_ref());
    }

    #[test]
    fn the_plain_kernel_answers_as_arrows_or_leaves_it_to_arrow() {
        let big = i128::MAX / 3;
        let values = [
            vec![Some(0), Some(-1), Some(7), None, Some(123_456_789)],
            vec![
                Some(i64::MAX as i128),
                Some(i64::MIN as i128 + 1),
                Some(-5),
                Some(2),
                None,
            ],
            vec![
                Some(1i128 << 70),
                Some(-(1i128 << 70)),
                Some(3),
                Some(-3),
                Some(0),
            ],
            vec![Some(big), Some(-big), Some(1), Some(-1), Some(2)],
            vec![
                Some(10i128.pow(37)),
                Some(-(10i128.pow(37))),
                None,
                None,
                Some(9),
            ],
        ];
        let types = [(38u8, 0i8), (15, 2), (38, 10), (20, -2), (5, 5)];
        let mut fast = 0;
        for op in [Operator::Plus, Operator::Minus, Operator::Multiply] {
            for (i, l) in values.iter().enumerate() {
                for (j, r) in values.iter().enumerate() {
                    let (lt, rt) = (types[i], types[(i + j) % types.len()]);
                    let mut sides = vec![(dec(l.clone(), lt.0, lt.1), dec(r.clone(), rt.0, rt.1))];
                    if let Some(x) = r[2] {
                        sides.push((dec(l.clone(), lt.0, lt.1), lit(x, rt.0, rt.1)));
                        sides.push((lit(x, rt.0, rt.1), dec(l.clone(), lt.0, lt.1)));
                    }
                    for (a, b) in &sides {
                        let want = arrow_kernel(op, a, b);
                        if let Some(got) = decimal128(op, a, b) {
                            fast += 1;
                            assert_eq!(got.as_ref(), want.unwrap().as_ref(), "{op} {i} {j}");
                        }
                    }
                }
            }
        }
        assert!(fast > 50, "{fast}");
        // A result past its precision, or past 128 bits, is arrow's to refuse.
        let max = dec(vec![Some(i128::MAX / 2)], 38, 0);
        assert!(decimal128(Operator::Multiply, &max, &lit(4, 38, 0)).is_none());
        assert!(arrow_kernel(Operator::Multiply, &max, &lit(4, 38, 0)).is_err());
        let most = dec(vec![Some(10i128.pow(38) - 1)], 38, 0);
        assert!(decimal128(Operator::Plus, &most, &lit(1, 38, 0)).is_none());
        assert!(arrow_kernel(Operator::Plus, &most, &lit(1, 38, 0)).is_err());
        assert!(decimal128(Operator::Plus, &lit(1, 38, 0), &lit(1, 38, 0)).is_none());
    }

    fn groups(in_ty: DataType, out_ty: DataType, mode: Mode) -> Groups<3> {
        Groups::new(Spec {
            mode,
            name: "checked_sum",
            in_ty,
            out_ty,
            lenient: false,
            hex: false,
        })
    }

    // #433: Q1's seven checked sums and averages over `Decimal128`, read from the value buffer.
    #[test]
    fn checked_sums_read_their_value_buffers_as_row_by_row() {
        let ty = DataType::Decimal128(15, 2);
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(5),
                None,
                Some(-7),
                Some(10i128.pow(15) - 1),
                Some(3),
                Some(-(10i128.pow(15) - 1)),
                Some(11),
            ])
            .with_precision_and_scale(15, 2)
            .unwrap(),
        );
        let gi = [0, 1, 2, 0, 1, 2, 3];
        let filter = BooleanArray::from(vec![
            Some(true),
            Some(true),
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(true),
        ]);
        for f in [None, Some(&filter)] {
            for (mode, out) in [
                (Mode::Sum, DataType::Decimal128(38, 2)),
                (Mode::Avg, DataType::Decimal128(38, 6)),
                (Mode::Avg, DataType::Float64),
            ] {
                let mut fast = groups(ty.clone(), out.clone(), mode);
                fast.grow(5);
                assert!(fast.update_narrow(&values, &gi, f).unwrap().is_ok());
                let mut slow = groups(ty.clone(), out, mode);
                slow.grow(5);
                let (mut sums, mut counts) = (slow.sums.clone(), slow.counts.clone());
                slow.each(&values, f, |i, v| {
                    sums[gi[i]] = sums[gi[i]].checked_add(v).unwrap();
                    counts[gi[i]] += 1;
                    Ok(())
                })
                .unwrap();
                (slow.sums, slow.counts) = (sums, counts);
                assert_eq!(fast.sums, slow.sums);
                assert_eq!(fast.counts, slow.counts);
                assert_eq!(
                    fast.evaluate(EmitTo::All).unwrap().as_ref(),
                    slow.evaluate(EmitTo::All).unwrap().as_ref()
                );
            }
        }
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(i64::MAX), None]));
        let mut g = groups(DataType::Int64, DataType::Decimal128(38, 0), Mode::Sum);
        g.grow(1);
        assert!(g.update_narrow(&ints, &[0, 0, 0], None).unwrap().is_ok());
        assert_eq!(g.sums[0], Wide::from_i128(2 * i64::MAX as i128));
        assert_eq!(g.counts[0], 2);
        let text: ArrayRef = Arc::new(arrow::array::StringArray::from(vec!["1"]));
        assert!(g.update_narrow(&text, &[0], None).is_none());
    }

    // #50: the least value at 128 bits was `i64::MIN >> (64 - 128)`.
    #[test]
    fn least_modulo_minus_one_at_every_signed_width() {
        let d128 = |v: Vec<i128>| {
            Decimal128Array::from(v)
                .with_precision_and_scale(38, 0)
                .unwrap()
        };
        let d256 = |v: Vec<i256>| {
            Decimal256Array::from(v)
                .with_precision_and_scale(76, 0)
                .unwrap()
        };
        let least = [
            pair(
                Int8Array::from(vec![7, i8::MIN]),
                Int8Array::from(vec![-1, -1]),
            ),
            pair(
                Int16Array::from(vec![7, i16::MIN]),
                Int16Array::from(vec![-1, -1]),
            ),
            pair(
                Int32Array::from(vec![7, i32::MIN]),
                Int32Array::from(vec![-1, -1]),
            ),
            pair(
                Int64Array::from(vec![7, i64::MIN]),
                Int64Array::from(vec![-1, -1]),
            ),
            pair(d128(vec![7, i128::MIN]), d128(vec![-1, -1])),
            pair(
                d256(vec![i256::from(7), i256::MIN]),
                d256(vec![i256::MINUS_ONE; 2]),
            ),
        ];
        for [l, r] in &least {
            let m = least_modulo_minus_one(l, r).unwrap_err().to_string();
            assert!(
                m.contains("Overflow in division of -"),
                "{}: {m}",
                l.data_type()
            );
        }
        let answers = [
            pair(
                Int8Array::from(vec![i8::MIN + 1, i8::MIN]),
                Int8Array::from(vec![-1, 2]),
            ),
            pair(
                Int16Array::from(vec![i16::MIN + 1, i16::MIN]),
                Int16Array::from(vec![-1, 2]),
            ),
            pair(
                Int32Array::from(vec![i32::MIN + 1, i32::MIN]),
                Int32Array::from(vec![-1, 2]),
            ),
            pair(
                Int64Array::from(vec![i64::MIN + 1, i64::MIN]),
                Int64Array::from(vec![-1, 2]),
            ),
            pair(
                d128(vec![i64::MIN as i128, i128::MIN + 1]),
                d128(vec![-1, -1]),
            ),
            pair(
                d256(vec![i256::from_i128(i128::MIN), i256::MIN]),
                d256(vec![i256::MINUS_ONE, i256::from(2)]),
            ),
        ];
        for [l, r] in &answers {
            assert!(least_modulo_minus_one(l, r).is_ok(), "{}", l.data_type());
        }
    }
}
