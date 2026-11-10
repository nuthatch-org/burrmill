//! What a statement builds stays inside its pool.
//!
//! DataFusion charges the pool for what an operator holds and never for what a function returns:
//! one `repeat('a', 1500000000)` grew the process 5.7 GB past a 512 MB pool. Three bounds close it.
//!
//! - **One value.** Nothing a function, `||` or a literal repeated per row builds may pass the value
//!   limit, 16 MiB unless the host sets another. Where an argument chooses the size (`repeat`,
//!   `lpad`, `array_repeat`, `range`, a `replace` target, a `printf` width...) it is refused from the
//!   arguments, before anything is allocated; elsewhere the result is at most a small multiple of
//!   its inputs and is refused once built. Nesting therefore cannot compound.
//! - **All of them.** Each call's output, and each literal a plan repeats on every row, is charged to
//!   the statement's pool for as long as its bytes live, so values under the limit cannot add up
//!   past the pool across rows, columns or a recursive CTE's iterations.
//! - **Series.** `range` and `generate_series` in a FROM clause are charged the same way.

use std::hash::{Hash, Hasher};
use std::panic::AssertUnwindSafe;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, ArrayData, ArrayRef, AsArray, BooleanArray, make_array};
use arrow::buffer::Buffer;
use arrow::datatypes::{
    DataType, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type, IntervalMonthDayNanoType,
    Schema, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, ExprSchema, Result, ScalarValue};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_expr::expr::{Case, Lambda, ScalarFunction};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion_expr::{
    Accumulator, BinaryExpr, ColumnarValue, Documentation, EmitTo, Expr, ExpressionPlacement,
    GroupsAccumulator, LogicalPlan, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, StructFieldMapping, Volatility,
};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_optimizer::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{
    BinaryExpr as PhysicalBinary, Column as PhysicalColumn, Literal as PhysicalLiteral,
};

/// The most one built value may hold, unless the host sets another.
pub const VALUE_LIMIT: usize = 16 << 20;

/// Outputs smaller than this are not charged. A batch of addresses is 86 KiB, and charging it would
/// count it twice under every sort or join that keeps it.
const CHARGE_FLOOR: usize = 256 << 10;

/// A literal longer than this, where a plan repeats it on every row, is built as a charged column.
const LITERAL_FLOOR: usize = 1 << 10;

/// A call over constants that would build more than this is not folded into the plan, which copies a
/// literal at every rewrite; it is built when the statement runs instead, and charged there.
const FOLD_LIMIT: usize = 4 << 10;

const LITERAL: &str = "burrmill_literal";
const CONCAT: &str = "burrmill_concat";

/// The value limit and the pool that built values are charged to: one per engine.
pub struct Limits {
    value: AtomicUsize,
    /// `None` without a budget, whose pool refuses nothing.
    charge: Option<MemoryReservation>,
}

impl std::fmt::Debug for Limits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Limits({})", self.value_limit())
    }
}

impl PartialEq for Limits {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for Limits {}

impl Hash for Limits {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(self, state)
    }
}

impl Limits {
    pub fn new(pool: Option<&Arc<dyn MemoryPool>>) -> Arc<Self> {
        Arc::new(Self {
            value: AtomicUsize::new(VALUE_LIMIT),
            charge: pool.map(|p| MemoryConsumer::new("burrmill built values").register(p)),
        })
    }

    pub fn value_limit(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }

    pub fn set_value_limit(&self, bytes: usize) {
        self.value.store(bytes, Ordering::Relaxed)
    }

    fn refuse(&self, what: &str, bytes: u128) -> Result<()> {
        let limit = self.value_limit();
        if bytes > limit as u128 {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "{what} would build a value of {bytes} bytes, past the limit of {limit} bytes on \
                 one value"
            )));
        }
        Ok(())
    }

    /// `bytes` held against the pool now, if there is a pool and it is worth the charge.
    fn reserve(&self, bytes: u128) -> Result<Option<MemoryReservation>> {
        let Some(charge) = &self.charge else {
            return Ok(None);
        };
        if bytes < CHARGE_FLOOR as u128 {
            return Ok(None);
        }
        let r = charge.new_empty();
        r.try_grow(usize::try_from(bytes).unwrap_or(usize::MAX))?;
        Ok(Some(r))
    }

    /// `array`, with what it newly holds charged to the pool until its bytes are freed.
    fn keep(
        &self,
        array: ArrayRef,
        inputs: &[*const u8],
        held: Option<MemoryReservation>,
    ) -> Result<ArrayRef> {
        let Some(charge) = &self.charge else {
            return Ok(array);
        };
        let data = array.to_data();
        let (mut fresh, mut seen, mut biggest) = (0usize, Vec::new(), (0usize, std::ptr::null()));
        new_buffers(&data, inputs, &mut seen, &mut fresh, &mut biggest);
        if fresh < CHARGE_FLOOR {
            return Ok(array);
        }
        let r = held.unwrap_or_else(|| charge.new_empty());
        r.try_resize(fresh)?;
        let mut r = Some(r);
        Ok(match attach(&data, biggest.1, &mut r) {
            Some(d) => make_array(d),
            None => array,
        })
    }
}

/// Bytes of `d`'s buffers that are none of its inputs', and the largest of them.
fn new_buffers(
    d: &ArrayData,
    inputs: &[*const u8],
    seen: &mut Vec<*const u8>,
    fresh: &mut usize,
    biggest: &mut (usize, *const u8),
) {
    for b in d.buffers() {
        let p = b.as_ptr();
        if b.is_empty() || inputs.contains(&p) || seen.contains(&p) {
            continue;
        }
        seen.push(p);
        *fresh += b.capacity();
        if b.capacity() > biggest.0 {
            *biggest = (b.capacity(), p);
        }
    }
    for c in d.child_data() {
        new_buffers(c, inputs, seen, fresh, biggest);
    }
}

fn input_buffers(d: &ArrayData, out: &mut Vec<*const u8>) {
    out.extend(d.buffers().iter().map(|b| b.as_ptr()));
    for c in d.child_data() {
        input_buffers(c, out);
    }
}

struct Charged {
    _bytes: Buffer,
    _charge: MemoryReservation,
}

/// `d` with the buffer at `target` owned by `charge` as well, which then lasts as long as the bytes.
fn attach(
    d: &ArrayData,
    target: *const u8,
    charge: &mut Option<MemoryReservation>,
) -> Option<ArrayData> {
    if let Some(k) = d.buffers().iter().position(|b| b.as_ptr() == target) {
        let b = &d.buffers()[k];
        let owner = Arc::new(AssertUnwindSafe(Charged {
            _bytes: b.clone(),
            _charge: charge.take()?,
        }));
        // SAFETY: the owner holds the original buffer, so the bytes stay valid as long as it does.
        let held = unsafe {
            Buffer::from_custom_allocation(NonNull::new(b.as_ptr() as *mut u8)?, b.len(), owner)
        };
        let mut buffers = d.buffers().to_vec();
        buffers[k] = held;
        // SAFETY: the same bytes at the same addresses; only who frees them changed.
        return Some(unsafe { d.clone().into_builder().buffers(buffers).build_unchecked() });
    }
    for (k, c) in d.child_data().iter().enumerate() {
        if let Some(c) = attach(c, target, charge) {
            let mut children = d.child_data().to_vec();
            children[k] = c;
            // SAFETY: as above.
            return Some(unsafe {
                d.clone()
                    .into_builder()
                    .child_data(children)
                    .build_unchecked()
            });
        }
    }
    None
}

/// The payload of row `i`: a string's or binary's bytes, a list's elements', a struct's fields'.
fn row_bytes(a: &dyn Array, i: usize) -> u128 {
    if a.is_null(i) {
        return 0;
    }
    (match a.data_type() {
        DataType::Utf8 => a.as_string::<i32>().value_length(i) as usize,
        DataType::LargeUtf8 => a.as_string::<i64>().value_length(i) as usize,
        DataType::Utf8View => a.as_string_view().views()[i] as u32 as usize,
        DataType::Binary => a.as_binary::<i32>().value_length(i) as usize,
        DataType::LargeBinary => a.as_binary::<i64>().value_length(i) as usize,
        DataType::BinaryView => a.as_binary_view().views()[i] as u32 as usize,
        DataType::List(_) => {
            let l = a.as_list::<i32>();
            let o = l.value_offsets();
            return (o[i] as usize..o[i + 1] as usize)
                .map(|j| row_bytes(l.values(), j))
                .sum();
        }
        DataType::LargeList(_) => {
            let l = a.as_list::<i64>();
            let o = l.value_offsets();
            return (o[i] as usize..o[i + 1] as usize)
                .map(|j| row_bytes(l.values(), j))
                .sum();
        }
        DataType::FixedSizeList(_, n) => {
            let l = a.as_fixed_size_list();
            let n = *n as usize;
            return (i * n..(i + 1) * n).map(|j| row_bytes(l.values(), j)).sum();
        }
        DataType::Struct(_) => {
            return a
                .as_struct()
                .columns()
                .iter()
                .map(|c| row_bytes(c, i))
                .sum();
        }
        DataType::Map(_, _) => {
            let m = a.as_map();
            let o = m.value_offsets();
            return (o[i] as usize..o[i + 1] as usize)
                .map(|j| row_bytes(m.entries(), j))
                .sum();
        }
        t => t
            .primitive_width()
            .unwrap_or_else(|| a.get_array_memory_size().div_ceil(a.len().max(1))),
    }) as u128
}

/// Elements in row `i`, counted through nested lists; one for anything that is not a list.
fn row_count(a: &dyn Array, i: usize) -> u128 {
    if a.is_null(i) {
        return 0;
    }
    let range = |o: &[usize]| o[0]..o[1];
    match a.data_type() {
        DataType::List(_) => {
            let l = a.as_list::<i32>();
            let o = l.value_offsets();
            range(&[o[i] as usize, o[i + 1] as usize])
                .map(|j| row_count(l.values(), j))
                .sum()
        }
        DataType::LargeList(_) => {
            let l = a.as_list::<i64>();
            let o = l.value_offsets();
            range(&[o[i] as usize, o[i + 1] as usize])
                .map(|j| row_count(l.values(), j))
                .sum()
        }
        DataType::FixedSizeList(_, n) => {
            let l = a.as_fixed_size_list();
            let n = *n as usize;
            (i * n..(i + 1) * n).map(|j| row_count(l.values(), j)).sum()
        }
        _ => 1,
    }
}

fn scalar_bytes(v: &ScalarValue) -> u128 {
    use ScalarValue as S;
    match v {
        S::Utf8(s) | S::LargeUtf8(s) | S::Utf8View(s) => s.as_ref().map_or(0, |s| s.len() as u128),
        S::Binary(b) | S::LargeBinary(b) | S::BinaryView(b) | S::FixedSizeBinary(_, b) => {
            b.as_ref().map_or(0, |b| b.len() as u128)
        }
        S::List(a) => row_bytes(a.as_ref(), 0),
        S::LargeList(a) => row_bytes(a.as_ref(), 0),
        S::FixedSizeList(a) => row_bytes(a.as_ref(), 0),
        S::Struct(a) => row_bytes(a.as_ref(), 0),
        S::Map(a) => row_bytes(a.as_ref(), 0),
        v => v.data_type().primitive_width().unwrap_or_else(|| v.size()) as u128,
    }
}

fn bytes(a: &ColumnarValue, i: usize) -> u128 {
    match a {
        ColumnarValue::Scalar(s) => scalar_bytes(s),
        ColumnarValue::Array(a) => row_bytes(a, i),
    }
}

fn count(a: &ColumnarValue, i: usize) -> u128 {
    use ScalarValue as S;
    match a {
        ColumnarValue::Scalar(S::List(a)) => row_count(a.as_ref(), 0),
        ColumnarValue::Scalar(S::LargeList(a)) => row_count(a.as_ref(), 0),
        ColumnarValue::Scalar(S::FixedSizeList(a)) => row_count(a.as_ref(), 0),
        ColumnarValue::Scalar(_) => 1,
        ColumnarValue::Array(a) => row_count(a, i),
    }
}

fn text(a: &ColumnarValue, i: usize) -> Option<&str> {
    use ScalarValue as S;
    match a {
        ColumnarValue::Scalar(S::Utf8(s) | S::LargeUtf8(s) | S::Utf8View(s)) => s.as_deref(),
        ColumnarValue::Scalar(_) => None,
        ColumnarValue::Array(a) if a.is_null(i) => None,
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => Some(a.as_string::<i32>().value(i)),
            DataType::LargeUtf8 => Some(a.as_string::<i64>().value(i)),
            DataType::Utf8View => Some(a.as_string_view().value(i)),
            _ => None,
        },
    }
}

/// An integer argument at row `i`; NULL, or anything not an integer, is `None`.
fn int(a: &ColumnarValue, i: usize) -> Option<i128> {
    use ScalarValue as S;
    match a {
        ColumnarValue::Scalar(v) => match v {
            S::Int8(n) => n.map(i128::from),
            S::Int16(n) => n.map(i128::from),
            S::Int32(n) => n.map(i128::from),
            S::Int64(n) => n.map(i128::from),
            S::UInt8(n) => n.map(i128::from),
            S::UInt16(n) => n.map(i128::from),
            S::UInt32(n) => n.map(i128::from),
            S::UInt64(n) => n.map(i128::from),
            _ => None,
        },
        ColumnarValue::Array(a) if a.is_null(i) => None,
        ColumnarValue::Array(a) => Some(match a.data_type() {
            DataType::Int8 => a.as_primitive::<Int8Type>().value(i).into(),
            DataType::Int16 => a.as_primitive::<Int16Type>().value(i).into(),
            DataType::Int32 => a.as_primitive::<Int32Type>().value(i).into(),
            DataType::Int64 => a.as_primitive::<Int64Type>().value(i).into(),
            DataType::UInt8 => a.as_primitive::<UInt8Type>().value(i).into(),
            DataType::UInt16 => a.as_primitive::<UInt16Type>().value(i).into(),
            DataType::UInt32 => a.as_primitive::<UInt32Type>().value(i).into(),
            DataType::UInt64 => a.as_primitive::<UInt64Type>().value(i).into(),
            _ => return None,
        }),
    }
}

fn nonneg(n: Option<i128>) -> u128 {
    n.map_or(0, |n| n.max(0) as u128)
}

/// A date, a timestamp in nanoseconds, or an integer, at row `i`.
fn instant(a: &ColumnarValue, i: usize) -> Option<i128> {
    let v = match a {
        ColumnarValue::Scalar(v) => v.clone(),
        ColumnarValue::Array(a) => ScalarValue::try_from_array(a, i).ok()?,
    };
    use ScalarValue as S;
    let ns = |n: Option<i64>, per: i128| n.map(|n| i128::from(n) * per);
    match v {
        S::Date32(d) => d.map(i128::from),
        S::Date64(ms) => ms.map(|ms| i128::from(ms) / 86_400_000),
        S::TimestampSecond(n, _) => ns(n, 1_000_000_000),
        S::TimestampMillisecond(n, _) => ns(n, 1_000_000),
        S::TimestampMicrosecond(n, _) => ns(n, 1_000),
        S::TimestampNanosecond(n, _) => ns(n, 1),
        v => int(&ColumnarValue::Scalar(v), 0),
    }
}

/// Elements `range` or `generate_series` would make from row `i`.
fn series_len(name: &str, args: &[ColumnarValue], i: usize) -> Result<u128> {
    let step_of = |a: &ColumnarValue| -> Option<(i128, i128, i128)> {
        let v = match a {
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(v)) => (*v)?,
            ColumnarValue::Scalar(_) => return None,
            ColumnarValue::Array(a) if a.is_null(i) => return None,
            ColumnarValue::Array(a) => match a.data_type() {
                DataType::Interval(_) => a.as_primitive::<IntervalMonthDayNanoType>().value(i),
                _ => return None,
            },
        };
        Some((v.months.into(), v.days.into(), v.nanoseconds.into()))
    };
    let (start, stop, step) = match args {
        [stop] => (Some(0), instant(stop, i), Some(1)),
        [start, stop] => (instant(start, i), instant(stop, i), Some(1)),
        [start, stop, step] => {
            let calendar = matches!(
                start.data_type(),
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
            );
            let step = if calendar {
                let Some((m, d, ns)) = step_of(step) else {
                    return Ok(0);
                };
                const DAY: i128 = 86_400_000_000_000;
                const HOUR: i128 = 3_600_000_000_000;
                // A month is 28 to 31 days, and a zoned day 23 to 25 hours.
                let corners: Vec<i128> = if matches!(start.data_type(), DataType::Timestamp(_, _)) {
                    [28, 31]
                        .into_iter()
                        .flat_map(|l| {
                            [DAY - HOUR, DAY + HOUR].map(move |day| m * l * day + d * day + ns)
                        })
                        .collect()
                } else {
                    vec![m * 28 + d, m * 31 + d]
                };
                let same_sign = corners.iter().all(|c| *c > 0) || corners.iter().all(|c| *c < 0);
                if !same_sign {
                    return Err(DataFusionError::Execution(format!(
                        "{name}: a step of {m} months, {d} days and {ns} nanoseconds can be zero"
                    )));
                }
                corners.iter().map(|c| c.abs()).min()
            } else {
                int(step, i)
            };
            (instant(start, i), instant(stop, i), step)
        }
        _ => return Ok(0),
    };
    let (Some(start), Some(stop), Some(step)) = (start, stop, step) else {
        return Ok(0);
    };
    let span = stop - start;
    if step == 0 || (span != 0 && span.signum() != step.signum()) {
        return Ok(0);
    }
    Ok((span.unsigned_abs() / step.unsigned_abs()).saturating_add(1))
}

#[derive(Debug, Default)]
struct Need {
    largest: u128,
    total: u128,
}

/// What `name` can build from `args`, from the arguments alone.
fn need(name: &str, args: &[ColumnarValue], number_rows: usize, limit: usize) -> Result<Need> {
    let rows = if name == LITERAL || args.iter().any(|a| matches!(a, ColumnarValue::Array(_))) {
        number_rows
    } else {
        1
    };
    let at = |k: usize| args.get(k);
    let row: Box<dyn Fn(usize) -> Result<u128> + '_> = match (name, args) {
        ("repeat", [s, n]) => Box::new(|i| Ok(bytes(s, i).saturating_mul(nonneg(int(n, i))))),
        ("lpad" | "rpad", [s, n, ..]) => Box::new(move |i| {
            let width = at(2).map_or(1, |f| {
                text(f, i).map_or(4, |t| if t.is_ascii() { 1 } else { 4 })
            });
            Ok(bytes(s, i).saturating_add(nonneg(int(n, i)).saturating_mul(width)))
        }),
        ("replace", [s, from, to]) => Box::new(|i| {
            let s = bytes(s, i);
            Ok(s.saturating_add((s / bytes(from, i).max(1) + 1).saturating_mul(bytes(to, i))))
        }),
        ("regexp_replace", [s, _, r, ..]) => Box::new(move |i| {
            let (s, rb) = (bytes(s, i), bytes(r, i));
            let refs = text(r, i).map_or(rb, |t| {
                t.bytes().filter(|c| matches!(c, b'\\' | b'$')).count() as u128
            });
            let global = at(3).is_some_and(|o| text(o, i).is_none_or(|t| t.contains('g')));
            let matches = if global { s + 1 } else { 1 };
            Ok(s.saturating_mul(refs + 1)
                .saturating_add(matches.saturating_mul(rb)))
        }),
        ("regexp_match" | "regexp_extract", [s, p, ..]) => Box::new(|i| {
            let groups = text(p, i).map_or(bytes(p, i), |t| t.matches('(').count() as u128);
            Ok((groups + 1).saturating_mul(bytes(s, i) + 16))
        }),
        ("array_repeat", [e, n]) => {
            Box::new(|i| Ok((bytes(e, i) + 8).saturating_mul(nonneg(int(n, i))) + 8))
        }
        ("array_resize", [l, n, ..]) => Box::new(move |i| {
            let fill = at(2).map_or(0, |f| bytes(f, i));
            Ok(bytes(l, i).saturating_add(nonneg(int(n, i)).saturating_mul(fill + 8)))
        }),
        ("range" | "generate_series", _) => {
            Box::new(|i| Ok(series_len(name, args, i)?.saturating_mul(8) + 8))
        }
        ("array_replace" | "array_replace_n" | "array_replace_all", [l, _, to, ..]) => {
            Box::new(|i| Ok(bytes(l, i).saturating_add(count(l, i).saturating_mul(bytes(to, i)))))
        }
        ("array_to_string", [l, sep, ..]) => Box::new(move |i| {
            let each = 48 + bytes(sep, i) + at(2).map_or(0, |n| bytes(n, i));
            Ok(bytes(l, i).saturating_add(count(l, i).saturating_mul(each)))
        }),
        ("concat_ws", [sep, rest @ ..]) => Box::new(move |i| {
            let parts: u128 = rest
                .iter()
                .map(|a| bytes(a, i))
                .fold(0, u128::saturating_add);
            Ok(parts.saturating_add((rest.len() as u128).saturating_mul(bytes(sep, i))))
        }),
        ("printf" | "format", [fmt, rest @ ..]) => Box::new(move |i| {
            let Some(f) = text(fmt, i) else {
                return Ok(0);
            };
            // Every number in the format is a width or a precision at most.
            let widths = f
                .split(|c: char| !c.is_ascii_digit())
                .filter(|d| !d.is_empty())
                .map(|d| d.parse::<u128>().unwrap_or(u128::MAX))
                .fold(0, u128::saturating_add);
            let refs = f.matches(['%', '{']).count() as u128;
            let widest = rest.iter().map(|a| bytes(a, i)).max().unwrap_or(0);
            Ok((f.len() as u128)
                .saturating_add(widths)
                .saturating_add(refs.saturating_mul(widest + 64)))
        }),
        ("to_char" | "strftime", [_, fmt, ..]) => Box::new(|i| Ok(16 * bytes(fmt, i) + 64)),
        ("arrays_zip", _) => Box::new(|i| {
            let all: u128 = args
                .iter()
                .map(|a| bytes(a, i))
                .fold(0, u128::saturating_add);
            let longest = args.iter().map(|a| count(a, i)).max().unwrap_or(0);
            Ok((args.len() as u128).saturating_mul(all.saturating_add(longest.saturating_mul(16))))
        }),
        (LITERAL, [v]) => {
            let one = bytes(v, 0);
            let total = match v.data_type() {
                // A repeated view shares one copy of the bytes.
                DataType::Utf8View | DataType::BinaryView => one + 16 * rows as u128,
                _ => one.saturating_mul(rows as u128),
            };
            return Ok(Need {
                largest: one,
                total,
            });
        }
        _ => {
            // At most what it is given: what is left either shrinks its input, keeps its size,
            // joins its arguments or grows them by a fixed factor, which the result is checked for.
            let (mut held, mut spread) = (0u128, 0u128);
            for a in args {
                match a {
                    ColumnarValue::Array(a) => held += a.get_array_memory_size() as u128,
                    ColumnarValue::Scalar(s) => spread += scalar_bytes(s),
                }
            }
            let total = held.saturating_add(spread.saturating_mul(rows as u128));
            if held.saturating_add(spread) <= limit as u128 {
                return Ok(Need {
                    largest: held + spread,
                    total,
                });
            }
            Box::new(|i| {
                Ok(args
                    .iter()
                    .map(|a| bytes(a, i))
                    .fold(0, u128::saturating_add))
            })
        }
    };
    let mut n = Need::default();
    for i in 0..rows {
        let b = row(i)?;
        n.largest = n.largest.max(b);
        n.total = n.total.saturating_add(b);
    }
    Ok(n)
}

thread_local! {
    static PLANNING: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Held while a statement is analyzed and optimized, when the only calls evaluated are constant
/// folding's.
pub struct Planning(());

impl Planning {
    pub fn enter() -> Self {
        PLANNING.with(|p| p.set(p.get() + 1));
        Self(())
    }
}

impl Drop for Planning {
    fn drop(&mut self) {
        PLANNING.with(|p| p.set(p.get() - 1));
    }
}

/// Refuses to fold a constant past [`FOLD_LIMIT`] while planning. Constant folding then leaves the
/// call in the plan, and it is built when the statement runs, where it is charged.
fn unfolded(name: &str, bytes: u128) -> Result<()> {
    if bytes > FOLD_LIMIT as u128 && PLANNING.with(|p| p.get() > 0) {
        return Err(DataFusionError::Execution(format!(
            "{name} over constants would build {bytes} bytes, too many to fold"
        )));
    }
    Ok(())
}

/// What a value of `t` may hold beyond a fixed width per row.
fn builds(t: &DataType) -> bool {
    match t {
        DataType::Dictionary(_, v) => builds(v),
        t => !t.is_primitive() && !matches!(t, DataType::Boolean | DataType::Null),
    }
}

/// The largest row `out` holds, when it could pass `limit` at all.
fn largest(out: &ColumnarValue, limit: usize) -> u128 {
    match out {
        ColumnarValue::Scalar(s) => scalar_bytes(s),
        ColumnarValue::Array(a) if a.get_array_memory_size() <= limit => 0,
        ColumnarValue::Array(a) => (0..a.len()).map(|i| row_bytes(a, i)).max().unwrap_or(0),
    }
}

/// A scalar function, refusing a value past the limit and charging what it builds to the pool.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Bounded {
    inner: Arc<ScalarUDF>,
    limits: Arc<Limits>,
}

/// `f`, bounded. `get_field` stays itself: the Parquet reader recognises it to read one field.
pub fn wrap(f: Arc<ScalarUDF>, limits: &Arc<Limits>) -> Arc<ScalarUDF> {
    if f.inner().is::<Bounded>() || f.name() == "get_field" {
        return f;
    }
    Arc::new(ScalarUDF::from(Bounded {
        inner: f,
        limits: Arc::clone(limits),
    }))
}

impl Bounded {
    fn imp(&self) -> &dyn ScalarUDFImpl {
        self.inner.inner().as_ref()
    }
}

impl ScalarUDFImpl for Bounded {
    fn name(&self) -> &str {
        self.imp().name()
    }
    fn aliases(&self) -> &[String] {
        self.imp().aliases()
    }
    fn display_name(&self, args: &[Expr]) -> Result<String> {
        #[allow(deprecated)]
        self.imp().display_name(args)
    }
    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        self.imp().schema_name(args)
    }
    fn signature(&self) -> &Signature {
        self.imp().signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.imp().return_type(arg_types)
    }
    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        let inner = Arc::new(self.imp().with_updated_config(config)?);
        Some(ScalarUDF::from(Self {
            inner,
            limits: Arc::clone(&self.limits),
        }))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.imp().return_field_from_args(args)
    }
    #[allow(deprecated)]
    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        self.imp().is_nullable(args, schema)
    }
    fn is_strict(&self) -> bool {
        self.imp().is_strict()
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !builds(args.return_field.data_type()) {
            return self.inner.invoke_with_args(args);
        }
        let name = self.name();
        let need = need(
            name,
            &args.args,
            args.number_rows,
            self.limits.value_limit(),
        )?;
        self.limits.refuse(name, need.largest)?;
        let constant = name != LITERAL
            && !args.args.is_empty()
            && args
                .args
                .iter()
                .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        if constant {
            unfolded(name, need.largest)?;
        }
        let held = self.limits.reserve(need.total)?;
        let rows = args.number_rows;
        let mut inputs = Vec::new();
        if self.limits.charge.is_some() {
            for a in &args.args {
                if let ColumnarValue::Array(a) = a {
                    input_buffers(&a.to_data(), &mut inputs);
                }
            }
        }
        let out = self.inner.invoke_with_args(args)?;
        let limit = self.limits.value_limit();
        let built = largest(
            &out,
            if constant {
                limit.min(FOLD_LIMIT)
            } else {
                limit
            },
        );
        self.limits.refuse(name, built)?;
        if constant {
            unfolded(name, built)?;
        }
        Ok(match out {
            ColumnarValue::Array(a) => ColumnarValue::Array(self.limits.keep(a, &inputs, held)?),
            // A constant the plan could not fold would be repeated per row by whatever reads it.
            ColumnarValue::Scalar(s)
                if constant
                    && rows > 1
                    && self.limits.charge.is_some()
                    && scalar_bytes(&s) > LITERAL_FLOOR as u128 =>
            {
                let held = self
                    .limits
                    .reserve(scalar_bytes(&s).saturating_mul(rows as u128))?;
                ColumnarValue::Array(self.limits.keep(s.to_array_of_size(rows)?, &[], held)?)
            }
            s => s,
        })
    }
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        Ok(match self.imp().simplify(args, info)? {
            ExprSimplifyResult::Simplified(e) => {
                ExprSimplifyResult::Simplified(bound_expr(e, false, &self.limits)?.data)
            }
            other => other,
        })
    }
    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        self.imp().preimage(args, lit_expr, info)
    }
    fn short_circuits(&self) -> bool {
        self.imp().short_circuits()
    }
    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.imp().conditional_arguments(args)
    }
    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        self.imp().evaluate_bounds(input)
    }
    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.imp().propagate_constraints(interval, inputs)
    }
    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        self.imp().output_ordering(inputs)
    }
    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.imp().preserves_lex_ordering(inputs)
    }
    fn strictly_order_preserving(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.imp().strictly_order_preserving(inputs)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.imp().coerce_types(arg_types)
    }
    fn struct_field_mapping(
        &self,
        literal_args: &[Option<ScalarValue>],
    ) -> Option<StructFieldMapping> {
        self.imp().struct_field_mapping(literal_args)
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.imp().documentation()
    }
    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.imp().placement(args)
    }
}

/// `a || b`, as DataFusion evaluates it, so that what it builds is bounded like a function's.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Concat(Signature);

impl ScalarUDFImpl for Concat {
    fn name(&self) -> &str {
        CONCAT
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn display_name(&self, args: &[Expr]) -> Result<String> {
        Ok(format!("{} || {}", args[0], args[1]))
    }
    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        Ok(format!(
            "{} || {}",
            args[0].schema_name(),
            args[1].schema_name()
        ))
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        BinaryTypeCoercer::new(&args[0], &Operator::StringConcat, &args[1]).get_result_type()
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(
            CONCAT,
            self.return_type(&types)?,
            nullable,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (mut fields, mut columns) = (Vec::new(), Vec::new());
        let mut side = |a: ColumnarValue| -> Arc<dyn PhysicalExpr> {
            match a {
                ColumnarValue::Scalar(s) => Arc::new(PhysicalLiteral::new(s)),
                ColumnarValue::Array(a) => {
                    let name = format!("c{}", columns.len());
                    fields.push(Field::new(&name, a.data_type().clone(), true));
                    columns.push(a);
                    Arc::new(PhysicalColumn::new(&name, columns.len() - 1))
                }
            }
        };
        let mut it = args.args.into_iter();
        let (Some(l), Some(r)) = (it.next(), it.next()) else {
            return datafusion_common::internal_err!("{CONCAT} takes two arguments");
        };
        let (l, r) = (side(l), side(r));
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(args.number_rows)),
        )?;
        PhysicalBinary::new(l, Operator::StringConcat, r).evaluate(&batch)
    }
}

/// A literal a plan repeats on every row, built as a column. Volatile, so constant folding leaves it.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Repeated(Signature);

impl ScalarUDFImpl for Repeated {
    fn name(&self) -> &str {
        LITERAL
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn display_name(&self, args: &[Expr]) -> Result<String> {
        Ok(args[0].to_string())
    }
    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        Ok(args[0].schema_name().to_string())
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(match &args.args[0] {
            ColumnarValue::Scalar(s) => ColumnarValue::Array(s.to_array_of_size(args.number_rows)?),
            a => a.clone(),
        })
    }
}

/// `e` with `||` and every scalar function bounded, and, where `spread` says the value is repeated
/// per row, a long literal built as a charged column.
fn bound_expr(e: Expr, spread: bool, limits: &Arc<Limits>) -> Result<Transformed<Expr>> {
    let recurse = |e: Expr, spread: bool| bound_expr(e, spread, limits);
    let t = match e {
        Expr::Alias(_) | Expr::Cast(_) | Expr::TryCast(_) | Expr::Negative(_) | Expr::Not(_) => {
            e.map_children(|c| recurse(c, spread))?
        }
        Expr::Case(Case {
            expr,
            when_then_expr,
            else_expr,
        }) => {
            let mut changed = false;
            let mut go = |e: Box<Expr>, spread: bool| -> Result<Box<Expr>> {
                let t = recurse(*e, spread)?;
                changed |= t.transformed;
                Ok(Box::new(t.data))
            };
            let expr = expr.map(|e| go(e, false)).transpose()?;
            let when_then_expr = when_then_expr
                .into_iter()
                .map(|(w, t)| Ok((go(w, false)?, go(t, true)?)))
                .collect::<Result<_>>()?;
            let else_expr = else_expr.map(|e| go(e, true)).transpose()?;
            let e = Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            });
            if changed {
                Transformed::yes(e)
            } else {
                Transformed::no(e)
            }
        }
        Expr::Lambda(Lambda { params, body }) => {
            let t = recurse(*body, true)?;
            t.map_data(|body| {
                Ok(Expr::Lambda(Lambda {
                    params,
                    body: Box::new(body),
                }))
            })?
        }
        e => e.map_children(|c| recurse(c, false))?,
    };
    t.transform_data(|e| {
        Ok(match e {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::StringConcat,
                right,
            }) => {
                let concat = Arc::new(ScalarUDF::from(Concat(Signature::any(
                    2,
                    Volatility::Immutable,
                ))));
                Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                    wrap(concat, limits),
                    vec![*left, *right],
                )))
            }
            Expr::ScalarFunction(f)
                if !f.func.inner().is::<Bounded>() && f.func.name() != "get_field" =>
            {
                Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                    wrap(f.func, limits),
                    f.args,
                )))
            }
            Expr::Literal(v, m) if spread && scalar_bytes(&v) > LITERAL_FLOOR as u128 => {
                Transformed::yes(repeated(Expr::Literal(v, m), limits))
            }
            e => Transformed::no(e),
        })
    })
}

fn repeated(e: Expr, limits: &Arc<Limits>) -> Expr {
    let f = Arc::new(ScalarUDF::from(Repeated(Signature::any(
        1,
        Volatility::Volatile,
    ))));
    Expr::ScalarFunction(ScalarFunction::new_udf(wrap(f, limits), vec![e]))
}

fn bound_plan(plan: LogicalPlan, limits: &Arc<Limits>) -> Result<Transformed<LogicalPlan>> {
    plan.transform_up_with_subqueries(|p| {
        let t = p.map_expressions(|e| bound_expr(e, true, limits))?;
        if t.transformed {
            Ok(Transformed::yes(t.data.recompute_schema()?))
        } else {
            Ok(t)
        }
    })
}

/// Bounds every value a plan builds: last among the analyzer rules, so none of them meets a
/// rewritten `||`, and last in each optimizer pass, for the literals constant folding makes.
#[derive(Debug)]
pub struct BoundValues(pub Arc<Limits>);

impl AnalyzerRule for BoundValues {
    fn name(&self) -> &str {
        "bound_values"
    }
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        bound_plan(plan, &self.0).map(|t| t.data)
    }
}

impl OptimizerRule for BoundValues {
    fn name(&self) -> &str {
        "bound_values"
    }
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        bound_plan(plan, &self.0)
    }
}

/// The column `build` makes in `bytes`, charged to the pool before it is built and for as long as
/// it lives.
pub fn charge_column(
    limits: &Limits,
    bytes: usize,
    build: impl FnOnce() -> ArrayRef,
) -> Result<ArrayRef> {
    let held = limits.reserve(bytes as u128)?;
    limits.keep(build(), &[], held)
}

/// The delimiter's bytes, for `string_agg(x, sep)`; zero when it is not a literal.
pub fn delimiter_len(args: &datafusion_expr::function::AccumulatorArgs) -> usize {
    args.exprs
        .get(1)
        .and_then(|e| e.as_ref().downcast_ref::<PhysicalLiteral>())
        .map_or(0, |l| scalar_bytes(l.value()) as usize)
}

/// `string_agg`'s one value, bounded as any built value is: refused before a batch would carry it
/// past the limit, since a long delimiter is repeated per row before the pool is next charged.
#[derive(Debug)]
pub struct BoundedAgg {
    inner: Box<dyn Accumulator>,
    delimiter: usize,
    limits: Arc<Limits>,
    projected: u128,
}

impl BoundedAgg {
    pub fn new(inner: Box<dyn Accumulator>, delimiter: usize, limits: &Arc<Limits>) -> Self {
        Self {
            inner,
            delimiter,
            limits: Arc::clone(limits),
            projected: 0,
        }
    }

    fn grow(&mut self, values: &ArrayRef) -> Result<()> {
        let d = self.delimiter as u128;
        for i in 0..values.len() {
            if values.is_valid(i) {
                let n = row_count(values, i);
                self.projected = self
                    .projected
                    .saturating_add(row_bytes(values, i))
                    .saturating_add(n.saturating_mul(d));
            }
        }
        self.limits.refuse("string_agg", self.projected)
    }
}

impl Accumulator for BoundedAgg {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.grow(&values[0])?;
        self.inner.update_batch(values)
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.grow(&states[0])?;
        self.inner.merge_batch(states)
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }
    fn size(&self) -> usize {
        self.inner.size()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(values)
    }
    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

/// [`BoundedAgg`] per group.
pub struct BoundedGroups {
    inner: Box<dyn GroupsAccumulator>,
    delimiter: usize,
    limits: Arc<Limits>,
    projected: Vec<u128>,
}

impl BoundedGroups {
    pub fn new(inner: Box<dyn GroupsAccumulator>, delimiter: usize, limits: &Arc<Limits>) -> Self {
        Self {
            inner,
            delimiter,
            limits: Arc::clone(limits),
            projected: Vec::new(),
        }
    }

    fn grow(
        &mut self,
        values: &ArrayRef,
        groups: &[usize],
        filter: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        if self.projected.len() < total {
            self.projected.resize(total, 0);
        }
        let d = self.delimiter as u128;
        for (i, &g) in groups.iter().enumerate() {
            if values.is_valid(i) && filter.is_none_or(|f| f.is_valid(i) && f.value(i)) {
                let p = &mut self.projected[g];
                *p = p
                    .saturating_add(row_bytes(values, i))
                    .saturating_add(row_count(values, i).saturating_mul(d));
                self.limits.refuse("string_agg", *p)?;
            }
        }
        Ok(())
    }

    fn emitted(&mut self, emit_to: EmitTo) {
        match emit_to {
            EmitTo::All => self.projected.clear(),
            EmitTo::First(n) => {
                self.projected.drain(..n.min(self.projected.len()));
            }
        }
    }
}

impl GroupsAccumulator for BoundedGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(&values[0], group_indices, opt_filter, total_num_groups)?;
        self.inner
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(&values[0], group_indices, None, total_num_groups)?;
        self.inner
            .merge_batch(values, group_indices, total_num_groups)
    }
    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.emitted(emit_to);
        self.inner.evaluate(emit_to)
    }
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.emitted(emit_to);
        self.inner.state(emit_to)
    }
    fn size(&self) -> usize {
        self.inner.size() + self.projected.capacity() * size_of::<u128>()
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, opt_filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(v.into())))
    }
    fn n(v: i64) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v)))
    }

    #[test]
    fn a_size_argument_is_read_before_anything_is_built() {
        assert_eq!(
            need("repeat", &[s("ab"), n(1 << 40)], 1, VALUE_LIMIT)
                .unwrap()
                .largest,
            2 << 40
        );
        assert_eq!(
            need("lpad", &[s("a"), n(1000)], 1, VALUE_LIMIT)
                .unwrap()
                .largest,
            1001
        );
        assert_eq!(
            need("lpad", &[s("a"), n(1000), s("é")], 1, VALUE_LIMIT)
                .unwrap()
                .largest,
            4001
        );
        assert_eq!(
            need("array_repeat", &[n(1), n(1000)], 1, VALUE_LIMIT)
                .unwrap()
                .largest,
            16008
        );
        assert_eq!(
            need("range", &[n(1000)], 1, VALUE_LIMIT).unwrap().largest,
            8016
        );
        assert_eq!(
            need("range", &[n(10), n(0), n(1)], 1, VALUE_LIMIT)
                .unwrap()
                .largest,
            8
        );
        assert_eq!(
            need("generate_series", &[n(0), n(-10), n(-5)], 1, VALUE_LIMIT)
                .unwrap()
                .largest,
            32
        );
        assert_eq!(
            need("replace", &[s("aaaa"), s("a"), s("bbb")], 1, VALUE_LIMIT)
                .unwrap()
                .largest,
            19
        );
        assert_eq!(
            need("printf", &[s("%1000000d"), n(1)], 1, VALUE_LIMIT)
                .unwrap()
                .largest,
            1000081
        );
        assert_eq!(
            need("concat_ws", &[s("--"), s("a"), s("b")], 1, VALUE_LIMIT)
                .unwrap()
                .largest,
            6
        );
    }

    #[test]
    fn a_calendar_step_that_can_be_zero_is_refused() {
        let date = ColumnarValue::Scalar(ScalarValue::Date32(Some(0)));
        let step = ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
            arrow::datatypes::IntervalMonthDayNano::new(1, -30, 0),
        )));
        assert!(need("range", &[date.clone(), date, step], 1, VALUE_LIMIT).is_err());
    }
}
