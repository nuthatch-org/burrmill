//! DuckDB's list functions where DataFusion's differ.
//!
//! `list_reduce(list, lambda acc, x: ...)` DataFusion does not have. The accumulator starts at the first element and the lambda folds in the rest in list order, so
//! a DOUBLE fold rounds exactly as DuckDB's does. Each step is one batch evaluation of the lambda
//! over every row still holding elements: rows are ordered longest first, which keeps those rows
//! a prefix and each step a slice. A NULL list is NULL, an empty one refuses as DuckDB does, and
//! the lambda's result is cast back to the element type, as DuckDB casts it.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, UInt32Array};
use arrow::compute::{cast, concat, take, take_arrays};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::type_coercion::binary::type_union_resolution;
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, ValueOrLambda, Volatility,
};

/// `list_prepend(x, list)` and `list_append(list, x)` with `x` and the elements unified the way
/// a list literal unifies them. DataFusion finds that type and then refuses to cast a struct to
/// it, so `list_prepend({'k': 'a'}, list({'k': text_column}))` failed on `Utf8` against `Utf8View`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ElementAndList {
    sig: Signature,
    inner: Arc<ScalarUDF>,
    element_first: bool,
}

impl ElementAndList {
    pub fn udf(inner: Arc<ScalarUDF>, element_first: bool) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            inner,
            element_first,
        }))
    }
}

impl ScalarUDFImpl for ElementAndList {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        let [a, b] = args else {
            return plan_err!("{} takes an element and a list", self.name());
        };
        let (x, list) = if self.element_first { (a, b) } else { (b, a) };
        let (e, large) = match list {
            DataType::List(f) | DataType::ListView(f) | DataType::FixedSizeList(f, _) => {
                (f.data_type().clone(), false)
            }
            DataType::LargeList(f) | DataType::LargeListView(f) => (f.data_type().clone(), true),
            DataType::Null => (DataType::Null, false),
            other => return plan_err!("{} expected a list, got {other}", self.name()),
        };
        let Some(u) = type_union_resolution(&[x.clone(), e.clone()]) else {
            return plan_err!("{} cannot combine {x} with a list of {e}", self.name());
        };
        let field = Arc::new(Field::new_list_field(u.clone(), true));
        let list = if large {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        };
        Ok(if self.element_first {
            vec![u, list]
        } else {
            vec![list, u]
        })
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.inner.return_type(args)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ListReduce {
    signature: HigherOrderSignature,
    aliases: Vec<String>,
}

impl ListReduce {
    pub fn udf() -> Arc<HigherOrderUDF> {
        Arc::new(HigherOrderUDF::new_from_impl(Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            aliases: vec!["array_reduce".into(), "reduce".into()],
        }))
    }
}

fn element(list: &DataType) -> Result<&FieldRef> {
    match list {
        DataType::List(f) | DataType::LargeList(f) => Ok(f),
        other => plan_err!("list_reduce expected a list, got {other}"),
    }
}

/// Offsets of a List or LargeList, relative to its (possibly sliced) values.
fn bounds(list: &ArrayRef) -> Result<(ArrayRef, Vec<(usize, usize)>)> {
    fn of<O: arrow::array::OffsetSizeTrait>(
        l: &arrow::array::GenericListArray<O>,
    ) -> (ArrayRef, Vec<(usize, usize)>) {
        let o = l.value_offsets();
        let base = o[0].as_usize();
        let end = o[o.len() - 1].as_usize();
        let values = l.values().slice(base, end - base);
        let b = o
            .windows(2)
            .map(|w| (w[0].as_usize() - base, w[1].as_usize() - base))
            .collect();
        (values, b)
    }
    match list.data_type() {
        DataType::List(_) => Ok(of(list.as_list::<i32>())),
        DataType::LargeList(_) => Ok(of(list.as_list::<i64>())),
        other => exec_err!("list_reduce expected a list, got {other}"),
    }
}

impl HigherOrderUDFImpl for ListReduce {
    fn name(&self) -> &str {
        "list_reduce"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [list] = arg_types else {
            return plan_err!("list_reduce takes a list and a lambda");
        };
        Ok(vec![match list {
            DataType::List(_) | DataType::LargeList(_) => list.clone(),
            DataType::FixedSizeList(f, _) | DataType::ListView(f) => DataType::List(Arc::clone(f)),
            DataType::LargeListView(f) => DataType::LargeList(Arc::clone(f)),
            other => return plan_err!("list_reduce expected a list, got {other}"),
        }])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let [ValueOrLambda::Value(list), ValueOrLambda::Lambda(_)] = fields else {
            return plan_err!("list_reduce takes a list and a lambda");
        };
        let e = element(list.data_type())?;
        let acc = Arc::new(e.as_ref().clone().with_nullable(true));
        Ok(LambdaParametersProgress::Complete(vec![vec![
            acc,
            Arc::clone(e),
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let [ValueOrLambda::Value(list), ValueOrLambda::Lambda(_)] = args.arg_fields else {
            return plan_err!("list_reduce takes a list and a lambda");
        };
        let e = element(list.data_type())?;
        Ok(Arc::new(Field::new("", e.data_type().clone(), true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [ValueOrLambda::Value(list), ValueOrLambda::Lambda(lambda)] = args.args.as_slice()
        else {
            return plan_err!("list_reduce takes a list and a lambda");
        };
        let out = args.return_type().clone();
        let list = list.to_array(args.number_rows)?;
        let (values, bounds) = bounds(&list)?;

        let mut rows: Vec<u32> = Vec::new();
        for (r, &(s, e)) in bounds.iter().enumerate() {
            if list.is_null(r) {
                continue;
            }
            if s == e {
                return exec_err!("Cannot perform list_reduce on an empty input list");
            }
            rows.push(r as u32);
        }
        rows.sort_by_key(|&r| {
            let (s, e) = bounds[r as usize];
            std::cmp::Reverse(e - s)
        });
        let at = |k: usize, step: usize| -> UInt32Array {
            rows[..k]
                .iter()
                .map(|&r| (bounds[r as usize].0 + step) as u32)
                .collect()
        };

        // Rows finish shortest first, from the end of `rows`; `done` collects them in that order.
        let mut acc = cast(&take(&values, &at(rows.len(), 0), None)?, &out)?;
        let mut done: Vec<ArrayRef> = Vec::new();
        let mut live = rows.len();
        let mut step = 1;
        while live > 0 {
            let k = rows[..live].partition_point(|&r| {
                let (s, e) = bounds[r as usize];
                e - s > step
            });
            if k < live {
                done.push(acc.slice(k, live - k));
            }
            if k == 0 {
                break;
            }
            let a = acc.slice(0, k);
            let x = take(&values, &at(k, step), None)?;
            let ids: UInt32Array = rows[..k].iter().copied().collect();
            let next = lambda
                .evaluate(
                    &[&|| Ok(Arc::clone(&a)), &|| Ok(Arc::clone(&x))],
                    |captured| Ok(take_arrays(captured, &ids, None)?),
                )?
                .into_array(k)?;
            acc = if next.data_type() == &out {
                next
            } else {
                cast(&next, &out)?
            };
            live = k;
            step += 1;
        }

        let finished: Vec<&dyn Array> = done.iter().rev().map(|a| a.as_ref()).collect();
        let sorted = if finished.is_empty() {
            arrow::array::new_empty_array(&out)
        } else {
            concat(&finished)?
        };
        let mut position = vec![None; args.number_rows];
        for (i, &r) in rows.iter().enumerate() {
            position[r as usize] = Some(i as u32);
        }
        Ok(ColumnarValue::Array(take(
            &sorted,
            &UInt32Array::from(position),
            None,
        )?))
    }
}

/// `list_min` and `list_max` with a NULL list NULL, as DuckDB has it; DataFusion's refuse the type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ListExtreme {
    sig: Signature,
    inner: Arc<ScalarUDF>,
}

impl ListExtreme {
    pub fn udf(inner: Arc<ScalarUDF>) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            inner,
        }))
    }
}

impl ScalarUDFImpl for ListExtreme {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [DataType::Null | DataType::List(_) | DataType::LargeList(_)] => Ok(args.to_vec()),
            [DataType::FixedSizeList(f, _)] => Ok(vec![DataType::List(Arc::clone(f))]),
            [other] => plan_err!("{} does not support type {other}", self.name()),
            _ => plan_err!("{} takes one list", self.name()),
        }
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args {
            [DataType::Null] => Ok(DataType::Null),
            _ => self.inner.return_type(args),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.first().map(|a| a.data_type()) {
            Some(DataType::Null) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
            _ => self.inner.invoke_with_args(args),
        }
    }
}

/// `list_sum` as DuckDB's `sum` over the elements: integers and decimals sum exactly to a
/// 38-digit decimal (HUGEINT here) and refuse past it, where DataFusion's sums every list as a
/// DOUBLE. Any other element type is left to DataFusion's.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ListSum {
    sig: Signature,
    inner: Arc<ScalarUDF>,
}

impl ListSum {
    pub fn udf(inner: Arc<ScalarUDF>) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            inner,
        }))
    }
}

/// The scale of a list's exact sum, or `None` where DataFusion's DOUBLE sum stands.
fn exact_scale(list: &DataType) -> Option<i8> {
    let e = match list {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => f.data_type(),
        _ => return None,
    };
    match e {
        t if t.is_integer() => Some(0),
        DataType::Decimal128(_, s) => Some(*s),
        _ => None,
    }
}

impl ScalarUDFImpl for ListSum {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [l @ (DataType::List(_) | DataType::LargeList(_))] if exact_scale(l).is_some() => {
                Ok(args.to_vec())
            }
            [l @ DataType::FixedSizeList(f, _)] if exact_scale(l).is_some() => {
                Ok(vec![DataType::List(Arc::clone(f))])
            }
            _ => self.inner.coerce_types(args),
        }
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args.first().and_then(exact_scale) {
            Some(s) => Ok(DataType::Decimal128(38, s)),
            None => self.inner.return_type(args),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(scale) = args.args.first().and_then(|a| exact_scale(&a.data_type())) else {
            return self.inner.invoke_with_args(args);
        };
        let list = args.args[0].to_array(args.number_rows)?;
        let (values, rows) = bounds(&list)?;
        let values = cast(&values, &DataType::Decimal128(38, scale))?;
        let values = values.as_primitive::<arrow::datatypes::Decimal128Type>();
        const LIMIT: i128 = 10i128.pow(38);
        let mut out = arrow::array::Decimal128Builder::with_capacity(rows.len());
        for (i, (start, end)) in rows.into_iter().enumerate() {
            let mut sum: Option<i128> = None;
            for j in (start..end).filter(|&j| values.is_valid(j)) {
                let s = sum.unwrap_or(0).saturating_add(values.value(j));
                if s <= -LIMIT || s >= LIMIT {
                    return exec_err!(
                        "Out of Range Error: list_sum does not fit DECIMAL(38,{scale})"
                    );
                }
                sum = Some(s);
            }
            out.append_option(sum.filter(|_| list.is_valid(i)));
        }
        Ok(ColumnarValue::Array(Arc::new(
            out.finish().with_precision_and_scale(38, scale)?,
        )))
    }
}
