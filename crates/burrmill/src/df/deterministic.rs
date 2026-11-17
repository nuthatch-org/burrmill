//! Deterministic mode (#340), off by default: the same statement over the same rows answers the same
//! bytes at any thread count and over any split of the rows into segments. Ties are broken by every
//! column, so a row order the statement leaves open is canonical; an aggregate that cannot be made
//! independent of the order rows arrive in is refused by name; and the answer's batches are cut to
//! one size. SUM and AVG over DOUBLE are already exact whatever the partitioning (#77).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, Result, plan_err};
use datafusion_expr::expr::{ScalarFunction, Sort, WindowFunction};
use datafusion_expr::logical_plan::{Limit, Sort as SortPlan};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, Volatility, WindowFrameUnits};
use datafusion_optimizer::AnalyzerRule;

/// Whether an engine's statements are held to deterministic mode, read as each is planned.
pub type Flag = Arc<AtomicBool>;

/// Rows per batch of a deterministic answer: the cut is the rows', not the plan's.
pub const BATCH_ROWS: usize = 8_192;

/// Aggregates whose floating answer depends on the order partial states are merged.
const FLOAT_ORDERED: &[&str] = &[
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "var",
    "var_pop",
    "var_samp",
    "var_population",
    "var_sample",
    "variance",
    "corr",
    "covar",
    "covar_pop",
    "covar_samp",
    "regr_slope",
    "regr_intercept",
    "regr_r2",
    "regr_avgx",
    "regr_avgy",
    "regr_sxx",
    "regr_syy",
    "regr_sxy",
];

/// Sketches whose answer depends on the order values were inserted.
const SKETCHES: &[&str] = &[
    "approx_percentile_cont",
    "approx_percentile_cont_with_weight",
    "approx_median",
];

/// Aggregates whose answer is the order rows arrive in; their arguments break ties.
const ORDERED_AGGREGATES: &[&str] = &[
    "array_agg",
    "string_agg",
    "first_value",
    "last_value",
    "nth_value",
    "burrmill_last_non_null",
];

/// Window functions that read the row order but not the frame.
const ROW_WINDOWS: &[&str] = &["row_number", "lag", "lead", "ntile"];

/// Window functions that read the frame's first, last or nth row.
const VALUE_WINDOWS: &[&str] = &["first_value", "last_value", "nth_value"];

/// What changes from one run to the next whatever the rows.
const CLOCKS: &[&str] = &[
    "now",
    "current_date",
    "current_time",
    "current_timestamp",
    "today",
];

#[derive(Debug)]
pub struct Deterministic(pub Flag);

impl AnalyzerRule for Deterministic {
    fn name(&self) -> &str {
        "deterministic"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        if !self.0.load(Ordering::Relaxed) || !answers_rows(&plan) {
            return Ok(plan);
        }
        let plan = plan
            .transform_up_with_subqueries(|p| {
                let schema = input_schema(&p);
                let t = p.map_expressions(|e| {
                    let name = e.schema_name().to_string();
                    let t = e.transform_up(|e| expr(e, &schema))?;
                    if t.transformed && t.data.schema_name().to_string() != name {
                        Ok(Transformed::yes(t.data.alias(name)))
                    } else {
                        Ok(t)
                    }
                })?;
                let t = t.transform_data(limit_ordered)?;
                if t.transformed {
                    Ok(Transformed::yes(t.data.recompute_schema()?))
                } else {
                    Ok(t)
                }
            })?
            .data;
        ordered_answer(plan)
    }
}

fn answers_rows(plan: &LogicalPlan) -> bool {
    !matches!(
        plan,
        LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
    )
}

fn input_schema(p: &LogicalPlan) -> DFSchema {
    let mut schema = DFSchema::empty();
    for i in p.inputs() {
        schema.merge(i.schema());
    }
    schema
}

/// Every column of `schema` as a sort key, ascending with NULLs first, past those already in
/// `keys`.
fn ties_broken(mut keys: Vec<Sort>, schema: &DFSchema) -> Vec<Sort> {
    for (q, f) in schema.iter() {
        let c = Expr::Column(Column::from((q, f.as_ref())));
        if !keys.iter().any(|k| k.expr == c) {
            keys.push(Sort::new(c, true, true));
        }
    }
    keys
}

fn expr(e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    match e {
        Expr::AggregateFunction(mut a) => {
            let name = a.func.name().to_string();
            let float_in = || {
                a.params
                    .args
                    .iter()
                    .any(|x| x.get_type(schema).is_ok_and(|t| t.is_floating()))
            };
            if FLOAT_ORDERED.contains(&name.as_str()) && float_in() {
                return plan_err!(
                    "deterministic mode: {name} over a float depends on the order rows are \
                     merged; refused"
                );
            }
            if SKETCHES.contains(&name.as_str()) {
                return plan_err!(
                    "deterministic mode: {name} depends on the order values arrive; \
                     quantile_cont or median is exact"
                );
            }
            if !ORDERED_AGGREGATES.contains(&name.as_str()) {
                return Ok(Transformed::no(Expr::AggregateFunction(a)));
            }
            let args: Vec<Expr> = a
                .params
                .args
                .iter()
                .filter(|x| !matches!(x, Expr::Literal(..)))
                .cloned()
                .collect();
            let mut keys = std::mem::take(&mut a.params.order_by);
            let before = keys.len();
            for x in args {
                if !keys.iter().any(|k| k.expr == x) {
                    keys.push(Sort::new(x, true, true));
                }
            }
            let changed = keys.len() != before;
            a.params.order_by = keys;
            Ok(if changed {
                Transformed::yes(Expr::AggregateFunction(a))
            } else {
                Transformed::no(Expr::AggregateFunction(a))
            })
        }
        Expr::WindowFunction(w) => window(*w, schema),
        Expr::ScalarFunction(ScalarFunction { ref func, .. }) => {
            // `error` is volatile so nothing folds it away, and it fails the same way every time.
            let volatile =
                func.signature().volatility == Volatility::Volatile && func.name() != "error";
            if volatile || CLOCKS.contains(&func.name()) {
                return plan_err!(
                    "deterministic mode: {} answers differently each time it runs",
                    func.name()
                );
            }
            Ok(Transformed::no(e))
        }
        e => Ok(Transformed::no(e)),
    }
}

fn window(mut w: WindowFunction, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let name = w.fun.name().to_string();
    let rows = w.params.window_frame.units == WindowFrameUnits::Rows;
    if let datafusion_expr::WindowFunctionDefinition::AggregateUDF(f) = &w.fun {
        let float_in = w
            .params
            .args
            .iter()
            .any(|x| x.get_type(schema).is_ok_and(|t| t.is_floating()));
        if (FLOAT_ORDERED.contains(&f.name()) && float_in) || SKETCHES.contains(&f.name()) {
            return plan_err!("deterministic mode: {name} depends on the order rows arrive");
        }
    }
    let by_rows = ROW_WINDOWS.contains(&name.as_str());
    let by_value = VALUE_WINDOWS.contains(&name.as_str());
    if by_value && !rows {
        return plan_err!(
            "deterministic mode: {name} over a peer frame picks among tied rows; write a ROWS \
             frame"
        );
    }
    // A ROWS frame counts rows, so which tied row is in it depends on their order; RANGE and
    // GROUPS frames take whole peer groups and do not.
    if !(by_rows || rows) {
        return Ok(Transformed::no(Expr::WindowFunction(Box::new(w))));
    }
    let before = w.params.order_by.len();
    w.params.order_by = ties_broken(std::mem::take(&mut w.params.order_by), schema);
    let changed = w.params.order_by.len() != before;
    let e = Expr::WindowFunction(Box::new(w));
    Ok(if changed {
        Transformed::yes(e)
    } else {
        Transformed::no(e)
    })
}

/// A LIMIT keeps the rows a total order puts first: its sort's ties broken by every column, or
/// a sort of every column where it has none.
fn limit_ordered(p: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Limit(Limit { skip, fetch, input }) = p else {
        return Ok(Transformed::no(p));
    };
    let input = Arc::unwrap_or_clone(input);
    let (input, changed) = totally_ordered(input)?;
    let p = LogicalPlan::Limit(Limit {
        skip,
        fetch,
        input: Arc::new(input),
    });
    Ok(if changed {
        Transformed::yes(p)
    } else {
        Transformed::no(p)
    })
}

/// `p` sorted by every column: its own sort's keys first, where it is a sort.
fn totally_ordered(p: LogicalPlan) -> Result<(LogicalPlan, bool)> {
    match p {
        LogicalPlan::Sort(s) => {
            let before = s.expr.len();
            let expr = ties_broken(s.expr, s.input.schema());
            let changed = expr.len() != before;
            Ok((
                LogicalPlan::Sort(SortPlan {
                    expr,
                    input: s.input,
                    fetch: s.fetch,
                }),
                changed,
            ))
        }
        other => {
            let expr = ties_broken(Vec::new(), other.schema());
            if expr.is_empty() {
                return Ok((other, false));
            }
            Ok((
                LogicalPlan::Sort(SortPlan {
                    expr,
                    input: Arc::new(other),
                    fetch: None,
                }),
                true,
            ))
        }
    }
}

/// The answer in a total order: the outermost sort, below any projection over it, with its ties
/// broken, or a sort of every output column on top.
fn ordered_answer(plan: LogicalPlan) -> Result<LogicalPlan> {
    fn reaches_sort(p: &LogicalPlan) -> bool {
        match p {
            LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => true,
            LogicalPlan::Projection(x) => reaches_sort(&x.input),
            LogicalPlan::SubqueryAlias(x) => reaches_sort(&x.input),
            _ => false,
        }
    }
    if !reaches_sort(&plan) {
        return Ok(totally_ordered(plan)?.0);
    }
    fn down(p: LogicalPlan) -> Result<LogicalPlan> {
        Ok(match p {
            // A LIMIT was ordered on the way up.
            LogicalPlan::Limit(_) => p,
            LogicalPlan::Sort(_) => totally_ordered(p)?.0,
            LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
                let p = p.map_children(|c| Ok(Transformed::yes(down(c)?)))?.data;
                p.recompute_schema()?
            }
            p => p,
        })
    }
    down(plan)
}

/// `batches` as a deterministic answer is handed over: concatenated, cut every [`BATCH_ROWS`]
/// rows, and each view column's buffers rewritten so its bytes follow from its values alone.
pub fn canonical(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    let all = arrow::compute::concat_batches(schema, batches)?;
    let starts = (0..all.num_rows().max(1)).step_by(BATCH_ROWS);
    starts
        .map(|at| {
            let n = BATCH_ROWS.min(all.num_rows() - at);
            let cols = all
                .slice(at, n)
                .columns()
                .iter()
                .map(compact)
                .collect::<Result<Vec<_>>>()?;
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(schema),
                cols,
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n)),
            )?)
        })
        .collect()
}

/// An array whose bytes follow from its values alone. What sits under a NULL is whatever the plan
/// left there, a view array's buffers are however they were filled, and -0.0 and each NaN equal
/// their canonical twin in SQL, so which of them a tie kept is the plan's choice. Nested children
/// likewise; the IPC writer already trims any other array to the rows of its slice.
fn compact(a: &ArrayRef) -> Result<ArrayRef> {
    use arrow::array::{GenericListArray, OffsetSizeTrait, StructArray};
    fn list<O: OffsetSizeTrait>(a: &ArrayRef) -> Result<ArrayRef> {
        let l = a.as_list::<O>();
        let (field, offsets, values, nulls) = l.clone().into_parts();
        let first = offsets[0].as_usize();
        let last = offsets[offsets.len() - 1].as_usize();
        let rebased = arrow::buffer::OffsetBuffer::new(
            offsets
                .iter()
                .map(|o| O::usize_as(o.as_usize() - first))
                .collect(),
        );
        let values = compact(&values.slice(first, last - first))?;
        Ok(Arc::new(GenericListArray::<O>::try_new(
            field, rebased, values, nulls,
        )?))
    }
    let a = match a.logical_null_count() {
        0 if a.nulls().is_some() => {
            arrow::array::make_array(a.to_data().into_builder().nulls(None).build()?)
        }
        0 => Arc::clone(a),
        _ => {
            let valid = arrow::compute::is_not_null(a)?;
            let blank = arrow::array::new_null_array(a.data_type(), a.len());
            arrow::compute::kernels::zip::zip(&valid, a, &blank)?
        }
    };
    Ok(match a.data_type() {
        DataType::Utf8View => Arc::new(a.as_string_view().gc()),
        DataType::BinaryView => Arc::new(a.as_binary_view().gc()),
        DataType::Float64 => Arc::new(
            a.as_primitive::<arrow::datatypes::Float64Type>()
                .unary::<_, arrow::datatypes::Float64Type>(|v| {
                    if v == 0.0 {
                        0.0
                    } else if v.is_nan() {
                        f64::NAN
                    } else {
                        v
                    }
                }),
        ),
        DataType::Float32 => Arc::new(
            a.as_primitive::<arrow::datatypes::Float32Type>()
                .unary::<_, arrow::datatypes::Float32Type>(|v| {
                    if v == 0.0 {
                        0.0
                    } else if v.is_nan() {
                        f32::NAN
                    } else {
                        v
                    }
                }),
        ),
        DataType::List(_) => list::<i32>(&a)?,
        DataType::LargeList(_) => list::<i64>(&a)?,
        DataType::Struct(_) => {
            let s = a.as_struct();
            let cols = s
                .columns()
                .iter()
                .map(compact)
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new(
                s.fields().clone(),
                cols,
                s.nulls().cloned(),
            )?)
        }
        _ => a,
    })
}
