//! `FoldSubstitution` (roadmap 6.6): the owned signed fold, run inside DataFusion-planned
//! statements wherever DataFusion would have aggregated the same shape itself.
//!
//! The rule may refuse where the general path answers, and must never answer differently. So it
//! substitutes only when the owned fold's semantics provably coincide:
//!
//! - `SUM` of `[-]CAST|TRY_CAST(text AS DECIMAL(38,0))` over a `UNION ALL` of projections straight
//!   off nest tables, grouped by columns or `lower`/`upper` of one. Anything else falls through.
//! - `TRY_CAST` runs strict: a value the fold cannot read refuses instead of dropping its row.
//! - A party whose values are all NULL is absent from the fold and NULL-summed in DataFusion, so
//!   the sum must be non-nullable or filtered above by a NULL-rejecting comparison.
//! - The output is checked against 38 digits, which DataFusion's checked sum also enforces.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, RecordBatch, StringArray, StringViewBuilder};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Decimal128Type, DecimalType, SchemaRef};
use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_execution::TaskContext;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::logical_plan::{Aggregate, Extension, Filter, TableScan};
use datafusion_expr::{
    BinaryExpr, Cast, Expr, LogicalPlan, Operator, TryCast, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_session::{ExtensionPlanner, PhysicalPlanner};
use futures::TryStreamExt;

use crate::exec::SignedFoldExec;
use crate::exec::agg::Rows;
use crate::limits::Limits;
use crate::plan::{FoldBranch, FoldValue, KeyCol, KeyFn, KeyPart, Plan, SignedFold};
use crate::segment::SealedSegments;

/// The nest tables the owned fold may read, by the name DataFusion scans them under, and the pool
/// it runs in.
#[derive(Clone)]
pub struct FoldTables {
    pub tables: Arc<HashMap<String, SealedSegments>>,
    pub pool: Arc<rayon::ThreadPool>,
}

impl fmt::Debug for FoldTables {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FoldTables({} tables)", self.tables.len())
    }
}

#[derive(Debug)]
pub struct FoldSubstitution(pub FoldTables);

impl AnalyzerRule for FoldSubstitution {
    fn name(&self) -> &str {
        "fold_substitution"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_down_with_subqueries(|p| {
            let replaced = match &p {
                LogicalPlan::Filter(f) => match f.input.as_ref() {
                    LogicalPlan::Aggregate(a) if null_rejecting(&f.predicate, a) => {
                        let drop_zero = is_nonzero_test(&f.predicate);
                        self.substitute(a, true, drop_zero)?.map(|node| {
                            let input = Arc::new(node);
                            LogicalPlan::Filter(
                                Filter::try_new(f.predicate.clone(), input).expect("same schema"),
                            )
                        })
                    }
                    _ => None,
                },
                LogicalPlan::Aggregate(a) => self.substitute(a, false, false)?,
                _ => None,
            };
            Ok(match replaced {
                Some(p) => Transformed::new(p, true, TreeNodeRecursion::Jump),
                None => Transformed::no(p),
            })
        })
        .map(|t| t.data)
    }
}

/// `sum <op> literal` on the aggregate's one sum: a NULL sum fails it, as an absent party does.
fn null_rejecting(pred: &Expr, a: &Aggregate) -> bool {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = pred else {
        return false;
    };
    let comparison = matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    );
    let literal = |e: &Expr| match e {
        Expr::Literal(v, _) => !v.is_null(),
        Expr::Cast(Cast { expr, .. }) => {
            matches!(expr.as_ref(), Expr::Literal(v, _) if !v.is_null())
        }
        _ => false,
    };
    let sum_field = a.schema.field(a.group_expr.len()).name();
    comparison
        && a.aggr_expr.len() == 1
        && matches!(left.as_ref(), Expr::Column(c) if c.name == *sum_field)
        && literal(right)
}

/// `sum <> 0`: the fold can drop those rows itself, so the filter above selects everything and
/// passes its batch through without a copy.
fn is_nonzero_test(pred: &Expr) -> bool {
    let Expr::BinaryExpr(BinaryExpr {
        op: Operator::NotEq,
        right,
        ..
    }) = pred
    else {
        return false;
    };
    let lit = match right.as_ref() {
        Expr::Cast(Cast { expr, .. }) => expr.as_ref(),
        e => e,
    };
    matches!(
        lit,
        Expr::Literal(
            ScalarValue::Int8(Some(0))
                | ScalarValue::Int16(Some(0))
                | ScalarValue::Int32(Some(0))
                | ScalarValue::Int64(Some(0))
                | ScalarValue::UInt8(Some(0))
                | ScalarValue::UInt16(Some(0))
                | ScalarValue::UInt32(Some(0))
                | ScalarValue::UInt64(Some(0))
                | ScalarValue::Decimal128(Some(0), _, _),
            _
        )
    )
}

fn unalias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => unalias(&a.expr),
        e => e,
    }
}

/// A nest table column: table, name, nullable, type.
type Base = (String, String, bool, DataType);

/// The nest table and column behind output column `idx`, through aliases and plain projections.
fn base_column(plan: &LogicalPlan, idx: usize) -> Option<Base> {
    match plan {
        LogicalPlan::TableScan(TableScan {
            table_name,
            projected_schema,
            filters,
            fetch,
            ..
        }) if filters.is_empty() && fetch.is_none() => {
            let f = projected_schema.field(idx);
            Some((
                table_name.table().to_string(),
                f.name().clone(),
                f.is_nullable(),
                f.data_type().clone(),
            ))
        }
        LogicalPlan::SubqueryAlias(s) => base_column(&s.input, idx),
        LogicalPlan::Projection(p) => match unalias(&p.expr[idx]) {
            Expr::Column(c) => base_column(&p.input, p.input.schema().index_of_column(c).ok()?),
            _ => None,
        },
        _ => None,
    }
}

/// A branch column read by `e`, a column expression over `input`.
fn branch_column(input: &LogicalPlan, e: &Expr) -> Option<Base> {
    let Expr::Column(c) = unalias(e) else {
        return None;
    };
    base_column(input, input.schema().index_of_column(c).ok()?)
}

/// A group key as the fold builds it: literals and columns concatenated, the columns optionally
/// case-folded. `||` and `concat` differ on NULL, which the fold refuses in either.
fn key_parts(
    e: &Expr,
    input: &LogicalPlan,
    out: &mut Vec<KeyPart>,
    same_table: &mut impl FnMut(String) -> bool,
) -> Option<()> {
    match strip_text_casts(e) {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::StringConcat,
            right,
        }) => {
            key_parts(left, input, out, same_table)?;
            key_parts(right, input, out, same_table)
        }
        Expr::ScalarFunction(f) if f.func.name() == "concat" => f
            .args
            .iter()
            .try_for_each(|a| key_parts(a, input, out, same_table)),
        Expr::Literal(
            ScalarValue::Utf8(Some(l))
            | ScalarValue::Utf8View(Some(l))
            | ScalarValue::LargeUtf8(Some(l)),
            _,
        ) => {
            out.push(KeyPart::Literal(l.clone()));
            Some(())
        }
        e => {
            let (e, key_fn) = match strip_text_casts(e) {
                Expr::ScalarFunction(f) if f.args.len() == 1 => match f.func.name() {
                    "lower" => (strip_text_casts(&f.args[0]), Some(KeyFn::Lower)),
                    "upper" => (strip_text_casts(&f.args[0]), Some(KeyFn::Upper)),
                    _ => return None,
                },
                e => (e, None),
            };
            let (t, name, _, ty) = branch_column(input, e)?;
            if !same_table(t)
                || !(is_text(&ty) || ty.is_integer() || super::addresses::is_address(&ty))
            {
                return None;
            }
            out.push(KeyPart::Column { name, key_fn });
            Some(())
        }
    }
}

/// A three-arm `UNION ALL` analyses as `Union(Union(a, b), c)`; arms match by position.
fn flatten_union<'a>(inputs: &'a [Arc<LogicalPlan>], out: &mut Vec<&'a LogicalPlan>) {
    for i in inputs {
        let mut p = i.as_ref();
        while let LogicalPlan::SubqueryAlias(s) = p {
            p = &s.input;
        }
        match p {
            LogicalPlan::Union(u) => flatten_union(&u.inputs, out),
            _ => out.push(i.as_ref()),
        }
    }
}

/// The expression that defines output column `idx` of a union arm, and the plan it reads from:
/// followed down through aliases and column pass-throughs, such as the casting projection union
/// coercion puts over an arm.
fn defining(plan: &LogicalPlan, idx: usize) -> Option<(&Expr, &LogicalPlan)> {
    match plan {
        LogicalPlan::SubqueryAlias(s) => defining(&s.input, idx),
        LogicalPlan::Projection(p) => {
            let e = strip_text_casts(&p.expr[idx]);
            if let Expr::Column(c) = e
                && matches!(
                    p.input.as_ref(),
                    LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_)
                )
                && let Some(d) = defining(&p.input, p.input.schema().index_of_column(c).ok()?)
            {
                return Some(d);
            }
            Some((e, &p.input))
        }
        _ => None,
    }
}

/// Aliases and text-to-text casts change no key's bytes.
fn strip_text_casts(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => strip_text_casts(&a.expr),
        // An address key read as fixed binary is the column the segment holds as text.
        Expr::ScalarFunction(f) if f.name() == "address_bytes" && f.args.len() == 1 => {
            strip_text_casts(&f.args[0])
        }
        Expr::Cast(Cast { expr, field }) if is_text(field.data_type()) => match expr.as_ref() {
            Expr::Column(_)
            | Expr::ScalarFunction(_)
            | Expr::Cast(_)
            | Expr::Alias(_)
            | Expr::Literal(..)
            | Expr::BinaryExpr(_) => strip_text_casts(expr),
            _ => e,
        },
        e => e,
    }
}

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)
}

impl FoldSubstitution {
    fn substitute(
        &self,
        a: &Aggregate,
        filtered: bool,
        drop_zero: bool,
    ) -> Result<Option<LogicalPlan>> {
        let Some(fold) = self.recognise(a, filtered, drop_zero) else {
            return Ok(None);
        };
        let node = OwnedFoldNode {
            fold,
            schema: Arc::clone(&a.schema),
            tables: self.0.clone(),
        };
        Ok(Some(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }

    fn recognise(&self, a: &Aggregate, filtered: bool, drop_zero: bool) -> Option<SignedFold> {
        let [e] = a.aggr_expr.as_slice() else {
            return None;
        };
        let Expr::AggregateFunction(AggregateFunction { func, params }) = unalias(e) else {
            return None;
        };
        if func.name() != "sum"
            || params.distinct
            || params.filter.is_some()
            || !params.order_by.is_empty()
            || a.group_expr.is_empty()
        {
            return None;
        }
        let in_schema = a.input.schema();
        let index = |e: &Expr| match unalias(e) {
            Expr::Column(c) => in_schema.index_of_column(c).ok(),
            _ => None,
        };
        let keys: Vec<usize> = a.group_expr.iter().map(index).collect::<Option<_>>()?;
        let sum = index(&params.args[0])?;

        let mut union = a.input.as_ref();
        while let LogicalPlan::SubqueryAlias(s) = union {
            union = &s.input;
        }
        let LogicalPlan::Union(u) = union else {
            return None;
        };

        let mut branches = Vec::with_capacity(u.inputs.len());
        let mut nullable = false;
        let mut arms = Vec::new();
        flatten_union(&u.inputs, &mut arms);
        for arm in arms {
            let mut table: Option<String> = None;
            let mut same_table = |t: String| match &table {
                Some(x) => *x == t,
                None => {
                    table = Some(t);
                    true
                }
            };

            let mut key = Vec::with_capacity(keys.len());
            for &k in &keys {
                let (e, input) = defining(arm, k)?;
                let mut parts = Vec::new();
                key_parts(e, input, &mut parts, &mut same_table)?;
                key.push(KeyCol { parts });
            }

            let (e, input) = defining(arm, sum)?;
            let (inner, negated) = match e {
                Expr::Negative(x) => (unalias(x), true),
                e => (e, false),
            };
            let (source, target) = match inner {
                Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) => {
                    (expr.as_ref(), field.data_type())
                }
                _ => return None,
            };
            if *target != DataType::Decimal128(38, 0) {
                return None;
            }
            // The owned fold reads the whole i128, as DuckDB's HUGEINT does.
            let source = match source {
                Expr::ScalarFunction(f) if f.func.name() == super::fastcast::HUGEINT_SOURCE => {
                    &f.args[0]
                }
                s => s,
            };
            let (t, col, is_null, ty) = branch_column(input, source)?;
            if !same_table(t.clone()) || !is_text(&ty) || !self.0.tables.contains_key(&t) {
                return None;
            }
            nullable |= is_null;
            branches.push(FoldBranch {
                table: t,
                key,
                values: vec![FoldValue {
                    col,
                    negated,
                    strict_cast: true,
                }],
            });
        }
        if nullable && !filtered {
            return None;
        }
        let names = |range: std::ops::Range<usize>| -> Vec<String> {
            range.map(|i| a.schema.field(i).name().clone()).collect()
        };
        let key_aliases = names(0..keys.len());
        let sum_aliases = names(keys.len()..keys.len() + 1);
        Some(SignedFold {
            branches,
            key_alias: key_aliases[0].clone(),
            key_aliases,
            sum_alias: sum_aliases[0].clone(),
            sum_aliases,
            drop_zero,
        })
    }
}

pub struct OwnedFoldNode {
    fold: SignedFold,
    schema: DFSchemaRef,
    tables: FoldTables,
}

impl fmt::Debug for OwnedFoldNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl OwnedFoldNode {
    fn describe(&self) -> String {
        Plan::SignedFold(self.fold.clone()).describe()
    }
}

impl PartialEq for OwnedFoldNode {
    fn eq(&self, other: &Self) -> bool {
        self.fold == other.fold && self.schema == other.schema
    }
}
impl Eq for OwnedFoldNode {}
impl Hash for OwnedFoldNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.describe().hash(state);
    }
}
impl PartialOrd for OwnedFoldNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.describe().partial_cmp(&other.describe())
    }
}

impl UserDefinedLogicalNodeCore for OwnedFoldNode {
    fn name(&self) -> &str {
        "OwnedSignedFold"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OwnedSignedFold: {}", self.describe())
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            fold: self.fold.clone(),
            schema: Arc::clone(&self.schema),
            tables: self.tables.clone(),
        })
    }
}

#[derive(Debug)]
pub struct OwnedFoldPlanner;

#[async_trait]
impl ExtensionPlanner for OwnedFoldPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &datafusion_expr::physical_planning_context::PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(n) = node.as_any().downcast_ref::<OwnedFoldNode>() else {
            return Ok(None);
        };
        let schema: SchemaRef = Arc::new(n.schema.as_arrow().clone());
        // Rows come out byte-wise ascending on the key, which is DataFusion's string order, so a
        // sort above is redundant. A composite key's bytes are length-prefixed, which is not.
        let eq = if n.fold.key_aliases.len() == 1 {
            let key = PhysicalSortExpr::new(
                Arc::new(PhysicalColumn::new(schema.field(0).name(), 0)),
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            );
            EquivalenceProperties::new_with_orderings(Arc::clone(&schema), [[key]])
        } else {
            EquivalenceProperties::new(Arc::clone(&schema))
        };
        let properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        let metrics = ExecutionPlanMetricsSet::new();
        let bytes = MetricBuilder::new(&metrics).global_counter(super::catalog::SEALED_BYTES);
        Ok(Some(Arc::new(OwnedFoldExec {
            fold: n.fold.clone(),
            schema,
            tables: n.tables.clone(),
            properties,
            metrics,
            bytes,
        })))
    }
}

#[derive(Debug)]
pub struct OwnedFoldExec {
    fold: SignedFold,
    schema: SchemaRef,
    tables: FoldTables,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    bytes: Count,
}

impl DisplayAs for OwnedFoldExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Plan::SignedFold(self.fold.clone()).describe())
    }
}

impl ExecutionPlan for OwnedFoldExec {
    fn name(&self) -> &str {
        "OwnedSignedFoldExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        _partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let bytes = self.bytes.clone();
        let (fold, schema, tables) = (
            self.fold.clone(),
            Arc::clone(&self.schema),
            self.tables.clone(),
        );
        let out = Arc::clone(&schema);
        // One chunk at a time, so what is live is the fold's own rows plus a chunk, not the rows
        // plus a copy of the whole answer.
        let stream = futures::stream::once(async move {
            let rows = tokio::task::spawn_blocking(move || {
                let (rows, read) = fold_rows(&fold, &tables)?;
                bytes.add(read as usize);
                Ok::<_, DataFusionError>((Arc::new(rows), fold))
            })
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))??;
            Ok::<_, DataFusionError>(rows)
        })
        .map_ok(move |(rows, fold)| {
            let n = rows.len();
            let out = Arc::clone(&out);
            futures::stream::iter(
                (0..n)
                    .step_by(CHUNK)
                    .map(move |start| to_batch(&rows, start..(start + CHUNK).min(n), &fold, &out)),
            )
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

const CHUNK: usize = 8192;

/// Runs the fold and checks every sum against 38 digits before a single row is emitted, so a
/// consumer never sees part of an answer followed by a refusal.
fn fold_rows(fold: &SignedFold, tables: &FoldTables) -> Result<(Rows, u64)> {
    // Every value here is read strictly, `TRY_CAST` included, so the owned fold's "the query used
    // CAST" would be wrong about half of them.
    let ext = |e: crate::BurrmillError| {
        let m = e.to_string().replace(
            ". The query used CAST, which errors; TRY_CAST would skip it",
            "; a substituted fold refuses it rather than dropping the row",
        );
        DataFusionError::Execution(m)
    };
    let segments: Vec<&SealedSegments> = fold
        .branches
        .iter()
        .map(|b| &tables.tables[&b.table])
        .collect();
    let (rows, metrics) = tables
        .pool
        .install(|| SignedFoldExec::new(fold, &segments, Limits::default()).run())
        .map_err(ext)?;
    for i in 0..rows.len() {
        let v = rows.sum(i);
        if !Decimal128Type::is_valid_decimal_precision(v, 38) {
            return Err(DataFusionError::Execution(format!(
                "checked_sum overflow: exact result {v} does not fit Decimal128(38, 0)"
            )));
        }
    }
    Ok((rows, metrics.bytes_read))
}

fn to_batch(
    rows: &Rows,
    range: std::ops::Range<usize>,
    fold: &SignedFold,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let n = range.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for k in 0..fold.key_aliases.len() {
        // `key_parts` yields nothing for a single empty key, so a single key reads whole.
        let key = |i: usize| {
            if fold.key_aliases.len() == 1 {
                rows.key(i)
            } else {
                rows.key_parts(i)
                    .nth(k)
                    .expect("key arity matches the plan")
            }
        };
        let want = schema.field(k).data_type();
        columns.push(if *want == DataType::Utf8View {
            let mut b = StringViewBuilder::with_capacity(n);
            range.clone().for_each(|i| b.append_value(key(i)));
            Arc::new(b.finish())
        } else if super::addresses::is_address(want) {
            let mut b =
                arrow::array::FixedSizeBinaryBuilder::with_capacity(n, super::addresses::WIDTH);
            for i in range.clone() {
                match super::addresses::decode(key(i)) {
                    Some(a) => b.append_value(a)?,
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        } else {
            arrow::compute::cast(&StringArray::from_iter_values(range.clone().map(key)), want)?
        });
    }
    let sums = Decimal128Array::from_iter_values(range.map(|i| rows.sum(i)))
        .with_precision_and_scale(38, 0)?;
    let sums: ArrayRef = Arc::new(sums);
    let want = schema.field(fold.key_aliases.len()).data_type();
    columns.push(if sums.data_type() == want {
        sums
    } else {
        arrow::compute::cast_with_options(
            &sums,
            want,
            &arrow::compute::CastOptions {
                safe: false,
                ..Default::default()
            },
        )?
    });
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}
