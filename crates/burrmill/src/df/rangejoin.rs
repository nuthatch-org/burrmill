//! `RangeJoin`: a join on `a >= b AND c <= d`, done by sorting instead of by nested loops.
//!
//! DataFusion 55 has no range join, so `JOIN epochs e ON x >= e.start AND x <= e.end` compares every
//! row with every row. On graph-allocations that made `lodestar_epochs` 1.9x DuckDB, four such joins
//! at 0.6 s of CPU each. This takes the `NestedLoopJoinExec` as planned and keeps what it does: the
//! left side collected once, the right side streamed by partition, the same output columns.
//!
//! Any two-sided pair of bounds normalises to `L_a >= R_a AND L_b <= R_b` (either may be strict).
//! The left rows are sorted by `L_a`, so each right row's candidates are a suffix found by binary
//! search, scanned in order until a suffix minimum of `L_b` shows nothing further can match. Events
//! against epochs, either way round, are then one search and one short scan per row.
//!
//! Taken only for an inner or right join (the right side is the one whose unmatched rows are kept),
//! plain columns in the filter, and one type per comparison that is not a float. NULL matches
//! nothing, as in the comparison it replaces.

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array, make_comparator,
};
use arrow::compute::{SortOptions, concat_batches, sort_to_indices, take};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, JoinSide, JoinType, Result};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use datafusion_physical_plan::joins::NestedLoopJoinExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_session::PhysicalOptimizerRule;

use super::sequential::execute_stream;
use futures::StreamExt;

#[derive(Debug, Default)]
pub struct RangeJoin;

impl PhysicalOptimizerRule for RangeJoin {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let Some(j) = p.downcast_ref::<NestedLoopJoinExec>() else {
                return Ok(Transformed::no(p));
            };
            Ok(match RangeJoinExec::try_from_nested(j)? {
                Some(r) => Transformed::yes(Arc::new(r) as Arc<dyn ExecutionPlan>),
                None => Transformed::no(p),
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "range_join"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// One side of a comparison: a column of the left or the right input.
fn side_column(e: &Arc<dyn PhysicalExpr>, j: &NestedLoopJoinExec) -> Option<(JoinSide, usize)> {
    let c = e.downcast_ref::<Column>()?;
    let i = j.filter()?.column_indices().get(c.index())?;
    Some((i.side, i.index))
}

/// `left op right` for one conjunct, turned round if it was written the other way.
fn normalise(
    e: &Arc<dyn PhysicalExpr>,
    j: &NestedLoopJoinExec,
) -> Option<(usize, Operator, usize)> {
    let b = e.downcast_ref::<BinaryExpr>()?;
    let (l, r) = (side_column(b.left(), j)?, side_column(b.right(), j)?);
    let op = *b.op();
    if !matches!(
        op,
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
    ) {
        return None;
    }
    match (l, r) {
        ((JoinSide::Left, a), (JoinSide::Right, b)) => Some((a, op, b)),
        ((JoinSide::Right, a), (JoinSide::Left, b)) => Some((b, op.swap()?, a)),
        _ => None,
    }
}

fn comparable(t: &DataType) -> bool {
    t.is_integer()
        || t.is_temporal()
        || matches!(
            t,
            DataType::Decimal128(..)
                | DataType::Decimal256(..)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
        )
}

/// `L_a >= R_a` (strict if `a_strict`) and `L_b <= R_b` (strict if `b_strict`).
#[derive(Debug, Clone, Copy)]
struct Bounds {
    la: usize,
    ra: usize,
    a_strict: bool,
    lb: usize,
    rb: usize,
    b_strict: bool,
}

#[derive(Debug)]
pub struct RangeJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    bounds: Bounds,
    join_type: JoinType,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    filter_text: String,
    built: Arc<tokio::sync::OnceCell<Arc<Built>>>,
    properties: Arc<PlanProperties>,
}

impl RangeJoinExec {
    fn try_from_nested(j: &NestedLoopJoinExec) -> Result<Option<Self>> {
        if !matches!(j.join_type(), JoinType::Inner | JoinType::Right) {
            return Ok(None);
        }
        let Some(filter) = j.filter() else {
            return Ok(None);
        };
        let Some(and) = filter.expression().downcast_ref::<BinaryExpr>() else {
            return Ok(None);
        };
        if *and.op() != Operator::And {
            return Ok(None);
        }
        let (Some(x), Some(y)) = (normalise(and.left(), j), normalise(and.right(), j)) else {
            return Ok(None);
        };
        let lower =
            |(_, op, _): &(usize, Operator, usize)| matches!(op, Operator::Gt | Operator::GtEq);
        let (a, b) = match (lower(&x), lower(&y)) {
            (true, false) => (x, y),
            (false, true) => (y, x),
            _ => return Ok(None),
        };
        let (ls, rs) = (j.left().schema(), j.right().schema());
        for (l, r) in [(a.0, a.2), (b.0, b.2)] {
            let t = ls.field(l).data_type();
            if t != rs.field(r).data_type() || !comparable(t) {
                return Ok(None);
            }
        }
        let bounds = Bounds {
            la: a.0,
            ra: a.2,
            a_strict: a.1 == Operator::Gt,
            lb: b.0,
            rb: b.2,
            b_strict: b.1 == Operator::Lt,
        };
        let projection = j.projection().as_ref().map(|p| p.to_vec());
        Ok(Some(Self {
            left: Arc::clone(j.left()),
            right: Arc::clone(j.right()),
            bounds,
            join_type: *j.join_type(),
            projection,
            schema: j.schema(),
            filter_text: filter.expression().to_string(),
            built: Arc::new(tokio::sync::OnceCell::new()),
            // What the nested loop promised downstream, ordering included: rows still leave in the
            // right side's order.
            properties: Arc::clone(j.properties()),
        }))
    }
}

/// The left side, sorted by `L_a` with its NULL bounds left out.
#[derive(Debug)]
struct Built {
    batch: RecordBatch,
    /// Rows of `batch` in `L_a` order.
    order: UInt32Array,
    a: ArrayRef,
    b: ArrayRef,
    /// `suffix_min[i]`: the position in `order` of the least `L_b` at or after `i`.
    suffix_min: Vec<u32>,
    /// All of the above, in the pool, as a hash join's build is.
    _reservation: MemoryReservation,
}

async fn build(
    left: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
    bounds: Bounds,
) -> Result<Built> {
    let reservation = MemoryConsumer::new("RangeJoinExec build").register(ctx.memory_pool());
    let schema = left.schema();
    let mut batches = Vec::new();
    let mut input = execute_stream(left, ctx)?;
    while let Some(b) = input.next().await {
        let b = b?;
        reservation.try_grow(b.get_array_memory_size())?;
        batches.push(b);
    }
    let batch = concat_batches(&schema, &batches)?;
    reservation.try_grow(batch.get_array_memory_size())?;
    drop(batches);
    let (a, b) = (batch.column(bounds.la), batch.column(bounds.lb));
    let sorted = sort_to_indices(
        a,
        Some(SortOptions {
            descending: false,
            nulls_first: true,
        }),
        None,
    )?;
    let order: UInt32Array = sorted
        .values()
        .iter()
        .copied()
        .filter(|&i| a.is_valid(i as usize) && b.is_valid(i as usize))
        .collect();
    let (a, b) = (take(a, &order, None)?, take(b, &order, None)?);
    let cmp = make_comparator(b.as_ref(), b.as_ref(), SortOptions::default())?;
    let mut suffix_min = vec![0u32; order.len()];
    for i in (0..order.len()).rev() {
        suffix_min[i] = match suffix_min.get(i + 1) {
            Some(&m) if cmp(m as usize, i) == Ordering::Less => m,
            _ => i as u32,
        };
    }
    // What is kept: the concatenated side, the sorted bounds and the order over them.
    reservation.try_resize(
        batch.get_array_memory_size()
            + a.get_array_memory_size()
            + b.get_array_memory_size()
            + order.get_array_memory_size()
            + suffix_min.len() * 4,
    )?;
    Ok(Built {
        batch,
        order,
        a,
        b,
        suffix_min,
        _reservation: reservation,
    })
}

/// Matches past this many in one call, or candidates examined past `STEPS`, and a probe stops to
/// hand them on and look at the token: one wide right row can match the whole left side.
const STEPS: usize = 1 << 16;

/// Where a probe of one right batch stopped.
#[derive(Default)]
struct Cursor {
    r: usize,
    /// The candidate reached in row `r`, once its search is done.
    i: Option<usize>,
    /// Whether row `r` has matched, for a join that keeps unmatched right rows.
    matched: bool,
}

/// Left and right row indices for the matches of `right`'s rows from `cur` on, and a NULL left for
/// each right row with none when the join keeps them; at most `max_out` of them, and `true` once the
/// batch is done.
fn probe(
    built: &Built,
    right: &RecordBatch,
    bounds: Bounds,
    keep: bool,
    cur: &mut Cursor,
    max_out: usize,
) -> Result<(UInt32Array, UInt32Array, bool)> {
    let (ra, rb) = (right.column(bounds.ra), right.column(bounds.rb));
    let ca = make_comparator(ra.as_ref(), built.a.as_ref(), SortOptions::default())?;
    let cb = make_comparator(rb.as_ref(), built.b.as_ref(), SortOptions::default())?;
    // `L_a >= R_a`: the right value orders at or below the left one.
    let a_holds = |r: usize, i: usize| match ca(r, i) {
        Ordering::Less => true,
        Ordering::Equal => !bounds.a_strict,
        Ordering::Greater => false,
    };
    // `L_b <= R_b`: the right value orders at or above the left one.
    let b_holds = |r: usize, i: usize| match cb(r, i) {
        Ordering::Greater => true,
        Ordering::Equal => !bounds.b_strict,
        Ordering::Less => false,
    };
    let n = built.order.len();
    let (mut left, mut right_idx) = (Vec::new(), Vec::new());
    let mut steps = 0;
    while cur.r < right.num_rows() {
        let r = cur.r;
        if ra.is_valid(r) && rb.is_valid(r) {
            // First left row where `L_a >= R_a` holds; it holds from there on, `L_a` being sorted.
            let mut i = cur.i.unwrap_or_else(|| {
                let (mut lo, mut hi) = (0, n);
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    if a_holds(r, mid) {
                        hi = mid
                    } else {
                        lo = mid + 1
                    }
                }
                lo
            });
            while i < n && b_holds(r, built.suffix_min[i] as usize) {
                if left.len() >= max_out || steps >= STEPS {
                    cur.i = Some(i);
                    return Ok((UInt32Array::from(left), UInt32Array::from(right_idx), false));
                }
                steps += 1;
                if b_holds(r, i) {
                    left.push(Some(built.order.value(i)));
                    right_idx.push(r as u32);
                    cur.matched = true;
                }
                i += 1;
            }
        }
        if keep && !cur.matched {
            left.push(None);
            right_idx.push(r as u32);
        }
        *cur = Cursor {
            r: r + 1,
            ..Cursor::default()
        };
    }
    Ok((UInt32Array::from(left), UInt32Array::from(right_idx), true))
}

fn assemble(
    built: &Built,
    right: &RecordBatch,
    left_idx: &UInt32Array,
    right_idx: &UInt32Array,
    projection: Option<&[usize]>,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(built.batch.num_columns() + right.num_columns());
    for c in built.batch.columns() {
        columns.push(take(c, left_idx, None)?);
    }
    for c in right.columns() {
        columns.push(take(c, right_idx, None)?);
    }
    if let Some(p) = projection {
        columns = p.iter().map(|&i| Arc::clone(&columns[i])).collect();
    }
    // `count(*)` projects no column, and a batch of none needs its row count said.
    let options = RecordBatchOptions::new().with_row_count(Some(left_idx.len()));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &options,
    )?)
}

impl DisplayAs for RangeJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RangeJoinExec: join_type={}, filter={}",
            self.join_type, self.filter_text
        )
    }
}

impl ExecutionPlan for RangeJoinExec {
    fn name(&self) -> &str {
        "RangeJoinExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let right = children.pop().expect("two children");
        let left = children.pop().expect("two children");
        let right_partitioning = right.properties().output_partitioning().clone();
        Ok(Arc::new(Self {
            left,
            right,
            bounds: self.bounds,
            join_type: self.join_type,
            projection: self.projection.clone(),
            schema: Arc::clone(&self.schema),
            filter_text: self.filter_text.clone(),
            built: Arc::new(tokio::sync::OnceCell::new()),
            // The right side's partitions are this operator's; a rule below may have changed them.
            properties: Arc::new(
                self.properties
                    .as_ref()
                    .clone()
                    .with_partitioning(right_partitioning),
            ),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (left, built) = (Arc::clone(&self.left), Arc::clone(&self.built));
        let (bounds, keep) = (self.bounds, self.join_type == JoinType::Right);
        let (projection, schema) = (self.projection.clone(), Arc::clone(&self.schema));
        let chunk = ctx.session_config().batch_size().max(1);
        let token = ctx
            .session_config()
            .get_extension::<crate::CancelToken>()
            .unwrap_or_default();
        let mut right = Some(self.right.execute(partition, Arc::clone(&ctx))?);
        let stream = futures::stream::once(async move {
            built
                .get_or_try_init(|| async move { build(left, ctx, bounds).await.map(Arc::new) })
                .await
                .cloned()
        })
        .flat_map(move |b| {
            let (projection, schema, token) =
                (projection.clone(), Arc::clone(&schema), Arc::clone(&token));
            match b {
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
                Ok(built) => right
                    .take()
                    .expect("built once")
                    .flat_map(move |r| {
                        let (built, projection, schema, token) = (
                            Arc::clone(&built),
                            projection.clone(),
                            Arc::clone(&schema),
                            Arc::clone(&token),
                        );
                        let r = match r {
                            Ok(r) => r,
                            Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
                        };
                        // One output batch per step, so neither a wide batch's matches nor its time
                        // are held in one piece.
                        futures::stream::unfold(Some(Cursor::default()), move |cur| {
                            let (built, r, projection, schema, token) = (
                                Arc::clone(&built),
                                r.clone(),
                                projection.clone(),
                                Arc::clone(&schema),
                                Arc::clone(&token),
                            );
                            async move {
                                let mut cur = cur?;
                                loop {
                                    if token.is_cancelled() {
                                        return Some((
                                            Err(DataFusionError::Execution("cancelled".into())),
                                            None,
                                        ));
                                    }
                                    let (li, ri, done) =
                                        match probe(&built, &r, bounds, keep, &mut cur, chunk) {
                                            Ok(p) => p,
                                            Err(e) => return Some((Err(e), None)),
                                        };
                                    if !li.is_empty() {
                                        let out = assemble(
                                            &built,
                                            &r,
                                            &li,
                                            &ri,
                                            projection.as_deref(),
                                            &schema,
                                        );
                                        return Some((out, (!done).then_some(cur)));
                                    }
                                    if done {
                                        return None;
                                    }
                                }
                            }
                        })
                        .boxed()
                    })
                    .boxed(),
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}
