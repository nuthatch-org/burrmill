//! A window over its whole input charges the pool for what it holds, and is not handed a cross
//! join's output one row at a time (#47).
//!
//! DataFusion's `WindowAggExec` keeps every input batch and concatenates them before it computes,
//! without a reservation, so nothing bounds it under a budget. A cross join collects its left input
//! and emits one batch per left row and right batch, which with a one-row right side is one batch
//! per row, each of whose strings is copied into a fresh 8 KiB block: `indexer.delegators_page` held
//! 1.8 GB in a window that way under a 1 GB pool.
//!
//! A sort that spills merges one run per batch it holds, and each run's merge cursor holds the
//! whole of a batch no larger than an output batch. At a budget's 128-row batches the cursors were
//! as large as the input, and the merge took the pool before writing anything (#56). The cursors
//! are the input over the ratio of a run to an output batch, so the runs grow with the batch.

use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::{BatchCoalescer, concat_batches};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::joins::{CrossJoinExec, HashJoinExec};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::windows::WindowAggExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, replace_children_if_necessary,
};
use datafusion_session::PhysicalOptimizerRule;
use futures::StreamExt;

/// A `WindowAggExec` whose input is charged to a reservation that lives as long as its output.
#[derive(Debug)]
pub(super) struct ChargedWindowExec {
    window: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for ChargedWindowExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ChargedWindowExec")
    }
}

impl ExecutionPlan for ChargedWindowExec {
    fn name(&self) -> &str {
        "ChargedWindowExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.window.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.window]
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
        Ok(Arc::new(Self {
            window: children.pop().expect("one child"),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The window drops its input stream before it concatenates, so the charge is held here.
        let reservation = Arc::new(
            MemoryConsumer::new(format!("WindowAggExec[{partition}]")).register(ctx.memory_pool()),
        );
        let input = Arc::new(ChargeExec {
            inner: Arc::clone(self.window.children()[0]),
            reservation: Arc::clone(&reservation),
        });
        let window = replace_children_if_necessary(Arc::clone(&self.window), vec![input])?;
        let size = ctx.session_config().batch_size().max(1);
        let stream = window.execute(partition, ctx)?;
        let schema = stream.schema();
        // The window emits its whole input as one batch; sliced, what sits above it works a batch
        // at a time, as it does above any other operator (nuthatch #1899).
        let held = stream.flat_map(move |item| {
            let _ = &reservation;
            let slices: Vec<Result<RecordBatch>> = match item {
                Ok(b) => (0..b.num_rows())
                    .step_by(size)
                    .map(|at| Ok(b.slice(at, size.min(b.num_rows() - at))))
                    .collect(),
                Err(e) => vec![Err(e)],
            };
            futures::stream::iter(slices)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, held)))
    }
}

/// Charges each batch that passes twice: once as the window keeps it, once as it is concatenated.
#[derive(Debug)]
struct ChargeExec {
    inner: Arc<dyn ExecutionPlan>,
    reservation: Arc<MemoryReservation>,
}

impl DisplayAs for ChargeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ChargeExec")
    }
}

impl ExecutionPlan for ChargeExec {
    fn name(&self) -> &str {
        "ChargeExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
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
        Ok(Arc::new(Self {
            inner: children.pop().expect("one child"),
            reservation: Arc::clone(&self.reservation),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, ctx)?;
        let schema = stream.schema();
        let reservation = Arc::clone(&self.reservation);
        let charged = stream.map(move |item| {
            item.and_then(|batch| {
                reservation.try_grow(2 * batch.get_array_memory_size())?;
                Ok(batch)
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, charged)))
    }
}

/// A cross join's output gathered into batches of the session's batch size.
#[derive(Debug)]
pub(super) struct CoalesceExec {
    inner: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for CoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CoalesceExec")
    }
}

impl ExecutionPlan for CoalesceExec {
    fn name(&self) -> &str {
        "CoalesceExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
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
        Ok(Arc::new(Self {
            inner: children.pop().expect("one child"),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let size = ctx.session_config().batch_size();
        let stream = self.inner.execute(partition, ctx)?;
        let schema = stream.schema();
        let coalescer = BatchCoalescer::new(Arc::clone(&schema), size);
        let out = futures::stream::unfold(
            (stream, coalescer, false),
            |(mut stream, mut c, mut done)| async move {
                loop {
                    if let Some(batch) = c.next_completed_batch() {
                        return Some((Ok(batch), (stream, c, done)));
                    }
                    if done {
                        return None;
                    }
                    let pushed = match stream.next().await {
                        Some(Ok(batch)) => c.push_batch(batch),
                        Some(Err(e)) => return Some((Err(e), (stream, c, true))),
                        None => {
                            done = true;
                            c.finish_buffered_batch()
                        }
                    };
                    if let Err(e) = pushed {
                        return Some((Err(DataFusionError::from(e)), (stream, c, true)));
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// Rows and bytes a [`GatherExec`] gathers a sort's input up to: 32 of a budget's 2,048-row
/// batches, which a 3,000,000-row sort spills under a 32 MB budget with room to halve.
const GATHER_ROWS: usize = 65536;
const GATHER_BYTES: usize = 8 << 20;

/// A sort's input gathered into batches of up to [`GATHER_ROWS`] rows or [`GATHER_BYTES`] bytes, so
/// its spill merges a few hundred runs rather than one per scan batch.
///
/// A hash join's build side too, with its view columns copied into one buffer per gathered batch
/// (`compact`). Built from 128-row batches, the join's table kept a buffer for each, and every batch
/// it put out referenced all of them, which DataFusion walks per batch for its `output_bytes`
/// metric: 0.8 s of BetSwirl's 1.9 s `bets` statement, against 160,802 rows (#1951).
#[derive(Debug)]
pub(super) struct GatherExec {
    inner: Arc<dyn ExecutionPlan>,
    compact: bool,
}

impl DisplayAs for GatherExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match self.compact {
            true => write!(f, "GatherExec: compacted"),
            false => write!(f, "GatherExec"),
        }
    }
}

impl ExecutionPlan for GatherExec {
    fn name(&self) -> &str {
        "GatherExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
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
        Ok(Arc::new(Self {
            inner: children.pop().expect("one child"),
            compact: self.compact,
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, ctx)?;
        let schema = stream.schema();
        let held = Gathered {
            compact: self.compact,
            ..Gathered::default()
        };
        let out = futures::stream::unfold(
            (stream, held, false),
            |(mut stream, mut held, done)| async move {
                if done {
                    return None;
                }
                loop {
                    let (gathered, done) = match stream.next().await {
                        Some(Ok(batch)) => match held.push(batch) {
                            Ok(None) => continue,
                            gathered => (gathered, false),
                        },
                        Some(Err(e)) => (Err(e), true),
                        None => (held.take(), true),
                    };
                    return match gathered {
                        Ok(Some(batch)) => Some((Ok(batch), (stream, held, done))),
                        Ok(None) => None,
                        Err(e) => Some((Err(e), (stream, held, true))),
                    };
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

#[derive(Default)]
struct Gathered {
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
    compact: bool,
}

impl Gathered {
    /// The batches held with `batch`, once they reach the bound.
    fn push(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        self.rows += batch.num_rows();
        self.bytes += held_bytes(&batch, self.compact)?;
        self.batches.push(batch);
        match self.rows >= GATHER_ROWS || self.bytes >= GATHER_BYTES {
            true => self.take(),
            false => Ok(None),
        }
    }

    fn take(&mut self) -> Result<Option<RecordBatch>> {
        let batches = std::mem::take(&mut self.batches);
        (self.rows, self.bytes) = (0, 0);
        let gathered = match batches.as_slice() {
            [] => return Ok(None),
            [one] => one.clone(),
            [first, ..] => concat_batches(&first.schema(), &batches)?,
        };
        if !self.compact {
            return Ok(Some(gathered));
        }
        let columns = gathered
            .columns()
            .iter()
            .map(|c| match c.data_type() {
                DataType::Utf8View => Arc::new(c.as_string_view().gc()) as ArrayRef,
                DataType::BinaryView => Arc::new(c.as_binary_view().gc()),
                _ => Arc::clone(c),
            })
            .collect();
        Ok(Some(RecordBatch::try_new(gathered.schema(), columns)?))
    }
}

/// What `batch`'s rows hold. A view column counts every buffer it keeps, which overstates it and only
/// ever passes a batch on sooner; once `compacted`, the bytes its rows will keep.
fn held_bytes(batch: &RecordBatch, compacted: bool) -> Result<usize> {
    batch.columns().iter().try_fold(0, |n, c| {
        Ok(n + match c.data_type() {
            DataType::Utf8View if compacted => {
                let v = c.as_string_view();
                v.views().inner().len() + v.total_buffer_bytes_used()
            }
            DataType::BinaryView if compacted => {
                let v = c.as_binary_view();
                v.views().inner().len() + v.total_buffer_bytes_used()
            }
            DataType::Utf8View | DataType::BinaryView => c.get_array_memory_size(),
            _ => c.to_data().get_slice_memory_size()?,
        })
    })
}

/// Puts [`ChargedWindowExec`] over every `WindowAggExec`, [`CoalesceExec`] over every cross join and
/// [`GatherExec`] under every sort without a fetch and every hash join's build side.
#[derive(Debug)]
pub(super) struct HeldInput;

impl PhysicalOptimizerRule for HeldInput {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            if p.downcast_ref::<WindowAggExec>().is_some() {
                return Ok(Transformed::yes(
                    Arc::new(ChargedWindowExec { window: p }) as Arc<dyn ExecutionPlan>
                ));
            }
            if let Some(j) = p.downcast_ref::<HashJoinExec>() {
                let left = Arc::new(GatherExec {
                    inner: Arc::clone(j.left()),
                    compact: true,
                }) as Arc<dyn ExecutionPlan>;
                let right = Arc::clone(j.right());
                return replace_children_if_necessary(p, vec![left, right]).map(Transformed::yes);
            }
            if p.downcast_ref::<CrossJoinExec>().is_some() {
                return Ok(Transformed::yes(
                    Arc::new(CoalesceExec { inner: p }) as Arc<dyn ExecutionPlan>
                ));
            }
            if let Some(s) = p.downcast_ref::<SortExec>()
                && s.fetch().is_none()
            {
                let input = Arc::new(GatherExec {
                    inner: Arc::clone(s.input()),
                    compact: false,
                }) as Arc<dyn ExecutionPlan>;
                return replace_children_if_necessary(p, vec![input]).map(Transformed::yes);
            }
            Ok(Transformed::no(p))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "held_input"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, AsArray, Int64Array, StringArray, StringViewArray};
    use arrow::datatypes::{Field, Int64Type, Schema};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_execution::config::SessionConfig;

    use super::*;

    fn gathered(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
        gather(batches, false)
    }

    fn gather(batches: Vec<RecordBatch>, compact: bool) -> Vec<RecordBatch> {
        let schema = batches[0].schema();
        let source = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
        let ctx = TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(1 << 20));
        let out = GatherExec {
            inner: source,
            compact,
        }
        .execute(0, Arc::new(ctx))
        .unwrap();
        futures::executor::block_on(datafusion_physical_plan::common::collect(out)).unwrap()
    }

    #[test]
    fn a_build_side_is_gathered_into_one_buffer_per_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        // 128-row batches over buffers of their own, as a cast back from offsets leaves them.
        let batches: Vec<RecordBatch> = (0..100)
            .map(|b| {
                let s: Vec<String> = (0..128).map(|i| format!("{b:04}-{i:040}")).collect();
                let v = StringViewArray::from_iter_values(s.iter());
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(v) as ArrayRef]).unwrap()
            })
            .collect();
        let buffers = |out: &[RecordBatch]| -> Vec<usize> {
            out.iter()
                .map(|b| b.column(0).as_string_view().data_buffers().len())
                .collect()
        };
        let plain = gather(batches.clone(), false);
        assert!(buffers(&plain).iter().any(|&n| n > 1));
        let compacted = gather(batches.clone(), true);
        assert!(
            buffers(&compacted).iter().all(|&n| n == 1),
            "{:?}",
            buffers(&compacted)
        );
        let values = |out: &[RecordBatch]| -> Vec<String> {
            out.iter()
                .flat_map(|b| {
                    let s = b.column(0).as_string_view();
                    (0..s.len())
                        .map(|i| s.value(i).to_string())
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        assert_eq!(values(&compacted), values(&batches));
        // Its rows' bytes bound a compacted gather, not the buffers they sit in.
        let rows: usize = compacted.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 12_800);
        assert!(compacted.len() < 10, "{}", compacted.len());
    }

    #[test]
    fn a_sorts_input_is_gathered_to_its_row_bound_in_order() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mut next = 0;
        let sizes = std::iter::repeat_n(1000, 126).chain([10_000, 5]);
        let batches = sizes
            .map(|n| {
                let v = Int64Array::from_iter_values(next..next + n);
                next += n;
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(v)]).unwrap()
            })
            .collect();
        let out = gathered(batches);
        let rows: Vec<_> = out.iter().map(|b| b.num_rows()).collect();
        assert_eq!(rows, [66_000, 70_000, 5]);
        let values: Vec<i64> = out
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect();
        assert_eq!(values, (0..next).collect::<Vec<_>>());
    }

    #[test]
    fn a_sorts_input_is_gathered_to_its_byte_bound() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let wide = "x".repeat(10_000);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![wide.as_str(); 60]))],
        )
        .unwrap();
        let out = gathered(vec![batch; 15]);
        let rows: Vec<_> = out.iter().map(|b| b.num_rows()).collect();
        assert_eq!(rows, [840, 60]);

        // A view slice is counted for the whole buffer it keeps, so it passes on alone.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let wider = "x".repeat(50_000);
        let views = Arc::new(StringViewArray::from(vec![wider.as_str(); 200])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![views]).unwrap();
        let out = gathered(vec![batch.slice(0, 10), batch.slice(10, 10)]);
        let rows: Vec<_> = out.iter().map(|b| b.num_rows()).collect();
        assert_eq!(rows, [10, 10]);
    }
}
