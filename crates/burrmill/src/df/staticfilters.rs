//! `StaticRowFilters`: a Parquet scan whose late materialization spares nothing worth its cost
//! evaluates its `WHERE` in the filter above it, on decoded batches, and not in the reader.
//!
//! With `pushdown_filters` the reader evaluates each conjunct of a scan's predicate in a pass of its
//! own, over the rows the ones before it kept, and decodes the scan's other columns only for the rows
//! all of them keep. TPC-H Q6 is five conjuncts on three columns over unclustered rows, with one other
//! column to read: at SF10 its `lineitem` scan took 3.27 s of CPU, 186.5 ms of it evaluating the
//! conjuncts, where reading all four columns took 955.62 ms and the `FilterExec` above 886.97 ms
//! (#433).
//! Its 1.9% of rows are scattered through every page, so the reader saved nothing it did not then
//! spend on the passes.
//!
//! So a scan keeps its row filter where it can spare something: where its predicate holds a top-k's
//! dynamic filter, which cuts the rows the operators above it see; where it holds a join's that
//! reaches it through another join, and the scan reads a column the predicate does not, whose
//! decoding the join's filter can save; where a conjunct is an equality with a literal, which alone
//! can keep few rows; and where it reads a column the predicate does not that is not fixed-width,
//! text or a nested value, whose decoding the filter can save.
//!
//! A join's filter on a scan that the join probes directly, through filters, projections and
//! repartitions alone, is not enough on its own: the join drops the same rows straight above the
//! scan, and over keys scattered through every page the reader spares little of the other columns'
//! decoding. TPC-H Q3 probes `lineitem` with a year of orders and `orders` with a segment's
//! customers; in the reader at SF10 those scans took 2.59 s and 1.03 s of CPU, 1.28 s and 0.43 s of
//! it the row filter, and the statement 686 ms at best, against 416 ms with both filters left to
//! the joins (#433). Through another join it is: Q7's `orders` scan holds the filter of the join to
//! the customers above the join to `lineitem`, and in the reader it cut what that join built on
//! from 15.00M orders to 1.21M.
//!
//! Where a scan reads only the columns its predicate reads, a join's filter in the reader can spare
//! only the decoding of those columns for the rows an earlier conjunct dropped, and the join drops
//! the same rows above the scan. TPC-H Q4's `lineitem` scan reads `l_orderkey`, `l_commitdate` and
//! `l_receiptdate`; its predicate is `l_receiptdate > l_commitdate` and the semi join's filter on
//! `l_orderkey`, which together keep 1.45M of 60M rows at SF10. In the reader that scan took 2.54 s
//! of CPU, 521 ms of it evaluating the predicate; read whole it took 0.87 s, with 0.15 s in the
//! filter above it and 0.34 s more in the join, and the statement went from 408–514 ms to 242–329
//! (#433).
//!
//! Every predicate such a scan holds is enforced above it as well. A table's `WHERE` reaches the scan
//! as `Inexact` (`catalog.rs`), so its `FilterExec` stays, and `CancelExec` does not pass a `WHERE`
//! down again. What else a scan holds, a join's or a top-k's filter or `RangeBounds`', is the join's
//! or the top-k's own to enforce. The predicate still prunes row groups and pages.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, DynamicFilterPhysicalExpr, Literal};
use datafusion_physical_expr::utils::{collect_columns, split_conjunction};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_session::PhysicalOptimizerRule;

#[derive(Debug, Default)]
pub struct StaticRowFilters;

/// The dynamic filters the plan's hash joins produce: their ids, and for each scan a join probes
/// directly ([`probed_scan`]), the ids of the filters that join gives it.
struct JoinFilters {
    ids: HashSet<u64>,
    direct: HashMap<usize, HashSet<u64>>,
}

/// The address of a plan node, which identifies a scan while the plan is walked.
fn address(p: &Arc<dyn ExecutionPlan>) -> usize {
    Arc::as_ptr(p) as *const () as usize
}

fn join_filters(plan: &Arc<dyn ExecutionPlan>) -> Result<JoinFilters> {
    let (mut ids, mut direct) = (HashSet::new(), HashMap::<usize, HashSet<u64>>::new());
    plan.apply(|node| {
        if let Some(j) = node.downcast_ref::<HashJoinExec>() {
            let produced: Vec<u64> = node
                .dynamic_expressions_produced()
                .iter()
                .filter_map(|f| f.expression_id())
                .collect();
            if let Some(scan) = probed_scan(j.right()) {
                direct.entry(address(scan)).or_default().extend(&produced);
            }
            ids.extend(produced);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(JoinFilters { ids, direct })
}

/// The scan under a join's probe side `p`, where the rows it gives reach the join through filters,
/// projections, repartitions and the catalogue's `CancelExec` alone. A join's filter that reaches a scan through another join
/// or an aggregate does not.
fn probed_scan(p: &Arc<dyn ExecutionPlan>) -> Option<&Arc<dyn ExecutionPlan>> {
    if p.is::<DataSourceExec>() {
        return Some(p);
    }
    if p.is::<FilterExec>()
        || p.is::<ProjectionExec>()
        || p.is::<RepartitionExec>()
        || p.is::<CoalescePartitionsExec>()
        || p.is::<super::cancel::CancelExec>()
    {
        return probed_scan(p.children().into_iter().next()?);
    }
    None
}

/// What dynamic filters `predicate` holds: whether it holds any, whether all it holds are joins',
/// and whether all it holds are from joins that probe the scan directly, whose ids are `direct`.
fn dynamic(
    predicate: &Arc<dyn PhysicalExpr>,
    joins: &HashSet<u64>,
    direct: Option<&HashSet<u64>>,
) -> (bool, bool, bool) {
    let (mut any, mut all_joins, mut all_direct) = (false, true, true);
    predicate
        .apply(|e| {
            if e.is::<DynamicFilterPhysicalExpr>() {
                any = true;
                let id = e.expression_id();
                all_joins &= id.is_some_and(|id| joins.contains(&id));
                all_direct &= id.is_some_and(|id| direct.is_some_and(|d| d.contains(&id)));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("the closure does not fail");
    (any, all_joins, all_direct)
}

/// `a = 5` or `5 = a`, over a column or an expression of one.
fn equality(conjunct: &Arc<dyn PhysicalExpr>) -> bool {
    conjunct.downcast_ref::<BinaryExpr>().is_some_and(|b| {
        *b.op() == Operator::Eq && (b.left().is::<Literal>() || b.right().is::<Literal>())
    })
}

/// Whether the reader should evaluate `predicate` for a scan that outputs `scan`'s columns, where
/// `joins` are the plan's join filters and `at` is the scan's [`address`].
fn spares_something(
    predicate: &Arc<dyn PhysicalExpr>,
    scan: &DataSourceExec,
    at: usize,
    joins: &JoinFilters,
) -> bool {
    let read: Vec<String> = collect_columns(predicate)
        .into_iter()
        .map(|c| c.name().to_string())
        .collect();
    let schema = scan.schema();
    let unread = || {
        schema
            .fields()
            .iter()
            .filter(|f| !read.iter().any(|r| r == f.name()))
    };
    match dynamic(predicate, &joins.ids, joins.direct.get(&at)) {
        (true, false, _) => return true,
        (true, true, false) if unread().next().is_some() => return true,
        _ => {}
    }
    if split_conjunction(predicate).into_iter().any(equality) {
        return true;
    }
    unread().any(|f| !fixed_width(f.data_type()))
}

/// A number, a date or time, or a boolean: a column whose rows decode at a fixed width.
fn fixed_width(t: &DataType) -> bool {
    t.is_primitive() || matches!(t, DataType::Boolean | DataType::Null)
}

impl PhysicalOptimizerRule for StaticRowFilters {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let joins = join_filters(&plan)?;
        Ok(plan
            .transform_up(|p| {
                let Some(scan) = p.downcast_ref::<DataSourceExec>() else {
                    return Ok(Transformed::no(p));
                };
                let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
                    return Ok(Transformed::no(p));
                };
                let Some(source) = config.file_source().downcast_ref::<ParquetSource>() else {
                    return Ok(Transformed::no(p));
                };
                let Some(predicate) = source.filter() else {
                    return Ok(Transformed::no(p));
                };
                if spares_something(&predicate, scan, address(&p), &joins) {
                    return Ok(Transformed::no(p));
                }
                let source = source.clone().with_pushdown_filters(false);
                let config = FileScanConfigBuilder::from(config.clone())
                    .with_source(Arc::new(source))
                    .build();
                Ok(Transformed::yes(
                    DataSourceExec::from_data_source(config) as _
                ))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "static_row_filters"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
