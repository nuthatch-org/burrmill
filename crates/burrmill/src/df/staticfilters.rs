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
//! dynamic filter, which cuts the rows the operators above it see; where it holds a join's and the
//! scan reads a column the predicate does not, whose decoding the join's filter can save; where a
//! conjunct is an equality with a literal, which alone can keep few rows; and where it reads a column
//! the predicate does not that is not fixed-width, text or a nested value, whose decoding the filter
//! can save.
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

use std::collections::HashSet;
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
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_session::PhysicalOptimizerRule;

#[derive(Debug, Default)]
pub struct StaticRowFilters;

/// The ids of the dynamic filters the plan's hash joins produce.
fn join_filters(plan: &Arc<dyn ExecutionPlan>) -> Result<HashSet<u64>> {
    let mut ids = HashSet::new();
    plan.apply(|node| {
        if node.is::<HashJoinExec>() {
            ids.extend(
                node.dynamic_expressions_produced()
                    .iter()
                    .filter_map(|f| f.expression_id()),
            );
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(ids)
}

/// What dynamic filters `predicate` holds: whether it holds any, and whether all it holds are
/// joins'.
fn dynamic(predicate: &Arc<dyn PhysicalExpr>, joins: &HashSet<u64>) -> (bool, bool) {
    let (mut any, mut all_joins) = (false, true);
    predicate
        .apply(|e| {
            if e.is::<DynamicFilterPhysicalExpr>() {
                any = true;
                all_joins &= e.expression_id().is_some_and(|id| joins.contains(&id));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("the closure does not fail");
    (any, all_joins)
}

/// `a = 5` or `5 = a`, over a column or an expression of one.
fn equality(conjunct: &Arc<dyn PhysicalExpr>) -> bool {
    conjunct.downcast_ref::<BinaryExpr>().is_some_and(|b| {
        *b.op() == Operator::Eq && (b.left().is::<Literal>() || b.right().is::<Literal>())
    })
}

/// Whether the reader should evaluate `predicate` for a scan that outputs `scan`'s columns, where
/// `joins` are the ids of the plan's join filters.
fn spares_something(
    predicate: &Arc<dyn PhysicalExpr>,
    scan: &DataSourceExec,
    joins: &HashSet<u64>,
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
    match dynamic(predicate, joins) {
        (true, false) => return true,
        (true, true) if unread().next().is_some() => return true,
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
                if spares_something(&predicate, scan, &joins) {
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
