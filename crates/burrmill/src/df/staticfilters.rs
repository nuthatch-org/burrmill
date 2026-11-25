//! `StaticRowFilters`: a Parquet scan whose late materialization spares nothing worth its cost
//! evaluates its `WHERE` in the filter above it, on decoded batches, and not in the reader.
//!
//! With `pushdown_filters` the reader evaluates each conjunct of a scan's predicate in a pass of its
//! own, over the rows the ones before it kept, and decodes the scan's other columns only for the rows
//! all of them keep. TPC-H Q6 is five conjuncts on three columns over unclustered rows, with one other
//! column to read: at SF10 its `lineitem` scan took 3.27 s of CPU, 0.19 s of it evaluating the
//! conjuncts, where reading all four columns took 0.96 s and the `FilterExec` above 0.89 s (#433).
//! Its 1.9% of rows are scattered through every page, so the reader saved nothing it did not then
//! spend on the passes.
//!
//! So a scan keeps its row filter where it can spare something: where its predicate holds a join's or
//! a top-k's dynamic filter, which cuts the rows the operators above it see; where a conjunct is an
//! equality with a literal, which alone can keep few rows; and where it reads a column the predicate
//! does not that is not fixed-width, text or a nested value, whose decoding the filter can save.
//!
//! Every predicate such a scan holds is enforced above it as well. A table's `WHERE` reaches the scan
//! as `Inexact` (`catalog.rs`), so its `FilterExec` stays, and `CancelExec` does not pass a `WHERE`
//! down again. What else a scan holds, a join's or a top-k's filter or `RangeBounds`', is the join's
//! or the top-k's own to enforce. The predicate still prunes row groups and pages.

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
use datafusion_session::PhysicalOptimizerRule;

#[derive(Debug, Default)]
pub struct StaticRowFilters;

/// Whether `predicate` holds a dynamic filter anywhere in it.
fn dynamic(predicate: &Arc<dyn PhysicalExpr>) -> bool {
    let mut found = false;
    predicate
        .apply(|e| {
            found = e.is::<DynamicFilterPhysicalExpr>();
            Ok(match found {
                true => TreeNodeRecursion::Stop,
                false => TreeNodeRecursion::Continue,
            })
        })
        .expect("the closure does not fail");
    found
}

/// `a = 5` or `5 = a`, over a column or an expression of one.
fn equality(conjunct: &Arc<dyn PhysicalExpr>) -> bool {
    conjunct.downcast_ref::<BinaryExpr>().is_some_and(|b| {
        *b.op() == Operator::Eq && (b.left().is::<Literal>() || b.right().is::<Literal>())
    })
}

/// Whether the reader should evaluate `predicate` for a scan that outputs `scan`'s columns.
fn spares_something(predicate: &Arc<dyn PhysicalExpr>, scan: &DataSourceExec) -> bool {
    if dynamic(predicate) || split_conjunction(predicate).into_iter().any(equality) {
        return true;
    }
    let read: Vec<String> = collect_columns(predicate)
        .into_iter()
        .map(|c| c.name().to_string())
        .collect();
    scan.schema()
        .fields()
        .iter()
        .any(|f| !read.iter().any(|r| r == f.name()) && !fixed_width(f.data_type()))
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
                if spares_something(&predicate, scan) {
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
