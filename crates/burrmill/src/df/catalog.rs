//! Nest tables as DataFusion providers: explicit file lists, sizes known, empty if unsealed.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result as DFResult};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_datasource_parquet::{
    CachedParquetFileReaderFactory, DefaultParquetFileReaderFactory, ParquetFileReaderFactory,
};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue,
};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;

pub(super) fn view_schema(schema: &Schema) -> Schema {
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                Arc::new(Field::new(f.name(), DataType::Utf8View, f.is_nullable()))
            }
            DataType::Binary | DataType::LargeBinary => {
                Arc::new(Field::new(f.name(), DataType::BinaryView, f.is_nullable()))
            }
            _ => Arc::clone(f),
        })
        .collect();
    Schema::new(fields)
}

/// One nest table: a known list of sealed files, or empty if declared and not yet sealed.
#[derive(Debug, Clone)]
pub struct NestTable {
    pub name: String,
    pub schema: SchemaRef,
    pub files: Vec<(PathBuf, u64)>,
    pub wide: Vec<String>,
    /// `files` are keys in the store at [`NEST_STORE`], not paths on this machine.
    pub keyed: bool,
    /// Columns the host writes lower-case: nuthatch renders every `address` as `0x` and lower hex.
    pub lowercase: Vec<String>,
}

/// Where an engine opened from an object store finds it.
pub const NEST_STORE: &str = "nest://segments";

/// Discover `<table>-<hash>.parquet` under `segments/`, last hyphen splitting table from hash.
pub fn discover_tables(segments: &Path) -> crate::Result<Vec<NestTable>> {
    let mut by_table: HashMap<String, Vec<(PathBuf, u64)>> = HashMap::new();
    for e in std::fs::read_dir(segments)?.flatten() {
        let p = e.path();
        let Some(stem) = p
            .file_name()
            .and_then(|f| f.to_str())
            .and_then(|f| f.strip_suffix(".parquet"))
        else {
            continue;
        };
        let Some((table, _)) = stem.rsplit_once('-') else {
            continue;
        };
        // The target's length: `DirEntry::metadata` reports a symlinked segment's link.
        let size = std::fs::metadata(&p)?.len();
        by_table
            .entry(table.to_string())
            .or_default()
            .push((p, size));
    }
    let mut tables = Vec::new();
    for (name, mut files) in by_table {
        files.sort_by(|a, b| a.0.cmp(&b.0));
        let f = std::fs::File::open(&files[0].0)?;
        let meta = ArrowReaderMetadata::load(&f, ArrowReaderOptions::new())?;
        let schema = Arc::new(view_schema(meta.schema()));
        tables.push(NestTable {
            name,
            schema,
            files,
            wide: Vec::new(),
            keyed: false,
            lowercase: Vec::new(),
        });
    }
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(tables)
}

/// Apply `schema.json` declarations: unsealed tables become empty, `word16`/`word32` get `_dec`.
pub fn apply_schema_json(tables: &mut Vec<NestTable>, schema_json: &Path) -> crate::Result<()> {
    apply_schema_text(tables, &std::fs::read_to_string(schema_json)?)
}

/// [`apply_schema_json`] over a document already read, from a store rather than a directory.
pub fn apply_schema_text(tables: &mut Vec<NestTable>, raw: &str) -> crate::Result<()> {
    let doc: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| crate::BurrmillError::Substrate(format!("schema.json: {e}")))?;
    let Some(arr) = doc["tables"].as_array() else {
        return Ok(());
    };
    let mut declared: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for t in arr {
        let Some(name) = t["table"].as_str() else {
            continue;
        };
        let cols = t["columns"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|c| {
                Some((
                    c["name"].as_str()?.to_string(),
                    c["storage"].as_str()?.to_string(),
                ))
            })
            .collect();
        declared.insert(name.to_string(), cols);
    }
    for (name, cols) in &declared {
        let wide: Vec<String> = cols
            .iter()
            .filter(|(_, s)| *s == "word16" || *s == "word32")
            .map(|(n, _)| n.clone())
            .collect();
        let lowercase: Vec<String> = cols
            .iter()
            .filter(|(_, s)| s == "address")
            .map(|(n, _)| n.clone())
            .collect();
        if let Some(t) = tables.iter_mut().find(|t| t.name == *name) {
            t.wide = wide;
            t.lowercase = lowercase;
            continue;
        }
        let mut fields: Vec<Field> = cols
            .iter()
            .map(|(n, s)| match s.as_str() {
                "u64" => Field::new(n, DataType::UInt64, false),
                _ => Field::new(n, DataType::Utf8View, true),
            })
            .collect();
        fields.push(Field::new("table", DataType::Utf8View, true));
        tables.push(NestTable {
            name: name.clone(),
            schema: Arc::new(Schema::new(fields)),
            files: Vec::new(),
            wide,
            keyed: false,
            lowercase,
        });
    }
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(())
}

/// What a scan holds for each segment it opens until the scan ends, beside two copies of its path:
/// DataFusion's per-file metrics and the opener's state. `tests/df_scanpool.rs` measures it.
const PER_SEGMENT: usize = 5_632;

/// The bytes `plan`, if it is a segment scan, holds for the segments of `partition` while it runs.
pub(super) fn bookkeeping(plan: &Arc<dyn ExecutionPlan>, partition: usize) -> usize {
    plan.downcast_ref::<DataSourceExec>()
        .and_then(|d| d.data_source().downcast_ref::<FileScanConfig>())
        .and_then(|c| c.file_groups.get(partition))
        .map_or(0, |g| {
            g.iter()
                .map(|f| PER_SEGMENT + 2 * f.object_meta.location.as_ref().len())
                .sum()
        })
}

/// DataFusion's reader, its per-file metrics registered into a set dropped as soon as it is made.
/// Kept, they held 4 KiB a segment until the scan ended (#86). The data bytes it reads go to the
/// scan's one counter instead, which [`crate::QueryMetrics`] reports.
#[derive(Debug)]
struct Unmetered(Arc<dyn ParquetFileReaderFactory>, Count);

/// The [`crate::QueryMetrics::sealed_bytes_scanned`] counter in a scan's metrics.
pub(super) const SEALED_BYTES: &str = "sealed_bytes_scanned";

impl ParquetFileReaderFactory for Unmetered {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> DFResult<Box<dyn AsyncFileReader + Send>> {
        let unread = ExecutionPlanMetricsSet::new();
        let inner = self
            .0
            .create_reader(partition_index, file, metadata_size_hint, &unread)?;
        Ok(Box::new(Counted {
            inner,
            bytes: self.1.clone(),
        }))
    }
}

/// The counter a plan's segment scan or owned fold reads into, taken before it runs: holding the
/// plan itself through the statement would keep every segment's DataFusion metrics alive (#86).
/// An owned fold's counter is flagged: it reads exactly what its footers select, so it counts as both.
pub(super) fn sealed_counters(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<(Count, bool)>) {
    let fold = plan.name() == "OwnedSignedFoldExec";
    for m in plan.metrics().iter().flat_map(|s| s.iter()) {
        if let MetricValue::Count { name, count } = m.value()
            && name == SEALED_BYTES
        {
            out.push((count.clone(), fold));
        }
    }
    for c in plan.children() {
        sealed_counters(c, out);
    }
}

/// One segment scan's static selection: what its footers decide it may read, taken from the plan
/// before it runs. A dynamic filter is still `true` then, so the predicate here is the static one.
pub(super) struct Selection {
    schema: SchemaRef,
    pruning: Option<Arc<datafusion_physical_optimizer::pruning::PruningPredicate>>,
    columns: Option<HashSet<String>>,
    store: Arc<dyn object_store::ObjectStore>,
    files: Vec<object_store::ObjectMeta>,
}

/// Every segment scan in `plan`, owned, so the plan is not held while the bytes are summed.
pub(super) static SELECTIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(super) fn selections(
    plan: &Arc<dyn ExecutionPlan>,
    env: &datafusion_execution::runtime_env::RuntimeEnv,
) -> DFResult<Vec<Selection>> {
    use datafusion_physical_expr::utils::collect_columns;
    use datafusion_physical_optimizer::pruning::PruningPredicateBuilder;

    SELECTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut out = Vec::new();
    let mut stack = vec![Arc::clone(plan)];
    while let Some(p) = stack.pop() {
        stack.extend(p.children().into_iter().cloned());
        let Some(c) = p
            .downcast_ref::<DataSourceExec>()
            .and_then(|d| d.data_source().downcast_ref::<FileScanConfig>())
        else {
            continue;
        };
        let Some(source) = c.file_source().downcast_ref::<ParquetSource>() else {
            continue;
        };
        let schema = Arc::clone(c.file_schema());
        let predicate = source.filter();
        let pruning = predicate.clone().and_then(|e| {
            PruningPredicateBuilder::new()
                .with_file_schema(Arc::clone(&schema))
                .build(e)
        });
        let columns = source.projection().map(|proj| {
            let mut names: HashSet<String> = proj
                .column_indices()
                .into_iter()
                .filter_map(|i| schema.fields().get(i).map(|f| f.name().clone()))
                .collect();
            if let Some(e) = &predicate {
                names.extend(collect_columns(e).into_iter().map(|c| c.name().to_string()));
            }
            names
        });
        out.push(Selection {
            schema,
            pruning,
            columns,
            store: env.object_store(&c.object_store_url)?,
            files: c
                .file_groups
                .iter()
                .flat_map(|g| g.iter().map(|f| f.object_meta.clone()))
                .collect(),
        });
    }
    Ok(out)
}

/// The data bytes the scans select: the projected and filtered column chunks of every row group
/// their row-group statistics keep, read from the footers. Unlike the bytes a scan reads, which a
/// TopK's dynamic filter or a `LIMIT` cut by how the partitions race, this repeats for the same
/// statement over the same segments.
pub(super) async fn selected_bytes(
    scans: Vec<Selection>,
    footers: Arc<datafusion_execution::cache::cache_manager::FileMetadataCache>,
) -> DFResult<u64> {
    use datafusion_datasource_parquet::metadata::DFParquetMetadata;
    use datafusion_datasource_parquet::{
        ParquetAccessPlan, ParquetFileMetrics, RowGroupAccessPlanFilter,
    };
    use parquet::arrow::ProjectionMask;

    let unread = ParquetFileMetrics::new(0, "selected", &ExecutionPlanMetricsSet::new());
    let mut total = 0u64;
    for s in scans {
        for f in s.files {
            let meta = DFParquetMetadata::new(s.store.as_ref(), &f)
                .with_file_metadata_cache(Some(Arc::clone(&footers)))
                .with_page_index_policy(Some(parquet::file::metadata::PageIndexPolicy::Skip))
                .fetch_metadata()
                .await?;
            let groups = meta.row_groups();
            let descr = meta.file_metadata().schema_descr();
            let mut keep = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(groups.len()));
            if let Some(p) = &s.pruning {
                keep.prune_by_statistics(&s.schema, descr, groups, p, &unread);
            }
            let mask = s
                .columns
                .as_ref()
                .map(|n| ProjectionMask::columns(descr, n.iter().map(String::as_str)));
            for g in keep.row_group_indexes() {
                total += groups[g]
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| mask.as_ref().is_none_or(|m| m.leaf_included(*i)))
                    .map(|(_, c)| c.compressed_size().max(0) as u64)
                    .sum::<u64>();
            }
        }
    }
    Ok(total)
}

/// Counts the byte ranges a scan reads. Footers come through `get_metadata` and the footer cache,
/// so a warm cache and a cold one count the same.
struct Counted {
    inner: Box<dyn AsyncFileReader + Send>,
    bytes: Count,
}

impl AsyncFileReader for Counted {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        self.bytes.add((range.end - range.start) as usize);
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        self.bytes
            .add(ranges.iter().map(|r| (r.end - r.start) as usize).sum());
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> futures::future::BoxFuture<
        'a,
        parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>,
    > {
        self.inner.get_metadata(options)
    }
}

#[derive(Debug)]
pub struct SegmentTable {
    schema: SchemaRef,
    files: Vec<PartitionedFile>,
    groups: usize,
    url: ObjectStoreUrl,
    /// The engine's token; every scan of this table stops at it.
    cancel: crate::CancelToken,
    lowercase: HashSet<String>,
}

impl SegmentTable {
    pub fn new(
        table: &NestTable,
        groups: usize,
        cancel: crate::CancelToken,
    ) -> crate::Result<Self> {
        let files = table
            .files
            .iter()
            .map(|(p, size)| {
                let loc = store_location(p, table.keyed)?;
                Ok(PartitionedFile::new(loc.to_string(), *size))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(Self {
            schema: table.schema.clone(),
            files,
            groups: groups.max(1),
            url: if table.keyed {
                ObjectStoreUrl::parse(NEST_STORE)
                    .map_err(|e| crate::BurrmillError::Substrate(e.to_string()))?
            } else {
                ObjectStoreUrl::local_filesystem()
            },
            cancel,
            lowercase: table.lowercase.iter().cloned().collect(),
        })
    }
}

/// `lower(c)` as `c` for a column the host writes lower-case, so the predicate the scan prunes with
/// can use the segments' statistics and bloom filters. The filter above the scan keeps `lower`.
fn unlowered(e: Expr, lowercase: &HashSet<String>) -> DFResult<Expr> {
    e.transform_up(|e| match &e {
        Expr::ScalarFunction(f) if f.func.name() == "lower" => match f.args.as_slice() {
            [Expr::Column(c)] if lowercase.contains(&c.name) => {
                Ok(Transformed::yes(Expr::Column(c.clone())))
            }
            _ => Ok(Transformed::no(e)),
        },
        _ => Ok(Transformed::no(e)),
    })
    .map(|t| t.data)
}

#[async_trait]
impl TableProvider for SegmentTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let url = self.url.clone();
        let mut opts = datafusion_common::config::TableParquetOptions {
            global: state.config_options().execution.parquet.clone(),
            ..Default::default()
        };
        opts.global.pushdown_filters = true;
        let store = state.runtime_env().object_store(&url)?;
        // The session's footer cache is read only through this factory; a limit on the cache
        // manager alone leaves every statement reading every footer again (burrmill#98).
        let footers = state.runtime_env().cache_manager.get_file_metadata_cache();
        let source = ParquetSource::new(self.schema.clone()).with_table_parquet_options(opts);
        let bytes = MetricBuilder::new(source.metrics()).global_counter(SEALED_BYTES);
        let mut source = source.with_parquet_file_reader_factory(Arc::new(Unmetered(
            Arc::new(CachedParquetFileReaderFactory::new(store, footers)),
            bytes,
        )));
        let filters = filters
            .iter()
            .map(|f| unlowered(f.clone(), &self.lowercase))
            .collect::<DFResult<Vec<_>>>()?;
        let filters = filters
            .into_iter()
            .filter_map(|f| super::addresses::as_text_filter(f).transpose())
            .collect::<DFResult<Vec<_>>>()?;
        if let Some(pred) = conjunction(filters) {
            let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
            source = source.with_predicate(state.create_physical_expr(pred, &df_schema)?);
        }

        let n = self.groups.min(self.files.len().max(1));
        let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); n];
        for (i, f) in self.files.iter().enumerate() {
            groups[i % n].push(f.clone());
        }
        let file_source: Arc<dyn FileSource> = Arc::new(source);
        let cfg = FileScanConfigBuilder::new(url, file_source)
            .with_file_groups(groups.into_iter().map(FileGroup::new).collect())
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        Ok(super::cancel::CancelExec::wrap(
            DataSourceExec::from_data_source(cfg),
            self.cancel.clone(),
        ))
    }
}

/// [`discover_tables`] over an object store: `<root>/segments/<table>-<hash>.parquet`, each table's
/// schema from its first segment's footer, read by range and not whole.
pub async fn discover_store_tables(
    store: &Arc<dyn object_store::ObjectStore>,
    root: &object_store::path::Path,
) -> crate::Result<Vec<NestTable>> {
    use futures::TryStreamExt;
    let sub = |e: object_store::Error| crate::BurrmillError::Substrate(e.to_string());
    let listed: Vec<_> = store
        .list(Some(&root.clone().join("segments")))
        .try_collect()
        .await
        .map_err(sub)?;
    let mut by_table: HashMap<String, Vec<(PathBuf, u64)>> = HashMap::new();
    for m in listed {
        let Some(stem) = m
            .location
            .filename()
            .and_then(|f| f.strip_suffix(".parquet"))
        else {
            continue;
        };
        let Some((table, _)) = stem.rsplit_once('-') else {
            continue;
        };
        by_table
            .entry(table.to_string())
            .or_default()
            .push((PathBuf::from(m.location.as_ref()), m.size));
    }
    let mut tables = Vec::new();
    for (name, mut files) in by_table {
        files.sort_by(|a, b| a.0.cmp(&b.0));
        let (first, size) = &files[0];
        let schema = footer_schema(store, &first.to_string_lossy(), *size).await?;
        tables.push(NestTable {
            name,
            schema,
            files,
            wide: Vec::new(),
            keyed: true,
            lowercase: Vec::new(),
        });
    }
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(tables)
}

/// The columns of the segment at `key`, from its footer alone, as a scan of it reads them.
pub async fn footer_schema(
    store: &Arc<dyn object_store::ObjectStore>,
    key: &str,
    size: u64,
) -> crate::Result<SchemaRef> {
    let sub =
        |e: datafusion_common::DataFusionError| crate::BurrmillError::Substrate(e.to_string());
    let file = PartitionedFile::new(store_location(Path::new(key), true)?.to_string(), size);
    let mut reader: Box<dyn AsyncFileReader> =
        DefaultParquetFileReaderFactory::new(Arc::clone(store))
            .create_reader(0, file, None, &ExecutionPlanMetricsSet::new())
            .map_err(sub)?;
    let opts = ArrowReaderOptions::new();
    let footer = reader.get_metadata(Some(&opts)).await?;
    let meta = ArrowReaderMetadata::try_new(footer, opts)?;
    Ok(Arc::new(view_schema(meta.schema())))
}

/// A segment's location: a key in the engine's store, or a file on this machine.
fn store_location(p: &Path, keyed: bool) -> crate::Result<object_store::path::Path> {
    let sub = |e: object_store::path::Error| crate::BurrmillError::Substrate(e.to_string());
    if keyed {
        return object_store::path::Path::parse(p.to_string_lossy()).map_err(sub);
    }
    #[cfg(not(target_arch = "wasm32"))]
    return object_store::path::Path::from_filesystem_path(p).map_err(sub);
    #[cfg(target_arch = "wasm32")]
    Err(crate::BurrmillError::Substrate(format!(
        "{} is a local path, and a tab has no files",
        p.display()
    )))
}
