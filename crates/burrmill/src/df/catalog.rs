//! Nest tables as DataFusion providers: explicit file lists, sizes known, empty if unsealed.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

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
use datafusion_execution::cache::cache_manager::FileMetadataCache;
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

/// Discover `<table>-<hash>.parquet` under `segments/`, last hyphen splitting table from hash. A
/// `.parquet` file with no hyphen is skipped.
pub fn discover_tables(segments: &Path) -> crate::Result<Vec<NestTable>> {
    let mut by_table: HashMap<String, Vec<(PathBuf, u64)>> = HashMap::new();
    let entries = std::fs::read_dir(segments).map_err(|e| {
        let at = segments.display();
        if e.kind() == std::io::ErrorKind::NotFound {
            crate::BurrmillError::NoSegments(format!("no segments directory at {at}"))
        } else {
            crate::BurrmillError::Substrate(format!("cannot read {at}: {e}"))
        }
    })?;
    for e in entries.flatten() {
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

/// What a scan holds for each segment until the scan ends, beside two copies of its path: `LISTED`
/// of it from the start, for the segment's place in the plan's file lists, and the rest once it
/// opens the segment, for DataFusion's per-file metrics and the opener's state.
/// `tests/df_scanpool.rs` measures both.
const PER_SEGMENT: usize = 5_632;
const LISTED: usize = 1_024;
const FOOTERS_IN_FLIGHT: usize = 1;

/// The bytes the segment scan under `plan` holds for listing the segments of `partition`.
pub(super) fn listed(plan: &Arc<dyn ExecutionPlan>, partition: usize) -> usize {
    // `CompactViews` wraps a scan of text columns partition for partition. Looking no further than
    // `plan` itself charged such a scan nothing.
    let mut plan = plan;
    let scan = loop {
        if let Some(d) = plan.downcast_ref::<DataSourceExec>() {
            break d;
        }
        match plan.children().as_slice() {
            [child] => plan = child,
            _ => return 0,
        }
    };
    scan.data_source()
        .downcast_ref::<FileScanConfig>()
        .and_then(|c| c.file_groups.get(partition))
        .map_or(0, |g| {
            g.iter()
                .map(|f| LISTED + 2 * f.object_meta.location.as_ref().len())
                .sum()
        })
}

/// The pool reservations of one scan's running partitions. Its reader factory charges a segment
/// to its partition's as the segment opens, so a scan holds for what it has opened and not for
/// every segment of the table before its first row (#156).
#[derive(Debug, Default)]
pub(super) struct Opened(Mutex<HashMap<usize, Vec<Running>>>);

type Held = datafusion_execution::memory_pool::MemoryReservation;

#[derive(Debug)]
struct Running {
    held: Weak<Held>,
    /// A segment's bloom filters are read through a second reader, which must not charge it twice.
    charged: HashSet<u64>,
}

impl Opened {
    /// `held` is what `partition` charges from here on, until its stream drops it.
    pub(super) fn running(&self, partition: usize, held: &Arc<Held>) {
        let mut all = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let runs = all.entry(partition).or_default();
        runs.retain(|r| r.held.strong_count() > 0);
        runs.push(Running {
            held: Arc::downgrade(held),
            charged: HashSet::new(),
        });
    }

    fn opened(&self, partition: usize, file: &PartitionedFile) {
        let location = file.object_meta.location.as_ref();
        let mut all = self.0.lock().unwrap_or_else(|p| p.into_inner());
        // A plan run again while its first run is still read charges the newer run.
        let Some((run, held)) = all
            .get_mut(&partition)
            .into_iter()
            .flatten()
            .rev()
            .find_map(|r| r.held.upgrade().map(|h| (r, h)))
        else {
            return;
        };
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(location, &mut h);
        if run.charged.insert(std::hash::Hasher::finish(&h)) {
            // Charged whether or not it fits, so the operators that can spill make room for it.
            held.grow(PER_SEGMENT - LISTED);
        }
    }
}

/// DataFusion's reader, its per-file metrics registered into a set dropped as soon as it is made.
/// Kept, they held 4 KiB a segment until the scan ended (#86). The data bytes it reads go to the
/// scan's one counter instead, which [`crate::QueryMetrics`] reports.
#[derive(Debug)]
struct Unmetered(Arc<dyn ParquetFileReaderFactory>, Count, Arc<Opened>);

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
        self.2.opened(partition_index, &file);
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
#[recursive::recursive]
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
    footers: Arc<FileMetadataCache>,
    files: Vec<object_store::ObjectMeta>,
}

/// The footer cache a scan of `url` reads: the process's for files on this machine, the engine's
/// for the keys of its store.
fn footers_for(
    url: &ObjectStoreUrl,
    env: &datafusion_execution::runtime_env::RuntimeEnv,
) -> Arc<FileMetadataCache> {
    if *url == ObjectStoreUrl::local_filesystem() {
        super::session::local_footers()
    } else {
        env.cache_manager.get_file_metadata_cache()
    }
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
            footers: footers_for(&c.object_store_url, env),
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
pub(super) async fn selected_bytes(scans: Vec<Selection>) -> DFResult<u64> {
    use datafusion_datasource_parquet::metadata::DFParquetMetadata;
    use datafusion_datasource_parquet::{
        ParquetAccessPlan, ParquetFileMetrics, RowGroupAccessPlanFilter,
    };
    use futures::{StreamExt, TryStreamExt};
    use parquet::arrow::ProjectionMask;

    let unread = Arc::new(ParquetFileMetrics::new(
        0,
        "selected",
        &ExecutionPlanMetricsSet::new(),
    ));
    let files = scans.into_iter().map(Arc::new).flat_map(|s| {
        let files = s.files.clone();
        files.into_iter().map(move |f| (Arc::clone(&s), f))
    });
    // Several footers in flight: a statement that stops after one segment waits on this pass.
    futures::stream::iter(files)
        .map(|(s, f)| {
            let unread = Arc::clone(&unread);
            async move {
                let meta = DFParquetMetadata::new(s.store.as_ref(), &f)
                    .with_file_metadata_cache(Some(Arc::clone(&s.footers)))
                    .with_page_index_policy(Some(parquet::file::metadata::PageIndexPolicy::Skip))
                    .fetch_metadata()
                    .await?;
                let groups = meta.row_groups();
                let descr = meta.file_metadata().schema_descr();
                let mut keep =
                    RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(groups.len()));
                if let Some(p) = &s.pruning {
                    keep.prune_by_statistics(&s.schema, descr, groups, p, &unread);
                }
                let mask = s
                    .columns
                    .as_ref()
                    .map(|n| ProjectionMask::columns(descr, n.iter().map(String::as_str)));
                let mut bytes = 0u64;
                for g in keep.row_group_indexes() {
                    bytes += groups[g]
                        .columns()
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| mask.as_ref().is_none_or(|m| m.leaf_included(*i)))
                        .map(|(_, c)| c.compressed_size().max(0) as u64)
                        .sum::<u64>();
                }
                DFResult::Ok(bytes)
            }
        })
        .buffer_unordered(FOOTERS_IN_FLIGHT)
        .try_fold(0u64, |total, bytes| async move { Ok(total + bytes) })
        .await
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

/// A table's segments as a scan reads them, built once for the file list it was given.
#[derive(Debug)]
pub(super) struct Listed {
    pub(super) files: Vec<(PathBuf, u64)>,
    /// Each distinct footer schema in the order the files first carry it, where a host bound them.
    pub(super) schemas: Vec<SchemaRef>,
    located: Vec<PartitionedFile>,
}

impl Listed {
    pub(super) fn new(
        files: Vec<(PathBuf, u64)>,
        schemas: Vec<SchemaRef>,
        keyed: bool,
    ) -> crate::Result<Self> {
        // A table's segments share one or two directories; canonicalising each file walked every
        // component of its path per request (nuthatch#2669).
        let mut parents: HashMap<PathBuf, PathBuf> = HashMap::new();
        let located = files
            .iter()
            .map(|(p, size)| {
                let loc = if keyed {
                    store_location(p, true)?
                } else {
                    local_location(p, &mut parents)?
                };
                Ok(PartitionedFile::new(loc.to_string(), *size))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(Self {
            files,
            schemas,
            located,
        })
    }
}

#[derive(Debug)]
pub struct SegmentTable {
    schema: SchemaRef,
    listed: Arc<Listed>,
    groups: usize,
    url: ObjectStoreUrl,
    lowercase: HashSet<String>,
}

impl SegmentTable {
    pub fn new(table: &NestTable, groups: usize) -> crate::Result<Self> {
        let listed = Listed::new(table.files.clone(), Vec::new(), table.keyed)?;
        Self::over(table, Arc::new(listed), groups)
    }

    /// `table` over `listed`, which was built from its files.
    pub(super) fn over(
        table: &NestTable,
        listed: Arc<Listed>,
        groups: usize,
    ) -> crate::Result<Self> {
        Ok(Self {
            schema: table.schema.clone(),
            listed,
            groups: groups.max(1),
            url: if table.keyed {
                ObjectStoreUrl::parse(NEST_STORE)
                    .map_err(|e| crate::BurrmillError::Substrate(e.to_string()))?
            } else {
                ObjectStoreUrl::local_filesystem()
            },
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
        // A footer cache is read only through this factory; a limit on the cache manager alone
        // leaves every statement reading every footer again (burrmill#98).
        let footers = footers_for(&url, state.runtime_env());
        let source = ParquetSource::new(self.schema.clone()).with_table_parquet_options(opts);
        let bytes = MetricBuilder::new(source.metrics()).global_counter(SEALED_BYTES);
        let opened = Arc::new(Opened::default());
        let mut source = source.with_parquet_file_reader_factory(Arc::new(Unmetered(
            Arc::new(CachedParquetFileReaderFactory::new(store, footers)),
            bytes,
            Arc::clone(&opened),
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

        let files = &self.listed.located;
        let n = self.groups.min(files.len().max(1));
        let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); n];
        for (i, f) in files.iter().enumerate() {
            groups[i % n].push(f.clone());
        }
        let file_source: Arc<dyn FileSource> = Arc::new(source);
        let cfg = FileScanConfigBuilder::new(url, file_source)
            .with_file_groups(groups.into_iter().map(FileGroup::new).collect())
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        Ok(super::cancel::CancelExec::wrap_scan(
            DataSourceExec::from_data_source(cfg),
            opened,
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

/// What `store_location` gives a local file, canonicalising its directory once per `parents`.
#[cfg(not(target_arch = "wasm32"))]
fn local_location(
    p: &Path,
    parents: &mut HashMap<PathBuf, PathBuf>,
) -> crate::Result<object_store::path::Path> {
    let (Some(parent), Some(name)) = (p.parent(), p.file_name()) else {
        return store_location(p, false);
    };
    // A symlinked file canonicalises to its target, which the directory alone cannot say, and a
    // missing one must still fail here as canonicalising it did.
    if !std::fs::symlink_metadata(p).is_ok_and(|m| m.file_type().is_file()) {
        return store_location(p, false);
    }
    let dir = match parents.get(parent) {
        Some(dir) => dir.clone(),
        None => {
            let dir = std::fs::canonicalize(parent)
                .map_err(|e| crate::BurrmillError::Substrate(format!("{}: {e}", p.display())))?;
            parents.insert(parent.to_path_buf(), dir.clone());
            dir
        }
    };
    object_store::path::Path::from_absolute_path(dir.join(name))
        .map_err(|e| crate::BurrmillError::Substrate(e.to_string()))
}

#[cfg(target_arch = "wasm32")]
fn local_location(
    p: &Path,
    _: &mut HashMap<PathBuf, PathBuf>,
) -> crate::Result<object_store::path::Path> {
    store_location(p, false)
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn a_local_location_is_what_canonicalising_the_file_gives() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        std::fs::create_dir(&real).unwrap();
        std::fs::write(real.join("a.parquet"), b"x").unwrap();
        std::fs::write(real.join("target.parquet"), b"y").unwrap();
        std::os::unix::fs::symlink(&real, tmp.path().join("linked")).unwrap();
        std::os::unix::fs::symlink(real.join("target.parquet"), real.join("b.parquet")).unwrap();
        let mut parents = HashMap::new();
        for p in [
            real.join("a.parquet"),
            tmp.path().join("linked/a.parquet"),
            real.join("b.parquet"),
            tmp.path().join("linked/b.parquet"),
        ] {
            assert_eq!(
                local_location(&p, &mut parents).unwrap(),
                object_store::path::Path::from_filesystem_path(&p).unwrap(),
                "{}",
                p.display()
            );
        }
        assert!(local_location(&real.join("missing.parquet"), &mut parents).is_err());
    }

    #[test]
    fn a_segment_is_charged_once_to_the_run_that_opens_it() {
        use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let run = |name: &str| Arc::new(MemoryConsumer::new(name).register(&pool));
        let opened = Opened::default();
        let (a, b) = (
            PartitionedFile::new("/s/a.parquet", 1),
            PartitionedFile::new("/s/b.parquet", 1),
        );
        // No partition is running: nothing to charge.
        opened.opened(0, &a);
        assert_eq!(pool.reserved(), 0);
        let first = run("first");
        opened.running(0, &first);
        opened.opened(0, &a);
        // The reader its bloom filters are read through.
        opened.opened(0, &a);
        opened.opened(1, &b);
        assert_eq!(first.size(), PER_SEGMENT - LISTED);
        opened.opened(0, &b);
        assert_eq!(first.size(), 2 * (PER_SEGMENT - LISTED));
        // The plan run again: the newer run is charged, and the older again once that has gone.
        let second = run("second");
        opened.running(0, &second);
        opened.opened(0, &a);
        assert_eq!(second.size(), PER_SEGMENT - LISTED);
        drop(second);
        assert_eq!(pool.reserved(), first.size());
        opened.opened(0, &PartitionedFile::new("/s/c.parquet", 1));
        assert_eq!(first.size(), 3 * (PER_SEGMENT - LISTED));
    }
}
