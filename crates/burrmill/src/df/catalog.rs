//! Nest tables as DataFusion providers: explicit file lists, sizes known, empty if unsealed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{DFSchema, Result as DFResult};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_datasource_parquet::{CachedParquetFileReaderFactory, ParquetFileReaderFactory};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
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
}

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
        });
    }
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(tables)
}

/// Apply `schema.json` declarations: unsealed tables become empty, `word16`/`word32` get `_dec`.
pub fn apply_schema_json(tables: &mut Vec<NestTable>, schema_json: &Path) -> crate::Result<()> {
    let raw = std::fs::read_to_string(schema_json)?;
    let doc: serde_json::Value = serde_json::from_str(&raw)
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
        if let Some(t) = tables.iter_mut().find(|t| t.name == *name) {
            t.wide = wide;
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
/// Kept, they held 4 KiB a segment until the scan ended (#86).
#[derive(Debug)]
struct Unmetered(Arc<dyn ParquetFileReaderFactory>);

impl ParquetFileReaderFactory for Unmetered {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> DFResult<Box<dyn AsyncFileReader + Send>> {
        let unread = ExecutionPlanMetricsSet::new();
        self.0
            .create_reader(partition_index, file, metadata_size_hint, &unread)
    }
}

#[derive(Debug)]
pub struct SegmentTable {
    schema: SchemaRef,
    files: Vec<PartitionedFile>,
    groups: usize,
    /// The engine's token; every scan of this table stops at it.
    cancel: crate::CancelToken,
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
                let loc = object_store::path::Path::from_filesystem_path(p)
                    .map_err(|e| crate::BurrmillError::Substrate(e.to_string()))?;
                Ok(PartitionedFile::new(loc.to_string(), *size))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(Self {
            schema: table.schema.clone(),
            files,
            groups: groups.max(1),
            cancel,
        })
    }
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
        let url = ObjectStoreUrl::local_filesystem();
        let mut opts = datafusion_common::config::TableParquetOptions {
            global: state.config_options().execution.parquet.clone(),
            ..Default::default()
        };
        opts.global.pushdown_filters = true;
        let store = state.runtime_env().object_store(&url)?;
        // The session's footer cache is read only through this factory; a limit on the cache
        // manager alone leaves every statement reading every footer again (burrmill#98).
        let footers = state.runtime_env().cache_manager.get_file_metadata_cache();
        let mut source = ParquetSource::new(self.schema.clone())
            .with_table_parquet_options(opts)
            .with_parquet_file_reader_factory(Arc::new(Unmetered(Arc::new(
                CachedParquetFileReaderFactory::new(store, footers),
            ))));
        if let Some(pred) = conjunction(filters.iter().cloned()) {
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
