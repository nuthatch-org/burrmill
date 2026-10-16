//! Tables a host registers itself, one at a time, on an engine opened empty.
//!
//! `open_nest` reads a directory. Nuthatch's `/sql` binds each table from a segment list it has
//! already filtered (the sealed watermark, segments failing verification, a historical window) and
//! its unsealed rows, and it rebinds on every statement against a cached session. Its shadow needs
//! the same, so these take exactly those inputs and replace whatever the name held before.
//!
//! A table registered this way is not in the fold substitution's table map, which is fixed when the
//! engine opens; its aggregates run as DataFusion plans them, to the same answer.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringViewArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_catalog::default_table_source::provider_as_source;
use datafusion_catalog::view::ViewTable;
use datafusion_catalog::{MemTable, TableProvider};
use datafusion_expr::LogicalPlanBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use serde_json::Value;

use super::catalog::{NestTable, SegmentTable, view_schema};
use super::{Engine, df_err, plan_query};
use crate::error::Result;
use crate::limits::Limits;

/// The four counter columns nuthatch seals as `UBIGINT`; every other column is canonical text.
fn counter(name: &str) -> bool {
    matches!(
        name,
        "block_number" | "log_index" | "_seq" | "block_timestamp"
    )
}

fn hidden(name: &str) -> bool {
    crate::inspect::hidden_table(name)
}

impl Engine {
    /// No tables until the host registers them.
    pub fn open_empty() -> Result<Self> {
        Self::from_tables(Vec::new(), Limits::default().max_threads, None, None)
    }

    /// As `open_empty`, with every statement's working memory and the footer cache held under
    /// `bytes` together, as DuckDB's `max_memory` holds a nest's session. A statement that needs
    /// more is refused; nothing spills.
    pub fn open_empty_within(bytes: usize) -> Result<Self> {
        Self::open_empty_budgeted(super::Budget {
            memory_bytes: bytes,
            threads: Limits::default().max_threads,
            spill: None,
        })
    }

    /// As `open_empty`, held to `budget`.
    pub fn open_empty_budgeted(budget: super::Budget) -> Result<Self> {
        Self::from_tables(Vec::new(), budget.threads, Some(budget), None)
    }

    /// As `open_empty_budgeted`, with its statements drawing on `pool`, which other engines share.
    pub fn open_empty_sharing(budget: super::Budget, pool: &super::SharedPool) -> Result<Self> {
        Self::from_tables(Vec::new(), budget.threads, Some(budget), Some(pool))
    }

    /// Define `name` over `files`, unioned by name with `hot` (unsealed rows as nuthatch keeps
    /// them: JSON objects, counters as numbers, everything else text), every column in `declared`
    /// present and NULL where no input carries it, `_dec` and `_overflow` beside each `word16` or
    /// `word32` column, and only rows with `after < block_number <= through` when a bound is given.
    /// Replaces an earlier `name`.
    pub fn register_facts(
        &mut self,
        name: &str,
        declared: &[(String, String)],
        files: Vec<(PathBuf, u64)>,
        hot: &[Value],
        window: (Option<u64>, Option<u64>),
    ) -> Result<()> {
        self.refuse_replacing_a_table(name)?;
        // Every footer, as DuckDB's `read_parquet` binds them all, so a file that is not Parquet
        // refuses the definition here and a host can define the table from what remains.
        for (path, len) in &files {
            if self.bound_segments.contains_key(&(path.clone(), *len)) {
                continue;
            }
            let bound = std::fs::File::open(path)
                .map_err(|e| e.to_string())
                .and_then(|f| {
                    ArrowReaderMetadata::load(&f, ArrowReaderOptions::new())
                        .map_err(|e| e.to_string())
                });
            match bound {
                Ok(meta) => {
                    let schema = Arc::new(view_schema(meta.schema()));
                    self.bound_segments.insert((path.clone(), *len), schema);
                }
                Err(e) => {
                    return Err(crate::BurrmillError::Substrate(format!(
                        "segment {} will not bind: {e}",
                        path.display()
                    )));
                }
            }
        }
        self.define_facts(name, declared, files, hot, window, false)
    }

    /// As [`Engine::register_facts`], over segments in the engine's store
    /// ([`Engine::attach_store`]): `keys` are its keys, each footer read by range.
    pub async fn register_store_facts(
        &mut self,
        name: &str,
        declared: &[(String, String)],
        keys: Vec<(String, u64)>,
        hot: &[Value],
        window: (Option<u64>, Option<u64>),
    ) -> Result<()> {
        self.refuse_replacing_a_table(name)?;
        let store = self.store()?;
        // Over HTTP each footer is a round trip, so they are asked for together.
        let unbound = keys
            .iter()
            .filter(|(k, l)| !self.bound_segments.contains_key(&(PathBuf::from(k), *l)));
        let read = futures::future::try_join_all(unbound.map(|(key, len)| {
            let store = &store;
            async move {
                super::catalog::footer_schema(store, key, *len)
                    .await
                    .map(|s| ((PathBuf::from(key), *len), s))
                    .map_err(|e| {
                        crate::BurrmillError::Substrate(format!("segment {key} will not bind: {e}"))
                    })
            }
        }))
        .await?;
        self.bound_segments.extend(read);
        let files = keys
            .into_iter()
            .map(|(k, l)| (PathBuf::from(k), l))
            .collect();
        self.define_facts(name, declared, files, hot, window, true)
    }

    fn define_facts(
        &mut self,
        name: &str,
        declared: &[(String, String)],
        files: Vec<(PathBuf, u64)>,
        hot: &[Value],
        window: (Option<u64>, Option<u64>),
        keyed: bool,
    ) -> Result<()> {
        let raw = format!("{name}__raw");
        let raw_schema: SchemaRef = if files.is_empty() {
            Arc::new(Schema::new(
                declared
                    .iter()
                    .map(|(c, _)| Field::new(c, column_type(c), true))
                    .collect::<Vec<_>>(),
            ))
        } else {
            // Every segment's columns by name, as DuckDB's `union_by_name`: a column the ABI gained
            // is in the later segments only, and the first one's schema would read it as NULL.
            let schemas: Vec<Schema> = files
                .iter()
                .map(|(p, l)| self.bound_segments[&(p.clone(), *l)].as_ref().clone())
                .collect();
            Arc::new(Schema::try_merge(widen_integers(schemas)).map_err(|e| {
                crate::BurrmillError::Substrate(format!("the segments of {name} disagree: {e}"))
            })?)
        };
        let raw_provider: Arc<dyn TableProvider> = if files.is_empty() {
            Arc::new(MemTable::try_new(raw_schema.clone(), vec![vec![]]).map_err(df_err)?)
        } else {
            Arc::new(SegmentTable::new(
                &NestTable {
                    name: raw.clone(),
                    schema: raw_schema.clone(),
                    files,
                    wide: Vec::new(),
                    keyed,
                    lowercase: declared
                        .iter()
                        .filter(|(_, t)| t == "address")
                        .map(|(c, _)| c.clone())
                        .collect(),
                },
                self.threads,
                self.cancel.clone(),
            )?)
        };
        self.session.register_table(&raw, Arc::clone(&raw_provider));

        let mut present: Vec<String> = raw_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let mut source = raw;
        if !hot.is_empty() {
            let hot_name = format!("{name}__hot");
            let (schema, batch) = json_batch(hot, true)?;
            present.extend(schema.fields().iter().map(|f| f.name().clone()));
            let hot_provider: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).map_err(df_err)?);
            self.session
                .register_table(&hot_name, Arc::clone(&hot_provider));
            let plan =
                LogicalPlanBuilder::scan(source.as_str(), provider_as_source(raw_provider), None)
                    .and_then(|b| {
                        b.union_by_name(
                            LogicalPlanBuilder::scan(
                                hot_name.as_str(),
                                provider_as_source(hot_provider),
                                None,
                            )?
                            .build()?,
                        )
                    })
                    .and_then(|b| b.build())
                    .map_err(df_err)?;
            source = format!("{name}__union");
            self.session
                .register_table(&source, Arc::new(ViewTable::new(plan, None)));
        }

        let stubs: String = declared
            .iter()
            .filter(|(c, _)| !present.contains(c))
            .map(|(c, _)| format!(", CAST(NULL AS {}) AS \"{c}\"", sql_type(c)))
            .collect();
        let dec: String = declared
            .iter()
            .filter(|(_, s)| s == "word16" || s == "word32")
            .map(|(c, _)| {
                format!(
                    ", TRY_CAST(\"{c}\" AS DECIMAL(38,0)) AS \"{c}_dec\", \
                     (\"{c}\" IS NOT NULL AND TRY_CAST(\"{c}\" AS DECIMAL(38,0)) IS NULL) \
                     AS \"{c}_overflow\""
                )
            })
            .collect();
        let range = match window {
            (None, None) => None,
            (Some(a), None) => Some(format!("block_number > {a}")),
            (None, Some(t)) => Some(format!("block_number <= {t}")),
            (Some(a), Some(t)) => Some(format!("block_number > {a} AND block_number <= {t}")),
        };
        // A row with no block number cannot be placed in the window: refused, as DuckDB refuses it,
        // not dropped by the comparison. The range stays a plain conjunct so segments still prune.
        let bound = range.map_or(String::new(), |r| {
            format!(
                " WHERE (block_number IS NULL OR ({r})) AND CASE WHEN block_number IS NULL THEN \
                 error('historical query requires block-stamped facts: unstamped archived row') \
                 ELSE true END"
            )
        });
        let sql = format!("SELECT *{dec} FROM (SELECT *{stubs} FROM \"{source}\"){bound}");
        let logical = plan_query(&self.session, &sql)?;
        self.session
            .register_table(name, Arc::new(ViewTable::new(logical, Some(sql))));
        // The view's plan already holds these. Leaving the names registered is how a later
        // statement reads past the window, or a tip this request did not rebind (#10).
        self.drop_hidden_registrations();
        self.session
            .build_information_schema(hidden)
            .map_err(df_err)
    }

    /// Unbind every `__raw`, `__hot` and `__union`. Public views keep the plan they captured.
    fn drop_hidden_registrations(&mut self) {
        let names: Vec<String> = self
            .session
            .table_names()
            .into_iter()
            .filter(|n| hidden(n))
            .collect();
        for n in names {
            let _ = self.drop_relation(&n);
        }
    }

    /// A table of text columns from JSON rows, for the small side inputs a nest carries beside its
    /// segments (label snapshots). Replaces an earlier `name`.
    pub fn register_rows(&mut self, name: &str, rows: &[Value]) -> Result<()> {
        let (schema, batch) = json_batch(rows, false)?;
        self.session.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).map_err(df_err)?),
        );
        self.session
            .build_information_schema(hidden)
            .map_err(df_err)
    }

    /// Whether `name` is a table or view the host or the nest defined.
    pub fn has_table(&self, name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        self.visible_tables()
            .iter()
            .any(|t| t.to_ascii_lowercase() == lower)
    }

    /// Every table and view except the parts a registration is built from, sorted.
    pub fn visible_tables(&self) -> Vec<String> {
        self.session
            .table_names()
            .into_iter()
            .filter(|t| !hidden(t))
            .collect()
    }
}

fn column_type(name: &str) -> DataType {
    if counter(name) {
        DataType::UInt64
    } else {
        DataType::Utf8View
    }
}

fn sql_type(name: &str) -> &'static str {
    if counter(name) { "UBIGINT" } else { "VARCHAR" }
}

/// One batch from JSON objects: the columns are the sorted union of the keys. With `typed`, the
/// counter columns are `UInt64` (0 where absent, as nuthatch seals them); every other column is
/// text, the string itself or the JSON value rendered, NULL where absent or null.
fn json_batch(rows: &[Value], typed: bool) -> Result<(SchemaRef, RecordBatch)> {
    let mut columns: std::collections::BTreeSet<String> = Default::default();
    for r in rows {
        if let Some(o) = r.as_object() {
            columns.extend(o.keys().cloned());
        }
    }
    if columns.is_empty() {
        return Err(crate::BurrmillError::NotAllowed(
            "rows have no columns".into(),
        ));
    }
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for c in &columns {
        if typed && counter(c) {
            // A row without one has NULL, as DuckDB reads it: a missing block number is not zero.
            fields.push(Field::new(c, DataType::UInt64, true));
            arrays.push(Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|r| r.get(c).and_then(Value::as_u64))
                    .collect::<Vec<_>>(),
            )));
        } else {
            fields.push(Field::new(c, DataType::Utf8View, true));
            arrays.push(Arc::new(StringViewArray::from(
                rows.iter()
                    .map(|r| match r.get(c) {
                        None | Some(Value::Null) => None,
                        Some(Value::String(s)) => Some(s.clone()),
                        Some(other) => Some(other.to_string()),
                    })
                    .collect::<Vec<Option<String>>>(),
            )));
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| crate::BurrmillError::Substrate(e.to_string()))?;
    Ok((schema, batch))
}

/// One column at two integer widths across segments takes the type DuckDB's `union_by_name` gives
/// it: the wider of one signedness, a signed type holding both otherwise, and HUGEINT for UBIGINT
/// with BIGINT. The scan casts each file's column up to it.
fn widen_integers(schemas: Vec<Schema>) -> Vec<Schema> {
    let mut widest: std::collections::HashMap<&str, DataType> = std::collections::HashMap::new();
    for f in schemas.iter().flat_map(|s| s.fields()) {
        if f.data_type().is_integer() {
            widest
                .entry(f.name())
                .and_modify(|t| {
                    if let Some(u) = super::dialect::duck_union(t, f.data_type()) {
                        *t = u;
                    }
                })
                .or_insert_with(|| f.data_type().clone());
        }
    }
    schemas
        .iter()
        .map(|s| {
            Schema::new(
                s.fields()
                    .iter()
                    .map(|f| match widest.get(f.name().as_str()) {
                        Some(t) if f.data_type().is_integer() && t != f.data_type() => {
                            Arc::new(f.as_ref().clone().with_data_type(t.clone()))
                        }
                        _ => Arc::clone(f),
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::{Budget, Engine};

    /// #1650: `/sql` opens a budgeted session. A thousand rows must not arrive as one batch, or the
    /// caller's byte cap runs only after that batch already exists.
    #[test]
    fn a_budgeted_session_reads_128_rows_at_a_time() {
        let budgeted = Engine::open_empty_budgeted(Budget {
            memory_bytes: 256 << 20,
            threads: 1,
            spill: None,
        })
        .unwrap();
        let mut sizes = Vec::new();
        budgeted
            .sql_for_each("SELECT range FROM range(1000)", |batch| {
                sizes.push(batch.num_rows());
                Ok(())
            })
            .unwrap();
        assert_eq!(
            sizes.iter().copied().max(),
            Some(128),
            "a budgeted read of 1000 rows arrived as {sizes:?}"
        );

        // No budget keeps DataFusion's own batch, so a fold scan is not cut into 128s.
        let plain = Engine::open_empty().unwrap();
        let mut plain_sizes = Vec::new();
        plain
            .sql_for_each("SELECT range FROM range(20000)", |batch| {
                plain_sizes.push(batch.num_rows());
                Ok(())
            })
            .unwrap();
        let plain_max = plain_sizes.iter().copied().max().unwrap_or(0);
        assert!(
            plain_max > 128,
            "an unbudgeted read was also cut down: {plain_sizes:?}"
        );
    }
}
