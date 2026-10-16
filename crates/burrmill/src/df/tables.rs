//! Tables a host keeps for itself: a query's result held in memory, Parquet written and read back,
//! and transactions over them. nuthatch's fold runtime is the host; none of this is reachable from
//! a statement, which can still only read.

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion_catalog::MemTable;
use datafusion_catalog::default_table_source::provider_as_source;
#[cfg(not(target_arch = "wasm32"))]
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{Engine, df_err, dialect, plan_query};
use crate::error::{BurrmillError, Result};

fn io(e: impl std::fmt::Display) -> BurrmillError {
    BurrmillError::Substrate(e.to_string())
}

impl Engine {
    /// `sql`'s result schema, from its plan, with the names a statement's batches carry.
    fn result_schema(&self, sql: &str) -> Result<SchemaRef> {
        let logical = plan_query(&self.session, sql)?;
        let empty = RecordBatch::new_empty(Arc::new(logical.schema().as_arrow().clone()));
        Ok(dialect::strip_dup_suffix(empty).schema())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn batches(&self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let batches = self.sql(sql)?;
        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => self.result_schema(sql)?,
        };
        Ok((schema, batches))
    }

    /// Bind `name`, remembering what it was bound to if a transaction is open and has not yet.
    fn bind(&mut self, name: &str, table: Option<MemTable>) {
        let before = self.session.table_source(name);
        if let Some(txn) = self.txn.as_mut() {
            txn.entry(name.to_string()).or_insert(before);
        }
        self.session
            .set_table_source(name, table.map(|t| provider_as_source(Arc::new(t))));
    }

    /// `CREATE OR REPLACE TABLE name AS sql`, held in memory. Returns its row count.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_table_as(&mut self, name: &str, sql: &str) -> Result<u64> {
        let (schema, batches) = self.batches(sql)?;
        let rows = batches.iter().map(|b| b.num_rows() as u64).sum();
        self.bind(
            name,
            Some(MemTable::try_new(schema, vec![batches]).map_err(df_err)?),
        );
        Ok(rows)
    }

    /// Unbind a table or view; false if nothing had the name.
    pub fn drop_relation(&mut self, name: &str) -> bool {
        let had = self.session.table_source(name).is_some();
        if had {
            self.bind(name, None);
        }
        had
    }

    pub fn begin(&mut self) -> Result<()> {
        if self.txn.is_some() {
            return Err(BurrmillError::NotAllowed(
                "a transaction is already open".into(),
            ));
        }
        self.txn = Some(Default::default());
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.txn
            .take()
            .map(drop)
            .ok_or_else(|| BurrmillError::NotAllowed("no transaction is open".into()))
    }

    /// Every name the transaction touched goes back to what it was before it.
    pub fn rollback(&mut self) -> Result<()> {
        let txn = self
            .txn
            .take()
            .ok_or_else(|| BurrmillError::NotAllowed("no transaction is open".into()))?;
        for (name, before) in txn {
            self.session.set_table_source(&name, before);
        }
        Ok(())
    }

    /// `sql`'s result written to `path` as one Parquet file. Returns its row count.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn write_parquet(&self, sql: &str, path: &Path) -> Result<u64> {
        let (schema, batches) = self.batches(sql)?;
        let file = std::fs::File::create(path).map_err(io)?;
        let mut w = ArrowWriter::try_new(file, schema, None).map_err(io)?;
        let mut rows = 0u64;
        for b in &batches {
            w.write(b).map_err(io)?;
            rows += b.num_rows() as u64;
        }
        w.close().map_err(io)?;
        Ok(rows)
    }

    /// One Parquet file held in memory as `name`. Returns its row count.
    pub fn load_parquet(&mut self, name: &str, path: &Path) -> Result<u64> {
        let file = std::fs::File::open(path).map_err(io)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(io)?
            .build()
            .map_err(io)?;
        let schema = reader.schema();
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(io)?;
        let rows = batches.iter().map(|b| b.num_rows() as u64).sum();
        self.bind(
            name,
            Some(MemTable::try_new(schema, vec![batches]).map_err(df_err)?),
        );
        Ok(rows)
    }

    /// `(column, type)` of `sql`'s result, from its plan and without running it, each type as
    /// DuckDB spells it.
    pub fn describe(&self, sql: &str) -> Result<Vec<(String, String)>> {
        Ok(self
            .result_schema(sql)?
            .fields()
            .iter()
            .map(|f| (f.name().clone(), duckdb_type(f.data_type())))
            .collect())
    }

    /// `sql`'s result as an Arrow IPC stream, for a host whose arrow is not this one.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn sql_ipc(&self, sql: &str) -> Result<Vec<u8>> {
        let (schema, batches) = self.batches(sql)?;
        let mut w = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &schema).map_err(io)?;
        for b in &batches {
            w.write(b).map_err(io)?;
        }
        w.into_inner().map_err(io)
    }
}

/// An Arrow type as DuckDB names the type it stands for. `HUGEINT` is carried as `Decimal128(38, 0)`
/// and so reads as `DECIMAL(38,0)`.
pub fn duckdb_type(t: &DataType) -> String {
    use DataType::*;
    match t {
        Utf8 | Utf8View | LargeUtf8 => "VARCHAR".into(),
        Boolean => "BOOLEAN".into(),
        Int8 => "TINYINT".into(),
        Int16 => "SMALLINT".into(),
        Int32 => "INTEGER".into(),
        Int64 => "BIGINT".into(),
        UInt8 => "UTINYINT".into(),
        UInt16 => "USMALLINT".into(),
        UInt32 => "UINTEGER".into(),
        UInt64 => "UBIGINT".into(),
        Float32 => "FLOAT".into(),
        Float64 => "DOUBLE".into(),
        Binary | BinaryView | LargeBinary | FixedSizeBinary(_) => "BLOB".into(),
        Date32 | Date64 => "DATE".into(),
        Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP".into(),
        Timestamp(_, Some(_)) => "TIMESTAMP WITH TIME ZONE".into(),
        Decimal128(p, s) | Decimal256(p, s) => format!("DECIMAL({p},{s})"),
        Null => "NULL".into(),
        List(f) | LargeList(f) => format!("{}[]", duckdb_type(f.data_type())),
        other => other.to_string(),
    }
}
