//! One table from any Parquet a host names: a file, a directory, or a prefix in an object store, by
//! whatever names its files have, with hive partitions read from the path as columns.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::ScalarValue;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

use super::catalog::{NestTable, footer_schema, view_schema};
use crate::error::{BurrmillError, Result};

/// What Hive writes for a NULL partition value.
const HIVE_NULL: &str = "__HIVE_DEFAULT_PARTITION__";

/// The table `name` over the Parquet at `path`: the file itself, or every `.parquet` file beneath
/// the directory. Its columns are the first file's in path order, as DuckDB's `read_parquet`.
pub(super) fn local_table(path: &Path, name: &str) -> Result<NestTable> {
    let mut files = Vec::new();
    if path.is_file() {
        files.push((path.to_path_buf(), std::fs::metadata(path)?.len()));
    } else {
        walk(path, path, &mut files)?;
    }
    if files.is_empty() {
        return Err(BurrmillError::NoSegments(format!(
            "no .parquet files under {}",
            path.display()
        )));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    let relative: Vec<String> = files
        .iter()
        .map(|(p, _)| {
            p.strip_prefix(path)
                .unwrap_or(p)
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/")
        })
        .collect();
    let f = std::fs::File::open(&files[0].0)?;
    let meta = ArrowReaderMetadata::load(&f, ArrowReaderOptions::new())?;
    let schema = Arc::new(view_schema(meta.schema()));
    table(name, schema, files, &relative, false)
}

/// As [`local_table`], over every `.parquet` key under `prefix` in the store registered at `url`.
pub(super) async fn store_table(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
    name: &str,
    url: &str,
) -> Result<NestTable> {
    use futures::TryStreamExt;
    let sub = |e: object_store::Error| BurrmillError::Substrate(e.to_string());
    let root = object_store::path::Path::parse(prefix)
        .map_err(|e| BurrmillError::Substrate(e.to_string()))?;
    let mut listed: Vec<_> = store.list(Some(&root)).try_collect().await.map_err(sub)?;
    listed.retain(|m| m.location.filename().is_some_and(is_data_file));
    listed.sort_by(|a, b| a.location.cmp(&b.location));
    let Some(first) = listed.first() else {
        return Err(BurrmillError::NoSegments(format!(
            "no .parquet files under {prefix}"
        )));
    };
    let schema = footer_schema(store, first.location.as_ref(), first.size).await?;
    let relative: Vec<String> = listed
        .iter()
        .map(|m| {
            let full = m.location.as_ref();
            full.strip_prefix(root.as_ref())
                .unwrap_or(full)
                .trim_start_matches('/')
                .to_string()
        })
        .collect();
    let files = listed
        .iter()
        .map(|m| (PathBuf::from(m.location.as_ref()), m.size))
        .collect();
    let mut t = table(name, schema, files, &relative, true)?;
    t.store_url = Some(url.to_string());
    Ok(t)
}

/// A data file, not a marker or a writer's temporary: Spark's `_SUCCESS`, `.crc` and the like.
fn is_data_file(name: &str) -> bool {
    name.ends_with(".parquet") && !name.starts_with('_') && !name.starts_with('.')
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<(PathBuf, u64)>) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            BurrmillError::NoSegments(format!("nothing at {}", root.display()))
        } else {
            BurrmillError::Substrate(format!("cannot read {}: {e}", dir.display()))
        }
    })?;
    for e in entries {
        let e = e.map_err(|e| BurrmillError::Substrate(format!("{}: {e}", dir.display())))?;
        let p = e.path();
        let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with('.') || name.starts_with('_') {
            continue;
        }
        let meta = std::fs::metadata(&p)?;
        if meta.is_dir() {
            walk(root, &p, out)?;
        } else if is_data_file(name) {
            out.push((p, meta.len()));
        }
    }
    Ok(())
}

fn table(
    name: &str,
    schema: arrow::datatypes::SchemaRef,
    files: Vec<(PathBuf, u64)>,
    relative: &[String],
    keyed: bool,
) -> Result<NestTable> {
    let (partitions, partition_values) = hive(relative)?;
    for f in &partitions {
        if schema.field_with_name(f.name()).is_ok() {
            return Err(BurrmillError::Config(format!(
                "{name}: the partition {} is also a column of its files",
                f.name()
            )));
        }
    }
    Ok(NestTable {
        name: name.to_string(),
        schema,
        files,
        wide: Vec::new(),
        keyed,
        lowercase: Vec::new(),
        partitions,
        partition_values,
        store_url: None,
    })
}

/// The partition columns `key=value` directories give, typed from every value they take, and each
/// file's values. Every file must sit under the same keys in the same order.
fn hive(relative: &[String]) -> Result<(Vec<FieldRef>, Vec<Vec<ScalarValue>>)> {
    let pairs: Vec<Vec<(String, Option<String>)>> = relative
        .iter()
        .map(|r| {
            let dirs = r.rsplit_once('/').map_or("", |(d, _)| d);
            dirs.split('/')
                .filter_map(|d| d.split_once('='))
                .map(|(k, v)| {
                    let v = unescape(v);
                    (unescape(k), (v != HIVE_NULL).then_some(v))
                })
                .collect()
        })
        .collect();
    let keys: Vec<String> = pairs[0].iter().map(|(k, _)| k.clone()).collect();
    if keys.is_empty() && pairs.iter().all(|p| p.is_empty()) {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut seen = HashSet::new();
    if let Some(k) = keys.iter().find(|k| !seen.insert(k.to_lowercase())) {
        return Err(BurrmillError::Config(format!(
            "the partition {k} appears twice in one path"
        )));
    }
    for (p, r) in pairs.iter().zip(relative) {
        if p.iter().map(|(k, _)| k).ne(keys.iter()) {
            return Err(BurrmillError::Config(format!(
                "{r} is not partitioned as {} is: every file sits under the same keys",
                relative[0]
            )));
        }
    }
    let types: Vec<DataType> = (0..keys.len())
        .map(|i| infer(pairs.iter().filter_map(|p| p[i].1.as_deref())))
        .collect();
    let fields = keys
        .iter()
        .zip(&types)
        .map(|(k, t)| Arc::new(Field::new(k, t.clone(), true)))
        .collect();
    let values = pairs
        .iter()
        .map(|p| {
            p.iter()
                .zip(&types)
                .map(|((_, v), t)| value(v.as_deref(), t))
                .collect()
        })
        .collect();
    Ok((fields, values))
}

/// BIGINT when every value is one, then DATE, else VARCHAR: DuckDB's `hive_types_autocast`, less
/// the floating and timestamp types, whose text does not round-trip.
fn infer<'a>(mut values: impl Iterator<Item = &'a str> + Clone) -> DataType {
    if values.clone().all(|v| v.parse::<i64>().is_ok()) {
        DataType::Int64
    } else if values.all(|v| parse_date(v).is_some()) {
        DataType::Date32
    } else {
        DataType::Utf8
    }
}

fn value(v: Option<&str>, t: &DataType) -> ScalarValue {
    match t {
        DataType::Int64 => ScalarValue::Int64(v.and_then(|v| v.parse().ok())),
        DataType::Date32 => ScalarValue::Date32(v.and_then(parse_date)),
        _ => ScalarValue::Utf8(v.map(str::to_string)),
    }
}

/// `YYYY-MM-DD` as days since 1970-01-01.
fn parse_date(v: &str) -> Option<i32> {
    let d = chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d").ok()?;
    if d.format("%Y-%m-%d").to_string() != v {
        return None;
    }
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from((d - epoch).num_days()).ok()
}

/// Hive's percent escapes, as its writers emit them for `/`, `=`, `%` and the like.
fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && let Some(h) = s.get(i + 1..i + 3)
            && let Ok(byte) = u8::from_str_radix(h, 16)
        {
            out.push(byte);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn partitions_are_typed_from_every_value() {
        let (f, v) = hive(&rel(&[
            "year=2024/day=2024-01-02/region=eu/a.parquet",
            "year=2025/day=2025-12-31/region=us%2Feast/b.parquet",
            "year=__HIVE_DEFAULT_PARTITION__/day=2025-01-01/region=7/c.parquet",
        ]))
        .unwrap();
        let types: Vec<_> = f.iter().map(|f| f.data_type().clone()).collect();
        assert_eq!(types, [DataType::Int64, DataType::Date32, DataType::Utf8]);
        assert_eq!(v[1][2], ScalarValue::Utf8(Some("us/east".into())));
        assert_eq!(v[2][0], ScalarValue::Int64(None));
        assert_eq!(v[0][1], ScalarValue::Date32(Some(19_724)));
    }

    #[test]
    fn a_file_under_other_keys_refuses() {
        let e = hive(&rel(&["a=1/x.parquet", "b=1/y.parquet"])).unwrap_err();
        assert!(e.to_string().contains("same keys"), "{e}");
        let e = hive(&rel(&["a=1/x.parquet", "y.parquet"])).unwrap_err();
        assert!(e.to_string().contains("same keys"), "{e}");
    }

    #[test]
    fn plain_directories_are_not_partitions() {
        let (f, v) = hive(&rel(&["2024/x.parquet", "y.parquet"])).unwrap();
        assert!(f.is_empty() && v.is_empty());
    }

    #[test]
    fn a_date_that_is_not_one_stays_text() {
        assert_eq!(infer(["2024-1-2"].into_iter()), DataType::Utf8);
        assert_eq!(infer(["2024-02-30"].into_iter()), DataType::Utf8);
    }
}
