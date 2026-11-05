//! Relic corpora from The Legacy (RFC-0004 §11), opened the way a nest is: Burrmill's own
//! providers, no file function reachable from SQL.
//!
//! Admission checks what the bytes are, not what they mean: canonical file names, sizes and
//! BLAKE3 against the manifest, and pact linkage across the set. Tries, headers and blooms are
//! `solo clean`'s job, and nothing here claims them.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, BooleanArray, StringArray, StringBuilder, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result as DFResult, exec_err, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    ColumnarValue, Expr, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TryCast, Volatility,
};
use datafusion_optimizer::analyzer::AnalyzerRule;
use legacy_format::manifest::Manifest;

use super::catalog::{NestTable, view_schema};
use crate::error::{BurrmillError, Result};

/// The five core tables and their uint256 columns, which get `_dec` and `_overflow` beside them.
const TABLES: [(&str, &[&str]); 5] = [
    (
        "headers",
        &["difficulty", "base_fee_per_gas", "total_difficulty"],
    ),
    (
        "transactions",
        &[
            "value",
            "gas_price",
            "max_fee_per_gas",
            "max_priority_fee_per_gas",
            "max_fee_per_blob_gas",
            "mint",
        ],
    ),
    (
        "receipts",
        &[
            "effective_gas_price",
            "blob_gas_price",
            "l1_fee",
            "l1_gas_used",
            "l1_gas_price",
        ],
    ),
    ("logs", &[]),
    ("withdrawals", &[]),
];

fn canonical_schema(table: &str) -> arrow::datatypes::SchemaRef {
    match table {
        "headers" => legacy_parquet::headers::schema(),
        "transactions" => legacy_parquet::transactions::schema(),
        "receipts" => legacy_parquet::receipts::schema(),
        "logs" => legacy_parquet::logs::schema(),
        _ => legacy_parquet::withdrawals::schema(),
    }
}

struct Relic {
    dir: PathBuf,
    manifest: Option<Manifest>,
    manifest_blake3: Option<String>,
    bytes_checked: bool,
    failure: Option<String>,
}

/// `root/manifest.json`, or every `relics/<index>/manifest.json` (or `<index>/manifest.json`).
fn relic_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    if root.join("manifest.json").is_file() {
        return Ok(vec![root.to_path_buf()]);
    }
    let base = if root.join("relics").is_dir() {
        root.join("relics")
    } else {
        root.to_path_buf()
    };
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(&base)?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.join("manifest.json").is_file())
        .collect();
    dirs.sort();
    if dirs.is_empty() {
        return Err(BurrmillError::NoSegments(format!(
            "no relic manifests under {}",
            root.display()
        )));
    }
    Ok(dirs)
}

fn admit(dir: PathBuf) -> Relic {
    let mut relic = Relic {
        dir,
        manifest: None,
        manifest_blake3: None,
        bytes_checked: false,
        failure: None,
    };
    let raw = match std::fs::read(relic.dir.join("manifest.json")) {
        Ok(raw) => raw,
        Err(e) => {
            relic.failure = Some(format!("manifest.json: {e}"));
            return relic;
        }
    };
    relic.manifest_blake3 = Some(legacy_format::hash::blake3(&raw).to_hex());
    let manifest: Manifest = match serde_json::from_slice(&raw) {
        Ok(m) => m,
        Err(e) => {
            relic.failure = Some(format!("manifest.json does not parse: {e}"));
            return relic;
        }
    };
    // `validate` refuses any file name that is not `<table>.parquet`, so nothing below can be
    // pointed at a path the manifest chose.
    if let Err(e) = manifest.validate() {
        relic.failure = Some(e.to_string());
        relic.manifest = Some(manifest);
        return relic;
    }
    for file in &manifest.files {
        let path = relic.dir.join(&file.name);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                relic.failure = Some(format!("{}: {e}", file.name));
                break;
            }
        };
        if bytes.len() as u64 != file.byte_size {
            relic.failure = Some(format!(
                "{} is {} bytes, the manifest says {}",
                file.name,
                bytes.len(),
                file.byte_size
            ));
            break;
        }
        if legacy_format::hash::blake3(&bytes) != file.blake3 {
            relic.failure = Some(format!("{} does not match its BLAKE3", file.name));
            break;
        }
    }
    relic.bytes_checked = relic.failure.is_none();
    relic.manifest = Some(manifest);
    relic
}

/// How the admitted run is anchored, once its links have been checked.
fn link(relics: &mut [Relic]) -> String {
    let admitted: Vec<usize> = (0..relics.len())
        .filter(|&i| relics[i].failure.is_none())
        .collect();
    let Some(&first) = admitted.first() else {
        return "no relic admitted".into();
    };
    let chain_id = relics[first].manifest.as_ref().map(|m| m.chain_id);
    for &i in &admitted[1..] {
        if relics[i].manifest.as_ref().map(|m| m.chain_id) != chain_id {
            relics[i].failure = Some("a different chain from the first admitted relic".into());
        }
    }
    let run: Vec<(usize, Manifest)> = admitted
        .iter()
        .filter(|&&i| relics[i].failure.is_none())
        .map(|&i| {
            (
                i,
                relics[i].manifest.clone().expect("admitted relics parsed"),
            )
        })
        .collect();
    let manifests: Vec<Manifest> = run.iter().map(|(_, m)| m.clone()).collect();
    let head = &manifests[0];
    let anchored = head.relic_index() == 0 || head.detached_pact_start;
    let checked = if anchored {
        legacy_format::pact::verify_chain(None, &manifests)
    } else {
        // Nothing below the first relic is here, so check the links within the set only.
        legacy_format::pact::verify_chain(Some(head), &manifests[1..])
    };
    if let Err(e) = checked {
        let at = match &e {
            legacy_format::Error::Pact { index, .. } => *index,
            _ => head.relic_index(),
        };
        for (i, m) in &run {
            if m.relic_index() >= at {
                relics[*i].failure = Some(format!("pact: {e}"));
            }
        }
    }
    match legacy_format::pact::detached_anchor(None, &manifests) {
        Some(index) => format!("detached start at relic {index}; link to genesis not established"),
        None if anchored => "anchored at the genesis relic".into(),
        None => format!(
            "unanchored: relic {} has no predecessor in this corpus",
            head.relic_index()
        ),
    }
}

/// The tables a relic corpus opens as, and its `relics` metadata table.
pub(super) fn open(root: &Path) -> Result<(Vec<NestTable>, RecordBatch)> {
    let mut relics: Vec<Relic> = relic_dirs(root)?.into_iter().map(admit).collect();
    relics.sort_by_key(|r| r.manifest.as_ref().map_or(u64::MAX, Manifest::relic_index));
    let anchor = link(&mut relics);

    let mut tables = Vec::new();
    for (name, uint256) in TABLES {
        let file_name = format!("{name}.parquet");
        let mut files = Vec::new();
        for r in relics.iter().filter(|r| r.failure.is_none()) {
            let m = r.manifest.as_ref().expect("admitted relics parsed");
            if let Some(f) = m.files.iter().find(|f| f.name == file_name) {
                files.push((r.dir.join(&f.name), f.byte_size));
            }
        }
        tables.push(NestTable {
            name: name.to_string(),
            schema: Arc::new(view_schema(&canonical_schema(name))),
            files,
            wide: Vec::new(),
            uint256: uint256.iter().map(|c| c.to_string()).collect(),
            keyed: false,
            lowercase: Vec::new(),
            binary_addresses: true,
        });
    }
    Ok((tables, relics_batch(&relics, &anchor)?))
}

fn relics_batch(relics: &[Relic], anchor: &str) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("index", DataType::UInt64, true),
        Field::new("start_block", DataType::UInt64, true),
        Field::new("end_block", DataType::UInt64, true),
        Field::new("manifest_blake3", DataType::Utf8, true),
        Field::new("pact_root", DataType::Utf8, true),
        Field::new("admitted", DataType::Boolean, false),
        Field::new("bytes_checked", DataType::Boolean, false),
        Field::new("detached", DataType::Boolean, true),
        Field::new("anchor", DataType::Utf8, false),
        Field::new("directory", DataType::Utf8, false),
        Field::new("failure", DataType::Utf8, true),
    ]));
    fn m(r: &Relic) -> Option<&Manifest> {
        r.manifest.as_ref()
    }
    let cols: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter(
            relics.iter().map(|r| m(r).map(Manifest::relic_index)),
        )),
        Arc::new(UInt64Array::from_iter(
            relics.iter().map(|r| m(r).map(|m| m.block_range.start)),
        )),
        Arc::new(UInt64Array::from_iter(
            relics.iter().map(|r| m(r).map(|m| m.block_range.end)),
        )),
        Arc::new(StringArray::from_iter(
            relics.iter().map(|r| r.manifest_blake3.clone()),
        )),
        Arc::new(StringArray::from_iter(
            relics.iter().map(|r| m(r).map(|m| m.pact_root.to_hex())),
        )),
        Arc::new(BooleanArray::from_iter(
            relics.iter().map(|r| Some(r.failure.is_none())),
        )),
        Arc::new(BooleanArray::from_iter(
            relics.iter().map(|r| Some(r.bytes_checked)),
        )),
        Arc::new(BooleanArray::from_iter(
            relics.iter().map(|r| m(r).map(|m| m.detached_pact_start)),
        )),
        Arc::new(StringArray::from_iter_values(relics.iter().map(|_| anchor))),
        Arc::new(StringArray::from_iter_values(
            relics.iter().map(|r| r.dir.display().to_string()),
        )),
        Arc::new(StringArray::from_iter(
            relics.iter().map(|r| r.failure.clone()),
        )),
    ];
    RecordBatch::try_new(schema, cols).map_err(|e| BurrmillError::Substrate(e.to_string()))
}

/// The only functions the feature registers (RFC-0004 §11.4). `hex` is Burrmill's DuckDB one.
pub(super) fn udfs() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::from(Word(Signature::user_defined(
            Volatility::Immutable,
        )))),
        Arc::new(ScalarUDF::from(U256Text(Signature::user_defined(
            Volatility::Immutable,
        )))),
        Arc::new(ScalarUDF::from(U256Dec(Signature::user_defined(
            Volatility::Immutable,
        )))),
    ]
}

fn is_bytes(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// `word(data, n)`: the n-th 32-byte word of ABI-encoded bytes, zero-based; NULL past the end.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Word(Signature);

impl ScalarUDFImpl for Word {
    fn name(&self) -> &str {
        "word"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [b, n] if is_bytes(b) && (n.is_integer() || n.is_null()) => {
                Ok(vec![DataType::Binary, DataType::Int64])
            }
            _ => plan_err!("word takes bytes and an integer word index"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let data = arrays[0].as_binary::<i32>();
        let index = arrays[1].as_primitive::<arrow::datatypes::Int64Type>();
        let mut out = BinaryBuilder::new();
        for i in 0..rows {
            if data.is_null(i) || index.is_null(i) || index.value(i) < 0 {
                out.append_null();
                continue;
            }
            let at = index.value(i) as usize * 32;
            match data.value(i).get(at..at + 32) {
                Some(w) => out.append_value(w),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// A big-endian magnitude of up to 32 bytes as decimal text; an error past 32, which no uint256 is.
fn decimal(bytes: &[u8]) -> std::result::Result<String, String> {
    if bytes.len() > 32 {
        return Err(format!("{} bytes is wider than uint256", bytes.len()));
    }
    let mut limbs = [0u64; 4];
    for (k, chunk) in bytes.rchunks(8).enumerate() {
        let mut word = [0u8; 8];
        word[8 - chunk.len()..].copy_from_slice(chunk);
        limbs[k] = u64::from_be_bytes(word);
    }
    if limbs == [0; 4] {
        return Ok("0".into());
    }
    let mut parts = Vec::new();
    while limbs != [0; 4] {
        let mut rem = 0u128;
        for limb in limbs.iter_mut().rev() {
            let cur = (rem << 64) | u128::from(*limb);
            *limb = (cur / 10_000_000_000_000_000_000) as u64;
            rem = cur % 10_000_000_000_000_000_000;
        }
        parts.push(rem as u64);
    }
    let mut text = parts.pop().expect("nonzero").to_string();
    for p in parts.iter().rev() {
        text.push_str(&format!("{p:019}"));
    }
    Ok(text)
}

/// `u256_text(b)`: the exact decimal text of a uint256 magnitude. Internal to `u256_dec`'s
/// rewrite, but harmless to call: text carries no precision limit.
#[derive(Debug, PartialEq, Eq, Hash)]
struct U256Text(Signature);

impl ScalarUDFImpl for U256Text {
    fn name(&self) -> &str {
        "u256_text"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [b] if is_bytes(b) || b.is_null() => Ok(vec![DataType::Binary]),
            _ => plan_err!("u256_text takes bytes"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let data = arrays[0].as_binary::<i32>();
        let mut out = StringBuilder::new();
        for i in 0..rows {
            if data.is_null(i) {
                out.append_null();
                continue;
            }
            match decimal(data.value(i)) {
                Ok(t) => out.append_value(t),
                Err(e) => return exec_err!("u256_dec: {e}"),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `u256_dec(b)`: the exact value as DECIMAL(38,0), NULL above 10^38 - 1.
///
/// It never runs as itself. `RelicScalars` rewrites it to `TRY_CAST(u256_text(b) AS
/// DECIMAL(38,0))` before the checked rewrite, so a sum over it is traced to the exact source and
/// refused on overflow exactly as a nest's `_dec` column is, rather than silently dropping NULLs.
#[derive(Debug, PartialEq, Eq, Hash)]
struct U256Dec(Signature);

impl ScalarUDFImpl for U256Dec {
    fn name(&self) -> &str {
        "u256_dec"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [b] if is_bytes(b) || b.is_null() => Ok(vec![args[0].clone()]),
            _ => plan_err!("u256_dec takes bytes"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Decimal128(38, 0))
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        exec_err!("u256_dec reached execution unrewritten")
    }
}

/// Rewrites `u256_dec(x)` to `TRY_CAST(u256_text(x) AS DECIMAL(38,0))`. Runs before coercion and
/// the checked rewrite, which then treat it as the lossy cast it is.
#[derive(Debug)]
pub(super) struct RelicScalars(pub Arc<ScalarUDF>);

impl RelicScalars {
    pub(super) fn new() -> Self {
        Self(Arc::new(ScalarUDF::from(U256Text(
            Signature::user_defined(Volatility::Immutable),
        ))))
    }
}

impl AnalyzerRule for RelicScalars {
    fn name(&self) -> &str {
        "relic_scalars"
    }
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform_up_with_subqueries(|p| {
            let names_matter = matches!(
                p,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            );
            let t = p.map_expressions(|e| {
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| match &e {
                    Expr::ScalarFunction(f) if f.func.name() == "u256_dec" => {
                        let [arg] = f.args.as_slice() else {
                            return plan_err!("u256_dec takes one argument");
                        };
                        let text = Expr::ScalarFunction(ScalarFunction::new_udf(
                            Arc::clone(&self.0),
                            vec![arg.clone()],
                        ));
                        Ok(Transformed::yes(Expr::TryCast(TryCast::new(
                            Box::new(text),
                            DataType::Decimal128(38, 0),
                        ))))
                    }
                    _ => Ok(Transformed::no(e)),
                })?;
                if t.transformed && names_matter && t.data.schema_name().to_string() != name {
                    Ok(Transformed::yes(t.data.alias(name)))
                } else {
                    Ok(t)
                }
            })?;
            if t.transformed {
                Ok(Transformed::yes(t.data.recompute_schema()?))
            } else {
                Ok(t)
            }
        })
        .map(|t| t.data)
    }
}

#[cfg(test)]
mod tests {
    use super::decimal;

    #[test]
    fn decimal_text_is_exact_at_the_edges() {
        assert_eq!(decimal(&[]).unwrap(), "0");
        assert_eq!(decimal(&[0; 32]).unwrap(), "0");
        assert_eq!(decimal(&[1]).unwrap(), "1");
        assert_eq!(
            decimal(&[0xff; 32]).unwrap(),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
        // 10^19 crosses the first limb of the conversion.
        assert_eq!(
            decimal(&10_000_000_000_000_000_000u128.to_be_bytes()).unwrap(),
            "10000000000000000000"
        );
        assert!(decimal(&[1; 33]).is_err());
    }
}
