//! Query receipts (#341): what anyone holding the same inputs needs to re-check an answer. The
//! normalised statement, the engine and the settings that decide answers, the content hash of
//! every input the statement reads, and the hash of the answer's canonical Arrow IPC. A digest over
//! the rest makes a receipt edited by hand fail to verify; it is a checksum, not a signature.
//!
//! Inputs are hashed by content, never by path or table name, so a nest on disk and the same files
//! in a bucket or a tab make the same receipt. Rows a host registered are hashed as canonical IPC.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_catalog::default_table_source::source_as_provider;
use datafusion_catalog::{MemTable, TableProvider};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_expr::LogicalPlan;
use sha2::{Digest, Sha256};

use super::catalog::SegmentTable;
use crate::error::{BurrmillError, Result};

/// The format, which a release that changes how any field is made must bump.
pub const FORMAT: &str = "burrmill-receipt/1";

/// Kept with the `=55.2.0` pins in Cargo.toml; a unit test holds them together.
const DATAFUSION: &str = "55.2.0";

/// A receipt as [`Receipt::to_json`] writes it and [`Receipt::parse`] reads it back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    /// `burrmill <version>, datafusion <version>`.
    pub engine: String,
    /// The settings that change an answer. Deterministic mode is implied: no receipt without it.
    pub strict_decimals: bool,
    /// The statement as the parser prints it back.
    pub statement: String,
    /// `parquet:<sha256>:<bytes>` per file and `rows:<sha256>:<rows>` per registered table,
    /// sorted and each once.
    pub inputs: Vec<String>,
    /// `<sha256>:<rows>` of the answer's canonical Arrow IPC.
    pub answer: String,
}

fn hex(d: impl AsRef<[u8]>) -> String {
    d.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

fn sha256(bytes: &[u8]) -> String {
    hex(Sha256::digest(bytes))
}

impl Receipt {
    fn body(&self) -> serde_json::Value {
        serde_json::json!({
            "receipt": FORMAT,
            "engine": self.engine,
            "settings": { "strict_decimals": self.strict_decimals },
            "statement": self.statement,
            "inputs": self.inputs,
            "answer": self.answer,
        })
    }

    fn digest(&self) -> String {
        sha256(self.body().to_string().as_bytes())
    }

    /// The receipt as one line of JSON, its digest last.
    pub fn to_json(&self) -> String {
        let mut v = self.body();
        v["digest"] = serde_json::Value::String(self.digest());
        v.to_string()
    }

    /// A receipt from [`Receipt::to_json`]'s text, refused when any field was changed after the
    /// digest was taken or the format is not this release's.
    pub fn parse(text: &str) -> Result<Self> {
        let bad = |why: &str| BurrmillError::Unverified(format!("not a receipt: {why}"));
        let v: serde_json::Value = serde_json::from_str(text).map_err(|e| bad(&e.to_string()))?;
        if v["receipt"] != FORMAT {
            return Err(bad(&format!("the format is not {FORMAT}")));
        }
        let text_of = |k: &str| {
            v[k].as_str()
                .map(str::to_string)
                .ok_or_else(|| bad(&format!("no {k}")))
        };
        let r = Self {
            engine: text_of("engine")?,
            strict_decimals: v["settings"]["strict_decimals"]
                .as_bool()
                .ok_or_else(|| bad("no settings.strict_decimals"))?,
            statement: text_of("statement")?,
            inputs: v["inputs"]
                .as_array()
                .ok_or_else(|| bad("no inputs"))?
                .iter()
                .map(|i| {
                    i.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| bad("an input"))
                })
                .collect::<Result<_>>()?,
            answer: text_of("answer")?,
        };
        if v["digest"] != r.digest() || v.as_object().is_some_and(|o| o.len() != 7) {
            return Err(bad("its fields do not match its digest"));
        }
        Ok(r)
    }
}

pub(super) fn engine_version() -> String {
    format!(
        "burrmill {}, datafusion {DATAFUSION}",
        env!("CARGO_PKG_VERSION")
    )
}

/// The statement as the parser prints it, so whitespace and comments are not part of it.
pub(super) fn normalised(sql: &str) -> String {
    use sqlparser::dialect::DuckDbDialect;
    match sqlparser::parser::Parser::parse_sql(&DuckDbDialect {}, sql) {
        Ok(s) if s.len() == 1 => s[0].to_string(),
        _ => sql.trim().to_string(),
    }
}

/// `<sha256>:<rows>` of `batches` as the canonical IPC a deterministic engine hands over.
pub(super) fn answer_hash(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<String> {
    let canon = super::deterministic::canonical(schema, batches).map_err(super::df_err)?;
    let rows: usize = canon.iter().map(|b| b.num_rows()).sum();
    Ok(format!("{}:{rows}", sha256(&ipc(schema, &canon)?)))
}

fn ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let io = |e: arrow::error::ArrowError| BurrmillError::Substrate(e.to_string());
    let mut w = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), schema).map_err(io)?;
    for b in batches {
        w.write(b).map_err(io)?;
    }
    w.into_inner().map_err(io)
}

/// What a statement reads: files in a store by location, and rows a host registered.
#[derive(Default)]
pub(super) struct Inputs {
    files: Vec<(
        datafusion_execution::object_store::ObjectStoreUrl,
        object_store::path::Path,
    )>,
    rows: Vec<(SchemaRef, Vec<RecordBatch>)>,
}

/// Every table `plan` reads, through views and subqueries. A table of any other kind is refused:
/// a receipt that left an input out would verify against different data.
pub(super) async fn inputs_of(plan: &LogicalPlan) -> Result<Inputs> {
    let mut providers = Vec::new();
    let mut stack = vec![plan.clone()];
    while let Some(p) = stack.pop() {
        p.apply_with_subqueries(|n| {
            if let LogicalPlan::TableScan(s) = n {
                let provider = source_as_provider(&s.source)?;
                if let Some(view) = provider.get_logical_plan() {
                    stack.push(view.into_owned());
                } else {
                    providers.push((s.table_name.to_string(), provider));
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(super::df_err)?;
    }
    let mut out = Inputs::default();
    for (name, p) in providers {
        if let Some(t) = p.downcast_ref::<SegmentTable>() {
            let (url, located) = t.located();
            out.files
                .extend(located.map(|l| (url.clone(), l.object_meta.location.clone())));
        } else if let Some(m) = p.downcast_ref::<MemTable>() {
            let mut batches = Vec::new();
            for part in &m.batches {
                batches.extend(part.read().await.iter().cloned());
            }
            out.rows.push((m.schema(), batches));
        } else {
            return Err(BurrmillError::NotAllowed(format!(
                "a receipt cannot hash what {name} reads"
            )));
        }
    }
    Ok(out)
}

/// Each input as the receipt names it, sorted and each once.
pub(super) async fn hash_inputs(
    inputs: Inputs,
    env: &datafusion_execution::runtime_env::RuntimeEnv,
) -> Result<Vec<String>> {
    let sub = |e: object_store::Error| BurrmillError::Substrate(e.to_string());
    let mut out = std::collections::BTreeSet::new();
    let mut seen = std::collections::HashSet::new();
    for (url, location) in inputs.files {
        if !seen.insert((url.to_string(), location.to_string())) {
            continue;
        }
        let store = env.object_store(&url).map_err(super::df_err)?;
        let bytes = object_store::ObjectStoreExt::get(&store, &location)
            .await
            .map_err(sub)?
            .bytes()
            .await
            .map_err(sub)?;
        out.insert(format!("parquet:{}:{}", sha256(&bytes), bytes.len()));
    }
    for (schema, batches) in inputs.rows {
        let canon = super::deterministic::canonical(&schema, &batches).map_err(super::df_err)?;
        let rows: usize = canon.iter().map(|b| b.num_rows()).sum();
        out.insert(format!("rows:{}:{rows}", sha256(&ipc(&schema, &canon)?)));
    }
    Ok(out.into_iter().collect())
}

/// The first field in which `got` differs from `want`, as verification reports it.
pub(super) fn mismatch(want: &Receipt, got: &Receipt) -> Option<String> {
    if want.engine != got.engine {
        return Some(format!(
            "made by {}, checked by {}",
            want.engine, got.engine
        ));
    }
    if want.strict_decimals != got.strict_decimals {
        return Some(format!(
            "made with strict decimals {}, checked with {}",
            want.strict_decimals, got.strict_decimals
        ));
    }
    if want.inputs != got.inputs {
        let missing: Vec<&String> = want
            .inputs
            .iter()
            .filter(|i| !got.inputs.contains(i))
            .collect();
        return Some(format!(
            "the inputs differ: {} of {} named are not here",
            missing.len(),
            want.inputs.len()
        ));
    }
    if want.answer != got.answer {
        return Some(format!("the answer differs: {} here", got.answer));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt() -> Receipt {
        Receipt {
            engine: engine_version(),
            strict_decimals: false,
            statement: "SELECT 1".into(),
            inputs: vec!["parquet:ab:3".into()],
            answer: "cd:1".into(),
        }
    }

    #[test]
    fn a_receipt_reads_back_and_any_edit_to_it_is_refused() {
        let text = receipt().to_json();
        assert_eq!(Receipt::parse(&text).unwrap(), receipt());
        for (i, b) in text.bytes().enumerate() {
            let mut t = text.clone().into_bytes();
            t[i] = if b == b'0' { b'1' } else { b'0' };
            if let Ok(s) = String::from_utf8(t) {
                assert!(Receipt::parse(&s).is_err(), "byte {i} of {text}");
            }
        }
    }

    #[test]
    fn the_datafusion_version_is_the_one_pinned() {
        let manifest = include_str!("../../Cargo.toml");
        assert!(
            manifest.contains(&format!("datafusion-sql = {{ version = \"={DATAFUSION}\"")),
            "bump DATAFUSION in receipt.rs with the pins"
        );
    }

    #[test]
    fn a_statement_is_normalised_by_its_parse() {
        assert_eq!(normalised("select  1 -- why\n"), normalised("SELECT 1"));
    }
}
