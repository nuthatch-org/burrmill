//! One uploaded table, named `t`, and the card the page is allowed to send.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Date32Type, Decimal128Type, Field, Int64Type, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use burrmill_ingest::{IngestReport, Issue, Locale, RowIssue, Table};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::chart::{Chart, check_chart};
use crate::format::{
    cell_string, format_date, format_number, format_timestamp, is_ident, is_numeric,
    reserved_suffix, type_name,
};
use crate::narrate::narrate;
use crate::redact::redact;

/// How many result rows the page is handed. The engine's memory budget is what
/// stops a huge plan.
pub const RESULT_ROWS: usize = 5000;
const MEMORY_BYTES: usize = 256 * 1024 * 1024;
const TABLE: &str = "t";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_name: String,
    pub numeric: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
    pub numeric: bool,
    pub temporal: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryOut {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Option<String>>>,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct PresentOut {
    pub chart: Chart,
    pub display: Vec<Vec<Option<String>>>,
    pub narration: Option<String>,
    pub narration_error: Option<String>,
}

pub struct Session {
    engine: Engine,
    batches: Vec<RecordBatch>,
    names: Vec<String>,
    secrets: Vec<String>,
    report: IngestReport,
}

impl Session {
    pub fn open(table: Table) -> Result<Self, String> {
        let report = table.report().clone();
        let batches = table.batches().to_vec();
        if batches.is_empty() || batches[0].num_columns() != report.columns.len() {
            return Err("the file has no columns".into());
        }
        let names = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let secrets = secrets_of(&batches);
        let engine = Engine::open_empty_within(MEMORY_BYTES).map_err(|e| e.to_string())?;
        let mut session = Self {
            engine,
            batches,
            names,
            secrets,
            report,
        };
        session.reregister()?;
        Ok(session)
    }

    pub fn columns(&self) -> Vec<ColumnInfo> {
        self.names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let inferred = &self.report.columns[i].inferred;
                ColumnInfo {
                    name: name.clone(),
                    type_name: inferred.name(),
                    numeric: matches!(
                        inferred,
                        burrmill_ingest::InferredType::Integer
                            | burrmill_ingest::InferredType::Decimal { .. }
                    ),
                    temporal: matches!(
                        inferred,
                        burrmill_ingest::InferredType::Date
                            | burrmill_ingest::InferredType::Timestamp
                    ),
                }
            })
            .collect()
    }

    pub fn preview(&self, n: usize) -> Vec<Vec<Option<String>>> {
        let n = n.min(100);
        let mut rows = Vec::new();
        for batch in &self.batches {
            for r in 0..batch.num_rows() {
                if rows.len() == n {
                    return rows;
                }
                rows.push(batch.columns().iter().map(|c| cell_string(c, r)).collect());
            }
        }
        rows
    }

    pub fn rename(&mut self, from: &str, to: &str) -> Result<(), String> {
        check_ident(to)?;
        let Some(idx) = self.names.iter().position(|n| n == from) else {
            return Err("no such column".into());
        };
        if self
            .names
            .iter()
            .enumerate()
            .any(|(i, n)| i != idx && n == to)
        {
            return Err("column already exists".into());
        }
        self.names[idx] = to.to_string();
        self.reregister()
    }

    /// Names, types, row count, and ingest errors. `signs` and `dates` are the
    /// columns the user ticked. A sign is a word. A date range is the min and max.
    pub fn card(&self, signs: &[String], dates: &[String]) -> Result<String, String> {
        for name in &self.names {
            check_ident(name)?;
        }
        let sign_on: BTreeSet<&str> = signs.iter().map(String::as_str).collect();
        let date_on: BTreeSet<&str> = dates.iter().map(String::as_str).collect();
        let mut columns = Vec::new();
        let mut sign_obj = serde_json::Map::new();
        let mut date_obj = serde_json::Map::new();
        for (i, name) in self.names.iter().enumerate() {
            let col = &self.report.columns[i];
            columns.push(json!({
                "name": name,
                "type": col.inferred.name(),
                "nulls": col.nulls,
            }));
            if sign_on.contains(name.as_str())
                && let Some(label) = sign_label(&self.batches, i)
            {
                sign_obj.insert(name.clone(), json!(label));
            }
            if date_on.contains(name.as_str())
                && let Some((min, max)) = date_bounds(&self.batches, i)
            {
                date_obj.insert(name.clone(), json!({ "min": min, "max": max }));
            }
        }
        let mut obj = json!({
            "table": TABLE,
            "rows": self.report.rows,
            "dialect": dialect_label(&self.report),
            "columns": columns,
            "issues": self.report.rejected.iter().map(|iss| issue_json(&self.report, &self.names, iss)).collect::<Vec<_>>(),
            "issues_omitted": self.report.rejected_omitted,
        });
        if !sign_obj.is_empty() {
            obj["signs"] = Value::Object(sign_obj);
        }
        if !date_obj.is_empty() {
            obj["dates"] = Value::Object(date_obj);
        }
        Ok(obj.to_string())
    }

    pub fn grammar(&self) -> String {
        format!(
            "burrmill subset v1\nSELECT and WITH only. The uploaded table is {TABLE}. No files, no catalogue, no DDL.\nFunctions: {}",
            self.engine.function_names().join(", ")
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn query(&self, sql: &str) -> Result<QueryOut, String> {
        self.guard(sql)?;
        match self.engine.sql(sql) {
            Ok(batches) => Ok(self.pack(batches)),
            Err(e) => Err(self.fail(&e.to_string())),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn query(&self, sql: &str) -> Result<QueryOut, String> {
        self.guard(sql)?;
        match self.engine.sql_async(sql).await {
            Ok(batches) => Ok(self.pack(batches)),
            Err(e) => Err(self.fail(&e.to_string())),
        }
    }

    fn guard(&self, sql: &str) -> Result<(), String> {
        let allowed = BTreeSet::from([TABLE.to_string()]);
        burrmill::inspect::reach_uploads(sql, &allowed).map_err(|e| self.fail(&e.to_string()))
    }

    fn fail(&self, err: &str) -> String {
        redact(err, &self.secrets, &self.names)
    }

    fn pack(&self, batches: Vec<RecordBatch>) -> QueryOut {
        let columns = batches
            .first()
            .map(|b| {
                b.schema()
                    .fields()
                    .iter()
                    .map(|f| Column {
                        name: f.name().clone(),
                        type_name: type_name(f.data_type()),
                        numeric: is_numeric(f.data_type()),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut rows = Vec::new();
        let mut truncated = false;
        'rows: for batch in &batches {
            for r in 0..batch.num_rows() {
                if rows.len() == RESULT_ROWS {
                    truncated = true;
                    break 'rows;
                }
                rows.push(batch.columns().iter().map(|c| cell_string(c, r)).collect());
            }
        }
        QueryOut {
            columns,
            rows,
            truncated,
        }
    }

    fn reregister(&mut self) -> Result<(), String> {
        let mut out = Vec::with_capacity(self.batches.len());
        for batch in &self.batches {
            out.push(rename_batch(batch, &self.names)?);
        }
        self.engine
            .register_batch(TABLE, out)
            .map_err(|e| e.to_string())
    }
}

pub fn feedback_error(message: &str) -> String {
    json!({ "error": message }).to_string()
}

pub fn feedback_shape(out: &QueryOut) -> String {
    let columns: Vec<_> = out
        .columns
        .iter()
        .map(|c| json!({ "name": c.name, "type": c.type_name }))
        .collect();
    json!({
        "rows": out.rows.len(),
        "columns": columns,
        "truncated": out.truncated,
    })
    .to_string()
}

pub fn present(
    answer: &QueryOut,
    chart: &Chart,
    narration: &str,
    group: &str,
    decimal: &str,
) -> PresentOut {
    let chart = check_chart(chart, &answer.columns, answer.rows.len());
    let display = answer
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, cell)| {
                    let raw = cell.as_ref()?;
                    if answer.columns.get(i).is_some_and(|c| c.numeric) {
                        Some(format_number(raw, group, decimal))
                    } else {
                        Some(raw.clone())
                    }
                })
                .collect()
        })
        .collect();
    match narrate(narration, &answer.columns, &answer.rows, group, decimal) {
        Ok(text) => PresentOut {
            chart,
            display,
            narration: Some(text),
            narration_error: None,
        },
        Err(err) => PresentOut {
            chart,
            display,
            narration: None,
            narration_error: Some(err),
        },
    }
}

fn check_ident(name: &str) -> Result<(), String> {
    if !is_ident(name) {
        return Err("column name must be an identifier".into());
    }
    if reserved_suffix(name) {
        return Err("column name is reserved".into());
    }
    Ok(())
}

fn rename_batch(batch: &RecordBatch, names: &[String]) -> Result<RecordBatch, String> {
    if batch.num_columns() != names.len() {
        return Err("column count changed".into());
    }
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .zip(names)
        .map(|(f, n)| Field::new(n, f.data_type().clone(), f.is_nullable()))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
        .map_err(|e| e.to_string())
}

fn secrets_of(batches: &[RecordBatch]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for batch in batches {
        for col in batch.columns() {
            for r in 0..batch.num_rows() {
                if let Some(s) = cell_string(col, r)
                    && s.len() >= 4
                {
                    set.insert(s);
                }
            }
        }
    }
    set.into_iter().collect()
}

fn sign_label(batches: &[RecordBatch], idx: usize) -> Option<&'static str> {
    let mut pos = false;
    let mut neg = false;
    for batch in batches {
        let col = batch.column(idx);
        match col.data_type() {
            DataType::Int64 => {
                let a = col.as_primitive::<Int64Type>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    let v = a.value(i);
                    pos |= v > 0;
                    neg |= v < 0;
                }
            }
            DataType::Decimal128(_, _) => {
                let a = col.as_primitive::<Decimal128Type>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    let v = a.value(i);
                    pos |= v > 0;
                    neg |= v < 0;
                }
            }
            _ => return None,
        }
    }
    Some(match (pos, neg) {
        (false, false) => "none",
        (true, false) => "positive",
        (false, true) => "negative",
        (true, true) => "mixed",
    })
}

fn date_bounds(batches: &[RecordBatch], idx: usize) -> Option<(String, String)> {
    let mut dates: Option<bool> = None;
    let mut min_d = i32::MAX;
    let mut max_d = i32::MIN;
    let mut min_t = i64::MAX;
    let mut max_t = i64::MIN;
    let mut any = false;
    for batch in batches {
        let col = batch.column(idx);
        match col.data_type() {
            DataType::Date32 => {
                if dates == Some(false) {
                    return None;
                }
                dates = Some(true);
                let a = col.as_primitive::<Date32Type>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    any = true;
                    let v = a.value(i);
                    min_d = min_d.min(v);
                    max_d = max_d.max(v);
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                if dates == Some(true) {
                    return None;
                }
                dates = Some(false);
                let a = col.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    any = true;
                    let v = a.value(i);
                    min_t = min_t.min(v);
                    max_t = max_t.max(v);
                }
            }
            _ => return None,
        }
    }
    if !any {
        return None;
    }
    if dates == Some(true) {
        Some((format_date(min_d), format_date(max_d)))
    } else {
        Some((format_timestamp(min_t), format_timestamp(max_t)))
    }
}

fn dialect_label(report: &IngestReport) -> String {
    let d = &report.dialect;
    format!(
        "delim={} quote={} encoding={} locale={} header={}",
        d.delimiter as char,
        d.quote as char,
        d.encoding.name(),
        match d.locale {
            Locale::Point => "point",
            Locale::Comma => "comma",
        },
        report.header,
    )
}

fn issue_json(report: &IngestReport, names: &[String], iss: &RowIssue) -> Value {
    let column = iss.column.as_ref().map(|orig| {
        report
            .columns
            .iter()
            .zip(names)
            .find(|(c, _)| c.name == *orig)
            .map(|(_, n)| n.clone())
            .unwrap_or_else(|| orig.clone())
    });
    let mut v = serde_json::Map::new();
    v.insert("row".into(), json!(iss.row));
    v.insert("column".into(), json!(column));
    match &iss.reason {
        Issue::Ragged { expected, got } => {
            v.insert("reason".into(), json!("ragged"));
            v.insert("expected".into(), json!(expected));
            v.insert("got".into(), json!(got));
        }
        Issue::UnclosedQuote => {
            v.insert("reason".into(), json!("unclosed_quote"));
        }
        Issue::TypeMismatch => {
            v.insert("reason".into(), json!("type_mismatch"));
        }
    }
    Value::Object(v)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::chart::{Chart, ChartKind};
    use burrmill_ingest::{Budget, FormatHint};

    const CSV: &str = "\
posted,merchant,amount
2024-01-02,CANARY_MERCHANT_AAA,-100.00
2024-02-03,CANARY_MERCHANT_BBB,-3.50
2024-03-04,CANARY_MERCHANT_CCC,-90.00
2024-04-05,CANARY_MERCHANT_DDD,-1.00
";

    const FORBIDDEN: &[&str] = &[
        "CANARY_MERCHANT_AAA",
        "CANARY_MERCHANT_BBB",
        "CANARY_MERCHANT_CCC",
        "CANARY_MERCHANT_DDD",
        "-100.00",
        "-3.50",
        "-90.00",
        "-1.00",
        "2024-01-02",
        "2024-02-03",
        "2024-03-04",
        "2024-04-05",
    ];

    fn session() -> Session {
        let table =
            burrmill_ingest::ingest_bytes(CSV.as_bytes(), FormatHint::Sniff, &Budget::untrusted())
                .unwrap();
        Session::open(table).unwrap()
    }

    fn assert_clean(label: &str, text: &str) {
        for word in FORBIDDEN {
            assert!(!text.contains(word), "{label} contains {word}: {text}");
        }
    }

    #[test]
    fn the_card_has_no_cell_and_a_sign_has_no_magnitude() {
        let s = session();
        let card = s.card(&[], &[]).unwrap();
        assert_clean("card", &card);
        assert!(card.contains("\"rows\":4"));
        assert!(card.contains("DECIMAL"));
        let signed = s.card(&["amount".into()], &[]).unwrap();
        assert_clean("sign", &signed);
        assert!(signed.contains("negative"));
        let dated = s.card(&[], &["posted".into()]).unwrap();
        assert!(dated.contains("2024-01-02"), "{dated}");
        assert!(dated.contains("2024-04-05"), "{dated}");
        assert!(!dated.contains("CANARY_MERCHANT_AAA"));
    }

    #[test]
    fn the_sum_is_exact_and_another_table_is_refused() {
        let s = session();
        let out = s.query("SELECT sum(amount) AS total FROM t").unwrap();
        assert_eq!(
            out.rows[0][0].as_deref(),
            Some("-194.50"),
            "{:?} {:?}",
            out.columns,
            out.rows
        );
        let shape = feedback_shape(&out);
        assert_clean("shape", &shape);
        let listed = s.query("SELECT merchant FROM t").unwrap();
        assert!(
            listed
                .rows
                .iter()
                .any(|r| r[0].as_deref() == Some("CANARY_MERCHANT_AAA"))
        );
        assert_clean("listed shape", &feedback_shape(&listed));

        let via = s
            .query("WITH other AS (SELECT sum(amount) AS total FROM t) SELECT total FROM other")
            .unwrap();
        assert_eq!(via.rows[0][0].as_deref(), Some("-194.50"));

        let other = s.query("SELECT * FROM other").unwrap_err();
        assert_clean("other", &other);
        let shadow = s
            .query("WITH t AS (SELECT * FROM other) SELECT * FROM t")
            .unwrap_err();
        assert_clean("shadow", &shadow);

        let cast = s
            .query("SELECT CAST(merchant AS INTEGER) FROM t")
            .unwrap_err();
        assert_clean("cast", &cast);
        assert!(s.grammar().starts_with("burrmill subset v1\n"));
        assert_clean("grammar", &s.grammar());
    }

    #[test]
    fn a_rename_is_what_the_query_sees() {
        let mut s = session();
        assert!(s.rename("amount", "amount__raw").is_err());
        s.rename("amount", "amt").unwrap();
        let out = s.query("SELECT sum(amt) AS total FROM t").unwrap();
        assert_eq!(out.rows[0][0].as_deref(), Some("-194.50"));
        let shown = present(
            &out,
            &Chart {
                kind: ChartKind::Number,
                x: None,
                y: vec!["total".into()],
            },
            "The total is {r0.total}.",
            ",",
            ".",
        );
        assert_eq!(shown.narration.as_deref(), Some("The total is -194.50."));
        assert_eq!(shown.chart.kind, ChartKind::Number);
    }
}
