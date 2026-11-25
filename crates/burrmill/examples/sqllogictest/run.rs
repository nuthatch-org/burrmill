//! One file's records against one engine, each classified.
//!
//! The engine answers queries and nothing else, so the harness keeps the tables DuckDB's setup
//! statements build. `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE VIEW` and
//! transactions are mapped onto [`Engine::register_batch`], [`Engine::register_view`] and
//! [`Engine::drop_relation`], and every value they write is computed by the engine: the column
//! types are the engine's for `CAST(NULL AS <type>)`, the rows its answer to the `VALUES` or
//! `SELECT` with each value cast to its column's type, an update its `CASE WHEN` over the table.
//! What the harness cannot map skips the record, and when DuckDB would have changed state there,
//! skips the rest of the file, so no answer is compared against state DuckDB never had.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema, SchemaRef};
use burrmill::{BurrmillError, Engine, Stop};
use sqlparser::ast::{
    self, AssignmentTarget, ColumnOption, Expr, FromTable, ObjectName, ObjectNamePart, ObjectType,
    SetExpr, Statement, TableConstraint, TableFactor,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use crate::parse::{Expect, Item, Kind, Record, Script, Sort};
use crate::render;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Class {
    Pass,
    Refused,
    Wrong,
    Error,
    Skipped,
}

impl Class {
    pub fn name(self) -> &'static str {
        match self {
            Class::Pass => "pass",
            Class::Refused => "refused",
            Class::Wrong => "wrong",
            Class::Error => "error",
            Class::Skipped => "skipped",
        }
    }
    pub fn parse(s: &str) -> Option<Class> {
        Some(match s {
            "pass" => Class::Pass,
            "refused" => Class::Refused,
            "wrong" => Class::Wrong,
            "error" => Class::Error,
            "skipped" => Class::Skipped,
            _ => return None,
        })
    }
}

/// A failure: its text, and whether the engine gave it or the harness decided it alone.
#[derive(Debug)]
struct Fail {
    msg: String,
    engine: Option<BurrmillError>,
}

impl Fail {
    fn harness(msg: impl Into<String>) -> Fail {
        Fail {
            msg: msg.into(),
            engine: None,
        }
    }
}

impl From<BurrmillError> for Fail {
    fn from(e: BurrmillError) -> Fail {
        Fail {
            msg: e.to_string(),
            engine: Some(e),
        }
    }
}

/// What one statement produced.
enum Done {
    /// The engine's answer.
    Rows(SchemaRef, Vec<RecordBatch>),
    /// A mapped write: rows affected, and whether the engine computed anything for it.
    Count(u64, bool),
    /// A mapped `CREATE`, which answers no rows in DuckDB; whether the engine computed anything.
    Empty(bool),
    /// Mapped by the harness alone: nothing for the engine to answer.
    Harness,
}

/// How a statement is to be run.
enum Plan {
    Engine(String),
    Mapped(Box<Statement>),
    /// `CREATE TABLE name AS <query>`, the query as written.
    Ctas {
        name: String,
        or_replace: bool,
        if_not_exists: bool,
        query: String,
    },
    /// `INSERT INTO name [(columns)] <query>`, the query as written.
    InsertQuery {
        name: String,
        columns: Vec<String>,
        query: String,
    },
    /// A setting that changes nothing an answer depends on.
    NoOp(String),
    /// Something the harness cannot give; `changes` if DuckDB would have changed state with it.
    Unmappable {
        why: String,
        changes: bool,
    },
}

#[derive(Clone)]
struct Column {
    name: String,
    /// The type as the `CAST` writing a value names it.
    ty: String,
    not_null: bool,
    default: Option<String>,
}

#[derive(Clone)]
struct Table {
    name: String,
    columns: Vec<Column>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    /// Column sets that must be unique; `true` for a primary key.
    keys: Vec<(Vec<usize>, bool)>,
}

#[derive(Clone, Default)]
struct State {
    tables: BTreeMap<String, Table>,
    /// In creation order: (key, name as written, body).
    views: Vec<(String, String, String)>,
}

pub struct Session {
    engine: Engine,
    state: State,
    txn: Option<State>,
    labels: HashMap<String, String>,
    ignore: Vec<String>,
    /// A resource limit the file set that the harness does not impose, so that an error DuckDB
    /// raises under it says nothing about the engine.
    limited: Option<String>,
    timeout: Duration,
}

/// A record's outcome, and whether the rest of the file can still run.
pub struct Outcome {
    pub class: Class,
    pub detail: String,
    pub stop: Option<String>,
}

fn outcome(class: Class, detail: impl Into<String>) -> Outcome {
    Outcome {
        class,
        detail: detail.into(),
        stop: None,
    }
}

/// Each file's engine's working memory.
const MEMORY_BYTES: usize = 2 << 30;

/// Further runs of a passing `nosort` query, to catch an order that is not stable.
const REPEATS: usize = 4;

/// Whether an engine error names a feature it does not support.
fn refusal(e: &BurrmillError) -> bool {
    if matches!(e, BurrmillError::NotAllowed(_)) {
        return true;
    }
    let m = e.to_string().to_ascii_lowercase();
    m.contains("not supported")
        || m.contains("not implemented")
        || m.contains("unsupported")
        || m.contains("refusing")
        || m.contains("invalid function '")
        || m.contains("no function matches the given name and argument types")
}

fn classify(e: &BurrmillError) -> Class {
    if refusal(e) {
        Class::Refused
    } else {
        Class::Error
    }
}

fn one_line(s: &str) -> String {
    let s: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.chars().count() > 300 {
        s.chars().take(300).collect::<String>() + "..."
    } else {
        s
    }
}

fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn key(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// The last part of a name, if the rest is DuckDB's default catalog and schema.
fn table_name(n: &ObjectName) -> Option<String> {
    let parts: Vec<String> =
        n.0.iter()
            .map(|p| match p {
                ObjectNamePart::Identifier(i) => Some(i.value.clone()),
                _ => None,
            })
            .collect::<Option<_>>()?;
    let (last, rest) = parts.split_last()?;
    let ok = match rest {
        [] => true,
        [s] => s.eq_ignore_ascii_case("main") || s.eq_ignore_ascii_case("temp"),
        [c, s] => {
            (c.eq_ignore_ascii_case("memory") || c.eq_ignore_ascii_case("temp"))
                && s.eq_ignore_ascii_case("main")
        }
        _ => false,
    };
    ok.then(|| last.clone())
}

/// Splits on top-level `;`, outside quotes and comments.
pub fn split_statements(sql: &str) -> Vec<String> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"') => {
                i += 1;
                while i < b.len() {
                    if b[i] == q {
                        if i + 1 < b.len() && b[i + 1] == q {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 1;
            }
            b'$' if b.get(i + 1) == Some(&b'$') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'$' && b[i + 1] == b'$') {
                    i += 1;
                }
                i += 1;
            }
            b';' => {
                let piece = sql[start..i].trim();
                if !strip_comments(piece).is_empty() {
                    out.push(piece.to_string());
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let piece = sql[start.min(sql.len())..].trim();
    if !strip_comments(piece).is_empty() {
        out.push(piece.to_string());
    }
    out
}

fn strip_comments(s: &str) -> String {
    s.lines()
        .map(|l| l.split("--").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

/// Leading words, upper-cased, comments skipped.
fn words(sql: &str, n: usize) -> Vec<String> {
    strip_comments(sql)
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';' || c == '=')
        .filter(|w| !w.is_empty())
        .take(n)
        .map(|w| w.to_ascii_uppercase())
        .collect()
}

/// Settings that change nothing a statement answers: threads, memory, storage, profiling,
/// verification, optimizer switches.
/// Of [`NO_OP_SETTINGS`], those under which DuckDB may refuse a statement the engine then answers.
const LIMIT_SETTINGS: &[&str] = &[
    "memory_limit",
    "max_memory",
    "temp_directory",
    "max_temp_directory_size",
    "max_expression_depth",
    "max_execution_time",
];

const NO_OP_SETTINGS: &[&str] = &[
    "threads",
    "worker_threads",
    "memory_limit",
    "max_memory",
    "disable_checkpoint_on_shutdown",
    "enable_checkpoint_on_shutdown",
    "force_compression",
    "enable_profiling",
    "disable_profiling",
    "enable_profile",
    "profiling_output",
    "profile_output",
    "profiling_mode",
    "profiling_coverage",
    "profiling_renderer_settings",
    "custom_profiling_settings",
    "tracked_metrics",
    "wal_autocheckpoint",
    "checkpoint_threshold",
    "verify_parallelism",
    "disable_verify_parallelism",
    "enable_verification",
    "disable_verification",
    "verify_external",
    "disable_verify_external",
    "verify_serializer",
    "disable_verify_serializer",
    "verify_fetch_row",
    "debug_verify_statement",
    "debug_verify_serializer",
    "debug_force_external",
    "debug_force_fetch_row",
    "debug_force_no_cross_product",
    "debug_checkpoint_abort",
    "debug_window_mode",
    "debug_asof_iejoin",
    "debug_skip_checkpoint_on_commit",
    "debug_disable_optimizer",
    "explain_output",
    "temp_directory",
    "max_temp_directory_size",
    "read_ahead_depth",
    "enable_object_cache",
    "disable_object_cache",
    "enable_progress_bar",
    "disable_progress_bar",
    "enable_progress_bar_print",
    "disable_print_progress_bar",
    "progress_bar_time",
    "disabled_optimizers",
    "disable_optimizer",
    "enable_optimizer",
    "index_scan_percentage",
    "index_scan_max_count",
    "max_vacuum_tasks",
    "partitioned_write_flush_threshold",
    "partitioned_write_max_open_files",
    "enable_logging",
    "logging_level",
    "logging_storage",
    "storage_compatibility_version",
    "immediate_transaction_mode",
    "force_bitpacking_mode",
    "variant_minimum_shredding_size",
    "force_variant_shredding",
    "perfect_ht_threshold",
    "ordered_aggregate_threshold",
    "asof_loop_join_threshold",
    "merge_join_threshold",
    "nested_loop_join_threshold",
    "prefer_range_joins",
    "preserve_insertion_order",
    "enable_http_metadata_cache",
    "enable_external_file_cache",
    "allocator_flush_threshold",
    "allocator_background_threads",
    "dynamic_or_filter_threshold",
    "late_materialization_max_rows",
    "streaming_buffer_size",
    "max_expression_depth",
    "max_execution_time",
    "enable_caching_operators",
    "initial_column_segment_size",
    "extension_repository_directory",
    "pivot_filter_threshold",
    "checkpoint_on_shutdown",
];

fn setting_name(sql: &str) -> String {
    let w = words(sql, 3);
    let mut name = w.get(1).cloned().unwrap_or_default();
    if matches!(name.as_str(), "GLOBAL" | "SESSION" | "LOCAL") {
        name = w.get(2).cloned().unwrap_or_default();
    }
    name.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .to_ascii_lowercase()
}

fn plan(sql: &str, in_query: bool) -> Plan {
    let w = words(sql, 3);
    let first = w.first().map(String::as_str).unwrap_or("");
    match first {
        "PRAGMA" | "SET" | "RESET" => {
            let name = setting_name(sql);
            if name == "variable" {
                return Plan::Unmappable {
                    why: "SET VARIABLE".into(),
                    changes: true,
                };
            }
            if NO_OP_SETTINGS.contains(&name.as_str()) {
                return Plan::NoOp(format!("{first} {name}"));
            }
            if first == "SET" && name == "timezone" && sql.to_ascii_lowercase().contains("utc") {
                return Plan::NoOp("SET TimeZone UTC".into());
            }
            if first == "PRAGMA" && in_query {
                return Plan::Engine(sql.to_string());
            }
            return Plan::Unmappable {
                why: format!("{first} {name}"),
                changes: true,
            };
        }
        "CHECKPOINT" | "VACUUM" | "ANALYZE" => return Plan::NoOp(first.into()),
        "FORCE" if w.get(1).is_some_and(|w| w == "CHECKPOINT") => {
            return Plan::NoOp("FORCE CHECKPOINT".into());
        }
        "LOAD" | "INSTALL" | "FORCE" | "ATTACH" | "DETACH" | "USE" | "COPY" | "EXPORT"
        | "IMPORT" | "ALTER" | "COMMENT" | "PREPARE" | "EXECUTE" | "DEALLOCATE" => {
            return Plan::Unmappable {
                why: first.into(),
                changes: true,
            };
        }
        _ => {}
    }
    if let Some(p) = text_plan(sql, false) {
        return p;
    }
    if first == "EXPLAIN" {
        // EXPLAIN ANALYZE runs its statement, so one of a write changes state.
        let analyzed_write = w.get(1).is_some_and(|w| w == "ANALYZE")
            && (w.get(2).is_some_and(|w| {
                matches!(
                    w.as_str(),
                    "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "CREATE" | "DROP" | "ALTER" | "COPY"
                )
            }) || dml_text(sql));
        return Plan::Unmappable {
            why: "EXPLAIN: DuckDB's own plan".into(),
            changes: analyzed_write,
        };
    }
    let parsed = Parser::parse_sql(&DuckDbDialect {}, sql);
    let stmt = match parsed {
        Ok(mut v) if v.len() == 1 => v.remove(0),
        _ => {
            if let Some(p) = text_plan(sql, true) {
                return p;
            }
            return match first {
                "SELECT" | "WITH" | "FROM" | "VALUES" | "TABLE" | "DESCRIBE" | "DESC" | "SHOW"
                | "SUMMARIZE" | "EXPLAIN" | "PIVOT" | "UNPIVOT" | "CALL" | "" => {
                    Plan::Engine(sql.to_string())
                }
                _ if first.starts_with('(') => Plan::Engine(sql.to_string()),
                _ => Plan::Unmappable {
                    why: format!("{first} the harness cannot parse"),
                    changes: true,
                },
            };
        }
    };
    match &stmt {
        Statement::Query(_)
        | Statement::Explain { .. }
        | Statement::ExplainTable { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowVariable { .. }
        | Statement::Pragma { .. } => Plan::Engine(sql.to_string()),
        Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::CreateTable(_)
        | Statement::CreateView(_)
        | Statement::CreateIndex(_)
        | Statement::Drop { .. }
        | Statement::Truncate(_)
        | Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. } => Plan::Mapped(Box::new(stmt)),
        other => Plan::Unmappable {
            why: one_line(&format!("{}", other))
                .split_whitespace()
                .take(2)
                .collect::<Vec<_>>()
                .join(" "),
            changes: true,
        },
    }
}

const NAME: &str = r#"(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*))*"#;
const IDENT: &str = r#"(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)"#;

/// A name as written, through [`table_name`]'s rule.
fn text_name(s: &str) -> Option<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for c in s.chars() {
        match c {
            '"' => quoted = !quoted,
            '.' if !quoted => parts.push(std::mem::take(&mut cur)),
            c => cur.push(c),
        }
    }
    parts.push(cur);
    let name = ObjectName(
        parts
            .into_iter()
            .map(|p| ObjectNamePart::Identifier(ast::Ident::new(p)))
            .collect(),
    );
    table_name(&name)
}

/// `CREATE TABLE ... AS` and `INSERT INTO ... SELECT`, recognised in the text so the query runs as
/// written, whether or not sqlparser reads DuckDB's syntax in it. `values` admits a `VALUES`
/// source too, for an `INSERT` sqlparser could not read.
fn text_plan(sql: &str, values: bool) -> Option<Plan> {
    let ctas = regex::Regex::new(&format!(
        r"(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?(?:(?:TEMP|TEMPORARY)\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?({NAME})\s+AS\s+(.+?)\s*;?\s*$"
    ))
    .ok()?;
    if let Some(c) = ctas.captures(sql) {
        return Some(match text_name(&c[3]) {
            Some(name) => Plan::Ctas {
                name,
                or_replace: c.get(1).is_some(),
                if_not_exists: c.get(2).is_some(),
                query: c[4].to_string(),
            },
            None => Plan::Unmappable {
                why: "a schema the harness has not".into(),
                changes: true,
            },
        });
    }
    let sources = if values {
        "SELECT|WITH|FROM|VALUES"
    } else {
        "SELECT|WITH|FROM"
    };
    let ins = regex::Regex::new(&format!(
        r"(?is)^\s*INSERT\s+INTO\s+({NAME})\s*(?:\(\s*({IDENT}(?:\s*,\s*{IDENT})*)\s*\))?\s*((?:{sources})\b.*?|\(\s*(?:SELECT|WITH|FROM|VALUES|\()(?:\b|\().*?)\s*;?\s*$"
    ))
    .ok()?;
    let c = ins.captures(sql)?;
    let name = text_name(&c[1])?;
    let columns = c
        .get(2)
        .map(|m| {
            m.as_str()
                .split(',')
                .map(|s| s.trim().trim_matches('"').to_string())
                .collect()
        })
        .unwrap_or_default();
    Some(Plan::InsertQuery {
        name,
        columns,
        query: c[3].to_string(),
    })
}

/// `N values hashing to <md5>`, as DuckDB's runner writes a hashed result.
fn is_hash(s: &str) -> bool {
    let Some((n, rest)) = s.split_once(" values hashing to ") else {
        return false;
    };
    !n.is_empty()
        && n.bytes().all(|b| b.is_ascii_digit())
        && rest.len() == 32
        && rest
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
}

fn hash(values: &[String]) -> String {
    let mut buf = Vec::new();
    for v in values {
        buf.extend_from_slice(v.as_bytes());
        buf.push(b'\n');
    }
    format!(
        "{} values hashing to {}",
        values.len(),
        crate::md5::hex(&buf)
    )
}

/// DuckDB's `CheckQueryResult`, stricter in one place: where it compares only a hash (a label,
/// or past the hash threshold) and the file also lists values, the values are compared too.
fn check(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    columns: usize,
    sort: Sort,
    label: &Option<String>,
    expected: &[String],
    labels: &mut HashMap<String, String>,
) -> Result<(), String> {
    let ncols = schema.fields().len();
    let classes: Vec<render::Class> = schema
        .fields()
        .iter()
        .map(|f| render::class(f.data_type()))
        .collect();
    let mut values = Vec::new();
    let mut nrows = 0;
    for b in batches {
        for r in 0..b.num_rows() {
            for c in 0..ncols {
                values.push(render::top(b.column(c), r));
            }
        }
        nrows += b.num_rows();
    }
    match sort {
        Sort::None => {}
        Sort::Value => values.sort(),
        Sort::Row => {
            if ncols > 0 {
                let mut rows: Vec<Vec<String>> = values.chunks(ncols).map(|r| r.to_vec()).collect();
                rows.sort();
                values = rows.concat();
            }
        }
    }
    let digest = hash(&values);
    if let Some(l) = label {
        match labels.get(l) {
            Some(h) if *h != digest => {
                return Err(format!("result differs from label {l}'s earlier result"));
            }
            Some(_) => {}
            None => {
                labels.insert(l.clone(), digest.clone());
            }
        }
        if expected.is_empty() {
            return Ok(());
        }
    }
    if expected.len() == 1 && is_hash(&expected[0]) {
        return if expected[0] == digest {
            Ok(())
        } else {
            Err(format!("{digest}, expected {}", expected[0]))
        };
    }
    if ncols != columns {
        return Err(format!("{ncols} columns, expected {columns}"));
    }
    if ncols == 0 {
        return Err("no columns".into());
    }
    let mut row_wise = ncols > 1 && expected.len() == nrows;
    if !row_wise {
        row_wise = !expected.is_empty() && expected.iter().all(|v| v.contains('\t'));
    }
    let expected_rows = if row_wise {
        expected.len()
    } else {
        if !expected.len().is_multiple_of(ncols) {
            return Err(format!(
                "{} expected values do not divide into {ncols} columns",
                expected.len()
            ));
        }
        expected.len() / ncols
    };
    if expected_rows != nrows {
        return Err(format!("{nrows} rows, expected {expected_rows}"));
    }
    let flat: Vec<String> = if row_wise {
        let mut flat = Vec::new();
        for (i, line) in expected.iter().enumerate() {
            // DuckDB's `StringUtil::Split`: empty pieces are dropped, so a trailing tab or two
            // tabs together separate nothing, and a line of nothing but tabs is one value.
            let mut parts: Vec<&str> = line.split('\t').filter(|p| !p.is_empty()).collect();
            if parts.is_empty() {
                parts.push(line);
            }
            if parts.len() != ncols {
                return Err(format!(
                    "expected row {} has {} values, the answer {ncols} columns",
                    i + 1,
                    parts.len()
                ));
            }
            flat.extend(parts.into_iter().map(str::to_string));
        }
        flat
    } else {
        expected.to_vec()
    };
    for (i, (a, e)) in values.iter().zip(&flat).enumerate() {
        let col = i % ncols;
        if !render::equal(a, e, classes[col]) {
            return Err(format!(
                "row {} column {}: {} <> {}",
                i / ncols + 1,
                col + 1,
                one_line(a),
                one_line(e)
            ));
        }
    }
    Ok(())
}

fn error_matches(expected: &str, actual: &str) -> bool {
    if expected.is_empty() || actual.contains(expected) {
        return true;
    }
    let pattern = expected
        .strip_prefix("<REGEX>:")
        .map(|p| (p, true))
        .or_else(|| expected.strip_prefix("<!REGEX>:").map(|p| (p, false)));
    if let Some((p, want)) = pattern
        && let Ok(re) = regex::RegexBuilder::new(&format!("^(?:{p})$"))
            .dot_matches_new_line(true)
            .build()
    {
        return re.is_match(&format!("{actual}\n")) == want;
    }
    false
}

impl Session {
    pub fn new(timeout: Duration) -> Result<Session, BurrmillError> {
        Ok(Session {
            // One runtime worker as well as one partition, so the order of rows a statement leaves
            // unordered (a UNION ALL's branches) does not depend on scheduling; and a bound, so a
            // statement that would take the machine's memory is refused instead.
            engine: Engine::open_empty_budgeted(burrmill::Budget {
                memory_bytes: MEMORY_BYTES,
                threads: 1,
                spill: None,
            })?,
            state: State::default(),
            txn: None,
            labels: HashMap::new(),
            ignore: Vec::new(),
            limited: None,
            timeout,
        })
    }

    fn query(&self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), BurrmillError> {
        let stop = Stop {
            deadline: Some(Instant::now() + self.timeout),
            ..Default::default()
        };
        let mut out = Vec::new();
        self.engine
            .sql_for_each_stoppable(sql, &[], &stop, None, |b| {
                out.push(b);
                Ok(())
            })?;
        let schema = match out.first() {
            Some(b) => b.schema(),
            None => self.engine.result_schema(sql)?,
        };
        Ok((schema, out))
    }

    /// Whether the engine's physical plan for `sql` leaves the order of its rows to scheduling:
    /// the operators a row passes on its way out, from the root down, reach a [`MERGES`] before
    /// any [`SORTS`]. A plan the engine will not explain is taken as fixing its order, so the
    /// repeats alone judge it.
    fn order_unfixed(&self, sql: &str) -> bool {
        let Ok((_, batches)) = self.query(&format!("EXPLAIN {sql}")) else {
            return false;
        };
        let mut lines = Vec::new();
        for b in &batches {
            let (Some(kind), Some(plan)) = (
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>(),
                b.column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>(),
            ) else {
                return false;
            };
            for i in 0..b.num_rows() {
                if kind.value(i) == "physical_plan" {
                    lines.extend(plan.value(i).lines().map(str::to_string));
                }
            }
        }
        unordered_plan(&lines)
    }

    /// Rebinds every table and view from the harness's state, as after a rollback.
    fn rebind_all(&mut self, before: &State) -> Result<(), Fail> {
        for (k, t) in &before.tables {
            if !self.state.tables.contains_key(k) {
                self.engine.drop_relation(&t.name);
            }
        }
        for (k, n, _) in &before.views {
            if !self.state.views.iter().any(|(k2, _, _)| k2 == k) {
                self.engine.drop_relation(n);
            }
        }
        let tables: Vec<Table> = self.state.tables.values().cloned().collect();
        for t in tables {
            self.bind_table(&t)?;
        }
        self.rebind_views();
        Ok(())
    }

    fn bind_table(&mut self, t: &Table) -> Result<(), Fail> {
        let batches = if t.batches.is_empty() {
            vec![RecordBatch::new_empty(t.schema.clone())]
        } else {
            t.batches.clone()
        };
        self.engine.register_batch(&t.name, batches)?;
        Ok(())
    }

    /// Views are planned when registered, so they are planned again after every write, as
    /// DuckDB binds a view each time it is read. A view whose tables have gone is left unbound.
    fn rebind_views(&mut self) {
        let views = self.state.views.clone();
        for (_, name, body) in views {
            self.engine.drop_relation(&name);
            let _ = self.engine.register_view(&name, &body);
        }
    }

    fn table(&self, n: &ObjectName) -> Result<Table, Fail> {
        let name = table_name(n).ok_or_else(|| Fail::harness("a schema the harness has not"))?;
        self.state.tables.get(&key(&name)).cloned().ok_or_else(|| {
            Fail::harness(format!(
                "Catalog Error: Table with name {name} does not exist!"
            ))
        })
    }

    fn exists(&self, k: &str) -> bool {
        self.state.tables.contains_key(k) || self.state.views.iter().any(|(v, _, _)| v == k)
    }

    /// The table's rows replaced by `batches`, checked against its constraints, and bound.
    fn store(&mut self, mut t: Table, batches: Vec<RecordBatch>) -> Result<(), Fail> {
        let mut fixed = Vec::new();
        for b in batches {
            if b.num_columns() != t.columns.len() {
                return Err(Fail::harness(format!(
                    "the engine answered {} columns for {}'s {}",
                    b.num_columns(),
                    t.name,
                    t.columns.len()
                )));
            }
            let cols: Vec<ArrayRef> = b
                .columns()
                .iter()
                .zip(t.schema.fields())
                .map(|(c, f)| {
                    if c.data_type() == f.data_type() {
                        Ok(c.clone())
                    } else {
                        arrow::compute::cast(c, f.data_type())
                            .map_err(|e| Fail::harness(format!("cast to the table's type: {e}")))
                    }
                })
                .collect::<Result<_, _>>()?;
            if b.num_rows() > 0 {
                fixed.push(
                    RecordBatch::try_new(t.schema.clone(), cols)
                        .map_err(|e| Fail::harness(e.to_string()))?,
                );
            }
        }
        for (i, c) in t.columns.iter().enumerate() {
            if c.not_null && fixed.iter().any(|b| b.column(i).logical_null_count() > 0) {
                return Err(Fail::harness(format!(
                    "Constraint Error: NOT NULL constraint failed: {}.{}",
                    t.name, c.name
                )));
            }
        }
        for (cols, primary) in &t.keys {
            let mut seen = HashSet::new();
            for b in &fixed {
                'rows: for r in 0..b.num_rows() {
                    let mut k = Vec::new();
                    for &c in cols {
                        if render::null_at(b.column(c), r) {
                            if *primary {
                                return Err(Fail::harness(format!(
                                    "Constraint Error: NOT NULL constraint failed: {}.{}",
                                    t.name, t.columns[c].name
                                )));
                            }
                            continue 'rows;
                        }
                        k.push(render::top(b.column(c), r));
                    }
                    if !seen.insert(k.join("\u{1f}")) {
                        let kind = if *primary { "primary key" } else { "unique" };
                        return Err(Fail::harness(format!(
                            "Constraint Error: Duplicate key \"{}\" violates {kind} constraint.",
                            k.join(", ")
                        )));
                    }
                }
            }
        }
        t.batches = fixed;
        self.bind_table(&t)?;
        self.state.tables.insert(key(&t.name), t);
        self.rebind_views();
        Ok(())
    }

    /// The schema of `CAST(NULL AS <type>)` for each column: the engine's types for DuckDB's.
    fn types(&self, cols: &[Column]) -> Result<SchemaRef, Fail> {
        let list: Vec<String> = cols
            .iter()
            .map(|c| format!("CAST(NULL AS {}) AS {}", c.ty, ident(&c.name)))
            .collect();
        let schema = self
            .engine
            .result_schema(&format!("SELECT {}", list.join(", ")))?;
        Ok(Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .zip(cols)
                .map(|(f, c)| Field::new(&c.name, f.data_type().clone(), true))
                .collect::<Vec<_>>(),
        )))
    }

    fn run_mapped(&mut self, stmt: &Statement) -> Result<Done, Fail> {
        match stmt {
            Statement::StartTransaction { .. } => {
                if self.txn.is_some() {
                    return Err(Fail::harness(
                        "TransactionContext Error: cannot start a transaction within a transaction",
                    ));
                }
                self.txn = Some(self.state.clone());
                Ok(Done::Harness)
            }
            Statement::Commit { .. } => {
                if self.txn.take().is_none() {
                    return Err(Fail::harness(
                        "TransactionContext Error: cannot commit - no transaction is active",
                    ));
                }
                Ok(Done::Harness)
            }
            Statement::Rollback { savepoint, .. } => {
                if savepoint.is_some() {
                    return Err(Fail::harness("ROLLBACK TO SAVEPOINT"));
                }
                let Some(before) = self.txn.take() else {
                    return Err(Fail::harness(
                        "TransactionContext Error: cannot rollback - no transaction is active",
                    ));
                };
                let now = std::mem::replace(&mut self.state, before);
                self.rebind_all(&now)?;
                Ok(Done::Harness)
            }
            Statement::CreateTable(ct) => self.create_table(ct),
            Statement::CreateView(cv) => {
                if cv.materialized {
                    return Err(Fail::harness("MATERIALIZED VIEW"));
                }
                let name = table_name(&cv.name)
                    .ok_or_else(|| Fail::harness("a schema the harness has not"))?;
                let k = key(&name);
                if self.state.tables.contains_key(&k) {
                    return Err(Fail::harness(format!(
                        "Catalog Error: Existing object {name} is of type Table, trying to replace with type View"
                    )));
                }
                let exists = self.state.views.iter().any(|(v, _, _)| *v == k);
                if exists && cv.if_not_exists {
                    return Ok(Done::Harness);
                }
                if exists && !cv.or_replace {
                    return Err(Fail::harness(format!(
                        "Catalog Error: View with name \"{name}\" already exists!"
                    )));
                }
                let mut body = cv.query.to_string();
                if !cv.columns.is_empty() {
                    let names: Vec<String> =
                        cv.columns.iter().map(|c| ident(&c.name.value)).collect();
                    // Fewer names than columns rename the first ones, as DuckDB does.
                    let (schema, _) = (self.engine.result_schema(&body)?, ());
                    if names.len() > schema.fields().len() {
                        return Err(Fail::harness(
                            "Binder Error: view has more column names than the query has columns",
                        ));
                    }
                    let mut all = names.clone();
                    for f in schema.fields().iter().skip(names.len()) {
                        all.push(ident(f.name()));
                    }
                    body = format!("SELECT * FROM ({body}) AS __v({})", all.join(", "));
                }
                if exists {
                    self.engine.drop_relation(&name);
                }
                if let Err(e) = self.engine.register_view(&name, &body) {
                    if exists {
                        self.rebind_views();
                    }
                    return Err(e.into());
                }
                self.state.views.retain(|(v, _, _)| *v != k);
                self.state.views.push((k, name, body));
                Ok(Done::Empty(true))
            }
            Statement::CreateIndex(ci) => {
                if ci.unique {
                    return Err(Fail::harness("CREATE UNIQUE INDEX"));
                }
                self.table(&ci.table_name)?;
                Ok(Done::Harness)
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => {
                let view = match object_type {
                    ObjectType::Table => false,
                    ObjectType::View => true,
                    ObjectType::Index => return Ok(Done::Harness),
                    other => return Err(Fail::harness(format!("DROP {other}"))),
                };
                for n in names {
                    let name = table_name(n)
                        .ok_or_else(|| Fail::harness("a schema the harness has not"))?;
                    let k = key(&name);
                    let is_table = self.state.tables.contains_key(&k);
                    let is_view = self.state.views.iter().any(|(v, _, _)| *v == k);
                    if !is_table && !is_view {
                        if *if_exists {
                            continue;
                        }
                        let kind = if view { "View" } else { "Table" };
                        return Err(Fail::harness(format!(
                            "Catalog Error: {kind} with name {name} does not exist!"
                        )));
                    }
                    if is_table == view {
                        let (has, want) = if view {
                            ("Table", "View")
                        } else {
                            ("View", "Table")
                        };
                        return Err(Fail::harness(format!(
                            "Catalog Error: Existing object {name} is of type {has}, trying to drop type {want}"
                        )));
                    }
                    if view {
                        let reg = self
                            .state
                            .views
                            .iter()
                            .find(|(v, _, _)| *v == k)
                            .map(|(_, n, _)| n.clone())
                            .unwrap_or_default();
                        self.state.views.retain(|(v, _, _)| *v != k);
                        self.engine.drop_relation(&reg);
                    } else if let Some(t) = self.state.tables.remove(&k) {
                        self.engine.drop_relation(&t.name);
                    }
                }
                self.rebind_views();
                Ok(Done::Harness)
            }
            Statement::Insert(ins) => self.insert(ins),
            Statement::Delete(del) => self.delete(del),
            Statement::Truncate(tr) => {
                for target in &tr.table_names {
                    let t = self.table(&target.name)?;
                    self.store(t, Vec::new())?;
                }
                Ok(Done::Harness)
            }
            Statement::Update(up) => self.update(up),
            _ => Err(Fail::harness("unmapped statement")),
        }
    }

    fn create_table(&mut self, ct: &ast::CreateTable) -> Result<Done, Fail> {
        if ct.like.is_some() || ct.clone.is_some() || ct.external {
            return Err(Fail::harness("CREATE TABLE LIKE/CLONE"));
        }
        let name =
            table_name(&ct.name).ok_or_else(|| Fail::harness("a schema the harness has not"))?;
        if !self.may_create(&name, ct.or_replace, ct.if_not_exists)? {
            return Ok(Done::Harness);
        }
        if let Some(q) = &ct.query {
            if !ct.columns.is_empty() {
                return Err(Fail::harness("CREATE TABLE with columns AS"));
            }
            return self.ctas_into(name, &q.to_string());
        }
        if ct.columns.is_empty() {
            return Err(Fail::harness("CREATE TABLE without columns"));
        }
        self.create_columns(name, ct)
    }

    /// Whether `CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name` goes ahead.
    fn may_create(&self, name: &str, or_replace: bool, if_not_exists: bool) -> Result<bool, Fail> {
        let k = key(name);
        if self.exists(&k) {
            if if_not_exists {
                return Ok(false);
            }
            let is_view = !self.state.tables.contains_key(&k);
            if !or_replace || is_view {
                return Err(Fail::harness(format!(
                    "Catalog Error: Table with name \"{name}\" already exists!"
                )));
            }
        }
        Ok(true)
    }

    fn ctas_into(&mut self, name: String, query: &str) -> Result<Done, Fail> {
        let k = key(&name);
        {
            let (schema, batches) = self.query(query)?;
            let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let mut seen = HashSet::new();
            for f in schema.fields() {
                if !seen.insert(key(f.name())) {
                    return Err(Fail::harness(format!(
                        "Binder Error: Column with name {} already exists!",
                        f.name()
                    )));
                }
            }
            let columns = schema
                .fields()
                .iter()
                .map(|f| Column {
                    name: f.name().clone(),
                    ty: burrmill::df::duckdb_type(f.data_type()),
                    not_null: false,
                    default: None,
                })
                .collect();
            let schema = Arc::new(Schema::new(
                schema
                    .fields()
                    .iter()
                    .map(|f| Field::new(f.name(), f.data_type().clone(), true))
                    .collect::<Vec<_>>(),
            ));
            if self.state.tables.contains_key(&k) {
                self.state.tables.remove(&k);
            }
            let t = Table {
                name,
                columns,
                schema,
                batches: Vec::new(),
                keys: Vec::new(),
            };
            self.store(t, batches)?;
            Ok(Done::Count(rows, true))
        }
    }

    fn create_columns(&mut self, name: String, ct: &ast::CreateTable) -> Result<Done, Fail> {
        let k = key(&name);
        let mut columns = Vec::new();
        let mut keys = Vec::new();
        let mut seen = HashSet::new();
        for (i, c) in ct.columns.iter().enumerate() {
            if !seen.insert(key(&c.name.value)) {
                return Err(Fail::harness(format!(
                    "Catalog Error: Column with name {} already exists!",
                    c.name.value
                )));
            }
            let mut col = Column {
                name: c.name.value.clone(),
                ty: c.data_type.to_string(),
                not_null: false,
                default: None,
            };
            for o in &c.options {
                match &o.option {
                    ColumnOption::Null | ColumnOption::Comment(_) => {}
                    ColumnOption::NotNull => col.not_null = true,
                    ColumnOption::Default(e) => col.default = Some(e.to_string()),
                    ColumnOption::PrimaryKey(_) => {
                        col.not_null = true;
                        keys.push((vec![i], true));
                    }
                    ColumnOption::Unique(_) => keys.push((vec![i], false)),
                    other => {
                        return Err(Fail::harness(format!(
                            "column option {}",
                            one_line(&other.to_string())
                        )));
                    }
                }
            }
            columns.push(col);
        }
        for tc in &ct.constraints {
            let (cols, primary) = match tc {
                TableConstraint::PrimaryKey(pk) => (&pk.columns, true),
                TableConstraint::Unique(u) => (&u.columns, false),
                other => {
                    return Err(Fail::harness(format!(
                        "constraint {}",
                        one_line(&other.to_string())
                    )));
                }
            };
            let mut idx = Vec::new();
            for ic in cols {
                let n = ic.column.expr.to_string();
                let n = n.trim_matches('"');
                let i = columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(n))
                    .ok_or_else(|| Fail::harness(format!("key column {n}")))?;
                if primary {
                    columns[i].not_null = true;
                }
                idx.push(i);
            }
            keys.push((idx, primary));
        }
        let schema = self.types(&columns)?;
        if self.state.tables.contains_key(&k) {
            self.state.tables.remove(&k);
        }
        let t = Table {
            name,
            columns,
            schema,
            batches: Vec::new(),
            keys,
        };
        self.store(t, Vec::new())?;
        Ok(Done::Empty(true))
    }

    fn value_for(&self, c: &Column, e: Option<&Expr>) -> String {
        match e {
            Some(Expr::Identifier(i))
                if i.quote_style.is_none() && i.value.eq_ignore_ascii_case("default") =>
            {
                self.value_for(c, None)
            }
            Some(e) => format!("CAST({e} AS {})", c.ty),
            None => format!(
                "CAST({} AS {})",
                c.default.as_deref().unwrap_or("NULL"),
                c.ty
            ),
        }
    }

    fn insert(&mut self, ins: &ast::Insert) -> Result<Done, Fail> {
        if ins.or.is_some() || ins.on.is_some() || ins.returning.is_some() || ins.replace_into {
            return Err(Fail::harness("INSERT OR / ON CONFLICT / RETURNING"));
        }
        if !ins.assignments.is_empty() || ins.partitioned.is_some() || ins.overwrite {
            return Err(Fail::harness("INSERT form"));
        }
        let ast::TableObject::TableName(n) = &ins.table else {
            return Err(Fail::harness("INSERT INTO a function"));
        };
        let name = table_name(n).ok_or_else(|| Fail::harness("a schema the harness has not"))?;
        if self.state.views.iter().any(|(v, _, _)| *v == key(&name)) {
            return Err(Fail::harness(format!(
                "Binder Error: Can only insert into a table, {name} is a view"
            )));
        }
        let t = self.table(n)?;
        let columns: Vec<String> = ins
            .columns
            .iter()
            .map(|c| c.to_string().trim_matches('"').to_string())
            .collect();
        let targets = self.targets(&t, &columns)?;
        let mut new = Vec::new();
        match ins.source.as_deref() {
            None => {
                let list: Vec<String> = t.columns.iter().map(|c| self.value_for(c, None)).collect();
                new.extend(self.query(&format!("SELECT {}", list.join(", ")))?.1);
            }
            Some(q)
                if q.with.is_none()
                    && q.order_by.is_none()
                    && q.limit_clause.is_none()
                    && matches!(q.body.as_ref(), SetExpr::Values(_)) =>
            {
                let SetExpr::Values(values) = q.body.as_ref() else {
                    unreachable!()
                };
                let mut rows = Vec::new();
                for row in &values.rows {
                    let row = &row.content;
                    if row.len() != targets.len() {
                        return Err(Fail::harness(format!(
                            "Binder Error: table {} has {} columns but {} values were supplied",
                            t.name,
                            targets.len(),
                            row.len()
                        )));
                    }
                    let cells: Vec<String> = t
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(j, c)| {
                            let e = targets.iter().position(|&x| x == j).map(|k| &row[k]);
                            self.value_for(c, e)
                        })
                        .collect();
                    rows.push(format!("({})", cells.join(", ")));
                }
                let names: Vec<String> = t.columns.iter().map(|c| ident(&c.name)).collect();
                let mut chunk = Vec::new();
                let mut size = 0;
                let mut flush = |chunk: &mut Vec<String>, this: &Self| -> Result<(), Fail> {
                    if chunk.is_empty() {
                        return Ok(());
                    }
                    let sql = format!(
                        "SELECT * FROM (VALUES {}) AS __v({})",
                        chunk.join(", "),
                        names.join(", ")
                    );
                    new.extend(this.query(&sql)?.1);
                    chunk.clear();
                    Ok(())
                };
                for r in rows {
                    size += r.len();
                    chunk.push(r);
                    if chunk.len() >= 1000 || size > 200_000 {
                        flush(&mut chunk, self)?;
                        size = 0;
                    }
                }
                flush(&mut chunk, self)?;
            }
            Some(q) => new.extend(self.insert_rows(&t, &targets, &q.to_string())?),
        }
        let count: u64 = new.iter().map(|b| b.num_rows() as u64).sum();
        let mut all = t.batches.clone();
        all.extend(new);
        self.store(t, all)?;
        Ok(Done::Count(count, true))
    }

    /// Which table column each supplied value goes to.
    fn targets(&self, t: &Table, columns: &[String]) -> Result<Vec<usize>, Fail> {
        if columns.is_empty() {
            return Ok((0..t.columns.len()).collect());
        }
        let mut v = Vec::new();
        for cname in columns {
            let i = t
                .columns
                .iter()
                .position(|tc| tc.name.eq_ignore_ascii_case(cname))
                .ok_or_else(|| {
                    Fail::harness(format!(
                        "Binder Error: Table \"{}\" does not have a column with name \"{cname}\"",
                        t.name
                    ))
                })?;
            if v.contains(&i) {
                return Err(Fail::harness(format!(
                    "Binder Error: Duplicate column name \"{cname}\" in INSERT"
                )));
            }
            v.push(i);
        }
        Ok(v)
    }

    fn insert_query(&mut self, name: &str, columns: &[String], query: &str) -> Result<Done, Fail> {
        if self.state.views.iter().any(|(v, _, _)| *v == key(name)) {
            return Err(Fail::harness(format!(
                "Binder Error: Can only insert into a table, {name} is a view"
            )));
        }
        let t = self.state.tables.get(&key(name)).cloned().ok_or_else(|| {
            Fail::harness(format!(
                "Catalog Error: Table with name {name} does not exist!"
            ))
        })?;
        let targets = self.targets(&t, columns)?;
        let new = self.insert_rows(&t, &targets, query)?;
        let count: u64 = new.iter().map(|b| b.num_rows() as u64).sum();
        let mut all = t.batches.clone();
        all.extend(new);
        self.store(t, all)?;
        Ok(Done::Count(count, true))
    }

    /// The rows `src` gives, cast to the table's columns in its order.
    fn insert_rows(
        &self,
        t: &Table,
        targets: &[usize],
        src: &str,
    ) -> Result<Vec<RecordBatch>, Fail> {
        let mut new = Vec::new();
        {
            {
                let schema = self.engine.result_schema(src)?;
                if schema.fields().len() != targets.len() {
                    return Err(Fail::harness(format!(
                        "Binder Error: table {} has {} columns but {} values were supplied",
                        t.name,
                        targets.len(),
                        schema.fields().len()
                    )));
                }
                let aliases: Vec<String> = (0..targets.len()).map(|i| format!("__c{i}")).collect();
                let list: Vec<String> = t
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(j, c)| match targets.iter().position(|&x| x == j) {
                        Some(k) => format!("CAST(__s.__c{k} AS {})", c.ty),
                        None => self.value_for(c, None),
                    })
                    .collect();
                let sql = format!(
                    "SELECT {} FROM ({src}) AS __s({})",
                    list.join(", "),
                    aliases.join(", ")
                );
                new.extend(self.query(&sql)?.1);
            }
        }
        Ok(new)
    }

    /// `FROM t [AS a]`, numbered so the rows keep their order, as DuckDB's do in place.
    fn numbered(&self, t: &Table, alias: Option<&ast::TableAlias>) -> Result<String, Fail> {
        if alias.is_some_and(|a| !a.columns.is_empty()) {
            return Err(Fail::harness("alias with columns"));
        }
        let a = alias
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| t.name.clone());
        Ok(format!(
            "(SELECT *, row_number() OVER () AS __rn FROM {}) AS {}",
            ident(&t.name),
            ident(&a)
        ))
    }

    fn target(&self, f: &TableFactor) -> Result<(Table, Option<ast::TableAlias>), Fail> {
        match f {
            TableFactor::Table {
                name, alias, args, ..
            } if args.is_none() => Ok((self.table(name)?, alias.clone())),
            _ => Err(Fail::harness("DML over something other than a table")),
        }
    }

    fn count_where(&self, from: &str, pred: Option<&Expr>) -> Result<u64, Fail> {
        let sql = match pred {
            Some(p) => format!("SELECT count(*) FROM {from} WHERE {p}"),
            None => format!("SELECT count(*) FROM {from}"),
        };
        let (_, b) = self.query(&sql)?;
        Ok(b.first()
            .filter(|b| b.num_rows() > 0)
            .map(|b| render::top(b.column(0), 0).parse::<u64>().unwrap_or(0))
            .unwrap_or(0))
    }

    fn delete(&mut self, del: &ast::Delete) -> Result<Done, Fail> {
        if del.using.is_some() || del.returning.is_some() || !del.tables.is_empty() {
            return Err(Fail::harness("DELETE USING / RETURNING"));
        }
        if del.limit.is_some() || !del.order_by.is_empty() {
            return Err(Fail::harness("DELETE ... LIMIT"));
        }
        let from = match &del.from {
            FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
        };
        let [twj] = from.as_slice() else {
            return Err(Fail::harness("DELETE from several tables"));
        };
        if !twj.joins.is_empty() {
            return Err(Fail::harness("DELETE with joins"));
        }
        let (t, alias) = self.target(&twj.relation)?;
        let src = self.numbered(&t, alias.as_ref())?;
        let count = self.count_where(&src, del.selection.as_ref())?;
        let cols: Vec<String> = t.columns.iter().map(|c| ident(&c.name)).collect();
        let sql = match &del.selection {
            Some(p) => format!(
                "SELECT {} FROM {src} WHERE NOT COALESCE(({p}), FALSE) ORDER BY __rn",
                cols.join(", ")
            ),
            None => format!("SELECT {} FROM {src} WHERE FALSE", cols.join(", ")),
        };
        let (_, batches) = self.query(&sql)?;
        self.store(t, batches)?;
        Ok(Done::Count(count, true))
    }

    fn update(&mut self, up: &ast::Update) -> Result<Done, Fail> {
        if up.from.is_some() || up.returning.is_some() || up.or.is_some() {
            return Err(Fail::harness("UPDATE FROM / RETURNING"));
        }
        if up.limit.is_some() || !up.order_by.is_empty() || !up.table.joins.is_empty() {
            return Err(Fail::harness("UPDATE form"));
        }
        let (t, alias) = self.target(&up.table.relation)?;
        let mut set: HashMap<usize, String> = HashMap::new();
        for a in &up.assignments {
            let AssignmentTarget::ColumnName(n) = &a.target else {
                return Err(Fail::harness("UPDATE SET (a, b) = ..."));
            };
            let cname = match n.0.last() {
                Some(ObjectNamePart::Identifier(i)) => i.value.clone(),
                _ => return Err(Fail::harness("UPDATE target")),
            };
            let i = t
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&cname))
                .ok_or_else(|| {
                    Fail::harness(format!(
                        "Binder Error: Referenced update column {cname} not found in table!"
                    ))
                })?;
            if set.contains_key(&i) {
                return Err(Fail::harness(
                    "Binder Error: Multiple assignments to same column",
                ));
            }
            let c = &t.columns[i];
            set.insert(i, self.value_for(c, Some(&a.value)));
        }
        let src = self.numbered(&t, alias.as_ref())?;
        let count = self.count_where(&src, up.selection.as_ref())?;
        let list: Vec<String> = t
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| match (set.get(&i), &up.selection) {
                (Some(v), Some(p)) => format!(
                    "CASE WHEN COALESCE(({p}), FALSE) THEN {v} ELSE {} END AS {}",
                    ident(&c.name),
                    ident(&c.name)
                ),
                (Some(v), None) => format!("{v} AS {}", ident(&c.name)),
                (None, _) => ident(&c.name),
            })
            .collect();
        let sql = format!("SELECT {} FROM {src} ORDER BY __rn", list.join(", "));
        let (_, batches) = self.query(&sql)?;
        self.store(t, batches)?;
        Ok(Done::Count(count, true))
    }

    fn run_one(&mut self, plan: &Plan) -> Result<Done, Fail> {
        match plan {
            Plan::Engine(sql) => {
                let (s, b) = self.query(sql)?;
                Ok(Done::Rows(s, b))
            }
            Plan::Mapped(_) | Plan::Ctas { .. } | Plan::InsertQuery { .. } => {
                let before = self.state.clone();
                let r = match plan {
                    Plan::Mapped(stmt) => self.run_mapped(stmt),
                    Plan::Ctas {
                        name,
                        or_replace,
                        if_not_exists,
                        query,
                    } => match self.may_create(name, *or_replace, *if_not_exists) {
                        Ok(true) => self.ctas_into(name.clone(), query),
                        Ok(false) => Ok(Done::Harness),
                        Err(e) => Err(e),
                    },
                    Plan::InsertQuery {
                        name,
                        columns,
                        query,
                    } => self.insert_query(name, columns, query),
                    _ => unreachable!(),
                };
                if r.is_err() {
                    // A failed write leaves nothing behind, as a failed statement in DuckDB.
                    let now = std::mem::replace(&mut self.state, before);
                    let _ = self.rebind_all(&now);
                }
                r
            }
            Plan::NoOp(_) | Plan::Unmappable { .. } => Ok(Done::Harness),
        }
    }

    /// Runs one record and classifies it.
    pub fn record(&mut self, rec: &Record) -> Outcome {
        let pieces = split_statements(&rec.sql);
        let in_query = matches!(rec.kind, Kind::Query { .. });
        let plans: Vec<Plan> = if pieces.is_empty() {
            vec![Plan::Engine(rec.sql.clone())]
        } else {
            pieces.iter().map(|p| plan(p, in_query)).collect()
        };
        let expect_ok = match &rec.kind {
            Kind::Statement(Expect::Ok) | Kind::Query { .. } => true,
            Kind::Statement(Expect::DontCare) => {
                return Outcome {
                    stop: Some("statement debug".into()),
                    ..outcome(Class::Skipped, "statement debug")
                };
            }
            _ => false,
        };
        for p in &plans {
            if let Plan::Unmappable { why, changes } = p {
                let stop = (expect_ok && *changes).then(|| why.clone());
                return Outcome {
                    class: Class::Skipped,
                    detail: format!("not mapped: {why}"),
                    stop,
                };
            }
        }
        if plans.iter().all(|p| matches!(p, Plan::NoOp(_))) {
            let why = plans
                .iter()
                .map(|p| match p {
                    Plan::NoOp(w) => w.as_str(),
                    _ => "",
                })
                .collect::<Vec<_>>()
                .join("; ");
            if let Some(l) = LIMIT_SETTINGS.iter().find(|l| why.contains(*l)) {
                self.limited = Some(l.to_string());
            }
            return outcome(Class::Skipped, format!("no-op setting: {why}"));
        }
        let writes = plans.iter().any(plan_writes);
        // A write the engine is handed inside a query (`INSERT ... RETURNING`, a DML CTE) never
        // reaches the harness's tables, whatever the engine answers.
        let unapplied = plans
            .iter()
            .any(|p| matches!(p, Plan::Engine(sql) if dml_text(sql)));
        let mut last = Done::Harness;
        let mut engine_used = false;
        let mut failure = None;
        for p in &plans {
            match self.run_one(p) {
                Ok(d) => {
                    engine_used |=
                        matches!(d, Done::Rows(..) | Done::Count(_, true) | Done::Empty(true));
                    last = d;
                }
                Err(f) => {
                    failure = Some(f);
                    break;
                }
            }
        }
        let in_txn = self.txn.is_some();
        let rows = match &last {
            Done::Rows(_, b) => b.iter().map(|b| b.num_rows()).sum(),
            _ => 0,
        };
        // Rows all alike answer the same in any order.
        let alike = match &last {
            Done::Rows(_, batches) => {
                let mut seen: Option<Vec<String>> = None;
                batches.iter().all(|b| {
                    (0..b.num_rows()).all(|r| {
                        let row: Vec<String> =
                            b.columns().iter().map(|c| render::top(c, r)).collect();
                        seen.get_or_insert_with(|| row.clone()) == &row
                    })
                })
            }
            _ => true,
        };
        let mut out = self.judge(rec, failure, last, engine_used, writes);
        if unapplied && out.stop.is_none() && expect_ok {
            out.stop = Some("a write inside a query, which the harness does not apply".into());
        }
        // A query whose order the file fixes but the statement may not (`nosort` over several rows)
        // runs again: an order that changes from run to run is a wrong answer however often it
        // happens to match, and a ratchet that counted it would flicker.
        if let (
            Class::Pass,
            Kind::Query {
                sort: Sort::None, ..
            },
            Some(Plan::Engine(sql)),
        ) = (out.class, &rec.kind, plans.last())
            && rows > 1
            && !alike
            && !writes
            && self.order_unfixed(sql)
        {
            out = Outcome {
                class: Class::Wrong,
                detail: "the rows matched, in an order the engine's plan does not fix (a union \
                         or a merge of partitions with no sort above it)"
                    .into(),
                stop: None,
            };
        }
        if let (
            Class::Pass,
            Kind::Query {
                sort: Sort::None, ..
            },
            Some(Plan::Engine(sql)),
        ) = (out.class, &rec.kind, plans.last())
            && rows > 1
            && !writes
        {
            for _ in 0..REPEATS {
                let again = match self.query(sql) {
                    Ok((s, b)) => self.judge(rec, None, Done::Rows(s, b), true, false),
                    Err(e) => outcome(classify(&e), one_line(&e.to_string())),
                };
                if again.class != Class::Pass {
                    out = Outcome {
                        class: Class::Wrong,
                        detail: format!("answered differently when run again: {}", again.detail),
                        stop: None,
                    };
                    break;
                }
            }
        }
        if let Some(f) = self.ignored(&out) {
            out = Outcome {
                class: Class::Skipped,
                detail: format!("ignore_error_messages: {f}"),
                stop: Some("ignore_error_messages".into()),
            };
        }
        if in_txn && out.class != Class::Pass && out.stop.is_none() {
            out.stop = Some("a failure inside a transaction".into());
        }
        out
    }

    fn ignored(&self, out: &Outcome) -> Option<String> {
        if !matches!(out.class, Class::Error | Class::Refused) {
            return None;
        }
        self.ignore
            .iter()
            .find(|m| out.detail.contains(m.as_str()))
            .cloned()
    }

    fn judge(
        &mut self,
        rec: &Record,
        failure: Option<Fail>,
        last: Done,
        engine_used: bool,
        writes: bool,
    ) -> Outcome {
        let stop_if = |c: bool, why: &str| c.then(|| why.to_string());
        match &rec.kind {
            Kind::Statement(expect) => {
                let (ok_expected, msg) = match expect {
                    Expect::Ok => (true, ""),
                    Expect::Error(m) => (false, m.as_str()),
                    Expect::Maybe(m) => (true, m.as_str()),
                    Expect::DontCare => (true, ""),
                };
                let maybe = matches!(expect, Expect::Maybe(_));
                match failure {
                    None if ok_expected => {
                        if engine_used {
                            outcome(Class::Pass, "")
                        } else {
                            outcome(Class::Skipped, "decided by the harness alone")
                        }
                    }
                    None => {
                        if writes {
                            // A constraint, default or clause DuckDB checks and the mapping does
                            // not: the harness cannot say the engine would have refused.
                            Outcome {
                                class: Class::Skipped,
                                detail: "a write DuckDB refuses and the harness's mapping does not"
                                    .into(),
                                stop: Some("the harness's state differs".into()),
                            }
                        } else if let Some(l) = &self.limited {
                            outcome(
                                Class::Skipped,
                                format!(
                                    "DuckDB errors under {l}, which the harness does not impose"
                                ),
                            )
                        } else if engine_used {
                            Outcome {
                                class: Class::Wrong,
                                detail: "succeeded where DuckDB errors".into(),
                                stop: None,
                            }
                        } else {
                            Outcome {
                                class: Class::Skipped,
                                detail: "the harness succeeded where DuckDB errors".into(),
                                stop: None,
                            }
                        }
                    }
                    Some(f) => {
                        let matched = !ok_expected || maybe;
                        if matched && error_matches(msg, &f.msg) {
                            return match f.engine {
                                Some(_) => outcome(Class::Pass, ""),
                                None => outcome(Class::Skipped, "decided by the harness alone"),
                            };
                        }
                        let detail = if ok_expected && !maybe {
                            one_line(&f.msg)
                        } else {
                            format!(
                                "error differs: {} (expected {})",
                                one_line(&f.msg),
                                one_line(msg)
                            )
                        };
                        match f.engine {
                            Some(e) => Outcome {
                                class: classify(&e),
                                detail,
                                stop: stop_if(ok_expected && writes, "a write that failed"),
                            },
                            None => Outcome {
                                class: Class::Skipped,
                                detail: format!("harness: {detail}"),
                                stop: stop_if(
                                    ok_expected && writes,
                                    "a write the harness could not map",
                                ),
                            },
                        }
                    }
                }
            }
            Kind::Query {
                columns,
                sort,
                label,
                expected,
            } => {
                if let Some(f) = failure {
                    return match f.engine {
                        Some(e) => Outcome {
                            class: classify(&e),
                            detail: one_line(&f.msg),
                            stop: stop_if(writes, "a write that failed"),
                        },
                        None => Outcome {
                            class: Class::Skipped,
                            detail: format!("harness: {}", one_line(&f.msg)),
                            stop: stop_if(writes, "a write the harness could not map"),
                        },
                    };
                }
                let (schema, batches) = match last {
                    Done::Rows(s, b) => (s, b),
                    Done::Count(n, true) => {
                        let schema = Arc::new(Schema::new(vec![Field::new(
                            "Count",
                            arrow::datatypes::DataType::Int64,
                            false,
                        )]));
                        let col: ArrayRef =
                            Arc::new(arrow::array::Int64Array::from(vec![n as i64]));
                        let b =
                            RecordBatch::try_new(schema.clone(), vec![col]).expect("one column");
                        (schema, vec![b])
                    }
                    Done::Empty(true) => {
                        let schema = Arc::new(Schema::new(vec![Field::new(
                            "Count",
                            arrow::datatypes::DataType::Int64,
                            false,
                        )]));
                        (schema, Vec::new())
                    }
                    _ => return outcome(Class::Skipped, "a query the harness answers alone"),
                };
                match check(
                    &schema,
                    &batches,
                    *columns,
                    *sort,
                    label,
                    expected,
                    &mut self.labels,
                ) {
                    Ok(()) => outcome(Class::Pass, ""),
                    Err(d) => outcome(Class::Wrong, d),
                }
            }
        }
    }

    pub fn item(&mut self, item: &Item) -> Option<Outcome> {
        match item {
            Item::Run(rec) => Some(self.record(rec)),
            Item::ResetLabel(l) => {
                self.labels.remove(l);
                None
            }
            Item::IgnoreErrors(v) => {
                self.ignore = v.clone();
                None
            }
            Item::Skip { why, sql, .. } => Some(Outcome {
                // A record DuckDB runs and the harness does not: if it writes, the rest of the
                // file would be compared against state DuckDB has and the harness does not.
                stop: sql
                    .as_deref()
                    .is_some_and(writes_sql)
                    .then(|| format!("a write skipped ({why})")),
                ..outcome(Class::Skipped, why.clone())
            }),
            Item::SkipRest { why, .. } => Some(Outcome {
                stop: Some(why.clone()),
                ..outcome(Class::Skipped, why.clone())
            }),
            Item::Halt => Some(Outcome {
                stop: Some("halt".into()),
                ..outcome(Class::Skipped, "halt")
            }),
        }
    }
}

/// Operators that hand on rows in the order they were sorted into.
const SORTS: &[&str] = &["SortExec", "SortPreservingMergeExec", "PartialSortExec"];
/// Operators whose output interleaves several inputs as they arrive.
const MERGES: &[&str] = &["UnionExec", "CoalescePartitionsExec", "InterleaveExec"];

/// [`Session::order_unfixed`] over the lines of a physical plan as DataFusion indents it (two
/// spaces a level). Down a chain of one-input operators the first sort fixes the order and the
/// first merge unfixes it; at an operator of several inputs (a join), a merge anywhere below it
/// unfixes it.
fn unordered_plan(lines: &[String]) -> bool {
    let nodes: Vec<(usize, &str)> = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let t = l.trim_start();
            let name = t.split([':', ' ', ',']).next().unwrap_or("");
            ((l.len() - t.len()) / 2, name)
        })
        .collect();
    let mut i = 0;
    while i < nodes.len() {
        let (depth, name) = nodes[i];
        if SORTS.contains(&name) {
            return false;
        }
        if MERGES.contains(&name) {
            return true;
        }
        let end = nodes[i + 1..]
            .iter()
            .position(|(d, _)| *d <= depth)
            .map_or(nodes.len(), |p| i + 1 + p);
        let children = nodes[i + 1..end]
            .iter()
            .filter(|(d, _)| *d == depth + 1)
            .count();
        if children > 1 {
            return nodes[i + 1..end].iter().any(|(_, n)| MERGES.contains(n));
        }
        i += 1;
    }
    false
}

/// Whether a record may change state, so that not running it leaves the rest of the file nothing
/// to compare against.
pub fn writes(rec: &Record) -> bool {
    writes_sql(&rec.sql)
}

/// [`writes`] for a record's SQL alone.
pub fn writes_sql(sql: &str) -> bool {
    split_statements(sql).iter().any(|p| match plan(p, true) {
        Plan::Engine(sql) => dml_text(&sql),
        Plan::NoOp(_) => false,
        _ => true,
    })
}

fn plan_writes(p: &Plan) -> bool {
    match p {
        Plan::Mapped(_) | Plan::Ctas { .. } | Plan::InsertQuery { .. } => true,
        Plan::Engine(sql) => dml_text(sql),
        _ => false,
    }
}

/// Whether SQL handed to the engine as a query holds a write: `INSERT INTO`, `UPDATE ... SET`,
/// `DELETE FROM` or `MERGE INTO` outside string literals and comments.
fn dml_text(sql: &str) -> bool {
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r"(?i)\b(?:INSERT\s+(?:OR\s+\w+\s+)?INTO|DELETE\s+FROM|MERGE\s+INTO)\b|\bUPDATE\s+\S+(?:\s+(?:AS\s+)?\w+)?\s+SET\b",
        )
        .expect("a valid pattern")
    });
    let mut bare = String::with_capacity(sql.len());
    let mut quote: Option<char> = None;
    for c in strip_comments(sql).chars() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == '\'' || c == '"' => quote = Some(c),
            None => bare.push(c),
        }
    }
    RE.is_match(&bare)
}

pub fn script_lines(s: &Script) -> &[usize] {
    &s.record_lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_lines(s: &str) -> Vec<String> {
        s.lines().map(str::to_string).collect()
    }

    #[test]
    fn a_union_with_no_sort_above_leaves_the_order_unfixed() {
        let union = "ProjectionExec: expr=[a@0 as a]\n  UnionExec\n    DataSourceExec: partitions=1\n    DataSourceExec: partitions=1";
        assert!(unordered_plan(&plan_lines(union)));
        let sorted = "SortExec: expr=[a@0 ASC]\n  UnionExec\n    DataSourceExec: partitions=1\n    DataSourceExec: partitions=1";
        assert!(!unordered_plan(&plan_lines(sorted)));
        let scan = "ProjectionExec: expr=[a@0 as a]\n  FilterExec: a@0 > 3\n    DataSourceExec: partitions=1";
        assert!(!unordered_plan(&plan_lines(scan)));
        let join = "HashJoinExec: mode=CollectLeft\n  DataSourceExec: partitions=1\n  ProjectionExec: expr=[]\n    UnionExec\n      DataSourceExec: partitions=1\n      DataSourceExec: partitions=1";
        assert!(unordered_plan(&plan_lines(join)));
        let plain_join = "HashJoinExec: mode=CollectLeft\n  SortExec: expr=[]\n    DataSourceExec: partitions=1\n  DataSourceExec: partitions=1";
        assert!(!unordered_plan(&plan_lines(plain_join)));
    }

    #[test]
    fn a_write_inside_a_query_is_seen_and_one_in_a_string_is_not() {
        assert!(dml_text(
            "WITH ins AS (INSERT INTO t VALUES (1) RETURNING i) SELECT * FROM ins"
        ));
        assert!(dml_text(
            "WITH u AS (UPDATE t SET j = 1 RETURNING j) SELECT * FROM u"
        ));
        assert!(dml_text("delete from t where i = 1 returning i"));
        assert!(!dml_text("SELECT 'DELETE FROM t', \"insert into\" FROM t"));
        assert!(!dml_text("SELECT updated, deleted FROM t -- DELETE FROM t"));
    }
}
