//! DuckDB's sqllogictest dialect, read as DuckDB's runner reads it (`test/sqlite/sqllogic_parser.cpp`
//! and `sqllogic_test_runner.cpp` at the pinned tag): records separated by blank lines, `loop`
//! and `foreach` expanded, `skipif`/`onlyif`, `mode skip`, and `require` deciding the file.

use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub enum Expect {
    Ok,
    /// `statement error`, with the message the error must contain (or empty for any error).
    Error(String),
    /// `statement maybe`: success, or an error containing the message.
    Maybe(String),
    /// `statement debug`: anything.
    DontCare,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Sort {
    None,
    Row,
    Value,
}

#[derive(Debug, Clone)]
pub enum Kind {
    Statement(Expect),
    Query {
        columns: usize,
        sort: Sort,
        label: Option<String>,
        expected: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub struct Record {
    /// 1-based line of the record's `statement` or `query` line: the record's identity.
    pub line: usize,
    pub sql: String,
    pub kind: Kind,
}

#[derive(Debug, Clone)]
pub enum Item {
    Run(Record),
    /// A record not run, and why; its SQL when DuckDB runs it, so that a write can end the file.
    Skip {
        line: usize,
        why: String,
        sql: Option<String>,
    },
    /// Nothing after this runs: the rest of the file needs what the harness cannot give.
    SkipRest {
        why: String,
    },
    ResetLabel(String),
    Halt,
    /// `set ignore_error_messages`: an error containing one of these ends the file, as in DuckDB.
    IgnoreErrors(Vec<String>),
}

pub struct Script {
    pub items: Vec<Item>,
    /// Set when the whole file is skipped (`require` of an extension, `require-env`, ...).
    pub skip_file: Option<String>,
    /// Every record's line, run or not, so a file skipped whole still counts its records.
    pub record_lines: Vec<usize>,
}

/// `require` arguments that hold for the harness as they hold for a stock DuckDB build: platform
/// and storage switches that change nothing a statement answers.
fn require_holds(params: &[String]) -> bool {
    let p = params.first().map(String::as_str).unwrap_or("");
    let n = |i: usize| params.get(i).and_then(|v| v.parse::<u64>().ok());
    match p {
        "skip_reload"
        | "noforcestorage"
        | "no_force_storage"
        | "no_alternative_verify"
        | "notwindows"
        | "notmingw"
        | "notmusl"
        | "linux"
        | "64bit"
        | "nothreadsan"
        | "no_extension_autoloading"
        | "no_vector_verification"
        | "no_latest_storage"
        | "core_functions" => true,
        // DuckDB's STANDARD_VECTOR_SIZE is 2048.
        "vector_size" => n(1).is_some_and(|v| v <= 2048),
        "exact_vector_size" => n(1) == Some(2048),
        "block_size" => n(1) == Some(262_144),
        _ => false,
    }
}

const SIGNED: &[&str] = &["tinyint", "smallint", "integer", "bigint", "hugeint"];
const UNSIGNED: &[&str] = &["utinyint", "usmallint", "uinteger", "ubigint", "uhugeint"];
const COMPRESSION: &[&str] = &[
    "none",
    "uncompressed",
    "rle",
    "bitpacking",
    "dictionary",
    "fsst",
    "dict_fsst",
    "alp",
    "alprd",
];

/// `foreach` tokens, with DuckDB's groups (`sqllogic_command.cpp`, `ForEachTokenReplace`).
/// `None` for a group the harness does not expand.
fn foreach_values(tokens: &[String]) -> Option<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for t in tokens {
        let lower = t.trim().to_ascii_lowercase();
        if let Some(rest) = t.strip_prefix('!')
            && let Some(i) = out.iter().position(|v| v == rest)
        {
            out.remove(i);
            continue;
        }
        let all = lower == "<alltypes>";
        let numeric = all || lower == "<numeric>";
        let integral = numeric || lower == "<integral>";
        let signed = integral || lower == "<signed>";
        let unsigned = integral || lower == "<unsigned>";
        let mut group = false;
        if signed {
            out.extend(SIGNED.iter().map(|s| s.to_string()));
            group = true;
        }
        if unsigned {
            out.extend(UNSIGNED.iter().map(|s| s.to_string()));
            group = true;
        }
        if numeric {
            out.extend(["float", "double"].iter().map(|s| s.to_string()));
        }
        if all {
            out.extend(
                ["bool", "interval", "varchar"]
                    .iter()
                    .map(|s| s.to_string()),
            );
        }
        if lower == "<compression>" {
            out.extend(COMPRESSION.iter().map(|s| s.to_string()));
            group = true;
        }
        if !group && lower.starts_with('<') && lower.ends_with('>') {
            return None;
        }
        if !group {
            out.push(t.clone());
        }
    }
    Some(out)
}

/// A `skipif`/`onlyif` condition on loop variables, `i=2` or `a<>b&&c>1`.
fn condition_holds(text: &str, vars: &HashMap<String, String>) -> Option<bool> {
    if !text.contains(['=', '<', '>']) {
        return None;
    }
    let mut all = true;
    for part in text.split("&&") {
        let ops = ["<>", ">=", ">", "<=", "<", "="];
        let (op, l, r) = ops.iter().find_map(|op| {
            part.split_once(op)
                .map(|(l, r)| (*op, l.trim().to_string(), r.trim().to_string()))
        })?;
        let l = vars.get(&l).cloned().unwrap_or(l);
        let r = vars.get(&r).cloned().unwrap_or(r);
        let ord = match (l.parse::<f64>(), r.parse::<f64>()) {
            (Ok(a), Ok(b)) => a.partial_cmp(&b)?,
            _ => l.cmp(&r),
        };
        use std::cmp::Ordering::*;
        let holds = match op {
            "<>" => ord != Equal,
            ">=" => ord != Less,
            ">" => ord == Greater,
            "<=" => ord != Greater,
            "<" => ord == Less,
            _ => ord == Equal,
        };
        all &= holds;
    }
    Some(all)
}

/// One parsed record or directive, before loops expand.
#[derive(Debug, Clone)]
enum Node {
    Record {
        line: usize,
        /// (skip_if, condition text) pairs on loop variables.
        conditions: Vec<(bool, String)>,
        skipped: Option<String>,
        /// The skip is DuckDB's own too (`skipif duckdb`, `mode skip`): the record changes nothing.
        duckdb_skips: bool,
        sql: String,
        kind: Kind,
    },
    Loop {
        var: String,
        values: Option<Vec<String>>,
        body: Vec<Node>,
    },
    Item(Item),
}

fn empty_or_comment(l: &str) -> bool {
    l.is_empty() || l.starts_with('#')
}

fn tokens(line: &str) -> Vec<String> {
    line.split(|c: char| c.is_ascii_whitespace())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

pub fn parse(path: &Path, env: &HashMap<String, String>) -> std::io::Result<Script> {
    let text = std::fs::read(path)?;
    let text = String::from_utf8_lossy(&text).replace('\r', "");
    let lines: Vec<&str> = text.split('\n').collect();
    let mut env = env.clone();
    let mut skip_file = None;
    let mut record_lines = Vec::new();
    let mut stack: Vec<(String, Option<Vec<String>>, Vec<Node>)> = Vec::new();
    let mut top: Vec<Node> = Vec::new();
    let mut skip_level = 0usize;
    let mut i = 0usize;

    macro_rules! push {
        ($node:expr) => {{
            let node = $node;
            match stack.last_mut() {
                Some((_, _, body)) => body.push(node),
                None => top.push(node),
            }
        }};
    }

    while i < lines.len() {
        // The first non-empty, non-comment line.
        if empty_or_comment(lines[i]) {
            i += 1;
            continue;
        }
        let mut toks = tokens(lines[i]);
        let mut conditions = Vec::new();
        let mut skipped: Option<String> = None;
        let mut duckdb_skips = false;
        // skipif / onlyif lines prefix the record.
        while matches!(toks.first().map(String::as_str), Some("skipif" | "onlyif")) {
            let skip_if = toks[0] == "skipif";
            let system = toks
                .get(1)
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_default();
            if system.contains(['=', '<', '>']) {
                conditions.push((skip_if, system));
            } else if (system == "duckdb") == skip_if {
                skipped.get_or_insert_with(|| format!("{} {system}", toks[0]));
                duckdb_skips = true;
            }
            i += 1;
            toks = if i < lines.len() {
                tokens(lines[i])
            } else {
                Vec::new()
            };
        }
        let head = toks.first().cloned().unwrap_or_default();
        let line_no = i + 1;
        match head.as_str() {
            "statement" | "query" => {
                // SQL runs to "----" or a blank line.
                let mut sql = Vec::new();
                i += 1;
                while i < lines.len() && !empty_or_comment(lines[i]) && lines[i] != "----" {
                    sql.push(lines[i]);
                    i += 1;
                }
                let mut sql = sql.join("\n");
                for (k, v) in &env {
                    sql = sql.replace(&format!("${{{k}}}", k = k), v);
                    sql = sql.replace(&format!("{{{k}}}"), v);
                }
                let mut result = Vec::new();
                if i < lines.len() && lines[i] == "----" {
                    i += 1;
                    while i < lines.len() && !lines[i].is_empty() {
                        result.push(lines[i].to_string());
                        i += 1;
                    }
                }
                let kind;
                let mut connection = None;
                if head == "statement" {
                    let msg = result.join("\n");
                    kind = Kind::Statement(match toks.get(1).map(String::as_str) {
                        Some("ok") => Expect::Ok,
                        Some("error") => Expect::Error(msg),
                        Some("maybe") => Expect::Maybe(msg),
                        Some("debug") => Expect::DontCare,
                        Some("debug_skip") => {
                            skip_level += 1;
                            Expect::DontCare
                        }
                        _ => {
                            skipped.get_or_insert_with(|| "unknown statement form".into());
                            Expect::DontCare
                        }
                    });
                    connection = toks.get(2).cloned();
                } else {
                    let types = toks.get(1).cloned().unwrap_or_default();
                    let mut sort = Sort::None;
                    let mut label = None;
                    if let Some(p) = toks.get(2) {
                        match p.as_str() {
                            "nosort" => {}
                            "rowsort" => sort = Sort::Row,
                            "valuesort" => sort = Sort::Value,
                            other => connection = Some(other.to_string()),
                        }
                    }
                    if let Some(l) = toks.get(3) {
                        label = Some(l.clone());
                    }
                    if types.is_empty() || types.chars().any(|c| !"TIR".contains(c)) {
                        skipped.get_or_insert_with(|| format!("query types {types:?}"));
                    }
                    if result.len() == 1 && result[0].starts_with("<FILE>:") {
                        skipped.get_or_insert_with(|| "result read from a file".into());
                    }
                    kind = Kind::Query {
                        columns: types.len(),
                        sort,
                        label,
                        expected: result,
                    };
                }
                if skip_level > 0 {
                    skipped.get_or_insert_with(|| "mode skip".into());
                    duckdb_skips = true;
                }
                if connection.is_some() {
                    skipped.get_or_insert_with(|| "a named connection".into());
                }
                record_lines.push(line_no);
                push!(Node::Record {
                    line: line_no,
                    conditions,
                    skipped,
                    duckdb_skips,
                    sql,
                    kind,
                });
                continue;
            }
            _ => {}
        }
        // Single-line directives. DuckDB skips the rest of the record's lines.
        i += 1;
        while i < lines.len() && !empty_or_comment(lines[i]) {
            i += 1;
        }
        if skipped.is_some() && head != "mode" {
            continue;
        }
        let params: Vec<String> = toks.iter().skip(1).cloned().collect();
        match head.as_str() {
            "mode" => match params.first().map(String::as_str) {
                Some("skip") => skip_level += 1,
                Some("unskip") => skip_level = skip_level.saturating_sub(1),
                _ => {}
            },
            "hash-threshold" | "tags" | "sleep" | "reconnect" => {}
            "halt" => push!(Node::Item(Item::Halt)),
            "loop" | "concurrentloop" => {
                let a = params.get(1).and_then(|v| v.parse::<i64>().ok());
                let b = params.get(2).and_then(|v| v.parse::<i64>().ok());
                let values = match (a, b) {
                    (Some(a), Some(b)) => Some((a..b).map(|v| v.to_string()).collect()),
                    _ => None,
                };
                stack.push((
                    params.first().cloned().unwrap_or_default(),
                    values,
                    Vec::new(),
                ));
            }
            "foreach" | "concurrentforeach" => {
                let values = foreach_values(&params[1.min(params.len())..]);
                stack.push((
                    params.first().cloned().unwrap_or_default(),
                    values,
                    Vec::new(),
                ));
            }
            "endloop" => {
                if let Some((var, values, body)) = stack.pop() {
                    push!(Node::Loop { var, values, body });
                }
            }
            "require" => {
                if !require_holds(&params) && skip_file.is_none() {
                    skip_file = Some(format!("require {}", params.join(" ")));
                }
            }
            "require-env" => {
                skip_file.get_or_insert_with(|| format!("require-env {}", params.join(" ")));
            }
            "require-env-not" => {}
            "test-env" => {
                if let (Some(k), Some(v)) = (params.first(), params.get(1)) {
                    env.insert(k.clone(), v.clone());
                }
            }
            "set" => match params.first().map(String::as_str) {
                Some("ignore_error_messages") => {
                    let list = params[1..].join(" ");
                    push!(Node::Item(Item::IgnoreErrors(
                        list.split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect()
                    )));
                }
                Some("variable") => {
                    if let (Some(k), Some(v)) = (params.get(1), params.get(2)) {
                        env.insert(k.clone(), v.clone());
                    }
                }
                Some("seed") => push!(Node::Item(Item::SkipRest {
                    why: "set seed: the harness cannot seed random()".into(),
                })),
                _ => {}
            },
            "reset" => {
                if params
                    .first()
                    .is_some_and(|p| p.eq_ignore_ascii_case("label"))
                    && let Some(l) = params.get(1)
                {
                    push!(Node::Item(Item::ResetLabel(l.clone())));
                }
            }
            other => push!(Node::Item(Item::SkipRest {
                why: format!("`{other}` in the test file"),
            })),
        }
    }
    while let Some((var, values, body)) = stack.pop() {
        push!(Node::Loop { var, values, body });
    }
    let mut items = Vec::new();
    expand(&top, &mut Vec::new(), &mut items);
    Ok(Script {
        items,
        skip_file,
        record_lines,
    })
}

fn substitute(text: &str, vars: &[(String, String)]) -> String {
    let mut text = text.to_string();
    for (name, value) in vars.iter().rev() {
        let names: Vec<&str> = name.split(',').collect();
        let values: Vec<&str> = if names.len() > 1 {
            value.split(',').collect()
        } else {
            vec![value.as_str()]
        };
        for (n, v) in names.iter().zip(values) {
            text = text.replace(&format!("${{{n}}}"), v);
            text = text.replace(&format!("{{{n}}}"), v);
        }
    }
    text
}

fn expand(nodes: &[Node], vars: &mut Vec<(String, String)>, out: &mut Vec<Item>) {
    for node in nodes {
        match node {
            Node::Item(item) => out.push(item.clone()),
            Node::Record {
                line,
                conditions,
                skipped,
                duckdb_skips,
                sql,
                kind,
            } => {
                if let Some(why) = skipped {
                    out.push(Item::Skip {
                        line: *line,
                        why: why.clone(),
                        sql: (!duckdb_skips).then(|| substitute(sql, vars)),
                    });
                    continue;
                }
                let map: HashMap<String, String> = vars.iter().cloned().collect();
                let mut run = true;
                for (skip_if, cond) in conditions {
                    match condition_holds(cond, &map) {
                        Some(holds) => run &= holds != *skip_if,
                        None => run = false,
                    }
                }
                if !run {
                    out.push(Item::Skip {
                        line: *line,
                        why: "loop condition".into(),
                        sql: None,
                    });
                    continue;
                }
                out.push(Item::Run(Record {
                    line: *line,
                    sql: substitute(sql, vars),
                    kind: kind.clone(),
                }));
            }
            Node::Loop { var, values, body } => match values {
                None => {
                    let mut lines = Vec::new();
                    collect_lines(body, &mut lines);
                    for (line, sql) in lines {
                        out.push(Item::Skip {
                            line,
                            why: "a foreach group the harness does not expand".into(),
                            sql: Some(sql),
                        });
                    }
                }
                Some(values) => {
                    for v in values {
                        vars.push((var.clone(), v.clone()));
                        expand(body, vars, out);
                        vars.pop();
                    }
                }
            },
        }
    }
}

fn collect_lines(nodes: &[Node], out: &mut Vec<(usize, String)>) {
    for n in nodes {
        match n {
            Node::Record { line, sql, .. } => out.push((*line, sql.clone())),
            Node::Loop { body, .. } => collect_lines(body, out),
            Node::Item(_) => {}
        }
    }
}
