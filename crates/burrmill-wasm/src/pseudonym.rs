//! Pseudonymised mode. Marked values leave as `CUSTOMER_N`. The map stays here.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use arrow::record_batch::RecordBatch;
use burrmill_ingest::InferredType;
use serde::Serialize;
use serde_json::{Value, json};
use sqlparser::ast::{
    Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, Visit, Visitor,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use crate::format::cell_string;
use crate::session::QueryOut;

const EXPR: &str = "an identifier column is used in an expression";
const UNTOKENISED: &str = "an identifier value was not tokenised";
const NOT_ONE: &str = "the statement was not one query";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Email,
    Iban,
    Phone,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Email => "email",
            Self::Iban => "iban",
            Self::Phone => "phone",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Proposal {
    pub name: String,
    pub kind: &'static str,
}

/// `CUSTOMER_N` assigned in first-seen order. `forward` is the value, `back` restores it.
#[derive(Clone, Debug, Default)]
pub struct TokenMap {
    forward: BTreeMap<String, String>,
    back: BTreeMap<String, String>,
}

impl TokenMap {
    pub fn token(&self, value: &str) -> Option<&str> {
        self.forward.get(value).map(String::as_str)
    }

    pub fn restorations(&self) -> &BTreeMap<String, String> {
        &self.back
    }

    /// A cell that merely contains a tokenised value still carries it.
    /// An exact hit is replaced by the caller and is not withheld here.
    fn hides(&self, text: &str) -> bool {
        self.forward
            .keys()
            .any(|value| value.len() >= 8 && value != text && text.contains(value.as_str()))
    }

    fn insert(&mut self, value: String, token: String) {
        self.back.insert(token.clone(), value.clone());
        self.forward.insert(value, token);
    }
}

pub fn proposals(
    names: &[String],
    inferred: &[InferredType],
    batches: &[RecordBatch],
) -> Vec<Proposal> {
    let mut out = Vec::new();
    for (i, name) in names.iter().enumerate() {
        if inferred
            .get(i)
            .is_some_and(|t| matches!(t, InferredType::Text))
            && let Some(kind) = column_kind(batches, i)
        {
            out.push(Proposal {
                name: name.clone(),
                kind: kind.name(),
            });
        }
    }
    out
}

/// Row order, then the table's column order. The same value keeps one token,
/// including across columns, so a later query still names that entity.
/// Checkbox order is not this order.
pub fn assign(names: &[String], marked: &BTreeSet<String>, batches: &[RecordBatch]) -> TokenMap {
    let mut map = TokenMap::default();
    let mut n = 0u64;
    for batch in batches {
        for r in 0..batch.num_rows() {
            for (i, name) in names.iter().enumerate() {
                if !marked.contains(name) {
                    continue;
                }
                let Some(value) = cell_string(batch.column(i), r) else {
                    continue;
                };
                if value.is_empty() || map.forward.contains_key(&value) {
                    continue;
                }
                n += 1;
                map.insert(value, format!("CUSTOMER_{n}"));
            }
        }
    }
    map
}

pub fn projection_allows(sql: &str, marked: &BTreeSet<String>) -> Result<(), &'static str> {
    let mut statements = Parser::parse_sql(&DuckDbDialect {}, sql).map_err(|_| NOT_ONE)?;
    if statements.len() != 1 {
        return Err(NOT_ONE);
    }
    let Some(Statement::Query(query)) = statements.pop() else {
        return Err(NOT_ONE);
    };
    check_query(&query, marked)
}

pub fn mask(out: &QueryOut, tokens: &TokenMap, bound: bool) -> Result<Vec<Value>, &'static str> {
    let mut rows = Vec::with_capacity(out.rows.len());
    for row in &out.rows {
        let mut obj = serde_json::Map::new();
        for (column, cell) in out.columns.iter().zip(row) {
            obj.insert(column.name.clone(), mask_cell(cell, tokens, bound)?);
        }
        rows.push(Value::Object(obj));
    }
    Ok(rows)
}

pub fn omitted(identifiers: &[String], reason: &str) -> String {
    json!({
        "identifiers": identifiers,
        "rows": [],
        "omitted": reason,
    })
    .to_string()
}

pub fn payload(identifiers: &[String], out: &QueryOut, rows: Vec<Value>) -> String {
    let columns: Vec<_> = out
        .columns
        .iter()
        .map(|c| json!({ "name": c.name, "type": c.type_name }))
        .collect();
    json!({
        "identifiers": identifiers,
        "columns": columns,
        "rows": rows,
        "truncated": out.truncated,
    })
    .to_string()
}

fn mask_cell(cell: &Option<String>, tokens: &TokenMap, bound: bool) -> Result<Value, &'static str> {
    let Some(text) = cell else {
        return Ok(Value::Null);
    };
    if let Some(token) = tokens.token(text) {
        return Ok(Value::String(token.to_string()));
    }
    if bound && kind_of(text).is_some() || tokens.hides(text) {
        return Err(UNTOKENISED);
    }
    Ok(Value::String(text.clone()))
}

fn column_kind(batches: &[RecordBatch], idx: usize) -> Option<Kind> {
    let mut kind: Option<Kind> = None;
    let mut any = false;
    for batch in batches {
        let col = batch.column(idx);
        for r in 0..batch.num_rows() {
            let Some(text) = cell_string(col, r) else {
                continue;
            };
            if text.is_empty() {
                continue;
            }
            let got = kind_of(&text)?;
            if kind.is_some_and(|k| k != got) {
                return None;
            }
            kind = Some(got);
            any = true;
        }
    }
    any.then_some(kind).flatten()
}

fn kind_of(text: &str) -> Option<Kind> {
    if is_email(text) {
        Some(Kind::Email)
    } else if is_iban(text) {
        Some(Kind::Iban)
    } else if is_phone(text) {
        Some(Kind::Phone)
    } else {
        None
    }
}

fn is_email(text: &str) -> bool {
    if text.chars().any(|c| c.is_whitespace()) {
        return false;
    }
    let Some((local, domain)) = text.split_once('@') else {
        return false;
    };
    if local.is_empty() || domain.is_empty() || domain.contains('@') {
        return false;
    }
    if !charset(local, true) || !charset(domain, false) || !domain.contains('.') {
        return false;
    }
    let Some((_, tld)) = domain.rsplit_once('.') else {
        return false;
    };
    tld.len() >= 2 && tld.chars().all(|c| c.is_ascii_alphabetic())
}

fn charset(text: &str, local: bool) -> bool {
    text.chars().all(|c| {
        c.is_ascii_alphanumeric()
            || c == '.'
            || c == '-'
            || (local && (c == '_' || c == '%' || c == '+'))
    })
}

fn is_iban(text: &str) -> bool {
    let compact: String = text.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    if !(15..=34).contains(&compact.len()) {
        return false;
    }
    let upper = compact.to_ascii_uppercase();
    let b = upper.as_bytes();
    if !(b[0].is_ascii_uppercase()
        && b[1].is_ascii_uppercase()
        && b[2].is_ascii_digit()
        && b[3].is_ascii_digit()
        && b[4..].iter().all(|c| c.is_ascii_alphanumeric()))
    {
        return false;
    }
    let rearranged = format!("{}{}", &upper[4..], &upper[..4]);
    let mut rem = 0u32;
    for c in rearranged.bytes() {
        let digits = if c.is_ascii_digit() {
            [c - b'0', 0]
        } else if c.is_ascii_uppercase() {
            let n = c - b'A' + 10;
            [n / 10, n % 10]
        } else {
            return false;
        };
        let width = if c.is_ascii_digit() { 1 } else { 2 };
        for d in &digits[..width] {
            rem = (rem * 10 + u32::from(*d)) % 97;
        }
    }
    rem == 1
}

fn is_phone(text: &str) -> bool {
    if text.is_empty() || text.starts_with('-') || text.contains(':') || date_shaped(text) {
        return false;
    }
    let b = text.as_bytes();
    let mut i = 0;
    let mut plus = false;
    if b[0] == b'+' {
        plus = true;
        i = 1;
    }
    let mut digits = 0;
    let mut sep = false;
    while i < b.len() {
        match b[i] {
            b'0'..=b'9' => digits += 1,
            b' ' | b'-' | b'.' | b'(' | b')' => sep = true,
            _ => return false,
        }
        i += 1;
    }
    (plus || sep) && (8..=15).contains(&digits)
}

fn date_shaped(text: &str) -> bool {
    let b = text.as_bytes();
    if b.len() != 10 {
        return false;
    }
    let digit = |i: usize| b[i].is_ascii_digit();
    (digit(0)
        && digit(1)
        && digit(2)
        && digit(3)
        && b[4] == b'-'
        && digit(5)
        && digit(6)
        && b[7] == b'-'
        && digit(8)
        && digit(9))
        || (digit(0)
            && digit(1)
            && b[2] == b'.'
            && digit(3)
            && digit(4)
            && b[5] == b'.'
            && digit(6)
            && digit(7)
            && digit(8)
            && digit(9))
}

fn check_query(query: &Query, marked: &BTreeSet<String>) -> Result<(), &'static str> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            check_query(&cte.query, marked)?;
        }
    }
    check_set(&query.body, marked)
}

fn check_set(body: &SetExpr, marked: &BTreeSet<String>) -> Result<(), &'static str> {
    match body {
        SetExpr::Select(select) => check_select(select, marked),
        SetExpr::Query(query) => check_query(query, marked),
        SetExpr::SetOperation { left, right, .. } => {
            check_set(left, marked)?;
            check_set(right, marked)
        }
        _ => Err(EXPR),
    }
}

fn check_select(select: &Select, marked: &BTreeSet<String>) -> Result<(), &'static str> {
    for item in &select.projection {
        check_item(item, marked)?;
    }
    for table in &select.from {
        check_factor(&table.relation, marked)?;
        for join in &table.joins {
            check_factor(&join.relation, marked)?;
        }
    }
    if let Some(expr) = &select.selection {
        check_expr_queries(expr, marked)?;
    }
    if let Some(expr) = &select.having {
        check_expr_queries(expr, marked)?;
    }
    Ok(())
}

fn check_item(item: &SelectItem, marked: &BTreeSet<String>) -> Result<(), &'static str> {
    let expr = match item {
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return Ok(()),
        SelectItem::UnnamedExpr(expr)
        | SelectItem::ExprWithAlias { expr, .. }
        | SelectItem::ExprWithAliases { expr, .. } => expr,
    };
    if is_bare(expr) {
        return Ok(());
    }
    if mentions(expr, marked) {
        return Err(EXPR);
    }
    check_expr_queries(expr, marked)
}

fn check_factor(factor: &TableFactor, marked: &BTreeSet<String>) -> Result<(), &'static str> {
    match factor {
        TableFactor::Derived { subquery, .. } => check_query(subquery, marked),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            check_factor(&table_with_joins.relation, marked)?;
            for join in &table_with_joins.joins {
                check_factor(&join.relation, marked)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn check_expr_queries(expr: &Expr, marked: &BTreeSet<String>) -> Result<(), &'static str> {
    struct Walk<'a> {
        marked: &'a BTreeSet<String>,
        err: Option<&'static str>,
    }
    impl Visitor for Walk<'_> {
        type Break = ();

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            match check_query(query, self.marked) {
                Ok(()) => ControlFlow::Continue(()),
                Err(reason) => {
                    self.err = Some(reason);
                    ControlFlow::Break(())
                }
            }
        }
    }
    let mut walk = Walk { marked, err: None };
    let _ = expr.visit(&mut walk);
    match walk.err {
        Some(reason) => Err(reason),
        None => Ok(()),
    }
}

fn is_bare(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
}

fn mentions(expr: &Expr, marked: &BTreeSet<String>) -> bool {
    struct Hit<'a> {
        marked: &'a BTreeSet<String>,
        hit: bool,
    }
    impl Visitor for Hit<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            let name = match expr {
                Expr::Identifier(id) => Some(id.value.as_str()),
                Expr::CompoundIdentifier(parts) => parts.last().map(|id| id.value.as_str()),
                _ => None,
            };
            if name.is_some_and(|n| self.marked.iter().any(|m| m.eq_ignore_ascii_case(n))) {
                self.hit = true;
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    let mut hit = Hit { marked, hit: false };
    let _ = expr.visit(&mut hit);
    hit.hit
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::chart::{Chart, ChartKind};
    use crate::session::Session;
    use burrmill_ingest::{Budget, FormatHint};

    // A leading "+" and only digits is an integer to ingest, so it is not proposed.
    // The phone column keeps a separator and stays text.
    const CSV: &str = "\
merchant,email,iban,phone,amount,mixed,bare
KEPT_MERCHANT,canary.one@example.com,GB82WEST12345698765432,+359 888 100001,-10.00,canary.one@example.com,+359888100009
OTHER_MERCHANT,canary.two@example.com,DE89370400440532013000,+359 888 100002,-3.50,hello,+359888100008
";

    const RAWS: &[&str] = &[
        "canary.one@example.com",
        "canary.two@example.com",
        "GB82WEST12345698765432",
        "DE89370400440532013000",
        "+359 888 100001",
        "+359 888 100002",
    ];

    fn session() -> Session {
        let table =
            burrmill_ingest::ingest_bytes(CSV.as_bytes(), FormatHint::Sniff, &Budget::untrusted())
                .unwrap();
        Session::open(table).unwrap()
    }

    fn bound() -> Session {
        let mut s = session();
        s.bind(&["email".into(), "iban".into(), "phone".into()]);
        s
    }

    fn assert_no(label: &str, text: &str) {
        for word in RAWS {
            assert!(!text.contains(word), "{label} contains {word}: {text}");
        }
    }

    #[test]
    fn classifiers_match_the_shapes_and_not_the_neighbours() {
        assert!(is_email("canary.one@example.com"));
        assert!(is_email("CANARY.ONE@EXAMPLE.COM"));
        assert!(!is_email("not-an-email"));
        assert!(!is_email("canary.one@example"));
        assert!(is_iban("GB82WEST12345698765432"));
        assert!(is_iban("DE89370400440532013000"));
        assert!(is_iban("GB82 WEST 1234 5698 7654 32"));
        assert!(!is_iban("GB82WEST12345698765433"));
        assert!(is_phone("+359888100001"));
        assert!(is_phone("+359 888 100001"));
        assert!(is_phone("359 888 100001"));
        assert!(!is_phone("123456789012"));
        assert!(!is_phone("-10.00"));
        assert!(!is_phone("2024-01-02"));
        assert!(!is_phone("01.02.2024"));
        assert!(!is_phone("2024-01-02T00:00:00"));
    }

    #[test]
    fn proposals_are_the_whole_column() {
        let s = session();
        let got: Vec<_> = s
            .proposals()
            .into_iter()
            .map(|p| (p.name, p.kind))
            .collect();
        assert_eq!(
            got,
            vec![
                ("email".into(), "email"),
                ("iban".into(), "iban"),
                ("phone".into(), "phone"),
            ]
        );
        let card = s.card(&[], &[]).unwrap();
        assert_no("card", &card);
    }

    #[test]
    fn tokens_are_stable_and_numbered_in_column_order() {
        let csv = "\
email,iban,phone
canary.one@example.com,GB82WEST12345698765432,+359 888 100001
canary.one@example.com,DE89370400440532013000,+359 888 100002
";
        let table =
            burrmill_ingest::ingest_bytes(csv.as_bytes(), FormatHint::Sniff, &Budget::untrusted())
                .unwrap();
        let mut s = Session::open(table).unwrap();
        s.bind(&["phone".into(), "email".into(), "iban".into()]);
        let payload = s
            .pseudonym_feedback("SELECT email, iban, phone FROM t")
            .unwrap();
        assert_no("stable", &payload);
        assert!(!payload.contains('@'), "{payload}");
        for token in [
            "CUSTOMER_1",
            "CUSTOMER_2",
            "CUSTOMER_3",
            "CUSTOMER_4",
            "CUSTOMER_5",
        ] {
            assert!(payload.contains(token), "{token} missing: {payload}");
        }
        assert!(!payload.contains("CUSTOMER_6"), "{payload}");
        let value: Value = serde_json::from_str(&payload).unwrap();
        let emails: Vec<_> = value["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["email"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(emails.len(), 2);
        assert!(
            emails.iter().all(|email| email == "CUSTOMER_1"),
            "{emails:?}"
        );
    }

    #[test]
    fn a_grouped_read_sends_tokens_not_values() {
        let s = bound();
        let sql = "SELECT merchant, email, iban, phone, sum(amount) AS total FROM t GROUP BY merchant, email, iban, phone";
        let payload = s.pseudonym_feedback(sql).unwrap();
        assert_no("grouped", &payload);
        assert!(!payload.contains('@'), "{payload}");
        assert!(payload.contains("CUSTOMER_1"), "{payload}");
        assert!(payload.contains("KEPT_MERCHANT"), "{payload}");
        assert!(payload.contains("OTHER_MERCHANT"), "{payload}");
        assert!(payload.contains("-10.00"), "{payload}");
        assert!(payload.contains("-3.50"), "{payload}");
        let local = s.query(sql).unwrap();
        assert!(local.rows.iter().any(|row| {
            row.iter()
                .any(|cell| cell.as_deref() == Some("canary.one@example.com"))
        }));
    }

    #[test]
    fn an_expression_sends_nothing() {
        let s = bound();
        for sql in [
            "SELECT upper(email) AS email FROM t",
            "SELECT email || 'x' AS email FROM t",
            "SELECT count(email) AS n FROM t",
            "SELECT CAST(email AS VARCHAR) AS email FROM t",
        ] {
            let payload = s.pseudonym_feedback(sql).unwrap();
            assert_no(sql, &payload);
            assert!(!payload.contains("canary"), "{sql}: {payload}");
            let value: Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(value["omitted"], EXPR, "{sql}: {payload}");
            assert!(value["rows"].as_array().unwrap().is_empty(), "{sql}");
        }
    }

    #[test]
    fn a_copied_value_sends_nothing() {
        let csv = "\
merchant,email
prefix-canary.one@example.com-suffix,canary.one@example.com
";
        let table =
            burrmill_ingest::ingest_bytes(csv.as_bytes(), FormatHint::Sniff, &Budget::untrusted())
                .unwrap();
        let mut s = Session::open(table).unwrap();
        s.bind(&["email".into()]);
        let payload = s.pseudonym_feedback("SELECT merchant FROM t").unwrap();
        assert!(!payload.contains("canary.one@example.com"), "{payload}");
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["omitted"], UNTOKENISED);

        let mut phone = session();
        phone.bind(&["email".into()]);
        let payload = phone.pseudonym_feedback("SELECT phone FROM t").unwrap();
        assert!(!payload.contains("+359 888 100001"), "{payload}");
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["omitted"], UNTOKENISED);
    }

    #[test]
    fn narration_restores_tokens_locally() {
        let s = bound();
        let out = s.query("SELECT merchant, email FROM t").unwrap();
        let chart = Chart {
            kind: ChartKind::Table,
            x: None,
            y: vec![],
        };
        let shown = s.present_bound(&out, &chart, "Person CUSTOMER_1.", ",", ".");
        assert_eq!(
            shown.narration.as_deref(),
            Some("Person canary.one@example.com.")
        );
        let unknown = s.present_bound(&out, &chart, "Person CUSTOMER_99.", ",", ".");
        let err = unknown.narration_error.unwrap();
        assert_eq!(err, "narration has a digit outside a slot");
        assert!(!err.chars().any(|c| c.is_ascii_digit()));
        let phone = s.present_bound(&out, &chart, "Call CUSTOMER_3.", ",", ".");
        assert_eq!(phone.narration.as_deref(), Some("Call +359 888 100001."));

        let mut cleared = bound();
        cleared.bind(&[]);
        let cleared = cleared.present_bound(&out, &chart, "Person CUSTOMER_1.", ",", ".");
        assert_eq!(
            cleared.narration_error.as_deref(),
            Some("narration has a digit outside a slot")
        );
    }
}
