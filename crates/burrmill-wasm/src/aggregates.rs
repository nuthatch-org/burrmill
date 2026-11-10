//! Aggregates mode. A group leaves only when this crate has counted at least
//! [`AGGREGATE_K`] source rows in it.

use sqlparser::ast::{
    DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, Visit, Visitor,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

use crate::session::QueryOut;
use serde_json::{Value, json};

/// Default group size from RFC-0047. Q2, what k should be and whether it is per
/// column, is still open.
pub const AGGREGATE_K: u64 = 10;

const SKIP: &str = "not a grouped read of t";
const TAINTED: &str = "a subquery or window is not a group";
const UNMEASURED: &str = "group size was not measured";

/// Rewrite a grouped read of `t` so `count(*) AS group_n` is measured here.
/// A count the model wrote is removed: `10 AS group_n` would let a small group through.
pub(crate) fn measure_sql(sql: &str) -> Result<String, &'static str> {
    let mut query = parse_one(sql)?;
    if !query_ok(&query) {
        return Err(SKIP);
    }
    {
        let select = outermost(&query.body)?;
        accept(select)?;
    }
    let Some(select) = outermost_mut(&mut query.body) else {
        return Err(SKIP);
    };
    // HAVING and LIMIT can hide how many groups were under k.
    select.having = None;
    select.projection.retain(|item| !alias_is_group_n(item));
    select.projection.push(count_star());
    clear_limits(&mut query);
    Ok(Statement::Query(query).to_string())
}

pub(crate) fn skipped(reason: &str) -> String {
    json!({
        "k": AGGREGATE_K,
        "dropped": Value::Null,
        "groups": [],
        "omitted": reason,
    })
    .to_string()
}

pub(crate) fn filter_payload(out: &QueryOut) -> String {
    let Some(idx) = out
        .columns
        .iter()
        .rposition(|c| c.name.eq_ignore_ascii_case("group_n") && c.type_name == "INTEGER")
    else {
        return skipped(UNMEASURED);
    };
    let mut groups = Vec::new();
    let mut dropped = 0u64;
    for row in &out.rows {
        match row
            .get(idx)
            .and_then(|cell| cell.as_deref())
            .and_then(parse_size)
        {
            Some(n) if n >= AGGREGATE_K => groups.push(row_object(&out.columns, row)),
            _ => dropped += 1,
        }
    }
    let columns: Vec<_> = out
        .columns
        .iter()
        .map(|c| json!({ "name": c.name, "type": c.type_name }))
        .collect();
    json!({
        "k": AGGREGATE_K,
        "dropped": dropped,
        "columns": columns,
        "groups": groups,
        "truncated": out.truncated,
    })
    .to_string()
}

fn parse_size(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn row_object(columns: &[crate::session::Column], row: &[Option<String>]) -> Value {
    let mut obj = serde_json::Map::new();
    for (column, cell) in columns.iter().zip(row) {
        obj.insert(
            column.name.clone(),
            match cell {
                Some(text) => Value::String(text.clone()),
                None => Value::Null,
            },
        );
    }
    Value::Object(obj)
}

fn parse_one(sql: &str) -> Result<Box<Query>, &'static str> {
    let mut statements = Parser::parse_sql(&DuckDbDialect {}, sql).map_err(|_| SKIP)?;
    if statements.len() != 1 {
        return Err(SKIP);
    }
    match statements.pop() {
        Some(Statement::Query(query)) => Ok(query),
        _ => Err(SKIP),
    }
}

fn query_ok(query: &Query) -> bool {
    query.locks.is_empty()
        && query.for_clause.is_none()
        && query.settings.is_none()
        && query.format_clause.is_none()
        && query.pipe_operators.is_empty()
}

fn outermost(body: &SetExpr) -> Result<&Select, &'static str> {
    match body {
        SetExpr::Select(select) => Ok(select),
        SetExpr::Query(inner) => outermost(&inner.body),
        _ => Err(SKIP),
    }
}

fn outermost_mut(body: &mut SetExpr) -> Option<&mut Select> {
    match body {
        SetExpr::Select(select) => Some(select),
        SetExpr::Query(inner) => outermost_mut(&mut inner.body),
        _ => None,
    }
}

fn clear_limits(query: &mut Query) {
    query.limit_clause = None;
    query.fetch = None;
    if let SetExpr::Query(inner) = query.body.as_mut() {
        clear_limits(inner);
    }
}

fn accept(select: &Select) -> Result<(), &'static str> {
    if !plain(select) || !from_table_t(select) {
        return Err(SKIP);
    }
    let exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() && !exprs.is_empty() => {
            exprs
        }
        _ => return Err(SKIP),
    };
    if exprs.iter().any(tainted) {
        return Err(TAINTED);
    }
    items_ok(&select.projection)
}

fn plain(select: &Select) -> bool {
    select.distinct.is_none()
        && select.optimizer_hints.is_empty()
        && select
            .select_modifiers
            .as_ref()
            .is_none_or(|m| !m.is_any_set())
        && select.top.is_none()
        && select.into.is_none()
        && select.exclude.is_none()
        && select.lateral_views.is_empty()
        && select.prewhere.is_none()
        && select.connect_by.is_empty()
        && select.cluster_by.is_empty()
        && select.distribute_by.is_empty()
        && select.sort_by.is_empty()
        && select.named_window.is_empty()
        && select.qualify.is_none()
        && select.value_table_mode.is_none()
}

fn from_table_t(select: &Select) -> bool {
    let [only] = select.from.as_slice() else {
        return false;
    };
    if !only.joins.is_empty() {
        return false;
    }
    let TableFactor::Table {
        name,
        args: None,
        with_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
        ..
    } = &only.relation
    else {
        return false;
    };
    if !with_hints.is_empty() || !partitions.is_empty() || !index_hints.is_empty() {
        return false;
    }
    let parts: Vec<&str> = name
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => ident.value.as_str(),
            _ => "",
        })
        .collect();
    match parts.as_slice() {
        [table] => table.eq_ignore_ascii_case("t"),
        [schema, table] => schema.eq_ignore_ascii_case("main") && table.eq_ignore_ascii_case("t"),
        _ => false,
    }
}

fn items_ok(items: &[SelectItem]) -> Result<(), &'static str> {
    if items.is_empty() {
        return Err(SKIP);
    }
    for item in items {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return Err(SKIP),
            SelectItem::UnnamedExpr(expr)
            | SelectItem::ExprWithAlias { expr, .. }
            | SelectItem::ExprWithAliases { expr, .. } => {
                if tainted(expr) {
                    return Err(TAINTED);
                }
            }
        }
    }
    Ok(())
}

fn alias_is_group_n(item: &SelectItem) -> bool {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.eq_ignore_ascii_case("group_n"),
        _ => false,
    }
}

fn tainted(expr: &Expr) -> bool {
    struct Taint(bool);
    impl Visitor for Taint {
        type Break = ();

        fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
            self.0 = true;
            ControlFlow::Break(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            let bad = match expr {
                Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => true,
                Expr::Function(function) => function.over.is_some(),
                _ => false,
            };
            if bad {
                self.0 = true;
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    let mut taint = Taint(false);
    let _ = expr.visit(&mut taint);
    taint.0
}

fn count_star() -> SelectItem {
    SelectItem::ExprWithAlias {
        expr: Expr::Function(Function {
            name: ObjectName::from(Ident::new("count")),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: Option::<DuplicateTreatment>::None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        }),
        alias: Ident::new("group_n"),
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::session::Session;
    use burrmill_ingest::{Budget, FormatHint};

    fn grouped_file() -> (Session, Vec<String>) {
        let mut lines = vec!["merchant,amount".to_string()];
        let mut forbidden = Vec::new();
        for n in 1..AGGREGATE_K {
            let merchant = format!("CANARY_SMALL_{n:02}");
            let amount = format!("{n}.01");
            let cents = n * (n * 100 + 1);
            let sum = format!("{}.{:02}", cents / 100, cents % 100);
            forbidden.push(merchant.clone());
            forbidden.push(amount.clone());
            if sum != amount {
                forbidden.push(sum);
            }
            for _ in 0..n {
                lines.push(format!("{merchant},{amount}"));
            }
        }
        for _ in 0..AGGREGATE_K {
            lines.push("KEPT_GROUP,8.00".into());
        }
        let table = burrmill_ingest::ingest_bytes(
            lines.join("\n").as_bytes(),
            FormatHint::Sniff,
            &Budget::untrusted(),
        )
        .unwrap();
        (Session::open(table).unwrap(), forbidden)
    }

    fn assert_no(label: &str, text: &str, forbidden: &[String]) {
        for word in forbidden {
            assert!(!text.contains(word), "{label} contains {word}: {text}");
        }
    }

    #[test]
    fn aggregates_mode_respects_k() {
        let (s, forbidden) = grouped_file();
        let sql = "SELECT merchant, sum(amount) AS total FROM t GROUP BY merchant";
        let local = s.query(sql).unwrap();
        assert!(
            local
                .rows
                .iter()
                .any(|row| row[0].as_deref() == Some("CANARY_SMALL_01"))
        );

        let payload = s.aggregate_feedback(sql).unwrap();
        assert_no("grouped", &payload, &forbidden);
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["k"], AGGREGATE_K);
        assert_eq!(value["dropped"], AGGREGATE_K - 1);
        assert_eq!(value["groups"].as_array().unwrap().len(), 1);
        assert_eq!(value["groups"][0]["merchant"], "KEPT_GROUP");
        assert_eq!(value["groups"][0]["total"], "80.00");
        assert_eq!(value["groups"][0]["group_n"], AGGREGATE_K.to_string());

        let lied = "SELECT merchant, sum(amount) AS total, 10 AS group_n FROM t GROUP BY merchant";
        let lied_payload = s.aggregate_feedback(lied).unwrap();
        assert_no("literal group_n", &lied_payload, &forbidden);
        let lied_value: Value = serde_json::from_str(&lied_payload).unwrap();
        assert_eq!(lied_value["groups"].as_array().unwrap().len(), 1);
        assert_eq!(lied_value["dropped"], AGGREGATE_K - 1);

        let wrapped = "WITH bound AS (SELECT 1 AS n) SELECT merchant, sum(amount) AS total FROM t WHERE (SELECT n FROM bound) = 1 GROUP BY merchant";
        let wrapped_payload = s.aggregate_feedback(wrapped).unwrap();
        assert_no("where subquery", &wrapped_payload, &forbidden);
        let wrapped_value: Value = serde_json::from_str(&wrapped_payload).unwrap();
        assert_eq!(wrapped_value["groups"][0]["merchant"], "KEPT_GROUP");
    }

    #[test]
    fn rows_and_unmeasured_queries_send_no_cell() {
        let (s, forbidden) = grouped_file();
        let queries = [
            "SELECT merchant, amount FROM t",
            "SELECT merchant, sum(amount) AS total FROM (SELECT * FROM t) AS s GROUP BY merchant",
            "WITH other AS (SELECT merchant, amount FROM t) SELECT merchant, sum(amount) AS total FROM other GROUP BY merchant",
            "SELECT merchant, (SELECT max(merchant) FROM t) AS peek, sum(amount) AS total FROM t GROUP BY merchant",
            "SELECT t.merchant, sum(t.amount) AS total FROM t JOIN t AS u ON t.merchant = u.merchant GROUP BY t.merchant",
            "SELECT * FROM t GROUP BY merchant",
        ];
        for sql in queries {
            let payload = s.aggregate_feedback(sql).unwrap();
            assert_no(sql, &payload, &forbidden);
            assert!(!payload.contains("KEPT_GROUP"), "{sql}: {payload}");
            let value: Value = serde_json::from_str(&payload).unwrap();
            assert!(value["dropped"].is_null(), "{sql}: {payload}");
            assert!(value["groups"].as_array().unwrap().is_empty(), "{sql}");
            assert!(value["omitted"].is_string(), "{sql}: {payload}");
        }
    }
}
