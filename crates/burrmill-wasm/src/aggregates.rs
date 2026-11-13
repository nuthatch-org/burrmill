//! Aggregates mode. A group leaves only when this crate has counted at least
//! [`AGGREGATE_K`] source rows in it.

use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, GroupByExpr, Ident, ObjectName, ObjectNamePart,
    OrderByKind, Query, Select, SelectItem, SetExpr, Statement, TableFactor, UnaryOperator,
    Value as SqlValue, Visit, Visitor,
};
use std::ops::ControlFlow;

use crate::session::{Failure, QueryOut};
use serde_json::{Value, json};

/// The smallest group that leaves (`docs/browser.md`).
pub const AGGREGATE_K: u64 = 10;

const SKIP: &str = "not a grouped read of t";
const TAINTED: &str = "a subquery or window is not a group";
const UNMEASURED: &str = "group size was not measured";
const NOT_SUMMARY: &str = "only grouping keys and count, sum or avg of a column leave";
const SHADOWED: &str = "a CTE named t is not the upload";
const FAILED: &str = "the statement failed while reading rows";
const COUNTED: &str = "a count leaves only as itself, or as what a sum or avg is divided by";
const HAVING_COUNT: &str = "HAVING over a count";
const LIMIT_COUNT: &str = "ORDER BY a count with a limit";

pub(crate) struct Measured {
    /// The statement as written, with `count(*) AS group_n` added to each group.
    pub answer: String,
    /// How many groups are under k, before `HAVING` or a limit can hide one.
    pub dropped: String,
    /// The output columns that are counts, `group_n` last, which leave as a [`bucket`].
    pub counts: Vec<usize>,
    /// What of the statement was not applied, because it would choose groups by an exact count.
    pub unapplied: Vec<&'static str>,
}

/// The size `n` leaves as: below k, then one order of magnitude, `10-99`, `100-999` and so on. One
/// member moves a count across a bucket only when the count sits on an edge.
pub(crate) fn bucket(n: u64) -> String {
    if n < AGGREGATE_K {
        return format!("0-{}", AGGREGATE_K - 1);
    }
    let mut low = AGGREGATE_K;
    while n / low >= 10 {
        low *= 10;
    }
    format!(
        "{low}-{}",
        low.checked_mul(10).map_or(u64::MAX, |high| high - 1)
    )
}

/// Rewrite a grouped read of `t` so `count(*) AS group_n` is measured here.
/// A count the model wrote is removed: `10 AS group_n` would let a small group through.
pub(crate) fn measure_sql(sql: &str) -> Result<Measured, &'static str> {
    let mut query = parse_one(sql)?;
    if !query_ok(&query) {
        return Err(SKIP);
    }
    if shadows_t(&query) {
        return Err(SHADOWED);
    }
    {
        let select = outermost(&query.body)?;
        accept(select)?;
    }
    let Some(select) = outermost_mut(&mut query.body) else {
        return Err(SKIP);
    };
    select.projection.retain(|item| !alias_is_group_n(item));
    let uses: Vec<CountUse> = select.projection.iter().map(item_use).collect();
    if uses.contains(&CountUse::Bad) {
        return Err(COUNTED);
    }
    // A name or position that stands for a count, as `HAVING` and `ORDER BY` may refer to one.
    let counted: Vec<(usize, Option<String>)> = select
        .projection
        .iter()
        .zip(&uses)
        .enumerate()
        .filter(|(_, (_, u))| **u != CountUse::Free)
        .map(|(i, (item, _))| (i, item_alias(item)))
        .collect();
    let mut unapplied = Vec::new();
    if select
        .having
        .as_ref()
        .is_some_and(|h| names_count(h, &counted, false))
    {
        select.having = None;
        unapplied.push(HAVING_COUNT);
    }
    let mut counts: Vec<usize> = (0..uses.len())
        .filter(|&i| uses[i] == CountUse::Whole)
        .collect();
    counts.push(uses.len());
    select.projection.push(count_star());
    if orders_by_count(&query, &counted) {
        clear_limits(&mut query);
        unapplied.push(LIMIT_COUNT);
    }
    let answer = Statement::Query(query.clone()).to_string();
    if let Some(select) = outermost_mut(&mut query.body) {
        select.having = None;
    }
    clear_limits(&mut query);
    let dropped = format!(
        "SELECT count(*) AS dropped FROM ({query}) AS measured WHERE group_n < {AGGREGATE_K}"
    );
    Ok(Measured {
        answer,
        dropped,
        counts,
        unapplied,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CountUse {
    /// No count in it.
    Free,
    /// A count, cast or rounded, which leaves as a bucket.
    Whole,
    /// A sum or avg divided by a count: the mean, which the sum and avg already say.
    Mean,
    /// Anything else over a count, such as `count(*) * 7` or `count(*) - 45`, which would put a
    /// bucket's edge wherever the statement chose.
    Bad,
}

fn item_use(item: &SelectItem) -> CountUse {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => count_use(expr),
        _ => CountUse::Free,
    }
}

fn item_alias(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_ascii_lowercase()),
        _ => None,
    }
}

fn count_use(expr: &Expr) -> CountUse {
    use CountUse::{Bad, Free, Mean, Whole};
    match expr {
        Expr::Nested(inner) | Expr::Cast { expr: inner, .. } => count_use(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => count_use(expr),
        Expr::UnaryOp { expr, .. } => match count_use(expr) {
            Free => Free,
            Mean => Mean,
            _ => Bad,
        },
        Expr::BinaryOp { left, op, right } => match (count_use(left), op, count_use(right)) {
            (Free, _, Free) => Free,
            (Free, BinaryOperator::Divide, Whole) if has_summary(left) => Mean,
            (Mean, _, Free | Mean) | (Free, _, Mean) => Mean,
            _ => Bad,
        },
        Expr::Function(function) if bare_name(function).as_deref() == Some("count") => Whole,
        Expr::Function(function) if bare_name(function).as_deref() == Some("round") => {
            let FunctionArguments::List(list) = &function.args else {
                return Free;
            };
            // `rounded` admits only a literal number of digits, so `round(count(*), -1)` is refused.
            match list.args.first() {
                Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))) => count_use(inner),
                _ => Free,
            }
        }
        _ => Free,
    }
}

/// Whether `expr` holds a `sum` or `avg`, so that dividing it by a count is a mean, not `1 / n`.
fn has_summary(expr: &Expr) -> bool {
    struct Has(bool);
    impl Visitor for Has {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if let Expr::Function(function) = expr
                && matches!(bare_name(function).as_deref(), Some("sum" | "avg"))
            {
                self.0 = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut has = Has(false);
    let _ = expr.visit(&mut has);
    has.0
}

/// Whether `expr` names a count: the function, or the alias or (with `ordinals`) the position of
/// an output that holds one.
fn names_count(expr: &Expr, counted: &[(usize, Option<String>)], ordinals: bool) -> bool {
    if ordinals
        && let Expr::Value(v) = expr
        && let SqlValue::Number(n, _) = &v.value
        && let Ok(n) = n.parse::<usize>()
    {
        return counted.iter().any(|(i, _)| *i + 1 == n);
    }
    struct Named<'a>(&'a [(usize, Option<String>)], bool);
    impl Visitor for Named<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            let hit = match expr {
                Expr::Function(function) => bare_name(function).as_deref() == Some("count"),
                Expr::Identifier(id) => {
                    let name = id.value.to_ascii_lowercase();
                    self.0
                        .iter()
                        .any(|(_, alias)| alias.as_deref() == Some(&name))
                }
                _ => false,
            };
            if hit {
                self.1 = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut named = Named(counted, false);
    let _ = expr.visit(&mut named);
    named.1
}

/// An `ORDER BY` over a count that a limit then cuts, at any level of the statement.
fn orders_by_count(query: &Query, counted: &[(usize, Option<String>)]) -> bool {
    let limited = query.limit_clause.is_some() || query.fetch.is_some();
    let ordered = match query.order_by.as_ref().map(|o| &o.kind) {
        Some(OrderByKind::All(_)) => true,
        Some(OrderByKind::Expressions(keys)) => {
            keys.iter().any(|key| names_count(&key.expr, counted, true))
        }
        None => false,
    };
    (limited && ordered)
        || match query.body.as_ref() {
            SetExpr::Query(inner) => orders_by_count(inner, counted),
            _ => false,
        }
}

/// Whether a grouped read failed can turn on one member (`error()` in a `WHERE`), so nothing of
/// how it failed goes with it.
pub(crate) fn ran_and_failed(failure: &Failure) -> String {
    skipped(if failure.before_rows {
        UNMEASURED
    } else {
        FAILED
    })
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

/// Every count in it is a [`bucket`]. An answer with no row and one whose groups are all under k
/// read alike, and the groups go in one order whatever `ORDER BY` said.
pub(crate) fn filter_payload(out: &QueryOut, dropped: &QueryOut, measured: &Measured) -> String {
    let Some(dropped) = dropped
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|cell| cell.as_deref())
        .and_then(parse_size)
    else {
        return skipped(UNMEASURED);
    };
    let mut groups = Vec::new();
    if !out.rows.is_empty() {
        let Some(idx) = out
            .columns
            .iter()
            .rposition(|c| c.name.eq_ignore_ascii_case("group_n") && c.type_name == "INTEGER")
        else {
            return skipped(UNMEASURED);
        };
        for row in &out.rows {
            if row
                .get(idx)
                .and_then(|cell| cell.as_deref())
                .and_then(parse_size)
                .is_some_and(|n| n >= AGGREGATE_K)
            {
                groups.push(row_object(&out.columns, row, &measured.counts, idx));
            }
        }
    }
    groups.sort_by_cached_key(Value::to_string);
    let any = !groups.is_empty();
    let mut payload = json!({
        "k": AGGREGATE_K,
        "dropped": bucket(dropped),
        "groups": groups,
        "truncated": out.truncated,
    });
    // The columns are known only from a row, so they go only with a group.
    if any {
        payload["columns"] = out
            .columns
            .iter()
            .map(|c| json!({ "name": c.name, "type": c.type_name }))
            .collect();
    }
    if !measured.unapplied.is_empty() {
        payload["not_applied"] = json!(measured.unapplied);
    }
    payload.to_string()
}

fn parse_size(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn row_object(
    columns: &[crate::session::Column],
    row: &[Option<String>],
    counts: &[usize],
    group_n: usize,
) -> Value {
    let mut obj = serde_json::Map::new();
    for (i, (column, cell)) in columns.iter().zip(row).enumerate() {
        let value = match cell {
            Some(text) if i == group_n || counts.contains(&i) => text
                .parse::<f64>()
                .ok()
                .filter(|n| n.is_finite() && *n >= 0.0)
                .map_or(Value::Null, |n| Value::String(bucket(n as u64))),
            Some(text) => Value::String(text.clone()),
            None => Value::Null,
        };
        obj.insert(column.name.clone(), value);
    }
    Value::Object(obj)
}

fn parse_one(sql: &str) -> Result<Box<Query>, &'static str> {
    let mut statements = burrmill::inspect::parse(sql).map_err(|_| SKIP)?;
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
    items_ok(&select.projection, exprs)?;
    match &select.having {
        Some(having) if !condition(having, &select.projection, exprs) => Err(NOT_SUMMARY),
        _ => Ok(()),
    }
}

/// `HAVING` chooses among groups by what may leave anyway, so `max(salary) > ..` cannot ask
/// about one member.
fn condition(expr: &Expr, items: &[SelectItem], keys: &[Expr]) -> bool {
    match expr {
        Expr::Nested(inner) => condition(inner, items, keys),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => condition(expr, items, keys),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                condition(left, items, keys) && condition(right, items, keys)
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => operand(left, items, keys) && operand(right, items, keys),
            _ => false,
        },
        _ => false,
    }
}

fn operand(expr: &Expr, items: &[SelectItem], keys: &[Expr]) -> bool {
    let named = |name: &Ident| {
        items.iter().any(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => {
                alias.value.eq_ignore_ascii_case(&name.value)
            }
            _ => false,
        })
    };
    match expr {
        Expr::Identifier(name) if named(name) => true,
        expr if keys.contains(expr) => true,
        expr => summary(expr),
    }
}

/// A CTE named `t` shadows the upload, so `FROM t` would read rows this crate never counted.
fn shadows_t(query: &Query) -> bool {
    struct Shadow(bool);
    impl Visitor for Shadow {
        type Break = ();

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            let named_t = query.with.as_ref().is_some_and(|with| {
                with.cte_tables
                    .iter()
                    .any(|cte| cte.alias.name.value.eq_ignore_ascii_case("t"))
            });
            if named_t {
                self.0 = true;
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    let mut shadow = Shadow(false);
    let _ = query.visit(&mut shadow);
    shadow.0
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

/// Each output is a grouping key or a summary. Anything else can carry one member's value out
/// of a group of k: `string_agg`, `min`, or `sum(CASE WHEN name = .. THEN salary END)`.
fn items_ok(items: &[SelectItem], keys: &[Expr]) -> Result<(), &'static str> {
    if items.is_empty() {
        return Err(SKIP);
    }
    for (i, item) in items.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return Err(SKIP),
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            SelectItem::ExprWithAliases { .. } => return Err(SKIP),
        };
        if tainted(expr) {
            return Err(TAINTED);
        }
        if !is_key(expr, alias, i, keys) && !summary(expr) {
            return Err(NOT_SUMMARY);
        }
    }
    Ok(())
}

fn is_key(expr: &Expr, alias: Option<&Ident>, index: usize, keys: &[Expr]) -> bool {
    keys.iter().any(|key| match key {
        Expr::Value(v) => matches!(&v.value, SqlValue::Number(n, _) if n.parse() == Ok(index + 1)),
        Expr::Identifier(name)
            if alias.is_some_and(|a| a.value.eq_ignore_ascii_case(&name.value)) =>
        {
            true
        }
        key => key == expr,
    })
}

fn summary(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        Expr::Nested(inner) | Expr::Cast { expr: inner, .. } => summary(inner),
        Expr::UnaryOp { op, expr } => {
            matches!(op, UnaryOperator::Minus | UnaryOperator::Plus) && summary(expr)
        }
        Expr::BinaryOp { left, op, right } => {
            matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            ) && summary(left)
                && summary(right)
        }
        Expr::Function(function) => counted(function) || rounded(function),
        _ => false,
    }
}

/// `count`, `sum` or `avg` of a bare column over the whole group, nothing filtered out of it.
fn counted(function: &Function) -> bool {
    let Some(name) = bare_name(function) else {
        return false;
    };
    let FunctionArguments::List(list) = &function.args else {
        return false;
    };
    let distinct_ok = match list.duplicate_treatment {
        None | Some(DuplicateTreatment::All) => true,
        Some(DuplicateTreatment::Distinct) => name == "count",
    };
    let [arg] = list.args.as_slice() else {
        return false;
    };
    let arg_ok = match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => name == "count",
        FunctionArg::Unnamed(FunctionArgExpr::Expr(
            Expr::Identifier(_) | Expr::CompoundIdentifier(_),
        )) => true,
        _ => false,
    };
    matches!(name.as_str(), "count" | "sum" | "avg")
        && distinct_ok
        && arg_ok
        && list.clauses.is_empty()
        && plain_call(function)
}

/// `round(summary)` or `round(summary, digits)`.
fn rounded(function: &Function) -> bool {
    let FunctionArguments::List(list) = &function.args else {
        return false;
    };
    let args: Vec<_> = list
        .args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
            _ => None,
        })
        .collect();
    let shape_ok = match args.as_slice() {
        [Some(value)] => summary(value),
        [Some(value), Some(Expr::Value(_))] => summary(value),
        _ => false,
    };
    bare_name(function).as_deref() == Some("round")
        && list.duplicate_treatment.is_none()
        && list.clauses.is_empty()
        && shape_ok
        && plain_call(function)
}

fn bare_name(function: &Function) -> Option<String> {
    match function.name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => Some(ident.value.to_ascii_lowercase()),
        _ => None,
    }
}

fn plain_call(function: &Function) -> bool {
    !function.uses_odbc_syntax
        && matches!(function.parameters, FunctionArguments::None)
        && function.filter.is_none()
        && function.null_treatment.is_none()
        && function.over.is_none()
        && function.within_group.is_empty()
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
        assert_eq!(value["dropped"], "0-9");
        assert_eq!(value["groups"].as_array().unwrap().len(), 1);
        assert_eq!(value["groups"][0]["merchant"], "KEPT_GROUP");
        assert_eq!(value["groups"][0]["total"], "80.00");
        assert_eq!(value["groups"][0]["group_n"], "10-99");

        let lied = "SELECT merchant, sum(amount) AS total, 10 AS group_n FROM t GROUP BY merchant";
        let lied_payload = s.aggregate_feedback(lied).unwrap();
        assert_no("literal group_n", &lied_payload, &forbidden);
        let lied_value: Value = serde_json::from_str(&lied_payload).unwrap();
        assert_eq!(lied_value["groups"].as_array().unwrap().len(), 1);
        assert_eq!(lied_value["dropped"], "0-9");

        let wrapped = "WITH bound AS (SELECT 1 AS n) SELECT merchant, sum(amount) AS total FROM t WHERE (SELECT n FROM bound) = 1 GROUP BY merchant";
        let wrapped_payload = s.aggregate_feedback(wrapped).unwrap();
        assert_no("where subquery", &wrapped_payload, &forbidden);
        let wrapped_value: Value = serde_json::from_str(&wrapped_payload).unwrap();
        assert_eq!(wrapped_value["groups"][0]["merchant"], "KEPT_GROUP");
    }

    #[test]
    fn a_member_value_does_not_leave_through_a_large_group() {
        let (s, forbidden) = grouped_file();
        let ten = ["SELECT * FROM x"; 10].join(" UNION ALL ");
        let queries = [
            "SELECT 1 AS g, string_agg(merchant, ',') AS e FROM t GROUP BY 1".to_string(),
            "SELECT 1 AS g, min(merchant) AS m FROM t GROUP BY 1".into(),
            "SELECT 1 AS g, max(amount) AS m FROM t GROUP BY 1".into(),
            "SELECT 1 AS g, list(merchant) AS m FROM t GROUP BY g".into(),
            "SELECT 1 AS g, any_value(merchant) AS m FROM t GROUP BY 1".into(),
            "SELECT 1 AS g, first(merchant) AS m FROM t GROUP BY 1".into(),
            "SELECT 1 AS g, sum(CASE WHEN merchant = 'CANARY_SMALL_05' THEN amount END) AS a FROM t GROUP BY 1".into(),
            "SELECT 1 AS g, count(*) FILTER (WHERE merchant = 'CANARY_SMALL_05') AS a FROM t GROUP BY 1".into(),
            "SELECT 1 AS g, sum(DISTINCT amount) AS a FROM t GROUP BY 1".into(),
            "SELECT 1 AS g, merchant FROM t GROUP BY 1".into(),
            "WITH t AS (SELECT a.* FROM t a, t b) SELECT merchant, sum(amount) AS total FROM t GROUP BY merchant".into(),
            format!("WITH x AS (SELECT * FROM t), t AS ({ten}) SELECT merchant, sum(amount) AS total FROM t GROUP BY merchant"),
            r#"WITH "T" AS (SELECT a.* FROM t a, t b) SELECT merchant, count(*) AS n FROM "T" GROUP BY merchant"#.into(),
        ];
        for sql in &queries {
            let payload = s.aggregate_feedback(sql).unwrap();
            assert_no(sql, &payload, &forbidden);
            let value: Value = serde_json::from_str(&payload).unwrap();
            assert!(
                value["groups"].as_array().unwrap().is_empty(),
                "{sql}: {payload}"
            );
            assert!(value["omitted"].is_string(), "{sql}: {payload}");
        }
    }

    #[test]
    fn summaries_over_keys_still_leave() {
        let (s, forbidden) = grouped_file();
        let queries = [
            "SELECT merchant, round(avg(amount), 2) AS mean, count(*) AS n, sum(amount) / count(*) AS per FROM t GROUP BY merchant",
            "SELECT merchant, CAST(sum(amount) AS DOUBLE) AS total FROM t GROUP BY 1",
            "SELECT lower(merchant) AS m, count(DISTINCT amount) AS kinds FROM t GROUP BY m",
            "SELECT merchant AS m, -sum(amount) AS owed FROM main.t GROUP BY merchant",
        ];
        for sql in queries {
            let payload = s.aggregate_feedback(sql).unwrap();
            assert_no(sql, &payload, &forbidden);
            let value: Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(
                value["groups"].as_array().unwrap().len(),
                1,
                "{sql}: {payload}"
            );
            assert_eq!(value["dropped"], "0-9", "{sql}: {payload}");
        }
    }

    #[test]
    fn having_and_limit_shape_the_answer_and_a_small_group_still_counts() {
        let mut lines = vec!["merchant,amount".to_string()];
        for (merchant, rows, amount) in [
            ("BIG_A", 10, "1.00"),
            ("BIG_B", 12, "5.00"),
            ("BIG_C", 15, "2.00"),
            ("CANARY_SMALL", 3, "0.50"),
        ] {
            for _ in 0..rows {
                lines.push(format!("{merchant},{amount}"));
            }
        }
        let table = burrmill_ingest::ingest_bytes(
            lines.join("\n").as_bytes(),
            FormatHint::Sniff,
            &Budget::untrusted(),
        )
        .unwrap();
        let s = Session::open(table).unwrap();
        let base = "SELECT merchant, sum(amount) AS total FROM t GROUP BY merchant";
        let cases = [
            (
                "HAVING sum(amount) > 15 ORDER BY merchant",
                vec!["BIG_B", "BIG_C"],
            ),
            (
                "HAVING total > 15 ORDER BY merchant",
                vec!["BIG_B", "BIG_C"],
            ),
            ("ORDER BY total DESC LIMIT 1", vec!["BIG_B"]),
            ("ORDER BY total LIMIT 1", vec![]),
            ("HAVING count(*) < 5", vec!["BIG_A", "BIG_B", "BIG_C"]),
        ];
        for (tail, want) in cases {
            let sql = format!("{base} {tail}");
            let payload = s.aggregate_feedback(&sql).unwrap();
            assert!(!payload.contains("CANARY_SMALL"), "{sql}: {payload}");
            let value: Value = serde_json::from_str(&payload).unwrap();
            let got: Vec<_> = value["groups"]
                .as_array()
                .unwrap()
                .iter()
                .map(|g| g["merchant"].as_str().unwrap())
                .collect();
            assert_eq!(got, want, "{sql}: {payload}");
            assert_eq!(value["dropped"], "0-9", "{sql}: {payload}");
        }
        let probe = format!("{base} HAVING max(amount) > 4");
        let payload = s.aggregate_feedback(&probe).unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["omitted"], NOT_SUMMARY, "{payload}");
    }

    /// Fifty ordinary rows and, unless `member` is `None`, one row for `CANARY_X` of that amount.
    fn with_member(member: Option<&str>) -> Session {
        let mut lines = vec!["merchant,amount".to_string()];
        for n in 0..50 {
            lines.push(format!("SHOP_{:02},5.00", n % 4));
        }
        if let Some(amount) = member {
            lines.push(format!("CANARY_X,{amount}"));
        }
        let table = burrmill_ingest::ingest_bytes(
            lines.join("\n").as_bytes(),
            FormatHint::Sniff,
            &Budget::untrusted(),
        )
        .unwrap();
        Session::open(table).unwrap()
    }

    #[test]
    fn one_member_does_not_change_what_is_sent() {
        let questions = [
            "SELECT (merchant <> 'CANARY_X' OR amount > 2) AS g, count(*) AS n FROM t GROUP BY 1",
            "SELECT (merchant <> 'CANARY_X' OR amount > 2) AS g, count(merchant) AS n, count(DISTINCT merchant) AS m FROM t GROUP BY 1",
            "SELECT (merchant <> 'CANARY_X' OR amount > 2) AS g, CAST(count(*) AS VARCHAR) AS n FROM t GROUP BY 1",
            "SELECT merchant, count(*) AS n FROM t WHERE merchant = 'CANARY_X' AND amount < 2 GROUP BY merchant",
            "SELECT merchant, count(*) AS n FROM t WHERE merchant <> 'CANARY_X' OR amount < 2 GROUP BY merchant",
        ];
        let below = with_member(Some("1.00"));
        let above = with_member(Some("3.00"));
        for sql in questions {
            let a = below.aggregate_feedback(sql).unwrap();
            let b = above.aggregate_feedback(sql).unwrap();
            assert_eq!(a, b, "{sql}");
            assert!(!a.contains("CANARY_X"), "{sql}: {a}");
            let value: Value = serde_json::from_str(&a).unwrap();
            assert!(value["omitted"].is_null(), "{sql}: {a}");
        }
        let card = |s: &Session| s.card(&[], &[], true).unwrap();
        assert_eq!(card(&with_member(None)), card(&below));
        assert!(
            card(&below).contains(r#""rows":"10-99""#),
            "{}",
            card(&below)
        );
        assert!(
            below
                .card(&[], &[], false)
                .unwrap()
                .contains(r#""rows":51"#)
        );
    }

    #[test]
    fn a_count_leaves_only_as_itself_and_never_chooses_a_group() {
        let s = with_member(Some("1.00"));
        for sql in [
            "SELECT merchant, count(*) * 7 AS n FROM t GROUP BY merchant",
            "SELECT merchant, count(*) - 45 AS n FROM t GROUP BY merchant",
            "SELECT merchant, 1 / count(*) AS n FROM t GROUP BY merchant",
            "SELECT merchant, sum(amount) + count(*) AS n FROM t GROUP BY merchant",
            "SELECT merchant, -count(*) AS n FROM t GROUP BY merchant",
        ] {
            let payload = s.aggregate_feedback(sql).unwrap();
            let value: Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(value["omitted"], COUNTED, "{sql}: {payload}");
            assert!(!s.admits("aggregates", sql), "{sql}");
        }
        let all = s
            .aggregate_feedback("SELECT merchant, count(*) AS n FROM t GROUP BY merchant")
            .unwrap();
        for (sql, unapplied) in [
            (
                "SELECT merchant, count(*) AS n FROM t GROUP BY merchant HAVING count(*) >= 13",
                HAVING_COUNT,
            ),
            (
                "SELECT merchant, count(*) AS n FROM t GROUP BY merchant HAVING n < 13",
                HAVING_COUNT,
            ),
            (
                "SELECT merchant, count(*) AS n FROM t GROUP BY merchant ORDER BY n DESC LIMIT 1",
                LIMIT_COUNT,
            ),
            (
                "SELECT merchant, count(*) AS n FROM t GROUP BY merchant ORDER BY 2 LIMIT 1",
                LIMIT_COUNT,
            ),
            (
                "SELECT merchant, count(*) AS n FROM t GROUP BY merchant ORDER BY count(*) LIMIT 2 OFFSET 1",
                LIMIT_COUNT,
            ),
        ] {
            let payload = s.aggregate_feedback(sql).unwrap();
            let mut value: Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(value["not_applied"], json!([unapplied]), "{sql}: {payload}");
            value.as_object_mut().unwrap().remove("not_applied");
            assert_eq!(value.to_string(), all, "{sql}");
        }
        let ordered = s
            .aggregate_feedback(
                "SELECT merchant, count(*) AS n FROM t GROUP BY merchant ORDER BY n DESC",
            )
            .unwrap();
        assert_eq!(ordered, all);
        let mean = s
            .aggregate_feedback(
                "SELECT merchant, round(sum(amount) / count(*), 2) AS mean FROM t GROUP BY merchant",
            )
            .unwrap();
        assert!(mean.contains(r#""mean":"5""#), "{mean}");
        assert_eq!(bucket(0), "0-9");
        assert_eq!(bucket(9), "0-9");
        assert_eq!(bucket(10), "10-99");
        assert_eq!(bucket(999), "100-999");
        assert_eq!(bucket(1000), "1000-9999");
        assert_eq!(bucket(u64::MAX), format!("{}-{}", 10u64.pow(19), u64::MAX));
    }

    #[test]
    fn a_grouped_read_that_fails_says_only_that_it_failed() {
        let (s, forbidden) = grouped_file();
        let mut seen = Vec::new();
        for sql in [
            "SELECT CAST(amount AS BIGINT) * 2305843009213693952 AS g, count(*) AS n FROM t GROUP BY 1",
            "SELECT merchant, count(*) AS n FROM t WHERE CASE WHEN merchant = 'CANARY_SMALL_05' THEN error('overflow') ELSE true END GROUP BY merchant",
            "SELECT CAST(merchant AS INT) AS m, count(*) AS n FROM t GROUP BY 1",
        ] {
            assert!(s.admits("aggregates", sql), "{sql}");
            let payload = s.aggregate_feedback(sql).unwrap();
            assert_no(sql, &payload, &forbidden);
            let value: Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(value["omitted"], FAILED, "{sql}: {payload}");
            assert!(value.get("kind").is_none(), "{sql}: {payload}");
            seen.push(payload);
        }
        assert!(seen.windows(2).all(|w| w[0] == w[1]), "{seen:?}");
        assert!(!s.admits("aggregates", "SELECT merchant FROM t"));
    }

    #[test]
    fn the_engines_own_syntax_is_measured_and_a_refused_form_sends_nothing() {
        let (s, forbidden) = grouped_file();
        let sql = "SELECT merchant, sum(amount) AS total FROM t WHERE merchant ^@ 'KEPT' GROUP BY merchant";
        let payload = s.aggregate_feedback(sql).unwrap();
        assert_no(sql, &payload, &forbidden);
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert!(value["omitted"].is_null(), "{payload}");
        assert!(payload.contains("KEPT_GROUP"), "{payload}");
        let sql = "SELECT merchant, sum(amount) AS total FROM t WHERE merchant GLOB 'KEPT*' GROUP BY merchant";
        let payload = s.aggregate_feedback(sql).unwrap();
        assert_no(sql, &payload, &forbidden);
        assert!(!payload.contains("KEPT_GROUP"), "{payload}");
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert!(value["omitted"].is_string() && value["groups"].as_array().unwrap().is_empty());
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
