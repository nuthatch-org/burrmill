//! DuckDB's dialect on DataFusion (roadmap 6.5).
//!
//! Statements parse with sqlparser's DuckDB dialect, so `HUGEINT`, `UBIGINT` and `//` parse at all,
//! and the AST is then rewritten into what DataFusion plans:
//!
//! - `HUGEINT` → `DECIMAL(38,0)`, the same 128 bits; `UHUGEINT` is refused, having no home.
//! - `BIGNUM` → `DECIMAL(38,0)` too: the 38-digit line, refused past it rather than approximated.
//! - `UBIGINT`, `UINTEGER`, `USMALLINT`, `UTINYINT` → DataFusion's `... UNSIGNED` spellings.
//! - `a // b` → `burrmill_intdiv(a, b)`, exact and truncating. Built on `/` it would become a
//!   float, because `/` is DOUBLE in DuckDB and is made so here too.
//! - `(a, b) > (c, d)` → the lexicographic comparison it means.
//!
//! Semantic differences that type checking must see (`/` as DOUBLE) live in [`DuckSemantics`], an
//! analyzer rule, not here.

use std::ops::ControlFlow;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Builder, Int8Builder, Int16Builder, Int32Builder,
    Int64Builder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{
    DataType, Decimal128Type, Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion_common::{DFSchema, Result as DFResult, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    BinaryExpr, Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TryCast, Volatility,
};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_sql::parser::{DFParser, Statement as DfStatement};
use sqlparser::ast::{
    self as sq, BinaryOperator, DataType as SqlType, ExactNumberInfo, Expr as SqlExpr, VisitorMut,
};
use sqlparser::dialect::DuckDbDialect;

use crate::error::{BurrmillError, Result};

/// Parse every statement. The set-operation bound is applied here and none of the rewrites are.
/// A parse error is [`BurrmillError::Parse`]; the depth check leaves those for the planner.
pub(crate) fn parse_statements(sql: &str) -> Result<Vec<DfStatement>> {
    // Before the parser: sqlparser rejects `[expr FOR x IN list]`, which the views still write.
    let expanded = crate::listcomp::before_parse(sql);
    super::depth::refuse_set_op_tokens(&expanded)?;
    DFParser::parse_sql_with_dialect(expanded.as_str(), &Duck)
        .map(|stmts| stmts.into_iter().collect())
        .map_err(|e| BurrmillError::Parse(super::errors::restate(format!("SQL error: {e:?}"))))
}

/// Parse with DuckDB's dialect and rewrite into what DataFusion plans. The result columns' DuckDB
/// names come back too, taken from the statement as written, before any rewrite.
pub fn parse(sql: &str, known: &Known) -> Result<(DfStatement, Vec<Option<String>>)> {
    let mut stmts = parse_statements(sql)?;
    if stmts.len() > 1 {
        return Err(BurrmillError::NotAllowed(format!(
            "{} statements where one is allowed",
            stmts.len()
        )));
    }
    let Some(mut stmt) = stmts.pop() else {
        return Err(BurrmillError::Parse("empty statement".into()));
    };
    if let DfStatement::Statement(s) = &mut stmt {
        let _ = crate::walk::walk_mut(s.as_mut(), &mut FromFirst);
    }
    let mut names = match &stmt {
        DfStatement::Statement(s) => match s.as_ref() {
            sq::Statement::Query(q) => super::names::default_names(q),
            _ => vec![],
        },
        _ => vec![],
    };
    rewrite(&mut stmt, known, &mut names)?;
    Ok((stmt, names))
}

/// DuckDB's `FROM t SELECT a` and `FROM t`, which DataFusion plans only as `SELECT ... FROM t`.
struct FromFirst;

impl VisitorMut for FromFirst {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut sq::Query) -> ControlFlow<()> {
        if let sq::SetExpr::Select(s) = q.body.as_mut() {
            if s.flavor == sq::SelectFlavor::FromFirstNoSelect && s.projection.is_empty() {
                s.projection = vec![sq::SelectItem::Wildcard(Default::default())];
            }
            s.flavor = sq::SelectFlavor::Standard;
        }
        ControlFlow::Continue(())
    }
}

/// DuckDB's dialect with `IS [NOT] DISTINCT FROM` bound as DuckDB binds it. sqlparser reads its
/// right side as a whole expression, so `a IS DISTINCT FROM b OR c = d` became
/// `a IS DISTINCT FROM (b OR c = d)`; here it stops where `IS` does, as in Postgres.
#[derive(Debug)]
pub(crate) struct Duck;

macro_rules! as_duckdb {
    ($($f:ident),* $(,)?) => {
        $(fn $f(&self) -> bool { DuckDbDialect {}.$f() })*
    };
}

impl sqlparser::dialect::Dialect for Duck {
    fn dialect(&self) -> std::any::TypeId {
        std::any::TypeId::of::<DuckDbDialect>()
    }
    fn is_identifier_start(&self, ch: char) -> bool {
        DuckDbDialect {}.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        DuckDbDialect {}.is_identifier_part(ch)
    }
    as_duckdb!(
        supports_trailing_commas,
        supports_filter_during_aggregation,
        supports_group_by_expr,
        supports_bitwise_shift_operators,
        supports_named_fn_args_with_eq_operator,
        supports_named_fn_args_with_assignment_operator,
        supports_dictionary_syntax,
        support_map_literal_syntax,
        supports_lambda_functions,
        allow_extract_single_quotes,
        supports_explain_with_utility_options,
        supports_load_extension,
        supports_array_typedef_with_brackets,
        supports_from_first_select,
        supports_order_by_all,
        supports_select_wildcard_exclude,
        supports_notnull_operator,
        supports_install,
        supports_detach,
        supports_select_wildcard_replace,
        supports_comma_separated_trim,
    );

    /// `last_value(x IGNORE NULLS)`, as DuckDB spells it; sqlparser's DuckDB dialect only reads it
    /// after the call.
    fn supports_window_function_null_treatment_arg(&self) -> bool {
        true
    }

    /// `PIVOT` and `UNPIVOT` are refused by name rather than as a syntax error.
    fn parse_statement(
        &self,
        parser: &mut sqlparser::parser::Parser,
    ) -> Option<std::result::Result<sq::Statement, sqlparser::parser::ParserError>> {
        use sqlparser::keywords::Keyword;
        match &parser.peek_token().token {
            sqlparser::tokenizer::Token::Word(w)
                if matches!(w.keyword, Keyword::PIVOT | Keyword::UNPIVOT) =>
            {
                let instead = match w.keyword {
                    Keyword::PIVOT => {
                        "one aggregate per value, with FILTER (WHERE ...) or CASE, under GROUP BY"
                    }
                    _ => "one SELECT per column, joined by UNION ALL",
                };
                Some(Err(sqlparser::parser::ParserError::ParserError(format!(
                    "{} is not supported here; write {instead}",
                    w.value.to_ascii_uppercase()
                ))))
            }
            _ => None,
        }
    }

    /// `^@` binds as DuckDB binds it; `GLOB` too, so it can be refused by name.
    fn get_next_precedence(
        &self,
        parser: &sqlparser::parser::Parser,
    ) -> Option<std::result::Result<u8, sqlparser::parser::ParserError>> {
        use sqlparser::dialect::Precedence;
        match &parser.peek_token().token {
            sqlparser::tokenizer::Token::CaretAt => Some(Ok(self.prec_value(Precedence::Like))),
            sqlparser::tokenizer::Token::Word(w) if w.value.eq_ignore_ascii_case("glob") => {
                Some(Ok(self.prec_value(Precedence::Like)))
            }
            _ => None,
        }
    }

    fn parse_infix(
        &self,
        parser: &mut sqlparser::parser::Parser,
        expr: &SqlExpr,
        _precedence: u8,
    ) -> Option<std::result::Result<SqlExpr, sqlparser::parser::ParserError>> {
        use sqlparser::dialect::Precedence;
        use sqlparser::keywords::Keyword::{DISTINCT, FROM, IS, NOT};
        match &parser.peek_token().token {
            // `a ^@ b` is `starts_with(a, b)`.
            sqlparser::tokenizer::Token::CaretAt => {
                parser.next_token();
                return Some(
                    parser
                        .parse_subexpr(self.prec_value(Precedence::Like))
                        .map(|b| call("starts_with", vec![expr.clone(), b])),
                );
            }
            sqlparser::tokenizer::Token::Word(w) if w.value.eq_ignore_ascii_case("glob") => {
                return Some(Err(sqlparser::parser::ParserError::ParserError(
                    "GLOB is not supported here; LIKE is, with % for * and _ for ?".into(),
                )));
            }
            _ => {}
        }
        let not = if parser.parse_keywords(&[IS, DISTINCT, FROM]) {
            false
        } else if parser.parse_keywords(&[IS, NOT, DISTINCT, FROM]) {
            true
        } else {
            return None;
        };
        let (a, b) = match parser.parse_subexpr(self.prec_value(Precedence::Is)) {
            Ok(b) => (Box::new(expr.clone()), Box::new(b)),
            Err(e) => return Some(Err(e)),
        };
        Some(Ok(if not {
            SqlExpr::IsNotDistinctFrom(a, b)
        } else {
            SqlExpr::IsDistinctFrom(a, b)
        }))
    }
}

/// Names a statement may refer to: the nest's tables and columns, and the statement's own aliases.
/// DuckDB resolves identifiers without regard to case, quoted or not; DataFusion, as configured
/// here, resolves them exactly.
///
/// The nest's names are built once and shared; a statement's aliases go in a small overlay, so a
/// query neither rebuilds nor copies thousands of names (half a millisecond of every query did).
#[derive(Clone, Default)]
pub struct Known {
    base: Arc<Names>,
    extra: Names,
}

/// Each name counted by the tables that carry it, so a table can be added or taken away without
/// building the rest again.
#[derive(Default, Clone, PartialEq, Debug)]
struct Names {
    exact: std::collections::HashMap<String, usize>,
    folded: std::collections::HashMap<String, std::collections::BTreeMap<String, usize>>,
    /// Each table's and view's columns in order, by lowercased name, for expanding `*`.
    tables: std::collections::HashMap<String, std::collections::BTreeMap<String, Vec<String>>>,
}

impl Names {
    fn add(&mut self, name: &str) {
        *self.exact.entry(name.to_string()).or_default() += 1;
        *self
            .folded
            .entry(name.to_lowercase())
            .or_default()
            .entry(name.to_string())
            .or_default() += 1;
    }

    fn remove(&mut self, name: &str) {
        if let Some(n) = self.exact.get_mut(name) {
            *n -= 1;
            if *n == 0 {
                self.exact.remove(name);
            }
        }
        let lower = name.to_lowercase();
        if let Some(f) = self.folded.get_mut(&lower) {
            if let Some(n) = f.get_mut(name) {
                *n -= 1;
                if *n == 0 {
                    f.remove(name);
                }
            }
            if f.is_empty() {
                self.folded.remove(&lower);
            }
        }
    }
}

impl Known {
    /// The nest's names, from its tables and views and their columns.
    pub fn of_tables<'a>(tables: impl IntoIterator<Item = (&'a str, Vec<String>)>) -> Self {
        let mut k = Known::default();
        for (name, columns) in tables {
            k.add_table(name, columns);
        }
        k
    }

    /// A table or view and its columns, added to the nest's names.
    pub fn add_table(&mut self, name: &str, columns: Vec<String>) {
        let n = Arc::make_mut(&mut self.base);
        n.add(name);
        for c in &columns {
            n.add(c);
        }
        n.tables
            .entry(name.to_lowercase())
            .or_default()
            .insert(name.to_string(), columns);
    }

    /// What [`Known::add_table`] added for `name` and `columns`, taken away again.
    pub fn remove_table(&mut self, name: &str, columns: &[String]) {
        let n = Arc::make_mut(&mut self.base);
        n.remove(name);
        for c in columns {
            n.remove(c);
        }
        let lower = name.to_lowercase();
        if let Some(t) = n.tables.get_mut(&lower) {
            t.remove(name);
            if t.is_empty() {
                n.tables.remove(&lower);
            }
        }
    }

    pub fn columns(&self, lowercased: &str) -> Option<Vec<String>> {
        self.base.tables.get(lowercased)?.values().next().cloned()
    }

    pub fn add(&mut self, name: &str) {
        self.extra.add(name);
    }

    /// An identifier written in another case than the one name it can mean, as that name.
    fn resolve(&self, written: &str) -> Option<&str> {
        if self.base.exact.contains_key(written) || self.extra.exact.contains_key(written) {
            return None;
        }
        let lower = written.to_lowercase();
        let (a, b) = (self.base.folded.get(&lower), self.extra.folded.get(&lower));
        let mut names = a
            .into_iter()
            .flat_map(|m| m.keys())
            .chain(b.into_iter().flat_map(|m| m.keys()));
        let first = names.next()?;
        if names.all(|n| n == first) {
            Some(first.as_str())
        } else {
            None
        }
    }
}

struct Aliases<'a>(&'a mut Known);

impl sq::Visitor for Aliases<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &sq::Query) -> ControlFlow<()> {
        for cte in q.with.iter().flat_map(|w| &w.cte_tables) {
            self.alias(&cte.alias);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &sq::Select) -> ControlFlow<()> {
        for item in &s.projection {
            if let sq::SelectItem::ExprWithAlias { alias, .. } = item {
                self.0.add(&alias.value);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &sq::TableFactor) -> ControlFlow<()> {
        match t {
            sq::TableFactor::Table { alias: Some(a), .. }
            | sq::TableFactor::Derived { alias: Some(a), .. } => self.alias(a),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl Aliases<'_> {
    fn alias(&mut self, a: &sq::TableAlias) {
        self.0.add(&a.name.value);
        for c in &a.columns {
            self.0.add(&c.name.value);
        }
    }
}

/// Every column name an expression mentions, lowercased and unqualified.
struct Named<'a>(&'a mut std::collections::HashSet<String>);

impl sq::Visitor for Named<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &SqlExpr) -> ControlFlow<()> {
        match e {
            SqlExpr::Identifier(i) => {
                self.0.insert(i.value.to_lowercase());
            }
            SqlExpr::CompoundIdentifier(v) => {
                self.0.extend(v.last().map(|i| i.value.to_lowercase()));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

struct CaseFix<'a>(&'a Known);

impl CaseFix<'_> {
    fn fix(&self, i: &mut sq::Ident) {
        if let Some(name) = self.0.resolve(&i.value) {
            i.value = name.to_string();
        }
    }
}

impl VisitorMut for CaseFix<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &mut SqlExpr) -> ControlFlow<()> {
        match e {
            SqlExpr::Identifier(i) => self.fix(i),
            SqlExpr::CompoundIdentifier(v) => v.iter_mut().for_each(|i| self.fix(i)),
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, r: &mut sq::ObjectName) -> ControlFlow<()> {
        for part in &mut r.0 {
            if let sq::ObjectNamePart::Identifier(i) = part {
                self.fix(i);
            }
        }
        ControlFlow::Continue(())
    }
}

/// Marks a private suffix on a repeated output name; [`strip_dup_suffix`] takes it off the result.
pub const DUP: char = '\u{1}';

/// DuckDB allows two result columns of one name (`SELECT *, value`); DataFusion's projection does
/// not. Repeated top-level items get a private suffix here and lose it again on the result.
fn dedupe_output_names(q: &mut sq::Query, names: &mut [Option<String>], known: &Known) {
    // `ORDER BY s.x` beside `... AS x`: DuckDB sorts by the source column, DataFusion adds it to
    // the projection and then refuses `s.x` beside `x` as ambiguous. The alias is suffixed instead.
    // The same inside an expression: `ORDER BY sum(CAST(x AS HUGEINT))` beside `... AS x` reads the
    // source `x`, as DuckDB and Postgres do, where only a bare `ORDER BY x` means the alias. With no
    // source `x`, as in `ORDER BY -x`, the alias it is.
    let sources = leftmost_select(&q.body).and_then(|s| from_columns(s, q.with.as_ref(), known));
    let mut ordered: std::collections::HashSet<String> = Default::default();
    if let Some(sq::OrderBy {
        kind: sq::OrderByKind::Expressions(items),
        ..
    }) = &q.order_by
    {
        for o in items {
            match &o.expr {
                SqlExpr::Identifier(_) => {}
                SqlExpr::CompoundIdentifier(v) => {
                    ordered.extend(v.last().map(|i| i.value.to_lowercase()));
                }
                e => {
                    let mut named = Default::default();
                    let _ = crate::walk::walk(e, &mut Named(&mut named));
                    ordered.extend(
                        named
                            .into_iter()
                            .filter(|n| sources.as_ref().is_none_or(|s| s.contains(n))),
                    );
                }
            }
        }
    }
    dedupe_set(q.body.as_mut(), names, &ordered, true);
}

fn leftmost_select(b: &sq::SetExpr) -> Option<&sq::Select> {
    match b {
        sq::SetExpr::Select(s) => Some(s),
        sq::SetExpr::SetOperation { left, .. } => leftmost_select(left),
        sq::SetExpr::Query(q) => leftmost_select(&q.body),
        _ => None,
    }
}

/// The columns a select's `FROM` provides, lowercased; `None` where a source's columns cannot be
/// told from the statement and the nest's tables.
fn from_columns(
    s: &sq::Select,
    with: Option<&sq::With>,
    known: &Known,
) -> Option<std::collections::HashSet<String>> {
    fn factor(
        t: &sq::TableFactor,
        with: Option<&sq::With>,
        known: &Known,
        out: &mut std::collections::HashSet<String>,
    ) -> Option<()> {
        let listed = |a: &Option<sq::TableAlias>| {
            a.as_ref().filter(|a| !a.columns.is_empty()).map(|a| {
                a.columns
                    .iter()
                    .map(|c| c.name.value.to_lowercase())
                    .collect::<Vec<_>>()
            })
        };
        match t {
            sq::TableFactor::Table { alias, .. } | sq::TableFactor::Derived { alias, .. }
                if listed(alias).is_some() =>
            {
                out.extend(listed(alias)?);
            }
            sq::TableFactor::Table {
                name, args: None, ..
            } => {
                let sq::ObjectNamePart::Identifier(n) = name.0.last()? else {
                    return None;
                };
                let cte = with
                    .into_iter()
                    .flat_map(|w| &w.cte_tables)
                    .find(|c| c.alias.name.value.eq_ignore_ascii_case(&n.value));
                match cte {
                    Some(c) if !c.alias.columns.is_empty() => {
                        out.extend(c.alias.columns.iter().map(|c| c.name.value.to_lowercase()));
                    }
                    Some(c) => out.extend(output_columns(&c.query, known)?),
                    None => out.extend(
                        known
                            .columns(&n.value.to_lowercase())?
                            .iter()
                            .map(|c| c.to_lowercase()),
                    ),
                }
            }
            sq::TableFactor::Derived { subquery, .. } => {
                out.extend(output_columns(subquery, known)?)
            }
            sq::TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                factor(&table_with_joins.relation, with, known, out)?;
                for j in &table_with_joins.joins {
                    factor(&j.relation, with, known, out)?;
                }
            }
            _ => return None,
        }
        Some(())
    }
    let mut out = std::collections::HashSet::new();
    for t in &s.from {
        factor(&t.relation, with, known, &mut out)?;
        for j in &t.joins {
            factor(&j.relation, with, known, &mut out)?;
        }
    }
    Some(out)
}

/// A query's result columns, lowercased, where the statement tells them.
fn output_columns(q: &sq::Query, known: &Known) -> Option<Vec<String>> {
    let s = leftmost_select(&q.body)?;
    let mut out = Vec::new();
    for item in &s.projection {
        match item {
            sq::SelectItem::ExprWithAlias { alias, .. } => out.push(alias.value.to_lowercase()),
            sq::SelectItem::UnnamedExpr(SqlExpr::Identifier(i)) => out.push(i.value.to_lowercase()),
            sq::SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(v)) => {
                out.push(v.last()?.value.to_lowercase())
            }
            sq::SelectItem::UnnamedExpr(_) => {}
            _ => out.extend(from_columns(s, q.with.as_ref(), known)?),
        }
    }
    Some(out)
}

/// What an unaliased item will be called in the plan: DataFusion names a cast after what it casts,
/// so `CAST(0 AS DECIMAL(38,0))` and a bare `0` beside it are one name there.
fn planned_name(e: &SqlExpr) -> String {
    match e {
        SqlExpr::Cast { expr, .. } | SqlExpr::Nested(expr) => planned_name(expr),
        e => e.to_string(),
    }
}

struct DedupeInner<'a>(&'a Known);

impl VisitorMut for DedupeInner<'_> {
    type Break = ();
    fn pre_visit_query(&mut self, q: &mut sq::Query) -> ControlFlow<()> {
        // Only under a column list: elsewhere a repeat keeps DuckDB's `_1` name, which a query may
        // select by (`subqueries::name`).
        for cte in q.with.iter_mut().flat_map(|w| w.cte_tables.iter_mut()) {
            if !cte.alias.columns.is_empty() {
                dedupe_output_names(&mut cte.query, &mut [], self.0);
            }
        }
        ControlFlow::Continue(())
    }
}

/// The leftmost branch names a set operation's output, so only it keeps `names` and the `ORDER BY`
/// rule; a later branch's repeats are suffixed too, or its projection is refused, and never show.
fn dedupe_set(
    body: &mut sq::SetExpr,
    names: &mut [Option<String>],
    ordered: &std::collections::HashSet<String>,
    leftmost: bool,
) {
    match body {
        sq::SetExpr::Select(sel) if leftmost => dedupe_select(sel, names, ordered),
        sq::SetExpr::Select(sel) => dedupe_select(sel, &mut [], &Default::default()),
        sq::SetExpr::SetOperation { left, right, .. } => {
            dedupe_set(left, names, ordered, leftmost);
            dedupe_set(right, &mut [], &Default::default(), false);
        }
        sq::SetExpr::Query(q) => dedupe_set(q.body.as_mut(), names, ordered, leftmost),
        _ => {}
    }
}

fn dedupe_select(
    sel: &mut sq::Select,
    names: &mut [Option<String>],
    ordered: &std::collections::HashSet<String>,
) {
    let wildcard = sel.projection.iter().any(|i| {
        matches!(
            i,
            sq::SelectItem::Wildcard(_) | sq::SelectItem::QualifiedWildcard(..)
        )
    });
    let mut seen = std::collections::HashSet::new();
    let mut pos = 0usize;
    for (i, item) in sel.projection.iter_mut().enumerate() {
        let collides = match item {
            sq::SelectItem::ExprWithAlias { expr, alias } => {
                ordered.contains(&alias.value.to_lowercase())
                    && !matches!(expr, SqlExpr::CompoundIdentifier(v)
                        if v.last().is_some_and(|l| l.value.eq_ignore_ascii_case(&alias.value)))
            }
            _ => false,
        };
        let (key, display, bare) = match item {
            sq::SelectItem::UnnamedExpr(e) => match e {
                SqlExpr::Identifier(id) => (id.value.to_lowercase(), id.value.clone(), true),
                SqlExpr::CompoundIdentifier(v) => {
                    let last = v.last().map(|x| x.value.clone()).unwrap_or_default();
                    (last.to_lowercase(), last, true)
                }
                e => {
                    let shown = names
                        .get(i)
                        .cloned()
                        .flatten()
                        .unwrap_or_else(|| e.to_string());
                    (planned_name(e), shown, false)
                }
            },
            sq::SelectItem::ExprWithAlias { alias, .. } => {
                (alias.value.to_lowercase(), alias.value.clone(), false)
            }
            _ => continue,
        };
        let repeated = !seen.insert(key) || (wildcard && bare) || collides;
        if repeated {
            let alias = sq::Ident::with_quote('"', format!("{display}{DUP}{pos}"));
            pos += 1;
            *item = match std::mem::replace(item, sq::SelectItem::Wildcard(Default::default())) {
                sq::SelectItem::UnnamedExpr(expr) | sq::SelectItem::ExprWithAlias { expr, .. } => {
                    sq::SelectItem::ExprWithAlias { expr, alias }
                }
                other => other,
            };
            if let Some(n) = names.get_mut(i) {
                *n = None;
            }
        }
    }
}

/// The result's field names without the suffixes [`dedupe_output_names`] added.
pub fn strip_dup_suffix(b: arrow::record_batch::RecordBatch) -> arrow::record_batch::RecordBatch {
    let schema = b.schema();
    if !schema.fields().iter().any(|f| f.name().contains(DUP)) {
        return b;
    }
    let fields: Vec<arrow::datatypes::FieldRef> = schema
        .fields()
        .iter()
        .map(|f| match f.name().split_once(DUP) {
            Some((n, _)) => Arc::new(f.as_ref().clone().with_name(n)),
            None => Arc::clone(f),
        })
        .collect();
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    arrow::record_batch::RecordBatch::try_new(schema, b.columns().to_vec()).expect("same columns")
}

/// The rewrites, on a statement or on the one an `EXPLAIN` wraps.
fn rewrite(stmt: &mut DfStatement, known: &Known, names: &mut [Option<String>]) -> Result<()> {
    let s = match stmt {
        DfStatement::Statement(s) => s,
        DfStatement::Explain(e) => return rewrite(e.statement.as_mut(), known, &mut []),
        _ => return Ok(()),
    };
    // Before any visitor. Aliases is the first walk, and a set operation descends its left
    // spine before a visitor can refuse it.
    super::depth::check_statement(s)?;
    let mut known = known.clone();
    let _ = crate::walk::walk(s.as_ref(), &mut Aliases(&mut known));
    let _ = crate::walk::walk_mut(s.as_mut(), &mut CaseFix(&known));
    if let sq::Statement::Query(q) = s.as_mut() {
        dedupe_output_names(q, names, &known);
        // A CTE whose column list names its output may repeat an expression, as DuckDB allows.
        let _ = crate::walk::walk_mut(q.as_mut(), &mut DedupeInner(&known));
        super::subqueries::name(q, &known);
    }
    let mut ctes = std::collections::HashSet::new();
    let _ = crate::walk::walk(s.as_ref(), &mut CteNames(&mut ctes));
    let mut rw = Rewriter {
        refused: None,
        lambda: vec![],
        known,
        ctes,
    };
    let _ = crate::walk::walk_mut(s.as_mut(), &mut rw);
    match rw.refused {
        Some(why) => Err(BurrmillError::NotAllowed(why)),
        None => Ok(()),
    }
}

struct Rewriter {
    refused: Option<String>,
    /// Parameters of the lambdas the walk is inside.
    lambda: Vec<String>,
    known: Known,
    /// Every CTE name in the statement, lowercased: a table of that name may be the CTE.
    ctes: std::collections::HashSet<String>,
}

struct CteNames<'a>(&'a mut std::collections::HashSet<String>);

impl sq::Visitor for CteNames<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &sq::Query) -> ControlFlow<()> {
        for c in q.with.iter().flat_map(|w| &w.cte_tables) {
            self.0.insert(c.alias.name.value.to_lowercase());
        }
        ControlFlow::Continue(())
    }
}

/// How many columns a query body outputs, where no wildcard leaves it to the catalog.
fn output_width(b: &sq::SetExpr) -> Option<usize> {
    match b {
        sq::SetExpr::Select(s) => {
            let wild = s.projection.iter().any(|i| {
                matches!(
                    i,
                    sq::SelectItem::Wildcard(_) | sq::SelectItem::QualifiedWildcard(..)
                )
            });
            (!wild).then_some(s.projection.len())
        }
        sq::SetExpr::SetOperation { left, .. } => output_width(left),
        sq::SetExpr::Query(q) => output_width(&q.body),
        _ => None,
    }
}

/// DuckDB sorts NULLs last in both directions unless told otherwise; DataFusion, as Postgres,
/// puts them first under `DESC`. An ordering that does not say is told.
fn nulls_last(v: &mut [sq::OrderByExpr]) {
    for o in v {
        o.options.nulls_first.get_or_insert(false);
    }
}

/// DuckDB refuses a string or non-integer number as an ordering key in a query or an aggregate
/// (though not in a window): it would order nothing.
fn literal_key(v: &[sq::OrderByExpr]) -> Option<String> {
    v.iter()
        .any(|o| match &o.expr {
            SqlExpr::Value(v) => match &v.value {
                sq::Value::SingleQuotedString(_) | sq::Value::DoubleQuotedString(_) => true,
                sq::Value::Number(n, _) => n.parse::<i64>().is_err(),
                _ => false,
            },
            _ => false,
        })
        .then(|| "Binder Error: ORDER BY non-integer literal has no effect.".to_string())
}

/// `arg_max(x, y)`: `x` from the row with the greatest `y`, rows with either NULL passed over, as
/// DuckDB does; `first_value(x ORDER BY y DESC) FILTER (WHERE x IS NOT NULL AND y IS NOT NULL)`.
/// Among equal `y` the row is arbitrary in both engines. The top-n and windowed forms are left.
fn arg_extreme(f: &mut sq::Function, max: bool) {
    let sq::FunctionArguments::List(l) = &mut f.args else {
        return;
    };
    let exprs: Vec<SqlExpr> = l
        .args
        .iter()
        .filter_map(|a| match a {
            sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(e)) => Some(e.clone()),
            _ => None,
        })
        .collect();
    let [x, y] = exprs.as_slice() else {
        return;
    };
    if f.over.is_some() || l.args.len() != 2 || l.duplicate_treatment.is_some() {
        return;
    }
    let not_null = |e: &SqlExpr| SqlExpr::IsNotNull(Box::new(SqlExpr::Nested(Box::new(e.clone()))));
    let both = binop(not_null(x), BinaryOperator::And, not_null(y));
    f.filter = Some(Box::new(match f.filter.take() {
        Some(w) => binop(SqlExpr::Nested(w), BinaryOperator::And, both),
        None => both,
    }));
    l.args = vec![sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(
        x.clone(),
    ))];
    l.clauses
        .push(sq::FunctionArgumentClause::OrderBy(vec![sq::OrderByExpr {
            expr: y.clone(),
            options: sq::OrderByOptions {
                asc: Some(!max),
                nulls_first: Some(false),
            },
            with_fill: None,
        }]));
    f.name = sq::ObjectName::from(vec![sq::Ident::new("first_value")]);
}

/// DuckDB's names for functions DataFusion has under another: the date-part shorthands
/// (`year(t)` is `date_part('year', t)`), `regexp_matches`, which is `regexp_like`, and the
/// aggregates `first` and `last`, which are `first_value` and `last_value`, NULLs included.
fn rename_function(f: &mut sq::Function) {
    let [sq::ObjectNamePart::Identifier(name)] = f.name.0.as_slice() else {
        return;
    };
    let lower = name.value.to_ascii_lowercase();
    if matches!(lower.as_str(), "arg_max" | "max_by" | "arg_min" | "min_by") {
        arg_extreme(f, lower.ends_with("max") || lower == "max_by");
        return;
    }
    if lower == "struct_pack" {
        struct_pack(f);
        return;
    }
    // DataFusion's unicode planner binds `substr` itself and rejects a negative length.
    // Ours is looked up by name, so the call has to stop being called substr.
    if matches!(lower.as_str(), "substr" | "substring") {
        f.name = sq::ObjectName::from(vec![sq::Ident::new("burrmill_substr")]);
        return;
    }
    let lit = |s: &str| {
        sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(SqlExpr::Value(
            sq::Value::SingleQuotedString(s.into()).into(),
        )))
    };
    if matches!(lower.as_str(), "list_reverse_sort" | "array_reverse_sort") {
        if let sq::FunctionArguments::List(l) = &mut f.args
            && l.args.len() == 1
        {
            l.args.extend([lit("DESC"), lit("NULLS LAST")]);
            f.name = sq::ObjectName::from(vec![sq::Ident::new("array_sort")]);
        }
        return;
    }
    // DuckDB's `split_part(s, d, 0)` is empty, as a part past the end is; DataFusion refuses 0.
    if lower == "split_part" {
        if let sq::FunctionArguments::List(l) = &mut f.args
            && let [_, _, sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(n))] =
                l.args.as_mut_slice()
        {
            let zero = SqlExpr::Value(sq::Value::Number("0".into(), false).into());
            *n = SqlExpr::Case {
                case_token: sq::helpers::attached_token::AttachedToken::empty(),
                end_token: sq::helpers::attached_token::AttachedToken::empty(),
                operand: None,
                conditions: vec![sq::CaseWhen {
                    condition: binop(n.clone(), BinaryOperator::Eq, zero),
                    result: SqlExpr::Value(sq::Value::Number(i64::MAX.to_string(), false).into()),
                }],
                else_result: Some(Box::new(n.clone())),
            };
        }
        return;
    }
    let part = match lower.as_str() {
        "year" | "month" | "day" | "hour" | "minute" | "second" | "quarter" | "week" | "epoch"
        | "millisecond" | "microsecond" | "isodow" | "decade" | "century" | "millennium" => {
            lower.clone()
        }
        "dayofweek" => "dow".into(),
        "dayofyear" => "doy".into(),
        "dayofmonth" => "day".into(),
        "weekofyear" => "week".into(),
        "regexp_matches" => {
            f.name = sq::ObjectName::from(vec![sq::Ident::new("regexp_like")]);
            return;
        }
        "regexp_full_match" => {
            full_match(f);
            return;
        }
        "first" | "last" => {
            f.name = sq::ObjectName::from(vec![sq::Ident::new(format!("{lower}_value"))]);
            return;
        }
        // DataFusion's `ifnull` is `nvl`, which has no DECIMAL and made every number DOUBLE.
        "ifnull" => {
            f.name = sq::ObjectName::from(vec![sq::Ident::new("coalesce")]);
            return;
        }
        _ => return,
    };
    let sq::FunctionArguments::List(l) = &mut f.args else {
        return;
    };
    if l.args.len() != 1 {
        return;
    }
    l.args.insert(
        0,
        sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(SqlExpr::Value(
            sq::Value::SingleQuotedString(part).into(),
        ))),
    );
    f.name = sq::ObjectName::from(vec![sq::Ident::new("date_part")]);
}

/// `regexp_full_match(s, p[, flags])` is `regexp_like` over the whole string: `^(?:p)$`.
fn full_match(f: &mut sq::Function) {
    let sq::FunctionArguments::List(l) = &mut f.args else {
        return;
    };
    let Some(sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(p))) = l.args.get_mut(1) else {
        return;
    };
    let lit = |s: &str| SqlExpr::Value(sq::Value::SingleQuotedString(s.into()).into());
    let literal = match p {
        SqlExpr::Value(v) => match &v.value {
            sq::Value::SingleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    };
    *p = match literal {
        Some(s) => lit(&format!("^(?:{s})$")),
        None => binop(
            binop(lit("^(?:"), BinaryOperator::StringConcat, p.clone()),
            BinaryOperator::StringConcat,
            lit(")$"),
        ),
    };
    f.name = sq::ObjectName::from(vec![sq::Ident::new("regexp_like")]);
}

/// `struct_pack(k := v, ...)` is DataFusion's `named_struct('k', v, ...)`. An unnamed argument takes
/// its column's name, as in DuckDB.
fn struct_pack(f: &mut sq::Function) {
    let sq::FunctionArguments::List(l) = &mut f.args else {
        return;
    };
    let mut args = Vec::with_capacity(l.args.len() * 2);
    for a in &l.args {
        let (key, v) = match a {
            sq::FunctionArg::Named {
                name,
                arg: sq::FunctionArgExpr::Expr(v),
                operator: sq::FunctionArgOperator::Assignment,
            } => (name.value.clone(), v.clone()),
            sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(v)) => match v {
                SqlExpr::Identifier(i) => (i.value.clone(), v.clone()),
                SqlExpr::CompoundIdentifier(p) => {
                    (p.last().expect("a part").value.clone(), v.clone())
                }
                _ => return,
            },
            _ => return,
        };
        args.push(SqlExpr::Value(sq::Value::SingleQuotedString(key).into()));
        args.push(v);
    }
    *f = match call("named_struct", args) {
        SqlExpr::Function(g) => g,
        _ => unreachable!(),
    };
}

/// A scalar subquery aggregating a correlated top-N,
/// `(SELECT agg(t.x) FROM (SELECT … FROM r c WHERE c.k = <outer> AND … ORDER BY o LIMIT n) t)`,
/// which DataFusion cannot decorrelate through the limit. Ranked within `c.k` instead, and
/// correlated outside the derived table, where DataFusion pulls an equality up over the aggregate:
/// `(SELECT agg(t.x ORDER BY t.rn) FROM (SELECT …, c.k AS k, row_number() OVER (PARTITION BY c.k
/// ORDER BY o) AS rn FROM r c WHERE …) t WHERE t.k = <outer> AND t.rn > m AND t.rn <= m + n)`.
/// With the correlation an equality on an inner column, a rank per key is a rank per outer row.
/// Whether every column `q` names is qualified by `outer` or by a table `q` itself reads.
fn names_only(q: &sq::Query, outer: &str) -> bool {
    struct Tables(Vec<String>);
    impl sq::Visitor for Tables {
        type Break = ();
        fn pre_visit_table_factor(&mut self, t: &sq::TableFactor) -> ControlFlow<()> {
            match t {
                sq::TableFactor::Table { alias: Some(a), .. }
                | sq::TableFactor::Derived { alias: Some(a), .. } => {
                    self.0.push(a.name.value.clone())
                }
                sq::TableFactor::Table {
                    name, alias: None, ..
                } => {
                    if let Some(i) = name.0.last().and_then(|p| p.as_ident()) {
                        self.0.push(i.value.clone())
                    }
                }
                _ => return ControlFlow::Break(()),
            }
            ControlFlow::Continue(())
        }
    }
    let mut tables = Tables(vec![outer.to_string()]);
    if crate::walk::walk(q, &mut tables).is_break() || q.with.is_some() {
        return false;
    }
    let mut known = true;
    let _ = crate::walk::exprs(q, |x| {
        known &= match x {
            SqlExpr::Identifier(_) => false,
            SqlExpr::CompoundIdentifier(p) => p.len() == 2 && tables.0.contains(&p[0].value),
            _ => true,
        };
        ControlFlow::<()>::Continue(())
    });
    known
}

fn top_n_correlated(q: &mut sq::Query) {
    let _ = try_top_n_correlated(q);
}

fn try_top_n_correlated(q: &mut sq::Query) -> Option<()> {
    const KEY: &str = "__burrmill_k";
    const RANK: &str = "__burrmill_rn";
    if q.with.is_some() || q.order_by.is_some() || q.limit_clause.is_some() || q.fetch.is_some() {
        return None;
    }
    let sq::SetExpr::Select(outer) = q.body.as_mut() else {
        return None;
    };
    if outer.selection.is_some()
        || outer.having.is_some()
        || outer.distinct.is_some()
        || !matches!(&outer.group_by, sq::GroupByExpr::Expressions(g, m) if g.is_empty() && m.is_empty())
    {
        return None;
    }
    let [from] = outer.from.as_mut_slice() else {
        return None;
    };
    if !from.joins.is_empty() {
        return None;
    }
    let sq::TableFactor::Derived {
        lateral: false,
        subquery: inner_q,
        alias: Some(t),
        sample: None,
    } = &mut from.relation
    else {
        return None;
    };
    let t = t.name.clone();
    if inner_q.with.is_some() || inner_q.fetch.is_some() {
        return None;
    }
    let Some(sq::OrderBy {
        kind: sq::OrderByKind::Expressions(order),
        interpolate: None,
    }) = inner_q.order_by.clone()
    else {
        return None;
    };
    let Some(sq::LimitClause::LimitOffset {
        limit: Some(limit),
        offset,
        limit_by,
    }) = inner_q.limit_clause.clone()
    else {
        return None;
    };
    let literal = |e: &SqlExpr| match e {
        SqlExpr::Value(v) => match &v.value {
            sq::Value::Number(n, _) => n.parse::<u64>().ok(),
            _ => None,
        },
        _ => None,
    };
    let n = literal(&limit)?;
    let m = match &offset {
        Some(o) => literal(&o.value)?,
        None => 0,
    };
    if !limit_by.is_empty() {
        return None;
    }
    let sq::SetExpr::Select(inner) = inner_q.body.as_mut() else {
        return None;
    };
    if inner.having.is_some()
        || inner.distinct.is_some()
        || !matches!(&inner.group_by, sq::GroupByExpr::Expressions(g, m) if g.is_empty() && m.is_empty())
    {
        return None;
    }
    let [rel] = inner.from.as_slice() else {
        return None;
    };
    let sq::TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = &rel.relation
    else {
        return None;
    };
    if !rel.joins.is_empty() {
        return None;
    }
    let c = match alias {
        Some(a) => a.name.value.clone(),
        None => name.0.last()?.as_ident()?.value.clone(),
    };
    // Every column named must say whose it is; `Some(true)` for the inner relation's alone. A
    // subquery naming only `c` and its own tables, a to-one lookup, counts as the inner relation's.
    let inner_only = |e: &SqlExpr| -> Option<bool> {
        let mut e = e.clone();
        let _ = crate::walk::exprs_mut(&mut e, |x| {
            if let SqlExpr::Subquery(s) = x
                && names_only(s, &c)
            {
                *x = SqlExpr::Value(sq::Value::Null.with_empty_span());
            }
            ControlFlow::<()>::Continue(())
        });
        let mut inside = true;
        let mut unknown = false;
        let _ = crate::walk::exprs(&e, |x| {
            match x {
                SqlExpr::Identifier(_) => unknown = true,
                SqlExpr::CompoundIdentifier(p) if p.len() == 2 => inside &= p[0].value == c,
                SqlExpr::CompoundIdentifier(_)
                | SqlExpr::Subquery(_)
                | SqlExpr::Exists { .. }
                | SqlExpr::InSubquery { .. } => unknown = true,
                _ => {}
            }
            ControlFlow::<()>::Continue(())
        });
        (!unknown).then_some(inside)
    };
    let mut conjuncts = Vec::new();
    if let Some(w) = inner.selection.clone() {
        split_and(w, &mut conjuncts);
    }
    let column = |e: &SqlExpr| matches!(e, SqlExpr::CompoundIdentifier(p) if p.len() == 2 && p[0].value == c);
    let mut key = None;
    let mut plain = Vec::new();
    for x in conjuncts {
        if let SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = &x
            && key.is_none()
        {
            if column(left) && inner_only(right) == Some(false) {
                key = Some(((**left).clone(), (**right).clone()));
                continue;
            }
            if column(right) && inner_only(left) == Some(false) {
                key = Some(((**right).clone(), (**left).clone()));
                continue;
            }
        }
        if inner_only(&x) != Some(true) {
            return None;
        }
        plain.push(x);
    }
    let (inner_key, outer_value) = key?;
    let items_inner = inner.projection.iter().all(|i| match i {
        sq::SelectItem::UnnamedExpr(e) | sq::SelectItem::ExprWithAlias { expr: e, .. } => {
            inner_only(e) == Some(true)
        }
        _ => false,
    });
    if !items_inner || order.iter().any(|o| inner_only(&o.expr) != Some(true)) {
        return None;
    }

    inner.selection = (!plain.is_empty()).then(|| rejoin(plain));
    let mut rank = call("row_number", vec![]);
    if let SqlExpr::Function(f) = &mut rank {
        f.over = Some(sq::WindowType::WindowSpec(sq::WindowSpec {
            window_name: None,
            partition_by: vec![inner_key.clone()],
            order_by: order,
            window_frame: None,
        }));
    }
    inner.projection.push(sq::SelectItem::ExprWithAlias {
        expr: inner_key,
        alias: sq::Ident::new(KEY),
    });
    inner.projection.push(sq::SelectItem::ExprWithAlias {
        expr: rank,
        alias: sq::Ident::new(RANK),
    });
    inner_q.order_by = None;
    inner_q.limit_clause = None;

    let col = |name: &str| SqlExpr::CompoundIdentifier(vec![t.clone(), sq::Ident::new(name)]);
    let num = |v: u64| SqlExpr::Value(sq::Value::Number(v.to_string(), false).into());
    outer.selection = Some(rejoin(vec![
        binop(col(KEY), BinaryOperator::Eq, outer_value),
        binop(col(RANK), BinaryOperator::Gt, num(m)),
        binop(col(RANK), BinaryOperator::LtEq, num(m + n)),
    ]));
    // An aggregate that sees its input's order takes it from the rank now.
    let _ = crate::walk::exprs_mut(&mut outer.projection, |x| {
        if let SqlExpr::Function(f) = x
            && f.over.is_none()
            && let [sq::ObjectNamePart::Identifier(id)] = f.name.0.as_slice()
            && matches!(
                id.value.to_ascii_lowercase().as_str(),
                "list" | "array_agg" | "string_agg" | "group_concat"
            )
            && let sq::FunctionArguments::List(l) = &mut f.args
            && !l
                .clauses
                .iter()
                .any(|c| matches!(c, sq::FunctionArgumentClause::OrderBy(_)))
        {
            l.clauses
                .push(sq::FunctionArgumentClause::OrderBy(vec![sq::OrderByExpr {
                    expr: col(RANK),
                    options: sq::OrderByOptions {
                        asc: None,
                        nulls_first: None,
                    },
                    with_fill: None,
                }]));
        }
        ControlFlow::<()>::Continue(())
    });
    Some(())
}

fn split_and(e: SqlExpr, out: &mut Vec<SqlExpr>) {
    match e {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            split_and(*left, out);
            split_and(*right, out);
        }
        SqlExpr::Nested(x)
            if matches!(
                *x,
                SqlExpr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ) =>
        {
            split_and(*x, out)
        }
        e => out.push(e),
    }
}

fn rejoin(v: Vec<SqlExpr>) -> SqlExpr {
    v.into_iter()
        .reduce(|a, b| binop(a, BinaryOperator::And, b))
        .expect("at least one conjunct")
}

/// `YYYY-MM-DD HH:MM`, with a space or a `T`.
fn minutes_only(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 16
        && matches!(b[10], b' ' | b'T')
        && b[13] == b':'
        && b.iter()
            .enumerate()
            .all(|(i, c)| matches!(i, 4 | 7 | 10 | 13) || c.is_ascii_digit())
        && b[4] == b'-'
        && b[7] == b'-'
}

/// DuckDB functions with no counterpart here, refused by name with what to write instead.
fn unsupported_function(f: &sq::Function) -> Option<String> {
    let [sq::ObjectNamePart::Identifier(name)] = f.name.0.as_slice() else {
        return None;
    };
    let instead = match name.value.to_ascii_lowercase().as_str() {
        "approx_count_distinct" => Some("COUNT(DISTINCT ...) gives the exact count"),
        "list_aggregate" | "array_aggregate" => {
            Some("aggregate in a subquery, as (SELECT sum(v) FROM (SELECT unnest(...) AS v))")
        }
        // `CAST(... AS TIMESTAMP)` is nanoseconds here and microseconds in DuckDB, so not named.
        "strptime" => None,
        "columns" => Some("name the columns, or use * EXCLUDE (...)"),
        _ => return None,
    };
    Some(match instead {
        Some(instead) => format!("{} is not supported here; {instead}", name.value),
        None => format!("{} is not supported here", name.value),
    })
}

/// DuckDB's `if(c, a, b)`, which is `CASE WHEN c THEN a ELSE b END`.
fn if_as_case(f: &sq::Function) -> Option<SqlExpr> {
    let [sq::ObjectNamePart::Identifier(name)] = f.name.0.as_slice() else {
        return None;
    };
    let sq::FunctionArguments::List(l) = &f.args else {
        return None;
    };
    if !name.value.eq_ignore_ascii_case("if") || f.over.is_some() || f.filter.is_some() {
        return None;
    }
    let [c, a, b] = l.args.as_slice() else {
        return None;
    };
    let arg = |x: &sq::FunctionArg| match x {
        sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(e)) => Some(e.clone()),
        _ => None,
    };
    Some(SqlExpr::Case {
        case_token: sq::helpers::attached_token::AttachedToken::empty(),
        end_token: sq::helpers::attached_token::AttachedToken::empty(),
        operand: None,
        conditions: vec![sq::CaseWhen {
            condition: arg(c)?,
            result: arg(a)?,
        }],
        else_result: Some(Box::new(arg(b)?)),
    })
}

/// DuckDB's LIKE has no escape character unless `ESCAPE` names one, which may be any one character;
/// Arrow's always takes `\`. The pattern is respelled for Arrow and the clause dropped.
fn like_pattern(
    pattern: &mut SqlExpr,
    escape: &mut Option<sq::ValueWithSpan>,
) -> std::result::Result<(), String> {
    let esc = match escape.as_ref().map(|v| &v.value) {
        None => None,
        Some(sq::Value::SingleQuotedString(s)) if s.chars().count() <= 1 => s.chars().next(),
        // Refused by the planner as DuckDB refuses it.
        Some(_) => return Ok(()),
    };
    let lit = |s: String| SqlExpr::Value(sq::Value::SingleQuotedString(s).into());
    if let SqlExpr::Value(v) = pattern
        && let sq::Value::SingleQuotedString(p) = &v.value
    {
        let mut out = String::with_capacity(p.len());
        let mut chars = p.chars();
        while let Some(c) = chars.next() {
            if Some(c) == esc {
                let Some(next) = chars.next() else {
                    return Err("Like pattern must not end with escape character!".into());
                };
                out.push('\\');
                out.push(next);
            } else if c == '\\' {
                out.push_str("\\\\");
            } else {
                out.push(c);
            }
        }
        *pattern = lit(out);
    } else {
        match esc {
            None => {
                let p = std::mem::replace(pattern, lit(String::new()));
                *pattern = call("replace", vec![p, lit("\\".into()), lit("\\\\".into())]);
            }
            Some('\\') => return Ok(()),
            Some(c) => {
                return Err(format!(
                    "LIKE ... ESCAPE '{c}' over a pattern that is not a literal is not supported here; ESCAPE '\\' is"
                ));
            }
        }
    }
    *escape = None;
    Ok(())
}

fn call(name: &str, args: Vec<SqlExpr>) -> SqlExpr {
    SqlExpr::Function(sq::Function {
        name: sq::ObjectName::from(vec![sq::Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: sq::FunctionArguments::None,
        args: sq::FunctionArguments::List(sq::FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|a| sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(a)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn binop(l: SqlExpr, op: BinaryOperator, r: SqlExpr) -> SqlExpr {
    SqlExpr::BinaryOp {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

/// `(a1, a2) op (b1, b2)`, lexicographically.
fn tuple_cmp(a: &[SqlExpr], b: &[SqlExpr], op: &BinaryOperator) -> SqlExpr {
    use BinaryOperator::*;
    let strict = if matches!(op, Gt | GtEq) { Gt } else { Lt };
    if a.len() == 1 {
        return binop(a[0].clone(), op.clone(), b[0].clone());
    }
    let head = binop(a[0].clone(), strict, b[0].clone());
    let eq = binop(a[0].clone(), Eq, b[0].clone());
    let tail = tuple_cmp(&a[1..], &b[1..], op);
    binop(head, Or, SqlExpr::Nested(Box::new(binop(eq, And, tail))))
}

/// DuckDB's type names DataFusion does not plan, renamed in place; `Some` is a refusal.
fn retype(t: &mut SqlType) -> Option<String> {
    *t = match t {
        SqlType::HugeInt => SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(38, 0)),
        SqlType::UBigInt => SqlType::BigIntUnsigned(None),
        SqlType::UTinyInt => SqlType::TinyIntUnsigned(None),
        SqlType::USmallInt => SqlType::SmallIntUnsigned(None),
        // `UINTEGER` and DuckDB's other unsigned spellings reach here as custom type names.
        SqlType::Custom(name, args) if args.is_empty() => {
            match name.to_string().to_ascii_uppercase().as_str() {
                "UINTEGER" | "UINT" | "UINT32" => SqlType::IntUnsigned(None),
                "UINT8" => SqlType::TinyIntUnsigned(None),
                "UINT16" => SqlType::SmallIntUnsigned(None),
                "UINT64" => SqlType::BigIntUnsigned(None),
                // Arbitrary precision in DuckDB; the nest's amounts stop at 38 digits, as
                // `HUGEINT` does here, and the checked arithmetic refuses past it.
                "BIGNUM" | "VARINT" => SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(38, 0)),
                _ => return None,
            }
        }
        SqlType::UHugeInt => {
            return Some(
                "UHUGEINT has no exact home here: DECIMAL(38,0) stops short of 2^128".into(),
            );
        }
        _ => return None,
    };
    None
}

impl VisitorMut for Rewriter {
    type Break = ();

    fn post_visit_expr(&mut self, e: &mut SqlExpr) -> ControlFlow<()> {
        if let SqlExpr::Lambda(l) = e {
            self.lambda.truncate(self.lambda.len() - l.params.len());
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, q: &mut sq::Query) -> ControlFlow<()> {
        super::subqueries::distinct_aggregate_names(q);
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, q: &mut sq::Query) -> ControlFlow<()> {
        if let Err(why) = super::subqueries::predicates_as_counts(q, &self.known, &self.ctes) {
            self.refused = Some(why);
            return ControlFlow::Break(());
        }
        for cte in q.with.iter().flat_map(|w| &w.cte_tables) {
            let cols = &cte.alias.columns;
            let mut seen = std::collections::HashSet::new();
            if !cols
                .iter()
                .all(|c| seen.insert(c.name.value.to_lowercase()))
            {
                let list: Vec<&str> = cols.iter().map(|c| c.name.value.as_str()).collect();
                self.refused = Some(format!(
                    "a CTE column list that repeats a name, {}({}), is not supported here",
                    cte.alias.name.value,
                    list.join(", ")
                ));
                return ControlFlow::Break(());
            }
        }
        // DuckDB rounds a fractional `LIMIT` half away from zero.
        if let Some(sq::LimitClause::LimitOffset {
            limit: Some(SqlExpr::Value(v)),
            ..
        }) = q.limit_clause.as_mut()
            && let sq::Value::Number(n, _) = &v.value
            && let Some(rounded) = n
                .contains(['.', 'e', 'E'])
                .then(|| n.parse::<f64>().ok())
                .flatten()
        {
            v.value = sq::Value::Number(format!("{}", rounded.round() as u64), false);
        }
        // DataFusion takes `ORDER BY ALL` only over bare columns; DuckDB orders by every output
        // column left to right, which is `ORDER BY 1, ..., n`.
        let width = output_width(&q.body);
        if let (Some(o), Some(n)) = (q.order_by.as_mut(), width)
            && let sq::OrderByKind::All(options) = &o.kind
        {
            let options = *options;
            o.kind = sq::OrderByKind::Expressions(
                (1..=n)
                    .map(|i| sq::OrderByExpr {
                        expr: SqlExpr::Value(sq::Value::Number(i.to_string(), false).into()),
                        options,
                        with_fill: None,
                    })
                    .collect(),
            );
        }
        match q.order_by.as_mut().map(|o| &mut o.kind) {
            Some(sq::OrderByKind::Expressions(v)) => {
                if let Some(why) = literal_key(v) {
                    self.refused = Some(why);
                    return ControlFlow::Break(());
                }
                nulls_last(v)
            }
            Some(sq::OrderByKind::All(o)) => {
                o.nulls_first.get_or_insert(false);
            }
            None => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &mut SqlExpr) -> ControlFlow<()> {
        if let SqlExpr::Subquery(q) = e {
            top_n_correlated(q);
        }
        if let SqlExpr::Function(f) = e {
            if let Some(why) = unsupported_function(f) {
                self.refused = Some(why);
                return ControlFlow::Break(());
            }
            if let Some(case) = if_as_case(f) {
                *e = case;
            }
        }
        if let SqlExpr::Function(f) = e {
            if let Some(sq::WindowType::WindowSpec(w)) = f.over.as_mut() {
                nulls_last(&mut w.order_by);
            }
            if let sq::FunctionArguments::List(l) = &mut f.args {
                for c in l.clauses.iter_mut() {
                    if let sq::FunctionArgumentClause::OrderBy(v) = c {
                        if let Some(why) = literal_key(v) {
                            self.refused = Some(why);
                            return ControlFlow::Break(());
                        }
                        nulls_last(v);
                    }
                }
            }
            nulls_last(&mut f.within_group);
            rename_function(f);
            // sqlparser's DuckDB dialect reads `f(flag = 'true')` as a named argument; DuckDB, in an
            // expression, reads a comparison.
            if let sq::FunctionArguments::List(l) = &mut f.args {
                for a in l.args.iter_mut() {
                    if let sq::FunctionArg::Named {
                        name,
                        arg: sq::FunctionArgExpr::Expr(v),
                        operator: sq::FunctionArgOperator::Equals,
                    } = a
                    {
                        // `f('x y' = v)` arrives with the string as the argument's name.
                        let left = if name.quote_style == Some('\'') {
                            SqlExpr::Value(sq::Value::SingleQuotedString(name.value.clone()).into())
                        } else {
                            SqlExpr::Identifier(name.clone())
                        };
                        let cmp = binop(left, BinaryOperator::Eq, v.clone());
                        *a = sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(cmp));
                    }
                }
            }
        }
        match e {
            SqlExpr::Lambda(l) => self
                .lambda
                .extend(l.params.iter().map(|p| p.name.value.clone())),
            // DuckDB types a literal by its spelling: `1e3` is DOUBLE, `0.5` DECIMAL(2,1) and `.5`
            // DECIMAL(1,1), digits as written, and past 38 digits DOUBLE. DataFusion drops the
            // leading zero, so the type is written out here, as a cast of the literal's own text
            // (text, so the walk that descends into the cast finds no number to wrap again).
            SqlExpr::Value(v) if matches!(&v.value, sq::Value::Number(n, _) if n.contains(['e', 'E', '.'])) =>
            {
                let sq::Value::Number(n, _) = &v.value else {
                    unreachable!()
                };
                let data_type = match n.split_once('.') {
                    Some((int, frac))
                        if !n.contains(['e', 'E']) && int.len() + frac.len() <= 38 =>
                    {
                        SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(
                            (int.len() + frac.len()) as u64,
                            frac.len() as i64,
                        ))
                    }
                    _ => SqlType::Double(ExactNumberInfo::None),
                };
                let text = SqlExpr::Value(sq::Value::SingleQuotedString(n.clone()).into());
                *e = SqlExpr::Cast {
                    kind: sq::CastKind::Cast,
                    expr: Box::new(text),
                    data_type,
                    array: false,
                    format: None,
                };
            }
            // DataFusion looks lambda parameters up by bare name only; `acc.rate` would be read as
            // column `rate` of a table `acc`. As a field access on `acc`, it reaches the parameter.
            SqlExpr::CompoundIdentifier(parts)
                if parts.len() > 1 && self.lambda.contains(&parts[0].value) =>
            {
                let root = SqlExpr::Identifier(parts[0].clone());
                let access_chain = parts[1..]
                    .iter()
                    .map(|f| {
                        sq::AccessExpr::Dot(SqlExpr::Value(
                            sq::Value::SingleQuotedString(f.value.clone()).into(),
                        ))
                    })
                    .collect();
                *e = SqlExpr::CompoundFieldAccess {
                    root: Box::new(root),
                    access_chain,
                };
            }
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::DuckIntegerDivide,
                right,
            } => {
                *e = call("burrmill_intdiv", vec![*left.clone(), *right.clone()]);
            }
            // DataFusion's SQL planner type-checks NOT before any analyzer rule could cast text;
            // `x = false` is the same three-valued answer and reaches `DuckComparisons`.
            SqlExpr::UnaryOp {
                op: sq::UnaryOperator::Not,
                expr,
            } => {
                let x = *expr.clone();
                *e = binop(
                    x,
                    BinaryOperator::Eq,
                    SqlExpr::Value(sq::Value::Boolean(false).into()),
                );
            }
            // `SUBSTRING(s FROM n FOR m)` is not a function call, so the rename above misses it,
            // and the unicode planner would bind DataFusion's substr.
            SqlExpr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut args = vec![(**expr).clone()];
                match (substring_from, substring_for) {
                    (Some(from), Some(for_)) => {
                        args.push((**from).clone());
                        args.push((**for_).clone());
                    }
                    (Some(from), None) => args.push((**from).clone()),
                    (None, Some(for_)) => {
                        args.push(SqlExpr::Value(sq::Value::Number("1".into(), false).into()));
                        args.push((**for_).clone());
                    }
                    (None, None) => {}
                }
                *e = call("burrmill_substr", args);
            }
            // DuckDB reads a TIMESTAMP literal without its seconds; Arrow does not.
            SqlExpr::TypedString(t) if matches!(t.data_type, SqlType::Timestamp(..)) => {
                if let sq::Value::SingleQuotedString(s) = &mut t.value.value
                    && minutes_only(s)
                {
                    s.push_str(":00");
                }
            }
            SqlExpr::Like {
                any: false,
                pattern,
                escape_char,
                ..
            }
            | SqlExpr::ILike {
                any: false,
                pattern,
                escape_char,
                ..
            } => {
                if let Err(why) = like_pattern(pattern, escape_char) {
                    self.refused = Some(why);
                    return ControlFlow::Break(());
                }
            }
            SqlExpr::Cast { data_type, .. } => {
                let hugeint = matches!(data_type, SqlType::HugeInt);
                if let Some(why) = retype(data_type) {
                    self.refused = Some(why);
                    return ControlFlow::Break(());
                }
                // DuckDB rounds a float to HUGEINT half to even, and to DECIMAL(38,0) half away
                // from zero; both are DECIMAL(38,0) here, so `DuckSemantics` is told which it was.
                if hugeint {
                    *e = call("burrmill_hugeint", vec![e.clone()]);
                }
            }
            // `f(x).field`: DataFusion takes a field name after a dot only as a string.
            SqlExpr::CompoundFieldAccess { access_chain, .. } => {
                for a in access_chain.iter_mut() {
                    if let sq::AccessExpr::Dot(SqlExpr::Identifier(i)) = a {
                        *a = sq::AccessExpr::Dot(SqlExpr::Value(
                            sq::Value::SingleQuotedString(i.value.clone()).into(),
                        ));
                    }
                }
            }
            // DuckDB's JSON operators: `->` is json_extract, `->>` json_extract_string.
            SqlExpr::BinaryOp {
                left,
                op: op @ (BinaryOperator::Arrow | BinaryOperator::LongArrow),
                right,
            } => {
                let f = if matches!(op, BinaryOperator::Arrow) {
                    "json_extract"
                } else {
                    "json_extract_string"
                };
                *e = call(f, vec![*left.clone(), *right.clone()]);
            }
            SqlExpr::BinaryOp { left, op, right } => {
                use BinaryOperator::*;
                if let (SqlExpr::Tuple(a), SqlExpr::Tuple(b)) = (left.as_ref(), right.as_ref())
                    && matches!(op, Gt | Lt | GtEq | LtEq)
                    && a.len() == b.len()
                    && !a.is_empty()
                {
                    *e = tuple_cmp(a, b, op);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// `burrmill_intdiv(a, b)`: DuckDB's `//`, exact and truncating, checked. Integers stay BIGINT;
/// anything decimal is `DECIMAL(38,0)`, as `HUGEINT` is.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IntDiv {
    sig: Signature,
}

impl IntDiv {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for IntDiv {
    fn name(&self) -> &str {
        "burrmill_intdiv"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        let [a, b] = args else {
            return plan_err!("// takes two operands");
        };
        let exact = |t: &DataType| {
            t.is_integer() || matches!(t, DataType::Decimal128(_, 0) | DataType::Null)
        };
        // Beside a DOUBLE or a scaled DECIMAL, DuckDB's `//` is ordinary division, in DOUBLE.
        if (a.is_numeric() || a.is_null())
            && (b.is_numeric() || b.is_null())
            && (!exact(a) || !exact(b))
        {
            return Ok(vec![DataType::Float64, DataType::Float64]);
        }
        if !exact(a) || !exact(b) {
            return plan_err!("// needs numeric operands, not {a} and {b}");
        }
        // As DuckDB types it: unsigned stays unsigned; unsigned beside signed is their union, BIGINT
        // where a signed type twice the unsigned width fits and HUGEINT past it (a literal having
        // been fitted to its partner already); the rest BIGINT.
        let unsigned = |t: &DataType| t.is_unsigned_integer() || t.is_null();
        let mixed = (a.is_unsigned_integer() && b.is_signed_integer())
            || (a.is_signed_integer() && b.is_unsigned_integer());
        let wide = matches!(a, DataType::Decimal128(..))
            || matches!(b, DataType::Decimal128(..))
            || (mixed && matches!(duck_union(a, b), Some(DataType::Decimal128(..))));
        let t = if wide {
            DataType::Decimal128(38, 0)
        } else if unsigned(a) && unsigned(b) && !(a.is_null() && b.is_null()) {
            DataType::UInt64
        } else {
            DataType::Int64
        };
        Ok(vec![t.clone(), t])
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        Ok(args[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (a, b) = (&arrays[0], &arrays[1]);
        let n = a.len();
        let out: ArrayRef = match a.data_type() {
            DataType::Float64 => {
                let (a, b) = (
                    a.as_primitive::<arrow::datatypes::Float64Type>(),
                    b.as_primitive::<arrow::datatypes::Float64Type>(),
                );
                let mut o = arrow::array::Float64Builder::with_capacity(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) || b.value(i) == 0.0 {
                        o.append_null();
                    } else {
                        o.append_value(a.value(i) / b.value(i));
                    }
                }
                std::sync::Arc::new(o.finish())
            }
            DataType::UInt64 => {
                let (a, b) = (
                    a.as_primitive::<UInt64Type>(),
                    b.as_primitive::<UInt64Type>(),
                );
                let mut o = arrow::array::UInt64Builder::with_capacity(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) || b.value(i) == 0 {
                        o.append_null();
                    } else {
                        o.append_value(a.value(i) / b.value(i));
                    }
                }
                std::sync::Arc::new(o.finish())
            }
            DataType::Int64 => {
                let (a, b) = (a.as_primitive::<Int64Type>(), b.as_primitive::<Int64Type>());
                let mut o = Int64Builder::with_capacity(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) || b.value(i) == 0 {
                        o.append_null();
                    } else {
                        match a.value(i).checked_div(b.value(i)) {
                            Some(v) => o.append_value(v),
                            None => {
                                return exec_err!(
                                    "Overflow in division of {} // {}",
                                    a.value(i),
                                    b.value(i)
                                );
                            }
                        }
                    }
                }
                std::sync::Arc::new(o.finish())
            }
            _ => {
                let (a, b) = (
                    a.as_primitive::<Decimal128Type>(),
                    b.as_primitive::<Decimal128Type>(),
                );
                let mut o = Decimal128Builder::with_capacity(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) || b.value(i) == 0 {
                        o.append_null();
                    } else {
                        match a.value(i).checked_div(b.value(i)) {
                            Some(v) => o.append_value(v),
                            None => {
                                return exec_err!(
                                    "Overflow in division of {} // {}",
                                    a.value(i),
                                    b.value(i)
                                );
                            }
                        }
                    }
                }
                std::sync::Arc::new(o.finish().with_precision_and_scale(38, 0)?)
            }
        };
        if scalar {
            Ok(ColumnarValue::Scalar(
                datafusion_common::ScalarValue::try_from_array(&out, 0)?,
            ))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// `a // b` beside a DuckDB DECIMAL, of any scale, is division in DOUBLE: DuckDB's `//` takes
/// integers and HUGEINT only. Both are `DECIMAL(38,0)` here, so which one an operand is comes from
/// what was written, read before coercion adds casts of its own.
fn decimal_intdiv(
    e: Expr,
    schema: &DFSchema,
    inputs: &[&LogicalPlan],
) -> DFResult<Transformed<Expr>> {
    let Expr::ScalarFunction(f) = &e else {
        return Ok(Transformed::no(e));
    };
    if f.func.name() != "burrmill_intdiv" || !f.args.iter().any(|a| duck_decimal(a, schema, inputs))
    {
        return Ok(Transformed::no(e));
    }
    let args = f
        .args
        .iter()
        .map(|a| a.clone().cast_to(&DataType::Float64, schema))
        .collect::<DFResult<Vec<_>>>()?;
    Ok(Transformed::yes(Expr::ScalarFunction(
        datafusion_expr::expr::ScalarFunction::new_udf(Arc::clone(&f.func), args),
    )))
}

/// Whether `e`, a scale-0 DECIMAL here, is a DECIMAL in DuckDB rather than a HUGEINT: a cast not
/// written as HUGEINT, or computed from one. A column is followed to what produced it.
fn duck_decimal(e: &Expr, schema: &DFSchema, inputs: &[&LogicalPlan]) -> bool {
    if !matches!(e.get_type(schema), Ok(DataType::Decimal128(_, 0))) {
        return false;
    }
    let any = |v: Vec<&Expr>| v.into_iter().any(|x| duck_decimal(x, schema, inputs));
    match e {
        Expr::Alias(a) => duck_decimal(&a.expr, schema, inputs),
        Expr::Cast(_) | Expr::TryCast(_) => true,
        Expr::ScalarFunction(f) if f.func.name() == "burrmill_hugeint" => false,
        Expr::ScalarFunction(f) => any(f.args.iter().collect()),
        Expr::AggregateFunction(f) => any(f.params.args.iter().collect()),
        Expr::Negative(x) => duck_decimal(x, schema, inputs),
        Expr::BinaryExpr(b) => any(vec![&b.left, &b.right]),
        Expr::Case(c) => any(c
            .when_then_expr
            .iter()
            .map(|(_, t)| t.as_ref())
            .chain(c.else_expr.as_deref())
            .collect()),
        Expr::Column(c) => inputs.iter().any(|p| {
            p.schema()
                .maybe_index_of_column(c)
                .is_some_and(|i| decimal_output(p, i))
        }),
        Expr::ScalarSubquery(s) => decimal_output(&s.subquery, 0),
        _ => false,
    }
}

/// Whether output column `i` of `p` is a DuckDB DECIMAL, by [`duck_decimal`].
#[recursive::recursive]
fn decimal_output(p: &LogicalPlan, i: usize) -> bool {
    let produced = |e: Option<&Expr>, input: &LogicalPlan| {
        e.is_some_and(|e| duck_decimal(e, input.schema(), &[input]))
    };
    match p {
        LogicalPlan::Projection(x) => produced(x.expr.get(i), &x.input),
        LogicalPlan::Aggregate(x) => {
            if x.group_expr
                .iter()
                .any(|g| matches!(g, Expr::GroupingSet(_)))
            {
                return false;
            }
            let g = x.group_expr.len();
            let e = if i < g {
                x.group_expr.get(i)
            } else {
                x.aggr_expr.get(i - g)
            };
            produced(e, &x.input)
        }
        LogicalPlan::Window(x) => {
            let n = x.input.schema().fields().len();
            if i < n {
                decimal_output(&x.input, i)
            } else {
                produced(x.window_expr.get(i - n), &x.input)
            }
        }
        LogicalPlan::Join(j) => {
            use datafusion_expr::JoinType::*;
            let n = j.left.schema().fields().len();
            match j.join_type {
                RightSemi | RightAnti | RightMark => decimal_output(&j.right, i),
                Inner | Left | Right | Full if i >= n => decimal_output(&j.right, i - n),
                _ => decimal_output(&j.left, i),
            }
        }
        LogicalPlan::Union(u) => u.inputs.iter().any(|x| decimal_output(x, i)),
        LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Distinct(datafusion_expr::Distinct::All(_))
        | LogicalPlan::Repartition(_) => p.inputs().first().is_some_and(|x| decimal_output(x, i)),
        _ => false,
    }
}

/// `xor(a, b)`, bitwise, typed as DuckDB types it: a literal takes its partner's integer type,
/// and mixed signs take the union (`xor` of `BIGINT` and `UBIGINT` is HUGEINT).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Xor {
    sig: Signature,
}

impl Xor {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for Xor {
    fn name(&self) -> &str {
        "xor"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        let [a, b] = args else {
            return plan_err!("xor takes two integers");
        };
        let exact = |t: &DataType| {
            t.is_integer() || matches!(t, DataType::Decimal128(_, 0) | DataType::Null)
        };
        if !exact(a) || !exact(b) {
            return plan_err!("xor takes two integers, not {a} and {b}");
        }
        // Literals are already fitted to a column partner, so equal inputs stay that width.
        let t = if a.is_null() {
            if b.is_null() {
                DataType::Int32
            } else {
                b.clone()
            }
        } else if b.is_null() || a == b {
            a.clone()
        } else if matches!(a, DataType::Decimal128(..)) || matches!(b, DataType::Decimal128(..)) {
            DataType::Decimal128(38, 0)
        } else {
            duck_union(a, b).unwrap_or(DataType::Int64)
        };
        Ok(vec![t.clone(), t])
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        Ok(args[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (a, b) = (&arrays[0], &arrays[1]);
        let n = a.len();
        macro_rules! xor_prim {
            ($ty:ty, $builder:ty) => {{
                let a = a.as_primitive::<$ty>();
                let b = b.as_primitive::<$ty>();
                let mut o = <$builder>::with_capacity(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) {
                        o.append_null();
                    } else {
                        o.append_value(a.value(i) ^ b.value(i));
                    }
                }
                Arc::new(o.finish()) as ArrayRef
            }};
        }
        let out = match a.data_type() {
            DataType::Int8 => xor_prim!(Int8Type, Int8Builder),
            DataType::Int16 => xor_prim!(Int16Type, Int16Builder),
            DataType::Int32 => xor_prim!(Int32Type, Int32Builder),
            DataType::Int64 => xor_prim!(Int64Type, Int64Builder),
            DataType::UInt8 => xor_prim!(UInt8Type, UInt8Builder),
            DataType::UInt16 => xor_prim!(UInt16Type, UInt16Builder),
            DataType::UInt32 => xor_prim!(UInt32Type, UInt32Builder),
            DataType::UInt64 => xor_prim!(UInt64Type, UInt64Builder),
            DataType::Decimal128(_, _) => {
                let (a, b) = (
                    a.as_primitive::<Decimal128Type>(),
                    b.as_primitive::<Decimal128Type>(),
                );
                let mut o = Decimal128Builder::with_capacity(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) {
                        o.append_null();
                    } else {
                        o.append_value(a.value(i) ^ b.value(i));
                    }
                }
                Arc::new(o.finish().with_precision_and_scale(38, 0)?) as ArrayRef
            }
            other => return exec_err!("xor has no form for {other}"),
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// DuckDB semantics that change a result's type, applied once types are known. `/` between
/// numbers is DOUBLE in DuckDB, where DataFusion divides integers as integers.
#[derive(Debug)]
pub struct DuckSemantics {
    round: Arc<ScalarUDF>,
}

impl Default for DuckSemantics {
    fn default() -> Self {
        Self {
            round: RoundInt::udf(),
        }
    }
}

impl AnalyzerRule for DuckSemantics {
    fn name(&self) -> &str {
        "duck_semantics"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform_up_with_subqueries(|p| {
            let mut schema = DFSchema::empty();
            for i in p.inputs() {
                schema.merge(i.schema());
            }
            let names_matter = matches!(
                p,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            );
            let t = p.map_expressions(|e| {
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| {
                    let t = divide_as_double(e, &schema)?;
                    let t = t.transform_data(|e| modulo_by_zero(e, &schema))?;
                    let t = t.transform_data(|e| timestamp_as_duckdb(e, &schema))?;
                    let t = t.transform_data(|e| round_before_int_cast(e, &schema, &self.round))?;
                    let t = t.transform_data(|e| timestamp_as_text(e, &schema))?;
                    let t = t.transform_data(|e| dates_as_duckdb(e, &schema))?;
                    let t = t.transform_data(|e| case_nullability(e, &schema))?;
                    let t = t.transform_data(try_as_duckdb)?;
                    let t = t.transform_data(|e| hugeint_rounding(e, &schema, &self.round))?;
                    t.transform_data(hugeint_null_test)
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

/// DuckDB's date arithmetic types: `date_trunc` of a DATE and `DATE ± INTERVAL` are TIMESTAMP (in
/// microseconds, where DataFusion gives nanoseconds or keeps the DATE and drops the hours).
/// `TRY(x)`: a cast becomes `TRY_CAST`, a function call is evaluated with failures as NULL
/// ([`super::duckfns::TryCall`]), and what cannot fail passes through. Anything else is refused:
/// its errors would pass through a `TRY` that promised NULL.
fn try_as_duckdb(e: Expr) -> DFResult<Transformed<Expr>> {
    let Expr::ScalarFunction(f) = &e else {
        return Ok(Transformed::no(e));
    };
    // A wrapped call takes its function's name, so what was rewritten is not rewritten again.
    if f.func.name() != "try" || f.args.len() != 1 {
        return Ok(Transformed::no(e));
    }
    // A function under another of its names arrives aliased (`unhex` is `from_hex`).
    let mut arg = &f.args[0];
    while let Expr::Alias(a) = arg {
        arg = &a.expr;
    }
    Ok(Transformed::yes(match arg {
        x @ (Expr::TryCast(_) | Expr::Column(_) | Expr::Literal(..)) => x.clone(),
        x @ (Expr::Cast(_) | Expr::ScalarFunction(_)) => try_within(x),
        other => {
            return plan_err!(
                "TRY is supported here around a cast or a function call, not {other}"
            );
        }
    }))
}

/// The calls under a `TRY`, each failing to NULL, as far as that is certainly DuckDB's answer: its
/// `TRY` makes the whole expression NULL, which a NULL argument does only through a function that
/// returns NULL for one. So `TRY(CAST(decode(unhex(x)) AS VARCHAR))` is NULL for hex that is not
/// hex; below a function not known to be strict (`coalesce`, a struct) nothing is touched, and an
/// inner cast stays a cast, failing loudly rather than tainting what is summed downstream.
fn try_within(e: &Expr) -> Expr {
    const STRICT: &[&str] = &[
        "decode",
        "from_hex",
        "hex",
        "substr",
        "substring",
        "lower",
        "upper",
        "length",
    ];
    fn call(e: &Expr) -> Expr {
        match e {
            Expr::Alias(a) => call(&a.expr),
            Expr::Cast(Cast { expr, field }) => {
                Expr::Cast(Cast::new(Box::new(call(expr)), field.data_type().clone()))
            }
            Expr::ScalarFunction(inner) => {
                let strict = STRICT.contains(&inner.func.name());
                Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
                    Arc::new(ScalarUDF::from(super::duckfns::TryCall {
                        inner: Arc::clone(&inner.func),
                    })),
                    if strict {
                        inner.args.iter().map(call).collect()
                    } else {
                        inner.args.clone()
                    },
                ))
            }
            other => other.clone(),
        }
    }
    match e {
        Expr::Cast(Cast { expr, field }) => Expr::TryCast(TryCast::new(
            Box::new(call(expr)),
            field.data_type().clone(),
        )),
        e => call(e),
    }
}

/// DataFusion's logical planner proves some `CASE` values never NULL from which branches are
/// reachable (`CASE WHEN m <> k THEN m ELSE 'x' END`); its physical expression cannot, calls the
/// column nullable, and the physical planner then refuses the plan as a schema mismatch whenever
/// that value is grouped or aggregated again. Declaring it nullable makes both say the same.
fn case_nullability(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let Expr::Case(c) = &e else {
        return Ok(Transformed::no(e));
    };
    let branch_nullable = c
        .when_then_expr
        .iter()
        .map(|(_, t)| t.as_ref())
        .chain(c.else_expr.as_deref())
        .map(|b| b.nullable(schema))
        .collect::<DFResult<Vec<_>>>()?
        .into_iter()
        .any(|n| n)
        || c.else_expr.is_none();
    if e.nullable(schema)? || !branch_nullable {
        return Ok(Transformed::no(e));
    }
    let f = Arc::new(ScalarUDF::from(super::duckfns::Nullable(
        Signature::user_defined(Volatility::Immutable),
    )));
    Ok(Transformed::yes(Expr::ScalarFunction(
        datafusion_expr::expr::ScalarFunction::new_udf(f, vec![e]),
    )))
}

/// A DATE, or one DataFusion's coercion has already cast to a timestamp.
fn is_date(e: &Expr, schema: &DFSchema) -> bool {
    let date = |t: DataType| matches!(t, DataType::Date32 | DataType::Date64);
    match e {
        Expr::Cast(Cast { expr, .. }) => expr.get_type(schema).is_ok_and(date),
        e => e.get_type(schema).is_ok_and(date),
    }
}

fn dates_as_duckdb(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let micros = |e: Expr| {
        Expr::Cast(Cast::new(
            Box::new(e),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ))
    };
    match e {
        Expr::ScalarFunction(ref f)
            if f.func.name() == "date_trunc"
                && matches!(
                    e.get_type(schema)?,
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                )
                && f.args.get(1).is_some_and(|a| is_date(a, schema)) =>
        {
            Ok(Transformed::yes(micros(e)))
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: op @ (Operator::Plus | Operator::Minus),
            right,
        }) => {
            let (lt, rt) = (left.get_type(schema)?, right.get_type(schema)?);
            let date = |t: &DataType| matches!(t, DataType::Date32 | DataType::Date64);
            // `date + 3` arrives as `CAST(CAST(3 * 86400 AS Duration) AS Interval)`, and is a DATE in
            // DuckDB too; only a written interval makes a TIMESTAMP.
            let interval = |t: &DataType, e: &Expr| {
                let mut inner = e;
                while let Expr::Cast(Cast { expr, .. }) = inner {
                    inner = expr;
                }
                matches!(t, DataType::Interval(_))
                    && !inner.get_type(schema).is_ok_and(|t| t.is_integer())
            };
            let (lt, rt) = (lt, rt);
            // Coercion makes those days an interval of nanoseconds, which overflows past 106,751 of
            // them (the year 2262). Days are what a DATE counts, so add them as days.
            let days = |e: &Expr| {
                let mut inner = e;
                while let Expr::Cast(Cast { expr, .. }) = inner {
                    inner = expr;
                }
                match inner {
                    Expr::BinaryExpr(BinaryExpr {
                        left,
                        op: Operator::Multiply,
                        right,
                    }) if matches!(
                        right.as_ref(),
                        Expr::Literal(v, _) if v.to_string() == "86400"
                    ) && left.get_type(schema).is_ok_and(|t| t.is_integer()) =>
                    {
                        Some(left.as_ref().clone())
                    }
                    _ => None,
                }
            };
            let add_days = |date: Expr, op: Operator, n: Expr| {
                let cast = |e: Expr, t: DataType| Expr::Cast(Cast::new(Box::new(e), t));
                let sum = Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(cast(cast(date, DataType::Int32), DataType::Int64)),
                    op,
                    Box::new(cast(n, DataType::Int64)),
                ));
                cast(cast(sum, DataType::Int32), DataType::Date32)
            };
            if lt == DataType::Date32
                && matches!(rt, DataType::Interval(_))
                && let Some(n) = days(&right)
            {
                return Ok(Transformed::yes(add_days(*left, op, n)));
            }
            if rt == DataType::Date32
                && matches!(lt, DataType::Interval(_))
                && op == Operator::Plus
                && let Some(n) = days(&left)
            {
                return Ok(Transformed::yes(add_days(*right, op, n)));
            }
            if date(&lt) && interval(&rt, &right) {
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(micros(*left)),
                    op,
                    right,
                ))))
            } else if interval(&lt, &left) && date(&rt) && op == Operator::Plus {
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    left,
                    op,
                    Box::new(micros(*right)),
                ))))
            } else {
                Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                    left,
                    op,
                    right,
                })))
            }
        }
        e => Ok(Transformed::no(e)),
    }
}

/// A timestamp cast to text in DuckDB's form (`2024-01-01 00:00:00+00`, not arrow's ISO `T`/`Z`).
fn timestamp_as_text(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let (inner, to) = match &e {
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) => {
            (expr, field.data_type())
        }
        _ => return Ok(Transformed::no(e)),
    };
    if !is_text(to) || !matches!(inner.get_type(schema)?, DataType::Timestamp(..)) {
        return Ok(Transformed::no(e));
    }
    let f = Arc::new(ScalarUDF::from(super::duckfns::TimestampText(
        Signature::user_defined(Volatility::Immutable),
    )));
    let text = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
        f,
        vec![inner.as_ref().clone()],
    ));
    Ok(Transformed::yes(if *to == DataType::Utf8 {
        text
    } else {
        Expr::Cast(Cast::new(Box::new(text), to.clone()))
    }))
}

/// `to_timestamp` is TIMESTAMPTZ in DuckDB: microseconds, UTC. DataFusion's is nanoseconds with
/// no zone, which nuthatch's JSON prints differently and `date_trunc` reads differently.
fn timestamp_as_duckdb(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let Expr::ScalarFunction(f) = &e else {
        return Ok(Transformed::no(e));
    };
    if f.func.name() != "to_timestamp"
        || !matches!(
            e.get_type(schema)?,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        )
    {
        return Ok(Transformed::no(e));
    }
    let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Ok(Transformed::yes(Expr::Cast(Cast::new(Box::new(e), utc))))
}

fn numeric(t: &DataType) -> bool {
    t.is_integer()
        || t.is_floating()
        || matches!(t, DataType::Decimal128(..) | DataType::Decimal256(..))
}

/// `x % 0` is NULL in DuckDB, where Arrow's kernel refuses the statement; the divisor becomes
/// `nullif(y, 0)`, which the kernel skips. A non-zero literal divisor is left as written.
fn modulo_by_zero(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Modulo,
        right,
    }) = e
    else {
        return Ok(Transformed::no(e));
    };
    let rt = right.get_type(schema)?;
    let zero = numeric(&rt)
        .then(|| ScalarValue::new_zero(&rt))
        .transpose()?;
    let Some(zero) = zero.filter(|z| !matches!(right.as_ref(), Expr::Literal(v, _) if v != z))
    else {
        return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Modulo,
            right,
        })));
    };
    let divisor = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
        datafusion_functions::core::nullif(),
        vec![*right, Expr::Literal(zero, None)],
    ));
    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        left,
        Operator::Modulo,
        Box::new(divisor),
    ))))
}

fn divide_as_double(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Divide,
        right,
    }) = e
    else {
        return Ok(Transformed::no(e));
    };
    let (lt, rt) = (left.get_type(schema)?, right.get_type(schema)?);
    if !numeric(&lt) || !numeric(&rt) || (lt == DataType::Float64 && rt == DataType::Float64) {
        return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Divide,
            right,
        })));
    }
    let f64 = |x: Box<Expr>, t: &DataType| {
        if *t == DataType::Float64 {
            *x
        } else {
            Expr::Cast(Cast::new(x, DataType::Float64))
        }
    };
    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        Box::new(f64(left, &lt)),
        Operator::Divide,
        Box::new(f64(right, &rt)),
    ))))
}

/// DuckDB's comparisons between text and other types, decided before DataFusion's coercion
/// inserts casts that would make a written `CAST` and an implicit one indistinguishable:
///
/// - ordering (`<`, `>`, `<=`, `>=`, `BETWEEN`) between text and a number is refused, as DuckDB
///   refuses it; equality casts, as DuckDB does, and DataFusion already agrees;
/// - text beside a boolean, or under `AND`/`OR`/`NOT`, is cast to BOOLEAN, as DuckDB casts it;
/// - `//` beside a DuckDB DECIMAL divides in DOUBLE ([`decimal_intdiv`]).
#[derive(Debug, Default)]
pub struct DuckComparisons;

impl AnalyzerRule for DuckComparisons {
    fn name(&self) -> &str {
        "duck_comparisons"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.rewrite_with_subqueries(&mut Comparisons(Vec::new()))
            .map(|t| t.data)
    }
}

/// One pass, bottom-up. A node is re-derived where its own expressions changed or an input's schema
/// did, and tells its parent whether its own schema changed. A union's schema was fixed when the
/// SQL was planned, from the types before this rule, and `recompute_schema` keeps it while the width
/// is unchanged, so a union under a changed input is rebuilt from its inputs instead.
struct Comparisons(Vec<bool>);

impl TreeNodeRewriter for Comparisons {
    type Node = LogicalPlan;

    fn f_down(&mut self, p: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
        self.0.push(false);
        Ok(Transformed::no(p))
    }

    fn f_up(&mut self, p: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
        let below = self.0.pop().unwrap_or(false);
        let before = Arc::clone(p.schema());
        let t = if let Some(n) = set_op_types(&p)? {
            Transformed::yes(n)
        } else if let Some(n) = union_floats(&p)? {
            Transformed::yes(n)
        } else if let LogicalPlan::Union(u) = p {
            if below {
                Transformed::yes(LogicalPlan::Union(
                    datafusion_expr::logical_plan::Union::try_new_with_loose_types(u.inputs)?,
                ))
            } else {
                Transformed::no(LogicalPlan::Union(u))
            }
        } else {
            let mut schema = DFSchema::empty();
            for i in p.inputs() {
                schema.merge(i.schema());
            }
            let names_matter = matches!(
                p,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            );
            let mut intdiv = false;
            p.apply_expressions(|e| {
                e.apply(|x| {
                    intdiv |=
                        matches!(x, Expr::ScalarFunction(f) if f.func.name() == "burrmill_intdiv");
                    Ok(if intdiv {
                        TreeNodeRecursion::Stop
                    } else {
                        TreeNodeRecursion::Continue
                    })
                })
            })?;
            let inputs: Vec<LogicalPlan> = if intdiv {
                p.inputs().into_iter().cloned().collect()
            } else {
                vec![]
            };
            let inputs: Vec<&LogicalPlan> = inputs.iter().collect();
            let t = p.map_expressions(|e| {
                let name = names_matter.then(|| e.schema_name().to_string());
                let t = e.transform_up(|e| {
                    compare_as_duckdb(e, &schema)?
                        .transform_data(|e| decimal_intdiv(e, &schema, &inputs))
                })?;
                match name {
                    Some(name) if t.transformed && t.data.schema_name().to_string() != name => {
                        Ok(Transformed::yes(t.data.alias(name)))
                    }
                    _ => Ok(t),
                }
            })?;
            if t.transformed || below {
                Transformed::yes(t.data.recompute_schema()?)
            } else {
                t
            }
        };
        if t.data.schema() != &before
            && let Some(parent) = self.0.last_mut()
        {
            *parent = true;
        }
        Ok(t)
    }
}

const DUP_ROW: &str = "__burrmill_dup";

/// A `UNION` column where a float meets any other number: DOUBLE in DuckDB, where DataFusion's
/// coercion makes a DECIMAL and casts the floats into it, binary error and all.
fn union_floats(p: &LogicalPlan) -> DFResult<Option<LogicalPlan>> {
    use datafusion_expr::LogicalPlanBuilder;
    let LogicalPlan::Union(u) = p else {
        return Ok(None);
    };
    let width = u.schema.fields().len();
    let doubles: Vec<bool> = (0..width)
        .map(|i| {
            let types: Vec<&DataType> = u
                .inputs
                .iter()
                .map(|x| x.schema().field(i).data_type())
                .collect();
            types.iter().all(|t| numeric(t) || t.is_null())
                && types.iter().any(|t| t.is_floating())
                && types.iter().any(|t| numeric(t) && **t != DataType::Float64)
        })
        .collect();
    if !doubles.contains(&true) {
        return Ok(None);
    }
    let inputs = u
        .inputs
        .iter()
        .map(|x| {
            let s = x.schema();
            let exprs: Vec<Expr> = (0..width)
                .map(|i| {
                    let (q, f) = s.qualified_field(i);
                    let c = Expr::Column(datafusion_common::Column::from((q, f)));
                    if doubles[i] && *f.data_type() != DataType::Float64 {
                        Expr::Cast(Cast::new(Box::new(c), DataType::Float64))
                            .alias_qualified(q.cloned(), f.name())
                    } else {
                        c
                    }
                })
                .collect();
            Ok(Arc::new(
                LogicalPlanBuilder::from(x.as_ref().clone())
                    .project(exprs)?
                    .build()?,
            ))
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(Some(LogicalPlan::Union(
        datafusion_expr::logical_plan::Union::try_new_with_loose_types(inputs)?,
    )))
}

/// DuckDB's type for two integer columns meeting in a set operation, where literals do not adapt:
/// the wider of one signedness, and for mixed signs a signed type twice the unsigned width, which
/// past 64 bits is HUGEINT.
pub(super) fn duck_union(a: &DataType, b: &DataType) -> Option<DataType> {
    use DataType::*;
    let bits = |t: &DataType| match t {
        Int8 | UInt8 => Some(8),
        Int16 | UInt16 => Some(16),
        Int32 | UInt32 => Some(32),
        Int64 | UInt64 => Some(64),
        _ => None,
    };
    let (x, y) = (bits(a)?, bits(b)?);
    let signed = |n: u32| match n {
        8 => Int8,
        16 => Int16,
        32 => Int32,
        64 => Int64,
        _ => Decimal128(38, 0),
    };
    Some(match (a.is_unsigned_integer(), b.is_unsigned_integer()) {
        (true, true) | (false, false) if x >= y => a.clone(),
        (true, true) | (false, false) => b.clone(),
        (true, false) => signed((2 * x).max(y)),
        (false, true) => signed((2 * y).max(x)),
    })
}

/// `INTERSECT` and `EXCEPT` are planned as a semi or anti join on every column, NULLs equal. Two
/// things are put right here. The join keeps the left side's types, where DuckDB gives each column
/// the pair's union type, so both sides are cast to it. And the `ALL` forms (no `Distinct` under
/// the left) were set operations, not bag ones: `EXCEPT ALL` removed every left row with a match
/// rather than one per match. Numbering each row within its duplicates on both sides, and joining
/// on that number too, keeps `min(m, n)` copies for `INTERSECT ALL` and `m - n` for `EXCEPT ALL`.
fn set_op_types(p: &LogicalPlan) -> DFResult<Option<LogicalPlan>> {
    use datafusion_common::NullEquality;
    use datafusion_expr::{ExprFunctionExt, JoinType, LogicalPlanBuilder};
    let LogicalPlan::Join(j) = p else {
        return Ok(None);
    };
    let (ls, rs) = (j.left.schema(), j.right.schema());
    if !matches!(j.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        || j.null_equality != NullEquality::NullEqualsNull
        || j.filter.is_some()
        || j.on.len() != ls.fields().len()
        || rs.fields().len() != ls.fields().len()
    {
        return Ok(None);
    }
    // Already rewritten: its keys carry the duplicate number.
    if ls.fields().iter().any(|f| f.name() == DUP_ROW) {
        return Ok(None);
    }
    let types: Vec<Option<DataType>> = ls
        .fields()
        .iter()
        .zip(rs.fields())
        .map(|(l, r)| {
            let (l, r) = (l.data_type(), r.data_type());
            // A float beside any other number is DOUBLE, as DuckDB has it; DataFusion makes DECIMAL.
            // Beside a DECIMAL (HUGEINT here), DataFusion's union type is DuckDB's too.
            let floats = numeric(l) && numeric(r) && (l.is_floating() || r.is_floating());
            floats
                .then_some(DataType::Float64)
                .or_else(|| duck_union(l, r))
                .or_else(|| {
                    (numeric(l) && numeric(r))
                        .then(|| {
                            datafusion_expr::type_coercion::binary::type_union_resolution(&[
                                l.clone(),
                                r.clone(),
                            ])
                        })
                        .flatten()
                })
                .filter(|t| t != l)
        })
        .collect();
    let bag = !matches!(j.left.as_ref(), LogicalPlan::Distinct(_));
    if types.iter().all(Option::is_none) && !bag {
        return Ok(None);
    }
    let side = |plan: &Arc<LogicalPlan>| -> DFResult<LogicalPlan> {
        let s = plan.schema();
        let exprs: Vec<Expr> = (0..s.fields().len())
            .map(|i| {
                let (q, f) = s.qualified_field(i);
                let c = Expr::Column(datafusion_common::Column::from((q, f)));
                match &types[i] {
                    Some(t) => Expr::Cast(Cast::new(Box::new(c), t.clone()))
                        .alias_qualified(q.cloned(), f.name()),
                    None => c,
                }
            })
            .collect();
        let b = LogicalPlanBuilder::from(plan.as_ref().clone()).project(exprs)?;
        if !bag {
            return b.build();
        }
        let s = b.schema().clone();
        let cols: Vec<Expr> = (0..s.fields().len())
            .map(|i| Expr::Column(datafusion_common::Column::from(s.qualified_field(i))))
            .collect();
        let rn = Expr::from(datafusion_expr::expr::WindowFunction::new(
            datafusion_expr::expr::WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::row_number::row_number_udwf(),
            ),
            vec![],
        ))
        .partition_by(cols)
        .build()?
        .alias(DUP_ROW);
        b.window(vec![rn])?.build()
    };
    let (left, right) = (side(&j.left)?, side(&j.right)?);
    let keys = |s: &DFSchema| -> Vec<datafusion_common::Column> {
        s.fields()
            .iter()
            .map(|f| datafusion_common::Column::from_name(f.name()))
            .collect()
    };
    let (lk, rk) = (keys(left.schema()), keys(right.schema()));
    let joined = LogicalPlanBuilder::from(left).join_detailed(
        right,
        j.join_type,
        (lk, rk),
        None,
        NullEquality::NullEqualsNull,
    )?;
    if !bag {
        return Ok(Some(joined.build()?));
    }
    let s = joined.schema().clone();
    let out: Vec<Expr> = (0..s.fields().len() - 1)
        .map(|i| Expr::Column(datafusion_common::Column::from(s.qualified_field(i))))
        .collect();
    Ok(Some(joined.project(out)?.build()?))
}

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)
}

fn duck_name(t: &DataType, e: &Expr) -> String {
    match (t, e) {
        (t, Expr::Literal(..)) if t.is_integer() => "INTEGER_LITERAL".into(),
        (t, _) => super::errors::duck_type(&t.to_string()),
    }
}

fn as_bool(e: Expr) -> Expr {
    Expr::Cast(Cast::new(Box::new(e), DataType::Boolean))
}

/// DuckDB types an integer literal to fit the integer beside it (`UBIGINT - 1` stays UBIGINT);
/// DataFusion widens such a pair to DECIMAL(20,0), which nuthatch prints as a string.
/// An integer column, literal or cast, or `+ - * %` and negation of those: an expression whose
/// integer type coercion will not change.
fn plainly_integer(e: &Expr, schema: &DFSchema) -> DFResult<bool> {
    Ok(match e {
        Expr::Column(_) | Expr::Literal(..) => e.get_type(schema)?.is_integer(),
        Expr::Cast(c) => c.field.data_type().is_integer(),
        Expr::TryCast(c) => c.field.data_type().is_integer(),
        Expr::Negative(x) => plainly_integer(x, schema)?,
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Modulo,
            right,
        }) => plainly_integer(left, schema)? && plainly_integer(right, schema)?,
        _ => false,
    })
}

/// DuckDB's binder refuses an integer literal beside a DATE or TIMESTAMP in an ordering comparison,
/// `BETWEEN`, `greatest`, `least` and `coalesce`; DataFusion makes the integer a date and answers.
/// (`=`, `<>` and `nullif` DuckDB binds as a cast that fails only on a row, so they are left.)
fn temporal_beside_integer(t: &DataType, v: &ScalarValue) -> DFResult<()> {
    if t.is_temporal() && v.data_type().is_integer() {
        return plan_err!(
            "Cannot compare values of type {t} and type INTEGER_LITERAL - an explicit cast is required"
        );
    }
    Ok(())
}

fn fit_literal(e: Expr, to: &DataType) -> Expr {
    match &e {
        Expr::Literal(v, meta)
            if v.data_type().is_integer() && to.is_integer() && v.data_type() != *to =>
        {
            match v.cast_to(to) {
                Ok(c) if !c.is_null() => Expr::Literal(c, meta.clone()),
                _ => e,
            }
        }
        _ => e,
    }
}

/// A float beside any other number among values that meet in one result: DuckDB makes all of them
/// DOUBLE. DataFusion agrees for integers, but mixed-sign integers become DECIMAL in its coercion,
/// after this has run, and DECIMAL beside a float stays DECIMAL there.
fn floats_win(exprs: &[&Expr], schema: &DFSchema) -> DFResult<bool> {
    let types = exprs
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<DFResult<Vec<_>>>()?;
    Ok(types.iter().any(|t| t.is_floating())
        && types.iter().any(|t| t.is_numeric() && !t.is_floating()))
}

/// DuckDB's type for integer values meeting in one result, where they mix signedness and so
/// DataFusion would pick another (a literal having been fitted already): `None` where it agrees.
fn integer_union(exprs: &[&Expr], schema: &DFSchema) -> DFResult<Option<DataType>> {
    let mut types = Vec::new();
    let mut literal = false;
    for e in exprs {
        if let Expr::Literal(v, _) = e {
            literal |= v.data_type().is_integer();
            continue;
        }
        let t = e.get_type(schema)?;
        if t.is_null() {
            continue;
        }
        // A scale-0 DECIMAL is HUGEINT here: beside an integer, DuckDB's result is HUGEINT.
        if matches!(t, DataType::Decimal128(_, 0)) {
            types.push(t);
            continue;
        }
        if !t.is_integer() {
            return Ok(None);
        }
        types.push(t);
    }
    let huge = types
        .iter()
        .any(|t| matches!(t, DataType::Decimal128(_, 0)));
    if huge {
        return Ok((literal
            || types.iter().any(|t| t.is_integer())
            || types.iter().any(|t| *t != types[0]))
        .then_some(DataType::Decimal128(38, 0)));
    }
    let mixed = types.iter().any(|t| t.is_unsigned_integer())
        && types.iter().any(|t| t.is_signed_integer());
    if !mixed {
        return Ok(None);
    }
    Ok(types
        .iter()
        .skip(1)
        .try_fold(types[0].clone(), |a, b| duck_union(&a, b)))
}

fn cast_to(e: Expr, t: &DataType, schema: &DFSchema) -> DFResult<Expr> {
    let from = e.get_type(schema)?;
    if from == *t {
        return Ok(e);
    }
    let cast = Expr::Cast(Cast::new(Box::new(e), t.clone()));
    // An integer widened to meet a HUGEINT is a HUGEINT in DuckDB; unmarked, `//` would read
    // this cast as a written DECIMAL and divide in DOUBLE (#69).
    Ok(
        if from.is_integer() && matches!(t, DataType::Decimal128(_, 0)) {
            Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
                HugeintMark::udf(),
                vec![cast],
            ))
        } else {
            cast
        },
    )
}

fn as_double(e: Expr, schema: &DFSchema) -> DFResult<Expr> {
    Ok(match e.get_type(schema)? {
        t if t.is_numeric() && t != DataType::Float64 => {
            Expr::Cast(Cast::new(Box::new(e), DataType::Float64))
        }
        _ => e,
    })
}

/// The one integer type among `types` that is not a bare literal, if there is exactly one.
/// DuckDB's element type for `[...]` of integers. A literal takes the column's type when it
/// fits (`[5, block_number]` is UBIGINT); a negative beside an unsigned column does not, and the
/// list is HUGEINT. `None` when there is nothing to correct.
fn list_element_type(args: &[Expr], schema: &DFSchema) -> DFResult<Option<DataType>> {
    let refs: Vec<&Expr> = args.iter().collect();
    if let Some(t) = sole_integer(&refs, schema)? {
        let unfit = args.iter().any(|a| match a {
            Expr::Literal(v, _) if v.data_type().is_integer() => {
                !v.cast_to(&t).is_ok_and(|c| !c.is_null())
            }
            _ => false,
        });
        return Ok(Some(if unfit {
            DataType::Decimal128(38, 0)
        } else {
            t
        }));
    }
    integer_union(&refs, schema)
}

fn sole_integer(exprs: &[&Expr], schema: &DFSchema) -> DFResult<Option<DataType>> {
    let mut found: Option<DataType> = None;
    for e in exprs {
        if matches!(e, Expr::Literal(..)) {
            continue;
        }
        let t = e.get_type(schema)?;
        if !t.is_integer() {
            return Ok(None);
        }
        match &found {
            Some(f) if *f != t => return Ok(None),
            _ => found = Some(t),
        }
    }
    Ok(found)
}

fn compare_as_duckdb(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    // `/` is DOUBLE before coercion too: typed as integer division there, `round(x / 7, 1) - h`
    // beside a HUGEINT was cast to DECIMAL(20,0), and the fraction lost once `/` became DOUBLE.
    divide_as_double(e, schema)?.transform_data(|e| {
        let quiet = std::cell::Cell::new(false);
        let t = compare_inner(e, schema, &quiet)?;
        Ok(if quiet.get() {
            Transformed::yes(t.data)
        } else {
            t
        })
    })
}

/// `quiet` is set where a literal is fitted or a cast added on a path that otherwise reports no
/// change; comparing each node with a copy of itself instead made planning quadratic in depth.
fn compare_inner(
    e: Expr,
    schema: &DFSchema,
    quiet: &std::cell::Cell<bool>,
) -> DFResult<Transformed<Expr>> {
    let fit_literal = |e: Expr, to: &DataType| {
        let was = match &e {
            Expr::Literal(v, _) => Some(v.data_type()),
            _ => None,
        };
        let out = fit_literal(e, to);
        if matches!((&was, &out), (Some(w), Expr::Literal(v, _)) if v.data_type() != *w) {
            quiet.set(true);
        }
        out
    };
    // `avg` of integers: DataFusion's coercion casts the argument to DOUBLE and sums in floats,
    // inexact past 2^53, where DuckDB sums exactly. As DECIMAL(38,0), which holds any 64-bit
    // integer, it reaches the checked rule's exact average instead. Only for an argument that is
    // an integer on its face: before coercion a `CASE` or `round` can report an integer type it
    // will not keep, and the cast would round its fractions away.
    let exact_avg = |args: &mut Vec<Expr>| -> DFResult<bool> {
        match args.as_mut_slice() {
            [a] if plainly_integer(a, schema)? => {
                *a = Expr::Cast(Cast::new(Box::new(a.clone()), DataType::Decimal128(38, 0)));
                Ok(true)
            }
            _ => Ok(false),
        }
    };
    let e = match e {
        Expr::AggregateFunction(mut f) if f.func.name() == "avg" => {
            if exact_avg(&mut f.params.args)? {
                return Ok(Transformed::yes(Expr::AggregateFunction(f)));
            }
            Expr::AggregateFunction(f)
        }
        Expr::WindowFunction(mut w) if matches!(&w.fun, datafusion_expr::expr::WindowFunctionDefinition::AggregateUDF(f) if f.name() == "avg") =>
        {
            if exact_avg(&mut w.params.args)? {
                return Ok(Transformed::yes(Expr::WindowFunction(w)));
            }
            Expr::WindowFunction(w)
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(
                op,
                Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Modulo
                    | Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::Gt
                    | Operator::LtEq
                    | Operator::GtEq
            ) =>
        {
            // Types only where a literal is fitted: asking re-types the whole subtree, and at every
            // node of a long chain that made planning quadratic.
            let (l, r) = match (left.as_ref(), right.as_ref()) {
                (Expr::Literal(v, _), r) if !matches!(r, Expr::Literal(..)) => {
                    let rt = right.get_type(schema)?;
                    if matches!(
                        op,
                        Operator::Lt | Operator::Gt | Operator::LtEq | Operator::GtEq
                    ) {
                        temporal_beside_integer(&rt, v)?;
                    }
                    (fit_literal(*left, &rt), *right)
                }
                (l, Expr::Literal(v, _)) if !matches!(l, Expr::Literal(..)) => {
                    let lt = left.get_type(schema)?;
                    if matches!(
                        op,
                        Operator::Lt | Operator::Gt | Operator::LtEq | Operator::GtEq
                    ) {
                        temporal_beside_integer(&lt, v)?;
                    }
                    (*left, fit_literal(*right, &lt))
                }
                _ => (*left, *right),
            };
            Expr::BinaryExpr(BinaryExpr::new(Box::new(l), op, Box::new(r)))
        }
        // DuckDB fits integer literal bounds to the operand; DataFusion widens all three, which
        // for UBIGINT against a literal is DECIMAL(20,0).
        Expr::Between(mut b)
            if !matches!(*b.expr, Expr::Literal(..))
                && (matches!(*b.low, Expr::Literal(..))
                    || matches!(*b.high, Expr::Literal(..))) =>
        {
            let t = b.expr.get_type(schema)?;
            for bound in [b.low.as_ref(), b.high.as_ref()] {
                if let Expr::Literal(v, _) = bound {
                    temporal_beside_integer(&t, v)?;
                }
            }
            b.low = Box::new(fit_literal(*b.low, &t));
            b.high = Box::new(fit_literal(*b.high, &t));
            Expr::Between(b)
        }
        Expr::Case(mut c) => {
            let mut branches: Vec<&Expr> =
                c.when_then_expr.iter().map(|(_, t)| t.as_ref()).collect();
            if let Some(e) = &c.else_expr {
                branches.push(e);
            }
            // A DOUBLE among the branches makes the result DOUBLE, as DuckDB has it; DataFusion
            // would make DECIMAL beside DECIMAL-typed branches.
            if floats_win(&branches, schema)? {
                for (_, then) in c.when_then_expr.iter_mut() {
                    **then = as_double(then.as_ref().clone(), schema)?;
                }
                if let Some(e) = c.else_expr.as_mut() {
                    **e = as_double(e.as_ref().clone(), schema)?;
                }
                return Ok(Transformed::yes(Expr::Case(c)));
            }
            if let Some(t) = integer_union(&branches, schema)? {
                for (_, then) in c.when_then_expr.iter_mut() {
                    **then = cast_to(then.as_ref().clone(), &t, schema)?;
                }
                if let Some(e) = c.else_expr.as_mut() {
                    **e = cast_to(e.as_ref().clone(), &t, schema)?;
                }
                return Ok(Transformed::yes(Expr::Case(c)));
            }
            if let Some(t) = sole_integer(&branches, schema)? {
                for (_, then) in c.when_then_expr.iter_mut() {
                    **then = fit_literal(then.as_ref().clone(), &t);
                }
                if let Some(e) = c.else_expr.as_mut() {
                    **e = fit_literal(e.as_ref().clone(), &t);
                }
            }
            Expr::Case(c)
        }
        // The list stays as DataFusion typed it (a lambda was already bound to that), and the
        // indexed value is cast to DuckDB's element type. Rewriting the list itself disagrees
        // with the lambda.
        Expr::ScalarFunction(f)
            if f.func.name() == "array_element"
                && f.args.first().is_some_and(
                    |a| matches!(a, Expr::ScalarFunction(m) if m.func.name() == "make_array"),
                ) =>
        {
            let args = match &f.args[0] {
                Expr::ScalarFunction(m) => m.args.clone(),
                _ => unreachable!(),
            };
            match list_element_type(&args, schema)? {
                Some(t) => {
                    let expr = Expr::ScalarFunction(f);
                    if expr.get_type(schema).ok().as_ref() == Some(&t) {
                        expr
                    } else {
                        quiet.set(true);
                        Expr::Cast(Cast::new(Box::new(expr), t))
                    }
                }
                None => Expr::ScalarFunction(f),
            }
        }
        Expr::ScalarFunction(mut f)
            if matches!(
                f.func.name(),
                "coalesce" | "greatest" | "least" | "nullif" | "burrmill_intdiv" | "xor"
            ) =>
        {
            if matches!(f.func.name(), "greatest" | "least" | "coalesce")
                && f.args
                    .iter()
                    .any(|a| matches!(a, Expr::Literal(v, _) if v.data_type().is_integer()))
            {
                for a in &f.args {
                    if !matches!(a, Expr::Literal(..)) {
                        let t = a.get_type(schema)?;
                        for v in f.args.iter().filter_map(|x| {
                            if let Expr::Literal(v, _) = x {
                                Some(v)
                            } else {
                                None
                            }
                        }) {
                            temporal_beside_integer(&t, v)?;
                        }
                    }
                }
            }
            if !matches!(f.func.name(), "burrmill_intdiv" | "xor")
                && floats_win(&f.args.iter().collect::<Vec<_>>(), schema)?
            {
                f.args = f
                    .args
                    .into_iter()
                    .map(|a| as_double(a, schema))
                    .collect::<DFResult<_>>()?;
                return Ok(Transformed::yes(Expr::ScalarFunction(f)));
            }
            if f.func.name() != "burrmill_intdiv"
                && let Some(t) = integer_union(&f.args.iter().collect::<Vec<_>>(), schema)?
            {
                f.args = f
                    .args
                    .into_iter()
                    .map(|a| cast_to(a, &t, schema))
                    .collect::<DFResult<_>>()?;
                return Ok(Transformed::yes(Expr::ScalarFunction(f)));
            }
            let args: Vec<&Expr> = f.args.iter().collect();
            if let Some(t) = sole_integer(&args, schema)? {
                f.args = f.args.into_iter().map(|a| fit_literal(a, &t)).collect();
            }
            Expr::ScalarFunction(f)
        }
        e => e,
    };
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::Gt
                    | Operator::LtEq
                    | Operator::GtEq
                    | Operator::And
                    | Operator::Or
            ) =>
        {
            let (lt, rt) = (left.get_type(schema)?, right.get_type(schema)?);
            // A float beside a DECIMAL compares as DOUBLE in DuckDB; DataFusion goes to DECIMAL.
            let decimal =
                |t: &DataType| matches!(t, DataType::Decimal128(..) | DataType::Decimal256(..));
            let comparison = matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::Gt
                    | Operator::LtEq
                    | Operator::GtEq
            );
            if comparison && (lt.is_floating() && decimal(&rt) || decimal(&lt) && rt.is_floating())
            {
                let l = if decimal(&lt) {
                    Expr::Cast(Cast::new(left, DataType::Float64))
                } else {
                    *left
                };
                let r = if decimal(&rt) {
                    Expr::Cast(Cast::new(right, DataType::Float64))
                } else {
                    *right
                };
                return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(l),
                    op,
                    Box::new(r),
                ))));
            }
            let ordering = matches!(
                op,
                Operator::Lt | Operator::Gt | Operator::LtEq | Operator::GtEq
            );
            // A string literal beside a number is cast to its type while binding, as DuckDB's is.
            let literal = |e: &Expr| match e {
                Expr::Literal(
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::Utf8View(Some(s))
                    | ScalarValue::LargeUtf8(Some(s)),
                    _,
                ) => Some(s.clone()),
                _ => None,
            };
            let bound = |s: &str, t: &DataType| -> DFResult<Box<Expr>> {
                match super::fastcast::literal_as(s, t) {
                    Some(v) => Ok(Box::new(Expr::Literal(v, None))),
                    None => plan_err!(
                        "Conversion Error: Could not convert string '{s}' to {}",
                        super::errors::duck_type(&t.to_string())
                    ),
                }
            };
            if ordering
                && numeric(&lt)
                && let Some(s) = literal(&right)
            {
                return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    left,
                    op,
                    bound(&s, &lt)?,
                ))));
            }
            if ordering
                && numeric(&rt)
                && let Some(s) = literal(&left)
            {
                return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    bound(&s, &rt)?,
                    op,
                    right,
                ))));
            }
            if ordering && (is_text(&lt) && numeric(&rt) || numeric(&lt) && is_text(&rt)) {
                return plan_err!(
                    "Binder Error: Cannot compare values of type {} and type {} - an explicit cast \
                     is required",
                    duck_name(&lt, &left),
                    duck_name(&rt, &right)
                );
            }
            let boolean = matches!(op, Operator::And | Operator::Or)
                || matches!(op, Operator::Eq | Operator::NotEq)
                    && (lt == DataType::Boolean || rt == DataType::Boolean);
            if boolean && (is_text(&lt) || is_text(&rt)) {
                let l = if is_text(&lt) { as_bool(*left) } else { *left };
                let r = if is_text(&rt) {
                    as_bool(*right)
                } else {
                    *right
                };
                return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(l),
                    op,
                    Box::new(r),
                ))));
            }
            Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                left,
                op,
                right,
            })))
        }
        Expr::Between(b) => {
            let (t, lo, hi) = (
                b.expr.get_type(schema)?,
                b.low.get_type(schema)?,
                b.high.get_type(schema)?,
            );
            if is_text(&t) && (numeric(&lo) || numeric(&hi))
                || numeric(&t) && (is_text(&lo) || is_text(&hi))
            {
                let other = if numeric(&lo) || is_text(&lo) && numeric(&t) {
                    (&lo, &b.low)
                } else {
                    (&hi, &b.high)
                };
                let (a, bn) = if is_text(&t) {
                    ("VARCHAR".to_string(), duck_name(other.0, other.1))
                } else {
                    (duck_name(&t, &b.expr), "VARCHAR".to_string())
                };
                return plan_err!(
                    "Binder Error: Cannot mix values of type {a} and {bn} in BETWEEN clause - an \
                     explicit cast is required"
                );
            }
            Ok(Transformed::no(Expr::Between(b)))
        }
        Expr::InList(mut l) => {
            let t = l.expr.get_type(schema)?;
            let types = l
                .list
                .iter()
                .map(|x| x.get_type(schema))
                .collect::<DFResult<Vec<_>>>()?;
            if t == DataType::Boolean {
                if let Some(n) = types.iter().find(|x| numeric(x)) {
                    l.expr = Box::new(Expr::Cast(Cast::new(l.expr, n.clone())));
                    return Ok(Transformed::yes(Expr::InList(l)));
                }
            } else if numeric(&t) && types.contains(&DataType::Boolean) {
                l.list = l
                    .list
                    .into_iter()
                    .zip(types)
                    .map(|(x, xt)| {
                        if xt == DataType::Boolean {
                            Expr::Cast(Cast::new(Box::new(x), t.clone()))
                        } else {
                            x
                        }
                    })
                    .collect();
                return Ok(Transformed::yes(Expr::InList(l)));
            }
            Ok(Transformed::no(Expr::InList(l)))
        }
        Expr::Not(x) if is_text(&x.get_type(schema)?) => {
            Ok(Transformed::yes(Expr::Not(Box::new(as_bool(*x)))))
        }
        e => Ok(Transformed::no(e)),
    }
}

/// A boolean compared with a number is read as the number, `true` as 1, so `2 = true` is false
/// and `2 > false` true. Done while the SQL is planned, because DataFusion types a `SELECT` list
/// there and refuses `Int64 = Boolean` before any analyzer rule could cast it.
#[derive(Debug)]
pub struct DuckPlanner;

impl datafusion_expr::planner::ExprPlanner for DuckPlanner {
    fn plan_binary_op(
        &self,
        mut e: datafusion_expr::planner::RawBinaryExpr,
        schema: &DFSchema,
    ) -> DFResult<datafusion_expr::planner::PlannerResult<datafusion_expr::planner::RawBinaryExpr>>
    {
        use datafusion_expr::planner::PlannerResult;
        use sqlparser::ast::BinaryOperator as B;
        if !matches!(e.op, B::Eq | B::NotEq | B::Lt | B::Gt | B::LtEq | B::GtEq) {
            return Ok(PlannerResult::Original(e));
        }
        let (Ok(lt), Ok(rt)) = (e.left.get_type(schema), e.right.get_type(schema)) else {
            return Ok(PlannerResult::Original(e));
        };
        // Through BIGINT: arrow casts a boolean to integers and floats but not to DECIMAL.
        let as_number = |b: Expr, t: DataType| {
            let i = Expr::Cast(Cast::new(Box::new(b), DataType::Int64));
            if t == DataType::Int64 {
                i
            } else {
                Expr::Cast(Cast::new(Box::new(i), t))
            }
        };
        if lt == DataType::Boolean && numeric(&rt) {
            e.left = as_number(e.left, rt);
        } else if rt == DataType::Boolean && numeric(&lt) {
            e.right = as_number(e.right, lt);
        }
        Ok(PlannerResult::Original(e))
    }
}

/// `burrmill_round_int(x)`: `x` rounded to a whole number as DuckDB rounds it when casting to an
/// integer: half to even for floats, half away from zero for decimals. Arrow's cast truncates.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RoundInt {
    sig: Signature,
}

impl RoundInt {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for RoundInt {
    fn name(&self) -> &str {
        "burrmill_round_int"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [DataType::Float32] | [DataType::Float16] => Ok(vec![DataType::Float64]),
            [t @ (DataType::Float64 | DataType::Decimal128(..))] => Ok(vec![t.clone()]),
            _ => plan_err!("burrmill_round_int takes a float or a decimal"),
        }
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        Ok(args[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow::array::{Decimal128Array, Float64Array};
        use arrow::datatypes::Float64Type;
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let out: ArrayRef = match a.data_type() {
            DataType::Float64 => Arc::new(
                a.as_primitive::<Float64Type>()
                    .iter()
                    .map(|v| v.map(f64::round_ties_even))
                    .collect::<Float64Array>(),
            ),
            DataType::Decimal128(p, s) => {
                let unit = 10i128.pow(*s as u32);
                let mut out = Vec::with_capacity(a.len());
                for v in a.as_primitive::<Decimal128Type>().iter() {
                    out.push(match v {
                        None => None,
                        Some(v) => {
                            let (q, r) = (v / unit, v % unit);
                            let q = if r.abs() * 2 >= unit {
                                q + r.signum()
                            } else {
                                q
                            };
                            match q.checked_mul(unit) {
                                Some(x) => Some(x),
                                None => {
                                    return Err(super::errors::overflow(format!(
                                        "Overflow rounding {v} to a whole number"
                                    )));
                                }
                            }
                        }
                    });
                }
                Arc::new(Decimal128Array::from(out).with_precision_and_scale(*p, *s)?)
            }
            t => return exec_err!("burrmill_round_int: unsupported {t}"),
        };
        if scalar {
            Ok(ColumnarValue::Scalar(
                datafusion_common::ScalarValue::try_from_array(&out, 0)?,
            ))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// A cast from a float or a scaled decimal to an integer rounds first, as DuckDB's does.
fn round_before_int_cast(
    e: Expr,
    schema: &DFSchema,
    round: &Arc<ScalarUDF>,
) -> DFResult<Transformed<Expr>> {
    let (inner, to, try_) = match &e {
        Expr::Cast(Cast { expr, field }) => (expr, field.data_type().clone(), false),
        Expr::TryCast(TryCast { expr, field }) => (expr, field.data_type().clone(), true),
        _ => return Ok(Transformed::no(e)),
    };
    if !to.is_integer() {
        return Ok(Transformed::no(e));
    }
    let from = inner.get_type(schema)?;
    let needs = from.is_floating() || matches!(from, DataType::Decimal128(_, s) if s > 0);
    if !needs
        || matches!(inner.as_ref(), Expr::ScalarFunction(f) if f.func.name() == "burrmill_round_int")
    {
        return Ok(Transformed::no(e));
    }
    let rounded = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
        Arc::clone(round),
        vec![inner.as_ref().clone()],
    ));
    Ok(Transformed::yes(if try_ {
        Expr::TryCast(TryCast::new(Box::new(rounded), to))
    } else {
        Expr::Cast(Cast::new(Box::new(rounded), to))
    }))
}

/// `burrmill_hugeint(CAST(x AS DECIMAL(38,0)))`, the marker the parse leaves on a HUGEINT cast:
/// from a float the value is rounded half to even first, as DuckDB's `nearbyint` does, and either
/// way the marker goes.
fn hugeint_rounding(
    e: Expr,
    schema: &DFSchema,
    round: &Arc<ScalarUDF>,
) -> DFResult<Transformed<Expr>> {
    let Expr::ScalarFunction(f) = &e else {
        return Ok(Transformed::no(e));
    };
    if f.func.name() != "burrmill_hugeint" {
        return Ok(Transformed::no(e));
    }
    let [arg] = f.args.as_slice() else {
        return plan_err!("burrmill_hugeint takes one argument");
    };
    let rounded = |inner: &Expr| {
        Box::new(Expr::ScalarFunction(
            datafusion_expr::expr::ScalarFunction::new_udf(Arc::clone(round), vec![inner.clone()]),
        ))
    };
    Ok(Transformed::yes(match arg {
        Expr::Cast(Cast { expr, field }) if expr.get_type(schema)?.is_floating() => {
            Expr::Cast(Cast::new(rounded(expr), field.data_type().clone()))
        }
        Expr::TryCast(TryCast { expr, field }) => {
            let source = if expr.get_type(schema)?.is_floating() {
                *rounded(expr)
            } else if is_text(&expr.get_type(schema)?) {
                expr.as_ref().clone()
            } else {
                return Ok(Transformed::yes(arg.clone()));
            };
            let marked = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
                super::fastcast::HugeintSource::udf(false),
                vec![source],
            ));
            Expr::TryCast(TryCast::new(Box::new(marked), field.data_type().clone()))
        }
        other => other.clone(),
    }))
}

/// `TRY_CAST(x AS HUGEINT) IS [NOT] NULL` over text or a float, answered for DuckDB's whole HUGEINT rather than
/// refused where the value itself could not be held.
fn hugeint_null_test(e: Expr) -> DFResult<Transformed<Expr>> {
    let (inner, null) = match &e {
        Expr::IsNull(x) => (x, true),
        Expr::IsNotNull(x) => (x, false),
        _ => return Ok(Transformed::no(e)),
    };
    let Expr::TryCast(TryCast { expr, .. }) = inner.as_ref() else {
        return Ok(Transformed::no(e));
    };
    let Expr::ScalarFunction(f) = expr.as_ref() else {
        return Ok(Transformed::no(e));
    };
    if f.func.name() != super::fastcast::HUGEINT_SOURCE {
        return Ok(Transformed::no(e));
    }
    let fits = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
        super::fastcast::HugeintSource::udf(true),
        f.args.clone(),
    ));
    Ok(Transformed::yes(if null { !fits } else { fits }))
}

/// Marks a cast written as HUGEINT until [`hugeint_rounding`] takes it off; returns its argument.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HugeintMark {
    sig: Signature,
}

impl HugeintMark {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for HugeintMark {
    fn name(&self) -> &str {
        "burrmill_hugeint"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        Ok(args.to_vec())
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        Ok(args[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(args.args[0].clone())
    }
}

/// DuckDB's `from_hex(text)`: the bytes a hex string spells, an odd length padded with a leading
/// zero (`abc` is `0a bc`), a bad digit refused, as measured against DuckDB.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FromHex {
    sig: Signature,
}

impl FromHex {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for FromHex {
    fn name(&self) -> &str {
        "from_hex"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null] => {
                Ok(vec![DataType::Utf8])
            }
            _ => plan_err!("from_hex takes one text argument"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow::array::BinaryBuilder;
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let a = a.as_string::<i32>();
        let mut out = BinaryBuilder::with_capacity(a.len(), 0);
        let digit = |c: u8| -> DFResult<u8> {
            (c as char).to_digit(16).map(|d| d as u8).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Invalid Input Error: Invalid input for hex digit: {}",
                    c as char
                ))
            })
        };
        let mut buf = Vec::new();
        for i in 0..a.len() {
            if a.is_null(i) {
                out.append_null();
                continue;
            }
            let s = a.value(i).as_bytes();
            buf.clear();
            let rest = if s.len() % 2 == 1 {
                buf.push(digit(s[0])?);
                &s[1..]
            } else {
                s
            };
            for pair in rest.chunks(2) {
                buf.push((digit(pair[0])? << 4) | digit(pair[1])?);
            }
            out.append_value(&buf);
        }
        let out: ArrayRef = Arc::new(out.finish());
        Ok(if scalar {
            ColumnarValue::Scalar(datafusion_common::ScalarValue::try_from_array(&out, 0)?)
        } else {
            ColumnarValue::Array(out)
        })
    }
}

/// `decode`: DuckDB's one-argument form, bytes to text, refused unless valid UTF-8. The
/// two-argument form stays DataFusion's own `decode(x, encoding)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Decode {
    sig: Signature,
    inner: Arc<ScalarUDF>,
}

impl Decode {
    pub fn udf(inner: Arc<ScalarUDF>) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            inner,
        }))
    }
}

impl ScalarUDFImpl for Decode {
    fn name(&self) -> &str {
        "decode"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [DataType::Binary | DataType::BinaryView | DataType::LargeBinary | DataType::Null] => {
                Ok(vec![DataType::Binary])
            }
            [_] => plan_err!("decode takes a BLOB"),
            _ => self.inner.coerce_types(args),
        }
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        match args {
            [_] => Ok(DataType::Utf8),
            _ => self.inner.return_type(args),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return self.inner.invoke_with_args(args);
        }
        use arrow::array::StringBuilder;
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let a = a.as_binary::<i32>();
        let mut out = StringBuilder::new();
        for i in 0..a.len() {
            if a.is_null(i) {
                out.append_null();
                continue;
            }
            match std::str::from_utf8(a.value(i)) {
                Ok(s) => out.append_value(s),
                Err(_) => {
                    return exec_err!(
                        "Conversion Error: Failure in decode: could not convert blob to UTF8 string, the blob contained invalid UTF8 characters."
                    );
                }
            }
        }
        let out: ArrayRef = Arc::new(out.finish());
        Ok(if scalar {
            ColumnarValue::Scalar(datafusion_common::ScalarValue::try_from_array(&out, 0)?)
        } else {
            ColumnarValue::Array(out)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Known;

    fn cols(c: &[&str]) -> Vec<String> {
        c.iter().map(|s| s.to_string()).collect()
    }

    /// Tables added, replaced and removed one at a time leave the names a rebuild would give,
    /// including two tables whose names differ only in case.
    #[test]
    fn names_kept_as_tables_change_are_the_names_rebuilt() {
        let mut k = Known::default();
        k.add_table("Bets", cols(&["Id", "amount"]));
        k.add_table("bets", cols(&["id", "Total"]));
        k.add_table("users", cols(&["Id"]));
        k.remove_table("users", &cols(&["Id"]));
        k.add_table("users", cols(&["address"]));
        k.remove_table("bets", &cols(&["id", "Total"]));
        let rebuilt = Known::of_tables([
            ("Bets", cols(&["Id", "amount"])),
            ("users", cols(&["address"])),
        ]);
        assert_eq!(*k.base, *rebuilt.base);
        assert_eq!(k.columns("bets"), Some(cols(&["Id", "amount"])));
    }
}
