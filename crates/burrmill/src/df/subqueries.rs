//! DuckDB's column names for derived tables and CTEs.
//!
//! Inside a subquery DuckDB renames a repeated column, `a, a` to `a, a_1`, compared without regard
//! to case, and names an unaliased expression by its text, `(x + 1)`; the outer query sees and may
//! use those names. DataFusion refuses the repeat, or renames it `a:1`, or, for an unaliased
//! subquery, passes both through under one name, where the result encoder keeps one of the two:
//! `SELECT * FROM (SELECT * FROM t a JOIN t b ON ...)` lost half its columns without a word.
//!
//! Each derived table and CTE is walked bottom-up for its output names, wildcards expanded from the
//! tables' columns or from an inner subquery already named, and its select list rewritten where a
//! name changes. Where the names cannot be known for certain (`USING`, semi joins, table functions,
//! wildcard options, an expression with no printed name) the query is left as it was.

use sqlparser::ast::{
    self as sq, Expr, Ident, JoinConstraint, JoinOperator, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, TableFactor, TableWithJoins,
};

use super::dialect::Known;

/// A relation in a FROM clause: the name it is referred to by, and its columns if known.
struct Relation {
    qualifier: Option<String>,
    columns: Option<Vec<String>>,
}

type Ctes = Vec<(String, Option<Vec<String>>)>;

/// Names every derived table and CTE under `q` as DuckDB does; `q`'s own output is left alone.
pub fn name(q: &mut Query, known: &Known) {
    let mut ctes = Vec::new();
    query(q, known, &mut ctes, false);
}

fn query(q: &mut Query, known: &Known, ctes: &mut Ctes, rename: bool) -> Option<Vec<String>> {
    let depth = ctes.len();
    if let Some(with) = q.with.as_mut() {
        for cte in with.cte_tables.iter_mut() {
            let names = if with.recursive {
                // The body refers to itself before its names exist.
                None
            } else {
                query(&mut cte.query, known, ctes, true)
            };
            let names = with_column_aliases(names, &cte.alias.columns);
            ctes.push((cte.alias.name.value.to_lowercase(), names));
        }
    }
    let out = set_expr(&mut q.body, known, ctes, rename);
    ctes.truncate(depth);
    out
}

fn set_expr(b: &mut SetExpr, known: &Known, ctes: &mut Ctes, rename: bool) -> Option<Vec<String>> {
    match b {
        SetExpr::Select(s) => select(s, known, ctes, rename),
        SetExpr::Query(q) => query(q, known, ctes, rename),
        // The first branch names a set operation's columns. The others are renamed too where they
        // repeat a name, which DataFusion refuses in any projection; by position, it never shows.
        SetExpr::SetOperation { left, right, .. } => {
            let names = set_expr(left, known, ctes, rename);
            set_expr(right, known, ctes, rename);
            names
        }
        _ => None,
    }
}

fn with_column_aliases(
    names: Option<Vec<String>>,
    aliases: &[sq::TableAliasColumnDef],
) -> Option<Vec<String>> {
    if aliases.is_empty() {
        return names;
    }
    let mut names = names?;
    if aliases.len() > names.len() {
        return None;
    }
    for (n, a) in names.iter_mut().zip(aliases) {
        *n = a.name.value.clone();
    }
    Some(dedupe(&names))
}

/// DuckDB's rule: a name already taken, compared without case, gets `_1`, `_2`, ... until free.
fn dedupe(names: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut count = std::collections::HashMap::<String, usize>::new();
    names
        .iter()
        .map(|n| {
            let mut name = n.clone();
            let c = count.entry(n.to_lowercase()).or_insert(0);
            while seen.contains(&name.to_lowercase()) {
                *c += 1;
                name = format!("{n}_{c}");
            }
            seen.insert(name.to_lowercase());
            name
        })
        .collect()
}

fn factor(t: &mut TableFactor, known: &Known, ctes: &mut Ctes, out: &mut Vec<Relation>) {
    match t {
        TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } => {
            let table = name.0.last().map(|p| p.to_string()).unwrap_or_default();
            let lower = table.trim_matches('"').to_lowercase();
            let columns = if name.0.len() == 1 {
                match ctes.iter().rev().find(|(n, _)| *n == lower) {
                    Some((_, c)) => c.clone(),
                    None => known.columns(&lower),
                }
            } else {
                known.columns(&lower)
            };
            let (qualifier, columns) = match alias {
                Some(a) => (
                    a.name.value.clone(),
                    with_column_aliases(columns, &a.columns),
                ),
                None => (table.trim_matches('"').to_string(), columns),
            };
            out.push(Relation {
                qualifier: Some(qualifier),
                columns,
            });
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let names = query(subquery, known, ctes, true);
            match alias {
                Some(a) => out.push(Relation {
                    qualifier: Some(a.name.value.clone()),
                    columns: with_column_aliases(names, &a.columns),
                }),
                None => out.push(Relation {
                    qualifier: None,
                    columns: names,
                }),
            }
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias: None,
        } => {
            from(
                std::slice::from_mut(table_with_joins.as_mut()),
                known,
                ctes,
                out,
            );
        }
        _ => out.push(Relation {
            qualifier: None,
            columns: None,
        }),
    }
}

/// The relations a FROM clause brings into scope, in the order `*` lists them. A join whose columns
/// `*` does not simply concatenate is a relation of unknown columns.
fn from(tables: &mut [TableWithJoins], known: &Known, ctes: &mut Ctes, out: &mut Vec<Relation>) {
    for t in tables {
        factor(&mut t.relation, known, ctes, out);
        for j in t.joins.iter_mut() {
            let plain = match &j.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c)
                | JoinOperator::CrossJoin(c) => {
                    matches!(c, JoinConstraint::On(_) | JoinConstraint::None)
                }
                _ => false,
            };
            factor(&mut j.relation, known, ctes, out);
            if !plain {
                out.push(Relation {
                    qualifier: None,
                    columns: None,
                });
            }
        }
    }
}

fn plain_wildcard(o: &sq::WildcardAdditionalOptions) -> bool {
    o.opt_ilike.is_none()
        && o.opt_exclude.is_none()
        && o.opt_except.is_none()
        && o.opt_replace.is_none()
        && o.opt_rename.is_none()
        && o.opt_alias.is_none()
}

/// What an item contributes: `(name, qualifier to spell it with)` per column, `None` if unknown.
fn item_names(item: &SelectItem, relations: &[Relation]) -> Option<Vec<(String, Option<String>)>> {
    match item {
        SelectItem::UnnamedExpr(Expr::Identifier(i)) => Some(vec![(i.value.clone(), None)]),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(v)) => {
            Some(vec![(v.last()?.value.clone(), None)])
        }
        SelectItem::UnnamedExpr(e) => Some(vec![(super::names::printed(e)?, None)]),
        SelectItem::ExprWithAlias { alias, .. } => Some(vec![(alias.value.clone(), None)]),
        SelectItem::Wildcard(o) if plain_wildcard(o) => {
            let mut out = Vec::new();
            for r in relations {
                for c in r.columns.as_ref()? {
                    out.push((c.clone(), Some(r.qualifier.clone()?)));
                }
            }
            Some(out)
        }
        SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(n), o)
            if plain_wildcard(o) =>
        {
            let q = n.0.last()?.to_string();
            let q = q.trim_matches('"');
            let r = relations.iter().find(|r| {
                r.qualifier
                    .as_deref()
                    .is_some_and(|x| x.eq_ignore_ascii_case(q))
            })?;
            let qualifier = r.qualifier.clone()?;
            Some(
                r.columns
                    .as_ref()?
                    .iter()
                    .map(|c| (c.clone(), Some(qualifier.clone())))
                    .collect(),
            )
        }
        _ => None,
    }
}

fn select(s: &mut Select, known: &Known, ctes: &mut Ctes, rename: bool) -> Option<Vec<String>> {
    let mut relations = Vec::new();
    from(&mut s.from, known, ctes, &mut relations);
    let items: Vec<Vec<(String, Option<String>)>> = s
        .projection
        .iter()
        .map(|i| item_names(i, &relations))
        .collect::<Option<_>>()?;
    let written: Vec<String> = items.iter().flatten().map(|(n, _)| n.clone()).collect();
    let finals = dedupe(&written);
    if !rename {
        return Some(finals);
    }
    let unnamed = |i: &SelectItem| matches!(i, SelectItem::UnnamedExpr(e) if !matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)));
    if finals == written && !s.projection.iter().any(unnamed) {
        return Some(finals);
    }
    let mut next = finals.iter();
    let mut projection = Vec::with_capacity(finals.len());
    for (item, cols) in s.projection.drain(..).zip(items) {
        let renamed: Vec<&String> = cols
            .iter()
            .map(|_| next.next().expect("one name per column"))
            .collect();
        let changed = cols.iter().zip(&renamed).any(|((w, _), f)| w != *f);
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) if changed => {
                for ((col, qualifier), f) in cols.into_iter().zip(renamed) {
                    let qualifier = qualifier.expect("a wildcard's columns are qualified");
                    projection.push(SelectItem::ExprWithAlias {
                        expr: Expr::CompoundIdentifier(vec![
                            Ident::with_quote('"', qualifier),
                            Ident::with_quote('"', col),
                        ]),
                        alias: Ident::with_quote('"', f.clone()),
                    });
                }
            }
            SelectItem::UnnamedExpr(e)
                if changed || unnamed(&SelectItem::UnnamedExpr(e.clone())) =>
            {
                projection.push(SelectItem::ExprWithAlias {
                    expr: e,
                    alias: Ident::with_quote('"', renamed[0].clone()),
                });
            }
            SelectItem::ExprWithAlias { expr, .. } if changed => {
                projection.push(SelectItem::ExprWithAlias {
                    expr,
                    alias: Ident::with_quote('"', renamed[0].clone()),
                });
            }
            item => projection.push(item),
        }
    }
    s.projection = projection;
    Some(finals)
}

/// `EXISTS` and `IN (subquery)` in a select list, as counts. DataFusion decorrelates them only as
/// `WHERE` predicates and refused them here, where DuckDB answers; it does decorrelate a scalar
/// aggregate anywhere, counting nothing as 0. `IN` keeps its three values, as DuckDB gives them:
/// false against no rows, NULL for a NULL operand, true on a match, NULL where only a NULL could
/// have matched, false otherwise. Only a plain subquery is rewritten (one SELECT, no grouping,
/// `DISTINCT`, `LIMIT` or aggregate); anything else is left for DataFusion to refuse.
pub fn predicates_as_counts(
    q: &mut Query,
    known: &Known,
    ctes: &std::collections::HashSet<String>,
) -> Result<(), String> {
    each_select(q.body.as_mut(), &mut |s| {
        hoist_from_aggregates(s);
        let outer = Outer::of(s, known, ctes);
        // Inside an aggregate in HAVING a predicate is evaluated per row, as in the select list,
        // where DataFusion's membership test cannot be planned: counts, as there.
        if let Some(h) = s.having.as_mut() {
            at_this_level(h, &mut |x| {
                let Expr::Function(f) = x else { return };
                if f.over.is_some()
                    || !AGGREGATES.contains(&f.name.to_string().to_ascii_lowercase().as_str())
                {
                    return;
                }
                let mut inner = Expr::Function(f.clone());
                at_this_level(&mut inner, &mut |y| {
                    if let Some(n) = as_counts(y, &outer) {
                        *y = n;
                    }
                });
                *x = inner;
            });
        }
        for clause in [s.selection.as_mut(), s.having.as_mut()]
            .into_iter()
            .flatten()
        {
            filter_predicates(clause, true)?;
        }
        for item in s.projection.iter_mut() {
            let e = match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                _ => continue,
            };
            at_this_level(e, &mut |x| {
                if let Some(n) = as_counts(x, &outer) {
                    *x = n;
                }
            });
        }
        Ok(())
    })
}

/// The SELECTs of a query body, each branch of a set operation included; a parenthesised query is
/// a query of its own and visited as one.
fn each_select(
    b: &mut SetExpr,
    f: &mut dyn FnMut(&mut Select) -> Result<(), String>,
) -> Result<(), String> {
    match b {
        SetExpr::Select(s) => f(s),
        SetExpr::SetOperation { left, right, .. } => {
            each_select(left, f)?;
            each_select(right, f)
        }
        _ => Ok(()),
    }
}

/// In a `WHERE` or `HAVING`, an `IN (subquery)` that is a condition of its own is planned by
/// DataFusion as a (null-aware) semi or anti join, correctly. Inside `OR`, `NOT` or `CASE` it
/// goes through a mark join that loses its NULL: `x = 3 OR x NOT IN (1, NULL)` kept `x = 2`,
/// where the answer is NULL. There the NULL cases are supplied ([`with_nulls`]), or, for a
/// subquery that is not a plain SELECT, the statement is refused rather than answered wrongly.
fn filter_predicates(e: &mut Expr, top: bool) -> Result<(), String> {
    match e {
        Expr::BinaryOp {
            left,
            op: sq::BinaryOperator::And,
            right,
        } if top => {
            filter_predicates(left, true)?;
            filter_predicates(right, true)
        }
        Expr::Nested(inner) if top => filter_predicates(inner, true),
        Expr::InSubquery { .. } | Expr::Exists { .. } if top => Ok(()),
        _ => {
            let mut refused = false;
            at_this_level(e, &mut |x| {
                if matches!(x, Expr::InSubquery { .. }) {
                    match with_nulls(x) {
                        Some(n) => *x = n,
                        None => refused = true,
                    }
                }
            });
            if refused {
                return Err("IN (subquery) inside OR, NOT or CASE is refused here unless the subquery is a \
                            plain SELECT: planned directly it would lose SQL's NULL"
                    .into());
            }
            Ok(())
        }
    }
}

/// The relations a SELECT reads, by the names their columns are qualified with, and their
/// columns where the catalog knows them.
struct Outer(Vec<(Ident, Option<Vec<String>>)>);

impl Outer {
    fn of(s: &Select, known: &Known, ctes: &std::collections::HashSet<String>) -> Self {
        let mut out = Vec::new();
        for t in &s.from {
            let plain = t.joins.iter().all(|j| match &j.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c)
                | JoinOperator::CrossJoin(c) => {
                    matches!(c, JoinConstraint::On(_) | JoinConstraint::None)
                }
                _ => false,
            });
            if !plain {
                return Outer(Vec::new());
            }
            for f in std::iter::once(&t.relation).chain(t.joins.iter().map(|j| &j.relation)) {
                let (name, columns) = match f {
                    TableFactor::Table {
                        name,
                        alias,
                        args: None,
                        ..
                    } => {
                        let Some(sq::ObjectNamePart::Identifier(table)) = name.0.last() else {
                            return Outer(Vec::new());
                        };
                        let lower = table.value.to_lowercase();
                        let columns = match alias {
                            Some(a) if !a.columns.is_empty() => None,
                            _ if name.0.len() == 1 && ctes.contains(&lower) => None,
                            _ => known.columns(&lower),
                        };
                        (
                            alias
                                .as_ref()
                                .map_or_else(|| table.clone(), |a| a.name.clone()),
                            columns,
                        )
                    }
                    TableFactor::Derived { alias: Some(a), .. } => (a.name.clone(), None),
                    _ => return Outer(Vec::new()),
                };
                out.push((name, columns));
            }
        }
        Outer(out)
    }

    /// The qualifier of a bare column: the one relation read, or the one of several whose known
    /// columns include it.
    fn qualifier(&self, column: &Ident) -> Option<Ident> {
        if let [(q, _)] = self.0.as_slice() {
            return Some(q.clone());
        }
        let mut having = self.0.iter().map(|(q, c)| {
            c.as_ref()
                .map(|c| (q, c.iter().any(|n| n.eq_ignore_ascii_case(&column.value))))
        });
        let mut found = None;
        for r in &mut having {
            match r? {
                (q, true) if found.is_none() => found = Some(q.clone()),
                (_, true) => return None,
                _ => {}
            }
        }
        found
    }
}

/// `x` with each bare column qualified by the outer relation it names, where that is certain;
/// one that is not stays bare, and [`movable`] then declines to move it.
fn qualified(x: &Expr, outer: &Outer) -> Expr {
    let mut x = x.clone();
    at_this_level(&mut x, &mut |e| {
        if let Expr::Identifier(i) = e
            && let Some(q) = outer.qualifier(i)
        {
            *e = Expr::CompoundIdentifier(vec![q, i.clone()]);
        }
    });
    x
}

/// Calls `f` on every expression of `e` that is not inside a nested query: a predicate in a nested
/// query's `WHERE` is that query's, and DataFusion plans it there.
fn at_this_level(e: &mut Expr, f: &mut dyn FnMut(&mut Expr)) {
    struct Level<'a> {
        depth: usize,
        f: &'a mut dyn FnMut(&mut Expr),
    }
    impl sq::VisitorMut for Level<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, _q: &mut Query) -> std::ops::ControlFlow<()> {
            self.depth += 1;
            std::ops::ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _q: &mut Query) -> std::ops::ControlFlow<()> {
            self.depth -= 1;
            std::ops::ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, e: &mut Expr) -> std::ops::ControlFlow<()> {
            if self.depth == 0 {
                (self.f)(e);
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let _ = crate::walk::walk_mut(e, &mut Level { depth: 0, f });
}

/// `x` can move into the subquery's `WHERE` without changing what it names: every column in it
/// qualified, by a name the subquery does not bind itself. `x IN (SELECT id FROM t)` with a bare
/// outer `id` would otherwise compare the inner `id` with itself.
fn movable(x: &Expr, sub: &Select) -> bool {
    let mut bound = std::collections::HashSet::new();
    let _ = crate::walk::relations(&sub.from, |r| {
        if let Some(sq::ObjectNamePart::Identifier(i)) = r.0.last() {
            bound.insert(i.value.to_lowercase());
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    for t in &sub.from {
        for f in std::iter::once(&t.relation).chain(t.joins.iter().map(|j| &j.relation)) {
            if let TableFactor::Table { alias: Some(a), .. }
            | TableFactor::Derived { alias: Some(a), .. } = f
            {
                bound.insert(a.name.value.to_lowercase());
            }
        }
    }
    let mut ok = true;
    let _ = crate::walk::exprs(x, |e| {
        match e {
            Expr::Identifier(_)
            | Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. } => ok = false,
            Expr::CompoundIdentifier(v)
                if v.len() < 2 || bound.contains(&v[v.len() - 2].value.to_lowercase()) =>
            {
                ok = false
            }
            _ => {}
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    ok
}

/// A subquery predicate inside an aggregate (`bool_and(x IN (SELECT ...))`) cannot be decorrelated
/// even as a count. Over a single relation it is computed per row in a wrapper that keeps the
/// relation's name, where it is a select-list predicate again, and the aggregate reads the column.
fn hoist_from_aggregates(s: &mut Select) {
    let [TableWithJoins { relation, joins }] = s.from.as_mut_slice() else {
        return;
    };
    if !joins.is_empty()
        || s.projection.iter().any(|i| {
            matches!(
                i,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
            )
        })
    {
        return;
    }
    let qualifier = match relation {
        TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } => match alias {
            Some(a) if a.columns.is_empty() => a.name.clone(),
            None => match name.0.last() {
                Some(sq::ObjectNamePart::Identifier(i)) => i.clone(),
                _ => return,
            },
            _ => return,
        },
        _ => return,
    };
    let mut hoisted: Vec<(String, Expr)> = Vec::new();
    let mut take = |e: &mut Expr| {
        at_this_level(e, &mut |x| {
            if let Expr::Function(f) = x
                && f.over.is_none()
                && AGGREGATES.contains(&f.name.to_string().to_ascii_lowercase().as_str())
            {
                let mut inner = Expr::Function(f.clone());
                at_this_level(&mut inner, &mut |y| {
                    if matches!(y, Expr::Exists { .. } | Expr::InSubquery { .. }) {
                        let name = format!("__burrmill_pred{}", hoisted.len());
                        let col =
                            Expr::CompoundIdentifier(vec![qualifier.clone(), Ident::new(&name)]);
                        hoisted.push((name, std::mem::replace(y, col)));
                    }
                });
                *x = inner;
            }
        });
    };
    for item in s.projection.iter_mut() {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
            take(e);
        }
    }
    if let Some(h) = s.having.as_mut() {
        take(h);
    }
    if hoisted.is_empty() {
        return;
    }
    let mut projection = vec![SelectItem::QualifiedWildcard(
        SelectItemQualifiedWildcardKind::ObjectName(sq::ObjectName::from(vec![qualifier.clone()])),
        sq::WildcardAdditionalOptions::default(),
    )];
    projection.extend(
        hoisted
            .into_iter()
            .map(|(name, e)| SelectItem::ExprWithAlias {
                expr: e,
                alias: Ident::new(name),
            }),
    );
    let inner = sq::Select {
        projection,
        from: vec![TableWithJoins {
            relation: relation.clone(),
            joins: vec![],
        }],
        ..empty_select()
    };
    *relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(inner))),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
        }),
        alias: Some(sq::TableAlias {
            explicit: true,
            name: qualifier,
            columns: vec![],
            at: None,
        }),
        sample: None,
    };
}

/// A SELECT with nothing in it, to fill in.
fn empty_select() -> sq::Select {
    let Ok(stmts) =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, "SELECT 1")
    else {
        unreachable!("a constant statement parses")
    };
    let Some(sq::Statement::Query(q)) = stmts.into_iter().next() else {
        unreachable!()
    };
    let SetExpr::Select(s) = *q.body else {
        unreachable!()
    };
    *s
}

pub(super) const AGGREGATES: &[&str] = &[
    "count",
    "sum",
    "min",
    "max",
    "avg",
    "mean",
    "any_value",
    "first",
    "last",
    "string_agg",
    "list",
    "array_agg",
    "bool_and",
    "bool_or",
    "stddev",
    "variance",
    "median",
    "arg_max",
    "arg_min",
    "approx_count_distinct",
    "count_star",
    "group_concat",
    "listagg",
    "arg_extreme",
    "max_by",
    "min_by",
];

fn plain(q: &Query) -> Option<&Select> {
    if q.with.is_some() || q.limit_clause.is_some() || q.fetch.is_some() {
        return None;
    }
    let SetExpr::Select(s) = q.body.as_ref() else {
        return None;
    };
    let grouped =
        !matches!(&s.group_by, sq::GroupByExpr::Expressions(v, m) if v.is_empty() && m.is_empty());
    if grouped
        || s.distinct.is_some()
        || s.having.is_some()
        || s.qualify.is_some()
        || s.top.is_some()
    {
        return None;
    }
    let mut aggregate = false;
    let _ = crate::walk::exprs(&s.projection, |x| {
        if let Expr::Function(f) = x
            && f.over.is_none()
            && AGGREGATES.contains(&f.name.to_string().to_ascii_lowercase().as_str())
        {
            aggregate = true;
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    (!aggregate).then_some(s.as_ref())
}

/// `(SELECT count(*) FROM <q's FROM> WHERE <q's WHERE> [AND extra])`.
fn count(q: &Query, extra: Option<Expr>) -> Expr {
    let mut c = q.clone();
    c.order_by = None;
    let SetExpr::Select(s) = c.body.as_mut() else {
        unreachable!("checked plain")
    };
    s.projection = vec![SelectItem::UnnamedExpr(Expr::Function(sq::Function {
        name: sq::ObjectName::from(vec![Ident::new("count")]),
        uses_odbc_syntax: false,
        parameters: sq::FunctionArguments::None,
        args: sq::FunctionArguments::List(sq::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Wildcard)],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    }))];
    if let Some(extra) = extra {
        s.selection = Some(match s.selection.take() {
            Some(w) => and(nested(w), extra),
            None => extra,
        });
    }
    Expr::Subquery(Box::new(c))
}

fn nested(e: Expr) -> Expr {
    Expr::Nested(Box::new(e))
}

fn and(a: Expr, b: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(a),
        op: sq::BinaryOperator::And,
        right: Box::new(b),
    }
}

fn cmp(a: Expr, op: sq::BinaryOperator, n: i64) -> Expr {
    Expr::BinaryOp {
        left: Box::new(a),
        op,
        right: Box::new(Expr::Value(sq::Value::Number(n.to_string(), false).into())),
    }
}

/// `x [NOT] IN (S)` in a filter, with DataFusion's membership test kept (its mark join gets that
/// right, and fast) and SQL's NULL cases supplied by counts over `S` alone, so `x` stays where it is:
/// no rows is false, a NULL `x` is NULL, a match true, a NULL in `S` NULL, else false.
fn with_nulls(e: &Expr) -> Option<Expr> {
    use sq::BinaryOperator::{Eq, Gt};
    let Expr::InSubquery {
        expr,
        subquery,
        negated,
    } = e
    else {
        return None;
    };
    let s = plain(subquery)?;
    let [SelectItem::UnnamedExpr(y) | SelectItem::ExprWithAlias { expr: y, .. }] =
        s.projection.as_slice()
    else {
        return None;
    };
    let lit = |b: Option<bool>| {
        Expr::Value(
            match b {
                Some(b) => sq::Value::Boolean(b),
                None => sq::Value::Null,
            }
            .into(),
        )
    };
    let member = Expr::InSubquery {
        expr: expr.clone(),
        subquery: subquery.clone(),
        negated: false,
    };
    let case = Expr::Case {
        case_token: sq::helpers::attached_token::AttachedToken::empty(),
        end_token: sq::helpers::attached_token::AttachedToken::empty(),
        operand: None,
        conditions: vec![
            sq::CaseWhen {
                condition: cmp(count(subquery, None), Eq, 0),
                result: lit(Some(false)),
            },
            sq::CaseWhen {
                condition: Expr::IsNull(Box::new(nested(expr.as_ref().clone()))),
                result: lit(None),
            },
            sq::CaseWhen {
                condition: member,
                result: lit(Some(true)),
            },
            sq::CaseWhen {
                condition: cmp(
                    count(subquery, Some(Expr::IsNull(Box::new(nested(y.clone()))))),
                    Gt,
                    0,
                ),
                result: lit(None),
            },
        ],
        else_result: Some(Box::new(lit(Some(false)))),
    };
    Some(if *negated {
        Expr::UnaryOp {
            op: sq::UnaryOperator::Not,
            expr: Box::new(nested(case)),
        }
    } else {
        nested(case)
    })
}

fn as_counts(x: &Expr, outer: &Outer) -> Option<Expr> {
    use sq::BinaryOperator::{Eq, Gt};
    match x {
        Expr::Exists { subquery, negated } => {
            plain(subquery)?;
            Some(nested(cmp(
                count(subquery, None),
                if *negated { Eq } else { Gt },
                0,
            )))
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let s = plain(subquery)?;
            let [SelectItem::UnnamedExpr(y) | SelectItem::ExprWithAlias { expr: y, .. }] =
                s.projection.as_slice()
            else {
                return None;
            };
            let expr = qualified(expr, outer);
            if !movable(&expr, s) {
                return None;
            }
            let (x, y) = (nested(expr), nested(y.clone()));
            let lit = |b: Option<bool>| {
                Expr::Value(
                    match b {
                        Some(b) => sq::Value::Boolean(b),
                        None => sq::Value::Null,
                    }
                    .into(),
                )
            };
            let all = count(subquery, None);
            let matched = count(
                subquery,
                Some(Expr::BinaryOp {
                    left: Box::new(y.clone()),
                    op: Eq,
                    right: Box::new(x.clone()),
                }),
            );
            let nulls = count(subquery, Some(Expr::IsNull(Box::new(y))));
            let case = Expr::Case {
                case_token: sq::helpers::attached_token::AttachedToken::empty(),
                end_token: sq::helpers::attached_token::AttachedToken::empty(),
                operand: None,
                conditions: vec![
                    sq::CaseWhen {
                        condition: cmp(all, Eq, 0),
                        result: lit(Some(false)),
                    },
                    sq::CaseWhen {
                        condition: Expr::IsNull(Box::new(x)),
                        result: lit(None),
                    },
                    sq::CaseWhen {
                        condition: cmp(matched, Gt, 0),
                        result: lit(Some(true)),
                    },
                    sq::CaseWhen {
                        condition: cmp(nulls, Gt, 0),
                        result: lit(None),
                    },
                ],
                else_result: Some(Box::new(lit(Some(false)))),
            };
            Some(if *negated {
                Expr::UnaryOp {
                    op: sq::UnaryOperator::Not,
                    expr: Box::new(nested(case)),
                }
            } else {
                nested(case)
            })
        }
        _ => None,
    }
}

/// DataFusion names an expression without its casts, so `max(CAST(x AS INT))` and `max(x)` in one
/// SELECT share a name and the planner refuses the aggregate, aliases or not. Each such aggregate
/// after the first gets `FILTER (WHERE n = n)`, which changes its name and nothing else.
pub fn distinct_aggregate_names(q: &mut Query) {
    let SetExpr::Select(s) = q.body.as_mut() else {
        return;
    };
    let mut exprs: Vec<&mut Expr> = s
        .projection
        .iter_mut()
        .filter_map(|i| match i {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => Some(e),
            _ => None,
        })
        .collect();
    exprs.extend(s.having.as_mut());
    exprs.extend(s.qualify.as_mut());
    if let Some(sq::OrderByKind::Expressions(v)) = q.order_by.as_mut().map(|o| &mut o.kind) {
        exprs.extend(v.iter_mut().map(|o| &mut o.expr));
    }
    let mut seen: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    for e in exprs {
        at_this_level(e, &mut |x| {
            let Expr::Function(f) = x else { return };
            if f.over.is_some()
                || !AGGREGATES.contains(&f.name.to_string().to_ascii_lowercase().as_str())
            {
                return;
            }
            let full = f.to_string();
            let mut stripped = Expr::Function(f.clone());
            let _ = crate::walk::exprs_mut(&mut stripped, |c| {
                if let Expr::Cast { expr, .. } = c {
                    *c = std::mem::replace(expr.as_mut(), Expr::Value(sq::Value::Null.into()));
                }
                std::ops::ControlFlow::<()>::Continue(())
            });
            let names = seen.entry(stripped.to_string()).or_default();
            let n = names.iter().position(|x| *x == full).unwrap_or_else(|| {
                names.push(full);
                names.len() - 1
            });
            if n > 0 {
                let lit = || Expr::Value(sq::Value::Number(n.to_string(), false).into());
                let tag = Expr::BinaryOp {
                    left: Box::new(lit()),
                    op: sq::BinaryOperator::Eq,
                    right: Box::new(lit()),
                };
                f.filter = Some(Box::new(match f.filter.take() {
                    Some(w) => and(nested(*w), tag),
                    None => tag,
                }));
            }
        });
    }
}
