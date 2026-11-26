//! `USING` and `NATURAL` joins as DuckDB answers them, rewritten in the statement before DataFusion
//! plans it.
//!
//! DuckDB's `SELECT *` over `l JOIN r USING (k)` is `l`'s columns, then `r`'s but `k`: each join
//! column once, where the left side has it, and from the side the join keeps (the right for `RIGHT
//! JOIN`, `COALESCE` of both for `FULL JOIN`). DataFusion keeps the copy whose table name sorts
//! first, where it sorts: `t3 NATURAL JOIN t2` put `t2`'s columns first, and over a `LEFT JOIN` the
//! right side's copy is NULL where the left row had a value. So a `*` over such joins is written out
//! here as each relation's own `*`, the right sides' join columns excluded and the kept ones
//! replaced. A `NATURAL JOIN` whose sides' columns are known becomes `USING` the columns they share,
//! and one that shares none is DuckDB's binder error, where DataFusion made a cross join.

use std::collections::HashSet;

use sqlparser::ast as sq;

use super::dialect::Known;

/// Whether the rewrite is a DuckDB error (a binder error) or a refusal of what is not supported.
pub(super) enum Failed {
    Binder(String),
    Refused(String),
}

/// The qualifier a relation's columns go by in the plan: its alias, or its name as written.
fn qualifier(t: &sq::TableFactor) -> Option<sq::ObjectName> {
    match t {
        sq::TableFactor::Table { alias: Some(a), .. }
        | sq::TableFactor::Derived { alias: Some(a), .. }
        | sq::TableFactor::Function { alias: Some(a), .. }
        | sq::TableFactor::UNNEST { alias: Some(a), .. } => {
            Some(sq::ObjectName::from(vec![a.name.clone()]))
        }
        sq::TableFactor::Table { name, .. } => Some(name.clone()),
        _ => None,
    }
}

/// What [`columns`] reads: the nest's tables, the statement's CTE names, which hide a table of the
/// name, and the columns the query's own CTEs list.
pub(super) struct Scope<'a> {
    pub known: &'a Known,
    pub ctes: &'a HashSet<String>,
    pub listed: Vec<(String, Vec<sq::Ident>)>,
}

impl<'a> Scope<'a> {
    pub fn of(q: &sq::Query, known: &'a Known, ctes: &'a HashSet<String>) -> Self {
        let listed = q
            .with
            .iter()
            .flat_map(|w| &w.cte_tables)
            .filter(|c| !c.alias.columns.is_empty())
            .map(|c| {
                (
                    c.alias.name.value.to_lowercase(),
                    c.alias.columns.iter().map(|c| c.name.clone()).collect(),
                )
            })
            .collect();
        Self {
            known,
            ctes,
            listed,
        }
    }
}

/// A relation's columns, where the statement or the nest's tables tell them.
fn columns(t: &sq::TableFactor, scope: &Scope) -> Option<Vec<sq::Ident>> {
    let listed = |a: &Option<sq::TableAlias>| {
        a.as_ref()
            .filter(|a| !a.columns.is_empty())
            .map(|a| a.columns.iter().map(|c| c.name.clone()).collect())
    };
    match t {
        sq::TableFactor::Table { alias, .. } | sq::TableFactor::Derived { alias, .. }
            if listed(alias).is_some() =>
        {
            listed(alias)
        }
        sq::TableFactor::Table {
            name, args: None, ..
        } if name.0.len() == 1 => {
            let n = name.0[0].as_ident()?.value.to_lowercase();
            if scope.ctes.contains(&n) {
                return scope
                    .listed
                    .iter()
                    .find(|(c, _)| *c == n)
                    .map(|(_, cs)| cs.clone());
            }
            Some(
                scope
                    .known
                    .columns(&n)?
                    .into_iter()
                    .map(sq::Ident::new)
                    .collect(),
            )
        }
        _ => None,
    }
}

fn constraint(op: &sq::JoinOperator) -> Option<(&sq::JoinConstraint, Kind)> {
    Some(match op {
        sq::JoinOperator::Join(c) | sq::JoinOperator::Inner(c) => (c, Kind::Inner),
        sq::JoinOperator::Left(c) | sq::JoinOperator::LeftOuter(c) => (c, Kind::Left),
        sq::JoinOperator::Right(c) | sq::JoinOperator::RightOuter(c) => (c, Kind::Right),
        sq::JoinOperator::FullOuter(c) => (c, Kind::Full),
        sq::JoinOperator::Semi(c)
        | sq::JoinOperator::LeftSemi(c)
        | sq::JoinOperator::RightSemi(c)
        | sq::JoinOperator::Anti(c)
        | sq::JoinOperator::LeftAnti(c)
        | sq::JoinOperator::RightAnti(c) => (c, Kind::Other),
        _ => return None,
    })
}

fn join_constraint(op: &mut sq::JoinOperator) -> Option<(&mut sq::JoinConstraint, Kind)> {
    Some(match op {
        sq::JoinOperator::Join(c) | sq::JoinOperator::Inner(c) => (c, Kind::Inner),
        sq::JoinOperator::Left(c) | sq::JoinOperator::LeftOuter(c) => (c, Kind::Left),
        sq::JoinOperator::Right(c) | sq::JoinOperator::RightOuter(c) => (c, Kind::Right),
        sq::JoinOperator::FullOuter(c) => (c, Kind::Full),
        sq::JoinOperator::Semi(c)
        | sq::JoinOperator::LeftSemi(c)
        | sq::JoinOperator::RightSemi(c)
        | sq::JoinOperator::Anti(c)
        | sq::JoinOperator::LeftAnti(c)
        | sq::JoinOperator::RightAnti(c) => (c, Kind::Other),
        _ => return None,
    })
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Inner,
    Left,
    Right,
    Full,
    Other,
}

fn has_using(s: &sq::Select) -> bool {
    s.from.iter().any(|t| {
        t.joins.iter().any(|j| {
            constraint(&j.join_operator).is_some_and(|(c, _)| {
                matches!(
                    c,
                    sq::JoinConstraint::Using(_) | sq::JoinConstraint::Natural
                )
            })
        })
    })
}

fn coalesce(a: sq::Expr, b: sq::Expr) -> sq::Expr {
    sq::Expr::Function(sq::Function {
        name: sq::ObjectName::from(vec![sq::Ident::new("coalesce")]),
        uses_odbc_syntax: false,
        parameters: sq::FunctionArguments::None,
        args: sq::FunctionArguments::List(sq::FunctionArgumentList {
            duplicate_treatment: None,
            args: [a, b]
                .into_iter()
                .map(|e| sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// The query's selects, rewritten, and its `ORDER BY` with them.
pub(super) fn rewrite_query(q: &mut sq::Query, scope: &Scope) -> Result<(), Failed> {
    let merges = rewrite_body(&mut q.body, scope)?;
    if merges.is_empty() {
        return Ok(());
    }
    if let Some(sq::OrderBy {
        kind: sq::OrderByKind::Expressions(es),
        ..
    }) = &mut q.order_by
    {
        let mut v = Names::new(&merges);
        for e in es {
            let _ = sq::VisitMut::visit(&mut e.expr, &mut v);
        }
        return v.failed.map_or(Ok(()), Err);
    }
    Ok(())
}

/// Each select of a query body, rewritten. For a body that is one select, its merged columns
/// that no alias of its own shadows, which an `ORDER BY` may name.
fn rewrite_body(body: &mut sq::SetExpr, scope: &Scope) -> Result<Vec<Merge>, Failed> {
    match body {
        sq::SetExpr::SetOperation { left, right, .. } => {
            rewrite_body(left, scope)?;
            rewrite_body(right, scope)?;
            Ok(vec![])
        }
        sq::SetExpr::Select(s) if has_using(s) => {
            naturals(s, scope)?;
            let merges = merges(s)?;
            let aliases: Vec<String> = s
                .projection
                .iter()
                .filter_map(|p| match p {
                    sq::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                    _ => None,
                })
                .collect();
            star(s, &merges)?;
            unqualified(s, &merges)?;
            Ok(merges
                .into_iter()
                .filter(|m| !aliases.iter().any(|a| a.eq_ignore_ascii_case(&m.name)))
                .collect())
        }
        _ => Ok(vec![]),
    }
}

/// Each `NATURAL JOIN` whose sides' columns are known, as `USING` the ones they share.
fn naturals(s: &mut sq::Select, scope: &Scope) -> Result<(), Failed> {
    for t in &mut s.from {
        let mut left: Option<Vec<(sq::ObjectName, sq::Ident)>> = qualifier(&t.relation)
            .zip(columns(&t.relation, scope))
            .map(|(q, cs)| cs.into_iter().map(|c| (q.clone(), c)).collect());
        for j in &mut t.joins {
            let right: Option<Vec<(sq::ObjectName, sq::Ident)>> = qualifier(&j.relation)
                .zip(columns(&j.relation, scope))
                .map(|(q, cs)| cs.into_iter().map(|c| (q.clone(), c)).collect());
            if let Some((c, _)) = join_constraint(&mut j.join_operator)
                && matches!(c, sq::JoinConstraint::Natural)
                && let (Some(l), Some(r)) = (&left, &right)
            {
                let mut shared: Vec<sq::Ident> = Vec::new();
                for (_, a) in l {
                    if r.iter().any(|(_, b)| b.value == a.value)
                        && !shared.iter().any(|s| s.value == a.value)
                    {
                        shared.push(a.clone());
                    }
                }
                if shared.is_empty() {
                    let list = |cs: &[(sq::ObjectName, sq::Ident)]| {
                        cs.iter()
                            .map(|(q, c)| format!("{q}.{}", c.value))
                            .collect::<Vec<_>>()
                            .join(", ")
                    };
                    return Err(Failed::Binder(format!(
                        "Binder Error: No columns found to join on in NATURAL JOIN. Use CROSS \
                         JOIN if you intended for this to be a cross-product. Left candidates: \
                         {}. Right candidates: {}",
                        list(l),
                        list(r)
                    )));
                }
                *c = sq::JoinConstraint::Using(
                    shared
                        .into_iter()
                        .map(|i| sq::ObjectName::from(vec![i]))
                        .collect(),
                );
            }
            left = match (left, right) {
                (Some(mut l), Some(r)) => {
                    l.extend(r);
                    Some(l)
                }
                _ => None,
            };
        }
    }
    Ok(())
}

/// A join column that a `RIGHT` or `FULL JOIN ... USING` takes from the right side or from both:
/// the expression it stands for, where the left side is one named relation and so can be told.
struct Merge {
    name: String,
    expr: Option<sq::Expr>,
}

fn using_columns(cols: &[sq::ObjectName]) -> Vec<sq::Ident> {
    cols.iter()
        .filter_map(|c| c.0.last().and_then(|p| p.as_ident().cloned()))
        .collect()
}

/// Every [`Merge`] of the select. A later `USING` on a merged column is refused: DataFusion would
/// join on the left side's copy alone.
fn merges(s: &sq::Select) -> Result<Vec<Merge>, Failed> {
    let mut out = Vec::new();
    for t in &s.from {
        let mut here: Vec<String> = Vec::new();
        let lq = qualifier(&t.relation);
        for (i, j) in t.joins.iter().enumerate() {
            let Some((sq::JoinConstraint::Using(cols), kind)) = constraint(&j.join_operator) else {
                continue;
            };
            let cols = using_columns(cols);
            if let Some(k) = cols.iter().find(|k| here.contains(&k.value)) {
                return Err(Failed::Refused(format!(
                    "a USING join on {}, which a RIGHT or FULL JOIN before it merged, is not \
                     supported here; join with ON",
                    k.value
                )));
            }
            if !matches!(kind, Kind::Right | Kind::Full) {
                continue;
            }
            let rq = qualifier(&j.relation);
            for k in cols {
                let expr = match (&lq, &rq) {
                    (Some(l), Some(r)) if i == 0 => Some(if kind == Kind::Right {
                        column(r, &k)
                    } else {
                        coalesce(column(l, &k), column(r, &k))
                    }),
                    _ => None,
                };
                here.push(k.value.clone());
                out.push(Merge {
                    name: k.value,
                    expr,
                });
            }
        }
    }
    Ok(out)
}

/// One relation's `*` in the rewritten select list.
struct Star {
    qualifier: sq::ObjectName,
    exclude: Vec<sq::Ident>,
    replace: Vec<(sq::Ident, sq::Expr)>,
}

fn column(q: &sq::ObjectName, c: &sq::Ident) -> sq::Expr {
    let mut parts: Vec<sq::Ident> = q.0.iter().filter_map(|p| p.as_ident().cloned()).collect();
    parts.push(c.clone());
    sq::Expr::CompoundIdentifier(parts)
}

fn unmerged(m: &Merge) -> Failed {
    Failed::Refused(format!(
        "the column {} of a RIGHT or FULL JOIN ... USING after another join, or of an unnamed \
         relation, is not supported here unless named with its table",
        m.name
    ))
}

/// The select's unqualified `*`, written out as DuckDB expands it over `USING` joins. Left as it
/// was where a relation has no name, a `NATURAL JOIN`'s columns are unknown, a semi or anti join
/// leaves only one side, or the `*` carries options of its own.
fn star(s: &mut sq::Select, merges: &[Merge]) -> Result<(), Failed> {
    let Some(at) = s.projection.iter().position(|p| match p {
        sq::SelectItem::Wildcard(o) => {
            o.opt_ilike.is_none()
                && o.opt_exclude.is_none()
                && o.opt_except.is_none()
                && o.opt_replace.is_none()
                && o.opt_rename.is_none()
                && o.opt_alias.is_none()
        }
        _ => false,
    }) else {
        return Ok(());
    };
    if let Some(m) = merges.iter().find(|m| m.expr.is_none()) {
        return Err(unmerged(m));
    }
    let mut stars: Vec<Star> = Vec::new();
    for t in &s.from {
        let Some(q) = qualifier(&t.relation) else {
            return Ok(());
        };
        let first = stars.len();
        stars.push(Star {
            qualifier: q,
            exclude: vec![],
            replace: vec![],
        });
        for j in &t.joins {
            let Some(rq) = qualifier(&j.relation) else {
                return Ok(());
            };
            let mut exclude = vec![];
            if let Some((c, kind)) = constraint(&j.join_operator) {
                match c {
                    sq::JoinConstraint::Natural => return Ok(()),
                    sq::JoinConstraint::Using(_) if kind == Kind::Other => return Ok(()),
                    sq::JoinConstraint::Using(cols) => {
                        let cols = using_columns(cols);
                        if matches!(kind, Kind::Right | Kind::Full) {
                            for k in &cols {
                                if let Some(e) = merges
                                    .iter()
                                    .find(|m| m.name == k.value)
                                    .and_then(|m| m.expr.clone())
                                {
                                    stars[first].replace.push((k.clone(), e));
                                }
                            }
                        }
                        exclude = cols;
                    }
                    _ => {}
                }
            } else if !matches!(j.join_operator, sq::JoinOperator::CrossJoin(_)) {
                return Ok(());
            }
            stars.push(Star {
                qualifier: rq,
                exclude,
                replace: vec![],
            });
        }
    }
    let items: Vec<sq::SelectItem> = stars
        .into_iter()
        .map(|st| {
            sq::SelectItem::QualifiedWildcard(
                sq::SelectItemQualifiedWildcardKind::ObjectName(st.qualifier),
                sq::WildcardAdditionalOptions {
                    opt_exclude: (!st.exclude.is_empty()).then(|| {
                        sq::ExcludeSelectItem::Multiple(
                            st.exclude
                                .into_iter()
                                .map(|c| sq::ObjectName::from(vec![c]))
                                .collect(),
                        )
                    }),
                    opt_replace: (!st.replace.is_empty()).then(|| sq::ReplaceSelectItem {
                        items: st
                            .replace
                            .into_iter()
                            .map(|(c, e)| {
                                Box::new(sq::ReplaceSelectElement {
                                    expr: e,
                                    column_name: c,
                                    as_keyword: true,
                                })
                            })
                            .collect(),
                    }),
                    ..Default::default()
                },
            )
        })
        .collect();
    s.projection.splice(at..=at, items);
    Ok(())
}

/// Each unqualified name of a [`Merge`], as the expression it stands for; DataFusion reads it as
/// the left side's copy. Subqueries are their own scope and are left.
struct Names<'a> {
    merges: &'a [Merge],
    depth: usize,
    failed: Option<Failed>,
}

impl<'a> Names<'a> {
    fn new(merges: &'a [Merge]) -> Self {
        Self {
            merges,
            depth: 0,
            failed: None,
        }
    }
}

impl sq::VisitorMut for Names<'_> {
    type Break = ();
    fn pre_visit_query(&mut self, _q: &mut sq::Query) -> std::ops::ControlFlow<()> {
        self.depth += 1;
        std::ops::ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, _q: &mut sq::Query) -> std::ops::ControlFlow<()> {
        self.depth -= 1;
        std::ops::ControlFlow::Continue(())
    }
    fn post_visit_expr(&mut self, e: &mut sq::Expr) -> std::ops::ControlFlow<()> {
        if self.depth > 0 {
            return std::ops::ControlFlow::Continue(());
        }
        if let sq::Expr::Identifier(i) = e
            && let Some(m) = self
                .merges
                .iter()
                .find(|m| m.name.eq_ignore_ascii_case(&i.value))
        {
            match &m.expr {
                Some(x) => *e = sq::Expr::Nested(Box::new(x.clone())),
                None => {
                    self.failed = Some(unmerged(m));
                    return std::ops::ControlFlow::Break(());
                }
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// [`Names`] over the select's own clauses.
fn unqualified(s: &mut sq::Select, merges: &[Merge]) -> Result<(), Failed> {
    use sq::VisitMut;
    if merges.is_empty() {
        return Ok(());
    }
    let mut v = Names::new(merges);
    for item in &mut s.projection {
        if let sq::SelectItem::UnnamedExpr(sq::Expr::Identifier(i)) = item
            && merges.iter().any(|m| m.name.eq_ignore_ascii_case(&i.value))
        {
            let alias = i.clone();
            *item = sq::SelectItem::ExprWithAlias {
                expr: sq::Expr::Identifier(alias.clone()),
                alias,
            };
        }
        let _ = match item {
            sq::SelectItem::UnnamedExpr(e) | sq::SelectItem::ExprWithAlias { expr: e, .. } => {
                e.visit(&mut v)
            }
            _ => std::ops::ControlFlow::Continue(()),
        };
    }
    let _ = s.selection.visit(&mut v);
    let _ = s.having.visit(&mut v);
    let _ = s.qualify.visit(&mut v);
    if let sq::GroupByExpr::Expressions(es, _) = &mut s.group_by {
        let _ = es.visit(&mut v);
    }
    for t in &mut s.from {
        for j in &mut t.joins {
            if let Some((sq::JoinConstraint::On(e), _)) = join_constraint(&mut j.join_operator) {
                let _ = e.visit(&mut v);
            }
        }
    }
    v.failed.map_or(Ok(()), Err)
}
