//! `COUNT(t.*)` as DuckDB answers it, rewritten in the statement before DataFusion plans it (#421).
//!
//! DuckDB counts the rows in which `t` is present: a row of `t` counts even when every column of it
//! is NULL, and a row an outer join NULL-extends `t` in does not. DataFusion read `COUNT(t.*)` as
//! `COUNT(*)`, which counts the NULL-extended rows too. So in each `SELECT`, a `COUNT(t.*)` over a
//! relation of its own `FROM` that no outer join NULL-extends is `COUNT(*)`; over one an outer join
//! does NULL-extend, it is `COUNT(t.c)` for a column `t.c` that a condition of such a join rejects
//! NULL in (`ON l.id = t.c`, `USING (c)`), since a row that passed the join has `t.c` set and a row
//! the join NULL-extended has it NULL. That holds for the NULL-extended side of a `LEFT` or `RIGHT`
//! join, whose rows pass only matched, and not for either side of a `FULL` join, whose rows may
//! pass unmatched. Over a relation with no such column (`ON true`, `IS NOT DISTINCT FROM`, a `FULL`
//! join) it is refused by name. A qualifier that names no relation of the `FROM` is left alone.

use std::ops::ControlFlow;

use sqlparser::ast::{self as sq, Expr as SqlExpr, VisitorMut};

/// A relation of a `FROM`: the name its columns go by, whether an outer join NULL-extends it, and
/// the columns of it that a NULL-extending join's condition rejects NULL in.
struct Rel {
    name: Option<String>,
    extended: bool,
    keys: Vec<sq::Ident>,
}

fn lower(i: &sq::Ident) -> String {
    i.value.to_lowercase()
}

/// The name a relation's columns go by: its alias, or the last part of its name as written.
fn name_of(t: &sq::TableFactor) -> Option<String> {
    match t {
        sq::TableFactor::Table { alias: Some(a), .. }
        | sq::TableFactor::Derived { alias: Some(a), .. }
        | sq::TableFactor::TableFunction { alias: Some(a), .. }
        | sq::TableFactor::Function { alias: Some(a), .. }
        | sq::TableFactor::UNNEST { alias: Some(a), .. }
        | sq::TableFactor::NestedJoin { alias: Some(a), .. } => Some(lower(&a.name)),
        sq::TableFactor::Table { name, .. } => name.0.last().and_then(|p| p.as_ident()).map(lower),
        _ => None,
    }
}

/// The relations one table factor brings: itself, or a nested join's own relations.
fn factor(t: &sq::TableFactor, out: &mut Vec<Rel>) {
    if let sq::TableFactor::NestedJoin {
        table_with_joins,
        alias,
    } = t
    {
        let start = out.len();
        joined(table_with_joins, out);
        if alias.is_some() {
            // The nest's columns go by its alias; whether a row of it is present is not one
            // relation's question, so it is counted only where nothing NULL-extends it.
            let extended = out[start..].iter().any(|r| r.extended);
            out.truncate(start);
            out.push(Rel {
                name: name_of(t),
                extended,
                keys: Vec::new(),
            });
        }
        return;
    }
    out.push(Rel {
        name: name_of(t),
        extended: false,
        keys: Vec::new(),
    });
}

/// The columns of the relation `name` a join condition rejects NULL in: a top-level conjunct that
/// compares `name.c`, or says `name.c IS NOT NULL`.
fn rejected(on: &SqlExpr, name: &str, out: &mut Vec<sq::Ident>) {
    let col = |e: &SqlExpr| match e {
        SqlExpr::CompoundIdentifier(p) if p.len() == 2 && lower(&p[0]) == name => {
            Some(p[1].clone())
        }
        _ => None,
    };
    match on {
        SqlExpr::Nested(e) => rejected(e, name, out),
        SqlExpr::BinaryOp {
            left,
            op: sq::BinaryOperator::And,
            right,
        } => {
            rejected(left, name, out);
            rejected(right, name, out);
        }
        SqlExpr::BinaryOp {
            left,
            op:
                sq::BinaryOperator::Eq
                | sq::BinaryOperator::NotEq
                | sq::BinaryOperator::Lt
                | sq::BinaryOperator::LtEq
                | sq::BinaryOperator::Gt
                | sq::BinaryOperator::GtEq,
            right,
        } => out.extend(col(left).into_iter().chain(col(right))),
        SqlExpr::IsNotNull(e) => out.extend(col(e)),
        _ => {}
    }
}

/// Marks `rels` NULL-extended by a join with `constraint`, and adds the columns it rejects NULL in.
fn extend(rels: &mut [Rel], constraint: Option<&sq::JoinConstraint>) {
    let single = rels.len() == 1;
    for r in rels.iter_mut() {
        r.extended = true;
        let Some(name) = r.name.clone() else { continue };
        match constraint {
            Some(sq::JoinConstraint::On(on)) => rejected(on, &name, &mut r.keys),
            // A USING column is the side's own only when the side is one relation.
            Some(sq::JoinConstraint::Using(cols)) if single => r.keys.extend(
                cols.iter()
                    .filter(|c| c.0.len() == 1)
                    .filter_map(|c| c.0[0].as_ident().cloned()),
            ),
            _ => {}
        }
    }
}

/// The relations of one `FROM` item, each marked as the item's outer joins NULL-extend it. A join
/// this does not know marks every relation of the item NULL-extended with no column, so a
/// `COUNT(t.*)` over it is refused rather than guessed.
fn joined(t: &sq::TableWithJoins, out: &mut Vec<Rel>) {
    let start = out.len();
    factor(&t.relation, out);
    for j in &t.joins {
        let left = out.len();
        factor(&j.relation, out);
        let (l, r) = out[start..].split_at_mut(left - start);
        match &j.join_operator {
            sq::JoinOperator::Join(_)
            | sq::JoinOperator::Inner(_)
            | sq::JoinOperator::CrossJoin(_)
            | sq::JoinOperator::CrossApply
            | sq::JoinOperator::StraightJoin(_) => {}
            sq::JoinOperator::Left(c) | sq::JoinOperator::LeftOuter(c) => extend(r, Some(c)),
            sq::JoinOperator::OuterApply => extend(r, None),
            sq::JoinOperator::Right(c) | sq::JoinOperator::RightOuter(c) => extend(l, Some(c)),
            // A row of either side may pass a full join unmatched, its columns as they were, so
            // no condition says that a side's row is present.
            sq::JoinOperator::FullOuter(_) => {
                extend(l, None);
                extend(r, None);
            }
            _ => {
                extend(l, None);
                extend(r, None);
            }
        }
    }
}

/// Rewrites the `COUNT(t.*)` of a query's `ORDER BY`, not of the queries nested in it.
struct Order<'a> {
    frames: &'a mut Frames,
    depth: usize,
}

impl VisitorMut for Order<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _: &mut sq::Query) -> ControlFlow<()> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &mut sq::Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &mut SqlExpr) -> ControlFlow<()> {
        if self.depth == 0 && self.frames.expr(e).is_err() {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// Whether `e` is a `COUNT(t.*)`.
fn is_count_star(e: &SqlExpr) -> bool {
    matches!(e, SqlExpr::Function(f)
        if f.name.0.len() == 1
            && f.name.to_string().eq_ignore_ascii_case("count")
            && matches!(&f.args, sq::FunctionArguments::List(l)
                if matches!(l.args.as_slice(),
                    [sq::FunctionArg::Unnamed(sq::FunctionArgExpr::QualifiedWildcard(_))])))
}

fn relations(s: &sq::Select) -> Vec<Rel> {
    let mut rels = Vec::new();
    for t in &s.from {
        joined(t, &mut rels);
    }
    rels
}

/// The relations of the `SELECT` each expression of a statement's walk is in, innermost last. The
/// statement's walk calls these as it enters and leaves each query and `SELECT`, and on each
/// expression; a query pushes a frame of no relations, so its own clauses (`LIMIT`, a set
/// operation's `ORDER BY`) see none.
#[derive(Default)]
pub(super) struct Frames {
    frames: Vec<Vec<Rel>>,
    /// The refusal a rewrite met, once.
    refused: Option<String>,
}

impl Frames {
    pub fn enter_query(&mut self, q: &mut sq::Query) -> Result<(), String> {
        self.frames.push(Vec::new());
        // A `SELECT`'s `ORDER BY` is the query's, and walked after the `SELECT` is left.
        if let (sq::SetExpr::Select(s), Some(o)) = (q.body.as_ref(), q.order_by.as_mut()) {
            let rels = relations(s);
            if !rels.is_empty() {
                self.frames.push(rels);
                let mut v = Order {
                    frames: self,
                    depth: 0,
                };
                let _ = crate::walk::walk_mut(o, &mut v);
                self.frames.pop();
            }
        }
        self.refused.take().map_or(Ok(()), Err)
    }

    pub fn leave_query(&mut self) {
        self.frames.pop();
    }

    pub fn enter_select(&mut self, s: &mut sq::Select) {
        let rels = relations(s);
        if !rels.is_empty() {
            // A `COUNT(t.*)` with no name of its own keeps DuckDB's, `count(t.*)`.
            for item in &mut s.projection {
                if let sq::SelectItem::UnnamedExpr(e) = item
                    && is_count_star(e)
                {
                    let alias = sq::Ident::with_quote('"', e.to_string());
                    *item = sq::SelectItem::ExprWithAlias {
                        expr: e.clone(),
                        alias,
                    };
                }
            }
        }
        self.frames.push(rels);
    }

    pub fn leave_select(&mut self) {
        self.frames.pop();
    }

    /// Rewrites `e` if it is a `COUNT(t.*)` over a relation of the innermost `SELECT`'s `FROM`.
    pub fn expr(&mut self, e: &mut SqlExpr) -> Result<(), String> {
        if !is_count_star(e) {
            return Ok(());
        }
        let SqlExpr::Function(f) = e else {
            return Ok(());
        };
        let Some(rels) = self.frames.last() else {
            return Ok(());
        };
        match arg(rels, f) {
            Ok(Some(new)) => {
                if let sq::FunctionArguments::List(list) = &mut f.args {
                    list.args = vec![sq::FunctionArg::Unnamed(new)];
                }
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(why) => {
                self.refused = Some(why.clone());
                Err(why)
            }
        }
    }
}

/// `Some(new argument)` for a `COUNT(t.*)` over a relation of `rels`, `None` to leave it, `Err`
/// to refuse it.
fn arg(rels: &[Rel], f: &sq::Function) -> Result<Option<sq::FunctionArgExpr>, String> {
    let sq::FunctionArguments::List(list) = &f.args else {
        return Ok(None);
    };
    let [sq::FunctionArg::Unnamed(sq::FunctionArgExpr::QualifiedWildcard(q))] =
        list.args.as_slice()
    else {
        return Ok(None);
    };
    let [part] = q.0.as_slice() else {
        return Ok(None);
    };
    let Some(ident) = part.as_ident() else {
        return Ok(None);
    };
    let name = lower(ident);
    let mut found = rels.iter().filter(|r| r.name.as_deref() == Some(&name));
    let (Some(rel), None) = (found.next(), found.next()) else {
        return Ok(None);
    };
    if list.duplicate_treatment == Some(sq::DuplicateTreatment::Distinct) {
        return Err(format!("COUNT(DISTINCT {q}.*) is not supported here"));
    }
    if !rel.extended {
        return Ok(Some(sq::FunctionArgExpr::Wildcard));
    }
    match rel.keys.first() {
        Some(c) => Ok(Some(sq::FunctionArgExpr::Expr(
            SqlExpr::CompoundIdentifier(vec![ident.clone(), c.clone()]),
        ))),
        None => Err(format!(
            "COUNT({q}.*) over a relation an outer join NULL-extends, with no column the join's \
             condition rejects NULL in, is not supported here"
        )),
    }
}
