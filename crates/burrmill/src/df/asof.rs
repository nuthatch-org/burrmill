//! `ASOF [LEFT] JOIN ... ON | USING` (#345), as DuckDB answers it: each left row joins the one
//! right row that matches every equality and is nearest by the single inequality, and under
//! `LEFT` a left row with none keeps NULLs.
//!
//! sqlparser reads only Snowflake's `MATCH_CONDITION` form, so the keyword is blanked before the
//! parse, which keeps every other token where it was, and the join it began is found again by its
//! relation's position and marked. [`AsofJoins`] then plans a marked join as the rows of an
//! inequality join, numbered per left row and kept at the first, which the range-join and
//! top-per-group rules already make fast.

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result, plan_err};
use datafusion_expr::expr::{ScalarFunction, Sort, WindowFunction};
use datafusion_expr::logical_plan::{Join, JoinType};
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{
    BinaryExpr, ColumnarValue, Expr, ExprFunctionExt, LogicalPlan, LogicalPlanBuilder, Operator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, WindowFunctionDefinition,
    col, lit,
};
use datafusion_optimizer::AnalyzerRule;
use sqlparser::ast::{self as sq, Spanned};
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Location, Token, Tokenizer};

pub const MARKER: &str = "burrmill_asof";

/// Words after which `asof` is an operand or a relation, never the join keyword.
const STRUCTURAL: &[Keyword] = &[
    Keyword::FROM,
    Keyword::JOIN,
    Keyword::AS,
    Keyword::ON,
    Keyword::AND,
    Keyword::OR,
    Keyword::NOT,
    Keyword::WHERE,
    Keyword::SELECT,
    Keyword::BY,
    Keyword::USING,
    Keyword::IN,
    Keyword::IS,
    Keyword::LIKE,
    Keyword::WHEN,
    Keyword::THEN,
    Keyword::ELSE,
    Keyword::CASE,
    Keyword::WITH,
    Keyword::DISTINCT,
    Keyword::LATERAL,
];

/// `sql` with each `ASOF` before `[LEFT [OUTER]] JOIN` blanked to spaces, and where each such
/// join's relation starts. Strings and comments are the tokenizer's, so never touched.
pub(crate) fn blank(sql: &str) -> (String, Vec<Location>) {
    if !super::dialect::may_hold_keyword(sql, &["asof"]) {
        return (sql.to_string(), Vec::new());
    }
    let dialect = super::duck_dialect();
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize_with_location() else {
        return (sql.to_string(), Vec::new());
    };
    let words: Vec<_> = tokens
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect();
    let keyword = |i: usize, k: Keyword| {
        words
            .get(i)
            .is_some_and(|t| matches!(&t.token, Token::Word(w) if w.keyword == k))
    };
    // Snowflake's form parses as it is, and is refused by name.
    if (0..words.len()).any(|i| keyword(i, Keyword::MATCH_CONDITION)) {
        return (sql.to_string(), Vec::new());
    }
    let mut blanked = Vec::new();
    let mut at = Vec::new();
    for i in 0..words.len() {
        // The keyword follows a relation or a join condition, which ends in a name or `)`. After
        // `FROM`, a `.` or an operator, `asof` is a table or a column.
        let after_relation = i > 0
            && match &words[i - 1].token {
                Token::RParen => true,
                Token::Word(w) => w.quote_style.is_some() || !STRUCTURAL.contains(&w.keyword),
                _ => false,
            };
        if !keyword(i, Keyword::ASOF) || !after_relation {
            continue;
        }
        let mut j = i + 1;
        if keyword(j, Keyword::LEFT) {
            j += 1;
            if keyword(j, Keyword::OUTER) {
                j += 1;
            }
        }
        if !keyword(j, Keyword::JOIN) {
            continue;
        }
        if let Some(rel) = words.get(j + 1) {
            blanked.push(words[i].span);
            at.push(rel.span.start);
            // A derived table's span starts inside its parenthesis.
            if matches!(rel.token, Token::LParen)
                && let Some(inner) = words.get(j + 2)
            {
                at.push(inner.span.start);
            }
        }
    }
    if blanked.is_empty() {
        return (sql.to_string(), at);
    }
    let mut out = String::with_capacity(sql.len());
    for (n, line) in sql.split_inclusive('\n').enumerate() {
        for (c, ch) in line.chars().enumerate() {
            let here = Location::new(n as u64 + 1, c as u64 + 1);
            let inside = blanked
                .iter()
                .any(|s| s.start <= here && here < s.end && s.start.line == here.line);
            out.push(if inside { ' ' } else { ch });
        }
    }
    (out, at)
}

/// Each join whose relation starts at one of `at`, marked as an ASOF join: `ON c` becomes
/// `ON c AND burrmill_asof()`, and `USING (k.., t)` the equalities on `k` and `left.t >= right.t`.
pub(crate) fn mark(stmt: &mut sq::Statement, at: &[Location]) -> std::result::Result<(), String> {
    if at.is_empty() {
        return Ok(());
    }
    let mut err = None;
    struct V<'a> {
        at: &'a [Location],
        err: &'a mut Option<String>,
    }
    impl sq::VisitorMut for V<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, q: &mut sq::Query) -> std::ops::ControlFlow<()> {
            if let Err(e) = query(q, self.at) {
                *self.err = Some(e);
                return std::ops::ControlFlow::Break(());
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let _ = sq::VisitMut::visit(stmt, &mut V { at, err: &mut err });
    err.map_or(Ok(()), Err)
}

fn query(q: &mut sq::Query, at: &[Location]) -> std::result::Result<(), String> {
    let sq::SetExpr::Select(s) = q.body.as_mut() else {
        return Ok(());
    };
    let wildcard = s
        .projection
        .iter()
        .any(|p| matches!(p, sq::SelectItem::Wildcard(_)));
    for t in &mut s.from {
        tables(t, at, wildcard)?;
    }
    Ok(())
}

fn tables(
    t: &mut sq::TableWithJoins,
    at: &[Location],
    wildcard: bool,
) -> std::result::Result<(), String> {
    if let sq::TableFactor::NestedJoin {
        table_with_joins, ..
    } = &mut t.relation
    {
        tables(table_with_joins, at, wildcard)?;
    }
    let left = name(&t.relation);
    for (i, j) in t.joins.iter_mut().enumerate() {
        if let sq::TableFactor::NestedJoin {
            table_with_joins, ..
        } = &mut j.relation
        {
            tables(table_with_joins, at, wildcard)?;
        }
        if !at.contains(&j.relation.span().start) {
            continue;
        }
        let right = name(&j.relation);
        let c = match &mut j.join_operator {
            sq::JoinOperator::Join(c)
            | sq::JoinOperator::Inner(c)
            | sq::JoinOperator::Left(c)
            | sq::JoinOperator::LeftOuter(c) => c,
            _ => return Err("ASOF joins only as ASOF JOIN or ASOF LEFT JOIN".into()),
        };
        let marker = sq::Expr::Function(sq::Function {
            name: sq::ObjectName::from(vec![sq::Ident::new(MARKER)]),
            uses_odbc_syntax: false,
            parameters: sq::FunctionArguments::None,
            args: sq::FunctionArguments::List(sq::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        });
        *c = match std::mem::replace(c, sq::JoinConstraint::None) {
            sq::JoinConstraint::On(e) => sq::JoinConstraint::On(sq::Expr::BinaryOp {
                left: Box::new(sq::Expr::Nested(Box::new(e))),
                op: sq::BinaryOperator::And,
                right: Box::new(marker),
            }),
            sq::JoinConstraint::Using(cols) => {
                let (Some(l), Some(r)) = (left.as_ref().filter(|_| i == 0), right.as_ref()) else {
                    return Err(
                        "ASOF JOIN ... USING after another join, or of an unnamed relation; write \
                         ASOF JOIN ... ON"
                            .into(),
                    );
                };
                if wildcard {
                    return Err(
                        "SELECT * over ASOF JOIN ... USING; name the columns, or write ON".into(),
                    );
                }
                let qualified = |t: &sq::Ident, c: &sq::ObjectName| {
                    let mut parts = vec![t.clone()];
                    parts.extend(c.0.iter().filter_map(|p| p.as_ident().cloned()));
                    sq::Expr::CompoundIdentifier(parts)
                };
                let Some((last, keys)) = cols.split_last() else {
                    return Err("ASOF JOIN ... USING names no columns".into());
                };
                let mut e = sq::Expr::BinaryOp {
                    left: Box::new(qualified(l, last)),
                    op: sq::BinaryOperator::GtEq,
                    right: Box::new(qualified(r, last)),
                };
                for k in keys {
                    e = sq::Expr::BinaryOp {
                        left: Box::new(sq::Expr::BinaryOp {
                            left: Box::new(qualified(l, k)),
                            op: sq::BinaryOperator::Eq,
                            right: Box::new(qualified(r, k)),
                        }),
                        op: sq::BinaryOperator::And,
                        right: Box::new(e),
                    };
                }
                sq::JoinConstraint::On(sq::Expr::BinaryOp {
                    left: Box::new(e),
                    op: sq::BinaryOperator::And,
                    right: Box::new(marker),
                })
            }
            _ => return Err("ASOF JOIN needs ON or USING".into()),
        };
    }
    Ok(())
}

/// What a column of `t` is qualified by: its alias, or its table's last name.
fn name(t: &sq::TableFactor) -> Option<sq::Ident> {
    match t {
        sq::TableFactor::Table { alias: Some(a), .. }
        | sq::TableFactor::Derived { alias: Some(a), .. } => Some(a.name.clone()),
        sq::TableFactor::Table { name, .. } => name.0.last().and_then(|p| p.as_ident().cloned()),
        _ => None,
    }
}

/// `burrmill_asof()`: true. Present only in the condition of a join that was written ASOF.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Marker(Signature);

impl Marker {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self(Signature::nullary(
            Volatility::Immutable,
        ))))
    }
}

impl ScalarUDFImpl for Marker {
    fn name(&self) -> &str {
        MARKER
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, _: &[arrow::datatypes::DataType]) -> Result<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Boolean)
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(true.into()))
    }
}

fn is_marker(e: &Expr) -> bool {
    matches!(e, Expr::ScalarFunction(ScalarFunction { func, .. }) if func.name() == MARKER)
}

#[derive(Debug, Default)]
pub struct AsofJoins;

impl AnalyzerRule for AsofJoins {
    fn name(&self) -> &str {
        "asof_joins"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let marked = |p: &LogicalPlan| {
            matches!(p, LogicalPlan::Join(j) if j.filter.as_ref()
                .is_some_and(|f| split_conjunction(f).into_iter().any(is_marker)))
        };
        // Its walk copies every shared view (`dag.rs`), so it is taken only where there is work.
        let mut any = false;
        plan.apply_with_subqueries(|p| {
            any = marked(p);
            Ok(match any {
                true => TreeNodeRecursion::Stop,
                false => TreeNodeRecursion::Continue,
            })
        })?;
        if !any {
            return Ok(plan);
        }
        let mut n = 0usize;
        plan.transform_up_with_subqueries(|p| match p {
            LogicalPlan::Join(j)
                if j.filter
                    .as_ref()
                    .is_some_and(|f| split_conjunction(f).into_iter().any(is_marker)) =>
            {
                n += 1;
                Ok(Transformed::yes(asof(j, n)?))
            }
            p => Ok(Transformed::no(p)),
        })
        .map(|t| t.data)
    }
}

/// Which side of the join every column of `e` is from, if all are from one.
fn side(e: &Expr, left: &DFSchema, right: &DFSchema) -> Option<bool> {
    let cols = e.column_refs();
    if cols.is_empty() {
        return None;
    }
    if cols.iter().all(|c| left.has_column(c)) {
        Some(true)
    } else if cols.iter().all(|c| right.has_column(c)) {
        Some(false)
    } else {
        None
    }
}

fn asof(j: Join, n: usize) -> Result<LogicalPlan> {
    if !matches!(j.join_type, JoinType::Inner | JoinType::Left) {
        return plan_err!("ASOF joins only as ASOF JOIN or ASOF LEFT JOIN");
    }
    let (ls, rs) = (j.left.schema().clone(), j.right.schema().clone());
    let mut keys: Vec<Expr> = j.on.iter().map(|(l, r)| l.clone().eq(r.clone())).collect();
    let mut inequality: Option<(Expr, Operator, Expr)> = None;
    for c in split_conjunction(j.filter.as_ref().expect("marked")) {
        if is_marker(c) {
            continue;
        }
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = c else {
            return plan_err!("ASOF JOIN takes equalities and one inequality, not {c}");
        };
        let (l, r) = match (side(left, &ls, &rs), side(right, &ls, &rs)) {
            (Some(true), Some(false)) => ((**left).clone(), (**right).clone()),
            (Some(false), Some(true)) => ((**right).clone(), (**left).clone()),
            _ => {
                return plan_err!("ASOF JOIN compares a left expression with a right one, not {c}");
            }
        };
        let flipped = side(left, &ls, &rs) == Some(false);
        let op = match (op, flipped) {
            (Operator::Eq, _) => {
                keys.push(l.eq(r));
                continue;
            }
            (Operator::GtEq, false) | (Operator::LtEq, true) => Operator::GtEq,
            (Operator::Gt, false) | (Operator::Lt, true) => Operator::Gt,
            (Operator::LtEq, false) | (Operator::GtEq, true) => Operator::LtEq,
            (Operator::Lt, false) | (Operator::Gt, true) => Operator::Lt,
            _ => return plan_err!("ASOF JOIN takes equalities and one inequality, not {c}"),
        };
        if inequality.replace((l, op, r)).is_some() {
            return plan_err!("ASOF JOIN takes exactly one inequality");
        }
    }
    let Some((l, op, r)) = inequality else {
        return plan_err!("ASOF JOIN needs one inequality between the two sides");
    };
    let rid = format!("__burrmill_asof_rid{n}");
    let rn = format!("__burrmill_asof_rn{n}");
    let row_number = || {
        Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::row_number::row_number_udwf(),
            ),
            vec![],
        ))
    };
    let numbered = LogicalPlanBuilder::from(Arc::unwrap_or_clone(j.left.clone()))
        .window(vec![row_number().alias(&rid)])?
        .build()?;
    let condition = conjunction(keys.into_iter().chain([Expr::BinaryExpr(BinaryExpr::new(
        Box::new(l),
        op,
        Box::new(r.clone()),
    ))]))
    .expect("one inequality at least");
    // Nearest first: the latest right row below or at a `>=`, the earliest above a `<=`.
    let latest = matches!(op, Operator::GtEq | Operator::Gt);
    let nearest = row_number()
        .partition_by(vec![col(&rid)])
        .order_by(vec![Sort::new(r, !latest, false)])
        .build()?
        .alias(&rn);
    let matched = LogicalPlanBuilder::from(numbered.clone())
        .join_on(
            Arc::unwrap_or_clone(j.right.clone()),
            JoinType::Inner,
            vec![condition],
        )?
        .window(vec![nearest])?
        .filter(col(&rn).eq(lit(1u64)))?;
    let wanted = |schema: &DFSchema| -> Vec<Expr> {
        schema
            .iter()
            .map(|(q, f)| Expr::Column(Column::from((q, f.as_ref()))))
            .collect()
    };
    let plan = if j.join_type == JoinType::Inner {
        matched.project(wanted(&j.schema))?.build()?
    } else {
        let rid2 = format!("{rid}_right");
        let right_side: Vec<Expr> = std::iter::once(col(&rid).alias(&rid2))
            .chain(wanted(&rs))
            .collect();
        LogicalPlanBuilder::from(numbered)
            .join_on(
                matched.project(right_side)?.build()?,
                JoinType::Left,
                vec![col(&rid).eq(col(&rid2))],
            )?
            .project(wanted(&j.schema))?
            .build()?
    };
    Ok(plan)
}
