//! A chain of binary operators is planned by recursion. Five hundred terms took 21 seconds
//! and a thousand aborted the process. A stack overflow is not a panic, so nothing catches it.
//! The bound is on the parsed tree, walked on the heap. What passes is planned on a stack of a
//! known size.

use datafusion_sql::parser::Statement as DfStatement;
use sqlparser::ast::{self as sq, Expr as SqlExpr};
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::error::{BurrmillError, Result};

/// Longest path of expression nodes. A left-associative chain of N literals has depth N.
const MAX_EXPR_DEPTH: usize = 64;
/// Nodes in one root expression: a select item, a predicate, an `ORDER BY` key, and the like.
const MAX_EXPR_TERMS: usize = 1024;
/// Nested queries and set operations. A visitor descends the left spine before it can stop.
const MAX_SET_DEPTH: usize = 64;
/// Tables, subqueries and table functions in one statement. Planning is about cubic in them:
/// 1,200 took 28 s and could not be cancelled. The largest authored view has under 80.
const MAX_RELATIONS: usize = 256;

/// Refuse a statement whose plan would recurse deep enough to abort the process.
///
/// A statement the parser rejects is left alone, so the planner reports that error itself.
pub fn check_expr_bounds(sql: &str) -> Result<()> {
    let stmts = match super::dialect::parse_statements(sql) {
        Ok(stmts) => stmts,
        Err(BurrmillError::Parse(_)) => return Ok(()),
        Err(e) => return Err(e),
    };
    for stmt in &stmts {
        check_df(stmt)?;
    }
    Ok(())
}

pub(crate) fn check_statement(stmt: &sq::Statement) -> Result<()> {
    Walk {
        work: vec![Work::Stmt(stmt, 0, 0)],
        groups: Vec::new(),
        relations: 0,
    }
    .run()
}

/// `UNION` / `INTERSECT` / `EXCEPT` as keywords, not as text. The tokenizer is iterative.
/// A comment or a string containing the word is not a keyword, and a quoted identifier is not either.
pub(crate) fn refuse_set_op_tokens(sql: &str) -> Result<()> {
    let mut tokenizer = Tokenizer::new(&super::dialect::Duck, sql);
    let tokens = match tokenizer.tokenize() {
        Ok(tokens) => tokens,
        Err(_) => return Ok(()),
    };
    let mut n = 0usize;
    for token in tokens {
        let Token::Word(word) = token else {
            continue;
        };
        if word.quote_style.is_none()
            && matches!(
                word.keyword,
                Keyword::UNION | Keyword::INTERSECT | Keyword::EXCEPT
            )
        {
            n += 1;
            if n > MAX_SET_DEPTH {
                return Err(too_nested());
            }
        }
    }
    Ok(())
}

fn check_df(stmt: &DfStatement) -> Result<()> {
    match stmt {
        DfStatement::Statement(s) => check_statement(s),
        DfStatement::Explain(e) => check_df(e.statement.as_ref()),
        DfStatement::CreateExternalTable(_) | DfStatement::CopyTo(_) | DfStatement::Reset(_) => {
            Ok(())
        }
    }
}

fn too_deep() -> BurrmillError {
    BurrmillError::NotAllowed(format!(
        "expression is deeper than {MAX_EXPR_DEPTH} and cannot be planned"
    ))
}

fn too_wide() -> BurrmillError {
    BurrmillError::NotAllowed(format!(
        "expression has more than {MAX_EXPR_TERMS} terms and cannot be planned"
    ))
}

fn too_many_relations() -> BurrmillError {
    BurrmillError::NotAllowed(format!(
        "statement reads more than {MAX_RELATIONS} tables or subqueries and cannot be planned"
    ))
}

fn too_nested() -> BurrmillError {
    BurrmillError::NotAllowed(format!(
        "query nests more than {MAX_SET_DEPTH} queries or set operations and cannot be planned"
    ))
}

enum Work<'a> {
    Stmt(&'a sq::Statement, usize, usize),
    Query(&'a sq::Query, usize, usize),
    Set(&'a sq::SetExpr, usize, usize, usize),
    Expr(&'a SqlExpr, usize, usize, usize),
}

struct Walk<'a> {
    work: Vec<Work<'a>>,
    groups: Vec<usize>,
    relations: usize,
}

impl<'a> Walk<'a> {
    fn run(&mut self) -> Result<()> {
        while let Some(work) = self.work.pop() {
            if self.relations > MAX_RELATIONS {
                return Err(too_many_relations());
            }
            match work {
                Work::Stmt(stmt, expr_depth, nest) => self.statement(stmt, expr_depth, nest)?,
                Work::Query(query, expr_depth, nest) => {
                    if nest > MAX_SET_DEPTH {
                        return Err(too_nested());
                    }
                    self.query(query, expr_depth, nest);
                }
                Work::Set(set, set_depth, expr_depth, nest) => {
                    self.set(set, set_depth, expr_depth, nest)?;
                }
                Work::Expr(expr, depth, group, nest) => {
                    self.groups[group] += 1;
                    if depth > MAX_EXPR_DEPTH {
                        return Err(too_deep());
                    }
                    if self.groups[group] > MAX_EXPR_TERMS {
                        return Err(too_wide());
                    }
                    self.expand(expr, depth, group, nest);
                }
            }
        }
        if self.relations > MAX_RELATIONS {
            return Err(too_many_relations());
        }
        Ok(())
    }

    fn root(&mut self, expr: &'a SqlExpr, depth: usize, nest: usize) {
        let group = self.groups.len();
        self.groups.push(0);
        self.work.push(Work::Expr(expr, depth, group, nest));
    }

    fn child(&mut self, expr: &'a SqlExpr, depth: usize, group: usize, nest: usize) {
        self.work.push(Work::Expr(expr, depth, group, nest));
    }

    fn query_at(&mut self, query: &'a sq::Query, expr_depth: usize, nest: usize) {
        self.work.push(Work::Query(query, expr_depth, nest));
    }

    fn statement(&mut self, stmt: &'a sq::Statement, expr_depth: usize, nest: usize) -> Result<()> {
        let root = expr_depth + 1;
        let nest = nest.max(1);
        match stmt {
            sq::Statement::Query(query) => self.query_at(query, expr_depth, nest),
            sq::Statement::Explain { statement, .. } => {
                self.work.push(Work::Stmt(statement, expr_depth, nest));
            }
            sq::Statement::Insert(insert) => {
                if let Some(source) = &insert.source {
                    self.query_at(source, expr_depth, nest + 1);
                }
                for assignment in &insert.assignments {
                    self.root(&assignment.value, root, nest);
                }
                if let Some(partitioned) = &insert.partitioned {
                    for expr in partitioned {
                        self.root(expr, root, nest);
                    }
                }
                if let Some(returning) = &insert.returning {
                    for item in returning {
                        self.select_item(item, root, nest);
                    }
                }
            }
            sq::Statement::Update(update) => {
                self.table_with_joins(&update.table, root, nest);
                for assignment in &update.assignments {
                    self.root(&assignment.value, root, nest);
                }
                if let Some(from) = &update.from {
                    let tables = match from {
                        sq::UpdateTableFromKind::BeforeSet(tables)
                        | sq::UpdateTableFromKind::AfterSet(tables) => tables,
                    };
                    for table in tables {
                        self.table_with_joins(table, root, nest);
                    }
                }
                if let Some(selection) = &update.selection {
                    self.root(selection, root, nest);
                }
                if let Some(returning) = &update.returning {
                    for item in returning {
                        self.select_item(item, root, nest);
                    }
                }
                for order in &update.order_by {
                    self.order_by(order, root, nest);
                }
                if let Some(limit) = &update.limit {
                    self.root(limit, root, nest);
                }
            }
            sq::Statement::Delete(delete) => {
                let tables = match &delete.from {
                    sq::FromTable::WithFromKeyword(tables)
                    | sq::FromTable::WithoutKeyword(tables) => tables,
                };
                for table in tables {
                    self.table_with_joins(table, root, nest);
                }
                if let Some(using) = &delete.using {
                    for table in using {
                        self.table_with_joins(table, root, nest);
                    }
                }
                if let Some(selection) = &delete.selection {
                    self.root(selection, root, nest);
                }
                if let Some(returning) = &delete.returning {
                    for item in returning {
                        self.select_item(item, root, nest);
                    }
                }
                for order in &delete.order_by {
                    self.order_by(order, root, nest);
                }
                if let Some(limit) = &delete.limit {
                    self.root(limit, root, nest);
                }
            }
            sq::Statement::CreateView(view) => self.query_at(&view.query, expr_depth, nest + 1),
            sq::Statement::CreateTable(table) => {
                if let Some(query) = &table.query {
                    self.query_at(query, expr_depth, nest + 1);
                }
            }
            // Set-operation keywords are refused before parse. An expression in a statement
            // kind this arm does not unwrap is still reachable by the later visitors.
            _ => {}
        }
        Ok(())
    }

    fn query(&mut self, query: &'a sq::Query, expr_depth: usize, nest: usize) {
        let root = expr_depth + 1;
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.query_at(&cte.query, expr_depth, nest + 1);
            }
        }
        self.work.push(Work::Set(&query.body, 1, expr_depth, nest));
        if let Some(order) = &query.order_by {
            self.order_kind(&order.kind, root, nest);
            if let Some(interpolate) = &order.interpolate
                && let Some(exprs) = &interpolate.exprs
            {
                for expr in exprs {
                    if let Some(expr) = &expr.expr {
                        self.root(expr, root, nest);
                    }
                }
            }
        }
        if let Some(limit) = &query.limit_clause {
            self.limit(limit, root, nest);
        }
        if let Some(fetch) = &query.fetch
            && let Some(quantity) = &fetch.quantity
        {
            self.root(quantity, root, nest);
        }
        for pipe in &query.pipe_operators {
            self.pipe(pipe, root, expr_depth, nest);
        }
    }

    fn set(
        &mut self,
        set: &'a sq::SetExpr,
        set_depth: usize,
        expr_depth: usize,
        nest: usize,
    ) -> Result<()> {
        let root = expr_depth + 1;
        match set {
            sq::SetExpr::Select(select) => self.select(select, root, nest),
            sq::SetExpr::Query(query) => self.query_at(query, expr_depth, nest + 1),
            sq::SetExpr::SetOperation { left, right, .. } => {
                if set_depth > MAX_SET_DEPTH {
                    return Err(too_nested());
                }
                self.work
                    .push(Work::Set(left, set_depth + 1, expr_depth, nest));
                self.work
                    .push(Work::Set(right, set_depth + 1, expr_depth, nest));
            }
            sq::SetExpr::Values(values) => {
                for row in &values.rows {
                    for expr in row.iter() {
                        self.root(expr, root, nest);
                    }
                }
            }
            sq::SetExpr::Insert(stmt)
            | sq::SetExpr::Update(stmt)
            | sq::SetExpr::Delete(stmt)
            | sq::SetExpr::Merge(stmt) => {
                self.work.push(Work::Stmt(stmt, expr_depth, nest));
            }
            sq::SetExpr::Table(_) => {}
        }
        Ok(())
    }

    fn select(&mut self, select: &'a sq::Select, root: usize, nest: usize) {
        if let Some(sq::Distinct::On(exprs)) = &select.distinct {
            for expr in exprs {
                self.root(expr, root, nest);
            }
        }
        if let Some(top) = &select.top
            && let Some(sq::TopQuantity::Expr(expr)) = &top.quantity
        {
            self.root(expr, root, nest);
        }
        for item in &select.projection {
            self.select_item(item, root, nest);
        }
        for table in &select.from {
            self.table_with_joins(table, root, nest);
        }
        for view in &select.lateral_views {
            self.root(&view.lateral_view, root, nest);
        }
        for expr in [
            &select.prewhere,
            &select.selection,
            &select.having,
            &select.qualify,
        ]
        .into_iter()
        .flatten()
        {
            self.root(expr, root, nest);
        }
        for connect in &select.connect_by {
            match connect {
                sq::ConnectByKind::ConnectBy { relationships, .. } => {
                    for expr in relationships {
                        self.root(expr, root, nest);
                    }
                }
                sq::ConnectByKind::StartWith { condition, .. } => self.root(condition, root, nest),
            }
        }
        match &select.group_by {
            sq::GroupByExpr::Expressions(exprs, modifiers) => {
                for expr in exprs {
                    self.root(expr, root, nest);
                }
                self.group_modifiers(modifiers, root, nest);
            }
            sq::GroupByExpr::All(modifiers) => self.group_modifiers(modifiers, root, nest),
        }
        for expr in select.cluster_by.iter().chain(&select.distribute_by) {
            self.root(expr, root, nest);
        }
        for order in &select.sort_by {
            self.order_by(order, root, nest);
        }
        for window in &select.named_window {
            if let sq::NamedWindowExpr::WindowSpec(spec) = &window.1 {
                self.window_spec(spec, root, nest);
            }
        }
    }

    fn group_modifiers(
        &mut self,
        modifiers: &'a [sq::GroupByWithModifier],
        root: usize,
        nest: usize,
    ) {
        for modifier in modifiers {
            if let sq::GroupByWithModifier::GroupingSets(expr) = modifier {
                self.root(expr, root, nest);
            }
        }
    }

    fn select_item(&mut self, item: &'a sq::SelectItem, root: usize, nest: usize) {
        match item {
            sq::SelectItem::UnnamedExpr(expr)
            | sq::SelectItem::ExprWithAlias { expr, .. }
            | sq::SelectItem::ExprWithAliases { expr, .. } => self.root(expr, root, nest),
            sq::SelectItem::QualifiedWildcard(kind, options) => {
                if let sq::SelectItemQualifiedWildcardKind::Expr(expr) = kind {
                    self.root(expr, root, nest);
                }
                self.wildcard(options, root, nest);
            }
            sq::SelectItem::Wildcard(options) => self.wildcard(options, root, nest),
        }
    }

    fn wildcard(&mut self, options: &'a sq::WildcardAdditionalOptions, root: usize, nest: usize) {
        if let Some(replace) = &options.opt_replace {
            for item in &replace.items {
                self.root(&item.expr, root, nest);
            }
        }
    }

    fn table_with_joins(&mut self, table: &'a sq::TableWithJoins, root: usize, nest: usize) {
        self.table_factor(&table.relation, root, nest);
        for join in &table.joins {
            self.table_factor(&join.relation, root, nest);
            let (asof, constraint) = join_parts(&join.join_operator);
            if let Some(expr) = asof {
                self.root(expr, root, nest);
            }
            if let Some(sq::JoinConstraint::On(expr)) = constraint {
                self.root(expr, root, nest);
            }
        }
    }

    fn table_factor(&mut self, factor: &'a sq::TableFactor, root: usize, nest: usize) {
        self.relations += 1;
        match factor {
            sq::TableFactor::Table {
                args,
                with_hints,
                version,
                json_path,
                sample,
                ..
            } => {
                if let Some(args) = args {
                    for arg in &args.args {
                        self.function_arg_root(arg, root, nest);
                    }
                }
                for hint in with_hints {
                    self.root(hint, root, nest);
                }
                if let Some(version) = version {
                    self.table_version(version, root, nest);
                }
                if let Some(path) = json_path {
                    self.json_path_roots(path, root, nest);
                }
                if let Some(sample) = sample {
                    self.sample(sample, root, nest);
                }
            }
            sq::TableFactor::Derived {
                subquery, sample, ..
            } => {
                self.query_at(subquery, root.saturating_sub(1), nest + 1);
                if let Some(sample) = sample {
                    self.sample(sample, root, nest);
                }
            }
            sq::TableFactor::TableFunction { expr, .. } => self.root(expr, root, nest),
            sq::TableFactor::Function { args, .. } => {
                for arg in args {
                    self.function_arg_root(arg, root, nest);
                }
            }
            sq::TableFactor::UNNEST { array_exprs, .. } => {
                for expr in array_exprs {
                    self.root(expr, root, nest);
                }
            }
            sq::TableFactor::JsonTable { json_expr, .. }
            | sq::TableFactor::OpenJsonTable { json_expr, .. } => self.root(json_expr, root, nest),
            sq::TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.table_with_joins(table_with_joins, root, nest);
            }
            sq::TableFactor::Pivot {
                table,
                aggregate_functions,
                value_column,
                value_source,
                default_on_null,
                ..
            } => {
                self.table_factor(table, root, nest);
                for expr in aggregate_functions {
                    self.root(&expr.expr, root, nest);
                }
                for expr in value_column {
                    self.root(expr, root, nest);
                }
                self.pivot_values(value_source, root, nest);
                if let Some(expr) = default_on_null {
                    self.root(expr, root, nest);
                }
            }
            sq::TableFactor::Unpivot {
                table,
                value,
                columns,
                ..
            } => {
                self.table_factor(table, root, nest);
                self.root(value, root, nest);
                for column in columns {
                    self.root(&column.expr, root, nest);
                }
            }
            sq::TableFactor::MatchRecognize {
                table,
                partition_by,
                order_by,
                measures,
                symbols,
                ..
            } => {
                self.table_factor(table, root, nest);
                for expr in partition_by {
                    self.root(expr, root, nest);
                }
                for order in order_by {
                    self.order_by(order, root, nest);
                }
                for measure in measures {
                    self.root(&measure.expr, root, nest);
                }
                for symbol in symbols {
                    self.root(&symbol.definition, root, nest);
                }
            }
            sq::TableFactor::XmlTable {
                namespaces,
                row_expression,
                passing,
                ..
            } => {
                for namespace in namespaces {
                    self.root(&namespace.uri, root, nest);
                }
                self.root(row_expression, root, nest);
                for argument in &passing.arguments {
                    self.root(&argument.expr, root, nest);
                }
            }
            sq::TableFactor::SemanticView {
                dimensions,
                metrics,
                facts,
                where_clause,
                ..
            } => {
                for expr in dimensions.iter().chain(metrics).chain(facts) {
                    self.root(expr, root, nest);
                }
                if let Some(expr) = where_clause {
                    self.root(expr, root, nest);
                }
            }
        }
    }

    fn table_version(&mut self, version: &'a sq::TableVersion, root: usize, nest: usize) {
        match version {
            sq::TableVersion::ForSystemTimeAsOf(expr)
            | sq::TableVersion::TimestampAsOf(expr)
            | sq::TableVersion::VersionAsOf(expr)
            | sq::TableVersion::Function(expr) => self.root(expr, root, nest),
            sq::TableVersion::Changes { changes, at, end } => {
                self.root(changes, root, nest);
                self.root(at, root, nest);
                if let Some(end) = end {
                    self.root(end, root, nest);
                }
            }
        }
    }

    fn sample(&mut self, sample: &'a sq::TableSampleKind, root: usize, nest: usize) {
        let sample = match sample {
            sq::TableSampleKind::BeforeTableAlias(sample)
            | sq::TableSampleKind::AfterTableAlias(sample) => sample,
        };
        if let Some(quantity) = &sample.quantity {
            self.root(&quantity.value, root, nest);
        }
        if let Some(offset) = &sample.offset {
            self.root(offset, root, nest);
        }
    }

    fn pivot_values(&mut self, source: &'a sq::PivotValueSource, root: usize, nest: usize) {
        match source {
            sq::PivotValueSource::List(values) => {
                for value in values {
                    self.root(&value.expr, root, nest);
                }
            }
            sq::PivotValueSource::Any(order) => {
                for order in order {
                    self.order_by(order, root, nest);
                }
            }
            sq::PivotValueSource::Subquery(query) => {
                self.query_at(query, root.saturating_sub(1), nest + 1);
            }
        }
    }

    fn order_kind(&mut self, kind: &'a sq::OrderByKind, root: usize, nest: usize) {
        if let sq::OrderByKind::Expressions(exprs) = kind {
            for expr in exprs {
                self.order_by(expr, root, nest);
            }
        }
    }

    fn order_by(&mut self, order: &'a sq::OrderByExpr, root: usize, nest: usize) {
        self.root(&order.expr, root, nest);
        if let Some(fill) = &order.with_fill {
            for expr in [&fill.from, &fill.to, &fill.step].into_iter().flatten() {
                self.root(expr, root, nest);
            }
        }
    }

    fn limit(&mut self, limit: &'a sq::LimitClause, root: usize, nest: usize) {
        match limit {
            sq::LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if let Some(limit) = limit {
                    self.root(limit, root, nest);
                }
                if let Some(offset) = offset {
                    self.root(&offset.value, root, nest);
                }
                for expr in limit_by {
                    self.root(expr, root, nest);
                }
            }
            sq::LimitClause::OffsetCommaLimit { offset, limit } => {
                self.root(offset, root, nest);
                self.root(limit, root, nest);
            }
        }
    }

    fn pipe(&mut self, pipe: &'a sq::PipeOperator, root: usize, expr_depth: usize, nest: usize) {
        match pipe {
            sq::PipeOperator::Limit { expr, offset } => {
                self.root(expr, root, nest);
                if let Some(offset) = offset {
                    self.root(offset, root, nest);
                }
            }
            sq::PipeOperator::Where { expr } => self.root(expr, root, nest),
            sq::PipeOperator::OrderBy { exprs } => {
                for expr in exprs {
                    self.order_by(expr, root, nest);
                }
            }
            sq::PipeOperator::Select { exprs } | sq::PipeOperator::Extend { exprs } => {
                for expr in exprs {
                    self.select_item(expr, root, nest);
                }
            }
            sq::PipeOperator::Set { assignments } => {
                for assignment in assignments {
                    self.root(&assignment.value, root, nest);
                }
            }
            sq::PipeOperator::Aggregate {
                full_table_exprs,
                group_by_expr,
            } => {
                for expr in full_table_exprs.iter().chain(group_by_expr) {
                    self.root(&expr.expr.expr, root, nest);
                }
            }
            sq::PipeOperator::TableSample { sample } => {
                if let Some(quantity) = &sample.quantity {
                    self.root(&quantity.value, root, nest);
                }
                if let Some(offset) = &sample.offset {
                    self.root(offset, root, nest);
                }
            }
            sq::PipeOperator::Union { queries, .. }
            | sq::PipeOperator::Intersect { queries, .. }
            | sq::PipeOperator::Except { queries, .. } => {
                for query in queries {
                    self.query_at(query, expr_depth, nest + 1);
                }
            }
            sq::PipeOperator::Call { function, .. } => {
                let group = self.groups.len();
                self.groups.push(0);
                self.function(function, root.saturating_sub(1), group, nest);
            }
            sq::PipeOperator::Pivot {
                aggregate_functions,
                value_source,
                ..
            } => {
                for expr in aggregate_functions {
                    self.root(&expr.expr, root, nest);
                }
                self.pivot_values(value_source, root, nest);
            }
            sq::PipeOperator::Join(join) => {
                self.table_factor(&join.relation, root, nest);
                let (asof, constraint) = join_parts(&join.join_operator);
                if let Some(expr) = asof {
                    self.root(expr, root, nest);
                }
                if let Some(sq::JoinConstraint::On(expr)) = constraint {
                    self.root(expr, root, nest);
                }
            }
            sq::PipeOperator::Drop { .. }
            | sq::PipeOperator::As { .. }
            | sq::PipeOperator::Rename { .. }
            | sq::PipeOperator::Unpivot { .. } => {}
        }
    }

    fn window_spec(&mut self, spec: &'a sq::WindowSpec, root: usize, nest: usize) {
        for expr in &spec.partition_by {
            self.root(expr, root, nest);
        }
        for order in &spec.order_by {
            self.order_by(order, root, nest);
        }
        if let Some(frame) = &spec.window_frame {
            self.frame_bound(&frame.start_bound, root, nest);
            if let Some(end) = &frame.end_bound {
                self.frame_bound(end, root, nest);
            }
        }
    }

    fn frame_bound(&mut self, bound: &'a sq::WindowFrameBound, root: usize, nest: usize) {
        if let sq::WindowFrameBound::Preceding(Some(expr))
        | sq::WindowFrameBound::Following(Some(expr)) = bound
        {
            self.root(expr, root, nest);
        }
    }

    fn function_arg_root(&mut self, arg: &'a sq::FunctionArg, root: usize, nest: usize) {
        match arg {
            sq::FunctionArg::Named { arg, .. } | sq::FunctionArg::Unnamed(arg) => {
                if let sq::FunctionArgExpr::Expr(expr) = arg {
                    self.root(expr, root, nest);
                }
            }
            sq::FunctionArg::ExprNamed { name, arg, .. } => {
                self.root(name, root, nest);
                if let sq::FunctionArgExpr::Expr(expr) = arg {
                    self.root(expr, root, nest);
                }
            }
        }
    }

    fn expand(&mut self, expr: &'a SqlExpr, depth: usize, group: usize, nest: usize) {
        let next = depth + 1;
        match expr {
            SqlExpr::Identifier(_)
            | SqlExpr::CompoundIdentifier(_)
            | SqlExpr::Value(_)
            | SqlExpr::TypedString(_)
            | SqlExpr::Wildcard(_)
            | SqlExpr::QualifiedWildcard(_, _)
            | SqlExpr::MatchAgainst { .. } => {}
            SqlExpr::CompoundFieldAccess { root, access_chain } => {
                self.child(root, next, group, nest);
                for access in access_chain {
                    match access {
                        sq::AccessExpr::Dot(expr) => self.child(expr, next, group, nest),
                        sq::AccessExpr::Subscript(subscript) => {
                            self.subscript(subscript, next, group, nest);
                        }
                    }
                }
            }
            SqlExpr::JsonAccess { value, path } => {
                self.child(value, next, group, nest);
                for elem in &path.path {
                    if let sq::JsonPathElem::Bracket { key }
                    | sq::JsonPathElem::ColonBracket { key } = elem
                    {
                        self.child(key, next, group, nest);
                    }
                }
            }
            SqlExpr::IsFalse(expr)
            | SqlExpr::IsNotFalse(expr)
            | SqlExpr::IsTrue(expr)
            | SqlExpr::IsNotTrue(expr)
            | SqlExpr::IsNull(expr)
            | SqlExpr::IsNotNull(expr)
            | SqlExpr::IsUnknown(expr)
            | SqlExpr::IsNotUnknown(expr)
            | SqlExpr::Nested(expr)
            | SqlExpr::OuterJoin(expr)
            | SqlExpr::Prior(expr) => self.child(expr, next, group, nest),
            SqlExpr::IsDistinctFrom(left, right) | SqlExpr::IsNotDistinctFrom(left, right) => {
                self.child(left, next, group, nest);
                self.child(right, next, group, nest);
            }
            SqlExpr::IsNormalized { expr, .. } => self.child(expr, next, group, nest),
            SqlExpr::InList { expr, list, .. } => {
                self.child(expr, next, group, nest);
                for expr in list {
                    self.child(expr, next, group, nest);
                }
            }
            SqlExpr::InSubquery { expr, subquery, .. } => {
                self.child(expr, next, group, nest);
                self.query_at(subquery, depth, nest + 1);
            }
            SqlExpr::InUnnest {
                expr, array_expr, ..
            } => {
                self.child(expr, next, group, nest);
                self.child(array_expr, next, group, nest);
            }
            SqlExpr::Between {
                expr, low, high, ..
            } => {
                self.child(expr, next, group, nest);
                self.child(low, next, group, nest);
                self.child(high, next, group, nest);
            }
            SqlExpr::BinaryOp { left, right, .. } => {
                self.child(left, next, group, nest);
                self.child(right, next, group, nest);
            }
            SqlExpr::Like { expr, pattern, .. }
            | SqlExpr::ILike { expr, pattern, .. }
            | SqlExpr::SimilarTo { expr, pattern, .. }
            | SqlExpr::RLike { expr, pattern, .. } => {
                self.child(expr, next, group, nest);
                self.child(pattern, next, group, nest);
            }
            SqlExpr::AnyOp { left, right, .. } | SqlExpr::AllOp { left, right, .. } => {
                self.child(left, next, group, nest);
                self.child(right, next, group, nest);
            }
            SqlExpr::UnaryOp { expr, .. }
            | SqlExpr::Extract { expr, .. }
            | SqlExpr::Ceil { expr, .. }
            | SqlExpr::Floor { expr, .. }
            | SqlExpr::Collate { expr, .. }
            | SqlExpr::Named { expr, .. }
            | SqlExpr::Prefixed { value: expr, .. } => self.child(expr, next, group, nest),
            SqlExpr::Convert { expr, styles, .. } => {
                self.child(expr, next, group, nest);
                for style in styles {
                    self.child(style, next, group, nest);
                }
            }
            SqlExpr::Cast { expr, .. } => self.child(expr, next, group, nest),
            SqlExpr::AtTimeZone {
                timestamp,
                time_zone,
            } => {
                self.child(timestamp, next, group, nest);
                self.child(time_zone, next, group, nest);
            }
            SqlExpr::Position { expr, r#in } => {
                self.child(expr, next, group, nest);
                self.child(r#in, next, group, nest);
            }
            SqlExpr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                self.child(expr, next, group, nest);
                if let Some(expr) = substring_from {
                    self.child(expr, next, group, nest);
                }
                if let Some(expr) = substring_for {
                    self.child(expr, next, group, nest);
                }
            }
            SqlExpr::Trim {
                expr,
                trim_what,
                trim_characters,
                ..
            } => {
                self.child(expr, next, group, nest);
                if let Some(expr) = trim_what {
                    self.child(expr, next, group, nest);
                }
                if let Some(chars) = trim_characters {
                    for expr in chars {
                        self.child(expr, next, group, nest);
                    }
                }
            }
            SqlExpr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                self.child(expr, next, group, nest);
                self.child(overlay_what, next, group, nest);
                self.child(overlay_from, next, group, nest);
                if let Some(expr) = overlay_for {
                    self.child(expr, next, group, nest);
                }
            }
            SqlExpr::Function(function) => self.function(function, depth, group, nest),
            SqlExpr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    self.child(operand, next, group, nest);
                }
                for when in conditions {
                    self.child(&when.condition, next, group, nest);
                    self.child(&when.result, next, group, nest);
                }
                if let Some(expr) = else_result {
                    self.child(expr, next, group, nest);
                }
            }
            SqlExpr::Exists { subquery, .. } | SqlExpr::Subquery(subquery) => {
                self.query_at(subquery, depth, nest + 1);
            }
            SqlExpr::GroupingSets(groups) | SqlExpr::Cube(groups) | SqlExpr::Rollup(groups) => {
                for group_exprs in groups {
                    for expr in group_exprs {
                        self.child(expr, next, group, nest);
                    }
                }
            }
            SqlExpr::Tuple(exprs) | SqlExpr::Array(sq::Array { elem: exprs, .. }) => {
                for expr in exprs {
                    self.child(expr, next, group, nest);
                }
            }
            SqlExpr::Struct { values, .. } => {
                for expr in values {
                    self.child(expr, next, group, nest);
                }
            }
            SqlExpr::Dictionary(fields) => {
                for field in fields {
                    self.child(&field.value, next, group, nest);
                }
            }
            SqlExpr::Map(map) => {
                for entry in &map.entries {
                    self.child(&entry.key, next, group, nest);
                    self.child(&entry.value, next, group, nest);
                }
            }
            SqlExpr::Interval(interval) => self.child(&interval.value, next, group, nest),
            SqlExpr::Lambda(lambda) => self.child(&lambda.body, next, group, nest),
            SqlExpr::MemberOf(member) => {
                self.child(&member.value, next, group, nest);
                self.child(&member.array, next, group, nest);
            }
        }
    }

    fn subscript(&mut self, subscript: &'a sq::Subscript, depth: usize, group: usize, nest: usize) {
        match subscript {
            sq::Subscript::Index { index } => self.child(index, depth, group, nest),
            sq::Subscript::Slice {
                lower_bound,
                upper_bound,
                stride,
            } => {
                for expr in [lower_bound.as_ref(), upper_bound.as_ref(), stride.as_ref()]
                    .into_iter()
                    .flatten()
                {
                    self.child(expr, depth, group, nest);
                }
            }
        }
    }

    fn function(&mut self, function: &'a sq::Function, depth: usize, group: usize, nest: usize) {
        let next = depth + 1;
        self.function_arguments(&function.parameters, depth, group, nest);
        self.function_arguments(&function.args, depth, group, nest);
        if let Some(filter) = &function.filter {
            self.child(filter, next, group, nest);
        }
        for order in &function.within_group {
            self.order_by(order, next, nest);
        }
        if let Some(over) = &function.over
            && let sq::WindowType::WindowSpec(spec) = over
        {
            self.window_spec(spec, next, nest);
        }
    }

    fn function_arguments(
        &mut self,
        args: &'a sq::FunctionArguments,
        depth: usize,
        group: usize,
        nest: usize,
    ) {
        match args {
            sq::FunctionArguments::None => {}
            sq::FunctionArguments::Subquery(query) => self.query_at(query, depth, nest + 1),
            sq::FunctionArguments::List(list) => {
                for arg in &list.args {
                    self.function_arg_child(arg, depth + 1, group, nest);
                }
                for clause in &list.clauses {
                    self.clause(clause, depth, group, nest);
                }
            }
        }
    }

    fn function_arg_child(
        &mut self,
        arg: &'a sq::FunctionArg,
        depth: usize,
        group: usize,
        nest: usize,
    ) {
        match arg {
            sq::FunctionArg::Named { arg, .. } | sq::FunctionArg::Unnamed(arg) => {
                if let sq::FunctionArgExpr::Expr(expr) = arg {
                    self.child(expr, depth, group, nest);
                }
            }
            sq::FunctionArg::ExprNamed { name, arg, .. } => {
                self.child(name, depth, group, nest);
                if let sq::FunctionArgExpr::Expr(expr) = arg {
                    self.child(expr, depth, group, nest);
                }
            }
        }
    }

    fn clause(
        &mut self,
        clause: &'a sq::FunctionArgumentClause,
        depth: usize,
        group: usize,
        nest: usize,
    ) {
        let next = depth + 1;
        match clause {
            sq::FunctionArgumentClause::IgnoreOrRespectNulls(_)
            | sq::FunctionArgumentClause::Separator(_)
            | sq::FunctionArgumentClause::JsonNullClause(_)
            | sq::FunctionArgumentClause::JsonReturningClause(_) => {}
            sq::FunctionArgumentClause::OrderBy(order) => {
                for order in order {
                    self.order_by(order, next, nest);
                }
            }
            sq::FunctionArgumentClause::Limit(expr) => self.root(expr, next, nest),
            sq::FunctionArgumentClause::OnOverflow(overflow) => {
                if let sq::ListAggOnOverflow::Truncate {
                    filler: Some(expr), ..
                } = overflow
                {
                    self.child(expr, next, group, nest);
                }
            }
            sq::FunctionArgumentClause::Having(bound) => self.child(&bound.1, next, group, nest),
        }
    }

    fn json_path_roots(&mut self, path: &'a sq::JsonPath, root: usize, nest: usize) {
        for elem in &path.path {
            if let sq::JsonPathElem::Bracket { key } | sq::JsonPathElem::ColonBracket { key } = elem
            {
                self.root(key, root, nest);
            }
        }
    }
}

fn join_parts(op: &sq::JoinOperator) -> (Option<&SqlExpr>, Option<&sq::JoinConstraint>) {
    match op {
        sq::JoinOperator::AsOf {
            match_condition,
            constraint,
        } => (Some(match_condition), Some(constraint)),
        sq::JoinOperator::Join(constraint)
        | sq::JoinOperator::Inner(constraint)
        | sq::JoinOperator::Left(constraint)
        | sq::JoinOperator::LeftOuter(constraint)
        | sq::JoinOperator::Right(constraint)
        | sq::JoinOperator::RightOuter(constraint)
        | sq::JoinOperator::FullOuter(constraint)
        | sq::JoinOperator::CrossJoin(constraint)
        | sq::JoinOperator::Semi(constraint)
        | sq::JoinOperator::LeftSemi(constraint)
        | sq::JoinOperator::RightSemi(constraint)
        | sq::JoinOperator::Anti(constraint)
        | sq::JoinOperator::LeftAnti(constraint)
        | sq::JoinOperator::RightAnti(constraint)
        | sq::JoinOperator::StraightJoin(constraint) => (None, Some(constraint)),
        sq::JoinOperator::CrossApply
        | sq::JoinOperator::OuterApply
        | sq::JoinOperator::ArrayJoin
        | sq::JoinOperator::LeftArrayJoin
        | sq::JoinOperator::InnerArrayJoin => (None, None),
    }
}
