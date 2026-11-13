//! What a statement reaches, from sqlparser's AST (roadmap 6.7).
//!
//! nuthatch asks DuckDB's parser (`json_serialize_sql`) which tables and table functions a `/sql`
//! statement touches, refuses anything unrecognised, and bounds its integrity sweep by the tables
//! reached (`reject_unknown_table_refs`, `walk_table_refs`). This answers the same questions by the
//! same rules, checked against that walker by `burrmill-bench reach-parity`:
//!
//! - a table function must be `generate_series`, `range` or `unnest`;
//! - a base table must be named `[A-Za-z0-9_]+`, which is what stops a quoted path in table position
//!   (DuckDB's replacement scan) reading a file;
//! - a schema other than `main` marks the statement as surveying the catalogue;
//! - the tables reached are returned lowercased, CTE names included, as DuckDB's walk includes them.
//!
//! It is stricter where nuthatch's is open. An unparseable statement is an error, not a pass; so is
//! more than one statement, and anything that is not a query.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, Query, Statement, TableFactor, Visitor, VisitorMut,
};
use sqlparser::parser::Parser;

use crate::error::{BurrmillError, Result};

/// nuthatch's `ALLOWED_TABLE_FNS`.
pub const ALLOWED_TABLE_FNS: &[&str] = &["generate_series", "range", "unnest"];

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Reach {
    /// Base tables and CTE names the statement names, lowercased.
    pub tables: BTreeSet<String>,
    /// It names a schema other than `main`, so it asks about the catalogue.
    pub surveys: bool,
}

/// What a nest's statement may name; the tab's is the one table it was given.
const NEST: &str = "this nest's tables and views";

fn refused(why: String) -> BurrmillError {
    BurrmillError::NotAllowed(why)
}

/// A refusal of what the statement reaches, with what the surface serves instead.
fn unreachable(why: String, serves: &str) -> BurrmillError {
    BurrmillError::NotAllowed(format!("{why} - the SQL surface serves {serves} only"))
}

/// The statements `sql` holds. The engine's own dialect where there is one, so a form it refuses by
/// name is refused by that name here and not as a syntax error.
#[doc(hidden)]
pub fn parse(sql: &str) -> std::result::Result<Vec<Statement>, sqlparser::parser::ParserError> {
    #[cfg(feature = "datafusion")]
    return crate::df::refuse_asof(Parser::parse_sql(&crate::df::duck_dialect(), sql));
    #[cfg(not(feature = "datafusion"))]
    Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, sql)
}

fn parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => i.value.clone(),
            other => other.to_string(),
        })
        .collect()
}

struct Walk {
    reach: Reach,
    bad: Option<String>,
}

impl Walk {
    fn table_function(&mut self, name: &ObjectName) {
        let f = parts(name).last().cloned().unwrap_or_default();
        let lower = f.to_ascii_lowercase();
        if !ALLOWED_TABLE_FNS.contains(&lower.as_str()) {
            self.bad = Some(format!("table function `{f}` is not permitted here"));
        }
    }
}

impl Visitor for Walk {
    type Break = ();

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        if self.bad.is_some() {
            return ControlFlow::Break(());
        }
        match t {
            TableFactor::Table {
                name,
                args: Some(_),
                ..
            } => self.table_function(name),
            TableFactor::Function { name, .. } => self.table_function(name),
            TableFactor::Table { name, .. } => {
                let p = parts(name);
                let table = p.last().cloned().unwrap_or_default();
                if table.is_empty() || !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                {
                    self.bad = Some(format!(
                        "`{table}` is not a table name - a quoted path in table position reads a file"
                    ));
                } else if hidden_table(&table) {
                    // The parts a registration is built from. The public view
                    // already carries the window (#10).
                    self.bad = Some(format!("`{table}` is not a table this surface serves"));
                } else {
                    self.reach.tables.insert(table.to_ascii_lowercase());
                }
                if p.len() >= 2 && !p[p.len() - 2].eq_ignore_ascii_case("main") {
                    self.reach.surveys = true;
                }
            }
            TableFactor::UNNEST { .. }
            | TableFactor::Derived { .. }
            | TableFactor::NestedJoin { .. }
            | TableFactor::Pivot { .. }
            | TableFactor::Unpivot { .. } => {}
            other => {
                self.bad = Some(format!("`{other}` is not a table this surface serves"));
            }
        }
        if self.bad.is_some() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

/// `__raw`, `__hot` and `__union` are how a registration is built, not names a statement can use.
pub(crate) fn hidden_table(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.ends_with("__raw") || lower.ends_with("__hot") || lower.ends_with("__union")
}

/// The first `__raw`, `__hot` or `__union` any statement in `sql` names, `EXPLAIN` included.
/// Separate from [`reach`], which stops at its first fault: a quoted CTE name ahead of a hidden
/// one was that fault, and the hidden name was never looked at (#189).
#[cfg(feature = "datafusion")]
pub(crate) fn hidden_name(sql: &str) -> Option<String> {
    struct Find(Option<String>);
    impl Visitor for Find {
        type Break = ();

        fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table { name, .. } = t
                && let Some(table) = parts(name).last()
                && hidden_table(table)
            {
                self.0 = Some(table.clone());
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let expanded = crate::listcomp::before_parse(sql);
    let stmts = parse(&expanded).ok()?;
    let mut find = Find(None);
    for stmt in &stmts {
        let _ = crate::walk::walk(stmt, &mut find);
        if find.0.is_some() {
            break;
        }
    }
    find.0
}

/// The tables `sql` reaches, or why it is refused.
pub fn reach(sql: &str) -> Result<Reach> {
    reach_serving(sql, NEST)
}

fn reach_serving(sql: &str, serves: &str) -> Result<Reach> {
    // The same rewrite `Engine` parses with, so a comprehension does not hide the tables in it.
    let expanded = crate::listcomp::before_parse(sql);
    let sql = expanded.as_str();
    let stmts = parse(sql).map_err(|e| BurrmillError::Parse(format!("Parser Error: {e}")))?;
    let [stmt] = stmts.as_slice() else {
        return Err(refused(format!(
            "{} statements where one is allowed",
            stmts.len()
        )));
    };
    if !matches!(stmt, Statement::Query(_)) {
        return Err(refused("only SELECT/WITH queries are allowed".into()));
    }
    let mut w = Walk {
        reach: Reach::default(),
        bad: None,
    };
    let _ = crate::walk::walk(stmt, &mut w);
    match w.bad {
        Some(why) => Err(unreachable(why, serves)),
        None => Ok(w.reach),
    }
}

/// The base tables (CTE names included) and table functions a statement names, lowercased, with no
/// policy applied: what a host asks of DuckDB's `json_serialize_sql` for `BASE_TABLE` and
/// `TABLE_FUNCTION` nodes. `None` for an unparseable statement or anything but one query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Refs {
    pub tables: BTreeSet<String>,
    pub functions: BTreeSet<String>,
}

struct Collect(Refs);

impl Visitor for Collect {
    type Break = ();

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        let last = |n: &ObjectName| {
            parts(n)
                .last()
                .cloned()
                .unwrap_or_default()
                .to_ascii_lowercase()
        };
        match t {
            TableFactor::Table {
                name,
                args: Some(_),
                ..
            }
            | TableFactor::Function { name, .. } => {
                self.0.functions.insert(last(name));
            }
            TableFactor::Table { name, .. } => {
                self.0.tables.insert(last(name));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

pub fn refs(sql: &str) -> Option<Refs> {
    let expanded = crate::listcomp::before_parse(sql);
    let stmts = parse(&expanded).ok()?;
    let [stmt @ Statement::Query(_)] = stmts.as_slice() else {
        return None;
    };
    let mut c = Collect(Refs::default());
    let _ = crate::walk::walk(stmt, &mut c);
    Some(c.0)
}

/// The scalar, aggregate and window functions a statement calls, each as the parts of its name,
/// lowercased: `main.upper(x)` is `["main", "upper"]`. Table functions are [`refs`]'s. A list
/// comprehension counts as the calls it is rewritten into. `None` for anything but one parseable
/// statement, of whatever kind. Operators the engine runs as functions (`::`, `||`, `->>`,
/// `EXTRACT`, `TRIM`, `[i]` and the like) are not calls here, so withholding one of those needs
/// more than this set.
pub fn calls(sql: &str) -> Option<BTreeSet<Vec<String>>> {
    struct Calls(BTreeSet<Vec<String>>);
    impl Visitor for Calls {
        type Break = ();
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            if let Expr::Function(f) = e {
                let name = parts(&f.name)
                    .iter()
                    .map(|p| p.to_ascii_lowercase())
                    .collect();
                self.0.insert(name);
            }
            ControlFlow::Continue(())
        }
    }
    let expanded = crate::listcomp::before_parse(sql);
    let stmts = parse(&expanded).ok()?;
    let [stmt] = stmts.as_slice() else {
        return None;
    };
    let mut c = Calls(BTreeSet::new());
    let _ = crate::walk::walk(stmt, &mut c);
    Some(c.0)
}

/// The physical tables a statement reads, lowercased: names bound by a `WITH` in scope are not
/// tables. A CTE's definition sees its earlier siblings, and itself only when recursive, so
/// `WITH t AS (SELECT * FROM t)` reads the table `t`; a qualified name is always a table. This is
/// nuthatch's `walk_base_table_refs` over DuckDB's AST. `None` as for [`refs`].
pub fn base_tables(sql: &str) -> Option<BTreeSet<String>> {
    let expanded = crate::listcomp::before_parse(sql);
    let stmts = parse(&expanded).ok()?;
    let [stmt @ Statement::Query(_)] = stmts.as_slice() else {
        return None;
    };
    let mut w = Scoped {
        scopes: Vec::new(),
        cte: HashMap::new(),
        out: BTreeSet::new(),
    };
    let _ = crate::walk::walk(stmt, &mut w);
    Some(w.out)
}

/// One query over the uploaded tables in `allowed`. A `WITH` name is not an upload:
/// `WITH other AS (SELECT * FROM t) SELECT * FROM other` reaches `t`, and
/// `WITH t AS (SELECT * FROM other)` reaches `other`. A schema other than `main` is refused.
pub fn reach_uploads(sql: &str, allowed: &BTreeSet<String>) -> Result<()> {
    const UPLOADS: &str = "the uploaded tables";
    let got = reach_serving(sql, UPLOADS)?;
    if got.surveys {
        return Err(unreachable(
            "a schema other than main surveys the catalogue".into(),
            UPLOADS,
        ));
    }
    let Some(bases) = base_tables(sql) else {
        return Err(refused("only SELECT/WITH queries are allowed".into()));
    };
    let allow: BTreeSet<String> = allowed.iter().map(|n| n.to_ascii_lowercase()).collect();
    for name in &bases {
        if hidden_table(name) || !allow.contains(name) {
            return Err(unreachable(
                format!("`{name}` is not an uploaded table"),
                UPLOADS,
            ));
        }
    }
    Ok(())
}

struct Scoped {
    scopes: Vec<BTreeSet<String>>,
    /// What each CTE's own query sees, keyed by its address, registered by the query holding it.
    cte: HashMap<*const Query, BTreeSet<String>>,
    out: BTreeSet<String>,
}

impl Visitor for Scoped {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        let outer = self
            .cte
            .remove(&(q as *const Query))
            .unwrap_or_else(|| self.scopes.last().cloned().unwrap_or_default());
        let mut body = outer.clone();
        if let Some(with) = &q.with {
            let mut seen = outer.clone();
            for c in &with.cte_tables {
                let name = c.alias.name.value.to_ascii_lowercase();
                let mut sees = seen.clone();
                if with.recursive {
                    sees.insert(name.clone());
                }
                self.cte.insert(&*c.query as *const Query, sees);
                seen.insert(name);
            }
            body = seen;
        }
        self.scopes.push(body);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table {
            name, args: None, ..
        } = t
        {
            let p = parts(name);
            let table = p.last().cloned().unwrap_or_default().to_ascii_lowercase();
            let bound = self.scopes.last().is_some_and(|s| s.contains(&table));
            if p.len() > 1 || !bound {
                self.out.insert(table);
            }
        }
        ControlFlow::Continue(())
    }
}

/// The statement as a key for a host's reuse (nuthatch's graft): sqlparser's AST printed back, so
/// whitespace and comments do not count, with table aliases renamed `__a0`, `__a1`, … in the order
/// they are declared, and every qualified column reference through one renamed to match. Only
/// declared aliases are touched, so a real table name is never renamed into another. `None` for
/// anything but one parseable statement.
pub fn canonical(sql: &str) -> Option<String> {
    let expanded = crate::listcomp::before_parse(sql);
    let mut stmts = parse(&expanded).ok()?;
    let [stmt] = stmts.as_mut_slice() else {
        return None;
    };
    let mut found = Aliases(Vec::new());
    let _ = crate::walk::walk(&*stmt, &mut found);
    let _ = crate::walk::walk_mut(stmt, &mut Rename(found.0));
    Some(stmt.to_string())
}

fn table_alias(t: &TableFactor) -> Option<&sqlparser::ast::TableAlias> {
    match t {
        TableFactor::Table { alias, .. }
        | TableFactor::Derived { alias, .. }
        | TableFactor::Function { alias, .. }
        | TableFactor::UNNEST { alias, .. } => alias.as_ref(),
        _ => None,
    }
}

struct Aliases(Vec<String>);

impl Visitor for Aliases {
    type Break = ();
    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        if let Some(a) = table_alias(t)
            && !a.name.value.is_empty()
            && !self.0.contains(&a.name.value)
        {
            self.0.push(a.name.value.clone());
        }
        ControlFlow::Continue(())
    }
}

struct Rename(Vec<String>);

impl Rename {
    fn to(&self, name: &str) -> Option<String> {
        self.0
            .iter()
            .position(|a| a == name)
            .map(|i| format!("__a{i}"))
    }
}

impl VisitorMut for Rename {
    type Break = ();
    fn pre_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<()> {
        let alias = match t {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::UNNEST { alias, .. } => alias.as_mut(),
            _ => None,
        };
        if let Some(a) = alias
            && let Some(new) = self.to(&a.name.value)
        {
            a.name = Ident::new(new);
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if let Expr::CompoundIdentifier(parts) = e
            && parts.len() > 1
            && let Some(new) = self.to(&parts[0].value)
        {
            parts[0] = Ident::new(new);
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables(sql: &str) -> Vec<String> {
        reach(sql).unwrap().tables.into_iter().collect()
    }

    #[test]
    fn reaches_through_joins_ctes_and_subqueries() {
        assert_eq!(
            tables(
                "WITH c AS (SELECT * FROM Transfer) SELECT * FROM c JOIN label l ON true \
                 WHERE EXISTS (SELECT 1 FROM mint) AND x IN (SELECT y FROM burn)"
            ),
            vec!["burn", "c", "label", "mint", "transfer"]
        );
    }

    #[test]
    fn refuses_files_and_foreign_table_functions() {
        for sql in [
            "SELECT * FROM read_csv('/etc/passwd')",
            "SELECT * FROM \"read_csv\"('/etc/passwd')",
            "SELECT * FROM '/etc/passwd'",
            "SELECT * FROM duckdb_tables()",
            "SELECT 1; SELECT 2",
            "COPY (SELECT 1) TO '/tmp/x'",
        ] {
            assert!(reach(sql).is_err(), "{sql}");
        }
        assert!(reach("SELECT * FROM range(3)").is_ok());
    }

    #[test]
    fn a_list_comprehension_does_not_hide_its_table() {
        assert_eq!(
            tables("SELECT [lower(name) FOR c IN string_split(name, '')][1] FROM label"),
            vec!["label"]
        );
    }

    #[test]
    fn calls_name_every_function_a_statement_calls() {
        let c = |sql: &str| calls(sql).unwrap().into_iter().collect::<Vec<_>>();
        let v = |s: &[&str]| s.iter().map(|p| p.to_string()).collect::<Vec<_>>();
        assert_eq!(
            c(
                "SELECT upper(x), \"VERSION\"(), main.Lower(y) FROM t, range(3) \
               WHERE x IN (SELECT max(z) OVER (PARTITION BY w) FROM u)"
            ),
            vec![
                v(&["main", "lower"]),
                v(&["max"]),
                v(&["upper"]),
                v(&["version"])
            ]
        );
        assert!(c("SELECT list_transform([1], y -> version())").contains(&v(&["version"])));
        assert!(c("SELECT [version() FOR x IN [1]]").contains(&v(&["version"])));
        assert!(c("EXPLAIN SELECT version()").contains(&v(&["version"])));
        assert!(calls("SELECT 1; SELECT 2").is_none());
        assert!(calls("SELEC 1").is_none());
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn calls_read_engine_only_syntax() {
        let c = calls("SELECT last_value(a IGNORE NULLS) OVER () FROM t WHERE a ^@ 'x'").unwrap();
        assert_eq!(
            c.into_iter().collect::<Vec<_>>(),
            vec![
                vec!["last_value".to_string()],
                vec!["starts_with".to_string()]
            ]
        );
    }

    #[test]
    fn refs_report_without_refusing() {
        let r =
            refs("WITH c AS (SELECT * FROM T) SELECT * FROM c, read_csv('/x'), range(3)").unwrap();
        assert_eq!(r.tables.into_iter().collect::<Vec<_>>(), vec!["c", "t"]);
        assert_eq!(
            r.functions.into_iter().collect::<Vec<_>>(),
            vec!["range", "read_csv"]
        );
        assert!(refs("SELECT 1; SELECT 2").is_none());
        assert!(refs("SELEC 1").is_none());
    }

    #[test]
    fn base_tables_respect_cte_scope() {
        let b = |sql: &str| base_tables(sql).unwrap().into_iter().collect::<Vec<_>>();
        assert_eq!(b("WITH c AS (SELECT * FROM t) SELECT * FROM c"), vec!["t"]);
        assert_eq!(b("WITH t AS (SELECT * FROM t) SELECT * FROM t"), vec!["t"]);
        assert_eq!(
            b("WITH a AS (SELECT * FROM b), b AS (SELECT * FROM x) SELECT * FROM a"),
            vec!["b", "x"]
        );
        assert_eq!(
            b("WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT * FROM r) SELECT * FROM r"),
            Vec::<String>::new()
        );
        assert_eq!(
            b("WITH c AS (SELECT 1) SELECT * FROM main.c, (SELECT * FROM c) s"),
            vec!["c"]
        );
        assert_eq!(
            b("SELECT * FROM (WITH c AS (SELECT 1) SELECT * FROM c) s, c"),
            vec!["c"]
        );
    }

    #[test]
    fn canonical_ignores_layout_and_alias_names_only() {
        let c = |sql: &str| canonical(sql).unwrap();
        assert_eq!(
            c("SELECT a.x FROM t a -- one\nJOIN u b ON a.k = b.k"),
            c("select   a2.x from t a2 join u b2 on a2.k = b2.k")
        );
        assert_ne!(c("SELECT t.x FROM t"), c("SELECT u.x FROM u"));
        assert_ne!(c("SELECT x FROM t"), c("SELECT x FROM t WHERE true"));
        assert_ne!(c("SELECT 5/2"), c("SELECT 5/2.0"));
        assert_ne!(c("SELECT x FROM t"), c("SELECT DISTINCT x FROM t"));
        // A real table name that happens to equal another query's alias is not renamed.
        assert!(c("SELECT t.x FROM t").contains("t.x"));
        assert!(canonical("SELEC 1").is_none());
    }

    #[test]
    fn the_engine_names_this_build() {
        let (name, hash) = crate::ENGINE.split_once('+').unwrap();
        assert!(name.starts_with("burrmill "));
        assert!(hash.len() == 16 && hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn uploads_allow_the_named_tables_and_not_cte_shadows() {
        use std::collections::BTreeSet;
        let allow = BTreeSet::from(["t".to_string()]);
        assert!(reach_uploads("SELECT 1", &allow).is_ok());
        assert!(reach_uploads("SELECT * FROM t", &allow).is_ok());
        assert!(
            reach_uploads(
                "WITH other AS (SELECT * FROM t) SELECT * FROM other",
                &allow
            )
            .is_ok()
        );
        assert!(reach_uploads("WITH t AS (SELECT * FROM other) SELECT * FROM t", &allow).is_err());
        assert!(reach_uploads("SELECT * FROM other", &allow).is_err());
        assert!(reach_uploads("SELECT * FROM information_schema.tables", &allow).is_err());
        assert!(reach_uploads("SELECT * FROM t__raw", &allow).is_err());
        assert!(reach_uploads("COPY (SELECT 1) TO '/tmp/x'", &allow).is_err());
    }

    #[test]
    fn only_a_reachability_refusal_names_the_surface() {
        let say = |e: BurrmillError| e.to_string();
        let count = say(reach("SELECT 1; SELECT 2").unwrap_err());
        assert!(
            count.contains("2 statements where one is allowed"),
            "{count}"
        );
        assert!(!count.contains("serves"), "{count}");
        let file = say(reach("SELECT * FROM read_csv('x')").unwrap_err());
        assert!(
            file.ends_with("serves this nest's tables and views only"),
            "{file}"
        );
        let allow = std::collections::BTreeSet::from(["t".to_string()]);
        let other = say(reach_uploads("SELECT * FROM u", &allow).unwrap_err());
        assert!(other.contains("`u` is not an uploaded table"), "{other}");
        assert!(
            other.ends_with("serves the uploaded tables only"),
            "{other}"
        );
    }

    /// Engine-only syntax, one statement each. `reach` must read whatever the engine reads, or an
    /// embedder that lets a parse failure through would run a statement the walk never saw.
    #[cfg(feature = "datafusion")]
    const ENGINE_ONLY: &[&str] = &[
        "SELECT 'a' ^@ (SELECT max(a) FROM e__raw)",
        "SELECT last_value(a IGNORE NULLS) OVER () AS x FROM e__raw",
        "FROM e__raw SELECT a",
        "FROM e__raw",
        "SELECT list_transform([1, 2], x -> x + (SELECT max(a) FROM e__raw))",
        "SELECT list_transform([1, 2], lambda x: x + (SELECT max(a) FROM e__raw))",
        "SELECT [x * 2 FOR x IN [1, 2, 3] IF x > 1] FROM e__raw",
        "SELECT {'a': 1, 'b': (SELECT max(a) FROM e__raw)}",
        "SELECT MAP {'a': 1}, a FROM e__raw",
        "SELECT * FROM e__raw POSITIONAL JOIN t",
        "SELECT * FROM e__raw ASOF JOIN t USING (k)",
        "SELECT * FROM e__raw ASOF JOIN t ON e__raw.k >= t.k",
        "SELECT * FROM e__raw PIVOT (sum(a) FOR k IN ('x'))",
        "SELECT * FROM e__raw UNPIVOT (v FOR k IN (a))",
        "SELECT a FROM e__raw QUALIFY row_number() OVER () = 1",
        "SELECT * EXCLUDE (a) FROM e__raw",
        "SELECT a IS DISTINCT FROM b FROM e__raw",
        "SELECT a // b FROM e__raw",
        "SELECT CAST(a AS UBIGINT) FROM e__raw",
        "SELECT a FROM e__raw WHERE a GLOB 'x*'",
    ];

    #[cfg(feature = "datafusion")]
    #[test]
    fn reach_parses_with_the_engines_dialect_so_what_the_engine_reads_is_walked() {
        let mut read = 0;
        for sql in ENGINE_ONLY {
            if !crate::df::engine_parses(sql) {
                continue;
            }
            read += 1;
            let got = reach(sql);
            assert!(
                !matches!(got, Err(BurrmillError::Parse(_))),
                "{sql}: {got:?}"
            );
            assert!(hidden_name(sql).is_some(), "{sql}");
            assert!(base_tables(sql).is_some(), "{sql}");
            assert!(refs(sql).is_some(), "{sql}");
            assert!(canonical(sql).is_some(), "{sql}");
            assert!(calls(sql).is_some(), "{sql}");
        }
        assert!(
            read >= 15,
            "the corpus stopped being read by the engine: {read}"
        );
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn a_hidden_table_behind_engine_only_syntax_is_refused() {
        for sql in [
            "SELECT 'a' ^@ (SELECT max(a) FROM e__raw)",
            "SELECT last_value(a IGNORE NULLS) OVER () AS x FROM e__raw",
        ] {
            let e = reach(sql).unwrap_err();
            assert!(matches!(e, BurrmillError::NotAllowed(_)), "{sql}: {e:?}");
            assert!(
                e.to_string()
                    .contains("`e__raw` is not a table this surface serves"),
                "{e}"
            );
        }
        let allow = std::collections::BTreeSet::from(["t".to_string()]);
        assert!(reach_uploads("SELECT a ^@ 'x' FROM t", &allow).is_ok());
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn a_form_the_engine_refuses_by_name_is_refused_by_that_name() {
        let e = reach("SELECT a FROM t WHERE a GLOB 'S*'").unwrap_err();
        assert!(matches!(e, BurrmillError::Parse(_)), "{e:?}");
        assert!(e.to_string().contains("GLOB is not supported here"), "{e}");
    }

    #[test]
    fn a_schema_other_than_main_surveys() {
        assert!(
            reach("SELECT * FROM information_schema.tables")
                .unwrap()
                .surveys
        );
        assert!(!reach("SELECT * FROM main.t").unwrap().surveys);
    }
}
