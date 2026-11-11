//! DataFusion's error text, restated in DuckDB's words (roadmap 6.4).
//!
//! nuthatch attaches its hints by matching DuckDB's phrasing (`sql_errors.rs`). Each message this
//! recognises gets DuckDB's first line, with DataFusion's own text kept below it, so a hint keys the
//! same and nothing the engine said is lost. Checked class by class against DuckDB by
//! `burrmill-bench error-parity`.

/// DuckDB's name for an Arrow type as DataFusion prints it.
pub(super) fn duck_type(t: &str) -> String {
    let t = t.trim();
    let named = match t {
        "Utf8" | "Utf8View" | "LargeUtf8" => "VARCHAR",
        "Boolean" => "BOOLEAN",
        "Int8" => "TINYINT",
        "Int16" => "SMALLINT",
        "Int32" => "INTEGER",
        "Int64" => "BIGINT",
        "UInt8" => "UTINYINT",
        "UInt16" => "USMALLINT",
        "UInt32" => "UINTEGER",
        "UInt64" => "UBIGINT",
        "Float32" => "FLOAT",
        "Float64" => "DOUBLE",
        "Date32" => "DATE",
        "Binary" | "BinaryView" | "LargeBinary" => "BLOB",
        _ if t.starts_with("Timestamp(") => "TIMESTAMP",
        _ => {
            if let Some(ps) = t
                .strip_prefix("Decimal128(")
                .and_then(|r| r.strip_suffix(')'))
            {
                return format!("DECIMAL({})", ps.replace(' ', ""));
            }
            return t.to_string();
        }
    };
    named.to_string()
}

fn after<'a>(s: &'a str, marker: &str) -> Option<&'a str> {
    s.find(marker).map(|i| &s[i + marker.len()..])
}

/// The parts of the dotted name `quoted_flat_name` printed at the head of `s`. A part needing quotes is
/// quoted with `""` escapes, so a `.` inside one, or the sentence after the name, is not a separator.
fn identifiers(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut chars = s.chars().peekable();
    loop {
        let mut part = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            while let Some(c) = chars.next() {
                if c == '"' {
                    if chars.peek() != Some(&'"') {
                        break;
                    }
                    chars.next();
                }
                part.push(c);
            }
        } else {
            while let Some(&c) = chars.peek() {
                if !(c.is_alphanumeric() || c == '_') {
                    break;
                }
                part.push(c);
                chars.next();
            }
            if part.is_empty() {
                break;
            }
        }
        parts.push(part);
        let mut ahead = chars.clone();
        match (ahead.next(), ahead.next()) {
            (Some('.'), Some(c)) if c == '"' || c.is_alphanumeric() || c == '_' => {
                chars.next();
            }
            _ => break,
        }
    }
    parts
}

/// DuckDB's first line for a DataFusion error, if it is one nuthatch keys a hint on.
pub fn duckdb_phrase(msg: &str) -> Option<String> {
    if let Some(rest) = after(msg, "no table ") {
        let name = rest.split_whitespace().next()?;
        return Some(format!(
            "Catalog Error: Table with name {name} does not exist!"
        ));
    }
    if let Some(rest) = after(msg, "No field named ") {
        let parts = identifiers(rest);
        return Some(match parts.as_slice() {
            [.., table, col] => {
                format!("Binder Error: Table \"{table}\" does not have a column named \"{col}\"")
            }
            [col] => format!("Binder Error: Referenced column \"{col}\" not found in FROM clause!"),
            [] => return None,
        });
    }
    if msg.contains("For SELECT DISTINCT, ORDER BY expressions") {
        return Some(
            "SELECT DISTINCT ordered by an expression it does not select is not supported here"
                .into(),
        );
    }
    // Arrives under "Optimizer rule ... failed", which says nothing on its own.
    if let Some(rest) = after(msg, "Unsupported CAST from ") {
        let (from, to) = rest.split_once(" to ")?;
        let from = from.split('(').next().unwrap_or(from);
        let to = to.lines().next().unwrap_or(to);
        return Some(format!(
            "CAST from {from} to {} is not supported here",
            duck_type(to)
        ));
    }
    if msg.contains("ParserError(") {
        if let Some(why) = after(msg, "ParserError(\"")
            .and_then(|r| r.split('"').next())
            .filter(|w| w.contains(" is not supported here"))
        {
            return Some(format!("Parser Error: {why}"));
        }
        let found = after(msg, "found: ").map(|r| {
            r.split(" at Line")
                .next()
                .unwrap_or(r)
                .trim_end_matches(['"', ')'])
        });
        return Some(match found {
            Some("EOF") | None => "Parser Error: syntax error at end of input".into(),
            Some(tok) => format!("Parser Error: syntax error at or near \"{tok}\""),
        });
    }
    if let Some(rest) = after(msg, "Failed to coerce then (") {
        let (then, rest) = rest.split_once(") and else (")?;
        let (els, _) = rest.split_once(')')?;
        return Some(format!(
            "Binder Error: Cannot mix values of type {} and {} in CASE expression - an explicit \
             cast is required",
            duck_type(els),
            duck_type(then)
        ));
    }
    if let Some(rest) = after(
        msg,
        "No function matches the given name and argument types '",
    ) {
        let (call, _) = rest.split_once("'")?;
        let (func, args) = call.split_once('(')?;
        let args: Vec<String> = args
            .trim_end_matches(')')
            .split(',')
            .map(duck_type)
            .collect();
        if func.eq_ignore_ascii_case("coalesce")
            && args.iter().any(|a| a == "VARCHAR")
            && args.iter().any(|a| a == "BOOLEAN")
        {
            return Some(
                "Binder Error: Cannot mix values of type VARCHAR and BOOLEAN in COALESCE \
                 operator - an explicit cast is required"
                    .into(),
            );
        }
        return Some(no_function(func, &args));
    }
    if let Some(rest) = after(msg, "Function '") {
        let (func, rest) = rest.split_once('\'')?;
        if rest.starts_with(" failed to match any signature") {
            let ty = after(rest, "(DataType: ")?.split(')').next()?;
            return Some(no_function(func, &[duck_type(ty)]));
        }
    }
    if let Some(rest) = after(msg, "Resources exhausted: ") {
        let first = rest.lines().next().unwrap_or(rest);
        // DuckDB names the setting a spill ran out of, and nuthatch's hint keys on the name.
        let setting = if first.contains("during the spilling process") {
            "\nThis limit was set by the 'max_temp_directory_size' setting."
        } else {
            ""
        };
        return Some(format!("Out of Memory Error: {first}{setting}"));
    }
    None
}

fn no_function(func: &str, args: &[String]) -> String {
    format!(
        "Binder Error: No function matches the given name and argument types '{func}({})'. You \
         might need to add explicit type casts.",
        args.join(", ")
    )
}

/// DuckDB's line first when there is one, then DataFusion's own text. The kept text must not
/// trip a class of its own ahead of the restated one, so its "No function matches" is lower-cased.
pub fn restate(msg: String) -> String {
    match duckdb_phrase(&msg) {
        Some(p) => format!(
            "{p}\n{}",
            msg.replace("No function matches", "no function matches")
        ),
        None => msg,
    }
}

#[cfg(test)]
mod tests {
    use super::duckdb_phrase as p;

    #[test]
    fn each_class_reads_as_duckdb_does() {
        assert_eq!(
            p("Error during planning: no table nosuch").unwrap(),
            "Catalog Error: Table with name nosuch does not exist!"
        );
        assert_eq!(
            p("Schema error: No field named valu. Did you mean 'token__transfer.value'?").unwrap(),
            "Binder Error: Referenced column \"valu\" not found in FROM clause!"
        );
        assert_eq!(
            p("Schema error: No field named t.valu. Did you mean 't.value'?").unwrap(),
            "Binder Error: Table \"t\" does not have a column named \"valu\""
        );
        assert_eq!(
            p("SQL error: ParserError(\"Expected: identifier, found: EOF\")").unwrap(),
            "Parser Error: syntax error at end of input"
        );
        assert_eq!(
            p("SQL error: ParserError(\"Expected: end of statement, found: foo at Line: 1, Column: 8\")")
                .unwrap(),
            "Parser Error: syntax error at or near \"foo\""
        );
        assert!(
            p(
                "Function 'sum' failed to match any signature, errors: Function 'sum' requires \
                   Decimal, but received String (DataType: Utf8View)."
            )
            .unwrap()
            .contains("'sum(VARCHAR)'")
        );
        assert!(
            p("No function matches the given name and argument types 'bool_and(Utf8View)'.")
                .unwrap()
                .contains("'bool_and(VARCHAR)'")
        );
        assert!(p("No function matches the given name and argument types 'coalesce(Utf8View, Boolean)'.")
            .unwrap()
            .contains("Cannot mix values of type VARCHAR and BOOLEAN"));
        assert!(p("Failed to coerce then (Utf8View) and else (Boolean) to common types in CASE WHEN expression")
            .unwrap()
            .contains("Cannot mix values of type BOOLEAN and VARCHAR in CASE"));
        assert!(
            p("Resources exhausted: Failed to allocate 10.0 MB")
                .unwrap()
                .starts_with("Out of Memory Error")
        );
        assert!(
            p("Resources exhausted: The used disk space during the spilling process has exceeded the allowable limit of 1.0 MB.")
                .unwrap()
                .ends_with("This limit was set by the 'max_temp_directory_size' setting.")
        );
    }

    #[test]
    fn a_missing_column_is_cut_where_its_name_ends() {
        assert_eq!(
            p("Schema error: No field named zzzz.\nValid fields are t.block_number, t.k, t.value.")
                .unwrap(),
            "Binder Error: Referenced column \"zzzz\" not found in FROM clause!"
        );
        assert_eq!(
            p("Schema error: No field named t.zzzz.\nValid fields are t.block_number, t.k.")
                .unwrap(),
            "Binder Error: Table \"t\" does not have a column named \"zzzz\""
        );
        assert_eq!(
            p("Schema error: No field named \"Parquet error: x\".\nValid fields are t.k.").unwrap(),
            "Binder Error: Referenced column \"Parquet error: x\" not found in FROM clause!"
        );
        assert_eq!(
            p("Schema error: No field named \"a. b\"\"c\". Did you mean 't.k'?").unwrap(),
            "Binder Error: Referenced column \"a. b\"c\" not found in FROM clause!"
        );
        assert_eq!(
            p("Schema error: No field named t.\"Big.Col\".\nValid fields are t.k.").unwrap(),
            "Binder Error: Table \"t\" does not have a column named \"Big.Col\""
        );
        assert_eq!(
            p("Schema error: No field named \"T x\".valu. Did you mean '\"T x\".value'?").unwrap(),
            "Binder Error: Table \"T x\" does not have a column named \"valu\""
        );
    }

    #[test]
    fn unrecognised_text_passes_and_the_kept_text_trips_nothing() {
        assert_eq!(p("Arrow error: Divide by zero error"), None);
        let r = super::restate(
            "No function matches the given name and argument types 'coalesce(Utf8View, Boolean)'."
                .into(),
        );
        assert_eq!(r.matches("No function matches").count(), 0, "{r}");
    }
}
