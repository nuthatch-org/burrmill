//! DuckDB's `[expr FOR x IN list]` and `[expr FOR x IN list IF pred]`.
//!
//! sqlparser stops at `FOR`. The views still write the comprehension (the hex fold over
//! `string_split`), and `list_transform` / `list_filter` already answer as DuckDB does, so the
//! text is rewritten into those before either parser sees it.

/// `[expr FOR x IN list]` as `list_transform`, and `IF` as a `list_filter` in front of it.
pub fn expand(sql: &str) -> String {
    let mut cur = sql.to_string();
    for _ in 0..16 {
        let next = pass(&cur);
        if next == cur {
            return cur;
        }
        cur = next;
    }
    cur
}

/// Every text rewrite made before a parser sees DuckDB's SQL.
pub fn before_parse(sql: &str) -> String {
    drop_materialized(&expand(sql))
}

/// `AS [NOT] MATERIALIZED (` as `AS (`: a planner hint to DuckDB, which sqlparser reads only for
/// Postgres. Strings and comments are left as written.
pub fn drop_materialized(sql: &str) -> String {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\'' || b[i] == b'"' {
            i = copy_string(sql, i, &mut out);
            continue;
        }
        if b[i] == b'-' && b.get(i + 1) == Some(&b'-') {
            i = copy_line(sql, i, &mut out);
            continue;
        }
        if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
            i = copy_block(sql, i, &mut out);
            continue;
        }
        let boundary = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
        if boundary && let Some(end) = materialized_after_as(b, i) {
            out.push_str("AS ");
            i = end;
            continue;
        }
        let ch = sql[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// At `AS`, the index of the `(` after `[NOT] MATERIALIZED`, if that is what follows.
fn materialized_after_as(b: &[u8], at: usize) -> Option<usize> {
    let word = |i: usize, w: &str| {
        b.len() >= i + w.len()
            && b[i..i + w.len()].eq_ignore_ascii_case(w.as_bytes())
            && b.get(i + w.len())
                .is_none_or(|c| !(c.is_ascii_alphanumeric() || *c == b'_'))
    };
    let space = |mut i: usize| {
        while b.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        i
    };
    if !word(at, "AS") {
        return None;
    }
    let mut i = space(at + 2);
    if word(i, "NOT") {
        i = space(i + 3);
    }
    if !word(i, "MATERIALIZED") {
        return None;
    }
    let i = space(i + "MATERIALIZED".len());
    (b.get(i) == Some(&b'(')).then_some(i)
}

fn pass(sql: &str) -> String {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\'' || b[i] == b'"' {
            i = copy_string(sql, i, &mut out);
            continue;
        }
        if b[i] == b'-' && b.get(i + 1) == Some(&b'-') {
            i = copy_line(sql, i, &mut out);
            continue;
        }
        if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
            i = copy_block(sql, i, &mut out);
            continue;
        }
        if b[i] == b'['
            && let Some((end, repl)) = comprehension(sql, i)
        {
            out.push_str(&repl);
            i = end;
            continue;
        }
        let ch = sql[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// `Some((index past the closing bracket, replacement))` when `[` at `open` is a comprehension.
fn comprehension(sql: &str, open: usize) -> Option<(usize, String)> {
    let b = sql.as_bytes();
    let mut j = open + 1;
    let mut brackets = 1i32;
    let mut parens = 0i32;
    let mut for_at = None;
    while j < b.len() && brackets > 0 {
        if b[j] == b'\'' || b[j] == b'"' {
            j = skip_string(sql, j);
            continue;
        }
        if b[j] == b'-' && b.get(j + 1) == Some(&b'-') {
            j = skip_line(sql, j);
            continue;
        }
        if b[j] == b'/' && b.get(j + 1) == Some(&b'*') {
            j = skip_block(sql, j);
            continue;
        }
        match b[j] {
            b'(' => parens += 1,
            b')' => parens -= 1,
            b'[' => brackets += 1,
            b']' => {
                brackets -= 1;
                if brackets == 0 {
                    break;
                }
            }
            _ if brackets == 1 && parens == 0 && for_at.is_none() && keyword(sql, j, "FOR") => {
                for_at = Some(j);
                j += 3;
                continue;
            }
            _ => {
                j += sql[j..].chars().next().unwrap().len_utf8();
                continue;
            }
        }
        j += 1;
    }
    let for_at = for_at?;
    if brackets != 0 {
        return None;
    }
    let close = j;
    let mut k = skip_ws(sql, for_at + 3);
    let var_end = ident_end(sql, k)?;
    let var = &sql[k..var_end];
    let var = var
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(var);
    if var.is_empty() {
        return None;
    }
    k = skip_ws(sql, var_end);
    if !keyword(sql, k, "IN") {
        return None;
    }
    k = skip_ws(sql, k + 2);
    let (list_end, pred) = list_and_if(sql, k, close)?;
    let expr = sql[open + 1..for_at].trim();
    let list = sql[k..list_end].trim();
    if expr.is_empty() || list.is_empty() {
        return None;
    }
    let repl = match pred {
        Some(pred) => format!(
            "list_transform(list_filter({list}, lambda {var}: {pred}), lambda {var}: {expr})"
        ),
        None => format!("list_transform({list}, lambda {var}: {expr})"),
    };
    Some((close + 1, repl))
}

/// End of the list expression, and the `IF` predicate when one is written.
fn list_and_if(sql: &str, start: usize, close: usize) -> Option<(usize, Option<&str>)> {
    let b = sql.as_bytes();
    let mut j = start;
    let mut parens = 0i32;
    let mut brackets = 0i32;
    while j < close {
        if b[j] == b'\'' || b[j] == b'"' {
            j = skip_string(sql, j);
            continue;
        }
        if parens == 0 && brackets == 0 && keyword(sql, j, "IF") {
            let pred = sql[j + 2..close].trim();
            if pred.is_empty() {
                return None;
            }
            return Some((j, Some(pred)));
        }
        match b[j] {
            b'(' => parens += 1,
            b')' => parens -= 1,
            b'[' => brackets += 1,
            b']' => brackets -= 1,
            _ => {
                j += sql[j..].chars().next().unwrap().len_utf8();
                continue;
            }
        }
        j += 1;
    }
    Some((close, None))
}

/// A keyword, not a prefix of an identifier and not a call (`if(`). Followed by whitespace,
/// which is how `FOR` / `IN` / `IF` are written.
fn keyword(sql: &str, i: usize, kw: &str) -> bool {
    let b = sql.as_bytes();
    let n = kw.len();
    // On bytes: `i + n` can land inside a multi-byte character, where a `str` slice panics.
    if i + n > b.len() || !b[i..i + n].eq_ignore_ascii_case(kw.as_bytes()) {
        return false;
    }
    let before = i == 0 || !is_ident(b[i - 1]);
    let after = i + n;
    before && after < b.len() && sql[after..].chars().next().unwrap().is_whitespace()
}

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

fn ident_end(sql: &str, i: usize) -> Option<usize> {
    let b = sql.as_bytes();
    if i >= b.len() {
        return None;
    }
    if b[i] == b'"' {
        let end = sql[i + 1..].find('"')?;
        return Some(i + 1 + end + 1);
    }
    if !b[i].is_ascii_alphabetic() && b[i] != b'_' {
        return None;
    }
    let mut j = i + 1;
    while j < b.len() && is_ident(b[j]) {
        j += 1;
    }
    Some(j)
}

fn skip_ws(sql: &str, mut i: usize) -> usize {
    while i < sql.len() {
        let ch = sql[i..].chars().next().unwrap();
        if !ch.is_whitespace() {
            break;
        }
        i += ch.len_utf8();
    }
    i
}

fn copy_string(sql: &str, i: usize, out: &mut String) -> usize {
    let end = skip_string(sql, i);
    out.push_str(&sql[i..end]);
    end
}

/// Past a `'string'` or a `"quoted identifier"`, whichever quote opens at `i`; a doubled quote
/// inside is an escape.
fn skip_string(sql: &str, i: usize) -> usize {
    let b = sql.as_bytes();
    let quote = b[i];
    let mut j = i + 1;
    while j < b.len() {
        if b[j] == quote {
            if b.get(j + 1) == Some(&quote) {
                j += 2;
                continue;
            }
            return j + 1;
        }
        j += sql[j..].chars().next().unwrap().len_utf8();
    }
    j
}

fn copy_line(sql: &str, i: usize, out: &mut String) -> usize {
    let end = skip_line(sql, i);
    out.push_str(&sql[i..end]);
    end
}

fn skip_line(sql: &str, i: usize) -> usize {
    sql[i..].find('\n').map(|n| i + n + 1).unwrap_or(sql.len())
}

fn copy_block(sql: &str, i: usize, out: &mut String) -> usize {
    let end = skip_block(sql, i);
    out.push_str(&sql[i..end]);
    end
}

fn skip_block(sql: &str, i: usize) -> usize {
    sql[i + 2..]
        .find("*/")
        .map(|n| i + 2 + n + 2)
        .unwrap_or(sql.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_comprehension_becomes_list_transform() {
        assert_eq!(
            expand("[x * 2 FOR x IN [1, 2, 3]]"),
            "list_transform([1, 2, 3], lambda x: x * 2)"
        );
        assert_eq!(
            expand("[x * 2 FOR x IN [1, 2, 3] IF x > 1]"),
            "list_transform(list_filter([1, 2, 3], lambda x: x > 1), lambda x: x * 2)"
        );
        // The lodestar hex fold, wrapped the way the view wraps it.
        let src = "[CAST(strpos('0123456789abcdef', c) - 1 AS HUGEINT)\n FOR c IN string_split('ab', '')]";
        let got = expand(src);
        assert!(
            got.starts_with("list_transform(string_split('ab', ''), lambda c: CAST("),
            "{got}"
        );
        assert!(!got.contains(" FOR "));
    }

    #[test]
    fn a_list_literal_and_a_string_are_left_alone() {
        let src = "SELECT [1, 2, 3], 'FOR x IN [1]' FROM t";
        assert_eq!(expand(src), src);
        // A quoted identifier is not a keyword either, inside or outside the brackets.
        let src = r#"SELECT ["for x" FOR x IN [1]], "FOR y IN [2]" FROM t"#;
        assert_eq!(
            expand(src),
            r#"SELECT list_transform([1], lambda x: "for x"), "FOR y IN [2]" FROM t"#
        );
        // Nested: the inner comprehension goes first, then the outer.
        assert_eq!(
            expand("[[y + 1 FOR y IN x] FOR x IN [[1], [2]]]"),
            "list_transform([[1], [2]], lambda x: list_transform(x, lambda y: y + 1))"
        );
    }

    #[test]
    fn a_multibyte_character_after_a_bracket_does_not_panic() {
        for sql in [
            "SELECT [ \tɚ",
            "SELECT [ɚ",
            "SELECT [ \tɚ]",
            "SELECT [x FOR ɚ IN [1]]",
            "SELECT [x FOR x IN ɚ]",
            "SELECT [x FOR x IN [1] Iɚ]",
            "SELECT [x Fɚ x IN [1]]",
            "SELECT [x FO😀 x IN [1]]",
            "SELECT [😀 FOR x IN [1]]",
            "SELECT [x FOR x I\u{301}N [1]]",
            "SELECT [e\u{301} FOR x IN [1] IF x\u{301} > 0]",
            "SELECT 1 AS ɚ MATERIALIZED (",
            "SELECT 1 Aɚ",
        ] {
            let _ = before_parse(sql);
            let _ = crate::inspect::reach(sql);
        }
        assert_eq!(
            expand("[x FOR x IN ['ɚ😀', 'e\u{301}']]"),
            "list_transform(['ɚ😀', 'e\u{301}'], lambda x: x)"
        );
        assert!(crate::inspect::reach("SELECT [ \tɚ").is_err());
    }

    proptest::proptest! {
        #[test]
        fn any_text_rewrites_without_panicking(sql in "\\PC*") {
            let _ = before_parse(&sql);
        }

        #[test]
        fn comprehension_shaped_text_rewrites_without_panicking(
            sql in "(SELECT )?\\[[ \t]*((FOR|IN|IF|AS|NOT|MATERIALIZED|[xɚ😀é\u{301}'\"\\[\\]()-/*]|\\PC)[ \t]*){0,12}\\]?",
        ) {
            let _ = before_parse(&sql);
        }
    }
}
