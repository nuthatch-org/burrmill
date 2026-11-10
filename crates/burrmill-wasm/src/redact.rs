//! Query errors are shown to the model. A cell string must not travel with them.
//!
//! Quoted spans of four characters or more are replaced, and so is any stored cell
//! string of that length, longest first. A column name is left in place: short
//! tokens such as `1` would otherwise eat the error text.

pub fn redact(err: &str, cells: &[String], columns: &[String]) -> String {
    let mut secrets: Vec<&str> = cells
        .iter()
        .map(String::as_str)
        .filter(|s| s.len() >= 4 && !columns.iter().any(|c| c == *s))
        .collect();
    secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    secrets.dedup();
    let mut out = err.to_string();
    for s in secrets {
        out = out.replace(s, "?");
    }
    redact_quotes(&out, columns)
}

fn redact_quotes(s: &str, columns: &[String]) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' && c != '"' {
            out.push(c);
            continue;
        }
        let mut inner = String::new();
        let mut closed = false;
        for n in chars.by_ref() {
            if n == c {
                closed = true;
                break;
            }
            inner.push(n);
        }
        if !closed {
            out.push(c);
            out.push_str(&inner);
            break;
        }
        let keep = columns.iter().any(|col| col == &inner) || inner.len() < 4;
        out.push(c);
        if keep {
            out.push_str(&inner);
        } else {
            out.push('?');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_long_cell_and_a_quoted_span_go_but_a_column_name_stays() {
        let cells = vec!["CANARY_VALUE_991".to_string(), "1".to_string()];
        let columns = vec!["amount".to_string()];
        let err = "Cannot cast 'CANARY_VALUE_991' to INT; column amount; also CANARY_VALUE_991";
        let got = redact(err, &cells, &columns);
        assert!(!got.contains("CANARY_VALUE_991"));
        assert!(got.contains("amount"));
        assert!(got.contains('?'));
    }

    #[test]
    fn a_cell_that_is_a_column_name_is_not_stripped() {
        let cells = vec!["amount".to_string()];
        let columns = vec!["amount".to_string()];
        let err = "no field named amount";
        assert_eq!(redact(err, &cells, &columns), err);
    }
}
