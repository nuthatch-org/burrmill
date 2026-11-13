//! Error text that may go to the model has read no row, but it can quote the statement.
//!
//! Quoted spans of four characters or more are replaced, and so is any tokenised value
//! of that length. A column name is left in place: short tokens such as `1` would
//! otherwise eat the error text.

use crate::pseudonym::TokenMap;

pub fn redact(err: &str, tokens: &TokenMap, columns: &[String]) -> String {
    let mut hits: Vec<_> = tokens
        .occurrences(err)
        .into_iter()
        .filter(|r| !columns.iter().any(|c| *c == err[r.clone()]))
        .collect();
    hits.sort_by_key(|r| r.start);
    let mut out = String::with_capacity(err.len());
    let mut at = 0;
    for r in hits {
        if r.start >= at {
            out.push_str(&err[at..r.start]);
            out.push('?');
        }
        at = at.max(r.end);
    }
    out.push_str(&err[at..]);
    redact_quotes(&out, columns)
}

/// A column as the card lists it, or qualified by its table as DataFusion's hint writes it.
fn is_column(span: &str, columns: &[String]) -> bool {
    let bare = match span.split_once('.') {
        Some((table, col)) if table.chars().all(|c| c.is_alphanumeric() || c == '_') => col,
        _ => span,
    };
    columns.iter().any(|col| col == span || col == bare)
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
        let keep = is_column(&inner, columns) || inner.len() < 4;
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

    fn tokens(values: &str) -> TokenMap {
        let table = burrmill_ingest::ingest_bytes(
            format!("email\n{values}").as_bytes(),
            burrmill_ingest::FormatHint::Sniff,
            &burrmill_ingest::Budget::untrusted(),
        )
        .unwrap();
        crate::pseudonym::assign(
            &["email".to_string()],
            &std::collections::BTreeSet::from(["email".to_string()]),
            table.batches(),
        )
    }

    #[test]
    fn a_quoted_span_goes_but_a_column_name_stays() {
        let columns = vec!["amount".to_string()];
        let err = "Cannot cast 'CANARY_VALUE_991' to INT; column amount";
        let got = redact(err, &TokenMap::default(), &columns);
        assert!(!got.contains("CANARY_VALUE_991"));
        assert!(got.contains("amount"));
        assert!(got.contains('?'));
    }

    #[test]
    fn a_qualified_column_name_stays_in_a_hint() {
        let columns = vec!["merchant".to_string()];
        let err = "Did you mean 't.merchant'? Valid fields are 'x.CANARY_VALUE_991'";
        let got = redact(err, &TokenMap::default(), &columns);
        assert!(got.contains("'t.merchant'"), "{got}");
        assert!(!got.contains("CANARY_VALUE_991"), "{got}");
    }

    #[test]
    fn a_tokenised_value_goes_and_overlaps_merge() {
        let tokens = tokens("alice@mail.example\nce@mail.example.org\n");
        let err = "no match for alice@mail.example.org near email";
        let got = redact(err, &tokens, &["email".to_string()]);
        assert_eq!(got, "no match for ? near email");
    }

    #[test]
    fn a_value_that_is_a_column_name_is_not_stripped() {
        let tokens = tokens("amount\n");
        let err = "no field named amount";
        assert_eq!(redact(err, &tokens, &["amount".to_string()]), err);
    }
}
