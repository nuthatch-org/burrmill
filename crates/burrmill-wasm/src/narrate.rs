//! Fill `{rN.column}` and `{column}` (row 0) from the result.
//!
//! A digit that is not inside a slot is a refusal. The check is on the template,
//! before any value is written in: the filled sentence is allowed to contain the
//! number the slot resolved to.
//!
//! A known `CUSTOMER_N` token is removed before that check, then replaced with
//! its value. The value may contain digits. An unknown `CUSTOMER_99` is not a
//! token, so its digits still refuse the line. Replacement reads the template,
//! not text a slot has already filled.

use std::collections::BTreeMap;

use crate::format::{format_number, is_ident};
use crate::session::Column;

struct Span {
    start: usize,
    end: usize,
    kind: Kind,
}

enum Kind {
    Slot { row: usize, column: String },
    Token(String),
}

pub fn narrate(
    template: &str,
    columns: &[Column],
    rows: &[Vec<Option<String>>],
    group: &str,
    decimal: &str,
) -> Result<String, String> {
    narrate_with(template, columns, rows, group, decimal, &BTreeMap::new())
}

pub fn narrate_with(
    template: &str,
    columns: &[Column],
    rows: &[Vec<Option<String>>],
    group: &str,
    decimal: &str,
    tokens: &BTreeMap<String, String>,
) -> Result<String, String> {
    let spans = find_spans(template, tokens);
    let mut bare = String::new();
    let mut at = 0;
    for span in &spans {
        bare.push_str(&template[at..span.start]);
        at = span.end;
    }
    bare.push_str(&template[at..]);
    if bare.chars().any(|c| c.is_ascii_digit()) {
        return Err("narration has a digit outside a slot".into());
    }
    let mut out = String::new();
    at = 0;
    for span in &spans {
        out.push_str(&template[at..span.start]);
        match &span.kind {
            Kind::Slot { row, column } => {
                out.push_str(&lookup(columns, rows, *row, column, group, decimal)?);
            }
            Kind::Token(value) => out.push_str(value),
        }
        at = span.end;
    }
    out.push_str(&template[at..]);
    Ok(out)
}

fn find_spans(template: &str, tokens: &BTreeMap<String, String>) -> Vec<Span> {
    let mut spans = find_slots(template);
    let slots: Vec<(usize, usize)> = spans.iter().map(|s| (s.start, s.end)).collect();
    let b = template.as_bytes();
    let needle = b"CUSTOMER_";
    let mut i = 0;
    while i + needle.len() < b.len() {
        if b[i..i + needle.len()] == *needle {
            let mut j = i + needle.len();
            if j < b.len() && b[j].is_ascii_digit() {
                while j < b.len() && b[j].is_ascii_digit() {
                    j += 1;
                }
                let token = &template[i..j];
                let overlaps = slots.iter().any(|(start, end)| i < *end && j > *start);
                if !overlaps && let Some(value) = tokens.get(token) {
                    spans.push(Span {
                        start: i,
                        end: j,
                        kind: Kind::Token(value.clone()),
                    });
                    i = j;
                    continue;
                }
            }
        }
        i += 1;
    }
    spans.sort_by_key(|span| span.start);
    spans
}

fn find_slots(template: &str) -> Vec<Span> {
    let b = template.as_bytes();
    let mut i = 0;
    let mut slots = Vec::new();
    while i < b.len() {
        if b[i] == b'{'
            && let Some(rel) = template[i + 1..].find('}')
        {
            let inner = &template[i + 1..i + 1 + rel];
            let end = i + 1 + rel + 1;
            if let Some((row, column)) = parse_slot(inner) {
                slots.push(Span {
                    start: i,
                    end,
                    kind: Kind::Slot { row, column },
                });
                i = end;
                continue;
            }
        }
        i += 1;
    }
    slots
}

fn parse_slot(inner: &str) -> Option<(usize, String)> {
    if let Some(rest) = inner.strip_prefix('r')
        && let Some((n, col)) = rest.split_once('.')
        && !n.is_empty()
        && n.bytes().all(|b| b.is_ascii_digit())
        && is_ident(col)
        && let Ok(row) = n.parse::<usize>()
    {
        return Some((row, col.to_string()));
    }
    if is_ident(inner) {
        return Some((0, inner.to_string()));
    }
    None
}

fn lookup(
    columns: &[Column],
    rows: &[Vec<Option<String>>],
    row: usize,
    column: &str,
    group: &str,
    decimal: &str,
) -> Result<String, String> {
    let Some(idx) = columns.iter().position(|c| c.name == column) else {
        return Err("narration slot cannot be filled".into());
    };
    let Some(cell) = rows.get(row).and_then(|r| r.get(idx)) else {
        return Err("narration slot cannot be filled".into());
    };
    let Some(raw) = cell else {
        return Ok("null".into());
    };
    if columns[idx].numeric {
        Ok(format_number(raw, group, decimal))
    } else {
        Ok(raw.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols() -> Vec<Column> {
        vec![
            Column {
                name: "total".into(),
                type_name: "DECIMAL(10,2)".into(),
                numeric: true,
            },
            Column {
                name: "merchant".into(),
                type_name: "TEXT".into(),
                numeric: false,
            },
        ]
    }

    fn rows() -> Vec<Vec<Option<String>>> {
        vec![vec![Some("-1234.50".into()), Some("Northwind".into())]]
    }

    #[test]
    fn slots_fill_and_the_scale_stays() {
        let got = narrate("Total {r0.total}.", &cols(), &rows(), ",", ".").unwrap();
        assert_eq!(got, "Total -1,234.50.");
        let got = narrate("Total {total}.", &cols(), &rows(), " ", ",").unwrap();
        assert_eq!(got, "Total -1 234,50.");
        let got = narrate("{merchant}", &cols(), &rows(), ",", ".").unwrap();
        assert_eq!(got, "Northwind");
    }

    #[test]
    fn a_digit_outside_a_slot_is_refused() {
        let err = narrate("Total 12 {r0.total}.", &cols(), &rows(), ",", ".").unwrap_err();
        assert_eq!(err, "narration has a digit outside a slot");
        assert!(!err.chars().any(|c| c.is_ascii_digit()));
    }

    #[test]
    fn an_unresolvable_slot_is_refused() {
        assert!(narrate("{missing}", &cols(), &rows(), ",", ".").is_err());
        assert!(narrate("{r1.total}", &cols(), &rows(), ",", ".").is_err());
    }

    #[test]
    fn a_known_token_is_restored_and_an_unknown_one_is_refused() {
        let mut tokens = BTreeMap::new();
        tokens.insert("CUSTOMER_1".into(), "canary.one@example.com".into());
        tokens.insert("CUSTOMER_17".into(), "+359888100017".into());
        let got = narrate_with("Person CUSTOMER_1.", &cols(), &rows(), ",", ".", &tokens).unwrap();
        assert_eq!(got, "Person canary.one@example.com.");
        let phone = narrate_with("Call CUSTOMER_17.", &cols(), &rows(), ",", ".", &tokens).unwrap();
        assert_eq!(phone, "Call +359888100017.");
        let err =
            narrate_with("Person CUSTOMER_99.", &cols(), &rows(), ",", ".", &tokens).unwrap_err();
        assert_eq!(err, "narration has a digit outside a slot");
        assert!(!err.chars().any(|c| c.is_ascii_digit()));
        let bare = narrate("Person CUSTOMER_1.", &cols(), &rows(), ",", ".").unwrap_err();
        assert_eq!(bare, "narration has a digit outside a slot");
        let mut only = BTreeMap::new();
        only.insert("CUSTOMER_1".into(), "canary.one@example.com".into());
        let prefix =
            narrate_with("Person CUSTOMER_17.", &cols(), &rows(), ",", ".", &only).unwrap_err();
        assert_eq!(prefix, "narration has a digit outside a slot");
        let both = narrate_with(
            "Total {total} for CUSTOMER_1.",
            &cols(),
            &rows(),
            ",",
            ".",
            &only,
        )
        .unwrap();
        assert_eq!(both, "Total -1,234.50 for canary.one@example.com.");
    }
}
