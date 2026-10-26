//! Fill `{rN.column}` and `{column}` (row 0) from the result.
//!
//! A digit that is not inside a slot is a refusal. The check is on the template,
//! before any value is written in: the filled sentence is allowed to contain the
//! number the slot resolved to.

use crate::format::{format_number, is_ident};
use crate::session::Column;

struct Slot {
    start: usize,
    end: usize,
    row: usize,
    column: String,
}

pub fn narrate(
    template: &str,
    columns: &[Column],
    rows: &[Vec<Option<String>>],
    group: &str,
    decimal: &str,
) -> Result<String, String> {
    let slots = find_slots(template);
    let mut bare = String::new();
    let mut at = 0;
    for s in &slots {
        bare.push_str(&template[at..s.start]);
        at = s.end;
    }
    bare.push_str(&template[at..]);
    if bare.chars().any(|c| c.is_ascii_digit()) {
        return Err("narration has a digit outside a slot".into());
    }
    let mut out = String::new();
    at = 0;
    for s in &slots {
        out.push_str(&template[at..s.start]);
        out.push_str(&lookup(columns, rows, s.row, &s.column, group, decimal)?);
        at = s.end;
    }
    out.push_str(&template[at..]);
    Ok(out)
}

fn find_slots(template: &str) -> Vec<Slot> {
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
                slots.push(Slot {
                    start: i,
                    end,
                    row,
                    column,
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
}
