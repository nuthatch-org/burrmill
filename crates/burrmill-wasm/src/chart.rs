//! Closed chart menu. A kind the result cannot draw becomes a table.

use serde::{Deserialize, Serialize};

use crate::session::Column;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChartKind {
    Number,
    Bar,
    Line,
    StackedBar,
    Table,
}

impl ChartKind {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "number" => Self::Number,
            "bar" => Self::Bar,
            "line" => Self::Line,
            "stacked_bar" => Self::StackedBar,
            "table" => Self::Table,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chart {
    pub kind: ChartKind,
    pub x: Option<String>,
    pub y: Vec<String>,
}

impl Chart {
    pub fn table() -> Self {
        Self {
            kind: ChartKind::Table,
            x: None,
            y: Vec::new(),
        }
    }
}

pub fn check_chart(spec: &Chart, columns: &[Column], row_count: usize) -> Chart {
    let col = |n: &str| columns.iter().find(|c| c.name == n);
    match spec.kind {
        ChartKind::Table => Chart::table(),
        ChartKind::Number => {
            let [y] = spec.y.as_slice() else {
                return Chart::table();
            };
            match col(y) {
                Some(c) if row_count == 1 && c.numeric => spec.clone(),
                _ => Chart::table(),
            }
        }
        ChartKind::Bar | ChartKind::Line | ChartKind::StackedBar => {
            let Some(x) = spec.x.as_deref() else {
                return Chart::table();
            };
            if col(x).is_none() || spec.y.is_empty() || row_count == 0 {
                return Chart::table();
            }
            if spec.y.iter().all(|y| col(y).is_some_and(|c| c.numeric)) {
                spec.clone()
            } else {
                Chart::table()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, numeric: bool) -> Column {
        Column {
            name: name.to_string(),
            type_name: if numeric { "DECIMAL(10,2)" } else { "TEXT" }.to_string(),
            numeric,
        }
    }

    #[test]
    fn a_number_needs_one_row_and_one_numeric_column() {
        let cols = vec![col("total", true), col("merchant", false)];
        let spec = Chart {
            kind: ChartKind::Number,
            x: None,
            y: vec!["total".into()],
        };
        assert_eq!(check_chart(&spec, &cols, 1).kind, ChartKind::Number);
        assert_eq!(check_chart(&spec, &cols, 2).kind, ChartKind::Table);
        let text = Chart {
            kind: ChartKind::Number,
            x: None,
            y: vec!["merchant".into()],
        };
        assert_eq!(check_chart(&text, &cols, 1).kind, ChartKind::Table);
    }

    #[test]
    fn a_bar_with_a_missing_column_falls_back() {
        let cols = vec![col("merchant", false), col("total", true)];
        let spec = Chart {
            kind: ChartKind::Bar,
            x: Some("missing".into()),
            y: vec!["total".into()],
        };
        assert_eq!(check_chart(&spec, &cols, 3).kind, ChartKind::Table);
        let ok = Chart {
            kind: ChartKind::Bar,
            x: Some("merchant".into()),
            y: vec!["total".into()],
        };
        assert_eq!(check_chart(&ok, &cols, 3).kind, ChartKind::Bar);
    }
}
