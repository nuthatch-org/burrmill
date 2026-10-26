//! What a caller can show before any query, and what a data card is later built from.
//!
//! The report has no cell values. [`crate::Table::preview`] is a separate call so a payload
//! builder that only has the report cannot reach rows.

use crate::budget::BudgetUse;
use crate::sniff::{Dialect, Locale};

/// Cap on stored issues. Further ones are counted and dropped, so a ragged file cannot grow
/// the report without a bound.
pub const MAX_ISSUES: usize = 64;

/// A column keeps a type when at least this fraction of its non-empty cells parse as that type.
pub const TYPE_CONFIDENCE: f64 = 0.90;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InferredType {
    Integer,
    Decimal { precision: u8, scale: i8 },
    Date,
    Timestamp,
    Boolean,
    Text,
}

impl InferredType {
    pub fn name(&self) -> String {
        match self {
            Self::Integer => "INTEGER".to_string(),
            Self::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
            Self::Date => "DATE".to_string(),
            Self::Timestamp => "TIMESTAMP".to_string(),
            Self::Boolean => "BOOLEAN".to_string(),
            Self::Text => "TEXT".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnReport {
    pub name: String,
    pub inferred: InferredType,
    pub nulls: u64,
    pub non_empty: u64,
    /// Non-empty cells that parsed as `inferred`.
    pub matched: u64,
}

impl ColumnReport {
    pub fn confidence(&self) -> f64 {
        if self.non_empty == 0 {
            1.0
        } else {
            self.matched as f64 / self.non_empty as f64
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Issue {
    Ragged { expected: usize, got: usize },
    UnclosedQuote,
    TypeMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowIssue {
    /// 1-based data row. Zero is the header line.
    pub row: u64,
    pub column: Option<String>,
    pub reason: Issue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestReport {
    pub dialect: Dialect,
    pub header: bool,
    pub rows: u64,
    pub columns: Vec<ColumnReport>,
    pub rejected: Vec<RowIssue>,
    pub rejected_omitted: u64,
    pub budget_used: BudgetUse,
}

impl IngestReport {
    pub fn push_issue(&mut self, issue: RowIssue) {
        if self.rejected.len() < MAX_ISSUES {
            self.rejected.push(issue);
        } else {
            self.rejected_omitted += 1;
        }
    }
}

impl std::fmt::Display for IngestReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "csv delim={} quote={} encoding={} locale={} header={} rows={}",
            self.dialect.delimiter as char,
            self.dialect.quote as char,
            self.dialect.encoding.name(),
            match self.dialect.locale {
                Locale::Point => "point",
                Locale::Comma => "comma",
            },
            self.header,
            self.rows,
        )?;
        for c in &self.columns {
            writeln!(
                f,
                "  {} {} nulls={} confidence={:.2}",
                c.name,
                c.inferred.name(),
                c.nulls,
                c.confidence(),
            )?;
        }
        writeln!(
            f,
            "issues {} (+{} omitted)",
            self.rejected.len(),
            self.rejected_omitted
        )
    }
}
