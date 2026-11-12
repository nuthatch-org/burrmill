//! The error type, and specifically the variant that is a product feature.

use std::fmt;

/// Every way a Burrmill query can fail to produce an answer.
///
/// `Overflow` is not an implementation detail leaking out. Nuthatch indexes a blockchain: balances
/// and exposures are exact integers, and a silently-wrapped sum is a **wrong answer that looks
/// right** - the worst failure an indexer has. DataFusion's integer arithmetic wraps silently and
/// there is still no core config flag to stop it (#17539, #14771 and #20034 all open as of August
/// 2026); DuckDB errors on `HUGEINT` overflow but is not watertight everywhere. Refusing is a
/// guarantee neither of them offers, so it gets a variant of its own rather than a footnote.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BurrmillError {
    /// The query is outside the admitted subset. Enforced against the parsed shape, never by
    /// string-matching SQL - a denylist over text is the failure mode that gave DuckDB
    /// CVE-2024-41672, where `sniff_csv` read the filesystem with `enable_external_access=false`.
    NotAllowed(String),
    /// Exact integer arithmetic left the representable range. The query is refused; no plausible
    /// wrong number is returned.
    Overflow(String),
    /// `Limits::timeout` elapsed. Checked at morsel boundaries, which is why the interval between
    /// asking to stop and stopping is bounded rather than hoped for.
    Timeout,
    /// Cancelled by the caller.
    Cancelled,
    /// The hot tip and the sealed cold segments disagree about the boundary - COR-1. A seam bug can
    /// double-count or drop rows, so it refuses rather than answering.
    Seam(String),
    /// The result exceeded `max_rows` or `max_bytes`, or the statement its memory budget or the
    /// value limit.
    LimitExceeded(String),
    /// The statement does not plan: a name it cannot resolve, a type it cannot reconcile, a shape
    /// a rule refuses. Raised before any page is read, as DuckDB's binder errors are, so a host can
    /// tell a wrong statement from damaged data.
    Plan(String),
    /// Something below us - Parquet decode, Arrow, or the filesystem.
    Substrate(String),
    /// SQL that will not parse at all.
    Parse(String),
    /// A table name resolved to **no segments at all**.
    ///
    /// This is a refusal rather than an empty answer on purpose, and it was found by pointing the
    /// harness at a table name that does not exist: DuckDB refused, Burrmill planned `files=0
    /// morsels=0` and would have returned an empty result. "No rows" and "no such table" are
    /// different answers, and the first one is a wrong answer that looks entirely plausible - the
    /// balances came back empty, so presumably nobody staked anything. A nest's segment directory
    /// holds every table, so a typo in a table name is one prefix away at all times.
    NoSegments(String),
    /// The engine was asked to open in a way that cannot work: a shared pool with no budget, a store
    /// source opened without `open_async`. Nothing about any statement.
    Config(String),
}

impl fmt::Display for BurrmillError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAllowed(m) => write!(f, "not in the admitted subset: {m}"),
            Self::Overflow(m) => write!(f, "exact arithmetic overflow, refused: {m}"),
            Self::Timeout => write!(f, "query exceeded its timeout"),
            Self::Cancelled => write!(f, "query cancelled"),
            Self::Seam(m) => write!(f, "hot/cold seam violation: {m}"),
            Self::LimitExceeded(m) => write!(f, "limit exceeded: {m}"),
            Self::Plan(m) => write!(f, "plan error: {m}"),
            Self::Substrate(m) => write!(f, "substrate error: {m}"),
            Self::Parse(m) => write!(f, "parse error: {m}"),
            Self::NoSegments(m) => write!(f, "no segments: {m}"),
            Self::Config(m) => write!(f, "invalid engine configuration: {m}"),
        }
    }
}

impl std::error::Error for BurrmillError {}

impl From<std::io::Error> for BurrmillError {
    fn from(e: std::io::Error) -> Self {
        Self::Substrate(e.to_string())
    }
}

impl From<parquet::errors::ParquetError> for BurrmillError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Substrate(e.to_string())
    }
}

impl From<arrow::error::ArrowError> for BurrmillError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Substrate(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, BurrmillError>;
