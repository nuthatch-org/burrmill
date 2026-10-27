//! `SignedFoldExec` - the owned operator, and the reason the project exists.
//!
//! Its ancestor is nuthatch's #987 spike, which measured **0.55-0.85x DuckDB across 24 of 24
//! configurations** on `net_balances` while general DataFusion on the same shape measured
//! 2.53-2.80x *slower* (#964). That is a three-to-fivefold swing between renting general execution
//! and owning a specialised one, and it is the entire architectural argument.
//!
//! What makes it fast is not clever: it is the same three things DuckDB does, applied to one shape.
//! Morsel-driven parallelism over row groups rather than files; thread-local hash tables merged
//! once at the end rather than one shared table under contention; and a fast non-cryptographic
//! hash, because std's SipHash is DoS resistance we are not paying for here.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Decimal128Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;

use crate::error::{BurrmillError, Result};
use crate::exec::checked::checked_neg;
use crate::limits::Limits;
use crate::plan::SignedFold;
use crate::seam::HotSnapshot;
use crate::segment::{Morsel, SealedSegments};

/// Cooperative cancellation.
///
/// §3.5's contract, and it is a design property here rather than a hope. DataFusion's own joins do
/// not yield to cancellation (#19358, with `make_cooperative` proposed in #19360); an owned
/// operator picks its own yield points, so the delay between asking to stop and stopping is bounded
/// by one morsel.
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
    /// Arm again, for a token that outlives one statement (`Engine::cancel_token`).
    pub fn reset(&self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// What the fold cost, so a caller can see it without attaching a profiler. Feeds `EXPLAIN ANALYZE`.
#[derive(Debug, Clone, Copy, Default)]
pub struct FoldMetrics {
    pub morsels: usize,
    pub rows_read: u64,
    pub rows_skipped: u64,
    pub groups: usize,
    /// Reading the segment footers and cutting them into morsels. Serial in the first version, and
    /// at ten thousand segments it was the whole query.
    pub plan_ms: u128,
    /// The parallel fold itself.
    pub scan_ms: u128,
    /// Merging the per-morsel tables, plus the canonical sort.
    pub merge_ms: u128,
    pub elapsed_ms: u128,
    /// The aggregate's own live bytes at the moment the scan finished, before any row is built.
    /// This is what `mem_pool_bytes` is checked against, and printing it beside peak RSS is how you
    /// tell an aggregation that is too big from an output phase that is.
    pub agg_bytes: usize,
    /// Compressed bytes of the column chunks the scan reads, each row group once, from the footers:
    /// the same figure for the same fold over the same segments.
    pub bytes_read: u64,
}

/// The aggregation lives in [`crate::exec::agg`]. It is not a `HashMap<String, i128>` and the
/// reason is measured rather than aesthetic: see that module's header for the three versions this
/// replaced and what each of them cost.
use crate::exec::agg::{Rows, Scatter, SharedAgg};

pub struct SignedFoldExec<'a> {
    plan: &'a SignedFold,
    /// One entry per branch of the fold, in branch order.
    segments: &'a [&'a SealedSegments],
    limits: Limits,
    cancel: CancelToken,
    /// How many parallel groups this query may split into when it is **sharing** the pool.
    ///
    /// `None` means "this query has the pool to itself": decompose for balance and use every worker.
    /// It is deliberately not "degree equals the pool size", because that would make the answer
    /// depend on `rayon::current_num_threads()` agreeing with the number the caller divided by, and
    /// when those two disagreed the single-query tail went from 199 ms to 266 for no reason visible
    /// in the code. A caller that knows it is alone says so.
    degree: Option<usize>,
    /// The pinned seam, when a hot tip is registered. See [`crate::seam`] for why the watermark and
    /// the rows arrive together rather than as two reads.
    seam: Option<&'a Seam<'a>>,
}

/// The pinned boundary a query reads against.
pub struct Seam<'a> {
    pub snapshot: &'a HotSnapshot,
    /// The column carrying the block number. `block_number` in every nest seen so far, and settable
    /// rather than assumed, because assuming a layout is what `tests/seal_layout.rs` exists to catch.
    pub block_col: &'a str,
}

impl<'a> SignedFoldExec<'a> {
    pub fn new(plan: &'a SignedFold, segments: &'a [&'a SealedSegments], limits: Limits) -> Self {
        Self {
            plan,
            segments,
            limits,
            cancel: CancelToken::new(),
            degree: None,
            seam: None,
        }
    }

    /// Branches grouped by the table they read, so a table is scanned **once** however many arms
    /// name it. Returns `(representative branch index, all arms on that table)`.
    ///
    /// **Without this, generalising to n branches would quietly halve the benchmark.** The
    /// one-table-read-twice shape becomes two arms over the same table, and the naive reading is two
    /// passes over files that one pass already has in hand. Trading measured throughput for coverage
    /// without saying so is the sort of thing this project spends its days catching.
    fn scans(&self) -> Vec<(usize, Vec<usize>)> {
        let mut out: Vec<(usize, Vec<usize>)> = Vec::new();
        for (i, b) in self.plan.branches.iter().enumerate() {
            match out
                .iter_mut()
                .find(|(rep, _)| self.plan.branches[*rep].table == b.table)
            {
                Some((_, arms)) => arms.push(i),
                None => out.push((i, vec![i])),
            }
        }
        out
    }

    /// Cap this query's parallel width (roadmap 5.3a).
    ///
    /// A query admitted while others are queued should occupy a slice of the pool, not all of it.
    /// Without this the gate limits how many queries *start* and not how wide each one gets, so four
    /// admitted queries still fight over eight workers and the throughput stays where 5.2 found it.
    pub fn with_degree(mut self, degree: Option<usize>) -> Self {
        self.degree = degree.map(|d| d.max(1));
        self
    }

    pub fn with_seam(mut self, seam: &'a Seam<'a>) -> Self {
        self.seam = Some(seam);
        self
    }

    pub fn with_cancel(mut self, cancel: CancelToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// Run the fold and return the canonically ordered rows plus what it cost.
    pub fn run(&self) -> Result<(Rows, FoldMetrics)> {
        let started = Instant::now();
        let scans = self.scans();
        // Morsels carry the scan they belong to, so one flat work list covers every table in the
        // fold and the morsel scheduler needs no notion of branches at all.
        let mut morsels: Vec<(usize, Morsel)> = Vec::new();
        for (scan_ix, (rep, _)) in scans.iter().enumerate() {
            for m in self.segments[*rep].morsels()?.iter() {
                // Cheap: the path is an `Arc` and the Parquet metadata is already behind one, so
                // this copies a handful of words rather than re-reading anything.
                morsels.push((scan_ix, m.clone()));
            }
        }
        let plan_ms = started.elapsed().as_millis();
        let bytes_read = self.chunk_bytes(&scans, &morsels);
        let deadline = started + self.limits.timeout;

        // **One aggregation per worker, not per work unit.** An earlier version gave every batch its
        // own table and combined them on a single thread: at two hundred thousand groups that was 30
        // ms of scanning against 182 ms of merging, and at a million, 36 against 419. The merge
        // *was* the query. A table count that follows the core count is also exactly the behaviour
        // this RFC criticises DataFusion for (#6937), so reproducing it would have been embarrassing
        // as well as slow.
        let scan_started = Instant::now();
        let batches = coalesce(&morsels, self.degree);

        // **One aggregate for the whole query, not one per worker.** The previous version gave each
        // worker a table spanning the whole key space and merged twelve of them at the end; that
        // passed on latency and failed the memory gate by 3.9x, because a million distinct parties
        // became roughly twelve million live entries to produce a 57 MB answer. Here a key lives in
        // exactly one partition table however many threads touched it, so the aggregate's size is a
        // property of the data rather than of the core count. See [`crate::exec::agg::SharedAgg`].
        let shared = SharedAgg::default();
        let counted: Vec<(u64, u64)> = batches
            .par_iter()
            .try_fold(
                || (Scatter::default(), 0u64, 0u64),
                |mut st, batch| -> Result<(Scatter, u64, u64)> {
                    for (scan_ix, m) in *batch {
                        let (read, skipped) =
                            self.fold_morsel(*scan_ix, &scans, m, deadline, &mut st.0, &shared)?;
                        st.1 += read;
                        st.2 += skipped;
                    }
                    // Drained at every batch boundary, so a worker's buffer never outlives the batch
                    // that filled it and the fold state can be dropped without a final pass.
                    st.0.flush(&shared)?;
                    // Per batch, not per row. The first version read the clock once per merged
                    // entry - five million clock reads at ten thousand morsels, to enforce a budget
                    // that cannot change that fast. Checking a limit must not cost more than the
                    // work it is limiting.
                    self.check_budget(shared.bytes(), started)?;
                    Ok(st)
                },
            )
            .map(|r| r.map(|(_, read, skipped)| (read, skipped)))
            .collect::<Result<Vec<_>>>()?;
        let scan_ms = scan_started.elapsed().as_millis();

        // **The hot half, folded into the same aggregate.** §3.4 asks for the seam to be scheduled
        // as two pipelines into a shared sink, which is DuckDB's own `UNION ALL` model and is what
        // this is: the cold scan above and this both write into `shared`. The hot side is small by
        // construction - it is only what has not been sealed yet - so it is folded on this thread
        // rather than given a pipeline of its own.
        let mut hot_read = 0u64;
        let mut hot_skipped = 0u64;
        if let Some(seam) = self.seam {
            seam.snapshot.check_disjoint()?;
            let mut scatter = Scatter::default();
            for r in &seam.snapshot.rows {
                let Some(v) = r.value.as_deref() else {
                    hot_skipped += 1;
                    continue;
                };
                let text = v.trim();
                let Ok(d) = text.parse::<i128>() else {
                    if looks_numeric(text) {
                        return Err(BurrmillError::NotAllowed(format!(
                            "a hot row carries the value {text:?}, which is not a canonical integer"
                        )));
                    }
                    hot_skipped += 1;
                    continue;
                };
                hot_read += 1;
                let minus_d = checked_neg(d, &r.debit)?;
                scatter.push(r.credit.as_bytes(), d, &shared)?;
                scatter.push(r.debit.as_bytes(), minus_d, &shared)?;
            }
            scatter.flush(&shared)?;
            self.check_budget(shared.bytes(), started)?;
        }

        let agg_bytes = shared.bytes();

        let merge_started = Instant::now();
        let mut rows_read = 0u64;
        let mut rows_skipped = 0u64;
        for (read, skipped) in &counted {
            rows_read += read;
            rows_skipped += skipped;
        }
        rows_read += hot_read;
        rows_skipped += hot_skipped;

        // There is no merge left to do. The workers wrote into the answer as they went, so this is
        // the output phase and nothing else: each partition's table becomes rows, in parallel,
        // because a partition's keys never meet another's.
        let mut out = shared.into_rows(
            self.plan.drop_zero,
            self.plan.key_aliases.len(),
            self.plan.sum_aliases.len(),
        )?;

        let groups = out.len();
        if groups as u64 > self.limits.max_rows {
            return Err(BurrmillError::LimitExceeded(format!(
                "{groups} rows exceeds max_rows {}",
                self.limits.max_rows
            )));
        }

        let bytes = out.bytes();
        if bytes as u64 > self.limits.max_bytes {
            return Err(BurrmillError::LimitExceeded(format!(
                "the result is {} MB, over max_bytes {} MB",
                bytes >> 20,
                self.limits.max_bytes >> 20
            )));
        }

        // **Canonical ordering, applied unconditionally** (§3.3). Byte-wise ascending on the key.
        // Required here twice over: an oracle whose row order varies between runs is not one anybody
        // can build on, and the partitions come back in hash order, which is no order at all.
        out.sort_canonical();
        let merge_ms = merge_started.elapsed().as_millis();

        Ok((
            out,
            FoldMetrics {
                morsels: morsels.len(),
                rows_read,
                rows_skipped,
                groups,
                plan_ms,
                scan_ms,
                merge_ms,
                elapsed_ms: started.elapsed().as_millis(),
                agg_bytes,
                bytes_read,
            },
        ))
    }

    fn chunk_bytes(&self, scans: &[(usize, Vec<usize>)], morsels: &[(usize, Morsel)]) -> u64 {
        let mut seen = std::collections::HashSet::new();
        let mut total = 0u64;
        for (scan_ix, m) in morsels {
            if !seen.insert((*scan_ix, Arc::clone(&m.path), m.row_group)) {
                continue;
            }
            let arms = &scans[*scan_ix].1;
            let mask = projection_mask(&m.meta, self.plan, arms, self.seam.map(|s| s.block_col));
            let rg = m.meta.metadata().row_group(m.row_group);
            total += (0..rg.num_columns())
                .filter(|c| mask.as_ref().is_none_or(|k| k.leaf_included(*c)))
                .map(|c| rg.column(c).compressed_size().max(0) as u64)
                .sum::<u64>();
        }
        total
    }

    /// One morsel, folded into a table it shares with the rest of its batch.
    ///
    /// The unit of cancellation and of the deadline check; **not** the unit of the hash table, which
    /// is the batch. See [`coalesce`].
    fn fold_morsel(
        &self,
        scan_ix: usize,
        scans: &[(usize, Vec<usize>)],
        m: &Morsel,
        deadline: Instant,
        scatter: &mut Scatter,
        shared: &SharedAgg,
    ) -> Result<(u64, u64)> {
        let arms = &scans[scan_ix].1;
        if self.cancel.is_cancelled() {
            return Err(BurrmillError::Cancelled);
        }
        if Instant::now() > deadline {
            return Err(BurrmillError::Timeout);
        }

        let file = std::fs::File::open(&m.path).map_err(|e| {
            BurrmillError::Seam(format!(
                "cannot open segment {}: {e}. If a seal is in flight this may be a segment being \
                 written; Burrmill cannot tell that from a corrupt one, so it refuses rather than \
                 skipping rows. Segments must be installed atomically - see \
                 docs/upstream/nuthatch-segment-install-not-atomic.md",
                m.path.display()
            ))
        })?;
        // `new_with_metadata`, not `try_new`: the footer was parsed once when the morsels were cut,
        // and re-parsing it per morsel would put the cost back that parallelising the scan removed.
        // Sound because a sealed segment is content-addressed and immutable - the footer cannot go
        // stale under us the way a live file's could.
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, m.meta.clone())
            // 8192, not Arrow's 1024 default. DuckDB's vector is 2048; the fold is a tight loop over
            // three string columns and larger batches amortise the per-batch downcast without
            // pushing the working set out of L2.
            .with_batch_size(8192)
            .with_row_groups(vec![m.row_group])
            .with_offset(m.offset)
            .with_limit(m.len);
        // **Read three columns, not fourteen.** Without this the fold decodes every column in the
        // segment and uses the three it asked for. The synthetic fixture is four columns wide so the
        // waste was invisible; a real nest event is twelve to fourteen, two of them 64-character hex
        // hashes, and on `staking_legacy__stake_delegated` this was the difference between 2.2x
        // DuckDB and parity. Measured on the real nest, which is the only place it shows.
        let builder =
            match projection_mask(&m.meta, self.plan, arms, self.seam.map(|s| s.block_col)) {
                Some(mask) => builder.with_projection(mask),
                None => builder,
            };
        let reader = builder.build()?;

        let mut rows_read = 0u64;
        let mut rows_skipped = 0u64;

        for batch in reader {
            let batch = batch?;
            // **One decode and one parse per distinct value column, not per arm.** The degenerate
            // one-table-read-twice shape has two arms sharing a value column, and parsing the same
            // forty-digit text twice per row cost 14% of the fold when the generalisation first
            // landed: the benchmark went 104 ms to 120 and back again.
            let mut value_names: Vec<&str> = Vec::new();
            for i in arms {
                for fv in &self.plan.branches[*i].values {
                    let n = fv.col.as_str();
                    if !value_names.contains(&n) {
                        value_names.push(n);
                    }
                }
            }
            let mut value_cols: Vec<StringArray> = Vec::with_capacity(value_names.len());
            for n in &value_names {
                value_cols.push(utf8_column(&batch, n)?);
            }
            // **Simple and composite arms are separated here, not per row.** An arm whose key is
            // one bare column - which is every arm of the shape the gate measures - keeps a
            // `StringArray` in hand and a loop with nothing in it but the push. Deciding per row
            // instead, by reaching through a `Vec<Vec<Option<_>>>`, cost 20% of the fold; the
            // composite machinery must be free when it is not used.
            // **The one-sum, one-column arm gets its own list.** That is every arm of the shape the
            // gate measures and seven of the eight real folds, and putting it through a loop over a
            // one-element `Vec` with a bounds-checked `b.values[j]` per row cost 15%. New machinery
            // has to be free when it is not used; it was not, twice in this item alone.
            let mut simple1: Vec<(&crate::plan::FoldValue, StringArray, usize)> = Vec::new();
            let mut simple: Vec<(&crate::plan::FoldBranch, StringArray, Vec<usize>)> = Vec::new();
            type Composite<'b> = (
                &'b crate::plan::FoldBranch,
                Vec<Vec<Option<StringArray>>>,
                Vec<usize>,
            );
            let mut composite: Vec<Composite<'_>> = Vec::new();
            for i in arms {
                let b = &self.plan.branches[*i];
                // Where each of this arm's summed columns landed in the decoded set.
                let vis: Vec<usize> = b
                    .values
                    .iter()
                    .map(|fv| {
                        value_names
                            .iter()
                            .position(|n| *n == fv.col)
                            .expect("collected above")
                    })
                    .collect();
                if let (1, [crate::plan::KeyPart::Column { name, key_fn: None }]) =
                    (b.key.len(), b.key[0].parts.as_slice())
                {
                    if b.values.len() == 1 {
                        simple1.push((&b.values[0], utf8_column(&batch, name)?, vis[0]));
                    } else {
                        simple.push((b, utf8_column(&batch, name)?, vis));
                    }
                    continue;
                }
                let mut key_arrays = Vec::with_capacity(b.key.len());
                for kc in &b.key {
                    let mut parts = Vec::with_capacity(kc.parts.len());
                    for p in &kc.parts {
                        parts.push(match p {
                            crate::plan::KeyPart::Literal(_) => None,
                            crate::plan::KeyPart::Column { name, .. } => {
                                Some(utf8_column(&batch, name)?)
                            }
                        });
                    }
                    key_arrays.push(parts);
                }
                composite.push((b, key_arrays, vis));
            }
            // **Cold is filtered to the pinned watermark.** A segment sealed after the snapshot was
            // taken holds rows above it, and those rows are also in the hot half - counting both is
            // the double-count arm of COR-1.
            let blocks = match self.seam {
                Some(seam) => Some(block_column(&batch, seam.block_col)?),
                None => None,
            };

            let mut parsed: Vec<Option<i128>> = vec![None; value_cols.len()];
            let mut keybuf: Vec<u8> = Vec::new();
            let mut casebuf = String::new();
            for i in 0..batch.num_rows() {
                if let (Some(blocks), Some(seam)) = (&blocks, self.seam) {
                    // `None` means nothing has been sealed, so nothing in a segment can be cold.
                    if seam
                        .snapshot
                        .sealed_through
                        .is_none_or(|w| blocks.value(i) > w)
                    {
                        rows_skipped += 1;
                        continue;
                    }
                }
                rows_read += 1;
                for (vi, vc) in value_cols.iter().enumerate() {
                    parsed[vi] = None;
                    if vc.is_null(i) {
                        continue;
                    }
                    // Trimmed, because DuckDB trims and " 7" is seven. The generated corpus found
                    // that within four hundred cases: the row was being silently skipped and a
                    // balance came back short.
                    let text = vc.value(i).trim();
                    match text.parse::<i128>() {
                        Ok(d) => parsed[vi] = Some(d),
                        Err(_) => {
                            // A plain `CAST` errors where `TRY_CAST` yields NULL, and the plan
                            // carries which was written. Reading one as the other would change an
                            // answer silently, which is the thing this engine does not do. A value
                            // carrying digits is refused either way rather than guessed at
                            // (roadmap 2.1a).
                            let uses_strictly = |b: &&crate::plan::FoldBranch, xs: &Vec<usize>| {
                                xs.iter()
                                    .enumerate()
                                    .any(|(j, x)| *x == vi && b.values[j].strict_cast)
                            };
                            let strict =
                                simple1.iter().any(|(fv, _, x)| *x == vi && fv.strict_cast)
                                    || simple.iter().any(|(b, _, xs)| uses_strictly(b, xs))
                                    || composite.iter().any(|(b, _, xs)| uses_strictly(b, xs));
                            if strict || looks_numeric(text) {
                                return Err(BurrmillError::NotAllowed(format!(
                                    "the value {text:?} will not cast to a 128-bit integer{}",
                                    if strict {
                                        ". The query used CAST, which errors; TRY_CAST would skip it"
                                    } else {
                                        ". It carries digits, so Burrmill refuses rather than \
                                         guessing at what was meant"
                                    }
                                )));
                            }
                        }
                    }
                }
                for (fv, key, vi) in simple1.iter() {
                    let Some(d) = parsed[*vi] else {
                        rows_skipped += 1;
                        continue;
                    };
                    if key.is_null(i) {
                        return Err(null_key());
                    }
                    let raw = key.value(i);
                    let signed = if fv.negated { checked_neg(d, raw)? } else { d };
                    scatter.push(raw.as_bytes(), signed, shared)?;
                }
                for (b, key, vis) in simple.iter() {
                    if key.is_null(i) && vis.iter().any(|vi| parsed[*vi].is_some()) {
                        return Err(null_key());
                    }
                    let raw = key.value(i);
                    for (j, vi) in vis.iter().enumerate() {
                        let Some(d) = parsed[*vi] else {
                            rows_skipped += 1;
                            continue;
                        };
                        let fv = &b.values[j];
                        let signed = if fv.negated { checked_neg(d, raw)? } else { d };
                        scatter.push_agg(raw.as_bytes(), j as u16, signed, shared)?;
                    }
                }
                for (b, key_arrays, vis) in composite.iter() {
                    if vis.iter().all(|vi| parsed[*vi].is_none()) {
                        rows_skipped += 1;
                        continue;
                    }
                    // **Length-prefixed only when the key is actually composite**, because that is
                    // exactly when `Rows` un-prefixes it. `("ab","c")` and `("a","bc")` are
                    // different keys and any separator byte can turn up in data somebody has not
                    // shown me yet, so four bytes of length beats a delimiter and a hope.
                    //
                    // Deciding to prefix on "did we take the fast path" instead put a length in
                    // front of every `lower(addr)` key and handed the caller `"*\0\0\00xabcd..."`.
                    // Two halves of one decision must share one condition.
                    let prefixed = b.key.len() > 1;
                    keybuf.clear();
                    for (kc, arrays) in b.key.iter().zip(key_arrays.iter()) {
                        let start = keybuf.len();
                        if prefixed {
                            keybuf.extend_from_slice(&[0u8; 4]);
                        }
                        for (part, arr) in kc.parts.iter().zip(arrays.iter()) {
                            match (part, arr) {
                                (crate::plan::KeyPart::Literal(l), _) => {
                                    keybuf.extend_from_slice(l.as_bytes())
                                }
                                (crate::plan::KeyPart::Column { key_fn, .. }, Some(a)) => {
                                    if a.is_null(i) {
                                        return Err(null_key());
                                    }
                                    let v = a.value(i);
                                    match key_fn {
                                        None => keybuf.extend_from_slice(v.as_bytes()),
                                        Some(kf) => {
                                            fold_case(v, *kf, &mut casebuf);
                                            keybuf.extend_from_slice(casebuf.as_bytes());
                                        }
                                    }
                                }
                                (crate::plan::KeyPart::Column { name, .. }, None) => {
                                    return Err(BurrmillError::Substrate(format!(
                                        "the key column `{name}` decoded to nothing"
                                    )));
                                }
                            }
                        }
                        if prefixed {
                            let n = (keybuf.len() - start - 4) as u32;
                            keybuf[start..start + 4].copy_from_slice(&n.to_le_bytes());
                        }
                    }
                    for (j, vi) in vis.iter().enumerate() {
                        let Some(d) = parsed[*vi] else {
                            rows_skipped += 1;
                            continue;
                        };
                        let fv = &b.values[j];
                        let signed = if fv.negated {
                            checked_neg(d, &String::from_utf8_lossy(&keybuf))?
                        } else {
                            d
                        };
                        scatter.push_agg(&keybuf, j as u16, signed, shared)?;
                    }
                }
            }
        }
        Ok((rows_read, rows_skipped))
    }

    /// The budget is now the query's whole aggregation rather than one worker's share of it.
    ///
    /// This used to read `mem_pool_bytes * threads` in practice and said so, because twelve workers
    /// each held a table of their own. With a single shared aggregate the number passed in is the
    /// real total, which is most of roadmap 1.3. It is still not process RSS: Parquet decode buffers
    /// and the Arrow batches in flight sit outside it, and the doc comment says so rather than
    /// implying a guarantee the operator cannot make.
    fn check_budget(&self, bytes: usize, started: Instant) -> Result<()> {
        if bytes as u64 > self.limits.mem_pool_bytes {
            return Err(BurrmillError::LimitExceeded(format!(
                "the query's aggregation is {} MB, over the mem_pool_bytes budget of {} MB",
                bytes >> 20,
                self.limits.mem_pool_bytes >> 20
            )));
        }
        if started.elapsed() > self.limits.timeout {
            return Err(BurrmillError::Timeout);
        }
        Ok(())
    }
}

/// Read a column as Utf8 whichever string layout the writer chose.
///
/// Parquet readers disagree here and it is a real migration hazard rather than a curiosity: nuthatch
/// seals `Utf8`, DataFusion reads the same bytes back as `Utf8View`. Casting rather than assuming
/// keeps the answer about the answer.
/// A NULL key's slot reads as `""`, which would merge the NULL party into the empty-string one.
/// `Rows` has no NULL key to give it, so the fold refuses rather than answers for either.
fn null_key() -> BurrmillError {
    BurrmillError::NotAllowed("a group key is NULL; the owned fold does not group NULLs".into())
}

fn utf8_column(batch: &RecordBatch, name: &str) -> Result<StringArray> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| BurrmillError::Substrate(format!("no column `{name}` in the segment")))?;
    let col = batch.column(idx);
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(s.clone());
    }
    let cast = arrow::compute::cast(col, &DataType::Utf8)?;
    cast.as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| BurrmillError::Substrate(format!("column `{name}` will not read as Utf8")))
}

/// The result as Arrow.
///
/// The sum is `Decimal128(38, 0)`, which is exactly i128 - not a float, and not text. uint256 needs
/// 78 decimal digits and `Decimal256` stops at 76, which is why the *stored* value stays text and
/// only the fold's output is a decimal.
pub fn to_record_batch(rows: &Rows, plan: &SignedFold) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(&plan.key_alias, DataType::Utf8, false),
        Field::new(&plan.sum_alias, DataType::Decimal128(38, 0), false),
    ]));
    let keys: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|(k, _)| k)));
    let sums: ArrayRef = Arc::new(
        Decimal128Array::from_iter_values(rows.iter().map(|(_, v)| v))
            .with_precision_and_scale(38, 0)?,
    );
    Ok(RecordBatch::try_new(schema, vec![keys, sums])?)
}

/// Group morsels into work units big enough to be worth a hash table of their own.
///
/// **This is the bimodal segment layout showing up as a performance bug.** With ten thousand
/// segments of two hundred rows each, one table per morsel meant ten thousand freshly allocated maps
/// each growing to roughly five hundred entries - so nearly every key in the fold allocated its own
/// boxed string instead of hitting an existing entry, four million allocations to answer a query
/// over two million rows. Measured cost: 1.05x DuckDB where the same operator was 0.55x on one
/// segment. The rows had not changed; only how they were filed.
///
/// A batch therefore targets a row budget rather than a file. The budget is derived from the
/// available parallelism rather than fixed, because a batch large enough to amortise allocation is
/// also large enough to starve the thread pool if the table is small.
/// The leaf columns the fold actually reads, as a Parquet projection.
///
/// `None` when the schema is not flat or a name is missing, in which case the reader falls back to
/// decoding everything - slower, never wrong. A sealed segment's schema is flat today, so this is a
/// guard against a future layout rather than a live branch, and [`SealedSegments`] has no path
/// dependency that would warn us if that changed.
fn projection_mask(
    meta: &parquet::arrow::arrow_reader::ArrowReaderMetadata,
    plan: &SignedFold,
    arms: &[usize],
    block_col: Option<&str>,
) -> Option<ProjectionMask> {
    let descr = meta.metadata().file_metadata().schema_descr();
    let root = descr.root_schema();
    // Flat only. A nested group would make root index and leaf index disagree, and a projection that
    // silently selects the wrong column is exactly the failure this project exists to refuse.
    if root.get_fields().iter().any(|f| !f.is_primitive()) {
        return None;
    }
    let index_of = |name: &str| root.get_fields().iter().position(|f| f.name() == name);
    let mut idx = Vec::with_capacity(4 * arms.len() + 1);
    for i in arms {
        let b = &plan.branches[*i];
        for kc in &b.key {
            for p in &kc.parts {
                if let crate::plan::KeyPart::Column { name, .. } = p {
                    idx.push(index_of(name)?);
                }
            }
        }
        for fv in &b.values {
            idx.push(index_of(&fv.col)?);
        }
    }
    // The seam needs the block column to filter cold rows to the pinned watermark, so it is part of
    // the projection exactly when there is a seam and not otherwise.
    if let Some(b) = block_col {
        idx.push(index_of(b)?);
    }
    idx.sort_unstable();
    idx.dedup();
    Some(ProjectionMask::roots(descr, idx))
}

fn coalesce(morsels: &[(usize, Morsel)], degree: Option<usize>) -> Vec<&[(usize, Morsel)]> {
    let total: usize = morsels.iter().map(|(_, m)| m.len).sum();
    let threads = rayon::current_num_threads().max(1);

    // **Two genuinely different cases, and conflating them is what 5.2 measured.**
    //
    // Alone - `degree` is `None` - decompose four ways per thread so work-stealing can even out the
    // bimodal seal sizes, and let rayon use every worker. That is the behaviour the latency gate
    // measures and it is untouched.
    //
    // Sharing - `degree` is `Some(d)` - and the group count **is** the width cap, because rayon
    // cannot run more groups at once than there are. So it must be exactly `d`. Over-decomposing
    // here would hand the query the whole pool again and undo the point of admitting it narrowly.
    // **Over-decomposed even when sharing, and the gate is why that is safe.** The first version
    // used exactly `d` groups, which caps the width perfectly and balances terribly: two coarse
    // groups of 250k rows leave a worker idle for whatever the slower one runs over by. Measured,
    // that idling was most of the gap - eight threads were only 66% busy against a work-bound of
    // 151 qps. Splitting each share four ways costs nothing in fairness because **admission** is
    // what bounds concurrency now, not decomposition.
    // Two, swept rather than picked, at 32 clients on a 32-core box (two runs each):
    //
    // | groups per share | qps | worst p99 | fair |
    // |---|---:|---:|---:|
    // | d x 1 | 101, 104 | 432, 451 ms | 0.89, 0.94 |
    // | **d x 2** | **112, 113** | **446, 402 ms** | **0.95** |
    // | d x 3 | 89, 91 | 515, 526 ms | 0.81 |
    //
    // A sharp optimum rather than a trend: one group per worker balances badly, three brings the
    // per-group fixed cost back. It is a constant and not a setting, because a library that claims
    // nothing to configure should not grow a knob the moment a number is inconvenient.
    const OVERSUB: usize = 2;
    let (groups, target) = match degree {
        Some(d) => (d * OVERSUB, (total / (d * OVERSUB).max(1)).max(1)),
        None => (
            usize::MAX,
            (total / (threads * 4)).clamp(4_096, crate::segment::MORSEL_ROWS),
        ),
    };

    let mut out = Vec::new();
    let mut start = 0usize;
    let mut rows = 0usize;
    for (i, (_, m)) in morsels.iter().enumerate() {
        rows += m.len;
        // When sharing, the last group takes the remainder rather than splitting further, so the
        // count is exactly `groups` and the width cap actually holds.
        if rows >= target && out.len() + 1 < groups {
            out.push(&morsels[start..=i]);
            start = i + 1;
            rows = 0;
        }
    }
    if start < morsels.len() {
        out.push(&morsels[start..]);
    }
    out
}

/// Does this text carry a number that is not a canonical integer?
///
/// Deliberately loose, and deliberately erring towards **refusal** rather than towards silence: a
/// false positive costs a query and says exactly why, a false negative silently drops a row and
/// shortens a balance. `"1e18"`, `"7.9"` and `"1_000"` are caught; `"not a number"` and `""` are
/// not, and stay `TRY_CAST` NULLs as the query asked.
fn looks_numeric(text: &str) -> bool {
    !text.is_empty()
        && text.bytes().any(|b| b.is_ascii_digit())
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.' | b'_' | b'e' | b'E'))
}

/// The block column as `u64`, whatever width the writer chose.
///
/// Cast rather than assumed, for the same reason [`utf8_column`] casts: nuthatch seals `UInt64`
/// today and a future one might not, and a seam that silently misreads a block number would produce
/// a wrong balance rather than an error.
fn block_column(batch: &RecordBatch, name: &str) -> Result<arrow::array::UInt64Array> {
    let col = batch.column_by_name(name).ok_or_else(|| {
        BurrmillError::Seam(format!(
            "the segment has no `{name}` column, so the \
             hot/cold boundary cannot be applied to it"
        ))
    })?;
    let cast = arrow::compute::cast(col, &DataType::UInt64)?;
    Ok(cast
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .ok_or_else(|| BurrmillError::Seam(format!("`{name}` will not cast to UInt64")))?
        .clone())
}

/// Apply an admitted key function into a reusable buffer.
///
/// The ASCII fast path is not an approximation: for a string that is entirely ASCII, Unicode case
/// folding and ASCII case folding agree by definition, and blockchain addresses are hex. Anything
/// else takes the full Unicode path, because a key that is *nearly* right is a party's balance split
/// in two.
#[inline]
fn fold_case(s: &str, kf: crate::plan::KeyFn, out: &mut String) {
    out.clear();
    match (kf, s.is_ascii()) {
        (crate::plan::KeyFn::Lower, true) => out.extend(s.chars().map(|c| c.to_ascii_lowercase())),
        (crate::plan::KeyFn::Upper, true) => out.extend(s.chars().map(|c| c.to_ascii_uppercase())),
        (crate::plan::KeyFn::Lower, false) => out.extend(s.chars().flat_map(|c| c.to_lowercase())),
        (crate::plan::KeyFn::Upper, false) => out.extend(s.chars().flat_map(|c| c.to_uppercase())),
    }
}
