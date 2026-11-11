//! The owned aggregation table.
//!
//! This file is the difference between "a fold written in Rust" and "an aggregation we own", and it
//! exists because the measurements said so. Three versions preceded it, each faster than the last
//! and each still losing to DuckDB above about ten thousand groups:
//!
//! | version | 200k groups | 1M groups |
//! |---|---|---|
//! | one `FxHashMap<Box<str>, _>` per work unit, serial merge | 3.11x DuckDB | 2.88x |
//! | one per thread, parallel tree merge | 1.96x | 2.29x |
//! | this: arena keys, radix partitions, partition-wise merge | see `docs/bench` | |
//!
//! Two things were wrong with the map-per-thread version, and they are the two things DuckDB does
//! differently:
//!
//! 1. **A heap allocation per distinct key.** A `Box<str>` for a forty-two byte address is a malloc,
//!    and at a million distinct parties across twelve worker tables that is on the order of twelve
//!    million of them to answer a query over two million rows. Here the key bytes are appended to a
//!    per-partition arena and the table holds an offset and a length, so a new group costs a memcpy
//!    into a `Vec<u8>` that is already warm.
//! 2. **Every table spanning the whole key space.** Twelve tables of a million entries each is both
//!    cache-hostile and, under a 256 MB budget, simply too much. Radix partitioning on the high bits
//!    of the hash means a partition's table is a sixty-fourth of the key space, the merge is
//!    parallel across partitions rather than serial across tables, and each partition is freed as
//!    soon as its rows are produced.

use hashbrown::HashTable;
use rayon::prelude::*;
use rustc_hash::FxHasher;
use std::hash::Hasher;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{BurrmillError, Result};
use crate::exec::checked::checked_add;

/// Radix partitions. Sixty-four is a compromise: enough that a partition's table stays small and
/// the merge has more units than the machine has cores, few enough that an arena per partition per
/// thread is not itself the memory problem.
pub const PARTITIONS: usize = 64;

#[derive(Clone, Copy)]
struct Entry {
    /// Kept rather than recomputed. Eight bytes against re-hashing forty-two on every resize, and
    /// the entry is padded to thirty-two either way.
    hash: u64,
    off: u32,
    /// Key length, with **bit 31 meaning "this entry has overflowed `i128` at least once"**. Keys
    /// are addresses; a 2 GB one is not a thing. See [`PartTable::wide`].
    len: u32,
    /// The low 128 bits of the running sum, which for an entry that has never overflowed *is* the
    /// running sum.
    sum: i128,
}

const WIDE: u32 = 1 << 31;

/// Hash, with the high bits deliberately mixed.
///
/// Both consumers here read the *top* bits - hashbrown for its control byte, and the radix split for
/// the partition index. A hash whose entropy sits in the low bits would put every key in one
/// partition and turn the parallel merge back into a serial one, silently and only at scale.
#[inline]
pub fn hash_key(k: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write(k);
    let x = h.finish();
    // splitmix64's finaliser. Cheap, and it makes the top bits as good as the bottom.
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[inline]
pub fn partition_of(hash: u64) -> usize {
    (hash >> 58) as usize & (PARTITIONS - 1)
}

/// One partition's table: a contiguous key arena plus a SwissTable of offsets into it.
#[derive(Default)]
pub struct PartTable {
    arena: Vec<u8>,
    table: HashTable<Entry>,
    /// Aggregates past the first, keyed by `(arena offset, aggregate index)`.
    ///
    /// **Empty for every query the gate measures**, and that is the design. A fold with one `SUM` -
    /// which is seven of the eight folds in a real workload - never touches this, so `Entry` keeps
    /// its single inline `i128` and the hot loop is exactly what it was. A second `SUM` costs a hash
    /// lookup per row on that aggregate only. Widening `Entry` to carry m sums inline would have put
    /// sixteen bytes per group on every query to serve one of them.
    extra: rustc_hash::FxHashMap<(u32, u16), i128>,
    /// The high 64 bits of the running sum, for the entries that need one, keyed by arena offset
    /// because that is the one thing about an entry that never moves.
    ///
    /// **Empty for every aggregate anyone will ever run.** A party whose running total leaves the
    /// `i128` range is not a thing a real nest produces; this exists so that when it does happen the
    /// refusal depends on the *answer* rather than on the order the rows arrived in. Paying sixteen
    /// bytes per group to widen every entry would have cost ~28 MB at a million groups against a
    /// budget already being missed, to carry a number that is zero everywhere.
    wide: rustc_hash::FxHashMap<u32, i64>,
}

impl std::fmt::Debug for PartTable {
    /// Deliberately shows shape and not contents: a debug print of a million-entry aggregate helps
    /// nobody, and the arena is bytes rather than anything a person reads.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartTable")
            .field("groups", &self.table.len())
            .field("arena_bytes", &self.arena.len())
            .finish()
    }
}

impl PartTable {
    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Approximate live bytes. Used for the memory budget, so it counts what actually grows: the
    /// arena, and the table's entries at SwissTable's load factor.
    pub fn bytes(&self) -> usize {
        self.arena.capacity()
            + self.table.capacity() * (std::mem::size_of::<Entry>() + 1)
            + self.wide.capacity() * std::mem::size_of::<(u32, i64)>()
            + self.extra.capacity() * std::mem::size_of::<((u32, u16), i128)>()
    }

    #[inline]
    pub fn add(&mut self, hash: u64, key: &[u8], agg: u16, v: i128) -> Result<()> {
        // Split the borrow explicitly: the equality closure reads the arena while the table is held
        // mutably, which only type-checks because these are two distinct fields.
        let Self {
            arena,
            table,
            wide,
            extra,
        } = self;
        let klen = key.len() as u32;
        let eq = |e: &Entry| {
            e.hash == hash
                && (e.len & !WIDE) == klen
                && &arena[e.off as usize..e.off as usize + key.len()] == key
        };
        if let Some(e) = table.find_mut(hash, eq) {
            if agg != 0 {
                let slot = extra.entry((e.off, agg)).or_insert(0);
                *slot = checked_add(*slot, v, "aggregate")?;
                return Ok(());
            }
            if e.len & WIDE == 0 {
                // The common path, and the only one that ever runs on real data: one checked add.
                if let Some(s) = e.sum.checked_add(v) {
                    e.sum = s;
                    return Ok(());
                }
                // First overflow for this key. Widen it rather than refusing, because the running
                // total leaving the range says nothing about whether the *answer* will.
                //
                // **Sign-extended, not zeroed.** The value is `hi * 2^128 + (lo as u128)`, so an
                // entry whose running sum is negative starts at a high word of -1; starting it at
                // zero would silently reinterpret -5 as 2^128 - 5. Caught by the generated corpus
                // within a second of being written, which is the entire argument for having it.
                e.len |= WIDE;
                wide.insert(e.off, (e.sum >> 127) as i64);
            }
            let hi = wide
                .get_mut(&e.off)
                .expect("a wide entry always has a high word");
            *hi = wide_add(hi, &mut e.sum, v)?;
            return Ok(());
        }
        let off = arena.len() as u32;
        arena.extend_from_slice(key);
        // A first sighting via a non-zero aggregate still needs the entry, with a zero in the
        // inline slot: the row exists, it simply has nothing in aggregate zero yet.
        let inline = if agg == 0 { v } else { 0 };
        table.insert_unique(
            hash,
            Entry {
                hash,
                off,
                len: klen,
                sum: inline,
            },
            |e| e.hash,
        );
        if agg != 0 {
            extra.insert((off, agg), v);
        }
        Ok(())
    }

    /// Consume the table into an index over its own arena, applying `HAVING SUM(d) <> 0`.
    ///
    /// **The arena is moved out, not copied.** The previous version built a `Box<str>` per group,
    /// which put a million individual forty-two byte mallocs between the aggregate and the answer -
    /// the exact per-key allocation this module's header says it removed, reintroduced at the output
    /// and costing more there than it ever did in the table. The hash table is dropped here, so a
    /// partition's buckets are freed the moment its rows exist.
    fn into_index(self, drop_zero: bool, sum_arity: usize) -> Result<PartIndex> {
        let Self {
            arena,
            table,
            wide,
            extra,
        } = self;
        let mut idx = Vec::with_capacity(table.len());
        for e in table {
            // **The refusal happens here, and only here.** An entry is refused when its *answer*
            // will not fit, not when some partial sum did not. `MAX, +1, -1` sums to exactly `MAX`
            // and is now returned; `MAX, +1` is refused, whatever order the rows arrived in.
            let sum = if e.len & WIDE == 0 {
                e.sum
            } else {
                let hi = *wide
                    .get(&e.off)
                    .expect("a wide entry always has a high word");
                narrow(hi, e.sum).ok_or_else(|| {
                    BurrmillError::Overflow(format!(
                        "the sum for one party does not fit in i128 (high word {hi})"
                    ))
                })?
            };
            if !drop_zero || sum != 0 {
                let mut all = Vec::with_capacity(sum_arity);
                all.push(sum);
                for a in 1..sum_arity {
                    all.push(extra.get(&(e.off, a as u16)).copied().unwrap_or(0));
                }
                idx.push((e.off, e.len & !WIDE, all));
            }
        }
        Ok((arena, idx))
    }
}

/// Add `v` into the 192-bit accumulator `(hi, lo)`, where the value is `hi * 2^128 + lo` and `lo`
/// is the `i128`'s bits read as unsigned.
///
/// Textbook multi-limb addition, and the only subtle part is that `v` sign-extends into the high
/// word: a negative `v` contributes `-1` there before any carry out of the low word.
#[inline]
fn wide_add(hi: &mut i64, lo: &mut i128, v: i128) -> Result<i64> {
    let (new_lo, carry) = (*lo as u128).overflowing_add(v as u128);
    let ext = (v >> 127) as i64; // -1 for negative, 0 for non-negative
    let new_hi = hi
        .checked_add(ext)
        .and_then(|h| h.checked_add(carry as i64))
        .ok_or_else(|| {
            // 2^63 additions past the range. Unreachable in this universe, refused rather than
            // wrapped anyway, because "unreachable" is how wrong answers get in.
            BurrmillError::Overflow("the high word of an aggregate overflowed i64".into())
        })?;
    *lo = new_lo as i128;
    Ok(new_hi)
}

/// The 192-bit value `hi * 2^128 + lo` as an `i128`, or `None` if it does not fit.
///
/// It fits in exactly two cases: the value is non-negative and the high word is zero, or it is
/// negative and the high word is the sign extension of a low word at or past `2^127`.
#[inline]
fn narrow(hi: i64, lo: i128) -> Option<i128> {
    let u = lo as u128;
    match hi {
        0 if u <= i128::MAX as u128 => Some(u as i128),
        -1 if u >= 1u128 << 127 => Some(u as i128),
        _ => None,
    }
}

/// One row of the answer: which partition arena holds its key, where, and the exact sum.
///
/// Thirty-two bytes, which is what an `i128`'s alignment costs whatever else is in the struct, and
/// the same width as the `(Box<str>, i128)` this replaced. The saving is not the width, it is the
/// million individual mallocs that are no longer behind it.
#[derive(Clone, Copy)]
struct Row {
    part: u32,
    off: u32,
    len: u32,
    sum: i128,
}

/// The answer, keyed into the partition arenas it was aggregated in.
///
/// A result set that copies each key into its own allocation carries two copies of the answer at the
/// moment it can least afford to - peak memory is exactly when the aggregate is complete and the
/// rows are being built - and then makes the canonical sort chase a pointer per comparison.
///
/// **The arenas are moved across, never copied, and this took three attempts.** A `Box<str>` per
/// group was the original and was the worst of both. Concatenating the sixty-four arenas into one
/// was the second: it sorted fast but allocated a second 42 MB copy of every key and then freed it,
/// and glibc holds a free that size rather than returning it, so it showed up in peak RSS twice
/// over. The third keeps the arenas where they are and hoists their base pointers into a flat slice
/// before sorting, which is the difference between two bounds-checked indirections per comparison
/// and one against sixty-four pointers that stay in L1. That was the whole reason the second attempt
/// looked necessary.
pub struct Rows {
    arenas: Vec<Vec<u8>>,
    idx: Vec<Row>,
    /// Aggregates past the first, stride `sum_arity - 1`. Empty for a single-`SUM` fold.
    extra_sums: Vec<i128>,
    /// How many `SUM` columns each row carries.
    sum_arity: usize,
    /// How many group-key columns each key carries.
    ///
    /// One is the overwhelming case and its bytes are the key verbatim, so nothing about it changed.
    /// Above one, the executor writes each column length-prefixed - `("ab","c")` and `("a","bc")`
    /// are different keys and any separator byte can turn up in data - and [`Rows::key_parts`]
    /// reads them back.
    key_arity: usize,
}

impl std::fmt::Debug for Rows {
    /// Shape, not contents: a debug print of a million-row answer helps nobody, and the arenas are
    /// bytes rather than anything a person reads.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rows")
            .field("rows", &self.idx.len())
            .field(
                "arena_bytes",
                &self.arenas.iter().map(|a| a.len()).sum::<usize>(),
            )
            .finish()
    }
}

impl Rows {
    pub fn len(&self) -> usize {
        self.idx.len()
    }

    pub fn is_empty(&self) -> bool {
        self.idx.is_empty()
    }

    #[inline]
    fn bytes_at<'a>(bases: &[&'a [u8]], r: &Row) -> &'a [u8] {
        &bases[r.part as usize][r.off as usize..r.off as usize + r.len as usize]
    }

    pub fn key_arity(&self) -> usize {
        self.key_arity
    }

    #[inline]
    fn raw(&self, i: usize) -> &[u8] {
        let r = &self.idx[i];
        &self.arenas[r.part as usize][r.off as usize..r.off as usize + r.len as usize]
    }

    /// The group key as text. For a composite key this is the **first** column; use
    /// [`Rows::key_parts`] for the rest, and [`Rows::key_arity`] to know whether there are any.
    #[inline]
    pub fn key(&self, i: usize) -> &str {
        if self.key_arity <= 1 {
            // Lossy would silently corrupt an address. The bytes came out of a Utf8 Arrow array, so
            // this cannot fail; if it ever did, a mangled key is worse than a panic.
            return std::str::from_utf8(self.raw(i))
                .expect("keys are copied verbatim out of a Utf8 array");
        }
        self.key_parts(i).next().unwrap_or("")
    }

    /// Every column of a composite group key, in order.
    pub fn key_parts(&self, i: usize) -> impl Iterator<Item = &str> + '_ {
        let mut rest = self.raw(i);
        let single = self.key_arity <= 1;
        std::iter::from_fn(move || {
            if rest.is_empty() {
                return None;
            }
            if single {
                let all = rest;
                rest = &[];
                return std::str::from_utf8(all).ok();
            }
            let (len_bytes, tail) = rest.split_at(4);
            let n = u32::from_le_bytes(len_bytes.try_into().expect("split_at(4)")) as usize;
            let (head, tail) = tail.split_at(n);
            rest = tail;
            std::str::from_utf8(head).ok()
        })
    }

    /// The first summed column.
    #[inline]
    pub fn sum(&self, i: usize) -> i128 {
        self.idx[i].sum
    }

    pub fn sum_arity(&self) -> usize {
        self.sum_arity
    }

    /// Summed column `j` of row `i`.
    #[inline]
    pub fn sum_at(&self, i: usize, j: usize) -> i128 {
        if j == 0 {
            return self.idx[i].sum;
        }
        self.extra_sums[i * (self.sum_arity - 1) + (j - 1)]
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, i128)> + '_ {
        (0..self.len()).map(move |i| (self.key(i), self.idx[i].sum))
    }

    /// Live bytes of the answer itself. Feeds `max_bytes`, which until now was a field in
    /// `Limits` that nothing read.
    pub fn bytes(&self) -> usize {
        self.arenas.iter().map(|a| a.capacity()).sum::<usize>()
            + self.idx.capacity() * std::mem::size_of::<Row>()
            + self.extra_sums.capacity() * std::mem::size_of::<i128>()
    }

    /// **Canonical ordering, byte-wise ascending on the key** (RFC-0044 §3.3).
    ///
    /// The base pointers are hoisted out of the `Vec<Vec<u8>>` first. Indexing the nested vector
    /// inside the comparator measured 79 ms against 26 ms for a sort over a single flat arena; a
    /// sixty-four entry slice of `&[u8]` costs one bounds check and stays resident, and gets that
    /// back without duplicating 42 MB of keys to do it.
    ///
    /// `par_sort_unstable_by` rather than the stable sort on purpose: it is an in-place parallel
    /// quicksort and allocates no scratch, and the keys are distinct so stability decides nothing.
    pub fn sort_canonical(&mut self) {
        // **With extra aggregates the sort has to carry them.** They are addressed by row position,
        // so sorting `idx` alone would leave every row past the first sum pointing at somebody
        // else's number - a wrong balance with no error, which is the failure this engine exists to
        // refuse. Sorting a permutation and applying it is the price of keeping `Row` at 32 bytes
        // for the single-`SUM` case, and that case does not pay it.
        if self.sum_arity > 1 {
            let stride = self.sum_arity - 1;
            let bases: Vec<&[u8]> = self.arenas.iter().map(|a| a.as_slice()).collect();
            let mut order: Vec<u32> = (0..self.idx.len() as u32).collect();
            order.par_sort_unstable_by(|a, b| {
                Self::bytes_at(&bases, &self.idx[*a as usize])
                    .cmp(Self::bytes_at(&bases, &self.idx[*b as usize]))
            });
            drop(bases);
            let old_idx = std::mem::take(&mut self.idx);
            let old_extra = std::mem::take(&mut self.extra_sums);
            self.idx.reserve(old_idx.len());
            self.extra_sums.reserve(old_extra.len());
            for o in &order {
                self.idx.push(old_idx[*o as usize]);
                let base = *o as usize * stride;
                self.extra_sums
                    .extend_from_slice(&old_extra[base..base + stride]);
            }
            return;
        }
        let Self { arenas, idx, .. } = self;
        let bases: Vec<&[u8]> = arenas.iter().map(|a| a.as_slice()).collect();
        idx.par_sort_unstable_by(|a, b| Self::bytes_at(&bases, a).cmp(Self::bytes_at(&bases, b)));
    }
}

/// A partition's keys and its index into them, handed over when the aggregate becomes an answer.
type PartIndex = (Vec<u8>, Vec<(u32, u32, Vec<i128>)>);

/// A row waiting to be applied, keyed into its [`Scatter`]'s own arena.
#[derive(Clone, Copy)]
struct Pending {
    hash: u64,
    off: u32,
    /// Key length in the low sixteen bits, **aggregate index in the high sixteen**.
    ///
    /// Packed rather than given a field of its own, because `i128`'s alignment means a bare `u16`
    /// costs sixteen bytes: `Pending` went 32 to 48 and every worker's scatter buffer with it. Keys
    /// are addresses, so sixteen bits of length is 65,535 bytes of one and nobody has such an
    /// address.
    len_agg: u32,
    val: i128,
}

const LEN_MASK: u32 = 0xFFFF;

/// Rows a worker buffers before it takes any lock.
///
/// The whole cost of a shared aggregate is the lock, so it has to be amortised over enough rows that
/// it disappears, and the two regimes this has to survive are far apart. A synthetic segment is
/// twenty thousand rows; a real nest segment averages **a hundred and sixteen**, so flushing per
/// batch would take sixty-four locks for every hundred-odd rows. Buffering across morsels instead
/// makes the flush rate a function of rows rather than of how the writer happened to cut the files.
///
/// **Sixteen thousand three hundred and eighty-four, and the first answer was tuned at the wrong
/// operating point.** Every worker holds one of these, so the constant is multiplied by the fold's
/// width and shows up in peak RSS - which is how it came to be swept at a million groups on
/// thirty-two threads, where memory was the binding constraint and 4,096 won:
///
/// | `FLUSH_ROWS` | peak RSS, 32 threads | peak RSS, 8 threads | latency |
/// |---|---:|---:|---:|
/// | 1 024 | 335 MB | 204 MB | 179 ms |
/// | 4 096 | 346 MB | 204 MB | 158 ms |
/// | 16 384 | 380 MB | 228 MB | 161 ms |
/// | 65 536 | 467 MB | 270 MB | 177 ms |
///
/// That operating point is not the serving one. A serving query is *small*, and a small query is
/// where the flush size stops being a memory knob and becomes a **contention** knob: each flush
/// takes up to sixty-four partition locks, so a worker that flushes four times as often contends
/// four times as much. On a 500k-row query at eight threads the scan goes **18 ms to 11**, and the
/// single-threaded cost does not move at all - which is exactly the signature of contention rather
/// than work.
///
/// The serving sweep agrees: 123 → 133-143 qps at four clients, 112 → 115-124 at thirty-two, with
/// better tails and the same fairness. The memory gate holds at 223-226 MB against 218, which is
/// six megabytes for eight to sixteen per cent of throughput.
///
/// Sixty-five thousand is worse on both counts, so this is an optimum and not a direction.
const FLUSH_ROWS: usize = 16_384;

/// One worker's outbound buffer: rows sorted into partitions, not yet aggregated.
///
/// It owns its key bytes rather than borrowing them from the Arrow batch, which costs one memcpy per
/// row - about eight milliseconds on two million rows - and buys the freedom to buffer across morsel
/// boundaries. Borrowing would tie the flush to the lifetime of a batch, and on a real nest a batch
/// is a hundred rows.
pub struct Scatter {
    arena: Vec<u8>,
    parts: Vec<Vec<Pending>>,
    pending: usize,
}

impl Default for Scatter {
    fn default() -> Self {
        Self {
            arena: Vec::new(),
            parts: (0..PARTITIONS).map(|_| Vec::new()).collect(),
            pending: 0,
        }
    }
}

impl Scatter {
    #[inline]
    pub fn push(&mut self, key: &[u8], val: i128, into: &SharedAgg) -> Result<()> {
        self.push_agg(key, 0, val, into)
    }

    #[inline]
    pub fn push_agg(&mut self, key: &[u8], agg: u16, val: i128, into: &SharedAgg) -> Result<()> {
        if key.len() > LEN_MASK as usize {
            return Err(BurrmillError::LimitExceeded(format!(
                "a group key of {} bytes is past the {LEN_MASK}-byte limit",
                key.len()
            )));
        }
        let hash = hash_key(key);
        let off = self.arena.len() as u32;
        self.arena.extend_from_slice(key);
        self.parts[partition_of(hash)].push(Pending {
            hash,
            off,
            len_agg: key.len() as u32 | ((agg as u32) << 16),
            val,
        });
        self.pending += 1;
        if self.pending >= FLUSH_ROWS {
            self.flush(into)?;
        }
        Ok(())
    }

    /// Drain every partition into the shared aggregate, one lock at a time.
    ///
    /// Partition `p`'s lock is taken once and held for its whole run of pending rows, so contention
    /// is a function of flushes rather than of rows. On an error the buffer is left dirty on
    /// purpose: the query is being abandoned, and tidying state nobody will read is work for its own
    /// sake.
    pub fn flush(&mut self, into: &SharedAgg) -> Result<()> {
        let Self {
            arena,
            parts,
            pending,
        } = self;
        for (p, rows) in parts.iter_mut().enumerate() {
            if rows.is_empty() {
                continue;
            }
            // A poisoned lock means another worker panicked mid-flush and this query is already
            // dying. Take the data anyway rather than panicking a second time and burying the first.
            let mut table = into.parts[p].lock().unwrap_or_else(|e| e.into_inner());
            for r in rows.iter() {
                let n = (r.len_agg & LEN_MASK) as usize;
                let key = &arena[r.off as usize..r.off as usize + n];
                table.add(r.hash, key, (r.len_agg >> 16) as u16, r.val)?;
            }
            into.bytes[p].store(table.bytes(), Ordering::Relaxed);
            rows.clear();
        }
        arena.clear();
        *pending = 0;
        Ok(())
    }
}

/// The query's whole aggregation: one table per radix partition, shared by every worker.
///
/// **This is the fix for slice 1's memory gate and it is a change of shape, not a tuning knob.** The
/// previous version gave each worker a table spanning the whole key space, so a million distinct
/// parties meant roughly twelve million live entries to produce a 57 MB answer, and peak RSS came in
/// at 1008 MB against a 256 MB budget. That is also precisely the DataFusion behaviour RFC-0044 §4.1
/// criticises by name (#6937, memory growing with core count), so reproducing it was embarrassing as
/// well as over budget.
///
/// Here a key exists in exactly one table no matter how many threads touched it, so the aggregate's
/// size is a property of the data and not of the machine. What replaces the duplication is a lock
/// per partition, amortised by [`Scatter`] over `FLUSH_ROWS` rows, and the merge phase disappears
/// entirely: there is nothing left to merge, because the workers were writing into the answer all
/// along.
pub struct SharedAgg {
    parts: Vec<Mutex<PartTable>>,
    /// Published on flush and read without locking. The budget check runs at every batch boundary
    /// and must not be the thing that serialises the workers it is measuring.
    bytes: Vec<AtomicUsize>,
}

impl Default for SharedAgg {
    fn default() -> Self {
        Self {
            parts: (0..PARTITIONS)
                .map(|_| Mutex::new(PartTable::default()))
                .collect(),
            bytes: (0..PARTITIONS).map(|_| AtomicUsize::new(0)).collect(),
        }
    }
}

impl SharedAgg {
    /// Approximate live bytes across every partition. Lock-free, and slightly stale by design - a
    /// budget checked at batch boundaries cannot be tighter than one batch anyway.
    pub fn bytes(&self) -> usize {
        self.bytes.iter().map(|b| b.load(Ordering::Relaxed)).sum()
    }

    pub fn len(&self) -> usize {
        self.parts
            .iter()
            .map(|m| m.lock().map(|t| t.len()).unwrap_or(0))
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Consume the aggregate into rows, one partition per task.
    ///
    /// Parallel because a partition's rows never meet another's: the radix split that bounds the
    /// memory also hands the output phase its parallelism for nothing. Each partition's hash table
    /// is freed as its index is built, so the buckets and the rows are never both fully live.
    pub fn into_rows(self, drop_zero: bool, key_arity: usize, sum_arity: usize) -> Result<Rows> {
        let tables: Vec<PartTable> = self
            .parts
            .into_iter()
            .map(|m| m.into_inner().unwrap_or_else(|e| e.into_inner()))
            .collect();
        let per_partition: Vec<PartIndex> = tables
            .into_par_iter()
            .map(|t| t.into_index(drop_zero, sum_arity))
            .collect::<Result<Vec<_>>>()?;

        let total_rows: usize = per_partition.iter().map(|(_, idx)| idx.len()).sum();

        // The index is allocated once at the exact size. The arenas are **moved**, not copied: these
        // are the same bytes the fold wrote, and the answer simply takes ownership of them.
        let mut arenas = Vec::with_capacity(per_partition.len());
        let mut idx = Vec::with_capacity(total_rows);
        let mut extra_sums: Vec<i128> = Vec::with_capacity(if sum_arity > 1 {
            total_rows * (sum_arity - 1)
        } else {
            0
        });
        for (part, (arena, rows)) in per_partition.into_iter().enumerate() {
            for (off, len, mut all) in rows {
                let sum = all[0];
                // Aggregates past the first live in a flat side vector with stride `sum_arity - 1`,
                // so a single-`SUM` fold - the measured path and seven of the eight real folds -
                // allocates nothing here at all and `Row` keeps its inline sum.
                if sum_arity > 1 {
                    extra_sums.extend(all.drain(1..));
                }
                idx.push(Row {
                    part: part as u32,
                    off,
                    len,
                    sum,
                });
            }
            arenas.push(arena);
        }
        Ok(Rows {
            arenas,
            idx,
            key_arity,
            sum_arity,
            extra_sums,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Materialise an answer for comparison. Tests want owned, sorted, comparable rows; the
    /// production path deliberately does not, which is the whole point of [`Rows`].
    fn collect(rows: &Rows) -> Vec<(String, i128)> {
        let mut v: Vec<(String, i128)> = rows.iter().map(|(k, s)| (k.to_string(), s)).collect();
        v.sort();
        v
    }

    /// The partition split must actually spread. A hash whose entropy is in the low bits would put
    /// every key in partition zero, and nothing downstream would notice - the answer would still be
    /// right, just serial, and only at scale.
    #[test]
    fn addresses_spread_across_partitions() {
        let mut counts = [0usize; PARTITIONS];
        for i in 0..100_000u64 {
            let k = format!("0x{i:040x}");
            counts[partition_of(hash_key(k.as_bytes()))] += 1;
        }
        let expected = 100_000 / PARTITIONS;
        let worst = counts.iter().map(|c| c.abs_diff(expected)).max().unwrap();
        assert!(
            worst < expected / 2,
            "partitions are lopsided: expected ~{expected} each, worst deviation {worst}, {counts:?}"
        );
    }

    #[test]
    fn two_scatters_into_one_aggregate_sum_exactly() {
        let shared = SharedAgg::default();
        let mut a = Scatter::default();
        let mut b = Scatter::default();
        for i in 0..1000u64 {
            a.push(format!("0x{i:040x}").as_bytes(), i as i128, &shared)
                .unwrap();
            b.push(format!("0x{i:040x}").as_bytes(), -(i as i128) * 2, &shared)
                .unwrap();
        }
        a.flush(&shared).unwrap();
        b.flush(&shared).unwrap();
        let rows = collect(&shared.into_rows(true, 1, 1).unwrap());
        assert_eq!(
            rows.len(),
            999,
            "party zero nets to zero and HAVING drops it"
        );
        for (k, v) in &rows {
            let i = i128::from_str_radix(k.trim_start_matches("0x"), 16).unwrap();
            assert_eq!(*v, -i, "{k} should be i - 2i");
        }
    }

    /// **The refusal happens on the answer, not on a partial sum** (roadmap 2.1b).
    ///
    /// This test used to assert that `flush` itself refused. It does not any more, and that is the
    /// change rather than a regression: an entry whose running total leaves `i128` is widened, and
    /// whether the query is refused is decided when the rows are produced. `MAX + 1` still refuses;
    /// `MAX + 1 - 1` no longer does, because it is representable and the old behaviour depended on
    /// the order the rows happened to arrive in.
    #[test]
    fn a_sum_that_does_not_fit_is_refused_when_the_rows_are_produced() {
        let shared = SharedAgg::default();
        let mut a = Scatter::default();
        let mut b = Scatter::default();
        a.push(b"0xdead", i128::MAX, &shared).unwrap();
        a.flush(&shared).unwrap();
        b.push(b"0xdead", 1, &shared).unwrap();
        // The partial sum left the range and was carried, not refused.
        b.flush(&shared).unwrap();
        let err = shared.into_rows(false, 1, 1).unwrap_err();
        assert!(
            matches!(err, crate::BurrmillError::Overflow(_)),
            "got {err:?}"
        );
    }

    /// The mirror: a running total that leaves `i128` and comes back is answered exactly.
    #[test]
    fn a_partial_sum_may_leave_the_range_and_return() {
        let shared = SharedAgg::default();
        let mut a = Scatter::default();
        a.push(b"0xdead", i128::MAX, &shared).unwrap();
        a.push(b"0xdead", 1, &shared).unwrap();
        a.push(b"0xdead", -1, &shared).unwrap();
        a.flush(&shared).unwrap();
        let rows = collect(&shared.into_rows(false, 1, 1).unwrap());
        assert_eq!(rows, vec![("0xdead".to_string(), i128::MAX)]);
    }

    /// The same, in the negative direction, because sign extension is where the first
    /// implementation of this was wrong and the corpus caught it inside a second.
    #[test]
    fn a_negative_running_total_widens_with_the_right_sign() {
        let shared = SharedAgg::default();
        let mut a = Scatter::default();
        a.push(b"0xdead", i128::MIN + 1, &shared).unwrap();
        a.push(b"0xdead", -1, &shared).unwrap();
        a.push(b"0xdead", 1, &shared).unwrap();
        a.flush(&shared).unwrap();
        let rows = collect(&shared.into_rows(false, 1, 1).unwrap());
        assert_eq!(rows, vec![("0xdead".to_string(), i128::MIN + 1)]);
    }

    /// **The property the shared aggregate exists to preserve.** Many threads writing into one
    /// partitioned table must produce exactly what one thread writing serially produces - every key,
    /// every sum, no lost update. A per-partition lock that was merely *mostly* right would show up
    /// here as a handful of short balances and nowhere else, and a balance that is quietly short is
    /// the worst failure this project has.
    #[test]
    fn concurrent_scatter_equals_serial_scatter() {
        const KEYS: u64 = 20_000;
        const WORKERS: usize = 8;
        const ROUNDS: u64 = 5;

        let serial = SharedAgg::default();
        let mut one = Scatter::default();
        for _ in 0..(WORKERS as u64 * ROUNDS) {
            for i in 0..KEYS {
                one.push(format!("0x{i:040x}").as_bytes(), i as i128, &serial)
                    .unwrap();
            }
        }
        one.flush(&serial).unwrap();
        let expected = collect(&serial.into_rows(false, 1, 1).unwrap());

        let shared = SharedAgg::default();
        std::thread::scope(|s| {
            for _ in 0..WORKERS {
                s.spawn(|| {
                    let mut sc = Scatter::default();
                    for _ in 0..ROUNDS {
                        for i in 0..KEYS {
                            sc.push(format!("0x{i:040x}").as_bytes(), i as i128, &shared)
                                .unwrap();
                        }
                    }
                    sc.flush(&shared).unwrap();
                });
            }
        });
        let got = collect(&shared.into_rows(false, 1, 1).unwrap());

        assert_eq!(
            got.len(),
            KEYS as usize,
            "every key must survive the exchange"
        );
        assert_eq!(
            got, expected,
            "concurrent and serial aggregation must agree exactly"
        );
    }

    /// A key must land in exactly one partition table, whoever wrote it. This is the memory claim
    /// stated as an assertion rather than as a benchmark: if a key could be duplicated across
    /// tables, the whole point of the change is gone and only a profiler would tell you.
    #[test]
    fn a_key_lives_in_exactly_one_partition() {
        let shared = SharedAgg::default();
        let sref = &shared;
        std::thread::scope(|s| {
            for w in 0..6 {
                s.spawn(move || {
                    let mut sc = Scatter::default();
                    for i in 0..5_000u64 {
                        sc.push(format!("0x{i:040x}").as_bytes(), w as i128, sref)
                            .unwrap();
                    }
                    sc.flush(sref).unwrap();
                });
            }
        });
        assert_eq!(
            shared.len(),
            5_000,
            "six workers, one table per key, no duplication"
        );
    }
}
