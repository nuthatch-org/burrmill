# Roadmap

RFC-0044 §7 is the *design* ladder. This is the schedule, and where the two disagree, this one is
the record of what measurement said. It is rewritten after every stage rather than defended.

**Where §7 was wrong, stated first.** It costed slices 0 and 1 together at eight to eleven weeks;
the code took a day. That is not a triumph, it is a calibration error in the other direction too:
§7 costed the *hardening* at zero, and hardening is where the remaining work actually lives. A fold
that beats DuckDB on a bench is perhaps a fifth of a fold you would let answer a stranger's query.
§7 also has no slice for input repartitioning, because nobody knew it would be needed until the
memory gate failed. Treat its engineer-week figures as an order of magnitude, not a plan.

---

## Stage 1 — slice 1 · **GATE PASSED**

The gate is "≤1.0x DuckDB at exact parity **under the 256 MB RSS gate**", and both legs are met, at a
stated parallelism, with the comparison held equal:

- **Latency 0.38-0.87x** across fourteen configurations, parity verified on every one.
- **210 MB** peak RSS at 989,690 groups, against 256, at the default eight-thread budget.

Two conditions attached, because both were wrong earlier in the day and the gate is only meaningful
with them stated. The fixture is nest-shaped at twelve columns, not the four it was; and **all three
engines get the same thread budget**, because bounding Burrmill's parallelism while leaving DuckDB on
a 32-core box's defaults compared 8 threads against 32 and called the difference an engine.

| # | Work | Done when |
|---|---|---|
| ~~1.1~~ | ~~Explain DuckDB's flat ~620 ms on the real nest directory~~ | **DONE 2026-08-31. Both halves happened: the comparison was wrong and the numbers are restated downward.** The nest's segments directory holds 38,429 files and DuckDB re-globbed all of them inside every timed run while Burrmill's `read_dir` sat outside the timer. Corrected, the published 0.11-0.71 became 1.00-2.98 — Burrmill *slower*. A fifth defect found in the process (the fold had no projection pushdown and decoded 14 columns to read 3) brings it to 0.48-0.78. See the progress log |
| ~~1.1a~~ | ~~A realistic-width synthetic fixture~~ | **DONE 2026-08-31.** Twelve columns matching a real nuthatch event, two of them 66-character hex. Demonstrated rather than asserted: with the projection deliberately disabled, the old fixture shows a **1.00-1.07x** penalty and the new one **1.6-2.5x**. It also flipped DataFusion from ~0.8-0.9x DuckDB to **2.0-3.9x**, because a wide table punishes an engine that decodes columns nobody asked for. Every published sweep number is restated on it |
| ~~1.1b~~ | ~~A gate that refuses a flat comparison~~ | **DONE 2026-08-31.** The nest harness halves its input and checks whether each engine's time follows. Over 50% fixed and **no ratio is printed** — the field reads `UNSAFE_fixed_duck=66pct_burr=17pct`. It refuses exactly the 0.11 that was published this morning and passes the legitimate ones. Its own first version gated a different quantity from the one it measured, and was caught the first time it ran |
| 1.2 | ~~Input repartitioning~~ **One shared partitioned aggregate, not one per worker** · PARTIALLY DONE | Peak RSS at 1M groups **538→146 MB at 1 thread, 584→210 at 8, 769→349 at 32**, latency better at every count, parity verified on all 14 sweep configurations. `agg_bytes` is 99 MB at every thread count, which is the claim as a measurement. **Passes to 8 threads, misses by 1% at 16 and 36% at 32**, so the gate is not passed |
| ~~1.2a~~ | ~~The last of the per-thread cost~~ **· CLOSED by 1.2c** | Two candidates ruled out by measurement, not argument: **streaming the output cannot help** (a globally sorted answer needs every row live), and **it is not the decoder** — with a trivial aggregate the same scan costs 1.3 MB/thread against 5.6 MB/thread at 1M groups, so the scaling follows the aggregate. What is left is concurrent hash-table growth and 36-67 MB of freed-but-unreturned memory behind it. Needs partition pre-sizing from a cardinality estimate, or an arena-allocated table |
| ~~1.2c~~ | ~~State the gate's parallelism~~ | **DECIDED AND ENFORCED 2026-08-31: 8 threads per query, and the operator bounds itself rather than inheriting the host's core count.** `Limits::max_threads` defaults to 8, the handle owns a pool that size, and `mem_pool_bytes` finally means something. Chosen on measurement: 1M groups goes 608 ms at 1 thread, 217 at 4, **165 at 8**, 158 at 16, 173 at 32 - eight is within 6% of the whole machine and leaves twenty-four cores for other queries, which is the entire concurrency argument. Old text: 256 MB is not a budget until it says at how many threads. The same binary passes on a laptop and fails on a build server, and that is a specification defect rather than a code one. An RFC amendment, not a patch |
| 1.2b | The small-aggregate regression | 509 groups went 48 MB → 76 MB and 0.76 → 0.82x. Sixty-four partition tables are pure overhead for an aggregate that fits in L2 as one, and a shared aggregate cannot use the old promotion threshold because the partitions are what make it shared. Small, real, and recorded rather than rounded away |
| ~~1.3~~ | ~~A real global memory budget~~ | **DONE as a consequence of 1.2.** `mem_pool_bytes` is now checked against the query's whole aggregation rather than one worker's share, because there is only one. Still not process RSS — decode buffers sit outside it — and the doc comment says so. `max_bytes`, which was a field nothing read, is now enforced too |
| ~~1.4~~ | ~~Seal-layout canary~~ | **DONE 2026-08-31.** `tests/seal_layout.rs` writes the layout contract down as assertions, and `BURRMILL_NEST=<dir>` checks it against a real nest (38,428 segments, 34 tables). It found a genuine bug on the way — **a table name matching no segments returned an empty answer instead of refusing**, where DuckDB refuses — and then found that its own `<contract>__<event>` grammar was wrong: `grt_total_supply` is a *call* table sealed from an `eth_call`, not a log |
| 1.4a | The fold has only ever run on event tables | The canary turned up a second sealed shape (`calldata` / `result` / `reverted`, no event name). The signed fold has no meaning on one, but the catalog opens it quite happily. Worth knowing what the admitted subset should say about it |

**1.1 was first** because it was an evening's work and it decided whether the best numbers already
published were real. They were not. Publishing a flattering measurement and defending it later is
the failure mode RFC-0004 exists to prevent, and the lesson to carry is that the parity guard — the
thing this project is proudest of — is blind to this entire class of error. Parity says the two
engines agree on the answer. It says nothing about whether they were asked the same question.

**Stop condition, and where it stands:** if repartitioning cannot hold 256 MB without giving back the
latency win, the honest outcome is a keep-amendment quantifying the trade, not a quietly relaxed
budget. Nothing has been given back — latency improved — so the condition has not been triggered.
The budget has not been relaxed either. The gate simply is not met at a million groups, and that is
recorded as a fail rather than argued away.

---

## Stage 2 — earn the right to be trusted

Owning execution means owning the bugs, and nineteen hand-written refusals are not a corpus.

| # | Work | Done when |
|---|---|---|
| 2.1 | ~~Allowlist-constrained query generator~~ **· DONE, and it found things** | Two oracles: a non-optimising reference in `tests/generated_folds.rs` that runs on every `cargo test` and survives DuckDB's removal, and `burrmill-bench gen` against DuckDB itself. **3,000 cases: 2,684 answers agreed exactly, 308 both refused, 8 order-dependent.** Mutation-checked — a `wrapping_add` and a dropped row are both caught with reproducible seeds. Still owed: nightly CI |
| ~~2.1a~~ | ~~Decide the `TRY_CAST` divergence~~ **· DECIDED: refuse, do not guess** | 8 of 20 edge literals differed. Whitespace is fixed (` 7` is seven, and we were dropping the row). DuckDB also takes `1e18`, `7.0`, `1_000` and **rounds `7.9` to 8**; adopting silent rounding into an engine whose first claim is exactness needs a decision. `burrmill-bench cast` prints the table |
| ~~2.1b~~ | ~~Decide whether refusal should be order-independent~~ **· DECIDED AND DONE: yes** | Refusal fires when an intermediate partial sum leaves `i128`, not when the answer does: `MAX, +1, -1` sums to exactly `MAX` and is declined. DuckDB does it too, in both directions. Fixing it means accumulating wider than `i128`, which costs 16 bytes per group — against a memory gate already being missed. `tests/generated_folds.rs` pins today's behaviour so a fix inverts a test deliberately |
| ~~2.2~~ | ~~Overflow reached by generation~~ | **DONE.** 86 of 180 generated cases in the library test reach the refusal path, against a benchmark fixture that topped out at 1e20 and could never have reached it at all |
| ~~2.3~~ | ~~`sqllogictest-rs` corpus~~ | **DONE 2026-08-31.** Hand-computed expectations in `crates/burrmill/tests/slt/`, run against Burrmill on every `cargo test` at three segment layouts, and against DuckDB via `burrmill-bench slt`. Both green. Mutation-checked. Choosing the standard format paid immediately: pointing the same files at DuckDB is what turned up 2.3a |
| 2.3a | ~~Report the DuckDB wrap upstream~~ **· ALREADY REPORTED: duckdb#24081, fixed on `main` by #24168, not in 1.5.x** | With ≥2 threads and ≥2 files, `SUM(HUGEINT)` returns `i128::MIN` where the true sum is `i128::MAX + 1`: a silently wrapped balance. Reproduced by `burrmill-bench duckdb-gaps` on libduckdb-sys 1.10501.0, and on the 1.5.5 CLI on 2026-09-16. Viktor Leis filed it on 2026-07-23 and the Combine fix merged on 2026-07-28, after 1.5.5 was cut, with no backport to `v1.5-variegata`. The remaining action is a backport request, and it is Chief's call. See `docs/upstream/duckdb-hugeint-parallel-wrap.md` |

---

## Stage 3 — the seam (RFC-0044 §3.4, slice 2) · **COR-1 HELD**

The highest-risk invariant in the design, and the only place where owning execution and getting the
semantics right are the same job.

| # | Work | Done when |
|---|---|---|
| ~~3.1~~ | ~~redb hot-tip provider~~ **· `HotTip` trait + `MemoryTip`, redb adapter deferred** | **DONE as the abstraction.** The tip's rows are JSON entities in a schema that is nuthatch's business, and `tests/seal_layout.rs` exists because assuming nuthatch's layout is how the reading breaks silently. `HotTip::snapshot` returns the watermark and the rows **together**, so the two-read bug cannot be written. A redb adapter is thin and belongs where that encoding is known |
| ~~3.2~~ | ~~Seam as two pipelines into one sink~~ | **DONE.** The cold scan and the hot rows both write into one `SharedAgg`, which is the `UNION ALL` model §2 asks for. Cold is filtered to the pinned watermark using the block column, in the projection only when a seam is pinned |
| ~~3.3~~ | ~~COR-1 property tests under concurrent seal~~ | **DONE.** 167-250 folds per run, **every one of them overlapping an active sealer**, asserting an answer the data fixes independently of where the boundary sits. It found three real bugs: the catalog was listed before the snapshot, `sealed_through: u64` could not distinguish "nothing sealed" from "block 0 sealed", and nuthatch installs segments non-atomically |
| 3.4 | A redb-backed `HotTip` | Needs nuthatch's hot entity encoding pinned down. The seam invariant is owned and tested; this is an adapter |
| 3.5 | **Nuthatch installs segments non-atomically** · drafted at `docs/upstream/nuthatch-segment-install-not-atomic.md`, not sent | `seal.rs:176` is a bare `std::fs::write`, so a reader globbing `segments/` can see a zero-length file. Its manifest right beside it is written tmp-then-rename with a comment explaining why. Outward-facing, so Chief's call |

**Stop condition:** COR-1 is a correctness invariant, not a performance target. A seam that is fast
and occasionally double-counts is worthless.

---

## Stage 4 — more shapes, and the ratchet that measures them

Today Burrmill owns exactly one plan shape. The coverage ratio (§4.6) is therefore 1 shape / n, and
publishing that honestly from the start is what stops "hybrid now, own more later" becoming
"hybrid forever".

| # | Work | Done when |
|---|---|---|
| ~~4.1~~ | ~~Count the plan shapes (experiment A4)~~ | **DONE 2026-08-31, and the answer is not the one the RFC hoped for.** 126 view files, 65 statements: **32 distinct shapes, 22 plan families**, top five families covering 48%. Not a dozen patterns; a long tail composed from nine primitives. **Coverage ratio 0/65.** Full result in `docs/bench/a4-plan-shapes.txt` |
| ~~4.1a~~ | ~~The admitted shape does not occur in the workload~~ **· DONE: the fold is n-branch now** | Burrmill folds *one table read twice*. Every `UNION ALL` in 65 real statements reads **different** tables, because a credit and a debit are different events. **8 of 65 are n-table signed folds** (five 2-branch, two 4-branch, one 5-branch). **Correction:** the first version of this said the one-table case occurs zero times; the detector counted union arms without checking whether the tables differed. It occurs **4 times** - the ERC-20 `Transfer` shape, where one row carries both a payer and a payee - and was refused over `lower()`, not over its shape. Generalising `SignedFold` from one table to n takes coverage from 0% to ~12% and throws nothing away - the current shape is the degenerate case |
| ~~4.1b~~ | ~~Computed group keys~~ **· DONE: `lower`/`upper` admitted, and two planner rules fixed** | **Fold sub-plans 1/8 → 6/8.** Three separate causes, not one: `lower()` on the key (4 folds, the ERC-20 balance shape), an alias demanded on union arms that SQL does not name (1), and later arms compared against the first when SQL ignores them entirely (1). Two of those were **my rules being wrong about SQL**, not the subset being narrow. No regression: 100-104 ms and 245 MB, the pre-generalisation baseline |
| ~~4.1c~~ | ~~Composite keys~~ **· DONE: `GROUP BY a, b` with literal-tagged key parts** | **Folds 6/8 → 7/8.** A key column is now a bare column, a literal, `lower`/`upper` of a column, a cast of one to text, or any `\|\|` of those; keys are length-prefixed when composite, never delimiter-joined. The aggregate is untouched - it hashes bytes and never learns the key had parts. Gate unmoved: 208 MB, 0.47x, parity verified |
| ~~4.1d~~ | ~~Several aggregates in one fold~~ **· DONE. FOLD COVERAGE 8/8** | Aggregates past the first live in a side map keyed by `(arena offset, index)`, so `Entry` keeps its single inline `i128` and a one-`SUM` fold never touches it. Gate unmoved: **199 MB, 0.49x, parity verified**. `HAVING` is refused with more than one `SUM`, because which sum decides survival is a question the query has not answered |
| 4.1f | CTEs and joins, for statement-level coverage | Every one of the 8 folds sits inside a `WITH` binding or a join, so statement coverage stays 0/65 however good the fold gets. The fold is the heavy part and owning it is the point, but the ratio §4.6 asks to publish will not move without these |
| 4.2 | ~~The remaining heavy folds~~ **· MEASURED on the real authored views, and the claim was false until it was fixed** | The headline had only ever been checked on a **synthetic** fixture. On the real nest's own views: **1.20x, 1.16x, 1.38x — 0/3 at or under 1.0x**. Cause: a real nest table is 6,000-10,000 tiny segments and 57-93 ms of every query went into re-reading immutable footers. Caching the cut morsels: **0.80x, 0.95x, 1.01x, 2/3 passing**. `docs/bench/real-views.txt` |
| 4.2a | The third fold, at 1.01x | `curation + gns`, four tables and 9,745 segments, scan-bound at 321 ms of 323. Nothing left to cache; this one is the per-morsel cost of ten thousand files, which is 4.2b territory |
| 4.2b | Per-morsel cost on many-tiny-segment tables | Each morsel opens its file and builds a reader. At 9,745 morsels that is 9,745 opens per query. Worth measuring against a reader cache or a coalesced read before assuming |
| 4.2c | **The 4.2 comparison caches footers on one side only** | `views.rs` warms Burrmill's morsel cache in the untimed parity run but opens DuckDB with only `SET threads`, and `parquet_metadata_cache` is off by default. Measured on the 1.5.5 CLI on 2026-09-16 over the curation fold (6,004 files, 8 threads, two rounds each): **151 ms off, 138 ms on**, about 9%. Extrapolated onto the table above, curation stays ahead at ~0.9x and staking lands near 1.0x. That is an estimate. Set the cache in `views.rs` and `serve.rs` and re-run before quoting 4.2 again |
| 4.3 | Publish the coverage ratio per release, monotonic | It may not go down |

---

## Stage 5 — serving · **the concurrency claim did not survive measurement**

Where the concurrency win was *expected* to be banked. It was not. §7 called this "the easiest
headline in the project" because Burrmill takes no global lock; 5.2 measured it and properly-embedded
DuckDB wins on throughput, tail latency and fairness alike.

The lock was never the whole story. What replaced it is a bounded shared pool, and a bounded pool
that hands every worker to one query at a time starves the queue exactly as a mutex does. Measuring
this before building 5.1 on top of it was the right order, and it is the only reason the streaming
work is not now sitting on an assumption.

| # | Work | Done when |
|---|---|---|
| 5.1 | ~~The cancellation contract~~ **· DEMONSTRATED, and the gate had broken it** · streaming still open | §3.5's "delay bounded by one morsel" had **never been tested**. It held for a query already folding; it did **not** hold for one waiting at the admission gate 5.3 added — measured at **57 ms against a 110 ms query**, a bound that had quietly become "however long everybody ahead of you takes". The gate is a yield point now (timed wait, 2 ms notice, forfeiting the turn so the queue cannot seize) and `tests/cancellation.rs` holds both halves. Streaming results is the remaining half of this item |
| 5.6 | A scheduler, if it is still worth it |
| ~~5.2~~ | ~~Re-run the #986 sweep~~ **· DONE, AND BURRMILL LOSES** | #986 reproduces exactly for DuckDB-behind-a-mutex: flat 55 qps from 1 to 32 clients, some client served **not at all**. But embedded its own way - one database, a connection per client - DuckDB scales to **171 qps** at 32 clients where Burrmill manages **96**, worst-client p99 **423 ms against 2378 ms**, and it serves every client where Burrmill starves some. Burrmill is faster at one client and slower at sixteen. Full result in `docs/bench/serve-concurrency.txt` |
| ~~5.3~~ | ~~Admission control~~ **· DONE. The starvation is gone** | A FIFO ticket gate of width `pool/2` in front of the pool. At 32 clients: fairness **0.00 → 0.81**, worst-client p99 **2378 ms → 601 ms**, every client served, for ~8% throughput. The starvation was a **liveness bug** — at a 20-second window a client still completed zero queries — and it is gone |
| ~~5.3a~~ | ~~Adaptive per-query parallelism~~ **· DONE** | A sharing query splits into exactly `pool / in_flight` groups, which caps its width because rayon cannot run more groups at once than there are. At 32 clients: **89 → 100 qps**, worst p99 **601 → 478 ms**, fairness **0.81 → 0.94**. At 4 clients Burrmill now **beats** `duck_multi` (108 vs 107) and is fairer at every count (0.94 vs 0.72 at 32). Single query unchanged. **Throughput at 16+ clients is still ~0.65x DuckDB** and that is now the honest standing gap |
| ~~5.4~~ | ~~Why Burrmill is still 0.65x DuckDB at 16+ clients~~ **· DIAGNOSED, and partly closed** | **Not fixed cost — utilisation.** The fold is 53 ms at one thread against DuckDB's implied ~48 ms, so the work is comparable; but eight threads give only **3.3x**, and a share split into coarse groups idles workers on the tail. Splitting each share twice recovers it: at 32 clients **100 → 112 qps**, p99 **478 → 410 ms**, fairness **0.94 → 0.89**. At 4 clients Burrmill now leads clearly, **123 qps against 109** |
| ~~5.5~~ | ~~The last of the throughput gap~~ **· no scheduler needed; the flush size was tuned at the wrong operating point** | Single-query scaling depends on query **size** — a 181 ms fold gets 7.9x from eight threads, a 48 ms one gets 4x — and the cause is contention on the aggregate's 64 partition locks, which bites small queries because they flush proportionally more often. `FLUSH_ROWS` 4096→16384 takes a 500k-row scan from **18 ms to 11** with the serial cost unchanged. Serving: **123 → 133-143 qps** at 4 clients, 112 → 115-124 at 32. Memory gate unchanged at 218 MB. The constant had been swept at 1M groups on 32 threads, where memory binds and contention does not |
| 5.6 | A scheduler, if it is still worth it | Burrmill now leads DuckDB below eight clients and trails above it (135 vs 160 at sixteen). The remaining gap is genuinely scheduling — work-stealing across queries rather than nested rayon pools. Cost it against 5.1 (streaming) before starting; it may no longer be the best next thing |

---

## Stage 6 — replacing DuckDB in nuthatch · **GATE 1 TAKEN AS PASSED, MIGRATION DECIDED 2026-09-26**

Chief's direction, RFC-0044 Amendment 2. The design, gates and risks are in
`docs/research/replacing-duckdb/plan.md`. The evidence is investigations 01-05 in the same
directory. Phases 2 and 3, in nuthatch, are tracked with tick lists in
`docs/research/replacing-duckdb/migration.md`; phase 2a (the engine trait, DuckDB behind it) began
on 2026-09-26 on nuthatch branch `pete/engine-trait`.

| # | Work | Done when |
|---|---|---|
| ~~6.0~~ | ~~Footprint spike: DataFusion component crates plus an owned physical planner~~ **· DONE 2026-09-17. C works, C fails burrmill#1, C still beats U.** | Nuthatch profile on the thinkpad, six test binaries, same fixture, all three engines 90 parties. Querying test binary **162 / 493 / 446 MB** (duck / U / C); release **41 / 127 / 105 MB**; target **3.27 / 4.48 / 4.14 GB**; incremental **1.1 / 1.9 / 1.9 s**. C is 2.75× DuckDB on the binary that would fill the disk. The swap is a trade. Report: `docs/research/replacing-duckdb/06-footprint-spike.md`. An RFC-0042 amendment with these numbers is Chief's |
| ~~6.1~~ | ~~`NestCatalog` provider~~ **· LANDED 2026-09-17 behind `feature = "datafusion"`** | `Engine::open_segments` / `open_nest`: explicit file lists, sizes known, statistics off, declared-but-unsealed tables empty, `_dec` via `TRY_CAST` on `word16`/`word32`. C-route (no umbrella crate). Tests in `tests/df_engine.rs`. Footer cache is DataFusion's `CacheManager` (1 GiB), not the private `CachedParquetFileReaderFactory`. **Wrong until 2026-10-16:** the cache manager is read only through that factory, so the cache was configured and never used; `SegmentTable` now scans through it (burrmill#98) |
| ~~6.2~~ | ~~Lockdown~~ **· LANDED with 6.1, census allowlist still open** | Non-SELECT refused at the surface. Tests for COPY, `CREATE EXTERNAL TABLE`, `read_csv`, replacement scan `FROM '/etc/passwd'`. No table factories, no URL tables. Function surface audited 2026-09-25 (`df_functions`): `input_file_name()` leaked server paths and is removed with `file_row_index()`; nothing else reaches outside the query, so no census allowlist |
| 6.3 | `CheckedArithmetic`, hardened from the investigation 03 prototype **· LANDED 2026-09-24; memory gate FAILS, on stock DataFusion first** | per-type accumulator, whitelist rule, wide literals refused, `TRY_CAST` tainted and its sums made exact through unions and negation; 03's table all exact-or-refused (`tests/df_checked.rs`). 989,690 groups, 8 threads, MacBook, **RSS taken over 5 folds per process and so overstated; see progress log 6.6b for single-fold figures**: stock DataFusion 590 MB, checked `CAST` 606-647, `TRY_CAST` 731-745, latency within 7%. A 192 MB fair spill pool reaches 339/360 MB at 2.2-2.8x. The 256 MB gate needs the aggregate's memory owned, not the arithmetic changed. Re-measure on the thinkpad |
| ~~6.4~~ | ~~Nuthatch result encoder and error mapping~~ **· LANDED 2026-09-24** | Encoder byte-identical to nuthatch's own `value_to_json` on DuckDB's batches, 17/17 (`encode-parity`); error text restated so nuthatch's hint classes key the same, 11/14 with the other three owned by 6.5 and 6.7 (`error-parity`). The census's `checks/expected` fixtures are gone from nuthatch; engine-level byte identity waits on 6.5's types |
| ~~6.5~~ | ~~Dialect layer and the silent differences~~ **· LANDED 2026-09-24** | DuckDB-dialect parse and AST rewrites, `/` as DOUBLE, `to_timestamp` µs UTC, DuckDB's text comparisons, case-insensitive identifiers, `information_schema` for `.tables`/`.schema`, DuckDB's default and repeated column names. `dialect-parity` **44/44** (39 identical, 5 refused by both); `error-parity` 14/14 |
| 6.6 | `FoldSubstitution` **· LANDED 2026-09-24, ahead of 6.4/6.5; gate STRADDLED on the thinkpad** | Owned fold swapped in by an analyzer rule wherever its answer provably equals DataFusion's (NULL-rejecting filter or non-null values, strict `TRY_CAST`, 38-digit check), keys with literal tags and case folds, visible in EXPLAIN, parity by digest on both machines. Thinkpad, 989,690 groups, one fold per process, streamed through `Engine::sql_for_each`: **249-261 MB**, 195-217 ms; bare fold 208-227 MB, stock DataFusion 491-525 MB. The 20-40 MB over the bare operator is mostly glibc retention: with `MALLOC_TRIM_THRESHOLD_` it passes at 215-223 MB, but at ~1.3x DuckDB's latency; **declined** (6.6c). Against DuckDB itself: **same answer, 201-206 vs 187-206 ms, 240-262 vs 572-598 MB**. **6.6d, 2026-09-25: the gap was never data** (peak live heap 163 MB in both) but the bench binary's code pages; in a Burrmill-only binary (`examples/hosted_fold`) the hosted fold peaks at **241-245 MB, a pass** |
| ~~6.7~~ | ~~Parser role~~ **· REACHABILITY LANDED 2026-09-25** | `burrmill::inspect::reach` (default build): nuthatch's allowlist and reachability rules on sqlparser's AST, failing closed. `reach-parity`: **38/46 identical to nuthatch's own walk over DuckDB, 8 stricter, 0 looser**. The other `json_serialize_sql` consumers (graft canonical form, entity gate, Dune, lowering) go with phase 2 |
| ~~6.8~~ | ~~DataFusion-path `serve` sweep~~ **· MEASURED 2026-09-24** | `Engine` took 5.3's FIFO gate after starving a client at 32. Synthetic fold: fair (0.83-0.91), tails level with DuckDB's, DuckDB ahead on throughput by being unfair. **Real views: about 2x DuckDB's qps at 1-32 clients, worst p99 255-1,350 ms against 507-7,424, fairness 0.89-0.97 against 0.58-0.00, and 3.9 GB against 15.4 at 32 clients**. DuckDB fails at 32 under the default `ulimit -n` |

**Gate 1, measured 2026-09-24 on the thinkpad's 1,925-segment graph-allocations copy:** parity
12/12 portable views (7 are DuckDB-only syntax, phase 1b); checks agree with DuckDB; speed
was 1.54x DuckDB (stock DataFusion 1.81-1.87x), and **two owned plan shapes (`TopPerGroup`,
`DistinctSplit`) bring it to 0.60x time-weighted, `FastTextCasts` and `ShareRepeats` to **0.53x**, a pass.** Per
statement, 9/12 within 1.5x; over are two views on a null-aware anti-join (41 and 51 ms, the
best plan measured) and one 9 ms view that is planning. See the progress log.

**Phase 1b, 2026-09-25, same machine and nest:** **22/22 views byte-identical**, 7 of them rewritten
on the nest's `pete/portable-views` branch (not pushed) and two now running unchanged through
`list_reduce`; both zero-row checks pass. **0.69x DuckDB time-weighted, 21/22 within 1.5x**; over was
`lodestar_epochs` at 1.92x, four range joins as nested loops. Transcript `docs/bench/phase1b-thinkpad.txt`.
With `RangeJoin` the same day: **0.65x time-weighted and 22/22 within 1.5x**, `lodestar_epochs` 0.5x
(`docs/bench/rangejoin-thinkpad.txt`). Gate 1's speed leg passes on this nest.
The memory leg passes as nuthatch would run it: **241-245 MB at 989,690 groups** in a Burrmill-only
binary (6.6d).
The footprint leg, re-measured on Burrmill itself (6.0b): **495 MB querying test binary, 112 MB
release, 4.76 GB target**, 7-15% over 6.0's stand-in, all of it traced to features added since.
**Accepted by Chief on 2026-09-26** as the price of taking DuckDB's C++ out of nuthatch; build time
is not a win and is tracked as burrmill#7.

**Gate 1:**

- every authored statement reaches parity once the Lodestar and qos views are rewritten (plan
  phase 1b, a nuthatch change);
- ≤1.0x DuckDB time-weighted, and ≤1.5x per statement;
- the overflow corpus refuses correctly;
- 256 MB at 1M groups;
- the footprint figures from 6.0 hold.

Phases 2 (engine trait, shadow mode) and 3 (cutover and removal) happen in nuthatch and are listed
in the plan.

---

## Not on this roadmap

- **DataFusion as a cold-path fallback.** §7 puts it at slice 3. Deliberately deferred: building the
  fallback before the fast path is finished is how the fast path stops being finished.
- **JIT, on-disk format changes, a custom Parquet decoder.** Non-goals in §9 and they stay non-goals.
- **crates.io.** The name is reserved by being unclaimed. Publishing a query engine that owns one
  plan shape and overruns its memory budget would be a promise we cannot keep.
