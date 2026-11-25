# TPC-H at SF1 and SF10, losses first (#346, #414)

Measured 2026-11-25 on 2d44933, on the ThinkPad (32 threads, i9-13980HX, 62 GiB, Linux 6.12),
every engine at **8 threads** and on CPU, best of three warm runs, over Parquet that tpchgen-cli
4.0.0 wrote in 16 parts per table:

- **DuckDB 2.0.1**, the release CLI (`duckdb --version`: `v2.0.1 (Cyanoptera) 8d78114e90`), which
  the bench runs as a process (`DUCKDB=`), after `SET threads = 8`, reading the same files through
  `read_parquet`. Its time is what its `.timer` reports for the statement. No GPU and no Sirius.
- **datafusion-cli 55.2.0**, the DataFusion Burrmill pins, built with LTO; its time is what it
  reports.
- **Burrmill** holds every statement to a 2 GiB budget, with no spill directory.

Each Burrmill answer is compared with DuckDB 2.0.1's (`burrmill-bench tpch`, floats to 1e-12)
before its time is printed. The answers were measured first, in a pass of one run each: 22 of 22 the
same at SF1, 21 of 22 at SF10 with Q21 refused. The timings came after, all three engines in the
same window, query by query. The host is shared and was busy; the 1-minute load was 6.0 at the
start of the SF1 run and 11.0 at its end (02:17:58 to 02:18:23), and 11.0 and 12.1 for SF10
(02:18:23 to 02:20:36). Ratios are Burrmill over DuckDB 2.0.1; below 1 is faster. Burrmill over
datafusion-cli is computed from the rounded figures.

## What it shows

**Burrmill is slower than DuckDB 2.0.1 on every TPC-H query:** 1.18x (Q14) to 4.80x (Q17) at SF1,
1.74x (Q14) to 5.26x (Q17) at SF10. DuckDB 2.0.1 answers SF10's 22 in 6.4 s in all, where the
bundled DuckDB 1.5.4 that #346 measured took 17.0 s, 1.8x to 3.4x as long a query; that run was on
another day, under a load it did not record.

**Q17, the widest gap, is now 2.56x to 3.00x DuckDB 2.0.1 at SF10, from 4.96x to 5.63x**, measured after
the tables below were: its subquery averages only the 2,040 parts the query keeps, not all 2M
([below](#q17-only-the-parts-the-query-keeps-433)). The tables below are from before that change.

**Against datafusion-cli it is faster on four at SF10** (7, 15, 18, 19) and within 1.5x on 13 of
the 21 it answers. It is the same DataFusion underneath, so that gap is Burrmill's own. Where it
goes is not measured yet; the candidates are the checked arithmetic, the exact sums and averages,
DuckDB's semantics rewrites and the 2,048-row batches a budgeted session runs at.

**Q2 and Q17 no longer grow faster than the data.** At SF1 they were 65 ms and 304 ms, and at SF10
1.9 s and 25.7 s (#346, against DuckDB 1.5.4). Both were a constant group key from decorrelation
that made the aggregate ordered, fixed by #399 (#381). In the tables below Q2 takes 41 ms at SF1
and 254 ms at SF10, 6.2 times the time for ten times the rows, and Q17 235 ms and 1,685 ms.

- **The cause, named (#381).** Decorrelating the subquery grouped by the key (`l_partkey` in Q17,
  `ps_partkey` in Q2) and a constant `true AS __always_true`, which DataFusion counts as sorted, so
  the aggregates ran in its ordered mode. That mode emits its last groups 2,048 at a time and
  renumbers every group still held at each emit: quadratic in the groups, 179 of Q17's partial
  aggregate's 190 CPU seconds at SF10. `LiteralGroupKeys` (`df/literalkeys.rs`, #399) takes the
  constant out of the key. It was not `checked_avg`, a hash table's growth or the pool.
- **The same cause in Q2, checked by switching the rule off.** On 2d44933 with an early return in
  `LiteralGroupKeys::rewrite`, built only for this and not committed, Q2's aggregates group by
  `ps_partkey` and `__always_true`, both `ordering_mode=PartiallySorted`. At SF10 Q2 then took
  2,117 ms against 260 with the rule, the partial aggregate 1.88 s of CPU against 72 ms and the
  final 456 ms against 60; at SF1 50 ms against 40. Burrmill alone, best of 3, back to back at a
  1-minute load of 12.2.
- **Q2 at SF10 is within 1.5x datafusion-cli at a load under 8 (#382).** Ten rounds of Q2 alone, all three engines,
  best of 3 each, on 2d44933:

  | round | 1-minute load, start / end | DuckDB 2.0.1 ms | Burrmill ms | datafusion-cli ms | Burrmill / datafusion-cli |
  |---|---|---:|---:|---:|---:|
  | 1 | 11.3 / 12.5 | 197 | 687 | 449 | 1.53x |
  | 2 | 12.5 / 14.9 | 251 | 878 | 595 | 1.48x |
  | 3 | 14.9 / 14.9 | 121 | 393 | 271 | 1.45x |
  | 4 | 14.9 / 15.4 | 111 | 381 | 220 | 1.73x |
  | 5 | 15.4 / 15.4 | 93 | 272 | 188 | 1.45x |
  | 6 | 7.6 / 7.6 | 72 | 242 | 177 | 1.37x |
  | 7 | 7.6 / 8.8 | 72 | 242 | 180 | 1.34x |
  | 8 | 8.8 / 8.8 | 75 | 248 | 189 | 1.31x |
  | 9 | 8.8 / 8.5 | 70 | 231 | 175 | 1.32x |
  | 10 | 8.5 / 8.5 | 84 | 244 | 175 | 1.39x |

  Rounds 6 to 10 ran once the 1-minute load had fallen under 8: 1.31x to 1.39x. Rounds 1 to 5 ran
  at loads of 11 to 15, and two of them, 1.53x and 1.73x, were over. In the SF10 table, at loads
  11.0 to 12.1, it is 1.45x. Every answer was the same as DuckDB 2.0.1's.
- **Q2's `EXPLAIN ANALYZE` has no ordered operator** at SF1 or SF10, and no operator spilled. Both
  aggregates group by `ps_partkey` alone, over 1.18M groups at SF10. The statement pool peaked at
  8 MiB at SF1 and 82 MiB at SF10.
- **What is left of Q2's gap is not in its aggregates or joins.** At SF10 Burrmill's aggregates
  take 78 and 49 ms of CPU against datafusion-cli's 91 and 207, over the same 1.60M joined rows. The
  scans differ: the joins' dynamic filters reach the `partsupp` scans as row filters
  (`pushdown_filters`, which Burrmill's tables set), so 1.60M of the 8.00M rows come out of the
  first, where datafusion-cli reads all 8.00M and its joins drop them. Decoding under that filter
  costs Burrmill's scans 1.18 s of CPU in all against datafusion-cli's 0.70 s.
  Burrmill also plans every join as partitioned, with a repartition each side, where
  datafusion-cli collects the small sides; the session does not collect statistics to tell. Which
  of these is the 1.3x is not measured apart.
- **Q17 at SF10 is within 1.5x datafusion-cli** in the table below (1.32x). #381 measured it in five
  rounds on 3283a14 on 2026-11-24: 0.98x to 1.30x, at loads of 6.5 to 11.2. Its `EXPLAIN ANALYZE`
  at SF10 had no ordered operator, the pool peaked at 139 MiB, and both aggregates group by
  `l_partkey` alone. Most of its time is the final aggregate's 2M groups: 4.81 CPU seconds over 60M
  rows at SF10, against datafusion-cli's 5.66 for its plain `avg`. `checked_avg` costs 423 ms of
  CPU in the partial aggregate against 96, and its semi join builds on the 2M averaged parts
  (113.8 MB) where datafusion-cli builds on the 61K joined rows (4.9 MB).

**Q21 is refused at SF10 without a spill directory:** it needs more than the 1,792 MB of the 2 GiB
budget a statement may use, which DuckDB and datafusion-cli answer without one. With a spill
directory it answers at SF10, SF30 and SF100 under 2 GiB, as DuckDB does, though one run in a
process was refused at SF30 and one at SF100, below (#383,
[Spilling](#spilling-q21-under-2-gib-383)).

**Every answer given agrees with DuckDB 2.0.1:** 22 of 22 at SF1, 21 of 22 at SF10 with Q21
refused.

**Q6 skips no row groups here.** tpchgen does not cluster `l_shipdate`, so every row group's range
spans the predicate; Q6 reads 84 MB of lineitem at SF1, more than an unfiltered scan of its two
columns, because it reads its filter columns too. Pruning works where the data is clustered: a
filter on `l_orderkey < 1000` reads 0.25 MB of the 64 MB.

A single large Parquet file is split by byte range across the scan's partitions, as DataFusion
splits it, so the 16 parts are not what makes the scans parallel.

## SF1

| Q | DuckDB 2.0.1 ms | Burrmill ms | ratio | datafusion-cli 55.2.0 ms | Burrmill / datafusion-cli | answer |
|---|---:|---:|---:|---:|---:|---|
| 1 | 45 | 131 | 2.92x | 91 | 1.44x | same |
| 2 | 19 | 41 | 2.18x | 29 | 1.41x | same |
| 3 | 33 | 79 | 2.40x | 62 | 1.27x | same |
| 4 | 21 | 83 | 3.95x | 35 | 2.37x | same |
| 5 | 44 | 114 | 2.60x | 77 | 1.48x | same |
| 6 | 19 | 58 | 3.03x | 34 | 1.71x | same |
| 7 | 41 | 97 | 2.35x | 97 | 1.00x | same |
| 8 | 51 | 124 | 2.43x | 77 | 1.61x | same |
| 9 | 66 | 174 | 2.64x | 94 | 1.85x | same |
| 10 | 51 | 110 | 2.16x | 85 | 1.29x | same |
| 11 | 14 | 41 | 2.90x | 22 | 1.86x | same |
| 12 | 28 | 119 | 4.25x | 59 | 2.02x | same |
| 13 | 81 | 122 | 1.50x | 58 | 2.10x | same |
| 14 | 47 | 56 | 1.18x | 51 | 1.10x | same |
| 15 | 28 | 54 | 1.92x | 90 | 0.60x | same |
| 16 | 25 | 37 | 1.47x | 25 | 1.48x | same |
| 17 | 49 | 235 | 4.80x | 153 | 1.54x | same |
| 18 | 70 | 226 | 3.22x | 202 | 1.12x | same |
| 19 | 41 | 91 | 2.22x | 84 | 1.08x | same |
| 20 | 41 | 92 | 2.25x | 86 | 1.07x | same |
| 21 | 107 | 278 | 2.60x | 199 | 1.40x | same |
| 22 | 25 | 42 | 1.70x | 19 | 2.21x | same |

## SF10

| Q | DuckDB 2.0.1 ms | Burrmill ms | ratio | datafusion-cli 55.2.0 ms | Burrmill / datafusion-cli | answer |
|---|---:|---:|---:|---:|---:|---|
| 1 | 356 | 1,073 | 3.02x | 676 | 1.59x | same |
| 2 | 78 | 254 | 3.25x | 175 | 1.45x | same |
| 3 | 267 | 627 | 2.35x | 458 | 1.37x | same |
| 4 | 155 | 586 | 3.78x | 186 | 3.15x | same |
| 5 | 289 | 920 | 3.18x | 679 | 1.35x | same |
| 6 | 128 | 467 | 3.65x | 286 | 1.63x | same |
| 7 | 319 | 712 | 2.23x | 1,053 | 0.68x | same |
| 8 | 366 | 1,082 | 2.96x | 742 | 1.46x | same |
| 9 | 656 | 1,669 | 2.54x | 1,239 | 1.35x | same |
| 10 | 316 | 807 | 2.55x | 607 | 1.33x | same |
| 11 | 71 | 243 | 3.42x | 147 | 1.65x | same |
| 12 | 185 | 747 | 4.04x | 407 | 1.84x | same |
| 13 | 513 | 896 | 1.75x | 460 | 1.95x | same |
| 14 | 263 | 457 | 1.74x | 365 | 1.25x | same |
| 15 | 194 | 431 | 2.22x | 601 | 0.72x | same |
| 16 | 90 | 164 | 1.82x | 108 | 1.52x | same |
| 17 | 320 | 1,685 | 5.26x | 1,276 | 1.32x | same |
| 18 | 414 | 1,528 | 3.69x | 1,845 | 0.83x | same |
| 19 | 235 | 538 | 2.29x | 547 | 0.98x | same |
| 20 | 276 | 570 | 2.07x | 537 | 1.06x | same |
| 21 | 744 | refused | | 2,060 | | refused under 2 GiB with no spill directory |
| 22 | 177 | 634 | 3.58x | 191 | 3.32x | same |

## Q17: only the parts the query keeps (#433)

Q17 had the widest ratio at SF10 in the table above (5.26x), and in a run of all 22 on 62ec1e8
(load 26.8 at the start, 10:26:43) it was still the widest, 4.90x, with Q12 next at 4.77x.

**The cause.** Decorrelating `l_quantity < (SELECT 0.2 * avg(l_quantity) ... WHERE l_partkey =
p_partkey)` groups all of `lineitem` by `l_partkey`, and only then joins the averages to the
outer query's parts. `EXPLAIN ANALYZE` at SF10 on 62ec1e8 (load 25.5): 59.97M rows into the final
aggregate, 9.72 s of its CPU and 1.14 s of the partial one's, 2.00M averages out, and a semi join
that built its table on those 2M (113.8 MB) to keep the 2,040 parts of `Brand#23` and `MED BOX`.
The partial aggregate skipped aggregating 59.18M of the rows, since nearly every part is a group.

**The A/B.** The same query written with the subquery's `lineitem` cut to those parts by hand,
`WHERE l_partkey IN (SELECT p_partkey FROM part WHERE p_brand = 'Brand#23' AND p_container = 'MED
BOX')`, gave the same answer as DuckDB 2.0.1 and took 2,097 ms against Q17's 3,282, best of 3 at a
load of 26.2 (10:35:47); built only for this, not committed. In its `EXPLAIN ANALYZE` the aggregate
saw 61.38K rows and took 1.6 ms. What was left was the two `lineitem` scans, each with a join's
dynamic filter: 5.04 s and 5.80 s of row-filter CPU over 60M rows each.

**The fix.** `JoinedGroups` (`crates/burrmill/src/df/joinedgroups.rs`) does that rewrite. It
applies to a join on a column that an aggregate on one side groups by, where every row on the other
side has a key that a filtered table scan holds. The aggregate's input becomes a semi join on that
scan's keys. A group outside them meets no row of the other side, and the groups are split by the
key, so no group that is kept changes. It applies only where the join drops the aggregate's
unmatched rows, never with NULL equal to NULL, and only when the scan reads at most a quarter of
what the aggregate's input does. In TPC-H, Q2, Q17 and Q20 take it, one more hash join each.

That semi join is also collected when its keys read at most a sixteenth of what it filters
(`buildside.rs`). Partitioned, the dynamic filter it pushes into the scan is a `CASE` over each
row's repartition hash. In one `EXPLAIN ANALYZE` at SF10, built with only the semi join collected,
the scan under the still-partitioned outer join took 3.19 s of row-filter CPU and the collected
semi join's 0.32 s, over the same 60M rows. Collecting every join whose build side reads a sixteenth
of its probe or less, tried and not committed, made Q17 faster again but Q18, Q19, Q20 and Q22
slower, at loads of 11 to 13.

`tests/df_joinedgroups.rs` catches the slow plan. With the rule off, the Q17-shape test fails: the
average is computed for all 50 parts instead of the 10 the query keeps.

**Before and after.** 62ec1e8 (before) and 9adc6d8 (after), alternating, three rounds each, best of
5 per round, DuckDB 2.0.1 in the same process run before each. Every answer was the same as DuckDB's.
The 1-minute load was 10.7 to 13.2 at SF10 (11:28:50 to 11:30:18) and 10.4 to 11.0 at SF1 (11:30:18
to 11:30:34). The host was over 8 for most of the hour: a wait from 11:12 ended at 11:14:50 at
7.5, and it was back over 9 within the first round.

| SF | round | Q17 DuckDB ms, before / after | Q17 Burrmill before ms | Q17 Burrmill after ms | Q2 before / after ms | Q20 before / after ms |
|---|---:|---:|---:|---:|---:|---:|
| 10 | 1 | 276 / 276 | 1,553 | 827 | 245 / 242 | 490 / 504 |
| 10 | 2 | 296 / 295 | 1,469 | 756 | 227 / 186 | 507 / 471 |
| 10 | 3 | 290 / 283 | 1,523 | 838 | 244 / 198 | 498 / 488 |
| 1 | 1 | 33 / 37 | 146 | 116 | 34 / 32 | 58 / 52 |
| 1 | 2 | 39 / 32 | 147 | 118 | 37 / 30 | 59 / 51 |
| 1 | 3 | 29 / 37 | 141 | 113 | 35 / 29 | 52 / 55 |

Against the DuckDB run beside each, Q17 at SF10 went from 4.96x to 5.63x to 2.56x to 3.00x, and
at SF1 from 3.77x to 4.86x to 3.05x to 3.69x. What is left at SF10 is the two `lineitem` scans'
dynamic filters, the outer join's still partitioned.

**Next widest.** A run of all 22 after the change (9adc6d8, best of 3, load 9.5 at 11:30:40 and
16.7 at 11:32:32) had Q18 at 6.01x, Q12 at 5.84x, Q1 at 5.46x and Q17 at 3.05x. Q18 was at
2.61x in the run of all 22 before the change, and the rule does not change its plan, so its 6.01x
is put down to the load. Q12 was second-widest in both runs.

## Spilling: Q21 under 2 GiB (#383)

Q21's `EXISTS` and `NOT EXISTS` are each a partitioned hash join that builds on all of `lineitem`.
A hash join holds its build side in memory and cannot spill it, so at SF10 the statement was
refused at 1,791 of its 1,792 MB, the refusal naming `HashJoinInput`, with a spill directory or
without. Under a budget with a spill directory the engine now plans a hash join whose build side
may not fit as a sort-merge join, whose sorts and key buffer spill
(`crates/burrmill/src/df/spillingjoins.rs` says when).

Measured 2026-11-25 on 2d44933 against **DuckDB 2.0.1** on CPU (the CLI, as above), on the ThinkPad,
**a shared host under other work**: the 1-minute load is given beside every timing, and DuckDB ran
in the same few minutes, so the ratio is fairer than the absolute times. Burrmill and DuckDB ran in
separate processes so that neither's allocator counts against the other's peak RSS (DuckDB 2.0.1's
is its CLI process's, from `wait4`); a third run of both compared the answers. Budget 2 GiB, 8
threads, spill directory on the same disk. Best of 3 at SF10 and SF30; at SF100 too, each run being
under 5 minutes. "Most spilled" is the most the spill directory held at once; "spilled in all" is
every operator's `spilled_bytes` from `EXPLAIN ANALYZE`, summed. The pool is what the engine's
operators reserved; the rest of the RSS is outside it. The two spill figures are taken differently,
one by sampling the directory and one from the operators' metrics, and at SF10 the first is the
larger; why is not measured.

| SF | DuckDB | DuckDB ms | DuckDB peak RSS MiB | Burrmill ms | ratio | Burrmill peak RSS MiB | pool peak MiB | most spilled MiB | spilled in all MiB | load at start | answer |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 10 | 2.0.1 | 1,556 | 2,384 | 3,903 | 2.51x | 2,261 | 1,652 | 636 | 563 | 11.3 / 9.0 | same |
| 30 | 2.0.1 | 1,839 | 1,577 | 14,157 | 7.70x | 2,336 | 1,681 | 6,367 | 12,024 | 11.6 / 9.3 | same |
| 100 | 2.0.1 | 6,354 | 4,252 | 147,454 | 23.21x | 3,143 | 1,678 | 21,657 | refused | 6.8 / 11.8 | same |

The two loads are at the start of Burrmill's runs and of DuckDB's. **At SF30 Q21 was refused once
under 2 GiB with a spill directory:** in the first process of three runs (load 7.5), one run ended
`Not enough memory to continue external sort`, an `ExternalSorter` refused more while four others
held 316 MB each. Its `EXPLAIN ANALYZE` in the same process answered, and so did all 12 runs after
it in eight more processes (14,157 to 15,491 ms each a single run or a best of three, loads 10.9 to
12.9); the SF30 row is the best of three in one of those, its pool peak and spilled in all from the
process that was refused once. Why is [below](#why-a-sort-was-refused-sometimes-434). **At SF100 the same
happened to the `EXPLAIN ANALYZE`:** the three timed runs answered, and the fourth run in that
process, the `EXPLAIN ANALYZE`, was refused over the 1,792 MB, so its spilled-in-all is missing; the
run that compared the answers took 102,440 ms at load 13.5, faster than the best of the three. At
SF10 the load rose to 23.3 by the end, and the run that compared the answers, one run each, took 881
ms on DuckDB 2.0.1 and 3,555 on Burrmill.

### Why a sort was refused sometimes (#434)

A sort that holds nothing, starting or having just spilled, has nothing to spill, so DataFusion's
`ExternalSorter` refused the statement when the pool refused its batch: `Not enough memory to
continue external sort`. The pool's sorts borrow past their fair share, and a loan leaves the reserve
and one fair share free: one share for every consumer under its share, not one each. A trace of the
pool's refusals in Q21 at SF30, on 2d44933 with a debug print, three processes of 10 runs, every run
answering, had 55 to 96 spillers registered and 42 to 187 MB held by consumers that cannot spill.
At 96 spillers and 119 MB a share was about 18 MB, (1,792 MiB less 119 MB) / 96, while the largest
holders held up to 983 MB and the pool sat at all but the reserve and about that share.
Repartitions under their shares took from it as they refilled after spilling (411,057 refusals in
one process), and what a spilling sort gave back was lent again to the others before it asked for
its next batch, which the sorts reserved at 64 KB to 6 MB. One that found less than that free
refused, the reserve's 112 MiB beside it. The 30 traced runs at 8 threads did not hit it; at 16
threads, with more spillers and so a smaller share, the engine without the fix hit it in every
process (below), and the test below hits it every time.

A spiller holding nothing may now take the reserve too, within its share; a repartition still
spills the batch. `sorters_share::a_sort_holding_nothing_gets_its_batch` in
`crates/burrmill/src/df/session.rs` runs 16 of DataFusion's sorts on one such pool, fed a batch at
a time on one thread: before, a sort starting while four others held loans was refused at 0 bytes
held, with that error; now every sort answers, in order.

Measured 2026-11-25 with the fix (0bfdd36) and, beside it, the same tree without it, against the
bench's bundled DuckDB v1.5.4 (no DuckDB CLI on this host), 2 GiB, spill directory on the same disk,
on the shared host: the 1-minute load at the start of each is given.

| SF | threads | engine | runs | answered | Burrmill best ms | DuckDB ms | peak RSS MiB, Burrmill alone | load at start |
|---|---:|---|---:|---|---:|---:|---:|---:|
| 30 | 8 | fixed | 10, compared | 10, same | 14,889 | 5,505 | | 13.3 |
| 30 | 8 | fixed | 10, alone | 10 | 15,756 | | 2,274 | 28.3 |
| 100 | 8 | fixed | 5, compared, and `EXPLAIN ANALYZE` | 5, same; the `EXPLAIN ANALYZE` too | 121,260 | 22,638 | | 12.7 |
| 30 | 16 | without | 4 processes of up to 5 | refused in all 4 | | | | 10.4 |
| 30 | 16 | fixed | 5 alone, then 10 compared | 15, same | 11,336 alone, 11,911 compared | 4,257 | 2,545 | 11.1 |

"Same" is the kept run's answer against DuckDB's; the bench compares that one. Each refusal at
16 threads was the one above, `Failed to allocate additional 5.8 MB for ExternalSorter[n] with 0.0
B already allocated`, 0.0 B available; in the first, five other sorts held 126 to 242 MB. At SF100 the operators spilled 45,023 MiB in all. With a spill directory at SF10, Q1, 3,
5, 9, 10, 12, 13, 17, 18 and 21 all answered as DuckDB did, one run each.

**Peak RSS at SF100 is still above the budget plus 1 GiB at times, with the fix or without.** One
run each, Burrmill alone, alternating: with the fix 3,347, 3,375, 3,298, 3,081 and 3,210 MiB;
without, 3,068, 3,087, 3,019 and 3,140. The pool peaked at 1,675 to 1,684 MiB in all of them. The
fix only grants what the pool would otherwise refuse when the total passes the pool less its
reserve, 1,680 MiB, so in the four fixed runs whose pool stayed under that it granted nothing the
old rule would not. Those runs still peaked at 3,081 to 3,375 MiB, so the spread is outside the
pool (see below) and not from the fix.

**Peak RSS is the budget plus up to 1,095 MiB** (3,143 MiB at SF100; on 2026-11-24 it was up to
926). The pool stays under its 1,792 MB at every scale factor; what is above it is the process
before the query (about 225 MiB: the binary, the tables' footers), Parquet decoding outside the
pool, and pages the allocator freed and kept. On 2026-11-24, at SF100 a build with the system
allocator in place of jemalloc (`--features alloc-stats`, 1 run, 284 s under its counting) peaked at
2,533 MiB, so jemalloc's retention is about 440 MiB of it. A host that budgets 2 GiB should allow
more than 3 GiB resident for such a statement: 3,143 MiB here.

**Without a spill directory nothing changes:** Q21 at SF10 is refused with the same error, the pool
at 1,791 MiB, and plans are as before.

**Other queries with a spill directory at SF10.** Q18 was the one regression the rule first
caused: as a sort-merge join it was refused under 2 GiB. Its `IN` builds on an aggregate's groups,
624 of 15.00 M orders, which the files do not measure, so a build side under an aggregate keeps its
hash join. Measured 2026-11-25 on 2d44933, DuckDB 2.0.1 on CPU. Every answer with a spill
directory at 2 GiB matched DuckDB 2.0.1's, Q17's too (1,794 ms against DuckDB's 305 and
datafusion-cli's 1,555). Best of 3; the three runs were back to back, on the busiest box of the
day: the 1-minute load was 22.7 at the start of the first column (02:31:38), with DuckDB 2.0.1 and
datafusion-cli in the same window, 22.5 at the start of the 16 GiB column, 18.4 at the start of
the last, and 16.6 at the end. At 16 GiB Q21 keeps its hash joins, which fit.

| Q | DuckDB 2.0.1 ms | spill dir, 2 GiB ms | spill dir, 16 GiB ms | no spill dir, 2 GiB ms | most spilled MiB at 2 GiB |
|---|---:|---:|---:|---:|---:|
| 1 | 394 | 1,411 | 1,144 | 1,231 | 0 |
| 3 | 285 | 739 | 730 | 746 | 0 |
| 5 | 622 | 2,291 | 1,001 | 1,015 | 0 |
| 9 | 1,903 | 2,330 | 1,702 | 1,810 | 0 |
| 10 | 363 | 1,006 | 818 | 861 | 0 |
| 12 | 181 | 910 | 864 | 885 | 0 |
| 13 | 501 | 822 | 821 | 893 | 0 |
| 18 | 522 | 1,895 | 1,585 | 1,713 | 0 |
| 21 | 1,407 | 5,874 | 2,208 | refused | 781 |

With a spill directory and memory to spare (16 GiB) no query is slower than without one at
2 GiB. Of the eight that spilled nothing, the 2 GiB column with a spill directory is the slowest
of the three on all but Q3 and Q13, by up to 1,290 ms (Q5, against 1,001 at 16 GiB); it ran first
and at the highest load, so that is not put down to the spill directory here.

## Running it

    tpchgen-cli parquet -s 10 --parts 16 --output-dir sf10
    DIR=sf10 THREADS=8 RUNS=3 DUCKDB=/path/to/duckdb DFCLI=$(which datafusion-cli) cargo run --release -p burrmill-bench -- tpch
    DIR=sf10 THREADS=8 RUNS=3 QUERIES=21 ENGINE=burrmill SPILL=spill ANALYZE=1 cargo run --release -p burrmill-bench -- tpch

Without `DUCKDB` the bench compares against its bundled DuckDB, which reports v1.5.4. With it,
DuckDB is that CLI: each answer comes back through a Parquet file it writes (a `HUGEINT` cast to
`DECIMAL(38,0)` first, as Burrmill types it), and each time is its `.timer`.

The queries are DataFusion's own (`crates/burrmill-bench/tpch/`), with Q15's view written as a `WITH`
for all three engines, because Burrmill serves queries only.
