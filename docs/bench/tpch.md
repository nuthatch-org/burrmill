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
Q4 is now 2.76x to 3.14x at SF10, from 3.40x to 4.35x, its semi join collected
([below](#q4-one-hash-table-for-the-lineitem-scans-filter-433)).
Q6 is now 2.25x to 2.64x at SF10, from 3.78x to 3.99x, its `WHERE` evaluated above the Parquet
reader and not in it ([below](#q6-its-filter-above-the-reader-433)).
Q11 is now 1.66x to 1.76x at SF10, from 2.74x to 3.20x, its total summed from its groups and its
join run once ([below](#q11-one-join-for-the-groups-and-their-total-433)).
Q18 is now 2.27x to 2.62x at SF10, from 3.64x to 4.05x, its join reading the `HAVING`'s groups
rather than `lineitem` again ([below](#q18-the-havings-groups-in-place-of-the-rows-433)).
Q1 is now 1.91x to 2.10x at SF10, from 3.37x to 3.53x, its decimal arithmetic and sums no longer
checked row by row ([below](#q1-decimal-arithmetic-and-sums-without-a-check-per-row-433)).
Q8 is now 1.55x to 2.02x at SF10, from 1.63x to 2.75x at loads of 7 to 177, its join on the parts
of one type collected ([below](#q8-the-join-on-one-types-parts-collected-433)).
Q5 is now 1.69x to 1.79x at SF10, from 2.98x to 3.33x, its customers given the nations of its
region and its `lineitem` join collected ([below](#q5-the-regions-nations-reach-customer-433)).
Q4 is now 1.76x to 2.08x at SF10, from 3.07x to 3.19x, and Q22 1.56x to 1.70x, from 3.15x to 3.27x,
a join's filter over a scan of only its predicate's columns left to the join
([below](#q4-again-the-joins-filter-left-to-the-join-433)).
Q3 is now 1.66x to 1.82x at SF10, from 2.60x to 3.01x, the filters of the joins that probe its
`lineitem` and `orders` scans left to those joins
([below](#q3-the-filters-of-the-joins-it-feeds-left-to-the-joins-433)).

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

`tests/df_literalkeys/joinedgroups.rs` catches the slow plan. With the rule off, the Q17-shape test fails: the
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

## Q12: building on the lineitem rows its filter keeps (#433)

**The cause.** A hash join built on the side whose files hold fewer bytes, whatever its filter kept
(`BuildOnSmaller`, `crates/burrmill/src/df/buildside.rs`). Q12's `lineitem` files hold 2.55 GB at
SF10 and `orders`' 678 MB, so the join built on every order. Its filter keeps 310.8K of
`lineitem`'s 60M rows. On c0ff95b at SF10 the `EXPLAIN ANALYZE` showed a partitioned join with
`build_input_rows=15.00 M`, `build_mem_used=930.2 MB` and `build_time=1.34s`, probed by those
310.8K rows. The pool peaked at 931 MiB. The dynamic filter from all 15M orders cut nothing from
`lineitem` and cost its scan 768 ms of row-filter CPU. DuckDB 2.0.1 builds on the `lineitem` rows.

**The fix.** A filter's share is now guessed from its predicate, as System R did: a tenth for an
equality, a third for a range or a comparison of two columns, the two bounds of one column counted
once. A predicate it cannot read keeps everything. The guess decides only when it puts one side
under an eighth of the other. Q12's `lineitem` is guessed at a 36th of `orders` at SF10. A join the
guess decided is also collected, if inner. Partitioned, its dynamic filter on `orders` was a `CASE`
over each row's repartition hash: 2.37 s of that scan's CPU against 0.30 s collected (one `EXPLAIN
ANALYZE` each, load 50). After the fix, Q12 at SF10 builds on 310.8K rows, 15.6 MB, the `orders`
scan gives 291.9K of 15M rows, and the pool peaks at 15 MiB (`EXPLAIN ANALYZE` on 9eaaa03, load
28.3).

**What was tried and not kept.** Two rules were tried before this one. Taking every filter to keep
DataFusion's default 20% changed the plans of Q5, 7, 10, 12, 13 and 21. Q13 built on the 15M
orders of a `NOT LIKE` rather than on 1.5M customers, and Q7 built on two years of `lineitem`.
Letting the guess decide at any margin built Q10 on the `lineitem` rows of `l_returnflag = 'R'`
rather than on the joined orders. With the rule kept, the `EXPLAIN` of all 22 before and after
differs in Q12 alone, at SF1 and at SF10. `a_side_whose_filter_keeps_few_rows_is_built_on_and_collected`
(`crates/burrmill/tests/df_buildside.rs`) fails if the join goes back to building on the side that
reads fewer bytes.

**Measured** 2026-11-25 on 9eaaa03 (9e98fab only adds the changelog) against c0ff95b, on the ThinkPad as above, 8 threads, 2 GiB, no
spill directory. There were three rounds at each scale factor, alternating before and after. Each
run is the best of 5 for Burrmill and for the DuckDB 2.0.1 CLI, in the same process, so both
engines ran in the same window. The load was over 8 for 20 minutes before this, up to 139; it fell
to 7.98 at 16:29:33 and rose again while these ran. Every answer was the same as DuckDB's.

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | before ms | ratio | DuckDB 2.0.1 ms | after ms | ratio | Burrmill peak RSS MiB, before / after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 8.66 / 12.92 | 211 | 970 | 4.60x | 272 | 904 | 3.32x | 1,675 / 407 |
| 10 | 2 | 12.92 / 13.82 | 221 | 1,318 | 5.96x | 209 | 654 | 3.13x | 1,678 / 424 |
| 10 | 3 | 13.82 / 13.67 | 168 | 938 | 5.58x | 180 | 654 | 3.63x | 1,696 / 413 |
| 1 | 1 | 13.67 / 13.67 | 23 | 85 | 3.69x | 22 | 62 | 2.82x | 510 / 378 |
| 1 | 2 | 13.67 / 14.34 | 22 | 94 | 4.26x | 23 | 66 | 2.86x | 520 / 370 |
| 1 | 3 | 14.34 / 14.34 | 24 | 84 | 3.49x | 23 | 58 | 2.51x | 506 / 375 |

Q12 at SF10 went from 4.60x–5.96x DuckDB 2.0.1 to 3.13x–3.63x, and at SF1 from 3.49x–4.26x to
2.51x–2.86x. What is left is in the scans. In that `EXPLAIN ANALYZE` at load 28.3, the
`lineitem` scan, filtering all 60M rows, took 12.29 s of CPU, 2.25 s of it the row filter. The
`orders` scan took 2.01 s. The join holds a 15.6 MB build. That load inflates CPU times, so these
are not compared with the figures above.

**All 22 after the fix**, at SF10 on 9eaaa03, best of 3, DuckDB 2.0.1 in the same process. The load
was 17.76 at the start (16:30:42) and 17.83 at the end (16:32:33). All 21 answers given were the same
as DuckDB's, and Q21 was refused as above. The widest ratios were Q17 at 5.33x (this tree does not
have #438), Q4 at 4.46x, Q11 and Q22 at 3.92x, Q18 at 3.78x, and Q6 at 3.47x. Q12 was at 3.19x. At
SF1, one run each at a load of 22.4, all 22 answers were the same as DuckDB's.

**Again on main with #438**, the merge 6e70f64 against main at 6e79d8d. #438's `collect_keys` stands
beside this rule. The `EXPLAIN` of all 22 differs in Q12 alone, at SF1 and at SF10, and all 22
answers are the same as DuckDB 2.0.1's at SF1; at SF10 21 are, Q21 being refused as above. The
rounds are as before, three of them alternating, each the best of 5, DuckDB 2.0.1 in the same
process:

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | main ms | ratio | DuckDB 2.0.1 ms | merge ms | ratio | Burrmill peak RSS MiB, main / merge |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 10.01 / 9.77 | 158 | 715 | 4.52x | 167 | 492 | 2.95x | 1,717 / 416 |
| 10 | 2 | 9.77 / 9.65 | 149 | 704 | 4.72x | 160 | 463 | 2.90x | 1,716 / 435 |
| 10 | 3 | 9.65 / 9.32 | 165 | 777 | 4.71x | 153 | 468 | 3.06x | 1,714 / 433 |
| 1 | 1 | 8.57 / 7.96 | 21 | 78 | 3.71x | 24 | 57 | 2.39x | 515 / 367 |
| 1 | 2 | 7.96 / 7.96 | 22 | 75 | 3.40x | 22 | 57 | 2.59x | 525 / 372 |
| 1 | 3 | 7.96 / 7.96 | 22 | 72 | 3.27x | 21 | 55 | 2.61x | 506 / 372 |

The 22 at SF10 on the merge, best of 3, DuckDB 2.0.1 in the same process. The load was 7.96 at the
start (17:30:02) and 9.74 at the end (17:31:16). The widest ratios were Q4 at 3.94x, Q6 at 3.74x,
Q18 at 3.66x, Q22 at 3.37x and Q12 at 3.29x. Q17 was at 2.65x.

## Q4: one hash table for the lineitem scan's filter (#433)

Q4 was the widest ratio at SF10 after #440. On 398b854, with the DuckDB 2.0.1 CLI downloaded for
this, it took 499 ms against DuckDB's 132 ms, 3.78x, best of 3 at a load of 14.88 (18:15:21).

**The cause.** Q4's `EXISTS` is a semi join that builds on the orders of one quarter and is probed
by `lineitem`. The build side was right, but the join was partitioned, so the dynamic filter it
pushed into the `lineitem` scan was a `CASE` over each row's repartition hash, with a hash lookup
in each branch. In that `EXPLAIN ANALYZE` at SF10 the join built on 573.7K orders (31.8 MB). The
`lineitem` scan read all 60M rows, let 1.45M through, took 3.12 s of CPU and spent 1.77 s of that in
the row filter. DuckDB 2.0.1's `EXPLAIN ANALYZE` builds the same join on the 573,671 orders and puts
`l_orderkey IN PRF(o_orderkey)` in its `lineitem` scan, which gives 2,294,856 rows, 1,450,571 after
`l_commitdate < l_receiptdate`. `BuildOnSmaller` collected a semi join only when it built on the
keys of #438's `JoinedGroups` rewrite; Q4's orders are guessed at a third of their 678 MB, so the
guess was not decisive and the join stayed partitioned.

**The A/B.** The same tree with Q4's join collected (the fix below, before its comments were
written). In its `EXPLAIN ANALYZE` at SF10 (load 10.15, 18:20:30) the build took 18.1 ms on the
same 573.7K rows, and the `lineitem` scan took 1.93 s of CPU, 0.39 s of it the row filter. Two
rounds at SF10, best of 5, alternating, loads 8.05 to 10.95 (18:19:44 to 18:20:22): 528 and 533 ms
before, 424 and 410 after. Every answer was the same as DuckDB's.

**The fix.** A semi join is now collected where its build side is guessed to keep at most an
eighth of what its probe side reads (`filters_probe`, `crates/burrmill/src/df/buildside.rs`). Q4's
orders are guessed at 226 MB against `lineitem`'s 2.55 GB. The `EXPLAIN` of all 22 before and
after differs in Q2, Q4 and Q20, at SF1 and at SF10, each a semi join going from `Partitioned` to
`CollectLeft`: Q2's on its joined-group keys, Q20's two on `p_partkey` and `ps_suppkey`.
`a_semi_join_on_a_few_filtered_rows_is_collected` (`crates/burrmill/tests/df_buildside.rs`) fails
if a Q4-shaped semi join goes back to being partitioned.

**Measured** 2026-11-25 on 51b010f against 398b854, on the ThinkPad as above, 8 threads, 2 GiB, no
spill directory: three rounds per scale factor, before then after, each the best of 5 for Burrmill
and for the DuckDB 2.0.1 CLI in the same process. The load was 9.56 at 18:25:46, and the wait for
it to fall under 8 ended at 18:26:11 (7.41). It rose to 9.17 during the rounds. Every answer was the
same as DuckDB's.

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | before ms | ratio | DuckDB 2.0.1 ms | after ms | ratio | Burrmill peak RSS MiB, before / after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 7.04 / 7.74 | 146 | 635 | 4.35x | 154 | 425 | 2.76x | 440 / 520 |
| 10 | 2 | 7.74 / 8.52 | 147 | 632 | 4.30x | 138 | 421 | 3.05x | 436 / 498 |
| 10 | 3 | 8.52 / 8.72 | 145 | 493 | 3.40x | 133 | 418 | 3.14x | 440 / 510 |
| 1 | 1 | 8.72 / 8.91 | 23 | 61 | 2.66x | 21 | 48 | 2.30x | 385 / 383 |
| 1 | 2 | 8.91 / 8.75 | 21 | 59 | 2.81x | 20 | 48 | 2.38x | 393 / 380 |
| 1 | 3 | 8.75 / 9.17 | 23 | 66 | 2.87x | 20 | 49 | 2.46x | 384 / 389 |

The same rounds timed Q2 and Q20, the other two plans that change, best of 5:

| SF | round | Q2 before / after ms | Q20 before / after ms |
|---|---:|---:|---:|
| 10 | 1 | 205 / 248 | 493 / 448 |
| 10 | 2 | 236 / 199 | 484 / 437 |
| 10 | 3 | 204 / 171 | 479 / 436 |
| 1 | 1 | 29 / 35 | 54 / 47 |
| 1 | 2 | 32 / 26 | 51 / 46 |
| 1 | 3 | 32 / 36 | 52 / 44 |

Q4 at SF10 went from 3.40x–4.35x DuckDB 2.0.1 to 2.76x–3.14x, and at SF1 from 2.66x–2.87x to
2.30x–2.46x. Q20 is faster in every round. Q2 is faster in three rounds of six and slower in three,
by up to 43 ms at SF10, so no change in Q2 is shown either way. Q4's peak RSS at SF10 rose by 58 to 80 MiB; why is not measured. What is left
of Q4 is mostly the `lineitem` scan, 1.93 s of CPU where DuckDB's took 480 ms in its
`EXPLAIN ANALYZE`, and the `orders` scan, 474 ms of CPU.

**All 22 after the fix**, at SF10 on 51b010f, best of 3, DuckDB 2.0.1 in the same process. The load
was 8.76 at the start (18:27:41) and 12.91 at the end (18:29:03). All 21 answers given were the same
as DuckDB's, and Q21 was refused as above. The widest ratios were Q6 at 5.07x, Q4 at 3.64x (510 ms),
Q18 at 3.52x, Q22 at 3.34x, Q11 at 3.11x and Q12 at 3.07x. At SF1, one run each at a load of 11.88,
all 22 answers were the same as DuckDB's.

## Q6: its filter above the reader (#433)

Q6 was the widest ratio at SF10 in the run of all 22 after #443, 5.07x. On f1b227a, with the DuckDB
2.0.1 CLI downloaded for this, four runs of Q6 alone, best of 5, gave 415 to 460 ms against DuckDB's
110 to 130, 3.32x to 3.77x, at loads of 16.25 down to 13.74 (19:38:43 to 19:39:01). At loads of
4.87 to 6.83, in the rounds below, it was 3.78x to 3.99x. So the gap is about 3.3x to 4x, and the 5.07x
included the load.

**The cause.** Burrmill reads Parquet with `pushdown_filters` on, so the reader evaluates a scan's
`WHERE` as a row filter: one pass per conjunct, each over the rows the passes before it kept, then
the other columns decoded for the rows all of them keep. Q6's `WHERE` is five conjuncts on three
columns, and the 1.9% of `lineitem` it keeps is scattered through every page, so no decoding is
saved. In an `EXPLAIN ANALYZE` at SF10 (load 9.07) the scan took 3.27 s of CPU, and only 186.5 ms
of it evaluated the conjuncts. The `FilterExec` above it, which checks the same `WHERE` again, took
18.35 ms and the partial aggregate 19.68 ms. DuckDB 2.0.1's `EXPLAIN ANALYZE` has the same three
filters in its scan, 1,139,264 rows out, and 1.21 s for the scan.

**The A/B.** The same tree with `pushdown_filters` off for every scan, built only for this and not
committed: the scan read all 59.99M rows in 955.62 ms of CPU and the `FilterExec` took 886.97 ms, at
the same load. Q6 took 291 and 290 ms against 422 and 402 with the row filter (loads 9.47 to 9.74,
19:41:46 to 19:42:00). The other things tried left the row filter in place and were not kept:

| tried (Q6 at SF10, best of 5, two runs, loads 8.84 to 10.89) | ms |
|---|---:|
| as on main | 447, 403 |
| `pushdown_filters` off | 281, 282 |
| conjuncts on the same columns as one predicate | 355, 359 |
| the whole `WHERE` as one predicate | 398, 367 |
| `reorder_filters` | 576, 480 |
| `force_filter_selections` | 347, 348 |
| `max_predicate_cache_size` 0 | 378, 387 |

The 2,048-row batch a budgeted session runs at is not the cause: at 8,192 rows Q6 took 380 and
400 ms with the row filter, 255 and 243 without. The cast in `CAST(l_shipdate AS Timestamp(ns)) <
788918400000000000`, which is how `date '1994-01-01' + interval '1' year` arrives, is not the cause
either. With the row filter, Q6 with `date '1995-01-01'` written in took 393 to 426 ms against 397
to 415 (loads 10.47 to 11.18). With the fix below it is 15 to 20 ms: 263 to 273 ms against 278 to 290
(loads 7.08 to 7.39). It is left as it is.

**Off for every scan is not the fix.** With all of TPC-H's scans filtering above the reader, 19 of
the 21 queries answered were faster at SF10, but Q7 and Q18 were slower (650 and 1,401 ms before,
921 and 1,664 after, Burrmill alone, best of 3, loads 12.12 to 12.87). Their small builds put
`IN (SET)` filters in the scans below, and evaluated in the reader those cut the rows that reach
the joins above: 117.0K of Q7's `lineitem` rows reached its join, not 18.23M, and Q18's `orders` gave 624, not
15.00M. With only the joins' filters in the row filter and the `WHERE` above, most queries were
slower than with both (Q3 809 ms, Q14 799). A row filter also pays where the rows it keeps are few or come
in runs, and where the columns it spares are text. Over `lineitem` at SF10, best of 3, two runs,
loads 6.26 to 7.85:

| probe | with the row filter, ms | filtered above, ms |
|---|---:|---:|
| `l_orderkey % 50 = 7`, with `max(l_comment)` | 358, 369 | 458, 436 |
| `l_shipdate = date '1995-03-01'`, three fixed-width columns summed | 206, 203 | 248, 250 |
| Q6's `WHERE`, with `max(l_comment)` and `max(l_shipinstruct)` | 649, 648 | 602, 592 |

**The fix.** `StaticRowFilters` (`crates/burrmill/src/df/staticfilters.rs`) switches the row
filter off for a Parquet scan whose predicate holds no dynamic filter, from a join or a top-k, and
no equality with a literal, and that reads no column besides the predicate's that is not
fixed-width. The predicate still prunes row groups and pages. Nothing is lost: a table's `WHERE`
reaches the scan as `Inexact`, so the `FilterExec` above it stays, and `CancelExec` does not pass a
`WHERE` down again. With it, of the scans of all 22 at SF1, the ones that lose their row filter are
in Q6, 7, 9, 12, 15, 16, 19, 20, 21 and 22: `lineitem` in Q6, 12, 15 and one of Q21's two, `part`
in Q9, 16, 19 and one of Q20's two, both of Q7's `nation`, Q16's `supplier` and both of Q22's
`customer`. All three probes above keep their row filter.
`a_q6_shaped_scan_filters_above_the_reader` (`crates/burrmill/tests/df_staticfilters.rs`) fails if a
Q6-shaped scan goes back to filtering in the reader; with the rule off its reader dropped 19.87K
rows. Three more tests there check that an equality, a text column and a join's filter each keep the
row filter.

**Measured** 2026-11-25 on 9c2dd97 (built before one expression in it was rewritten for clippy,
the same logic) against f1b227a, on the ThinkPad as above, 8 threads, 2 GiB, no
spill directory: three rounds per scale factor, before then after, each the best of 5 for Burrmill
and for the DuckDB 2.0.1 CLI in the same process. The load was under 8 throughout, 4.87 at the
start (20:17:25) and 6.76 at the end (20:17:46). Every answer was the same as DuckDB's.

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | before ms | ratio | DuckDB 2.0.1 ms | after ms | ratio | Burrmill peak RSS MiB, before / after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 4.87 / 5.44 | 105 | 404 | 3.85x | 125 | 282 | 2.25x | 460 / 395 |
| 10 | 2 | 5.44 / 6.38 | 117 | 442 | 3.78x | 102 | 268 | 2.63x | 461 / 396 |
| 10 | 3 | 6.38 / 6.83 | 105 | 419 | 3.99x | 112 | 296 | 2.64x | 462 / 396 |
| 1 | 1 | 6.83 / 6.83 | 18 | 46 | 2.55x | 18 | 32 | 1.75x | 427 / 343 |
| 1 | 2 | 6.83 / 6.83 | 18 | 52 | 2.90x | 19 | 33 | 1.73x | 426 / 344 |
| 1 | 3 | 6.83 / 6.76 | 16 | 46 | 2.88x | 16 | 30 | 1.87x | 427 / 342 |

Q6 at SF10 went from 3.78x–3.99x DuckDB 2.0.1 to 2.25x–2.64x, and at SF1 from 2.55x–2.90x to
1.73x–1.87x. What is left is the scan and the `FilterExec` over all 60M rows, 992.68 ms and 914.44 ms
of CPU in an `EXPLAIN ANALYZE` after the fix (load 8.49), where DuckDB's scan takes 1.21 s.

The other plans that change, in two rounds of all 22 at SF10 before and after, alternating, Burrmill
alone, best of 3, loads 4.91 to 12.54 (20:09:01 to 20:11:49): Q12 took 512 and 458 ms before and 301
and 378 after, and Q15 359 and 411 before and 315 and 328 after. Q7, 9, 19, 20 and 22 were within
the spread of the two runs before, and Q16 took 131 and 169 ms against 139 and 151. Q21 was refused
in all four, as above.

**All 22 after the fix**, at SF10 on 9c2dd97, best of 3, DuckDB 2.0.1 in the same process. The load
was 6.22 at the start (20:17:56) and 7.89 at the end (20:19:09). All 21 answers given were the same
as DuckDB's, and Q21 was refused as above. The widest ratios were Q11 at 3.89x (233 ms against 60),
Q18 at 3.77x, Q1 at 3.48x, Q5 at 3.08x and Q8 at 3.06x. Q6 was at 2.65x, Q12 at 1.99x and Q15 at
1.56x. At SF1, one run each at a load of 7.89, all 22 answers were the same as DuckDB's.

## Q11: one join for the groups and their total (#433)

Q11 was the widest ratio at SF10 in the run of all 22 after #453, 3.89x. On 03ed088, with the DuckDB
2.0.1 CLI downloaded for this, Q11 alone at SF10, best of 5, took 495 ms against DuckDB's 133, 3.72x,
at a load of 19.45 (21:10:55); in the rounds below, at loads under 7, it was 2.74x to 3.20x.

**The cause.** Q11 keeps the parts whose value is over a share of the value of all of them:
`GROUP BY ps_partkey HAVING sum(ps_supplycost * ps_availqty) > (SELECT sum(ps_supplycost *
ps_availqty) * 0.0001 FROM partsupp, supplier, nation WHERE ...)`, the subquery over the same join
as the query. Both were planned and run whole: two scans of `partsupp`, two joins to `supplier` and
two to `nation`. In an `EXPLAIN ANALYZE` at SF10 on 03ed088 (load 15.35 at its end, 21:11:18) each `partsupp`
scan read all 8.00M rows and gave 323.9K, in 679.70 and 626.64 ms of CPU, 436.53 and 429.26 ms of it
the row filter, a partitioned join's `CASE` over each row's repartition hash. The join to the 4.05K
German suppliers took 4.45 and 5.19 ms, the aggregate over 323.9K rows 11.28 ms partial and 17.49 ms
final, and the total's 7.44 ms. So nearly all of it is the scan, done twice. DuckDB 2.0.1's
`EXPLAIN ANALYZE` (loads 10.27 to 6.68, 21:24:30 to 21:25:01) also scans `partsupp` twice, 240 and
230 ms, and gives the same 323,920 rows each time; its total was 150 ms.

**The A/B.** Q11 written by hand with the grouped join as a `WITH`, and the total as `SELECT
sum(value) * 0.0001 FROM j`, which `ShareRepeats` computes once; built only for this, not committed.
It gave the same answer as DuckDB 2.0.1's and took 109 and 101 ms against Q11's 224 and 226, two
rounds alternating, best of 5, DuckDB on the same rewrite 43 and 39 ms against 68 and 75 (loads 90.64
to 120.51, 21:12:28 to 21:12:52, so only the ratio between the two counts).

**The fix.** `TotalOfGroups` (`crates/burrmill/src/df/grouptotal.rs`) makes that rewrite. Where a
`HAVING`'s filter over an aggregate holds an uncorrelated scalar subquery that is an aggregate with
no groups over a plan equal to the one the `HAVING`'s aggregate groups, and each of its aggregates
is one of that aggregate's `checked_sum`s, the subquery sums the aggregate's columns instead, and the
aggregate is defined once and read by both. `checked_sum` is exact and wide enough that no partial
sum overflows, so the sum of the groups' sums is the sum of the rows, of the same type. A sum of
doubles, a subquery over other rows, grouping sets (a rollup's groups hold its total too), and
anything correlated, volatile or recursive are left alone.
After it, the `EXPLAIN ANALYZE` at SF10 has one `partsupp` scan, 323.9K rows out of it (load 10.27,
21:24:30). The `EXPLAIN` of all 22 before and after differs in Q11 alone, at SF1 and at SF10.
`a_q11_shaped_total_sums_the_groups_and_scans_once` (`crates/burrmill/tests/df_grouptotal.rs`)
fails if the table goes back to being scanned twice; with the rule off it and the test of a total
over no rows fail, and the tests of a sum of doubles, of a total over other rows and of a rollup
still pass. The rollup's test was added after the measurements below, with the guard that leaves
grouping sets alone: without it the rule took the rollup's grouping id for its sum and the query was
refused. The guard does not change Q11's plan.

**Measured** 2026-11-25 on 271900c (built before rustfmt, which moved only layout) against 03ed088,
on the ThinkPad as above, 8 threads, 2 GiB, no spill directory: three rounds per scale factor,
before then after, each the best of 5 for Burrmill and for the DuckDB 2.0.1 CLI in the same process.
The load was under 8 throughout, 6.23 at the start (21:25:08) and 6.73 at the end (21:25:22). Every
answer was the same as DuckDB's.

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | before ms | ratio | DuckDB 2.0.1 ms | after ms | ratio | Burrmill peak RSS MiB, before / after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 6.23 / 6.53 | 73 | 201 | 2.75x | 69 | 120 | 1.74x | 411 / 412 |
| 10 | 2 | 6.53 / 6.97 | 69 | 189 | 2.74x | 63 | 111 | 1.76x | 415 / 429 |
| 10 | 3 | 6.97 / 6.97 | 65 | 208 | 3.20x | 61 | 101 | 1.66x | 406 / 416 |
| 1 | 1 | 6.97 / 6.73 | 13 | 29 | 2.27x | 12 | 24 | 2.02x | 353 / 353 |
| 1 | 2 | 6.73 / 6.73 | 13 | 25 | 1.96x | 14 | 25 | 1.80x | 356 / 359 |
| 1 | 3 | 6.73 / 6.73 | 14 | 29 | 2.04x | 14 | 26 | 1.85x | 349 / 351 |

Q11 at SF10 went from 2.74x–3.20x DuckDB 2.0.1 to 1.66x–1.76x, and at SF1 from 1.96x–2.27x to
1.80x–2.02x. What is left at SF10 is the one `partsupp` scan's row filter, still the partitioned
join's `CASE`: 572.14 ms of the scan's 907.92 ms of CPU in the `EXPLAIN ANALYZE` after the fix, at a
higher load than the one before.

**All 22 after the fix**, at SF10 on the same build, best of 3, DuckDB 2.0.1 in the same process.
The load was 6.51 at the start (21:25:28) and 9.53 at the end (21:26:40). All 21 answers given were
the same as DuckDB's, and Q21 was refused as above. The widest ratios were Q1 at 3.41x (981 ms
against 288), Q22 at 3.29x, Q18 at 3.24x, Q5 at 3.19x and Q4 at 3.01x. Q11 was at 1.96x. At SF1, one
run each at a load of 9.08, all 22 answers were the same as DuckDB's.

## Q18: the `HAVING`'s groups in place of the rows (#433)

Q18 was third widest at SF10 in the run of all 22 after #455, 3.24x. On f47a89b, with the DuckDB
2.0.1 CLI downloaded for this, Q18 alone at SF10, best of 5, took 1,708 ms against DuckDB's 480,
3.56x, at a load of 20.13 (23:07:09, much of it this run's own build).

**The cause.** Q18 keeps the orders whose lines hold over 300 in all, `o_orderkey IN (SELECT
l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)`, and then joins
`customer`, `orders` and `lineitem` and groups them to sum `l_quantity` again. The subquery has
already summed every order's lines, but `lineitem` was scanned and joined a second time. In an
`EXPLAIN ANALYZE` at SF10 on f47a89b (load 18.80, 23:07:28) the subquery's aggregate read all 59.99M
rows: 2.97 s of CPU partial, 1.97 s final, 15.00M groups, 624 kept. The semi join built on those 624,
partitioned, and the scans under it waited for them. Then the `orders` scan took 2.85 s of CPU, 2.24 s
of it the row filter, and the second `lineitem` scan 3.04 s, 2.59 s of it the row filter, both the
partitioned joins' `CASE` over each row's repartition hash, to give 624 and 4.37K rows. Q18 without
its outer query, the subquery alone under a `count(*)`, took 672 and 770 ms against Q18's 1,529 and
1,495, so half the time came after the aggregate (best of 5, loads 15.78 to 15.34, 23:26:44 to
23:27:26). DuckDB 2.0.1's `EXPLAIN ANALYZE` (load 10.97, 23:08:17) also scans `lineitem` twice: one
hash group-by of 2.16 s, then 190 ms for the second scan, which its filter prunes to 370 of 528 row
groups, and 550 ms for `orders`.

**The A/B.** Q18 written by hand with the groups joined in place of the second `lineitem`, `FROM
customer, orders, (SELECT l_orderkey, sum(l_quantity) AS q FROM lineitem GROUP BY l_orderkey HAVING
sum(l_quantity) > 300) a` and `sum(q)`, built only for this and not committed, gave the same answer
as DuckDB 2.0.1 but was refused under 2 GiB: the join built on all 15M orders and their customers
beside the 15M groups. A `HAVING` was guessed to keep a third of its input, `lineitem`'s 2.55 GB,
more than `orders` and `customer`'s 817 MB. With a `HAVING`'s groups guessed at a tenth of its input
too, the rewrite took 881, 912 and 886 ms against Q18's 1,439, 1,349 and 1,250, three rounds
alternating, best of 5, Burrmill alone, loads 9.99 to 11.41 (23:39:18 to 23:40:17). With the guess
alone, Q18 itself took 1,242, 1,154 and 1,270. Collecting Q18's three joins by hand instead, also
not committed, took 925 to 1,105 ms against 1,365 to 1,502 (three rounds, loads 14.96 to 11.93),
and skipping the partial aggregate did not help.

**The fix.** `HavingGroups` (`crates/burrmill/src/df/havinggroups.rs`) makes that rewrite. Where a
semi join on an aggregate's groups sits over an inner join to that aggregate's own input on its
group key, and every aggregate above is a `checked_sum` of a column of that input which the groups
sum too, the join reads the groups the `HAVING` keeps and each sum above sums the groups' sums. The
semi join goes, as the `HAVING` now filters the groups the join meets. `checked_sum` is exact, so
the sum of the sums is the sum of the rows, of the same type. A count or a sum of the other side's
columns, which counts each of its rows once per line, a sum of doubles, a `max`, an `IN` on another
column, NULL equal to NULL, a join with any other condition, and anything volatile or correlated are
left alone. `BuildOnSmaller` (`buildside.rs`) now guesses a `HAVING` to keep a tenth of its
aggregate's input as groups, before its predicate's third, so the join builds on the 624 groups and
is collected. Taking every grouped aggregate to keep a tenth changed Q2's and Q17's plans as well and
was not kept. The `EXPLAIN` of all 22 before and after differs in Q18 alone, at SF1 and at SF10.
`a_q18_shaped_join_reads_the_havings_groups_and_scans_once` (`crates/burrmill/tests/df_havinggroups.rs`)
fails with the rule off, its table scanned twice, and with the guess off, the join no longer built
on the groups; `a_semi_join_on_a_havings_groups_is_collected` fails with the guess off.

**Measured** 2026-11-26 on 2202748 against f47a89b, on the ThinkPad as above, 8 threads, 2 GiB, no
spill directory: three rounds per scale factor, before then after, each the best of 5 for Burrmill
and for the DuckDB 2.0.1 CLI in the same process. The load was 11.14 at 00:06:12; the wait for it to
fall under 8 ended at 00:06:56 (7.39), and it rose to 11.21 during the rounds. Every answer was the
same as DuckDB's.

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | before ms | ratio | DuckDB 2.0.1 ms | after ms | ratio | Burrmill peak RSS MiB, before / after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 6.80 / 9.37 | 384 | 1,398 | 3.64x | 377 | 884 | 2.34x | 2,065 / 2,201 |
| 10 | 2 | 9.37 / 10.29 | 403 | 1,567 | 3.89x | 428 | 1,121 | 2.62x | 2,024 / 2,014 |
| 10 | 3 | 10.29 / 11.21 | 406 | 1,643 | 4.05x | 390 | 884 | 2.27x | 2,075 / 2,115 |
| 1 | 1 | 11.21 / 11.21 | 41 | 124 | 3.03x | 43 | 71 | 1.65x | 650 / 669 |
| 1 | 2 | 11.21 / 11.19 | 40 | 112 | 2.79x | 42 | 61 | 1.46x | 670 / 675 |
| 1 | 3 | 11.19 / 11.19 | 40 | 109 | 2.71x | 43 | 63 | 1.47x | 662 / 653 |

Q18 at SF10 went from 3.64x–4.05x DuckDB 2.0.1 to 2.27x–2.62x, and at SF1 from 2.71x–3.03x to
1.46x–1.65x. Peak RSS is not lower: the 15M groups are held as before, and the pool peaked at 1,311
MiB after against 1,265 to 1,560 before. What is left at SF10 is the aggregate, 2.33 s of CPU partial
and 1.70 s final over the 59.99M rows, against DuckDB's 2.16 s for its group-by, and the `orders`
scan, 1.58 s of CPU, 1.09 s of it the row filter: the customer join's partitioned `CASE` comes first
in it and is evaluated on all 15M orders before the groups' filter (`EXPLAIN ANALYZE` after the fix,
load 9.86, 00:08:16).

**All 22 after the fix**, at SF10 on 2202748, best of 3, DuckDB 2.0.1 in the same process. The load
was 9.39 at the start (00:08:24) and 9.92 at the end (00:09:38). All 21 answers given were the same
as DuckDB's, and Q21 was refused as above. The widest ratios were Q1 at 3.23x (1,012 ms against 313),
Q5 at 3.15x, Q9 at 2.89x, Q4 at 2.88x and Q17 at 2.81x. Q18 was at 2.42x. At SF1, one run each at a
load of 9.92, all 22 answers were the same as DuckDB's.

## Q1: decimal arithmetic and sums without a check per row (#433)

Q1 was the widest ratio at SF10 in the run of all 22 after #464, 3.23x. On fb2da5a, with the DuckDB
2.0.1 CLI downloaded for this, Q1 alone at SF10, best of 5, took 1,065 ms against DuckDB's 302,
3.53x, at a load of 16.09 (06:26:34); in the rounds below, at loads of 6.93 to 9.62, it was 3.37x to
3.53x.

**The cause.** Q1 scans all of `lineitem`, keeps 59.14M of its 59.99M rows, and folds them into 4
groups with five checked sums, three checked averages and a count over decimal expressions. In an
`EXPLAIN ANALYZE` at SF10 on fb2da5a (load 14.30, 06:26:46) the projection of
`checked_mul(l_extendedprice, checked_sub(1, l_discount))` took 1.02 s of CPU, and the partial
aggregate 3.53 s: 1.01 s evaluating its arguments, `checked_mul(.., checked_add(1, l_tax))`, and
2.49 s aggregating. The scan took 2.14 s and the `FilterExec` 114.90 ms. Arrow's checked decimal
kernel makes a checked 128-bit multiply per row, a call into compiler-rt, and for `1 - l_discount`
it rescales the literal, spread to a column, with two more per row. On one core over 59M rows, a
checked `i128` multiply took 166 ms where one 64-bit multiply took 49 ms (a loop built only for
this). `checked_sum` and `checked_avg` read each row through `dyn Array::is_valid` and a closure,
seven times per row. DuckDB 2.0.1's `EXPLAIN ANALYZE` (load 10.16, 07:32:31) takes 90.0 and 110.0 ms
for its two projections, 1.21 s for its group-by and 1.28 s for its scan.

**The A/B.** The reader's row filter is not the cause. Q1 keeps it, since the scan reads two text
columns besides the predicate's. With `StaticRowFilters` made to drop it, built only for this and not
committed, Q1 took 905, 903 and 918 ms against 881, 889 and 1,042 with it, three rounds alternating,
best of 5, Burrmill alone, loads 8.03 to 8.36. The arithmetic and the sums as below, before the
skipped precision pass was added, took Q1 from 1,159, 1,124 and 1,145 ms to 747, 706 and 993, three
rounds alternating, Burrmill alone, loads 8.71 to 9.87.

**The fix.** `checked_add`, `checked_sub` and `checked_mul` over `Decimal128`
(`crates/burrmill/src/df/checked.rs`) now run in plain `i128` where the batch's magnitudes show no
row can overflow: under 2^127 for a product, each rescaled operand under 2^126 for a sum or a
difference, and one 64-bit multiply where both factors are under 2^63. The result has arrow's
precision and scale, and the pass checking it against its precision is skipped where the magnitudes
already bound it. A batch that could overflow, a result past its precision, a NULL or lone literal,
and a negative scale, which arrow's precision check refuses, go to arrow's kernel as before, so
what is refused is unchanged. `checked_sum` and `checked_avg` over integers and `Decimal128` add from
the value buffer, with NULLs and the filter folded into one mask. Tests in `checked.rs` compare
both paths with the old ones over operands up to 2^127, NULLs, literals and negative scales, and
`q1s_decimal_arithmetic_takes_the_plain_kernel` fails if Q1's four expressions go back to arrow's
kernel. After the fix, in an `EXPLAIN ANALYZE` at SF10 (load 11.27, 07:32:23), the projection took
332.57 ms of CPU, the aggregate's arguments 333.12 ms and its aggregating 1.49 s.

**Measured** 2026-11-26 on 18edef4 (built before rustfmt, which moved only layout) against fb2da5a,
on the ThinkPad as above, 8 threads, 2 GiB, no spill directory: three rounds per scale factor,
before then after, each the best of 5 for Burrmill and for the DuckDB 2.0.1 CLI in the same process.
The load was over 8 from 06:26 until 07:29 (up to 150 at 07:21); the wait for it ended at 07:29:40
(7.55), and it rose to 9.81 during the rounds. Every answer was the same as DuckDB's.

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | before ms | ratio | DuckDB 2.0.1 ms | after ms | ratio | Burrmill peak RSS MiB, before / after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 6.93 / 7.32 | 334 | 1,165 | 3.49x | 322 | 677 | 2.10x | 362 / 355 |
| 10 | 2 | 7.32 / 8.32 | 302 | 1,065 | 3.53x | 301 | 629 | 2.09x | 361 / 361 |
| 10 | 3 | 8.32 / 9.62 | 318 | 1,070 | 3.37x | 348 | 664 | 1.91x | 366 / 363 |
| 1 | 1 | 9.62 / 9.81 | 39 | 128 | 3.29x | 38 | 72 | 1.90x | 344 / 348 |
| 1 | 2 | 9.81 / 9.81 | 36 | 118 | 3.29x | 35 | 82 | 2.34x | 343 / 346 |
| 1 | 3 | 9.81 / 9.19 | 39 | 118 | 3.02x | 39 | 70 | 1.79x | 345 / 349 |

Q1 at SF10 went from 3.37x–3.53x DuckDB 2.0.1 to 1.91x–2.10x, and at SF1 from 3.02x–3.29x to
1.79x–2.34x. What is left at SF10 is the scan, 2.37 s of CPU after the fix against DuckDB's 1.28 s,
and the aggregate's 1.49 s, which counts the two text group keys' hash lookups as well as the
seven sums. Probed alone at SF10 (Burrmill, best of 3, load 12.05, 06:42:08), the scan of the four
decimal columns took 1.41 s of CPU, of the two text columns 843.46 ms and of `l_shipdate` alone
154.98 ms, and a `count(*)` grouped by the two text columns took 681.41 ms aggregating. DuckDB also
answers each average as its sum over its count, where Burrmill accumulates `sum(l_quantity)` and
`avg(l_quantity)`, and `l_extendedprice`'s two, apart.

**All 22 after the fix**, at SF10 on the same build, best of 3, DuckDB 2.0.1 in the same process.
The load was 8.53 at the start (07:30:45) and 12.03 at the end (07:32:05). All 21 answers given were
the same as DuckDB's, and Q21 was refused as above. The widest ratios were Q8 at 3.46x (1,193 ms
against 345), Q5 at 3.39x, Q2 at 3.27x, Q4 at 3.14x and Q22 at 3.00x. Q1 was at 1.77x. At SF1, one
run each at a load of 11.07, all 22 answers were the same as DuckDB's.

## Q8: the join on one type's parts collected (#433)

Q8 was the widest gap at SF10 in the run of all 22 for #465. On fd05542, with the DuckDB
2.0.1 CLI downloaded for this, Q8 alone at SF10, best of 5, took 2,700 ms against DuckDB's 656,
4.12x, at a load of 34.53 (08:34:42).

**The cause.** Q8 joins `lineitem` to the 13.45K parts of `p_type = 'ECONOMY ANODIZED STEEL'`,
then to the suppliers, to two years of orders and on to their customers and nations. Every join
built on the side that reads fewer bytes, as it should, but partitioned, so each pushed into the
`lineitem` scan a dynamic filter that is a `CASE` over each row's repartition hash: three of them,
for `l_partkey`, `l_suppkey` and `l_orderkey`. The part filter comes first and is evaluated on all
60M rows. In an `EXPLAIN ANALYZE` at SF10 on fd05542 (load 17.97, 09:07:36) the `lineitem` scan took
6.79 s of CPU, 4.29 s of it the row filter, to give 24.25K rows; the `orders` scan 1.13 s, 0.34 s
the row filter, and no other scan over 0.11 s; no join took over 36 ms. DuckDB 2.0.1's `EXPLAIN
ANALYZE` (load 20.60, 08:37:23) joins the same tables with `lineitem` as the probe and puts the same
three filters on its scan, which took 2.11 s, and its `orders` scan 0.59 s.

**The A/B.** Built only for this and not committed, an inner join was collected where its build
side was filtered and guessed to keep at most a given share of what its probe keeps. At a 64th, Q8
collects the part join and the one-row `region` join. Back to back, best of 5, Burrmill alone, Q8
took 969 and 965 ms partitioned against 702 and 737 collected (loads 15.89 to 14.29, 08:39:59 to
08:40:24). Collecting at an eighth also took the 910.4K orders, and Q8 was no faster (735 and 727
ms). Collecting unfiltered sides too, the 100K suppliers and the customers, took 1,119 ms against
946 for the eighth on filtered sides alone, one run each at a load of 30. At a 16th, Q3's customers of one segment and Q19's parts were
collected as well, and in the one round under a load of 13 Q19 took 709 ms against 562 and Q3 724
against 696.

**The fix.** `BuildOnSmaller` (`crates/burrmill/src/df/buildside.rs`) collects an inner join whose
build side is filtered and guessed to keep at most a 64th of what its probe side keeps, where the
bytes read already chose that side. The shares are its guesses as above. After it, in an `EXPLAIN
ANALYZE` at SF10 (load 18.30, 09:07:39), the `lineitem` scan took 3.17 s of CPU, 0.81 s of it the
row filter, and the pool peaked at 52 MiB before and after. The `EXPLAIN` of all 22 at SF1 and SF10
changes in Q2, 5, 7, 8, 9, 11, 17, 20 and 21, the same joins at both: the part joins of Q2, 8, 9
and 17, and joins on one nation or region in the rest. `a_join_on_a_few_filtered_rows_of_a_small_table_is_collected`
(`crates/burrmill/tests/df_buildside.rs`) fails with the rule off.

**Measured** 2026-11-26 on 4c4566c against fd05542, on the ThinkPad as above, 8 threads, 2 GiB, no
spill directory: three rounds per scale factor, before then after, each the best of 5 for Burrmill
and for the DuckDB 2.0.1 CLI in the same process. The load was 10.93 at 08:57:24; the wait for it to
fall under 8 ended at 08:57:57 (7.46), and it rose to 177 during the SF10 rounds, other work on the
host. Every answer was the same as DuckDB's.

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | before ms | ratio | DuckDB 2.0.1 ms | after ms | ratio | Burrmill peak RSS MiB, before / after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 7.37 / 141.67 | 400 | 1,099 | 2.75x | 476 | 740 | 1.55x | 316 / 473 |
| 10 | 2 | 141.67 / 103.69 | 649 | 1,059 | 1.63x | 377 | 750 | 1.99x | 448 / 461 |
| 10 | 3 | 103.69 / 50.50 | 434 | 1,013 | 2.33x | 355 | 717 | 2.02x | 430 / 473 |
| 1 | 1 | 43.58 / 40.33 | 40 | 114 | 2.84x | 49 | 82 | 1.68x | 389 / 382 |
| 1 | 2 | 40.33 / 33.33 | 40 | 123 | 3.08x | 44 | 78 | 1.77x | 382 / 389 |
| 1 | 3 | 33.33 / 28.57 | 48 | 123 | 2.55x | 48 | 78 | 1.62x | 387 / 386 |

Q8's own time at SF10 went from 1,013–1,099 ms to 717–750 ms, and at SF1 from 114–123 ms to 78–82
ms. DuckDB's times moved with the load, from 355 to 649 ms at SF10, so the ratios are wide: 1.63x to
2.75x before, 1.55x to 2.02x after. The same rounds ran the other plans that changed, Burrmill's
best of 5 at SF10, before / after: Q2 200–217 / 155–157 ms, Q9 1,680–1,855 / 1,416–1,569 and Q17
808–890 / 466–489, faster; Q5 924–965 / 880–971, Q7 685–802 / 746–984, Q11 100–125 / 116–148 and
Q20 453–461 / 465–509. Five more rounds of those four alone, alternating, best of 5, at loads of
9.96 to 24.53 (09:06:50 to 09:11:32), found no difference: Q5 873–1,653 / 862–1,708, Q7 689–1,097 /
687–1,423, Q11 110–300 / 114–230 and Q20 455–1,019 / 457–878: the after was faster in 11 of
the 20 pairs, and within 5% in 6 of the other 9. Q21 is refused at SF10 as before, and its SF1 answer is the same.
What is left of Q8 at SF10 is the `lineitem` scan, 3.17 s of CPU against DuckDB's 2.11 s, and the
`orders` scan's partitioned filter from the customer join.

**All 22 after the fix**, at SF10 on 4c4566c, best of 3, DuckDB 2.0.1 in the same process, at a
load of 15.93 at the start (09:07:48) and 12.92 at the end (09:09:01). All 21 answers given were the
same as DuckDB's, and Q21 was refused as above. Q8 was at 2.22x (765 ms against 345). The widest were
Q5 at 3.17x (965 ms against 304), Q4 at 2.94x, Q22 at 2.89x and Q7 at 2.85x.

## Q5: the region's nations reach `customer` (#433)

Q5 was the widest ratio at SF10 in the run of all 22 for #467, 3.17x. On e51530c, with the DuckDB
2.0.1 CLI downloaded for this, Q5 alone at SF10, best of 5, took 917 ms against DuckDB's 259, 3.54x,
at a load of 16.29 (10:43:48).

**The cause.** Q5 joins in the order written: `customer` to a year of `orders`, to `lineitem`, to
`supplier` on `l_suppkey = s_suppkey` and `c_nationkey = s_nationkey`, then to `nation` and to
`region`'s `ASIA`. The five nations reach `supplier` through the dynamic filter of the join on
`s_nationkey`, 20.04K of 100K suppliers, but never `customer`: the join that equates `c_nationkey`
with `s_nationkey` is on two columns, and its dynamic filter cannot be split between the scans of
its probe side. In an `EXPLAIN ANALYZE` at SF10 on e51530c (load 14.52, 10:44:02) the `customer` scan
gave all 1.50M rows, the `orders` scan 2.28M, and the `lineitem` scan 9.10M of 59.99M, at 4.68 s of
CPU, 2.84 s of it the row filter: the join to the orders being partitioned, that filter is a `CASE`
over each row's repartition hash. The join to `supplier` then kept 72.98K of the 9.10M. DuckDB 2.0.1
joins `nation` and `region` to `customer` first, through `c_nationkey = s_nationkey = n_nationkey`;
its `EXPLAIN ANALYZE` shows 456,771 orders and 1,825,856 `lineitem` rows.

**The A/B.** Q5 with `c_nationkey IN (SELECT n_nationkey FROM nation, region WHERE n_regionkey =
r_regionkey AND r_name = 'ASIA')` added, written by hand and not committed, gave the same answer as
DuckDB and took 732 and 721 ms against Q5's 911 and 888, two rounds alternating, best of 5, loads
5.95 to 9.25 (10:45:16 to 10:45:53); Q5 written with `nation, region` first and `c_nationkey =
n_nationkey` added took 749 and 780. Its `lineitem` scan gave 1.83M rows but still took 4.10 s of CPU,
2.75 s of it the row filter. With every inner join collected whose probe reads over 2 GB and build
under 1 GB, a build only for this, Q5 took 677 and 647 ms and the hand-written semi join 517 and 479,
against 941, 916, 740 and 752 without (Burrmill alone, best of 5, loads 10.45 to 9.95, 10:48:38 to
10:49:21).

**The fix.** Two rules. `TransitiveKeys` (`crates/burrmill/src/df/transitivekeys.rs`) puts the
columns a tree of inner joins equates in classes. A leaf filtered, or joined to a filtered leaf, is a
dimension: every row out of the tree holds one of its rows that met the filter, so another column of
it in a class takes only the values the dimension keeps. A leaf with a column in that class, not
joined to the dimension itself, gets a semi join on those values, built on the dimension, whose
dynamic filter then reaches its scan; a row it drops meets no row of the dimension, so the tree drops
it too. In Q5 that is `customer` on the five nations of `ASIA`. The dimension is read again, so it
must read at most a sixteenth of what the table does; a volatile filter, NULL equal to NULL and a
null-aware join are left alone. `BuildOnSmaller` (`buildside.rs`) now also collects an inner join
whose filtered build side is guessed at a quarter or less of an unfiltered probe side, so the probe's
scan, which evaluates the join's dynamic filter on every row, holds one hash lookup. Q5's year of
orders, with their customers, is guessed at a seventh of `lineitem`. On main with #467, which
collects a join on a build side guessed at a 64th of its probe, the `EXPLAIN` of all 22 before and
after differs in Q5 and Q16 alone, at SF1 and at SF10: Q5 takes both rules, and Q16 collects its
join of the parts it keeps to `partsupp`. In `crates/burrmill/tests/df_transitivekeys.rs`,
`a_table_equated_with_a_filtered_dimension_through_another_reads_its_keys` fails with the first rule
off, and `a_filtered_build_under_an_unfiltered_probe_is_collected` with the second. After the fix,
in an `EXPLAIN ANALYZE` at SF10 (load 10.33, 10:56:57), the `customer` scan gave 300.3K rows, the
`orders` scan 456.8K, and the `lineitem` scan 1.83M at 2.27 s of CPU, 0.77 s of it the row filter.

**Measured** 2026-11-26 on 75796f2, the merge of main at 81e675b (#467) into this branch, against
81e675b, on the ThinkPad as above, 8 threads, 2 GiB, no spill directory: three rounds per scale
factor, before then after, each the best of 5 for Burrmill and for the DuckDB 2.0.1 CLI in the same
process. The wait for the load to fall under 8 ended at 12:13:04 (7.96), and it rose to 9.43 during
the rounds. Every answer was the same as DuckDB's.

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | before ms | ratio | DuckDB 2.0.1 ms | after ms | ratio | Burrmill peak RSS MiB, before / after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 7.96 / 7.66 | 310 | 924 | 2.98x | 297 | 502 | 1.69x | 609 / 513 |
| 10 | 2 | 7.66 / 8.08 | 270 | 899 | 3.33x | 292 | 522 | 1.79x | 605 / 489 |
| 10 | 3 | 8.08 / 8.86 | 298 | 985 | 3.31x | 288 | 517 | 1.79x | 604 / 507 |
| 1 | 1 | 8.86 / 8.86 | 37 | 98 | 2.64x | 35 | 50 | 1.44x | 389 / 374 |
| 1 | 2 | 8.86 / 8.86 | 35 | 97 | 2.77x | 36 | 51 | 1.41x | 393 / 375 |
| 1 | 3 | 9.43 / 9.43 | 41 | 128 | 3.12x | 43 | 57 | 1.33x | 393 / 378 |

Q5 at SF10 went from 2.98x–3.33x DuckDB 2.0.1 to 1.69x–1.79x, and at SF1 from 2.64x–3.12x to
1.33x–1.44x. Before the merge, on 9d625c7 against e51530c in the same way at loads of 5.94 to 9.05
(11:07:12 to 11:07:53), it went from 875–905 ms to 486–515 ms at SF10 and from 90–100 ms to 46–49 ms
at SF1. What is left at SF10 is mostly the `lineitem` scan, 2.27 s of CPU for 1.83M rows of 59.99M,
and the `orders` scan, 0.91 s.

**Q16**, the other plan that changes, at SF10 on the same two builds, three rounds alternating, best
of 7, DuckDB 2.0.1 in the same process, loads 9.43 to 11.31 (12:13:50 to 12:14:05): 171, 180 and 175
ms before, 1.85x–2.06x DuckDB, and 128, 139 and 136 after, 1.30x–1.52x. Every answer was the same.
Before the merge, without #467, the collected join also changed one join each in Q8, 9, 11, 17 and
20 and two in Q2; in three rounds at loads of 8.35 to 30.55 (11:07:59 to 11:13:09), as Burrmill over
DuckDB in the same window, Q2 went from 2.77x–3.24x to 2.13x–2.47x, Q8 from 2.80x–3.48x to
1.91x–2.13x, Q9 from 2.81x–4.21x to 1.98x–3.09x, Q17 from 2.71x–3.00x to 1.45x–1.66x and Q20 from
1.76x–1.98x to 1.45x–1.83x, and Q11, again best of 7 at loads of 7.53 to 9.88, took 99 to 116 ms
before and 97 to 103 after. #467 makes the same changes to those joins.

**All 22 after the fix**, at SF10 on 75796f2, best of 3, DuckDB 2.0.1 in the same process, after a
run on 81e675b just before it. The load was 11.68 at the start of the run before (12:14:10), 17.57
between them and 20.70 at the end (12:17:30); at that load queries whose plans did not change, Q13,
Q14 and Q15, differed by a third or more between the two runs. All 21 answers given were the same as
DuckDB's, and Q21 was refused as above. The widest ratios were Q4 at 3.12x (490 ms against 157), Q22
at 2.77x, Q2 at 2.72x, Q3 at 2.67x and Q7 at 2.61x. Q5 was at 1.78x, against 2.78x in the run before.
At SF1, one run each at a load of 16.63 (12:17:47), all 22 answers were the same as DuckDB's.

## Q4 again: the join's filter left to the join (#433)

Q4 was the widest ratio at SF10 in the run of all 22 for #468, 3.12x. On ae35eff, with the DuckDB
2.0.1 CLI downloaded for this, Q4 alone at SF10, best of 5, took 406, 395 and 402 ms against
DuckDB's 125, 122 and 139, 2.89x to 3.25x, at loads of 4.34 to 4.19 (23:08:00 to 23:08:13).

**The cause.** The `lineitem` scan reads `l_orderkey`, `l_commitdate` and `l_receiptdate`, and its
predicate, `l_receiptdate > l_commitdate` and the collected semi join's filter on `l_orderkey`,
reads all three. A dynamic filter kept the scan's row filter in the reader whatever the scan read
(`StaticRowFilters`, `crates/burrmill/src/df/staticfilters.rs`), so the reader evaluated the
comparison over all 59.99M rows, then the join's filter over the 37.93M it kept, and gave 1.45M.
In an `EXPLAIN ANALYZE` at SF10 on ae35eff (load 8.59, 23:30:28) that scan took 2.54 s of CPU,
521 ms of it evaluating the predicate; the `orders` scan took 618 ms and the join 79 ms. The reader
can spare there only the decoding of the predicate's own columns for rows an earlier conjunct
dropped, and the join drops the same rows above it. DuckDB 2.0.1's `EXPLAIN ANALYZE`, run straight
after those (its load not recorded), puts `l_orderkey IN PRF(o_orderkey)` in its `lineitem` scan, 2,294,856 rows in 590 ms, then
50 ms of filter to 1,450,571, a 130 ms join and a 360 ms `orders` scan, in its profile.

**The A/B.** The same tree with the row filter off for every scan whose predicate holds a dynamic
filter and gives no other reason to keep it, a build only for this: Q4 took 243 and 247 ms against
341 and 343 with it on, two rounds alternating, best of 5, Burrmill alone, loads 8.39 to 9.04. Turning the row filter off as well for the `orders` scan, which reads the text
`o_orderpriority` besides its predicate's column, took 225 and 232: 18 ms more, not taken here.
Moving the join's filter ahead of the comparison in the reader, the row filter kept, took 343 and
339 ms against 409 and 405 (loads 4.01 to 4.25, 23:11).

**The fix.** A hash join's filter keeps a scan's row filter in the reader only where the scan reads
a column the predicate does not, whose decoding the filter can spare. A top-k's filter keeps it as
before, and so do an equality with a literal and an unread column that is not fixed-width. After
it, in an `EXPLAIN ANALYZE` at SF10 on 79e9a52 (load 8.59, 23:30:29), the `lineitem` scan gave all
59.99M rows in 0.87 s of CPU, the `FilterExec` above it took 152 ms to keep 37.93M, and the join
383 ms, 342 ms of it probing those 38.46M rows. Of the 21 plans other than Q21's, which was not
checked, only two scans change, at SF1 and at SF10: Q4's `lineitem` and Q22's `orders`, whose predicate is the `NOT
EXISTS` join's filter alone over `o_custkey`, the one column it reads. There the `orders` scan went
from 1.48 s of CPU, 1.20 s of it the predicate, to 0.20 s, and the join from 66 ms to 217 ms.
`a_join_filter_over_only_its_columns_filters_above_the_reader`
(`crates/burrmill/tests/df_staticfilters.rs`) fails if a Q4-shaped scan evaluates the join's filter
in the reader again.

**Measured** 2026-11-26 on 79e9a52 against ae35eff, 8 threads, 2 GiB, no spill directory: three
rounds per scale factor, before then after, each the best of 5 for Burrmill and for the DuckDB
2.0.1 CLI in the same process. Every answer was the same as DuckDB's. A first set of SF1 rounds ran
at loads of 8.27 to 8.73 and is not shown; the SF1 rounds below waited for the load to fall under
8 (7.90 at 23:34:16).

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | before ms | ratio | DuckDB 2.0.1 ms | after ms | ratio | Burrmill peak RSS MiB, before / after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 5.36 / 5.08 | 129 | 408 | 3.17x | 130 | 242 | 1.87x | 514 / 498 |
| 10 | 2 | 5.08 / 7.41 | 161 | 514 | 3.19x | 158 | 329 | 2.08x | 473 / 450 |
| 10 | 3 | 7.41 / 8.27 | 162 | 497 | 3.07x | 146 | 257 | 1.76x | 499 / 475 |
| 1 | 1 | 7.90 / 7.90 | 20 | 49 | 2.43x | 18 | 32 | 1.80x | 368 / 354 |
| 1 | 2 | 7.90 / 7.43 | 19 | 48 | 2.50x | 19 | 32 | 1.66x | 374 / 355 |
| 1 | 3 | 7.43 / 7.43 | 19 | 48 | 2.51x | 19 | 32 | 1.66x | 366 / 354 |

The same rounds timed Q22, the other plan that changes, best of 5:

| SF | round | DuckDB 2.0.1 ms, before / after | Q22 before / after ms | ratio, before / after |
|---|---:|---|---|---|
| 10 | 1 | 82 / 83 | 258 / 141 | 3.15x / 1.70x |
| 10 | 2 | 99 / 102 | 321 / 167 | 3.25x / 1.63x |
| 10 | 3 | 97 / 84 | 317 / 131 | 3.27x / 1.56x |
| 1 | 1 | 17 / 16 | 31 / 19 | 1.84x / 1.17x |
| 1 | 2 | 18 / 17 | 32 / 17 | 1.75x / 1.03x |
| 1 | 3 | 17 / 15 | 31 / 18 | 1.82x / 1.19x |

Q4 at SF10 went from 3.07x–3.19x DuckDB 2.0.1 to 1.76x–2.08x, and at SF1 from 2.43x–2.51x to
1.66x–1.80x. What is left of Q4 is the join's probe of 38.46M rows, the `orders` scan, 644 ms of
CPU against DuckDB's 360, and the `lineitem` scan, which DuckDB's PRF cuts to 2.29M rows.

**All 22 before and after**, at SF10, best of 3, DuckDB 2.0.1 in the same process, ae35eff then
79e9a52 straight after it. The load was 6.70 at the start (23:31:08), 11.49 between them and 12.34
at the end (23:33:38). All 21 answers given were the same as DuckDB's in both, and Q21 was refused
as above. Before, the widest ratios were Q22 at 3.54x, Q4 at 3.23x, Q7 at 2.97x and Q3 at 2.93x.
After, they were Q3 at 2.97x (771 ms against 260), Q7 at 2.92x, Q2 at 2.62x, Q18 at 2.46x and Q6 at
2.34x; Q4 was at 1.95x and Q22 at 1.76x. No other plan changed, so the other differences are the
load's.

## Q3: the filters of the joins it feeds left to the joins (#433)

Q3 was the widest ratio at SF10 in the run of all 22 for #469, 2.97x. On 0b7b533, with the DuckDB
2.0.1 CLI downloaded for this, Q3 alone at SF10, best of 5, took 623, 505 and 509 ms against
DuckDB's 199, 208 and 216, 2.36x to 3.13x, and Q7 589 to 598 ms, 2.21x to 2.33x, at loads of 3.47
to 7.80 (22:38:28 to 22:38:58). Q3 was still the wider.

**The cause.** Burrmill and DuckDB 2.0.1 run the same plan: `customer`'s `BUILDING` segment is the
build side of the join to `orders`, the orders before 1995-03-15 that join keeps are the build side
of the join to `lineitem`, and the 114K groups go to a top 10. In their `EXPLAIN ANALYZE`s the
scans give the same rows: 300.3K customers, 1.46M orders and 302.1K `lineitem` rows. The difference
is in the two large scans. In Burrmill's at SF10 on 0b7b533 (load 6.60, 22:39:07), both joins are
partitioned, so each scan holds its join's dynamic filter as a `CASE` over each row's repartition
hash, and `StaticRowFilters` kept it in the Parquet reader, since both read fixed-width columns
their predicates do not (`l_extendedprice` and `l_discount`, `o_orderkey` and `o_shippriority`).
The `lineitem` scan took 2.59 s of CPU, 1.28 s of it the row filter, and the `orders` scan 1.03 s,
0.43 s of it the row filter. In DuckDB 2.0.1's (load 3.14, 22:40:00) those scans took 960 ms and
520 ms. The filter's join is the operator straight above each scan, and it drops the same rows
there; the keys it keeps are scattered through every page, so the reader spares little of the other
columns' decoding.

**The A/B.** With a build only for this that turned the reader's row filter off by column name, Q3
took 413 and 405 ms against 555 and 524 with it on, two rounds alternating, best of 5, Burrmill
alone, loads 5.19 to 7.48 (22:50:15 to 22:50:44). Collecting both joins as well took 361 and 369,
and collecting them with the row filter on, 503 and 458; seven more rounds at loads of 9.92 to
13.28 did not separate collecting from not, so it is not taken here. In the `EXPLAIN ANALYZE` of
the row filter off (load 6.48, 22:50:55), the `lineitem` scan gave all 59.99M rows in 1.39 s of CPU,
the filter above it took 217 ms to keep 32.33M and the join 355 ms; the `orders` scan took 452 ms,
its filter 79 ms and its join 194 ms. Turning the reader's row filter off for every scan whose
predicate holds only joins' filters, with nothing else to keep it, changed scans in 13 of the 21
plans, and in three rounds alternating at loads of 10.62 to 30.77 (23:00:45 to 23:06:38) Q7 was
slower in each, 729–876 ms against 866–1,083. Its `orders` scan holds the filter of the join to the customers, which reaches
it through the join to `lineitem`: in the reader it cut that join's build side to 1.21M orders, and
left to its join, that join built on all 15.00M and the joins above carried 18.23M rows where they
had carried 117.0K (load 29.11, 23:06:47).

**The fix.** A join's dynamic filter keeps a scan's row filter in the reader only where it reaches
the scan through another join. Where the join probes the scan through filters, projections and
repartitions alone, the filter is left to the join, as the scan's own `WHERE` is where nothing else
keeps it in the reader: a top-k's filter, an equality with a literal, or an unread column that is
not fixed-width still does, and #469's scans of only their predicates' columns are unchanged. Of the
21 plans other than Q21's, eight change at SF1 and at SF10, all in where scans evaluate their
predicates: Q3's `lineitem` and `orders`, Q5's `nation`, `customer`, `orders` and `lineitem`, Q10's
and Q13's `orders`, Q11's and Q16's `partsupp`, and Q14's and Q17's `lineitem`. In
`crates/burrmill/tests/df_staticfilters.rs`,
`a_join_filter_from_the_join_it_feeds_filters_above_the_reader` fails on 0b7b533, and
`a_join_filter_through_another_join_still_filters_in_the_reader` fails with every join's filter
left to its join.

**Measured** 2026-11-27 on db35a72 against 0b7b533, on the ThinkPad as above, 8 threads, 2 GiB, no
spill directory: three rounds per scale factor, before then after, each the best of 5 for Burrmill
and for the DuckDB 2.0.1 CLI in the same process. The wait for the load to fall under 8 ended at
23:28:15 (7.97). Every answer was the same as DuckDB's.

| SF | round | 1-minute load, start / end | DuckDB 2.0.1 ms | before ms | ratio | DuckDB 2.0.1 ms | after ms | ratio | Burrmill peak RSS MiB, before / after |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|
| 10 | 1 | 7.42 / 7.97 | 209 | 629 | 3.01x | 213 | 387 | 1.82x | 474 / 526 |
| 10 | 2 | 7.97 / 8.21 | 222 | 593 | 2.67x | 218 | 397 | 1.82x | 490 / 533 |
| 10 | 3 | 8.21 / 8.28 | 233 | 606 | 2.60x | 235 | 391 | 1.66x | 487 / 534 |
| 1 | 1 | 8.28 / 7.77 | 32 | 68 | 2.12x | 32 | 47 | 1.47x | 367 / 416 |
| 1 | 2 | 7.77 / 7.77 | 32 | 67 | 2.09x | 33 | 50 | 1.52x | 365 / 421 |
| 1 | 3 | 7.77 / 7.77 | 33 | 67 | 2.04x | 34 | 46 | 1.37x | 369 / 419 |

Q3 at SF10 went from 2.60x–3.01x DuckDB 2.0.1 to 1.66x–1.82x, and at SF1 from 2.04x–2.12x to
1.37x–1.52x. Its peak RSS rose by about 50 MiB, the scans' batches now holding every row they read.

The same rounds timed the other plans that change, at SF10, best of 5, loads 7.68 to 13.98 (23:29:00
to 23:32:08); every answer was the same:

| Q | DuckDB 2.0.1 ms, before | before ms | ratio | DuckDB 2.0.1 ms, after | after ms | ratio |
|---|---|---|---|---|---|---|
| 5 | 263, 264, 251 | 441, 516, 447 | 1.68x–1.96x | 253, 275, 260 | 404, 405, 416 | 1.47x–1.60x |
| 10 | 299, 289, 298 | 749, 747, 731 | 2.45x–2.58x | 283, 296, 315 | 694, 691, 917 | 2.33x–2.91x |
| 11 | 75, 82, 77 | 141, 153, 152 | 1.86x–1.97x | 79, 70, 76 | 86, 76, 78 | 1.02x–1.09x |
| 13 | 523, 570, 527 | 784, 989, 1,028 | 1.50x–1.95x | 522, 556, 522 | 565, 586, 556 | 1.05x–1.08x |
| 14 | 247, 203, 199 | 417, 343, 367 | 1.69x–1.85x | 246, 195, 267 | 314, 352, 357 | 1.28x–1.80x |
| 16 | 101, 101, 94 | 140, 145, 133 | 1.39x–1.43x | 98, 95, 90 | 142, 132, 126 | 1.39x–1.45x |
| 17 | 317, 401, 390 | 495, 632, 593 | 1.52x–1.58x | 340, 377, 459 | 753, 582, 686 | 1.49x–2.22x |

Q10 and Q17 differ within what the load moves, one round of each 2.9x and 2.2x as it rose; four
more rounds of Burrmill alone, alternating, at loads of 5.46 to 11.22 (23:33:08 to 23:34:19), took
685, 911, 860 and 933 ms before and 656, 912, 915 and 945 after for Q10, and 453, 448, 493 and 501
before and 414, 473, 506 and 527 after for Q17. Q17's `lineitem` scan, whose only predicate is the
filter of a collected join to 2,040 parts, now gives all 59.99M rows and its join probes them:
1.28 s of CPU in the scan and 203 ms in the join, against 1.41 s in the scan before, and its peak
RSS rose from about 370 MiB to 440.

**All 22 before and after**, at SF10, best of 3, DuckDB 2.0.1 in the same process, 0b7b533 then
db35a72 straight after it. The load was 10.82 at the start (23:34:31), 15.68 between them and
14.12 at the end (23:37:20). All 21 answers given were the same as DuckDB's in both, and Q21 was
refused as above; at SF1, one run at loads of 13.47 to 12.56 (23:37:26 to 23:37:36), all 22 were
the same. Before, the widest ratios were Q18 at 3.29x, Q7 at 3.11x, Q10 at 2.66x and Q3 at 2.65x.
After, they were Q2 at 2.67x (332 ms against 124), Q10 at 2.66x, Q7 at 2.51x, Q19 at 2.41x and Q12
at 2.40x; Q3 was at 1.84x. Q2, Q7, Q12, Q18 and Q19 have the same plans in both, so their
differences are the load's.

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
