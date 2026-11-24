# TPC-H at SF1 and SF10, losses first (#346)

Measured 2026-11-17 on the ThinkPad (32 cores, Linux 6.12), every engine at **8 threads**, best of
three warm runs, over Parquet that tpchgen-cli 4.0.0 wrote in 16 parts per table. DuckDB is the
bench's bundled 1.5.5, reading the same files through `read_parquet`; datafusion-cli is 55.2.0, the
DataFusion Burrmill pins, built with LTO. Burrmill holds every statement to a 2 GiB budget. Each
Burrmill answer is compared with DuckDB's (`burrmill-bench tpch`, floats to 1e-12) before its time
is printed. Ratios are Burrmill over DuckDB; below 1 is faster.

## What it shows

**Burrmill is slower than both on most of TPC-H.** At SF10 it beats DuckDB on six queries (13, 14,
15, 16, 19, 20) and datafusion-cli on two (7, 15), and loses the rest. It is the same DataFusion
underneath, so the gap to datafusion-cli is Burrmill's own. Where it goes is not measured yet; the
candidates are the checked arithmetic, the exact sums and averages, DuckDB's semantics rewrites and
the 2,048-row batches a budgeted session runs at.

**Three losses are not overhead but defects, and are open:**

- **Q17, 25.7 s at SF10 against DuckDB's 0.8 s.** At SF1 it is 0.3 s, so ten times the rows cost
  about 85 times the time. The plan averages `l_quantity` per `l_partkey` over all of lineitem
  before the join narrows it to 204 parts, as all three engines do; the exact `checked_avg` costs
  2.4x datafusion-cli at SF1, but that does not explain the growth.
- **Q2, 1.9 s at SF10 against 0.16 s,** also growing faster than the data: 65 ms at SF1.

  Since measured: both were a constant group key from decorrelation that made the aggregate
  ordered, and are fixed (#381; the progress log has the cause and figures). The tables below are
  from before.

  **The cause, named (#381).** Decorrelating the subquery grouped lineitem by `l_partkey` and a
  constant `true AS __always_true`, which DataFusion counts as sorted, so both aggregates ran in
  its ordered mode. That mode emits its last groups 2,048 at a time and renumbers every group still
  held at each emit: quadratic in the 2M parts, 179 of the partial aggregate's 190 CPU seconds at
  SF10. `LiteralGroupKeys` (`df/literalkeys.rs`, #399) takes the constant out of the key. It was
  not `checked_avg`, a hash table's growth or the pool. Re-measured on 3283a14 on 2026-11-24, at
  8 threads and best of 3:

  - **Q17 at SF10 is within 1.5x datafusion-cli.** Five rounds ran with all three engines: Burrmill
    took 1,432 to 1,585 ms against datafusion-cli's 1,217 to 1,528 ms, 0.98x to 1.30x. The 1-minute
    load was 6.5 to 11.2. SF1 took 161 ms against 137 ms, so ten times the rows now cost 8.9 to 9.8
    times the time. Every answer was the same as DuckDB's.
  - **Its `EXPLAIN ANALYZE` at SF10 has no ordered operator,** and no operator spilled. The
    statement pool peaked at 139 MiB of 1,792. Both aggregates group by `l_partkey` alone. The
    partial one skips aggregation for 59.18M of its 59.97M rows, as datafusion-cli's does
    (59.13M).
  - **Most of the time is the final aggregate's 2M groups:** 4.81 CPU seconds over 60M rows at SF10
    against 51 ms over 1.55M at SF1. That is 80 ns a row against 33 as the groups outgrow the
    caches. datafusion-cli's plain `avg` takes 5.66 s at the same step, so that growth is
    DataFusion's and not the exact average's.
  - **What `checked_avg` still costs** is in the partial aggregate's skipped rows: 423 ms CPU
    against datafusion-cli's 96 ms; the difference is about 40 ms of wall time at 8 threads.
  - **The semi join builds differently.** It builds on the 2M averaged parts (113.8 MB), where
    datafusion-cli builds on the 61K joined rows (4.9 MB). The session does not collect statistics
    to swap it. This is not measured apart from the rest.

  The SF10 spot-check of [Spilling](#spilling-q21-under-2-gib-383), run again on 3283a14 with a
  spill directory, still gives DuckDB's answer for all ten queries. The load was 6.8 at the start
  and 11.2 at the end; DuckDB / Burrmill / datafusion-cli in ms: Q1 720 / 1,162 / 759, Q3 664 / 754 / 480,
  Q5 746 / 981 / 699, Q9 1,533 / 1,719 / 1,163, Q10 722 / 757 / 579, Q12 411 / 718 / 363, Q13
  954 / 806 / 415, Q17 715 / 1,543 / 1,246, Q18 983 / 1,318 / 1,594, Q21 1,847 / 2,427 / 1,497.
- **Q21 is refused at SF10:** it needs more than the 1,792 MB of the 2 GiB budget a statement may
  use, which DuckDB and datafusion-cli answer without one. The refusal is the budget working; that
  Q21 needs it is the loss.

  Since measured: with a spill directory it answers at SF10, SF30 and SF100 under 2 GiB, as DuckDB
  does; without one it is refused as above (#383, [Spilling](#spilling-q21-under-2-gib-383)).

**Every answer given agrees with DuckDB:** 22 of 22 at SF1, 21 of 22 at SF10 with Q21 refused.

**Q6 skips no row groups here.** tpchgen does not cluster `l_shipdate`, so every row group's range
spans the predicate; Q6 reads 84 MB of lineitem at SF1, more than an unfiltered scan of its two
columns, because it reads its filter columns too. Pruning works where the data is clustered: a
filter on `l_orderkey < 1000` reads 0.25 MB of the 64 MB.

A single large Parquet file is split by byte range across the scan's partitions, as DataFusion
splits it, so the 16 parts are not what makes the scans parallel.

## SF1

| Q | DuckDB ms | Burrmill ms | ratio | datafusion-cli ms | answer |
|---|---:|---:|---:|---:|---|
| 1 | 93 | 118 | 1.27x | 75 | same |
| 2 | 33 | 65 | 1.95x | 26 | same |
| 3 | 61 | 80 | 1.32x | 53 | same |
| 4 | 42 | 68 | 1.61x | 29 | same |
| 5 | 66 | 103 | 1.57x | 69 | same |
| 6 | 32 | 56 | 1.74x | 34 | same |
| 7 | 90 | 113 | 1.25x | 83 | same |
| 8 | 87 | 137 | 1.57x | 65 | same |
| 9 | 146 | 164 | 1.12x | 93 | same |
| 10 | 79 | 98 | 1.24x | 67 | same |
| 11 | 23 | 45 | 2.00x | 16 | same |
| 12 | 45 | 88 | 1.97x | 51 | same |
| 13 | 91 | 69 | 0.76x | 37 | same |
| 14 | 55 | 49 | 0.89x | 39 | same |
| 15 | 36 | 48 | 1.33x | 54 | same |
| 16 | 46 | 39 | 0.85x | 19 | same |
| 17 | 64 | 304 | 4.77x | 128 | same |
| 18 | 124 | 124 | 1.00x | 119 | same |
| 19 | 76 | 72 | 0.94x | 59 | same |
| 20 | 92 | 104 | 1.13x | 57 | same |
| 21 | 210 | 230 | 1.09x | 140 | same |
| 22 | 29 | 36 | 1.24x | 17 | same |

## SF10

| Q | DuckDB ms | Burrmill ms | ratio | datafusion-cli ms | answer |
|---|---:|---:|---:|---:|---|
| 1 | 683 | 906 | 1.33x | 583 | same |
| 2 | 155 | 1903 | 12.30x | 160 | same |
| 3 | 507 | 590 | 1.16x | 410 | same |
| 4 | 319 | 506 | 1.59x | 193 | same |
| 5 | 597 | 1046 | 1.75x | 732 | same |
| 6 | 420 | 609 | 1.45x | 305 | same |
| 7 | 822 | 977 | 1.19x | 1025 | same |
| 8 | 1007 | 1358 | 1.35x | 811 | same |
| 9 | 1846 | 2106 | 1.14x | 1201 | same |
| 10 | 911 | 973 | 1.07x | 655 | same |
| 11 | 164 | 274 | 1.67x | 156 | same |
| 12 | 537 | 971 | 1.81x | 440 | same |
| 13 | 1343 | 964 | 0.72x | 439 | same |
| 14 | 676 | 477 | 0.71x | 357 | same |
| 15 | 518 | 461 | 0.89x | 635 | same |
| 16 | 262 | 189 | 0.72x | 105 | same |
| 17 | 831 | 25731 | 30.97x | 1131 | same |
| 18 | 1323 | 1725 | 1.30x | 1640 | same |
| 19 | 799 | 703 | 0.88x | 535 | same |
| 20 | 754 | 665 | 0.88x | 526 | same |
| 21 | 2221 | refused | | 1629 | limit exceeded: Out of Memory Error: the statement needed more than the engine's memory budget of 1792.0 MB; a spill directory would let ope |
| 22 | 324 | 334 | 1.03x | 106 | same |

## Spilling: Q21 under 2 GiB (#383)

Q21's `EXISTS` and `NOT EXISTS` are each a partitioned hash join that builds on all of `lineitem`.
A hash join holds its build side in memory and cannot spill it, so at SF10 the statement was
refused at 1,791 of its 1,792 MB, the refusal naming `HashJoinInput`, with a spill directory or
without. Under a budget with a spill directory the engine now plans a hash join whose build side
may not fit as a sort-merge join, whose sorts and key buffer spill
(`crates/burrmill/src/df/spillingjoins.rs` says when).

Measured 2026-11-24 on the ThinkPad, **a shared host under other work**: the 1-minute load is given
beside every timing, and DuckDB ran in the same few minutes, so the ratio is fairer than the
absolute times. Burrmill and DuckDB ran in separate processes so that neither's allocator counts
against the other's peak RSS; a third run of both compared the answers. Budget 2 GiB, 8 threads,
spill directory on the same disk. Best of 3 at SF10 and SF30; at SF100 too, each run being under 5
minutes. "Most spilled" is the most the spill directory held at once; "spilled in all" is every
operator's `spilled_bytes` from `EXPLAIN ANALYZE`, summed. The pool is what the engine's
operators reserved; the rest of the RSS is outside it. The two spill figures are taken differently,
one by sampling the directory and one from the operators' metrics, and at SF10 the first is the
larger; why is not measured.

| SF | DuckDB ms | DuckDB peak RSS MiB | Burrmill ms | ratio | Burrmill peak RSS MiB | pool peak MiB | most spilled MiB | spilled in all MiB | load at start | answer |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 10 | 1,767 | 953 | 2,388 | 1.35x | 2,396 | 1,652 | 709 | 575 | 4.9 / 6.9 | same |
| 30 | 5,505 | 1,941 | 13,955 | 2.54x | 2,278 | 1,676 | 6,496 | 12,238 | 8.4 / 10.1 | same |
| 100 | 20,503 | 5,483 | 96,993 | 4.73x | 2,974 | 1,678 | 21,659 | 42,910 | 3.5 / 3.6 | same |

The two loads are at the start of Burrmill's runs and of DuckDB's. The SF100 row is from the
build before Q18's exception below, which does not change Q21's plan (its build sides group
nothing); SF10 and SF30 are from the final build.

**Peak RSS is the budget plus up to 926 MiB.** The pool stays under its 1,792 MB at every scale
factor; what is above it is the process before the query (about 225 MiB: the binary, the tables'
footers), Parquet decoding outside the pool, and pages the allocator freed and kept. At SF100 a
build with the system allocator in place of jemalloc (`--features alloc-stats`, 1 run, 284 s
under its counting) peaked at 2,533 MiB, so jemalloc's retention is about 440 MiB of it. A host
that budgets 2 GiB should allow 3 GiB resident for such a statement.

**Without a spill directory nothing changes:** Q21 at SF10 is refused with the same error, the pool
at 1,791 MiB, and plans are as before.

**Other queries with a spill directory at SF10.** Q18 was the one regression the rule first
caused: as a sort-merge join it was refused under 2 GiB. Its `IN` builds on an aggregate's groups,
624 of 15.00 M orders, which the files do not measure, so a build side under an aggregate keeps its
hash join. Every answer below with a spill directory at 2 GiB matched DuckDB's. Best of 3; the
three runs were back to back, at loads 5.9, 12.5 and 13.9 at their starts and climbing, so the
column without a spill directory, last, ran on the busiest box. Peak RSS at 2 GiB counts DuckDB too,
which ran in that process; at 16 GiB Q21 keeps its hash joins, which fit.

| Q | DuckDB ms | spill dir, 2 GiB ms | spill dir, 16 GiB ms | no spill dir, 2 GiB ms | spilled MiB at 2 GiB |
|---|---:|---:|---:|---:|---:|
| 1 | 778 | 1,081 | 958 | 1,411 | 0 |
| 3 | 536 | 569 | 554 | 1,730 | 0 |
| 5 | 624 | 915 | 906 | 2,272 | 0 |
| 9 | 1,482 | 1,962 | 1,990 | 4,120 | 0 |
| 10 | 865 | 1,014 | 1,000 | 1,464 | 0 |
| 12 | 476 | 935 | 1,045 | 1,502 | 0 |
| 13 | 1,191 | 1,026 | 979 | 1,001 | 0 |
| 18 | 1,484 | 1,696 | 1,785 | 1,727 | 0 |
| 21 | 2,097 | 2,961 | 2,662 | refused | 636 |

With a spill directory and memory to spare (16 GiB) no query is slower than without one by more
than Q18's 58 ms (1,785 against 1,727), and the load moved more than that between the columns.

## Running it

    tpchgen-cli parquet -s 10 --parts 16 --output-dir sf10
    DIR=sf10 THREADS=8 RUNS=3 DFCLI=$(which datafusion-cli) cargo run --release -p burrmill-bench -- tpch
    DIR=sf10 THREADS=8 RUNS=3 QUERIES=21 ENGINE=burrmill SPILL=spill ANALYZE=1 cargo run --release -p burrmill-bench -- tpch

The queries are DataFusion's own (`crates/burrmill-bench/tpch/`), with Q15's view written as a `WITH`
for all three engines, because Burrmill serves queries only.
