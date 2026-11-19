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
- **Q21 is refused at SF10:** it needs more than the 1,792 MB of the 2 GiB budget a statement may
  use, which DuckDB and datafusion-cli answer without one. The refusal is the budget working; that
  Q21 needs it is the loss.

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

## Running it

    tpchgen-cli parquet -s 10 --parts 16 --output-dir sf10
    DIR=sf10 THREADS=8 RUNS=3 DFCLI=$(which datafusion-cli) cargo run --release -p burrmill-bench -- tpch

The queries are DataFusion's own (`crates/burrmill-bench/tpch/`), with Q15's view written as a `WITH`
for all three engines, because Burrmill serves queries only.
