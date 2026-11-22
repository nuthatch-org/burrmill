# What Burrmill's own costs are worth on TPC-H at SF10 (#384)

Measured 2026-11-22 on the ThinkPad (32 cores), `origin/main` at 5ae8262 (after #399), TPC-H SF10 as
tpchgen-cli 4.0.0 wrote it (16 parts), **8 threads**, best of 3 warm runs, 2 GiB budget, answers
compared with DuckDB's by `burrmill-bench tpch` before any time is printed.

## Method

The four candidates were switched off one at a time by a **temporary** env, `BURRMILL_PROBE_OFF`
(comma list), read in the engine and not committed (16 lines: an early return in
`CheckedArithmetic::rewrite` for `+ - *` and unary minus, one in `DuckSemantics::analyze`, a skip of
the `checked_sum` / `checked_avg` rewrite in `CheckedArithmetic::aggregate`, and skipping
`with_batch_size(2048)` so DataFusion's 8,192 applies):

| Column | Off |
|---|---|
| on | nothing (current main) |
| arith | checked `+ - *` and negation: plain DataFusion operators, which wrap on overflow |
| sums | `checked_sum` / `checked_avg`: DataFusion's `sum` / `avg` |
| duck | the `DuckSemantics` analyzer rule (DuckDB's `/`, `%`, timestamp, rounding, date rewrites) |
| batch | the 2,048-row batch under a budget (8,192 instead) |
| all | the four together |

```
DIR=~/burrmill-orchestra/data/tpch/sf10 THREADS=8 RUNS=3 [DFCLI=$(which datafusion-cli)] \
  BURRMILL_PROBE_OFF=<list> target/release/burrmill-bench tpch
```

**Load was not controlled.** Another worker was building on the box. The 1-minute load average at the
start of each run ranged from 0.6 to 38 (round 1: on 0.6, arith 26.8, sums 14.5, duck 10.6, batch
9.7, all 10.7; round 2, run in the reverse order: all 9.0, batch 9.8, duck 8.5, sums 10.7, arith
12.6, on 37.9). DuckDB's own summed time over the 22 queries is a gauge: 17.7 to 26.0 s across the
twelve runs, the slowest being round 2's "on" (26.0 s). Each cell below is the **minimum over the
two rounds**. `*` marks a difference from "on" of more than 10% and 20 ms, which is what I count as
signal; with this load, many of those stars are still the box, not the switch. datafusion-cli was
run once, with round 1's "on".

## Answers and refusals

- **sums off changes answers**: Q1 and Q12 differ from DuckDB's (same row counts, different values), so those
  sums buy DuckDB's exact decimal results. Not droppable.
- arith, duck, batch off: all 21 answers the same, Q21 refused for the same budget reason as "on".
  On TPC-H alone that proves nothing about arith and duck: they exist for overflow errors (arith:
  wrapping would answer where DuckDB errors) and for the cases DuckDB's rewrites cover, which TPC-H
  does not exercise.

## Per-query ms (SF10)

| Q | DuckDB | datafusion-cli | on | arith off | sums off | duck off | batch off | all off |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 747 | 651 | 1329 | 1195 * | 1046 * (differs) | 1318 | 1244 | 953 * (differs) |
| 2 | 175 | 166 | 264 | 277 | 315 * | 320 * | 233 * | 285 |
| 3 | 591 | 428 | 791 | 731 | 731 | 927 * | 653 * | 658 * |
| 4 | 373 | 181 | 654 | 652 | 652 | 779 * | 565 * | 572 * |
| 5 | 640 | 635 | 955 | 1132 * | 1128 * | 1230 * | 1039 | 1061 * |
| 6 | 321 | 276 | 570 | 496 * | 494 * | 594 | 505 * | 497 * |
| 7 | 625 | 960 | 613 | 875 * | 1074 * | 900 * | 809 * | 803 * |
| 8 | 1048 | 896 | 1300 | 1249 | 1534 * | 1256 | 1147 * | 1146 * |
| 9 | 1894 | 1299 | 2226 | 2130 | 2337 | 2109 | 1896 * | 2059 |
| 10 | 947 | 649 | 1146 | 1122 | 997 * | 1042 | 857 * | 829 * |
| 11 | 165 | 155 | 246 | 320 * | 302 * | 297 * | 240 | 218 * |
| 12 | 540 | 437 | 893 | 1156 * | 847 (differs) | 1078 * | 834 | 836 (differs) |
| 13 | 1293 | 475 | 996 | 1169 * | 991 | 1038 | 853 * | 895 * |
| 14 | 676 | 347 | 405 | 522 * | 411 | 446 * | 370 | 357 * |
| 15 | 521 | 1121 | 462 | 529 * | 448 | 474 | 449 | 423 |
| 16 | 317 | 137 | 222 | 214 | 188 * | 200 | 158 * | 162 * |
| 17 | 1040 | 2430 | 2221 | 1970 * | 1751 * | 2002 | 1551 * | 1412 * |
| 18 | 1769 | 2374 | 2377 | 1603 * | 1679 * | 1883 * | 1487 * | 1314 * |
| 19 | 1131 | 904 | 916 | 656 * | 706 * | 707 * | 562 * | 569 * |
| 20 | 1025 | 909 | 881 | 686 * | 674 * | 629 * | 567 * | 553 * |
| 21 | 2857 | 2180 | refused | refused | refused | refused | refused | refused |
| 22 | 342 | 150 | 450 | 378 * | 374 * | 321 * | 369 * | 282 * |

(DuckDB column is its best of the two "on" runs.)

## What it says

- No switch is a clean, large, answer-preserving win that the load does not explain. The ones that
  look biggest (Q17 to Q20, all-off 35-45% under "on") are also where "on" ran at load 27-38,
  and DuckDB's own times there moved by a similar margin. Batch-off is the one that keeps every answer and
  is faster on most starred queries; duck-off and arith-off have as many starred queries slower than
  "on" (Q3-Q5, Q7, Q11-Q13) as faster, which is what noise looks like.
- The 1.1x to 2x tax over datafusion-cli is therefore not explained by one of these four on this
  evidence: "all off" still trails datafusion-cli on Q1, Q3-Q6 and Q10-Q12. A quiet-box rerun, and
  profiling those queries (`EXPLAIN ANALYZE`), is what is left.
- None of the four costs something that is only needed under `--strict` or `--deterministic`: sums
  changes answers, arith and duck guard DuckDB's errors and rewrites in default mode, and the batch
  size is the #1650 memory bound under a budget. So no default was changed.
