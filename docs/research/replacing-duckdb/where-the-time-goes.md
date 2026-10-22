# Where a view's time goes, and what a JIT could touch

2026-10-22. A perf profile of the nine heaviest allocations views through `Engine`, split by
pipeline stage, taken to answer an outside brief on a Cranelift expression JIT. Thinkpad, 32 cores,
`gan-portable` (1,925 segments), burrmill 4b4c8e8 with two additions (`REPEATS` and
`BUDGET_MB` on `engine-analyze`). Transcript: `docs/bench/profile-views-thinkpad-2026-10-22.txt`.

## The question

The brief (an external research note, not kept here) asked whether a Cranelift JIT over fused
filter → cast → fold loops would make Burrmill faster, and answered itself with "measure first: if
filter plus aggregate is under about 30% of wall time, Amdahl caps the JIT at 1.4x and specialised
kernels take most of that". RFC-0044 §4.3 and §9 already hold the JIT as a non-goal on the Umbra
numbers (Cranelift 20-35% faster to compile than LLVM, 16x slower than a single-pass emitter, code
about LLVM -O0). This note is the measurement the brief asked for.

## Method

`perf record -F 499 -g` over `burrmill-bench engine-analyze <nest> <view>`, frame pointers on,
`REPEATS=3` warm runs after the cold one so the process is the query rather than opening the nest.
Self-time per symbol, bucketed by crate and module (parquet-rs and the compression codecs are
"decode"; `fastcast`/`checked` are "cast"; `arrow_ord`, the sort streams and `arrow_row` are
"sort"; libc's `memmove`/`memcmp` and glibc's allocator are "memcpy/alloc"). The thinkpad's CPU
is hybrid, so perf reports the P-cores and E-cores as two tables; each is weighted by its cycle
count. DataFusion's `EXPLAIN ANALYZE` was collected beside every run and agrees on the shape.

## The split, unbudgeted (`open_nest`: 8 threads, 8,192-row batches)

Self-time, percent of samples. Warm medians in the last column.

| view | memcpy/alloc | sort/merge | decode | cast | agg/hash | window | take | LOWER | expr | join | ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| lodestar_indexer_ledger | 21.7 | 17.7 | 12.6 | 5.9 | 5.8 | 5.5 | 4.5 | 3.0 | 1.3 | 1.1 | 610 |
| lodestar_indexer_deployment_daily | 28.2 | 11.1 | 10.8 | 8.2 | 7.9 | 3.4 | 5.4 | 2.8 | 1.3 | 1.7 | 891 |
| lodestar_delegator_stakes | 23.3 | 13.7 | 13.0 | 7.0 | 9.0 | 3.6 | 4.3 | 3.2 | 2.0 | 1.2 | 853 |
| lodestar_delegators | 24.7 | 4.2 | 13.3 | 9.9 | 14.8 | - | 5.0 | 3.0 | 3.6 | 1.0 | 223 |
| lodestar_allocations | 28.8 | 0.6 | 14.9 | 5.6 | 11.1 | 0.3 | 6.3 | 0.0 | 3.1 | 2.7 | 188 |
| lodestar_delegations | 18.1 | - | 29.0 | 10.8 | 0.5 | - | - | 8.6 | 4.0 | - | 44 |
| lodestar_network | 32.0 | 7.0 | 8.9 | 6.6 | 12.9 | 2.4 | 3.6 | 1.5 | 1.1 | 1.0 | 1305 |
| lodestar_indexer_daily | 28.3 | 10.4 | 10.2 | 8.7 | 8.3 | 3.2 | 5.4 | 2.7 | 1.2 | 1.7 | 906 |
| lodestar_indexers | 22.3 | 13.3 | 13.7 | 7.8 | 7.7 | 4.1 | 3.9 | 3.3 | 1.4 | 1.7 | 849 |

The remainder is kernel (page faults, 5-19%) and "other", which is per-batch bookkeeping:
`PrimitiveArray::slice`, `DataType` clone and compare, drop glue, `clock_gettime` from the
metrics.

**What a JIT could touch** is cast + expr + LOWER + the arithmetic half of agg/hash: 12-20% of
self-time on every view. Amdahl at a generous 2x on that stage is 1.06-1.11x end to end. The
brief's own threshold was 30%; no view reaches it. Decode alone is as large as the whole
expression stage, and sort plus data movement is twice it. The JIT stays in §9.

The `fastcast` text-to-i128 parse, which the brief expected to dominate, is 4-6% on the big
views: the cast was most of a scan's time on 2026-09-16 (fastcast's module comment) and is not now.
Parse-at-seal would remove it, but it is the fourth lever, not the first.

## Where memcpy comes from

`__memmove_avx_unaligned_erms` is 12-14% of self-time on its own, `memcmp` 3-4%, glibc's
allocator 5-8%. The callers perf can attribute (frame-pointer unwinding loses the leaf's immediate
caller, so about half is unattributed):

- `CoalesceBatchesExec` copying string views: `InProgressByteViewArray::copy_rows`, 2.5-3.2%.
- `take` on fixed-size binary and `GenericByteViewArray::gc_with_max_buffer_size` after a take,
  1-2% each. Every operator boundary re-materialises the 42-character address strings.
- The aggregate's `ByteViewGroupValueBuilder` appending group keys, ~1%.
- `memcmp` from `equal_rows_arr` in the hash join and from the byte-view comparator in sorts.

So the dominant cost is moving and comparing address and id strings between operators, not
arithmetic on values. That is a layout question (addresses as 20-byte fixed binary or a
dictionary, not Utf8View) and a batching question, which is the next section.

## The batch size a host actually runs

Under a `Budget` the session reads 128 rows per batch (session.rs, 2026-10-02, for nuthatch #1650:
a wide-cell projection materialises a whole 8,192-row batch in Arrow before the byte cap sees a
row). nuthatch always opens budgeted, so production never runs the 8,192-row path measured above.
The same views, 2 GB budget, no spill, warm medians (ms):

| view | 2 thr, 128 | 2 thr, 2,048 | 2 thr, 8,192 | 8 thr, 128 | 8 thr, 2,048 | 8 thr, 8,192 |
|---|---|---|---|---|---|---|
| lodestar_indexer_ledger | 2,765 | 1,419 | 1,431 | 1,344 | 693 | 749 |
| lodestar_indexer_deployment_daily | 5,049 | 2,759 | 2,646 | 2,455 | 1,143 | 1,254 |
| lodestar_delegator_stakes | refused* | 2,174 | 1,987 | 2,105 | 950 | 978 |
| lodestar_network | 6,605 | 3,239 | 3,176 | 2,652 | 1,750 | 1,694 |
| lodestar_indexers | 3,415 | 1,882 | 2,016 | 1,840 | 942 | 953 |
| lodestar_indexer_daily | 5,160 | 2,716 | 2,809 | 2,377 | 1,363 | 1,231 |

\* `Memory Exhausted while SpillPool` at 2 GB with 128-row batches; the same view at 8,192 rows
completed inside the budget. Small batches cost pool headroom as well as time; not chased here.

**128-row batches cost 1.7-2.1x on every view, at both thread counts, and 2,048 rows recovers
all of it**: every 2,048 figure is within noise of its 8,192 neighbour, and `delegator_stakes`,
which refused at 128, completes. The ledger pushes 32
million operator-rows either way, in 7,258 batches at 8,192 rows and 264,557 at 128. Under the
128-row profile the allocator alone is 17% of self-time (`_int_malloc` 5.9, `cfree` 4.1,
`malloc` 3.0, `_int_free_chunk` 2.4, `malloc_consolidate` 1.9) and `memmove` falls to 4%: the
per-batch fixed cost has replaced the per-row one.

At 512 MB and 2 threads, which is nuthatch's default, four of the six views refuse for memory
without spill (the known shortfall, roadmap and RFC-0047 amendment); the two that run, the ledger
and `lodestar_indexers`, take 2.7 s and 3.4 s against 0.6 s and 0.85 s unbudgeted. On the landed
tree (2,048 rows, the gathered runs of the next section) they take 1.43 s and 1.95 s, and the 2 GB
matrix reproduces within noise (transcript, "Final tree").

For comparison the budgeted 8-thread, 8,192-row runs are 15-25% slower than the unbudgeted ones
on the same views (749 vs 610 ms on the ledger). The pool's accounting and the sort spill
reservation are the only differences; not chased here either.

## The split on the production shape (2 threads, 2 GB, 2,048 rows, landed tree)

Self-time, percent of samples, the same buckets as above.

| view | memcpy/alloc | decode | other | sort/merge | cast | agg/hash | window | kernel | take | LOWER | join | expr |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| lodestar_indexer_ledger | 22.9 | 16.7 | 13.1 | 13.0 | 6.5 | 5.6 | 5.9 | 3.1 | 3.1 | 2.4 | 1.2 | 1.2 |
| lodestar_indexer_deployment_daily | 24.1 | 15.7 | 11.6 | 10.8 | 8.6 | 6.5 | 4.7 | 4.0 | 3.4 | 2.2 | 1.5 | 1.1 |
| lodestar_delegator_stakes | 23.3 | 15.5 | 11.8 | 11.0 | 7.9 | 7.4 | 4.6 | 3.0 | 3.5 | 2.8 | 1.2 | 2.1 |
| lodestar_network | 27.7 | 13.4 | 12.8 | 6.4 | 6.4 | 10.9 | 3.1 | 6.2 | 2.3 | 1.4 | 1.1 | 1.2 |
| lodestar_indexers | 22.2 | 16.7 | 13.0 | 10.2 | 7.9 | 6.7 | 4.8 | 3.6 | 2.8 | 2.9 | 2.0 | 1.4 |
| lodestar_indexer_daily | 24.3 | 16.2 | 10.9 | 9.9 | 9.2 | 7.0 | 4.4 | 3.8 | 3.5 | 2.4 | 1.6 | 1.2 |

Reading the ledger's callers at this shape, by what perf can attribute:

- **Reading segments is 10% before any decoding.** `object_store`'s local `get_ranges` allocates
  a zero-filled `Vec` per range (`__memset`, 3.1%) and `pread` copies the page cache into it
  (`_copy_to_iter`, 6.8%). Snappy is 3.7%. The zero fill is avoidable outright; the copy only by
  mapping the segment, which the RSS gate would see as resident pages.
- **The allocator is 12%** (`malloc` 3.1, `_int_malloc` 2.9, `cfree` 2.7, `malloc_consolidate`
  1.8, `_int_free_chunk` 1.3, `_int_realloc` 0.7), and it is glibc's: the bench had no global
  allocator, while nuthatch runs jemalloc. The bench now takes jemalloc on Linux. Re-profiled on
  main after #113 (transcript, "Allocator and gather controls"): jemalloc is 10.7% of samples
  across 67 symbols against glibc's 12.3%, and 3-6% of wall time (ledger 919 against 959 ms,
  network 1,853 against 1,953). So the cost is the number of allocations, not the allocator:
  per-batch `Arc<dyn Array>`, `Vec<u8>` growth and `ScalarValue` churn, which a batch of 2,048
  rows pays 16x less often than 128 and still pays.
- **Main itself moved.** The matrix above was taken on 4b4c8e8 with the batch size patched;
  the same ledger on c22b182, fourteen PRs later, is 919 ms at 2 GB and 2 threads against
  1,419, and `lodestar_network` 1,853 against 3,239. The shared-definition pruning (#97, #99,
  #102) and the one-pass running windows (#107) are in that. At 512 MB and 2 threads without a
  spill directory the ledger and `lodestar_indexers` now refuse in the sort where 4b4c8e8
  answered, with either gather size, so that is the pool being charged honestly for what a shared subquery holds (#99), not
  the gathered runs; production's allocations nest runs at 2 GB with spill.
- **Planning is 2% of a 1.4 s statement** (`core::fmt::write` 0.9 and `stacker::remaining_stack`
  1.0 on the `burrmill-plan` thread, inside `SqlToRel`): about 30 ms per statement to plan the
  ledger's union, paid on every statement. A plan cache keyed by statement text would remove it.
- **Byte-view work is 10%** across 22 symbols (sort comparators on `GenericStringType`, view
  builders, `is_ascii`, gc after take) against 10% on primitive sort and take. So address
  columns as fixed binary, the second lever above, is bounded by about 10% at this shape, not the
  40-50% the unbudgeted buckets suggested: the rest of memcpy/alloc is the allocator and the
  reads.
- The text-to-i128 cast is 6.5-9.2%, the largest single symbol on every view.

## What follows

1. **Batch size under a budget: landed at 2,048** (session.rs, same day). The 128 came from
   #1650's Arrow-side transient, whose JSON side nuthatch already handles by slicing
   (`ENCODE_ROWS = 32`). A byte bound is not available where the transient arises: a projection
   materialises its whole input batch before anything downstream can split it, and DataFusion's
   scan is sized in rows. So the row count is the lever, and #1650 itself costs DuckDB's 2,048-row
   vector as fine (`repeat('A', 100000)` is ~200 MB in Arrow at 2,048, ~13 MB at 128, ~819 MB at
   8,192). Measured above; the probe ran the same line patched, not a separate path.

   One coupling came out in the tests. A sort that spills merges one run per batch it holds, and
   each run's cursor holds an output batch, so the spill-time merge needs the input over the
   run-to-batch ratio: 64 at 8,192-row runs and 128-row batches, 4 at 2,048. A 3,000,000-row sort
   under a 32 MB budget (`a_sort_too_large_for_the_pool_spills`) refused at 2,048 with the merge
   holding 15.4 MB beside the sorter's 12.6. The sort's `GatherExec` now gathers runs of 65,536
   rows or 8 MiB (ratio 32; half of either still passes), and the suite is green. Spilling is
   unchanged in kind, it merges a few hundred runs as before, only each is eight times larger.
2. **Address columns as fixed binary.** Sort, join, group-by and every copy between operators are
   on 42-byte strings that are 20 bytes of information. A `FixedSizeBinary(20)` view of the
   address columns, applied at scan, would shrink the byte-view share, which is about 10% at the
   production shape. The views apply `lower()` (already folded to the column), equality, `count`,
   six literal comparisons and fourteen concatenations to those columns, so the surface is small.
   Touches the catalogue, not the segment format. Behind the reads and the planner now.
2a. **Segment reads without the zero fill**, and a plan cache per statement text: 3% and 2% on
   the ledger, each a day, measured above.
3. **Decode.** Snappy at 3-4% plus buffer slicing. A different codec at seal is nuthatch's call
   and a format change; parked.
4. **Parse-at-seal for uint256.** Removes the 4-9% cast. Format change; parked behind 2.
5. **JIT.** Stays in §9. The evidence the brief asked for says the stage it would compile is
   12-20% of the time.
