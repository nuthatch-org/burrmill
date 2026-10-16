# RFC-0046: Keep what sealed history has already answered

- Status: Draft, 2026-10-16. Nothing in it is started. Answers burrmill#100.
- Depends on: burrmill#97 (a shared definition cut to what its readers take) and #99 (a repeated
  window computed once), both of which change what a view's plan looks like before anything here
  touches it; nuthatch's segment manifest (`segments/manifest.json`: per segment its content hash,
  block range and row count).

## §0 What this RFC is for

Burrmill keeps nothing from one statement to the next. Every view a nest serves is a fold over all
of its history, recomputed on every request, although almost all of that history sits in sealed
segments that will never change again. The only reuse today is nuthatch's memo (nuthatch#1186), which keys
a whole answer on the head and so goes cold at every cursor commit: on graph-allocations-nest-next
between 3 and 13 October it answered 34 to 47% of statements, and the rest, about 570,000
statements in ten days on a 4-core box, folded six million rows to tell a dashboard what had changed
in the last few thousand.

This is the one advantage DuckDB has no way to take, because it never knew its files were
immutable. This RFC says which plan shapes can hold partial state over sealed segments and fold only
what is new, what that state costs, what keeps it exact, and what it would buy on the production
nest. The measurement that matters most is in §6, and it changes the order of the work: the
simple slice buys a quarter, not twentyfold, on the view it was meant for.

## §1 The opportunity, measured

Every figure below is `burrmill-bench engine-sql` on the ThinkPad, warm medians of five, burrmill
017f0e5, pinned to four cores as the Helsinki box has. The "full" column is the production nest as
it stood when it was stood down (4,818 segments, 6,012,007 rows); the "1 h" column is a copy holding
only the segments sealed in its last hour of blocks (22 segments, 8,312 rows, 0.14%), which is what
a fold of only the new would have to read. Both columns ran back to back on a box that was also
building, so the full figures are higher than quiet ones (`lodestar_indexer_daily` is 2,256 ms
quiet); the ratio is what to read.

| view | full | 1 h | ratio |
| --- | ---: | ---: | ---: |
| `lodestar_indexer_deployment_daily` | 5,009 ms | 134 ms | 37x |
| `lodestar_indexer_daily` | 3,395 ms | 154 ms | 22x |
| `lodestar_deployments` | 1,301 ms | 77 ms | 17x |
| `lodestar_epochs` | 1,431 ms | 88 ms | 16x |
| `lodestar_delegator_stakes` | 2,145 ms | 151 ms | 14x |
| `lodestar_indexers` | 2,098 ms | 211 ms | 10x |
| `lodestar_network` | 3,894 ms | 969 ms | 4x |

The 1 h column includes each statement's fixed costs (planning, opening the files, the parts of
`lodestar_network` that do not read the new segments), so it is roughly what an incremental answer
would cost if every branch of every view could fold the new rows alone. §3 is about which branches
can.

## §2 What can hold partial state

A segment is named by the hash of its content and is never rewritten; the manifest says which block
range it covers. Two properties follow, and each slice rests on one of them.

**Slice 1: grouped aggregates over per-row branches.** An `Aggregate` whose input is a `UNION ALL`
of branches, each branch a scan of one table followed only by projections, filters and scalar
expressions: no join, no window, no subquery reading another table, nothing volatile. Its aggregate
functions must merge exactly: the checked `SUM` and `AVG`, `COUNT`, `MIN`, `MAX`, `BOOL_AND/OR`, and
`first_value`/`last_value` with an `ORDER BY` (the state is the ordering key and the value). For such
a branch the partial state over a set of segments is the merge of the partial state over each, in
any order. Held: per branch, one merged state over every sealed segment it has seen, and the set of
segment hashes behind it. A statement folds the segments not in that set, merges, folds the tip, and
evaluates.

A view need not qualify whole. Branches merge into one aggregate, so the qualifying ones are held and
the rest are computed as today and merged in; the gain is the share of the cost in branches that
qualify (§6).

An aggregate over an aggregate whose keys are a subset of the inner one's and whose functions are
`SUM` and `COUNT` (`lodestar_indexer_daily` over `lodestar_indexer_deployment_daily`) rolls up into
one aggregate at the coarser keys, so the state is held at 20,224 groups, not 849,421.

**Slice 2: running windows ordered by block.** A window `PARTITION BY p ORDER BY k ROWS UNBOUNDED
PRECEDING` with `k` derived from the block can be carried forward. In the allocations nest fourteen
of the running windows are ordered by `k = block_number * 100000 + log_index` and three by epoch,
which only grows with the block; the descending ones are latest-row picks, which `TopPerGroup`
already makes ordered aggregates and slice 1 covers. A carried window holds each partition's
accumulator and last key, and a new row's value is the accumulator folded with it. That holds only while every
new segment's `from_block` is past the held `to_block` of every table the window reads, which the
manifest answers without opening a file; a nest still backfilling out of order fails it and
recomputes. The as-of joins built on these windows (the newest cut at or before a reward's key,
`legacy_reward_share`'s pattern) carry the same way. The window's rows then feed a slice 1 aggregate
directly.

**Not in either slice.** A branch joined to a table that also grows, where a new row on the joined
side can change an old row's output (`legacy_rewards` joins every reward to `legacy_deployment` and to
`lodestar_indexer_ledger`); `COUNT(DISTINCT)` and other aggregates whose state is a set; top-k and
`LIMIT` over history; windows ordered by anything but the block.

## §3 Coverage on the allocations nest

Read off the view text, not measured branch by branch except where §6 says so.

| view | qualifies in slice 1 | needs slice 2 | out |
| --- | --- | --- | --- |
| `lodestar_indexer_deployment_daily`, `_daily` | indexing rewards collected, rebates, allocation collected | `horizon_fees` (running pool shares, as-of cuts) | `legacy_rewards` |
| `lodestar_network` | `staked`, `delegation`, `curator_positions`, the scalar sums | | `allocated` through `lodestar_allocations`, `lodestar_indexers` |
| `lodestar_deployments` | `signal`, `fees` | | `allocs` and `curators` (joins inside `lodestar_allocations`) |
| `lodestar_indexers`' sums over `lodestar_indexer_ledger` | the ledger's fifteen direct branches | `legacy_reward_share`, `legacy_path_share` | |

## §4 What the state costs

The checked sum's partial state is a three-word integer and a count for a `DECIMAL(38,0)` input, 32
bytes, and 24 for an integer (`df/checked.rs`, `width_for`). `lodestar_indexer_daily` holds nine
decimal sums and two counts per group, 336 bytes, plus about 60 of keys, so roughly 600 bytes a group
with the hash table's overhead: 12 MB for 20,224 groups. Held at
`lodestar_indexer_deployment_daily`'s keys it would be about 600 MB, which is why the rollup in §2
matters. These are arithmetic on the layout, not measurements.

The state outlives statements, so it sits outside the statement pool as the footer cache does
(#98), with its own share of the bound, reported as a gauge, and evicted by the milliseconds of
recompute a byte of it saves: `lodestar_indexer_daily` saves about 2 s per 12 MB, and a state worth
less than it occupies should not be held at all.

## §5 What keeps it exact

The checked sums merge their partial states without rounding or narrowing (`merge_batch` folds the
wide integer), and the refusal is decided when the final value is evaluated, so an answer from held
partials is the answer and the refusal a single pass would give (roadmap 2.1b). A state is keyed by
the branch's plan fingerprint and the hashes of the segments it covers; a removed or replaced
segment, an edited view or a changed `nuthatch.toml` drops it, and the next statement recomputes
that branch. Hot tip rows are never held: they are folded on every statement, as now. Determinism is
what makes this admissible, the same argument the memo rests on: the state is a function of its
inputs, so keeping it is keeping a fact.

## §6 What a second `SELECT * FROM lodestar_indexer_daily` costs

With every branch incremental: about the 1 h figure, **154 ms against 3,395 ms** under the same
load (§1), so 150 to 200 ms once the merge and the 20,224-row answer are added. That is the target,
and it needs both slices and the as-of joins.

With slice 1 alone it is much less. A scratch view with the same CTEs and only the two branches slice
1 cannot hold (`legacy_rewards` and `horizon_fees`, rolled up to indexer and day as the real view is)
costs **2,218 ms of the view's 2,891 ms**, same run, same nest, answers identical on both engines. So
slice 1 alone takes `lodestar_indexer_daily` from about 2.9 s to about 2.2 s: a quarter. The cost
lives in the branches that read the running pool shares and the ledger, which is slice 2 and the
growing-join problem, not in the plain sums.

That reorders the work. Slice 1 is still the place to start, because it is where the state, the
keying, the budget share and the exactness argument get built and tested on the simplest shape,
and `lodestar_network`'s and `lodestar_deployments`' qualifying CTEs are a larger share of their
cost. But the gate for calling this worth it is slice 2 on `lodestar_indexer_daily`.

## §7 Slices and gates

1. **Slice 1** behind a flag, held state for qualifying branches only. Gate: every view of the
   allocations nest answers identically with the flag on and off across ten successive seals
   replayed from the manifest, and `lodestar_network` costs less after a seal than before by at
   least the share its qualifying CTEs take today.
2. **The rollup** of aggregates over aggregates at key subsets. Gate: `lodestar_indexer_daily`'s
   held state is under 20 MB.
3. **Slice 2**, carried windows and as-of joins with the manifest's ordering check. Gate: a second
   `SELECT * FROM lodestar_indexer_daily` after one seal under 300 ms on four cores, answers
   identical, and a deliberately out-of-order segment falls back to recomputing.
4. **The growing join** (`legacy_rewards`) is left until slice 2 says whether it is worth a design.

## §8 Open questions for Chief

- Is a share of the bound for held state acceptable on nests already near their figure (the QoS
  nest at 2 GB), or should held state be opt-in per nest?
- Does nuthatch ever rewrite a sealed segment in place of others (compaction), rather than only add?
  If it does, slice 1 recomputes after every such rewrite, and that should be measured before the
  gate is believed.

## §9 How the numbers were taken

On the ThinkPad, from the copy of the production nest in `~/scratch/alloc-prod`, with
`burrmill-bench` built at 017f0e5 (`cargo build --release -p burrmill-bench`):

```
# the 1 h slice: every segment whose to_block is within 14,400 blocks (an hour of Arbitrum) of
# the newest; nuthatch.toml, schema.json, semantic.toml, abis, checks and views linked beside it
python3 -c '
import json, shutil
src = "alloc-prod/segments"
m = json.load(open(src + "/manifest.json"))["tables"]
top = max(s["to_block"] for l in m.values() for s in l)
for l in m.values():
    for s in l:
        if s["to_block"] > top - 14400:
            shutil.copy(src + "/" + s["file"], "alloc-slice/segments/")
'
taskset -c 28-31 burrmill-bench engine-sql ~/scratch/alloc-prod  <statements>
taskset -c 28-31 burrmill-bench engine-sql ~/scratch/alloc-slice <statements>

# the branches slice 1 cannot hold: views/98-diag-impure.sql in a copy of the nest, the
# deployment-daily view with `events` cut to legacy_rewards and horizon_fees, grouped by 1, 2
taskset -c 28-31 burrmill-bench engine-sql ~/scratch/alloc-diag impure.sql indexer_daily.sql
```

The slice's segments are copied, not linked: `discover_tables` sizes a symlinked segment as its link
and reads the footer from the wrong place (filed separately).
