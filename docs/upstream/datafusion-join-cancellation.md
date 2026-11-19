# DataFusion: a join under an aggregate cannot be aborted until it runs out of input

**Status: drafted, not filed.** Written 2026-11-19 for #350 and the deadline clause of #344;
reproduced in plain DataFusion 55.2.0 with no Burrmill rule in the plan. Upstream this is
apache/datafusion#19358; its fix, #19360 (wrap join operators with `make_cooperative()`), went
stale and closed on 2026-05-08. Filing or commenting is Chief's call.

Burrmill does not wait on it. `df/cancel.rs`'s `Cancellable` rule puts the statement's token on
every join's output and inside every join filter, and `tests/df_host.rs`
`a_join_under_an_aggregate_stops_at_its_deadline` holds a cross, nested-loop and hash join (one with
a filter that rejects every pair) to 100 ms past their deadline. They stop 0.1-1.4 ms after it in
release and 0.3-3.2 ms in debug. With the rule switched off, the same test took 1.25 s for the
cross join. A nested-loop join over two `range(1000000)` took 1,201 s, and a hash join's tasks ran
11.7 s after the caller had its `Timeout`.

## What spins

Tokio aborts a task only when its future returns `Pending`. DataFusion 55.2.0 spends coop budget
only where `EnsureCooperative` put a `CooperativeExec` (leaves and exchanges): one unit per batch a
leaf yields. The loops that matter spend none.

- `AggregateStream` (`aggregates/aggregate_stream.rs:341`) is
  `loop { this.input.next().await ... }`. While its input is `Ready` it never returns.
- `CrossJoinStream::BuildBatches` (`joins/cross_join.rs:691`) emits one batch per build row for
  each probe batch. 60,000 build rows give 60,000 output batches for one unit of leaf budget, and
  the 128-unit budget runs out after about 7.7 million batches, which is never.
- `NestedLoopJoinStream`'s `ProbeRight` state (`joins/nested_loop_join.rs:1275`) and
  `HashJoinStream::ProcessProbeBatch` (`joins/hash_join/stream.rs:619`) loop inside one poll
  while the join filter rejects pairs. They emit nothing and touch no leaf until the probe batch is
  used up.

So the aggregate's task polls only the join, the join reaches a leaf once in thousands of
iterations, and the task does not yield before the input runs out. With `target_partitions = 1`
it is all one task. With more partitions, each partition's task does the same behind a
`RepartitionExec` or `CoalescePartitionsExec`, and the consumer is aborted while the producers keep
running.

## Reproduction

DataFusion 55.2.0 (`default-features = false, features = ["sql"]`), tokio 1 multi-thread,
release build:

```rust
use datafusion::prelude::*;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.sql("CREATE TABLE t AS SELECT value AS v, 'x' AS k FROM generate_series(1, 60000)")
        .await?
        .collect()
        .await?;
    for sql in [
        "SELECT sum(a.v * b.v) FROM t a, t b",
        "SELECT count(*) FROM t a JOIN t b ON (a.v * 3 + b.v) % 1000003 = 7",
        "SELECT sum(a.v + b.v) FROM t a JOIN t b ON a.k = b.k",
        "SELECT count(*) FROM t a JOIN t b ON a.k = b.k AND a.v + b.v < 0",
    ] {
        let df = ctx.sql(sql).await?;
        let task = tokio::spawn(df.collect());
        tokio::time::sleep(Duration::from_millis(300)).await;
        task.abort();
        let asked = Instant::now();
        let how = match task.await {
            Err(e) if e.is_cancelled() => "cancelled",
            _ => "ran to the end",
        };
        println!("{:>10.1?} to stop ({how}): {sql}", asked.elapsed());
    }
    Ok(())
}
```

Time from `abort()` to the task ending, on an M-series Mac:

| Join | As shipped | Join outputs wrapped in `CooperativeExec` (#19360's shape) |
|---|---|---|
| Cross, under `sum` | 2.3 s, ran to the end | 0.4 ms, cancelled |
| Nested loop, filter rejects nearly all | 5.4 s, ran to the end | 3.2 s, ran to the end |
| Hash, every key equal, under `sum` | 26.2 s, ran to the end | 0.08 ms, cancelled |
| Hash, filter rejects all | 18.2 s, ran to the end | 21.5 s, ran to the end |

The right-hand column wraps every `*JoinExec` in `CooperativeExec` with a physical optimizer rule
appended to the default ones. That fixes the joins that emit often and does nothing for the ones
that emit nothing. Their loop never returns a batch for the wrapper to count. At
`target_partitions = 8` all four cancelled within 0.5 ms on this fixture, which is why the
original report looked timing-dependent.

## The change to propose

Spend budget in the loops themselves, as pepijnve suggested on #19360 (branch `coop_joins`). Each
`continue` of a long-running state machine first calls a non-async `consume_budget`:

```rust
/// Consumes a unit of the task's coop budget; `Pending` once it is spent.
pub fn consume_budget(cx: &mut Context<'_>) -> Poll<()> {
    tokio::task::coop::poll_proceed(cx).map(|r| r.made_progress())
}
```

and the smallest set of loops to call it from is:

1. `AggregateStream`'s, `GroupedHashAggregateStream`'s and the other draining loops, once per
   input batch. That covers every join that emits, and any other fast producer, from the consumer
   side.
2. `NestedLoopJoinStream`'s `ProbeRight` and `HashJoinStream`'s `ProcessProbeBatch`, once per
   chunk processed. That covers the joins whose filter rejects everything, which item 1 cannot
   reach.

A unit is a counter decrement until 128 are spent, so the cost is one yield per 128 iterations.
This change has not been built and run here. Only the wrapper column above was measured.
Wrapping join outputs (#19360) is item 1 done from the producer side. It is easier to land, but by
the table above it does not cover item 2.

Burrmill's equivalent costs nothing measurable. With `Cancellable` on and off, the four joins
above (20,000 × 5,000 rows, nine runs each, release) had medians within 2% of each other in both
directions: 313 against 309 ms, 153 against 151, 780 against 770, 568 against 581.
