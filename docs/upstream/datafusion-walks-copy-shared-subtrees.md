# DataFusion: a plan walk copies every shared subtree, and type coercion re-derives every schema

**Status: drafted, not filed.** Found 2026-11-19 on #347. Not a wrong answer, a cost. Filing
upstream is Chief's call. DataFusion 55.2.0.

## What happens

`LogicalPlanBuilder::scan` inlines a view as a shallow clone of the view's root, so every
reference to a view holds the same `Arc`s below that root. A statement over the network nest's
views (`corpus/deep-views/network-capacity.jsonl`) is a tree of 6,458 nodes and 1,080 distinct
ones.

Every `TreeNode` walk over a `LogicalPlan` gives that up at the first node it passes.
`TreeNodeContainer for Arc<C>` maps a child as `f(Arc::unwrap_or_clone(self))?.update_data(Arc::new)`:
a shared child is cloned and rewritten once per reference, and comes back as a new `Arc` whether
anything in it changed or not. From the first analyzer rule on, the plan is a tree, and each rule
does its work once per copy.

`TypeCoercion` adds to it: `analyze_internal` merges every input's schema, saves the name of every
expression (formatting it) and calls `recompute_schema` at every node, changed or not. That was
the largest single rule here, 13% of planning, two thirds of it `Projection::try_new` formatting
column names.

## Repro

```rust
let x = Arc::new(
    LogicalPlanBuilder::empty(true)
        .project(vec![lit(1).alias("a")])?
        .filter(col("a").gt(lit(0)))?
        .build()?,
);
let side = |n| LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::clone(&x), n).unwrap());
let plan = LogicalPlanBuilder::from(side("l")).cross_join(side("r"))?.build()?;
let mut seen = 0;
let out = plan.transform_up(|p| { seen += 1; Ok(Transformed::no(p)) })?.data;
// seen == 9, not 6, and the two inputs of the join no longer share an `Arc`.
```

## What Burrmill does instead

`crates/burrmill/src/df/dag.rs`: a bottom-up and a top-down walk that keep a subtree nothing
changed as the `Arc` it was, and do a shared one once, by its address. The analyzer rules before
`ShareRepeats` walk with it, and `type_coercion` is DataFusion's own per-node work given to it
(`coercion.rs`). The network statement: 1,555 distinct nodes through those rules where there were
6,440, and the statement answers in 410 ms where it took 596 (release, back to back;
`docs/bench/deep-views-thinkpad-2026-11-19.txt`).
The optimizer's rules still copy, after `ShareRepeats` has made the plan a tree anyway.

## To file, it needs

A `map_children` that keeps an untransformed child's `Arc`, which is the cheap half and would
help every walk; and either a memo in the walks or a statement that a shared subtree is the
caller's to deduplicate. The second is a design question for the maintainers, not a bug report.
