# DataFusion: a sort key with a function over a function does not satisfy itself

**Status: drafted, not filed.** Found 2026-11-19 by the determinism oracle (seed 6120). Not a
wrong answer, a refused statement. Filing upstream is Chief's call. DataFusion 55.2.0.

## What happens

`ScalarUDFImpl::evaluate_bounds` defaults to an unbounded range of type NULL. Sixteen of the
default functions fail on such a range in their sort properties or bounds: `abs`, `acos`, `acosh`,
`asin`, `atan`, `atanh`, `cos`, `cosh`, `exp`, `ln`, `log2`, `log10`, `radians`, `sin`, `sqrt`
and `tanh`. `abs` asks for `Interval::make_zero(&DataType::Null)`.
`EquivalenceProperties::get_expr_properties` turns any error in the walk into
`ExprProperties::new_unknown()`, which drops the exact match against the plan's orderings as well. So data sorted by `abs(abs(x))` is not known to be sorted by
`abs(abs(x))`.

## Repro

```rust
let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::UInt64, false)]));
let config = Arc::new(ConfigOptions::default());
let abs = datafusion_functions::math::abs();
let call = |a: Arc<dyn PhysicalExpr>| -> Arc<dyn PhysicalExpr> {
    Arc::new(ScalarFunctionExpr::try_new(Arc::clone(&abs), vec![a], &schema, Arc::clone(&config)).unwrap())
};
let order = LexOrdering::new([PhysicalSortExpr::new(call(call(col("x", &schema)?)), SortOptions::default())]).unwrap();
let mut eq = EquivalenceProperties::new(Arc::clone(&schema));
eq.add_ordering(order.clone());
assert!(!eq.ordering_satisfy(order)?); // abs(x) alone: true
```

## Where it bit

`EnforceSorting` parallelises a top-k as a `SortExec` per partition under a
`SortPreservingMergeExec` without asking whether the order holds; `SanityCheckPlan` then asks,
gets no, and refuses the plan. At one partition there is no merge, so the statement answered at
one thread and was refused at two.

## What Burrmill does instead

`crates/burrmill/src/df/ordering.rs` wraps each default function whose sort properties or bounds
fail on an unknown argument, and reports such a failure as no order known and an unbounded range.
The key is then matched against the orderings as any other. The fix upstream is either a typed
default for `evaluate_bounds` (the return type's unbounded range) or `get_expr_properties` keeping
the exact match when a child fails.
