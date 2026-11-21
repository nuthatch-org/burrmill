# Time per rule on small statements: the probe, as run

The timers behind the per-rule section of `../../where-small-statements-go.md` (2026-11-21).
`rule-times.patch` applies to `c8df718`. With `BURRMILL_RULE_PROFILE` set, `cancel::Stoppable`
times every analyzer, optimizer and physical rule and records whether it changed the plan (an
equality check against a clone for the analyzer, `Transformed` for the optimizer, pointer identity
for physical rules); `dialect-latency` prints the sums at the end. The clone and node count sit
inside the analyzer's stage timer, so take its rule sums, not its stage total.

`mac-2026-11-21.txt` is `FRESH=1 WARMUP=1 REPEATS=9 burrmill-bench dialect-latency` on an M5 Pro,
8 threads, warmup rounds included in the sums.
