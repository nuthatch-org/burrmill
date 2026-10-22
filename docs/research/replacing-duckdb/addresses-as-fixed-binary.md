# Addresses as fixed binary

2026-10-22. The second lever of `where-the-time-goes.md`, scoped before it is built. An address
column is 42 bytes of text holding 20 bytes of information, and every sort, join, group-by and copy
between operators on the ledger family runs on that text. Byte-view work is about 10% of self-time
at the production shape, and the comparators, group-key builders and `gc` after `take` are most of
it.

## The mechanism

- **Above the scan, not in it.** The table keeps its text schema: DataFusion types every
  projection while it parses the SQL, before any analyzer runs, and a string function over a
  fixed-binary column refuses right there. So the `Addresses` rule, first in the analyzer after
  the outer order, puts `address_bytes` over each scan of a table whose host declared the column
  `address` (the session notes those at registration, sealed and unsealed alike), under the
  table's own name again so a reference the planner qualified still resolves, with plain column
  names so a filter pushed down by name finds the decoded column. Every node above is told to
  recompute its schema, since a node keeps the schema it was built with when a child changes
  under it. A filter that reaches the scan is turned back into text: `address_bytes(c)` is `c`,
  a fixed-binary literal is its `0x` text, and a filter that still holds `address_text` after
  that is not pushed at all (pushdown is inexact, the filter stays above). The parquet reader
  therefore prunes by statistics as before, and a top-k over an address, which could not push
  its threshold into a scan over text views, now can: 500 rows read where it read 32,000. A
  value in an address column that is not `0x` and forty hex digits refuses the statement rather
  than reading as nothing, and `Engine::stream`, if no batch has reached the host yet, turns the
  lever off for that engine and runs the statement again over text: nuthatch's own fixtures write
  `0xa`, and a nest written by hand may too.
- **Through the plan.** An analyzer rule, `Addresses`, before `DuckComparisons` and coercion:
  `lower(a)` is `a`; a comparison, `IN` or `BETWEEN` between an address and text that spells an
  address compares fixed binary against the decoded literal; any other text context (another
  text operand, `||`, `LIKE`, `substr`, `length`, `CAST(.. AS VARCHAR)`, `upper`, any function
  not in the small set that keeps its argument's type, a `CASE` or `UNION` branch beside text)
  gets `address_text(a)`. Equality, joins, `GROUP BY`, `ORDER BY`, `count`, `min`, `max`,
  `array_agg`, `coalesce`, `nullif` and window partitions keep the binary; a window's `ORDER BY`
  and `arrow_typeof` see the text, the one because DataFusion runs a RANGE frame over what it
  can compare as a scalar, the other because the host's type is text. Join keys are not
  comparisons the expression walk meets, so the rule looks at them itself. Byte order and
  lower-case hex order agree, so `ORDER BY` and `min`/`max` answer as before.
- **The owned fold.** Its keys come out of the segments as text; a key the plan wants as fixed
  binary is decoded as the batch is built.
- **At the boundary.** `Engine::stream` puts a projection over the statement's physical plan that
  renders every `FixedSizeBinary(20)` column as its `0x` text. A host sees the schema and the
  rows it saw before; `EXPLAIN` shows the projection.

## What could go wrong

- A text context the rule does not cover refuses the statement at coercion. The gate's 75
  allocations statements, the 22 lodestar views against DuckDB (`engine-views`), and a test file
  of the contexts above are the net.
- A fixed-binary column leaking to a host through a path that is not `stream`: `parquet_scans`
  and `EXPLAIN` do not return rows; the information schema reports the column as binary, which a
  host reading types for its own rewrite (`temporal_columns`) does not look at for addresses.
- The fold declining address keys would cost more than the lever gains; it decodes instead.

## The measurement

Interleaved A/B on a quiet ThinkPad, the ledger, `lodestar_network` and
`lodestar_delegator_stakes` at 2 GB and 2 threads, four warm runs by three rounds, as in the
transcript's reader section. Then `engine-views` parity, then the gate.

## Measured

Interleaved A/B on a quiet ThinkPad, 2 GB and 2 threads, four warm runs by three rounds, medians
in ms (transcript, "Addresses as fixed binary"):

| view | text | fixed binary |
|---|---|---|
| lodestar_indexer_ledger | 925 | 923 |
| lodestar_network | 1,914 | 1,721 |
| lodestar_delegator_stakes | 1,560 | 1,496 |

`engine-views`: 22 of 22 lodestar views identical to DuckDB. The release gate on the ThinkPad,
all six nests against v5.9.0: pass, 0 answers differ; the allocations copy's peak RSS 1,017 MiB
against 1,227 on the same day's tree with addresses as text, which is the 42-byte strings
becoming 20 bytes wherever a plan holds them. The ledger does not move, since its
sorts and joins are on other keys; `lodestar_network`, which groups and joins on addresses most,
gains 10%. Within the 10% the production-shape profile bounded it to.
