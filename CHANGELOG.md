# Changelog

Nuthatch pins a git revision of burrmill. Each revision it adopts is tagged `vX.Y.Z` and gets an
entry here, newest first, naming what changed since the last tag. A tag makes a GitHub
Release with prebuilt binaries and nothing else (`docs/releasing.md`). The README says what a
version number promises.

A release that changes any receipt in `corpus/receipts/receipts.jsonl` says which, and why, in its
entry: a receipt someone kept stops verifying on that release (`tests/receipt_corpus.rs`).

## Unreleased

Engine (`burrmill`, feature `datafusion`)

- Decimal `+`, `-` and `*` run in plain 128-bit arithmetic where a batch's magnitudes show no row
  can overflow, and `sum` and `avg` over integers and decimals add from the column's buffer, not
  row by row (#433). Arrow's checked kernel made a checked 128-bit multiply per row, and two more to
  rescale an operand. A batch that could overflow, or whose result exceeds its precision, goes to
  that kernel as before, so the same statements are refused. TPC-H Q1 at SF10 went from
  1,065–1,165 ms to 629–677 ms, from 3.37x–3.53x DuckDB 2.0.1 to 1.91x–2.10x. Only the kernels
  change, not the plans.
- A table equated with a filtered dimension's key only through another table reads only the keys
  that dimension keeps (#433). TPC-H Q5 equates `c_nationkey` with `s_nationkey` in a join on two
  columns, whose dynamic filter cannot be split between its probe's scans, so the five nations of
  `ASIA` never reached `customer` and its joins carried 2.28M orders and 9.10M `lineitem` rows at
  SF10, where DuckDB carries 456.8K and 1.83M. `customer` now gets a semi join on the nations the
  region keeps. A dimension reading more than a sixteenth of the table, a volatile filter, and a
  table already joined to the dimension itself are left alone; in TPC-H only Q5's plan changes.
- An inner join whose filtered build side is guessed at a quarter or less of an unfiltered probe
  side is collected rather than partitioned (#433), so the dynamic filter the probe's scan
  evaluates on every row is one hash lookup and not a `CASE` over each row's repartition hash. In
  Q5 at SF10 that filter's CPU went from 2.77 s to 0.77 s. With both, Q5 at SF10 went from 875–905
  ms to 486–515 ms, from 3.03x–3.35x DuckDB 2.0.1 to 1.67x–1.84x, and at SF1 from 90–100 ms to
  46–49 ms. Q2, 8, 9, 11, 16, 17 and 20 take the collected join too.
- `<`, `>`, `<=`, `>=` and `BETWEEN` between structs or lists order a NULL inside them after every
  value, as DuckDB does (#423): `{'x': NULL} > {'x': 0}` and `[1, NULL] > [1, 2]` are true, where
  Arrow's comparator put the NULL first and answered false.
- `SELECT *` over a `USING` or `NATURAL` join is the left side's columns, then the right side's
  without the join columns, as in DuckDB (#423). DataFusion kept whichever copy's table name
  sorted first, so `t3 NATURAL JOIN t2` put `t2`'s columns first, and over a `LEFT JOIN` it could
  answer the right side's NULL for a row the left side had. A join column of a `RIGHT JOIN ... USING`
  is the right side's and of a `FULL JOIN` both coalesced, in `*` and when named without a table.
  A `NATURAL JOIN` whose sides share no column is DuckDB's binder error, where it was a cross join.
- `POSITIONAL JOIN` is refused by name (#423); sqlparser read `POSITIONAL` as an alias and the
  `JOIN` as a cross join. `JOIN BY (TYPE ...)` and `NEAREST ... BY` are refused by name too, not as
  syntax errors. In DuckDB's sqllogictest suite, `test/sql/join` has 1177 pass, 304 refused, 1
  wrong and 120 error, where it had 1169, 176, 11 and 246; the whole suite 12227 pass and 604
  wrong, where 12199 and 634.
- The sqllogictest harness judges a statement under `SharedDefsExec` by the statement's own sort,
  where it took the shared subqueries for a join's inputs and the plan for unordered (#423).

- A hash join builds on the side its filter plainly cuts below the other, where it built on the
  side that reads fewer bytes (#433). Statistics being off, a filter's share is guessed from its
  predicate, a tenth for an equality and a third for a range or a comparison of two columns, and
  decides only when it leaves one side under an eighth of the other; a join it decides that way is
  also collected, if inner. TPC-H Q12 at SF10 built its table on all 15M orders, 930 MB, and now on
  the 310.8K `lineitem` rows it keeps, 15.6 MB, and no other TPC-H plan changes.
- A semi join whose build side is guessed, as above, to keep at most an eighth of what its probe
  side reads is collected rather than partitioned (#433), so the dynamic filter it puts in the
  probe's scan is one hash lookup and not a `CASE` over each row's repartition hash. TPC-H Q4 at
  SF10 builds on 573.7K orders and probes 60M `lineitem` rows; that filter's CPU went from 1.77 s to
  0.39 s and Q4 from 493–635 ms to 418–425 ms. Q2 and Q20 take it too, and no other TPC-H plan
  changes.
- A `UNION ALL` answers its first arm's rows, then the second's, as DuckDB does, where nothing
  above it reorders: `SELECT 1 UNION ALL SELECT * FROM range(2, 100) UNION ALL SELECT 999 LIMIT 5`
  answered `1, 999, 2, 3, 4`, as the coalesce above the union passed on whichever arm's batch came
  first (#430). Such a union is now an `OrderedUnionExec`, which still starts every arm at once and
  reads them in turn; an arm of several partitions is coalesced as before. Under an aggregate, a
  join or a sort the plan does not change, and TPC-H's plans are the same. Of the sqllogictest
  suite's records, 12199 pass where 12168 did and 634 are wrong where 665 were.
- A Parquet scan evaluates its `WHERE` in the `FilterExec` above it, not in the reader's row
  filter, where the predicate holds no join or top-k filter and no equality with a literal and the
  scan reads no text or nested column besides the predicate's (#433). The reader evaluated each
  conjunct in a pass of its own and saved no decoding when the rows it kept were scattered. TPC-H
  Q6's `lineitem` scan at SF10 took 3.27 s of CPU that way, against 1.84 s read whole and filtered
  above, and Q6 went from 404–442 ms to 268–296 ms. The predicate still prunes row groups and pages.
  Scans in Q7, 9, 12, 15, 16, 19, 20, 21 and 22 take it too.
- A `HAVING` that compares each group's `sum` with an uncorrelated subquery summing the same rows
  with no groups, as TPC-H Q11 does, now sums the groups for that total, and the groups are computed
  once and read by both (#433). Q11's subquery joined `partsupp`, `supplier` and `nation` again, so
  `partsupp` was scanned twice; at SF10 Q11 went from 189–208 ms to 101–120 ms. Only exact sums are
  taken apart this way, so the total is the same; a sum of doubles, a subquery over other rows,
  grouping sets, and anything correlated or volatile are left alone. No other TPC-H plan changes.
- A semi join on an aggregate's groups, `IN (SELECT k FROM t GROUP BY k HAVING ...)`, over an
  inner join to `t` on `k` whose aggregate above only sums `t`'s columns as the groups do, as TPC-H
  Q18 does, now joins the groups the `HAVING` keeps and sums their sums, so `t` is scanned and
  aggregated once (#433). A `HAVING` is guessed to keep a tenth of its input as groups, so that join
  builds on the groups and is collected. Q18 at SF10 went from 1,398–1,643 ms to 884–1,121 ms. Only
  exact sums are taken apart; a count, a sum of the other side's columns or of doubles, and anything
  correlated or volatile are left alone. No other TPC-H plan changes.

Build and CI

- The `burrmill` job frees runner disk before it builds. A Cargo.lock change misses most of the
  build cache, and the full debug build then failed with "No space left on device".

Dependencies

- Every DataFusion crate is 55.3.0, where it was 55.2.0, for branch-55's fixes to LEAD/LAG IGNORE
  NULLS, outer joins with deferred filters, NULL-padded rows in sort-merge join filters and integer
  interval propagation (#452). Both vendored copies are rebased on it; upstream's sources for those
  two crates are the same in both releases, so each change carries over as it was. `arrow` and
  `parquet` do not move. A receipt names the DataFusion it was made on, so one made on 0.4.0 does
  not verify on this revision; `demo/receipt.json` is regenerated, its answer unchanged, and every
  answer in `corpus/receipts` is unchanged. Every sqllogictest record keeps its class.
- Dependabot no longer opens DataFusion bumps; they move by hand, all crates at once, as
  `vendor/README.md` describes (#452).

## 0.4.0 - 2026-11-25

Prebuilt `burrmill` binaries for five targets, as a GitHub Release. What changed since 0.3.0:

A receipt still verifies only on the release that made it. `corpus/receipts` and `demo/receipt.json`
are regenerated for 0.4.0; every statement, input and answer is unchanged.

Engine (`burrmill`, feature `datafusion`)

- Answers as DuckDB 2.0.1 does where DuckDB's sqllogictest suite found small differences (#430):
  `//` and `%` over integers, HUGEINT and DECIMAL raise `Division by zero` where they were NULL,
  except under `CASE`, `AND`, `OR`, `COALESCE`, `TRY` or a window function's arguments, which
  DuckDB evaluates lazily and DataFusion does not, where they stay NULL; beside a float they are
  NULL as before. `list_distinct` drops NULL; a list `||` a NULL is NULL; `array_to_string` with a
  NULL separator joins with `,`; `corr` over pairs with no spread is NaN, one pair included. A bare
  `DECIMAL` or `NUMERIC` is DECIMAL(18,3), within a list, struct or map too, as DuckDB's type names
  there are now renamed as they are at the top. `typeof` names TIMESTAMP_S, TIMESTAMP_MS,
  INTERVAL, STRUCT and MAP as DuckDB does. A NATURAL JOIN on columns named alike but for case is
  refused, where it answered with both columns. Of the suite's 79719 records, 12168 pass where
  12088 did and 665 are wrong where 718 were; `docs/bench/sqllogictest-baseline.tsv` holds the new
  counts.
- An aggregate joined on one of its group keys computes only the groups the join can keep, where
  every row on the other side has a key a filtered scan holds: its input becomes a semi join on
  that scan's keys (`JoinedGroups`, #433). Decorrelating TPC-H Q17's subquery averaged all 2M
  parts over 60M rows at SF10 to keep 2,040. Where the keys read at most a sixteenth of what the
  semi join filters, it is collected, so the dynamic filter on the scan it probes is not a `CASE`
  over each row's repartition hash. Q17 at SF10 went from 1,469 to 1,553 ms to 756 to 838 against
  DuckDB 2.0.1's 276 to 296, and at SF1 from 141 to 147 to 113 to 118. Q2 and Q20 also take the
  semi join; every TPC-H answer is still DuckDB's.
- Under a budget with a spill directory, a hash join whose build side's files hold more than half
  the statement pool, or cannot be measured, is planned as a sort-merge join, whose sorts and key
  buffer can spill where a hash join's build side cannot. TPC-H Q21, refused at SF10 under 2 GiB,
  answers at SF10, SF30 and SF100 with a spill directory, and as DuckDB does (#383). A join that
  fits keeps its hash join, and so does one that builds on an aggregate's groups, which the files
  do not measure, as TPC-H Q18's `IN` does. Without a spill directory no plan changes and Q21 is
  refused as before.
- A spilling budget's spillers leave a sixteenth of the pool free for consumers that cannot spill,
  such as a sort's merge, and a repartition spills past its fair share rather than borrowing; both
  had starved Q21 at SF30 (#383).
- A sort that holds nothing, starting or just spilled, may take its batch from that reserve, within
  its fair share. It has nothing to spill, so refused it refused the statement, `Not enough memory
  to continue external sort`: what spilling sorts gave back was lent again to the ones borrowing,
  whose loans leave one fair share free for every consumer under its share, 18 MB in all with
  Q21's 96 spillers. Q21 at SF30 under 2 GiB was refused once in at least 14 runs, and its `EXPLAIN
  ANALYZE` at SF100 once; at 16 threads at SF30 it was refused in each of 4 processes, and now
  answers 15 of 15 (#434).
- JSON and NDJSON are read as CSV is since #407: on the budget's threads, a batch every 4 MiB of
  text, never held as rows before the columns are typed, and the 1 GiB cap on a JSON file is gone.
  Objects one after another are cut at record starts; a JSON array is cut the same way, at the
  `{` after a `,`, and where a guess at a cut lands inside a record that chunk is read again from
  where the one before it ended. Types, notes and refusals are the tab's: one inference path over
  the whole file, so a value far into it that its key's type does not read is refused at its row
  and key. Text that does not parse is still refused at its line and column, found by one walk
  over the file. The table, the receipt (`json:<sha256>:<bytes>`, unchanged) and every refusal are
  the same at any thread count. The 1 GB file of `burrmill-bench json-ingest` reads at the CLI's
  default `--memory` of 4 GiB, in 1.43 s at 8 threads with a peak of 1.43 GiB, where it peaked at
  3.32 GiB before (docs/bench/json-ingest.md) (#413).
- TPC-H Q17 at SF10 is within 1.5x datafusion-cli at 8 threads (0.98x to 1.30x over five rounds),
  with its answer DuckDB's. No code changed since #399 fixed the cause, which
  `docs/bench/tpch.md` now names (#381).
- TPC-H Q2 at SF10 is within 1.5x datafusion-cli at 8 threads once the load allows: 1.31x to 1.39x
  over five rounds at a 1-minute load of 7.6 to 8.8, and 1.45x to 1.73x at 11 to 15. Its answer is
  DuckDB 2.0.1's, and no aggregate in its plan is ordered. No code changed: #399's
  `LiteralGroupKeys` fixed it with Q17 (#382).

CLI (`burrmill`)

- `burrmill` with no command, or `burrmill PATH...`, is a shell: a prompt with history, statements
  over several lines that run once one ends with `;`, and `.tables`, `.schema [TABLE]` (both from
  `information_schema`), `.timer on|off`, `.mode table|csv|json`, `.read FILE`, `.help` and
  `.quit`. The banner and the prompt name the session's trust (a sandbox, the only kind until #387)
  and its budget. Ctrl-C cancels the statement running through the engine's token and returns to the
  prompt; Ctrl-D leaves. With stdin not a terminal it runs stdin's statements with no prompt and
  exits 1 if any failed. Line editing is rustyline, pure Rust; the wasm build does not reach it.
  `burrmill` alone used to print the usage and exit 2; `burrmill --help` still does. `q` and
  `verify` are unchanged (#390).

Bench (`burrmill-bench`)

- `DUCKDB=<path to the duckdb CLI>` makes `tpch` and `csv-ingest` compare against that CLI rather
  than the bundled DuckDB (which reports v1.5.4): its answers come back through a Parquet file it
  writes, its times from its `.timer`, its peak RSS from `wait4`, and the version it reports is
  printed. `docs/bench/tpch.md`, `docs/bench/csv-ingest.md` and the README's TPC-H and CSV figures
  are re-measured against DuckDB 2.0.1 on CPU, with Burrmill and datafusion-cli in the same window;
  the README says what Sirius is and that no Sirius numbers were measured (#414).

Build and CI

- DuckDB's own sqllogictest suite, every `test/sql/**/*.test` at v2.0.1, fetched as data and never
  committed, runs against the engine in a CI job of its own (`yatr sqllogictest`, run by
  `docs/bench/sqllogictest.sh`). Each record is a pass, a refusal by name, a wrong answer, an error
  or a skip, judged as DuckDB's runner judges it. `docs/bench/sqllogictest.md` publishes the counts
  per directory: 12088 pass, 6828 refused, 718 wrong, 3594 error and 56491 skipped of 79719
  records. The job fails if any directory's wrong answers rise or its passes fall against
  `docs/bench/sqllogictest-baseline.tsv`, and says how to lock in an improvement (#386). The
  harness is an example behind a feature of its own, `sqllogictest`, which adds nothing to the
  library and keeps it out of `cargo test --features datafusion`.

## 0.3.0 - 2026-11-24

Prebuilt `burrmill` binaries for five targets, as a GitHub Release. What changed since 0.2.0:

A receipt still verifies only on the release that made it. `corpus/receipts` and `demo/receipt.json`
are regenerated for 0.3.0; every statement, input and answer is unchanged.

Engine (`burrmill`, feature `datafusion`)

- `EngineBuilder::csv(path, table)` and `json(path, table)` open a CSV, JSON or NDJSON file as a
  table through `burrmill-ingest`, the tab's reader, so a column is typed as the tab types it. The
  table is held in memory, and that memory comes out of the budget's; a file that would need more
  than is left, or is JSON over 1 GiB, is refused. A short row, a quote never closed or a cell its
  column's type does not read is refused at its row and column. A receipt names such a file
  `csv:<sha256>:<bytes>` or `json:<sha256>:<bytes>`, by its bytes, as it names a Parquet file; no
  existing receipt changes (#389).
- A CSV is read on the budget's threads and built as a batch every 4 MiB of text, not held as rows
  before its columns are typed, and the 1 GiB cap on a CSV file is gone. Each column's type is still
  chosen over the whole file, so a cell far into it that its column's type does not read is refused
  at its row and column. The table, the receipt and every refusal are the same at any thread count.
  The 1 GB file of `burrmill-bench csv-ingest` now reads at the CLI's default `--memory` of 4 GiB
  (#407).

CLI (`burrmill`)

- `burrmill q` and `verify` read a `.csv` file as CSV and a `.json`, `.ndjson` or `.jsonl` file as
  JSON, named as a Parquet file is, `NAME=PATH` included. Directories are read for Parquet only, as
  before (#389).

Ingest (`burrmill-ingest`)

- JSON that does not parse is `IngestError::Syntax`, with the line and column it went wrong at,
  where it was `Invalid` (#389).
- An unclosed quote's issue names the column it opened in (#389).
- `ingest_parallel` reads CSV on up to a given number of threads, with the same table, report and
  refusals as `ingest_bytes`, which is that reader on one thread. A CSV table is now a batch every
  4 MiB of text rather than one batch, and the tab and the engine type it by the same pass (#407).

Dependencies

- `parquet` and the `arrow` 59 crates are 59.3.3, for RUSTSEC-2026-0595, an unsoundness
  advisory against `parquet` 59.3.2 (#409).

## 0.2.0 - 2026-11-22

Prebuilt `burrmill` binaries for five targets, as a GitHub Release. What changed since 0.1.0:

A receipt still verifies only on the release that made it. `corpus/receipts` and `demo/receipt.json`
are regenerated for 0.2.0; every statement, input and answer is unchanged.

Engine (`burrmill`, feature `datafusion`)

- TPC-H Q17 was quadratic in its parts through a constant group key; literal keys are taken out of
  the aggregate so that group is not ordered. Q2's correlated `min` is the same shape and fell with
  it. At SF10, Q17 went from 25.6 s to 1.7 s (#399, #381).
- `now()` is each statement's own, not the moment the engine opened, and a plan that calls a
  function that is not immutable is not kept (#372).
- Small statements (scans under 4 MiB) plan in one partition, on the statement's own thread, with
  the plan kept for the same text (#370); they get one optimizer pass, and analyzer rules skip
  expressions they cannot rewrite (#371, #376, #403). Finished statement threads are kept for the
  next statement (#396). Small local file ranges are read on the calling thread (#379).
  `datafusion-expr` is vendored so an expression is named once (#378).
- Analyzer rules before sharing walk each shared subtree once (#367).
- A sort key nesting `abs` or `sqrt` satisfies the order it was sorted by (#368).
- Every in-memory scan now sees the statement's deadline as it reads, and the relation bound is 96,
  after the untrusted-SQL sandbox found both holes (#369).
- The `url` crate is past RUSTSEC-2026-0592 (#398).

Tools and packaging

- A `v*` tag on main builds `burrmill` for five targets (static musl on both Linux architectures,
  both macOS architectures, and Windows), smokes each binary, writes `SHA256SUMS` and publishes the
  GitHub Release. A push to `release-dry-run` does the same and publishes nothing (#397, #391,
  #400, #401).
- The untrusted-SQL sandbox's threat model and a refused corpus of 106 attempts (#369).
- TPC-H at SF10: Burrmill's own costs against datafusion-cli measured, no default changed
  (#402, #384).


## 0.1.0 - 2026-11-19

The first release on crates.io, with `burrmill-parquet-source` 55.2.0. What changed since 0.0.1:

Engine (`burrmill`, feature `datafusion`)

- Any Parquet file, directory or store prefix opens as a table, with hive partitions (#352).
- Strict decimals: exact at the declared scale or refused, with `div(a, b, scale, mode)` (#353);
  `DECIMAL` up to 76 digits, strict mode included (#359).
- Deterministic mode answers the same bytes at any thread count and segment layout (#354).
- Query receipts: a re-checkable hash of statement, settings, inputs and answer (#355). A receipt
  verifies on the release that made it; `corpus/receipts` is regenerated for 0.1.0, with every
  statement, input and answer unchanged.
- `ASOF [LEFT] JOIN ... ON` and `USING`, at DuckDB's answers (#357).
- A join under an aggregate stops within 100 ms of its deadline, now tested (#365).
- A spilling budget's pool is shared fairly, a spilled repartition batch gets its own file, and
  a fold is left to DataFusion's aggregate when the budget can spill (#285, #330).
- A metered statement is costed from kept row-group statistics and column sizes, not footers
  (#317); a scan's bookkeeping is charged to the pool, and its parquet metrics are one set per
  partition (#313, #325).
- A held definition's built values are charged once (#316). A `HAVING` with no aggregate answers,
  and a panic below `Engine::sql` is an internal error (#328).
- `inspect` exports the functions a statement calls (#321); every statement path parses with the
  engine's dialect (#319).

The tab (`burrmill-wasm`, `demo/`)

- Aggregates and pseudonymised modes leak less: counts as order-of-magnitude buckets, failures by
  variant only, pseudonyms dealt afresh per ask and never applied to a literal (#282, #314, #315,
  #324). SQL runs with no model at all (#335).

Tools and packaging

- `burrmill`, the CLI, for strict, deterministic and receipted queries over Parquet (#356), static
  on x86_64 musl (#363).
- The Parquet source is its own crate, `burrmill-parquet-source`, so no consumer needs a
  `[patch]`; the README states the semver and MSRV policy (#364).
- TPC-H at SF1 and SF10 against DuckDB and datafusion-cli, losses first (#358).
- yatr runs every task (#331-#337).

## 0.0.1 - 2026-11-11

The first tag, on main as it stands. What the pull requests since #95 changed, #277 the last:

Engine (`burrmill`, feature `datafusion`)

- A batch over 4 MiB is handed to the caller alone, so nothing is built behind it (#273).
- A spill past its disk cap is `LimitExceeded` (#272).
- A statement reads at most 128 relations (#271).
- Rules over shared definitions move the plans they walk (#274).
- Local segments' footers are cached once for the process (#267), a table bound again to the same
  files reuses its file list (#268), and a scan holds pool memory for the segments it has
  opened (#269). RFC-0047 says what a process keeps (#270).
- A statement still planning stops when cancelled; SQL is capped at 1 MiB and error text at
  4 KiB (#252).
- Cancelling one statement stops that statement, and a statement can have a deadline (#239).
- An overflow or a budget refusal is its own error, `Overflow` or `LimitExceeded`, and
  `BurrmillError` is non-exhaustive (#230).
- `Engine::builder()` opens an engine from any source, budget and shared pool (#222).
- A statement can take `$n` placeholders (#233).
- A host can refuse EXPLAIN, which names segment paths (#246).
- A cast to TIMESTAMP is microseconds (#250).
- `list_sum`, `list_min` and `list_max` answer as DuckDB's (#248).
- A refused DuckDB form names what to write instead, and `docs/sql.md` lists what is
  admitted (#232).
- An address decodes only from lower-case hex (#219), and `register_facts` says what declaring
  one promises (#259).
- A hot tip folds only the credit and debit shape, and a cold row without a block is
  refused (#216).
- A shared subquery's readers hold a bounded number of its rows (#228).
- DataFusion 55.2 (#249); hashbrown 0.17 (#231).
- A statement always runs on its own thread, and one its caller stops still reports what it read (#207).
- Only a decode the rewrite placed can turn addresses off (#205).
- A hidden registration name is refused whatever comes before it (#204).
- `levenshtein` refuses past a bounded amount of work, and a cancel frees the gate (#198).
- List comprehensions are found in one pass (#197); a multi-byte character after a bracket no
  longer panics that rewrite (#119).
- Wide nests read footer schemas once per process and canonicalise directories once (#127).
- A union's top-k sorts by its merge's whole order (#126).
- `Engine::result_schema` is public (#124).
- Statement and scan-planning threads get the planner's 8 MiB stack (#123).
- The memory a statement's computed values hold is bounded and charged to the pool (#122, #125).
- A metered statement reports what it selected, read and held (#120), and times its segment scans
  on the thread CPU clock (#121).
- Addresses are fixed binary between the scan and the answer (#116), through recursive queries,
  correlated subqueries and pushed filters (#118); a column holding something else turns that
  off (#117), and `lower()` on one reaches the scan as the column itself (#108).
- A shared subquery too big to hold runs a fresh plan for each reader (#113); a shared definition
  is cut to what its readers take (#102); a repeated window is computed once (#103).
- A budgeted session reads 2,048 rows at a time (#112).
- Running max and min over a growing frame are one pass per partition (#107), and plan under one
  target partition (#111).
- RFC-0046: what sealed history has already answered is kept (#104).
- A symlinked segment is sized by its target (#105); `SegmentTable` reads the session's footer
  cache (#101).
- A view's or derived table's ORDER BY orders the answer when nothing above reorders (#95).
- Runs in a browser tab: an async query path with no tokio spawn on wasm32 (#106, #109).

Ingest and the tab (`burrmill-ingest`, `burrmill-wasm`, `demo/`)

- Ingest opens Parquet (#257) and JSON (#262) under the same budgets as CSV. A Parquet page
  claiming more than the budget (#261) and a list or map column (#266) are refused first.
- A first row is taken as names only when the rows under it are not all text (#238).
- A decimal with a negative scale prints with its zeros appended (#234).
- Aggregates mode returns the statement's own groups and still counts every small one (#221).
- Pseudonymised mode finds a tokenised value by rolling hash (#218) and deals token numbers at
  random (#256).
- In schema mode only a statement that does not plan goes back to the model (#240).
- The demo page passes an axe run (#241), fits a 360 px screen, exports CSV and runs edited
  SQL (#255), and requires Trusted Types, caches only its shell and budgets aggregate
  asks (#260).
- The tab is described in `docs/browser.md` (#235).
- Untrusted CSV becomes Arrow in a browser tab, and the demo page asks a model about it in four
  privacy modes (#128).
- Values are no longer changed silently: decimal marks per column, real dates, offsets, leading
  zeros (#195).
- The memory budget counts empty fields, rows and the built columns (#196).
- A sniff window that splits a character still reads as UTF-8, and delimiter sniffing is
  linear (#208).

Build and CI

- A nightly job fuzzes each target for ten minutes, SQL among them (#251, #258, #263, #264).
- CI runs every demo browser test against the built engine (#242, #277) and keeps the wasm32
  build free of warnings (#265).
- Every crate sets `publish = false` (#229). Dependabot leaves what moves with DataFusion, and
  the DuckDB oracle, alone (#217, #237, #247).
- The README's QoS and allocations figures are re-measured (#276), and the docs describe the
  tree as it stands (#220).
- Arrow 59.3.2; zstd and tokio move past their advisories (#202).
- CI checks dependencies with cargo deny, runs read-only, and pins the toolchain (#206).
- CI tests `burrmill-wasm` and gates the tab's engine at 8 MiB gzipped (#192).
- The bench allocates as nuthatch does (#114); where the time goes is measured (#115).
- The organisation is `nuthatch-org` (#96).
