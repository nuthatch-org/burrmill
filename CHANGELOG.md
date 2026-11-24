# Changelog

Nuthatch pins a git revision of burrmill. Each revision it adopts is tagged `vX.Y.Z` and gets an
entry here, newest first, naming what changed since the last tag. A tag is also what goes to
crates.io, with `burrmill-parquet-source` first when it has changed. The README says what a
version number promises.

A release that changes any receipt in `corpus/receipts/receipts.jsonl` says which, and why, in its
entry: a receipt someone kept stops verifying on that release (`tests/receipt_corpus.rs`).

## Unreleased

Engine (`burrmill`, feature `datafusion`)

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
