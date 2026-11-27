# The untrusted-SQL sandbox

What `Engine` guarantees to a host that runs SQL it did not write, how the host must open it for
those guarantees to hold, and what it does not defend. The corpus in `corpus/sandbox/` is the
public list of attempts; `crates/burrmill/tests/sandbox_corpus.rs` runs every one of them against
an engine opened as below and fails if any is not refused with the kind its header names (#344).

## Who the attacker is

Someone who writes the whole statement: a customer typing into a query box, or an LLM agent asked
to answer a question with SQL. They choose every byte of it, may send as many as the host lets
them, and read whatever comes back, the error text included. They have no access to the process,
the host's code or its files except through statements.

## What they get

The tables and views the host registered, under the names it gave them, read only. Nothing else:
no file, no URL, no setting, no other statement's state, and no registration the host did not mean
to serve.

## Assets

- The host's filesystem and network: files beside the segments, `/etc`, object stores the process
  has credentials for, anything over HTTP.
- The registrations a statement may not name: `<table>__raw` beneath a wide table's public view,
  `<table>__hot` and `<table>__union` that a host's tip is staged in, and the segment paths behind
  every table.
- The host's in-memory tables (label snapshots, uploads), which must not change.
- Other statements: their answers, their settings, their place in the queue.
- Other tenants' memory and CPU on the same engine or the same `SharedPool`.
- The process itself: a panic or a stack overflow ends every tenant's statements, not one.

## The configuration a host must use

Every guarantee below assumes all of this. `sandboxed()` in `sandbox_corpus.rs` is this
configuration in code.

1. **A `Budget`.** `Engine::builder().budget(Budget { memory_bytes, threads, spill })` (or
   `open_nest_budgeted`, `open_empty_budgeted`). Without one there is no pool, so nothing a
   statement builds is charged, and values each under the value limit add up without bound
   (`df_host::a_bounded_engine_refuses_what_an_unbounded_one_answers`). The corpus runs at 256 MiB
   and four threads with no spill directory. With one, give it a cap: past it a spill is
   `LimitExceeded` (`df_host::a_spill_past_its_disk_cap_is_a_limit`).
2. **A deadline on every statement.** Run statements through `Engine::sql_for_each_stoppable`
   with `Stop { deadline: Some(..), .. }`. `Engine::sql` and the other forms have no deadline of
   their own; `Limits::timeout` is the owned operator's and does not apply here. The corpus uses
   one second.
3. **`set_explain(false)`.** A plan names the server's segment paths and the hidden registrations.
4. **A cap on what the host reads of an answer.** The engine has no row or byte cap on a result;
   the host's `f` counts and refuses, as `Limits::serving` caps a result at 32 MiB. The corpus does
   this and `exhaust-result-size` relies on it.
5. **The limits left at their defaults or lowered**: `set_sql_limit` (1 MiB) and `set_value_limit`
   (16 MiB).
6. **No host function it has not audited.** `register_scalar_udf` and `register_text_function`
   are the host's own; `df_functions` audits only what Burrmill registers.
7. **Only what the caller may read is registered.** A view the host registers runs with whatever it
   reads; the hidden-name check is about names a statement writes, not what a view reaches.

## Guarantees

Each with its mechanism and the test that proves it. A row with corpus cases has them under the
named prefix.

| Guarantee | Mechanism | Proved by |
|---|---|---|
| No file, directory or URL is read | No table factory, no file function, no replacement scan: a quoted path is a table name like any other and resolves to nothing (`NoSegments`). `read_csv`, `read_parquet`, `glob`, `sniff_csv` and every other table function but `range` and `generate_series` are refused (`MiniSession::get_table_function_source` in `df/session.rs`). `input_file_name` and `file_row_index` are removed. | `file-*`; `df_engine::read_csv_table_function_is_refused`, `replacement_scan_is_refused`; `df_functions::nothing_registered_reaches_outside_the_query` |
| Nothing is written, created, attached, loaded or set | Only a query, `VALUES`, `FROM`-first or `EXPLAIN` of one plans (`refuse_df_statement`, `refuse_non_query` in `df/mod.rs`); `COPY`, `CREATE EXTERNAL TABLE`, `RESET` by name; `ATTACH`, `INSTALL`, `PRAGMA` and the rest as non-queries. `EXPLAIN ANALYZE` is checked as the statement it wraps (#13). More than one statement is refused whole. | `write-*`, `config-*`; `df_host::explain_does_not_admit_what_it_wraps`, `several_statements_are_refused_not_run_in_part`; the corpus test checks `labels` and the registrations are unchanged after every case |
| Hidden registrations cannot be named | `refuse_hidden_names` refuses `__raw`, `__hot` and `__union` in any spelling, qualified or quoted, whatever fault comes first (#10, #189); `information_schema` lists none of them | `hidden-*`; `df_host::a_statement_cannot_name_the_hidden_parts_of_a_registration`, `a_hidden_name_is_refused_whatever_comes_before_it` |
| A plan does not leave | `set_explain(false)` refuses `EXPLAIN` and `EXPLAIN ANALYZE` in every spelling (#187) | `leak-explain*`; `df_engine::explain_is_refused_when_the_host_says_so` |
| Error text is bounded and asks nothing of the caller | Cut at 4 KiB (`clipped` in `df/mod.rs`), so an error cannot quote a 16 MiB input back (#190). DataFusion's request to file a bug is taken off its internal errors, and a call matching no signature reads as the plan error it is (`without_bug_report` in `df/errors.rs`). | `leak-error-echo-*`, `leak-internal-error-advice`; `df_engine::long_statements_and_long_error_text_are_cut`, `a_call_matching_no_signature_is_a_plan_error_and_asks_for_no_bug_report` |
| A statement cannot change what another sees | No setting is reachable; `address_bytes`, whose refusal once turned binary addresses off for every caller, is not callable (#188); `$1` parameters are values, bound after every refusal | `hidden-address-bytes`, `config-set-*`; `df_params::a_parameter_is_a_value_and_never_sql`, `a_parameterised_statement_meets_every_refusal` |
| Text is bounded before it is parsed | A statement over 1 MiB is refused before any rewrite runs; the list-comprehension pre-parse is one linear pass (#185) | `exhaust-giant-sql`, `exhaust-unmatched-brackets`; `df_functions::parsing_grows_linearly_with_the_statement` |
| Shape is bounded before it plans | Brackets nested deeper than 64 before the parse, and a parse that starts more than four prefix expressions a byte stops; expressions deeper than 64, types deeper than 32, `IN` lists over 1,024 terms, more than 64 set operations, more than 96 relations, a `GROUP BY` of more than 4,096 grouping sets (`df/depth.rs`, `df/dialect.rs`, #25, #186, #271, #344); sqlparser's own recursion limit | `exhaust-deep-*`, `exhaust-cube`, `exhaust-in-list`, `exhaust-many-relations`, `exhaust-join-chain`, `exhaust-join-chain-past-the-bound`; `df_engine::a_statement_over_too_many_relations_is_refused_before_planning`, `a_thousand_term_chain_is_refused_and_the_process_lives` |
| Memory is bounded | Every holding operator charges the pool; a value past the value limit is refused and large ones are charged (`df/bound.rs`, #122); `range` and `generate_series` past ten million rows are refused while planning, and charged below it; a batch over 4 MiB is handed over alone (#273) | `exhaust-repeat*`, `exhaust-string-agg`, `exhaust-array-agg`, `exhaust-list-*`, `exhaust-group-by`, `exhaust-sort`, `exhaust-window`, `exhaust-range-table`; `df_valuebound::no_statement_builds_values_past_its_pool`, `df_host::nothing_is_built_behind_a_wide_batch_the_caller_holds` |
| Quadratic work inside one poll is bounded | `levenshtein` refuses past 10^8 steps a pair and 10^9 a batch (#184), as `LimitExceeded` | `exhaust-levenshtein*`; `df_engine::levenshtein_answers_short_strings_and_refuses_a_long_pair_at_once` |
| A statement stops at its deadline | The statement's token carries the deadline. Nest scans, scans of rows in memory (`range`, `generate_series`, `VALUES`, a host's tables), and every join's output and filter check it (`df/cancel.rs`); a scan of rows in memory hands its batches on cut to the statement's size, copied as they are read, so a filter over them is not one poll (`cut` in `df/cancel.rs`). The batch loop races it, and every planning rule checks it (#252). A cancel also frees a statement queued at the gate (`enter_cancellable`). | `exhaust-cross-join-chain`, `exhaust-recursive-*`, `exhaust-*-over-range`, `join-*`; `df_host::a_statement_stops_at_its_own_token_and_deadline`, `a_statement_over_rows_in_memory_stops_at_its_deadline`, `a_join_under_an_aggregate_stops_at_its_deadline`, `a_cancel_stops_a_statement_still_planning`; `sandbox_corpus::a_join_stops_within_100ms_of_its_deadline` |
| Exact arithmetic is not wrapped | Overflow is refused, never answered (`Overflow`) | `exhaust-overflow`; `tests/df_checked.rs` |
| A fault in the engine does not unwind into the host | Planning and running are on threads of their own with 8 MiB stacks; a panic there is `Internal` | `df_engine::planning_does_not_use_the_caller_stack` |

Under that configuration the corpus test also holds every case to the deadline plus one second, the
pool to its budget, and the process's resident high-water mark to 1 GiB of growth over the whole
corpus. On an M-series Mac in a debug build that growth was 700 to 930 MB across runs: the pool
holds 224 MB of the 256 MiB budget, and the rest is what statements build outside it and what the
allocator keeps between them.

## What it does not defend

- **One batch is one poll.** The token is seen between batches, so the delay past a deadline is
  what one batch costs: one of the statement's size (2,048 rows under a budget) for a scan of rows
  in memory, one row group for a segment. Building one hash table and one call of a function over
  a batch are not checked inside.
- **One optimizer rule inside the relation bound.** DataFusion's `eliminate_cross_join` is one call
  no token reaches, and it is about cubic in the relations. The bound is 96 so that it stays under
  a second: 96 relations plan in 0.67 s in release, where 128 held it for 1.3 s past any deadline
  (#252, #271). The largest production statement reads 65. A deadline shorter than that planning
  time is still overrun by it.
- **An engine opened without a `Budget`, or a statement run without a deadline.** Nothing above
  about memory or time holds for them.
- **Memory outside the pool.** Batches in flight, values under the value limit, plans, and the
  footer caches, each held to an eighth of a budget, are not charged. The corpus bound above is the
  measure, not a guarantee.
- **Timing and other side channels.** How long a statement takes, whether it was refused for
  memory, and the plan error a name produces all depend on the data and the catalogue. An attacker
  learns nothing they could not read, but what they can read they can also learn by timing.
- **Error text quotes what the statement read.** Up to 4 KiB of it, cells included (`error(text)`
  is callable). A host that must not show cell values to a reader of errors filters them itself, as
  the tab's schema mode does.
- **Some refusals are not `NotAllowed`.** A path as a table is `NoSegments`, `COPY ... FROM` and
  `EXPORT DATABASE` do not parse (`Parse`), a removed function is `Plan`, a regular expression too
  big to compile is `Substrate`, and `range` past ten million rows is `Plan`. Each refuses before
  reading anything; the corpus pins which kind each one is.
- **Bugs in DataFusion, Arrow, parquet-rs and sqlparser.** A wrong answer, a panic the engine turns
  into `Internal`, or a stack overflow on a path the depth bounds miss, which aborts the process.
  The bounds narrow this; they do not remove it.
- **Fair share between tenants.** The gate admits `max(threads / 2, 2)` statements at once in
  arrival order, and each takes up to the budget's threads. One tenant sending many statements
  delays the others; rate limiting is the host's.
- **`sql_receipt` and `verify_receipt`.** They take no `Stop`, and hashing reads every input whole
  before the statement's own refusals run. Not for untrusted callers.
- **The host's own views and functions,** and anything a host does with an answer.
- **The tab.** `sql_async` takes no `Stop`; in a tab the token is seen only between batches, and the
  page's 60 s limit ends the worker instead (docs/browser.md). The model-assisted modes there rest
  on this engine and add their own checks, which this document does not cover.

## The corpus

One statement per file under `corpus/sandbox/`, with a header:

```
-- expect: NotAllowed             one kind, or alternatives: LimitExceeded | Timeout
-- says: not a table this surface serves      optional: the error text contains this
-- tries: what the statement attempts
-- gap: ...                       optional: a known gap above, run only in its ignored test
-- pad-to: 1048577                optional: padded with a trailing comment to this many bytes
SELECT ...
```

`NoRows` as the expected kind means the statement answers, with nothing. The `join-*` cases are
also held to 100 ms past their deadline. Alternatives are for
exhaustion, where the pool or the deadline may come first depending on the machine. File names
start with their class: `file`, `config`, `write`, `hidden`, `leak`, `exhaust`, `join` and `gap`.
A new case that is not refused is a finding: it is not weakened to pass, it is fixed or marked
`gap` and listed above.

```
cargo test -p burrmill --features datafusion --test sandbox_corpus
cargo test -p burrmill --features datafusion --test sandbox_corpus -- --ignored   # the gaps
```
