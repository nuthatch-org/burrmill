# The SQL surface

What `Engine::sql` admits and refuses. Statements are read as DuckDB's dialect and rewritten into
what DataFusion plans (`crates/burrmill/src/df/dialect.rs`), so a nuthatch view written for DuckDB
answers unchanged. The check is `dialect-parity` (`crates/burrmill-bench/src/dialect_parity.rs`):
every statement there runs through DuckDB and through `Engine`, and both answers are encoded as
nuthatch encodes them and compared byte for byte. A difference fails the run unless `KNOWN` gives
its reason.

```
cargo run -p burrmill-bench --release -- dialect-parity
SQL='SELECT ...' cargo run -p burrmill-bench --release -- dialect-parity   # one statement
```

Everything below marked "corpus" has at least one statement there. The rest is admitted and tested
in `crates/burrmill/tests/`, but not yet compared with DuckDB.

## Statements

`SELECT`, `WITH`, `VALUES`, `FROM`-first (`FROM t SELECT a`, `FROM t`) and `EXPLAIN` of any of
them. Anything else is refused: `only SELECT/WITH is admitted`, and `CREATE EXTERNAL TABLE`,
`COPY` and `RESET` by name. The nest's hidden tables (`__raw`, `__hot`, `__union`) are refused
wherever the statement names them.

## Admitted DuckDB spellings

| Written | Planned as | |
|---|---|---|
| `HUGEINT`, `BIGNUM` | `DECIMAL(38,0)`, refused past 38 digits | corpus |
| `DECIMAL(p, s)`, `38 < p <= 76` (DuckDB stops at 38) | a 256-bit decimal: exact `+ - *` and `SUM`, refused past 76 digits; `AVG` is DOUBLE, as for any DECIMAL | `tests/df_decimal76.rs` |
| `UBIGINT`, `UINTEGER`, `USMALLINT`, `UTINYINT` | the unsigned types | corpus |
| `a // b` | exact truncating division; DOUBLE beside a DuckDB DECIMAL | corpus |
| `a / b` | DOUBLE, as DuckDB divides | corpus |
| `a % 0` | NULL | corpus |
| `(a, b) > (c, d)` | the lexicographic comparison | corpus |
| `a IS [NOT] DISTINCT FROM b` | bound as DuckDB binds it | corpus |
| `a ^@ b` | `starts_with(a, b)` | corpus |
| `LIKE ... ESCAPE 'c'` | any one escape character, none unless named | corpus |
| `if(c, a, b)` | `CASE WHEN c THEN a ELSE b END` | corpus |
| `ifnull` | `coalesce` | |
| `struct_pack(k := v)` | `named_struct` | corpus |
| `arg_max`, `arg_min`, `max_by`, `min_by` | the row with the extreme key, NULLs passed over | corpus |
| `first`, `last` | `first_value`, `last_value`, NULLs included | corpus |
| `last_value(x IGNORE NULLS)` | as DuckDB places the clause | corpus |
| `year(t)`, `month(t)`, `epoch(t)`, `dayofweek(t)`, ... | `date_part` | corpus |
| `regexp_matches`, `regexp_full_match` | `regexp_like`, anchored for the second | corpus |
| `list`, `array_agg`, `string_agg` with `ORDER BY` | ordered aggregates | corpus |
| `[x FOR x IN l IF p]`, `lambda x: ...` | list comprehension and lambdas | corpus |
| `list_reverse_sort`, `split_part` (DuckDB's part 0) | DuckDB's answers | corpus |
| `list_sum`, `list_min`, `list_max` | `sum`, `min`, `max` over the elements; an exact list sums to HUGEINT or `DECIMAL(38,s)` | corpus |
| `TRY(x)`, `TRY_CAST` | NULL on failure, where that is certainly DuckDB's answer | corpus |
| `to_timestamp(n)` | TIMESTAMPTZ, microseconds, UTC | corpus |
| `date_trunc`, `DATE ± INTERVAL` | TIMESTAMP in microseconds | corpus |
| `CAST(s AS TIMESTAMP)`, `DATETIME` | microseconds, a finer fraction cut off | corpus |
| `printf`, `format`, `strftime`, `date_diff`, `hex`, `unhex`, `decode`, `xor` | DuckDB's | corpus |
| `to_json`, `from_json`, `json_extract`, `string_split`, `len`, `typeof` | DuckDB's | corpus |
| `range(...)`, `generate_series(...)` over integer literals | a BIGINT column | corpus |
| `unnest(l)` in the select list | DataFusion's unnest | corpus |
| `GROUP BY ALL`, `ORDER BY ALL`, `QUALIFY`, `FILTER (WHERE ...)` | as DuckDB means them | corpus |
| `ORDER BY` with no `NULLS` clause | NULLs last in both directions | corpus |
| `LIMIT 1.5` | rounded half away from zero | corpus |
| `SELECT *, a` | two columns of one name, as DuckDB allows | corpus |
| identifiers in any case, quoted or not | the one name they can mean | corpus |
| `information_schema` | follows the nest's views | corpus |
| `* EXCLUDE (...)` | DataFusion's | corpus |

## Refused by name

`Engine` refuses each of these with a message that says so, and names what to write instead where
there is a supported equivalent. The refusals and their messages are held by
`duckdb_answers_still_refused_say_so_by_name`, `refusals_name_what_to_write_instead`,
`pivot_after_from_is_refused_by_name_not_dumped`, `asof_join_is_refused_by_name_and_the_word_stays_usable` and `median_over_decimal_says_what_to_write`
(`crates/burrmill/tests/df_functions.rs`). Equivalents other than the `DECIMAL` median's are in
the corpus and answer as DuckDB does.

| Refused | Write instead |
|---|---|
| `approx_count_distinct(x)` | `COUNT(DISTINCT x)`, the exact count |
| `list_aggregate(l, 'sum')`, `array_aggregate` | `(SELECT sum(v) FROM (SELECT unnest(l) AS v))` |
| `strptime(s, fmt)` | for ISO 8601 text, `CAST(s AS TIMESTAMP)` |
| `try_strptime(s, fmt)` | for ISO 8601 text, `TRY_CAST(s AS TIMESTAMP)` |
| `COLUMNS(...)` | the columns by name, or `* EXCLUDE (...)` |
| `a GLOB p` | `a LIKE p`, with `%` for `*` and `_` for `?` |
| `PIVOT`, also after `FROM` | one aggregate per value, `sum(a) FILTER (WHERE k = 'x')` or `CASE`, under `GROUP BY` |
| `UNPIVOT`, also after `FROM` | one `SELECT` per column, joined by `UNION ALL` |
| `ASOF JOIN ... ON` or `USING` (`ASOF LEFT JOIN` stays a syntax error) | join on the inequality and keep the nearest row with `row_number() OVER (PARTITION BY ... ORDER BY ... DESC) = 1` |
| `median(x)` over a `DECIMAL` | `median(CAST(x AS DOUBLE))`, `quantile_cont(x, 0.5)` or `approx_median(x)` |
| `LIKE ... ESCAPE 'c'` over a pattern that is not a literal | a literal pattern, or `ESCAPE '\'` |
| `UHUGEINT` | none: `DECIMAL(38,0)` stops short of 2^128 |
| `unnest` in `FROM` | `unnest` in the select list |
| `range` over anything but integer literals | none |
| `SELECT DISTINCT a ... ORDER BY b`, `b` not selected | none: DuckDB picks no defined row for `b` |
| `WITH x(a, a)` | distinct column names |
| `CAST(struct AS VARCHAR)` | none: `to_json` writes JSON, not DuckDB's struct text |
| an integer literal wider than 64 bits | `CAST('n' AS DECIMAL(38,0))` |
| a `HUGEINT` past 10^38 | none: `DECIMAL(38,0)` cannot hold it |

## Bounds

Expression depth (64) and terms (1,024) and query or set-operation nesting (64) are bounded
before planning (`df/depth.rs`), the memory a statement holds by the engine's budget, and `range`
at ten million rows. Past a bound the statement is refused, never truncated.

## Strict decimals

Off by default, where DECIMAL follows DuckDB. `Engine::set_strict_decimals(true)` holds every
DECIMAL answer exact at its declared scale or refuses the statement (`tests/df_strict.rs`, which
checks a generated corpus against an exact oracle):

| Written | Under strict decimals |
|---|---|
| `a / b` over integers or DECIMALs | refused; `div(a, b, scale, 'half_even')`, or `//` for integers |
| `AVG(x)` over a DECIMAL | refused; `div(SUM(x), COUNT(x), scale, 'half_even')` |
| a function or aggregate turning a DECIMAL into DOUBLE | refused, unless the statement writes `CAST(x AS DOUBLE)` |
| a cast dropping nonzero digits (`1.005` to `DECIMAL(10,2)`, `2.5` to `INTEGER`) | refused; `round(x, 2)` first. `TRY_CAST` gives NULL |
| `DECIMAL * DECIMAL` past scale 38 | refused, as without the mode |

`div(a, b, scale, mode)` is admitted in either mode: `a / b` exactly, rounded once to `scale`
digits by `half_even`, `half_up`, `down`, `up`, `floor` or `ceiling`, as `DECIMAL(38, scale)`, and
refused for a zero divisor or past 38 digits, and for a wider DECIMAL, which is cast down first.

## Deterministic mode

Off by default. With `Engine::set_deterministic(true)`, a statement over the same rows answers
the same Arrow IPC bytes from `sql` and `sql_ipc` at any thread count and over any split of the
rows into segments (`tests/df_deterministic.rs`; `yatr run determinism` runs 10,000 generated
statements at 1, 2, 8 and 32 threads over three layouts):

| Where order could show | In deterministic mode |
|---|---|
| rows the statement leaves unordered | sorted by every column |
| ties in the ORDER BY under a LIMIT, or of the answer | broken by every column, after the written keys |
| `row_number`, `lag`, `lead`, `ntile`, and any window over a ROWS frame | the window's ORDER BY ties broken by every column |
| `array_agg`, `string_agg`, `first_value`, `last_value` | ordered by their arguments after any written ORDER BY |
| `first_value`, `last_value`, `nth_value` over a RANGE or GROUPS frame | refused; write a ROWS frame |
| `stddev`, `var*`, `corr`, `covar*`, `regr_*` over floats, integers included | refused: computed in floats, merged in arrival order |
| `approx_percentile_cont`, `approx_median` | refused; `quantile_cont` and `median` are exact |
| `random`, `uuid`, `now`, `current_date` and the other clocks | refused |

SUM and AVG over DOUBLE are exact whatever the partitioning in either mode. The answer is cut into
batches of 8,192 rows, with nothing left under a NULL; a batch streamed by `sql_for_each` is not.

## Receipts

`Engine::sql_receipt(sql)` answers with a receipt, in deterministic mode only: the statement as the
parser prints it back, the engine's and DataFusion's versions, the strict-decimals setting, the
SHA-256 of every input file the statement reads (and of the canonical Arrow IPC of rows a host
registered), and the SHA-256 of the answer's canonical IPC. `Receipt::to_json` writes it as one
line; `Engine::verify_receipt` runs it again over the inputs this engine holds and names the
first field that differs. Inputs are hashed by content, not path or table name, so a nest on disk,
the same files in a bucket and the same rows in the tab give the same receipt
(`tests/df_receipt.rs`, `demo/receipt.mjs`). A digest over the fields refuses a receipt edited by
hand; it is a checksum, not a signature. Every input file is read whole to hash it.
