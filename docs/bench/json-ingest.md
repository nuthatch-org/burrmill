# Reading a 1 GB NDJSON or JSON file, against DuckDB's read_ndjson and read_json (#413)

Measured 2026-11-24 on a 32-thread Linux 6.12 machine (i9-13980HX) with 62 GiB, release build.
The machine was shared with another worker's benchmarks, and the 1-minute load is given before and
after each measurement; it never fell under 4 while timings were taken, so these are loaded
figures, with DuckDB timed in the same window so the ratios stay fair. Each figure is the best of
three warm runs, and every run of the bench is shown. `burrmill-bench json-ingest` writes the
NDJSON file, then the same records as one JSON array, into a temporary directory (the same bytes on
every machine), checks that both engines answer one statement over every column alike, then times
each reading the whole file into an in-memory table, at `THREADS` and at one thread, and deletes
the file:

    cargo build --locked --release -p burrmill-bench
    target/release/burrmill-bench json-ingest     # BYTES=1000000000 THREADS=8 RUNS=3 BUDGET=4GiB

The NDJSON file is 1,000,000,016 bytes, 11,592,173 records of
`{"id":…,"amount":…,"day":"…","who":"…","qty":…,"ok":…}`, one a line; the array is
1,000,000,047 bytes, 11,460,778 of the same records between `[` and `]`, one a line after a `,`.
Burrmill reads them through `EngineBuilder::json`, which is `burrmill-ingest`, the tab's reader,
under a budget of 4 GiB (the CLI's default `--memory`) and the thread count shown. DuckDB is the
bench's bundled one (`libduckdb-sys` 1.10504.0), `CREATE TABLE AS SELECT * FROM read_ndjson(...)` or
`read_json(..., format = 'array')`, after `SET threads`.

## Since #413

Seconds each, best of three:

| run | file | burrmill 8 threads | duckdb 8 threads | ratio | burrmill 1 thread | duckdb 1 thread | ratio |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 (load 7.70 to 8.68) | NDJSON | 1.41 | 0.54 | 2.61 | 7.25 | 3.72 | 1.95 |
| 1 | JSON array | 1.39 | 4.42 | 0.31 | 7.70 | 4.20 | 1.83 |
| 2 (load 5.17 to 6.46) | NDJSON | 1.49 | 0.64 | 2.31 | 7.37 | 3.68 | 2.00 |
| 2 | JSON array | 1.37 | 4.47 | 0.31 | 7.19 | 4.15 | 1.73 |

**At 8 threads Burrmill reads NDJSON at 2.31 to 2.61 times DuckDB's time, and a JSON array in under a
third of it; on one thread, 1.73 to 2.00 times DuckDB's time.** DuckDB's `read_json` reads an
array file at about the same speed whatever its thread count; Burrmill cuts an array as it cuts
NDJSON. At 8 threads Burrmill reads at 673.1 to 730.4 MB/s, against 114.8 (NDJSON) and 115.9 (array)
MB/s before.

Through the CLI, `burrmill q "SELECT COUNT(*) AS n FROM trades" trades.ndjson` at the default
`--memory` (4 GiB), timed with `getrusage` around the process, best of three:

| `--threads` | load | wall | user CPU | system CPU | peak RSS |
|---:|---|---:|---:|---:|---:|
| 8 | 6.46 to 6.74 | 1.43 s | 7.65 s | 0.42 s | 1.43 GiB |
| 1 | 6.74 to 5.86 | 7.75 s | 7.32 s | 0.43 s | 1.43 GiB |

The peak is the file's bytes (1.0 GB), held while it is read and hashed, and the table's Arrow.
Before, the same command peaked at 3.32 GiB (3,404 MiB at 8 threads, 3,406 MiB at 1).

A 1,200,000,001-byte NDJSON file from the same generator (`BYTES=1200000000`), 13,885,130 records,
reads in 1.82 s at 8 threads with a peak of 1.71 GiB; before, the CLI refused it
(`budget max_bytes exceeded: limit 1073741824, at 1200000001`).

Types, the same before and after: `id` BIGINT, `amount` DECIMAL(7,2), `day` DATE, `who` VARCHAR,
`qty` BIGINT, `ok` BOOLEAN. DuckDB reads `amount` as DOUBLE; the parity statement sums it cast to
DECIMAL(18,2), and both answer `579614229984.81` for the NDJSON file and `573044532646.61` for the
array. A receipt over the NDJSON file from the CLI before and after is the same bytes
(`json:da1dc7b6…:1000000016` as its input), and each CLI verifies the other's.

**How it reads.** As CSV has since #407: the text is cut into chunks of 4 MiB at record starts. A
chunk's start is a guess, the first `{` after its offset that follows a `}` (records one after
another) or a `,` (an array); the chunk before it, read from a record start it knows, says where
its records really end, and a chunk whose guess was wrong (the `{` was in a string or a nested
value) is read again from there. NDJSON is not cut at line ends: serde_json refuses a raw line end
inside a string, but a record may span lines. The first chunk is read alone and gives a guess at
every key's type; the other chunks are then read at once, each counting what its values read as and
building an Arrow batch under the guess. Each column's type is chosen from the counts over the
whole file by the tab's rule, the batches built under a guess that stood are kept, a key the first
chunk never met is a column of nulls in them, and any other chunk is built again. A value its key's
type does not read, however far into the file, is reported at its row and key, and the engine
refuses the file there. Records are never held as rows. Text that does not parse is refused at its
line and column, found by one walk over the file from its start, so the refusal is the same however
the file was cut.

What is left behind DuckDB: one thread parses NDJSON at 135.6 to 138.0 MB/s against DuckDB's 269.0 to
271.5 MB/s;
each record goes through serde_json's visitor and each value is classified as text, as a CSV cell
is.

## Before #413

JSON was read on one thread into rows held in memory, every row typed after the whole file was
parsed, and a file over 1 GiB refused. Run in the same window as run 1 and 2 above, load 8.47 to
5.55, seconds each, best of three:

| file | burrmill 8 threads | duckdb 8 threads | ratio | burrmill 1 thread | duckdb 1 thread | ratio |
|---|---:|---:|---:|---:|---:|---:|
| NDJSON | 8.71 | 0.63 | 13.84 | 9.38 | 3.68 | 2.55 |
| JSON array | 8.63 | 4.41 | 1.96 | 9.96 | 4.16 | 2.39 |

Where the time went, timed by phase on main (`5444292`) with timers added, `burrmill-ingest` alone
over the NDJSON file under a 4 GiB budget, one run at load 1.04 to 1.51: 8.65 s in all, 3,399 MiB
peak RSS, 2,787,144,280 bytes charged to the budget.

| phase | seconds |
|---|---:|
| sniff the encoding, decode to a `String` | 0.06 |
| parse every record into a row of its cells | 5.56 |
| count every cell's type, column by column, and choose the types | 1.91 |
| build each column's array | 0.97 |
| free the rows | 0.15 |

Two thirds of it was the sequential parse into rows, which were then held (2.8 GB for a 1 GB file)
while a second pass typed them. The reader now counts a value's type as it parses it, once, builds
the batch in the same pass, and does both on every chunk at once.
