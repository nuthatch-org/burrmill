# Reading a 1 GB CSV, against DuckDB's read_csv (#389)

Measured 2026-11-23 on a 32-core Linux 6.12 machine with 62 GiB, release build, best of three warm
runs. `burrmill-bench csv-ingest` writes the CSV into a temporary directory (the same bytes on
every machine), checks that both engines answer one statement over every column alike, then times
each reading the whole file into an in-memory table, and deletes the file:

    cargo build --locked --release -p burrmill-bench
    target/release/burrmill-bench csv-ingest     # BYTES=1000000000 THREADS=8 RUNS=3 BUDGET=8GiB

The file is 1,000,000,010 bytes, 22,863,606 rows of `id,amount,day,who,qty,ok`. Burrmill reads it
through `EngineBuilder::csv`, which is `burrmill-ingest`, the tab's reader, on one thread, under an
8 GiB budget. DuckDB is the bench's bundled 1.5.5, `CREATE TABLE AS SELECT * FROM read_csv(...)`.

| reader | seconds | MB/s |
|---|---:|---:|
| burrmill `EngineBuilder::csv` (1 thread) | 26.06 | 38.4 |
| duckdb `read_csv` (8 threads) | 0.74 | 1343.6 |
| duckdb `read_csv` (1 thread) | 5.16 | 193.7 |

**Burrmill is 35 times slower than DuckDB at 8 threads, and 5 times slower on one thread.** The
reader was built for a file a stranger drops into a tab, at most 200 MiB: it decodes the whole file
to a `String`, holds every row's fields before typing any column, and types each column in a second
pass, all on one thread. Nothing in it is parallel or streaming yet.

Types: `id` BIGINT, `amount` DECIMAL(7,2), `day` DATE, `who` VARCHAR, `qty` BIGINT, `ok` BOOLEAN.
DuckDB reads `amount` as DOUBLE; the parity statement sums it cast to DECIMAL(18,2), and both
answer `1143191597145.51`.

**Memory.** Ingest counts the bytes, the decoded text, the parsed rows and the columns, and the
count for this file passes 4 GiB: under `BUDGET=4294967296` (the CLI's default `--memory`) it is
refused, `budget max_memory_bytes exceeded: limit 4294967296, at 4535643211`, rather than
exhausting the machine. A 1 GB CSV needs `--memory` of more than 4.5 GB.
