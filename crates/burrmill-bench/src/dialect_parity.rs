//! `dialect-parity` (roadmap 6.5): the same SQL through DuckDB and `burrmill::Engine`, each result
//! encoded as nuthatch encodes it, compared byte for byte.
//!
//! DuckDB reads the fixture through views built as nuthatch builds them (`_dec` by `TRY_CAST`);
//! Burrmill opens the same directory as a nest. A difference is either the dialect layer's to close
//! or listed in `KNOWN` with the reason it stands.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

const CORPUS: &[&str] = &[
    "SELECT printf('%d-x', 42) AS a, printf('%5d|%-5d|%05d|%+d|% d', 42, 42, 42, 42, 42) AS b, printf('%x %X %o %#x %#o', 255, 255, 8, 255, 8) AS c, printf('%c%%', 65) AS d",
    "SELECT printf('%s and %s', 'a', 'b') AS a, printf('%10s|%-10s|%.2s', 'abc', 'abc', 'abc') AS b, printf('%d', -7) AS d",
    "SELECT printf('%s', 42) AS a",
    "SELECT printf('%s', true) AS a",
    "SELECT printf('%d', true) AS a",
    "SELECT printf('%x', -1) AS a",
    "SELECT printf('%s', 1.5::DOUBLE) AS a",
    "SELECT printf('%d', 1.5::DOUBLE) AS a",
    "SELECT printf('%f', 2) AS a",
    "SELECT printf('%d', CAST(value AS HUGEINT)) AS a FROM transfer ORDER BY block_number, log_index",
    "SELECT printf('%f|%.2f|%10.3f|%-10.1f|%+.1f', 3.14159::DOUBLE, 2.675::DOUBLE, -1.5::DOUBLE, 0.05::DOUBLE, 1::DOUBLE) AS a, printf('%e|%.2E|%g|%g|%g|%#g|%G', 12345.678::DOUBLE, 0.000123::DOUBLE, 100000::DOUBLE, 1000000::DOUBLE, 0.0001::DOUBLE, 1.5::DOUBLE, 1e-10::DOUBLE) AS b",
    "SELECT printf('%d|%s', block_number, \"from\") AS a, printf('%.4f', CAST(value AS DOUBLE) / 1e18) AS b, printf('%s', NULL) AS d FROM transfer ORDER BY block_number, log_index",
    "SELECT format('{}', 42) AS a, format('{}', -7) AS b, format('{:s}', 'x') AS c, format('{}', CAST(value AS HUGEINT)) AS d FROM transfer ORDER BY block_number, log_index",
    "SELECT format('{} and {}', 'a', 42) AS a, format('{1} {0}', 'x', 'y') AS b, format('{:>6}|{:<6}|{:^6}|{:*^7}', 'ab', 'ab', 'ab', 'ab') AS c, format('{:.2f}|{:08.3f}|{:+d}|{:x}|{:#x}|{:05d}', 3.14159::DOUBLE, -2.5::DOUBLE, 5, 255, 255, 42) AS d",
    "SELECT format('{}', 0.1::DOUBLE) AS a, format('{}', 1e20::DOUBLE) AS b, format('{}', 1e-7::DOUBLE) AS c, format('{}', 123456.789::DOUBLE) AS d, format('{}', 1e15::DOUBLE) AS e, format('{}', 1e16::DOUBLE) AS f, format('{}', 2.0::DOUBLE) AS g, format('{}', true) AS h, format('{{}} {}', block_number) AS i FROM transfer ORDER BY block_number LIMIT 1",
    "SELECT to_json(struct_pack(\"a\" := block_number, b := \"from\", c := enabled)) AS j FROM transfer ORDER BY block_number, log_index",
    "SELECT to_json(list(s)) AS j FROM (SELECT struct_pack(k := log_index, v := value) AS s FROM transfer ORDER BY block_number DESC, log_index LIMIT 5) t",
    "SELECT b.block_number, coalesce((SELECT to_json(list(t.s)) FROM (SELECT struct_pack(\"id\" := c.\"to\", \"n\" := c.log_index) AS s FROM transfer c WHERE c.\"from\" = b.\"from\" ORDER BY c.block_number DESC, c.log_index LIMIT 3) t), '[]') AS j FROM transfer b ORDER BY b.block_number, b.log_index",
    "SELECT coalesce((SELECT to_json(list(t.s)) FROM (SELECT struct_pack(\"id\" := c.\"to\") AS s FROM transfer c WHERE c.block_number < 0) t), '[]') AS j",
    "SELECT to_json('a\"b\\c/' || chr(10) || chr(9) || chr(1) || chr(31) || chr(127) || 'é€😀') AS j",
    "SELECT to_json(CAST(value AS HUGEINT)) AS a, to_json(block_number) AS b, to_json(\"from\") AS c FROM transfer ORDER BY block_number, log_index LIMIT 3",
    "SELECT to_json(NULL) AS a, to_json(struct_pack(x := NULL, y := 1)) AS b, to_json([1, NULL, 3]) AS c, to_json(true) AS d, to_json(struct_pack(\"we\"\"ird\" := 'v')) AS e",
    "SELECT to_json(list(x)) AS j FROM (SELECT 1 AS x WHERE false) t",
    "SELECT b.block_number, b.log_index, (SELECT string_agg(t.v, ',') FROM (SELECT c.value AS v FROM transfer c WHERE b.\"from\" = c.\"from\" AND c.log_index >= 0 ORDER BY c.value DESC, c.log_index LIMIT 2 OFFSET 1) t) AS j FROM transfer b ORDER BY 1, 2",
    "SELECT b.block_number, (SELECT to_json(list(t.s)) FROM (SELECT struct_pack(\"id\" := c.\"to\") AS s FROM transfer c WHERE c.\"from\" = b.\"from\" ORDER BY c.block_number LIMIT 0) t) AS j FROM transfer b ORDER BY b.block_number, b.log_index",
    "SELECT count(*) FROM transfer",
    "SELECT sum(value_dec) FROM transfer",
    // 10^38 does not fit the _dec column; -10^38+5 does. The sum is the column's one value.
    "SELECT sum(value_dec) FROM wide",
    "SELECT sum(block_number) FROM transfer",
    "SELECT avg(block_number) FROM transfer",
    "SELECT avg(block_number - log_index) AS a, avg(CAST(value AS HUGEINT)) AS b, avg(CAST(\"tokensRewards\" AS DECIMAL(10,2))) AS c, avg(greatest(18, log_index)) AS d FROM transfer",
    "SELECT \"from\", avg(block_number) AS a FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT block_number, avg(log_index) OVER (ORDER BY block_number, log_index) AS a FROM transfer ORDER BY 1, 2",
    "SELECT block_number / 100 AS q FROM transfer ORDER BY 1",
    "SELECT block_number // 100 AS b, count(*) AS n FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT CAST(value AS HUGEINT) * 2 AS v FROM transfer ORDER BY 1",
    "SELECT -CAST(value AS HUGEINT) AS v FROM transfer ORDER BY 1",
    "SELECT \"from\", sum(CAST(value AS HUGEINT)) AS s FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT 7 / 2 AS a, 7 // 2 AS b, -7 // 2 AS c, 7 % -2 AS d, 1 / 0 AS e, 1 // 0 AS f, 0.5 / 2 AS g",
    "SELECT to_timestamp(block_timestamp) AS t FROM transfer ORDER BY 1 LIMIT 2",
    "SELECT date_trunc('day', to_timestamp(block_timestamp)) AS d, count(*) AS n FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT extract(year FROM to_timestamp(block_timestamp)) AS y FROM transfer LIMIT 1",
    "SELECT round(CAST(value AS DOUBLE) / 1e18, 4) AS r FROM transfer ORDER BY 1",
    "SELECT lower(\"to\") AS t FROM transfer ORDER BY 1",
    "SELECT count(*) AS n FROM transfer WHERE enabled = 'true'",
    "SELECT count(*) AS n FROM transfer WHERE value = 10",
    "SELECT count(*) AS n FROM transfer WHERE value IN (10, 4)",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" = 10",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" IN (5, 7, 10)",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" > 9",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" BETWEEN 1 AND 9",
    "SELECT count(*) AS n FROM transfer WHERE 9 < \"tokensRewards\"",
    "SELECT count(*) AS n FROM transfer WHERE CAST(\"tokensRewards\" AS INTEGER) > 9",
    "SELECT count(*) AS n FROM transfer WHERE enabled = true",
    "SELECT count(*) AS n FROM transfer WHERE enabled AND true",
    "SELECT count(*) AS n FROM transfer WHERE NOT enabled",
    "SELECT \"tokensRewards\" <> 5 AS ne FROM transfer ORDER BY 1",
    "SELECT CAST(value AS HUGEINT), block_number // 2, NOT (block_number > 2) FROM transfer ORDER BY 2",
    "SELECT \"Value\", VALUE, \"TOKENSREWARDS\", t.\"FROM\", \"Value\" || 'x', sum(Block_Number) OVER () FROM Transfer t ORDER BY 1",
    "SELECT count(*) FROM TRANSFER T WHERE t.ENABLED = 'true'",
    "SELECT table_name FROM information_schema.tables WHERE NOT starts_with(table_name, '__hot_') ORDER BY table_name",
    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'transfer' ORDER BY ordinal_position",
    "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables ORDER BY 3",
    "SELECT column_name, ordinal_position, is_nullable FROM information_schema.columns WHERE table_name = 'label' ORDER BY 2",
    "SELECT *, value_dec, \"from\" FROM transfer ORDER BY block_number, log_index",
    "SELECT 1 AS a, 2 AS a, block_number AS a FROM transfer ORDER BY 3",
    "SELECT block_number - 1 AS a, COALESCE(block_number - log_index, 0) AS b, CASE WHEN log_index = 0 THEN 0 ELSE block_number END AS c, block_number * 2 + 1 AS d FROM transfer ORDER BY 1",
    "SELECT t.\"from\", CAST(t.block_number / 10 AS BIGINT) AS block_number FROM transfer t ORDER BY t.block_number DESC",
    "SELECT CAST(0.5::DOUBLE AS BIGINT) a, CAST(1.5::DOUBLE AS BIGINT) b, CAST(2.5::DOUBLE AS BIGINT) c, CAST(-0.5::DOUBLE AS BIGINT) d, CAST(-1.5::DOUBLE AS BIGINT) e, CAST(0.6::DOUBLE AS BIGINT) f, CAST(2.5::DECIMAL(3,1) AS BIGINT) l, CAST(-2.5::DECIMAL(3,1) AS BIGINT) m, CAST(2.4::DECIMAL(3,1) AS BIGINT) n",
    "SELECT CAST(CAST(value AS DOUBLE) / 3 AS BIGINT) AS q, CAST(block_number / 4 AS INTEGER) AS r FROM transfer WHERE block_number < 100 ORDER BY 1, 2",
    "SELECT k FROM (SELECT block_number * 100000 + log_index AS k FROM transfer UNION ALL SELECT block_number * 100000 + log_index FROM transfer) ORDER BY 1 LIMIT 3",
    "SELECT CAST('0x1F' AS BIGINT) a, CAST(' 0x1f' AS BIGINT) b, CAST('0b101' AS BIGINT) c, CAST('0x1_f' AS INTEGER) d, CAST('0x7fffffffffffffff' AS BIGINT) e, CAST('42' AS BIGINT) f",
    "SELECT CAST('0xffffffffffffffff' AS BIGINT)",
    "SELECT CAST('-0x1f' AS BIGINT)",
    "SELECT decode(from_hex('68c3a96c6c6f')) a, decode(from_hex('')) b, from_hex('abc') c, from_hex('4142') d",
    "SELECT decode(from_hex('ff'))",
    "SELECT from_hex('zz')",
    "SELECT decode(unhex('68c3a96c6c6f')) a, unhex('4142') b, TRY(unhex('zz')) IS NULL c",
    "SELECT typeof(CAST(1 AS INTEGER)) a, typeof('x') b, typeof(1.5) c, typeof(TRUE) d, typeof(CAST(1 AS BIGINT)) e, typeof(DATE '2024-01-01') f, typeof(NULL) g, typeof(CAST(1 AS DOUBLE)) h, typeof(CAST(1 AS DECIMAL(38,0))) i",
    "SELECT typeof(v) AS t, count(*) AS n FROM (VALUES (CAST(1 AS BIGINT)), (2)) x(v) GROUP BY 1",
    "SELECT typeof(block_number) a, typeof(\"value\") b, typeof(to_timestamp(block_timestamp)) c, typeof(count(*)) d FROM transfer GROUP BY ALL ORDER BY ALL LIMIT 1",
    "SELECT typeof(1) AS a",
    "SELECT k, count(DISTINCT (a, b)) AS n, count(DISTINCT a) AS m FROM (VALUES (1, 1, 'x'), (1, 1, 'x'), (1, 1, NULL), (1, NULL, NULL), (1, NULL, ''), (2, 2, 'y'), (2, 3, 'y')) t(k, a, b) GROUP BY k ORDER BY k",
    "SELECT DATE '1970-01-01' + CAST(d AS INTEGER) AS day FROM range(106750, 106755) t(d) ORDER BY 1",
    "SELECT DATE '2024-03-01' - 1 AS a, 5 + DATE '2024-02-27' AS b, DATE '1970-01-01' + 115739 AS c, DATE '2024-03-01' + CAST(-366 AS INTEGER) AS d",
    "SELECT h, TRY(CAST(decode(unhex(substr(h, 3))) AS VARCHAR)) AS s FROM (VALUES ('0x6162'), ('0xzz'), ('0xff'), ('0x'), (NULL)) t(h) ORDER BY h",
    "SELECT h, count(TRY(CAST(decode(unhex(substr(h, 1))) AS VARCHAR))) OVER () AS n FROM (VALUES ('6162'), ('zz'), ('ff')) t(h) ORDER BY h",
    "SELECT sum(length(TRY(CAST(decode(unhex(substr(h, 3))) AS VARCHAR)))) AS s, count(*) AS n FROM (VALUES ('0x6162'), ('0xzz'), ('0xff')) t(h)",
    "SELECT count(*) n, min(day) lo, max(day) hi FROM (SELECT DATE '1970-01-01' + CAST(d AS INTEGER) AS day FROM range(11575, 115740) t(d)) x",
    "SELECT DATE '2024-03-01' + 2147483647",
    "SELECT CAST(('0x' || substr('000000000000000000000000000000000000000000000000000000000000002a', 49, 16)) AS BIGINT) AS n",
    "SELECT max(\"from\") AS m FROM transfer",
    "SELECT (block_number, log_index) > (2, 0) AS gt FROM transfer ORDER BY block_number, log_index",
    "SELECT CAST(block_number AS UBIGINT) AS b FROM transfer ORDER BY 1 LIMIT 1",
    "SELECT \"tokensRewards\" AS r FROM transfer ORDER BY 1",
    "SELECT t.\"to\", l.name FROM transfer t LEFT JOIN label l ON l.addr = t.\"to\" ORDER BY 1, 2",
    "SELECT count(DISTINCT \"from\") AS n FROM transfer",
    "SELECT \"from\" || ':' || CAST(block_number AS VARCHAR) AS k FROM transfer ORDER BY 1",
    "SELECT 1 = true AS a, 2 = true AS b, 0 = false AS c, 1 < true AS d, 2 > false AS e, 1.5 = true AS f",
    "SELECT block_number FROM transfer WHERE (block_number = 1) = true OR log_index = true ORDER BY 1",
    "SELECT true IN (1, 2) AS a, 1 IN (true, false) AS b, 2 IN (true) AS c",
    "SELECT 1 IS DISTINCT FROM 2 AND 3 IS DISTINCT FROM 3 OR 4 IS NOT DISTINCT FROM 4 AS v",
    "SELECT block_number FROM transfer WHERE \"from\" IS DISTINCT FROM '0xa' AND log_index = 0 OR \"to\" IS NOT DISTINCT FROM '0xe' ORDER BY 1",
    "SELECT CAST(2.5::DOUBLE AS HUGEINT) AS a, CAST(-3.5::DOUBLE AS HUGEINT) AS b, CAST(2.5::DOUBLE AS DECIMAL(38,0)) AS c",
    // #44: TRY_CAST to HUGEINT of 10^38: refused where its value is read, answered where it is tested or summed.
    "SELECT TRY_CAST('100000000000000000000000000000000000000' AS HUGEINT) AS a",
    "SELECT value, TRY_CAST(value AS HUGEINT) IS NULL AS a, TRY_CAST(value AS HUGEINT) IS NOT NULL AS b, TRY_CAST(value AS DECIMAL(38,0)) AS c FROM wide ORDER BY 1",
    "SELECT sum(TRY_CAST(value AS HUGEINT)) AS s, (SELECT sum(d) FROM (SELECT TRY_CAST(value AS HUGEINT) AS d FROM wide) t) AS t FROM wide",
    // #63: the same band from a float, which DuckDB answers below 2^127 in magnitude. DECIMAL never reaches it.
    "SELECT TRY_CAST(1.5e38::DOUBLE AS HUGEINT) AS a",
    "SELECT TRY_CAST(1e38::DOUBLE AS HUGEINT) AS a, TRY_CAST(1.7014118346046923e38::DOUBLE AS HUGEINT) AS b, TRY_CAST(-1.7014118346046923e38::DOUBLE AS HUGEINT) AS c, TRY_CAST('nan'::DOUBLE AS HUGEINT) AS d, TRY_CAST(2.5::DOUBLE AS HUGEINT) AS e, TRY_CAST(-2.5::FLOAT AS HUGEINT) AS f",
    "SELECT value, TRY_CAST(CAST(value AS DOUBLE) * 1.5 AS HUGEINT) IS NULL AS a, TRY_CAST(CAST(value AS FLOAT) * 1.5 AS HUGEINT) IS NOT NULL AS b, TRY_CAST(CAST(value AS DOUBLE) * 2 AS HUGEINT) IS NULL AS c FROM wide ORDER BY 1",
    "SELECT sum(TRY_CAST(CAST(value AS DOUBLE) * 1.5 AS HUGEINT)) AS s, (SELECT sum(d) FROM (SELECT TRY_CAST(CAST(value AS FLOAT) * 1.5 AS HUGEINT) AS d FROM wide) t) AS t FROM wide",
    "SELECT sum(TRY_CAST(x AS HUGEINT)) AS s FROM (VALUES (1.5e38::DOUBLE), (-1.5e38::DOUBLE), (2.5::DOUBLE), (1.7014118346046923e38::DOUBLE), ('nan'::DOUBLE)) t(x)",
    "SELECT TRY_CAST(CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) AS HUGEINT) AS a, TRY_CAST(CAST('-9999999999999999999999999999999999999.5' AS DECIMAL(38,1)) AS HUGEINT) AS b, TRY_CAST(2.5::DECIMAL(10,1) AS HUGEINT) AS c, TRY_CAST(-2.5::DECIMAL(10,1) AS HUGEINT) AS d",
    "SELECT CAST(CAST('47582028310819253533' AS HUGEINT) AS DOUBLE) AS a, CAST(CAST('9791626625542365.709860864' AS DECIMAL(38,9)) AS DOUBLE) AS b",
    "SELECT CAST(value AS HUGEINT)::DOUBLE / 3 AS d FROM transfer ORDER BY 1",
    "SELECT 1.5 AS a, 1.5 + 1 AS b, 1.5 * 2.25 AS c, 0.5 AS d, -2.50 AS e, 100.0 / 3 AS f, 1e3 AS g, 1.5e2 AS h, 0.001 AS i",
    "SELECT 1.5 - 0.25 AS a, 10.5 % 3 AS b, block_number * 1.5 AS c, block_number + 0.25 AS d FROM transfer ORDER BY 1, 3",
    "SELECT round(2.345, 2) AS a, CAST(1.25 AS DOUBLE) AS b, 1.10 = 1.1 AS c, 12345678901234567890.5 AS d",
    "SELECT round(9.995, 2) AS a, round(-2.345, 2) AS b, round(2.345) AS c, round(2.345, 5) AS d, round(1234.5, -2) AS e, round(2.5) AS f, round(-2.5) AS g",
    "SELECT round(CAST(value AS HUGEINT) / 7, 3) AS r FROM transfer ORDER BY 1",
    "SELECT * FROM (SELECT block_number, block_number FROM transfer) ORDER BY 1",
    "SELECT count(*) AS n FROM (SELECT t.\"to\", l.addr AS \"to\" FROM transfer t JOIN label l ON l.addr = t.\"to\")",
    "SELECT * FROM (SELECT t.\"from\", l.* FROM transfer t JOIN label l ON l.addr = t.\"from\") ORDER BY 1, 2, 3",
    "WITH x AS (SELECT block_number, block_number + 0 FROM transfer) SELECT * FROM x ORDER BY 1",
    "SELECT * FROM (SELECT l.addr, l.* FROM label l) ORDER BY 1",
    "SELECT block_number FROM (SELECT block_number, block_number FROM transfer) ORDER BY 1",
    "SELECT * FROM (SELECT * FROM label a JOIN label b ON a.addr = b.addr) ORDER BY 1",
    "SELECT TRY_CAST('0xa' AS BIGINT) a, TRY_CAST('0XA' AS BIGINT) b, TRY_CAST('0xzz' AS BIGINT) c, TRY_CAST(' 12 ' AS BIGINT) d, TRY_CAST('abc' AS BIGINT) e, TRY_CAST('0xffffffffffffffff' AS BIGINT) f, TRY_CAST('1.5' AS BIGINT) g, TRY_CAST('' AS BIGINT) h",
    "SELECT CAST(' 12 ' AS BIGINT) d, CAST('1.5' AS BIGINT) g, CAST('-2.5' AS BIGINT) i, CAST('1e2' AS BIGINT) j, CAST('+7' AS BIGINT) k",
    "SELECT TRY_CAST(\"from\" AS BIGINT) AS f, TRY_CAST(value AS INTEGER) AS v FROM transfer ORDER BY block_number, log_index",
    "SELECT f, sum(v) AS s, count(*) AS n FROM (SELECT \"to\" AS f, TRY_CAST(lower(\"from\") AS BIGINT) AS v FROM transfer) GROUP BY f ORDER BY 1",
    "SELECT sum(TRY_CAST(\"from\" AS BIGINT)) AS a, sum(TRY_CAST(value AS HUGEINT)) AS b, sum(TRY_CAST(enabled AS BIGINT)) AS c FROM transfer",
    "SELECT block_number // 2 AS a, block_number // log_index AS b, greatest(block_number, 82) AS c, least(log_index, 1) AS d FROM transfer ORDER BY block_number, log_index",
    "SELECT block_number AS x FROM transfer INTERSECT SELECT 3 ORDER BY 1",
    "SELECT CAST(block_number AS BIGINT) - 10 AS x FROM transfer EXCEPT SELECT log_index FROM transfer ORDER BY 1",
    "SELECT block_number AS x FROM transfer EXCEPT ALL SELECT log_index FROM transfer ORDER BY 1",
    "SELECT block_number AS x FROM transfer UNION SELECT -1 ORDER BY 1",
    "SELECT \"to\" AS x FROM transfer INTERSECT ALL SELECT \"from\" FROM transfer ORDER BY 1",
    "SELECT \"to\" AS x, log_index AS y FROM transfer EXCEPT ALL SELECT \"from\", 0 FROM transfer ORDER BY 1, 2",
    "SELECT n AS x FROM (VALUES (1), (1), (1), (NULL), (NULL), (2)) t(n) INTERSECT ALL SELECT n FROM (VALUES (1), (1), (NULL), (3)) u(n) ORDER BY 1",
    "SELECT n AS x FROM (VALUES (1), (1), (1), (NULL), (NULL), (2)) t(n) EXCEPT ALL SELECT n FROM (VALUES (1), (NULL), (3)) u(n) ORDER BY 1",
    "SELECT DISTINCT \"to\" AS x FROM transfer EXCEPT ALL SELECT \"from\" FROM transfer ORDER BY 1",
    "SELECT 32 AS x FROM transfer EXCEPT ALL SELECT CAST(log_index AS BIGINT) * block_number FROM transfer ORDER BY 1",
    "SELECT log_index AS x FROM transfer EXCEPT SELECT greatest(log_index, CAST(block_number AS BIGINT)) FROM transfer ORDER BY 1",
    "SELECT log_index AS x FROM transfer INTERSECT SELECT 1.5 ORDER BY 1",
    "SELECT log_index FROM transfer WHERE CASE WHEN block_number < -16 THEN 48 ELSE log_index END < (block_number - 95)",
    "SELECT \"to\" AS t FROM transfer ORDER BY 1 DESC LIMIT 3",
    "SELECT n FROM (VALUES (1), (NULL), (2)) t(n) ORDER BY n DESC",
    "SELECT string_agg(n::VARCHAR, ',' ORDER BY n DESC) AS a FROM (VALUES (1), (NULL), (2)) t(n)",
    "SELECT CAST(to_timestamp(block_timestamp) AS VARCHAR) AS a, to_timestamp(block_timestamp) || '|' AS b FROM transfer ORDER BY 1",
    "SELECT year(to_timestamp(block_timestamp)) AS y, month(to_timestamp(block_timestamp)) AS m, dayofweek(to_timestamp(block_timestamp)) AS w, epoch(to_timestamp(block_timestamp)) AS e, hour(to_timestamp(block_timestamp)) AS h FROM transfer ORDER BY 1, 2, 3, 4",
    "SELECT strftime(to_timestamp(block_timestamp), '%Y-%m-%d %H:%M|%b %a %j') AS s, date_diff('day', to_timestamp(1700000000), to_timestamp(block_timestamp)) AS d, datediff('month', to_timestamp(1700000000), to_timestamp(block_timestamp)) AS m FROM transfer ORDER BY 1",
    "SELECT regexp_extract(value, '[0-9]{2}') AS a, regexp_matches(\"from\", '(?i)0XA') AS b, regexp_replace(value, '0', 'z', 'g') AS c, regexp_replace(value, '(1)', '\\1\\1') AS d, regexp_replace(value, '^1', '\\2') AS e FROM transfer ORDER BY block_number, log_index",
    "SELECT sign(CAST(block_number AS BIGINT) - 3) AS a, sign(-2.5) AS b, NULLIF(block_number, 2) AS c FROM transfer ORDER BY block_number, log_index",
    "SELECT 1 AS a ORDER BY 'x'",
    "SELECT CAST(3 AS BIGINT) - CAST(5 AS UBIGINT) AS d, CAST(3 AS UBIGINT) * CAST(-5 AS BIGINT) AS m, CAST(3 AS INTEGER) + CAST(5 AS UINTEGER) AS e",
    "SELECT CAST(log_index AS BIGINT) - block_number AS d FROM transfer ORDER BY block_number, log_index",
    "SELECT avg(CAST(x AS UBIGINT) * 2982776736) AS a, sum(CAST(x AS UBIGINT) * 2982776736) AS s FROM range(1000000000, 1000000063) t(x)",
    "SELECT max(CAST(x AS UBIGINT)), max(CAST(x AS BIGINT)), max(x), max(x) + 1 AS d FROM range(1, 6) t(x)",
    "SELECT x % 2 AS k, sum(CAST(x AS UBIGINT)) AS a FROM range(1, 6) t(x) GROUP BY 1 HAVING sum(CAST(x AS BIGINT)) > 4 ORDER BY sum(x) DESC",
    "SELECT avg(CAST(x AS UBIGINT) * 2982776736) AS a, avg(CAST(x AS BIGINT) * 2982776736) AS b FROM range(1000000000, 1000000063) t(x)",
    "SELECT epoch(to_timestamp(ts)) = COALESCE(ts, w) AS eq, 1.5::DOUBLE < CAST(2 AS HUGEINT) AS lt FROM (VALUES (CAST(1700000000 AS UBIGINT), CAST(3 AS BIGINT))) t(ts, w)",
    "SELECT extract(minute FROM to_timestamp(ts)) AS x FROM (VALUES (CAST(1700000040 AS UBIGINT)), (CAST(1700000100 AS UBIGINT)), (CAST(1700000160 AS UBIGINT))) t(ts) EXCEPT ALL SELECT abs(a) FROM (VALUES (CAST(1 AS BIGINT)), (CAST(-2 AS BIGINT))) u(a) ORDER BY 1",
    "SELECT CASE WHEN true THEN extract(minute FROM to_timestamp(ts)) ELSE (w - ts) END AS x FROM (VALUES (CAST(1700000040 AS UBIGINT), CAST(3 AS BIGINT))) t(ts, w) EXCEPT ALL SELECT abs(a) FROM (VALUES (CAST(1 AS BIGINT))) u(a) ORDER BY 1",
    "SELECT \"from\", arg_max(block_number, block_timestamp * 10 + log_index) AS a, arg_min(\"to\", block_timestamp * 10 + log_index) AS b, max_by(value, block_timestamp * 10 + log_index) AS c, min_by(value, block_timestamp * 10 + log_index) FILTER (WHERE value <> '4') AS d FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT arg_max(x, y) AS a, arg_min(x, y) AS b FROM (VALUES (1, 5), (NULL, 9), (3, NULL), (4, 6)) t(x, y)",
    "SELECT \"from\", last(value ORDER BY block_number, log_index) AS a, first(value ORDER BY block_number, log_index) AS b, last(\"to\" ORDER BY block_number, log_index) FILTER (WHERE value <> '4') AS c FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT last(x ORDER BY y) AS a, first(x ORDER BY y) AS b, last(x ORDER BY y) FILTER (WHERE x IS NOT NULL) AS c FROM (VALUES (1, 5), (NULL, 9), (3, 2), (4, 6)) t(x, y)",
    "SELECT CAST(sum(CAST(value AS BIGNUM)) AS VARCHAR) AS s, CAST(CAST(0 AS BIGNUM) - CAST(value AS BIGNUM) AS VARCHAR) AS neg FROM transfer GROUP BY value ORDER BY value",
    "SELECT hex(10) AS a, hex(255) AS b, hex(0) AS c, lpad(hex(block_number & 255), 2, '0') AS d, regexp_full_match(\"from\", '0x[0-9a-f]+') AS e, regexp_full_match(\"from\", '0x') AS f FROM transfer ORDER BY block_number, log_index",
    "WITH RECURSIVE f(a, b, c, d) AS (SELECT DISTINCT 1, CAST(0 AS BIGNUM), CAST(0 AS BIGNUM), 0 UNION ALL SELECT a + 1, b, c, d FROM f WHERE a < 3) SELECT a, CAST(b AS VARCHAR) AS b, d FROM f ORDER BY a",
    "WITH c(id, b) AS (VALUES (1, 10), (1, 10), (1, 30), (2, 10), (3, 50)), d(id, b, v) AS (VALUES (1, 5, 'early'), (1, 20, 'mid'), (1, 40, 'late'), (2, 15, 'two'), (3, 40, 'before')) SELECT c.id, c.b, (SELECT d.v FROM d WHERE d.id = c.id AND d.b > c.b ORDER BY d.b DESC LIMIT 1) AS v FROM c ORDER BY c.id, c.b",
    "WITH c(id, b) AS (VALUES (1, 10), (1, 10), (1, 30), (2, 10), (3, 50)), d(id, b, v) AS (VALUES (1, 5, 'early'), (1, 20, 'mid'), (1, 40, 'late'), (2, 15, 'two'), (3, 40, 'before')) SELECT c.id, c.b, x.v FROM c LEFT JOIN LATERAL (SELECT d.v FROM d WHERE d.id = c.id AND d.b > c.b ORDER BY d.b ASC LIMIT 1) x ON true ORDER BY c.id, c.b",
    "WITH c(id, b) AS (VALUES (1, 10), (1, 10), (1, 30), (2, 10), (3, 50)), d(id, b, v) AS (VALUES (1, 5, 'early'), (1, 20, 'mid'), (1, 40, 'late'), (2, 15, 'two'), (3, 40, 'before')) SELECT c.id, c.b, x.v FROM c CROSS JOIN LATERAL (SELECT d.v, d.b FROM d WHERE d.id = c.id AND d.b > c.b ORDER BY d.b DESC LIMIT 2) x ORDER BY c.id, c.b, x.b",
    "WITH c(id, b) AS (VALUES (1, 10), (1, 10), (1, 30), (2, 10), (3, 50)) SELECT c.id, c.b, r.x, r.y FROM c CROSS JOIN LATERAL (SELECT CASE WHEN c.b > 20 THEN c.b * 2 ELSE 0 END AS x, c.id + 100 AS y) r ORDER BY c.id, c.b",
    "WITH s AS (SELECT true AS p WHERE false) SELECT coalesce((SELECT p FROM s), false) AS x, (SELECT p FROM s) IS NULL AS y",
    "SELECT log_index >> 8 AS a, log_index >> 64 AS b, (log_index >> 1) & 255 AS d, log_index << 3 AS e FROM transfer ORDER BY 1",
    "SELECT CAST(-1000 AS BIGINT) >> 70 AS a, CAST(-1000 AS BIGINT) >> 3 AS b, CAST(1000 AS INTEGER) >> 40 AS c, CAST(1 AS UBIGINT) << 63 AS d",
    "SELECT CAST(1 AS BIGINT) << 63 AS a",
    "SELECT CAST(1 AS INTEGER) << 31 AS a",
    "SELECT CAST(-1 AS BIGINT) << 2 AS a",
    "SELECT CAST(3 AS BIGINT) << -1 AS a",
    "WITH pool(id, liquidity) AS (VALUES ('0xaaa', 5), ('0xbbb', 6), ('0xbbb', 8)), swap(id, pool) AS (VALUES ('s1', '0xaaa'), ('s2', '0xaaa'), ('s5', '0xccc')) SELECT c.id, (SELECT p.liquidity FROM pool p WHERE p.id = c.pool) AS l FROM swap c ORDER BY 1",
    "WITH pool(id, liquidity) AS (VALUES ('0xaaa', 5), ('0xbbb', 6), ('0xbbb', 8)), swap(id, pool) AS (VALUES ('s1', '0xaaa'), ('s4', '0xbbb')) SELECT c.id, (SELECT p.liquidity FROM pool p WHERE p.id = c.pool) AS l FROM swap c ORDER BY 1",
    "WITH pool(id, liquidity) AS (VALUES ('0xaaa', 5), ('0xbbb', 6)), swap(id, pool) AS (VALUES ('s1', '0xaaa'), ('s2', '0xaaa'), ('s3', '0xaaa'), ('s4', '0xbbb')) SELECT b.id, (SELECT to_json(list(t.s)) FROM (SELECT struct_pack(\"id\" := c.id, \"pool\" := (SELECT struct_pack(\"id\" := p.id, \"liquidity\" := p.liquidity) FROM pool p WHERE p.id = c.pool)) AS s FROM swap c WHERE c.pool = b.id ORDER BY c.id LIMIT 2 OFFSET 1) t) AS l FROM pool b ORDER BY 1",
    "SELECT \"from\", coalesce((SELECT true FROM label WHERE addr = 'nobody'), false) AS x FROM transfer ORDER BY 1 LIMIT 2",
    "WITH RECURSIVE t(n, s) AS MATERIALIZED (SELECT 1, CAST('1' AS BIGNUM) UNION ALL SELECT n + 1, s + CAST('1000000000000000000' AS BIGNUM) FROM t WHERE n < 4) SELECT n, CAST(s AS VARCHAR) AS s FROM t ORDER BY n",
    "SELECT json_extract_string('{\"a\": {\"b\": 7}, \"c\": [1, 2]}', '$.a.b') AS a, json_extract('{\"a\": {\"b\": 7}}', '$.a') AS b, json_extract_string('{\"c\": [1, 2]}', '$.c[1]') AS c, json_extract_string('{\"a\": 1}', '$.zz') AS d",
    "SELECT '{\"a\": 1, \"b\": \"x\"}'->>'b' AS a, '{\"a\": {\"k\": 2}}'->'a' AS b, '{\"a\": {\"k\": 2}}'->>'$.a.k' AS c",
    "SELECT json_type('{\"a\": 1}') AS a, json_type('[1]') AS b, json_type('1.5') AS c, json_type('\"x\"') AS d, json_type('1') AS e, json_type('-1') AS f, json_type('0') AS g",
    "SELECT json_extract('{\"a\": [1, {\"b\": null}], \"c\": \"x\", \"d\": 1.50, \"e\": 12345678901234567890}', '$.a[1]') a, json_extract_string('{\"a\": {\"b\": null}}', '$.a.b') b, json_extract_string('{\"c\": \"x\"}', 'c') c, json_extract('[10, 20]', 1) d, json_extract_string('{\"d\": 1.50}', '$.d') e, json_extract('{\"e\": 12345678901234567890}', '$.e') f, json_type('{\"e\": 12345678901234567890}', '$.e') g, json_type('{\"x\": -1}', '$.x') h, json_type('null') i, json_type('true') j, json_extract('{\"a b\": 1}', '$.\"a b\"') k, json_extract('[1,2,3]', '$[#-1]') l, json_extract('{\"a\":1}', '$.z') m",
    "SELECT '[1,2]'->1 AS v, '{\"a\": {\"b\": 3}}'->'a'->>'b' AS w, json_extract_string(NULL, '$.a') AS x",
    "SELECT json_extract('not json', '$.a')",
    "SELECT from_json('{\"a\": 1, \"b\": \"x\"}', '{\"a\": \"INTEGER\", \"b\": \"VARCHAR\"}').b AS b",
    "SELECT s.a, s.b, s.c, s.d, s.e FROM (SELECT from_json('{\"a\": \"5\", \"b\": 7, \"c\": 2.5, \"d\": true}', '{\"a\": \"INTEGER\", \"b\": \"VARCHAR\", \"c\": \"DOUBLE\", \"d\": \"BOOLEAN\", \"e\": \"VARCHAR\"}') AS s)",
    "SELECT from_json('{\"a\": \"x\"}', '{\"a\": \"INTEGER\"}').a AS a, from_json('{\"A\": 1}', '{\"a\": \"INTEGER\"}').a AS c, from_json('{\"n\": {\"k\": \"v\"}}', '{\"n\": {\"k\": \"VARCHAR\"}}').n.k AS k, len(from_json('{\"a\": [1, 2]}', '{\"a\": [\"BIGINT\"]}').a) AS n",
    "SELECT TRY(CAST('x' AS INTEGER)) AS a, TRY(1 // 0) AS b, TRY(CAST('7' AS INTEGER)) AS c",
    "SELECT j, TRY(json_extract_string(j, '$.a')) AS a FROM (VALUES ('{\"a\": 1}'), ('bad'), (NULL), ('{\"a\": \"x\"}')) t(j) ORDER BY 1",
    "SELECT TRY(from_hex(h)) IS NULL AS failed FROM (VALUES ('4142'), ('zz'), ('00')) t(h) ORDER BY h",
    "SELECT \"from\", block_number FROM transfer QUALIFY row_number() OVER (PARTITION BY \"from\" ORDER BY block_number DESC, log_index DESC) = 1 ORDER BY 1",
    "SELECT block_number, sum(log_index) OVER w AS s FROM transfer WINDOW w AS (ORDER BY block_number, log_index) ORDER BY 1, 2",
    "SELECT \"from\", block_number, log_index, last_value(CASE WHEN log_index % 3 = 0 THEN block_number END IGNORE NULLS) OVER (PARTITION BY \"from\" ORDER BY block_number, log_index ROWS UNBOUNDED PRECEDING) AS l FROM transfer ORDER BY 1, 2, 3",
    "SELECT block_number, log_index, log_index FROM transfer UNION ALL SELECT block_number, block_number, log_index FROM transfer ORDER BY 1, 2, 3",
    "SELECT count(*), count(DISTINCT b) FROM (SELECT block_number AS a, log_index AS b FROM transfer UNION ALL SELECT block_number, block_number FROM transfer) s",
    "SELECT \"from\", block_number, log_index, first_value(CASE WHEN log_index % 3 = 1 THEN log_index END IGNORE NULLS) OVER (PARTITION BY \"from\" ORDER BY block_number, log_index) AS f FROM transfer ORDER BY 1, 2, 3",
    "SELECT range AS r FROM range(3) ORDER BY 1",
    "SELECT * FROM range(2, 11, 4) ORDER BY 1",
    "SELECT * FROM generate_series(5, 1, -2) ORDER BY 1 DESC",
    "SELECT count(*) AS n FROM range(0)",
    "SELECT r.x, t.block_number FROM range(1, 3) r(x) JOIN transfer t ON t.block_number = r.x ORDER BY 1, 2",
    "SELECT * FROM generate_series(1, 3) t(x) ORDER BY 1",
    "SELECT unnest([1, 2, 3]) AS u",
    "SELECT x FROM (VALUES (1), (2), (3)) t(x) WHERE x = 3 OR x NOT IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) ORDER BY 1",
    "SELECT x FROM (VALUES (1), (2), (3)) t(x) WHERE CASE WHEN x NOT IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) THEN true ELSE x = 3 END ORDER BY 1",
    "SELECT x FROM (VALUES (1), (2), (3)) t(x) WHERE x = 3 OR x IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) ORDER BY 1",
    "SELECT x FROM (VALUES (1), (2), (3)) t(x) WHERE x NOT IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) ORDER BY 1",
    "SELECT (SELECT count(DISTINCT addr) FROM label) AS n_all, (SELECT count(DISTINCT addr) FROM label WHERE addr NOT IN (SELECT \"to\" AS addr FROM transfer WHERE log_index > 0)) AS n_active",
    "SELECT \"from\", \"from\" IN (SELECT \"to\" FROM transfer) AS bare_outer, t.\"from\" IN (SELECT \"to\" FROM transfer) AS qualified FROM transfer t ORDER BY block_number, log_index",
    "SELECT 7.5::DOUBLE // 2 AS a, -7.5::DOUBLE // 2 AS b, 7::DOUBLE // 0 AS c, CAST(7 AS BIGINT) // 2.0::DOUBLE AS f, 1.5 // 1 AS g, block_number // 2.5 AS h FROM transfer ORDER BY block_number, log_index",
    "SELECT \"from\", bool_and(enabled = 'true') AS a, count(value = '10') AS b, bool_or('10' = value) AS c, bool_or(\"from\" = \"to\") AS d FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT \"from\", count(DISTINCT CASE WHEN \"to\" <> \"from\" THEN \"to\" ELSE '0xz' END) AS d, count(*) AS n FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT k, count(*) AS n FROM (SELECT CASE WHEN \"to\" <> \"from\" THEN \"to\" ELSE '0xz' END AS k FROM transfer GROUP BY 1) GROUP BY 1 ORDER BY 1",
    "SELECT to_timestamp(64814395658) AS a, to_timestamp(1.5) AS b, to_timestamp(-1.5) AS c, CAST(to_timestamp(64814395658) AS VARCHAR) AS d",
    "SELECT x, x IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) AS a, x NOT IN (SELECT y FROM (VALUES (1), (NULL)) s(y)) AS b, x IN (SELECT y FROM (VALUES (1)) s(y)) AS c, x IN (SELECT y FROM (VALUES (1)) s(y) WHERE false) AS d FROM (VALUES (1), (2), (NULL)) t(x) ORDER BY x",
    "SELECT block_number, \"to\" IN (SELECT addr FROM label) AS labelled, EXISTS (SELECT 1 FROM label l WHERE l.addr = t.\"from\") AS known, NOT EXISTS (SELECT 1 FROM label l WHERE l.addr = t.\"to\" AND l.name = 'carol') AS not_carol FROM transfer t ORDER BY block_number, log_index",
    "SELECT \"from\", count(*) FILTER (WHERE true) AS n, bool_or(\"to\" IN (SELECT addr FROM label WHERE name <> 'alice')) AS any_label FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT CASE WHEN EXISTS (SELECT 1 FROM label l WHERE lower(l.addr) = lower(t.\"from\")) THEN 'known' ELSE 'unknown' END AS k FROM transfer t ORDER BY block_number, log_index",
    "SELECT block_number, log_index, (SELECT sum(length(name)) FROM label l WHERE lower(l.addr) = lower(t.\"from\") AND l.name <> 'x') AS n, (SELECT max(l.name) FROM label l WHERE upper(l.addr) = upper(t.\"to\")) AS m FROM transfer t ORDER BY 1, 2",
    "SELECT \"from\" FROM transfer t WHERE (SELECT count(*) FROM label l WHERE lower(l.addr) = lower(t.\"from\")) = 0 ORDER BY 1",
    "SELECT t.\"from\", bool_or(EXISTS (SELECT 1 FROM label x WHERE x.addr = t.\"to\")) AS a, sum(CASE WHEN t.\"to\" IN (SELECT addr FROM label) THEN 1 ELSE 0 END) AS s, count(*) AS n FROM transfer t JOIN label l ON lower(l.addr) = lower(t.\"from\") GROUP BY 1 ORDER BY 1",
    "SELECT t.block_number, t.log_index, \"to\" IN (SELECT \"to\" FROM transfer x WHERE x.log_index > 0) AS a, \"to\" NOT IN (SELECT \"to\" FROM transfer x WHERE x.block_number > 2) AS b FROM transfer t LEFT JOIN label l ON l.addr = t.\"from\" ORDER BY 1, 2",
    "SELECT block_number AS x FROM transfer t WHERE EXISTS (SELECT 1 FROM label l WHERE l.addr = t.\"from\") UNION ALL SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM label l WHERE l.addr = t.\"to\") THEN 1 ELSE 0 END FROM transfer t ORDER BY 1",
    "SELECT \"from\", sum(CASE WHEN \"to\" IN (SELECT addr FROM label) THEN 1 ELSE 0 END) AS to_labelled, count(*) FILTER (WHERE EXISTS (SELECT 1 FROM label l WHERE l.addr = transfer.\"from\")) AS from_labelled FROM transfer GROUP BY 1 HAVING bool_or(\"to\" NOT IN (SELECT addr FROM label)) IS NOT NULL ORDER BY 1",
    "SELECT year(to_timestamp(block_timestamp)) AS y FROM transfer WHERE 'bob' <> CAST(to_timestamp(block_timestamp) AS VARCHAR) ORDER BY 1",
    "SELECT date_trunc('day', CAST(to_timestamp(block_timestamp) AS DATE)) AS a, CAST(to_timestamp(block_timestamp) AS DATE) + INTERVAL 33 HOUR AS b, CAST(to_timestamp(block_timestamp) AS DATE) + 3 AS c FROM transfer ORDER BY 1, 2",
    "SELECT CASE WHEN log_index = 0 THEN CAST(value AS DECIMAL(20,2)) ELSE 2.5::DOUBLE END AS a, COALESCE(TRY_CAST(value AS HUGEINT), 0.5::DOUBLE) AS b FROM transfer ORDER BY block_number, log_index",
    "SELECT log_index FROM transfer WHERE block_number - 95 > 0",
    "SELECT block_number - 95 AS d FROM transfer",
    "SELECT length('héllo') AS a, char_length('ab') AS b, length(\"from\") AS c FROM transfer ORDER BY block_number, log_index",
    "SELECT count(*) AS n FROM (SELECT * FROM transfer x JOIN transfer y ON x.block_number = y.block_number)",
    "WITH j AS (SELECT * FROM label a JOIN label b ON a.addr = b.addr) SELECT addr_1, name_1 FROM j ORDER BY 1",
    "SELECT block_number FROM transfer WHERE block_number > 1.5 AND block_number < 150.0 ORDER BY 1",
    "SELECT sum(block_number * 0.5) AS s, avg(block_number) + 0.5 AS a FROM transfer",
    "SELECT list_reduce([1, 2, 3], lambda a, x: a * 10 + x) AS a, list_reduce([1.5, 2.25], lambda a, x: a + x) AS b",
    "SELECT \"from\", list_reduce(list(block_number ORDER BY block_number, log_index), lambda a, x: a * 1000 + x) AS r FROM transfer GROUP BY 1 ORDER BY 1",
    // DuckDB moves constants across a comparison before evaluating, so these do not overflow.
    "SELECT block_number, block_number - 95 > 0 AS a, 200 - block_number < 100 AS b, block_number * 2 = 300 AS c, block_number * 2 = 301 AS d, block_number * 2 <> 301 AS e, block_number + 5 = 3 AS f, block_number * -2 < -4 AS g FROM transfer ORDER BY 1, 2",
    "SELECT block_number, log_index, (log_index - 87) BETWEEN 10 AND 49 AS a, (block_number - 2) NOT BETWEEN 0 AND 100 AS b, (200 - block_number) BETWEEN 50 AND 199 AS c, block_number BETWEEN 2 AND 3 AS d, log_index BETWEEN -1 AND 1 AS e FROM transfer WHERE (log_index - 1) NOT BETWEEN 5 AND 9 ORDER BY 1, 2",
    "SELECT count(*) FILTER (WHERE block_number + 1 - 2 > 0) AS a, count(*) FILTER (WHERE 0 < block_number - 2) AS b, count(*) FILTER (WHERE block_number - NULL > 0) AS c, count(*) FILTER (WHERE 3 - block_number >= 1) AS d FROM transfer",
    // An avg argument an integer only before coercion keeps its fractions.
    "SELECT \"from\", avg(CASE WHEN log_index > 0 THEN -8 ELSE 2.5 END) AS a, avg(round(block_number / 7, 1) - 28) AS b FROM transfer GROUP BY 1 ORDER BY 1",
    // Set operations and CASE: a float beside any number is DOUBLE; a literal beside HUGEINT is HUGEINT.
    "SELECT epoch(date_trunc('month', to_timestamp(block_timestamp))) AS x FROM transfer UNION ALL SELECT CAST(block_number AS HUGEINT) FROM transfer ORDER BY 1",
    "SELECT CAST(log_index AS DOUBLE) AS x FROM transfer UNION SELECT CAST(block_number AS DECIMAL(10,2)) FROM transfer ORDER BY 1",
    "SELECT epoch(to_timestamp(block_timestamp)) AS x FROM transfer EXCEPT SELECT CAST(block_number AS HUGEINT) * 2 FROM transfer ORDER BY 1",
    "SELECT greatest(log_index, 1) AS x FROM transfer EXCEPT SELECT CASE WHEN log_index = 1 THEN 18 ELSE CASE WHEN block_number > 2 THEN block_number ELSE -CAST(log_index AS BIGINT) END END FROM transfer ORDER BY 1",
    // Refused by DuckDB's binder, whatever the rows.
    "SELECT count(*) AS n FROM transfer WHERE CAST(to_timestamp(block_timestamp) AS DATE) > 48",
    "SELECT greatest(CAST(to_timestamp(block_timestamp) AS DATE), 3) AS n FROM transfer",
    "SELECT CAST(to_timestamp(block_timestamp) AS DATE) + 1 AS d FROM transfer ORDER BY 1",
    // A subquery inside an aggregate in HAVING.
    "SELECT \"from\", count(*) AS n FROM transfer t GROUP BY 1 HAVING count(*) FILTER (WHERE t.\"to\" IN (SELECT addr FROM label)) > 0 ORDER BY 1",
    // Aggregate subqueries correlated through a non-equality.
    "SELECT block_number, log_index, (SELECT count(*) FROM label x WHERE x.addr = t.\"to\" AND length(x.name) > t.log_index) AS s, (SELECT max(length(x.name)) + 1 FROM label x WHERE length(x.name) > t.log_index + 2) AS m FROM transfer t ORDER BY 1, 2",
    "SELECT block_number, log_index, (SELECT count(*) FROM label x WHERE length(x.name) < t.block_number) + 1 AS c FROM transfer t ORDER BY 1, 2",
    "SELECT count(*) AS n FROM transfer t WHERE (SELECT count(*) FROM label x WHERE length(x.name) > t.log_index + 3) = 0",
    "SELECT t.block_number, (SELECT sum(length(x.name)) FROM label x WHERE x.addr <> t.\"from\") AS s FROM transfer t JOIN transfer u ON u.block_number = t.block_number AND u.log_index = t.log_index ORDER BY 1, 2",
    "SELECT t.block_number, t.log_index, (SELECT avg(length(x.name)) FROM label x WHERE x.addr > t.\"from\") AS a, (SELECT string_agg(x.name, ',' ORDER BY x.name) FROM label x WHERE x.addr < t.\"to\") AS g, (SELECT bool_or(x.name > 'b') FROM label x WHERE x.addr < t.\"to\") AS b FROM transfer t ORDER BY 1, 2",
    // ORDER BY ALL over expressions and a union.
    "SELECT lower(\"from\") AS f, count(*) AS n FROM transfer GROUP BY ALL ORDER BY ALL",
    "SELECT lower(\"to\") AS f, log_index FROM transfer UNION SELECT \"from\", 7 FROM transfer ORDER BY ALL DESC NULLS FIRST",
    // Elements and lengths of lists inside aggregates.
    "SELECT \"from\", array_length(list(block_number ORDER BY block_number, log_index)) AS n, list(log_index ORDER BY block_number, log_index)[1] AS f, list_sort(list(block_number))[-1] AS l FROM transfer GROUP BY 1 ORDER BY 1",
    // `/` is DOUBLE before coercion, beside a HUGEINT too.
    "SELECT x, round(x / 7, 1) - CAST(1 AS HUGEINT) AS a, (x / 7) - CAST(1 AS HUGEINT) AS d FROM range(-5, 5) t(x) ORDER BY x",
    // A TRY_CAST key beside the column it casts.
    "SELECT TRY_CAST(\"value\" AS BIGINT) AS k, \"value\" AS v, count(*) AS n FROM transfer GROUP BY 1, 2 ORDER BY 2",
    // A negative substr start counts from the end; a negative length runs backwards.
    "SELECT substr(\"value\", -3, 2) AS a, substr(\"value\", 0, 2) AS b, substr(\"value\", -2) AS c, substr(\"value\", 2, -2) AS d FROM transfer ORDER BY block_number, log_index",
    "SELECT substr('naïve', -2, 2) AS a, substr('abcdef', -10) AS b, substr('abcdef', 5, -3) AS c, substr('ééé', 0, 2) AS d",
    // string_split: empty delimiter is characters, NULL delimiter is the string unchanged.
    "SELECT string_split('a,b,c', ',')[1] AS a, string_split('a,,c', ',')[2] AS b, string_split('abc', '')[2] AS c, string_split('ab', NULL)[1] AS d, string_split('', '')[1] AS e, str_split('a::b', '::')[-1] AS f",
    "SELECT \"from\", string_split(COALESCE(substr(\"value\", -4), ''), '')[1] AS c FROM transfer ORDER BY block_number, log_index",
    // xor fits a literal to its partner, and mixed signs are HUGEINT.
    "SELECT xor(block_number, 7) AS a, xor(log_index, 3) AS b, xor(CAST(block_number AS BIGINT), log_index) AS c FROM transfer ORDER BY block_number, log_index",
    "SELECT xor(1, 2) AS a, xor(-1, 1) AS b, least(block_number, 2) AS c, power(2, log_index) AS d FROM transfer ORDER BY block_number, log_index",
    // List literals, a full join, a values list, and unnest of a split.
    "SELECT [1, 2, 3][1] AS a, [1, 2, 3][-1] AS b, list_reduce([1, 2, block_number], lambda a, x: a + x) AS r FROM transfer ORDER BY block_number",
    "SELECT [5, block_number, 4][1] AS a, [5, block_number, 4][2] AS b, [5, block_number, 4][-1] AS c, [-1, block_number][1] AS n FROM transfer ORDER BY block_number",
    "SELECT t.block_number, l.name FROM transfer t FULL JOIN label l ON l.addr = t.\"to\" ORDER BY 1, 2",
    "SELECT t.block_number, v.n FROM transfer t JOIN (VALUES (0), (1), (NULL)) v(n) ON t.log_index = v.n ORDER BY 1, 2",
    "SELECT unnest(string_split('a,b,c', ',')) AS c",
    "SELECT decode(from_hex('6162')) AS t, from_hex('abc') AS b, (block_number::VARCHAR) AS s, (5::HUGEINT) AS h FROM transfer ORDER BY block_number LIMIT 1",
    // A list comprehension is list_transform, and IF is a list_filter in front of it.
    "SELECT [x * 2 FOR x IN [1, 2, 3]][1] AS a, [x * 2 FOR x IN [1, 2, 3]][-1] AS b, [x * 2 FOR x IN [1, 2, 3] IF x > 1][1] AS c",
    "SELECT list_reduce([CAST(strpos('0123456789abcdef', c) - 1 AS HUGEINT) FOR c IN string_split(substr('00ab10ff', -4), '')], lambda acc, d: acc * 16 + d) AS h",
    "SELECT [c FOR c IN string_split(\"value\", '') IF c <> '0'][1] AS c FROM transfer ORDER BY block_number, log_index",
    // An aggregate aliased to its own source column, then repeated in ORDER BY: inside the
    // aggregate, `value` is the base column, not the alias (the dashboard's tally statements).
    "SELECT lower(\"to\") AS p, CAST(SUM(CAST(\"value\" AS HUGEINT)) AS VARCHAR) AS value FROM transfer GROUP BY 1 ORDER BY SUM(CAST(\"value\" AS HUGEINT)) DESC",
    // `//` is integer division for integers and HUGEINT; beside a DECIMAL it divides in DOUBLE.
    "SELECT value_dec // 1000000000000000000 AS a FROM transfer ORDER BY block_number, log_index",
    "SELECT sum(value_dec) // 3 AS a, 1::DECIMAL(38,0) // 3 AS b, CAST(7 AS HUGEINT) // 2 AS c, CAST(7 AS DECIMAL(10,0)) // 2 AS d, CAST(7 AS BIGNUM) // 2 AS e, sum(block_number) // 2 AS f FROM transfer",
    "SELECT value_dec // 3 AS a, value_dec // 0 AS b FROM wide",
    // A HUGEINT beside an integer in COALESCE, CASE, greatest, least or NULLIF is still HUGEINT (#69).
    "SELECT COALESCE(CAST(7 AS HUGEINT), 0) // CAST(2 AS HUGEINT) AS a, COALESCE(CAST(7 AS HUGEINT), 0) // 2 AS b, IFNULL(CAST(7 AS HUGEINT), 0) // 2 AS c",
    "SELECT CASE WHEN true THEN CAST(7 AS HUGEINT) ELSE 0 END // 2 AS a, CASE WHEN true THEN CAST(7 AS HUGEINT) ELSE -CAST(7 AS HUGEINT) END // 2 AS b",
    "SELECT CAST(7 AS HUGEINT) // CAST(2 AS HUGEINT) AS a, CAST(7 AS DECIMAL(10,0)) // 2 AS b, CAST(CAST(7 AS HUGEINT) AS DECIMAL(38,0)) // 2 AS c",
    "SELECT sum(CAST(value AS HUGEINT)) // 3 AS a, COALESCE(sum(CAST(value AS HUGEINT)), 0) // 3 AS b FROM transfer",
    "SELECT greatest(CAST(7 AS HUGEINT), 0) // 2 AS a, least(CAST(7 AS HUGEINT), 9) // 2 AS b, NULLIF(CAST(7 AS HUGEINT), 0) // 2 AS c",
    "SELECT COALESCE(value_dec, 0) // 2 AS a, COALESCE(CAST(7 AS HUGEINT), CAST(0 AS DECIMAL(38,0))) // 2 AS b, CASE WHEN true THEN CAST(7 AS HUGEINT) ELSE CAST(0 AS DECIMAL(10,0)) END // 2 AS c FROM transfer ORDER BY block_number, log_index",
    "SELECT COALESCE(CAST(value AS HUGEINT), block_number) // 2 AS a, CAST(7 AS HUGEINT) * (1000000 - COALESCE(CAST(3 AS HUGEINT), 0)) // 1000000 AS b FROM transfer ORDER BY block_number, log_index",
    "SELECT d // 2 AS a, x // 4 AS b FROM (SELECT COALESCE(CAST(value AS HUGEINT), 0) AS d, CASE WHEN log_index > 0 THEN CAST(value AS HUGEINT) ELSE 0 END AS x FROM transfer) t ORDER BY 1, 2",
    "SELECT COALESCE(CAST(7 AS UBIGINT), CAST(7 AS HUGEINT)) // 2 AS a, COALESCE(CAST(7 AS INTEGER), CAST(7 AS DECIMAL(10,0))) // 2 AS b",
    // IFNULL is COALESCE: it keeps a HUGEINT, a DECIMAL and an integer exact, and its own name.
    "SELECT IFNULL(CAST(value AS HUGEINT), 0) AS a, IFNULL(value_dec, 0) AS c, IFNULL(block_number, 0) AS d, IFNULL(NULL, 'x') AS e FROM transfer ORDER BY block_number, log_index",
    "SELECT IFNULL(block_number, 0), ifnull(\"from\", 'z') FROM transfer ORDER BY block_number, log_index",
    // An ORDER BY expression over an output alias with no source column of that name reads the alias.
    "SELECT \"from\" AS who, sum(value_dec) AS s FROM transfer GROUP BY who ORDER BY -s",
    "SELECT \"from\" AS who, sum(value_dec) AS s FROM transfer GROUP BY who ORDER BY s::DOUBLE DESC, who",
    "SELECT \"from\" AS who, sum(value_dec) AS s FROM transfer GROUP BY who ORDER BY CAST(s AS VARCHAR), who",
    // `%` by zero is NULL, a row at a time.
    "SELECT block_number % 0 AS a, log_index % 0 AS b, 7 % 0 AS c, 7.5::DOUBLE % 0 AS d, 7.5 % 0 AS e, value_dec % 0 AS f, block_number % log_index AS g FROM transfer ORDER BY block_number, log_index",
    // The least signed integer `% -1` overflows, as its `//` does (#41).
    "SELECT CAST(-9223372036854775808 AS BIGINT) % -1 AS a",
    "SELECT CAST(-2147483648 AS INTEGER) % -1 AS a",
    "SELECT CAST(-9223372036854775807 AS BIGINT) % -1 AS a, CAST(-7 AS BIGINT) % -2 AS b, CAST(-2147483647 AS INTEGER) % -1 AS c",
    // HUGEINT's least value overflows `% -1` in DuckDB and refuses at its cast here (#42, #50).
    "SELECT CAST('-170141183460469231731687303715884105728' AS HUGEINT) % -1 AS a",
    "SELECT CAST('-9223372036854775808' AS HUGEINT) % -1 AS a, CAST('-99999999999999999999999999999999999999' AS HUGEINT) % CAST(-1 AS HUGEINT) AS b, CAST('-7' AS HUGEINT) % -2 AS c",
    // An ordered aggregate orders its own input, not that of an unordered list() beside it.
    "SELECT string_agg(x, ',' ORDER BY x DESC) AS s, to_json(array_agg(x)) AS l, to_json(list(x)) AS m FROM (VALUES ('a'), ('c'), ('b'), (NULL)) t(x)",
    "SELECT \"from\", string_agg(value, ',' ORDER BY block_number DESC, log_index DESC) AS s, to_json(list(value)) AS m FROM transfer GROUP BY 1 ORDER BY 1",
    // LIKE has no escape character unless ESCAPE names one (#17).
    r#"SELECT 'ab' LIKE 'a\b' AS a, 'a\b' LIKE 'a\\b' AS b, 'a%' LIKE 'a$%' ESCAPE '$' AS d, 'a$' LIKE 'a$$' ESCAPE '$' AS f, 'a_c' LIKE 'a\_c' ESCAPE '\' AS g, 'A%' ILIKE 'a#%' ESCAPE '#' AS h, 'a' LIKE 'a' ESCAPE '' AS i"#,
    r#"SELECT "from" LIKE '0x_' AS a, "value" LIKE '25%' AS b, "from" || '\' LIKE '%\' AS c FROM transfer ORDER BY block_number, log_index"#,
    r#"SELECT s LIKE p AS m FROM (VALUES ('a\b', 'a\b'), ('ab', 'a\b'), ('a\b', 'a\\b'), ('x', NULL)) t(s, p)"#,
    r#"SELECT 'a$' LIKE 'a$' ESCAPE '$' AS a"#,
    // Text to DECIMAL(p,0) and HUGEINT in every spelling DuckDB reads (#18).
    "SELECT TRY_CAST('1e3' AS DECIMAL(38,0)) a, TRY_CAST('1.5e18' AS HUGEINT) b, TRY_CAST('1_000' AS HUGEINT) c, TRY_CAST(' 12 ' AS HUGEINT) d, TRY_CAST('+7' AS DECIMAL(38,0)) e, TRY_CAST('1.5' AS DECIMAL(38,0)) f, TRY_CAST('-2.5' AS HUGEINT) g, TRY_CAST('0x10' AS HUGEINT) h, TRY_CAST('abc' AS HUGEINT) i, TRY_CAST('' AS DECIMAL(38,0)) j",
    "SELECT TRY_CAST('1e38' AS DECIMAL(38,0)) a, TRY_CAST('99999999999999999999999999999999999999.5' AS DECIMAL(38,0)) c, TRY_CAST('123' AS DECIMAL(2,0)) d, TRY_CAST('1e2' AS DECIMAL(3,0)) e, TRY_CAST('1e-1' AS DECIMAL(38,0)) h, TRY_CAST('5e-1' AS HUGEINT) i, TRY_CAST('1E3' AS DECIMAL(38,0)) j, TRY_CAST('1__0' AS HUGEINT) k, TRY_CAST('1__0' AS BIGINT) l, TRY_CAST('1_0e1_0' AS HUGEINT) m, TRY_CAST('1_0.5_5' AS DECIMAL(38,0)) n",
    "SELECT CAST('1e3' AS DECIMAL(38,0)) a, CAST('1_000' AS HUGEINT) b, CAST(' 7 ' AS HUGEINT) c, CAST('1.5' AS DECIMAL(38,0)) d",
    "SELECT CAST('1e39' AS DECIMAL(38,0)) a",
    // HUGEINT is DECIMAL(38,0) here: 10^38 refuses where DuckDB's 128 bits hold it (#42).
    "SELECT CAST('100000000000000000000000000000000000000' AS HUGEINT) AS a",
    "SELECT CAST('99999999999999999999999999999999999999' AS HUGEINT) AS a",
    "SELECT TRY_CAST(\"value\" || 'e1' AS HUGEINT) AS a, TRY_CAST(\"value\" || '.5' AS DECIMAL(38,0)) AS b FROM transfer ORDER BY block_number, log_index",
    // Statements DuckDB answered and burrmill refused (#20).
    "SELECT to_json(1.5::DOUBLE) a, to_json(0.1::DOUBLE) b, to_json(1e20::DOUBLE) c, to_json(1e-7::DOUBLE) d, to_json(2.0::DOUBLE) e, to_json(-0.0::DOUBLE) f, to_json(1e21::DOUBLE) g, to_json(1e-6::DOUBLE) h, to_json(123456789012345678901234.0::DOUBLE) i, to_json('nan'::DOUBLE) j, to_json('-inf'::DOUBLE) k, to_json(0.1::FLOAT) l, to_json(1/3) m",
    "SELECT to_json(1.50) a, to_json(100.00) b, to_json(-0.05) c, to_json(CAST(12345678901234567890.123 AS DECIMAL(38,3))) d, to_json(0.0) e, to_json(struct_pack(x := 1.5::DOUBLE, y := 2.25)) f, to_json([1.5::DOUBLE, NULL]) g, to_json(CAST(-12.340 AS DECIMAL(10,3))) h",
    "SELECT to_json(list(s)) AS j FROM (SELECT struct_pack(k := log_index, r := CAST(value AS DOUBLE) / 1e18) AS s FROM transfer ORDER BY block_number, log_index) t",
    "SELECT printf('%.2f', 1.5) a, printf('%f', 2.25) b, printf('%5.1f', 1.25) c, printf('%g', 1.5) d, printf('%e', 1.5) e, format('{}', 1.5) f, format('{}', 100.00) g, format('{:.3f}', 2.5) h, format('{}', CAST(12345678901234567890.123 AS DECIMAL(38,3))) i",
    "SELECT printf('%d', 1.5) a",
    "SELECT printf('%,d', 1234567) a, printf('%,d', -1234) b, printf('%,d', 12) c, printf('%,5d|%-,8d|%,x', 1234, 1234, 255) d, printf('%,f', 1234.5::DOUBLE) e, printf('%,.0f', 999.5::DOUBLE) f, printf('%,.2f', 1234567.891) g, printf('%,X|%#,x|%,s', 4096, 4096, 'ab') h",
    "SELECT printf('%,d', CAST(value AS HUGEINT)) a, hex(CAST(value AS HUGEINT)) b FROM transfer ORDER BY block_number, log_index",
    "SELECT hex(CAST(255 AS HUGEINT)) a, hex(CAST(-1 AS HUGEINT)) b, hex(CAST(0 AS HUGEINT)) c, hex(CAST(-256 AS HUGEINT)) d",
    "SELECT if(1 < 2, 'a', 'b') a, if(NULL, 1, 2) b, if(false, 1, 2.5) c, 'abc' ^@ 'ab' d, 'abc' ^@ 'b' e, NULL ^@ 'a' f, 'ab' || 'c' ^@ 'abc' g",
    "SELECT if(log_index > 0, 'big', NULL) AS a, \"from\" ^@ '0xa' AS b FROM transfer ORDER BY block_number, log_index",
    "SELECT list_reverse_sort([3, NULL, 1, 2])[1] a, list_reverse_sort([3, NULL, 1, 2])[4] b, list_reverse_sort(['b', 'a', NULL])[2] c, list_reverse_sort(NULL) d",
    "SELECT split_part('a,b,c', ',', 0) a, split_part('a,b,c', ',', -1) b, split_part('a,b,c', ',', 5) c",
    "SELECT split_part(s, ',', n) AS p FROM (VALUES ('a,b', 0), (NULL, 0), ('a,b', NULL), ('a,b', 2)) t(s, n)",
    "SELECT TIMESTAMP '2024-01-02 03:04' = TIMESTAMP '2024-01-02 03:04:00' AS a, CAST(TIMESTAMP '2024-01-02T03:04' AS VARCHAR) AS b",
    "SELECT TIMESTAMP '2024-01-02 03' a",
    "SELECT count(*) AS n FROM transfer WHERE block_number < '150' AND '2' <= block_number",
    "SELECT count(*) AS n FROM (SELECT CAST(x AS BIGINT) AS u FROM range(5) t(x)) s WHERE u < '2.5'",
    "FROM transfer SELECT block_number WHERE log_index = 1",
    "FROM label",
    "SELECT block_number FROM transfer ORDER BY 1 LIMIT 1.5",
    "SELECT block_number FROM transfer ORDER BY 1 LIMIT 0.4",
    // `list_sum` is `sum` over the elements: HUGEINT for integers, DECIMAL(38,s) for decimals.
    "SELECT list_sum([1, 2, 3]) AS a, list_sum([1.5, 2.25]) AS b, list_sum([1.5::DOUBLE, 2]) AS c, list_sum([]) AS d, list_sum(NULL) AS e, list_sum([NULL, 1]) AS f, list_sum([NULL::INT]) AS g",
    "SELECT \"from\", list_sum(list(block_number)) AS s, list_sum(list(CAST(value AS HUGEINT))) AS v FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT list_sum([1, 2]) * 2 AS a, list_sum([7, 2]) // 2 AS b",
    "SELECT list_min([3, 1, NULL, 2]) AS a, list_max([3, 1, 2]) AS b, list_min([]) AS c, list_min(['b', 'a']) AS d, list_min(NULL) AS e",
    "SELECT \"from\", list_min(list(block_number)) AS lo, list_max(list(log_index)) AS hi FROM transfer GROUP BY 1 ORDER BY 1",
    // DuckDB answers these; burrmill refuses each by name (#20).
    "SELECT approx_count_distinct(block_number) AS n FROM transfer",
    "SELECT list_aggregate([1, 2, 3], 'sum') AS a",
    "SELECT strptime('2024-01-02 03:04:05', '%Y-%m-%d %H:%M:%S') AS a",
    "SELECT try_strptime('2024-01-02', '%Y-%m-%d') AS a",
    "SELECT COLUMNS('a') FROM (SELECT 1 AS a, 2 AS b) t",
    "SELECT 'abc' GLOB 'a*' AS a",
    "PIVOT (SELECT 1 AS a, 'x' AS k) ON k USING sum(a)",
    // What each refusal above names instead (#173).
    "SELECT COUNT(DISTINCT block_number) AS n FROM transfer",
    "SELECT (SELECT sum(v) FROM (SELECT unnest([1, 2, 3]) AS v)) AS a",
    "SELECT * EXCLUDE (b) FROM (SELECT 1 AS a, 2 AS b) t",
    "SELECT 'abc' LIKE 'a_c%' AS a",
    r"SELECT s LIKE p ESCAPE '\' AS m FROM (VALUES ('a%', 'a\%'), ('ab', 'a\%')) t(s, p)",
    "SELECT sum(a) FILTER (WHERE k = 'x') AS x FROM (SELECT 1 AS a, 'x' AS k) t",
    "SELECT 'a' AS k, a AS v FROM (SELECT 1 AS a, 2 AS b) t UNION ALL SELECT 'b', b FROM (SELECT 1 AS a, 2 AS b) t ORDER BY 1",
    // What strptime and try_strptime name for ISO 8601 text: DuckDB's TIMESTAMP, in microseconds.
    "SELECT CAST('2024-01-02 03:04:05' AS TIMESTAMP) AS a",
    "SELECT CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP) AS a, TRY_CAST('2024-01-02 03:04:05.1234567' AS TIMESTAMP) AS b, TRY_CAST('x' AS TIMESTAMP) AS c, '2024-01-02T03:04:05'::TIMESTAMP AS d, CAST('2024-01-02' AS DATETIME) AS e",
    "SELECT CAST(CAST('2024-01-02 03:04:05.5' AS TIMESTAMP) AS VARCHAR) AS a, date_trunc('hour', CAST('2024-01-02 03:04:05' AS TIMESTAMP)) AS b, CAST('2024-01-02 03:04:05' AS TIMESTAMP) + INTERVAL 1 DAY AS c, CAST('2024-01-02' AS TIMESTAMP) < TIMESTAMP '2024-01-03' AS d",
    "SELECT CAST(to_timestamp(block_timestamp) AS TIMESTAMP) AS t, CAST(CAST(to_timestamp(block_timestamp) AS VARCHAR) AS TIMESTAMP) AS u FROM transfer ORDER BY 1 LIMIT 2",
    "SELECT * FROM unnest([1, 2]) t(x)",
    "SELECT * FROM range(DATE '2024-01-01', DATE '2024-01-03', INTERVAL 1 DAY) t(d)",
    "SELECT * FROM (SELECT 3 AS n) s, range(s.n) t(x)",
    "SELECT DISTINCT a FROM (VALUES (1, 2), (2, 1)) t(a, b) ORDER BY b",
    "WITH x(a, a) AS (SELECT 1, 2) SELECT * FROM x",
    "SELECT CAST(struct_pack(a := 1, b := 'x') AS VARCHAR) AS a",
    // #47: windows over a table cross joined with the one row a filter leaves of a grouped relation.
    "SELECT b.\"from\", b.block_number, b.log_index, p.n, COUNT(*) OVER () AS total, COUNT(*) FILTER (WHERE b.log_index > 0) OVER () AS active FROM transfer b CROSS JOIN (SELECT f, n FROM (SELECT \"from\" AS f, count(*) AS n FROM transfer GROUP BY 1) g WHERE f = (SELECT min(\"from\") FROM transfer)) p ORDER BY 2 DESC, 3, 1 LIMIT 25",
];

/// Differences that stand, and why.
const KNOWN: &[(&str, &str)] = &[
    (
        "SELECT TRY_CAST(1.5e38::DOUBLE AS HUGEINT) AS a",
        "#44's band from a DOUBLE or FLOAT: DuckDB answers a float under 2^127 in magnitude, and past \
         10^38 DECIMAL(38,0) cannot hold it, so the value is refused by name; its NULL test and its \
         sum answer as DuckDB does (#63)",
    ),
    (
        "SELECT TRY_CAST('100000000000000000000000000000000000000' AS HUGEINT) AS a",
        "the same 10^38 to 2^127 - 1 as the CAST of it, which DECIMAL(38,0) cannot hold. TRY_CAST \
         refuses it by name rather than answering NULL, a value DuckDB does not give; a NULL test \
         or a sum of the cast still answers as DuckDB does (#44)",
    ),
    (
        "SELECT CAST('100000000000000000000000000000000000000' AS HUGEINT) AS a",
        "HUGEINT is DECIMAL(38,0), the type nuthatch's planner gives every HUGEINT column, so 10^38 \
         to 2^127 - 1 refuses as a cast to it does; DuckDB's 128-bit HUGEINT holds them. Widening \
         would change every HUGEINT column's type (#42)",
    ),
    (
        "SELECT typeof(1) AS a",
        "a whole-number literal is a BIGINT here and an INTEGER in DuckDB; the values agree, the \
         name of the type does not",
    ),
    (
        "SELECT year(to_timestamp(block_timestamp)) AS y FROM transfer WHERE 'bob' <> CAST(to_timestamp(block_timestamp) AS VARCHAR) ORDER BY 1",
        "a DuckDB 1.5 bug (docs/upstream/duckdb-cast-comparison-null-constant.md): once the zone is \
         consulted, here by year(), text compared with a TIMESTAMPTZ cast to text is compared with \
         NULL. Burrmill compares the text",
    ),
    (
        "SELECT approx_count_distinct(block_number) AS n FROM transfer",
        "DuckDB's HyperLogLog estimate is its own; refused by name rather than estimated differently",
    ),
    (
        "SELECT list_aggregate([1, 2, 3], 'sum') AS a",
        "refused by name: no list_aggregate here",
    ),
    (
        "SELECT strptime('2024-01-02 03:04:05', '%Y-%m-%d %H:%M:%S') AS a",
        "refused by name: DuckDB's format codes are not chrono's",
    ),
    (
        "SELECT try_strptime('2024-01-02', '%Y-%m-%d') AS a",
        "refused by name, as strptime is",
    ),
    (
        "SELECT COLUMNS('a') FROM (SELECT 1 AS a, 2 AS b) t",
        "refused by name: no COLUMNS() expansion here",
    ),
    (
        "SELECT 'abc' GLOB 'a*' AS a",
        "refused by name: no GLOB here",
    ),
    (
        "PIVOT (SELECT 1 AS a, 'x' AS k) ON k USING sum(a)",
        "refused by name: no PIVOT here",
    ),
    (
        "SELECT * FROM unnest([1, 2]) t(x)",
        "refused by name: unnest is a table function here only in the select list",
    ),
    (
        "SELECT * FROM range(DATE '2024-01-01', DATE '2024-01-03', INTERVAL 1 DAY) t(d)",
        "refused by name: range takes integer literals here",
    ),
    (
        "SELECT * FROM (SELECT 3 AS n) s, range(s.n) t(x)",
        "refused by name: range takes integer literals here",
    ),
    (
        "SELECT DISTINCT a FROM (VALUES (1, 2), (2, 1)) t(a, b) ORDER BY b",
        "refused by name: DuckDB orders a DISTINCT by an expression it does not select, with no defined row for it",
    ),
    (
        "WITH x(a, a) AS (SELECT 1, 2) SELECT * FROM x",
        "refused by name: a CTE column list that repeats a name",
    ),
    (
        "SELECT CAST(struct_pack(a := 1, b := 'x') AS VARCHAR) AS a",
        "refused by name: no STRUCT to VARCHAR cast here",
    ),
];

fn fixture(root: &std::path::Path) -> anyhow::Result<()> {
    let segs = root.join("segments");
    std::fs::create_dir_all(&segs)?;
    let s = |v: &[&str]| Arc::new(StringArray::from(v.to_vec())) as ArrayRef;
    let u = |v: &[u64]| Arc::new(UInt64Array::from(v.to_vec())) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, true),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("enabled", DataType::Utf8, true),
        Field::new("tokensRewards", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            u(&[1, 2, 2, 3, 150, 301]),
            u(&[0, 0, 1, 0, 2, 0]),
            u(&[
                1700000000, 1700000012, 1700000012, 1700086400, 1700172800, 1703980800,
            ]),
            s(&["0xa", "0xb", "0xA", "0xc", "0xa", "0xd"]),
            s(&["0xb", "0xc", "0xc", "0xa", "0xe", "0xa"]),
            s(&["10", "4", "1", "010", "250000000000000000000", "7"]),
            s(&["true", "false", "true", "true", "false", "true"]),
            s(&["5", "6", "7", "8", "9", "10"]),
        ],
    )?;
    let f = std::fs::File::create(segs.join(format!("transfer-{:064x}.parquet", 1)))?;
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    let lschema = Arc::new(Schema::new(vec![
        Field::new("addr", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let labels = RecordBatch::try_new(
        lschema.clone(),
        vec![s(&["0xa", "0xc"]), s(&["alice", "carol"])],
    )?;
    let f = std::fs::File::create(segs.join(format!("label-{:064x}.parquet", 2)))?;
    let mut w = parquet::arrow::ArrowWriter::try_new(f, lschema, None)?;
    w.write(&labels)?;
    w.close()?;
    let wschema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let wide = RecordBatch::try_new(
        wschema.clone(),
        vec![s(&[
            "100000000000000000000000000000000000000",
            "-99999999999999999999999999999999999995",
        ])],
    )?;
    let f = std::fs::File::create(segs.join(format!("wide-{:064x}.parquet", 3)))?;
    let mut w = parquet::arrow::ArrowWriter::try_new(f, wschema, None)?;
    w.write(&wide)?;
    w.close()?;
    std::fs::write(
        root.join("schema.json"),
        concat!(
            r#"{"tables":[{"table":"transfer","columns":["#,
            r#"{"name":"from","storage":"text"},{"name":"to","storage":"text"},"#,
            r#"{"name":"value","storage":"word32"},"#,
            r#"{"name":"tokensRewards","storage":"word32"}]},"#,
            r#"{"table":"wide","columns":[{"name":"value","storage":"word32"}]}]}"#,
        ),
    )?;
    Ok(())
}

pub fn run() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    fixture(tmp.path())?;
    let segs = tmp.path().join("segments");
    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch(&format!(
        "CREATE VIEW transfer AS SELECT *, TRY_CAST(\"value\" AS DECIMAL(38,0)) AS \"value_dec\", \
         (\"value\" IS NOT NULL AND TRY_CAST(\"value\" AS DECIMAL(38,0)) IS NULL) AS \"value_overflow\", \
         TRY_CAST(\"tokensRewards\" AS DECIMAL(38,0)) AS \"tokensRewards_dec\", \
         (\"tokensRewards\" IS NOT NULL AND TRY_CAST(\"tokensRewards\" AS DECIMAL(38,0)) IS NULL) AS \"tokensRewards_overflow\" \
         FROM read_parquet('{0}/transfer-*.parquet');
         CREATE VIEW label AS SELECT * FROM read_parquet('{0}/label-*.parquet');
         CREATE VIEW wide AS SELECT *, \
         TRY_CAST(\"value\" AS DECIMAL(38,0)) AS \"value_dec\" \
         FROM read_parquet('{0}/wide-*.parquet');",
        segs.display()
    ))?;
    let root = tmp.path().to_path_buf();
    let engine = std::thread::spawn(move || burrmill::Engine::open_nest(&root))
        .join()
        .expect("open")?;
    let engine = Arc::new(engine);
    let mut failed = 0;
    // `SQL=<query>` runs that one query against the fixture instead of the corpus.
    let adhoc = std::env::var("SQL").ok();
    let corpus: Vec<&str> = match &adhoc {
        Some(q) => vec![q.as_str()],
        None => CORPUS.to_vec(),
    };
    for sql in corpus {
        let want = match crate::encode_parity::nuthatch_rows(&duck, sql) {
            Ok(v) => serde_json::to_string(&v)?,
            Err(e) => format!("ERROR {}", e.to_string().lines().next().unwrap_or("")),
        };
        let e2 = Arc::clone(&engine);
        let q = sql.to_string();
        let got = std::thread::spawn(move || -> String {
            match e2.sql(&q) {
                Ok(bs) => {
                    let mut rows = Vec::new();
                    for b in &bs {
                        match burrmill::df::encode::rows(b) {
                            Ok(r) => rows.extend(r),
                            Err(e) => return format!("ERROR {e}"),
                        }
                    }
                    serde_json::to_string(&Value::Array(rows)).unwrap()
                }
                Err(e) => format!("ERROR {}", e.to_string().replace('\n', " | ")),
            }
        })
        .join()
        .expect("engine thread");
        let known = KNOWN.iter().find(|(k, _)| *k == sql).map(|(_, why)| *why);
        let both_refuse = want.starts_with("ERROR") && got.starts_with("ERROR");
        let tag = match (want == got || both_refuse, known) {
            (true, _) if both_refuse => "BOTH-REFUSE".to_string(),
            (true, _) => "SAME ".to_string(),
            (false, Some(why)) => format!("KNOWN ({why})"),
            (false, None) => {
                failed += 1;
                "DIFF ".to_string()
            }
        };
        println!("{tag}  {sql}");
        if (want != got && !both_refuse) || adhoc.is_some() {
            let cap = if adhoc.is_some() { usize::MAX } else { 300 };
            println!(
                "    duckdb   {}",
                want.chars().take(cap).collect::<String>()
            );
            println!("    burrmill {}", got.chars().take(cap).collect::<String>());
        }
    }
    println!("DIALECT\tcases={}\tdiffering={failed}", corpus_len(&adhoc));
    std::thread::spawn(move || drop(engine))
        .join()
        .expect("drop engine");
    anyhow::ensure!(failed == 0, "{failed} dialect differences");
    Ok(())
}

fn corpus_len(adhoc: &Option<String>) -> usize {
    if adhoc.is_some() {
        1
    } else {
        CORPUS.len()
    }
}

/// `duck-names <sql>`: DuckDB's own column names for a statement over an empty `t`, one per line.
pub fn duck_names(sql: &str) -> anyhow::Result<()> {
    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch(
        "CREATE TABLE t(\"from\" VARCHAR, \"to\" VARCHAR, \"value\" VARCHAR, block_number UBIGINT, \
         log_index UBIGINT, \"tokensRewards\" VARCHAR);",
    )?;
    let mut stmt = duck.prepare(sql)?;
    let rows = stmt.query([])?;
    for n in rows.as_ref().map(|s| s.column_names()).unwrap_or_default() {
        println!("{n}");
    }
    Ok(())
}

/// `duck-keywords`: DuckDB's keyword list with categories, for the naming printer's quoting rule.
pub fn duck_keywords() -> anyhow::Result<()> {
    let duck = duckdb::Connection::open_in_memory()?;
    let mut stmt =
        duck.prepare("SELECT keyword_name, keyword_category FROM duckdb_keywords() ORDER BY 1")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let (k, c): (String, String) = (r.get(0)?, r.get(1)?);
        println!("{k}\t{c}");
    }
    Ok(())
}

/// `duck-eval <sql>`: DuckDB's answer as nuthatch's JSON, for probing semantics.
pub fn duck_eval(sql: &str) -> anyhow::Result<()> {
    let duck = duckdb::Connection::open_in_memory()?;
    println!(
        "{}",
        serde_json::to_string(&crate::encode_parity::nuthatch_rows(&duck, sql)?)?
    );
    Ok(())
}
