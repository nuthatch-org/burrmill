-- Each statement's receipt is in receipts.jsonl, in this order. `strict:` runs one under strict decimals.
SELECT COUNT(*) AS n FROM transfer
SELECT who, SUM(CAST(amount AS HUGEINT)) AS total, COUNT(*) AS n FROM transfer GROUP BY who
SELECT who, SUM(price) AS s, AVG(price) AS a, MIN(price) AS lo, MAX(price) AS hi FROM transfer GROUP BY who
SELECT block_number, who FROM transfer ORDER BY who LIMIT 25
SELECT * FROM transfer LIMIT 10
SELECT who, row_number() OVER (PARTITION BY who ORDER BY price) AS r, block_number FROM transfer
SELECT a.who, COUNT(*) AS pairs FROM transfer a JOIN transfer b ON a.who = b.who AND a.block_number < b.block_number WHERE a.block_number < 50 GROUP BY a.who
SELECT who, array_agg(block_number) AS blocks FROM transfer WHERE block_number % 17 = 0 GROUP BY who
SELECT DISTINCT who FROM transfer
strict:SELECT who, div(SUM(CAST(amount AS DECIMAL(38,0))), COUNT(*), 4, 'half_even') AS mean FROM transfer GROUP BY who
strict:SELECT CAST(CAST(amount AS DECIMAL(38,0)) AS DECIMAL(38,2)) AS a FROM transfer WHERE block_number < 5
