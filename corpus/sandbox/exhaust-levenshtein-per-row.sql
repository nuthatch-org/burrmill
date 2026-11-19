-- expect: LimitExceeded
-- says: levenshtein over
-- tries: levenshtein under the pair bound, over many rows
SELECT sum(levenshtein(repeat(CAST(range % 10 AS VARCHAR), 3000), repeat('b', 3000))) AS n FROM range(10000)
