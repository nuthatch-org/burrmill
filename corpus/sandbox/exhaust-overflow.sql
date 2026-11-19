-- expect: Overflow
-- tries: a wrapped integer that would read as a plausible answer
SELECT CAST(9223372036854775807 AS BIGINT) + 1 AS n
