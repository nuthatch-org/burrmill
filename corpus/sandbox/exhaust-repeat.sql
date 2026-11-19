-- expect: LimitExceeded
-- tries: one value of a gigabyte
SELECT repeat('a', 1000000000) AS s
