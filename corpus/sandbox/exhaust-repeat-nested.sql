-- expect: LimitExceeded
-- tries: a value under the limit repeated past it
SELECT repeat(repeat('a', 10000), 10000) AS s
