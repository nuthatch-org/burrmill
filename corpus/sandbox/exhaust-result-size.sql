-- expect: LimitExceeded
-- says: result cap
-- tries: an answer of over a hundred megabytes, read by the host until its cap
SELECT range, repeat('a', 10) AS s FROM range(9000000)
