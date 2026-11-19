-- expect: LimitExceeded
-- tries: array_agg of every row into one list
SELECT array_agg(repeat('a', 1000)) AS l FROM range(1000000)
