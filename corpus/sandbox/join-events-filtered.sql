-- expect: Timeout | LimitExceeded
-- tries: the same join with a filter on every pair
SELECT count(*) AS n FROM events a JOIN events b ON a.who = b.who AND a.block_number <> b.block_number
